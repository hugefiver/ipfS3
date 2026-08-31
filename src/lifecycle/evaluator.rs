use chrono::{DateTime, Days, TimeZone, Utc};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter};

use crate::{
    error::{AppError, AppResult},
    lifecycle::{
        config::from_canonical_json,
        filter::matches_filter,
        model::{
            CanonicalLifecycleConfiguration, ClaimedLifecycleScan, CurrentExpiration,
            LifecycleActionKind, LifecycleCandidate, LifecycleCandidatePage, LifecycleRuleStatus,
            NewLifecycleAction, NoncurrentExpiration, RuleIdentity,
        },
    },
    pinning::tags::ObjectTag,
    store::{
        entities::object_version,
        lifecycle_action::{idempotency_key, insert_idempotent},
        lifecycle_scan::scan_candidate_page,
        object_version::VersionKind,
        pinning::tags::list_object_tags,
    },
};

/// All facts the pure lifecycle evaluator needs for one immutable target. The caller owns the
/// database reads; in particular, tags are supplied only for matching and never copied into an
/// action.
pub struct LifecycleEvaluationContext<'a> {
    pub candidate: &'a LifecycleCandidate,
    pub tags: &'a [ObjectTag],
    pub public_version_count: u64,
    pub newer_noncurrent_count: u64,
    pub config_revision: i64,
    pub configuration: &'a CanonicalLifecycleConfiguration,
    pub database_now: DateTime<Utc>,
}

struct LifecycleProposal {
    action: NewLifecycleAction,
    stable_rule_identity: String,
}

/// Returns the first UTC midnight strictly after `days` full calendar days from `start`.
pub fn next_utc_midnight_after_full_days(
    start: DateTime<Utc>,
    days: u32,
) -> AppResult<DateTime<Utc>> {
    let aged = start
        .checked_add_days(Days::new(u64::from(days)))
        .ok_or_else(|| {
            AppError::InvalidArgument(
                "lifecycle due time is outside the UTC timestamp range".to_owned(),
            )
        })?;
    let aged_date = aged.date_naive();
    // The result must be strictly after `aged`: an aged instant at midnight cannot use that same
    // midnight, while every later instant on the date also advances to the following date.
    let next_date = aged_date.succ_opt().ok_or_else(|| {
        AppError::InvalidArgument(
            "lifecycle due time is outside the UTC timestamp range".to_owned(),
        )
    })?;
    let midnight = next_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| AppError::InvalidArgument("lifecycle due time is invalid".to_owned()))?;
    Ok(Utc.from_utc_datetime(&midnight))
}

/// Evaluates one immutable candidate using only the supplied context and returns its single
/// deterministic durable-action proposal, if eligible.
pub fn evaluate_candidate(
    context: &LifecycleEvaluationContext<'_>,
) -> AppResult<Option<NewLifecycleAction>> {
    if context.config_revision <= 0 {
        return Err(AppError::InvalidArgument(
            "lifecycle configuration revision must be positive".to_owned(),
        ));
    }

    let mut proposals = Vec::new();
    for (ordinal, rule) in context.configuration.rules.iter().enumerate() {
        if rule.status != LifecycleRuleStatus::Enabled
            || !matches_filter(
                &rule.selector,
                &context.candidate.target.key,
                context.candidate.size,
                context.tags,
            )
        {
            continue;
        }

        let (rule_identity, stable_rule_identity) = rule_identity(rule.id.as_deref(), ordinal)?;
        if context.candidate.is_latest {
            collect_current_proposal(
                &mut proposals,
                context,
                rule.expiration.as_ref(),
                rule_identity,
                stable_rule_identity,
            )?;
        } else {
            collect_noncurrent_proposal(
                &mut proposals,
                context,
                rule.noncurrent_version_expiration.as_ref(),
                rule_identity,
                stable_rule_identity,
            )?;
        }
    }

    Ok(select_winner(proposals).map(|proposal| proposal.action))
}

/// Evaluates one bounded scan page using the scan claim's database clock and idempotently writes
/// no more than one action for each candidate. It deliberately does not retain scan-time tags in
/// the durable action; execution must reread them later.
pub async fn schedule_claimed_scan_page<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleScan,
    page_limit: u64,
) -> AppResult<LifecycleCandidatePage> {
    let configuration = from_canonical_json(&claim.canonical_json)?;
    let page = scan_candidate_page(db, &claim.bucket, claim.cursor.as_ref(), page_limit).await?;

    for candidate in &page.candidates {
        let tags = match candidate.target.object_id.as_deref() {
            Some(object_id) => list_object_tags(db, object_id).await?,
            None => Vec::new(),
        };
        let public_version_count = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq(&candidate.target.bucket))
            .filter(object_version::Column::Key.eq(&candidate.target.key))
            .count(db)
            .await?;
        let newer_noncurrent_count = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq(&candidate.target.bucket))
            .filter(object_version::Column::Key.eq(&candidate.target.key))
            .filter(object_version::Column::IsLatest.eq(false))
            .filter(object_version::Column::Sequence.gt(candidate.target.sequence))
            .count(db)
            .await?;
        let context = LifecycleEvaluationContext {
            candidate,
            tags: &tags,
            public_version_count,
            newer_noncurrent_count,
            config_revision: claim.config_revision,
            configuration: &configuration,
            database_now: claim.database_now,
        };
        if let Some(mut action) = evaluate_candidate(&context)? {
            action.idempotency_key = idempotency_key(&action)?;
            insert_idempotent(db, action, claim.database_now).await?;
        }
    }

    Ok(page)
}

fn collect_current_proposal(
    proposals: &mut Vec<LifecycleProposal>,
    context: &LifecycleEvaluationContext<'_>,
    expiration: Option<&CurrentExpiration>,
    rule_identity: RuleIdentity,
    stable_rule_identity: String,
) -> AppResult<()> {
    let Some(expiration) = expiration else {
        return Ok(());
    };

    match context.candidate.target.kind {
        VersionKind::Object => {
            let Some(due_at) = current_due_at(expiration, context.candidate)? else {
                return Ok(());
            };
            if context.database_now >= due_at {
                proposals.push(proposal(
                    context,
                    rule_identity,
                    stable_rule_identity,
                    LifecycleActionKind::ExpireCurrent,
                    due_at,
                ));
            }
        }
        VersionKind::DeleteMarker => match expiration {
            CurrentExpiration::ExpiredObjectDeleteMarker if context.public_version_count == 1 => {
                proposals.push(proposal(
                    context,
                    rule_identity,
                    stable_rule_identity,
                    LifecycleActionKind::DeleteExpiredMarker,
                    context.candidate.lifecycle_age_started_at,
                ));
            }
            CurrentExpiration::Date { .. } | CurrentExpiration::Days { .. }
                if context.public_version_count == 1 =>
            {
                let Some(due_at) = current_due_at(expiration, context.candidate)? else {
                    return Ok(());
                };
                if context.database_now >= due_at {
                    proposals.push(proposal(
                        context,
                        rule_identity,
                        stable_rule_identity,
                        LifecycleActionKind::DeleteExpiredMarker,
                        due_at,
                    ));
                }
            }
            _ => {}
        },
    }
    Ok(())
}

fn collect_noncurrent_proposal(
    proposals: &mut Vec<LifecycleProposal>,
    context: &LifecycleEvaluationContext<'_>,
    expiration: Option<&NoncurrentExpiration>,
    rule_identity: RuleIdentity,
    stable_rule_identity: String,
) -> AppResult<()> {
    let Some(expiration) = expiration else {
        return Ok(());
    };
    let Some(became_noncurrent_at) = context.candidate.became_noncurrent_at else {
        return Ok(());
    };
    let due_at =
        next_utc_midnight_after_full_days(became_noncurrent_at, expiration.noncurrent_days)?;
    let newer_satisfied = expiration
        .newer_noncurrent_versions
        .is_none_or(|required| context.newer_noncurrent_count > u64::from(required));
    if context.database_now >= due_at && newer_satisfied {
        proposals.push(proposal(
            context,
            rule_identity,
            stable_rule_identity,
            LifecycleActionKind::ExpireNoncurrent,
            due_at,
        ));
    }
    Ok(())
}

fn current_due_at(
    expiration: &CurrentExpiration,
    candidate: &LifecycleCandidate,
) -> AppResult<Option<DateTime<Utc>>> {
    match expiration {
        CurrentExpiration::Date { utc_midnight } => Ok(Some(*utc_midnight)),
        CurrentExpiration::Days { days } => {
            next_utc_midnight_after_full_days(candidate.lifecycle_age_started_at, *days).map(Some)
        }
        CurrentExpiration::ExpiredObjectDeleteMarker => Ok(None),
    }
}

fn proposal(
    context: &LifecycleEvaluationContext<'_>,
    rule_identity: RuleIdentity,
    stable_rule_identity: String,
    action_kind: LifecycleActionKind,
    due_at: DateTime<Utc>,
) -> LifecycleProposal {
    LifecycleProposal {
        action: NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: context.candidate.target.bucket.clone(),
            config_revision: context.config_revision,
            rule_identity,
            action_kind,
            target: context.candidate.target.clone(),
            due_at,
        },
        stable_rule_identity,
    }
}

fn select_winner(mut proposals: Vec<LifecycleProposal>) -> Option<LifecycleProposal> {
    proposals.sort_by(|left, right| proposal_sort_key(left).cmp(&proposal_sort_key(right)));
    proposals.into_iter().next()
}

fn proposal_sort_key(proposal: &LifecycleProposal) -> (u8, DateTime<Utc>, &str) {
    let permanence = match proposal.action.action_kind {
        LifecycleActionKind::ExpireNoncurrent => 0,
        LifecycleActionKind::ExpireCurrent | LifecycleActionKind::DeleteExpiredMarker => 1,
    };
    (
        permanence,
        proposal.action.due_at,
        &proposal.stable_rule_identity,
    )
}

fn rule_identity(id: Option<&str>, ordinal: usize) -> AppResult<(RuleIdentity, String)> {
    match id {
        Some(id) => Ok((RuleIdentity::Id(id.to_owned()), format!("id:{id}"))),
        None => {
            let ordinal = u16::try_from(ordinal).map_err(|_| {
                AppError::InvalidArgument("lifecycle rule ordinal is out of range".to_owned())
            })?;
            Ok((
                RuleIdentity::Ordinal(ordinal),
                format!("ordinal:{ordinal:05}"),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use sea_orm::{ConnectionTrait, Database, EntityTrait, PaginatorTrait, Set};

    use super::{
        LifecycleEvaluationContext, evaluate_candidate, next_utc_midnight_after_full_days,
        schedule_claimed_scan_page,
    };
    use crate::{
        lifecycle::model::{
            CanonicalFilter, CanonicalLifecycleConfiguration, CanonicalLifecycleRule,
            CanonicalRuleSelector, CanonicalTag, ClaimedLifecycleScan, CurrentExpiration,
            LifecycleActionKind, LifecycleCandidate, LifecycleRuleStatus, NewLifecycleAction,
            NoncurrentExpiration, RuleIdentity, VersionTargetIdentity,
        },
        pinning::tags::ObjectTag,
        store::{
            bucket,
            entities::{lifecycle_action, object, object_tag, object_version},
            lifecycle_action::idempotency_key,
            object_version::{PublicVersionId, VersionKind},
            run_migrations,
        },
    };

    fn at(day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, day, hour, minute, 0)
            .single()
            .unwrap()
    }

    fn content_current(age: DateTime<Utc>) -> LifecycleCandidate {
        candidate(true, VersionKind::Object, age, None)
    }

    fn marker_current(age: DateTime<Utc>) -> LifecycleCandidate {
        candidate(true, VersionKind::DeleteMarker, age, None)
    }

    fn noncurrent(kind: VersionKind, became_noncurrent_at: DateTime<Utc>) -> LifecycleCandidate {
        candidate(false, kind, at(1, 0, 0), Some(became_noncurrent_at))
    }

    fn candidate(
        is_latest: bool,
        kind: VersionKind,
        lifecycle_age_started_at: DateTime<Utc>,
        became_noncurrent_at: Option<DateTime<Utc>>,
    ) -> LifecycleCandidate {
        let is_content = kind == VersionKind::Object;
        LifecycleCandidate {
            target: VersionTargetIdentity {
                bucket: "bucket".to_owned(),
                key: "logs/object".to_owned(),
                version_row_id: if is_content {
                    "content-row".to_owned()
                } else {
                    "marker-row".to_owned()
                },
                public_version_id: PublicVersionId::Opaque(
                    "00000000-0000-4000-8000-000000000001".to_owned(),
                ),
                kind,
                object_id: is_content.then(|| "object-id".to_owned()),
                sequence: 1,
            },
            is_latest,
            size: if is_content { 5 } else { 0 },
            lifecycle_age_started_at,
            became_noncurrent_at,
        }
    }

    fn rule(
        id: &str,
        selector: CanonicalRuleSelector,
        expiration: Option<CurrentExpiration>,
        noncurrent_version_expiration: Option<NoncurrentExpiration>,
    ) -> CanonicalLifecycleRule {
        CanonicalLifecycleRule {
            id: Some(id.to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector,
            expiration,
            noncurrent_version_expiration,
        }
    }

    fn all() -> CanonicalRuleSelector {
        CanonicalRuleSelector::Modern {
            filter: CanonicalFilter::All,
        }
    }

    fn configuration(rules: Vec<CanonicalLifecycleRule>) -> CanonicalLifecycleConfiguration {
        CanonicalLifecycleConfiguration {
            schema_version: 1,
            rules,
        }
    }

    fn context<'a>(
        candidate: &'a LifecycleCandidate,
        tags: &'a [ObjectTag],
        public_version_count: u64,
        newer_noncurrent_count: u64,
        rules: &'a CanonicalLifecycleConfiguration,
        database_now: DateTime<Utc>,
    ) -> LifecycleEvaluationContext<'a> {
        LifecycleEvaluationContext {
            candidate,
            tags,
            public_version_count,
            newer_noncurrent_count,
            config_revision: 7,
            configuration: rules,
            database_now,
        }
    }

    #[test]
    fn days_boundaries_are_the_first_utc_midnight_strictly_after_full_days() {
        let cases = [
            (at(1, 0, 0), 1, at(3, 0, 0), "aged at midnight"),
            (at(1, 23, 59), 1, at(3, 0, 0), "aged before midnight"),
            (at(1, 0, 1), 1, at(3, 0, 0), "aged after midnight"),
        ];

        for (start, days, expected, name) in cases {
            assert_eq!(
                next_utc_midnight_after_full_days(start, days).unwrap(),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn current_date_days_and_past_due_use_stable_due_boundaries() {
        let candidate = content_current(at(1, 0, 0));
        let cases = [
            (
                CurrentExpiration::Date {
                    utc_midnight: at(5, 0, 0),
                },
                at(4, 23, 59),
                None,
                "date before due",
            ),
            (
                CurrentExpiration::Date {
                    utc_midnight: at(5, 0, 0),
                },
                at(6, 0, 0),
                Some(at(5, 0, 0)),
                "past due date",
            ),
            (
                CurrentExpiration::Days { days: 1 },
                at(3, 0, 0),
                Some(at(3, 0, 0)),
                "days due at first following midnight",
            ),
        ];

        for (expiration, now, expected_due, name) in cases {
            let rules = configuration(vec![rule("current", all(), Some(expiration), None)]);
            let action = evaluate_candidate(&context(&candidate, &[], 1, 0, &rules, now)).unwrap();
            assert_eq!(
                action.as_ref().map(|action| action.due_at),
                expected_due,
                "{name}"
            );
            assert!(
                action
                    .is_none_or(|action| action.action_kind == LifecycleActionKind::ExpireCurrent)
            );
        }
    }

    #[test]
    fn disabled_rules_never_produce_actions() {
        let candidate = content_current(at(1, 0, 0));
        let mut disabled = rule(
            "disabled",
            all(),
            Some(CurrentExpiration::Date {
                utc_midnight: at(1, 0, 0),
            }),
            None,
        );
        disabled.status = LifecycleRuleStatus::Disabled;
        let rules = configuration(vec![disabled]);

        assert!(
            evaluate_candidate(&context(&candidate, &[], 1, 0, &rules, at(2, 0, 0)))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn current_filters_use_prefix_exact_tags_exclusive_size_and_and() {
        let candidate = content_current(at(1, 0, 0));
        let tags = [
            ObjectTag::new("class", "cold"),
            ObjectTag::new("region", "us"),
        ];
        let matching = CanonicalRuleSelector::Modern {
            filter: CanonicalFilter::And {
                prefix: Some("logs/".to_owned()),
                tags: vec![
                    CanonicalTag {
                        key: "class".to_owned(),
                        value: "cold".to_owned(),
                    },
                    CanonicalTag {
                        key: "region".to_owned(),
                        value: "us".to_owned(),
                    },
                ],
                object_size_greater_than: Some(4),
                object_size_less_than: Some(6),
            },
        };
        let mismatch = CanonicalRuleSelector::Modern {
            filter: CanonicalFilter::ObjectSizeGreaterThan { bytes: 5 },
        };
        let rules = configuration(vec![
            rule(
                "mismatch",
                mismatch,
                Some(CurrentExpiration::Date {
                    utc_midnight: at(1, 0, 0),
                }),
                None,
            ),
            rule(
                "match",
                matching,
                Some(CurrentExpiration::Date {
                    utc_midnight: at(1, 0, 0),
                }),
                None,
            ),
        ]);

        let action = evaluate_candidate(&context(&candidate, &tags, 1, 0, &rules, at(2, 0, 0)))
            .unwrap()
            .unwrap();
        assert_eq!(
            action.rule_identity,
            crate::lifecycle::model::RuleIdentity::Id("match".to_owned())
        );
    }

    #[test]
    fn markers_have_no_tags_and_zero_size_for_filter_matching() {
        let candidate = marker_current(at(1, 0, 0));
        let rules = configuration(vec![
            rule(
                "tagged",
                CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::Tag {
                        tag: CanonicalTag {
                            key: "class".to_owned(),
                            value: "cold".to_owned(),
                        },
                    },
                },
                Some(CurrentExpiration::Date {
                    utc_midnight: at(1, 0, 0),
                }),
                None,
            ),
            rule(
                "size-zero",
                CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::ObjectSizeLessThan { bytes: 1 },
                },
                Some(CurrentExpiration::Date {
                    utc_midnight: at(1, 0, 0),
                }),
                None,
            ),
        ]);

        let action = evaluate_candidate(&context(&candidate, &[], 1, 0, &rules, at(2, 0, 0)))
            .unwrap()
            .unwrap();
        assert_eq!(action.action_kind, LifecycleActionKind::DeleteExpiredMarker);
        assert_eq!(
            action.rule_identity,
            crate::lifecycle::model::RuleIdentity::Id("size-zero".to_owned())
        );
    }

    #[test]
    fn current_marker_timed_and_eodm_actions_require_a_sole_public_marker() {
        let marker = marker_current(at(1, 0, 0));
        let timed = configuration(vec![rule(
            "timed",
            all(),
            Some(CurrentExpiration::Days { days: 1 }),
            None,
        )]);
        assert!(
            evaluate_candidate(&context(&marker, &[], 2, 0, &timed, at(3, 0, 0)))
                .unwrap()
                .is_none()
        );
        let timed_action = evaluate_candidate(&context(&marker, &[], 1, 0, &timed, at(3, 0, 0)))
            .unwrap()
            .unwrap();
        assert_eq!(
            timed_action.action_kind,
            LifecycleActionKind::DeleteExpiredMarker
        );
        assert_eq!(timed_action.due_at, at(3, 0, 0));

        let dated = configuration(vec![rule(
            "dated",
            all(),
            Some(CurrentExpiration::Date {
                utc_midnight: at(2, 0, 0),
            }),
            None,
        )]);
        let dated_action = evaluate_candidate(&context(&marker, &[], 1, 0, &dated, at(2, 0, 0)))
            .unwrap()
            .unwrap();
        assert_eq!(
            dated_action.action_kind,
            LifecycleActionKind::DeleteExpiredMarker
        );
        assert_eq!(dated_action.due_at, at(2, 0, 0));

        let eodm = configuration(vec![rule(
            "eodm",
            all(),
            Some(CurrentExpiration::ExpiredObjectDeleteMarker),
            None,
        )]);
        let immediate = evaluate_candidate(&context(&marker, &[], 1, 0, &eodm, at(9, 0, 0)))
            .unwrap()
            .unwrap();
        assert_eq!(
            immediate.action_kind,
            LifecycleActionKind::DeleteExpiredMarker
        );
        assert_eq!(immediate.due_at, marker.lifecycle_age_started_at);
    }

    #[test]
    fn noncurrent_expiration_covers_content_markers_age_and_newer_thresholds() {
        let expiration = NoncurrentExpiration {
            noncurrent_days: 1,
            newer_noncurrent_versions: None,
        };
        let rules = configuration(vec![rule("nve", all(), None, Some(expiration))]);
        for kind in [VersionKind::Object, VersionKind::DeleteMarker] {
            let candidate = noncurrent(kind, at(1, 0, 0));
            let action = evaluate_candidate(&context(&candidate, &[], 2, 0, &rules, at(3, 0, 0)))
                .unwrap()
                .unwrap();
            assert_eq!(action.action_kind, LifecycleActionKind::ExpireNoncurrent);
            assert_eq!(action.due_at, at(3, 0, 0));
        }

        let before_age = noncurrent(VersionKind::Object, at(1, 0, 0));
        assert!(
            evaluate_candidate(&context(&before_age, &[], 2, 0, &rules, at(2, 23, 59)))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn noncurrent_newer_counts_require_more_than_the_configured_threshold() {
        let candidate = noncurrent(VersionKind::Object, at(1, 0, 0));
        let cases = [
            (None, 0, true, "threshold absent"),
            (Some(1), 1, false, "exactly one"),
            (Some(1), 2, true, "more than one"),
            (Some(100), 100, false, "exactly one hundred"),
            (Some(100), 101, true, "more than one hundred"),
        ];
        for (newer_noncurrent_versions, newer_count, expected, name) in cases {
            let rules = configuration(vec![rule(
                "nve",
                all(),
                None,
                Some(NoncurrentExpiration {
                    noncurrent_days: 1,
                    newer_noncurrent_versions,
                }),
            )]);
            assert_eq!(
                evaluate_candidate(&context(
                    &candidate,
                    &[],
                    2,
                    newer_count,
                    &rules,
                    at(3, 0, 0),
                ))
                .unwrap()
                .is_some(),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn winner_prefers_permanent_deletion_then_earliest_due_then_stable_rule_identity() {
        let target = content_current(at(1, 0, 0)).target;
        let permanent = super::LifecycleProposal {
            action: NewLifecycleAction {
                idempotency_key: String::new(),
                bucket: "bucket".to_owned(),
                config_revision: 7,
                rule_identity: RuleIdentity::Id("permanent".to_owned()),
                action_kind: LifecycleActionKind::ExpireNoncurrent,
                target: target.clone(),
                due_at: at(4, 0, 0),
            },
            stable_rule_identity: "id:permanent".to_owned(),
        };
        let current = super::LifecycleProposal {
            action: NewLifecycleAction {
                idempotency_key: String::new(),
                bucket: "bucket".to_owned(),
                config_revision: 7,
                rule_identity: RuleIdentity::Id("current".to_owned()),
                action_kind: LifecycleActionKind::ExpireCurrent,
                target,
                due_at: at(2, 0, 0),
            },
            stable_rule_identity: "id:current".to_owned(),
        };
        assert_eq!(
            super::select_winner(vec![current, permanent])
                .unwrap()
                .action
                .action_kind,
            LifecycleActionKind::ExpireNoncurrent,
            "permanent exact deletion wins before a sooner current expiration"
        );

        let noncurrent_candidate = noncurrent(VersionKind::Object, at(1, 0, 0));
        let rules = configuration(vec![
            rule(
                "z-later",
                all(),
                None,
                Some(NoncurrentExpiration {
                    noncurrent_days: 2,
                    newer_noncurrent_versions: None,
                }),
            ),
            rule(
                "a-earlier",
                all(),
                None,
                Some(NoncurrentExpiration {
                    noncurrent_days: 1,
                    newer_noncurrent_versions: None,
                }),
            ),
        ]);
        let action = evaluate_candidate(&context(
            &noncurrent_candidate,
            &[],
            2,
            0,
            &rules,
            at(4, 0, 0),
        ))
        .unwrap()
        .unwrap();
        assert_eq!(action.action_kind, LifecycleActionKind::ExpireNoncurrent);
        assert_eq!(action.due_at, at(3, 0, 0));
        assert_eq!(
            action.rule_identity,
            crate::lifecycle::model::RuleIdentity::Id("a-earlier".to_owned())
        );

        let current_candidate = content_current(at(1, 0, 0));
        let same_due_rules = configuration(vec![
            rule(
                "z-rule",
                all(),
                Some(CurrentExpiration::Date {
                    utc_midnight: at(2, 0, 0),
                }),
                None,
            ),
            rule(
                "a-rule",
                all(),
                Some(CurrentExpiration::Date {
                    utc_midnight: at(2, 0, 0),
                }),
                None,
            ),
        ]);
        let action = evaluate_candidate(&context(
            &current_candidate,
            &[],
            1,
            0,
            &same_due_rules,
            at(3, 0, 0),
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            action.rule_identity,
            crate::lifecycle::model::RuleIdentity::Id("a-rule".to_owned())
        );
    }

    #[tokio::test]
    async fn claimed_scan_page_reads_immutable_tags_and_replay_inserts_once() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        run_migrations(&db).await.unwrap();
        bucket::create(&db, "bucket", None).await.unwrap();

        let created_at = at(1, 0, 0);
        object::Entity::insert(object::ActiveModel {
            id: Set("object-id".to_owned()),
            bucket: Set("bucket".to_owned()),
            key: Set("logs/object".to_owned()),
            cid: Set("cid".to_owned()),
            size: Set(5),
            content_type: Set(None),
            etag: Set("cid".to_owned()),
            metadata: Set(None),
            encrypted: Set(false),
            key_wrap: Set(None),
            sse_c_key_fingerprint: Set(None),
            multipart: Set(false),
            is_latest: Set(true),
            created_at: Set(created_at),
        })
        .exec(&db)
        .await
        .unwrap();
        object_tag::Entity::insert(object_tag::ActiveModel {
            object_id: Set("object-id".to_owned()),
            key: Set("class".to_owned()),
            value: Set("cold".to_owned()),
        })
        .exec(&db)
        .await
        .unwrap();
        object_version::Entity::insert(object_version::ActiveModel {
            id: Set("version-row".to_owned()),
            bucket: Set("bucket".to_owned()),
            key: Set("logs/object".to_owned()),
            version_id: Set(Some("00000000-0000-4000-8000-000000000001".to_owned())),
            kind: Set("object".to_owned()),
            object_id: Set(Some("object-id".to_owned())),
            sequence: Set(1),
            is_latest: Set(true),
            lifecycle_age_started_at: Set(created_at),
            became_noncurrent_at: Set(None),
            created_at: Set(created_at),
            updated_at: Set(created_at),
        })
        .exec(&db)
        .await
        .unwrap();

        let rules = configuration(vec![rule(
            "tagged-current",
            CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::Tag {
                    tag: CanonicalTag {
                        key: "class".to_owned(),
                        value: "cold".to_owned(),
                    },
                },
            },
            Some(CurrentExpiration::Date {
                utc_midnight: at(1, 0, 0),
            }),
            None,
        )]);
        let claim = ClaimedLifecycleScan {
            bucket: "bucket".to_owned(),
            config_revision: 7,
            canonical_json: crate::lifecycle::config::canonical_json(&rules).unwrap(),
            cursor: None,
            lease_epoch: 1,
            database_now: at(2, 0, 0),
            lease_until: at(3, 0, 0),
        };

        let first = schedule_claimed_scan_page(&db, &claim, 10).await.unwrap();
        let second = schedule_claimed_scan_page(&db, &claim, 10).await.unwrap();
        assert_eq!(first.candidates.len(), 1);
        assert_eq!(second.candidates.len(), 1);
        assert_eq!(
            lifecycle_action::Entity::find().count(&db).await.unwrap(),
            1,
            "replaying the same claimed page must not create a second action"
        );
        let action = lifecycle_action::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(action.target_version_row_id, "version-row");
        assert_eq!(action.action_kind, "expire_current");
        assert_eq!(
            action.idempotency_key,
            idempotency_key(
                &evaluate_candidate(&context(
                    &first.candidates[0],
                    &[ObjectTag::new("class", "cold")],
                    1,
                    0,
                    &rules,
                    claim.database_now,
                ))
                .unwrap()
                .unwrap()
            )
            .unwrap()
        );
    }
}
