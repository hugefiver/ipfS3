use chrono::{DateTime, Days, TimeZone, Utc};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter};

use crate::{
    error::{AppError, AppResult},
    lifecycle::{
        config::from_canonical_json,
        filter::matches_filter,
        model::{
            CanonicalFilter, CanonicalLifecycleConfiguration, CanonicalRuleSelector,
            ClaimedLifecycleScan, CurrentExpiration, CurrentTransition, LifecycleActionKind,
            LifecycleCandidate, LifecycleCandidatePage, LifecycleRuleStatus,
            LifecycleTargetIdentity, MultipartUploadTargetIdentity, NewLifecycleAction,
            NoncurrentExpiration, RuleIdentity, VersionLifecycleCandidate,
        },
    },
    pinning::tags::ObjectTag,
    residency::model::StorageClass,
    store::{
        entities::object_version,
        lifecycle_action::{idempotency_key, insert_idempotent},
        lifecycle_scan::scan_candidate_page,
        object_version::{BucketVersioningState, PublicVersionId, VersionKind},
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
    effect_priority: u8,
}

struct VersionEvaluationContext<'a> {
    candidate: &'a VersionLifecycleCandidate,
    facts: &'a LifecycleEvaluationContext<'a>,
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

    let candidate = match context.candidate {
        LifecycleCandidate::Version(candidate) => candidate,
        LifecycleCandidate::MultipartUpload(target) => return evaluate_multipart(context, target),
    };
    let context = &VersionEvaluationContext {
        candidate,
        facts: context,
    };

    let mut proposals = Vec::new();
    for (ordinal, rule) in context.facts.configuration.rules.iter().enumerate() {
        if rule.status != LifecycleRuleStatus::Enabled
            || !matches_filter(
                &rule.selector,
                &context.candidate.target.key,
                context.candidate.size,
                context.facts.tags,
            )
        {
            continue;
        }

        let (rule_identity, stable_rule_identity) = rule_identity(rule.id.as_deref(), ordinal)?;
        collect_transition_proposal(
            &mut proposals,
            context,
            rule,
            rule_identity.clone(),
            stable_rule_identity.clone(),
        )?;
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

fn evaluate_multipart(
    context: &LifecycleEvaluationContext<'_>,
    target: &MultipartUploadTargetIdentity,
) -> AppResult<Option<NewLifecycleAction>> {
    let mut proposals = Vec::new();
    for (ordinal, rule) in context.configuration.rules.iter().enumerate() {
        if rule.status != LifecycleRuleStatus::Enabled {
            continue;
        }
        let Some(abort) = &rule.abort_incomplete_multipart_upload else {
            continue;
        };
        let matches = match &rule.selector {
            CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::All,
            } => true,
            CanonicalRuleSelector::LegacyPrefix { prefix }
            | CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::Prefix { prefix },
            } => target.key.starts_with(prefix),
            _ => {
                return Err(AppError::InvalidArgument(
                    "invalid multipart lifecycle selector".to_owned(),
                ));
            }
        };
        if !matches {
            continue;
        }
        // Canonical days are positive. A boundary beyond the timestamp range is therefore
        // still in the future, not a reason to discard other rules or fail the scan page.
        let Ok(due_at) =
            next_utc_midnight_after_full_days(target.initiated_at, abort.days_after_initiation)
        else {
            continue;
        };
        if context.database_now < due_at {
            continue;
        }
        let (rule_identity, stable_rule_identity) = rule_identity(rule.id.as_deref(), ordinal)?;
        proposals.push(LifecycleProposal {
            action: NewLifecycleAction {
                idempotency_key: String::new(),
                bucket: target.bucket.clone(),
                config_revision: context.config_revision,
                rule_identity,
                action_kind: LifecycleActionKind::AbortIncompleteMultipartUpload,
                target: LifecycleTargetIdentity::MultipartUpload(target.clone()),
                due_at,
            },
            stable_rule_identity,
            effect_priority: 0,
        });
    }
    proposals.sort_by(|left, right| {
        (left.action.due_at, &left.stable_rule_identity)
            .cmp(&(right.action.due_at, &right.stable_rule_identity))
    });
    Ok(proposals.into_iter().next().map(|proposal| proposal.action))
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
        let (tags, public_version_count, newer_noncurrent_count) = match candidate {
            LifecycleCandidate::MultipartUpload(_) => (Vec::new(), 0, 0),
            LifecycleCandidate::Version(candidate) => {
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
                (tags, public_version_count, newer_noncurrent_count)
            }
        };
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
            if matches!(
                action.action_kind,
                LifecycleActionKind::TransitionCurrent | LifecycleActionKind::TransitionNoncurrent
            ) && matches!(candidate, LifecycleCandidate::Version(version) if !version.hot_residency_verified)
            {
                // Do not let marker creation win while hot verification is pending.
                // The bounded scan cycles again under the same config revision.
                continue;
            }
            action.idempotency_key = idempotency_key(&action)?;
            insert_idempotent(db, action, claim.database_now).await?;
        }
    }

    Ok(page)
}

fn collect_current_proposal(
    proposals: &mut Vec<LifecycleProposal>,
    context: &VersionEvaluationContext<'_>,
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
            if context.facts.database_now >= due_at {
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
            CurrentExpiration::ExpiredObjectDeleteMarker
                if context.facts.public_version_count == 1 =>
            {
                proposals.push(proposal(
                    context,
                    rule_identity,
                    stable_rule_identity,
                    LifecycleActionKind::DeleteExpiredMarker,
                    context.candidate.lifecycle_age_started_at,
                ));
            }
            CurrentExpiration::Date { .. } | CurrentExpiration::Days { .. }
                if context.facts.public_version_count == 1 =>
            {
                let Some(due_at) = current_due_at(expiration, context.candidate)? else {
                    return Ok(());
                };
                if context.facts.database_now >= due_at {
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

fn collect_transition_proposal(
    proposals: &mut Vec<LifecycleProposal>,
    context: &VersionEvaluationContext<'_>,
    rule: &crate::lifecycle::model::CanonicalLifecycleRule,
    rule_identity: RuleIdentity,
    stable_rule_identity: String,
) -> AppResult<()> {
    let candidate = context.candidate;
    if candidate.target.kind != VersionKind::Object
        || candidate.primary_storage_class != Some(StorageClass::Standard)
    {
        return Ok(());
    }
    let (kind, due_at) = if candidate.is_latest {
        let Some(transition) = &rule.transition else {
            return Ok(());
        };
        let due = match transition {
            CurrentTransition::Date { utc_midnight } => *utc_midnight,
            CurrentTransition::Days { days } => {
                let Ok(due) =
                    next_utc_midnight_after_full_days(candidate.lifecycle_age_started_at, *days)
                else {
                    // Positive, validated days beyond chrono's range are still in
                    // the future, just as for multipart abort eligibility.
                    return Ok(());
                };
                due
            }
        };
        (LifecycleActionKind::TransitionCurrent, due)
    } else {
        let (Some(transition), Some(start)) = (
            &rule.noncurrent_version_transition,
            candidate.became_noncurrent_at,
        ) else {
            return Ok(());
        };
        if transition
            .newer_noncurrent_versions
            .is_some_and(|required| context.facts.newer_noncurrent_count <= u64::from(required))
        {
            return Ok(());
        }
        let Ok(due) = next_utc_midnight_after_full_days(start, transition.noncurrent_days) else {
            return Ok(());
        };
        (LifecycleActionKind::TransitionNoncurrent, due)
    };
    if context.facts.database_now >= due_at {
        proposals.push(proposal(
            context,
            rule_identity,
            stable_rule_identity,
            kind,
            due_at,
        ));
    }
    Ok(())
}

fn collect_noncurrent_proposal(
    proposals: &mut Vec<LifecycleProposal>,
    context: &VersionEvaluationContext<'_>,
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
        .is_none_or(|required| context.facts.newer_noncurrent_count > u64::from(required));
    if context.facts.database_now >= due_at && newer_satisfied {
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
    candidate: &VersionLifecycleCandidate,
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
    context: &VersionEvaluationContext<'_>,
    rule_identity: RuleIdentity,
    stable_rule_identity: String,
    action_kind: LifecycleActionKind,
    due_at: DateTime<Utc>,
) -> LifecycleProposal {
    LifecycleProposal {
        action: NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: context.candidate.target.bucket.clone(),
            config_revision: context.facts.config_revision,
            rule_identity,
            action_kind,
            target: LifecycleTargetIdentity::Version(context.candidate.target.clone()),
            due_at,
        },
        stable_rule_identity,
        effect_priority: match action_kind {
            LifecycleActionKind::ExpireCurrent => match context.candidate.bucket_versioning_state {
                BucketVersioningState::Unversioned => 0,
                BucketVersioningState::Suspended
                    if context.candidate.target.public_version_id == PublicVersionId::Null =>
                {
                    0
                }
                BucketVersioningState::Enabled | BucketVersioningState::Suspended => 2,
            },
            LifecycleActionKind::TransitionCurrent | LifecycleActionKind::TransitionNoncurrent => 1,
            LifecycleActionKind::ExpireNoncurrent
            | LifecycleActionKind::DeleteExpiredMarker
            | LifecycleActionKind::AbortIncompleteMultipartUpload => 0,
        },
    }
}

fn select_winner(mut proposals: Vec<LifecycleProposal>) -> Option<LifecycleProposal> {
    proposals.sort_by(|left, right| proposal_sort_key(left).cmp(&proposal_sort_key(right)));
    proposals.into_iter().next()
}

fn proposal_sort_key(proposal: &LifecycleProposal) -> (u8, DateTime<Utc>, &str) {
    (
        proposal.effect_priority,
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
    mod transition_tests;
    use crate::lifecycle::model::{
        AbortIncompleteMultipartUploadAction, LifecycleTargetIdentity,
        MultipartUploadTargetIdentity, VersionLifecycleCandidate,
    };
    use crate::residency::model::StorageClass;
    use crate::store::lifecycle_action::{canonical_action_bytes, insert_idempotent};
    use crate::store::object_version::BucketVersioningState;
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

    fn upload_candidate(key: &str, initiated_at: DateTime<Utc>) -> LifecycleCandidate {
        LifecycleCandidate::MultipartUpload(MultipartUploadTargetIdentity {
            bucket: "bucket".to_owned(),
            key: key.to_owned(),
            upload_id: "upload-1".to_owned(),
            initiated_at,
        })
    }

    fn abort_rule(id: &str, days: u32, selector: CanonicalRuleSelector) -> CanonicalLifecycleRule {
        let mut rule = rule(id, selector, None, None);
        rule.abort_incomplete_multipart_upload = Some(AbortIncompleteMultipartUploadAction {
            days_after_initiation: days,
        });
        rule
    }

    fn abort_action() -> NewLifecycleAction {
        let initiated_at = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
        let candidate = upload_candidate("key", initiated_at);
        let rules = configuration(vec![abort_rule("abort", 1, all())]);
        evaluate_candidate(&context(
            &candidate,
            &[],
            0,
            0,
            &rules,
            initiated_at + chrono::Duration::days(2),
        ))
        .unwrap()
        .unwrap()
    }

    #[test]
    fn abort_max_days_is_not_due_and_preserves_midnight_helper_errors() {
        let candidate = upload_candidate("key", at(1, 0, 0));
        let rules = configuration(vec![abort_rule("far-future", i32::MAX as u32, all())]);
        assert!(next_utc_midnight_after_full_days(at(1, 0, 0), i32::MAX as u32).is_err());
        assert!(next_utc_midnight_after_full_days(DateTime::<Utc>::MAX_UTC, 1).is_err());
        assert!(
            evaluate_candidate(&context(&candidate, &[], 0, 0, &rules, at(9, 0, 0)))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn abort_max_days_does_not_hide_a_due_rule_in_either_order() {
        let candidate = upload_candidate("key", at(1, 0, 0));
        let mut rules = configuration(vec![
            abort_rule("far-future", i32::MAX as u32, all()),
            abort_rule("due", 1, all()),
        ]);
        for _ in 0..2 {
            let action = evaluate_candidate(&context(&candidate, &[], 0, 0, &rules, at(9, 0, 0)))
                .unwrap()
                .unwrap();
            assert_eq!(action.rule_identity, RuleIdentity::Id("due".to_owned()));
            assert_eq!(action.due_at, at(3, 0, 0));
            rules.rules.reverse();
        }
    }

    #[test]
    fn abort_evaluator_uses_initiation_midnight_prefix_and_database_now() {
        for start in [at(1, 0, 0), at(1, 0, 1), at(1, 23, 59)] {
            let due = next_utc_midnight_after_full_days(start, 1).unwrap();
            for selector in [
                all(),
                CanonicalRuleSelector::LegacyPrefix {
                    prefix: "日志/".to_owned(),
                },
                CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::Prefix {
                        prefix: "日志/".to_owned(),
                    },
                },
            ] {
                let mut rules = configuration(vec![abort_rule("abort", 1, selector)]);
                let candidate = upload_candidate("日志/object", start);
                assert!(
                    evaluate_candidate(&context(
                        &candidate,
                        &[],
                        0,
                        0,
                        &rules,
                        due - chrono::Duration::nanoseconds(1)
                    ))
                    .unwrap()
                    .is_none()
                );
                let action = evaluate_candidate(&context(
                    &candidate,
                    &[ObjectTag::new("ignored", "ignored")],
                    u64::MAX,
                    u64::MAX,
                    &rules,
                    due,
                ))
                .unwrap()
                .unwrap();
                assert_eq!(action.due_at, due);
                assert_eq!(
                    action.action_kind,
                    LifecycleActionKind::AbortIncompleteMultipartUpload
                );
                assert!(matches!(
                    action.target,
                    LifecycleTargetIdentity::MultipartUpload(_)
                ));
                rules.rules[0].status = LifecycleRuleStatus::Disabled;
                assert!(
                    evaluate_candidate(&context(&candidate, &[], 0, 0, &rules, due))
                        .unwrap()
                        .is_none()
                );
            }
        }
        let rules = configuration(vec![abort_rule(
            "prefix",
            1,
            CanonicalRuleSelector::LegacyPrefix {
                prefix: "Logs/é/".to_owned(),
            },
        )]);
        for key in [
            "logs/é/file",
            "Logs/e\u{301}/file",
            "Logs/é",
            "Logs/%C3%A9/file",
        ] {
            assert!(
                evaluate_candidate(&context(
                    &upload_candidate(key, at(1, 0, 0)),
                    &[],
                    0,
                    0,
                    &rules,
                    at(9, 0, 0)
                ))
                .unwrap()
                .is_none(),
                "{key}"
            );
        }
    }

    #[test]
    fn abort_evaluator_chooses_earliest_due_then_stable_rule_identity() {
        let candidate = upload_candidate("key", at(1, 0, 0));
        let mut rules = configuration(vec![
            abort_rule("a-later", 2, all()),
            abort_rule("z-earlier", 1, all()),
            abort_rule("b-earlier", 1, all()),
        ]);
        for _ in 0..3 {
            let action = evaluate_candidate(&context(&candidate, &[], 0, 0, &rules, at(9, 0, 0)))
                .unwrap()
                .unwrap();
            assert_eq!(action.due_at, at(3, 0, 0));
            assert_eq!(
                action.rule_identity,
                RuleIdentity::Id("b-earlier".to_owned())
            );
            rules.rules.rotate_left(1);
        }
        for rule in &mut rules.rules {
            rule.id = None;
        }
        let action = evaluate_candidate(&context(&candidate, &[], 0, 0, &rules, at(9, 0, 0)))
            .unwrap()
            .unwrap();
        assert_eq!(action.rule_identity, RuleIdentity::Ordinal(1));
    }

    #[tokio::test]
    async fn abort_scan_replay_inserts_one_polymorphic_action() {
        use crate::store::entities::multipart_upload;
        let db = Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();
        bucket::create(&db, "bucket", None).await.unwrap();
        multipart_upload::Entity::insert(multipart_upload::ActiveModel {
            upload_id: Set("upload-1".to_owned()),
            object_id: Set("unpublished".to_owned()),
            bucket: Set("bucket".to_owned()),
            key: Set("key".to_owned()),
            created_at: Set(at(1, 0, 0)),
            encryption_mode: Set("none".to_owned()),
            key_wrap: Set(None),
            sse_c_key_fingerprint: Set(None),
            content_type: Set(None),
            metadata: Set(None),
            tags_json: Set(serde_json::json!([])),
            decompress_zip_target: Set(None),
            decompress_zip_result: Set(false),
        })
        .exec(&db)
        .await
        .unwrap();
        let rules = configuration(vec![abort_rule("z", 2, all()), abort_rule("a", 1, all())]);
        let claim = ClaimedLifecycleScan {
            bucket: "bucket".to_owned(),
            config_revision: 7,
            canonical_json: crate::lifecycle::config::canonical_json(&rules).unwrap(),
            cursor: None,
            lease_epoch: 1,
            database_now: at(9, 0, 0),
            lease_until: at(10, 0, 0),
        };
        for _ in 0..2 {
            let page = schedule_claimed_scan_page(&db, &claim, 1).await.unwrap();
            assert_eq!(page.candidates.len(), 1);
            assert!(page.cycle_complete);
        }
        assert_eq!(
            lifecycle_action::Entity::find().count(&db).await.unwrap(),
            1
        );
        let stored = lifecycle_action::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.target_type, "multipart_upload");
        assert_eq!(stored.target_upload_id.as_deref(), Some("upload-1"));
        assert_eq!(stored.target_upload_created_at, Some(at(1, 0, 0)));
        assert!(stored.target_version_row_id.is_none());
        assert!(stored.target_public_version_id.is_none());
        assert!(stored.target_object_id.is_none());
        assert!(stored.target_sequence.is_none());
        assert_eq!(stored.rule_id, "id:a");
        let proposed = evaluate_candidate(&context(
            &upload_candidate("key", at(1, 0, 0)),
            &[],
            0,
            0,
            &rules,
            claim.database_now,
        ))
        .unwrap()
        .unwrap();
        assert!(
            !insert_idempotent(&db, proposed, claim.database_now)
                .await
                .unwrap()
        );
    }

    #[test]
    fn multipart_action_key_is_canonical_and_version_key_is_unchanged() {
        let action = abort_action();
        assert_eq!(
            String::from_utf8(canonical_action_bytes(&action).unwrap()).unwrap(),
            r#"{"bucket":"bucket","config_revision":7,"rule_identity":"id:abort","action_kind":"abort_incomplete_multipart_upload","target_upload_id":"upload-1","target_upload_created_at":"2026-09-01T00:00:00.000000000Z","due_at":"2026-09-03T00:00:00.000000000Z"}"#
        );
        assert_eq!(
            idempotency_key(&action).unwrap(),
            "b0cc004f498978dab985d957699c30782cc76a2f6a0e2d4fe769fa0a543cd8c5"
        );
        let version = NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: "bucket".to_owned(),
            config_revision: 7,
            rule_identity: RuleIdentity::Id("expire".to_owned()),
            action_kind: LifecycleActionKind::ExpireCurrent,
            target: LifecycleTargetIdentity::Version(VersionTargetIdentity {
                bucket: "bucket".to_owned(),
                key: "key".to_owned(),
                version_row_id: "version-row".to_owned(),
                public_version_id: PublicVersionId::Opaque(
                    "00000000-0000-4000-8000-000000000001".to_owned(),
                ),
                kind: VersionKind::Object,
                object_id: Some("object-id".to_owned()),
                sequence: 1,
            }),
            due_at: Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap(),
        };
        assert_eq!(
            idempotency_key(&version).unwrap(),
            "d9c49eddf8319d6726c99c8162ff9b72150f6d57f23ad121be9a263647f55578"
        );
    }

    #[tokio::test]
    async fn polymorphic_action_insert_rejects_shape_kind_or_supplied_key_mismatch() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();
        bucket::create(&db, "bucket", None).await.unwrap();
        for case in [
            "kind",
            "bucket",
            "key",
            "upload",
            "revision",
            "supplied_key",
            "version_abort",
        ] {
            let mut action = abort_action();
            match case {
                "kind" => action.action_kind = LifecycleActionKind::ExpireCurrent,
                "bucket" => action.bucket = "other".to_owned(),
                "revision" => action.config_revision = 0,
                "supplied_key" => action.idempotency_key = "wrong".to_owned(),
                "version_abort" => {
                    let LifecycleCandidate::Version(candidate) = content_current(at(1, 0, 0))
                    else {
                        unreachable!()
                    };
                    action.target = LifecycleTargetIdentity::Version(candidate.target)
                }
                "key" | "upload" => {
                    let LifecycleTargetIdentity::MultipartUpload(target) = &mut action.target
                    else {
                        unreachable!()
                    };
                    if case == "key" {
                        target.key.clear();
                    } else {
                        target.upload_id.clear();
                    }
                }
                _ => unreachable!(),
            }
            assert!(
                matches!(
                    insert_idempotent(&db, action, at(9, 0, 0)).await,
                    Err(crate::error::AppError::InvalidArgument(_))
                ),
                "{case}"
            );
        }
        assert_eq!(
            lifecycle_action::Entity::find().count(&db).await.unwrap(),
            0
        );
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
        LifecycleCandidate::Version(VersionLifecycleCandidate {
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
            bucket_versioning_state: BucketVersioningState::Enabled,
            primary_storage_class: is_content.then_some(StorageClass::Standard),
            hot_residency_verified: is_content,
            size: if is_content { 5 } else { 0 },
            lifecycle_age_started_at,
            became_noncurrent_at,
        })
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
            abort_incomplete_multipart_upload: None,
            transition: None,
            noncurrent_version_transition: None,
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
        let LifecycleCandidate::Version(marker) = marker else {
            unreachable!()
        };
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
        let LifecycleCandidate::Version(candidate) = content_current(at(1, 0, 0)) else {
            unreachable!()
        };
        let target = LifecycleTargetIdentity::Version(candidate.target);
        let permanent = super::LifecycleProposal {
            effect_priority: 0,
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
            effect_priority: 2,
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
        assert_eq!(action.target_version_row_id.as_deref(), Some("version-row"));
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
