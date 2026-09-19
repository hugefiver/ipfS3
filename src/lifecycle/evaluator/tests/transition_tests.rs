use super::*;
use crate::lifecycle::model::{CurrentTransition, NoncurrentTransition};
use crate::residency::model::StorageClass;
use crate::store::object_version::BucketVersioningState;

fn transition_rule() -> CanonicalLifecycleRule {
    let mut value = rule("transition", all(), None, None);
    value.transition = Some(CurrentTransition::Days { days: 1 });
    value.noncurrent_version_transition = Some(NoncurrentTransition {
        noncurrent_days: 1,
        newer_noncurrent_versions: None,
    });
    value
}

#[test]
fn transition_boundaries_and_noncurrent_count_are_strict() {
    for latest in [true, false] {
        let target = candidate(latest, VersionKind::Object, at(1, 0, 0), Some(at(1, 0, 0)));
        let mut rules = configuration(vec![transition_rule()]);
        for (now, due) in [
            (at(2, 23, 59), false),
            (at(3, 0, 0), true),
            (at(3, 0, 1), true),
        ] {
            let action = evaluate_candidate(&context(&target, &[], 5, 4, &rules, now)).unwrap();
            assert_eq!(action.is_some(), due);
            if let Some(action) = action {
                assert_eq!(
                    action.action_kind,
                    if latest {
                        LifecycleActionKind::TransitionCurrent
                    } else {
                        LifecycleActionKind::TransitionNoncurrent
                    }
                );
            }
        }
        if !latest {
            rules.rules[0]
                .noncurrent_version_transition
                .as_mut()
                .unwrap()
                .newer_noncurrent_versions = Some(2);
            assert!(
                evaluate_candidate(&context(&target, &[], 5, 2, &rules, at(3, 0, 0)))
                    .unwrap()
                    .is_none()
            );
            assert!(
                evaluate_candidate(&context(&target, &[], 5, 3, &rules, at(3, 0, 0)))
                    .unwrap()
                    .is_some()
            );
        }
    }
    let rules = configuration(vec![CanonicalLifecycleRule {
        transition: Some(CurrentTransition::Date {
            utc_midnight: at(3, 0, 0),
        }),
        ..transition_rule()
    }]);
    let target = content_current(at(1, 0, 0));
    assert!(
        evaluate_candidate(&context(&target, &[], 1, 0, &rules, at(2, 23, 59)))
            .unwrap()
            .is_none()
    );
    assert!(
        evaluate_candidate(&context(&target, &[], 1, 0, &rules, at(3, 0, 0)))
            .unwrap()
            .is_some()
    );
}

#[test]
fn actual_delete_effect_beats_transition_which_beats_marker_creation() {
    for state in [
        BucketVersioningState::Unversioned,
        BucketVersioningState::Enabled,
        BucketVersioningState::Suspended,
    ] {
        for null in [false, true] {
            for latest in [false, true] {
                let mut target =
                    candidate(latest, VersionKind::Object, at(1, 0, 0), Some(at(1, 0, 0)));
                let LifecycleCandidate::Version(version) = &mut target else {
                    unreachable!()
                };
                version.bucket_versioning_state = state;
                if null {
                    version.target.public_version_id = PublicVersionId::Null;
                }
                let expiration = rule(
                    "expiration",
                    all(),
                    Some(CurrentExpiration::Days { days: 1 }),
                    Some(NoncurrentExpiration {
                        noncurrent_days: 1,
                        newer_noncurrent_versions: None,
                    }),
                );
                let rules = configuration(vec![expiration, transition_rule()]);
                let expected = if !latest {
                    LifecycleActionKind::ExpireNoncurrent
                } else if state == BucketVersioningState::Unversioned
                    || (state == BucketVersioningState::Suspended && null)
                {
                    LifecycleActionKind::ExpireCurrent
                } else {
                    LifecycleActionKind::TransitionCurrent
                };
                assert_eq!(
                    evaluate_candidate(&context(&target, &[], 2, 1, &rules, at(3, 0, 0)))
                        .unwrap()
                        .unwrap()
                        .action_kind,
                    expected
                );
                let LifecycleCandidate::Version(version) = &mut target else {
                    unreachable!()
                };
                version.primary_storage_class = Some(StorageClass::StandardIa);
                assert_eq!(
                    evaluate_candidate(&context(&target, &[], 2, 1, &rules, at(3, 0, 0)))
                        .unwrap()
                        .unwrap()
                        .action_kind,
                    if latest {
                        LifecycleActionKind::ExpireCurrent
                    } else {
                        LifecycleActionKind::ExpireNoncurrent
                    }
                );
            }
        }
    }
}

#[test]
fn markers_never_transition_and_pending_hot_still_wins_conflicts() {
    let rules = configuration(vec![transition_rule()]);
    for target in [
        marker_current(at(1, 0, 0)),
        noncurrent(VersionKind::DeleteMarker, at(1, 0, 0)),
    ] {
        assert!(
            evaluate_candidate(&context(&target, &[], 1, 3, &rules, at(3, 0, 0)))
                .unwrap()
                .is_none()
        );
    }
    let mut target = content_current(at(1, 0, 0));
    let LifecycleCandidate::Version(version) = &mut target else {
        unreachable!()
    };
    version.hot_residency_verified = false;
    version.bucket_versioning_state = BucketVersioningState::Enabled;
    let rules = configuration(vec![
        transition_rule(),
        rule(
            "expire",
            all(),
            Some(CurrentExpiration::Days { days: 1 }),
            None,
        ),
    ]);
    assert_eq!(
        evaluate_candidate(&context(&target, &[], 1, 0, &rules, at(3, 0, 0)))
            .unwrap()
            .unwrap()
            .action_kind,
        LifecycleActionKind::TransitionCurrent
    );
}

#[test]
fn transition_filters_disabled_and_stable_ties_share_the_existing_evaluator() {
    let target = content_current(at(1, 0, 0));
    let mut value = transition_rule();
    value.selector = CanonicalRuleSelector::Modern {
        filter: CanonicalFilter::And {
            prefix: Some("logs/".into()),
            tags: vec![CanonicalTag {
                key: "class".into(),
                value: "cold".into(),
            }],
            object_size_greater_than: Some(4),
            object_size_less_than: Some(6),
        },
    };
    let mut rules = configuration(vec![value]);
    let tags = [ObjectTag::new("class", "cold")];
    assert!(
        evaluate_candidate(&context(&target, &tags, 1, 0, &rules, at(3, 0, 0)))
            .unwrap()
            .is_some()
    );
    assert!(
        evaluate_candidate(&context(&target, &[], 1, 0, &rules, at(3, 0, 0)))
            .unwrap()
            .is_none()
    );
    for size in [4, 6] {
        let mut target = target.clone();
        let LifecycleCandidate::Version(version) = &mut target else {
            unreachable!()
        };
        version.size = size;
        assert!(
            evaluate_candidate(&context(&target, &tags, 1, 0, &rules, at(3, 0, 0)))
                .unwrap()
                .is_none()
        );
    }
    rules.rules[0].status = LifecycleRuleStatus::Disabled;
    assert!(
        evaluate_candidate(&context(&target, &tags, 1, 0, &rules, at(3, 0, 0)))
            .unwrap()
            .is_none()
    );
    rules = configuration(vec![
        transition_rule(),
        CanonicalLifecycleRule {
            id: Some("a".into()),
            ..transition_rule()
        },
    ]);
    for _ in 0..2 {
        assert_eq!(
            evaluate_candidate(&context(&target, &[], 1, 0, &rules, at(3, 0, 0)))
                .unwrap()
                .unwrap()
                .rule_identity,
            RuleIdentity::Id("a".into())
        );
        rules.rules.reverse();
    }
}

#[test]
fn far_future_transition_does_not_block_due_actions() {
    let mut value = transition_rule();
    value.transition = Some(CurrentTransition::Days {
        days: i32::MAX as u32,
    });
    value
        .noncurrent_version_transition
        .as_mut()
        .unwrap()
        .noncurrent_days = i32::MAX as u32;
    let rules = configuration(vec![
        value,
        rule(
            "expire",
            all(),
            Some(CurrentExpiration::Days { days: 1 }),
            Some(NoncurrentExpiration {
                noncurrent_days: 1,
                newer_noncurrent_versions: None,
            }),
        ),
    ]);
    for target in [
        content_current(at(1, 0, 0)),
        noncurrent(VersionKind::Object, at(1, 0, 0)),
    ] {
        assert!(
            evaluate_candidate(&context(&target, &[], 2, 1, &rules, at(9, 0, 0)))
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test]
async fn pending_hot_is_rediscovered_without_revision_change_and_actions_deduplicate() {
    use crate::residency::model::{PhysicalVerification, VersionResidencyIdentity};
    use sea_orm::{ActiveModelTrait, TransactionTrait};
    let db = Database::connect("sqlite::memory:").await.unwrap();
    run_migrations(&db).await.unwrap();
    bucket::create(&db, "bucket", None).await.unwrap();
    bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
        .await
        .unwrap();
    for (id, latest, sequence) in [("old", false, 1), ("current", true, 2)] {
        let now = at(1, 0, 0);
        object::ActiveModel {
            id: Set(id.into()),
            bucket: Set("bucket".into()),
            key: Set("key".into()),
            cid: Set(format!("cid-{id}")),
            etag: Set(format!("cid-{id}")),
            size: Set(5),
            encrypted: Set(false),
            multipart: Set(false),
            is_latest: Set(latest),
            created_at: Set(now),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        object_version::ActiveModel {
            id: Set(id.into()),
            bucket: Set("bucket".into()),
            key: Set("key".into()),
            version_id: Set(Some(uuid::Uuid::new_v4().to_string())),
            kind: Set("object".into()),
            object_id: Set(Some(id.into())),
            sequence: Set(sequence),
            is_latest: Set(latest),
            lifecycle_age_started_at: Set(now),
            became_noncurrent_at: Set((!latest).then_some(now)),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&db)
        .await
        .unwrap();
    }
    let rules = configuration(vec![
        transition_rule(),
        rule(
            "marker",
            all(),
            Some(CurrentExpiration::Days { days: 1 }),
            None,
        ),
    ]);
    let claim = ClaimedLifecycleScan {
        bucket: "bucket".into(),
        config_revision: 7,
        canonical_json: crate::lifecycle::config::canonical_json(&rules).unwrap(),
        cursor: None,
        lease_epoch: 1,
        database_now: at(9, 0, 0),
        lease_until: at(10, 0, 0),
    };
    let page = schedule_claimed_scan_page(&db, &claim, 10).await.unwrap();
    assert_eq!(page.candidates.len(), 2);
    assert_eq!(
        lifecycle_action::Entity::find().count(&db).await.unwrap(),
        0
    );
    for id in ["old", "current"] {
        let txn = db.begin().await.unwrap();
        crate::store::residency::attach_hot_in_transaction(
            &txn,
            &VersionResidencyIdentity::new(id, id, format!("cid-{id}")),
            &PhysicalVerification::verified("hot-node", "test-receipt"),
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }
    // Two concurrent/repeated scans share the same canonical identity. Verification
    // completed after the ordinary retry window; no action attempt was consumed.
    let (left, right) = tokio::join!(
        schedule_claimed_scan_page(&db, &claim, 10),
        schedule_claimed_scan_page(&db, &claim, 10)
    );
    assert!(left.is_ok() && right.is_ok());
    let actions = lifecycle_action::Entity::find().all(&db).await.unwrap();
    assert_eq!(actions.len(), 2);
    assert!(
        actions
            .iter()
            .all(|action| action.config_revision == 7 && action.attempts == 0)
    );
    assert!(
        actions
            .iter()
            .any(|action| action.action_kind == "transition_current")
    );
    assert!(
        actions
            .iter()
            .any(|action| action.action_kind == "transition_noncurrent")
    );
}
