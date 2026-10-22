use super::*;
use crate::pinning::management::wire;
use crate::store::pinning::management::{
    ManagementError,
    records::locator::{AuthorityMembership, DependencyKind, SourceClassification},
};

#[tokio::test]
async fn loaded_candidates_follow_only_the_requested_membership_closure_and_remaining_work() {
    let fixture = crate::stage7_management_test_support::Fixture::new().await;
    let work = records::SharedDecodeBudget::default();
    let context = records::DecodeWork::shared(&work);
    let job_selector = AuthoritySelector::LegacyJob("target".into(), "job".into());
    let resource_selector = AuthoritySelector::LegacyResource(
        "remote_pin_ledger".into(),
        "target".into(),
        Some("resource".into()),
    );
    let mut view = admission::authority::load(
        &fixture.db,
        &[job_selector.clone(), resource_selector.clone()],
        context,
    )
    .await
    .unwrap();
    assert!(view.originals.is_empty());
    let job_op = native::NativeKey {
        table: "pin_legacy_migrations".into(),
        values: vec!["job-op".into()],
    };
    let resource_op = native::NativeKey {
        table: "pin_legacy_migrations".into(),
        values: vec!["resource-op".into()],
    };
    let job_item = native::NativeKey {
        table: "pin_legacy_migration_items".into(),
        values: vec!["job-op".into(), "0".into()],
    };
    for membership in &mut view.proof.memberships {
        membership.sources = if membership.selector == job_selector {
            vec![job_item.clone()]
        } else {
            vec![resource_op.clone()]
        };
    }
    let operation_selector = AuthoritySelector::LegacyOperation("job-op".into());
    view.proof.memberships.push(AuthorityMembership {
        selector: operation_selector.clone(),
        sources: vec![job_op.clone(), job_item.clone()],
    });
    for (key, column, id) in [
        (&job_op, "id", "job-op"),
        (&job_item, "operation_id", "job-op"),
        (&resource_op, "id", "resource-op"),
    ] {
        view.originals
            .insert(key.clone(), [(column.into(), Some(id.into()))].into());
    }
    let mut job_class = SourceClassification::default();
    job_class
        .dependencies
        .insert(job_op.clone(), DependencyKind::LegacyOperation);
    job_class
        .membership_lookups
        .insert(operation_selector.clone());
    view.classifications.insert(job_item.clone(), job_class);
    let mut operation_class = SourceClassification::default();
    operation_class.selectors.insert(resource_selector);
    operation_class
        .membership_lookups
        .insert(operation_selector.clone());
    view.classifications.insert(job_op.clone(), operation_class);
    view.classifications
        .insert(resource_op, SourceClassification::default());
    assert_eq!(
        operation_ids(&view, &job_selector, &context).unwrap(),
        BTreeSet::from(["job-op".into()])
    );

    let original = view.originals.remove(&job_op).unwrap();
    assert!(operation_ids(&view, &job_selector, &context).is_err());
    view.originals.insert(job_op.clone(), original);
    let classification = view.classifications.remove(&job_op).unwrap();
    assert!(operation_ids(&view, &job_selector, &context).is_err());
    view.classifications.insert(job_op, classification);
    for selector in [&operation_selector, &job_selector] {
        let index = view
            .proof
            .memberships
            .iter()
            .position(|member| &member.selector == selector)
            .unwrap();
        let membership = view.proof.memberships.remove(index);
        assert!(operation_ids(&view, &job_selector, &context).is_err());
        view.proof.memberships.insert(index, membership);
    }
    let used = work.lock().unwrap().usage().0;
    work.lock()
        .unwrap()
        .consume(wire::MAX_WIRE_NODES - used, 0)
        .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            operation_ids(&view, &job_selector, &context),
            Err(ManagementError::Budget)
        ));
        assert!(matches!(
            active_for_loaded(&fixture.db, &job_selector, &view, &context, context).await,
            Err(ManagementError::Budget)
        ));
    }
}

#[tokio::test]
async fn operation_projection_is_root_local_and_follows_reversal_dependencies() {
    let fixture = crate::stage7_management_test_support::Fixture::new().await;
    let work = records::SharedDecodeBudget::default();
    let context = records::DecodeWork::shared(&work);
    let apply = AuthoritySelector::LegacyOperation("apply".into());
    let rollback = AuthoritySelector::LegacyOperation("rollback".into());
    let other = AuthoritySelector::LegacyOperation("other".into());
    let mut view = admission::authority::load(
        &fixture.db,
        &[apply.clone(), rollback.clone(), other.clone()],
        context,
    )
    .await
    .unwrap();
    let operation = |id: &str| native::NativeKey {
        table: "pin_legacy_migrations".into(),
        values: vec![id.into()],
    };
    let item = |id: &str| native::NativeKey {
        table: "pin_legacy_migration_items".into(),
        values: vec![id.into(), "0".into()],
    };
    let apply_key = operation("apply");
    let rollback_key = operation("rollback");
    let other_key = operation("other");
    let apply_item = item("apply");
    let rollback_item = item("rollback");
    view.proof.memberships = vec![
        AuthorityMembership {
            selector: apply.clone(),
            sources: vec![apply_key.clone(), apply_item.clone(), rollback_key.clone()],
        },
        AuthorityMembership {
            selector: rollback.clone(),
            sources: vec![rollback_key.clone(), rollback_item.clone()],
        },
        AuthorityMembership {
            selector: other.clone(),
            sources: vec![other_key.clone()],
        },
    ];
    for (key, column, id) in [
        (&apply_key, "id", "apply"),
        (&rollback_key, "id", "rollback"),
        (&other_key, "id", "other"),
        (&apply_item, "operation_id", "apply"),
        (&rollback_item, "operation_id", "rollback"),
    ] {
        view.originals
            .insert(key.clone(), [(column.into(), Some(id.into()))].into());
        view.classifications
            .insert(key.clone(), SourceClassification::default());
    }
    let apply_class = view.classifications.get_mut(&apply_key).unwrap();
    apply_class.selectors.insert(other.clone());
    apply_class.membership_lookups.insert(apply.clone());
    let rollback_class = view.classifications.get_mut(&rollback_key).unwrap();
    rollback_class
        .dependencies
        .insert(apply_key.clone(), DependencyKind::Reversal);
    rollback_class.membership_lookups.insert(rollback.clone());
    for (key, parent, lookup) in [
        (&apply_item, &apply_key, &apply),
        (&rollback_item, &rollback_key, &rollback),
    ] {
        let class = view.classifications.get_mut(key).unwrap();
        class
            .dependencies
            .insert(parent.clone(), DependencyKind::LegacyOperation);
        class.membership_lookups.insert(lookup.clone());
    }
    let related = BTreeSet::from([&apply_key, &apply_item, &rollback_key, &rollback_item]);
    assert_eq!(source_closure(&view, &apply, &context).unwrap(), related);
    assert_eq!(source_closure(&view, &rollback, &context).unwrap(), related);
    assert_eq!(
        source_closure(&view, &other, &context).unwrap(),
        BTreeSet::from([&other_key])
    );

    for key in [&apply_key, &rollback_key] {
        let original = view.originals.remove(key).unwrap();
        assert!(source_closure(&view, &apply, &context).is_err());
        assert!(source_closure(&view, &other, &context).is_ok());
        view.originals.insert(key.clone(), original);
    }
    let class = view.classifications.remove(&rollback_key).unwrap();
    assert!(source_closure(&view, &apply, &context).is_err());
    view.classifications.insert(rollback_key, class);
    let membership = view.proof.memberships.remove(1);
    assert!(source_closure(&view, &apply, &context).is_err());
    assert!(source_closure(&view, &other, &context).is_ok());
    view.proof.memberships.insert(1, membership);
    view.proof.memberships.remove(0);
    assert!(source_closure(&view, &apply, &context).is_err());
}

#[tokio::test]
async fn candidate_operation_roots_are_empty_or_bounded_by_distinct_identity() {
    let fixture = crate::stage7_management_test_support::Fixture::new().await;
    let work = records::SharedDecodeBudget::default();
    let context = records::DecodeWork::shared(&work);
    let selector =
        AuthoritySelector::LegacyResource("pin_submit_observations".into(), "target".into(), None);
    let other =
        AuthoritySelector::LegacyResource("pin_resource_history".into(), "target".into(), None);
    let mut view =
        admission::authority::load(&fixture.db, &[selector.clone(), other.clone()], context)
            .await
            .unwrap();
    assert!(
        active_for_loaded(&fixture.db, &selector, &view, &context, context)
            .await
            .unwrap()
            .is_empty()
    );
    let root = view
        .proof
        .memberships
        .iter()
        .position(|member| member.selector == selector)
        .unwrap();
    let membership = view.proof.memberships.remove(root);
    assert!(matches!(
        active_for_loaded(&fixture.db, &selector, &view, &context, context).await,
        Err(ManagementError::Changed)
    ));
    view.proof.memberships.insert(root, membership);
    for requested in [&selector, &other] {
        let membership = view
            .proof
            .memberships
            .iter_mut()
            .find(|member| &member.selector == requested)
            .unwrap();
        for index in 0..128 {
            let prefix = if requested == &selector {
                "submit"
            } else {
                "history"
            };
            let id = format!("{prefix}-{index:03}");
            let key = native::NativeKey {
                table: "pin_legacy_migrations".into(),
                values: vec![id.clone()],
            };
            view.originals
                .insert(key.clone(), [("id".into(), Some(id))].into());
            view.classifications
                .insert(key.clone(), SourceClassification::default());
            membership.sources.push(key);
        }
    }
    let duplicate = view.proof.memberships[root].sources[0].clone();
    view.proof.memberships[root].sources.push(duplicate);
    for requested in [&selector, &other] {
        assert_eq!(
            operation_ids(&view, requested, &context).unwrap().len(),
            128
        );
    }
    let key = native::NativeKey {
        table: "pin_legacy_migrations".into(),
        values: vec!["overflow".into()],
    };
    view.originals
        .insert(key.clone(), [("id".into(), Some("overflow".into()))].into());
    view.classifications
        .insert(key.clone(), SourceClassification::default());
    view.proof.memberships[root].sources.push(key);
    assert!(matches!(
        operation_ids(&view, &selector, &context),
        Err(ManagementError::Changed)
    ));
    assert!(matches!(
        active_for_loaded(&fixture.db, &selector, &view, &context, context).await,
        Err(ManagementError::Changed)
    ));
    assert_eq!(operation_ids(&view, &other, &context).unwrap().len(), 128);
}
