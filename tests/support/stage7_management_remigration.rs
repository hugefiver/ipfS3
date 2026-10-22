use super::{archive, provider_route, selected, support};
use ipfs_s3_gateway::{
    pinning::management::*,
    store::{
        entities::pin_lease_target,
        pinning::management::{actions, records},
    },
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

fn object_capture(plan: &DryRunPlan) -> &ObjectCapture {
    let Some(Capture::Object(capture)) = &plan.items()[0].capture else {
        panic!("object capture unavailable: {:?}", plan.items()[0].status);
    };
    capture
}

pub async fn two_slot_planning(one: bool) {
    let mut fixture = support::Fixture::new().await;
    fixture.add_provider("second").await;
    if one {
        fixture.raw.pinning.policies[0].provider_mode = "one".into();
        fixture.config = local_config::compile(&fixture.raw, |_| None)
            .unwrap()
            .with_source_node(support::NODE.into())
            .unwrap();
    }
    let identity = fixture.publish("two-slots", false).await;
    let birth = fixture
        .confirm(&fixture.create_request("birth", identity.clone(), false))
        .await;
    let created = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &birth)
        .await
        .unwrap();
    let lease = &created.items[0].receipt.intent_ids[0];
    fixture.settle_with_worker(lease).await;
    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(lease))
        .all(&fixture.db)
        .await
        .unwrap();
    let source = targets
        .iter()
        .find(|target| target.state == "pinned")
        .unwrap();
    let other = targets.iter().find(|target| target.id != source.id);
    fixture.add_provider("destination").await;
    let destination = provider_route(&fixture, "destination-provider");
    let first_request = selected(
        "first-migration",
        identity.clone(),
        source,
        Action::Migrate {
            destination: Box::new(destination.clone()),
        },
    );
    let first_binding = fixture.confirm(&first_request).await;
    let Some(Capture::Object(before)) = &first_binding.confirmation.bindings[0].capture else {
        unreachable!()
    };
    let before = before.target.as_ref().unwrap();
    let first = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &first_binding)
        .await
        .unwrap();
    let point = first.items[0]
        .migration_points
        .first()
        .expect("settled native source must migrate");
    let moved = pin_lease_target::Entity::find_by_id(point.destination_target_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.add_provider("third").await;
    let next = selected(
        "aaa-next-migration",
        identity.clone(),
        &moved,
        Action::Migrate {
            destination: Box::new(provider_route(&fixture, "third-provider")),
        },
    );
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &next)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&next, &snapshot).unwrap();
    assert!(matches!(
        plan.items()[0].status,
        PlanStatus::Ready | PlanStatus::Candidate
    ));
    let capture = object_capture(&plan);
    let target = capture.target.as_ref().unwrap();
    assert!(target.requirements == before.requirements);
    assert_eq!(
        target.frozen_candidates.len(),
        before.frozen_candidates.len()
    );
    for (original, current) in before
        .frozen_candidates
        .iter()
        .zip(&target.frozen_candidates)
    {
        assert_eq!(current.slot, original.slot);
        assert!(current.route == original.route);
        if current.slot == point.slot {
            assert_eq!(current.allocation, moved.provider);
            assert!(current.route.as_ref() != Some(&capture.route.identity));
        } else {
            assert!(current == original);
        }
    }

    // All's unrelated target cannot enter another slot's current resource.
    // One must also retain the unallocated alternative's birth-resource fence.
    let (collision_target, collision_destination) = if let Some(other) = other {
        (other, destination.clone())
    } else {
        (
            &moved,
            before
                .frozen_candidates
                .iter()
                .find(|candidate| candidate.slot != point.slot)
                .unwrap()
                .route
                .clone()
                .unwrap(),
        )
    };
    let collision = selected(
        "other-slot-collision",
        identity.clone(),
        collision_target,
        Action::Migrate {
            destination: Box::new(collision_destination),
        },
    );
    let collision_snapshot =
        db_snapshot::collect_with_config(&fixture.db, &fixture.config, &collision)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
    assert_eq!(
        dry_run(&collision, &collision_snapshot).unwrap().items()[0].status,
        PlanStatus::Blocked(SafeError::InvalidActionScope)
    );
    let same_route = selected(
        "same-route",
        identity.clone(),
        &moved,
        Action::Migrate {
            destination: Box::new(destination),
        },
    );
    let same_snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &same_route)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    assert_eq!(
        dry_run(&same_route, &same_snapshot).unwrap().items()[0].status,
        PlanStatus::Blocked(SafeError::InvalidActionScope)
    );
    let stale_target = selected(
        "stale-target",
        identity,
        source,
        next.items[0].action.clone(),
    );
    let stale_snapshot =
        db_snapshot::collect_with_config(&fixture.db, &fixture.config, &stale_target)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
    assert!(matches!(
        dry_run(&stale_target, &stale_snapshot).unwrap().items()[0].status,
        PlanStatus::Blocked(_)
    ));

    let acknowledged = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect::<Vec<_>>();
    // Public applied claims cannot supply the private confirmation or suppress
    // the independent review required by the actual native plan.
    let mut claimed = snapshot.clone();
    let mut claimed_entry = plan.items()[0].clone();
    claimed_entry.status = PlanStatus::Ready;
    claimed.applied.push(AppliedItem {
        binding: claimed_entry,
    });
    let claimed_plan = dry_run(&next, &claimed).unwrap();
    assert!(
        records::confirm(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            (&next, &claimed, &claimed_plan),
            vec![],
            chrono::Duration::minutes(5)
        )
        .await
        .is_err()
    );

    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&next, &snapshot, &plan),
        acknowledged,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    let bytes = archive::export(&binding);
    assert!(
        archive::restore(&fixture.db, &fixture.auth, &next, &bytes)
            .await
            .unwrap()
            == binding
    );
    let forged = archive::rewrite(&bytes, |frame| {
        if let wire::v3::Frame::Acknowledgements { item_ids } = frame {
            *item_ids = None;
        }
    });
    match forged {
        Ok(bytes) => assert!(
            archive::restore(&fixture.db, &fixture.auth, &next, &bytes)
                .await
                .is_err()
        ),
        Err(error) => assert_eq!(error, SafeError::ConfirmationMismatch),
    }
}
