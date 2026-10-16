use super::{provider_route, selected, support};
use ipfs_s3_gateway::{
    pinning::management::*,
    store::{
        self,
        entities::{pin_lease, pin_lease_requirement, pin_lease_target},
        pinning::{
            jobs,
            management::actions,
            publication::{PinTargetSpec, management::attach_destination_in_transaction},
        },
    },
};
use sea_orm::{EntityTrait, Set, TransactionTrait};

pub async fn native_unknown_birth(one: bool) {
    let mut fixture = support::Fixture::new().await;
    let identity = fixture.publish("native-unknown-alternative", false).await;
    let allocation = fixture.config.pinning.providers[0].name.clone();
    let order = vec![allocation.clone(), "historical-unknown-alternative".into()];
    let mode = if one { "one" } else { "all" };
    let lease_id = format!("historical-native-{}", uuid::Uuid::new_v4());
    let tx = fixture.db.begin().await.unwrap();
    let now = store::database_clock::database_now(&tx).await.unwrap();
    pin_lease::Entity::insert(pin_lease::ActiveModel {
        id: Set(lease_id.clone()),
        owner_object_id: Set(identity.object_id.clone()),
        source: Set("automatic".into()),
        policy_id: Set(fixture.config.pinning.policies[0].identity.clone()),
        provider_mode: Set(mode.into()),
        content_mode: Set("object".into()),
        created_at: Set(now),
        last_touched_at: Set(now),
        expires_at: Set(now + chrono::Duration::hours(1)),
        generation: Set(1),
        state: Set("active".into()),
    })
    .exec(&tx)
    .await
    .unwrap();
    // Initial native birth captures the absent historical route as None. No
    // existing contract is updated, and no management grant is manufactured.
    pin_lease_requirement::contract::insert(
        &tx,
        &lease_id,
        &identity.object_id,
        mode,
        "object",
        std::iter::once(identity.cid.clone()),
        &order,
        now,
    )
    .await
    .unwrap();
    let source = attach_destination_in_transaction(
        &tx,
        &lease_id,
        &allocation,
        &PinTargetSpec {
            cid: identity.cid.clone(),
            logical_size: 10,
        },
        &fixture.config.pinning.provider_limits,
        now,
    )
    .await
    .unwrap();
    let jobs::NewPinJob::Target(job) =
        jobs::submit_job(&allocation, &source.cid, &lease_id, &source.id, 1, now)
    else {
        unreachable!()
    };
    jobs::ensure_or_reactivate_submit_job(&tx, job, now)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let original_contract = pin_lease_requirement::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap()
        .contract;
    assert!(
        pin_lease_requirement::contract::management_requirements(&fixture.db, &lease_id)
            .await
            .unwrap()
            .is_none()
    );
    let birth = pin_lease_requirement::contract::get(&fixture.db, &lease_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(birth.candidate_order, order);
    assert!(birth.candidates[&order[0]].is_some());
    assert!(birth.candidates[&order[1]].is_none());
    if one {
        fixture.settle_with_worker(&lease_id).await;
    }
    fixture.add_provider("destination").await;
    let request = selected(
        "native-first-migration",
        identity.clone(),
        &source,
        Action::Migrate {
            destination: Box::new(provider_route(&fixture, "destination-provider")),
        },
    );
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let Some(Capture::Object(capture)) = &plan.items()[0].capture else {
        panic!(
            "native birth capture unavailable: {:?}",
            plan.items()[0].status
        );
    };
    let before = capture.target.as_ref().unwrap();
    assert_eq!(
        before
            .frozen_candidates
            .iter()
            .map(|candidate| candidate.slot.clone())
            .collect::<Vec<_>>(),
        order
    );
    assert!(before.frozen_candidates[1].route.is_none());
    if !one {
        assert!(before.requirements.is_none());
        assert!(matches!(plan.items()[0].status, PlanStatus::Blocked(_)));
        return;
    }
    let requirements = before.requirements.clone().unwrap();
    assert_eq!(requirements.mode, ProviderMode::One);
    assert_eq!(requirements.provider_ids, vec!["fixture-provider"]);
    assert_eq!(requirements.min_replicas, 1);
    assert_eq!(requirements.max_age_secs, None);
    assert_eq!(requirements.levels, vec![ConfirmationLevel::RemotePinned]);
    let binding = fixture.confirm(&request).await;
    let first = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let point = first.items[0]
        .migration_points
        .first()
        .expect("native source migration must produce its private point");
    let moved = pin_lease_target::Entity::find_by_id(point.destination_target_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.settle_with_worker(&lease_id).await;
    fixture.add_provider("third").await;
    let next = selected(
        "aaa-native-remigration",
        identity,
        &moved,
        Action::Migrate {
            destination: Box::new(provider_route(&fixture, "third-provider")),
        },
    );
    let next_snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &next)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let next_plan = dry_run(&next, &next_snapshot).unwrap();
    assert!(
        matches!(
            next_plan.items()[0].status,
            PlanStatus::Ready | PlanStatus::Candidate
        ),
        "native One remigration must preserve its known projection: {:?}",
        next_plan.items()[0].status
    );
    let Some(Capture::Object(projected)) = &next_plan.items()[0].capture else {
        unreachable!()
    };
    let projected = projected.target.as_ref().unwrap();
    assert!(projected.requirements.as_ref() == Some(&requirements));
    assert_eq!(
        projected
            .frozen_candidates
            .iter()
            .map(|candidate| candidate.slot.clone())
            .collect::<Vec<_>>(),
        order
    );
    assert!(projected.frozen_candidates[1] == before.frozen_candidates[1]);
    assert!(projected.frozen_candidates[0].route == before.frozen_candidates[0].route);
    assert_eq!(projected.frozen_candidates[0].allocation, moved.provider);
    let acknowledged = next_plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let archive = wire::encode_plan(
        &next,
        &next_snapshot,
        &next_plan,
        Some(&next_plan.confirmation(acknowledged)),
    )
    .unwrap();
    assert!(wire::decode_plan(&archive).unwrap().plan().items() == next_plan.items());
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(lease_id)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .contract,
        original_contract
    );
}
