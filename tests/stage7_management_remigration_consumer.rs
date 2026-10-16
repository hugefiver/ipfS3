#[path = "support/stage7_management_remigration.rs"]
mod slot_cases;
#[path = "support/stage7_management_actions.rs"]
pub mod support;
#[path = "support/stage7_management_remigration_unknown.rs"]
mod unknown_cases;

use ipfs_s3_gateway::{
    pinning::management::*,
    store::{
        entities::{pin_lease, pin_lease_requirement, pin_lease_target},
        pinning::management::{actions, native, records},
    },
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

fn selected(
    plan: &str,
    identity: ExactObject,
    target: &pin_lease_target::Model,
    action: Action,
) -> PlanRequest {
    PlanRequest {
        plan_id: plan.into(),
        items: vec![ItemRequest {
            item_id: "object".into(),
            action,
            subject: Subject::Object {
                identity,
                target: TargetSelection::Existing {
                    target_id: target.id.clone(),
                    source: None,
                },
            },
        }],
    }
}

fn provider_route(fixture: &support::Fixture, id: &str) -> RouteIdentity {
    RouteIdentity::from_original(
        fixture
            .config
            .pinning
            .providers
            .iter()
            .find(|provider| provider.identity.provider_id == id)
            .unwrap()
            .route_snapshot(),
    )
}

#[tokio::test]
async fn remigration_consumes_current_slot_after_native_prefix() {
    let mut fixture = support::Fixture::new().await;
    let identity = fixture.publish("chain", false).await;
    let create = fixture
        .confirm(&fixture.create_request("chain-create", identity.clone(), false))
        .await;
    let created = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &create)
        .await
        .unwrap();
    let lease_id = &created.items[0].receipt.intent_ids[0];
    fixture.settle_with_worker(lease_id).await;
    let original = pin_lease::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let target = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    for (id, action) in [
        (
            "chain-renew",
            Action::Renew {
                expires_at: original.expires_at.timestamp() + 300,
            },
        ),
        ("chain-retry", Action::Retry),
        ("chain-reconcile", Action::Reconcile),
    ] {
        let binding = fixture
            .confirm(&selected(id, identity.clone(), &target, action))
            .await;
        let result = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap_or_else(|error| {
                let Some(Capture::Object(capture)) = &binding.confirmation.bindings[0].capture else {
                    panic!("native prefix {id} apply failed: {error:?}");
                };
                let candidates = capture.target.as_ref().unwrap().frozen_candidates.iter()
                    .map(|candidate| (&candidate.slot, &candidate.allocation))
                    .collect::<Vec<_>>();
                panic!("native prefix {id} apply failed: {error:?}; lease={} cid={} target={} nativeAllocation={} providerID={} slots={candidates:?}",
                    lease_id, target.cid, target.id, target.provider, capture.route.identity.provider_id);
            });
        if id == "chain-reconcile" {
            assert_eq!(result.items[0].state, records::ActionState::ReadOnly);
            assert!(
                result.items[0]
                    .jobs
                    .iter()
                    .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
            );
        }
    }
    fixture.add_provider("destination").await;
    let migrate = fixture
        .confirm(&selected(
            "chain-migrate",
            identity.clone(),
            &target,
            Action::Migrate {
                destination: Box::new(provider_route(&fixture, "destination-provider")),
            },
        ))
        .await;
    let first = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &migrate)
        .await
        .unwrap();
    assert_eq!(first.items[0].migration_points.len(), 1);
    fixture.settle_with_worker(lease_id).await;
    let first_point = &first.items[0].migration_points[0];
    let moved = pin_lease_target::Entity::find_by_id(first_point.destination_target_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.add_provider("third").await;
    let request = selected(
        "aaa-chain-remigrate",
        identity,
        &moved,
        Action::Migrate {
            destination: Box::new(provider_route(&fixture, "third-provider")),
        },
    );
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let resolved =
        actions::points::resolved_slot(&fixture.db, lease_id, &first_point.cid, &first_point.slot)
            .await
            .unwrap()
            .unwrap();
    assert!(matches!(
        plan.items()[0].status,
        PlanStatus::Ready | PlanStatus::Candidate
    ));
    let acknowledged = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect::<Vec<_>>();
    let archive = wire::encode_plan(
        &request,
        &snapshot,
        &plan,
        Some(&plan.confirmation(acknowledged.clone())),
    )
    .unwrap();
    let imported = wire::decode_plan(&archive).unwrap();
    let confirmed = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        acknowledged,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    let second = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
        .await
        .unwrap();
    let second_point = &second.items[0].migration_points[0];
    assert!(second_point.predecessor.as_ref() == Some(&resolved.record_key));
    assert_eq!(second_point.lease_id, first_point.lease_id);
    assert_eq!(second_point.cid, first_point.cid);
    assert_eq!(second_point.slot, first_point.slot);
    assert_eq!(
        second_point.source_target_id,
        first_point.destination_target_id
    );
    assert!(second_point.original_contract == first_point.original_contract);
    assert_eq!(second_point.started_at, first_point.started_at);
    assert_eq!(second_point.expires_at, first_point.expires_at);
    fixture.settle_with_worker(lease_id).await;
    let tip = actions::points::resolved_slot(
        &fixture.db,
        lease_id,
        &second_point.cid,
        &second_point.slot,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        tip.point.destination_target_id,
        second_point.destination_target_id
    );
    assert!(
        tip.record_key
            == native::NativeKey {
                table: "pin_management_records".into(),
                values: vec![
                    confirmed.database_scope.clone(),
                    request.plan_id.clone(),
                    "object".into(),
                    "action".into()
                ]
            }
    );
    let current_lease = pin_lease::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current_lease.created_at, original.created_at);
    assert_eq!(current_lease.expires_at, first_point.expires_at);
    assert_eq!(current_lease.generation, first_point.source_generation);
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(lease_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .contract,
        native::value(&first_point.original_contract, "contract").unwrap()
    );
    let before_replay = native::capture(&fixture.db).await.unwrap();
    let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
        .await
        .unwrap();
    assert!(replay.replayed && replay.items == second.items);
    let receipt = records::lookup_applied(&fixture.db, &fixture.auth, &confirmed, "object")
        .await
        .unwrap()
        .unwrap();
    assert!(receipt == second.items[0].receipt);
    let restored = records::load_confirmed_from_imported(&fixture.db, &fixture.auth, &imported)
        .await
        .unwrap();
    assert!(restored == confirmed);
    assert!(
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, &restored)
            .await
            .unwrap()
            .replayed
    );
    assert!(native::capture(&fixture.db).await.unwrap() == before_replay);
}

#[tokio::test]
async fn one_keeps_birth_alternatives_and_rejects_other_slot_collision() {
    slot_cases::two_slot_planning(true).await;
}

#[tokio::test]
async fn all_keeps_birth_slots_and_rejects_other_current_destination() {
    slot_cases::two_slot_planning(false).await;
}

#[tokio::test]
async fn native_one_unknown_birth_alternative_survives_remigration_projection() {
    unknown_cases::native_unknown_birth(true).await;
}

#[tokio::test]
async fn native_all_unknown_birth_alternative_still_refuses_migration() {
    unknown_cases::native_unknown_birth(false).await;
}
