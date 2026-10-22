//! Scoped planning keeps unrelated objects outside its read/decode work.
#[path = "support/stage7_management_actions.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::{
    pinning::management::{db_snapshot, dry_run},
    store::pinning::management::records,
};
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};

const UNRELATED_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

#[tokio::test]
async fn registered_route_uses_private_database_original_not_configuration_fallback() {
    use ipfs_s3_gateway::pinning::management::{PlanStatus, SafeError};
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("private-route", false).await;
    let request = fixture.create_request("private-route", identity, false);
    let original = fixture.config.pinning.providers[0].route_snapshot();
    let allocation = &fixture.config.pinning.providers[0].name;
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let frame = snapshot.native_frame.as_ref().unwrap();
    assert_eq!(
        frame.row("pin_provider_routes", &[allocation]).unwrap()["snapshot"],
        None
    );
    assert!(
        frame
            .original_value("pin_provider_routes", &[allocation], "snapshot")
            .is_some()
    );
    assert!(
        snapshot
            .routes
            .iter()
            .any(|route| route.original.as_ref() == Some(&original) && route.allocation_allowed)
    );
    assert_eq!(
        dry_run(&request, &snapshot).unwrap().items()[0].status,
        PlanStatus::Candidate
    );

    let mut replacement = fixture.config.pinning.providers[0].identity.clone();
    replacement.endpoint_revision += 1;
    ipfs_s3_gateway::store::pinning::ledger::register_route(&fixture.db, allocation, &replacement)
        .await
        .unwrap();
    let missing = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    assert!(
        !missing
            .routes
            .iter()
            .any(|route| route.original.as_ref() == Some(&original))
    );
    assert_eq!(
        dry_run(&request, &missing).unwrap().items()[0].status,
        PlanStatus::Blocked(SafeError::MissingRoute)
    );
}

#[tokio::test]
async fn existing_target_preserves_private_birth_contract_after_registration_changes() {
    use ipfs_s3_gateway::pinning::management::Action;
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("private-contract", true).await;
    let request = fixture
        .existing_request("private-contract", identity, Action::Cancel)
        .await;
    let provider = &fixture.config.pinning.providers[0];
    let original = provider.route_snapshot();
    let mut replacement = provider.identity.clone();
    replacement.endpoint_revision += 1;
    ipfs_s3_gateway::store::pinning::ledger::register_route(
        &fixture.db,
        &provider.name,
        &replacement,
    )
    .await
    .unwrap();
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let target = &snapshot.targets[0];
    assert!(target.route.as_ref().unwrap().original.as_ref() == Some(&original));
    assert!(
        target.frozen_candidates[0]
            .route
            .as_ref()
            .unwrap()
            .original
            .as_ref()
            == Some(&original)
    );
    assert!(target.requirements.is_some());
    let frame = snapshot.native_frame.as_ref().unwrap();
    assert_eq!(
        frame
            .row("pin_lease_requirements", &[&target.lease.id])
            .unwrap()["contract"],
        None
    );
    assert_eq!(
        target.frozen_contract.as_ref().unwrap()["contract"].as_deref(),
        frame.original_value("pin_lease_requirements", &[&target.lease.id], "contract")
    );
    assert!(
        snapshot
            .routes
            .iter()
            .any(|route| route.original.as_ref() == Some(&original) && !route.allocation_allowed)
    );
}

#[tokio::test]
async fn local_planning_snapshot_and_scope_share_the_selected_read_view() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("selected", false).await;
    let request = fixture.create_request("scoped-planning", identity.clone(), false);
    let mut unrelated = Vec::new();
    for key in ["unrelated/one", "unrelated/two"] {
        unrelated.push(
            support::base::publish_cid(&fixture.db, &fixture.config, key, UNRELATED_CID, false)
                .await,
        );
    }
    let work = records::SharedDecodeBudget::default();
    let captured = db_snapshot::collect_with_session(&fixture.db, &fixture.config, &request, work)
        .await
        .unwrap();
    let measured_work = captured.work().unwrap();
    let measured_peak = captured.peak_resident().unwrap();
    assert!(measured_work.sql > 0 && measured_work.rows > 0 && measured_work.raw_bytes > 0);
    assert!(measured_work.copied_bytes > 0 && measured_peak > 0);
    let (snapshot, scope) = captured.planning_with_scope().unwrap();
    let scope_work = scope.work();
    assert_eq!(
        (
            scope_work.sql,
            scope_work.rows,
            scope_work.cells,
            scope_work.raw_bytes,
            scope_work.copied_bytes,
            scope_work.output_bytes,
            scope_work.pages,
        ),
        (
            measured_work.sql,
            measured_work.rows,
            measured_work.cells,
            measured_work.raw_bytes,
            measured_work.copied_bytes,
            measured_work.output_bytes,
            measured_work.pages,
        )
    );
    assert_eq!(scope.peak_resident(), measured_peak);
    assert_eq!(snapshot.objects.len(), 1);
    assert!(snapshot.objects[0].identity == identity);
    let frame = snapshot.native_frame.as_ref().unwrap();
    assert_eq!(frame.database_scope, scope.database_scope);
    assert_eq!(frame.database_identity, scope.database_identity);
    assert_eq!(frame.frontier.len(), scope.row_count());
    for key in &frame.frontier {
        let original = scope.originals().get(key).unwrap();
        let values: Vec<_> = key.values.iter().map(String::as_str).collect();
        for (column, value) in original {
            assert_eq!(
                frame.original_value(&key.table, &values, column),
                value.as_deref()
            );
        }
    }
    let plan = dry_run(&request, &snapshot).unwrap();
    assert!(plan.items().iter().all(|item| !matches!(
        item.status,
        ipfs_s3_gateway::pinning::management::PlanStatus::Blocked(_)
    )));
    assert!(scope.has_empty_membership());
    assert!(
        scope
            .authority_memberships
            .get(&records::locator::AuthoritySelector::RecordMembers(
                scope.database_scope.clone(),
                request.plan_id.clone()
            ))
            .is_some_and(Vec::is_empty)
    );
    assert!(
        snapshot
            .inventories
            .iter()
            .all(|inventory| !inventory.complete)
    );
    for object in unrelated {
        assert!(frame.frontier.iter().all(|key| {
            !key.values
                .iter()
                .any(|value| value == &object.object_id || value == &object.private_version_id)
        }));
    }
}

#[tokio::test]
async fn cancellation_keeps_shared_owners_without_expanding_their_other_objects() {
    use ipfs_s3_gateway::pinning::management::Action;
    let fixture = support::Fixture::new().await;
    let selected = fixture.publish("selected", true).await;
    let shared = fixture.publish("shared", true).await;
    let request = fixture
        .existing_request("shared-closure", selected.clone(), Action::Cancel)
        .await;
    let captured = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap();
    let (snapshot, scope) = captured.planning_with_scope().unwrap();
    assert_eq!(snapshot.objects.len(), 1);
    assert!(
        snapshot
            .targets
            .iter()
            .any(|target| target.lease.object_id == selected.object_id)
    );
    assert!(
        snapshot
            .targets
            .iter()
            .any(|target| target.lease.object_id == shared.object_id)
    );
    assert!(
        !scope
            .originals()
            .keys()
            .any(|key| key.table == "objects" && key.values == [shared.object_id.clone()])
    );
    assert!(scope.memberships.keys().any(|selector|matches!(selector,ipfs_s3_gateway::store::pinning::management::native::scope::Selector::LeaseRequirements {..})));
    let plan = dry_run(&request, &snapshot).unwrap();
    assert!(!matches!(
        plan.items()[0].status,
        ipfs_s3_gateway::pinning::management::PlanStatus::Blocked(_)
    ));
    fixture.publish("new-shared", true).await;
    let tx = fixture.db.begin().await.unwrap();
    let mut budget = ipfs_s3_gateway::store::pinning::management::native::meter::Budget::default();
    assert!(scope.revalidate(&tx, &mut budget).await.is_err());
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn authority_reseal_changes_fence_even_when_original_rows_and_memberships_are_unchanged() {
    use ipfs_s3_gateway::store::pinning::management::{native::meter::Budget, records::locator};
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("selected", false).await;
    let request = fixture.create_request("authority-generation", identity, false);
    let (_, scope) = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_with_scope()
        .unwrap();
    let preparation = scope
        .authority_reads()
        .iter()
        .flat_map(|read| &read.proof.seals)
        .map(|seal| &seal.source)
        .find(|key| {
            key.table == "pin_management_records"
                && key
                    .values
                    .get(3)
                    .is_some_and(|kind| kind == "evidence_preparation")
        })
        .unwrap()
        .clone();
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "UPDATE pin_management_records SET principal=principal WHERE database_scope=$1 AND plan_id=$2 AND item_id=$3 AND kind=$4",
        preparation.values.iter().map(|value| sea_orm::Value::from(value.clone())).collect::<Vec<_>>()
    )).await.unwrap();
    locator::rebuild_source(&tx, &preparation, &mut Budget::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let fresh = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap();
    assert!(scope.originals() == fresh.scope().unwrap().originals());
    assert!(scope.authority_memberships == fresh.scope().unwrap().authority_memberships);
    let tx = fixture.db.begin().await.unwrap();
    assert_eq!(
        scope.revalidate(&tx, &mut Budget::default()).await,
        Err(db_snapshot::SnapshotError::Changed)
    );
    tx.rollback().await.unwrap();
}
