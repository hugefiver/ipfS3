//! Actual mocked Cluster adapter observations, never physical replica/ownership proof.
#[allow(dead_code)]
#[path = "stage6_cluster_fixture.rs"]
mod cluster;
#[path = "stage7_retirement_barrier.rs"]
mod retirement;
pub use super::support::base as foundation;

use ipfs_s3_gateway::{
    pinning::identity::{ConfirmationRequirement, ProviderRouteSnapshot},
    store::{
        entities::{
            pin_lease, pin_lease_requirement, pin_provider_route, pin_provider_usage, remote_pin,
        },
        pinning::{ledger, management::native},
    },
};
use sea_orm::EntityTrait;

async fn fixture() -> (tempfile::TempDir, cluster::Fixture) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retirement-replicas.sqlite");
    let database = format!(
        "sqlite://{}?mode=rwc",
        path.to_string_lossy().replace('\\', "/")
    );
    let fixture = cluster::Fixture::new("rest", false, &database).await;
    (directory, fixture)
}

async fn close(fixture: &mut cluster::Fixture, plan: &str) -> ProviderRouteSnapshot {
    // Exercise the real startup transaction and retired metadata; no fake
    // invocation, replica row, release or ownership fact is inserted.
    fixture
        .reconfigure(|config| config.pinning_identity.providers[0].retired = true)
        .await;
    let config = retirement::foundation::management_config(&fixture.config);
    retirement::retire(fixture.state.store.db(), &fixture.config, &config, plan).await;
    let registered = pin_provider_route::Entity::find()
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(registered.retired);
    let original: ProviderRouteSnapshot = serde_json::from_str(&registered.snapshot).unwrap();
    assert_eq!(original, config.pinning.providers[0].route_snapshot());
    original
}

#[tokio::test]
async fn retired_registration_does_not_hide_original_reader_fact_or_relax_its_bounds() {
    let (_directory, mut fixture) = fixture().await;
    fixture.put("original-reader").await;
    fixture.start();
    let original = fixture.wait("Complete", 0).await;
    fixture.stop().await;
    assert!(original.projectable);
    let route = close(&mut fixture, "retired-original-reader").await;
    let db = fixture.state.store.db();
    let read_at = original.observed_at;
    // Explicit query time makes the reader-boundary case deterministic without
    // editing any recorded timestamp, claim, configured deadline or max age.
    assert!(
        ipfs_s3_gateway::store::entities::pin_replica_observation::receipt::fresh_complete(
            db,
            &original.provider,
            &original.expected_cid,
            &route,
            read_at,
        )
        .await
        .unwrap()
    );
    let mut stricter = route.clone();
    stricter.confirmation = Some(ConfirmationRequirement::ClusterReplicas {
        min_replicas: 3,
        max_age_secs: 2,
    });
    assert!(
        !ipfs_s3_gateway::store::entities::pin_replica_observation::receipt::fresh_complete(
            db,
            &original.provider,
            &original.expected_cid,
            &stricter,
            read_at,
        )
        .await
        .unwrap()
    );
    let mut later_reader = route.clone();
    later_reader.confirmation = Some(ConfirmationRequirement::ClusterReplicas {
        min_replicas: 2,
        max_age_secs: 30,
    });
    assert!(
        !ipfs_s3_gateway::store::entities::pin_replica_observation::receipt::fresh_complete(
            db,
            &original.provider,
            &original.expected_cid,
            &later_reader,
            read_at + chrono::Duration::seconds(2),
        )
        .await
        .unwrap(),
        "retirement or a later reader cannot extend the recorded two-second upper bound"
    );
    for change in [
        "profile",
        "endpoint",
        "credential",
        "secret",
        "backend",
        "scope",
    ] {
        let mut incompatible = route.clone();
        match change {
            "profile" => incompatible.api_profile = "cluster-proxy".into(),
            "endpoint" => incompatible.endpoint_revision += 1,
            "credential" => incompatible.credential_revision += 1,
            "secret" => incompatible.secret_ref = Some("env:DIFFERENT_READER".into()),
            "backend" => incompatible.backend = "other-cluster".into(),
            _ => incompatible.scope = "another-pinset".into(),
        }
        assert!(
            !ipfs_s3_gateway::store::entities::pin_replica_observation::receipt::fresh_complete(
                db,
                &original.provider,
                &original.expected_cid,
                &incompatible,
                read_at,
            )
            .await
            .unwrap(),
            "retirement must not weaken original read compatibility: {change}"
        );
    }
    assert!(
        !ipfs_s3_gateway::store::entities::pin_replica_observation::receipt::fresh_complete(
            db,
            &original.provider,
            &cluster::other_cid(),
            &route,
            read_at,
        )
        .await
        .unwrap()
    );
    assert_eq!(
        ipfs_s3_gateway::store::entities::pin_replica_observation::Entity::find_by_id(
            original.sequence
        )
        .one(db)
        .await
        .unwrap()
        .unwrap(),
        original,
        "read-only qualification never rewrites history"
    );
}

#[tokio::test]
async fn retired_original_lifetime_cluster_poll_can_settle_without_new_post_delete_or_ownership() {
    let (_directory, mut fixture) = fixture().await;
    let lease_id = fixture.put("existing-lifetime").await;
    fixture.start();
    let original = fixture.wait("Complete", 0).await;
    fixture.stop().await;
    let route = close(&mut fixture, "retired-existing-lifetime").await;
    let db = fixture.state.store.db();
    let before = native::capture(db).await.unwrap();
    let lease = pin_lease::Entity::find_by_id(lease_id.clone())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let birth = pin_lease_requirement::Entity::find_by_id(lease_id)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let usage = pin_provider_usage::Entity::find_by_id(original.provider.clone())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let remote =
        remote_pin::Entity::find_by_id((original.provider.clone(), original.expected_cid.clone()))
            .one(db)
            .await
            .unwrap()
            .unwrap();
    let ledger_before = ledger::get(db, &remote.provider, &remote.cid)
        .await
        .unwrap()
        .unwrap();
    let requests = fixture.rest.received_requests().await.unwrap().len();
    fixture.due().await;
    fixture.start();
    let settled = fixture.wait("Complete", original.sequence).await;
    fixture.stop().await;
    let db = fixture.state.store.db();
    assert!(
        settled.projectable,
        "a retired row alone cannot suppress a live original-lifetime receipt"
    );
    assert_eq!(settled.job_id, original.job_id);
    assert_eq!(settled.remote_epoch, original.remote_epoch);
    assert_eq!(settled.expected_generation, original.expected_generation);
    assert_eq!(settled.route, original.route);
    assert!(
        ipfs_s3_gateway::store::entities::pin_replica_observation::receipt::fresh_complete(
            db,
            &settled.provider,
            &settled.expected_cid,
            &route,
            settled.observed_at,
        )
        .await
        .unwrap()
    );
    assert_eq!(
        pin_lease::Entity::find_by_id(lease.id.clone())
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        lease
    );
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(birth.lease_id.clone())
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        birth
    );
    assert_eq!(
        pin_provider_usage::Entity::find_by_id(usage.provider.clone())
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        usage
    );
    let after_remote =
        remote_pin::Entity::find_by_id((remote.provider.clone(), remote.cid.clone()))
            .one(db)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(after_remote.epoch, remote.epoch);
    assert_eq!(after_remote.request_id, remote.request_id);
    assert_eq!(
        ledger::get(db, &remote.provider, &remote.cid)
            .await
            .unwrap()
            .unwrap()
            .ownership,
        ledger_before.ownership,
        "read settlement cannot grant ownership"
    );
    let after = native::capture(db).await.unwrap();
    for table in ["pin_resource_history", "pin_submit_history"] {
        assert!(
            before.table(table) == after.table(table),
            "historical polling changed {table}"
        );
    }
    let calls = fixture.rest.received_requests().await.unwrap();
    let new_calls = &calls[requests..];
    assert!(!new_calls.is_empty());
    assert!(
        new_calls.iter().all(|call| call.method.as_str() == "GET"),
        "historical polling cannot dispatch POST/DELETE"
    );
}
