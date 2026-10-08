use crate::{
    pinning::identity::{CleanupMode, ProviderIdentity},
    store::{
        self,
        pinning::{ledger, legacy_migration as migration},
    },
};
use sea_orm::ConnectionTrait;
use sea_orm_migration::{MigrationTrait, MigratorTrait};

struct Stage5;
#[async_trait::async_trait]
impl MigratorTrait for Stage5 {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        crate::store::migrator::Migrator::migrations()
            .into_iter()
            .take_while(|m| m.name() != "m20261004_000002_cluster_replica_contract")
            .collect()
    }
}

#[tokio::test]
async fn known_stage5_inventory_recognizes_missing_stage6_but_apply_requires_formal_upgrade() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    Stage5::up(&db, None).await.unwrap();
    let identity = ProviderIdentity {
        provider_id: "account".into(),
        display_name: "fixture".into(),
        backend: "noop".into(),
        scope: "account".into(),
        storage_domain: "fixture".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        retired: false,
        cleanup: CleanupMode::Retain,
    };
    ledger::register_route(&db, &identity.allocation_key(), &identity)
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    let declaration:migration::Declaration=serde_json::from_value(serde_json::json!({"version":1,"database_scope":"fixture","ttl_seconds":300,"maintenance":{"all_writers_stopped":true,"all_workers_drained":true},"namespaces":[{"source":"old","target":identity.allocation_key(),"backend":"noop","scope":"account","resource_type":identity.resource_type(),"binding_only":false,"target_route":identity.route_snapshot(),"bindings":[{"table":"pin_provider_usage","key":{"provider":"old"},"provider":"old","historical_route":null}]}]})).unwrap();
    let old = migration::inventory(&db, &declaration).await.unwrap();
    assert!(
        old.before
            .iter()
            .find(|t| t.table == "pin_lease_requirements")
            .unwrap()
            .columns
            .is_empty()
    );
    assert!(migration::apply(&db, &declaration, &old).await.is_err());
    store::run_migrations(&db).await.unwrap();
    assert!(
        migration::apply(&db, &declaration, &old).await.is_err(),
        "schema upgrade requires a new confirmation"
    );
    let fresh = migration::inventory(&db, &declaration).await.unwrap();
    migration::apply(&db, &declaration, &fresh).await.unwrap();
}

#[tokio::test]
async fn unknown_columns_are_not_an_empty_inventory() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let schema = super::schema(&db, false).await.unwrap();
    db.execute_unprepared("ALTER TABLE pin_resource_history ADD COLUMN unknown_evidence TEXT")
        .await
        .unwrap();
    assert!(super::read(&db, &schema).await.is_err());
}
