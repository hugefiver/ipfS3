use crate::{
    pinning::identity::{CleanupMode, ProviderIdentity},
    store::{self, pinning::legacy_migration as migration},
};
use sea_orm::ConnectionTrait;
use sea_orm_migration::{MigrationTrait, MigratorTrait};

struct Stage5;
#[async_trait::async_trait]
impl MigratorTrait for Stage5 {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        crate::store::migrations::Historical::migrations()
            .into_iter()
            .take_while(|m| m.name() != "m20261004_000002_cluster_replica_contract")
            .collect()
    }
}

#[tokio::test]
async fn known_stage5_inventory_recognizes_missing_stage6_but_apply_requires_formal_upgrade() {
    let (_directory, db) = file_database().await;
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
    seed_historical_route(&db, &identity).await;
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
    upgrade(&db).await;
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

async fn native_catalog(last: &str) -> sea_orm::DatabaseConnection {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    initialize_catalog(&db, last).await;
    db
}

async fn file_database() -> (tempfile::TempDir, sea_orm::DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("legacy.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    (directory, db)
}

async fn file_catalog(last: &str) -> (tempfile::TempDir, sea_orm::DatabaseConnection) {
    let (directory, db) = file_database().await;
    initialize_catalog(&db, last).await;
    (directory, db)
}

async fn initialize_catalog(db: &sea_orm::DatabaseConnection, last: &str) {
    if last == crate::store::pinning::management::schema::VERSION {
        store::run_migrations(db).await.unwrap();
        return;
    }
    let registry = store::migrations::Historical::migrations();
    let count = registry.iter().position(|m| m.name() == last).unwrap() + 1;
    store::migrations::Historical::up(db, Some(count as u32))
        .await
        .unwrap();
    let actual: Vec<String> = db
        .query_all(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await
        .unwrap()
        .iter()
        .map(|r| r.try_get("", "version").unwrap())
        .collect();
    assert_eq!(
        actual,
        registry[..count]
            .iter()
            .map(|m| m.name().to_owned())
            .collect::<Vec<_>>()
    );
}

async fn seed_historical_route(db: &sea_orm::DatabaseConnection, identity: &ProviderIdentity) {
    db.execute(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_provider_routes(provider,snapshot,display_name,retired) VALUES($1,$2,$3,$4)",
        [identity.allocation_key().into(),serde_json::to_string(&identity.route_snapshot()).unwrap().into(),identity.display_name.clone().into(),identity.retired.into()]
    )).await.unwrap();
}

async fn upgrade(db: &sea_orm::DatabaseConnection) {
    if store::initialization::classify(db).await.unwrap()
        == store::initialization::SchemaState::Current
    {
        return;
    }
    let source = "m20261013_000001_zip_root_logical_release";
    let registry = store::migrations::Historical::migrations();
    let source_count = registry
        .iter()
        .position(|migration| migration.name() == source)
        .unwrap()
        + 1;
    let applied = db
        .query_all(sea_orm::Statement::from_string(
            db.get_database_backend(),
            "SELECT version FROM seaql_migrations",
        ))
        .await
        .unwrap()
        .len();
    if applied < source_count {
        store::migrations::Historical::up(db, Some((source_count - applied) as u32))
            .await
            .unwrap();
    }
    let (scope, identity) = store::pinning::management::native::scope::binding(db)
        .await
        .unwrap();
    store::initialization::upgrade_existing(
        db,
        store::initialization::UpgradeRequest {
            database_scope: &scope,
            native_identity: &identity,
            source_version: source,
            target_version: store::pinning::management::schema::VERSION,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn known_stage6_catalog_remains_read_only() {
    let db = native_catalog("m20261004_000002_cluster_replica_contract").await;
    let versions = super::schema(&db, false).await.unwrap();
    let images = super::read(&db, &versions).await.unwrap();
    assert!(
        !images
            .iter()
            .find(|t| t.table == "pin_lease_requirements")
            .unwrap()
            .columns
            .is_empty()
    );
    assert!(super::schema(&db, true).await.is_err());
}

#[tokio::test]
async fn known_r26_catalog_without_zip_capture_remains_exact() {
    let db = native_catalog(migration::SCHEMA).await;
    let versions = super::schema(&db, true).await.unwrap();
    let images = super::read(&db, &versions).await.unwrap();
    for (table, column) in [
        ("multipart_uploads", "zip_intent_json"),
        ("import_jobs", "zip_intent_json"),
        ("zip_batches", "legacy_input_proof_json"),
    ] {
        assert!(
            !images
                .iter()
                .find(|t| t.table == table)
                .unwrap()
                .columns
                .iter()
                .any(|c| c == column)
        );
    }
}

#[tokio::test]
async fn current_catalog_includes_registered_zip_capture_columns() {
    let db = native_catalog("m20261008_000001_zip_intent_capture").await;
    let versions = super::schema(&db, true).await.unwrap();
    let images = super::read(&db, &versions).await.unwrap();
    for (table, column) in [
        ("multipart_uploads", "zip_intent_json"),
        ("import_jobs", "zip_intent_json"),
        ("zip_batches", "legacy_input_proof_json"),
    ] {
        assert!(
            images
                .iter()
                .find(|t| t.table == table)
                .unwrap()
                .columns
                .iter()
                .any(|c| c == column)
        );
    }
}

#[tokio::test]
async fn registered_prefix_does_not_authorize_hybrid_or_partial_catalog() {
    let stage5 = native_catalog("m20261004_000001_rpc_submission_ledger").await;
    crate::store::migrations::m20261004_000002_cluster_replica_contract::Migration
        .up(&sea_orm_migration::SchemaManager::new(&stage5))
        .await
        .unwrap();
    let versions = super::schema(&stage5, false).await.unwrap();
    assert!(super::read(&stage5, &versions).await.is_err());

    let r26 = native_catalog(migration::SCHEMA).await;
    r26.execute_unprepared("ALTER TABLE multipart_uploads ADD COLUMN zip_intent_json TEXT")
        .await
        .unwrap();
    let versions = super::schema(&r26, false).await.unwrap();
    assert!(super::read(&r26, &versions).await.is_err());

    let current = native_catalog("m20261008_000001_zip_intent_capture").await;
    current
        .execute_unprepared("ALTER TABLE import_jobs DROP COLUMN zip_intent_json")
        .await
        .unwrap();
    let versions = super::schema(&current, false).await.unwrap();
    assert!(super::read(&current, &versions).await.is_err());
}

#[path = "snapshot_capture_tests.rs"]
mod capture_tests;

#[path = "zip_capture_binding_tests.rs"]
mod zip_bindings;

#[path = "snapshot_management_catalog_tests.rs"]
mod management_catalog_tests;

#[path = "snapshot_management_raw_tests.rs"]
mod management_raw_tests;

#[path = "snapshot_management_postgres_tests.rs"]
mod management_postgres_tests;

#[path = "management_typed_tests.rs"]
mod management_typed_tests;
