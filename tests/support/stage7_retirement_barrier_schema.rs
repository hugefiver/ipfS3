//! Unit regressions use the actual registered migrator, not copied old DDL or
//! deleted Stage7 markers. Noop is a metadata fixture, never physical PIN proof.
use crate::{
    pinning::{config::ValidatedPinningConfig, coordinator::PinningCoordinator},
    store::{
        self,
        entities::pin_provider_route,
        pinning::{
            ledger,
            management::{admission, schema},
        },
    },
};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, Statement, TransactionTrait,
};
use sea_orm_migration::MigratorTrait;

const PREFIXES: [&str; 4] = [
    "m20261004_000001_rpc_submission_ledger",
    "m20261004_000002_cluster_replica_contract",
    "m20261007_000001_legacy_pinning_migration",
    "m20261008_000001_zip_intent_capture",
];

async fn prefix(last: &str) -> (tempfile::TempDir, DatabaseConnection) {
    let directory = tempfile::Builder::new()
        .prefix("stage7-retirement-schema-")
        .tempdir()
        .unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("prefix.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    let registry = store::migrator::Migrator::migrations();
    let count = registry
        .iter()
        .position(|migration| migration.name() == last)
        .unwrap()
        + 1;
    store::migrator::Migrator::up(&db, Some(count as u32))
        .await
        .unwrap();
    let versions: Vec<String> = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT version FROM main.seaql_migrations ORDER BY version",
        ))
        .await
        .unwrap()
        .iter()
        .map(|row| row.try_get("", "version").unwrap())
        .collect();
    assert_eq!(
        versions,
        registry[..count]
            .iter()
            .map(|migration| migration.name().to_owned())
            .collect::<Vec<_>>()
    );
    if last != schema::VERSION {
        assert_management_absent(&db).await;
    }
    (directory, db)
}

async fn assert_management_absent(db: &DatabaseConnection) {
    let row = db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT 1 FROM main.sqlite_schema WHERE name IN ('pin_management_records','pin_management_retirements') OR tbl_name IN ('pin_management_records','pin_management_retirements') LIMIT 1"))
        .await.unwrap();
    assert!(row.is_none());
}

fn coordinator() -> std::sync::Arc<PinningCoordinator> {
    let raw: crate::config::Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name="fixture"
        kind="noop"
        priority=1
        max_bytes=1000000
        max_pins=1000
        [pinning_identity]
        primary_storage_domain="hot"
        [[pinning_identity.providers]]
        config_name="fixture"
        provider_id="fixture-provider"
        display_name="Fixture"
        backend="noop"
        scope="isolated"
        storage_domain="remote"
        credential_revision=1
        endpoint_revision=1
        api_profile="noop"
        strategy="cid"
        cleanup="retain"
    "#,
    )
    .unwrap();
    PinningCoordinator::build(ValidatedPinningConfig::from_config(&raw, |_| None).unwrap()).unwrap()
}

#[tokio::test]
async fn registered_historical_prefixes_keep_generic_route_and_owned_startup_compatibility() {
    for last in PREFIXES {
        let (_directory, db) = prefix(last).await;
        assert!(
            admission::domains::unregistered_legacy(&db, "pre-registry-fixture")
                .await
                .unwrap()
        );
        let coordinator = coordinator();
        let provider = &coordinator.effective_config().providers[0];
        // This is explicitly metadata compatibility, not an autocommit barrier.
        ledger::register_route(&db, &provider.name, &provider.identity)
            .await
            .unwrap();
        coordinator
            .register_identities(&store::Store::new(db.clone()))
            .await
            .unwrap();
        let row = pin_provider_route::Entity::find_by_id(provider.name.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(&row.snapshot)
                .unwrap(),
            provider.route_snapshot()
        );
        assert!(!row.retired);
        assert!(
            !admission::domains::unregistered_legacy(&db, "unknown-alias")
                .await
                .unwrap()
        );
        assert_management_absent(&db).await;
        // The caller owns this transaction and its existing usage frontier.
        let tx = db.begin().await.unwrap();
        admission::locks::lock_existing_usage(&tx, std::slice::from_ref(&provider.name))
            .await
            .unwrap();
        admission::lock_open_in_transaction(&tx, &[provider.route_snapshot()])
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        let tx = db.begin().await.unwrap();
        admission::locks::lock_existing_usage(&tx, std::slice::from_ref(&provider.name))
            .await
            .unwrap();
        assert!(
            admission::lock_retirement_write_in_transaction(&tx)
                .await
                .is_err(),
            "native registration compatibility is not retirement-write authority"
        );
        tx.rollback().await.unwrap();
        assert_management_absent(&db).await;
    }
}

#[tokio::test]
async fn old_schema_retired_native_domain_stays_closed_across_restart_and_alias() {
    for last in PREFIXES {
        let (_directory, db) = prefix(last).await;
        let coordinator = coordinator();
        let provider = &coordinator.effective_config().providers[0];
        let mut retired = provider.identity.clone();
        retired.retired = true;
        ledger::register_route(&db, &provider.name, &retired)
            .await
            .unwrap();
        let original = pin_provider_route::Entity::find().all(&db).await.unwrap();
        coordinator
            .register_identities(&store::Store::new(db.clone()))
            .await
            .unwrap();
        let mut alias = provider.identity.clone();
        alias.provider_id = "restart-alias".into();
        ledger::register_route(&db, &alias.allocation_key(), &alias)
            .await
            .unwrap();
        assert_eq!(
            pin_provider_route::Entity::find().all(&db).await.unwrap(),
            original
        );
        assert!(
            matches!(admission::check_open(&db, &[provider.route_snapshot()]).await,
            Err(crate::error::AppError::InvalidPinningRequest(message)) if message == "allocation domain is permanently retired")
        );
        let tx = db.begin().await.unwrap();
        admission::locks::lock_existing_usage(&tx, std::slice::from_ref(&provider.name))
            .await
            .unwrap();
        assert!(
            admission::lock_open_in_transaction(&tx, &[provider.route_snapshot()])
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        // A complete different account capture remains distinct; no alias/TOML
        // interpretation of the closed original account supplies this route.
        let mut distinct = provider.identity.clone();
        distinct.provider_id = "different-account-provider".into();
        distinct.scope = "different-account".into();
        ledger::register_route(&db, &distinct.allocation_key(), &distinct)
            .await
            .unwrap();
        admission::check_open(&db, &[distinct.route_snapshot()])
            .await
            .unwrap();
        assert_management_absent(&db).await;
    }
}

#[tokio::test]
async fn historical_fallback_requires_registered_original_legacy_and_full_declared_tuple() {
    let (_directory, db) = prefix(PREFIXES[0]).await;
    let coordinator = coordinator();
    let provider = &coordinator.effective_config().providers[0];
    let mut legacy = provider.identity.clone();
    legacy.provider_id = "legacy:original-alias".into();
    ledger::register_route(&db, &legacy.allocation_key(), &legacy)
        .await
        .unwrap();
    admission::check_open(&db, &[legacy.route_snapshot()])
        .await
        .unwrap();
    let mut invented = legacy.route_snapshot();
    invented.provider_id = "legacy:unknown-alias".into();
    invented.scope = "invented-old-account".into();
    assert!(admission::check_open(&db, &[invented]).await.is_err());
    assert!(
        admission::check_provider_open(&db, "unknown-alias")
            .await
            .is_err()
    );
    ledger::register_route(&db, &provider.name, &provider.identity)
        .await
        .unwrap();
    let mut mismatched = provider.route_snapshot();
    mismatched.scope = "not-the-declared-tuple".into();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE main.pin_provider_routes SET snapshot=$1 WHERE provider=$2",
        [
            serde_json::to_string(&mismatched).unwrap().into(),
            provider.name.clone().into(),
        ],
    ))
    .await
    .unwrap();
    assert!(
        admission::check_provider_open(&db, &provider.name)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn partial_version_mismatch_and_unknown_registry_are_not_historical_availability() {
    for alteration in [
        schema::records_ddl(DatabaseBackend::Sqlite),
        schema::retirements_ddl(DatabaseBackend::Sqlite),
        format!(
            "{}; {}",
            schema::records_ddl(DatabaseBackend::Sqlite),
            schema::retirements_ddl(DatabaseBackend::Sqlite)
        ),
        format!(
            "INSERT INTO seaql_migrations(version,applied_at) VALUES ('{}',0)",
            schema::VERSION
        ),
        "INSERT INTO seaql_migrations(version,applied_at) VALUES ('unknown_retirement_schema',0)"
            .into(),
        "DELETE FROM seaql_migrations WHERE version='m20250701_000001_init'".into(),
        "ALTER TABLE pin_provider_routes ADD COLUMN unknown_authority TEXT".into(),
    ] {
        let (_directory, db) = prefix(PREFIXES[3]).await;
        let coordinator = coordinator();
        let provider = &coordinator.effective_config().providers[0];
        coordinator
            .register_identities(&store::Store::new(db.clone()))
            .await
            .unwrap();
        let before = pin_provider_route::Entity::find().all(&db).await.unwrap();
        db.execute_unprepared(&alteration).await.unwrap();
        assert!(
            admission::check_open(&db, &[provider.route_snapshot()])
                .await
                .is_err()
        );
        assert!(
            admission::domains::unregistered_legacy(&db, "unknown-alias")
                .await
                .is_err()
        );
        assert!(
            coordinator
                .register_identities(&store::Store::new(db.clone()))
                .await
                .is_err()
        );
        assert_eq!(
            pin_provider_route::Entity::find().all(&db).await.unwrap(),
            before
        );
    }
    // A registered but unsupported intermediate prefix is not one of the four
    // supported full historical catalog endpoints.
    let (_directory, db) = prefix("m20260927_000007_zip_v2_mpu_completion").await;
    let coordinator = coordinator();
    let provider = &coordinator.effective_config().providers[0];
    assert!(
        ledger::register_route(&db, &provider.name, &provider.identity)
            .await
            .is_err()
    );
    assert!(
        pin_provider_route::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn historical_and_current_shadow_catalogs_fail_closed_on_the_same_connection() {
    for last in [PREFIXES[3], schema::VERSION] {
        for shadow in [
            "CREATE TEMP VIEW pin_management_records AS SELECT 1 AS forged",
            "CREATE TEMP VIEW pin_management_retirements AS SELECT 1 AS forged",
            "CREATE TEMP VIEW seaql_migrations AS SELECT * FROM main.seaql_migrations",
            "CREATE TEMP VIEW pin_provider_routes AS SELECT * FROM main.pin_provider_routes",
        ] {
            let (_directory, db) = prefix(last).await;
            let coordinator = coordinator();
            let provider = &coordinator.effective_config().providers[0];
            coordinator
                .register_identities(&store::Store::new(db.clone()))
                .await
                .unwrap();
            // TEMP objects are connection-local; use one owned transaction for
            // creation, fail-fast, and the final admission/startup-lock probes.
            let tx = db.begin().await.unwrap();
            tx.execute_unprepared(shadow).await.unwrap();
            assert!(
                admission::check_open(&tx, &[provider.route_snapshot()])
                    .await
                    .is_err()
            );
            assert!(
                admission::domains::unregistered_legacy(&tx, "unknown-alias")
                    .await
                    .is_err()
            );
            admission::locks::lock_existing_usage(&tx, std::slice::from_ref(&provider.name))
                .await
                .unwrap();
            assert!(
                admission::lock_open_in_transaction(&tx, &[provider.route_snapshot()])
                    .await
                    .is_err()
            );
            assert!(
                admission::lock_registration_in_transaction(&tx, &provider.name)
                    .await
                    .is_err()
            );
            tx.rollback().await.unwrap();
        }
    }
}

#[tokio::test]
async fn historical_absence_is_not_cached_after_normal_upgrade_or_current_catalog_damage() {
    let (_directory, db) = prefix(PREFIXES[3]).await;
    let coordinator = coordinator();
    let provider = &coordinator.effective_config().providers[0];
    coordinator
        .register_identities(&store::Store::new(db.clone()))
        .await
        .unwrap();
    admission::check_open(&db, &[provider.route_snapshot()])
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    admission::check_open(&db, &[provider.route_snapshot()])
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    admission::locks::lock_existing_usage(&tx, std::slice::from_ref(&provider.name))
        .await
        .unwrap();
    admission::lock_retirement_write_in_transaction(&tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    // Missing just one current table cannot reuse the earlier absence decision.
    db.execute_unprepared("DROP TABLE pin_management_retirements")
        .await
        .unwrap();
    assert!(
        admission::check_open(&db, &[provider.route_snapshot()])
            .await
            .is_err()
    );
    assert!(
        admission::domains::unregistered_legacy(&db, "unknown-alias")
            .await
            .is_err()
    );
    assert!(
        coordinator
            .register_identities(&store::Store::new(db.clone()))
            .await
            .is_err()
    );
    let (_directory, db) = prefix(schema::VERSION).await;
    coordinator
        .register_identities(&store::Store::new(db.clone()))
        .await
        .unwrap();
    db.execute_unprepared("ALTER TABLE pin_management_retirements ADD COLUMN unknown_receipt TEXT")
        .await
        .unwrap();
    assert!(
        admission::check_open(&db, &[provider.route_snapshot()])
            .await
            .is_err()
    );
    assert!(
        coordinator
            .register_identities(&store::Store::new(db.clone()))
            .await
            .is_err()
    );
}
