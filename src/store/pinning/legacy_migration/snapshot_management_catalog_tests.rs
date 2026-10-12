use super::{migration, native_catalog, store};
use crate::store::pinning::{ledger, management::schema as management_schema, quota};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};

const ENDPOINTS: [&str; 4] = [
    "m20261004_000001_rpc_submission_ledger",
    "m20261004_000002_cluster_replica_contract",
    migration::SCHEMA,
    "m20261008_000001_zip_intent_capture",
];

pub(super) async fn declaration(db: &DatabaseConnection) -> migration::Declaration {
    let identity = crate::pinning::identity::ProviderIdentity {
        provider_id: "r26-account".into(),
        display_name: "R26 fixture".into(),
        backend: "noop".into(),
        scope: "account".into(),
        storage_domain: "fixture".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        retired: false,
        cleanup: crate::pinning::identity::CleanupMode::Retain,
    };
    ledger::register_route(db, &identity.allocation_key(), &identity)
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    serde_json::from_value(serde_json::json!({
        "version": 1, "database_scope": "fixture", "ttl_seconds": 300,
        "maintenance": {"all_writers_stopped": true, "all_workers_drained": true},
        "namespaces": [{"source": "old", "target": identity.allocation_key(),
            "backend": "noop", "scope": "account", "resource_type": identity.resource_type(),
            "binding_only": false, "target_route": identity.route_snapshot(), "bindings": [{
                "table": "pin_provider_usage", "key": {"provider": "old"},
                "provider": "old", "historical_route": null
            }]}]
    }))
    .unwrap()
}

pub(super) async fn receipt_rows(db: &DatabaseConnection) -> Vec<migration::NativeRow> {
    db.query_all(Statement::from_string(
        db.get_database_backend(),
        "SELECT operation_id,CAST(ordinal AS TEXT) AS ordinal,table_kind,original_key,before_image,after_image,bindings FROM pin_legacy_migration_items ORDER BY operation_id,ordinal",
    ))
    .await
    .unwrap()
    .iter()
    .map(|row| {
        ["operation_id", "ordinal", "table_kind", "original_key", "before_image", "after_image", "bindings"]
            .into_iter()
            .map(|column| (column.into(), row.try_get("", column).unwrap()))
            .collect()
    })
    .collect()
}

#[tokio::test]
async fn all_four_historical_endpoints_keep_absent_management_and_original_authority() {
    for endpoint in ENDPOINTS {
        let db = native_catalog(endpoint).await;
        let declaration = declaration(&db).await;
        let plan = migration::inventory(&db, &declaration).await.unwrap();
        assert_eq!(plan.schema.last().map(String::as_str), Some(endpoint));
        assert!(plan.before.iter().all(|table| !table.table.starts_with("pin_management_")));
        assert!(!migration::source_is_migrated(&db, "old").await.unwrap());
        assert!(migration::resolve_legacy_binding(
            &db, "pin_provider_usage", &[("provider".into(), "old".into())].into(), "old",
        ).await.unwrap().is_none());
        for table in ["pin_replica_observations", "pin_lease_requirements"] {
            assert_eq!(plan.before.iter().find(|image| image.table == table).unwrap().columns.is_empty(), endpoint == ENDPOINTS[0]);
        }
        if matches!(endpoint, migration::SCHEMA | "m20261008_000001_zip_intent_capture") {
            let receipt = migration::apply(&db, &declaration, &plan).await.unwrap();
            assert_eq!(migration::apply(&db, &declaration, &plan).await.unwrap(), receipt);
            let reverse = migration::rollback(&db, &receipt.id, &plan).await.unwrap();
            assert_eq!(migration::rollback(&db, &receipt.id, &plan).await.unwrap(), reverse);
        } else {
            assert!(migration::apply(&db, &declaration, &plan).await.is_err());
            assert_eq!(super::super::read(&db, &plan.schema).await.unwrap(), plan.before);
            store::run_migrations(&db).await.unwrap();
            assert!(migration::apply(&db, &declaration, &plan).await.is_err());
            let fresh = migration::inventory(&db, &declaration).await.unwrap();
            migration::apply(&db, &declaration, &fresh).await.unwrap();
        }
        assert_eq!(quota::read_usage(&db, "old").await.unwrap().map(|row| (row.reserved_bytes, row.reserved_pins)),
            if matches!(endpoint, migration::SCHEMA | "m20261008_000001_zip_intent_capture") {Some((97, 3))} else {None});
    }
}

#[tokio::test]
async fn old_committed_receipt_replays_after_upgrade_without_new_table_items_or_history_changes() {
    for endpoint in &ENDPOINTS[2..] {
        let db = native_catalog(endpoint).await;
        let declaration = declaration(&db).await;
        let plan = migration::inventory(&db, &declaration).await.unwrap();
        let receipt = migration::apply(&db, &declaration, &plan).await.unwrap();
        let items = receipt_rows(&db).await;
        let original = super::super::super::receipt::lookup(&db, &receipt.id).await.unwrap().unwrap();
        assert!(items.iter().all(|row| !row["table_kind"].as_deref().unwrap().starts_with("pin_management_")));
        store::run_migrations(&db).await.unwrap();
        assert_eq!(migration::apply(&db, &declaration, &plan).await.unwrap(), receipt);
        assert_eq!(receipt_rows(&db).await, items);
        assert_eq!(super::super::super::receipt::lookup(&db, &receipt.id).await.unwrap().unwrap(), original);
        let resolved = migration::resolve_legacy_binding(&db, "pin_provider_usage",
            &[("provider".into(), "old".into())].into(), "old").await.unwrap().unwrap();
        assert_eq!(resolved.operational_key, declaration.namespaces[0].target);
        assert!(migration::rollback(&db, &receipt.id, &plan).await.is_err());
        let mut rollback = declaration.clone();
        rollback.rollback_receipt = Some(receipt.id.clone());
        assert!(migration::rollback_inventory(&db, &receipt.id, &rollback).await.is_err());
        assert_eq!(receipt_rows(&db).await, items);
    }
}

#[tokio::test]
async fn historical_prefix_rejects_unregistered_management_tables_without_repair() {
    for endpoint in ENDPOINTS {
        let db = native_catalog(endpoint).await;
        crate::store::migrations::m20261012_000001_pinning_management::Migration
            .up(&SchemaManager::new(&db)).await.unwrap();
        assert!(super::super::schema(&db, false).await.is_err());
        let registered: i64 = db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM seaql_migrations WHERE version='m20261012_000001_pinning_management'"))
            .await.unwrap().unwrap().try_get("", "n").unwrap();
        assert_eq!(registered, 0);
    }
}

#[tokio::test]
async fn management_endpoint_keeps_r26_budget_limits_and_accepts_more_than_stage7_native_rows() {
    let db = native_catalog(management_schema::VERSION).await;
    let declaration = declaration(&db).await;
    db.execute_unprepared(
        "WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<4200) INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) SELECT 'unrelated-'||n,0,0 FROM numbers",
    ).await.unwrap();
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    assert_eq!(migration::admission_schema(&db).await.unwrap(), plan.schema);
    assert!(plan.before.iter().map(|table| table.rows.len()).sum::<usize>() > 4096);
    assert_eq!(migration::MAX_ROWS, 10_000);
    assert_eq!(migration::MAX_BYTES, 8 * 1024 * 1024);
}

#[tokio::test]
async fn registered_management_requires_exact_columns_keys_constraints_and_no_shadow() {
    for damage in [
        "DROP TABLE pin_management_retirements",
        "ALTER TABLE pin_management_records ADD COLUMN shadow TEXT",
        "ALTER TABLE pin_management_records DROP COLUMN native_before",
        "CREATE TEMP TABLE pin_management_records (shadow TEXT)",
        "CREATE TEMP VIEW pin_management_retirements AS SELECT 'shadow' AS allocation_domain",
        "CREATE TEMP TABLE seaql_migrations (version TEXT NOT NULL PRIMARY KEY)",
        "CREATE UNIQUE INDEX shadow_management_key ON pin_management_records(plan_id,item_id,kind)",
        "UPDATE seaql_migrations SET version='m20261012_000002_unknown' WHERE version='m20261012_000001_pinning_management'",
        "DELETE FROM seaql_migrations WHERE version='m20261004_000002_cluster_replica_contract'",
    ] {
        let db = native_catalog(management_schema::VERSION).await;
        db.execute_unprepared(damage).await.unwrap();
        assert!(super::super::schema(&db, false).await.is_err(), "{damage}");
    }
    for replacement in [
        management_schema::records_ddl(DatabaseBackend::Sqlite).replace(
            "PRIMARY KEY (database_scope,plan_id,item_id,kind)",
            "PRIMARY KEY (database_scope,plan_id,item_id)",
        ),
        management_schema::records_ddl(DatabaseBackend::Sqlite).replace(
            "binding TEXT NOT NULL", "binding TEXT",
        ),
        management_schema::records_ddl(DatabaseBackend::Sqlite).replace(
            "kind IN ('database_scope','evidence_preparation','action')", "length(kind)>0",
        ),
    ] {
        let db = native_catalog(management_schema::VERSION).await;
        db.execute_unprepared("DROP TABLE pin_management_retirements").await.unwrap();
        db.execute_unprepared("DROP TABLE pin_management_records").await.unwrap();
        db.execute_unprepared(&replacement).await.unwrap();
        db.execute_unprepared(&management_schema::retirements_ddl(DatabaseBackend::Sqlite)).await.unwrap();
        assert!(super::super::schema(&db, false).await.is_err());
    }
}

#[test]
fn prefix_specific_fence_order_keeps_retirement_between_usage_and_routes_and_records_in_tail() {
    for endpoint in ENDPOINTS.into_iter().chain([management_schema::VERSION]) {
        let versions = vec![endpoint.into()];
        let order = super::super::super::fence::lock_order(&versions);
        let names = order.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>();
        let mut first = vec!["buckets", "objects", "object_versions", "pin_leases", "pin_lease_targets", "remote_pins", "pin_provider_usage"];
        if endpoint == management_schema::VERSION {first.push(management_schema::RETIREMENTS);}
        first.push("pin_provider_routes");
        assert_eq!(&names[..first.len()], first);
        assert_eq!(order[4].1, "created_at,id");
        assert_eq!(names.iter().any(|name| *name == management_schema::RECORDS), endpoint == management_schema::VERSION);
        assert_eq!(names.iter().any(|name| *name == management_schema::RETIREMENTS), endpoint == management_schema::VERSION);
        if endpoint == management_schema::VERSION {
            assert!(names.iter().position(|name| *name == management_schema::RECORDS).unwrap() > names.iter().position(|name| *name == "pin_jobs").unwrap());
        }
    }
}

#[tokio::test]
async fn admission_schema_returns_exact_prefixes_and_does_not_cache_historical_absence() {
    for endpoint in ENDPOINTS.into_iter().chain([management_schema::VERSION]) {
        let db = native_catalog(endpoint).await;
        let versions = migration::admission_schema(&db).await.unwrap();
        assert_eq!(versions, super::super::schema(&db, false).await.unwrap());
        assert_eq!(versions.last().map(String::as_str), Some(endpoint));
        if endpoint != management_schema::VERSION {
            assert!(db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
                "SELECT 1 FROM main.sqlite_schema WHERE name IN ('pin_management_records','pin_management_retirements') LIMIT 1"))
                .await.unwrap().is_none());
        }
    }
    let db = native_catalog(ENDPOINTS[3]).await;
    let old = migration::admission_schema(&db).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let current = migration::admission_schema(&db).await.unwrap();
    assert_eq!(&current[..old.len()], old.as_slice());
    assert_eq!(current.last().map(String::as_str), Some(management_schema::VERSION));
    db.execute_unprepared("DROP TABLE pin_management_retirements").await.unwrap();
    assert!(migration::admission_schema(&db).await.is_err());
}

#[tokio::test]
async fn admission_schema_rejects_migration_and_route_shadows_and_native_route_damage() {
    for endpoint in [ENDPOINTS[0], management_schema::VERSION] {
        for damage in [
            "CREATE TEMP VIEW seaql_migrations AS SELECT * FROM main.seaql_migrations",
            "CREATE TEMP VIEW pin_provider_routes AS SELECT * FROM main.pin_provider_routes",
            "CREATE TEMP TABLE pin_provider_routes (shadow TEXT)",
            "ALTER TABLE pin_provider_routes ADD COLUMN unknown_authority TEXT",
            "DROP TABLE pin_provider_routes",
            "DROP TABLE pin_provider_routes; CREATE VIEW pin_provider_routes AS SELECT '' AS provider,'' AS snapshot,'' AS display_name,0 AS retired",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL,snapshot TEXT NOT NULL,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL)",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL PRIMARY KEY,snapshot BLOB NOT NULL,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL)",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL PRIMARY KEY,snapshot TEXT NOT NULL,display_name TEXT NOT NULL,retired INTEGER NOT NULL)",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL PRIMARY KEY,snapshot TEXT,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL)",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL,snapshot TEXT NOT NULL,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL,PRIMARY KEY(provider,display_name))",
        ] {
            let db = native_catalog(endpoint).await;
            let original = migration::admission_schema(&db).await.unwrap();
            let tx = db.begin().await.unwrap();
            tx.execute_unprepared(damage).await.unwrap();
            assert!(migration::admission_schema(&tx).await.is_err(), "{endpoint}: {damage}");
            tx.rollback().await.unwrap();
            assert_eq!(migration::admission_schema(&db).await.unwrap(), original);
        }
    }
}
