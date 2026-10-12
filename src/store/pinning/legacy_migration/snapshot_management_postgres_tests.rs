//! Source-only additions for the parent's existing owned R26 PostgreSQL lane.
use super::{management_catalog_tests::declaration, migration, store};
use crate::store::pinning::{management::schema, quota};
use futures_util::FutureExt;
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection, Statement, TransactionTrait};
use sea_orm_migration::MigratorTrait;
use std::{future::Future, panic::AssertUnwindSafe};

async fn isolated<F, Fut>(endpoint: &str, body: F)
where
    F: FnOnce(DatabaseConnection, DatabaseConnection) -> Fut,
    Fut: Future<Output = ()>,
{
    assert_eq!(std::env::var("IPFS_S3_R26_PG_OWNED").as_deref(),
        Ok("legacy-r26-fixture-only"), "explicit parent-owned fixture marker required");
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("authorized isolated PG URL required");
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("r26_{}", uuid::Uuid::new_v4().simple());
    admin.execute_unprepared(&format!("CREATE SCHEMA {schema}")).await.unwrap();
    let result = AssertUnwindSafe(async {
        let mut connections = vec![];
        for _ in 0..2 {
            let mut options = ConnectOptions::new(&url);
            options.max_connections(1).min_connections(1);
            let db = Database::connect(options).await.unwrap();
            db.execute_unprepared(&format!("SET search_path TO {schema}")).await.unwrap();
            db.execute_unprepared("SET TIME ZONE 'UTC'").await.unwrap();
            db.execute_unprepared("SET statement_timeout='8s'").await.unwrap();
            connections.push(db);
        }
        let registry = store::migrator::Migrator::migrations();
        let count = registry.iter().position(|migration| migration.name() == endpoint).unwrap() + 1;
        store::migrator::Migrator::up(&connections[0], Some(count as u32)).await.unwrap();
        body(connections[0].clone(), connections[1].clone()).await;
        for db in connections {db.close().await.unwrap();}
    }).catch_unwind().await;
    assert!(schema.starts_with("r26_") && schema.len() == 36);
    let owned = admin.query_one(Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres,
        "SELECT 1 FROM pg_namespace WHERE nspname=$1 AND nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)",
        [schema.clone().into()])).await.unwrap();
    assert!(owned.is_some(), "cleanup target no longer owned by this fixture");
    admin.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE")).await.unwrap();
    admin.close().await.unwrap();
    if let Err(error) = result {std::panic::resume_unwind(error);}
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn native_management_record_row_and_empty_retirement_phantom_are_nowait_fenced() {
    isolated(schema::VERSION, |a, b| async move {
        let declaration = declaration(&a).await;
        let plan = migration::inventory(&a, &declaration).await.unwrap();
        assert!(plan.before.iter().find(|table| table.table == schema::RETIREMENTS).unwrap().rows.is_empty());
        for writer_sql in [
            "UPDATE pin_management_records SET binding=binding WHERE kind='database_scope'",
            // Even no rows take the same RowExclusive table lock as a first INSERT.
            "UPDATE pin_management_retirements SET inventory=inventory WHERE 1=0",
        ] {
            let writer = b.begin().await.unwrap();
            writer.execute_unprepared(writer_sql).await.unwrap();
            assert!(migration::apply(&a, &declaration, &plan).await.is_err(), "{writer_sql}");
            writer.rollback().await.unwrap();
            assert_eq!(migration::inventory(&a, &declaration).await.unwrap().before, plan.before);
            assert_eq!(super::super::frontier(&a, &plan.schema).await.unwrap(), plan.receipt_frontier);
            let usage = quota::read_usage(&a, "old").await.unwrap().unwrap();
            assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
        }
        let receipt = migration::apply(&a, &declaration, &plan).await.unwrap();
        assert_eq!(migration::apply(&b, &declaration, &plan).await.unwrap(), receipt);
        migration::rollback(&b, &receipt.id, &plan).await.unwrap();
    }).await;
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn native_forward_guard_fences_retirement_absence_before_route_row_locks() {
    isolated(schema::VERSION, |a, b| async move {
        let declaration = declaration(&a).await;
        let plan = migration::inventory(&a, &declaration).await.unwrap();
        let route_row = plan.before.iter().find(|table| table.table == "pin_provider_routes")
            .unwrap().rows.iter().find(|row| row["provider"].as_deref()
                == Some(declaration.namespaces[0].target.as_str())).unwrap();
        let route: crate::pinning::identity::ProviderRouteSnapshot =
            serde_json::from_str(route_row["snapshot"].as_deref().unwrap()).unwrap();
        assert_eq!(route, declaration.namespaces[0].target_route);
        assert!(plan.before.iter().find(|table| table.table == schema::RETIREMENTS).unwrap().rows.is_empty());
        let writer = b.begin().await.unwrap();
        writer.execute_unprepared("UPDATE pin_management_retirements SET inventory=inventory WHERE 1=0")
            .await.unwrap();
        let caller = a.begin().await.unwrap();
        migration::fence::lock_before_routes(&caller, &plan.schema).await.unwrap();
        let route_row_fence: i64 = caller.query_one(Statement::from_string(
            caller.get_database_backend(),
            "SELECT COUNT(*) AS n FROM pg_locks WHERE pid=pg_backend_pid() AND relation='pin_provider_routes'::regclass AND mode='RowShareLock' AND granted",
        )).await.unwrap().unwrap().try_get("", "n").unwrap();
        assert_eq!(route_row_fence, 0);
        assert!(migration::fence::lock_from_routes(&caller, &plan.schema,
            Some(std::slice::from_ref(&route))).await.is_err());
        caller.rollback().await.unwrap();
        writer.rollback().await.unwrap();
        assert_eq!(super::super::read(&a, &plan.schema).await.unwrap(), plan.before);
        assert_eq!(super::super::frontier(&a, &plan.schema).await.unwrap(), plan.receipt_frontier);
    }).await;
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn native_management_same_count_key_private_text_and_subsecond_time_drift_are_zero_write_rejections() {
    for damage in [
        "UPDATE pin_management_records SET database_scope=database_scope||'-changed' WHERE kind='database_scope'",
        "UPDATE pin_management_records SET binding=binding||' ' WHERE kind='database_scope'",
        "UPDATE pin_management_records SET native_before=native_before||' ' WHERE kind='database_scope'",
        "UPDATE pin_management_records SET created_at=created_at+interval '1 microsecond' WHERE kind='database_scope'",
    ] {
        isolated(schema::VERSION, |a, b| async move {
            let declaration = declaration(&a).await;
            let plan = migration::inventory(&a, &declaration).await.unwrap();
            b.execute_unprepared(damage).await.unwrap();
            let changed = super::super::read(&a, &plan.schema).await.unwrap();
            assert_eq!(changed.iter().find(|table| table.table == schema::RECORDS).unwrap().rows.len(), 1);
            assert_ne!(changed, plan.before);
            assert!(migration::apply(&a, &declaration, &plan).await.is_err(), "{damage}");
            assert_eq!(super::super::read(&a, &plan.schema).await.unwrap(), changed);
            assert_eq!(super::super::frontier(&a, &plan.schema).await.unwrap(), plan.receipt_frontier);
            let usage = quota::read_usage(&a, "old").await.unwrap().unwrap();
            assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
        }).await;
    }
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn native_original_four_prefixes_remain_readable_and_old_writable_prefixes_lock_no_management_table() {
    for endpoint in ["m20261004_000001_rpc_submission_ledger", "m20261004_000002_cluster_replica_contract",
        migration::SCHEMA, "m20261008_000001_zip_intent_capture"] {
        isolated(endpoint, |a, b| async move {
            let declaration = declaration(&a).await;
            let plan = migration::inventory(&a, &declaration).await.unwrap();
            assert_eq!(plan.schema.last().map(String::as_str), Some(endpoint));
            assert!(plan.before.iter().all(|table| !table.table.starts_with("pin_management_")));
            if matches!(endpoint, migration::SCHEMA | "m20261008_000001_zip_intent_capture") {
                let receipt = migration::apply(&a, &declaration, &plan).await.unwrap();
                assert_eq!(migration::apply(&b, &declaration, &plan).await.unwrap(), receipt);
                migration::rollback(&b, &receipt.id, &plan).await.unwrap();
            } else {
                assert!(migration::apply(&a, &declaration, &plan).await.is_err());
                assert_eq!(super::super::read(&a, &plan.schema).await.unwrap(), plan.before);
                store::run_migrations(&a).await.unwrap();
                assert!(migration::apply(&a, &declaration, &plan).await.is_err());
                let fresh = migration::inventory(&a, &declaration).await.unwrap();
                migration::apply(&a, &declaration, &fresh).await.unwrap();
            }
        }).await;
    }
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn native_management_partial_columns_primary_keys_constraints_and_temp_shadow_are_refused() {
    for damage in [
        "ALTER TABLE pin_management_records ADD COLUMN shadow TEXT",
        "ALTER TABLE pin_management_records ALTER COLUMN binding DROP NOT NULL",
        "ALTER TABLE pin_management_records DROP CONSTRAINT ck_management_result",
        "ALTER TABLE pin_management_retirements DROP CONSTRAINT pin_management_retirements_pkey",
        "drop retirement foreign key",
        "DROP TABLE pin_management_retirements",
        "CREATE TEMP TABLE pin_management_records (shadow TEXT)",
        "CREATE TEMP TABLE pin_management_retirements (allocation_domain TEXT)",
    ] {
        isolated(schema::VERSION, |a, _| async move {
            let declaration = declaration(&a).await;
            let plan = migration::inventory(&a, &declaration).await.unwrap();
            if damage == "drop retirement foreign key" {
                let key: String = a.query_one(Statement::from_string(a.get_database_backend(),
                    "SELECT conname::text AS name FROM pg_constraint WHERE conrelid='pin_management_retirements'::regclass AND contype='f'"))
                    .await.unwrap().unwrap().try_get("", "name").unwrap();
                a.execute_unprepared(&format!("ALTER TABLE pin_management_retirements DROP CONSTRAINT \"{}\"", key.replace('"', "\"\""))).await.unwrap();
            } else {
                a.execute_unprepared(damage).await.unwrap();
            }
            assert!(migration::inventory(&a, &declaration).await.is_err(), "{damage}");
            assert!(migration::apply(&a, &declaration, &plan).await.is_err(), "{damage}");
            let usage = quota::read_usage(&a, "old").await.unwrap().unwrap();
            assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
            let count: i64 = a.query_one(Statement::from_string(a.get_database_backend(),
                "SELECT COUNT(*) AS n FROM pin_legacy_migrations")).await.unwrap().unwrap().try_get("", "n").unwrap();
            assert_eq!(count, 0);
        }).await;
    }
}
