#[path = "support/stage7_management.rs"]
pub mod support;
use ipfs_s3_gateway::{
    pinning::management::db_snapshot,
    store::{
        self,
        pinning::management::{native, schema},
    },
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};

#[tokio::test]
async fn native_catalog_rejects_type_pk_constraint_and_shadow_changes_without_repairs() {
    for fault in ["type", "lease-time", "key", "immutable", "shadow"] {
        let raw = support::config("http://127.0.0.1:1");
        let config = support::management_config(&raw);
        let (_directory, db) = support::database().await;
        support::setup(&db, &config).await;
        let baseline = native::capture(&db).await.unwrap();
        let lease_time = baseline
            .table("standard_mutation_leases")
            .unwrap()
            .definitions
            .iter()
            .find(|column| column.name == "lease_until")
            .unwrap();
        assert_eq!(lease_time.sqlite_type, "TEXT");
        assert_eq!(lease_time.postgres_type, "timestamp with time zone");
        assert!(!lease_time.nullable);
        match fault {
            "type" => {
                db.execute_unprepared("ALTER TABLE pin_provider_usage RENAME TO old_usage")
                    .await
                    .unwrap();
                db.execute_unprepared("CREATE TABLE pin_provider_usage (provider TEXT PRIMARY KEY NOT NULL,reserved_bytes TEXT NOT NULL,reserved_pins BIGINT NOT NULL,observed_bytes BIGINT,observed_pins BIGINT,observed_at timestamp_with_timezone_text)").await.unwrap();
            }
            "lease-time" => {
                db.execute_unprepared(
                    "ALTER TABLE standard_mutation_leases RENAME TO old_mutation_leases",
                )
                .await
                .unwrap();
                db.execute_unprepared("CREATE TABLE standard_mutation_leases (bucket TEXT NOT NULL,key TEXT NOT NULL,mutation_id TEXT NOT NULL,generation BIGINT NOT NULL,lease_until TIMESTAMP NOT NULL,PRIMARY KEY(bucket,key),FOREIGN KEY(bucket,key) REFERENCES import_destinations(bucket,key) ON DELETE CASCADE)").await.unwrap();
            }
            "key" => {
                db.execute_unprepared("ALTER TABLE pin_invocation_routes RENAME TO old_routes")
                    .await
                    .unwrap();
                db.execute_unprepared("CREATE TABLE pin_invocation_routes (job_id TEXT NOT NULL,route TEXT NOT NULL,remote_epoch BIGINT NOT NULL)").await.unwrap();
            }
            "immutable" => {
                db.execute_unprepared("DROP TRIGGER pin_lease_requirements_no_update")
                    .await
                    .unwrap();
            }
            "shadow" => {
                let tx = db.begin().await.unwrap();
                tx.execute_unprepared("CREATE TEMP TABLE objects (id TEXT)")
                    .await
                    .unwrap();
                assert!(native::capture(&tx).await.is_err());
                tx.rollback().await.unwrap();
                continue;
            }
            _ => unreachable!(),
        }
        assert!(native::capture(&db).await.is_err(), "{fault}");
    }
}

#[tokio::test]
async fn global_inventory_byte_and_row_limits_include_unselected_records() {
    let raw = support::config("http://127.0.0.1:1");
    let config = support::management_config(&raw);
    let (_directory, db) = support::database().await;
    support::setup(&db, &config).await;
    let identity = support::publish(&db, &config, "selected", false).await;
    db.execute_unprepared("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<4096) INSERT INTO pin_invocation_routes(job_id,route,remote_epoch) SELECT 'unselected-'||i,'{}',1 FROM n").await.unwrap();
    assert_eq!(
        native::capture(&db).await.err(),
        Some(ipfs_s3_gateway::store::pinning::management::ManagementError::Budget)
    );
    db.execute_unprepared("DELETE FROM pin_invocation_routes")
        .await
        .unwrap();
    let text = "é".repeat(2 * 1024 * 1024 + 1);
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE objects SET metadata=$1 WHERE id=$2",
        [text.into(), identity.object_id.clone().into()],
    ))
    .await
    .unwrap();
    assert_eq!(
        db_snapshot::collect_with_config(&db, &config, &support::request(identity, &config, false))
            .await
            .err(),
        Some(db_snapshot::SnapshotError::LimitExceeded)
    );
}

#[tokio::test]
async fn formal_up_down_preserves_raw_business_fields_and_refuses_debt_or_receipts() {
    let raw = support::config("http://127.0.0.1:1");
    let config = support::management_config(&raw);
    let (_directory, db) = support::database().await;
    support::setup(&db, &config).await;
    let identity = support::publish(&db, &config, "raw", false).await;
    db.execute_unprepared("UPDATE objects SET created_at='2026-10-12T00:00:00.000000001Z',metadata='{\"raw\":\"保留\"}'").await.unwrap();
    let fields = |frame: &native::NativeFrame| {
        (
            frame
                .row("objects", &[&identity.object_id])
                .unwrap()
                .clone(),
            frame
                .original_value("objects", &[&identity.object_id], "metadata")
                .unwrap()
                .to_owned(),
        )
    };
    let before = fields(&native::capture(&db).await.unwrap());
    let migration = store::migrations::m20261012_000001_pinning_management::Migration;
    migration.down(&SchemaManager::new(&db)).await.unwrap();
    migration.up(&SchemaManager::new(&db)).await.unwrap();
    assert!(schema::validate(&db).await.is_ok());
    assert!(before == fields(&native::capture(&db).await.unwrap()));
    db.execute_unprepared("INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES('unknown-debt',1,1)").await.unwrap();
    assert!(migration.down(&SchemaManager::new(&db)).await.is_err());
    assert!(before == fields(&native::capture(&db).await.unwrap()));
    db.execute_unprepared("DELETE FROM pin_provider_usage WHERE provider='unknown-debt'")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO pin_management_records(database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) SELECT database_scope,'receipt','item','action','local_configuration','test','{}','{}','{}','[]','[]','applied',created_at,'2099-01-01T00:00:00Z',created_at FROM pin_management_records WHERE kind='database_scope'").await.unwrap();
    assert!(migration.down(&SchemaManager::new(&db)).await.is_err());
    assert!(before == fields(&native::capture(&db).await.unwrap()));
}
