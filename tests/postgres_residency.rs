use std::time::Duration;

use ipfs_s3_gateway::{
    lifecycle::model::{LifecycleScanCursor, LifecycleScanSource},
    store::{
        self,
        entities::{bucket_lifecycle_config, lifecycle_action, object, object_version},
        migrations::*,
    },
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait, QueryOrder,
    Statement, TransactionTrait,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait};

struct PreResidencyMigrator;

impl MigratorTrait for PreResidencyMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250701_000001_init::Migration),
            Box::new(m20260707_000001_decompress_zip::Migration),
            Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m20260729_000001_ipfs3_import::Migration),
            Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m20260730_000001_standard_mutation_fence::Migration),
            Box::new(m20260813_000001_postgres_json_columns::Migration),
            Box::new(m20260825_000001_object_versioning::Migration),
            Box::new(m20260826_000001_lifecycle_expiration::Migration),
            Box::new(m20260831_000001_bucket_cors::Migration),
            Box::new(m20260901_000001_lifecycle_abort_multipart::Migration),
        ]
    }
}

struct SchemaCleanup {
    url: String,
    schema: Option<String>,
}

impl SchemaCleanup {
    fn new(url: String, schema: String) -> Self {
        assert!(valid_schema(&schema));
        Self {
            url,
            schema: Some(schema),
        }
    }

    fn disarm(&mut self) {
        self.schema = None;
    }
}

impl Drop for SchemaCleanup {
    fn drop(&mut self) {
        let Some(schema) = self.schema.take() else {
            return;
        };
        if !valid_schema(&schema) {
            return;
        }
        let url = self.url.clone();
        if let Ok(thread) = std::thread::Builder::new()
            .name("postgres-residency-schema-cleanup".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                runtime.block_on(async move {
                    let mut options = ConnectOptions::new(url);
                    options.max_connections(1).min_connections(1);
                    let Ok(Ok(db)) =
                        tokio::time::timeout(Duration::from_secs(10), Database::connect(options))
                            .await
                    else {
                        return;
                    };
                    let _ = tokio::time::timeout(
                        Duration::from_secs(10),
                        db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE")),
                    )
                    .await;
                    let _ = tokio::time::timeout(Duration::from_secs(10), db.close()).await;
                });
            })
        {
            let _ = thread.join();
        }
    }
}

fn valid_schema(schema: &str) -> bool {
    schema.strip_prefix("residency_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

async fn connection(url: &str, schema: Option<&str>) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url.to_owned());
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options).await.unwrap();
    if let Some(schema) = schema {
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .unwrap();
    }
    db
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_down_waits_for_verification_then_refuses_without_losing_data() {
    use ipfs_s3_gateway::residency::{PhysicalVerification, VersionResidencyIdentity};
    use sea_orm::{DatabaseBackend, QuerySelect};
    use sea_orm_migration::SchemaManager;

    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("IPFS_S3_TEST_POSTGRES_URL must be set for ignored PostgreSQL tests");
    let schema = format!("residency_{}", uuid::Uuid::new_v4().simple());
    let mut cleanup = SchemaCleanup::new(url.clone(), schema.clone());
    let db = connection(&url, None).await;
    db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('object', 'bucket', 'key', 'QmDownRace', 7, 'QmDownRace', TRUE, CURRENT_TIMESTAMP)",
    ).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO object_versions \
         (id, bucket, key, kind, object_id, sequence, is_latest, lifecycle_age_started_at, \
          created_at, updated_at) VALUES \
         ('version', 'bucket', 'key', 'object', 'object', 1, TRUE, CURRENT_TIMESTAMP, \
          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
    let identity = VersionResidencyIdentity::new("version", "object", "QmDownRace");
    let txn = db.begin().await.unwrap();
    store::residency::attach_hot_in_transaction(&txn, &identity, &PhysicalVerification::Pending)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let down_db = connection(&url, Some(&schema)).await;
    let down_pid: i32 = down_db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT pg_backend_pid() AS pid",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "pid")
        .unwrap();
    // Keep verification uncommitted while down starts. The old implementation
    // checked pending state, blocked only at DROP, then lost this receipt.
    let writer = db.begin().await.unwrap();
    store::residency::mark_hot_verified_in_transaction(
        &writer,
        &identity,
        "hot-node",
        "verified-before-down",
    )
    .await
    .unwrap();
    let verified = store::residency::resolve_version_residency(&writer, "version")
        .await
        .unwrap();
    let mut down = tokio::spawn(async move {
        let result = m20260912_000001_residency_references::Migration
            .down(&SchemaManager::new(&down_db))
            .await;
        down_db.close().await.unwrap();
        result
    });
    let blocked_query = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            writer
                .query_one(Statement::from_string(
                    DatabaseBackend::Postgres,
                    "SELECT pg_stat_clear_snapshot()",
                ))
                .await
                .unwrap();
            let row = writer
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT query FROM pg_stat_activity WHERE pid = $1 \
                 AND pg_backend_pid() = ANY(pg_blocking_pids(pid))",
                    [down_pid.into()],
                ))
                .await
                .unwrap();
            if let Some(row) = row {
                break row.try_get::<String>("", "query").unwrap();
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    writer.commit().await.unwrap();
    let result = match tokio::time::timeout(Duration::from_secs(10), &mut down).await {
        Ok(result) => result.unwrap(),
        Err(_) => {
            down.abort();
            let _ = down.await;
            panic!("down must finish after the writer commits");
        }
    };
    let blocked_query = blocked_query.expect("down must block on the existing verification writer");
    assert!(
        result.is_err(),
        "down must reject newly committed verification"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("non-reconstructable")
    );
    assert!(
        blocked_query.starts_with("LOCK TABLE"),
        "down must wait before the state check, not at DROP"
    );
    assert_eq!(
        store::residency::resolve_version_residency(&db, "version")
            .await
            .unwrap(),
        verified
    );
    // Every table remains available after the refused down transaction rolls back.
    assert_eq!(
        store::entities::residency_reference::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store::entities::residency_backfill::Entity::find()
            .limit(1)
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(object_version_snapshot(&db).await.len(), 1);
    db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    cleanup.disarm();
    db.close().await.unwrap();
}

async fn seed_old_lifecycle_state(db: &DatabaseConnection) {
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('lifecycle-tombstone')")
        .await
        .unwrap();
    let cursor = store::lifecycle_scan::encode_cursor(&LifecycleScanCursor {
        source: LifecycleScanSource::Noncurrent,
        bucket: "bucket".to_owned(),
        key: "a".to_owned(),
        sequence: Some(1),
        version_row_id: Some("version-a".to_owned()),
        multipart_created_at: None,
        multipart_upload_id: None,
    });
    db.execute_unprepared(&format!(
        "INSERT INTO bucket_lifecycle_configs \
         (bucket, canonical_json, revision, scan_cursor, scan_lease_epoch, scan_lease_until, \
          created_at, updated_at, last_scanned_at) VALUES \
         ('bucket', '{{\"rules\":[]}}', 7, '{cursor}', 5, '2026-09-10T00:05:00Z', \
          '2026-09-01T00:00:00Z', '2026-09-10T00:00:00Z', '2026-09-09T00:00:00Z'), \
         ('lifecycle-tombstone', NULL, 13, NULL, 0, NULL, \
          '2026-08-01T00:00:00Z', '2026-09-11T00:00:00Z', NULL)"
    ))
    .await
    .unwrap();
    db.execute_unprepared(
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
          next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
          last_error_redacted, created_at, updated_at, finished_at) VALUES \
         ('phase-a-claimed', 'phase-a-claimed-key', 'bucket', 'a', 7, 'id:expire', \
          'expire_noncurrent', 'version', 'version-a', \
          '00000000-0000-4000-8000-000000000001', 'object-a', 1, NULL, NULL, \
          '2026-09-08T00:00:00Z', 'claimed', 3, '2026-09-08T00:01:00Z', 9, \
          '2026-09-08T00:05:00Z', 'worker-a', 'database_contention', \
          'lifecycle action failed', '2026-09-07T00:00:00Z', '2026-09-08T00:00:30Z', NULL), \
         ('phase-a-succeeded', 'phase-a-succeeded-key', 'bucket', 'b', 7, 'id:current', \
          'expire_current', 'version', 'version-b', 'null', 'object-b', 1, NULL, NULL, \
          '2026-09-06T00:00:00Z', 'succeeded', 1, '2026-09-06T00:00:00Z', 2, NULL, NULL, \
          NULL, NULL, '2026-09-05T00:00:00Z', '2026-09-06T00:00:30Z', \
          '2026-09-06T00:00:30Z'), \
         ('phase-a-cancelled', 'phase-a-cancelled-key', 'bucket', 'marker', 7, 'id:marker', \
          'delete_expired_marker', 'version', 'marker', \
          '00000000-0000-4000-8000-000000000003', NULL, 1, NULL, NULL, \
          '2026-09-07T00:00:00Z', 'cancelled', 2, '2026-09-07T00:00:00Z', 3, NULL, NULL, \
          'cancelled_stale', 'lifecycle action failed', '2026-09-04T00:00:00Z', \
          '2026-09-07T00:00:30Z', '2026-09-07T00:00:30Z'), \
         ('phase-b-claimed', 'phase-b-claimed-key', 'bucket', 'multipart', 7, 'id:abort', \
          'abort_incomplete_multipart_upload', 'multipart_upload', NULL, NULL, NULL, NULL, \
          'upload-claimed', '2026-09-01T00:00:00Z', '2026-09-08T00:00:00Z', 'claimed', 4, \
          '2026-09-08T00:02:00Z', 10, '2026-09-08T00:06:00Z', 'worker-b', \
          'internal_dependency', 'lifecycle action failed', '2026-09-01T00:00:00Z', \
          '2026-09-08T00:01:00Z', NULL), \
         ('phase-b-failed-safe', 'phase-b-failed-safe-key', 'bucket', 'multipart', 7, \
          'id:abort', 'abort_incomplete_multipart_upload', 'multipart_upload', NULL, NULL, NULL, \
          NULL, 'upload-failed', '2026-08-01T00:00:00Z', '2026-09-04T00:00:00Z', \
          'failed_safe', 8, '2026-09-04T00:00:00Z', 12, NULL, NULL, 'internal_dependency', \
          'lifecycle action failed', '2026-08-01T00:00:00Z', '2026-09-04T00:01:00Z', \
          '2026-09-04T00:01:00Z')",
    )
    .await
    .unwrap();
}

async fn lifecycle_config_snapshot(db: &DatabaseConnection) -> Vec<bucket_lifecycle_config::Model> {
    bucket_lifecycle_config::Entity::find()
        .order_by_asc(bucket_lifecycle_config::Column::Bucket)
        .all(db)
        .await
        .unwrap()
}

async fn lifecycle_action_snapshot(db: &DatabaseConnection) -> Vec<lifecycle_action::Model> {
    lifecycle_action::Entity::find()
        .order_by_asc(lifecycle_action::Column::Id)
        .all(db)
        .await
        .unwrap()
}

async fn object_snapshot(db: &DatabaseConnection) -> Vec<object::Model> {
    object::Entity::find()
        .order_by_asc(object::Column::Id)
        .all(db)
        .await
        .unwrap()
}

async fn object_version_snapshot(db: &DatabaseConnection) -> Vec<object_version::Model> {
    object_version::Entity::find()
        .order_by_asc(object_version::Column::Id)
        .all(db)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_upgrade_constraints_and_two_connection_startup() {
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("IPFS_S3_TEST_POSTGRES_URL must be set for ignored PostgreSQL tests");
    let schema = format!("residency_{}", uuid::Uuid::new_v4().simple());
    let mut cleanup = SchemaCleanup::new(url.clone(), schema.clone());
    let db = connection(&url, None).await;
    db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    PreResidencyMigrator::up(&db, None).await.unwrap();
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO objects \
         (id, bucket, key, cid, size, etag, encrypted, key_wrap, sse_c_key_fingerprint, \
          multipart, is_latest, created_at) VALUES \
         ('object-a', 'bucket', 'a', 'QmShared', 1, 'etag-a', TRUE, 'wrap-a', \
          'fingerprint-a', FALSE, FALSE, '2026-09-01T00:00:00Z'), \
         ('object-b', 'bucket', 'b', 'QmShared', 1, 'etag-b', FALSE, NULL, NULL, FALSE, TRUE, \
          '2026-09-02T00:00:00Z'), \
         ('orphan', 'bucket', 'orphan', 'QmOrphan', 1, 'etag-orphan', FALSE, NULL, NULL, FALSE, \
          TRUE, '2026-09-03T00:00:00Z')",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
          lifecycle_age_started_at, became_noncurrent_at, created_at, updated_at) \
         VALUES ('version-a', 'bucket', 'a', '00000000-0000-4000-8000-000000000001', \
                  'object', 'object-a', 1, FALSE, '2026-09-01T00:00:00Z', \
                  '2026-09-02T00:00:00Z', '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z'), \
                 ('version-b', 'bucket', 'b', NULL, 'object', 'object-b', 1, TRUE, \
                  '2026-09-02T00:00:00Z', NULL, \
                  '2026-09-02T00:00:00Z', '2026-09-02T00:00:00Z'), \
                ('marker', 'bucket', 'marker', '00000000-0000-4000-8000-000000000003', \
                 'delete_marker', NULL, 1, TRUE, '2026-09-03T00:00:00Z', NULL, \
                 '2026-09-03T00:00:00Z', '2026-09-03T00:00:00Z')",
    )
    .await
    .unwrap();
    seed_old_lifecycle_state(&db).await;
    let configs_before = lifecycle_config_snapshot(&db).await;
    let actions_before = lifecycle_action_snapshot(&db).await;
    let objects_before = object_snapshot(&db).await;
    let versions_before = object_version_snapshot(&db).await;

    let second = connection(&url, Some(&schema)).await;
    let (first_result, second_result) =
        tokio::join!(store::run_migrations(&db), store::run_migrations(&second));
    first_result.unwrap();
    second_result.unwrap();

    assert_eq!(lifecycle_config_snapshot(&db).await, configs_before);
    assert_eq!(lifecycle_action_snapshot(&db).await, actions_before);
    assert_eq!(object_snapshot(&db).await, objects_before);
    assert_eq!(object_version_snapshot(&db).await, versions_before);

    let row = db
        .query_one(Statement::from_string(
            sea_orm::DatabaseBackend::Postgres,
            "SELECT \
                 (SELECT COUNT(*) FROM version_residencies)::bigint AS versions, \
                 (SELECT COUNT(*) FROM physical_residencies)::bigint AS physical, \
                 (SELECT COUNT(*) FROM residency_references)::bigint AS refs, \
                 (SELECT COUNT(*) FROM version_residencies vr \
                  JOIN object_versions ov ON ov.id = vr.version_row_id \
                  JOIN objects o ON o.id = vr.object_id \
                  WHERE ov.id = 'version-a' AND ov.is_latest = FALSE \
                    AND ov.became_noncurrent_at IS NOT NULL AND o.encrypted = TRUE \
                    AND o.key_wrap = 'wrap-a' \
                    AND o.sse_c_key_fingerprint = 'fingerprint-a')::bigint AS legacy",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "versions").unwrap(), 2);
    assert_eq!(row.try_get::<i64>("", "physical").unwrap(), 1);
    assert_eq!(row.try_get::<i64>("", "refs").unwrap(), 2);
    assert_eq!(row.try_get::<i64>("", "legacy").unwrap(), 1);
    assert!(
        db.execute_unprepared(
            "INSERT INTO version_residencies \
             (version_row_id, object_id, primary_tier, storage_class, cid, revision, created_at, updated_at) \
             VALUES ('marker', 'object-a', 'hot', 'STANDARD', 'QmShared', 1, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .is_err()
    );

    let timestamp_types = db
        .query_all(Statement::from_string(
            sea_orm::DatabaseBackend::Postgres,
            "SELECT data_type FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND table_name IN ('version_residencies', 'physical_residencies', \
                                  'residency_references', 'residency_backfill') \
               AND column_name IN ('created_at', 'updated_at', 'verified_at', 'lease_until')",
        ))
        .await
        .unwrap();
    assert!(!timestamp_types.is_empty());
    assert!(timestamp_types.iter().all(|row| {
        row.try_get::<String>("", "data_type").unwrap() == "timestamp with time zone"
    }));

    let txn = db.begin().await.unwrap();
    txn.rollback().await.unwrap();
    second.close().await.unwrap();
    db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    cleanup.disarm();
    db.close().await.unwrap();
}
