use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::{Duration as ChronoDuration, Utc};
use ipfs_s3_gateway::{
    config::{LifecycleWorkerConfig, ValidatedLifecycleConfig},
    error::AppError,
    lifecycle::evaluator::schedule_claimed_scan_page,
    lifecycle::model::{
        AbortIncompleteMultipartUploadAction, CanonicalFilter, CanonicalLifecycleConfiguration,
        CanonicalLifecycleRule, CanonicalRuleSelector, ClaimedLifecycleAction, CurrentExpiration,
        LifecycleActionKind, LifecycleRuleStatus, LifecycleScanCursor, LifecycleScanSource,
        MultipartUploadTargetIdentity, NewLifecycleAction, RuleIdentity, VersionTargetIdentity,
    },
    lifecycle::worker::{
        LifecycleAfterClaimGate, LifecycleWorkerHandle, LifecycleWorkerTestControl, start_worker,
        start_worker_for_test,
    },
    store::{
        self, Store,
        database_clock::database_now,
        entities::{import_destination, lifecycle_action, object, object_version},
        import::ownership::lock_bucket_for_ownership,
        lifecycle_action::{
            claim_due, claim_due_with_max_attempts, idempotency_key, insert_idempotent,
            lock_claim_for_execution, mark_succeeded,
        },
        lifecycle_config::{claim_next_scan, finish_scan_page, put_configuration},
        migrations::{
            m20250701_000001_init, m20260707_000001_decompress_zip,
            m20260720_000001_sse_c_key_fingerprint, m20260721_000001_multi_provider_pinning,
            m20260729_000001_ipfs3_import, m20260729_000002_postgres_utc_timestamps,
            m20260730_000001_standard_mutation_fence, m20260813_000001_postgres_json_columns,
            m20260825_000001_object_versioning, m20260826_000001_lifecycle_expiration,
            m20260831_000001_bucket_cors, m20260901_000001_lifecycle_abort_multipart,
        },
        object_version::{PublicVersionId, VersionKind},
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    DbErr, EntityTrait, QueryFilter, QuerySelect, Statement, TransactionTrait, sea_query::Expr,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait, SchemaManager};
use tokio_util::sync::CancellationToken;

struct PreLifecycleMigrator;

impl MigratorTrait for PreLifecycleMigrator {
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
        ]
    }
}

#[tokio::test]
async fn postgres_lifecycle_abort_migration_preserves_version_identity_and_checks_shapes() {
    let Some(fixture) = phase_a_fixture().await else {
        return;
    };
    seed_phase_a_actions(&fixture.db).await;
    let before = phase_a_action_snapshot(&fixture.db).await;
    m20260901_000001_lifecycle_abort_multipart::Migration
        .up(&SchemaManager::new(&fixture.db))
        .await
        .unwrap();
    assert_eq!(
        phase_a_action_snapshot(&fixture.db).await.as_bytes(),
        before.as_bytes()
    );
    let columns = action_columns(&fixture.db).await;
    for id in ["phase-a-action", "phase-a-terminal"] {
        let row = lifecycle_action::Entity::find_by_id(id)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.target_type, "version");
        assert_eq!(row.target_upload_id, None);
        assert_eq!(row.target_upload_created_at, None);
    }
    assert_eq!(
        columns["target_upload_created_at"].0,
        "timestamp with time zone"
    );
    assert_eq!(columns["target_type"].1, "NO");
    for column in [
        "target_version_row_id",
        "target_public_version_id",
        "target_sequence",
        "target_upload_id",
        "target_upload_created_at",
    ] {
        assert_eq!(columns[column].1, "YES", "{column}");
    }
    let indexes = action_indexes(&fixture.db).await;
    assert!(
        indexes["idx_lifecycle_actions_target"]
            .contains("(bucket, object_key, target_version_row_id)")
    );
    assert!(
        indexes["idx_lifecycle_actions_multipart_target"]
            .contains("(bucket, object_key, target_upload_id, target_upload_created_at)")
    );
    for (assignment, constraint) in [
        (
            "target_upload_id = 'hybrid'",
            "ck_lifecycle_actions_target_shape",
        ),
        (
            "target_upload_created_at = clock_timestamp()",
            "ck_lifecycle_actions_target_shape",
        ),
        (
            "target_version_row_id = NULL",
            "ck_lifecycle_actions_target_shape",
        ),
        (
            "action_kind = 'abort_incomplete_multipart_upload'",
            "ck_lifecycle_actions_kind_target",
        ),
    ] {
        assert_action_check_rejects(&fixture.db, assignment, constraint).await;
    }
    seed_multipart_action(&fixture.db).await;
    for assignment in [
        "target_object_id = 'hybrid'",
        "target_sequence = 1",
        "target_upload_created_at = NULL",
    ] {
        let error = fixture
            .db
            .execute_unprepared(&format!(
                "UPDATE lifecycle_actions SET {assignment} WHERE id = 'multipart-action'"
            ))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ck_lifecycle_actions_target_shape")
        );
    }
    assert_eq!(
        phase_a_action_snapshot(&fixture.db).await.as_bytes(),
        before.as_bytes()
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_abort_down_refuses_abort_state_and_restores_phase_a() {
    let Some(fixture) = phase_a_fixture().await else {
        return;
    };
    seed_phase_a_actions(&fixture.db).await;
    let before = phase_a_action_snapshot(&fixture.db).await;
    let columns = action_columns(&fixture.db).await;
    let checks = action_constraints(&fixture.db).await;
    let indexes = action_indexes(&fixture.db).await;
    let migration = m20260901_000001_lifecycle_abort_multipart::Migration;
    migration
        .up(&SchemaManager::new(&fixture.db))
        .await
        .unwrap();
    seed_multipart_action(&fixture.db).await;
    let polymorphic_columns = action_columns(&fixture.db).await;
    let polymorphic_checks = action_constraints(&fixture.db).await;
    let polymorphic_indexes = action_indexes(&fixture.db).await;
    let error = migration
        .down(&SchemaManager::new(&fixture.db))
        .await
        .unwrap_err();
    assert!(
        matches!(error, DbErr::Migration(ref message) if message == "lifecycle abort schema contains multipart state")
    );
    assert_eq!(action_columns(&fixture.db).await, polymorphic_columns);
    assert_eq!(action_constraints(&fixture.db).await, polymorphic_checks);
    assert_eq!(action_indexes(&fixture.db).await, polymorphic_indexes);
    assert!(
        lifecycle_action::Entity::find_by_id("multipart-action")
            .one(&fixture.db)
            .await
            .unwrap()
            .is_some()
    );
    fixture
        .db
        .execute_unprepared("DELETE FROM lifecycle_actions WHERE id = 'multipart-action'")
        .await
        .unwrap();
    migration
        .down(&SchemaManager::new(&fixture.db))
        .await
        .unwrap();
    assert_eq!(
        action_columns(&fixture.db).await,
        columns,
        "Phase A column types, nullability and defaults"
    );
    assert_eq!(
        action_constraints(&fixture.db).await,
        checks,
        "Phase A checks, primary/unique keys and cascade foreign key"
    );
    assert_eq!(
        action_indexes(&fixture.db).await,
        indexes,
        "Phase A indexes only"
    );
    assert_eq!(
        phase_a_action_snapshot(&fixture.db).await.as_bytes(),
        before.as_bytes()
    );
    assert_action_check_rejects(
        &fixture.db,
        "action_kind = 'abort_incomplete_multipart_upload'",
        "ck_lifecycle_actions_action_kind",
    )
    .await;
    assert_action_check_rejects(
        &fixture.db,
        "target_sequence = -1",
        "ck_lifecycle_actions_target_sequence",
    )
    .await;
    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_abort_multiworker_claim_crash_retry_is_fenced() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    let store_a = Store::new(fixture.independent_connection().await);
    let store_b = Store::new(fixture.independent_connection().await);
    let target = create_due_upload(&fixture.db, "abort-workers").await;
    configure_abort(&fixture.db, &target.bucket, abort_configuration()).await;
    let scan = claim_next_scan(store_a.db(), ChronoDuration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    let (page_a, page_b) = tokio::join!(
        schedule_claimed_scan_page(store_a.db(), &scan, 100),
        schedule_claimed_scan_page(store_b.db(), &scan, 100),
    );
    assert!(page_a.unwrap().cycle_complete);
    assert!(page_b.unwrap().cycle_complete);
    assert!(
        finish_scan_page(store_a.db(), &scan, None, true)
            .await
            .unwrap()
    );
    let rows = abort_action_rows(&fixture.db, &target.bucket).await;
    assert_eq!(
        rows.len(),
        1,
        "concurrent scans must preserve one MPU idempotency key"
    );
    assert_eq!(rows[0].state, "pending");
    assert_eq!(
        rows[0].target_upload_id.as_deref(),
        Some(target.upload_id.as_str())
    );

    let locked = fixture.db.begin().await.unwrap();
    lifecycle_action::Entity::find_by_id(&rows[0].id)
        .lock_exclusive()
        .one(&locked)
        .await
        .unwrap()
        .unwrap();
    let skipped = tokio::time::timeout(
        Duration::from_secs(2),
        claim_due(store_b.db(), "skip-locked", ChronoDuration::seconds(30), 1),
    )
    .await
    .expect("FOR UPDATE SKIP LOCKED must not wait for the locked pending action")
    .unwrap();
    assert!(skipped.is_empty());
    locked.rollback().await.unwrap();

    let gate_a = LifecycleAfterClaimGate::new("mpu-worker-a");
    let worker_a = gated_abort_worker(store_a.clone(), "mpu-worker-a", gate_a.clone());
    let claim_a = wait_abort_claim(&gate_a).await;
    assert!(
        claim_due(
            store_b.db(),
            "second-current-owner",
            ChronoDuration::seconds(30),
            1
        )
        .await
        .unwrap()
        .is_empty()
    );
    let cancelled = worker_a.abort_for_test().await.unwrap_err();
    assert!(cancelled.is_cancelled());
    assert_eq!(
        abort_action_rows(&fixture.db, &target.bucket).await[0],
        claim_a.action
    );
    fixture.db.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "UPDATE lifecycle_actions SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
        [claim_a.action.id.clone().into()],
    )).await.unwrap();

    let gate_b = LifecycleAfterClaimGate::new("mpu-worker-b");
    let worker_b = gated_abort_worker(store_b.clone(), "mpu-worker-b", gate_b.clone());
    let claim_b = wait_abort_claim(&gate_b).await;
    assert_eq!(claim_b.claim_epoch, claim_a.claim_epoch + 1);
    assert_eq!(claim_b.action.attempts, 2);
    assert!(
        lock_claim_for_execution(store_a.db(), &claim_a)
            .await
            .unwrap()
            .is_none(),
        "stale A cannot enter its mutation transaction"
    );
    assert!(
        !mark_succeeded(
            store_a.db(),
            &claim_a,
            database_now(store_a.db()).await.unwrap()
        )
        .await
        .unwrap()
    );
    assert!(
        store::multipart::get_upload(store_a.db(), &target.upload_id)
            .await
            .is_ok()
    );
    gate_b.release();
    let terminal = wait_abort_terminal(&fixture.db, &target.bucket).await;
    worker_b.shutdown(Duration::from_secs(2)).await;
    assert_eq!(terminal.state, "succeeded");
    assert_eq!(terminal.attempts, 2);
    assert!(terminal.finished_at.is_some());
    assert_eq!(terminal.lease_until, None);
    assert_eq!(terminal.claimed_by, None);
    assert!(
        !mark_succeeded(
            store_a.db(),
            &claim_a,
            database_now(store_a.db()).await.unwrap()
        )
        .await
        .unwrap()
    );
    assert_eq!(
        abort_action_rows(&fixture.db, &target.bucket).await,
        vec![terminal]
    );
    assert_upload_absent(&fixture.db, &target).await;
    assert!(
        object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq(&target.bucket))
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
    drop(store_a);
    drop(store_b);
    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_abort_configuration_and_bucket_lock_races_are_atomic() {
    if std::env::var("IPFS_S3_TEST_POSTGRES_URL").is_err() {
        eprintln!("skipping PostgreSQL abort races: IPFS_S3_TEST_POSTGRES_URL is unset");
        return;
    }
    for race in [
        AbortRace::Replace,
        AbortRace::Disable,
        AbortRace::ExplicitAbort,
        AbortRace::DeleteBucket,
        AbortRace::Complete,
        AbortRace::UploadPart,
    ] {
        run_abort_bucket_race(race).await;
    }
    for sqlstate in ["40001", "40P01"] {
        for persistent in [false, true] {
            assert_abort_sqlstate_is_bounded_and_redacted(sqlstate, persistent).await;
        }
    }
}

struct OwnedPgSchemaCleanup {
    url: String,
    schema: Option<String>,
}

impl OwnedPgSchemaCleanup {
    fn new(url: String, schema: String) -> Self {
        assert!(is_owned_lifecycle_schema(&schema));
        Self {
            url,
            schema: Some(schema),
        }
    }

    fn disarm(&mut self) {
        self.schema = None;
    }
}

impl Drop for OwnedPgSchemaCleanup {
    fn drop(&mut self) {
        let Some(schema) = self.schema.take() else {
            return;
        };
        if !is_owned_lifecycle_schema(&schema) {
            return;
        }
        let url = self.url.clone();
        if let Ok(thread) = std::thread::Builder::new()
            .name("postgres-lifecycle-schema-cleanup".to_owned())
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

struct PgFixture {
    db: DatabaseConnection,
    schema: String,
    url: String,
    cleanup_guard: OwnedPgSchemaCleanup,
}

impl PgFixture {
    async fn independent_connection(&self) -> DatabaseConnection {
        let mut options = ConnectOptions::new(self.url.clone());
        options.max_connections(1).min_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared(&format!("SET search_path TO {}", self.schema))
            .await
            .unwrap();
        db
    }

    async fn cleanup(mut self) {
        self.db
            .execute_unprepared(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
        self.cleanup_guard.disarm();
        self.db.close().await.unwrap();
    }
}

fn is_owned_lifecycle_schema(schema: &str) -> bool {
    schema.strip_prefix("lifecycle_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

async fn lifecycle_fixture() -> Option<PgFixture> {
    let fixture = phase_a_fixture().await?;
    store::run_migrations(&fixture.db).await.unwrap();
    Some(fixture)
}

async fn phase_a_fixture() -> Option<PgFixture> {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL lifecycle tests: IPFS_S3_TEST_POSTGRES_URL is unset");
        return None;
    };
    let mut options = ConnectOptions::new(url.clone());
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options).await.unwrap();
    let schema = format!("lifecycle_{}", uuid::Uuid::new_v4().simple());
    let cleanup_guard = OwnedPgSchemaCleanup::new(url.clone(), schema.clone());
    db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    // Legacy rows must exist before versioning backfills the current projection.
    PreLifecycleMigrator::up(&db, Some(8)).await.unwrap();
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('lifecycle-bucket')")
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('legacy-hidden', 'lifecycle-bucket', 'key', 'cid-hidden', 1, 'cid-hidden', FALSE, \
                 '2026-08-25 00:00:00+00:00'), \
                ('current-object', 'lifecycle-bucket', 'key', 'cid-current', 1, 'cid-current', TRUE, \
                 '2026-08-25 00:01:00+00:00')",
    )
    .await
    .unwrap();
    PreLifecycleMigrator::up(&db, None).await.unwrap();
    Some(PgFixture {
        db,
        schema,
        url,
        cleanup_guard,
    })
}

async fn table_exists(db: &DatabaseConnection, table: &str) -> bool {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT 1 FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_name = $1",
        [table.to_owned().into()],
    ))
    .await
    .unwrap()
    .is_some()
}

async fn seed_phase_a_actions(db: &DatabaseConnection) {
    db.execute_unprepared(
        "INSERT INTO lifecycle_actions (id, idempotency_key, bucket, object_key, config_revision, \
         rule_id, action_kind, target_version_row_id, target_public_version_id, target_object_id, \
         target_sequence, due_at, state, attempts, next_attempt_at, claim_epoch, lease_until, \
         claimed_by, failure_class, last_error_redacted, created_at, updated_at, finished_at) \
         VALUES ('phase-a-action', 'phase-a:key|保留', 'lifecycle-bucket', 'key', 7, 'id:expire', \
         'expire_current', 'version-row', 'null', 'current-object', 1, \
         '2026-08-25 01:02:03.123456+00', 'claimed', 3, '2026-08-25 01:02:04+00', 9, \
         '2026-08-25 01:02:05+00', 'worker-a', 'database_contention', 'lifecycle action failed', \
         '2026-08-24 00:00:00+00', '2026-08-25 01:02:06+00', NULL), \
         ('phase-a-terminal', 'terminal:key', 'lifecycle-bucket', 'key', 6, 'ordinal:0', \
         'delete_expired_marker', 'marker-row', 'opaque-version', NULL, 0, \
         '2026-08-25 00:00:00+00', 'succeeded', 2, '2026-08-25 00:00:01+00', 2, NULL, NULL, \
         NULL, NULL, '2026-08-24 00:00:00+00', '2026-08-25 00:00:02+00', '2026-08-25 00:00:02+00')",
    )
    .await
    .unwrap();
}

async fn phase_a_action_snapshot(db: &DatabaseConnection) -> String {
    db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT json_agg(snapshot ORDER BY id)::text AS snapshot FROM (SELECT id, json_build_array(\
         id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
         target_version_row_id, target_public_version_id, target_object_id, target_sequence, \
         due_at, state, attempts, next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
         last_error_redacted, created_at, updated_at, finished_at) AS snapshot \
         FROM lifecycle_actions WHERE id IN ('phase-a-action', 'phase-a-terminal')) AS snapshots",
    )).await.unwrap().unwrap().try_get("", "snapshot").unwrap()
}

async fn action_columns(
    db: &DatabaseConnection,
) -> BTreeMap<String, (String, String, Option<String>)> {
    db.query_all(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'lifecycle_actions'",
    )).await.unwrap().into_iter().map(|row| (
        row.try_get("", "column_name").unwrap(),
        (row.try_get("", "data_type").unwrap(), row.try_get("", "is_nullable").unwrap(), row.try_get("", "column_default").unwrap()),
    )).collect()
}

async fn action_constraints(db: &DatabaseConnection) -> BTreeMap<String, String> {
    db.query_all(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT conname, pg_get_constraintdef(oid) AS definition FROM pg_constraint \
         WHERE conrelid = 'lifecycle_actions'::regclass",
    ))
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.try_get("", "conname").unwrap(),
            row.try_get("", "definition").unwrap(),
        )
    })
    .collect()
}

async fn action_indexes(db: &DatabaseConnection) -> BTreeMap<String, String> {
    db.query_all(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema() \
         AND tablename = 'lifecycle_actions'",
    ))
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.try_get("", "indexname").unwrap(),
            row.try_get("", "indexdef").unwrap(),
        )
    })
    .collect()
}

async fn assert_action_check_rejects(db: &DatabaseConnection, assignment: &str, constraint: &str) {
    let error = db
        .execute_unprepared(&format!(
            "UPDATE lifecycle_actions SET {assignment} WHERE id = 'phase-a-action'"
        ))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(constraint),
        "expected named check {constraint}"
    );
}

async fn seed_multipart_action(db: &DatabaseConnection) {
    db.execute_unprepared(
        "INSERT INTO lifecycle_actions (id, idempotency_key, bucket, object_key, config_revision, \
         rule_id, action_kind, target_type, target_upload_id, target_upload_created_at, due_at, \
         state, next_attempt_at, created_at, updated_at) VALUES ('multipart-action', 'multipart-key', \
         'lifecycle-bucket', 'mpu-key', 7, 'id:abort', 'abort_incomplete_multipart_upload', \
         'multipart_upload', 'missing-upload', '2026-08-01 00:00:00+00', \
         '2026-08-03 00:00:00+00', 'pending', '2026-08-03 00:00:00+00', \
         '2026-08-03 00:00:00+00', '2026-08-03 00:00:00+00')",
    ).await.unwrap();
}

fn abort_configuration() -> CanonicalLifecycleConfiguration {
    CanonicalLifecycleConfiguration {
        schema_version: 1,
        rules: vec![CanonicalLifecycleRule {
            id: Some("abort-mpu".to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector: CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::All,
            },
            expiration: None,
            noncurrent_version_expiration: None,
            abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUploadAction {
                days_after_initiation: 1,
            }),
        }],
    }
}

async fn configure_abort(
    db: &DatabaseConnection,
    bucket: &str,
    configuration: CanonicalLifecycleConfiguration,
) {
    let json = ipfs_s3_gateway::lifecycle::config::canonical_json(&configuration).unwrap();
    put_configuration(db, bucket, &json).await.unwrap();
}

async fn create_due_upload(db: &DatabaseConnection, bucket: &str) -> MultipartUploadTargetIdentity {
    store::bucket::create(db, bucket, None).await.unwrap();
    let upload_id = uuid::Uuid::new_v4().to_string();
    store::multipart::create_upload(
        db,
        &upload_id,
        "unpublished-object",
        bucket,
        "mpu-key",
        "none",
        None,
        None,
        None,
        None,
        &[],
        None,
        false,
    )
    .await
    .unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "UPDATE multipart_uploads SET created_at = clock_timestamp() - INTERVAL '3 days' WHERE upload_id = $1",
        [upload_id.clone().into()],
    )).await.unwrap();
    let upload = store::multipart::get_upload(db, &upload_id).await.unwrap();
    let target = MultipartUploadTargetIdentity {
        bucket: bucket.to_owned(),
        key: upload.key,
        upload_id,
        initiated_at: upload.created_at,
    };
    store::multipart::upsert_part_for_active_upload(db, &target, 1, "part-cid", 7, "part-cid")
        .await
        .unwrap();
    assert_eq!(
        store::multipart::get_upload(db, &target.upload_id)
            .await
            .unwrap()
            .created_at,
        target.initiated_at
    );
    target
}

async fn abort_action_rows(db: &DatabaseConnection, bucket: &str) -> Vec<lifecycle_action::Model> {
    lifecycle_action::Entity::find()
        .filter(lifecycle_action::Column::Bucket.eq(bucket))
        .all(db)
        .await
        .unwrap()
}

fn pg_abort_worker_config() -> ValidatedLifecycleConfig {
    LifecycleWorkerConfig {
        poll_interval_ms: 10,
        scan_page_size: 100,
        scan_lease_secs: 30,
        action_lease_secs: 30,
        worker_concurrency: 1,
        max_attempts: 2,
        base_backoff_secs: 1,
        max_backoff_secs: 1,
    }
    .validate()
    .unwrap()
}

fn gated_abort_worker(
    store: Store,
    id: &str,
    gate: Arc<LifecycleAfterClaimGate>,
) -> LifecycleWorkerHandle {
    start_worker_for_test(
        store,
        pg_abort_worker_config(),
        CancellationToken::new(),
        LifecycleWorkerTestControl {
            worker_id: id.to_owned(),
            after_claim: Some(gate),
        },
    )
}

async fn wait_abort_claim(gate: &LifecycleAfterClaimGate) -> ClaimedLifecycleAction {
    tokio::time::timeout(Duration::from_secs(15), gate.wait_claim())
        .await
        .expect("MPU action must be scanned and claimed within 15 seconds")
}

async fn wait_abort_terminal(db: &DatabaseConnection, bucket: &str) -> lifecycle_action::Model {
    let mut last = Vec::new();
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            last = abort_action_rows(db, bucket).await;
            assert!(last.len() <= 1, "duplicate MPU actions");
            if let Some(row) = last.first()
                && matches!(
                    row.state.as_str(),
                    "succeeded" | "cancelled" | "failed_safe"
                )
            {
                return row.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    result.unwrap_or_else(|_| {
        panic!(
            "MPU terminal wait elapsed {:?}; observed {:?}",
            started.elapsed(),
            last.iter()
                .map(|row| (&row.state, row.attempts, row.claim_epoch))
                .collect::<Vec<_>>()
        )
    })
}

async fn assert_upload_absent(db: &DatabaseConnection, target: &MultipartUploadTargetIdentity) {
    assert!(matches!(
        store::multipart::get_upload(db, &target.upload_id).await,
        Err(AppError::NoSuchUpload(_))
    ));
    assert!(
        store::multipart::list_parts(db, &target.upload_id)
            .await
            .unwrap()
            .is_empty(),
        "parts must cascade, never resurrect"
    );
}

#[derive(Clone, Copy, Debug)]
enum AbortRace {
    Replace,
    Disable,
    ExplicitAbort,
    DeleteBucket,
    Complete,
    UploadPart,
}

async fn backend_pid(db: &DatabaseConnection) -> i32 {
    db.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT pg_backend_pid() AS pid",
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "pid")
    .unwrap()
}

async fn wait_for_pg_lock(db: &DatabaseConnection, pid: i32, blocker: i32) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = db
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT $2 = ANY(pg_blocking_pids($1)) AS blocked",
                    [pid.into(), blocker.into()],
                ))
                .await
                .unwrap()
                .unwrap();
            if row.try_get::<bool>("", "blocked").unwrap() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("contender must demonstrably wait on the owned bucket lock within 10 seconds");
}

async fn race_mutation(
    db: &DatabaseConnection,
    target: &MultipartUploadTargetIdentity,
    race: AbortRace,
) -> bool {
    match race {
        AbortRace::Replace | AbortRace::Disable => {
            let mut configuration = abort_configuration();
            if matches!(race, AbortRace::Disable) {
                configuration.rules[0].status = LifecycleRuleStatus::Disabled;
            } else {
                configuration.rules[0].selector = CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::Prefix {
                        prefix: "not-this-upload/".to_owned(),
                    },
                };
            }
            configure_abort(db, &target.bucket, configuration).await;
            true
        }
        AbortRace::ExplicitAbort => {
            let txn = db.begin().await.unwrap();
            lock_bucket_for_ownership(&txn, &target.bucket)
                .await
                .unwrap();
            let result =
                store::multipart::abort_exact_incomplete_upload_in_transaction(&txn, target)
                    .await
                    .unwrap();
            assert!(matches!(
                result,
                store::multipart::AbortExactIncompleteUploadResult::Applied
                    | store::multipart::AbortExactIncompleteUploadResult::AlreadySatisfied
            ));
            txn.commit().await.unwrap();
            result == store::multipart::AbortExactIncompleteUploadResult::Applied
        }
        AbortRace::DeleteBucket => match store::bucket::delete(db, &target.bucket).await {
            Ok(()) => true,
            Err(AppError::BucketNotEmpty(_)) => false,
            Err(error) => panic!("unexpected bucket-delete race classification: {error:?}"),
        },
        AbortRace::Complete => {
            use ipfs_s3_gateway::{
                pinning::policy::PublicationPolicy,
                store::pinning::publication::{
                    PinTargetSpec, PublicationObject, PublicationRequest, publish_completed_upload,
                },
            };
            let mut object = PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                &target.bucket,
                &target.key,
                "completed-cid".to_owned(),
                7,
                None,
                None,
                false,
                None,
                None,
                database_now(db).await.unwrap(),
            );
            object.multipart = true;
            let request = PublicationRequest {
                object,
                tags: vec![],
                policy: PublicationPolicy {
                    tags: vec![],
                    leases: vec![],
                },
                object_target: PinTargetSpec {
                    cid: "completed-cid".to_owned(),
                    logical_size: 7,
                },
            };
            match publish_completed_upload(db, target, request, &Default::default()).await {
                Ok(_) => true,
                Err(store::multipart::CommitCompletedUploadError::RolledBack {
                    source: AppError::NoSuchUpload(_),
                    ..
                }) => false,
                Err(error) => panic!("unexpected Complete publication race: {error:?}"),
            }
        }
        AbortRace::UploadPart => match store::multipart::upsert_part_for_active_upload(
            db,
            target,
            2,
            "late-part-cid",
            4,
            "late-part-cid",
        )
        .await
        {
            Ok(()) => true,
            Err(AppError::NoSuchUpload(_)) => false,
            Err(error) => panic!("unexpected UploadPart race: {error:?}"),
        },
    }
}

async fn run_abort_bucket_race(race: AbortRace) {
    let fixture = lifecycle_fixture().await.unwrap();
    let target = create_due_upload(&fixture.db, "bucket-lock-race").await;
    configure_abort(&fixture.db, &target.bucket, abort_configuration()).await;
    let worker_store = Store::new(fixture.independent_connection().await);
    let worker_pid = backend_pid(worker_store.db()).await;
    let contender = fixture.independent_connection().await;
    let contender_pid = backend_pid(&contender).await;
    let holder = fixture.independent_connection().await;
    let holder_pid = backend_pid(&holder).await;
    let gate = LifecycleAfterClaimGate::new("bucket-race-worker");
    let worker = gated_abort_worker(worker_store.clone(), "bucket-race-worker", gate.clone());
    let claim = wait_abort_claim(&gate).await;
    let held = holder.begin().await.unwrap();
    lock_bucket_for_ownership(&held, &target.bucket)
        .await
        .unwrap();
    let mutation_target = target.clone();
    let mutation =
        tokio::spawn(async move { race_mutation(&contender, &mutation_target, race).await });
    wait_for_pg_lock(&fixture.db, contender_pid, holder_pid).await;
    gate.release();
    // The second waiter can be queued behind the first waiter rather than the lock holder.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = fixture
                .db
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                    [worker_pid.into()],
                ))
                .await
                .unwrap()
                .unwrap();
            if row.try_get::<bool>("", "blocked").unwrap() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("lifecycle execution must contend for the same bucket lock");
    assert_eq!(
        abort_action_rows(&fixture.db, &target.bucket).await[0].state,
        "claimed"
    );
    assert!(
        store::multipart::get_upload(&fixture.db, &target.upload_id)
            .await
            .is_ok()
    );
    held.commit().await.unwrap();
    let mutation_won = tokio::time::timeout(Duration::from_secs(15), mutation)
        .await
        .expect("bucket mutation must settle within 15 seconds")
        .unwrap();
    if matches!(race, AbortRace::DeleteBucket) && mutation_won {
        worker.shutdown(Duration::from_secs(2)).await;
        assert!(
            !store::bucket::exists(&fixture.db, &target.bucket)
                .await
                .unwrap()
        );
        assert!(
            abort_action_rows(&fixture.db, &target.bucket)
                .await
                .is_empty(),
            "bucket deletion owns the audit cascade"
        );
        assert_upload_absent(&fixture.db, &target).await;
    } else {
        let terminal = wait_abort_terminal(&fixture.db, &target.bucket).await;
        worker.shutdown(Duration::from_secs(2)).await;
        assert_eq!(terminal.id, claim.action.id);
        assert_eq!(terminal.claim_epoch, claim.claim_epoch);
        assert_eq!(terminal.attempts, 1);
        assert!(terminal.finished_at.is_some());
        assert_eq!(terminal.claimed_by, None);
        assert_eq!(terminal.lease_until, None);
        match race {
            AbortRace::Replace | AbortRace::Disable => {
                assert!(matches!(terminal.state.as_str(), "cancelled" | "succeeded"));
                if terminal.state == "cancelled" {
                    assert_eq!(
                        store::multipart::get_upload(&fixture.db, &target.upload_id)
                            .await
                            .unwrap()
                            .created_at,
                        target.initiated_at
                    );
                    assert_eq!(
                        store::multipart::list_parts(&fixture.db, &target.upload_id)
                            .await
                            .unwrap()
                            .len(),
                        1
                    );
                } else {
                    assert_upload_absent(&fixture.db, &target).await;
                }
            }
            _ => {
                assert_eq!(terminal.state, "succeeded", "{race:?}");
                assert_upload_absent(&fixture.db, &target).await;
            }
        }
        let versions = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq(&target.bucket))
            .all(&fixture.db)
            .await
            .unwrap();
        let objects = object::Entity::find()
            .filter(object::Column::Bucket.eq(&target.bucket))
            .all(&fixture.db)
            .await
            .unwrap();
        if matches!(race, AbortRace::Complete) && mutation_won {
            assert_eq!(versions.len(), 1);
            assert_eq!(objects.len(), 1);
            assert_eq!(objects[0].cid, "completed-cid");
            assert_eq!(
                versions[0].object_id.as_deref(),
                Some(objects[0].id.as_str())
            );
        } else {
            assert!(versions.is_empty());
            assert!(objects.is_empty());
        }
        if matches!(race, AbortRace::UploadPart) {
            assert!(matches!(
                store::multipart::upsert_part_for_active_upload(
                    &fixture.db,
                    &target,
                    3,
                    "retry-cid",
                    1,
                    "retry-cid"
                )
                .await,
                Err(AppError::NoSuchUpload(_))
            ));
        }
        if matches!(race, AbortRace::DeleteBucket) {
            store::bucket::delete(&fixture.db, &target.bucket)
                .await
                .unwrap();
            assert!(
                !store::bucket::exists(&fixture.db, &target.bucket)
                    .await
                    .unwrap()
            );
        }
    }
    drop(worker_store);
    holder.close().await.unwrap();
    fixture.cleanup().await;
}

async fn assert_abort_sqlstate_is_bounded_and_redacted(sqlstate: &str, persistent: bool) {
    assert!(matches!(sqlstate, "40001" | "40P01"));
    let fixture = lifecycle_fixture().await.unwrap();
    assert!(is_owned_lifecycle_schema(&fixture.schema));
    let target = create_due_upload(&fixture.db, "abort-retry").await;
    configure_abort(&fixture.db, &target.bucket, abort_configuration()).await;
    // A sequence survives rollback, so retries cannot erase the injection count.
    fixture
        .db
        .execute_unprepared("CREATE SEQUENCE abort_fault_count")
        .await
        .unwrap();
    fixture
        .db
        .execute_unprepared(
            "CREATE FUNCTION inject_abort_fault() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN IF nextval('abort_fault_count') = 1 OR TG_ARGV[1] = 'persistent' THEN \
         RAISE EXCEPTION USING ERRCODE = TG_ARGV[0], \
         MESSAGE = CASE WHEN TG_ARGV[0] = '40001' \
         THEN 'could not serialize access due to concurrent update; PRIVATE_SQL_PASSWORD_SENTINEL' \
         ELSE 'deadlock detected; PRIVATE_SQL_PASSWORD_SENTINEL' END; \
         END IF; RETURN OLD; END $$",
        )
        .await
        .unwrap();
    let mode = if persistent { "persistent" } else { "once" };
    fixture
        .db
        .execute_unprepared(&format!(
            "CREATE TRIGGER owned_abort_fault AFTER DELETE ON multipart_uploads FOR EACH ROW \
         EXECUTE FUNCTION inject_abort_fault('{sqlstate}', '{mode}')"
        ))
        .await
        .unwrap();
    let worker_store = Store::new(fixture.independent_connection().await);
    let worker = start_worker(
        worker_store.clone(),
        pg_abort_worker_config(),
        CancellationToken::new(),
    );
    let terminal = wait_abort_terminal(&fixture.db, &target.bucket).await;
    worker.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        terminal.attempts, 2,
        "SQLSTATE {sqlstate} must take one bounded retry"
    );
    assert_eq!(terminal.claim_epoch, 2);
    let calls: i64 = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT last_value FROM abort_fault_count",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "last_value")
        .unwrap();
    assert_eq!(calls, 2, "no retries beyond the configured bound");
    assert_eq!(terminal.claimed_by, None);
    assert_eq!(terminal.lease_until, None);
    assert!(terminal.finished_at.is_some());
    if persistent {
        assert_eq!(terminal.state, "failed_safe");
        assert_eq!(
            terminal.failure_class.as_deref(),
            Some("database_contention")
        );
        assert_eq!(
            terminal.last_error_redacted.as_deref(),
            Some("lifecycle action failed")
        );
        assert!(
            store::multipart::get_upload(&fixture.db, &target.upload_id)
                .await
                .is_ok()
        );
        assert_eq!(
            store::multipart::list_parts(&fixture.db, &target.upload_id)
                .await
                .unwrap()
                .len(),
            1
        );
    } else {
        assert_eq!(terminal.state, "succeeded");
        assert_upload_absent(&fixture.db, &target).await;
    }
    let diagnostics = format!(
        "{:?} {:?}",
        terminal.failure_class, terminal.last_error_redacted
    );
    for private in [
        "PRIVATE_SQL_PASSWORD_SENTINEL",
        "inject_abort_fault",
        target.upload_id.as_str(),
        "DELETE",
        "40001",
        "40P01",
    ] {
        assert!(
            !diagnostics.contains(private),
            "diagnostics must redact injected database details"
        );
    }
    assert!(
        claim_due_with_max_attempts(
            &fixture.db,
            "after-terminal",
            ChronoDuration::seconds(30),
            2,
            1
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        abort_action_rows(&fixture.db, &target.bucket).await,
        vec![terminal]
    );
    drop(worker_store);
    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_migration_and_database_clock_are_engine_owned() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    let before = Utc::now();
    let now = database_now(&fixture.db).await.unwrap();
    let after = Utc::now();

    assert!(
        now >= before - chrono::Duration::seconds(1) && now <= after + chrono::Duration::seconds(1),
        "PostgreSQL clock_timestamp() must be current"
    );
    assert!(table_exists(&fixture.db, "bucket_lifecycle_configs").await);
    assert!(table_exists(&fixture.db, "lifecycle_actions").await);
    let row = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT lifecycle_age_started_at, became_noncurrent_at FROM object_versions",
        ))
        .await
        .unwrap()
        .unwrap();
    let age: chrono::DateTime<chrono::Utc> = row.try_get("", "lifecycle_age_started_at").unwrap();
    let became_noncurrent: Option<chrono::DateTime<chrono::Utc>> =
        row.try_get("", "became_noncurrent_at").unwrap();
    assert_eq!(
        age,
        chrono::DateTime::parse_from_rfc3339("2026-08-25T00:01:00Z")
            .unwrap()
            .with_timezone(&Utc)
    );
    assert_eq!(became_noncurrent, None);

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_down_refuses_durable_configuration() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    fixture
        .db
        .execute_unprepared(
            "INSERT INTO bucket_lifecycle_configs \
             (bucket, canonical_json, revision, scan_lease_epoch, created_at, updated_at) \
             VALUES ('lifecycle-bucket', NULL, 1, 0, clock_timestamp(), clock_timestamp())",
        )
        .await
        .unwrap();

    let result = m20260826_000001_lifecycle_expiration::Migration
        .down(&SchemaManager::new(&fixture.db))
        .await;
    assert!(matches!(result, Err(DbErr::Migration(_))));
    assert!(table_exists(&fixture.db, "bucket_lifecycle_configs").await);

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_claim_uses_database_clock_locks_and_epoch_fences() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    let canonical = r#"{"schema_version":1,"rules":[]}"#;
    put_configuration(&fixture.db, "lifecycle-bucket", canonical)
        .await
        .unwrap();

    let first_scan = claim_next_scan(&fixture.db, ChronoDuration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert!(
        claim_next_scan(&fixture.db, ChronoDuration::seconds(30))
            .await
            .unwrap()
            .is_none()
    );
    fixture
        .db
        .execute_unprepared(
            "UPDATE bucket_lifecycle_configs \
             SET scan_lease_until = clock_timestamp() - INTERVAL '1 second' \
             WHERE bucket = 'lifecycle-bucket'",
        )
        .await
        .unwrap();
    let replacement_scan = claim_next_scan(&fixture.db, ChronoDuration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replacement_scan.lease_epoch, first_scan.lease_epoch + 1);
    let cursor = LifecycleScanCursor {
        source: LifecycleScanSource::Current,
        bucket: "lifecycle-bucket".to_owned(),
        key: "key".to_owned(),
        sequence: Some(1),
        version_row_id: Some("row-1".to_owned()),
        multipart_created_at: None,
        multipart_upload_id: None,
    };
    assert!(
        !finish_scan_page(&fixture.db, &first_scan, Some(&cursor), false)
            .await
            .unwrap()
    );
    assert!(
        finish_scan_page(&fixture.db, &replacement_scan, Some(&cursor), false)
            .await
            .unwrap()
    );

    let now = database_now(&fixture.db).await.unwrap();
    let action = NewLifecycleAction {
        idempotency_key: String::new(),
        bucket: "lifecycle-bucket".to_owned(),
        config_revision: replacement_scan.config_revision,
        rule_identity: RuleIdentity::Ordinal(0),
        action_kind: LifecycleActionKind::ExpireCurrent,
        target: ipfs_s3_gateway::lifecycle::model::LifecycleTargetIdentity::Version(
            VersionTargetIdentity {
                bucket: "lifecycle-bucket".to_owned(),
                key: "key".to_owned(),
                version_row_id: "row-1".to_owned(),
                public_version_id: PublicVersionId::Opaque(
                    "00000000-0000-4000-8000-000000000001".to_owned(),
                ),
                kind: VersionKind::Object,
                object_id: Some("current-object".to_owned()),
                sequence: 1,
            },
        ),
        due_at: now - ChronoDuration::seconds(1),
    };
    assert!(
        insert_idempotent(&fixture.db, action.clone(), now)
            .await
            .unwrap()
    );
    assert!(!insert_idempotent(&fixture.db, action, now).await.unwrap());
    let first_action = claim_due(&fixture.db, "pg-worker-a", ChronoDuration::seconds(30), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(
        lock_claim_for_execution(&fixture.db, &first_action)
            .await
            .unwrap()
            .is_some()
    );
    fixture
        .db
        .execute_unprepared(&format!(
            "UPDATE lifecycle_actions \
             SET lease_until = clock_timestamp() - INTERVAL '1 second' \
             WHERE id = '{}'",
            first_action.action.id
        ))
        .await
        .unwrap();
    let replacement_action = claim_due(&fixture.db, "pg-worker-b", ChronoDuration::seconds(30), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(replacement_action.claim_epoch, first_action.claim_epoch + 1);
    assert!(
        !mark_succeeded(
            &fixture.db,
            &first_action,
            database_now(&fixture.db).await.unwrap()
        )
        .await
        .unwrap()
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_final_recovery_crash_is_bounded_and_preserves_foreign_tokens() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    let version = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("lifecycle-bucket"))
        .filter(object_version::Column::Key.eq("key"))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let configured_max_attempts = 2_i64;
    let terminal_attempt = configured_max_attempts + 1;

    for token_case in ["owned", "user"] {
        let now = database_now(&fixture.db).await.unwrap();
        let mut pending = NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: "lifecycle-bucket".to_owned(),
            config_revision: 1,
            rule_identity: RuleIdentity::Id(format!("terminal-{token_case}")),
            action_kind: LifecycleActionKind::ExpireCurrent,
            target: ipfs_s3_gateway::lifecycle::model::LifecycleTargetIdentity::Version(
                VersionTargetIdentity {
                    bucket: "lifecycle-bucket".to_owned(),
                    key: "key".to_owned(),
                    version_row_id: version.id.clone(),
                    public_version_id: PublicVersionId::Null,
                    kind: VersionKind::Object,
                    object_id: version.object_id.clone(),
                    sequence: version.sequence,
                },
            ),
            due_at: now - ChronoDuration::seconds(1),
        };
        pending.idempotency_key = idempotency_key(&pending).unwrap();
        let key = pending.idempotency_key.clone();
        assert!(insert_idempotent(&fixture.db, pending, now).await.unwrap());
        let action = lifecycle_action::Entity::find()
            .filter(lifecycle_action::Column::IdempotencyKey.eq(key))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        lifecycle_action::Entity::update_many()
            .col_expr(lifecycle_action::Column::State, Expr::value("claimed"))
            .col_expr(
                lifecycle_action::Column::Attempts,
                Expr::value(terminal_attempt),
            )
            .col_expr(
                lifecycle_action::Column::ClaimEpoch,
                Expr::value(terminal_attempt),
            )
            .col_expr(
                lifecycle_action::Column::ClaimedBy,
                Expr::value(Some("crashed-final-worker".to_owned())),
            )
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(now - ChronoDuration::seconds(1))),
            )
            .filter(lifecycle_action::Column::Id.eq(&action.id))
            .exec(&fixture.db)
            .await
            .unwrap();
        let token = if token_case == "owned" {
            format!("lifecycle:{}:{terminal_attempt}", action.id)
        } else {
            "user-mutation-token".to_owned()
        };
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "INSERT INTO import_destinations \
                 (bucket, key, generation, owner_job_id, mutation_id, mutation_prefix, updated_at) \
                 VALUES ($1, $2, 1, NULL, $3, NULL, clock_timestamp()) \
                 ON CONFLICT (bucket, key) DO UPDATE SET mutation_id = EXCLUDED.mutation_id, \
                 mutation_prefix = NULL, updated_at = clock_timestamp()",
                [
                    "lifecycle-bucket".into(),
                    "key".into(),
                    token.clone().into(),
                ],
            ))
            .await
            .unwrap();

        assert!(
            claim_due_with_max_attempts(
                &fixture.db,
                "must-not-reclaim",
                ChronoDuration::seconds(30),
                configured_max_attempts,
                1,
            )
            .await
            .unwrap()
            .is_empty()
        );
        let terminal = lifecycle_action::Entity::find_by_id(&action.id)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.state, "failed_safe");
        assert_eq!(terminal.attempts, terminal_attempt);
        let destination = import_destination::Entity::find_by_id((
            "lifecycle-bucket".to_owned(),
            "key".to_owned(),
        ))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
        if token_case == "owned" {
            assert_eq!(destination.mutation_id, None);
        } else {
            assert_eq!(destination.mutation_id.as_deref(), Some(token.as_str()));
        }
    }

    fixture.cleanup().await;
}

fn abort_reclaim_worker_config() -> ValidatedLifecycleConfig {
    LifecycleWorkerConfig {
        poll_interval_ms: 1,
        scan_page_size: 1,
        scan_lease_secs: 30,
        action_lease_secs: 1,
        worker_concurrency: 1,
        max_attempts: 8,
        base_backoff_secs: 1,
        max_backoff_secs: 60,
    }
    .validate()
    .unwrap()
}

fn abort_reclaim_configuration() -> CanonicalLifecycleConfiguration {
    CanonicalLifecycleConfiguration {
        schema_version: 1,
        rules: vec![CanonicalLifecycleRule {
            id: Some("abort-reclaim".to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector: CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::All,
            },
            expiration: Some(CurrentExpiration::Date {
                utc_midnight: chrono::DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
            }),
            noncurrent_version_expiration: None,
            abort_incomplete_multipart_upload: None,
        }],
    }
}

#[tokio::test]
async fn postgres_lifecycle_worker_abort_reclaims_and_fences_stale_epoch() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    let store_a = Store::new(fixture.independent_connection().await);
    let store_b = Store::new(fixture.independent_connection().await);
    let configuration =
        ipfs_s3_gateway::lifecycle::config::canonical_json(&abort_reclaim_configuration()).unwrap();
    let revision = put_configuration(store_a.db(), "lifecycle-bucket", &configuration)
        .await
        .unwrap();
    let target_version = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("lifecycle-bucket"))
        .filter(object_version::Column::ObjectId.eq("current-object"))
        .one(store_a.db())
        .await
        .unwrap()
        .unwrap();
    assert!(target_version.is_latest);
    assert_eq!(target_version.version_id, None);
    assert_eq!(target_version.object_id.as_deref(), Some("current-object"));

    let due_at = chrono::DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let now = database_now(store_a.db()).await.unwrap();
    let mut pending = NewLifecycleAction {
        idempotency_key: String::new(),
        bucket: "lifecycle-bucket".to_owned(),
        config_revision: revision,
        rule_identity: RuleIdentity::Id("abort-reclaim".to_owned()),
        action_kind: LifecycleActionKind::ExpireCurrent,
        target: ipfs_s3_gateway::lifecycle::model::LifecycleTargetIdentity::Version(
            VersionTargetIdentity {
                bucket: "lifecycle-bucket".to_owned(),
                key: "key".to_owned(),
                version_row_id: target_version.id.clone(),
                public_version_id: PublicVersionId::Null,
                kind: VersionKind::Object,
                object_id: Some("current-object".to_owned()),
                sequence: target_version.sequence,
            },
        ),
        due_at,
    };
    pending.idempotency_key = idempotency_key(&pending).unwrap();
    let action_idempotency_key = pending.idempotency_key.clone();
    assert!(insert_idempotent(store_a.db(), pending, now).await.unwrap());
    let seeded = lifecycle_action::Entity::find()
        .filter(lifecycle_action::Column::IdempotencyKey.eq(action_idempotency_key))
        .one(store_a.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seeded.state, "pending");

    let start_a = Arc::new(tokio::sync::Barrier::new(2));
    let gate_a = ipfs_s3_gateway::lifecycle::worker::LifecycleAfterClaimGate::new("worker-a");
    let worker_a_store = store_a.clone();
    let worker_a_gate = gate_a.clone();
    let worker_a_start = start_a.clone();
    let worker_a = tokio::spawn(async move {
        worker_a_start.wait().await;
        ipfs_s3_gateway::lifecycle::worker::start_worker_for_test(
            worker_a_store,
            abort_reclaim_worker_config(),
            CancellationToken::new(),
            ipfs_s3_gateway::lifecycle::worker::LifecycleWorkerTestControl {
                worker_id: "worker-a".to_owned(),
                after_claim: Some(worker_a_gate),
            },
        )
    });
    start_a.wait().await;
    let worker_a = tokio::time::timeout(Duration::from_secs(15), worker_a)
        .await
        .expect("worker A must start within 15 seconds")
        .unwrap();
    let claim_a = tokio::time::timeout(Duration::from_secs(15), gate_a.wait_claim())
        .await
        .expect("worker A must claim the pending action within 15 seconds");
    let lease_until = claim_a.action.lease_until.expect("worker A claim lease");
    let claimed_before_abort = lifecycle_action::Entity::find_by_id(claim_a.action.id.clone())
        .one(store_a.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed_before_abort.state, "claimed");
    assert_eq!(claimed_before_abort.claimed_by.as_deref(), Some("worker-a"));
    assert_eq!(claimed_before_abort.claim_epoch, claim_a.claim_epoch);

    let aborted_a = worker_a.abort_for_test();
    let cancelled = tokio::time::timeout(Duration::from_secs(15), aborted_a)
        .await
        .expect("worker A abort join must settle within 15 seconds")
        .expect_err("aborted worker A join must be cancelled");
    assert!(cancelled.is_cancelled());
    let claimed_after_abort = lifecycle_action::Entity::find_by_id(claim_a.action.id.clone())
        .one(store_a.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed_after_abort.state, "claimed");
    assert_eq!(claimed_after_abort.claimed_by.as_deref(), Some("worker-a"));
    assert_eq!(claimed_after_abort.claim_epoch, claim_a.claim_epoch);
    assert_eq!(claimed_after_abort.lease_until, Some(lease_until));

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if database_now(store_b.db()).await.unwrap() > lease_until {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("database clock must pass worker A lease within 15 seconds");

    let gate_b = ipfs_s3_gateway::lifecycle::worker::LifecycleAfterClaimGate::new("worker-b");
    let worker_b = ipfs_s3_gateway::lifecycle::worker::start_worker_for_test(
        store_b.clone(),
        abort_reclaim_worker_config(),
        CancellationToken::new(),
        ipfs_s3_gateway::lifecycle::worker::LifecycleWorkerTestControl {
            worker_id: "worker-b".to_owned(),
            after_claim: Some(gate_b.clone()),
        },
    );
    let claim_b = tokio::time::timeout(Duration::from_secs(15), gate_b.wait_claim())
        .await
        .expect("worker B must reclaim the expired action within 15 seconds");
    assert_eq!(claim_b.worker_id, "worker-b");
    assert_eq!(claim_b.claim_epoch, claim_a.claim_epoch + 1);
    let claimed_by_b = lifecycle_action::Entity::find_by_id(claim_b.action.id.clone())
        .one(store_b.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed_by_b.claimed_by.as_deref(), Some("worker-b"));
    assert_eq!(claimed_by_b.claim_epoch, claim_b.claim_epoch);

    let terminal_notification = Arc::new(tokio::sync::Notify::new());
    let terminal_observer = {
        let db = store_b.db().clone();
        let action_id = claim_b.action.id.clone();
        let terminal_notification = terminal_notification.clone();
        tokio::spawn(async move {
            loop {
                let row = lifecycle_action::Entity::find_by_id(action_id.clone())
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap();
                if row.state == "succeeded" {
                    terminal_notification.notify_one();
                    return row;
                }
                tokio::task::yield_now().await;
            }
        })
    };
    let terminal_arrived = terminal_notification.notified();
    gate_b.release();
    tokio::time::timeout(Duration::from_secs(15), terminal_arrived)
        .await
        .expect("worker B must complete exactly one terminal action within 15 seconds");
    let terminal = terminal_observer.await.unwrap();
    worker_b.shutdown(Duration::from_secs(1)).await;

    let terminal_rows = lifecycle_action::Entity::find()
        .all(store_b.db())
        .await
        .unwrap();
    assert_eq!(terminal_rows.len(), 1);
    assert_eq!(terminal.state, "succeeded");
    assert_eq!(terminal.claimed_by, None);
    assert_eq!(terminal.lease_until, None);
    let exact_version_deleted = object_version::Entity::find_by_id(target_version.id.clone())
        .one(store_b.db())
        .await
        .unwrap()
        .is_none();
    let target_object_projection = object::Entity::find_by_id("current-object")
        .one(store_b.db())
        .await
        .unwrap();
    assert!(
        exact_version_deleted,
        "worker B must delete the exact due version"
    );
    assert!(
        target_object_projection
            .as_ref()
            .is_none_or(|projection| !projection.is_latest),
        "worker B must remove the exact current object projection"
    );

    let stale_terminal_write = mark_succeeded(
        store_a.db(),
        &claim_a,
        database_now(store_a.db()).await.unwrap(),
    )
    .await
    .unwrap();
    assert!(
        !stale_terminal_write,
        "stale terminal CAS must affect zero rows"
    );
    let terminal_after_stale_write =
        lifecycle_action::Entity::find_by_id(claim_a.action.id.clone())
            .one(store_a.db())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(terminal_after_stale_write.state, "succeeded");
    assert_eq!(terminal_after_stale_write.claimed_by, None);
    assert_eq!(terminal_after_stale_write.lease_until, None);
    assert_eq!(terminal_after_stale_write, terminal);
    assert!(
        object_version::Entity::find_by_id(target_version.id)
            .one(store_a.db())
            .await
            .unwrap()
            .is_none(),
        "stale completion must not restore the deleted version"
    );

    drop(store_b);
    drop(store_a);
    fixture.cleanup().await;
}
