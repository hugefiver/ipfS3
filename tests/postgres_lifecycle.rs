use std::{collections::BTreeMap, time::Duration};

use ipfs_s3_gateway::store::{
    entities::lifecycle_action,
    migrations::{
        m20250701_000001_init, m20260707_000001_decompress_zip,
        m20260720_000001_sse_c_key_fingerprint, m20260721_000001_multi_provider_pinning,
        m20260729_000001_ipfs3_import, m20260729_000002_postgres_utc_timestamps,
        m20260730_000001_standard_mutation_fence, m20260813_000001_postgres_json_columns,
        m20260825_000001_object_versioning, m20260826_000001_lifecycle_expiration,
        m20260831_000001_bucket_cors, m20260901_000001_lifecycle_abort_multipart,
    },
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, DbErr,
    EntityTrait, Statement,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait, SchemaManager};

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
    cleanup_guard: OwnedPgSchemaCleanup,
}

impl PgFixture {
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
        cleanup_guard,
    })
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
