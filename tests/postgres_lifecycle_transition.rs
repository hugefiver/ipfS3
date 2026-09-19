use std::time::Duration;

use ipfs_s3_gateway::store::{self, migrations::*};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, Statement, TransactionTrait,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait, SchemaManager};

struct PreTransitionMigrator;

impl MigratorTrait for PreTransitionMigrator {
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
            Box::new(m20260912_000001_residency_references::Migration),
        ]
    }
}

async fn connect(url: &str) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url.to_owned());
    options.max_connections(1).min_connections(1);
    tokio::time::timeout(Duration::from_secs(10), Database::connect(options))
        .await
        .expect("PostgreSQL connection timed out")
        .unwrap()
}

fn postgres_url() -> String {
    std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("NOT RUN: set IPFS_S3_TEST_POSTGRES_URL and invoke this ignored test explicitly")
}

async fn in_schema(db: &DatabaseConnection, schema: &str) {
    assert!(
        schema
            .strip_prefix("lifecycle_transition_")
            .is_some_and(|suffix| {
                suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
    );
    db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
}

async fn seed_ab(db: &DatabaseConnection) {
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, target_upload_id, target_upload_created_at, due_at, state, attempts, \
          next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
          last_error_redacted, created_at, updated_at, finished_at) VALUES \
         ('a', 'a-key', 'bucket', 'key', 2, 'id:a', 'expire_current', 'version', \
          'row', 'public', 'object', 1, NULL, NULL, clock_timestamp(), 'claimed', 3, \
          clock_timestamp(), 5, clock_timestamp() + interval '1 minute', 'worker', \
          'database_contention', 'lifecycle action failed', clock_timestamp(), \
          clock_timestamp(), NULL), \
         ('b', 'b-key', 'bucket', 'upload', 2, 'id:b', \
          'abort_incomplete_multipart_upload', 'multipart_upload', NULL, NULL, NULL, NULL, \
          'upload', clock_timestamp(), clock_timestamp(), 'failed_safe', 8, clock_timestamp(), \
          9, NULL, NULL, 'internal_dependency', 'lifecycle action failed', clock_timestamp(), \
          clock_timestamp(), clock_timestamp())",
    )
    .await
    .unwrap();
}

async fn snapshot(db: &DatabaseConnection) -> Vec<u8> {
    db.query_all(Statement::from_string(
        sea_orm::DatabaseBackend::Postgres,
        "SELECT row_to_json(a)::text AS snapshot \
         FROM lifecycle_actions AS a ORDER BY id",
    ))
    .await
    .unwrap()
    .into_iter()
    .flat_map(|row| {
        let mut bytes = row.try_get::<String>("", "snapshot").unwrap().into_bytes();
        bytes.push(b'\n');
        bytes
    })
    .collect()
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires explicit IPFS_S3_TEST_POSTGRES_URL endpoint"]
async fn postgres_upgrade_from_thirteen_preserves_ab_rows_and_updates_checks_in_place() {
    let url = postgres_url();
    let db = connect(&url).await;
    let schema = format!("lifecycle_transition_{}", uuid::Uuid::new_v4().simple());
    in_schema(&db, &schema).await;
    PreTransitionMigrator::up(&db, None).await.unwrap();
    seed_ab(&db).await;
    let before = snapshot(&db).await;

    m20260912_000002_lifecycle_transition::Migration
        .up(&SchemaManager::new(&db))
        .await
        .unwrap();
    assert_eq!(snapshot(&db).await, before);
    db.execute_unprepared(
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, due_at, state, next_attempt_at, created_at, updated_at) VALUES \
         ('transition', 'transition-key', 'bucket', 'key', 2, 'id:t', 'transition_current', \
          'version', 'row', 'public', 'object', 1, clock_timestamp(), 'pending', \
          clock_timestamp(), clock_timestamp(), clock_timestamp())",
    )
    .await
    .unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE lifecycle_actions SET target_object_id = NULL WHERE id = 'transition'"
        )
        .await
        .is_err()
    );
    db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires explicit IPFS_S3_TEST_POSTGRES_URL endpoint"]
async fn postgres_fresh_schema_uses_timestamptz_and_creates_transition_foundation() {
    let url = postgres_url();
    let db = connect(&url).await;
    let schema = format!("lifecycle_transition_{}", uuid::Uuid::new_v4().simple());
    in_schema(&db, &schema).await;
    store::run_migrations(&db).await.unwrap();
    for statement in [
        "INSERT INTO buckets (name) VALUES ('bucket')",
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('object', 'bucket', 'key', 'QmPgTransition', 7, 'QmPgTransition', TRUE, \
                 clock_timestamp())",
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
          lifecycle_age_started_at, created_at, updated_at) VALUES \
         ('version', 'bucket', 'key', 'public', 'object', 'object', 1, TRUE, \
          clock_timestamp(), clock_timestamp(), clock_timestamp())",
        "INSERT INTO physical_residencies \
         (tier, cid, node_identity, verification_state, verification_receipt, verified_at, \
          created_at, updated_at) VALUES \
         ('hot', 'QmPgTransition', 'hot-node', 'verified', 'hot-receipt', clock_timestamp(), \
          clock_timestamp(), clock_timestamp())",
        "INSERT INTO version_residencies \
         (version_row_id, object_id, primary_tier, storage_class, cid, revision, \
          created_at, updated_at) VALUES \
         ('version', 'object', 'hot', 'STANDARD', 'QmPgTransition', 1, \
          clock_timestamp(), clock_timestamp())",
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, due_at, state, next_attempt_at, created_at, updated_at) VALUES \
         ('transition', 'transition-key', 'bucket', 'key', 1, 'id:t', 'transition_noncurrent', \
          'version', 'version', 'public', 'object', 1, clock_timestamp(), 'pending', \
          clock_timestamp(), clock_timestamp(), clock_timestamp())",
        "INSERT INTO lifecycle_transitions \
         (id, action_id, action_kind, bucket, object_key, config_revision, rule_id, \
          target_version_row_id, target_public_version_id, target_object_id, target_sequence, \
          source_tier, destination_tier, source_cid, destination_cid, \
          source_residency_revision, expected_source_node_identity, \
          expected_destination_node_identity, ownership_generation, checkpoint, \
          created_at, updated_at) VALUES \
         ('saga', 'transition', 'transition_noncurrent', 'bucket', 'key', 1, 'id:t', \
          'version', 'public', 'object', 1, 'hot', 'cold', 'QmPgTransition', 'QmPgTransition', \
          1, 'hot-node', 'cold-node', 3, 'prepare', clock_timestamp(), clock_timestamp())",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    assert!(
        db.execute_unprepared(
            "UPDATE lifecycle_transitions SET target_sequence = 2 WHERE id = 'saga'"
        )
        .await
        .is_err(),
        "PostgreSQL must reject mutation of the immutable target snapshot"
    );
    db.execute_unprepared(
        "UPDATE lifecycle_transitions SET checkpoint = 'copy', updated_at = clock_timestamp() \
         WHERE id = 'saga'",
    )
    .await
    .unwrap();
    let row = db
        .query_one(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Postgres,
            "SELECT data_type FROM information_schema.columns \
             WHERE table_schema = current_schema() AND table_name = 'lifecycle_transitions' \
               AND column_name = 'created_at'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.try_get::<String>("", "data_type").unwrap(),
        "timestamp with time zone"
    );
    db.execute_unprepared("DELETE FROM object_versions WHERE id = 'version'")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM objects WHERE id = 'object'")
        .await
        .unwrap();
    for statement in [
        "DELETE FROM lifecycle_transitions WHERE id = 'saga'",
        "DELETE FROM lifecycle_actions WHERE id = 'transition'",
        "DELETE FROM buckets WHERE name = 'bucket'",
    ] {
        assert!(
            db.execute_unprepared(statement).await.is_err(),
            "unsettled PostgreSQL saga deletion unexpectedly succeeded: {statement}"
        );
    }
    assert!(store::bucket::delete(&db, "bucket").await.is_err());
    db.execute_unprepared(
        "UPDATE lifecycle_actions SET state = 'cancelled', finished_at = clock_timestamp() \
         WHERE id = 'transition'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE lifecycle_transitions SET settlement_kind = 'cancelled', \
         completed_at = clock_timestamp(), updated_at = clock_timestamp() WHERE id = 'saga'",
    )
    .await
    .unwrap();
    let txn = db.begin().await.unwrap();
    assert!(
        store::lifecycle_transition::delete_settled_in_transaction(&txn, "saga")
            .await
            .unwrap()
    );
    txn.commit().await.unwrap();
    store::bucket::delete(&db, "bucket").await.unwrap();

    for statement in [
        "INSERT INTO buckets (name) VALUES ('published-bucket')",
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('published-object', 'published-bucket', 'key', 'QmPgTransition', 7, \
                 'QmPgTransition', TRUE, clock_timestamp())",
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
          lifecycle_age_started_at, created_at, updated_at) VALUES \
         ('published-version', 'published-bucket', 'key', 'public', 'object', \
          'published-object', 1, TRUE, clock_timestamp(), clock_timestamp(), clock_timestamp())",
        "INSERT INTO version_residencies \
         (version_row_id, object_id, primary_tier, storage_class, cid, revision, \
          created_at, updated_at) VALUES \
         ('published-version', 'published-object', 'hot', 'STANDARD', 'QmPgTransition', 1, \
          clock_timestamp(), clock_timestamp())",
        "INSERT INTO lifecycle_actions \
         (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
          target_type, target_version_row_id, target_public_version_id, target_object_id, \
          target_sequence, due_at, state, next_attempt_at, created_at, updated_at) VALUES \
         ('published-action', 'published-key', 'published-bucket', 'key', 1, 'id:t', \
          'transition_current', 'version', 'published-version', 'public', 'published-object', 1, \
          clock_timestamp(), 'pending', clock_timestamp(), clock_timestamp(), clock_timestamp())",
        "INSERT INTO lifecycle_transitions \
         (id, action_id, action_kind, bucket, object_key, config_revision, rule_id, \
          target_version_row_id, target_public_version_id, target_object_id, target_sequence, \
          source_tier, destination_tier, source_cid, destination_cid, \
          source_residency_revision, expected_source_node_identity, \
          expected_destination_node_identity, ownership_generation, checkpoint, \
          verification_receipt, publication_receipt, created_at, updated_at) VALUES \
         ('published-saga', 'published-action', 'transition_current', 'published-bucket', 'key', \
          1, 'id:t', 'published-version', 'public', 'published-object', 1, 'hot', 'cold', \
          'QmPgTransition', 'QmPgTransition', 1, 'hot-node', 'cold-node', 4, 'publish', \
          'verify-receipt', 'publish-receipt', clock_timestamp(), clock_timestamp())",
        "DELETE FROM object_versions WHERE id = 'published-version'",
        "DELETE FROM objects WHERE id = 'published-object'",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    for statement in [
        "DELETE FROM lifecycle_transitions WHERE id = 'published-saga'",
        "DELETE FROM lifecycle_actions WHERE id = 'published-action'",
        "DELETE FROM buckets WHERE name = 'published-bucket'",
    ] {
        assert!(
            db.execute_unprepared(statement).await.is_err(),
            "published unsettled PostgreSQL saga deletion unexpectedly succeeded: {statement}"
        );
    }
    assert!(
        store::bucket::delete(&db, "published-bucket")
            .await
            .is_err()
    );
    db.execute_unprepared(
        "UPDATE lifecycle_actions SET state = 'succeeded', finished_at = clock_timestamp() \
         WHERE id = 'published-action'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE lifecycle_transitions SET checkpoint = 'cleanup', \
         settlement_kind = 'cleanup_complete', completed_at = clock_timestamp(), \
         updated_at = clock_timestamp() WHERE id = 'published-saga'",
    )
    .await
    .unwrap();
    let txn = db.begin().await.unwrap();
    assert!(
        store::lifecycle_transition::delete_settled_in_transaction(&txn, "published-saga")
            .await
            .unwrap()
    );
    txn.commit().await.unwrap();
    store::bucket::delete(&db, "published-bucket")
        .await
        .unwrap();
    db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}
