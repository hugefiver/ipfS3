use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement, TransactionTrait};
use sea_orm_migration::prelude::*;

const LIFECYCLE_BACKFILL_COUNT_MISMATCH: &str = "lifecycle expiration backfill count mismatch";
const LIFECYCLE_DOWN_REFUSAL: &str = "lifecycle expiration schema contains durable state";
const ACTION_INDEX_NAMES: [&str; 4] = [
    "idx_lifecycle_actions_due",
    "idx_lifecycle_actions_reclaim",
    "idx_lifecycle_actions_bucket_revision",
    "idx_lifecycle_actions_target",
];

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        if matches!(
            connection.get_database_backend(),
            DatabaseBackend::Sqlite | DatabaseBackend::Postgres
        ) {
            let transaction = connection.begin().await?;
            let result = apply_up(&transaction).await;
            return finish_transaction(transaction, result).await;
        }
        apply_up(connection).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        if matches!(
            connection.get_database_backend(),
            DatabaseBackend::Sqlite | DatabaseBackend::Postgres
        ) {
            let transaction = connection.begin().await?;
            let result = apply_down(&transaction).await;
            return finish_transaction(transaction, result).await;
        }
        apply_down(connection).await
    }
}

async fn finish_transaction(
    transaction: sea_orm::DatabaseTransaction,
    result: Result<(), DbErr>,
) -> Result<(), DbErr> {
    match result {
        Ok(()) => transaction.commit().await,
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

fn timestamp_type(backend: DatabaseBackend) -> &'static str {
    if backend == DatabaseBackend::Postgres {
        "TIMESTAMPTZ"
    } else {
        "TIMESTAMP"
    }
}

fn lifecycle_schema_statements(backend: DatabaseBackend) -> Vec<String> {
    let timestamp = timestamp_type(backend);
    let json_check = if backend == DatabaseBackend::Postgres {
        "pg_input_is_valid(canonical_json, 'jsonb')"
    } else {
        "json_valid(canonical_json)"
    };
    vec![
        format!("ALTER TABLE object_versions ADD COLUMN lifecycle_age_started_at {timestamp}"),
        format!("ALTER TABLE object_versions ADD COLUMN became_noncurrent_at {timestamp}"),
        format!(
            "CREATE TABLE bucket_lifecycle_configs (\
                 bucket TEXT PRIMARY KEY NOT NULL REFERENCES buckets(name) ON DELETE CASCADE, \
                 canonical_json TEXT, \
                 revision BIGINT NOT NULL, \
                 scan_cursor TEXT, \
                 scan_lease_epoch BIGINT NOT NULL DEFAULT 0, \
                 scan_lease_until {timestamp}, \
                 created_at {timestamp} NOT NULL, \
                 updated_at {timestamp} NOT NULL, \
                 last_scanned_at {timestamp}, \
                 CONSTRAINT ck_bucket_lifecycle_configs_canonical_json \
                     CHECK (canonical_json IS NULL OR {json_check}), \
                 CONSTRAINT ck_bucket_lifecycle_configs_revision CHECK (revision > 0), \
                 CONSTRAINT ck_bucket_lifecycle_configs_scan_lease_epoch \
                     CHECK (scan_lease_epoch >= 0)\
             )"
        ),
        format!(
            "CREATE TABLE lifecycle_actions (\
                 id TEXT PRIMARY KEY NOT NULL, \
                 idempotency_key TEXT NOT NULL UNIQUE, \
                 bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE, \
                 object_key TEXT NOT NULL, \
                 config_revision BIGINT NOT NULL, \
                 rule_id TEXT NOT NULL, \
                 action_kind TEXT NOT NULL, \
                 target_version_row_id TEXT NOT NULL, \
                 target_public_version_id TEXT NOT NULL, \
                 target_object_id TEXT, \
                 target_sequence BIGINT NOT NULL, \
                 due_at {timestamp} NOT NULL, \
                 state TEXT NOT NULL, \
                 attempts BIGINT NOT NULL DEFAULT 0, \
                 next_attempt_at {timestamp} NOT NULL, \
                 claim_epoch BIGINT NOT NULL DEFAULT 0, \
                 lease_until {timestamp}, \
                 claimed_by TEXT, \
                 failure_class TEXT, \
                 last_error_redacted TEXT, \
                 created_at {timestamp} NOT NULL, \
                 updated_at {timestamp} NOT NULL, \
                 finished_at {timestamp}, \
                 CONSTRAINT ck_lifecycle_actions_config_revision CHECK (config_revision > 0), \
                 CONSTRAINT ck_lifecycle_actions_action_kind CHECK (\
                     action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker')), \
                 CONSTRAINT ck_lifecycle_actions_target_sequence CHECK (target_sequence >= 0), \
                 CONSTRAINT ck_lifecycle_actions_state CHECK (\
                     state IN ('pending', 'claimed', 'succeeded', 'cancelled', 'failed_safe')), \
                 CONSTRAINT ck_lifecycle_actions_attempts CHECK (attempts >= 0), \
                 CONSTRAINT ck_lifecycle_actions_claim_epoch CHECK (claim_epoch >= 0), \
                 CONSTRAINT ck_lifecycle_action_claim CHECK (\
                     (state = 'claimed' AND lease_until IS NOT NULL AND claimed_by IS NOT NULL) OR \
                     (state <> 'claimed' AND lease_until IS NULL AND claimed_by IS NULL)), \
                 CONSTRAINT ck_lifecycle_action_terminal CHECK (\
                     (state IN ('succeeded', 'cancelled', 'failed_safe') AND finished_at IS NOT NULL) OR \
                     (state IN ('pending', 'claimed') AND finished_at IS NULL))\
             )"
        ),
        "CREATE INDEX idx_lifecycle_actions_due \
         ON lifecycle_actions(state, next_attempt_at, due_at, id)"
            .to_owned(),
        "CREATE INDEX idx_lifecycle_actions_reclaim ON lifecycle_actions(state, lease_until, id)"
            .to_owned(),
        "CREATE INDEX idx_lifecycle_actions_bucket_revision \
         ON lifecycle_actions(bucket, config_revision, id)"
            .to_owned(),
        "CREATE INDEX idx_lifecycle_actions_target \
         ON lifecycle_actions(bucket, object_key, target_version_row_id)"
            .to_owned(),
    ]
}

fn object_versions_rebuild_statement() -> &'static str {
    "CREATE TABLE object_versions_lifecycle_rebuild (\
         id TEXT PRIMARY KEY NOT NULL, \
         bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE, \
         key TEXT NOT NULL, \
         version_id TEXT, \
         kind TEXT NOT NULL, \
         object_id TEXT REFERENCES objects(id) ON DELETE CASCADE, \
         sequence BIGINT NOT NULL, \
         is_latest BOOLEAN NOT NULL, \
         lifecycle_age_started_at TIMESTAMP NOT NULL, \
         became_noncurrent_at TIMESTAMP, \
         created_at TIMESTAMP NOT NULL, \
         updated_at TIMESTAMP NOT NULL, \
         CONSTRAINT ck_object_versions_kind \
             CHECK (kind IN ('object', 'delete_marker')), \
         CONSTRAINT ck_object_versions_kind_object \
             CHECK ((kind = 'object' AND object_id IS NOT NULL) OR \
                    (kind = 'delete_marker' AND object_id IS NULL))\
     )"
}

fn object_version_index_statements() -> [&'static str; 7] {
    [
        "CREATE UNIQUE INDEX uq_object_versions_latest \
         ON object_versions(bucket, key) WHERE is_latest = TRUE",
        "CREATE UNIQUE INDEX uq_object_versions_sequence \
         ON object_versions(bucket, key, sequence)",
        "CREATE UNIQUE INDEX uq_object_versions_null_slot \
         ON object_versions(bucket, key) WHERE version_id IS NULL",
        "CREATE UNIQUE INDEX uq_object_versions_public_id \
         ON object_versions(bucket, key, version_id) WHERE version_id IS NOT NULL",
        "CREATE INDEX idx_object_versions_exact ON object_versions(bucket, key, version_id)",
        "CREATE INDEX idx_object_versions_key_order \
         ON object_versions(bucket, key, sequence DESC)",
        "CREATE INDEX idx_object_versions_bucket_order \
         ON object_versions(bucket, key ASC, sequence DESC, version_id)",
    ]
}

async fn execute_statements(
    connection: &impl ConnectionTrait,
    statements: impl IntoIterator<Item = String>,
) -> Result<(), DbErr> {
    for statement in statements {
        connection.execute_unprepared(&statement).await?;
    }
    Ok(())
}

fn backfill_statement(backend: DatabaseBackend) -> Statement {
    Statement::from_string(
        backend,
        "UPDATE object_versions AS current \
         SET lifecycle_age_started_at = current.created_at, \
             became_noncurrent_at = (\
                 SELECT successor.created_at \
                 FROM object_versions AS successor \
                 WHERE successor.bucket = current.bucket \
                   AND successor.key = current.key \
                   AND successor.sequence > current.sequence \
                 ORDER BY successor.sequence ASC, successor.id ASC \
                 LIMIT 1\
             )",
    )
}

fn object_versions_count_statement(backend: DatabaseBackend, predicate: &str) -> Statement {
    Statement::from_string(
        backend,
        format!("SELECT COUNT(*) AS count FROM object_versions WHERE {predicate}"),
    )
}

async fn query_count(
    connection: &impl ConnectionTrait,
    statement: Statement,
) -> Result<i64, DbErr> {
    connection
        .query_one(statement)
        .await?
        .ok_or_else(|| DbErr::Migration("lifecycle count query returned no row".to_owned()))?
        .try_get("", "count")
}

#[cfg(test)]
tokio::task_local! {
    static LIFECYCLE_INJECTION: u8;
}

fn lifecycle_injection_mode() -> u8 {
    #[cfg(test)]
    {
        LIFECYCLE_INJECTION
            .try_with(|mode| *mode)
            .unwrap_or_default()
    }
    #[cfg(not(test))]
    {
        0
    }
}

async fn verify_backfill(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    let backend = connection.get_database_backend();
    let total = query_count(connection, object_versions_count_statement(backend, "TRUE")).await?;
    let age_count = query_count(
        connection,
        object_versions_count_statement(backend, "lifecycle_age_started_at IS NOT NULL"),
    )
    .await?;
    let expected_noncurrent = query_count(
        connection,
        object_versions_count_statement(
            backend,
            "EXISTS (SELECT 1 FROM object_versions AS successor \
             WHERE successor.bucket = object_versions.bucket \
               AND successor.key = object_versions.key \
               AND successor.sequence > object_versions.sequence)",
        ),
    )
    .await?;
    let actual_noncurrent = query_count(
        connection,
        object_versions_count_statement(backend, "became_noncurrent_at IS NOT NULL"),
    )
    .await?;
    let age_count = if lifecycle_injection_mode() == 2 {
        age_count.saturating_sub(1)
    } else {
        age_count
    };

    if total != age_count || expected_noncurrent != actual_noncurrent {
        return Err(DbErr::Migration(
            LIFECYCLE_BACKFILL_COUNT_MISMATCH.to_owned(),
        ));
    }
    Ok(())
}

async fn rebuild_sqlite_object_versions(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    let backend = connection.get_database_backend();
    let before = query_count(connection, object_versions_count_statement(backend, "TRUE")).await?;
    connection
        .execute_unprepared(object_versions_rebuild_statement())
        .await?;
    if lifecycle_injection_mode() == 1 {
        return Err(DbErr::Custom(
            "injected lifecycle migration statement failure".to_owned(),
        ));
    }
    connection
        .execute_unprepared(
            "INSERT INTO object_versions_lifecycle_rebuild \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
              lifecycle_age_started_at, became_noncurrent_at, created_at, updated_at) \
             SELECT id, bucket, key, version_id, kind, object_id, sequence, is_latest, \
                    lifecycle_age_started_at, became_noncurrent_at, created_at, updated_at \
             FROM object_versions",
        )
        .await?;
    connection
        .execute_unprepared("DROP TABLE object_versions")
        .await?;
    connection
        .execute_unprepared(
            "ALTER TABLE object_versions_lifecycle_rebuild RENAME TO object_versions",
        )
        .await?;
    execute_statements(
        connection,
        object_version_index_statements()
            .into_iter()
            .map(str::to_owned),
    )
    .await?;
    let after = query_count(connection, object_versions_count_statement(backend, "TRUE")).await?;
    if before != after {
        return Err(DbErr::Migration(
            LIFECYCLE_BACKFILL_COUNT_MISMATCH.to_owned(),
        ));
    }
    Ok(())
}

async fn apply_up(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    let backend = connection.get_database_backend();
    execute_statements(connection, lifecycle_schema_statements(backend)).await?;
    connection.execute(backfill_statement(backend)).await?;
    verify_backfill(connection).await?;
    if backend == DatabaseBackend::Sqlite {
        rebuild_sqlite_object_versions(connection).await
    } else if backend == DatabaseBackend::Postgres {
        connection
            .execute_unprepared(
                "ALTER TABLE object_versions ALTER COLUMN lifecycle_age_started_at SET NOT NULL",
            )
            .await?;
        Ok(())
    } else {
        Ok(())
    }
}

async fn schema_contains_durable_state(connection: &impl ConnectionTrait) -> Result<bool, DbErr> {
    let backend = connection.get_database_backend();
    let age_diff = if backend == DatabaseBackend::Postgres {
        "lifecycle_age_started_at IS DISTINCT FROM created_at"
    } else {
        "julianday(lifecycle_age_started_at) <> julianday(created_at)"
    };
    let statement = Statement::from_string(
        backend,
        format!(
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM bucket_lifecycle_configs) \
             OR EXISTS (SELECT 1 FROM lifecycle_actions) \
             OR EXISTS (SELECT 1 FROM object_versions WHERE became_noncurrent_at IS NOT NULL) \
             OR EXISTS (SELECT 1 FROM object_versions WHERE {age_diff})"
        ),
    );
    Ok(connection.query_one(statement).await?.is_some())
}

fn down_schema_statements() -> Vec<String> {
    let mut statements = ACTION_INDEX_NAMES
        .iter()
        .rev()
        .map(|name| format!("DROP INDEX IF EXISTS {name}"))
        .collect::<Vec<_>>();
    statements.extend([
        "DROP TABLE lifecycle_actions".to_owned(),
        "DROP TABLE bucket_lifecycle_configs".to_owned(),
        "ALTER TABLE object_versions DROP COLUMN became_noncurrent_at".to_owned(),
        "ALTER TABLE object_versions DROP COLUMN lifecycle_age_started_at".to_owned(),
    ]);
    statements
}

async fn apply_down(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    if schema_contains_durable_state(connection).await? {
        return Err(DbErr::Migration(LIFECYCLE_DOWN_REFUSAL.to_owned()));
    }
    execute_statements(connection, down_schema_statements()).await
}

#[cfg(test)]
mod tests {
    use std::{future::Future, sync::Arc};

    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};
    use sea_orm_migration::MigratorTrait;

    use super::*;
    use crate::store::migrations::{
        m20250701_000001_init, m20260707_000001_decompress_zip,
        m20260720_000001_sse_c_key_fingerprint, m20260721_000001_multi_provider_pinning,
        m20260729_000001_ipfs3_import, m20260729_000002_postgres_utc_timestamps,
        m20260730_000001_standard_mutation_fence, m20260813_000001_postgres_json_columns,
        m20260825_000001_object_versioning,
    };

    const VERSIONING_BUCKET: &str = "lifecycle-bucket";

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
            ]
        }
    }

    struct ObjectVersioningMigrator;

    impl MigratorTrait for ObjectVersioningMigrator {
        fn migrations() -> Vec<Box<dyn MigrationTrait>> {
            let mut migrations = PreLifecycleMigrator::migrations();
            migrations.push(Box::new(m20260825_000001_object_versioning::Migration));
            migrations
        }
    }

    struct LifecycleMigrator;

    impl MigratorTrait for LifecycleMigrator {
        fn migrations() -> Vec<Box<dyn MigrationTrait>> {
            let mut migrations = ObjectVersioningMigrator::migrations();
            migrations.push(Box::new(Migration));
            migrations
        }
    }

    async fn versioning_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        PreLifecycleMigrator::up(&db, None).await.unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO buckets (name) VALUES ('{VERSIONING_BUCKET}')"
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
             VALUES ('legacy-hidden', '{VERSIONING_BUCKET}', 'key', 'cid-hidden', 1, 'cid-hidden', FALSE, \
                     '2026-08-25 00:00:00+00:00'), \
                    ('current-object', '{VERSIONING_BUCKET}', 'key', 'cid-current', 1, 'cid-current', TRUE, \
                     '2026-08-25 00:01:00+00:00')"
        ))
        .await
        .unwrap();
        ObjectVersioningMigrator::up(&db, None).await.unwrap();
        db
    }

    async fn migrate(db: &DatabaseConnection) -> Result<(), DbErr> {
        LifecycleMigrator::up(db, None).await
    }

    async fn migrate_with_injection(db: &DatabaseConnection, mode: u8) -> Result<(), DbErr> {
        LIFECYCLE_INJECTION
            .scope(mode, LifecycleMigrator::up(db, None))
            .await
    }

    async fn with_lifecycle_injection<T>(mode: u8, future: impl Future<Output = T>) -> T {
        LIFECYCLE_INJECTION.scope(mode, future).await
    }

    async fn table_exists(db: &DatabaseConnection, table: &str) -> bool {
        db.query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table.to_owned().into()],
        ))
        .await
        .unwrap()
        .is_some()
    }

    async fn column_exists(db: &DatabaseConnection, table: &str, column: &str) -> bool {
        db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!("PRAGMA table_info({table})"),
        ))
        .await
        .unwrap()
        .iter()
        .any(|row| row.try_get::<String>("", "name").unwrap() == column)
    }

    async fn assert_rejected(db: &DatabaseConnection, statement: &str) {
        assert!(
            db.execute_unprepared(statement).await.is_err(),
            "SQLite accepted invalid lifecycle input: {statement}"
        );
    }

    async fn insert_public_history(db: &DatabaseConnection) {
        db.execute_unprepared(
            "UPDATE object_versions SET is_latest = FALSE WHERE id = (SELECT id FROM object_versions LIMIT 1)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('public-object', 'lifecycle-bucket', 'key', 'public-2', 'object', 'current-object', 2, FALSE, \
                     '2026-08-25 00:02:00+00:00', '2026-08-25 00:02:00+00:00'), \
                    ('public-marker', 'lifecycle-bucket', 'key', 'public-3', 'delete_marker', NULL, 3, TRUE, \
                     '2026-08-25 00:03:00+00:00', '2026-08-25 00:03:00+00:00')",
        )
        .await
        .unwrap();
    }

    async fn assert_failed_migration_rolled_back(db: &DatabaseConnection) {
        assert!(!table_exists(db, "bucket_lifecycle_configs").await);
        assert!(!table_exists(db, "lifecycle_actions").await);
        assert!(!column_exists(db, "object_versions", "lifecycle_age_started_at").await);
        assert!(!column_exists(db, "object_versions", "became_noncurrent_at").await);
        let marker = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT 1 FROM seaql_migrations WHERE version = 'm20260826_000001_lifecycle_expiration'",
            ))
            .await
            .unwrap();
        assert!(marker.is_none());
        let versions: i64 = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM object_versions",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "count")
            .unwrap();
        assert_eq!(
            versions, 1,
            "migration failure must preserve ninth-migration data"
        );
    }

    #[tokio::test]
    async fn lifecycle_injection_scope_does_not_leak_to_an_ordinary_migration() {
        let injected_db = versioning_db().await;
        let ordinary_db = versioning_db().await;
        let entered_injection = Arc::new(tokio::sync::Barrier::new(2));
        let release_injection = Arc::new(tokio::sync::Notify::new());

        let injected_barrier = entered_injection.clone();
        let injected_release = release_injection.clone();
        let injected = tokio::spawn(async move {
            with_lifecycle_injection(1, async {
                injected_barrier.wait().await;
                injected_release.notified().await;
                LifecycleMigrator::up(&injected_db, None).await
            })
            .await
        });
        entered_injection.wait().await;

        let ordinary = tokio::spawn(async move { LifecycleMigrator::up(&ordinary_db, None).await });
        let ordinary_result = ordinary.await.unwrap();
        release_injection.notify_one();
        let injected_result = injected.await.unwrap();

        assert!(matches!(
            injected_result,
            Err(DbErr::Custom(message)) if message == "injected lifecycle migration statement failure"
        ));
        assert!(
            ordinary_result.is_ok(),
            "ordinary migration observed an injected mode: {ordinary_result:?}"
        );
    }

    #[tokio::test]
    async fn lifecycle_injection_scope_is_nestable_and_defaults_to_zero() {
        assert_eq!(lifecycle_injection_mode(), 0);
        with_lifecycle_injection(1, async {
            assert_eq!(lifecycle_injection_mode(), 1);
            with_lifecycle_injection(2, async {
                assert_eq!(lifecycle_injection_mode(), 2);
            })
            .await;
            assert_eq!(lifecycle_injection_mode(), 1);
        })
        .await;
        assert_eq!(lifecycle_injection_mode(), 0);
    }

    #[tokio::test]
    async fn lifecycle_migration_creates_config_action_tables_indexes_and_checks() {
        let db = versioning_db().await;
        migrate(&db).await.unwrap();

        assert!(table_exists(&db, "bucket_lifecycle_configs").await);
        assert!(table_exists(&db, "lifecycle_actions").await);
        assert!(column_exists(&db, "object_versions", "lifecycle_age_started_at").await);
        assert!(column_exists(&db, "object_versions", "became_noncurrent_at").await);
        let indexes = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'lifecycle_actions'",
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").unwrap())
            .collect::<Vec<_>>();
        for name in [
            "idx_lifecycle_actions_due",
            "idx_lifecycle_actions_reclaim",
            "idx_lifecycle_actions_bucket_revision",
            "idx_lifecycle_actions_target",
        ] {
            assert!(indexes.contains(&name.to_owned()), "missing index {name}");
        }
        let version_indexes = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'object_versions'",
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").unwrap())
            .collect::<Vec<_>>();
        for name in [
            "uq_object_versions_latest",
            "uq_object_versions_sequence",
            "uq_object_versions_null_slot",
            "uq_object_versions_public_id",
            "idx_object_versions_exact",
            "idx_object_versions_key_order",
            "idx_object_versions_bucket_order",
        ] {
            assert!(
                version_indexes.contains(&name.to_owned()),
                "rebuild lost object_versions index {name}"
            );
        }
        db.execute_unprepared(
            "INSERT INTO bucket_lifecycle_configs \
             (bucket, canonical_json, revision, scan_lease_epoch, created_at, updated_at) \
             VALUES ('lifecycle-bucket', '{}', 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('lifecycle-invalid-bucket')")
            .await
            .unwrap();
        assert_rejected(
            &db,
            "INSERT INTO bucket_lifecycle_configs \
             (bucket, canonical_json, revision, scan_lease_epoch, created_at, updated_at) \
              VALUES ('lifecycle-invalid-bucket', 'not-json', 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO bucket_lifecycle_configs \
             (bucket, canonical_json, revision, scan_lease_epoch, created_at, updated_at) \
             VALUES ('lifecycle-invalid-bucket', NULL, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO bucket_lifecycle_configs \
             (bucket, canonical_json, revision, scan_lease_epoch, created_at, updated_at) \
             VALUES ('lifecycle-invalid-bucket', NULL, 1, -1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO lifecycle_actions \
             (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
              target_version_row_id, target_public_version_id, target_sequence, due_at, state, \
              attempts, next_attempt_at, claim_epoch, created_at, updated_at) \
             VALUES ('invalid-action', 'invalid-action', 'lifecycle-bucket', 'key', 0, 'rule', 'invalid', \
                     'row', 'public', -1, CURRENT_TIMESTAMP, 'claimed', 0, CURRENT_TIMESTAMP, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO lifecycle_actions \
             (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
              target_version_row_id, target_public_version_id, target_sequence, due_at, state, \
              attempts, next_attempt_at, claim_epoch, created_at, updated_at) \
             VALUES ('unclaimed-action', 'unclaimed-action', 'lifecycle-bucket', 'key', 1, 'rule', \
                     'expire_current', 'row', 'public', 1, CURRENT_TIMESTAMP, 'claimed', 0, \
                     CURRENT_TIMESTAMP, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO lifecycle_actions \
             (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
              target_version_row_id, target_public_version_id, target_sequence, due_at, state, \
              attempts, next_attempt_at, claim_epoch, created_at, updated_at) \
             VALUES ('unfinished-success', 'unfinished-success', 'lifecycle-bucket', 'key', 1, 'rule', \
                     'expire_current', 'row', 'public', 1, CURRENT_TIMESTAMP, 'succeeded', 0, \
                     CURRENT_TIMESTAMP, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
    }

    #[test]
    fn lifecycle_migration_postgres_schema_preserves_text_json_and_timestamptz() {
        let statements = lifecycle_schema_statements(DatabaseBackend::Postgres);
        assert!(statements[0].contains("lifecycle_age_started_at TIMESTAMPTZ"));
        assert!(statements[1].contains("became_noncurrent_at TIMESTAMPTZ"));
        let config = statements
            .iter()
            .find(|statement| statement.starts_with("CREATE TABLE bucket_lifecycle_configs"))
            .unwrap();
        assert!(config.contains("canonical_json TEXT"));
        assert!(config.contains("pg_input_is_valid(canonical_json, 'jsonb')"));
        assert!(!config.contains("pg_input_error_info"));
        let action = statements
            .iter()
            .find(|statement| statement.starts_with("CREATE TABLE lifecycle_actions"))
            .unwrap();
        assert!(action.contains("target_object_id TEXT"));
        assert!(action.contains("ck_lifecycle_action_claim"));
        assert!(action.contains("ck_lifecycle_action_terminal"));
    }

    #[tokio::test]
    async fn lifecycle_migration_backfills_successor_noncurrent_time() {
        let db = versioning_db().await;
        insert_public_history(&db).await;
        migrate(&db).await.unwrap();

        let rows = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT id, created_at, lifecycle_age_started_at, became_noncurrent_at \
                 FROM object_versions ORDER BY sequence ASC",
            ))
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        for row in &rows {
            let created: chrono::DateTime<chrono::Utc> = row.try_get("", "created_at").unwrap();
            let age: chrono::DateTime<chrono::Utc> =
                row.try_get("", "lifecycle_age_started_at").unwrap();
            assert_eq!(age, created);
        }
        let first_noncurrent: Option<chrono::DateTime<chrono::Utc>> =
            rows[0].try_get("", "became_noncurrent_at").unwrap();
        let second_noncurrent: Option<chrono::DateTime<chrono::Utc>> =
            rows[1].try_get("", "became_noncurrent_at").unwrap();
        let latest_noncurrent: Option<chrono::DateTime<chrono::Utc>> =
            rows[2].try_get("", "became_noncurrent_at").unwrap();
        assert_eq!(
            first_noncurrent,
            Some(rows[1].try_get("", "created_at").unwrap())
        );
        assert_eq!(
            second_noncurrent,
            Some(rows[2].try_get("", "created_at").unwrap())
        );
        assert_eq!(latest_noncurrent, None, "the latest row remains current");
    }

    #[tokio::test]
    async fn lifecycle_migration_keeps_latest_null_and_legacy_nonlatest_hidden() {
        let db = versioning_db().await;
        migrate(&db).await.unwrap();

        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT created_at, object_id, lifecycle_age_started_at, became_noncurrent_at \
                 FROM object_versions",
            ))
            .await
            .unwrap()
            .unwrap();
        let object_id: Option<String> = row.try_get("", "object_id").unwrap();
        let created: chrono::DateTime<chrono::Utc> = row.try_get("", "created_at").unwrap();
        let age: chrono::DateTime<chrono::Utc> =
            row.try_get("", "lifecycle_age_started_at").unwrap();
        let noncurrent: Option<chrono::DateTime<chrono::Utc>> =
            row.try_get("", "became_noncurrent_at").unwrap();
        assert_eq!(object_id.as_deref(), Some("current-object"));
        assert_eq!(age, created);
        assert_eq!(noncurrent, None);
        let hidden: i64 = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM object_versions WHERE object_id = 'legacy-hidden'",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "count")
            .unwrap();
        assert_eq!(hidden, 0, "ambiguous legacy rows must stay invisible");
    }

    #[tokio::test]
    async fn lifecycle_migration_count_mismatch_rolls_back() {
        let db = versioning_db().await;
        let result = migrate_with_injection(&db, 2).await;

        assert!(matches!(
            result,
            Err(DbErr::Migration(message)) if message == LIFECYCLE_BACKFILL_COUNT_MISMATCH
        ));
        assert_failed_migration_rolled_back(&db).await;
    }

    #[tokio::test]
    async fn lifecycle_migration_insert_failure_rolls_back() {
        let db = versioning_db().await;
        let result = migrate_with_injection(&db, 1).await;

        assert!(matches!(
            result,
            Err(DbErr::Custom(message)) if message == "injected lifecycle migration statement failure"
        ));
        assert_failed_migration_rolled_back(&db).await;
    }

    #[tokio::test]
    async fn lifecycle_down_allows_only_pristine_backfill() {
        let db = versioning_db().await;
        migrate(&db).await.unwrap();

        Migration.down(&SchemaManager::new(&db)).await.unwrap();

        assert!(!table_exists(&db, "bucket_lifecycle_configs").await);
        assert!(!table_exists(&db, "lifecycle_actions").await);
        assert!(!column_exists(&db, "object_versions", "lifecycle_age_started_at").await);
        assert!(!column_exists(&db, "object_versions", "became_noncurrent_at").await);
    }

    #[tokio::test]
    async fn lifecycle_down_refuses_each_durable_state() {
        for mutation in [
            "INSERT INTO bucket_lifecycle_configs \
             (bucket, canonical_json, revision, scan_lease_epoch, created_at, updated_at) \
             VALUES ('lifecycle-bucket', NULL, 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO lifecycle_actions \
             (id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
              target_version_row_id, target_public_version_id, target_sequence, due_at, state, \
              attempts, next_attempt_at, claim_epoch, created_at, updated_at) \
             VALUES ('action', 'action', 'lifecycle-bucket', 'key', 1, 'rule', 'expire_current', \
                     'row', 'public', 1, CURRENT_TIMESTAMP, 'pending', 0, CURRENT_TIMESTAMP, 0, \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "UPDATE object_versions SET became_noncurrent_at = created_at",
            "UPDATE object_versions SET lifecycle_age_started_at = '2026-08-26 00:00:00+00:00'",
        ] {
            let db = versioning_db().await;
            migrate(&db).await.unwrap();
            db.execute_unprepared(mutation).await.unwrap();

            let result = Migration.down(&SchemaManager::new(&db)).await;
            assert!(matches!(
                result,
                Err(DbErr::Migration(message)) if message == LIFECYCLE_DOWN_REFUSAL
            ));
            assert!(table_exists(&db, "bucket_lifecycle_configs").await);
            assert!(column_exists(&db, "object_versions", "lifecycle_age_started_at").await);
        }
    }
}
