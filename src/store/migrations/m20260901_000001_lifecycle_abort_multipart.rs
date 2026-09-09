use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement, TransactionTrait};
use sea_orm_migration::prelude::*;

const COPY_COUNT_MISMATCH: &str = "lifecycle abort action copy count mismatch";
const DOWN_REFUSAL: &str = "lifecycle abort schema contains multipart state";
const POSTGRES_COLUMN_DEPENDENCY: &str =
    "lifecycle abort down found a remaining target-column constraint";
const POSTGRES_TARGET_COLUMN_CONSTRAINT_QUERY: &str = "SELECT conname \
     FROM pg_constraint \
     WHERE conrelid = 'lifecycle_actions'::regclass \
       AND (\
         pg_get_constraintdef(oid) LIKE '%target_type%' \
         OR pg_get_constraintdef(oid) LIKE '%target_upload_id%' \
         OR pg_get_constraintdef(oid) LIKE '%target_upload_created_at%'\
       )";

const PHASE_A_INDEX_STATEMENTS: [&str; 4] = [
    "CREATE INDEX idx_lifecycle_actions_due \
     ON lifecycle_actions(state, next_attempt_at, due_at, id)",
    "CREATE INDEX idx_lifecycle_actions_reclaim ON lifecycle_actions(state, lease_until, id)",
    "CREATE INDEX idx_lifecycle_actions_bucket_revision \
     ON lifecycle_actions(bucket, config_revision, id)",
    "CREATE INDEX idx_lifecycle_actions_target \
     ON lifecycle_actions(bucket, object_key, target_version_row_id)",
];

const MULTIPART_INDEX_STATEMENT: &str = "CREATE INDEX idx_lifecycle_actions_multipart_target \
     ON lifecycle_actions(bucket, object_key, target_upload_id, target_upload_created_at)";

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
        Err(DbErr::Migration(
            "lifecycle abort migration supports only SQLite and PostgreSQL".to_owned(),
        ))
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
        Err(DbErr::Migration(
            "lifecycle abort down migration supports only SQLite and PostgreSQL".to_owned(),
        ))
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

fn polymorphic_table_statement(backend: DatabaseBackend, table: &str) -> String {
    let timestamp = timestamp_type(backend);
    format!(
        "CREATE TABLE {table} (\
             id TEXT PRIMARY KEY NOT NULL, \
             idempotency_key TEXT NOT NULL UNIQUE, \
             bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE, \
             object_key TEXT NOT NULL, \
             config_revision BIGINT NOT NULL, \
             rule_id TEXT NOT NULL, \
             action_kind TEXT NOT NULL, \
             target_type TEXT NOT NULL, \
             target_version_row_id TEXT, \
             target_public_version_id TEXT, \
             target_object_id TEXT, \
             target_sequence BIGINT, \
             target_upload_id TEXT, \
             target_upload_created_at {timestamp}, \
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
                 action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker', \
                                 'abort_incomplete_multipart_upload')), \
             CONSTRAINT ck_lifecycle_actions_target_sequence CHECK (\
                 target_sequence IS NULL OR target_sequence >= 0), \
             CONSTRAINT ck_lifecycle_actions_target_shape CHECK (\
                 (target_type = 'version' \
                  AND target_version_row_id IS NOT NULL \
                  AND target_public_version_id IS NOT NULL \
                  AND target_sequence IS NOT NULL \
                  AND target_upload_id IS NULL \
                  AND target_upload_created_at IS NULL) \
                 OR \
                 (target_type = 'multipart_upload' \
                  AND target_version_row_id IS NULL \
                  AND target_public_version_id IS NULL \
                  AND target_object_id IS NULL \
                  AND target_sequence IS NULL \
                  AND target_upload_id IS NOT NULL \
                  AND target_upload_created_at IS NOT NULL)), \
             CONSTRAINT ck_lifecycle_actions_kind_target CHECK (\
                 (action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker') \
                  AND target_type = 'version') \
                 OR \
                 (action_kind = 'abort_incomplete_multipart_upload' \
                  AND target_type = 'multipart_upload')), \
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
    )
}

fn phase_a_table_statement(table: &str) -> String {
    format!(
        "CREATE TABLE {table} (\
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
             due_at TIMESTAMP NOT NULL, \
             state TEXT NOT NULL, \
             attempts BIGINT NOT NULL DEFAULT 0, \
             next_attempt_at TIMESTAMP NOT NULL, \
             claim_epoch BIGINT NOT NULL DEFAULT 0, \
             lease_until TIMESTAMP, \
             claimed_by TEXT, \
             failure_class TEXT, \
             last_error_redacted TEXT, \
             created_at TIMESTAMP NOT NULL, \
             updated_at TIMESTAMP NOT NULL, \
             finished_at TIMESTAMP, \
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
    )
}

async fn execute_statements(
    connection: &impl ConnectionTrait,
    statements: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<(), DbErr> {
    for statement in statements {
        connection.execute_unprepared(statement.as_ref()).await?;
    }
    Ok(())
}

async fn query_count(connection: &impl ConnectionTrait, table: &str) -> Result<i64, DbErr> {
    connection
        .query_one(Statement::from_string(
            connection.get_database_backend(),
            format!("SELECT COUNT(*) AS count FROM {table}"),
        ))
        .await?
        .ok_or_else(|| DbErr::Migration("lifecycle abort count query returned no row".to_owned()))?
        .try_get("", "count")
}

async fn verify_copy_count(
    connection: &impl ConnectionTrait,
    expected: i64,
    table: &str,
) -> Result<(), DbErr> {
    if query_count(connection, table).await? != expected {
        return Err(DbErr::Migration(COPY_COUNT_MISMATCH.to_owned()));
    }
    Ok(())
}

fn should_fail_before_swap() -> bool {
    #[cfg(test)]
    {
        FAIL_AFTER_COPY_BEFORE_SWAP.try_with(|_| ()).is_ok()
    }
    #[cfg(not(test))]
    {
        false
    }
}

async fn rebuild_sqlite_up(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    const REBUILD: &str = "lifecycle_actions_abort_rebuild";
    let before = query_count(connection, "lifecycle_actions").await?;
    connection
        .execute_unprepared(&polymorphic_table_statement(
            DatabaseBackend::Sqlite,
            REBUILD,
        ))
        .await?;
    connection
        .execute_unprepared(
            "INSERT INTO lifecycle_actions_abort_rebuild (\
                 id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
                 target_type, target_version_row_id, target_public_version_id, target_object_id, \
                 target_sequence, target_upload_id, target_upload_created_at, due_at, state, \
                 attempts, next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
                 last_error_redacted, created_at, updated_at, finished_at\
             ) SELECT \
                 id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
                 'version', target_version_row_id, target_public_version_id, target_object_id, \
                 target_sequence, NULL, NULL, due_at, state, attempts, next_attempt_at, claim_epoch, \
                 lease_until, claimed_by, failure_class, last_error_redacted, created_at, updated_at, \
                 finished_at \
             FROM lifecycle_actions",
        )
        .await?;
    verify_copy_count(connection, before, REBUILD).await?;
    if should_fail_before_swap() {
        return Err(DbErr::Custom(
            "injected lifecycle abort migration failure before swap".to_owned(),
        ));
    }
    connection
        .execute_unprepared("DROP TABLE lifecycle_actions")
        .await?;
    connection
        .execute_unprepared(
            "ALTER TABLE lifecycle_actions_abort_rebuild RENAME TO lifecycle_actions",
        )
        .await?;
    execute_statements(connection, PHASE_A_INDEX_STATEMENTS).await?;
    connection
        .execute_unprepared(MULTIPART_INDEX_STATEMENT)
        .await?;
    verify_copy_count(connection, before, "lifecycle_actions").await
}

fn postgres_up_statements() -> Vec<&'static str> {
    vec![
        "ALTER TABLE lifecycle_actions ADD COLUMN target_type TEXT",
        "ALTER TABLE lifecycle_actions ADD COLUMN target_upload_id TEXT",
        "ALTER TABLE lifecycle_actions ADD COLUMN target_upload_created_at TIMESTAMPTZ",
        "UPDATE lifecycle_actions SET target_type = 'version'",
        "ALTER TABLE lifecycle_actions ALTER COLUMN target_type SET NOT NULL",
        "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_action_kind",
        "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_target_sequence",
        "ALTER TABLE lifecycle_actions ALTER COLUMN target_version_row_id DROP NOT NULL",
        "ALTER TABLE lifecycle_actions ALTER COLUMN target_public_version_id DROP NOT NULL",
        "ALTER TABLE lifecycle_actions ALTER COLUMN target_sequence DROP NOT NULL",
        "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_action_kind CHECK (\
             action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker', \
                             'abort_incomplete_multipart_upload'))",
        "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_target_sequence CHECK (\
             target_sequence IS NULL OR target_sequence >= 0)",
        "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_target_shape CHECK (\
             (target_type = 'version' \
              AND target_version_row_id IS NOT NULL \
              AND target_public_version_id IS NOT NULL \
              AND target_sequence IS NOT NULL \
              AND target_upload_id IS NULL \
              AND target_upload_created_at IS NULL) \
             OR \
             (target_type = 'multipart_upload' \
              AND target_version_row_id IS NULL \
              AND target_public_version_id IS NULL \
              AND target_object_id IS NULL \
              AND target_sequence IS NULL \
              AND target_upload_id IS NOT NULL \
              AND target_upload_created_at IS NOT NULL))",
        "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_kind_target CHECK (\
             (action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker') \
              AND target_type = 'version') \
             OR \
             (action_kind = 'abort_incomplete_multipart_upload' \
              AND target_type = 'multipart_upload'))",
        MULTIPART_INDEX_STATEMENT,
    ]
}

async fn apply_up(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    match connection.get_database_backend() {
        DatabaseBackend::Sqlite => rebuild_sqlite_up(connection).await,
        DatabaseBackend::Postgres => execute_statements(connection, postgres_up_statements()).await,
        _ => Err(DbErr::Migration(
            "lifecycle abort migration supports only SQLite and PostgreSQL".to_owned(),
        )),
    }
}

async fn contains_multipart_state(connection: &impl ConnectionTrait) -> Result<bool, DbErr> {
    Ok(connection
        .query_one(Statement::from_string(
            connection.get_database_backend(),
            "SELECT 1 FROM lifecycle_actions \
             WHERE target_type <> 'version' \
                OR action_kind = 'abort_incomplete_multipart_upload' \
                OR target_upload_id IS NOT NULL \
                OR target_upload_created_at IS NOT NULL \
             LIMIT 1",
        ))
        .await?
        .is_some())
}

async fn rebuild_sqlite_down(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    const REBUILD: &str = "lifecycle_actions_phase_a_rebuild";
    let before = query_count(connection, "lifecycle_actions").await?;
    connection
        .execute_unprepared(&phase_a_table_statement(REBUILD))
        .await?;
    connection
        .execute_unprepared(
            "INSERT INTO lifecycle_actions_phase_a_rebuild (\
                 id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
                 target_version_row_id, target_public_version_id, target_object_id, target_sequence, \
                 due_at, state, attempts, next_attempt_at, claim_epoch, lease_until, claimed_by, \
                 failure_class, last_error_redacted, created_at, updated_at, finished_at\
             ) SELECT \
                 id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
                 target_version_row_id, target_public_version_id, target_object_id, target_sequence, \
                 due_at, state, attempts, next_attempt_at, claim_epoch, lease_until, claimed_by, \
                 failure_class, last_error_redacted, created_at, updated_at, finished_at \
             FROM lifecycle_actions",
        )
        .await?;
    verify_copy_count(connection, before, REBUILD).await?;
    connection
        .execute_unprepared("DROP TABLE lifecycle_actions")
        .await?;
    connection
        .execute_unprepared(
            "ALTER TABLE lifecycle_actions_phase_a_rebuild RENAME TO lifecycle_actions",
        )
        .await?;
    execute_statements(connection, PHASE_A_INDEX_STATEMENTS).await?;
    verify_copy_count(connection, before, "lifecycle_actions").await
}

fn postgres_down_before_dependency_check() -> Vec<&'static str> {
    vec![
        "DROP INDEX IF EXISTS idx_lifecycle_actions_multipart_target",
        "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_target_shape",
        "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_kind_target",
        "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_action_kind",
        "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_target_sequence",
        "ALTER TABLE lifecycle_actions ALTER COLUMN target_version_row_id SET NOT NULL",
        "ALTER TABLE lifecycle_actions ALTER COLUMN target_public_version_id SET NOT NULL",
        "ALTER TABLE lifecycle_actions ALTER COLUMN target_sequence SET NOT NULL",
    ]
}

fn postgres_down_after_dependency_check() -> Vec<&'static str> {
    vec![
        "ALTER TABLE lifecycle_actions DROP COLUMN target_type",
        "ALTER TABLE lifecycle_actions DROP COLUMN target_upload_id",
        "ALTER TABLE lifecycle_actions DROP COLUMN target_upload_created_at",
        "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_action_kind CHECK (\
             action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker'))",
        "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_target_sequence CHECK (\
             target_sequence >= 0)",
    ]
}

async fn postgres_down(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    execute_statements(connection, postgres_down_before_dependency_check()).await?;
    let dependencies = connection
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            POSTGRES_TARGET_COLUMN_CONSTRAINT_QUERY,
        ))
        .await?;
    if !dependencies.is_empty() {
        return Err(DbErr::Migration(POSTGRES_COLUMN_DEPENDENCY.to_owned()));
    }
    execute_statements(connection, postgres_down_after_dependency_check()).await
}

async fn apply_down(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    if contains_multipart_state(connection).await? {
        return Err(DbErr::Migration(DOWN_REFUSAL.to_owned()));
    }
    match connection.get_database_backend() {
        DatabaseBackend::Sqlite => rebuild_sqlite_down(connection).await,
        DatabaseBackend::Postgres => postgres_down(connection).await,
        _ => Err(DbErr::Migration(
            "lifecycle abort down migration supports only SQLite and PostgreSQL".to_owned(),
        )),
    }
}

#[cfg(test)]
tokio::task_local! {
    static FAIL_AFTER_COPY_BEFORE_SWAP: ();
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};
    use sea_orm_migration::MigratorTrait;

    use super::*;
    use crate::store::migrations::{
        m20250701_000001_init, m20260707_000001_decompress_zip,
        m20260720_000001_sse_c_key_fingerprint, m20260721_000001_multi_provider_pinning,
        m20260729_000001_ipfs3_import, m20260729_000002_postgres_utc_timestamps,
        m20260730_000001_standard_mutation_fence, m20260813_000001_postgres_json_columns,
        m20260825_000001_object_versioning, m20260826_000001_lifecycle_expiration,
        m20260831_000001_bucket_cors,
    };

    struct PhaseAMigrator;

    impl MigratorTrait for PhaseAMigrator {
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

    struct LifecycleAbortMigrator;

    impl MigratorTrait for LifecycleAbortMigrator {
        fn migrations() -> Vec<Box<dyn MigrationTrait>> {
            let mut migrations = PhaseAMigrator::migrations();
            migrations.push(Box::new(Migration));
            migrations
        }
    }

    async fn phase_a_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        PhaseAMigrator::up(&db, None).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        db.execute_unprepared(
            "INSERT INTO lifecycle_actions (\
                 id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
                 target_version_row_id, target_public_version_id, target_object_id, target_sequence, \
                 due_at, state, attempts, next_attempt_at, claim_epoch, lease_until, claimed_by, \
                 failure_class, last_error_redacted, created_at, updated_at, finished_at\
             ) VALUES (\
                 'phase-a-action', 'phase-a-key', 'bucket', 'key', 7, 'id:expire', 'expire_current', \
                 'version-row', '00000000-0000-4000-8000-000000000001', 'object-id', 1, \
                 '2026-09-01 00:00:00+00:00', 'claimed', 3, '2026-09-01 00:01:00+00:00', 9, \
                 '2026-09-01 00:02:00+00:00', 'worker-a', 'database_contention', \
                 'lifecycle action failed', '2026-08-31 23:00:00+00:00', \
                 '2026-09-01 00:00:30+00:00', NULL\
             )",
        )
        .await
        .unwrap();
        db
    }

    async fn phase_a_snapshot(db: &DatabaseConnection) -> String {
        db.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT json_array(\
                 id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
                 target_version_row_id, target_public_version_id, target_object_id, target_sequence, \
                 due_at, state, attempts, next_attempt_at, claim_epoch, lease_until, claimed_by, \
                 failure_class, last_error_redacted, created_at, updated_at, finished_at\
             ) AS snapshot FROM lifecycle_actions WHERE id = 'phase-a-action'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "snapshot")
        .unwrap()
    }

    async fn table_sql(db: &DatabaseConnection) -> String {
        db.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'lifecycle_actions'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "sql")
        .unwrap()
    }

    async fn explicit_indexes(db: &DatabaseConnection) -> BTreeMap<String, String> {
        db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT name, sql FROM sqlite_master \
             WHERE type = 'index' AND tbl_name = 'lifecycle_actions' AND sql IS NOT NULL \
             ORDER BY name",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.try_get("", "name").unwrap(),
                normalize_sql(&row.try_get::<String>("", "sql").unwrap()),
            )
        })
        .collect()
    }

    async fn columns(db: &DatabaseConnection) -> BTreeMap<String, (String, i64)> {
        db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA table_info(lifecycle_actions)",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.try_get("", "name").unwrap(),
                (
                    row.try_get("", "type").unwrap(),
                    row.try_get("", "notnull").unwrap(),
                ),
            )
        })
        .collect()
    }

    async fn migration_versions(db: &DatabaseConnection) -> Vec<String> {
        db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get("", "version").unwrap())
        .collect()
    }

    fn normalize_sql(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    async fn assert_rejected(db: &DatabaseConnection, statement: &str) {
        assert!(
            db.execute_unprepared(statement).await.is_err(),
            "SQLite accepted invalid lifecycle action: {statement}"
        );
    }

    async fn insert_multipart_action(db: &DatabaseConnection, id: &str) {
        db.execute_unprepared(&format!(
            "INSERT INTO lifecycle_actions (\
                 id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
                 target_type, target_version_row_id, target_public_version_id, target_object_id, \
                 target_sequence, target_upload_id, target_upload_created_at, due_at, state, \
                 attempts, next_attempt_at, claim_epoch, created_at, updated_at\
             ) VALUES (\
                 '{id}', '{id}', 'bucket', 'mpu-key', 7, 'id:abort', \
                 'abort_incomplete_multipart_upload', 'multipart_upload', NULL, NULL, NULL, NULL, \
                 'upload-id', '2026-08-01 00:00:00+00:00', '2026-09-01 00:00:00+00:00', \
                 'pending', 0, '2026-09-01 00:00:00+00:00', 0, \
                 '2026-09-01 00:00:00+00:00', '2026-09-01 00:00:00+00:00'\
             )"
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn lifecycle_abort_migration_rebuilds_sqlite_without_changing_version_actions() {
        let db = phase_a_db().await;
        let before = phase_a_snapshot(&db).await;

        Migration.up(&SchemaManager::new(&db)).await.unwrap();

        assert_eq!(phase_a_snapshot(&db).await, before);
        let target = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT target_type, target_upload_id, target_upload_created_at \
                 FROM lifecycle_actions WHERE id = 'phase-a-action'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            target.try_get::<String>("", "target_type").unwrap(),
            "version"
        );
        assert_eq!(
            target
                .try_get::<Option<String>>("", "target_upload_id")
                .unwrap(),
            None
        );
        assert_eq!(
            target
                .try_get::<Option<String>>("", "target_upload_created_at")
                .unwrap(),
            None
        );

        let columns = columns(&db).await;
        assert_eq!(columns["target_type"], ("TEXT".to_owned(), 1));
        assert_eq!(columns["target_version_row_id"].1, 0);
        assert_eq!(columns["target_public_version_id"].1, 0);
        assert_eq!(columns["target_object_id"].1, 0);
        assert_eq!(columns["target_sequence"].1, 0);
        assert_eq!(columns["target_upload_id"], ("TEXT".to_owned(), 0));
        assert_eq!(
            columns["target_upload_created_at"],
            ("TIMESTAMP".to_owned(), 0)
        );
        assert_eq!(
            explicit_indexes(&db).await,
            BTreeMap::from([
                (
                    "idx_lifecycle_actions_bucket_revision".to_owned(),
                    "CREATE INDEX idx_lifecycle_actions_bucket_revision ON lifecycle_actions(bucket, config_revision, id)".to_owned(),
                ),
                (
                    "idx_lifecycle_actions_due".to_owned(),
                    "CREATE INDEX idx_lifecycle_actions_due ON lifecycle_actions(state, next_attempt_at, due_at, id)".to_owned(),
                ),
                (
                    "idx_lifecycle_actions_multipart_target".to_owned(),
                    "CREATE INDEX idx_lifecycle_actions_multipart_target ON lifecycle_actions(bucket, object_key, target_upload_id, target_upload_created_at)".to_owned(),
                ),
                (
                    "idx_lifecycle_actions_reclaim".to_owned(),
                    "CREATE INDEX idx_lifecycle_actions_reclaim ON lifecycle_actions(state, lease_until, id)".to_owned(),
                ),
                (
                    "idx_lifecycle_actions_target".to_owned(),
                    "CREATE INDEX idx_lifecycle_actions_target ON lifecycle_actions(bucket, object_key, target_version_row_id)".to_owned(),
                ),
            ])
        );
        let table = normalize_sql(&table_sql(&db).await);
        assert_eq!(table.matches("CONSTRAINT ck_").count(), 10);
        for check in [
            "ck_lifecycle_actions_action_kind",
            "ck_lifecycle_actions_target_sequence",
            "ck_lifecycle_actions_target_shape",
            "ck_lifecycle_actions_kind_target",
        ] {
            assert!(table.contains(check), "missing final check {check}");
        }
    }

    #[tokio::test]
    async fn lifecycle_abort_migration_rejects_hybrid_targets_and_kind_mismatches() {
        let db = phase_a_db().await;
        Migration.up(&SchemaManager::new(&db)).await.unwrap();
        insert_multipart_action(&db, "valid-multipart").await;

        for statement in [
            "INSERT INTO lifecycle_actions (id, idempotency_key, bucket, object_key, config_revision, \
             rule_id, action_kind, target_type, target_version_row_id, target_public_version_id, \
             target_object_id, target_sequence, target_upload_id, target_upload_created_at, due_at, \
             state, attempts, next_attempt_at, claim_epoch, created_at, updated_at) VALUES \
             ('hybrid-version', 'hybrid-version', 'bucket', 'key', 7, 'id:expire', 'expire_current', \
              'version', 'row', 'public', 'object', 1, 'upload', NULL, CURRENT_TIMESTAMP, 'pending', \
              0, CURRENT_TIMESTAMP, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO lifecycle_actions (id, idempotency_key, bucket, object_key, config_revision, \
             rule_id, action_kind, target_type, target_version_row_id, target_public_version_id, \
             target_object_id, target_sequence, target_upload_id, target_upload_created_at, due_at, \
             state, attempts, next_attempt_at, claim_epoch, created_at, updated_at) VALUES \
             ('hybrid-multipart', 'hybrid-multipart', 'bucket', 'key', 7, 'id:abort', \
              'abort_incomplete_multipart_upload', 'multipart_upload', 'row', NULL, NULL, NULL, \
              'upload', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'pending', 0, CURRENT_TIMESTAMP, 0, \
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO lifecycle_actions (id, idempotency_key, bucket, object_key, config_revision, \
             rule_id, action_kind, target_type, target_version_row_id, target_public_version_id, \
             target_object_id, target_sequence, target_upload_id, target_upload_created_at, due_at, \
             state, attempts, next_attempt_at, claim_epoch, created_at, updated_at) VALUES \
             ('object-multipart', 'object-multipart', 'bucket', 'key', 7, 'id:abort', \
              'abort_incomplete_multipart_upload', 'multipart_upload', NULL, NULL, 'object', NULL, \
              'upload', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'pending', 0, CURRENT_TIMESTAMP, 0, \
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO lifecycle_actions (id, idempotency_key, bucket, object_key, config_revision, \
             rule_id, action_kind, target_type, target_version_row_id, target_public_version_id, \
             target_object_id, target_sequence, target_upload_id, target_upload_created_at, due_at, \
             state, attempts, next_attempt_at, claim_epoch, created_at, updated_at) VALUES \
             ('abort-version', 'abort-version', 'bucket', 'key', 7, 'id:abort', \
              'abort_incomplete_multipart_upload', 'version', 'row', 'public', NULL, 1, NULL, NULL, \
              CURRENT_TIMESTAMP, 'pending', 0, CURRENT_TIMESTAMP, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO lifecycle_actions (id, idempotency_key, bucket, object_key, config_revision, \
             rule_id, action_kind, target_type, target_version_row_id, target_public_version_id, \
             target_object_id, target_sequence, target_upload_id, target_upload_created_at, due_at, \
             state, attempts, next_attempt_at, claim_epoch, created_at, updated_at) VALUES \
             ('expire-multipart', 'expire-multipart', 'bucket', 'key', 7, 'id:expire', \
              'expire_current', 'multipart_upload', NULL, NULL, NULL, NULL, 'upload-2', \
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'pending', 0, CURRENT_TIMESTAMP, 0, \
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO lifecycle_actions (id, idempotency_key, bucket, object_key, config_revision, \
             rule_id, action_kind, target_type, target_version_row_id, target_public_version_id, \
             target_object_id, target_sequence, target_upload_id, target_upload_created_at, due_at, \
             state, attempts, next_attempt_at, claim_epoch, created_at, updated_at) VALUES \
             ('negative-sequence', 'negative-sequence', 'bucket', 'key', 7, 'id:expire', \
              'expire_current', 'version', 'row', 'public', NULL, -1, NULL, NULL, CURRENT_TIMESTAMP, \
              'pending', 0, CURRENT_TIMESTAMP, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        ] {
            assert_rejected(&db, statement).await;
        }
    }

    #[test]
    fn lifecycle_abort_migration_postgres_sql_preserves_dependency_safe_order() {
        let up = postgres_up_statements();
        assert_eq!(
            &up[..10],
            [
                "ALTER TABLE lifecycle_actions ADD COLUMN target_type TEXT",
                "ALTER TABLE lifecycle_actions ADD COLUMN target_upload_id TEXT",
                "ALTER TABLE lifecycle_actions ADD COLUMN target_upload_created_at TIMESTAMPTZ",
                "UPDATE lifecycle_actions SET target_type = 'version'",
                "ALTER TABLE lifecycle_actions ALTER COLUMN target_type SET NOT NULL",
                "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_action_kind",
                "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_target_sequence",
                "ALTER TABLE lifecycle_actions ALTER COLUMN target_version_row_id DROP NOT NULL",
                "ALTER TABLE lifecycle_actions ALTER COLUMN target_public_version_id DROP NOT NULL",
                "ALTER TABLE lifecycle_actions ALTER COLUMN target_sequence DROP NOT NULL",
            ]
        );
        let up_sql = normalize_sql(&up.join("; "));
        for required in [
            "abort_incomplete_multipart_upload",
            "ck_lifecycle_actions_target_shape",
            "ck_lifecycle_actions_kind_target",
            "target_sequence IS NULL OR target_sequence >= 0",
            "CREATE INDEX idx_lifecycle_actions_multipart_target ON lifecycle_actions(bucket, object_key, target_upload_id, target_upload_created_at)",
        ] {
            assert!(
                up_sql.contains(required),
                "PostgreSQL up omitted {required}"
            );
        }

        assert_eq!(
            postgres_down_before_dependency_check(),
            [
                "DROP INDEX IF EXISTS idx_lifecycle_actions_multipart_target",
                "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_target_shape",
                "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_kind_target",
                "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_action_kind",
                "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_target_sequence",
                "ALTER TABLE lifecycle_actions ALTER COLUMN target_version_row_id SET NOT NULL",
                "ALTER TABLE lifecycle_actions ALTER COLUMN target_public_version_id SET NOT NULL",
                "ALTER TABLE lifecycle_actions ALTER COLUMN target_sequence SET NOT NULL",
            ]
        );
        assert_eq!(
            normalize_sql(POSTGRES_TARGET_COLUMN_CONSTRAINT_QUERY),
            "SELECT conname FROM pg_constraint WHERE conrelid = 'lifecycle_actions'::regclass AND (pg_get_constraintdef(oid) LIKE '%target_type%' OR pg_get_constraintdef(oid) LIKE '%target_upload_id%' OR pg_get_constraintdef(oid) LIKE '%target_upload_created_at%')"
        );
        assert_eq!(
            postgres_down_after_dependency_check(),
            [
                "ALTER TABLE lifecycle_actions DROP COLUMN target_type",
                "ALTER TABLE lifecycle_actions DROP COLUMN target_upload_id",
                "ALTER TABLE lifecycle_actions DROP COLUMN target_upload_created_at",
                "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_action_kind CHECK (action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker'))",
                "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_target_sequence CHECK (target_sequence >= 0)",
            ]
        );
        assert!(
            postgres_down_before_dependency_check()
                .into_iter()
                .chain(postgres_down_after_dependency_check())
                .all(|statement| !statement.contains("CASCADE"))
        );
    }

    #[tokio::test]
    async fn lifecycle_abort_migration_failure_before_swap_preserves_old_table_rows_and_indexes() {
        let db = phase_a_db().await;
        let before_row = phase_a_snapshot(&db).await;
        let before_table = table_sql(&db).await;
        let before_indexes = explicit_indexes(&db).await;
        let before_markers = migration_versions(&db).await;

        let result = FAIL_AFTER_COPY_BEFORE_SWAP
            .scope((), LifecycleAbortMigrator::up(&db, None))
            .await;

        assert!(matches!(
            result,
            Err(DbErr::Custom(message))
                if message == "injected lifecycle abort migration failure before swap"
        ));
        assert_eq!(phase_a_snapshot(&db).await, before_row);
        assert_eq!(table_sql(&db).await, before_table);
        assert_eq!(explicit_indexes(&db).await, before_indexes);
        assert_eq!(migration_versions(&db).await, before_markers);
        let marker = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT version FROM seaql_migrations \
                 WHERE version = 'm20260901_000001_lifecycle_abort_multipart'",
            ))
            .await
            .unwrap();
        assert!(marker.is_none());
        let rebuild = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT 1 FROM sqlite_master \
                 WHERE type = 'table' AND name = 'lifecycle_actions_abort_rebuild'",
            ))
            .await
            .unwrap();
        assert!(rebuild.is_none());
    }

    #[tokio::test]
    async fn lifecycle_abort_migration_failure_hook_is_task_local() {
        let injected_db = phase_a_db().await;
        let ordinary_db = phase_a_db().await;
        let injected_manager = SchemaManager::new(&injected_db);
        let ordinary_manager = SchemaManager::new(&ordinary_db);

        let (injected, ordinary) = tokio::join!(
            FAIL_AFTER_COPY_BEFORE_SWAP.scope((), Migration.up(&injected_manager),),
            Migration.up(&ordinary_manager),
        );

        assert!(matches!(
            injected,
            Err(DbErr::Custom(message))
                if message == "injected lifecycle abort migration failure before swap"
        ));
        assert!(ordinary.is_ok(), "failure hook leaked: {ordinary:?}");
        assert!(!columns(&injected_db).await.contains_key("target_type"));
        assert_eq!(
            columns(&ordinary_db).await["target_type"],
            ("TEXT".to_owned(), 1)
        );
    }

    #[tokio::test]
    async fn lifecycle_abort_down_refuses_multipart_or_abort_state_and_restores_phase_a_shape() {
        for refusal in ["multipart", "abort_kind", "upload_id", "upload_created_at"] {
            let db = phase_a_db().await;
            Migration.up(&SchemaManager::new(&db)).await.unwrap();
            match refusal {
                "multipart" => insert_multipart_action(&db, "refuse-multipart").await,
                "abort_kind" => {
                    db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
                        .await
                        .unwrap();
                    db.execute_unprepared(
                        "UPDATE lifecycle_actions \
                         SET action_kind = 'abort_incomplete_multipart_upload' \
                         WHERE id = 'phase-a-action'",
                    )
                    .await
                    .unwrap();
                }
                "upload_id" => {
                    db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
                        .await
                        .unwrap();
                    db.execute_unprepared(
                        "UPDATE lifecycle_actions SET target_upload_id = 'unexpected' \
                         WHERE id = 'phase-a-action'",
                    )
                    .await
                    .unwrap();
                }
                "upload_created_at" => {
                    db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
                        .await
                        .unwrap();
                    db.execute_unprepared(
                        "UPDATE lifecycle_actions SET target_upload_created_at = CURRENT_TIMESTAMP \
                         WHERE id = 'phase-a-action'",
                    )
                    .await
                    .unwrap();
                }
                _ => unreachable!(),
            }

            let result = Migration.down(&SchemaManager::new(&db)).await;
            assert!(
                matches!(result, Err(DbErr::Migration(ref message)) if message == "lifecycle abort schema contains multipart state"),
                "down accepted {refusal}: {result:?}"
            );
            assert!(columns(&db).await.contains_key("target_type"));
        }

        let db = phase_a_db().await;
        let before = phase_a_snapshot(&db).await;
        Migration.up(&SchemaManager::new(&db)).await.unwrap();
        Migration.down(&SchemaManager::new(&db)).await.unwrap();

        assert_eq!(phase_a_snapshot(&db).await, before);
        let columns = columns(&db).await;
        assert_eq!(columns["target_version_row_id"].1, 1);
        assert_eq!(columns["target_public_version_id"].1, 1);
        assert_eq!(columns["target_sequence"].1, 1);
        assert_eq!(columns["target_object_id"].1, 0);
        for absent in [
            "target_type",
            "target_upload_id",
            "target_upload_created_at",
        ] {
            assert!(!columns.contains_key(absent), "down retained {absent}");
        }

        let table = normalize_sql(&table_sql(&db).await);
        let exact_phase_a_checks = [
            "CONSTRAINT ck_lifecycle_actions_config_revision CHECK (config_revision > 0)",
            "CONSTRAINT ck_lifecycle_actions_action_kind CHECK (action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker'))",
            "CONSTRAINT ck_lifecycle_actions_target_sequence CHECK (target_sequence >= 0)",
            "CONSTRAINT ck_lifecycle_actions_state CHECK (state IN ('pending', 'claimed', 'succeeded', 'cancelled', 'failed_safe'))",
            "CONSTRAINT ck_lifecycle_actions_attempts CHECK (attempts >= 0)",
            "CONSTRAINT ck_lifecycle_actions_claim_epoch CHECK (claim_epoch >= 0)",
            "CONSTRAINT ck_lifecycle_action_claim CHECK ((state = 'claimed' AND lease_until IS NOT NULL AND claimed_by IS NOT NULL) OR (state <> 'claimed' AND lease_until IS NULL AND claimed_by IS NULL))",
            "CONSTRAINT ck_lifecycle_action_terminal CHECK ((state IN ('succeeded', 'cancelled', 'failed_safe') AND finished_at IS NOT NULL) OR (state IN ('pending', 'claimed') AND finished_at IS NULL))",
        ];
        assert_eq!(table.matches("CONSTRAINT ck_").count(), 8);
        for check in exact_phase_a_checks {
            assert!(
                table.contains(check),
                "missing exact Phase A check: {check}"
            );
        }
        assert!(!table.contains("ck_lifecycle_actions_target_shape"));
        assert!(!table.contains("ck_lifecycle_actions_kind_target"));

        assert_eq!(
            explicit_indexes(&db).await,
            BTreeMap::from([
                (
                    "idx_lifecycle_actions_bucket_revision".to_owned(),
                    "CREATE INDEX idx_lifecycle_actions_bucket_revision ON lifecycle_actions(bucket, config_revision, id)".to_owned(),
                ),
                (
                    "idx_lifecycle_actions_due".to_owned(),
                    "CREATE INDEX idx_lifecycle_actions_due ON lifecycle_actions(state, next_attempt_at, due_at, id)".to_owned(),
                ),
                (
                    "idx_lifecycle_actions_reclaim".to_owned(),
                    "CREATE INDEX idx_lifecycle_actions_reclaim ON lifecycle_actions(state, lease_until, id)".to_owned(),
                ),
                (
                    "idx_lifecycle_actions_target".to_owned(),
                    "CREATE INDEX idx_lifecycle_actions_target ON lifecycle_actions(bucket, object_key, target_version_row_id)".to_owned(),
                ),
            ])
        );
    }
}
