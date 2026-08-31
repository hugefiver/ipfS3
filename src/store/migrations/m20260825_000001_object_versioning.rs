use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};
use sea_orm_migration::prelude::*;

const BACKFILL_COUNT_MISMATCH: &str = "object_versions backfill count mismatch";
const PUBLIC_STATE_DOWN_REFUSAL: &str = "object versioning schema contains public state";

const INDEX_NAMES: [&str; 7] = [
    "uq_object_versions_latest",
    "uq_object_versions_sequence",
    "uq_object_versions_null_slot",
    "uq_object_versions_public_id",
    "idx_object_versions_exact",
    "idx_object_versions_key_order",
    "idx_object_versions_bucket_order",
];

#[derive(DeriveMigrationName)]
pub struct Migration;

fn object_versions_timestamp_type(backend: DatabaseBackend) -> &'static str {
    if backend == DatabaseBackend::Postgres {
        "TIMESTAMPTZ"
    } else {
        "TIMESTAMP"
    }
}

fn up_schema_statements(backend: DatabaseBackend) -> Vec<String> {
    let add_bucket_status = if backend == DatabaseBackend::Postgres {
        vec![
            "ALTER TABLE buckets ADD COLUMN versioning_status TEXT".to_owned(),
            "ALTER TABLE buckets ADD CONSTRAINT ck_buckets_versioning_status \
             CHECK (versioning_status IS NULL OR versioning_status IN ('Enabled', 'Suspended'))"
                .to_owned(),
        ]
    } else {
        vec![
            "ALTER TABLE buckets ADD COLUMN versioning_status TEXT \
             CHECK (versioning_status IS NULL OR versioning_status IN ('Enabled', 'Suspended'))"
                .to_owned(),
        ]
    };
    let mut statements = add_bucket_status;
    statements.push(format!(
        "CREATE TABLE object_versions (\
             id TEXT PRIMARY KEY NOT NULL, \
             bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE, \
             key TEXT NOT NULL, \
             version_id TEXT, \
             kind TEXT NOT NULL, \
             object_id TEXT REFERENCES objects(id) ON DELETE CASCADE, \
             sequence BIGINT NOT NULL, \
             is_latest BOOLEAN NOT NULL, \
             created_at {} NOT NULL, \
             updated_at {} NOT NULL, \
             CONSTRAINT ck_object_versions_kind \
                 CHECK (kind IN ('object', 'delete_marker')), \
             CONSTRAINT ck_object_versions_kind_object \
                 CHECK ((kind = 'object' AND object_id IS NOT NULL) OR \
                        (kind = 'delete_marker' AND object_id IS NULL))\
         )",
        object_versions_timestamp_type(backend),
        object_versions_timestamp_type(backend),
    ));
    statements.extend([
        "CREATE UNIQUE INDEX uq_object_versions_latest \
         ON object_versions(bucket, key) WHERE is_latest = TRUE"
            .to_owned(),
        "CREATE UNIQUE INDEX uq_object_versions_sequence \
         ON object_versions(bucket, key, sequence)"
            .to_owned(),
        "CREATE UNIQUE INDEX uq_object_versions_null_slot \
         ON object_versions(bucket, key) WHERE version_id IS NULL"
            .to_owned(),
        "CREATE UNIQUE INDEX uq_object_versions_public_id \
         ON object_versions(bucket, key, version_id) WHERE version_id IS NOT NULL"
            .to_owned(),
        "CREATE INDEX idx_object_versions_exact \
         ON object_versions(bucket, key, version_id)"
            .to_owned(),
        "CREATE INDEX idx_object_versions_key_order \
         ON object_versions(bucket, key, sequence DESC)"
            .to_owned(),
        "CREATE INDEX idx_object_versions_bucket_order \
         ON object_versions(bucket, key ASC, sequence DESC, version_id)"
            .to_owned(),
    ]);
    statements
}

fn down_schema_statements() -> Vec<String> {
    let mut statements = INDEX_NAMES
        .iter()
        .rev()
        .map(|name| format!("DROP INDEX IF EXISTS {name}"))
        .collect::<Vec<_>>();
    statements.push("DROP TABLE object_versions".to_owned());
    statements.push("ALTER TABLE buckets DROP COLUMN versioning_status".to_owned());
    statements
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

fn backfill_select_statement(backend: DatabaseBackend) -> Statement {
    Statement::from_string(
        backend,
        "SELECT id, bucket, key, created_at \
         FROM objects \
         WHERE is_latest = TRUE \
         ORDER BY bucket ASC, key ASC, id ASC",
    )
}

fn backfill_insert_statement(
    backend: DatabaseBackend,
    version_row_id: String,
    bucket: String,
    key: String,
    object_id: String,
    created_at: chrono::DateTime<chrono::Utc>,
) -> Statement {
    let sql = if backend == DatabaseBackend::Postgres {
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
         VALUES ($1, $2, $3, NULL, 'object', $4, 1, TRUE, $5, $5)"
    } else {
        "INSERT INTO object_versions \
         (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
         VALUES (?, ?, ?, NULL, 'object', ?, 1, TRUE, ?, ?)"
    };
    let values = if backend == DatabaseBackend::Postgres {
        vec![
            version_row_id.into(),
            bucket.into(),
            key.into(),
            object_id.into(),
            created_at.into(),
        ]
    } else {
        vec![
            version_row_id.into(),
            bucket.into(),
            key.into(),
            object_id.into(),
            created_at.into(),
            created_at.into(),
        ]
    };
    Statement::from_sql_and_values(backend, sql, values)
}

#[cfg(test)]
tokio::task_local! {
    static BACKFILL_INSERT_INJECTION: u8;
}

fn backfill_insert_injection_mode() -> u8 {
    #[cfg(test)]
    {
        BACKFILL_INSERT_INJECTION
            .try_with(|mode| *mode)
            .unwrap_or_default()
    }
    #[cfg(not(test))]
    {
        0
    }
}

async fn execute_backfill_insert(
    connection: &impl ConnectionTrait,
    statement: Statement,
) -> Result<u64, DbErr> {
    match backfill_insert_injection_mode() {
        1 => {
            return Err(DbErr::Custom(
                "injected object_versions insert failure".to_owned(),
            ));
        }
        2 => return Ok(0),
        _ => {}
    }
    Ok(connection.execute(statement).await?.rows_affected())
}

async fn backfill_object_versions(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    let backend = connection.get_database_backend();
    let selected_rows = connection
        .query_all(backfill_select_statement(backend))
        .await?;
    let selected_count = selected_rows.len() as u64;
    let mut inserted_count = 0;

    for row in selected_rows {
        let object_id: String = row.try_get("", "id")?;
        let bucket: String = row.try_get("", "bucket")?;
        let key: String = row.try_get("", "key")?;
        let created_at: chrono::DateTime<chrono::Utc> = row.try_get("", "created_at")?;
        inserted_count += execute_backfill_insert(
            connection,
            backfill_insert_statement(
                backend,
                uuid::Uuid::new_v4().to_string(),
                bucket,
                key,
                object_id,
                created_at,
            ),
        )
        .await?;
    }

    if inserted_count != selected_count {
        return Err(DbErr::Migration(BACKFILL_COUNT_MISMATCH.to_owned()));
    }
    Ok(())
}

async fn schema_contains_public_state(connection: &impl ConnectionTrait) -> Result<bool, DbErr> {
    let statement = Statement::from_string(
        connection.get_database_backend(),
        "SELECT 1 \
         WHERE EXISTS (SELECT 1 FROM buckets WHERE versioning_status IS NOT NULL) \
            OR EXISTS (SELECT 1 FROM object_versions \
                       WHERE version_id IS NOT NULL OR kind = 'delete_marker')",
    );
    Ok(connection.query_one(statement).await?.is_some())
}

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
            return match result {
                Ok(()) => transaction.commit().await,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error)
                }
            };
        }
        apply_up(connection).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        if connection.get_database_backend() == DatabaseBackend::Sqlite {
            let transaction = connection.begin().await?;
            let result = apply_down(&transaction).await;
            return match result {
                Ok(()) => transaction.commit().await,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error)
                }
            };
        }
        apply_down(connection).await
    }
}

async fn apply_up(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    execute_statements(
        connection,
        up_schema_statements(connection.get_database_backend()),
    )
    .await?;
    backfill_object_versions(connection).await
}

async fn apply_down(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    if schema_contains_public_state(connection).await? {
        return Err(DbErr::Migration(PUBLIC_STATE_DOWN_REFUSAL.to_owned()));
    }
    execute_statements(connection, down_schema_statements()).await
}

#[cfg(test)]
mod tests {
    use std::{future::Future, sync::Arc, time::Duration};

    use sea_orm::{
        ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
        TransactionTrait,
    };
    use sea_orm_migration::MigratorTrait;

    use super::*;
    use crate::store::migrations::{
        m20250701_000001_init, m20260707_000001_decompress_zip,
        m20260720_000001_sse_c_key_fingerprint, m20260721_000001_multi_provider_pinning,
        m20260729_000001_ipfs3_import, m20260729_000002_postgres_utc_timestamps,
        m20260730_000001_standard_mutation_fence, m20260813_000001_postgres_json_columns,
    };

    struct PreObjectVersioningMigrator;

    impl MigratorTrait for PreObjectVersioningMigrator {
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
            vec![
                Box::new(m20250701_000001_init::Migration),
                Box::new(m20260707_000001_decompress_zip::Migration),
                Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
                Box::new(m20260721_000001_multi_provider_pinning::Migration),
                Box::new(m20260729_000001_ipfs3_import::Migration),
                Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
                Box::new(m20260730_000001_standard_mutation_fence::Migration),
                Box::new(m20260813_000001_postgres_json_columns::Migration),
                Box::new(Migration),
            ]
        }
    }

    struct OwnedPostgresSchema {
        url: String,
        schema: Option<String>,
    }

    impl OwnedPostgresSchema {
        fn new(url: String, schema: String) -> Self {
            assert!(is_owned_postgres_schema(&schema));
            Self {
                url,
                schema: Some(schema),
            }
        }

        async fn cleanup(&mut self, db: &DatabaseConnection) {
            let schema = self.schema.as_deref().expect("owned schema is armed");
            db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
                .await
                .unwrap();
            self.schema = None;
        }
    }

    impl Drop for OwnedPostgresSchema {
        fn drop(&mut self) {
            let Some(schema) = self.schema.take() else {
                return;
            };
            if !is_owned_postgres_schema(&schema) {
                return;
            }
            let url = self.url.clone();
            if let Ok(thread) = std::thread::Builder::new()
                .name("object-versioning-pg-cleanup".to_owned())
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
                        let Ok(Ok(db)) = tokio::time::timeout(
                            Duration::from_secs(10),
                            Database::connect(options),
                        )
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

    fn is_owned_postgres_schema(schema: &str) -> bool {
        schema.strip_prefix("versioning_").is_some_and(|suffix| {
            suffix.len() == 32
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    }

    async fn legacy_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        PreObjectVersioningMigrator::up(&db, None).await.unwrap();
        db
    }

    async fn apply_object_versioning_migration(db: &DatabaseConnection) {
        ObjectVersioningMigrator::up(db, None).await.unwrap();
    }

    async fn with_backfill_insert_injection<T>(mode: u8, future: impl Future<Output = T>) -> T {
        BACKFILL_INSERT_INJECTION.scope(mode, future).await
    }

    async fn assert_rejected(db: &DatabaseConnection, statement: &str) {
        assert!(
            db.execute_unprepared(statement).await.is_err(),
            "SQLite accepted invalid object-versioning input: {statement}"
        );
    }

    async fn insert_legacy_rows(db: &DatabaseConnection) {
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('versioning-bucket')")
            .await
            .unwrap();
        for statement in [
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
             VALUES ('legacy-a', 'versioning-bucket', 'alpha', 'QmLegacyA', 1, 'QmLegacyA', FALSE, \
                     '2026-08-25 00:00:00+00:00')",
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
             VALUES ('current-a', 'versioning-bucket', 'alpha', 'QmCurrentA', 2, 'QmCurrentA', TRUE, \
                     '2026-08-25 00:01:00+00:00')",
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
             VALUES ('current-b', 'versioning-bucket', 'beta', 'QmCurrentB', 3, 'QmCurrentB', TRUE, \
                     '2026-08-25 00:02:00+00:00')",
        ] {
            db.execute_unprepared(statement).await.unwrap();
        }
    }

    async fn object_versions_count(db: &DatabaseConnection) -> i64 {
        db.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM object_versions",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "count")
        .unwrap()
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

    async fn bucket_has_versioning_status(db: &DatabaseConnection) -> bool {
        db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA table_info(buckets)",
        ))
        .await
        .unwrap()
        .iter()
        .any(|row| row.try_get::<String>("", "name").unwrap() == "versioning_status")
    }

    async fn assert_failed_migration_rolled_back(db: &DatabaseConnection) {
        assert!(!table_exists(db, "object_versions").await);
        assert!(!bucket_has_versioning_status(db).await);
    }

    #[tokio::test]
    async fn object_versioning_injection_scope_does_not_leak_to_an_ordinary_migration() {
        let injected_db = legacy_db().await;
        insert_legacy_rows(&injected_db).await;
        let ordinary_db = legacy_db().await;
        insert_legacy_rows(&ordinary_db).await;
        let entered_injection = Arc::new(tokio::sync::Barrier::new(2));
        let release_injection = Arc::new(tokio::sync::Notify::new());

        let injected_barrier = entered_injection.clone();
        let injected_release = release_injection.clone();
        let injected = tokio::spawn(async move {
            with_backfill_insert_injection(1, async {
                injected_barrier.wait().await;
                injected_release.notified().await;
                ObjectVersioningMigrator::up(&injected_db, None).await
            })
            .await
        });
        entered_injection.wait().await;

        let ordinary =
            tokio::spawn(async move { ObjectVersioningMigrator::up(&ordinary_db, None).await });
        let ordinary_result = ordinary.await.unwrap();
        release_injection.notify_one();
        let injected_result = injected.await.unwrap();

        assert!(matches!(
            injected_result,
            Err(DbErr::Custom(message)) if message == "injected object_versions insert failure"
        ));
        assert!(
            ordinary_result.is_ok(),
            "ordinary migration observed an injected mode: {ordinary_result:?}"
        );
    }

    #[tokio::test]
    async fn object_versioning_injection_scope_is_nestable_and_defaults_to_zero() {
        assert_eq!(backfill_insert_injection_mode(), 0);
        with_backfill_insert_injection(1, async {
            assert_eq!(backfill_insert_injection_mode(), 1);
            with_backfill_insert_injection(2, async {
                assert_eq!(backfill_insert_injection_mode(), 2);
            })
            .await;
            assert_eq!(backfill_insert_injection_mode(), 1);
        })
        .await;
        assert_eq!(backfill_insert_injection_mode(), 0);
    }

    #[tokio::test]
    async fn object_versioning_migration_adds_status_table_indexes_and_checks() {
        let db = legacy_db().await;
        insert_legacy_rows(&db).await;
        apply_object_versioning_migration(&db).await;

        assert!(bucket_has_versioning_status(&db).await);
        assert!(table_exists(&db, "object_versions").await);
        let indexes = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'object_versions'",
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "name").unwrap())
            .collect::<Vec<_>>();
        for name in INDEX_NAMES {
            assert!(
                indexes.contains(&name.to_owned()),
                "missing index {name}: {indexes:?}"
            );
        }

        assert_rejected(
            &db,
            "UPDATE buckets SET versioning_status = 'enabled' WHERE name = 'versioning-bucket'",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('invalid-kind', 'versioning-bucket', 'invalid-kind', NULL, 'unexpected', \
                     'current-a', 1, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('invalid-marker-object', 'versioning-bucket', 'invalid-marker', NULL, \
                     'delete_marker', 'current-a', 1, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('invalid-object-null', 'versioning-bucket', 'invalid-object', NULL, 'object', \
                     NULL, 1, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;

        db.execute_unprepared(
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('unique-source', 'versioning-bucket', 'unique', 'version-1', 'object', \
                     'current-a', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        assert_rejected(
            &db,
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('duplicate-latest', 'versioning-bucket', 'unique', 'version-2', 'object', \
                     'current-a', 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('duplicate-sequence', 'versioning-bucket', 'unique', 'version-3', 'object', \
                     'current-a', 1, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        assert_rejected(
            &db,
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('duplicate-public-id', 'versioning-bucket', 'unique', 'version-1', 'object', \
                     'current-a', 3, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
        db.execute_unprepared(
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('null-source', 'versioning-bucket', 'null-slot', NULL, 'object', 'current-a', \
                     1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        assert_rejected(
            &db,
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('duplicate-null-slot', 'versioning-bucket', 'null-slot', NULL, 'object', \
                     'current-a', 2, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await;
    }

    #[tokio::test]
    async fn object_versioning_backfills_only_legacy_latest_as_hidden_null() {
        let db = legacy_db().await;
        insert_legacy_rows(&db).await;
        apply_object_versioning_migration(&db).await;

        let rows = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT key, version_id, kind, object_id, sequence, is_latest \
                 FROM object_versions ORDER BY key ASC",
            ))
            .await
            .unwrap();
        let values = rows
            .iter()
            .map(|row| {
                (
                    row.try_get::<String>("", "key").unwrap(),
                    row.try_get::<Option<String>>("", "version_id").unwrap(),
                    row.try_get::<String>("", "kind").unwrap(),
                    row.try_get::<Option<String>>("", "object_id").unwrap(),
                    row.try_get::<i64>("", "sequence").unwrap(),
                    row.try_get::<bool>("", "is_latest").unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            [
                (
                    "alpha".to_owned(),
                    None,
                    "object".to_owned(),
                    Some("current-a".to_owned()),
                    1,
                    true,
                ),
                (
                    "beta".to_owned(),
                    None,
                    "object".to_owned(),
                    Some("current-b".to_owned()),
                    1,
                    true,
                ),
            ]
        );
        let timestamps = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT versions.created_at AS version_created_at, \
                        versions.updated_at AS version_updated_at, \
                        object.created_at AS object_created_at \
                 FROM object_versions versions \
                 JOIN objects object ON object.id = versions.object_id",
            ))
            .await
            .unwrap();
        for row in timestamps {
            let version_created_at: chrono::DateTime<chrono::Utc> =
                row.try_get("", "version_created_at").unwrap();
            let version_updated_at: chrono::DateTime<chrono::Utc> =
                row.try_get("", "version_updated_at").unwrap();
            let object_created_at: chrono::DateTime<chrono::Utc> =
                row.try_get("", "object_created_at").unwrap();
            assert_eq!(version_created_at, object_created_at);
            assert_eq!(version_updated_at, object_created_at);
        }
        assert_eq!(
            object_versions_count(&db).await,
            2,
            "legacy non-latest object rows must remain hidden"
        );
    }

    #[tokio::test]
    async fn object_versioning_backfill_count_must_match() {
        let db = legacy_db().await;
        insert_legacy_rows(&db).await;
        let transaction = db.begin().await.unwrap();
        let manager = SchemaManager::new(&transaction);
        let result = with_backfill_insert_injection(2, Migration.up(&manager)).await;

        assert!(matches!(
            result,
            Err(DbErr::Migration(message)) if message == BACKFILL_COUNT_MISMATCH
        ));
        transaction.rollback().await.unwrap();
        assert_failed_migration_rolled_back(&db).await;
    }

    #[tokio::test]
    async fn object_versioning_migration_insert_failure_rolls_back() {
        let db = legacy_db().await;
        insert_legacy_rows(&db).await;
        let result =
            with_backfill_insert_injection(1, ObjectVersioningMigrator::up(&db, None)).await;

        assert!(result.is_err());
        assert_failed_migration_rolled_back(&db).await;
    }

    #[tokio::test]
    async fn postgres_object_versioning_migration_rolls_back_injected_failure() {
        let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
            eprintln!(
                "skipping PostgreSQL migration rollback test: IPFS_S3_TEST_POSTGRES_URL is unset"
            );
            return;
        };
        let schema = format!("versioning_{}", uuid::Uuid::new_v4().simple());
        let mut cleanup = OwnedPostgresSchema::new(url.clone(), schema.clone());
        let mut options = ConnectOptions::new(url);
        options.max_connections(1).min_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .unwrap();
        PreObjectVersioningMigrator::up(&db, None).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('versioning-bucket')")
            .await
            .unwrap();
        db.execute_unprepared(
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
             VALUES ('current-a', 'versioning-bucket', 'alpha', 'QmCurrentA', 2, 'QmCurrentA', TRUE, \
                     '2026-08-25 00:01:00+00:00')",
        )
        .await
        .unwrap();

        let result =
            with_backfill_insert_injection(1, ObjectVersioningMigrator::up(&db, None)).await;
        assert!(matches!(
            result,
            Err(DbErr::Custom(message)) if message == "injected object_versions insert failure"
        ));

        let object_versions_exists = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = current_schema() AND table_name = 'object_versions'",
            ))
            .await
            .unwrap();
        assert!(object_versions_exists.is_none());
        let status_column_exists = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = current_schema() \
                   AND table_name = 'buckets' \
                   AND column_name = 'versioning_status'",
            ))
            .await
            .unwrap();
        assert!(status_column_exists.is_none());
        let ninth_marker_exists = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT 1 FROM seaql_migrations \
                 WHERE version = 'm20260825_000001_object_versioning'",
            ))
            .await
            .unwrap();
        assert!(ninth_marker_exists.is_none());
        let bucket: String = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT name FROM buckets",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "name")
            .unwrap();
        assert_eq!(bucket, "versioning-bucket");
        let legacy_object = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT id, bucket, key, cid, size, etag, is_latest FROM objects",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            legacy_object.try_get::<String>("", "id").unwrap(),
            "current-a"
        );
        assert_eq!(
            legacy_object.try_get::<String>("", "bucket").unwrap(),
            "versioning-bucket"
        );
        assert_eq!(legacy_object.try_get::<String>("", "key").unwrap(), "alpha");
        assert_eq!(
            legacy_object.try_get::<String>("", "cid").unwrap(),
            "QmCurrentA"
        );
        assert_eq!(legacy_object.try_get::<i64>("", "size").unwrap(), 2);
        assert_eq!(
            legacy_object.try_get::<String>("", "etag").unwrap(),
            "QmCurrentA"
        );
        assert!(legacy_object.try_get::<bool>("", "is_latest").unwrap());

        cleanup.cleanup(&db).await;
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn object_versioning_down_allows_only_hidden_null_rows() {
        let db = legacy_db().await;
        insert_legacy_rows(&db).await;
        apply_object_versioning_migration(&db).await;

        Migration.down(&SchemaManager::new(&db)).await.unwrap();

        assert!(!table_exists(&db, "object_versions").await);
        assert!(!bucket_has_versioning_status(&db).await);
    }

    #[tokio::test]
    async fn object_versioning_down_refuses_public_state_versions_and_markers() {
        for public_state_statement in [
            "UPDATE buckets SET versioning_status = 'Enabled' WHERE name = 'versioning-bucket'",
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('public-version', 'versioning-bucket', 'public', 'opaque-version', 'object', \
                     'current-a', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            "INSERT INTO object_versions \
             (id, bucket, key, version_id, kind, object_id, sequence, is_latest, created_at, updated_at) \
             VALUES ('public-marker', 'versioning-bucket', 'marker', NULL, 'delete_marker', NULL, \
                     1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        ] {
            let db = legacy_db().await;
            insert_legacy_rows(&db).await;
            apply_object_versioning_migration(&db).await;
            db.execute_unprepared(public_state_statement).await.unwrap();

            let result = Migration.down(&SchemaManager::new(&db)).await;
            assert!(matches!(
                result,
                Err(DbErr::Migration(message)) if message == PUBLIC_STATE_DOWN_REFUSAL
            ));
            assert!(table_exists(&db, "object_versions").await);
            assert!(bucket_has_versioning_status(&db).await);
        }
    }

    #[test]
    fn postgres_schema_uses_timestamptz_and_all_required_index_shapes() {
        let statements = up_schema_statements(DatabaseBackend::Postgres);
        assert!(statements[0].contains("ADD COLUMN versioning_status TEXT"));
        assert!(statements[1].contains("ck_buckets_versioning_status"));
        let table = statements
            .iter()
            .find(|statement| statement.starts_with("CREATE TABLE object_versions"))
            .unwrap();
        assert!(table.contains("created_at TIMESTAMPTZ NOT NULL"));
        assert!(table.contains("updated_at TIMESTAMPTZ NOT NULL"));
        assert!(table.contains("REFERENCES buckets(name) ON DELETE CASCADE"));
        assert!(table.contains("REFERENCES objects(id) ON DELETE CASCADE"));
        for index in INDEX_NAMES {
            assert!(
                statements.iter().any(|statement| statement.contains(index)),
                "missing {index}"
            );
        }
    }
}
