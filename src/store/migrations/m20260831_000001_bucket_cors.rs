use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, TransactionTrait};
use sea_orm_migration::prelude::*;

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

fn create_bucket_cors_statement(backend: DatabaseBackend) -> String {
    let timestamp = timestamp_type(backend);
    format!(
        "CREATE TABLE bucket_cors_configs (\
             bucket TEXT PRIMARY KEY NOT NULL REFERENCES buckets(name) ON DELETE CASCADE, \
             canonical_json TEXT NOT NULL, \
             created_at {timestamp} NOT NULL, \
             updated_at {timestamp} NOT NULL\
         )"
    )
}

#[cfg(test)]
tokio::task_local! {
    static FAIL_AFTER_CREATE: ();
}

fn should_fail_after_create() -> bool {
    #[cfg(test)]
    {
        FAIL_AFTER_CREATE.try_with(|_| ()).is_ok()
    }
    #[cfg(not(test))]
    {
        false
    }
}

async fn apply_up(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    connection
        .execute_unprepared(&create_bucket_cors_statement(
            connection.get_database_backend(),
        ))
        .await?;
    if should_fail_after_create() {
        return Err(DbErr::Custom(
            "injected bucket CORS migration failure".to_owned(),
        ));
    }
    Ok(())
}

async fn apply_down(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    connection
        .execute_unprepared("DROP TABLE bucket_cors_configs")
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};
    use sea_orm_migration::MigratorTrait;

    use super::*;
    use crate::store::migrations::{
        m20250701_000001_init, m20260707_000001_decompress_zip,
        m20260720_000001_sse_c_key_fingerprint, m20260721_000001_multi_provider_pinning,
        m20260729_000001_ipfs3_import, m20260729_000002_postgres_utc_timestamps,
        m20260730_000001_standard_mutation_fence, m20260813_000001_postgres_json_columns,
        m20260825_000001_object_versioning, m20260826_000001_lifecycle_expiration,
    };

    struct FirstTenMigrator;

    impl MigratorTrait for FirstTenMigrator {
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
            ]
        }
    }

    async fn first_ten_migrations_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        FirstTenMigrator::up(&db, None).await.unwrap();
        db
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

    async fn cors_columns(db: &DatabaseConnection) -> Vec<(String, String, i64)> {
        db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA table_info(bucket_cors_configs)",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.try_get("", "name").unwrap(),
                row.try_get("", "type").unwrap(),
                row.try_get("", "pk").unwrap(),
            )
        })
        .collect()
    }

    async fn cors_bucket_foreign_key(db: &DatabaseConnection) -> (String, String, String, String) {
        let rows = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA foreign_key_list(bucket_cors_configs)",
            ))
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "CORS configuration has one bucket foreign key"
        );
        let row = &rows[0];
        (
            row.try_get("", "from").unwrap(),
            row.try_get("", "table").unwrap(),
            row.try_get("", "to").unwrap(),
            row.try_get("", "on_delete").unwrap(),
        )
    }

    async fn bucket_exists(db: &DatabaseConnection, bucket: &str) -> bool {
        db.query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT 1 FROM buckets WHERE name = ?",
            [bucket.to_owned().into()],
        ))
        .await
        .unwrap()
        .is_some()
    }

    async fn cors_row_count(db: &DatabaseConnection) -> i64 {
        db.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM bucket_cors_configs",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "count")
        .unwrap()
    }

    async fn insert_bucket(db: &DatabaseConnection, bucket: &str) {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO buckets (name) VALUES (?)",
            [bucket.to_owned().into()],
        ))
        .await
        .unwrap();
    }

    async fn insert_cors_row(db: &DatabaseConnection, bucket: &str) {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO bucket_cors_configs (bucket, canonical_json, created_at, updated_at) \
             VALUES (?, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            [bucket.to_owned().into()],
        ))
        .await
        .unwrap();
    }

    #[test]
    fn bucket_cors_schema_uses_backend_specific_raw_timestamp_types() {
        let sqlite = create_bucket_cors_statement(DatabaseBackend::Sqlite);
        assert!(sqlite.contains("created_at TIMESTAMP NOT NULL"));
        assert!(sqlite.contains("updated_at TIMESTAMP NOT NULL"));

        let postgres = create_bucket_cors_statement(DatabaseBackend::Postgres);
        assert!(postgres.contains("created_at TIMESTAMPTZ NOT NULL"));
        assert!(postgres.contains("updated_at TIMESTAMPTZ NOT NULL"));
    }

    #[tokio::test]
    async fn bucket_cors_migration_preserves_pre_cors_bucket() {
        let db = first_ten_migrations_db().await;
        insert_bucket(&db, "existing").await;
        Migration.up(&SchemaManager::new(&db)).await.unwrap();

        assert!(bucket_exists(&db, "existing").await);
        assert_eq!(
            cors_columns(&db).await,
            vec![
                ("bucket".to_owned(), "TEXT".to_owned(), 1),
                ("canonical_json".to_owned(), "TEXT".to_owned(), 0),
                ("created_at".to_owned(), "TIMESTAMP".to_owned(), 0),
                ("updated_at".to_owned(), "TIMESTAMP".to_owned(), 0),
            ]
        );
        assert_eq!(
            cors_bucket_foreign_key(&db).await,
            (
                "bucket".to_owned(),
                "buckets".to_owned(),
                "name".to_owned(),
                "CASCADE".to_owned(),
            )
        );

        insert_cors_row(&db, "existing").await;
        db.execute_unprepared("DELETE FROM buckets WHERE name = 'existing'")
            .await
            .unwrap();
        assert_eq!(cors_row_count(&db).await, 0);
    }

    #[tokio::test]
    async fn bucket_cors_down_drops_only_the_cors_table() {
        let db = first_ten_migrations_db().await;
        insert_bucket(&db, "down-check").await;
        Migration.up(&SchemaManager::new(&db)).await.unwrap();
        insert_cors_row(&db, "down-check").await;

        Migration.down(&SchemaManager::new(&db)).await.unwrap();

        assert!(!table_exists(&db, "bucket_cors_configs").await);
        assert!(table_exists(&db, "buckets").await);
        assert!(bucket_exists(&db, "down-check").await);
    }

    #[tokio::test]
    async fn injected_bucket_cors_migration_failure_rolls_back_the_new_table() {
        let db = first_ten_migrations_db().await;
        insert_bucket(&db, "rollback-check").await;

        let result = FAIL_AFTER_CREATE
            .scope((), Migration.up(&SchemaManager::new(&db)))
            .await;

        assert!(matches!(
            result,
            Err(DbErr::Custom(message)) if message == "injected bucket CORS migration failure"
        ));
        assert!(!table_exists(&db, "bucket_cors_configs").await);
        assert!(bucket_exists(&db, "rollback-check").await);
    }
}
