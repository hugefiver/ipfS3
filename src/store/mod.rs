pub mod bucket;
pub mod entities;
pub mod import;
pub mod migrations;
pub mod multipart;
pub mod object;
pub mod pinning;

use sea_orm::DatabaseConnection;

/// Busy timeout applied to every SQLite connection. Without it, a concurrent
/// writer fails immediately with `database is locked (code 5)` instead of
/// waiting for the current writer to finish.
pub const SQLITE_BUSY_TIMEOUT_MS: i32 = 5_000;
pub const POSTGRES_MIGRATION_LOCK_KEY_1: i32 = 1_229_997_651;
pub const POSTGRES_MIGRATION_LOCK_KEY_2: i32 = 1_395_879_239;

pub fn is_sqlite_url(database_url: &str) -> bool {
    database_url.starts_with("sqlite:")
}

/// Apply the SQLite busy timeout to `options`. No-op for non-SQLite URLs.
pub fn apply_sqlite_busy_timeout(options: &mut sea_orm::ConnectOptions) {
    if !is_sqlite_url(options.get_url()) {
        return;
    }
    let timeout = std::time::Duration::from_millis(SQLITE_BUSY_TIMEOUT_MS as u64);
    options.map_sqlx_sqlite_opts(move |sqlite_opts| sqlite_opts.busy_timeout(timeout));
}

/// Connect to the configured database, applying the SQLite busy timeout.
pub async fn connect_database(database_url: &str) -> Result<DatabaseConnection, sea_orm::DbErr> {
    let mut options = sea_orm::ConnectOptions::new(database_url.to_owned());
    apply_sqlite_busy_timeout(&mut options);
    sea_orm::Database::connect(options).await
}

#[derive(Clone)]
pub struct Store {
    db: DatabaseConnection,
}

impl Store {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }
}

mod migrator {
    use crate::store::migrations::m20250701_000001_init::Migration as InitMigration;
    use crate::store::migrations::m20260707_000001_decompress_zip::Migration as DecompressZipMigration;
    use crate::store::migrations::m20260720_000001_sse_c_key_fingerprint::Migration as SseCKeyFingerprintMigration;
    use crate::store::migrations::m20260721_000001_multi_provider_pinning::Migration as MultiProviderPinningMigration;
    use crate::store::migrations::m20260729_000001_ipfs3_import::Migration as Ipfs3ImportMigration;
    use crate::store::migrations::m20260729_000002_postgres_utc_timestamps::Migration as PostgresUtcTimestampsMigration;
    use crate::store::migrations::m20260730_000001_standard_mutation_fence::Migration as StandardMutationFenceMigration;
    use crate::store::migrations::m20260813_000001_postgres_json_columns::Migration as PostgresJsonColumnsMigration;
    use sea_orm_migration::prelude::*;

    pub struct Migrator;
    impl MigratorTrait for Migrator {
        fn migrations() -> Vec<Box<dyn MigrationTrait>> {
            vec![
                Box::new(InitMigration),
                Box::new(DecompressZipMigration),
                Box::new(SseCKeyFingerprintMigration),
                Box::new(MultiProviderPinningMigration),
                Box::new(Ipfs3ImportMigration),
                Box::new(PostgresUtcTimestampsMigration),
                Box::new(StandardMutationFenceMigration),
                Box::new(PostgresJsonColumnsMigration),
            ]
        }
    }
}

fn postgres_migration_failure(category: &'static str) -> sea_orm::DbErr {
    tracing::error!(migration_lock = "failure", category);
    sea_orm::DbErr::Custom(format!("PostgreSQL migration {category} failed"))
}

async fn run_postgres_migrations(db: &sea_orm::DatabaseConnection) -> Result<(), sea_orm::DbErr> {
    use sea_orm::{ConnectionTrait, TransactionTrait};
    use sea_orm_migration::MigratorTrait;

    let txn = db
        .begin()
        .await
        .map_err(|_| postgres_migration_failure("setup"))?;
    txn.execute_unprepared("SET LOCAL lock_timeout = '60s'")
        .await
        .map_err(|_| postgres_migration_failure("setup"))?;
    tracing::info!(migration_lock = "waiting");
    txn.execute_unprepared("SELECT pg_advisory_xact_lock(1229997651, 1395879239)")
        .await
        .map_err(|_| postgres_migration_failure("timeout"))?;
    tracing::info!(migration_lock = "acquired");
    migrator::Migrator::up(&txn, None)
        .await
        .map_err(|_| postgres_migration_failure("migration"))?;
    txn.commit()
        .await
        .map_err(|_| postgres_migration_failure("commit"))
}

pub async fn run_migrations(db: &sea_orm::DatabaseConnection) -> Result<(), sea_orm::DbErr> {
    use sea_orm::ConnectionTrait;
    use sea_orm_migration::MigratorTrait;

    if db.get_database_backend() != sea_orm::DatabaseBackend::Postgres {
        return migrator::Migrator::up(db, None).await;
    }
    run_postgres_migrations(db).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::ConnectionTrait;
    use sea_orm_migration::MigratorTrait;

    #[test]
    fn postgres_json_columns_migration_is_registered_last() {
        let names = migrator::Migrator::migrations()
            .into_iter()
            .map(|migration| migration.name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "m20250701_000001_init",
                "m20260707_000001_decompress_zip",
                "m20260720_000001_sse_c_key_fingerprint",
                "m20260721_000001_multi_provider_pinning",
                "m20260729_000001_ipfs3_import",
                "m20260729_000002_postgres_utc_timestamps",
                "m20260730_000001_standard_mutation_fence",
                "m20260813_000001_postgres_json_columns",
            ]
        );
    }

    #[tokio::test]
    async fn file_backed_sqlite_connections_have_a_five_second_busy_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("busy-timeout.db");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );

        let db = connect_database(&database_url).await.unwrap();

        let row = db
            .query_one(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "PRAGMA busy_timeout",
            ))
            .await
            .unwrap()
            .expect("PRAGMA busy_timeout must return a row");
        let timeout_ms: i32 = row.try_get_by(0).unwrap();
        assert_eq!(
            timeout_ms, SQLITE_BUSY_TIMEOUT_MS,
            "file-backed SQLite connections must wait instead of failing with `database is locked`"
        );
    }

    #[test]
    fn busy_timeout_is_only_applied_to_sqlite_urls() {
        assert!(is_sqlite_url("sqlite:///data/ipfs-s3.db"));
        assert!(is_sqlite_url("sqlite::memory:"));
        assert!(!is_sqlite_url("postgres://user:pw@localhost/ipfs_s3"));
        assert!(!is_sqlite_url("postgresql://user:pw@localhost/ipfs_s3"));
    }

    #[tokio::test]
    async fn test_migration_runs() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();
        let rows = db
            .query_one(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT GROUP_CONCAT(name, ',') FROM (\
                 SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (\
                     'buckets', 'objects', 'multipart_uploads', 'multipart_parts', \
                      'object_tags', 'pin_leases', 'pin_lease_targets', 'remote_pins', \
                      'pin_jobs', 'pin_provider_usage', 'import_jobs', 'import_destinations', \
                      'import_prefix_claims', 'import_job_targets', 'import_job_results'\
                 ) ORDER BY name)",
                [],
            ))
            .await
            .unwrap()
            .unwrap();
        let names: String = rows.try_get_by(0).unwrap();
        let table_names: std::collections::BTreeSet<_> = names.split(',').collect();
        let expected: std::collections::BTreeSet<_> = [
            "buckets",
            "objects",
            "multipart_uploads",
            "multipart_parts",
            "object_tags",
            "pin_leases",
            "pin_lease_targets",
            "remote_pins",
            "pin_jobs",
            "pin_provider_usage",
            "import_jobs",
            "import_destinations",
            "import_prefix_claims",
            "import_job_targets",
            "import_job_results",
        ]
        .into_iter()
        .collect();
        assert_eq!(
            table_names, expected,
            "all fifteen application tables must exist"
        );
    }

    #[tokio::test]
    async fn migrations_create_import_tables() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();

        let rows = db
            .query_all(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (\
                     'import_jobs', 'import_destinations', 'import_prefix_claims', \
                     'import_job_targets', 'import_job_results'\
                 ) ORDER BY name",
            ))
            .await
            .unwrap();
        let names: Vec<String> = rows.iter().map(|row| row.try_get_by(0).unwrap()).collect();

        assert_eq!(
            names,
            [
                "import_destinations",
                "import_job_results",
                "import_job_targets",
                "import_jobs",
                "import_prefix_claims",
            ],
            "all five import tables must exist"
        );
    }

    #[tokio::test]
    async fn test_multipart_upload_and_object_extension_columns_exist() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();

        let rows = db
            .query_all(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                "PRAGMA table_info(multipart_uploads)",
                [],
            ))
            .await
            .unwrap();

        let names: Vec<String> = rows
            .iter()
            .map(|row| row.try_get::<String>("", "name").unwrap())
            .collect();

        assert!(
            names.contains(&"decompress_zip_target".to_string()),
            "multipart_uploads must persist the decompression target prefix"
        );
        assert!(
            names.contains(&"decompress_zip_result".to_string()),
            "multipart_uploads must persist whether Complete returns DecompressZipResult XML"
        );
        assert!(
            names.contains(&"sse_c_key_fingerprint".to_string()),
            "multipart_uploads must persist the SSE-C key fingerprint"
        );
        assert!(
            names.contains(&"tags_json".to_string()),
            "multipart_uploads must persist object tags as JSON"
        );

        let object_rows = db
            .query_all(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                "PRAGMA table_info(objects)",
                [],
            ))
            .await
            .unwrap();
        let object_names: Vec<String> = object_rows
            .iter()
            .map(|row| row.try_get::<String>("", "name").unwrap())
            .collect();
        assert!(
            object_names.contains(&"sse_c_key_fingerprint".to_string()),
            "objects must persist the SSE-C key fingerprint"
        );
    }
}
