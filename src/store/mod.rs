pub mod bucket;
pub mod entities;
pub mod migrations;
pub mod multipart;
pub mod object;

use sea_orm::DatabaseConnection;

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

pub async fn run_migrations(db: &DatabaseConnection) -> Result<(), sea_orm::DbErr> {
    use sea_orm_migration::MigratorTrait;

    mod migrator {
        use crate::store::migrations::m20250701_000001_init::Migration as InitMigration;
        use crate::store::migrations::m20260707_000001_decompress_zip::Migration as DecompressZipMigration;
        use crate::store::migrations::m20260720_000001_sse_c_key_fingerprint::Migration as SseCKeyFingerprintMigration;
        use sea_orm_migration::prelude::*;

        pub struct Migrator;
        impl MigratorTrait for Migrator {
            fn migrations() -> Vec<Box<dyn MigrationTrait>> {
                vec![
                    Box::new(InitMigration),
                    Box::new(DecompressZipMigration),
                    Box::new(SseCKeyFingerprintMigration),
                ]
            }
        }
    }

    migrator::Migrator::up(db, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::ConnectionTrait;

    #[tokio::test]
    async fn test_migration_runs() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();
        let result: i64 = db
            .query_one(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                [],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by(0)
            .unwrap();
        assert!(result >= 4, "expected at least 4 tables, got {result}");
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
