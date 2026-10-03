use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, TransactionTrait};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !matches!(
            manager.get_database_backend(),
            DatabaseBackend::Sqlite | DatabaseBackend::Postgres
        ) {
            return Err(DbErr::Migration(
                "ZIP MPU replay requires SQLite or PostgreSQL".into(),
            ));
        }
        let tx = manager.get_connection().begin().await?;
        tx.execute_unprepared(r#"CREATE TABLE zip_mpu_replays (
            batch_id TEXT PRIMARY KEY NOT NULL REFERENCES zip_batches(id) ON DELETE RESTRICT,
            request_fingerprint TEXT NOT NULL,
            request_contract TEXT NOT NULL,
            prepared_archive_cid TEXT,
            archive_cid TEXT,
            archive_size BIGINT,
            public_version_id TEXT,
            server_side_encryption TEXT,
            response_xml TEXT,
            response_headers_json TEXT,
            CONSTRAINT ck_zip_mpu_request CHECK (length(request_fingerprint)=64 AND length(request_contract)>0),
            CONSTRAINT ck_zip_mpu_prepared_archive CHECK (prepared_archive_cid IS NULL OR length(prepared_archive_cid)>0),
            CONSTRAINT ck_zip_mpu_result CHECK (
                (archive_cid IS NULL AND archive_size IS NULL AND public_version_id IS NULL
                    AND server_side_encryption IS NULL AND response_xml IS NULL AND response_headers_json IS NULL)
                OR (archive_cid IS NOT NULL AND prepared_archive_cid IS NOT NULL
                    AND archive_cid=prepared_archive_cid AND length(archive_cid)>0 AND archive_size IS NOT NULL
                    AND archive_size>=0 AND response_xml IS NOT NULL AND length(response_xml)>0
                    AND response_headers_json IS NOT NULL AND length(response_headers_json)>0
                    AND (public_version_id IS NULL OR length(public_version_id)>0)
                    AND (server_side_encryption IS NULL OR server_side_encryption='AES256')))
        )"#).await?;
        tx.commit().await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let tx = manager.get_connection().begin().await?;
        if manager.get_database_backend() == DatabaseBackend::Postgres {
            tx.execute_unprepared("LOCK TABLE zip_mpu_replays IN ACCESS EXCLUSIVE MODE")
                .await?;
        }
        if tx
            .query_one(sea_orm::Statement::from_string(
                manager.get_database_backend(),
                "SELECT 1 FROM zip_mpu_replays LIMIT 1",
            ))
            .await?
            .is_some()
        {
            return Err(DbErr::Migration(
                "ZIP MPU replay downgrade would erase durable completion evidence".into(),
            ));
        }
        tx.execute_unprepared("DROP TABLE zip_mpu_replays").await?;
        tx.commit().await
    }
}
