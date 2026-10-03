use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement, TransactionTrait};
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
                "ZIP v2 MPU requires SQLite or PostgreSQL".into(),
            ));
        }
        manager.get_connection().execute_unprepared(
            "CREATE TABLE zip_v2_mpu_completions (
                upload_id TEXT PRIMARY KEY NOT NULL REFERENCES zip_v2_mpu_intakes(original_upload_id) ON DELETE RESTRICT,
                owner TEXT NOT NULL, token TEXT NOT NULL, execution_id TEXT NOT NULL,
                bucket TEXT NOT NULL, archive_key TEXT NOT NULL,
                part_contract TEXT NOT NULL,
                response_xml TEXT, response_headers TEXT,
                CONSTRAINT ck_zip_v2_mpu_receipt CHECK (
                    length(owner)>0 AND length(token)>0 AND length(part_contract)>0
                    AND ((response_xml IS NULL AND response_headers IS NULL) OR
                         (length(response_xml)>0 AND length(response_headers)>0)))
            )",
        ).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        let tx = db.begin().await?;
        if db.get_database_backend() == DatabaseBackend::Postgres {
            tx.execute_unprepared("LOCK TABLE zip_v2_mpu_completions IN ACCESS EXCLUSIVE MODE")
                .await?;
        }
        if tx
            .query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT 1 FROM zip_v2_mpu_completions LIMIT 1",
            ))
            .await?
            .is_some()
        {
            return Err(DbErr::Migration(
                "ZIP v2 MPU downgrade would erase Complete identity or receipts".into(),
            ));
        }
        tx.execute_unprepared("DROP TABLE zip_v2_mpu_completions")
            .await?;
        tx.commit().await
    }
}
