use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement, TransactionTrait};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn schema(backend: DatabaseBackend) -> Result<Vec<String>, DbErr> {
    let time = match backend {
        DatabaseBackend::Postgres => "TIMESTAMPTZ",
        DatabaseBackend::Sqlite => "TIMESTAMP",
        _ => {
            return Err(DbErr::Migration(
                "ZIP v2 MPU requires SQLite or PostgreSQL".into(),
            ));
        }
    };
    Ok(vec![format!(
        r#"CREATE TABLE zip_v2_mpu_intakes (
        original_upload_id TEXT PRIMARY KEY NOT NULL,
        active_upload_id TEXT UNIQUE REFERENCES multipart_uploads(upload_id) ON DELETE SET NULL,
        execution_id TEXT NOT NULL UNIQUE REFERENCES zip_v2_executions(id) ON DELETE RESTRICT,
        owner TEXT NOT NULL, token TEXT NOT NULL,
        bucket TEXT NOT NULL, archive_key TEXT NOT NULL, target_prefix TEXT NOT NULL,
        request_fingerprint TEXT NOT NULL, captured_options TEXT NOT NULL,
        captured_config TEXT NOT NULL, rule_revision TEXT NOT NULL,
        created_at {time} NOT NULL,
        CONSTRAINT ck_zip_v2_mpu_identity CHECK (
            length(original_upload_id)>0 AND length(execution_id)>0 AND length(owner)>0
            AND length(token)>0 AND length(bucket)>0 AND length(archive_key)>0
            AND length(target_prefix)>0 AND length(request_fingerprint)=64
            AND length(captured_options)>0 AND length(captured_config)>0
            AND length(rule_revision)>0),
        CONSTRAINT ck_zip_v2_mpu_active CHECK (
            active_upload_id IS NULL OR active_upload_id=original_upload_id),
        CONSTRAINT uq_zip_v2_mpu_owner_token UNIQUE (owner,token)
    )"#
    )])
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let tx = manager.get_connection().begin().await?;
        for sql in schema(manager.get_database_backend())? {
            tx.execute_unprepared(&sql).await?;
        }
        tx.commit().await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        schema(manager.get_database_backend())?;
        let tx = manager.get_connection().begin().await?;
        if manager.get_database_backend() == DatabaseBackend::Postgres {
            tx.execute_unprepared("LOCK TABLE zip_v2_mpu_intakes IN ACCESS EXCLUSIVE MODE")
                .await?;
        }
        if tx
            .query_one(Statement::from_string(
                manager.get_database_backend(),
                "SELECT 1 FROM zip_v2_mpu_intakes LIMIT 1",
            ))
            .await?
            .is_some()
        {
            return Err(DbErr::Migration(
                "ZIP v2 MPU downgrade would erase durable token identity".into(),
            ));
        }
        tx.execute_unprepared("DROP TABLE zip_v2_mpu_intakes")
            .await?;
        tx.commit().await
    }
}
