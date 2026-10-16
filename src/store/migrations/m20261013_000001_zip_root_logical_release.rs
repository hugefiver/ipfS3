use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn timestamp_type(backend: DatabaseBackend) -> Result<&'static str, DbErr> {
    match backend {
        DatabaseBackend::Postgres => Ok("TIMESTAMPTZ"),
        DatabaseBackend::Sqlite => Ok("TEXT"),
        _ => Err(DbErr::Migration(
            "ZIP root logical release requires SQLite or PostgreSQL".into(),
        )),
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let time = timestamp_type(manager.get_database_backend())?;
        manager
            .get_connection()
            .execute_unprepared(&format!(
                "ALTER TABLE zip_root_references ADD COLUMN logical_released_at {time} DEFAULT NULL"
            ))
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        timestamp_type(backend)?;
        let tx = manager.get_connection().begin().await?;
        match backend {
            DatabaseBackend::Sqlite => {
                // Acquire write intent before reading, including an empty table.
                tx.execute_unprepared("UPDATE zip_root_references SET updated_at=updated_at")
                    .await?;
            }
            DatabaseBackend::Postgres => {
                tx.execute_unprepared("LOCK TABLE zip_root_references IN ACCESS EXCLUSIVE MODE")
                    .await?;
            }
            _ => unreachable!(),
        }
        if tx
            .query_one(Statement::from_string(
                backend,
                "SELECT 1 FROM zip_root_references WHERE logical_released_at IS NOT NULL LIMIT 1",
            ))
            .await?
            .is_some()
        {
            return Err(DbErr::Migration(
                "ZIP root downgrade would erase logical releases and their receipt semantics"
                    .into(),
            ));
        }
        tx.execute_unprepared("ALTER TABLE zip_root_references DROP COLUMN logical_released_at")
            .await?;
        tx.commit().await
    }
}
