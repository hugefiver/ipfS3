use sea_orm::{ConnectionTrait, DatabaseBackend};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        let mpu_json = match manager.get_database_backend() {
            DatabaseBackend::Postgres => "JSONB",
            _ => "TEXT",
        };
        // Historical NULL is deliberately not backfilled from current policy.
        for sql in [
            format!("ALTER TABLE multipart_uploads ADD COLUMN pin_decision_json {mpu_json}"),
            "ALTER TABLE multipart_uploads ADD COLUMN zip_intent_json TEXT".into(),
            "ALTER TABLE import_jobs ADD COLUMN pin_decision_json TEXT".into(),
            "ALTER TABLE import_jobs ADD COLUMN zip_intent_json TEXT".into(),
            "ALTER TABLE zip_batches ADD COLUMN legacy_input_proof_json TEXT".into(),
        ] {
            manager.get_connection().execute_unprepared(&sql).await?;
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::bridge::SharedMigration::up_body(self, &super::bridge::Manager::native(manager))
            .await
    }
    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "ZIP intent/input capture cannot be downgraded without losing admission evidence"
                .into(),
        ))
    }
}
