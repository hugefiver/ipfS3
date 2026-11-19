use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        // NULL is intentionally unknown: legacy uploads have no captured authority,
        // regardless of the reserved tags they may contain.
        let column_type = if manager.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
            "JSONB"
        } else {
            "TEXT"
        };
        manager
            .get_connection()
            .execute_unprepared(&format!(
                "ALTER TABLE multipart_uploads ADD COLUMN pin_decision_json {column_type}"
            ))
            .await?;
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
            "multipart decisions cannot be downgraded without losing pending upload intent".into(),
        ))
    }
}
