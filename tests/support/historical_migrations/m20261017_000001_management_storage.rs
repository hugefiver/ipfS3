use sea_orm_migration::prelude::*;
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        crate::store::pinning::management::schema::storage::install(manager.get_connection()).await
    }
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::bridge::SharedMigration::up_body(self, &super::bridge::Manager::native(manager))
            .await
    }
    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "management history requires authorized forward repair, not downgrade".into(),
        ))
    }
}
