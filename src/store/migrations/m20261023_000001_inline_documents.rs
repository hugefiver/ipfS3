use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;
mod import_requests;

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        import_requests::migrate(manager.get_connection()).await?;
        crate::store::pinning::management::schema::storage::install_inline(manager.get_connection())
            .await
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
            "immutable documents require authorized forward repair".into(),
        ))
    }
}
