use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;
mod import_requests;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        import_requests::migrate(manager.get_connection()).await?;
        crate::store::pinning::management::schema::storage::install_inline(manager.get_connection())
            .await
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "immutable documents require authorized forward repair".into(),
        ))
    }
}
