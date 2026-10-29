use sea_orm_migration::prelude::*;

pub struct Migration;
impl MigrationName for Migration {
    fn name(&self) -> &str {
        super::baseline::VERSION
    }
}
#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        super::baseline::install(manager.get_connection()).await
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
            "schema baseline requires authorized forward repair".into(),
        ))
    }
}
