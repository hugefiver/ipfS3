use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // NULL denotes a pre-Stage4 job: never retroactively apply today's default.
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("import_jobs"))
                    .add_column(ColumnDef::new(Alias::new("root_capture_json")).text())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "import ZIP root capture cannot be downgraded without losing admission evidence".into(),
        ))
    }
}
