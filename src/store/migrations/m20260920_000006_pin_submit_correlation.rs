use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Existing history has already captured the old on-wire job ID; NULL
        // preserves that protocol for in-flight recovery and safe retries.
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("pin_submit_history"))
                    .add_column(ColumnDef::new(Alias::new("correlation")).text())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "pin submit correlation cannot be downgraded without losing recovery evidence".into(),
        ))
    }
}
