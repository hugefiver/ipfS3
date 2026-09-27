use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // No backfill. A pre-migration job's raw tags are never evidence of an
        // authorized pin decision, even when a later process has a matching policy.
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("import_jobs"))
                    .add_column(ColumnDef::new(Alias::new("pin_decision_json")).text())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "import pin decisions cannot be downgraded without losing intent evidence".into(),
        ))
    }
}
