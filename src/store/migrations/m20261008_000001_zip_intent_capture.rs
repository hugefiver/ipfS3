use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Historical NULL is deliberately not backfilled from current policy.
        for table in ["multipart_uploads", "import_jobs"] {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(table))
                        .add_column(ColumnDef::new(Alias::new("zip_intent_json")).text())
                        .to_owned(),
                )
                .await?;
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("zip_batches"))
                    .add_column(ColumnDef::new(Alias::new("legacy_input_proof_json")).text())
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "ZIP intent/input capture cannot be downgraded without losing admission evidence"
                .into(),
        ))
    }
}
