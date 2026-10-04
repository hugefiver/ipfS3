use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let mut table = Table::create();
        table.table(Alias::new("pin_submit_observations"));
        table.col(
            ColumnDef::new(Alias::new("id"))
                .text()
                .not_null()
                .primary_key(),
        );
        for column in [
            "job_id",
            "provider",
            "expected_cid",
            "route",
            "effect",
            "resources",
            "outcome",
        ] {
            table.col(ColumnDef::new(Alias::new(column)).text().not_null());
        }
        table.col(
            ColumnDef::new(Alias::new("submit_call"))
                .integer()
                .not_null(),
        );
        table.col(
            ColumnDef::new(Alias::new("remote_epoch"))
                .big_integer()
                .not_null(),
        );
        table.col(
            ColumnDef::new(Alias::new("claim_until"))
                .timestamp_with_time_zone()
                .not_null(),
        );
        table.col(
            ColumnDef::new(Alias::new("started_at"))
                .timestamp_with_time_zone()
                .not_null(),
        );
        table.col(ColumnDef::new(Alias::new("observed_at")).timestamp_with_time_zone());
        table.col(ColumnDef::new(Alias::new("safe_error")).text());
        table.col(
            ColumnDef::new(Alias::new("needs_attention"))
                .boolean()
                .not_null(),
        );
        manager.create_table(table.to_owned()).await?;
        manager
            .create_index(
                Index::create()
                    .name("pin_submit_observation_domain_debt")
                    .table(Alias::new("pin_submit_observations"))
                    .col(Alias::new("provider"))
                    .col(Alias::new("needs_attention"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("pin_submit_observation_job")
                    .table(Alias::new("pin_submit_observations"))
                    .col(Alias::new("job_id"))
                    .col(Alias::new("submit_call"))
                    .unique()
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("pin_submit_observation_resource")
                    .table(Alias::new("pin_submit_observations"))
                    .col(Alias::new("provider"))
                    .col(Alias::new("expected_cid"))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom("RPC submission ledger downgrade is unsafe; stop workers and preserve resource evidence".into()))
    }
}
