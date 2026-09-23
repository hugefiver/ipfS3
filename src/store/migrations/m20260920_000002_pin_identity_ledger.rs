use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("pin_invocation_routes"))
                    .col(
                        ColumnDef::new(Alias::new("job_id"))
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Alias::new("route")).text().not_null())
                    .col(
                        ColumnDef::new(Alias::new("remote_epoch"))
                            .big_integer()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("pin_resource_history"))
                    .col(ColumnDef::new(Alias::new("provider")).text().not_null())
                    .col(ColumnDef::new(Alias::new("cid")).text().not_null())
                    .col(ColumnDef::new(Alias::new("epoch")).big_integer().not_null())
                    .col(ColumnDef::new(Alias::new("ledger")).text().not_null())
                    .primary_key(
                        Index::create()
                            .col(Alias::new("provider"))
                            .col(Alias::new("cid"))
                            .col(Alias::new("epoch")),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("pin_provider_routes"))
                    .col(
                        ColumnDef::new(Alias::new("provider"))
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Alias::new("snapshot")).text().not_null())
                    .col(ColumnDef::new(Alias::new("display_name")).text().not_null())
                    .col(ColumnDef::new(Alias::new("retired")).boolean().not_null())
                    .to_owned(),
            )
            .await?;
        let mut table = Table::create();
        table
            .table(Alias::new("remote_pin_ledger"))
            .col(ColumnDef::new(Alias::new("provider")).text().not_null())
            .col(ColumnDef::new(Alias::new("cid")).text().not_null())
            .col(ColumnDef::new(Alias::new("route")).text())
            .col(ColumnDef::new(Alias::new("ownership")).text().not_null())
            .col(ColumnDef::new(Alias::new("effect")).text().not_null())
            .primary_key(
                Index::create()
                    .col(Alias::new("provider"))
                    .col(Alias::new("cid")),
            );
        for name in [
            "first_observed_at",
            "last_observed_at",
            "remote_pinned_at",
            "gateway_verified_at",
            "content_verified_at",
        ] {
            table.col(ColumnDef::new(Alias::new(name)).timestamp_with_time_zone());
        }
        for name in ["first_error", "last_error"] {
            table.col(ColumnDef::new(Alias::new(name)).text());
        }
        manager.create_table(table.to_owned()).await?;
        // Stop all writers before upgrading. A provider alias/request-id alone
        // proves neither account scope nor ownership, even with Stage 1 history.
        // The old release protocol wrote status='absent' only after confirmed
        // release; all other statuses retain unknown effect and route.
        manager.get_connection().execute_unprepared("INSERT INTO remote_pin_ledger (provider,cid,ownership,effect,first_error,last_error) SELECT provider,cid,'unknown',CASE WHEN status='absent' THEN 'absent' ELSE 'unknown' END,'historical identity requires explicit migration','historical identity requires explicit migration' FROM remote_pins").await?;
        // Historical remotes without a usage row are still physical occupancy.
        // Preserve any existing usage counter; never turn unknown retention into
        // a free slot merely because an older writer did not persist the counter.
        manager.get_connection().execute_unprepared("INSERT INTO pin_provider_usage (provider,reserved_bytes,reserved_pins) SELECT provider,SUM(cid_size),COUNT(*) FROM remote_pins WHERE status<>'absent' GROUP BY provider ON CONFLICT(provider) DO NOTHING").await?;
        manager.get_connection().execute_unprepared("UPDATE pin_provider_usage SET reserved_bytes=CASE WHEN reserved_bytes < COALESCE((SELECT SUM(cid_size) FROM remote_pins WHERE remote_pins.provider=pin_provider_usage.provider AND status<>'absent'),0) THEN (SELECT SUM(cid_size) FROM remote_pins WHERE remote_pins.provider=pin_provider_usage.provider AND status<>'absent') ELSE reserved_bytes END, reserved_pins=CASE WHEN reserved_pins < (SELECT COUNT(*) FROM remote_pins WHERE remote_pins.provider=pin_provider_usage.provider AND status<>'absent') THEN (SELECT COUNT(*) FROM remote_pins WHERE remote_pins.provider=pin_provider_usage.provider AND status<>'absent') ELSE reserved_pins END").await?;
        manager.get_connection().execute_unprepared("UPDATE pin_jobs SET state='running', locked_until=NULL, last_error='historical identity unavailable; needs_attention' WHERE state<>'done' AND EXISTS (SELECT 1 FROM remote_pin_ledger l WHERE l.provider=pin_jobs.provider AND l.cid=pin_jobs.cid)").await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "pinning ledger downgrade is unsafe; preserve resource evidence and stop workers"
                .into(),
        ))
    }
}
