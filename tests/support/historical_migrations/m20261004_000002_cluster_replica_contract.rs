use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        let mut contracts = Table::create();
        contracts.table(Alias::new("pin_lease_requirements"));
        contracts.col(
            ColumnDef::new(Alias::new("lease_id"))
                .text()
                .not_null()
                .primary_key(),
        );
        for column in ["owner_object_id", "contract"] {
            contracts.col(ColumnDef::new(Alias::new(column)).text().not_null());
        }
        contracts.col(
            ColumnDef::new(Alias::new("created_at"))
                .timestamp_with_time_zone()
                .not_null(),
        );
        manager.create_table(contracts.to_owned()).await?;
        let mut observations = Table::create();
        observations.table(Alias::new("pin_replica_observations"));
        observations.col(
            ColumnDef::new(Alias::new("sequence"))
                .big_integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        );
        for column in [
            "job_id",
            "provider",
            "expected_cid",
            "route",
            "phase",
            "resources",
            "outcome",
        ] {
            observations.col(ColumnDef::new(Alias::new(column)).text().not_null());
        }
        observations.col(
            ColumnDef::new(Alias::new("remote_epoch"))
                .big_integer()
                .not_null(),
        );
        observations.col(ColumnDef::new(Alias::new("expected_generation")).big_integer());
        for column in ["lease_id", "target_id", "safe_error"] {
            observations.col(ColumnDef::new(Alias::new(column)).text());
        }
        for column in ["claim_until", "observed_at"] {
            observations.col(
                ColumnDef::new(Alias::new(column))
                    .timestamp_with_time_zone()
                    .not_null(),
            );
        }
        observations.col(
            ColumnDef::new(Alias::new("projectable"))
                .boolean()
                .not_null(),
        );
        manager.create_table(observations.to_owned()).await?;
        manager
            .create_index(
                Index::create()
                    .name("pin_replica_scope_lifetime")
                    .table(Alias::new("pin_replica_observations"))
                    .col(Alias::new("provider"))
                    .col(Alias::new("expected_cid"))
                    .col(Alias::new("sequence"))
                    .to_owned(),
            )
            .await?;
        // No current-config backfill. Old contracts are unknown, not today's min.
        // Both tables are immutable independently of object/job/lease lifetimes.
        let db = manager.get_connection();
        match manager.get_database_backend() {
            sea_orm::DatabaseBackend::Sqlite => {
                for table in ["pin_lease_requirements", "pin_replica_observations"] {
                    for operation in ["UPDATE", "DELETE"] {
                        db.execute_unprepared(&format!(
                            "CREATE TRIGGER {table}_no_{} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'immutable pin history'); END",
                            operation.to_ascii_lowercase())).await?;
                    }
                }
            }
            sea_orm::DatabaseBackend::Postgres => {
                db.execute_unprepared("CREATE FUNCTION reject_pin_history_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable pin history'; END $$").await?;
                for table in ["pin_lease_requirements", "pin_replica_observations"] {
                    db.execute_unprepared(&format!("CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION reject_pin_history_mutation()" )).await?;
                }
            }
            _ => return Err(DbErr::Custom("unsupported pin history database".into())),
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::bridge::SharedMigration::up_body(self, &super::bridge::Manager::native(manager))
            .await
    }
    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "Cluster history downgrade loses birth and replica evidence".into(),
        ))
    }
}
