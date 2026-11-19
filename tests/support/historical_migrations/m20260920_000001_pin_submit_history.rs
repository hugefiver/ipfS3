use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("pin_submit_history"))
                    .col(
                        ColumnDef::new(Alias::new("job_id"))
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Alias::new("api")).text().not_null())
                    .col(ColumnDef::new(Alias::new("strategy")).text().not_null())
                    .col(ColumnDef::new(Alias::new("effect")).text().not_null())
                    .col(ColumnDef::new(Alias::new("state")).text().not_null())
                    .col(ColumnDef::new(Alias::new("first_error")).text())
                    .col(ColumnDef::new(Alias::new("last_error")).text())
                    .col(
                        ColumnDef::new(Alias::new("submit_calls"))
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(Alias::new("recovery_queries"))
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(Alias::new("started_at"))
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        // This migration requires all old writers/workers stopped. No historical
        // strategy can be reconstructed from today's TOML, even for pending work.
        manager.get_connection().execute_unprepared("INSERT INTO pin_submit_history (job_id,api,strategy,effect,state,first_error,last_error,started_at) SELECT id,'unknown','unknown','unknown','needs_attention','historical route unknown','historical route unknown',created_at FROM pin_jobs WHERE operation='submit' AND state<>'done'").await?;
        // running + NULL lock is deliberately unclaimable by both old and new
        // claim predicates; non-done/recovering preserves the ambiguity barrier.
        manager.get_connection().execute_unprepared("UPDATE pin_jobs SET state='running', locked_until=NULL, submit_phase='recovering' WHERE operation='submit' AND state<>'done'").await?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::bridge::SharedMigration::up_body(self, &super::bridge::Manager::native(manager))
            .await
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        let unsettled = db.query_one(sea_orm::Statement::from_string(manager.get_database_backend(),
            "SELECT COUNT(*) AS n FROM pin_submit_history h LEFT JOIN pin_jobs j ON j.id=h.job_id WHERE j.id IS NULL OR j.state<>'done' OR h.state<>'settled' OR h.effect='unknown'".to_owned())).await?.ok_or_else(|| DbErr::Custom("missing pin history count".into()))?;
        if unsettled.try_get::<i64>("", "n")? != 0 {
            return Err(DbErr::Custom("cannot downgrade unsettled pin submission history; stop workers and reconcile first".into()));
        }
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("pin_submit_history"))
                    .to_owned(),
            )
            .await
    }
}
