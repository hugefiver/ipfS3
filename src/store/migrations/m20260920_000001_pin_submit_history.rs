use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ConnectionTrait, Database, EntityTrait};

    #[tokio::test]
    async fn stage1_migration_quarantines_old_writers_and_refuses_unsettled_downgrade() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        let manager = SchemaManager::new(&db);
        Migration.down(&manager).await.unwrap();
        db.execute_unprepared("INSERT INTO pin_jobs (id,operation,provider,cid,lease_id,target_id,expected_generation,state,next_attempt_at,submit_phase) VALUES ('old','submit','pinata','cid','lease','target',1,'pending','2026-09-20T00:00:00Z','recovering')").await.unwrap();
        Migration.up(&manager).await.unwrap();
        let row = crate::store::pinning::jobs::submission_history(&db, "old")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                row.api.as_str(),
                row.strategy.as_str(),
                row.effect.as_str(),
                row.state.as_str()
            ),
            ("unknown", "unknown", "unknown", "needs_attention")
        );
        let job = crate::store::entities::pin_job::Entity::find_by_id("old")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.state, "running");
        assert_eq!(job.locked_until, None);
        assert!(
            crate::store::pinning::jobs::claim_due_jobs(
                &db,
                chrono::Utc::now() + chrono::Duration::days(365),
                chrono::Duration::seconds(30),
                10
            )
            .await
            .unwrap()
            .is_empty()
        );
        assert!(Migration.down(&manager).await.is_err());
        assert!(manager.has_table("pin_submit_history").await.unwrap());
    }
}
