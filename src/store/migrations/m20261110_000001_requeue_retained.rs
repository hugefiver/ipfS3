use sea_orm::{ConnectionTrait, DbErr};
use sea_orm_migration::prelude::*;

pub const VERSION: &str = "m20261110_000001_requeue_retained";
pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        VERSION
    }
}

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        requeue(manager.get_connection()).await
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
            "requeued pin jobs cannot be rolled back".into(),
        ))
    }
}

pub(crate) async fn requeue<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    db.execute_unprepared("UPDATE remote_pin_ledger SET last_error='cleanup needs attention: verify historical submission or creation receipt' WHERE effect='retained' AND last_error IS NULL;\n        UPDATE pin_jobs SET state='pending', attempts=0, next_attempt_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE operation='reconcile' AND state='done' AND EXISTS (SELECT 1 FROM remote_pin_ledger l JOIN remote_pins r ON r.provider=l.provider AND r.cid=l.cid WHERE l.effect='retained' AND l.provider=pin_jobs.provider AND l.cid=pin_jobs.cid AND r.epoch=pin_jobs.expected_remote_epoch)").await?;
    // A pruned reconcile job must be recreated with the old route, not today's configuration.
    let retained = "SELECT 'reconcile:' || l.provider || ':' || l.cid || ':e' || CAST(r.epoch AS TEXT) AS id,l.provider,l.cid,l.route,r.epoch FROM remote_pin_ledger l JOIN remote_pins r ON r.provider=l.provider AND r.cid=l.cid WHERE l.effect='retained' AND l.route IS NOT NULL AND r.request_id IS NOT NULL";
    db.execute_unprepared(&format!("INSERT INTO pin_invocation_routes(job_id,route,remote_epoch) SELECT x.id,x.route,x.epoch FROM ({retained}) x WHERE NOT EXISTS (SELECT 1 FROM pin_jobs j WHERE j.id=x.id) ON CONFLICT(job_id) DO NOTHING")).await?;
    db.execute_unprepared(&format!("INSERT INTO pin_jobs(id,provider,cid,operation,expected_remote_epoch,state,attempts,submit_phase,next_attempt_at,created_at,updated_at) SELECT x.id,x.provider,x.cid,'reconcile',x.epoch,'pending',0,NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM ({retained}) x JOIN pin_invocation_routes i ON i.job_id=x.id AND i.route=x.route AND i.remote_epoch=x.epoch WHERE NOT EXISTS (SELECT 1 FROM pin_jobs j WHERE j.operation='reconcile' AND j.provider=x.provider AND j.cid=x.cid AND j.expected_remote_epoch=x.epoch) ON CONFLICT(id) DO NOTHING")).await?;
    Ok(())
}
