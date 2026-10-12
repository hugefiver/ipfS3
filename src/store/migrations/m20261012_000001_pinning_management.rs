use sea_orm_migration::prelude::*;
use sea_orm::TransactionTrait;
use crate::store::pinning::management::schema;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        if !matches!(backend, sea_orm::DatabaseBackend::Sqlite | sea_orm::DatabaseBackend::Postgres) {
            return Err(DbErr::Migration("unsupported management database".into()));
        }
        if manager.has_table(schema::RECORDS).await? || manager.has_table(schema::RETIREMENTS).await? {
            return Err(DbErr::Migration("partial or existing management schema requires repair".into()));
        }
        let tx=manager.get_connection().begin().await?;
        let db=&tx;
        db.execute_unprepared(&schema::records_ddl(backend)).await?;
        db.execute_unprepared(&schema::retirements_ddl(backend)).await?;
        let now = crate::store::database_clock::database_now(db).await.map_err(|_| DbErr::Migration("management database clock unavailable".into()))?;
        db.execute(sea_orm::Statement::from_sql_and_values(backend,
            "INSERT INTO pin_management_records (database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES ($1,'schema','identity','database_scope','','','{}','{}',NULL,'[]','[]','identity',$2,NULL,NULL)",
            [uuid::Uuid::new_v4().to_string().into(), now.into()])).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let tx=manager.get_connection().begin().await?;
        let db=&tx;
        if manager.get_database_backend()==sea_orm::DatabaseBackend::Sqlite {db.execute_unprepared("UPDATE pin_management_records SET database_scope=database_scope WHERE kind='database_scope'").await?;}
        if manager.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
            let tables=crate::store::pinning::management::native::registry().iter().map(|table|format!("\"{}\"",table.table)).collect::<Vec<_>>().join(",");
            db.execute_unprepared(&format!("LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT")).await?;
        }
        crate::store::pinning::management::native::capture(db).await.map_err(|_|DbErr::Migration("management downgrade requires a complete known native catalog".into()))?;
        let unsafe_rows = db.query_one(sea_orm::Statement::from_string(manager.get_database_backend(),
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM pin_management_records WHERE kind<>'database_scope') OR EXISTS (SELECT 1 FROM pin_management_retirements) OR EXISTS (SELECT 1 FROM remote_pins WHERE status<>'absent') OR EXISTS (SELECT 1 FROM pin_resource_history) OR EXISTS (SELECT 1 FROM pin_leases WHERE state='active') OR EXISTS (SELECT 1 FROM pin_provider_usage WHERE reserved_bytes<>0 OR reserved_pins<>0) OR EXISTS (SELECT 1 FROM remote_pin_ledger WHERE effect NOT IN ('absent','not_created')) OR EXISTS (SELECT 1 FROM pin_submit_observations WHERE needs_attention OR outcome='in_flight') OR EXISTS (SELECT 1 FROM pin_jobs WHERE state<>'done') OR EXISTS (SELECT 1 FROM pin_submit_history WHERE state<>'settled' OR effect='unknown') OR EXISTS (SELECT 1 FROM zip_root_references)"))
            .await?;
        if unsafe_rows.is_some() { return Err(DbErr::Migration("management downgrade would erase receipts, barriers or debt".into())); }
        db.execute_unprepared("DROP TABLE pin_management_retirements").await?;
        db.execute_unprepared("DROP TABLE pin_management_records").await?;
        tx.commit().await?;
        Ok(())
    }
}
