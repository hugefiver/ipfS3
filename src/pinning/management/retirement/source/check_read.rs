use super::*;
use sea_orm::{DbErr, ExecResult, QueryResult};
use std::sync::atomic::{AtomicU64, Ordering};

/// Only the binding/physical-identity/revision fence reads use this adapter.
/// Their actual SQL projections, rather than the formatted identity, are billed.
pub(super) struct CheckRead<'a> {
    tx: &'a DatabaseTransaction,
    bytes: AtomicU64,
}

impl<'a> CheckRead<'a> {
    pub(super) fn new(tx: &'a DatabaseTransaction) -> Self {
        Self {
            tx,
            bytes: AtomicU64::new(0),
        }
    }
    pub(super) fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn observe(&self, row: &QueryResult) -> std::result::Result<(), DbErr> {
        let bytes = if let Ok(value) = row.try_get::<String>("", "database_scope") {
            value.len() as u64
        } else if let Ok(value) = row.try_get::<String>("", "identity") {
            value.len() as u64
        } else if row.try_get::<bool>("", "allowed").is_ok() {
            1
        } else {
            let table = row.try_get::<String>("", "source_table")?;
            row.try_get::<i64>("", "revision")?;
            table.len() as u64 + 8
        };
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for CheckRead<'_> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.tx.get_database_backend()
    }
    async fn execute(&self, _: Statement) -> std::result::Result<ExecResult, DbErr> {
        Err(DbErr::Custom("source fence is read only".into()))
    }
    async fn execute_unprepared(&self, _: &str) -> std::result::Result<ExecResult, DbErr> {
        Err(DbErr::Custom("source fence is read only".into()))
    }
    async fn query_one(
        &self,
        statement: Statement,
    ) -> std::result::Result<Option<QueryResult>, DbErr> {
        let row = self.tx.query_one(statement).await?;
        if let Some(row) = &row {
            self.observe(row)?;
        }
        Ok(row)
    }
    async fn query_all(
        &self,
        statement: Statement,
    ) -> std::result::Result<Vec<QueryResult>, DbErr> {
        let rows = self.tx.query_all(statement).await?;
        for row in &rows {
            self.observe(row)?;
        }
        Ok(rows)
    }
}
