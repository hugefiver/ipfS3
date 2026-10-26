//! Meter actual SQLite queries/results through the existing operation ledger.
use super::{DecodeWork, management};
use management::{ManagementError, Result, native::meter::WorkMeter};
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, ExecResult, QueryResult, Statement};
use sqlx::{Row, TypeInfo, ValueRef};

pub(super) fn before_sql(work: DecodeWork<'_>) -> Result<()> {
    work.with(|session| {
        let mut budget = session.storage_budget()?;
        let charged = budget.charge(1, 0, 0, 0);
        let finished = session.finish_storage(&budget);
        charged.and(finished)
    })
}

pub(super) fn cleanup_sql(work: DecodeWork<'_>) -> Result<()> {
    work.with(|session| {
        session.charge_storage(WorkMeter {
            sql: 1,
            ..Default::default()
        })
    })
}

pub(super) struct SqliteRead<'a> {
    db: &'a sea_orm::DatabaseTransaction,
    work: DecodeWork<'a>,
}

impl<'a> SqliteRead<'a> {
    pub(super) fn new(db: &'a sea_orm::DatabaseTransaction, work: DecodeWork<'a>) -> Self {
        Self { db, work }
    }

    fn record<'r>(
        &self,
        rows: impl IntoIterator<Item = &'r QueryResult>,
    ) -> std::result::Result<(), DbErr> {
        let mut used = WorkMeter::default();
        let measured = (|| {
            for row in rows {
                add(&mut used.rows, 1)?;
                let row = row.try_as_sqlite_row().ok_or_else(unsupported)?;
                for index in 0..row.len() {
                    add(&mut used.cells, 1)?;
                    let value = row.try_get_raw(index).map_err(|_| unsupported())?;
                    let bytes = if value.is_null() {
                        0
                    } else {
                        match value.type_info().name() {
                            "INTEGER" | "REAL" => 8,
                            // Both storage classes have borrowed bytes. Measuring
                            // TEXT must not lose IO debt on invalid UTF-8.
                            "TEXT" | "BLOB" => row
                                .try_get_unchecked::<&[u8], _>(index)
                                .map_err(|_| unsupported())?
                                .len(),
                            _ => return Err(unsupported()),
                        }
                    };
                    add(&mut used.raw_bytes, bytes as u64)?;
                }
            }
            Ok(())
        })();
        // Returned IO stays charged even if cancellation/deadline fired in flight
        // or a later cell cannot be decoded. No lock crosses the database await.
        self.work
            .with(|session| session.charge_storage(used))
            .map_err(work_error)?;
        measured
    }
}

fn unsupported() -> DbErr {
    DbErr::Custom("unsupported runtime schema result".into())
}

fn work_error(_: ManagementError) -> DbErr {
    // The original operation ledger retains budget failure for the outer boundary.
    DbErr::Custom("runtime schema work unavailable".into())
}

fn add(total: &mut u64, amount: u64) -> std::result::Result<(), DbErr> {
    *total = total.checked_add(amount).ok_or_else(unsupported)?;
    Ok(())
}

#[async_trait::async_trait]
impl ConnectionTrait for SqliteRead<'_> {
    fn get_database_backend(&self) -> DatabaseBackend {
        DatabaseBackend::Sqlite
    }

    async fn execute(&self, _: Statement) -> std::result::Result<ExecResult, DbErr> {
        Err(unsupported())
    }

    async fn execute_unprepared(&self, _: &str) -> std::result::Result<ExecResult, DbErr> {
        Err(unsupported())
    }

    async fn query_one(
        &self,
        statement: Statement,
    ) -> std::result::Result<Option<QueryResult>, DbErr> {
        before_sql(self.work).map_err(work_error)?;
        let row = self.db.query_one(statement).await?;
        self.record(row.iter())?;
        Ok(row)
    }

    async fn query_all(
        &self,
        statement: Statement,
    ) -> std::result::Result<Vec<QueryResult>, DbErr> {
        before_sql(self.work).map_err(work_error)?;
        let rows = self.db.query_all(statement).await?;
        self.record(&rows)?;
        Ok(rows)
    }
}
