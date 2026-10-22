use super::*;
use crate::pinning::management::retirement::spool::Run;
use sea_orm::{DatabaseBackend, DatabaseTransaction, Statement, Value};

pub(super) struct Usage {
    pub rows: u64,
    pub bytes: u64,
}

impl Usage {
    pub async fn read(db: &impl ConnectionTrait) -> Result<Self> {
        let row = one(db, "SELECT rows,bytes FROM work WHERE id=1", vec![])
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
        Ok(Self {
            rows: u64::try_from(get::<i64>(&row, "rows")?)
                .map_err(|_| ManagementError::InvalidEvidence)?,
            bytes: u64::try_from(get::<i64>(&row, "bytes")?)
                .map_err(|_| ManagementError::InvalidEvidence)?,
        })
    }
}

/// The existing work counters prepay the read before any derived transaction.
/// A failed read or interrupted process retains that allowance; decoded rows
/// settle it before parsing or applying facts, so rollback cannot erase work.
struct Page {
    before: Usage,
    rows: u64,
    bytes: u64,
}
impl Page {
    async fn reserve(db: &DatabaseConnection, run: &Run<'_>, rows: u64) -> Result<Self> {
        let before = Usage::read(db).await?;
        let rows = rows.min(
            run.total_rows
                .checked_sub(before.rows)
                .ok_or(ManagementError::Budget)?,
        );
        let bytes = (1024 * 1024).min(
            run.total_bytes
                .checked_sub(before.bytes)
                .ok_or(ManagementError::Budget)?,
        );
        if rows == 0 || bytes < 8 {
            return Err(ManagementError::Budget);
        }
        run.task(before.rows + rows, before.bytes + bytes)?;
        let tx = db.begin().await.map_err(|_| ManagementError::Database)?;
        cas(
            &tx,
            "UPDATE work SET rows=$1,bytes=$2 WHERE id=1 AND rows=$3 AND bytes=$4",
            vec![
                (before.rows + rows).into(),
                (before.bytes + bytes).into(),
                before.rows.into(),
                before.bytes.into(),
            ],
        )
        .await?;
        exec(
            &tx,
            "UPDATE task_budget SET pages_used=pages_used+1 WHERE id=1",
            vec![],
        )
        .await?;
        tx.commit().await.map_err(|_| ManagementError::Database)?;
        Ok(Self {
            before,
            rows,
            bytes,
        })
    }
    async fn settle(self, db: &DatabaseConnection, rows: u64, bytes: u64) -> Result<()> {
        let rows = rows.max(1);
        if rows > self.rows || bytes > self.bytes {
            return Err(ManagementError::Budget);
        }
        let tx = db.begin().await.map_err(|_| ManagementError::Database)?;
        cas(&tx, "UPDATE work SET rows=$1,bytes=$2 WHERE id=1 AND rows=$3 AND bytes=$4 AND $1 >= (SELECT forwarded_rows FROM task_budget WHERE id=1) AND $2 >= (SELECT forwarded_bytes FROM task_budget WHERE id=1)",
            vec![(self.before.rows+rows).into(),(self.before.bytes+bytes).into(),(self.before.rows+self.rows).into(),(self.before.bytes+self.bytes).into()]).await?;
        tx.commit().await.map_err(|_| ManagementError::Database)
    }
}

pub(super) async fn facts(
    db: &DatabaseConnection,
    source: &DatabaseConnection,
    kind: &str,
    after: &str,
    run: &Run<'_>,
) -> Result<Vec<sea_orm::QueryResult>> {
    let page = Page::reserve(db, run, 512).await?;
    let count = (page.rows / 2).max(1).min(page.bytes / 8).min(256);
    let sizes = source.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        format!("SELECT length(CAST(identity AS BLOB))+length(CAST(payload AS BLOB)) AS bytes FROM facts WHERE kind=$1 AND identity>$2 ORDER BY identity LIMIT {count}"),
        vec![kind.into(),after.into()])).await.map_err(|_|ManagementError::Database)?;
    let mut bytes = sizes.len() as u64 * 8;
    let mut count = 0usize;
    for row in &sizes {
        let size = u64::try_from(get::<i64>(row, "bytes")?)
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if size > page.bytes - bytes || sizes.len() as u64 + count as u64 + 1 > page.rows {
            break;
        }
        bytes += size;
        count += 1;
    }
    if count == 0 {
        page.settle(db, sizes.len() as u64, bytes).await?;
        return if sizes.is_empty() {
            Ok(Vec::new())
        } else {
            Err(ManagementError::Budget)
        };
    }
    let rows = source.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        format!("SELECT identity,payload FROM facts WHERE kind=$1 AND identity>$2 ORDER BY identity LIMIT {count}"),
        vec![kind.into(),after.into()])).await.map_err(|_|ManagementError::Database)?;
    page.settle(db, (sizes.len() + rows.len()) as u64, bytes)
        .await?;
    if rows.len() != count {
        return Err(ManagementError::Changed);
    }
    Ok(rows)
}

pub(super) async fn output_row(
    db: &DatabaseConnection,
    stage: i64,
    after: &str,
    run: &Run<'_>,
) -> Result<Option<sea_orm::QueryResult>> {
    let (table, key, columns, size) = match stage {
        0 => (
            "quotes",
            "identity",
            "identity,bytes,unknown,inflight,cleanup",
            "length(CAST(identity AS BLOB))+length(CAST(bytes AS BLOB))+length(CAST(unknown AS BLOB))+length(CAST(inflight AS BLOB))+length(CAST(cleanup AS BLOB))",
        ),
        1 => (
            "cids",
            "cid",
            "cid AS identity",
            "length(CAST(cid AS BLOB))",
        ),
        2 => (
            "transfers",
            "identity",
            "identity",
            "length(CAST(identity AS BLOB))",
        ),
        3 => (
            "unknowns",
            "identity",
            "identity",
            "length(CAST(identity AS BLOB))",
        ),
        _ => return Err(ManagementError::InvalidEvidence),
    };
    let page = Page::reserve(db, run, 2).await?;
    let size = one(
        db,
        &format!("SELECT {size} AS bytes FROM {table} WHERE {key}>$1 ORDER BY {key} LIMIT 1"),
        vec![after.into()],
    )
    .await?;
    let Some(size) = size else {
        page.settle(db, 1, 0).await?;
        return Ok(None);
    };
    let bytes =
        u64::try_from(get::<i64>(&size, "bytes")?).map_err(|_| ManagementError::InvalidEvidence)?;
    if page.rows < 2 || bytes > page.bytes - 8 {
        page.settle(db, 1, 8).await?;
        return Err(ManagementError::Budget);
    }
    let row = one(
        db,
        &format!("SELECT {columns} FROM {table} WHERE {key}>$1 ORDER BY {key} LIMIT 1"),
        vec![after.into()],
    )
    .await?;
    page.settle(db, 2, bytes + 8).await?;
    row.map(Some).ok_or(ManagementError::Changed)
}

pub(super) async fn cas(tx: &DatabaseTransaction, sql: &str, values: Vec<Value>) -> Result<()> {
    let changed = tx
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            sql,
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    if changed.rows_affected() != 1 {
        return Err(ManagementError::Changed);
    }
    Ok(())
}

pub(super) async fn commit(tx: DatabaseTransaction, outcome: Result<()>) -> Result<()> {
    match outcome {
        Ok(()) => {
            super::super::cache_guard::accept(&tx).await?;
            tx.commit().await.map_err(|_| ManagementError::Database)
        }
        Err(error) => {
            tx.rollback().await.map_err(|_| ManagementError::Database)?;
            Err(error)
        }
    }
}
