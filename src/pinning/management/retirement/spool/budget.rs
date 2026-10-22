use super::*;

pub(in crate::pinning::management::retirement) const SCHEMA: &str = "CREATE TABLE task_budget(id INTEGER PRIMARY KEY CHECK(id=1),rows_limit BIGINT NOT NULL CHECK(rows_limit>=0),bytes_limit BIGINT NOT NULL CHECK(bytes_limit>=0),rows_used BIGINT NOT NULL DEFAULT 0,bytes_used BIGINT NOT NULL DEFAULT 0,pages_used BIGINT NOT NULL DEFAULT 0,forwarded_rows BIGINT NOT NULL DEFAULT 0,forwarded_bytes BIGINT NOT NULL DEFAULT 0,forwarded_pages BIGINT NOT NULL DEFAULT 0,confirmation_pending TEXT); CREATE TABLE phase_work(phase TEXT PRIMARY KEY,pages BIGINT NOT NULL,elapsed_micros BIGINT NOT NULL) WITHOUT ROWID";
pub(in crate::pinning::management::retirement) const QUERY_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS query_work(phase TEXT PRIMARY KEY,calls BIGINT NOT NULL,rows BIGINT NOT NULL,cells BIGINT NOT NULL,bytes BIGINT NOT NULL,elapsed_micros BIGINT NOT NULL) WITHOUT ROWID";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryWork {
    pub phase: String,
    pub calls: u64,
    pub rows: u64,
    pub cells: u64,
    pub bytes: u64,
    pub elapsed: Duration,
}
impl QueryWork {
    pub(in crate::pinning::management::retirement) fn observe(
        &mut self,
        rows: u64,
        cells: u64,
        bytes: u64,
        elapsed: Duration,
    ) {
        self.calls += 1;
        self.rows += rows;
        self.cells += cells;
        self.bytes += bytes;
        self.elapsed += elapsed;
    }
}
pub(in crate::pinning::management::retirement) async fn record_queries(
    db: &impl ConnectionTrait,
    phase: &str,
    work: &QueryWork,
) -> Result<()> {
    if work.calls == 0 {
        return Ok(());
    }
    exec(db,"INSERT INTO query_work VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(phase) DO UPDATE SET calls=calls+excluded.calls,rows=rows+excluded.rows,cells=cells+excluded.cells,bytes=bytes+excluded.bytes,elapsed_micros=elapsed_micros+excluded.elapsed_micros",
        vec![phase.into(),(work.calls as i64).into(),(work.rows as i64).into(),(work.cells as i64).into(),(work.bytes as i64).into(),i64::try_from(work.elapsed.as_micros()).map_err(|_|ManagementError::Budget)?.into()]).await
}

pub(in crate::pinning::management::retirement) async fn task_run<'a>(
    db: &DatabaseConnection,
    limits: &'a RunLimits,
) -> Result<Run<'a>> {
    let mut run = Run::new(limits);
    run.task(0, 0)?;
    if run.remaining().is_zero() {
        return Ok(run);
    }
    let rows = i64::try_from(limits.total_rows).map_err(|_| ManagementError::Budget)?;
    let bytes = i64::try_from(limits.total_bytes).map_err(|_| ManagementError::Budget)?;
    exec(
        db,
        "INSERT INTO task_budget(id,rows_limit,bytes_limit) VALUES(1,$1,$2) ON CONFLICT(id) DO NOTHING",
        vec![rows.into(), bytes.into()],
    )
    .await?;
    let stored = one(
        db,
        "SELECT rows_limit,bytes_limit,rows_used,bytes_used FROM task_budget WHERE id=1",
        vec![],
    )
    .await?
    .ok_or(ManagementError::InvalidEvidence)?;
    run.total_rows = limits.total_rows.min(
        u64::try_from(get::<i64>(&stored, "rows_limit")?)
            .map_err(|_| ManagementError::InvalidEvidence)?,
    );
    run.total_bytes = limits.total_bytes.min(
        u64::try_from(get::<i64>(&stored, "bytes_limit")?)
            .map_err(|_| ManagementError::InvalidEvidence)?,
    );
    run.task(
        u64::try_from(get::<i64>(&stored, "rows_used")?)
            .map_err(|_| ManagementError::InvalidEvidence)?,
        u64::try_from(get::<i64>(&stored, "bytes_used")?)
            .map_err(|_| ManagementError::InvalidEvidence)?,
    )?;
    Ok(run)
}

pub(in crate::pinning::management::retirement) async fn charge(
    db: &impl ConnectionTrait,
    run: &Run<'_>,
    rows: u64,
    bytes: u64,
) -> Result<()> {
    let changed = db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE task_budget SET rows_used=rows_used+$1,bytes_used=bytes_used+$2,pages_used=pages_used+1 WHERE id=1 AND rows_used<=MIN(rows_limit,$3)-$1 AND bytes_used<=MIN(bytes_limit,$4)-$2",
        vec![i64::try_from(rows).map_err(|_|ManagementError::Budget)?.into(),i64::try_from(bytes).map_err(|_|ManagementError::Budget)?.into(),i64::try_from(run.total_rows).map_err(|_|ManagementError::Budget)?.into(),i64::try_from(run.total_bytes).map_err(|_|ManagementError::Budget)?.into()],
    )).await.map_err(|_|ManagementError::Database)?;
    if changed.rows_affected() != 1 {
        return Err(ManagementError::Budget);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskWork {
    pub rows: u64,
    pub bytes: u64,
    pub pages: u64,
}

pub(in crate::pinning::management::retirement) async fn copy_work(
    db: &impl ConnectionTrait,
    run: &Run<'_>,
    bytes: usize,
) -> Result<()> {
    if bytes == 0 {
        return Ok(());
    }
    let changed=db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE task_budget SET bytes_used=bytes_used+$1 WHERE id=1 AND bytes_used<=MIN(bytes_limit,$2)-$1",
        vec![i64::try_from(bytes).map_err(|_|ManagementError::Budget)?.into(),i64::try_from(run.total_bytes).map_err(|_|ManagementError::Budget)?.into()],
    )).await.map_err(|_|ManagementError::Database)?;
    if changed.rows_affected() != 1 {
        return Err(ManagementError::Budget);
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhaseWork {
    pub phase: String,
    pub pages: u64,
    pub elapsed: Duration,
}

pub(in crate::pinning::management::retirement) async fn timed(
    db: &impl ConnectionTrait,
    phase: &str,
    elapsed: Duration,
) -> Result<()> {
    exec(db,"INSERT INTO phase_work VALUES($1,1,$2) ON CONFLICT(phase) DO UPDATE SET pages=pages+1,elapsed_micros=elapsed_micros+excluded.elapsed_micros",vec![phase.into(),i64::try_from(elapsed.as_micros()).map_err(|_|ManagementError::Budget)?.into()]).await
}

impl InventorySpool {
    pub async fn query_work(&self) -> Result<Vec<QueryWork>> {
        let rows = self
            .db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT * FROM query_work ORDER BY phase LIMIT 16",
            ))
            .await
            .map_err(|_| ManagementError::Database)?;
        rows.into_iter()
            .map(|row| {
                Ok(QueryWork {
                    phase: get(&row, "phase")?,
                    calls: u64::try_from(get::<i64>(&row, "calls")?)
                        .map_err(|_| ManagementError::InvalidEvidence)?,
                    rows: u64::try_from(get::<i64>(&row, "rows")?)
                        .map_err(|_| ManagementError::InvalidEvidence)?,
                    cells: u64::try_from(get::<i64>(&row, "cells")?)
                        .map_err(|_| ManagementError::InvalidEvidence)?,
                    bytes: u64::try_from(get::<i64>(&row, "bytes")?)
                        .map_err(|_| ManagementError::InvalidEvidence)?,
                    elapsed: Duration::from_micros(
                        u64::try_from(get::<i64>(&row, "elapsed_micros")?)
                            .map_err(|_| ManagementError::InvalidEvidence)?,
                    ),
                })
            })
            .collect()
    }
    pub async fn phase_work(&self) -> Result<Vec<PhaseWork>> {
        let rows = self
            .db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT phase,pages,elapsed_micros FROM phase_work ORDER BY phase",
            ))
            .await
            .map_err(|_| ManagementError::Database)?;
        rows.into_iter()
            .map(|row| {
                Ok(PhaseWork {
                    phase: get(&row, "phase")?,
                    pages: u64::try_from(get::<i64>(&row, "pages")?)
                        .map_err(|_| ManagementError::InvalidEvidence)?,
                    elapsed: Duration::from_micros(
                        u64::try_from(get::<i64>(&row, "elapsed_micros")?)
                            .map_err(|_| ManagementError::InvalidEvidence)?,
                    ),
                })
            })
            .collect()
    }
    pub async fn task_work(&self) -> Result<TaskWork> {
        let row = one(
            &self.db,
            "SELECT rows_used,bytes_used,pages_used FROM task_budget WHERE id=1",
            vec![],
        )
        .await?
        .ok_or(ManagementError::InvalidEvidence)?;
        Ok(TaskWork {
            rows: u64::try_from(get::<i64>(&row, "rows_used")?)
                .map_err(|_| ManagementError::InvalidEvidence)?,
            bytes: u64::try_from(get::<i64>(&row, "bytes_used")?)
                .map_err(|_| ManagementError::InvalidEvidence)?,
            pages: u64::try_from(get::<i64>(&row, "pages_used")?)
                .map_err(|_| ManagementError::InvalidEvidence)?,
        })
    }
    /// Explicitly increase the persisted task envelope. Progress, source identity
    /// and authority are unchanged; ordinary continuation never enlarges it.
    pub async fn increase_task_budget(&self, total_rows: u64, total_bytes: u64) -> Result<()> {
        let rows = i64::try_from(total_rows).map_err(|_| ManagementError::Budget)?;
        let bytes = i64::try_from(total_bytes).map_err(|_| ManagementError::Budget)?;
        let changed = self.db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "UPDATE task_budget SET rows_limit=$1,bytes_limit=$2 WHERE id=1 AND rows_limit<=$1 AND bytes_limit<=$2",
            vec![rows.into(), bytes.into()],
        )).await.map_err(|_| ManagementError::Database)?;
        if changed.rows_affected() != 1 {
            return Err(ManagementError::Changed);
        }
        Ok(())
    }
}
