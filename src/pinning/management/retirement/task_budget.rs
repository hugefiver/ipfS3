use super::{
    spool::{get, one},
    *,
};
use crate::store::pinning::management::native::meter::{Budget, WorkMeter};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
#[cfg(test)]
mod tests;

/// Capture row clones are independent of raw reads. Writer output_bytes is a
/// completion observation of already-paid writes, not another capture charge.
pub(super) fn capture_work(budget: &Budget) -> Result<TaskWork> {
    let delta = |used: u64, paid: u64| {
        used.checked_sub(paid)
            .ok_or(ManagementError::InvalidEvidence)
    };
    Ok(TaskWork {
        rows: delta(budget.work.rows, budget.retirement_accounted.rows)?,
        bytes: delta(budget.work.raw_bytes, budget.retirement_accounted.raw_bytes)?
            .checked_add(delta(
                budget.work.copied_bytes,
                budget.retirement_accounted.copied_bytes,
            )?)
            .ok_or(ManagementError::Budget)?,
        pages: delta(budget.work.pages, budget.retirement_accounted.pages)?,
    })
}

/// One retained retirement envelope, including all source work and the union.
pub(crate) struct TaskBudget {
    sources: Vec<DatabaseConnection>,
    union: DatabaseConnection,
    envelope: (u64, u64),
    output: tokio::sync::Mutex<()>,
}
impl TaskBudget {
    pub(super) fn envelope_limits(&self, limits: &RunLimits) -> RunLimits {
        limits.with_task_caps(
            self.envelope.0.min(limits.total_rows),
            self.envelope.1.min(limits.total_bytes),
        )
    }

    pub(super) async fn union_caps(
        &self,
        union: &PlanUnion,
        limits: &RunLimits,
    ) -> Result<RunLimits> {
        checkpoint::task_caps(
            self.envelope,
            limits,
            self.total().await?,
            union.task_work().await?,
        )
    }
    pub(crate) fn reserve_capture(&self, budget: &Budget, limits: &RunLimits) -> Result<RunLimits> {
        limits.reserve_capture(self.envelope, budget)
    }
    /// Fold a canonical cache's monotone counters into the original task in the
    /// same attached-SQLite transaction as its forwarding coordinates.
    pub(super) async fn forward_cache(db: &DatabaseConnection, union: bool) -> Result<()> {
        let tx = db.begin().await.map_err(|_| ManagementError::Database)?;
        let sql = if union {
            "SELECT work.rows AS rows_used,work.bytes AS bytes_used,b.pages_used,b.forwarded_rows,b.forwarded_bytes,b.forwarded_pages FROM work JOIN task_budget b ON b.id=work.id WHERE work.id=1"
        } else {
            "SELECT rows_used,bytes_used,pages_used,forwarded_rows,forwarded_bytes,forwarded_pages FROM task_budget WHERE id=1"
        };
        let row = one(&tx, sql, vec![])
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
        let mut delta = Vec::new();
        let mut current = Vec::new();
        for (used, forwarded) in [
            ("rows_used", "forwarded_rows"),
            ("bytes_used", "forwarded_bytes"),
            ("pages_used", "forwarded_pages"),
        ] {
            let used = get::<i64>(&row, used)?;
            let previous = get::<i64>(&row, forwarded)?;
            if previous < 0 || used < previous {
                return Err(ManagementError::InvalidEvidence);
            }
            delta.push(used - previous);
            current.push(used);
        }
        let forwarded=tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "UPDATE original.task_budget SET rows_used=rows_used+$1,bytes_used=bytes_used+$2,pages_used=pages_used+$3 WHERE id=1 AND rows_used<=rows_limit-$1 AND bytes_used<=bytes_limit-$2",
            delta.into_iter().map(sea_orm::Value::from))).await.map_err(|_| ManagementError::Database)?;
        if forwarded.rows_affected() != 1 {
            return Err(ManagementError::Budget);
        }
        spool::exec(&tx,"UPDATE task_budget SET forwarded_rows=$1,forwarded_bytes=$2,forwarded_pages=$3 WHERE id=1",current.into_iter().map(Into::into).collect()).await?;
        tx.commit().await.map_err(|_| ManagementError::Database)
    }
    pub(super) fn new(spools: &[InventorySpool], union: &PlanUnion, envelope: (u64, u64)) -> Self {
        Self {
            sources: spools.iter().map(|s| s.db.clone()).collect(),
            union: union.budget_connection(),
            envelope,
            output: tokio::sync::Mutex::new(()),
        }
    }
    async fn source_work(db: &DatabaseConnection) -> Result<TaskWork> {
        let row = one(
            db,
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
    pub(crate) async fn total(&self) -> Result<TaskWork> {
        let mut total = TaskWork {
            rows: 0,
            bytes: 0,
            pages: 0,
        };
        for db in &self.sources {
            let own = Self::source_work(db).await?;
            total.rows = total
                .rows
                .checked_add(own.rows)
                .ok_or(ManagementError::Budget)?;
            total.bytes = total
                .bytes
                .checked_add(own.bytes)
                .ok_or(ManagementError::Budget)?;
            total.pages = total
                .pages
                .checked_add(own.pages)
                .ok_or(ManagementError::Budget)?;
        }
        let row = one(
            &self.union,
            "SELECT work.rows,work.bytes,COALESCE(task_budget.pages_used,0) AS pages FROM work LEFT JOIN task_budget ON task_budget.id=work.id WHERE work.id=1",
            vec![],
        )
        .await?
        .ok_or(ManagementError::InvalidEvidence)?;
        total.rows = total
            .rows
            .checked_add(
                u64::try_from(get::<i64>(&row, "rows")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?,
            )
            .ok_or(ManagementError::Budget)?;
        total.bytes = total
            .bytes
            .checked_add(
                u64::try_from(get::<i64>(&row, "bytes")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?,
            )
            .ok_or(ManagementError::Budget)?;
        total.pages = total
            .pages
            .checked_add(
                u64::try_from(get::<i64>(&row, "pages")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?,
            )
            .ok_or(ManagementError::Budget)?;
        Ok(total)
    }
    pub(crate) async fn caps(
        &self,
        spool: &InventorySpool,
        limits: &RunLimits,
    ) -> Result<RunLimits> {
        checkpoint::task_caps(
            self.envelope,
            limits,
            self.total().await?,
            spool.task_work().await?,
        )
    }
    /// Prepay a bounded default read session in the original task counters.
    pub(crate) async fn begin_read<'a>(
        &'a self,
        limits: &RunLimits,
    ) -> Result<(
        ReadAccounting<'a>,
        crate::store::pinning::management::records::ReadSession,
    )> {
        let mut budget = Budget::default();
        let mut accounting = self.adopt_output(limits, &mut budget, false).await?;
        let used = self.total().await?;
        let rows = self
            .envelope
            .0
            .min(limits.total_rows)
            .checked_sub(used.rows)
            .ok_or(ManagementError::Budget)?;
        let bytes = self
            .envelope
            .1
            .min(limits.total_bytes)
            .checked_sub(used.bytes)
            .ok_or(ManagementError::Budget)?;
        if rows == 0 || bytes == 0 {
            return Err(ManagementError::Budget);
        }
        let rows = rows.min(
            crate::store::pinning::management::native::meter::Limits::default().rows
                + super::super::wire::MAX_WIRE_NODES as u64,
        );
        let bytes = bytes.min(
            crate::store::pinning::management::native::meter::Limits::default().bytes * 3
                + super::super::wire::MAX_WIRE_BYTES as u64,
        );
        accounting
            .persist(WorkMeter {
                rows,
                raw_bytes: bytes,
                ..Default::default()
            })
            .await?;
        Ok((
            ReadAccounting {
                accounting,
                rows,
                bytes,
                pages: 0,
            },
            crate::store::pinning::management::records::ReadSession::for_task(
                budget,
                rows,
                bytes,
                limits.cancellation.clone(),
            ),
        ))
    }

    /// One writer owns the remaining task allowance through durable completion.
    pub(crate) async fn begin_output<'a>(
        &'a self,
        limits: &RunLimits,
    ) -> Result<(OutputAccounting<'a>, Budget)> {
        let mut budget = super::super::wire::v3::files::stream::budget();
        let accounting = self.adopt_output(limits, &mut budget, false).await?;
        Ok((accounting, budget))
    }
    /// Settle observed capture work without admitting new IO. Stopped invocations
    /// still pay against the original envelope and keep their Budget unchanged.
    pub(crate) async fn account_capture(
        &self,
        limits: &RunLimits,
        budget: &mut Budget,
    ) -> Result<()> {
        let _guard = self.output.lock().await;
        self.record_capture(limits, budget).await
    }
    async fn record_capture(&self, limits: &RunLimits, budget: &mut Budget) -> Result<()> {
        let ceiling = (
            self.envelope.0.min(limits.total_rows),
            self.envelope.1.min(limits.total_bytes),
        );
        let before = self.total().await?;
        if let Err(error) = self
            .record_output(before, ceiling, capture_work(budget)?)
            .await
        {
            credit(
                &mut budget.retirement_accounted,
                budget.work,
                difference(self.total().await?, before)?,
            );
            return Err(error);
        }
        budget.retirement_accounted = budget.work;
        Ok(())
    }
    /// A recovery probe may have charged capture before authority reads. Only
    /// unaccounted coordinates of the same Budget are charged at later handoffs.
    pub(crate) async fn adopt_output<'a>(
        &'a self,
        limits: &RunLimits,
        budget: &mut Budget,
        include_capture: bool,
    ) -> Result<OutputAccounting<'a>> {
        let guard = self.output.lock().await;
        let ceiling = (
            self.envelope.0.min(limits.total_rows),
            self.envelope.1.min(limits.total_bytes),
        );
        if include_capture {
            self.record_capture(limits, budget).await?;
        }
        let before = self.total().await?;
        let run = spool::Run::new(limits);
        budget.restrict_remaining(
            ceiling
                .0
                .checked_sub(before.rows)
                .ok_or(ManagementError::Budget)?,
            ceiling
                .1
                .checked_sub(before.bytes)
                .ok_or(ManagementError::Budget)?,
            run.remaining(),
        )?;
        run.check()?;
        Ok(OutputAccounting {
            task: self,
            before,
            initial: budget.work,
            ceiling,
            _guard: guard,
        })
    }
    async fn record_output(
        &self,
        now: TaskWork,
        ceiling: (u64, u64),
        work: TaskWork,
    ) -> Result<()> {
        let remaining_rows = ceiling
            .0
            .checked_sub(now.rows)
            .ok_or(ManagementError::Budget)?;
        let remaining_bytes = ceiling
            .1
            .checked_sub(now.bytes)
            .ok_or(ManagementError::Budget)?;
        // Budget prepays IO; denial may include an unperformed frame. Consume
        // that failed allowance without recording counters beyond the envelope.
        let db = self
            .sources
            .first()
            .ok_or(ManagementError::InvalidEvidence)?;
        spool::exec(db,
            "UPDATE task_budget SET rows_used=rows_used+$1,bytes_used=bytes_used+$2,pages_used=pages_used+$3 WHERE id=1",
            vec![i64::try_from(work.rows.min(remaining_rows)).map_err(|_|ManagementError::Budget)?.into(),
                 i64::try_from(work.bytes.min(remaining_bytes)).map_err(|_|ManagementError::Budget)?.into(),
                 i64::try_from(work.pages).map_err(|_|ManagementError::Budget)?.into()],
        ).await?;
        if work.rows > remaining_rows || work.bytes > remaining_bytes {
            return Err(ManagementError::Budget);
        }
        Ok(())
    }
}

pub(crate) struct OutputAccounting<'a> {
    task: &'a TaskBudget,
    before: TaskWork,
    initial: WorkMeter,
    ceiling: (u64, u64),
    _guard: tokio::sync::MutexGuard<'a, ()>,
}

pub(crate) struct ReadAccounting<'a> {
    accounting: OutputAccounting<'a>,
    rows: u64,
    bytes: u64,
    pages: u64,
}
impl ReadAccounting<'_> {
    /// Interrupted reads retain the prepayment. Completed attempts return only
    /// unused allowance, including when decoding or authority validation failed.
    pub(crate) async fn finish(self, used: WorkMeter) -> Result<()> {
        self.settle(used).await.0
    }
    async fn settle(self, used: WorkMeter) -> (Result<()>, TaskWork) {
        let mut paid = TaskWork {
            rows: self.rows,
            bytes: self.bytes,
            pages: self.pages,
        };
        let result = async {
        let db = self
            .accounting
            .task
            .sources
            .first()
            .ok_or(ManagementError::InvalidEvidence)?;
        let total = self.accounting.task.total().await?;
        let available_rows = self.accounting.ceiling.0.checked_sub(total.rows).and_then(|left| left.checked_add(self.rows)).ok_or(ManagementError::InvalidEvidence)?;
        let available_bytes = self.accounting.ceiling.1.checked_sub(total.bytes).and_then(|left| left.checked_add(self.bytes)).ok_or(ManagementError::InvalidEvidence)?;
        let rows = used.rows.min(available_rows);
        let bytes = used.raw_bytes.min(available_bytes);
        let changed=db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "UPDATE task_budget SET rows_used=rows_used-$1+$4,bytes_used=bytes_used-$2+$5,pages_used=pages_used-$3+$6 WHERE id=1 AND rows_used>=$1 AND bytes_used>=$2 AND pages_used>=$3",
            [self.rows, self.bytes, self.pages, rows, bytes, used.pages].into_iter().map(|value| i64::try_from(value).map(Into::into).map_err(|_|ManagementError::Budget)).collect::<Result<Vec<sea_orm::Value>>>()?)).await.map_err(|_|ManagementError::Database)?;
        if changed.rows_affected() != 1 {
            return Err(ManagementError::InvalidEvidence);
        }
        paid = TaskWork { rows, bytes, pages: used.pages };
        if used.rows > rows || used.raw_bytes > bytes {
            return Err(ManagementError::Budget);
        }
        Ok(())
        }.await;
        (result, paid)
    }
    pub(crate) async fn finish_output(self, initial: WorkMeter, budget: &mut Budget) -> Result<()> {
        let used = WorkMeter {
            rows: budget
                .work
                .rows
                .checked_sub(initial.rows)
                .ok_or(ManagementError::InvalidEvidence)?,
            raw_bytes: budget
                .work
                .raw_bytes
                .checked_sub(initial.raw_bytes)
                .and_then(|raw| {
                    raw.checked_add(budget.work.copied_bytes.checked_sub(initial.copied_bytes)?)
                })
                .ok_or(ManagementError::InvalidEvidence)?,
            pages: budget
                .work
                .pages
                .checked_sub(initial.pages)
                .ok_or(ManagementError::InvalidEvidence)?,
            ..Default::default()
        };
        let (result, paid) = self.settle(used).await;
        credit(&mut budget.retirement_accounted, budget.work, paid);
        result
    }
}
impl<'a> OutputAccounting<'a> {
    /// Reserve the remaining raw/copy transfer allowance before any file IO.
    /// A dropped transfer retains its payment; completed attempts refund unused work.
    pub(crate) async fn prepay_transfer(
        mut self,
        budget: &mut Budget,
        limits: &RunLimits,
    ) -> Result<ReadAccounting<'a>> {
        budget.restrict_transfer_bytes(
            self.ceiling
                .1
                .checked_sub(self.before.bytes)
                .ok_or(ManagementError::Budget)?,
        )?;
        let remaining = budget.remaining_io_allowance()?;
        let rows = remaining.0.min(
            self.ceiling
                .0
                .checked_sub(self.before.rows)
                .ok_or(ManagementError::Budget)?,
        );
        let bytes = self
            .ceiling
            .1
            .checked_sub(self.before.bytes)
            .ok_or(ManagementError::Budget)?;
        let pages = limits.pages_remaining();
        self.persist(WorkMeter {
            rows: self
                .initial
                .rows
                .checked_add(rows)
                .ok_or(ManagementError::Budget)?,
            raw_bytes: self
                .initial
                .raw_bytes
                .checked_add(bytes)
                .ok_or(ManagementError::Budget)?,
            pages: self
                .initial
                .pages
                .checked_add(pages)
                .ok_or(ManagementError::Budget)?,
            ..self.initial
        })
        .await?;
        Ok(ReadAccounting {
            accounting: self,
            rows,
            bytes,
            pages,
        })
    }
    /// Archive pages already persist their producer/codec work. Record only the
    /// remaining prefix/copy/footer work, including unsuccessful output attempts.
    pub(crate) async fn finish(mut self, work: WorkMeter) -> Result<()> {
        self.persist(work).await
    }
    pub(crate) async fn finish_metered(mut self, budget: &mut Budget) -> Result<()> {
        let result = self.persist(budget.work).await;
        budget.retirement_accounted = self.initial;
        result
    }
    /// Persist prepaid/observed work before a target-DB page attempt. A later
    /// finish charges only the work beyond these coordinates.
    pub(crate) async fn persist(&mut self, work: WorkMeter) -> Result<()> {
        let now = self.task.total().await?;
        credit(&mut self.initial, work, difference(now, self.before)?);
        self.before = now;
        let rows = work.rows.saturating_sub(self.initial.rows);
        let bytes = work
            .raw_bytes
            .saturating_sub(self.initial.raw_bytes)
            .checked_add(work.copied_bytes.saturating_sub(self.initial.copied_bytes))
            .ok_or(ManagementError::Budget)?;
        let pages = work.pages.saturating_sub(self.initial.pages);
        let result = self
            .task
            .record_output(now, self.ceiling, TaskWork { rows, bytes, pages })
            .await;
        if let Err(error) = result {
            let now = self.task.total().await?;
            credit(&mut self.initial, work, difference(now, self.before)?);
            self.before = now;
            return Err(error);
        }
        self.before.rows += rows;
        self.before.bytes += bytes;
        self.before.pages += pages;
        self.initial = work;
        Ok(())
    }
}

fn difference(after: TaskWork, before: TaskWork) -> Result<TaskWork> {
    Ok(TaskWork {
        rows: after
            .rows
            .checked_sub(before.rows)
            .ok_or(ManagementError::InvalidEvidence)?,
        bytes: after
            .bytes
            .checked_sub(before.bytes)
            .ok_or(ManagementError::InvalidEvidence)?,
        pages: after
            .pages
            .checked_sub(before.pages)
            .ok_or(ManagementError::InvalidEvidence)?,
    })
}

fn credit(paid: &mut WorkMeter, observed: WorkMeter, work: TaskWork) {
    paid.rows += work.rows.min(observed.rows.saturating_sub(paid.rows));
    let raw = work
        .bytes
        .min(observed.raw_bytes.saturating_sub(paid.raw_bytes));
    paid.raw_bytes += raw;
    paid.copied_bytes +=
        (work.bytes - raw).min(observed.copied_bytes.saturating_sub(paid.copied_bytes));
    paid.pages += work.pages.min(observed.pages.saturating_sub(paid.pages));
}
