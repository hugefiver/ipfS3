//! Resident memory is reclaimable; work and cancellation are operation-wide.
use super::{ManagementError, Result};
mod reader;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub const PAGE_ROWS: usize = 256;
pub const PAGE_BYTES: usize = 1024 * 1024;
pub const CELL_SLICE: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct WorkMeter {
    pub sql: u64,
    pub rows: u64,
    pub cells: u64,
    pub raw_bytes: u64,
    pub copied_bytes: u64,
    pub output_bytes: u64,
    pub pages: u64,
}

#[derive(Clone, Copy)]
pub struct Limits {
    pub rows: u64,
    pub bytes: u64,
    pub sql: u64,
    pub resident: usize,
    pub elapsed: Duration,
}

/// Durable resource limits, not execution authority. A persisted deadline never
/// extends the task's original time allowance.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    work: WorkMeter,
    rows: u64,
    bytes: u64,
    sql: u64,
    resident: usize,
    elapsed_millis: u64,
    recorded_at_millis: u64,
    deadline_millis: u64,
    peak: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            rows: 4096,
            bytes: 16 * 1024 * 1024,
            sql: 16384,
            resident: 8 * 1024 * 1024,
            elapsed: Duration::from_secs(30),
        }
    }
}

#[derive(Default)]
struct Resident {
    used: usize,
    peak: usize,
}
pub struct ResidentGuard {
    state: Arc<Mutex<Resident>>,
    bytes: usize,
}
impl Drop for ResidentGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.used -= self.bytes;
        }
    }
}
pub struct Budget {
    pub work: WorkMeter,
    /// Invocation-local coordinates already charged to a retained retirement
    /// task. Work, deadlines and resource ownership are never reset at handoff.
    pub(crate) retirement_accounted: WorkMeter,
    limits: Limits,
    deadline: Instant,
    wall_deadline: Option<std::time::SystemTime>,
    enforce_wall_deadline: bool,
    cancelled: Arc<AtomicBool>,
    resident: Arc<Mutex<Resident>>,
    failed: bool,
    task_bytes: Option<u64>,
    transfer_bytes: Option<u64>,
}
impl Default for Budget {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}
impl Budget {
    pub fn new(limits: Limits) -> Self {
        Self {
            work: WorkMeter::default(),
            retirement_accounted: WorkMeter::default(),
            deadline: Instant::now() + limits.elapsed,
            wall_deadline: std::time::SystemTime::now().checked_add(limits.elapsed),
            enforce_wall_deadline: false,
            limits,
            cancelled: Arc::new(AtomicBool::new(false)),
            resident: Arc::default(),
            failed: false,
            task_bytes: None,
            transfer_bytes: None,
        }
    }
    pub fn cancellation(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }
    pub(crate) fn remaining_io_allowance(&self) -> Result<(u64, u64)> {
        self.check()?;
        Ok((
            self.limits
                .rows
                .checked_sub(self.work.rows)
                .ok_or(ManagementError::Budget)?,
            self.limits
                .bytes
                .checked_sub(self.work.raw_bytes)
                .ok_or(ManagementError::Budget)?,
        ))
    }
    /// A sequential read borrows the remaining allowance and the same operation
    /// deadline, cancellation and resident pool. Its work is returned to its owner.
    pub(crate) fn remaining_budget(&self) -> Result<Self> {
        self.check()?;
        Ok(Self {
            work: WorkMeter::default(),
            retirement_accounted: WorkMeter::default(),
            limits: Limits {
                rows: self
                    .limits
                    .rows
                    .checked_sub(self.work.rows)
                    .ok_or(ManagementError::Budget)?,
                bytes: self
                    .limits
                    .bytes
                    .checked_sub(self.work.raw_bytes)
                    .ok_or(ManagementError::Budget)?,
                sql: self
                    .limits
                    .sql
                    .checked_sub(self.work.sql)
                    .ok_or(ManagementError::Budget)?,
                ..self.limits
            },
            deadline: self.deadline,
            wall_deadline: self.wall_deadline,
            enforce_wall_deadline: self.enforce_wall_deadline,
            cancelled: self.cancelled.clone(),
            resident: self.resident.clone(),
            failed: self.failed,
            task_bytes: self.task_bytes.map(|limit| {
                limit.saturating_sub(
                    self.work
                        .raw_bytes
                        .saturating_add(self.work.copied_bytes)
                        .saturating_add(self.work.output_bytes),
                )
            }),
            transfer_bytes: self
                .transfer_bytes
                .map(|limit| limit - (self.work.raw_bytes + self.work.copied_bytes)),
        })
    }
    /// Transfers charge raw IO and independent copies; output_bytes only records
    /// completion of writes already included in raw IO.
    pub(crate) fn restrict_transfer_bytes(&mut self, remaining: u64) -> Result<()> {
        let limit = self
            .work
            .raw_bytes
            .checked_add(self.work.copied_bytes)
            .and_then(|used| used.checked_add(remaining))
            .ok_or(ManagementError::Budget)?;
        self.transfer_bytes = Some(self.transfer_bytes.map_or(limit, |old| old.min(limit)));
        self.check()
    }
    pub(crate) fn restrict_work_bytes(&mut self, bytes: u64) -> Result<()> {
        self.task_bytes = Some(self.task_bytes.map_or(bytes, |old| old.min(bytes)));
        self.check()
    }
    pub fn check(&self) -> Result<()> {
        if let Some(limit) = self.transfer_bytes
            && self
                .work
                .raw_bytes
                .checked_add(self.work.copied_bytes)
                .is_none_or(|bytes| bytes > limit)
        {
            return Err(ManagementError::Budget);
        }
        if let Some(limit) = self.task_bytes
            && self
                .work
                .raw_bytes
                .checked_add(self.work.copied_bytes)
                .and_then(|bytes| bytes.checked_add(self.work.output_bytes))
                .is_none_or(|bytes| bytes > limit)
        {
            return Err(ManagementError::Budget);
        }
        if self.failed
            || Instant::now() >= self.deadline
            || self.cancelled.load(Ordering::Relaxed)
            || self.enforce_wall_deadline
                && self
                    .wall_deadline
                    .is_none_or(|end| std::time::SystemTime::now() >= end)
        {
            Err(ManagementError::Budget)
        } else {
            Ok(())
        }
    }
    pub fn remaining(&self) -> Result<Duration> {
        self.check()?;
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if self.enforce_wall_deadline {
            Ok(remaining.min(
                self.wall_deadline
                    .ok_or(ManagementError::Budget)?
                    .duration_since(std::time::SystemTime::now())
                    .map_err(|_| ManagementError::Budget)?,
            ))
        } else {
            Ok(remaining)
        }
    }
    /// Tighten an existing operation for additional task work. All previously
    /// charged work, resident guards, cancellation and original deadlines stay.
    pub(crate) fn restrict_remaining(
        &mut self,
        rows: u64,
        bytes: u64,
        elapsed: Duration,
    ) -> Result<()> {
        self.limits.rows = self.limits.rows.min(
            self.work
                .rows
                .checked_add(rows)
                .ok_or(ManagementError::Budget)?,
        );
        self.limits.bytes = self.limits.bytes.min(
            self.work
                .raw_bytes
                .checked_add(bytes)
                .ok_or(ManagementError::Budget)?,
        );
        self.deadline = self.deadline.min(Instant::now() + elapsed);
        self.charge(0, 0, 0, 0)
    }
    pub fn charge(&mut self, sql: u64, rows: u64, cells: u64, bytes: u64) -> Result<()> {
        self.check()?;
        let add = |a: u64, b: u64| a.checked_add(b).ok_or(ManagementError::Budget);
        let result = (|| {
            self.work.sql = add(self.work.sql, sql)?;
            self.work.rows = add(self.work.rows, rows)?;
            self.work.cells = add(self.work.cells, cells)?;
            self.work.raw_bytes = add(self.work.raw_bytes, bytes)?;
            if self.work.sql > self.limits.sql
                || self.work.rows > self.limits.rows
                || self.work.raw_bytes > self.limits.bytes
            {
                return Err(ManagementError::Budget);
            }
            Ok(())
        })();
        let result = result.and_then(|_| self.check());
        self.failed |= result.is_err();
        result
    }
    pub fn reserve(&mut self, bytes: usize) -> Result<ResidentGuard> {
        self.check()?;
        let mut state = self.resident.lock().map_err(|_| ManagementError::Budget)?;
        let total = state
            .used
            .checked_add(bytes)
            .ok_or(ManagementError::Budget)?;
        if total > self.limits.resident {
            self.failed = true;
            return Err(ManagementError::Budget);
        }
        state.used = total;
        state.peak = state.peak.max(total);
        Ok(ResidentGuard {
            state: self.resident.clone(),
            bytes,
        })
    }
    pub fn peak_resident(&self) -> usize {
        self.resident.lock().map_or(usize::MAX, |s| s.peak)
    }

    pub fn checkpoint(&self) -> Result<Checkpoint> {
        self.check()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| ManagementError::Budget)?;
        let recorded_at_millis =
            u64::try_from(now.as_millis()).map_err(|_| ManagementError::Budget)?;
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        let deadline_millis = recorded_at_millis
            .checked_add(u64::try_from(remaining.as_millis()).map_err(|_| ManagementError::Budget)?)
            .ok_or(ManagementError::Budget)?
            .min(
                u64::try_from(
                    self.wall_deadline
                        .ok_or(ManagementError::Budget)?
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|_| ManagementError::Budget)?
                        .as_millis(),
                )
                .map_err(|_| ManagementError::Budget)?,
            );
        if deadline_millis <= recorded_at_millis {
            return Err(ManagementError::Budget);
        }
        Ok(Checkpoint {
            work: self.work,
            rows: self.limits.rows,
            bytes: self.limits.bytes,
            sql: self.limits.sql,
            resident: self.limits.resident,
            elapsed_millis: u64::try_from(self.limits.elapsed.as_millis())
                .map_err(|_| ManagementError::Budget)?,
            recorded_at_millis,
            deadline_millis,
            peak: self.peak_resident(),
        })
    }
    /// Explicit operator ceilings cannot expand a persisted task's limits.
    /// Future-dated checkpoints, expiry and exhausted work are fail-closed.
    pub fn resume(checkpoint: Checkpoint, ceiling: Limits) -> Result<Self> {
        let now = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| ManagementError::Budget)?
                .as_millis(),
        )
        .map_err(|_| ManagementError::Budget)?;
        if now < checkpoint.recorded_at_millis
            || now >= checkpoint.deadline_millis
            || checkpoint.rows > ceiling.rows
            || checkpoint.bytes > ceiling.bytes
            || checkpoint.sql > ceiling.sql
            || checkpoint.resident > ceiling.resident
            || checkpoint.peak > checkpoint.resident
            || checkpoint.elapsed_millis
                > u64::try_from(ceiling.elapsed.as_millis()).map_err(|_| ManagementError::Budget)?
            || checkpoint.deadline_millis - checkpoint.recorded_at_millis
                > checkpoint.elapsed_millis
        {
            return Err(ManagementError::Budget);
        }
        let mut budget = Self::new(Limits {
            rows: checkpoint.rows,
            bytes: checkpoint.bytes,
            sql: checkpoint.sql,
            resident: checkpoint.resident,
            elapsed: Duration::from_millis(checkpoint.deadline_millis - now),
        });
        budget.limits.elapsed = Duration::from_millis(checkpoint.elapsed_millis);
        budget.wall_deadline =
            std::time::UNIX_EPOCH.checked_add(Duration::from_millis(checkpoint.deadline_millis));
        budget.enforce_wall_deadline = true;
        budget.work = checkpoint.work;
        budget
            .resident
            .lock()
            .map_err(|_| ManagementError::Budget)?
            .peak = checkpoint.peak;
        budget.charge(0, 0, 0, 0)?;
        Ok(budget)
    }
}
