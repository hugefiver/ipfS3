use super::super::wire::v3::{self, Projection, Writer};
use super::*;
use crate::store::pinning::management::{native::meter::Budget, records::ConfirmedBinding};
use sea_orm::DatabaseConnection;
use std::{
    fs::File,
    io::{Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
mod preparation;
#[cfg(test)]
mod tests;

#[derive(Default)]
pub(crate) struct PageMeter {
    used: AtomicU64,
    denied: AtomicBool,
}
impl PageMeter {
    fn result<T>(&self, result: Result<T>) -> Result<T> {
        if self.denied.load(Ordering::Relaxed) {
            Err(ManagementError::Budget)
        } else {
            result
        }
    }
}

pub(crate) struct PageOutput<'a, W> {
    output: W,
    run: spool::Run<'a>,
    remaining: usize,
    pages: Arc<PageMeter>,
    input_bytes: Option<u64>,
}
impl<'a, W> PageOutput<'a, W> {
    pub(crate) fn new(output: W, limits: &'a RunLimits, pages: Arc<PageMeter>) -> Self {
        Self {
            output,
            run: spool::Run::new(limits),
            remaining: 0,
            pages,
            input_bytes: None,
        }
    }
    pub(crate) fn into_inner(self) -> W {
        self.output
    }
    pub(crate) fn reader(
        output: W,
        limits: &'a RunLimits,
        pages: Arc<PageMeter>,
        bytes: u64,
    ) -> Self {
        let mut reader = Self::new(output, limits, pages);
        reader.input_bytes = Some(bytes);
        reader
    }
    fn start_page(&mut self, length: usize) -> std::io::Result<usize> {
        self.run.check().map_err(|error| self.denied(error))?;
        if length == 0 {
            return Ok(0);
        }
        if self.remaining == 0 {
            if !self.run.page(0, 0).map_err(|error| self.denied(error))? {
                return Err(self.denied(ManagementError::Budget));
            }
            self.pages.used.fetch_add(1, Ordering::Relaxed);
            self.remaining = crate::store::pinning::management::native::meter::PAGE_BYTES;
        }
        Ok(length.min(self.remaining))
    }
    fn observed(&mut self, length: usize) {
        self.remaining -= length;
    }
    fn denied(&self, error: ManagementError) -> std::io::Error {
        self.pages.denied.store(true, Ordering::Relaxed);
        std::io::Error::other(error)
    }
}
impl<W: Write> Write for PageOutput<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let length = self.start_page(bytes.len())?;
        let n = self.output.write(&bytes[..length])?;
        self.observed(n);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}
impl<W: Read> Read for PageOutput<'_, W> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.run.check().map_err(|error| self.denied(error))?;
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.input_bytes == Some(0) {
            return match self.output.read(&mut bytes[..1])? {
                0 => Ok(0),
                _ => Err(std::io::Error::other(ManagementError::InvalidEvidence)),
            };
        }
        let length = self.start_page(bytes.len())?;
        let length = self
            .input_bytes
            .map_or(length, |left| length.min(left as usize));
        let n = self.output.read(&mut bytes[..length])?;
        self.observed(n);
        if let Some(left) = &mut self.input_bytes {
            *left -= n as u64;
        }
        Ok(n)
    }
}

/// The caller's budget follows either the sink or a protected output handle.
/// No error path unlinks a partial file or removes retained task progress.
struct TaskOutput<'a, W> {
    writer: Writer<W>,
    accounting: Option<OutputPayment<'a>>,
    pages: Option<Arc<PageMeter>>,
}
enum OutputPayment<'a> {
    Producer(task_budget::OutputAccounting<'a>),
    Transfer(
        task_budget::ReadAccounting<'a>,
        crate::store::pinning::management::native::meter::WorkMeter,
    ),
}
impl OutputPayment<'_> {
    async fn finish(self, budget: &mut Budget) -> Result<()> {
        match self {
            Self::Producer(accounting) => accounting.finish_metered(budget).await,
            Self::Transfer(accounting, initial) => accounting.finish_output(initial, budget).await,
        }
    }
}

impl ReadyConfirmation {
    /// The accepted Archive phase has already encoded the complete stable
    /// document. Preflight checks only bounded bindings and immutable roots.
    pub async fn preflight(
        &self,
        db: &DatabaseConnection,
        mut budget: Budget,
        header: v3::planning::Header<'_>,
        acknowledgements: &[String],
        limits: &RunLimits,
    ) -> (Result<()>, Budget) {
        let accounting = match self.task.adopt_output(limits, &mut budget, true).await {
            Ok(accounting) => accounting,
            Err(error) => return (Err(error), budget),
        };
        let result = async {
            spool::Run::new(limits).check()?;
            let accepted = super::super::plan::validate_acks(&self.plan, acknowledgements)
                .map_err(planning::safe)?;
            if header.projection != Projection::Review
                || header.database_scope != self.origin.database_scope
                || header.database_identity != self.origin.database_identity
                || header.principal != self.principal
                || header.catalog != self.origin.catalog
                || header.expires_at.is_some()
                || Some(header.created_at) != self.snapshot.database_now.map(|now| now.to_rfc3339())
                || self.plan.items().iter().any(|entry| {
                    matches!(entry.status, super::super::PlanStatus::Blocked(_))
                        || entry.status == super::super::PlanStatus::Candidate
                            && !accepted.contains(&entry.request.item_id)
                })
            {
                return Err(ManagementError::Confirmation);
            }
            budget.charge(1, 1, 1, self.header.len() as u64)?;
            if crate::store::pinning::management::records::segmented::progress::load(
                db,
                &self.archive_id,
                &self.header,
                false,
            )
            .await?
            .as_ref()
                != Some(&self.position)
                || !self.position.sealed
            {
                return Err(ManagementError::Changed);
            }
            crate::store::pinning::management::records::prepared_output_reference(
                db,
                &self.origin.database_scope,
                &self.request.plan_id,
                self.plan.items(),
                &mut budget,
            )
            .await?;
            budget.check()
        }
        .await;
        let accounted = accounting.finish_metered(&mut budget).await;
        (result.and(accounted), budget)
    }
}
impl<'a, W: Write> TaskOutput<'a, W> {
    async fn start(
        task: Option<&'a task_budget::TaskBudget>,
        output: W,
        projection: Projection,
        mut budget: Budget,
        include_capture: bool,
        limits: &RunLimits,
        pages: Option<Arc<PageMeter>>,
    ) -> std::result::Result<Self, (ManagementError, Box<Budget>)> {
        let accounting = match task {
            Some(task) => {
                match task
                    .adopt_output(limits, &mut budget, include_capture)
                    .await
                {
                    Ok(accounting) => Some(if pages.is_some() {
                        match accounting.prepay_transfer(&mut budget, limits).await {
                            Ok(accounting) => OutputPayment::Transfer(accounting, budget.work),
                            Err(error) => return Err((error, Box::new(budget))),
                        }
                    } else {
                        OutputPayment::Producer(accounting)
                    }),
                    Err(error) => return Err((error, Box::new(budget))),
                }
            }
            None => {
                if let Err(error) = spool::Run::new(limits).check() {
                    return Err((error, Box::new(budget)));
                }
                None
            }
        };
        match Writer::new_metered(output, projection, budget) {
            Ok(writer) => Ok(Self {
                writer,
                accounting,
                pages,
            }),
            Err((error, mut budget)) => {
                budget.work.pages += pages
                    .as_ref()
                    .map_or(0, |pages| pages.used.load(Ordering::Relaxed));
                if let Some(accounting) = accounting
                    && let Err(error) = accounting.finish(&mut budget).await
                {
                    return Err((error, budget));
                }
                let error = if pages
                    .as_ref()
                    .is_some_and(|pages| pages.denied.load(Ordering::Relaxed))
                {
                    ManagementError::Budget
                } else {
                    planning::safe(error)
                };
                Err((error, budget))
            }
        }
    }
    async fn finish(
        self,
        result: Result<()>,
        durable: impl FnOnce(W) -> Result<()>,
    ) -> (Result<()>, Budget) {
        let (result, mut budget) = match result {
            Ok(()) => {
                let (result, budget) = self.writer.finalize();
                let result = result
                    .map_err(planning::safe)
                    .and_then(durable)
                    .and_then(|()| budget.check());
                (result, budget)
            }
            Err(error) => (Err(error), self.writer.abort()),
        };
        budget.work.pages += self
            .pages
            .as_ref()
            .map_or(0, |pages| pages.used.load(Ordering::Relaxed));
        let result = if let Some(pages) = &self.pages {
            pages.result(result)
        } else {
            result
        };
        if let Some(accounting) = self.accounting
            && let Err(error) = accounting.finish(&mut budget).await
        {
            return (Err(error), budget);
        }
        (result, budget)
    }
}

impl PreparedPlanning {
    /// First output handoff: charge fresh capture once and complete dry-run
    /// output through footer/flush/sync. Both outcomes return the original Budget.
    pub async fn export_file(
        &self,
        (output, budget): (File, Budget),
        header: v3::planning::Header<'_>,
        request: &PlanRequest,
        source: (&Snapshot, Option<&super::super::db_snapshot::PlanningScope>),
        acks: Option<&[String]>,
        limits: &RunLimits,
    ) -> (Result<()>, Budget) {
        let task = self.spools.first().and_then(|spool| spool.task.as_deref());
        let mut output =
            match TaskOutput::start(task, output, header.projection, budget, true, limits, None)
                .await
            {
                Ok(output) => output,
                Err((error, budget)) => return (Err(error), *budget),
            };
        let result = tokio::select! {
            biased;
            _ = limits.cancellation.cancelled() => Err(ManagementError::Budget),
            result = self.export(&mut output.writer, header, request, source, acks, limits) => result,
        };
        output.finish(result, sync).await
    }
    /// First output handoff: persist the caller's fresh-capture work once, then
    /// meter the real sink writer through footer and flush. Check the result
    /// before confirmation; pass the returned Budget to export_confirmed_file.
    pub async fn preflight_local(
        &self,
        budget: Budget,
        header: v3::planning::Header<'_>,
        request: &PlanRequest,
        source: (&Snapshot, Option<&super::super::db_snapshot::PlanningScope>),
        acks: Option<&[String]>,
        limits: &RunLimits,
    ) -> (Result<()>, Budget) {
        if !self.spools.is_empty() {
            return (Err(ManagementError::Confirmation), budget);
        }
        let task = self.spools.first().and_then(|spool| spool.task.as_deref());
        let mut output = match TaskOutput::start(
            task,
            std::io::sink(),
            header.projection,
            budget,
            true,
            limits,
            None,
        )
        .await
        {
            Ok(output) => output,
            Err((error, budget)) => return (Err(error), *budget),
        };
        let result = tokio::select! {
            biased;
            _ = limits.cancellation.cancelled() => Err(ManagementError::Budget),
            result = self.export(&mut output.writer, header, request, source, acks, limits) => result,
        };
        output.finish(result, |_| Ok(())).await
    }
    /// Pass the Budget returned by successful preflight, without replacing it or
    /// charging fresh capture again. Both outcomes retain its accumulated work.
    pub async fn export_confirmed_file(
        &self,
        db: &DatabaseConnection,
        output: (File, Budget),
        binding: &ConfirmedBinding,
        projection: Projection,
        limits: &RunLimits,
    ) -> (Result<()>, Budget) {
        if binding.review().entries() != self.plan.items() {
            return (Err(ManagementError::Confirmation), output.1);
        }
        let task = self.spools.first().and_then(|spool| spool.task.as_deref());
        confirmed_file(task, db, output, binding, projection, false, limits).await
    }
}

pub(super) async fn confirmed_file(
    task: Option<&task_budget::TaskBudget>,
    db: &DatabaseConnection,
    (file, budget): (File, Budget),
    binding: &ConfirmedBinding,
    projection: Projection,
    include_capture: bool,
    limits: &RunLimits,
) -> (Result<()>, Budget) {
    let pages = Arc::new(PageMeter::default());
    let mut output = match TaskOutput::start(
        task,
        PageOutput::new(file, limits, pages.clone()),
        projection,
        budget,
        include_capture,
        limits,
        Some(pages.clone()),
    )
    .await
    {
        Ok(output) => output,
        Err((error, budget)) => return (Err(error), *budget),
    };
    let result = tokio::select! {
        biased;
        _ = limits.cancellation.cancelled() => Err(ManagementError::Budget),
        result = v3::planning::confirmed_with_limits(db, &mut output.writer, binding, projection, limits, pages) => result.map_err(planning::safe),
    };
    output
        .finish(result, |output| sync(output.into_inner()))
        .await
}

fn sync(file: File) -> Result<()> {
    file.sync_all().map_err(|_| ManagementError::Database)
}
