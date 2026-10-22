//! Complete DB management history, explicitly private and archive-only.
//!
//! No imported value is accepted by confirmation, receipt replay or a business
//! writer. `Recovery` owns only task storage; it never opens a target database.
//! SQL originals retain their text/NULL representation. The binary payloads of
//! archive_chunks use hex cells; `Recovery::read_archive` decodes and verifies
//! their original S75A1 storage contract, including the original header bytes.
use super::{AppliedTrust, CellValue, Event, Frame, Projection, Section, Writer, files};
use crate::store::pinning::management::native::meter::Limits;
use crate::{
    pinning::management::{NativeKey, SafeError},
    store::pinning::management::{
        ManagementError,
        native::{meter::Budget, scan::history as source},
        transaction::{CancellationToken, ManagementTransaction, TransactionFailure},
    },
};
use sea_orm::DatabaseConnection;
use std::path::{Path, PathBuf};
use std::{
    fs::File,
    io::{Read, Write},
    time::Duration,
};
mod recovery;
mod spool;
pub use recovery::Recovery;
type Result<T> = std::result::Result<T, SafeError>;
fn storage(error: ManagementError) -> SafeError {
    match error {
        ManagementError::Budget => SafeError::LimitExceeded,
        ManagementError::Changed => SafeError::SnapshotChanged,
        _ => SafeError::UnknownEvidence,
    }
}
fn transaction(error: TransactionFailure) -> SafeError {
    if !error.rollback_confirmed {
        SafeError::UnknownEvidence
    } else {
        storage(error.management_error())
    }
}
fn invalid(_: impl std::fmt::Display) -> SafeError {
    SafeError::InvalidInput
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress {
    Pending,
    Sealed,
}

/// A bounded pass. `advance` yields after the caller's page allowance without
/// resetting the operation budget, deadline, cancellation or revision vector.
/// A failed pass cannot publish; start a new pass after SnapshotChanged.
pub struct Export {
    spool: spool::Spool,
    checkpoint: source::Checkpoint,
    principal: String,
    created_at: String,
    budget: Option<Budget>,
    sealed: bool,
    failed: bool,
    resume_cursor: Option<source::Checkpoint>,
    verification_started: bool,
}
impl Export {
    pub async fn start(
        db: &DatabaseConnection,
        principal: &str,
        budget: Budget,
        cancel: CancellationToken,
    ) -> Result<Self> {
        if principal.is_empty() || principal.len() > 4096 {
            return Err(SafeError::InvalidInput);
        }
        let deadline = Duration::from_secs(5).min(budget.remaining().map_err(storage)?);
        let mut tx = ManagementTransaction::begin(db, deadline, cancel)
            .await
            .map_err(transaction)?;
        let (checkpoint, mut budget) = tx
            .run(move |tx, _| {
                Box::pin(async move {
                    let mut budget = budget;
                    let checkpoint = source::start(tx, &mut budget).await?;
                    Ok((checkpoint, budget))
                })
            })
            .await
            .map_err(transaction)?;
        tx.rollback().await.map_err(transaction)?;
        let mut this = Self {
            spool: spool::Spool::create(&mut budget).await?,
            checkpoint,
            principal: principal.into(),
            created_at: chrono::Utc::now().to_rfc3339(),
            budget: Some(budget),
            sealed: false,
            failed: false,
            resume_cursor: None,
            verification_started: false,
        };
        this.save().await?;
        Ok(this)
    }
    pub fn budget(&self) -> Result<&Budget> {
        self.budget.as_ref().ok_or(SafeError::InvalidInput)
    }
    pub async fn close(self) -> Result<()> {
        self.spool.close().await
    }
    /// Close all handles while retaining this task's durable checkpoint. The
    /// returned local path is never embedded in an exported archive.
    pub async fn suspend(mut self) -> Result<PathBuf> {
        if self.failed || self.budget.is_none() {
            return Err(SafeError::InvalidInput);
        }
        self.save().await?;
        self.spool.suspend().await
    }
    /// Reopen an owner-protected local checkpoint within operator ceilings.
    /// Pending prefixes are compared with every original source slice before
    /// continuing. Sealed files are reverified without depending on current DB.
    pub async fn resume(path: &Path, ceiling: Limits) -> Result<Self> {
        let resumed = spool::Spool::resume(path, ceiling).await?;
        let resume_cursor = (!resumed.sealed).then(|| resumed.source.rewind());
        Ok(Self {
            spool: resumed.spool,
            checkpoint: resumed.source,
            principal: resumed.principal,
            created_at: resumed.created_at,
            budget: Some(resumed.budget),
            sealed: resumed.sealed,
            failed: false,
            resume_cursor,
            verification_started: false,
        })
    }
    async fn save(&mut self) -> Result<()> {
        self.spool
            .checkpoint(
                &self.checkpoint,
                self.budget.as_mut().ok_or(SafeError::InvalidInput)?,
                self.sealed,
                &self.principal,
                &self.created_at,
            )
            .await
    }
    pub async fn advance(
        &mut self,
        db: &DatabaseConnection,
        pages: u64,
        cancel: CancellationToken,
    ) -> Result<Progress> {
        if self.failed {
            return Err(SafeError::InvalidInput);
        }
        let result = self.advance_inner(db, pages, cancel).await;
        self.failed = result.is_err();
        if self.failed {
            self.spool.fail().await?;
        }
        result
    }
    async fn advance_inner(
        &mut self,
        db: &DatabaseConnection,
        pages: u64,
        cancel: CancellationToken,
    ) -> Result<Progress> {
        if self.sealed {
            return Ok(Progress::Sealed);
        }
        for _ in 0..pages {
            self.budget()?.check().map_err(storage)?;
            self.spool
                .active(self.budget.as_mut().ok_or(SafeError::InvalidInput)?)
                .await?;
            if self.resume_cursor.is_some() {
                self.verify_resume_page(db, cancel.clone()).await?;
                continue;
            }
            if self.checkpoint.complete() {
                recovery::ArchiveCells {
                    spool: &self.spool,
                    budget: self.budget.as_mut().ok_or(SafeError::InvalidInput)?,
                }
                .verify_archives()
                .await?;
                let mut writer = Writer::new(
                    files::stream::temporary().map_err(invalid)?,
                    Projection::PrivateRecovery,
                    self.budget.take().ok_or(SafeError::InvalidInput)?,
                )?;
                self.write(&mut writer).await?;
                let (mut file, budget) = writer.finish_with_budget()?;
                self.budget = Some(budget);
                self.spool
                    .store_wire(
                        &mut file,
                        self.budget.as_mut().ok_or(SafeError::InvalidInput)?,
                    )
                    .await?;
            }
            let deadline = Duration::from_secs(5).min(self.budget()?.remaining().map_err(storage)?);
            let mut tx = ManagementTransaction::begin(db, deadline, cancel.clone())
                .await
                .map_err(transaction)?;
            let mut budget = self.budget.take().ok_or(SafeError::InvalidInput)?;
            let checkpoint = self.checkpoint.clone();
            if checkpoint.complete() {
                let budget = tx
                    .run(move |tx, _| {
                        Box::pin(async move {
                            source::seal(tx, &checkpoint, &mut budget).await?;
                            Ok(budget)
                        })
                    })
                    .await
                    .map_err(transaction)?;
                tx.rollback().await.map_err(transaction)?;
                self.budget = Some(budget);
                self.sealed = true;
                self.save().await?;
                return Ok(Progress::Sealed);
            }
            let (page, budget) = tx
                .run(move |tx, _| {
                    Box::pin(async move {
                        let page = source::page(tx, &checkpoint, &mut budget).await?;
                        Ok((page, budget))
                    })
                })
                .await
                .map_err(transaction)?;
            tx.rollback().await.map_err(transaction)?;
            self.budget = Some(budget);
            self.spool
                .stage(
                    &page.parts,
                    &page.checkpoint,
                    self.budget.as_mut().ok_or(SafeError::InvalidInput)?,
                    &self.principal,
                    &self.created_at,
                )
                .await?;
            self.checkpoint = page.checkpoint.clone();
        }
        Ok(Progress::Pending)
    }
    async fn verify_resume_page(
        &mut self,
        db: &DatabaseConnection,
        cancel: CancellationToken,
    ) -> Result<()> {
        let mut budget = self.budget.take().ok_or(SafeError::InvalidInput)?;
        let checkpoint = self.resume_cursor.clone().ok_or(SafeError::InvalidInput)?;
        if !self.verification_started {
            let expected = self.checkpoint.rewind();
            let deadline = Duration::from_secs(5).min(budget.remaining().map_err(storage)?);
            let mut tx = ManagementTransaction::begin(db, deadline, cancel)
                .await
                .map_err(transaction)?;
            let budget = tx
                .run(move |tx, _| {
                    Box::pin(async move {
                        if source::start(tx, &mut budget).await? != expected {
                            return Err(ManagementError::Changed);
                        }
                        Ok(budget)
                    })
                })
                .await
                .map_err(transaction)?;
            tx.rollback().await.map_err(transaction)?;
            self.budget = Some(budget);
            self.spool
                .begin_verification(self.budget.as_mut().ok_or(SafeError::InvalidInput)?)
                .await?;
            self.verification_started = true;
        } else if checkpoint == self.checkpoint {
            self.spool.finish_verification(&mut budget).await?;
            self.resume_cursor = None;
            self.budget = Some(budget);
        } else {
            if checkpoint.complete() {
                return Err(SafeError::InvalidInput);
            }
            let deadline = Duration::from_secs(5).min(budget.remaining().map_err(storage)?);
            let mut tx = ManagementTransaction::begin(db, deadline, cancel)
                .await
                .map_err(transaction)?;
            let (page, budget) = tx
                .run(move |tx, _| {
                    Box::pin(async move {
                        let page = source::page(tx, &checkpoint, &mut budget).await?;
                        Ok((page, budget))
                    })
                })
                .await
                .map_err(transaction)?;
            tx.rollback().await.map_err(transaction)?;
            self.budget = Some(budget);
            for part in &page.parts {
                self.spool
                    .verify_part(part, self.budget.as_mut().ok_or(SafeError::InvalidInput)?)
                    .await?;
            }
            self.resume_cursor = Some(page.checkpoint.clone());
        }
        self.save().await
    }
    /// Only a sealed task can create a complete file. File reservation is
    /// exclusive and owner-protected, never creates parents, and never overwrites.
    pub async fn write_private(&mut self, path: &str) -> Result<()> {
        if self.failed || !self.sealed {
            return Err(SafeError::InvalidInput);
        }
        let mut file = files::reserve(path)
            .await
            .map_err(invalid)?
            .into_std()
            .await;
        self.spool
            .active(self.budget.as_mut().ok_or(SafeError::InvalidInput)?)
            .await?;
        let staged = self
            .spool
            .load_wire(self.budget.as_mut().ok_or(SafeError::InvalidInput)?)
            .await?;
        let budget = self.budget.take().ok_or(SafeError::InvalidInput)?;
        let (head, staged, budget) =
            super::document::inspect(staged, Projection::PrivateRecovery, budget)?;
        if head.database_scope != self.checkpoint.database_scope
            || head.database_identity != self.checkpoint.database_identity
            || head.principal != self.principal
            || head.catalog != self.checkpoint.catalog
            || head.created_at != self.created_at
            || head.expires_at.is_some()
            || head.request.is_some()
            || head.configuration.is_some()
            || head.acknowledgements.is_some()
            || !head.items.is_empty()
        {
            return Err(SafeError::InvalidInput);
        }
        drop(head);
        self.budget = Some(budget);
        // inspect verified the complete private source on this same task-owned
        // handle. Copy exact bytes; output errors retain the charged budget.
        let mut staged = staged;
        let budget = self.budget.as_mut().ok_or(SafeError::InvalidInput)?;
        let _memory = budget.reserve(super::CELL_BYTES).map_err(storage)?;
        let durable = (|| {
            let mut bytes = vec![0; super::CELL_BYTES];
            loop {
                budget.check().map_err(storage)?;
                let n = staged.read(&mut bytes).map_err(invalid)?;
                if n == 0 {
                    break;
                }
                budget.charge(0, 0, 0, 2 * n as u64).map_err(storage)?;
                file.write_all(&bytes[..n]).map_err(invalid)?;
                budget.work.output_bytes = budget
                    .work
                    .output_bytes
                    .checked_add(n as u64)
                    .ok_or(SafeError::LimitExceeded)?;
                budget.work.copied_bytes = budget
                    .work
                    .copied_bytes
                    .checked_add(n as u64)
                    .ok_or(SafeError::LimitExceeded)?;
            }
            file.sync_all().map_err(invalid)
        })();
        self.save().await?;
        durable
    }
    async fn write(&self, writer: &mut Writer<File>) -> Result<()> {
        writer.frame(&Frame::Header {
            schema_version: super::SCHEMA_VERSION,
            projection: Projection::PrivateRecovery,
            database_scope: self.checkpoint.database_scope.clone(),
            database_identity: self.checkpoint.database_identity.clone(),
            principal: self.principal.clone(),
            applied_trust: AppliedTrust::UntrustedClaimsOnly,
            catalog: self.checkpoint.catalog.clone(),
            created_at: self.created_at.clone(),
            expires_at: None,
        })?;
        writer.frame(&Frame::Request { request: None })?;
        writer.frame(&Frame::Configuration {
            configuration: None,
        })?;
        writer.frame(&Frame::Acknowledgements { item_ids: None })?;
        writer.frame(&Frame::Summary {
            summary: Default::default(),
        })?;
        self.spool.write_rows(writer).await
    }
}

/// Explicit archive recovery. The input must be a complete PrivateRecovery v3
/// history file with Request(None); restored cells are review data, not authority.
pub async fn restore_private(path: &str, budget: Budget) -> Result<Recovery> {
    recovery::restore(path, budget).await
}
