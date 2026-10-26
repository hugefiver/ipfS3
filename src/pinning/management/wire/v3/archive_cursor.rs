//! Frozen runtime archives use a complete private v3 file as their S75A1 body.
//! This module does not convert arbitrary/legacy S75A1 bodies. It stages raw
//! storage chunks, verifies the storage digest AND v3 terminal marker, and then
//! exposes the existing wire events. Neither a file reference nor this cursor
//! grants confirmation, applied-receipt trust, or permission to modify business
//! rows. File spools must be task-owned protected exclusive-create handles.
use super::{Event, Frame, Projection, Reader, Writer};
use crate::pinning::management::SafeError;
use crate::store::pinning::management::{
    ManagementError,
    native::meter::Budget,
    records::segmented::{self, ArchiveHeader, ArchiveRef},
};
use sea_orm::{ConnectionTrait, DatabaseTransaction};
use std::io::{Read, Seek, SeekFrom, Write};
mod binding;
mod projection;

pub type ArchiveEvent = Event;
type Result<T> = std::result::Result<T, SafeError>;
fn storage(error: ManagementError) -> SafeError {
    match error {
        ManagementError::Budget => SafeError::LimitExceeded,
        ManagementError::Changed => SafeError::SnapshotChanged,
        _ => SafeError::UnknownEvidence,
    }
}
fn seek_start(spool: &mut impl Seek) -> Result<()> {
    spool
        .seek(SeekFrom::Start(0))
        .map_err(|_| SafeError::InvalidInput)?;
    Ok(())
}

/// Structurally validated import, NOT a locally authorized plan. Its bounded
/// request/config/ACK/item facts and all original rows remain streamed events.
/// The local binder must separately authenticate scope, physical identity,
/// principal, original membership, configuration, ACKs and durable receipts.
pub struct StagedImport<S> {
    cursor: ReviewCursor<S>,
}
impl<S: Read + Seek> StagedImport<S> {
    pub fn into_cursor(self) -> ReviewCursor<S> {
        self.cursor
    }
    pub fn archive_claim(&self) -> Option<&ArchiveRef> {
        self.cursor.archive_claim()
    }
}

pub struct ReviewCursor<S> {
    reader: Reader<S>,
    source_projection: Projection,
    projection: projection::ProjectionState,
    reference_claim: Option<ArchiveRef>,
    started: bool,
}
impl ReviewCursor<std::fs::File> {
    pub fn sync_spool(&self) -> Result<()> {
        self.reader
            .source_ref()
            .sync_all()
            .map_err(|_| SafeError::InvalidInput)?;
        self.reader.budget().check().map_err(storage)
    }
}
impl<S: Read + Seek> ReviewCursor<S> {
    pub(super) fn take_budget(&mut self) -> Budget {
        self.reader.take_budget()
    }
    pub(super) fn restore_budget(&mut self, budget: Budget) {
        self.reader.restore_budget(budget);
    }
    pub fn archive_claim(&self) -> Option<&ArchiveRef> {
        self.reference_claim.as_ref()
    }
    pub fn budget(&self) -> &Budget {
        self.reader.budget()
    }
    pub fn budget_mut(&mut self) -> &mut Budget {
        self.reader.budget_mut()
    }
    pub fn verified(&self) -> bool {
        self.reader.verified()
    }
    pub fn project_to(mut self, projection: Projection) -> Result<Self> {
        if self.started
            || (self.source_projection == Projection::Review
                && projection == Projection::PrivateRecovery)
        {
            return Err(SafeError::InvalidInput);
        }
        self.projection = projection::ProjectionState::new(projection);
        Ok(self)
    }
    /// Consume each event before requesting another. EOF is successful only when
    /// the underlying Reader has reverified the complete file and terminal EOF.
    pub fn next_event(&mut self) -> Result<Option<ArchiveEvent>> {
        self.started = true;
        loop {
            match self.reader.next_frame()? {
                Some(event) => {
                    if let Some(event) = self.projection.apply(event) {
                        return Ok(Some(event));
                    }
                }
                None => return Ok(None),
            }
        }
    }
    pub fn into_verified(self) -> Result<(S, Budget)> {
        self.reader.into_verified()
    }

    /// Same operation budget moves between the input decoder and output encoder.
    /// Source read, digest or projection failure prevents the complete marker.
    /// Output/durability errors never return successful publication; the owner
    /// must sync the returned output before publishing it.
    pub fn export<W: Write>(self, output: W, projection: Projection) -> Result<(W, Budget)> {
        let mut cursor = self.project_to(projection)?;
        let mut writer = Writer::new(output, projection, cursor.reader.take_budget())?;
        loop {
            cursor.reader.restore_budget(writer.take_budget());
            let event = cursor.next_event();
            writer.restore_budget(cursor.reader.take_budget());
            match event? {
                Some(Event::Frame(frame)) => writer.frame(&frame)?,
                Some(Event::Chunk(bytes)) => writer
                    .cell_output()
                    .write_all(&bytes)
                    .map_err(|_| SafeError::InvalidInput)?,
                None => break,
            }
        }
        writer.finish_with_budget()
    }
}

/// A previously protected spool containing a complete untrusted v3 file. This
/// verifies all frames before returning a handle; no DB is queried or changed.
pub fn stage_import<S: Read + Seek>(
    spool: S,
    projection: Projection,
    budget: Budget,
) -> Result<StagedImport<S>> {
    stage(spool, projection, budget, None, None)
}

fn stage<S: Read + Seek>(
    spool: S,
    projection: Projection,
    budget: Budget,
    header: Option<&ArchiveHeader>,
    reference_claim: Option<ArchiveRef>,
) -> Result<StagedImport<S>> {
    stage_checked(
        spool,
        projection,
        budget,
        reference_claim,
        |frame, budget| match header {
            Some(header) => binding::check(header, frame, budget),
            None => Ok(()),
        },
    )
}

/// Internal typed archive adapters supply their own envelope contract. This
/// does not change the ordinary Request-based archive binding above.
pub(crate) fn stage_checked<S: Read + Seek>(
    mut spool: S,
    projection: Projection,
    budget: Budget,
    reference_claim: Option<ArchiveRef>,
    mut check: impl FnMut(&Frame, &mut Budget) -> Result<()>,
) -> Result<StagedImport<S>> {
    seek_start(&mut spool)?;
    let mut reader = Reader::new(spool, projection, budget)?;
    reader.enable_buffering()?;
    while let Some(event) = reader.next_frame()? {
        if let Event::Frame(frame) = &event {
            check(frame, reader.budget_mut())?;
        }
    }
    let (mut spool, budget) = reader.into_verified()?;
    seek_start(&mut spool)?;
    let mut reader = Reader::new(spool, projection, budget)?;
    reader.enable_buffering()?;
    Ok(StagedImport {
        cursor: ReviewCursor {
            reader,
            source_projection: projection,
            projection: projection::ProjectionState::new(projection),
            reference_claim,
            started: false,
        },
    })
}

/// Copy exact sealed bytes to the empty task spool first. No cursor or input
/// events escape before storage digest, header/body binding and v3 EOF succeed.
/// An ArchiveRef obtained from a file remains a claim even when this read works.
pub async fn load<C: ConnectionTrait, S: Read + Write + Seek>(
    db: &C,
    reference: &ArchiveRef,
    mut spool: S,
    projection: Projection,
    mut budget: Budget,
) -> Result<ReviewCursor<S>> {
    if reference.archive_id.is_empty()
        || reference.archive_id.len() > 1024
        || reference.bytes < super::MAGIC.len() as i64
        || reference.chunks < 0
        || reference.digest.len() != 64
        || !reference
            .digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(SafeError::InvalidInput);
    }
    if spool
        .seek(SeekFrom::End(0))
        .map_err(|_| SafeError::InvalidInput)?
        != 0
    {
        return Err(SafeError::InvalidInput);
    }
    let header = segmented::read_archive(db, reference, &mut spool, &mut budget)
        .await
        .map_err(storage)?;
    let _header_memory = budget
        .reserve(binding::resident(&header)?)
        .map_err(storage)?;
    budget.work.copied_bytes = budget
        .work
        .copied_bytes
        .checked_add(reference.bytes as u64)
        .ok_or(SafeError::LimitExceeded)?;
    Ok(stage(
        spool,
        projection,
        budget,
        Some(&header),
        Some(reference.clone()),
    )?
    .into_cursor())
}

/// Production staging seam for NEW runtime archives: the complete body must be
/// private v3, not a serialized Snapshot or arbitrary historical archive bytes.
/// This seals byte storage only, never confirmation/authority/readiness. The
/// caller owns the transaction and must roll it back on every error.
pub async fn seal_private<S: Read + Seek>(
    tx: &DatabaseTransaction,
    id: &str,
    header: &ArchiveHeader,
    staged: StagedImport<S>,
) -> Result<(ArchiveRef, Budget)> {
    let cursor = staged.cursor;
    if cursor.started || cursor.source_projection != Projection::PrivateRecovery {
        return Err(SafeError::InvalidInput);
    }
    let (spool, mut budget) = cursor.reader.into_staging_parts();
    let header_memory = budget
        .reserve(binding::resident(header)?)
        .map_err(storage)?;
    let staged = stage(
        spool,
        Projection::PrivateRecovery,
        budget,
        Some(header),
        None,
    )?;
    let (mut spool, mut budget) = staged.cursor.reader.into_staging_parts();
    drop(header_memory);
    seek_start(&mut spool)?;
    let reference = segmented::write_archive(tx, id, header, &mut spool, &mut budget)
        .await
        .map_err(storage)?;
    budget.check().map_err(storage)?;
    Ok((reference, budget))
}
