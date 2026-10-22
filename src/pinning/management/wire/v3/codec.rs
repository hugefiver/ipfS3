use super::{state::State, strict, *};
use crate::store::pinning::management::native::meter::ResidentGuard;
use crate::{
    pinning::management::SafeError,
    store::pinning::management::native::meter::{Budget, WorkMeter},
};
use sha2::digest::common::hazmat::{SerializableState, SerializedState};
use sha2::{Digest, Sha256};
use std::io::{self, Read, Write};

#[cfg(test)]
mod continuation_tests;
mod native_source;
mod read_buffer;
mod short_cell;
use short_cell::PendingCell;
pub(super) use short_cell::Record;
#[cfg(test)]
mod reader_tests;
#[cfg(test)]
mod short_cell_tests;
use read_buffer::{ReadBuffer, probe_eof, read_direct_exact};

// Projection moves one operation budget between its reader and writer. Empty
// slots are private to that handoff; no work, deadline or cancellation is reset.
struct BudgetSlot(Option<Budget>);
impl BudgetSlot {
    fn take(&mut self) -> Budget {
        self.0.take().expect("codec owns operation budget")
    }
    fn restore(&mut self, budget: Budget) {
        assert!(self.0.is_none());
        self.0 = Some(budget);
    }
}
impl std::ops::Deref for BudgetSlot {
    type Target = Budget;
    fn deref(&self) -> &Budget {
        self.0.as_ref().expect("codec owns operation budget")
    }
}
impl std::ops::DerefMut for BudgetSlot {
    fn deref_mut(&mut self) -> &mut Budget {
        self.0.as_mut().expect("codec owns operation budget")
    }
}

type Result<T> = std::result::Result<T, SafeError>;
fn invalid(_: io::Error) -> SafeError {
    SafeError::InvalidInput
}
pub(super) fn write_counted(
    output: &mut impl Write,
    work: &mut WorkMeter,
    mut bytes: &[u8],
) -> Result<()> {
    while !bytes.is_empty() {
        match output.write(bytes) {
            Ok(0) => return Err(SafeError::InvalidInput),
            Ok(n) => {
                work.output_bytes = work
                    .output_bytes
                    .checked_add(n as u64)
                    .ok_or(SafeError::LimitExceeded)?;
                bytes = &bytes[n..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(invalid(error)),
        }
    }
    Ok(())
}
fn limit<T>(result: crate::store::pinning::management::Result<T>) -> Result<T> {
    result.map_err(|_| SafeError::LimitExceeded)
}

fn record_header(kind: u8, payload_len: usize) -> [u8; 5] {
    let mut header = [0; 5];
    header[..4].copy_from_slice(&((payload_len + 1) as u32).to_be_bytes());
    header[4] = kind;
    header
}

fn column_copy(budget: &mut Budget, column: &str) -> Result<()> {
    limit(budget.check())?;
    budget.work.copied_bytes = budget
        .work
        .copied_bytes
        .checked_add(column.len() as u64)
        .ok_or(SafeError::LimitExceeded)?;
    limit(budget.check())
}

struct Buffer(Vec<u8>);
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > FRAME_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("frame limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Integrity {
    digest: Sha256,
    frames: u64,
    bytes: u64,
}
impl Integrity {
    fn new() -> Self {
        Self {
            digest: Sha256::new_with_prefix(MAGIC),
            frames: 0,
            bytes: MAGIC.len() as u64,
        }
    }
    fn add(&mut self, kind: u8, payload: &[u8]) -> Result<()> {
        self.digest.update(record_header(kind, payload.len()));
        if !payload.is_empty() {
            self.digest.update(payload);
        }
        self.frames = self.frames.checked_add(1).ok_or(SafeError::LimitExceeded)?;
        self.bytes = self
            .bytes
            .checked_add(payload.len() as u64 + 5)
            .ok_or(SafeError::LimitExceeded)?;
        Ok(())
    }
    fn trailer(&self) -> Frame {
        Frame::Trailer {
            frames: self.frames,
            bytes: self.bytes,
            sha256: hex::encode(self.digest.clone().finalize()),
        }
    }
    fn verify(&self, frames: u64, bytes: u64, sha256: &str) -> Result<()> {
        if frames != self.frames
            || bytes != self.bytes
            || sha256 != hex::encode(self.digest.clone().finalize())
        {
            return Err(SafeError::InvalidInput);
        }
        Ok(())
    }
}

/// No implicit whole-archive size cap: the caller supplies the operation-wide
/// Budget (including total bytes, resident space, deadline and cancellation).
/// A failure poisons the writer; only `finish` emits a terminal complete marker.
pub struct Writer<W> {
    output: W,
    state: State,
    integrity: Integrity,
    budget: BudgetSlot,
    _state_memory: ResidentGuard,
    failed: bool,
}
/// Internal continuation accepted only together with a target archive's CAS.
/// It is not a wire input, review proof, or stand-alone authority token.
#[derive(Serialize, Deserialize)]
pub(crate) struct WriterContinuation {
    state: State,
    digest: String,
    frames: u64,
    bytes: u64,
}
impl WriterContinuation {
    pub(crate) fn projection(&self) -> Projection {
        self.state.projection
    }
}
impl<W: Write> Writer<W> {
    pub(crate) fn continue_metered(
        output: W,
        continuation: WriterContinuation,
        mut budget: Budget,
    ) -> std::result::Result<Self, (SafeError, Box<Budget>)> {
        let resumed = (|| {
            limit(budget.check())?;
            let memory = limit(budget.reserve(FRAME_BYTES))?;
            let bytes = hex::decode(&continuation.digest).map_err(|_| SafeError::InvalidInput)?;
            let state = SerializedState::<Sha256>::try_from(bytes.as_slice())
                .map_err(|_| SafeError::InvalidInput)?;
            let digest = <Sha256 as SerializableState>::deserialize(&state)
                .map_err(|_| SafeError::InvalidInput)?;
            Ok::<_, SafeError>((memory, digest))
        })();
        let (memory, digest) = match resumed {
            Ok(value) => value,
            Err(error) => return Err((error, Box::new(budget))),
        };
        Ok(Self {
            output,
            state: continuation.state,
            integrity: Integrity {
                digest,
                frames: continuation.frames,
                bytes: continuation.bytes,
            },
            budget: BudgetSlot(Some(budget)),
            _state_memory: memory,
            failed: false,
        })
    }
    pub(crate) fn pause(mut self) -> (Result<(W, WriterContinuation)>, Budget) {
        let result = (|| {
            if self.failed {
                return Err(SafeError::InvalidInput);
            }
            self.output.flush().map_err(invalid)?;
            limit(self.budget.check())
        })();
        let budget = self.budget.take();
        let continuation = WriterContinuation {
            state: self.state,
            digest: hex::encode(SerializableState::serialize(&self.integrity.digest)),
            frames: self.integrity.frames,
            bytes: self.integrity.bytes,
        };
        (result.map(|()| (self.output, continuation)), budget)
    }
    pub fn new(output: W, projection: Projection, budget: Budget) -> Result<Self> {
        Self::new_metered(output, projection, budget).map_err(|(error, _)| error)
    }
    /// A failed initial write still returns the operation's consumed work.
    pub fn new_metered(
        mut output: W,
        projection: Projection,
        mut budget: Budget,
    ) -> std::result::Result<Self, (SafeError, Box<Budget>)> {
        // Two bounded key copies plus ACK/item identities survive frame buffers.
        let started: Result<_> = (|| {
            let state_memory = limit(budget.reserve(FRAME_BYTES))?;
            limit(budget.charge(0, 0, 0, MAGIC.len() as u64))?;
            write_counted(&mut output, &mut budget.work, MAGIC)?;
            Ok(state_memory)
        })();
        let state_memory = match started {
            Ok(memory) => memory,
            Err(error) => return Err((error, Box::new(budget))),
        };
        Ok(Self {
            output,
            state: State::new(projection),
            integrity: Integrity::new(),
            budget: BudgetSlot(Some(budget)),
            _state_memory: state_memory,
            failed: false,
        })
    }
    pub fn budget(&self) -> &Budget {
        &self.budget
    }
    /// Charge producer SQL/page work to this same operation before emitting it.
    pub fn budget_mut(&mut self) -> &mut Budget {
        &mut self.budget
    }
    pub(super) fn take_budget(&mut self) -> Budget {
        self.budget.take()
    }
    pub(super) fn restore_budget(&mut self, budget: Budget) {
        self.budget.restore(budget);
    }
    pub fn frame(&mut self, frame: &Frame) -> Result<()> {
        let result = self.frame_inner(frame);
        self.failed |= result.is_err();
        result
    }
    fn frame_inner(&mut self, frame: &Frame) -> Result<()> {
        if self.failed || matches!(frame, Frame::Trailer { .. }) {
            return Err(SafeError::InvalidInput);
        }
        limit(self.budget.check())?;
        let redacted;
        let frame = if self.state.redacting()
            && matches!(
                frame,
                Frame::CellEnd {
                    value: CellValue::Text
                }
            ) {
            redacted = Frame::CellEnd {
                value: CellValue::Redacted,
            };
            &redacted
        } else {
            frame
        };
        let compact = match frame {
            Frame::Cell { column } => {
                if column.is_empty() || column.len() > 256 {
                    return Err(SafeError::InvalidInput);
                }
                Some((2, column.as_bytes()))
            }
            Frame::CellEnd { value } => Some((
                match value {
                    CellValue::Null => 3,
                    CellValue::Text => 4,
                    CellValue::Redacted => 5,
                },
                &[][..],
            )),
            Frame::RowEnd => Some((6, &[][..])),
            _ => None,
        };
        if let Some((kind, payload)) = compact {
            let _resident = limit(
                self.budget
                    .reserve(2 * payload.len() + std::mem::size_of::<Frame>()),
            )?;
            if let Frame::Cell { column } = frame {
                column_copy(&mut self.budget, column)?;
            }
            self.state.frame(frame)?;
            return self.emit(kind, payload, true);
        }
        let _resident = limit(self.budget.reserve(5 * FRAME_BYTES))?;
        let mut buffer = Buffer(Vec::new());
        serde_json::to_writer(&mut buffer, frame).map_err(|_| SafeError::LimitExceeded)?;
        strict::check(&buffer.0)?;
        self.state.frame(frame)?;
        if matches!(frame, Frame::Row { .. }) {
            limit(self.budget.charge(0, 1, 0, 0))?;
        }
        self.emit(0, &buffer.0, true)
    }
    fn emit(&mut self, kind: u8, payload: &[u8], hashed: bool) -> Result<()> {
        limit(self.budget.charge(0, 0, 1, payload.len() as u64 + 5))?;
        write_counted(
            &mut self.output,
            &mut self.budget.work,
            &record_header(kind, payload.len()),
        )?;
        write_counted(&mut self.output, &mut self.budget.work, payload)?;
        if hashed {
            self.integrity.add(kind, payload)?;
        }
        Ok(())
    }
    fn chunk(&mut self, bytes: &[u8]) -> Result<()> {
        let result = (|| {
            if self.failed {
                return Err(SafeError::InvalidInput);
            }
            limit(self.budget.check())?;
            let suppressed = self.state.redacting();
            self.state.chunk(bytes, suppressed)?;
            if suppressed {
                limit(self.budget.charge(0, 0, 0, bytes.len() as u64))
            } else {
                self.emit(1, bytes, true)
            }
        })();
        self.failed |= result.is_err();
        result
    }
    /// Adapter for Write-based sources (SQL callers should use native_cell to
    /// share its budget rather than introducing a separate source budget).
    /// Begin with Cell, stream bytes, then emit CellEnd with Null/Text according
    /// to the source's result. Confidential review bytes are never emitted.
    pub fn cell_output(&mut self) -> CellOutput<'_, W> {
        CellOutput(self)
    }
    pub fn finish(self) -> Result<W> {
        self.finish_with_budget().map(|(output, _)| output)
    }
    pub fn finish_with_budget(self) -> Result<(W, Budget)> {
        let (output, budget) = self.finalize();
        output.map(|output| (output, budget))
    }
    /// Recover work without emitting a trailer after a producer failure.
    pub fn abort(mut self) -> Budget {
        self.budget.take()
    }
    /// Footer and flush failures retain the same budget, including failed work.
    pub fn finalize(mut self) -> (Result<W>, Budget) {
        let result = self.finish_inner();
        let budget = self.budget.take();
        (result.map(|()| self.output), budget)
    }
    fn finish_inner(&mut self) -> Result<()> {
        if self.failed {
            return Err(SafeError::InvalidInput);
        }
        let trailer = self.integrity.trailer();
        self.state.frame(&trailer)?;
        let mut buffer = Buffer(Vec::new());
        serde_json::to_writer(&mut buffer, &trailer).map_err(|_| SafeError::InvalidInput)?;
        self.emit(0, &buffer.0, false)?;
        self.output.flush().map_err(invalid)?;
        limit(self.budget.check())?;
        Ok(())
    }
}
pub struct CellOutput<'a, W>(&'a mut Writer<W>);
impl<W: Write> Write for CellOutput<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for chunk in bytes.chunks(CELL_BYTES) {
            self.0.chunk(chunk).map_err(io::Error::other)?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Events remain untrusted staging data until EOF immediately after the verified
/// trailer. No receipt or applied claim becomes local authority through decoding.
/// Consume/drop each event before reading the next; consumers retaining decoded
/// payloads must account for those allocations in their own staging budget.
pub struct Reader<R> {
    input: R,
    buffer: Option<ReadBuffer>,
    state: State,
    integrity: Integrity,
    budget: BudgetSlot,
    _state_memory: ResidentGuard,
    failed: bool,
    verified: bool,
    started: bool,
    last_frame_resident: usize,
    pending: Option<PendingCell>,
}
impl<R: Read> Reader<R> {
    pub fn new(input: R, projection: Projection, budget: Budget) -> Result<Self> {
        Self::new_metered(input, projection, budget).map_err(|(error, _)| error)
    }
    /// Moving a writer budget into a reader must not lose it on an initial error.
    pub fn new_metered(
        mut input: R,
        projection: Projection,
        mut budget: Budget,
    ) -> std::result::Result<Self, (SafeError, Box<Budget>)> {
        let started: Result<_> = (|| {
            let state_memory = limit(budget.reserve(FRAME_BYTES))?;
            limit(budget.charge(0, 0, 0, MAGIC.len() as u64))?;
            let mut magic = [0; MAGIC.len()];
            read_direct_exact(&mut input, &budget, &mut magic)?;
            if magic != *MAGIC {
                return Err(SafeError::UnsupportedSchemaVersion);
            }
            Ok(state_memory)
        })();
        let state_memory = match started {
            Ok(memory) => memory,
            Err(error) => return Err((error, Box::new(budget))),
        };
        Ok(Self {
            input,
            buffer: None,
            state: State::new(projection),
            integrity: Integrity::new(),
            budget: BudgetSlot(Some(budget)),
            _state_memory: state_memory,
            failed: false,
            verified: false,
            started: false,
            last_frame_resident: 0,
            pending: None,
        })
    }
    pub fn verified(&self) -> bool {
        self.verified
    }
    /// Opt in before the first frame to a resident-metered 64 KiB heap cache.
    /// This reserves memory without reading or advancing the underlying input.
    pub fn enable_buffering(&mut self) -> Result<()> {
        let result = (|| {
            if self.failed || self.started || self.verified {
                return Err(SafeError::InvalidInput);
            }
            limit(self.budget.check())?;
            if self.buffer.is_none() {
                self.buffer = Some(ReadBuffer::new(&mut self.budget)?);
            }
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
            self.budget.poison_reader();
        }
        result
    }
    pub(super) fn source_ref(&self) -> &R {
        &self.input
    }
    pub fn budget(&self) -> &Budget {
        &self.budget
    }
    /// Local comparison/staging work shares the decoder's cumulative budget.
    pub fn budget_mut(&mut self) -> &mut Budget {
        &mut self.budget
    }
    pub(super) fn last_frame_resident(&self) -> usize {
        self.last_frame_resident
    }
    pub(super) fn take_budget(&mut self) -> Budget {
        self.budget.take()
    }
    pub(super) fn restore_budget(&mut self, budget: Budget) {
        self.budget.restore(budget);
    }
    pub(super) fn into_staging_parts(mut self) -> (R, Budget) {
        (self.input, self.budget.take())
    }
    /// Recover the same operation budget only after the complete file is valid.
    pub fn into_verified(self) -> Result<(R, Budget)> {
        if !self.verified || self.failed {
            return Err(SafeError::InvalidInput);
        }
        Ok(self.into_staging_parts())
    }
    pub fn next_frame(&mut self) -> Result<Option<Event>> {
        self.started = true;
        let result = self.next_public();
        self.failed |= result.is_err();
        result
    }
    fn next_public(&mut self) -> Result<Option<Event>> {
        if self.failed {
            return Err(SafeError::InvalidInput);
        }
        if self.pending.is_none() {
            match self.next_inner()? {
                Some(Record::Event(event)) => return Ok(Some(event)),
                Some(Record::ShortCell(cell)) => self.pending = Some(PendingCell::new(cell)),
                None => return Ok(None),
            }
        }
        limit(self.budget.check())?;
        let (event, resident, done) = self.pending.as_mut().unwrap().next();
        self.last_frame_resident = resident;
        if done {
            self.pending = None;
        }
        Ok(Some(event))
    }
    pub(super) fn next_record(&mut self) -> Result<Option<Record>> {
        self.started = true;
        let result = if self.pending.is_some() {
            Err(SafeError::InvalidInput)
        } else {
            self.next_inner()
        };
        self.failed |= result.is_err();
        result
    }
    fn prepay_read(&mut self, bytes: usize) -> Result<()> {
        let (raw, copied) = match &self.buffer {
            Some(buffer) => (bytes.saturating_sub(buffer.available()), bytes),
            None => (bytes, 0),
        };
        limit(self.budget.prepay_reader(raw as u64, copied as u64))
    }
    fn read_prepaid_exact(&mut self, bytes: &mut [u8]) -> Result<()> {
        match &mut self.buffer {
            Some(buffer) => buffer.read_prepaid_exact(&mut self.input, &mut self.budget, bytes),
            None => read_direct_exact(&mut self.input, &self.budget, bytes),
        }
    }
    fn next_inner(&mut self) -> Result<Option<Record>> {
        if self.failed {
            return Err(SafeError::InvalidInput);
        }
        limit(self.budget.check())?;
        if self.verified {
            return Ok(None);
        }
        self.last_frame_resident = 0;
        // Prepay each bounded portion before I/O so short reads and cancellation
        // retain their debt without trying to settle through a failed budget.
        let cached_header = self
            .buffer
            .as_ref()
            .is_some_and(|buffer| buffer.available() >= 5);
        let header_bytes: usize = if cached_header { 5 } else { 4 };
        let (raw, copied) = match &self.buffer {
            Some(buffer) => (
                header_bytes.saturating_sub(buffer.available()),
                header_bytes,
            ),
            None => (header_bytes, 0),
        };
        limit(
            self.budget
                .prepay_reader_frame_header(raw as u64, copied as u64),
        )?;
        let mut header = [0; 5];
        self.read_prepaid_exact(&mut header[..header_bytes])?;
        let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if length == 0 || length > FRAME_BYTES + 1 {
            return Err(SafeError::LimitExceeded);
        }
        if !cached_header {
            self.prepay_read(1)?;
            self.read_prepaid_exact(&mut header[4..])?;
        }
        let kind = header[4];
        if !match kind {
            0 => true,
            1 => (2..=CELL_BYTES + 1).contains(&length),
            2 => (2..=257).contains(&length),
            3..=6 => length == 1,
            7 => (5..=4 + 256 + CELL_BYTES).contains(&length),
            _ => false,
        } {
            return Err(SafeError::InvalidInput);
        }
        if kind == 7 {
            return self.read_short_cell(length).map(Some);
        }
        // Prepay work and peak parsing/key state before requesting the payload.
        if length > 1 {
            self.prepay_read(length - 1)?;
        }
        let resident = match kind {
            0 => 5 * FRAME_BYTES,
            1 => length - 1 + 4,
            _ => 2 * (length - 1) + std::mem::size_of::<Frame>(),
        };
        let _resident = limit(self.budget.reserve(resident))?;
        let mut payload = vec![0; length - 1];
        if !payload.is_empty() {
            self.read_prepaid_exact(&mut payload)?;
        }
        if kind == 1 {
            if self.state.redacting() {
                return Err(SafeError::InvalidInput);
            }
            self.state.chunk(&payload, false)?;
            self.integrity.add(1, &payload)?;
            self.last_frame_resident = resident;
            return Ok(Some(Record::Event(Event::Chunk(payload))));
        }
        if kind != 0 {
            self.last_frame_resident = resident;
            // Integrity uses the physical payload before its allocation becomes
            // the column String. State retains its own independently billed copy.
            self.integrity.add(kind, &payload)?;
            let frame = match kind {
                2 => Frame::Cell {
                    column: String::from_utf8(payload).map_err(|_| SafeError::InvalidInput)?,
                },
                3 => Frame::CellEnd {
                    value: CellValue::Null,
                },
                4 => Frame::CellEnd {
                    value: CellValue::Text,
                },
                5 => Frame::CellEnd {
                    value: CellValue::Redacted,
                },
                6 => Frame::RowEnd,
                _ => unreachable!(),
            };
            if let Frame::Cell { column } = &frame {
                column_copy(&mut self.budget, column)?;
            }
            self.state.frame(&frame)?;
            return Ok(Some(Record::Event(Event::Frame(frame))));
        }
        self.last_frame_resident = strict::resident_cost(&payload)?;
        let frame: Frame = serde_json::from_slice(&payload).map_err(|_| SafeError::InvalidInput)?;
        if matches!(
            frame,
            Frame::Cell { .. } | Frame::CellEnd { .. } | Frame::RowEnd
        ) {
            return Err(SafeError::InvalidInput);
        }
        self.state.frame(&frame)?;
        if matches!(&frame, Frame::Row { .. }) {
            limit(self.budget.charge(0, 1, 0, 0))?;
        }
        if let Frame::Trailer {
            frames,
            bytes,
            sha256,
        } = &frame
        {
            self.integrity.verify(*frames, *bytes, sha256)?;
            if self
                .buffer
                .as_ref()
                .is_some_and(|buffer| buffer.available() != 0)
            {
                return Err(SafeError::InvalidInput);
            }
            self.prepay_read(1)?;
            probe_eof(&mut self.input, &self.budget)?;
            self.verified = true;
            return Ok(None);
        }
        self.integrity.add(0, &payload)?;
        Ok(Some(Record::Event(Event::Frame(frame))))
    }
}

#[cfg(test)]
mod finalizer_tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};

    #[test]
    fn integrity_covers_the_complete_physical_record_sequence() {
        let payload = vec![0xa5; 65_536];
        let records: &[(u8, &[u8])] = &[
            (0, b"{}"),
            (1, &payload),
            (2, b"column"),
            (3, b""),
            (4, b""),
            (5, b""),
            (6, b""),
            (7, b"\0\x06\x01columntext"),
        ];
        let mut physical = MAGIC.to_vec();
        let mut integrity = Integrity::new();
        for &(kind, bytes) in records {
            physical.extend_from_slice(&(bytes.len() as u32 + 1).to_be_bytes());
            physical.push(kind);
            physical.extend_from_slice(bytes);
            integrity.add(kind, bytes).unwrap();
        }
        let expected = hex::encode(Sha256::digest(&physical));
        integrity
            .verify(records.len() as u64, physical.len() as u64, &expected)
            .unwrap();
    }

    struct FailingOutput {
        fail_write: Rc<Cell<bool>>,
        accept_prefix: bool,
        fail_flush: bool,
        flushed: Rc<Cell<bool>>,
    }
    impl Write for FailingOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.fail_write.get() {
                if self.accept_prefix {
                    self.accept_prefix = false;
                    Ok(bytes.len())
                } else {
                    Err(io::Error::other("output unavailable"))
                }
            } else {
                Ok(bytes.len())
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flushed.set(true);
            if self.fail_flush {
                Err(io::Error::other("output unavailable"))
            } else {
                Ok(())
            }
        }
    }
    fn ready(output: FailingOutput) -> Writer<FailingOutput> {
        let mut writer = Writer::new(output, Projection::Review, Budget::default()).unwrap();
        for frame in [
            Frame::Header {
                schema_version: SCHEMA_VERSION,
                projection: Projection::Review,
                database_scope: "scope".into(),
                database_identity: "identity".into(),
                principal: "principal".into(),
                applied_trust: AppliedTrust::UntrustedClaimsOnly,
                catalog: vec!["catalog".into()],
                created_at: "time".into(),
                expires_at: None,
            },
            Frame::Request { request: None },
            Frame::Configuration {
                configuration: None,
            },
            Frame::Acknowledgements { item_ids: None },
            Frame::Summary {
                summary: Default::default(),
            },
        ] {
            writer.frame(&frame).unwrap();
        }
        writer
    }
    #[test]
    fn footer_write_failure_returns_consumed_budget_without_flushing() {
        let fail_write = Rc::new(Cell::new(false));
        let flushed = Rc::new(Cell::new(false));
        let writer = ready(FailingOutput {
            fail_write: fail_write.clone(),
            accept_prefix: true,
            fail_flush: false,
            flushed: flushed.clone(),
        });
        let before = writer.budget().work;
        fail_write.set(true);
        let (result, budget) = writer.finalize();
        assert!(result.is_err());
        assert!(!flushed.get());
        assert!(budget.work.raw_bytes > before.raw_bytes);
        assert_eq!(budget.work.output_bytes, before.output_bytes + 5);
        assert_eq!(budget.work.rows, before.rows);
    }
    #[test]
    fn flush_failure_returns_footer_and_prior_consumption() {
        let flushed = Rc::new(Cell::new(false));
        let writer = ready(FailingOutput {
            fail_write: Rc::new(Cell::new(false)),
            accept_prefix: false,
            fail_flush: true,
            flushed: flushed.clone(),
        });
        let before = writer.budget().work;
        let (result, budget) = writer.finalize();
        assert!(result.is_err());
        assert!(flushed.get());
        assert!(budget.work.output_bytes > before.output_bytes);
        assert_eq!(
            budget.work.raw_bytes - before.raw_bytes,
            budget.work.output_bytes - before.output_bytes,
        );
        assert_eq!(budget.work.rows, before.rows);
    }
}
