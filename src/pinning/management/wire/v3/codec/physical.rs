//! A checked record borrows only the current physical payload, not file trust.
use super::*;

pub(crate) enum Value<'a> {
    Frame(Frame),
    Chunk(&'a [u8]),
    Cell { column: &'a str, bytes: &'a [u8], value: CellValue },
}

pub(crate) struct CheckedRecord<'a> {
    kind: u8,
    payload: &'a [u8],
    projection: Projection,
    pub(crate) value: Value<'a>,
    pub(super) resident: usize,
    _memory: ResidentGuard,
}
impl CheckedRecord<'_> {
    pub(crate) fn terminal(&self) -> bool {
        matches!(self.value, Value::Frame(Frame::Trailer { .. }))
    }
    pub(crate) fn physical_len(&self) -> usize { self.payload.len() + 5 }
    pub(super) fn into_record(self, budget: &mut Budget) -> Result<Record> {
        Ok(match self.value {
            Value::Frame(frame) => Record::Event(Event::Frame(frame)),
            Value::Chunk(bytes) => {
                limit(budget.prepay_reader(0, bytes.len() as u64))?;
                Record::Event(Event::Chunk(bytes.to_vec()))
            }
            Value::Cell { column, bytes, value } => {
                limit(budget.prepay_reader(0, (column.len() + bytes.len()) as u64))?;
                Record::ShortCell(short_cell::HeldCell {
                    cell: short_cell::ShortCell { column: column.into(), bytes: bytes.to_vec(), value },
                    _resident: self._memory,
                })
            }
        })
    }
}

pub(super) fn shape(kind: u8, length: usize) -> Result<()> {
    if length == 0 || length > FRAME_BYTES + 1 { return Err(SafeError::LimitExceeded); }
    if !match kind {
        0 => true,
        1 => (2..=CELL_BYTES + 1).contains(&length),
        2 => (2..=257).contains(&length),
        3..=6 => length == 1,
        7 => (5..=4 + 256 + CELL_BYTES).contains(&length),
        _ => false,
    } { return Err(SafeError::InvalidInput); }
    Ok(())
}

pub(super) fn decode<'a>(
    state: &mut State, integrity: &mut Integrity, kind: u8,
    payload: &'a [u8], budget: &mut Budget,
) -> Result<CheckedRecord<'a>> {
    limit(budget.check())?;
    shape(kind, payload.len() + 1)?;
    let resident = if kind == 0 { 5 * FRAME_BYTES } else { 2 * payload.len() + std::mem::size_of::<Frame>() };
    let memory = limit(budget.reserve(resident))?;
    let value = match kind {
        0 => {
            strict::check(payload)?;
            let frame: Frame = serde_json::from_slice(payload).map_err(|_| SafeError::InvalidInput)?;
            if matches!(frame, Frame::Cell { .. } | Frame::CellEnd { .. } | Frame::RowEnd) {
                return Err(SafeError::InvalidInput);
            }
            state.frame(&frame)?;
            if matches!(frame, Frame::Row { .. }) { limit(budget.charge(0, 1, 0, 0))?; }
            if let Frame::Trailer { frames, bytes, sha256 } = &frame {
                integrity.verify(*frames, *bytes, sha256)?;
            }
            Value::Frame(frame)
        }
        1 => {
            if state.redacting() { return Err(SafeError::InvalidInput); }
            state.chunk(payload, false)?;
            Value::Chunk(payload)
        }
        7 => {
            let n = u16::from_be_bytes([payload[0], payload[1]]) as usize;
            let value = match payload[2] {
                0 => CellValue::Null, 1 => CellValue::Text, 2 => CellValue::Redacted,
                _ => return Err(SafeError::InvalidInput),
            };
            if !(1..=256).contains(&n) || 3 + n > payload.len() { return Err(SafeError::InvalidInput); }
            let column = std::str::from_utf8(&payload[3..3+n]).map_err(|_| SafeError::InvalidInput)?;
            let bytes = &payload[3+n..];
            short_cell::shape(column, bytes, value)?;
            if state.redacts_column(column) { return Err(SafeError::InvalidInput); }
            short_cell::validate(state, budget, column, bytes, value)?;
            Value::Cell { column, bytes, value }
        }
        _ => {
            let frame = match kind {
                2 => Frame::Cell { column: std::str::from_utf8(payload).map_err(|_| SafeError::InvalidInput)?.into() },
                3 => Frame::CellEnd { value: CellValue::Null },
                4 => Frame::CellEnd { value: CellValue::Text },
                5 => Frame::CellEnd { value: CellValue::Redacted },
                6 => Frame::RowEnd,
                _ => unreachable!(),
            };
            if let Frame::Cell { column } = &frame { column_copy(budget, column)?; }
            state.frame(&frame)?;
            Value::Frame(frame)
        }
    };
    if !matches!(value, Value::Frame(Frame::Trailer { .. })) { integrity.add(kind, payload)?; }
    Ok(CheckedRecord { kind, payload, projection: state.projection, value, resident, _memory: memory })
}

/// Synchronous consumption of asynchronously fetched pages. Only crossing
/// records are assembled; complete records borrow the storage page directly.
pub(crate) struct Decoder {
    state: State,
    integrity: Integrity,
    scratch: Vec<u8>,
    magic: bool,
    terminal: bool,
    failed: bool,
    _memory: ResidentGuard,
}
pub(crate) trait Sink {
    fn budget(&mut self) -> &mut Budget;
    fn record(&mut self, record: CheckedRecord<'_>) -> Result<()>;
}
impl Decoder {
    pub(crate) fn new(projection: Projection, budget: &mut Budget) -> Result<Self> {
        Ok(Self {
            state: State::new(projection), integrity: Integrity::new(),
            scratch: Vec::new(), magic: false, terminal: false, failed: false,
            _memory: limit(budget.reserve(3 * FRAME_BYTES + 10))?,
        })
    }
    pub(crate) fn feed(
        &mut self, bytes: &[u8], sink: &mut impl Sink,
    ) -> Result<()> {
        let result = self.feed_inner(bytes, sink);
        self.failed |= result.is_err();
        result
    }
    fn feed_inner(
        &mut self, mut bytes: &[u8], sink: &mut impl Sink,
    ) -> Result<()> {
        if self.failed { return Err(SafeError::InvalidInput); }
        while !bytes.is_empty() {
            let budget = sink.budget();
            limit(budget.check())?;
            if self.terminal { return Err(SafeError::InvalidInput); }
            let prefix = if self.magic { 5 } else { MAGIC.len() };
            if !self.scratch.is_empty() || bytes.len() < prefix {
                let need = prefix.saturating_sub(self.scratch.len()).min(bytes.len());
                limit(budget.prepay_reader(0, need as u64))?;
                self.scratch.extend_from_slice(&bytes[..need]);
                bytes = &bytes[need..];
                if self.scratch.len() < prefix { break; }
                if !self.magic {
                    if self.scratch != MAGIC { return Err(SafeError::UnsupportedSchemaVersion); }
                    self.scratch.clear(); self.magic = true; continue;
                }
                let length = u32::from_be_bytes(self.scratch[..4].try_into().unwrap()) as usize;
                shape(self.scratch[4], length)?;
                let need = (length + 4 - self.scratch.len()).min(bytes.len());
                limit(budget.prepay_reader(0, need as u64))?;
                self.scratch.extend_from_slice(&bytes[..need]); bytes = &bytes[need..];
                if self.scratch.len() < length + 4 { break; }
                limit(budget.charge(0, 0, 1, 0))?;
                let record = decode(&mut self.state, &mut self.integrity, self.scratch[4], &self.scratch[5..], budget)?;
                self.terminal = record.terminal();
                sink.record(record)?;
                self.scratch.clear();
            } else if !self.magic {
                if &bytes[..prefix] != MAGIC { return Err(SafeError::UnsupportedSchemaVersion); }
                self.magic = true; bytes = &bytes[prefix..];
            } else {
                let length = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
                shape(bytes[4], length)?;
                if bytes.len() < length + 4 {
                    limit(budget.prepay_reader(0, bytes.len() as u64))?;
                    self.scratch.extend_from_slice(bytes); break;
                }
                limit(budget.charge(0, 0, 1, 0))?;
                let record = decode(&mut self.state, &mut self.integrity, bytes[4], &bytes[5..length+4], budget)?;
                self.terminal = record.terminal();
                sink.record(record)?;
                bytes = &bytes[length+4..];
            }
        }
        Ok(())
    }
    pub(crate) fn finish(&self, budget: &Budget) -> Result<()> {
        limit(budget.check())?;
        if self.failed || !self.terminal || !self.scratch.is_empty() { return Err(SafeError::InvalidInput); }
        Ok(())
    }
}

struct Compare<'a> { expected: &'a [u8], position: usize, mismatch: bool, budget: &'a Budget }
impl Write for Compare<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.budget.check().map_err(io::Error::other)?;
        if self.expected.get(self.position..self.position.saturating_add(bytes.len())) != Some(bytes) {
            self.mismatch = true;
            return Err(io::Error::other("encoding differs"));
        }
        self.position += bytes.len(); Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}
impl<W: Write> Writer<W> {
    pub(crate) fn checked_record(&mut self, record: &CheckedRecord<'_>) -> Result<()> {
        let result = self.checked_inner(record);
        self.failed |= result.is_err(); result
    }
    fn checked_inner(&mut self, record: &CheckedRecord<'_>) -> Result<()> {
        if self.failed || record.terminal() || record.projection != self.state.projection {
            return Err(SafeError::InvalidInput);
        }
        limit(self.budget.check())?;
        match &record.value {
            Value::Frame(frame) if record.kind == 0 => {
                let mut compare = Compare { expected: record.payload, position: 0, mismatch: false, budget: &self.budget };
                let serialized = serde_json::to_writer(&mut compare, frame);
                // Cancellation/deadline errors are never a canonical mismatch.
                limit(self.budget.check())?;
                if serialized.is_err() && !compare.mismatch { return Err(SafeError::InvalidInput); }
                if compare.mismatch || compare.position != record.payload.len() {
                    return self.frame(frame);
                }
                self.state.frame(frame)?;
                if matches!(frame, Frame::Row { .. }) { limit(self.budget.charge(0, 1, 0, 0))?; }
            }
            Value::Frame(frame) => {
                // Compact records are unique physical encodings of these values.
                if let Frame::Cell { column } = frame { column_copy(&mut self.budget, column)?; }
                self.state.frame(frame)?;
            }
            Value::Chunk(bytes) => { self.state.chunk(bytes, false)?; }
            Value::Cell { column, bytes, value } => {
                if self.state.redacts_column(column) { return self.complete_cell(column, bytes, *value); }
                short_cell::validate(&mut self.state, &mut self.budget, column, bytes, *value)?;
            }
        }
        self.emit(record.kind, record.payload, true)
    }
}
