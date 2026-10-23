//! A temporary row index is useful only with its exclusively owned verified raw
//! source. Neither handle nor a writable alias escapes this module.
use super::*;
use physical::{CheckedRecord, Value};
use std::{fs::File, io::{BufWriter, Seek, SeekFrom}};

pub struct OwnedSource { file: File }
impl Read for OwnedSource {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> { self.file.read(bytes) }
}
impl Seek for OwnedSource {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> { self.file.seek(position) }
}

pub(crate) struct Builder {
    raw: File,
    index: BufWriter<File>,
    position: u64,
    written: u64,
    rows: u64,
    terminal: Option<u64>,
    _memory: ResidentGuard,
}
impl Builder {
    pub(crate) fn new(budget: &mut Budget) -> Result<Self> {
        Ok(Self {
            raw: files::stream::temporary().map_err(|_| SafeError::InvalidInput)?,
            index: BufWriter::with_capacity(CELL_BYTES, files::stream::temporary().map_err(|_| SafeError::InvalidInput)?),
            position: MAGIC.len() as u64, written: 0, rows: 0, terminal: None,
            _memory: limit(budget.reserve(CELL_BYTES + FRAME_BYTES))?,
        })
    }
    pub(crate) fn write_raw(&mut self, bytes: &[u8], budget: &mut Budget) -> Result<()> {
        limit(budget.charge(0, 0, 0, bytes.len() as u64))?;
        write_counted(&mut self.raw, &mut budget.work, bytes)?;
        self.written = self.written.checked_add(bytes.len() as u64).ok_or(SafeError::LimitExceeded)?;
        Ok(())
    }
    pub(crate) fn record(&mut self, record: &CheckedRecord<'_>, budget: &mut Budget) -> Result<()> {
        if self.terminal.is_some() { return Err(SafeError::InvalidInput); }
        if let Value::Frame(Frame::Row { section, key, cells }) = &record.value {
            let length = 25 + key.table.len() + key.values.iter().map(|value| 4 + value.len()).sum::<usize>();
            if length > FRAME_BYTES { return Err(SafeError::LimitExceeded); }
            let mut bytes = Vec::with_capacity(length);
            bytes.extend_from_slice(&self.position.to_be_bytes());
            bytes.extend_from_slice(&(record.physical_len() as u32).to_be_bytes());
            bytes.push(match section { Section::Native => 0, Section::Membership => 1, Section::Inventory => 2, Section::Result => 3, Section::AppliedClaim => 4 });
            bytes.extend_from_slice(&cells.to_be_bytes());
            string(&mut bytes, &key.table)?;
            bytes.extend_from_slice(&(key.values.len() as u32).to_be_bytes());
            for value in &key.values { string(&mut bytes, value)?; }
            if bytes.len() > FRAME_BYTES { return Err(SafeError::LimitExceeded); }
            // Encoding allocation and buffered index write are real copies.
            limit(budget.prepay_reader(0, (2 * bytes.len() + 4) as u64))?;
            limit(budget.charge(0, 0, 0, (bytes.len()+4) as u64))?;
            write_counted(&mut self.index, &mut budget.work, &(bytes.len() as u32).to_be_bytes())?;
            write_counted(&mut self.index, &mut budget.work, &bytes)?;
            self.rows = self.rows.checked_add(1).ok_or(SafeError::LimitExceeded)?;
        }
        if record.terminal() { self.terminal = Some(self.position); }
        self.position = self.position.checked_add(record.physical_len() as u64).ok_or(SafeError::LimitExceeded)?;
        Ok(())
    }
    pub(crate) fn finish(mut self, mut budget: Budget) -> Result<Reader<OwnedSource>> {
        limit(budget.check())?;
        let terminal = self.terminal.ok_or(SafeError::InvalidInput)?;
        if self.written != self.position { return Err(SafeError::InvalidInput); }
        self.index.flush().map_err(invalid)?;
        let mut index = self.index.into_inner().map_err(|_| SafeError::InvalidInput)?;
        index.seek(SeekFrom::Start(0)).map_err(invalid)?;
        self.raw.seek(SeekFrom::Start(0)).map_err(invalid)?;
        let index_memory = limit(budget.reserve(2 * FRAME_BYTES))?;
        let index_buffer = ReadBuffer::new(&mut budget)?;
        let mut reader = Reader::new(OwnedSource { file: self.raw }, Projection::PrivateRecovery, budget)?;
        reader.enable_buffering()?;
        reader.replay = Some(Index {
            file: index, buffer: index_buffer, left: self.rows, next: None,
            position: MAGIC.len() as u64, terminal, end: self.position, _memory: index_memory,
        });
        Ok(reader)
    }
}
fn string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    if value.len() > FRAME_BYTES || output.len().saturating_add(4 + value.len()) > FRAME_BYTES {
        return Err(SafeError::LimitExceeded);
    }
    output.extend_from_slice(&(value.len() as u32).to_be_bytes()); output.extend_from_slice(value.as_bytes()); Ok(())
}
struct Row { position: u64, length: usize, frame: Frame }
pub(super) struct Index {
    file: File,
    buffer: ReadBuffer,
    left: u64,
    next: Option<Row>,
    position: u64,
    terminal: u64,
    end: u64,
    _memory: ResidentGuard,
}
impl Index {
    fn read(&mut self, bytes: &mut [u8], budget: &mut Budget) -> Result<()> {
        limit(budget.prepay_reader(bytes.len().saturating_sub(self.buffer.available()) as u64, bytes.len() as u64))?;
        self.buffer.read_prepaid_exact(&mut self.file, budget, bytes)
    }
    fn next_row(&mut self, budget: &mut Budget) -> Result<()> {
        if self.next.is_some() || self.left == 0 { return Ok(()); }
        let mut length = [0; 4]; self.read(&mut length, budget)?;
        let length = u32::from_be_bytes(length) as usize;
        if length > FRAME_BYTES { return Err(SafeError::InvalidInput); }
        let mut bytes = vec![0; length]; self.read(&mut bytes, budget)?;
        let mut input = bytes.as_slice();
        let position = u64::from_be_bytes(take(&mut input, 8)?.try_into().unwrap());
        let length = number(&mut input)? as usize;
        let section = match take(&mut input, 1)?[0] {
            0 => Section::Native, 1 => Section::Membership, 2 => Section::Inventory, 3 => Section::Result, 4 => Section::AppliedClaim,
            _ => return Err(SafeError::InvalidInput),
        };
        let cells = number(&mut input)?;
        let table = text(&mut input)?;
        let count = number(&mut input)? as usize;
        if count > input.len()/4 { return Err(SafeError::InvalidInput); }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count { values.push(text(&mut input)?); }
        if !input.is_empty() || position < self.position || position >= self.terminal || length > FRAME_BYTES+5 {
            return Err(SafeError::InvalidInput);
        }
        limit(budget.prepay_reader(0, bytes.len() as u64))?;
        self.next = Some(Row { position, length, frame: Frame::Row { section, key: NativeKey { table, values }, cells } });
        self.left -= 1;
        Ok(())
    }
    pub(super) fn record(&mut self, kind: u8, payload: &[u8], budget: &mut Budget) -> Result<Option<Record>> {
        self.next_row(budget)?;
        let at = self.position;
        self.position = self.position.checked_add((payload.len()+5) as u64).ok_or(SafeError::LimitExceeded)?;
        if self.position > self.end || self.next.as_ref().is_some_and(|row| row.position < at) {
            return Err(SafeError::InvalidInput);
        }
        if self.next.as_ref().is_some_and(|row| row.position == at) {
            let row = self.next.take().unwrap();
            if kind != 0 || row.length != payload.len()+5 { return Err(SafeError::InvalidInput); }
            limit(budget.charge(0, 1, 0, 0))?;
            return Ok(Some(Record::Event(Event::Frame(row.frame))));
        }
        if at == self.terminal {
            if kind != 0 || self.position != self.end || self.left != 0 || self.next.is_some() || self.buffer.available() != 0 {
                return Err(SafeError::InvalidInput);
            }
            limit(budget.prepay_reader(1, 0))?;
            probe_eof(&mut self.file, budget)?;
            return Ok(None);
        }
        let frame = match kind {
            0 => {
                let frame: Frame = serde_json::from_slice(payload).map_err(|_| SafeError::InvalidInput)?;
                if matches!(frame, Frame::Row { .. } | Frame::Trailer { .. }) { return Err(SafeError::InvalidInput); }
                frame
            }
            1 => {
                limit(budget.prepay_reader(0, payload.len() as u64))?;
                return Ok(Some(Record::Event(Event::Chunk(payload.to_vec()))));
            }
            2 => Frame::Cell { column: std::str::from_utf8(payload).map_err(|_| SafeError::InvalidInput)?.into() },
            3 => Frame::CellEnd { value: CellValue::Null },
            4 => Frame::CellEnd { value: CellValue::Text },
            5 => Frame::CellEnd { value: CellValue::Redacted },
            6 => Frame::RowEnd,
            7 => {
                let n = u16::from_be_bytes(payload[..2].try_into().unwrap()) as usize;
                let value = match payload[2] { 0 => CellValue::Null, 1 => CellValue::Text, 2 => CellValue::Redacted, _ => return Err(SafeError::InvalidInput) };
                let column = payload.get(3..3+n).ok_or(SafeError::InvalidInput)?;
                let bytes = &payload[3+n..];
                let resident = limit(budget.reserve(2*payload.len()+std::mem::size_of::<Frame>()))?;
                limit(budget.prepay_reader(0, payload.len() as u64))?;
                return Ok(Some(Record::ShortCell(short_cell::HeldCell {
                    cell: short_cell::ShortCell { column: std::str::from_utf8(column).map_err(|_| SafeError::InvalidInput)?.into(), bytes: bytes.to_vec(), value },
                    _resident: resident,
                })));
            }
            _ => return Err(SafeError::InvalidInput),
        };
        Ok(Some(Record::Event(Event::Frame(frame))))
    }
}
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if n > bytes.len() { return Err(SafeError::InvalidInput); }
    let (value, rest) = bytes.split_at(n); *bytes = rest; Ok(value)
}
fn number(bytes: &mut &[u8]) -> Result<u32> { Ok(u32::from_be_bytes(take(bytes, 4)?.try_into().unwrap())) }
fn text(bytes: &mut &[u8]) -> Result<String> {
    let n = number(bytes)? as usize;
    Ok(std::str::from_utf8(take(bytes, n)?).map_err(|_| SafeError::InvalidInput)?.into())
}
