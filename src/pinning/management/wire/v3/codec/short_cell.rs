use super::*;

// kind 7: u16 column length, u8 value (0 NULL, 1 text, 2 redacted),
// column UTF-8, then at most one complete cell slice of text.
pub(crate) struct ShortCell {
    pub column: String,
    pub bytes: Vec<u8>,
    pub value: CellValue,
}

pub(crate) struct HeldCell {
    pub cell: ShortCell,
    pub _resident: ResidentGuard,
}

pub(crate) enum Record {
    Event(Event),
    ShortCell(HeldCell),
}

enum Phase {
    Column,
    Text,
    End,
}

pub(super) struct PendingCell {
    held: HeldCell,
    phase: Phase,
}
impl PendingCell {
    pub fn new(held: HeldCell) -> Self {
        Self {
            held,
            phase: Phase::Column,
        }
    }
    pub fn next(&mut self) -> (Event, usize, bool) {
        let cell = &mut self.held.cell;
        match self.phase {
            Phase::Column => {
                self.phase = if cell.bytes.is_empty() {
                    Phase::End
                } else {
                    Phase::Text
                };
                let resident = 2 * cell.column.len() + std::mem::size_of::<Frame>();
                (
                    Event::Frame(Frame::Cell {
                        column: std::mem::take(&mut cell.column),
                    }),
                    resident,
                    false,
                )
            }
            Phase::Text => {
                self.phase = Phase::End;
                let resident = cell.bytes.len() + 4;
                (
                    Event::Chunk(std::mem::take(&mut cell.bytes)),
                    resident,
                    false,
                )
            }
            Phase::End => (
                Event::Frame(Frame::CellEnd { value: cell.value }),
                std::mem::size_of::<Frame>(),
                true,
            ),
        }
    }
}

fn shape(column: &str, bytes: &[u8], value: CellValue) -> Result<()> {
    if column.is_empty()
        || column.len() > 256
        || bytes.len() > CELL_BYTES
        || (value != CellValue::Text && !bytes.is_empty())
    {
        return Err(SafeError::InvalidInput);
    }
    Ok(())
}

fn validate(
    state: &mut State,
    budget: &mut Budget,
    column: &str,
    bytes: &[u8],
    value: CellValue,
) -> Result<()> {
    column_copy(budget, column)?;
    state.frame(&Frame::Cell {
        column: column.into(),
    })?;
    if !bytes.is_empty() {
        state.chunk(bytes, false)?;
    }
    state.frame(&Frame::CellEnd { value })
}

impl<W: Write> Writer<W> {
    pub(crate) fn redacts_column(&self, column: &str) -> bool {
        self.state.redacts_column(column)
    }
    /// Write one bounded, complete cell. Confidential review cells always use
    /// the streaming representation, including NULL and empty source values.
    pub fn complete_cell(&mut self, column: &str, bytes: &[u8], value: CellValue) -> Result<()> {
        let result = self.complete_cell_inner(column, bytes, value);
        self.failed |= result.is_err();
        result
    }
    fn complete_cell_inner(&mut self, column: &str, bytes: &[u8], value: CellValue) -> Result<()> {
        if self.failed {
            return Err(SafeError::InvalidInput);
        }
        limit(self.budget.check())?;
        shape(column, bytes, value)?;
        if self.state.redacts_column(column) {
            self.frame(&Frame::Cell {
                column: column.into(),
            })?;
            if !bytes.is_empty() {
                self.chunk(bytes)?;
            }
            return self.frame(&Frame::CellEnd { value });
        }
        let payload_len = 3 + column.len() + bytes.len();
        let _resident = limit(
            self.budget
                .reserve(payload_len + 2 * column.len() + std::mem::size_of::<Frame>()),
        )?;
        validate(&mut self.state, &mut self.budget, column, bytes, value)?;
        let mut payload = Vec::with_capacity(payload_len);
        payload.extend_from_slice(&(column.len() as u16).to_be_bytes());
        payload.push(match value {
            CellValue::Null => 0,
            CellValue::Text => 1,
            CellValue::Redacted => 2,
        });
        payload.extend_from_slice(column.as_bytes());
        payload.extend_from_slice(bytes);
        self.emit(7, &payload, true)
    }
}

impl<R: Read> Reader<R> {
    pub(super) fn read_short_cell(&mut self, length: usize) -> Result<Record> {
        // Validate the fixed prefix and the entire shape before any variable
        // payload allocation, even for records not produced by our writer.
        self.prepay_read(3)?;
        let mut prefix = [0; 3];
        self.read_prepaid_exact(&mut prefix)?;
        let column_len = u16::from_be_bytes([prefix[0], prefix[1]]) as usize;
        let value = match prefix[2] {
            0 => CellValue::Null,
            1 => CellValue::Text,
            2 => CellValue::Redacted,
            _ => return Err(SafeError::InvalidInput),
        };
        let remaining = length - 4;
        if !(1..=256).contains(&column_len)
            || column_len > remaining
            || remaining - column_len > CELL_BYTES
            || (value != CellValue::Text && remaining != column_len)
        {
            return Err(SafeError::InvalidInput);
        }
        self.prepay_read(remaining)?;
        let resident = limit(
            self.budget
                .reserve(2 * remaining + 3 + std::mem::size_of::<Frame>()),
        )?;
        let mut payload = vec![0; remaining + 3];
        payload[..3].copy_from_slice(&prefix);
        self.read_prepaid_exact(&mut payload[3..])?;
        let column = std::str::from_utf8(&payload[3..3 + column_len])
            .map_err(|_| SafeError::InvalidInput)?;
        if self.state.redacts_column(column) {
            return Err(SafeError::InvalidInput);
        }
        validate(
            &mut self.state,
            &mut self.budget,
            column,
            &payload[3 + column_len..],
            value,
        )?;
        self.integrity.add(7, &payload)?;
        let cell = ShortCell {
            column: column.into(),
            bytes: payload.split_off(3 + column_len),
            value,
        };
        Ok(Record::ShortCell(HeldCell {
            cell,
            _resident: resident,
        }))
    }
}
