use super::*;
use crate::store::pinning::management::{ManagementError, native::cells};

impl<W: Write> Writer<W> {
    /// Copy a native SQL cell through A's 64 KiB slicing API with the SAME
    /// operation budget. The caller owns transaction consistency and row order.
    /// SQL NULL and empty text stay distinct; review mode suppresses secrets.
    pub async fn native_cell(
        &mut self,
        tx: &sea_orm::DatabaseTransaction,
        key: &NativeKey,
        column: &str,
    ) -> Result<()> {
        let result = self.native_cell_inner(tx, key, column).await;
        self.failed |= result.is_err();
        result
    }
    async fn native_cell_inner(
        &mut self,
        tx: &sea_orm::DatabaseTransaction,
        key: &NativeKey,
        column: &str,
    ) -> Result<()> {
        if self.failed || !self.state.source_key(key) {
            return Err(SafeError::InvalidInput);
        }
        self.frame(&Frame::Cell {
            column: column.into(),
        })?;
        let value = {
            let mut output = NativeCellOutput {
                output: &mut self.output,
                state: &mut self.state,
                integrity: &mut self.integrity,
            };
            cells::copy_cell_with(tx, key, column, &mut self.budget, |chunk, budget| {
                output.emit(chunk, budget)
            })
            .await
            .map_err(|error| match error {
                ManagementError::Budget => SafeError::LimitExceeded,
                ManagementError::Changed => SafeError::SnapshotChanged,
                _ => SafeError::UnknownEvidence,
            })?
        };
        self.frame(&Frame::CellEnd {
            value: if value.is_some() {
                CellValue::Text
            } else {
                CellValue::Null
            },
        })
    }
}

// A's cell copier charges SQL/source bytes, resident space and cancellation.
// The emitter prepays framing and counts physical writes even on failure.
struct NativeCellOutput<'a, W> {
    output: &'a mut W,
    state: &'a mut State,
    integrity: &'a mut Integrity,
}
impl<W: Write> NativeCellOutput<'_, W> {
    fn emit(
        &mut self,
        bytes: &[u8],
        budget: &mut Budget,
    ) -> crate::store::pinning::management::Result<()> {
        for chunk in bytes.chunks(CELL_BYTES) {
            let suppressed = self.state.redacting();
            self.state
                .chunk(chunk, suppressed)
                .map_err(|_| ManagementError::InvalidEvidence)?;
            if !suppressed {
                budget.charge(0, 0, 1, 5)?;
                cells::write_counted(self.output, budget, &record_header(1, chunk.len()))?;
                cells::write_counted(self.output, budget, chunk)?;
                self.integrity
                    .add(1, chunk)
                    .map_err(|_| ManagementError::Budget)?;
            }
        }
        Ok(())
    }
}
