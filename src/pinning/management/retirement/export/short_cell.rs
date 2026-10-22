use super::*;

impl RetirementArchiveCursor {
    // Only consume an already validated, complete native initial part. A miss
    // neither queries the spool nor changes the streaming cursor.
    pub(super) fn complete_initial<W: Write>(
        &mut self,
        writer: &mut Writer<W>,
        physical_remaining: u64,
        logical_remaining: usize,
    ) -> Result<Option<usize>> {
        let Some(row) = self.row.as_mut() else {
            return Ok(None);
        };
        if row.opened
            || row.offset != 0
            || row.length.is_some()
            || !matches!(row.source, Source::Native(_))
        {
            return Ok(None);
        }
        let Some(column) = row.columns.get(row.column) else {
            return Ok(None);
        };
        if writer.redacts_column(column) {
            return Ok(None);
        }
        let Some(initial) = row.initial.as_mut() else {
            return Ok(None);
        };
        let Some(part) = initial.peek(column)? else {
            return Ok(None);
        };
        if part.bytes.len() > CELL_SLICE
            || part
                .length
                .is_some_and(|length| length != part.bytes.len() as u64)
            || (part.length.is_none() && !part.bytes.is_empty())
        {
            return Ok(None);
        }
        let events = if part.bytes.is_empty() { 2 } else { 3 };
        let physical = 8 + column.len() + part.bytes.len();
        if physical as u64 > physical_remaining || events > logical_remaining {
            return Ok(None);
        }
        let part = initial
            .take(column)?
            .ok_or(ManagementError::InvalidEvidence)?;
        let value = if part.length.is_none() {
            CellValue::Null
        } else {
            CellValue::Text
        };
        writer
            .complete_cell(column, &part.bytes, value)
            .map_err(map_wire)?;
        row.column += 1;
        Ok(Some(events))
    }
}
