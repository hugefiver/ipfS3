use super::*;
use crate::{
    pinning::management::{MAX_ITEMS, PlanStatus, SafeError},
    store::pinning::management::native,
};
use std::collections::HashSet;

type Result<T> = std::result::Result<T, SafeError>;

#[derive(Default, Serialize, Deserialize)]
struct Utf8 {
    tail: Vec<u8>,
}
impl Utf8 {
    fn feed(&mut self, bytes: &[u8]) -> Result<()> {
        let mut bytes = bytes;
        if !self.tail.is_empty() {
            let needed = match self.tail[0] {
                0xc2..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf4 => 4,
                _ => return Err(SafeError::InvalidInput),
            } - self.tail.len();
            let n = needed.min(bytes.len());
            self.tail.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            match std::str::from_utf8(&self.tail) {
                Ok(_) => self.tail.clear(),
                Err(e) if e.error_len().is_none() => return Ok(()),
                Err(_) => return Err(SafeError::InvalidInput),
            }
        }
        if let Err(e) = std::str::from_utf8(bytes) {
            if e.error_len().is_some() {
                return Err(SafeError::InvalidInput);
            }
            self.tail.extend_from_slice(&bytes[e.valid_up_to()..]);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Row {
    section: Section,
    key: NativeKey,
    remaining: u32,
    last_column: String,
    cell: Option<(Utf8, u64, u64)>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct State {
    pub projection: Projection,
    phase: u8,
    last_row: Option<(Section, NativeKey)>,
    row: Option<Row>,
    items: HashSet<String>,
    identities: HashSet<String>,
    candidates: HashSet<String>,
    acknowledgements: Option<Vec<String>>,
}
impl State {
    pub fn new(projection: Projection) -> Self {
        Self {
            projection,
            phase: 0,
            last_row: None,
            row: None,
            items: HashSet::new(),
            identities: HashSet::new(),
            candidates: HashSet::new(),
            acknowledgements: None,
        }
    }
    pub fn redacting(&self) -> bool {
        self.projection == Projection::Review
            && self
                .row
                .as_ref()
                .is_some_and(|r| private_cell(r.section, &r.key.table, &r.last_column))
    }
    pub fn redacts_column(&self, column: &str) -> bool {
        self.projection == Projection::Review
            && self
                .row
                .as_ref()
                .is_some_and(|r| private_cell(r.section, &r.key.table, column))
    }
    pub fn source_key(&self, key: &NativeKey) -> bool {
        self.row.as_ref().is_some_and(|row| {
            row.section == Section::Native && row.key == *key && row.cell.is_none()
        })
    }
    pub fn chunk(&mut self, bytes: &[u8], suppressed: bool) -> Result<()> {
        if bytes.is_empty() || bytes.len() > CELL_BYTES {
            return Err(SafeError::LimitExceeded);
        }
        let (utf8, original, length) = self
            .row
            .as_mut()
            .and_then(|r| r.cell.as_mut())
            .ok_or(SafeError::InvalidInput)?;
        utf8.feed(bytes)?;
        *original = original
            .checked_add(bytes.len() as u64)
            .ok_or(SafeError::LimitExceeded)?;
        if !suppressed {
            *length = length
                .checked_add(bytes.len() as u64)
                .ok_or(SafeError::LimitExceeded)?;
        }
        Ok(())
    }
    pub fn frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Header {
                schema_version,
                projection,
                database_scope,
                database_identity,
                principal,
                applied_trust: AppliedTrust::UntrustedClaimsOnly,
                catalog,
                created_at,
                expires_at,
            } if self.phase == 0 => {
                if *schema_version != SCHEMA_VERSION {
                    return Err(SafeError::UnsupportedSchemaVersion);
                }
                if *projection != self.projection
                    || principal.is_empty()
                    || principal.len() > 4096
                    || database_scope.is_empty()
                    || database_identity.is_empty()
                    || database_scope.len() > 4096
                    || database_identity.len() > 4096
                    || catalog.is_empty()
                    || catalog.len() > 256
                    || catalog
                        .iter()
                        .enumerate()
                        .any(|(i, c)| c.is_empty() || c.len() > 1024 || catalog[..i].contains(c))
                    || created_at.is_empty()
                    || created_at.len() > 1024
                    || expires_at.as_ref().is_some_and(|t| t.len() > 1024)
                {
                    return Err(SafeError::InvalidInput);
                }
                self.phase = 1;
            }
            Frame::Request { request } if self.phase == 1 => {
                if let Some(request) = request {
                    crate::pinning::management::validate_historical_request(request)?;
                }
                self.phase = 2;
            }
            Frame::Configuration { .. } if self.phase == 2 => {
                self.phase = 3;
            }
            Frame::Acknowledgements { item_ids } if self.phase == 3 => {
                if let Some(ids) = item_ids {
                    if ids.len() > MAX_ITEMS
                        || ids.iter().enumerate().any(|(i, id)| {
                            id.is_empty() || id.len() > 1024 || ids[..i].contains(id)
                        })
                    {
                        return Err(SafeError::ConfirmationMismatch);
                    }
                    self.acknowledgements = Some(ids.clone());
                }
                self.phase = 4;
            }
            Frame::Item { entry } if self.phase == 4 => {
                if self.items.len() >= MAX_ITEMS
                    || entry.request.item_id.is_empty()
                    || entry.request.item_id.len() > 1024
                    || entry.id.as_str().is_empty()
                    || entry.id.as_str().len() > 1024
                    || !self.identities.insert(entry.id.as_str().to_owned())
                    || !self.items.insert(entry.request.item_id.clone())
                {
                    return Err(SafeError::DuplicateIdentity);
                }
                if entry.status == PlanStatus::Candidate {
                    self.candidates.insert(entry.request.item_id.clone());
                }
            }
            Frame::Summary { summary } if self.phase == 4 => {
                if summary.items != self.items.len() || summary.candidates != self.candidates.len()
                {
                    return Err(SafeError::InvalidInput);
                }
                if self.acknowledgements.as_ref().is_some_and(|ids| {
                    ids.len() != self.candidates.len()
                        || ids.iter().any(|id| !self.candidates.contains(id))
                }) {
                    return Err(SafeError::ConfirmationMismatch);
                }
                self.phase = 5;
            }
            Frame::Row {
                section,
                key,
                cells,
            } if self.phase == 5 && self.row.is_none() => {
                if self.projection == Projection::Review
                    && *section == Section::Native
                    && !native::current_catalog().iter().any(|layout| {
                        layout.table == key.table
                            && layout.primary_key.len() == key.values.len()
                            && layout
                                .primary_key
                                .iter()
                                .all(|column| !native::confidential(&key.table, column))
                    })
                {
                    return Err(SafeError::InvalidInput);
                }
                if !identifier(&key.table)
                    || key.values.is_empty()
                    || key.values.len() > 64
                    || key.values.iter().any(|value| value.len() > 1024)
                    || *cells > 256
                    || self
                        .last_row
                        .as_ref()
                        .is_some_and(|last| (*section, key) <= (last.0, &last.1))
                {
                    return Err(SafeError::DuplicateIdentity);
                }
                self.last_row = Some((*section, key.clone()));
                self.row = Some(Row {
                    section: *section,
                    key: key.clone(),
                    remaining: *cells,
                    last_column: String::new(),
                    cell: None,
                });
            }
            Frame::Cell { column } if self.phase == 5 => {
                let row = self.row.as_mut().ok_or(SafeError::InvalidInput)?;
                if row.cell.is_some()
                    || row.remaining == 0
                    || !identifier(column)
                    || *column <= row.last_column
                {
                    return Err(SafeError::InvalidInput);
                }
                row.last_column.clone_from(column);
                row.cell = Some((Utf8::default(), 0, 0));
            }
            Frame::CellEnd { value } if self.phase == 5 => {
                let redacting = self.redacting();
                let row = self.row.as_mut().ok_or(SafeError::InvalidInput)?;
                let (utf8, original, length) = row.cell.take().ok_or(SafeError::InvalidInput)?;
                if !utf8.tail.is_empty()
                    || (*value != CellValue::Text && length != 0)
                    || (*value == CellValue::Null && original != 0)
                    || (redacting && *value == CellValue::Text)
                    || (self.projection == Projection::PrivateRecovery
                        && *value == CellValue::Redacted)
                {
                    return Err(SafeError::InvalidInput);
                }
                row.remaining -= 1;
            }
            Frame::RowEnd if self.phase == 5 => {
                let row = self.row.take().ok_or(SafeError::InvalidInput)?;
                if row.remaining != 0 || row.cell.is_some() {
                    return Err(SafeError::InvalidInput);
                }
            }
            Frame::Trailer { .. } if self.phase == 5 && self.row.is_none() => {
                self.phase = 6;
            }
            _ => return Err(SafeError::InvalidInput),
        }
        Ok(())
    }
}
fn identifier(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 256
        && text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

pub(super) fn private_cell(section: Section, table: &str, column: &str) -> bool {
    section != Section::Native
        || {
            let catalog = native::current_catalog();
            match catalog.binary_search_by(|layout| layout.table.as_str().cmp(table)) {
                Ok(index) => catalog[index]
                    .columns
                    .binary_search_by(|name| name.as_str().cmp(column))
                    .is_err(),
                Err(_) => true,
            }
        }
        || matches!(
            table,
            "pin_management_archive"
                | "pin_management_archive_chunks"
                | "pin_management_inline_archive"
        )
        || native::confidential(table, column)
}
