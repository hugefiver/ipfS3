//! Read-only consistency checks for historical confirmation declarations.
//! Neither the progress CAS nor these payloads carry an independent content hash.
//! Passing these checks cannot construct ReadyConfirmation or execution authority.
use super::{State, Stage, SourceCheckpoint, PlanRequest, ConfigurationBinding, Summary, RetirementInventoryRef, NativeKey};
use crate::{pinning::management::SafeError, store::pinning::management::native::{self, NativeTable, meter::{Budget, ResidentGuard}}};
use serde::{Deserialize, Serialize};

pub(crate) mod json;
mod payload;
use payload::{Page, PayloadRow};

type Result<T> = std::result::Result<T, SafeError>;
fn invalid<T>(_: T) -> SafeError { SafeError::InvalidInput }
fn resource<T>(_: T) -> SafeError { SafeError::LimitExceeded }

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    format: String,
    principal: String,
    request: PlanRequest,
    local_digest: String,
    local_observed_at: i64,
    local_database_now: Option<chrono::DateTime<chrono::Utc>>,
    configuration: ConfigurationBinding,
    origin: SourceCheckpoint,
    reviewed_digest: String,
    summary: Summary,
    inventories: Vec<RetirementInventoryRef>,
}

/// Coordinates are a read-only projection of the existing checkpoint, not a
/// substitute checkpoint model or a call into its live-database fence.
#[derive(Clone, PartialEq, Eq, Deserialize)]
struct Coordinates {
    status_scan: bool,
    table: usize,
    after: Option<NativeKey>,
    row: Option<NativeKey>,
    column: usize,
    offset: u64,
}
fn coordinates(source: &SourceCheckpoint, budget: &mut Budget) -> Result<Coordinates> {
    budget.check().map_err(resource)?;
    let _guard = budget.reserve(3 * 1024 * 1024).map_err(resource)?;
    let raw = serde_json::to_vec(source).map_err(invalid)?;
    budget.charge(0,0,0,(raw.len() * 2) as u64).map_err(resource)?;
    serde_json::from_slice(&raw).map_err(invalid)
}

pub(crate) struct Evidence {
    header: Header,
    state: State,
    cursor: super::super::source::attest::Cursor,
    catalog: &'static [NativeTable],
    wire: bool,
    compared: bool,
    source_tail: bool,
    wire_tail: bool,
    wire_count: i64,
    wire_after: Option<(String, Vec<u8>)>,
    cell_length: Option<Option<u64>>,
    utf8_tail: Vec<u8>,
    _resident: Vec<ResidentGuard>,
}

/// A page's keys are indexed by the caller in a bounded disk spool. No complete
/// membership or wire-key set is accumulated in memory.
pub(crate) enum Keys {
    Source(Vec<NativeKey>),
    Wire(Vec<NativeKey>),
    None,
}

impl Evidence {
    pub(crate) fn open(header: &[u8], state: &[u8], id: &str, scope: &str, identity: &str, sealed: bool, budget: &mut Budget) -> Result<Self> {
        let (header, mut guards) = json::envelope::<Header>(header, budget)?;
        let (state, state_guards) = json::envelope::<State>(state, budget)?;
        guards.extend(state_guards);
        let catalog = native::archived_catalog(&header.origin.catalog).map_err(invalid)?;
        let start = coordinates(&header.origin, budget)?;
        let source = coordinates(&state.source.source, budget)?;
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if header.format != "S75-confirm-prefix-1"
            || header.inventories.is_empty()
            || header.inventories.iter().any(|inventory| inventory.archive_id.is_empty() || uuid::Uuid::parse_str(&inventory.inventory_id).is_err() || inventory.reducer_version != super::super::REDUCER_VERSION)
            || id != format!("checkpoint:{}", header.inventories[0].archive_id)
            || scope != header.origin.database_scope || identity != header.origin.database_identity
            || scope.is_empty() || identity.is_empty() || header.principal.is_empty()
            || !digest(&header.local_digest) || !digest(&header.reviewed_digest)
            || start.status_scan || start.table != 0 || start.row.is_some() || start.after.is_some() || start.column != 0 || start.offset != 0
            || header.origin.catalog.last() != Some(&header.origin.catalog_revision)
            || header.origin.revisions.len() != catalog.len()
            || catalog.iter().any(|t| header.origin.revisions.get(&t.table).is_none_or(|r| *r < 0))
            || header.local_database_now.is_none_or(|now| now > header.origin.observed_at)
            || state.source.source.rewind() != header.origin
            || sealed != (state.stage == Stage::Ready)
            || state.provider > header.inventories.len() || state.source.ordinal < 0 || state.source.position < 0
            || state.source.partial > 65536 || state.source.eof > catalog.len()
            || state.wire_count < 0 || state.wire_count > state.source.position
            || source.status_scan || source.table > catalog.len() || source.offset > i64::MAX as u64
            || source.row.is_none() && (source.column != 0 || source.offset != 0)
            || state.source.eof != source.table
            || state.comparison.table > if state.stage == Stage::UnionCompare { 7 } else { 8 }
            || state.comparison.after.len() > 2
        { return Err(SafeError::InvalidInput); }
        for key in source.row.iter().chain(source.after.iter()) {
            let table = catalog.get(source.table).ok_or(SafeError::InvalidInput)?;
            valid_key(key, table)?;
            if source.row.is_some() && source.column >= table.columns.len() { return Err(SafeError::InvalidInput); }
        }
        let complete = source.table == catalog.len() && source.row.is_none();
        if state.stage != Stage::Source && !complete
            || state.stage == Stage::Source && (state.wire_count != 0 || state.wire_after.is_some())
            || !matches!(state.stage, Stage::Source | Stage::Wire) && state.wire_count != state.source.position
            || (state.wire_count == 0) != state.wire_after.is_none()
            || complete && state.source.partial != 0
            || matches!(state.stage, Stage::UnionCompare | Stage::Archive | Stage::Ready) != state.canonical.is_some()
            || matches!(state.stage, Stage::Source | Stage::Wire | Stage::Union | Stage::UnionCompare | Stage::Archive | Stage::Ready) && state.provider != 0
            || !matches!(state.stage, Stage::Compare | Stage::UnionCompare) && (state.comparison.table != 0 || !state.comparison.after.is_empty())
        { return Err(SafeError::InvalidInput); }
        match (&state.metered_before, &state.metered_after) {
            (None, None) => if state.stage != Stage::Source || state.source != super::super::source::attest::Cursor::new(header.origin.clone()) { return Err(SafeError::InvalidInput); },
            (Some(a), Some(b)) => if a.rows < 0 || a.bytes < 0 || a.pages < 0 || b.rows < a.rows || b.bytes < a.bytes || b.pages < a.pages || b.pages == 0 { return Err(SafeError::InvalidInput); },
            _ => return Err(SafeError::InvalidInput),
        }
        if let Some(plan) = &state.canonical {
            budget.charge(0,0,0,state_bytes(plan)?).map_err(resource)?;
            if plan.summary != header.summary || plan.items.len() != header.request.items.len()
                || plan.items.iter().any(|entry| !header.request.items.contains(&entry.request))
                || plan.items.windows(2).any(|pair| pair[0].request.item_id >= pair[1].request.item_id)
                || super::binding_digest(&plan.items).map_err(invalid)? != header.reviewed_digest
            { return Err(SafeError::InvalidInput); }
        }
        let cursor = super::super::source::attest::Cursor::new(header.origin.clone());
        Ok(Self { header, state, cursor, catalog, wire: false, compared: false, source_tail: false, wire_tail: false, wire_count: 0, wire_after: None, cell_length: None, utf8_tail: Vec::new(), _resident: guards })
    }

    pub(crate) fn accept(&mut self, raw: &[u8], budget: &mut Budget) -> Result<Keys> {
        budget.charge(0,0,0,raw.len() as u64).map_err(resource)?;
        if raw.first() == Some(&b'{') {
            if self.wire || self.compared || self.source_tail { return Err(SafeError::InvalidInput); }
            let page: Page = json::typed(raw, budget)?;
            self.source_page(page, budget)
        } else {
            if !self.source_tail { return Err(SafeError::InvalidInput); }
            let rows: Vec<PayloadRow> = json::typed(raw, budget)?;
            // [] is both Wire EOF and a comparison page. Once Source is closed,
            // the first empty array closes Wire; subsequent empties are neutral.
            if rows.is_empty() {
                self.wire = true;
                self.wire_tail = true;
                return Ok(Keys::None);
            }
            match &rows[0] {
                PayloadRow::Wire(_) => {
                    if self.wire_tail || self.compared { return Err(SafeError::InvalidInput); }
                    self.wire = true;
                    let mut keys = Vec::with_capacity(rows.len());
                    for row in rows {
                        let PayloadRow::Wire(key) = row else { return Err(SafeError::InvalidInput); };
                        let table = self.catalog.iter().find(|t| t.table == key.table).ok_or(SafeError::InvalidInput)?;
                        valid_key(&key, table)?;
                        let order = (key.table.clone(), super::super::spool::wire_key(&key));
                        if self.wire_after.as_ref().is_some_and(|old| *old >= order) { return Err(SafeError::InvalidInput); }
                        self.wire_after = Some(order);
                        self.wire_count = self.wire_count.checked_add(1).ok_or(SafeError::LimitExceeded)?;
                        keys.push(key);
                    }
                    Ok(Keys::Wire(keys))
                },
                PayloadRow::Comparison { .. } => {
                    if !self.wire_tail { return Err(SafeError::InvalidInput); }
                    self.compared = true;
                    for row in rows {
                        let PayloadRow::Comparison { key, cells } = row else { return Err(SafeError::InvalidInput); };
                        if key.is_empty() || key.len() > 2 || cells.len() < key.len() || cells.len() > 9 { return Err(SafeError::InvalidInput); }
                    }
                    Ok(Keys::None)
                },
            }
        }
    }

    fn source_page(&mut self, page: Page, budget: &mut Budget) -> Result<Keys> {
        let has_parts = page.parts().next().is_some();
        if page.before != self.cursor || page.after.source.rewind() != self.header.origin
            || page.after.ordinal < page.before.ordinal || page.after.partial > 65536
            || page.after.ordinal == page.before.ordinal && page.after.partial < page.before.partial
            || page.after.position != page.before.position.checked_add(page.members.len() as i64).ok_or(SafeError::LimitExceeded)?
            || page.after.eof != page.before.eof + page.eof.len()
            || !has_parts && (page.after.ordinal != page.before.ordinal || page.after.partial != page.before.partial)
            || has_parts && page.after.ordinal == page.before.ordinal && page.after.partial == page.before.partial
        { return Err(SafeError::InvalidInput); }
        let mut cp = coordinates(&page.before.source, budget)?;
        let began_complete = cp.table == self.catalog.len() && cp.row.is_none();
        let mut eof = page.eof.iter().peekable();
        let mut members = page.members.iter();
        for part in page.parts() {
            budget.check().map_err(resource)?;
            while self.catalog.get(cp.table).is_some_and(|t| t.table != part.table && eof.peek().is_some_and(|name| **name == t.table)) && cp.row.is_none() {
                eof.next(); cp.table += 1; cp.after = None;
            }
            let table = self.catalog.get(cp.table).ok_or(SafeError::InvalidInput)?;
            valid_key(part.key, table)?;
            if part.table != table.table || part.column != table.columns.get(cp.column).ok_or(SafeError::InvalidInput)?
                || part.offset != cp.offset || part.bytes.len() > 65536
                || part.length.is_none() && (!part.bytes.is_empty() || part.offset != 0)
                || part.length.is_some_and(|n| n > i64::MAX as u64 || part.offset.checked_add(part.bytes.len() as u64).is_none_or(|end| end > n) || part.bytes.is_empty() && n != 0)
            { return Err(SafeError::InvalidInput); }
            if let Some(row) = &cp.row {
                if row != part.key { return Err(SafeError::InvalidInput); }
            } else {
                if cp.after.as_ref().is_some_and(|old| old == part.key) { return Err(SafeError::InvalidInput); }
                cp.row = Some(part.key.clone());
            }
            if self.cell_length.is_some_and(|length| length != part.length) { return Err(SafeError::InvalidInput); }
            self.cell_length = Some(part.length);
            let _scratch = budget.reserve(65540).map_err(resource)?;
            budget.charge(0,0,0,(self.utf8_tail.len() + part.bytes.len()) as u64).map_err(resource)?;
            let mut utf8 = std::mem::take(&mut self.utf8_tail);
            utf8.extend_from_slice(part.bytes);
            if let Err(error) = std::str::from_utf8(&utf8) {
                if error.error_len().is_some() { return Err(SafeError::InvalidInput); }
                self.utf8_tail.extend_from_slice(&utf8[error.valid_up_to()..]);
            }
            if let Some(index) = table.primary_key.iter().position(|column| column == part.column) {
                let expected = part.key.values[index].as_bytes();
                let start = usize::try_from(part.offset).map_err(invalid)?;
                let end = start.checked_add(part.bytes.len()).ok_or(SafeError::LimitExceeded)?;
                if part.length != Some(expected.len() as u64) || expected.get(start..end) != Some(part.bytes) { return Err(SafeError::InvalidInput); }
            }
            cp.offset += part.bytes.len() as u64;
            if part.length.is_none_or(|length| cp.offset == length) {
                if !self.utf8_tail.is_empty() { return Err(SafeError::InvalidInput); }
                cp.column += 1; cp.offset = 0; self.cell_length = None;
            }
            if cp.column == table.columns.len() {
                if members.next() != cp.row.as_ref() { return Err(SafeError::InvalidInput); }
                cp.after = cp.row.take(); cp.column = 0;
            }
        }
        for name in eof {
            if cp.row.is_some() || self.catalog.get(cp.table).is_none_or(|t| t.table != *name) { return Err(SafeError::InvalidInput); }
            cp.table += 1; cp.after = None;
        }
        if members.next().is_some() || cp != coordinates(&page.after.source, budget)?
            || page.after.eof != cp.table
            || cp.table == self.catalog.len() && page.after.partial != 0
        { return Err(SafeError::InvalidInput); }
        if began_complete {
            if page.after != page.before || !page.members.is_empty() { return Err(SafeError::InvalidInput); }
            self.source_tail = true;
        }
        self.cursor = page.after;
        Ok(Keys::Source(page.members))
    }

    pub(crate) fn finish(&self) -> Result<()> {
        if self.cursor != self.state.source || self.wire_count != self.state.wire_count || self.wire_after != self.state.wire_after
            || (self.state.stage == Stage::Source) == self.source_tail
            || !matches!(self.state.stage, Stage::Source | Stage::Wire) && !self.wire_tail
            || self.state.stage == Stage::Wire && self.wire_tail
            || matches!(self.state.stage, Stage::Source | Stage::Wire | Stage::Reduce) && self.compared
        { return Err(SafeError::InvalidInput); }
        Ok(())
    }
}

fn state_bytes(plan: &super::CanonicalPlan) -> Result<u64> {
    struct Count(u64);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.checked_add(bytes.len() as u64).ok_or_else(|| std::io::Error::other("size"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, plan).map_err(invalid)?;
    Ok(count.0)
}

fn valid_key(key: &NativeKey, table: &NativeTable) -> Result<()> {
    if key.table != table.table || key.values.len() != table.primary_key.len() || key.values.iter().any(|v| v.len() > 1024) { return Err(SafeError::InvalidInput); }
    Ok(())
}
