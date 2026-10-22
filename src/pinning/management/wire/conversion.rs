//! Explicit, file-only v2 archival. No planner, receipt binder or runtime adapter.
//! The original review and every supplied claim remain unauthenticated data.
use super::{MAX_WIRE_BYTES, SafeError, v3};
use crate::{
    pinning::management::{
        NativeFrame, NativeKey, PlanRequest, Summary, validate_historical_request,
    },
    store::{
        initialization::history::legacy,
        pinning::management::{
            ManagementError,
            native::meter::{Budget, ResidentGuard},
            records,
        },
    },
};
use serde::Deserialize;
use std::io::{Read, Write};

type Result<T> = std::result::Result<T, SafeError>;

/// Mandatory v3 header text for metadata the v2 file did not record. It is not
/// a generated principal, database identity, timestamp, catalog or authorization.
pub const NOT_RECORDED: &str = "not-recorded-in-wire-v2";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalPlan {
    pub items: Vec<legacy::PlanEntry>,
    pub summary: Summary,
}

/// Actual Stage 7 provider captures, including their complete inventory, rather
/// than the incompatible current retirement DTO. These are claims, not receipts.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalDocument {
    pub schema_version: u32,
    pub applied_trust: v3::AppliedTrust,
    pub request: PlanRequest,
    pub snapshot: legacy::Snapshot,
    #[serde(deserialize_with = "Option::deserialize")]
    pub native: Option<NativeFrame>,
    pub plan: OriginalPlan,
    #[serde(deserialize_with = "Option::deserialize")]
    pub confirmation: Option<Vec<String>>,
    pub migration_points: Vec<records::MigrationPoint>,
}

/// Complete, recoverable source document. No conversion to an executable plan
/// or trusted Snapshot is provided. Exact whitespace/NULL/private text survives.
pub struct RecoveredV2 {
    original: Vec<u8>,
    document: OriginalDocument,
    _resident: Vec<ResidentGuard>,
}
impl RecoveredV2 {
    pub fn original(&self) -> &[u8] {
        &self.original
    }
    pub fn document(&self) -> &OriginalDocument {
        &self.document
    }

    /// Emit an archive-only private v3 file. The caller supplies a private writer
    /// with a protected exclusive-create handle and the same operation budget.
    pub fn write_archive<W: Write>(self, mut writer: v3::Writer<W>) -> Result<(W, Budget)> {
        let header = header(&self.document);
        let Self {
            original,
            document,
            mut _resident,
        } = self;
        drop(document);
        // The typed decode arena is no longer live; retain the original bytes.
        _resident.pop();
        writer.frame(&header)?;
        for frame in body() {
            writer.frame(&frame)?;
        }
        writer.frame(&row())?;
        writer.frame(&cell())?;
        for bytes in original.chunks(v3::CELL_BYTES) {
            writer
                .cell_output()
                .write_all(bytes)
                .map_err(|_| SafeError::InvalidInput)?;
        }
        writer.frame(&v3::Frame::CellEnd {
            value: v3::CellValue::Text,
        })?;
        writer.frame(&v3::Frame::RowEnd)?;
        writer.finish_with_budget()
    }
}

fn limit<T>(result: std::result::Result<T, ManagementError>) -> Result<T> {
    result.map_err(|error| {
        if error == ManagementError::Budget {
            SafeError::LimitExceeded
        } else {
            SafeError::InvalidInput
        }
    })
}

fn decode(
    original: Vec<u8>,
    mut resident: Vec<ResidentGuard>,
    budget: &mut Budget,
) -> Result<RecoveredV2> {
    resident.pop();
    resident.push(limit(budget.reserve(original.capacity()))?);
    // ReadSession prepays structure, duplicate-key scratch and DTO allocations
    // against the v2 16 MiB/node/depth arena before deserializing them.
    resident.push(limit(budget.reserve(MAX_WIRE_BYTES))?);
    let text = std::str::from_utf8(&original).map_err(|_| SafeError::InvalidInput)?;
    let mut session = records::ReadSession::default();
    let document: OriginalDocument = limit(session.typed(text))?;
    limit(budget.charge(0, 0, 0, session.usage().1 as u64))?;
    resident.pop();
    resident.push(limit(budget.reserve(session.usage().1))?);
    if document.schema_version != 2 {
        return Err(SafeError::UnsupportedSchemaVersion);
    }
    validate_historical_request(&document.request)?;
    // Preserve the saved review; do not rerun a current planner over old data.
    // Shape and item identity checks are not a claim of business consistency.
    if document.plan.items.len() != document.request.items.len()
        || document.plan.summary.items != document.plan.items.len()
        || document
            .plan
            .items
            .iter()
            .enumerate()
            .any(|(index, entry)| {
                !document.request.items.contains(&entry.request)
                    || entry.id.as_str()
                        != format!(
                            "management-v1:{}:{}",
                            document.request.plan_id, entry.request.item_id
                        )
                    || document.plan.items[..index]
                        .iter()
                        .any(|prior| prior.request.item_id == entry.request.item_id)
            })
    {
        return Err(SafeError::InvalidInput);
    }
    if let Some(native) = &document.native {
        limit(native.validate())?;
    }
    limit(budget.check())?;
    Ok(RecoveredV2 {
        original,
        document,
        _resident: resident,
    })
}

/// Strict isolated v2 reader, including duplicate keys inside native maps.
/// This bounded legacy JSON limit does not apply to normal streaming v3 files.
pub fn read_v2<R: Read>(mut source: R, budget: &mut Budget) -> Result<RecoveredV2> {
    let resident = vec![limit(budget.reserve(MAX_WIRE_BYTES))?];
    let mut original = Vec::new();
    let mut buffer = [0; v3::CELL_BYTES];
    loop {
        limit(budget.check())?;
        let n = source
            .read(&mut buffer)
            .map_err(|_| SafeError::InvalidInput)?;
        if n == 0 {
            break;
        }
        if n > MAX_WIRE_BYTES.saturating_sub(original.len()) {
            return Err(SafeError::LimitExceeded);
        }
        limit(budget.charge(0, 0, 0, n as u64))?;
        original.reserve_exact(n);
        original.extend_from_slice(&buffer[..n]);
    }
    decode(original, resident, budget)
}

fn header(document: &OriginalDocument) -> v3::Frame {
    let native = document.native.as_ref();
    v3::Frame::Header {
        schema_version: 3,
        projection: v3::Projection::PrivateRecovery,
        database_scope: native.map_or(NOT_RECORDED, |n| &n.database_scope).into(),
        database_identity: native.map_or(NOT_RECORDED, |n| &n.database_identity).into(),
        principal: NOT_RECORDED.into(),
        applied_trust: v3::AppliedTrust::UntrustedClaimsOnly,
        catalog: native.map_or_else(|| vec![NOT_RECORDED.into()], |n| n.schema_versions.clone()),
        created_at: NOT_RECORDED.into(),
        expires_at: None,
    }
}
fn body() -> [v3::Frame; 4] {
    [
        v3::Frame::Request { request: None },
        v3::Frame::Configuration {
            configuration: None,
        },
        v3::Frame::Acknowledgements { item_ids: None },
        v3::Frame::Summary {
            summary: Summary::default(),
        },
    ]
}
fn row() -> v3::Frame {
    v3::Frame::Row {
        section: v3::Section::Result,
        key: NativeKey {
            table: "legacy_wire_v2".into(),
            values: vec!["original".into()],
        },
        cells: 1,
    }
}
fn cell() -> v3::Frame {
    v3::Frame::Cell {
        column: "document".into(),
    }
}
fn expect<R: Read>(reader: &mut v3::Reader<R>, frame: v3::Frame) -> Result<()> {
    match reader.next_frame()? {
        Some(v3::Event::Frame(actual)) if actual == frame => Ok(()),
        _ => Err(SafeError::InvalidInput),
    }
}

/// Explicit private recovery of a converted file. A verified trailer is required
/// before any bytes or typed historical data are returned. Public redactions
/// cannot be recovered. Even a self-consistent rewritten file grants no authority.
pub fn recover_v3<R: Read>(source: R, budget: Budget) -> Result<(RecoveredV2, Budget)> {
    let mut reader = v3::Reader::new(source, v3::Projection::PrivateRecovery, budget)?;
    let Some(v3::Event::Frame(source_header @ v3::Frame::Header { .. })) = reader.next_frame()?
    else {
        return Err(SafeError::InvalidInput);
    };
    let header_memory = limit(reader.budget_mut().reserve(v3::FRAME_BYTES))?;
    for frame in body() {
        expect(&mut reader, frame)?;
    }
    expect(&mut reader, row())?;
    expect(&mut reader, cell())?;
    let resident = vec![limit(reader.budget_mut().reserve(MAX_WIRE_BYTES))?];
    let mut original = Vec::new();
    loop {
        match reader.next_frame()? {
            Some(v3::Event::Chunk(bytes)) => {
                if bytes.len() > MAX_WIRE_BYTES.saturating_sub(original.len()) {
                    return Err(SafeError::LimitExceeded);
                }
                original.reserve_exact(bytes.len());
                original.extend_from_slice(&bytes);
            }
            Some(v3::Event::Frame(v3::Frame::CellEnd {
                value: v3::CellValue::Text,
            })) => break,
            _ => return Err(SafeError::InvalidInput),
        }
    }
    expect(&mut reader, v3::Frame::RowEnd)?;
    if reader.next_frame()?.is_some() {
        return Err(SafeError::InvalidInput);
    }
    let (_, mut budget) = reader.into_verified()?;
    let recovered = decode(original, resident, &mut budget)?;
    if source_header != header(recovered.document()) {
        return Err(SafeError::InvalidInput);
    }
    drop(header_memory);
    Ok((recovered, budget))
}
