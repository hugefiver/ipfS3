//! Sequential management archive, never an execution capability.
//!
//! Each frame is a big-endian u32 length (including its one-byte kind), followed
//! by kind 0 + strict control JSON, kind 1 + original cell bytes, kind 2 + column
//! UTF-8, or payload-free kinds 3/4/5 (NULL/text/redacted cell end) and 6 (row end).
//! Kind 7 holds a complete short cell: u16BE column length, u8 value tag
//! (0 NULL, 1 text, 2 redacted), column UTF-8, and at most CELL_BYTES text bytes.
//! The digest covers the
//! magic and every encoded frame except the terminal trailer. There are no
//! external references. Consumers stage events in task-owned storage and must
//! not publish them before `Reader::verified()`. Binding to local originals and
//! authorization are separate operations; even Result/AppliedClaim rows carry
//! only untrusted claims. A private recovery is archive-only, not DB restoration.

pub mod archive_cursor;
mod codec;
pub mod document;
pub mod files;
pub mod history;
pub mod matching;
pub mod planning;
mod state;
mod strict;
pub(crate) use codec::WriterContinuation;
pub use codec::{CellOutput, Reader, Writer};

use super::super::model::{
    ConfigurationBinding, IdempotencyId, ItemRequest, NativeKey, PlanEntry, PlanRequest,
    PlanStatus, PlannedStep, Summary,
};
use serde::{Deserialize, Serialize};

pub const MAGIC: &[u8; 10] = b"IPFSMGMT\0\x03";
pub const SCHEMA_VERSION: u32 = 3;
pub const FRAME_BYTES: usize = 1024 * 1024;
pub const CELL_BYTES: usize = 64 * 1024;

/// The caller must explicitly select private recovery on both sides. Review
/// mode cannot accept a private file, even when its cells happen to be empty.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Projection {
    Review,
    PrivateRecovery,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Section {
    Native,
    Membership,
    Inventory,
    Result,
    AppliedClaim,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CellValue {
    Null,
    Text,
    Redacted,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppliedTrust {
    UntrustedClaimsOnly,
}

/// Bounded decision facts only. Captures remain private segmented cells; global
/// inventory is represented by frozen references and cursors, never a bulk DTO.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemFact {
    pub id: IdempotencyId,
    pub request: ItemRequest,
    pub status: PlanStatus,
    #[serde(deserialize_with = "Option::deserialize")]
    pub step: Option<PlannedStep>,
}
impl From<&PlanEntry> for Box<ItemFact> {
    fn from(entry: &PlanEntry) -> Self {
        Box::new(ItemFact {
            id: entry.id.clone(),
            request: entry.request.clone(),
            status: entry.status,
            step: entry.step.clone(),
        })
    }
}

/// Small typed parts are bounded by FRAME_BYTES before allocation. Ordered rows
/// are independently segmented; no Snapshot or ImportedPlan is reconstructed.
/// `catalog` is the exact historical schema-version list, not a current catalog
/// substituted for the archive. Row keys and columns use strict lexical order
/// within each section, permitting bounded-memory duplicate detection. Keys
/// have at most 64 components of 1024 bytes each; a row has at most 256 cells.
#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Frame {
    Header {
        schema_version: u32,
        projection: Projection,
        database_scope: String,
        database_identity: String,
        principal: String,
        applied_trust: AppliedTrust,
        catalog: Vec<String>,
        created_at: String,
        #[serde(deserialize_with = "Option::deserialize")]
        expires_at: Option<String>,
    },
    /// None is reserved for archive-only recovery, not a confirmable plan.
    Request {
        #[serde(deserialize_with = "Option::deserialize")]
        request: Option<PlanRequest>,
    },
    Configuration {
        #[serde(deserialize_with = "Option::deserialize")]
        configuration: Option<ConfigurationBinding>,
    },
    Acknowledgements {
        #[serde(deserialize_with = "Option::deserialize")]
        item_ids: Option<Vec<String>>,
    },
    Item {
        entry: Box<ItemFact>,
    },
    Summary {
        summary: Summary,
    },
    Row {
        section: Section,
        key: NativeKey,
        cells: u32,
    },
    Cell {
        column: String,
    },
    /// Explicitly distinguishes SQL NULL, empty UTF-8, and withheld private data.
    CellEnd {
        value: CellValue,
    },
    RowEnd,
    Trailer {
        frames: u64,
        bytes: u64,
        sha256: String,
    },
}

pub enum Event {
    Frame(Frame),
    Chunk(Vec<u8>),
}
