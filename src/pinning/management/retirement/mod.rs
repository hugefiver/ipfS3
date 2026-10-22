//! Disk-backed retirement evidence. Coordinates and projections are not authority.
mod export;
mod import;
mod observations;
pub use import::FrozenRetirementImport;
mod reducer;
mod restore;
mod review;
mod shared;
mod source;
mod spool;
mod status;
mod totals;
mod union;
mod verify;
mod workspace;
pub use workspace::Workspace;
mod planning;
pub use planning::PreparedPlanning;
mod cache_guard;
mod checkpoint;
mod comparison;
pub(crate) mod confirmation;
pub use confirmation::{
    ConfirmationAdvance, ConfirmationEffects, ConfirmationPhase, ConfirmationProgress,
    ReadyConfirmation,
};
mod output;
pub(crate) use output::{PageMeter, PageOutput};
mod task_budget;
pub use checkpoint::{
    CommittedPlanning, PlanningCheckpoint, PlanningCheckpointInput, PlanningProgress,
};

pub use export::{ArchiveExport, RetirementEvidenceRefs, RetirementMutationRef};
pub use reducer::{InventoryTotals, ResourceIdentity};
pub use review::{
    CurrentRetirementInventory, RetirementFence, RetirementMutationScope, RetirementReview,
    RetirementTransition,
};
pub use source::{SourceCheckpoint, SourceTransition};
pub use spool::budget::{PhaseWork, QueryWork, TaskWork};
pub use spool::{
    ArchiveCursor, ArchivePart, InventoryMembership, InventorySpool, MembershipCursor, Progress,
    RunLimits,
};
pub use status::CurrentRetirementScan;
pub use union::PlanUnion;

use super::model::*;
use crate::store::pinning::management::{ManagementError, Result};
use serde::{Deserialize, Serialize};

pub const REDUCER_VERSION: u32 = 1;

/// An address, never proof that a scan completed or that execution is permitted.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementInventoryRef {
    pub archive_id: String,
    pub inventory_id: String,
    pub reducer_version: u32,
}

fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(|_| ManagementError::InvalidEvidence)
}
fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).map_err(|_| ManagementError::InvalidEvidence)
}
