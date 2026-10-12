//! Safe owned projections. Caller/database text deliberately has no Debug impl.
use super::super::{CleanupMode, ConfirmationLevel, Ownership, SideEffect};
use super::*;
use crate::pinning::provider::{ObservedResourceStatus, SubmitEffect};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceGap {
    TargetGenerationUnavailable,
    TargetRequirementsUnavailable,
    AggregateFrontierUnavailable,
    ProviderInventoryFrontierUnavailable,
    ProviderRootAttributionUnavailable,
    UnattributedLease,
    StoredByteSafetyUnavailable,
    ReadabilityNotProbed,
    DecisionRevisionUnavailable,
    HistoricalRouteUnavailable,
    HistoricalAccountingUnavailable,
    OrphanLedgerEpochUnavailable,
    SubmissionProjectionUnavailable,
    ManifestRevisionUnavailable,
    PersistedStateUnsupported,
    OwnershipProofUnavailable,
    CanonicalAliasCollision,
}

#[derive(Clone, Serialize)]
pub struct DatabaseSnapshot {
    pub observed_at: i64,
    pub objects: Vec<ObjectEvidence>,
    pub routes: Vec<RouteEvidence>,
    pub leases: Vec<LeaseEvidence>,
    pub targets: Vec<TargetEvidence>,
    pub remotes: Vec<RemoteEvidence>,
    pub historical_remotes: Vec<RemoteEvidence>,
    pub jobs: Vec<JobEvidence>,
    pub submissions: Vec<SubmissionEvidence>,
    /// Real provider allocation namespaces and persisted logical reservations.
    pub usage: Vec<UsageEvidence>,
    /// Receipts are deliberately not inferred by the unauthenticated collector;
    /// records::lookup_applied resolves the exact local authorized DB binding.
    pub applied: Vec<AppliedItem>,
    pub needs_attention: Vec<EvidenceGap>,
    #[serde(skip)]
    pub(super) planning: Option<Snapshot>,
}

impl DatabaseSnapshot {
    /// Full native planning capture; unknown item evidence remains nullable and
    /// blocks only its actual scope. This method grants no execution authority.
    pub fn planning_snapshot(&self) -> Result<Snapshot> {
        self.planning.clone().ok_or(SnapshotError::UnsupportedEvidence)
    }
}

#[derive(Clone, Serialize)]
pub struct ObjectEvidence {
    pub identity: ExactObject,
    pub original_cid: String,
    pub logical_bytes: u64,
    pub encrypted: bool,
    pub decision: Option<DecisionState>,
    pub legacy_unknown: bool,
    pub control_revision: Option<String>,
    pub config_revision: Option<String>,
    pub selection: SelectionEvidence,
    /// Committed exact bindings; root_revision is NOT a manifest revision.
    pub manifest_bindings: Vec<ManifestBinding>,
}

#[derive(Clone, Serialize)]
pub struct ManifestBinding {
    pub batch_id: String,
    pub entry_path: String,
}

#[derive(Clone, Serialize)]
pub struct RouteEvidence {
    pub identity: RouteIdentity,
    pub available: bool,
    pub allocation_allowed: bool,
    pub cleanup: CleanupMode,
    /// No exclusive-management agreement is inferred from cleanup=managed.
    pub exclusive_management: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredLeaseState {
    Active,
    Expired,
    Cancelled,
    Evicted,
    Unknown,
}

#[derive(Clone, Serialize)]
pub struct LeaseEvidence {
    pub id: String,
    pub object_id: String,
    pub generation: i64,
    pub started_at: i64,
    pub expires_at: i64,
    pub state: StoredLeaseState,
    pub policy_id: String,
    pub provider_mode: Option<super::super::ProviderMode>,
    /// Leases without targets cannot be attributed to a provider from current TOML.
    pub has_targets: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredTargetState {
    Waiting,
    Submitted,
    Pinned,
    Degraded,
    QuotaWaiting,
    QuotaBlocked,
    Evicted,
    Released,
    Unknown,
}

#[derive(Clone, Serialize)]
pub struct TargetEvidence {
    pub id: String,
    pub lease_id: String,
    pub allocation: String,
    pub cid: String,
    pub original_cid: String,
    pub logical_bytes: u64,
    pub state: StoredTargetState,
    /// Registration at read time, NOT the historical target invocation route.
    pub registered_route: Option<RouteIdentity>,
    pub created_at: i64,
    pub last_touched_at: i64,
    // Intentionally no target generation or reconstructed full requirement set.
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerState {
    Reserved,
    NotCreated,
    Confirmed,
    Retained,
    CleanupPending,
    Absent,
    Unknown,
}

#[derive(Clone, Serialize)]
pub struct RemoteEvidence {
    pub allocation: String,
    pub cid: String,
    pub original_cid: String,
    pub route: Option<RouteIdentity>,
    pub resource_id: Option<String>,
    pub epoch: Option<i64>,
    pub effect: SideEffect,
    pub ledger_state: LedgerState,
    pub ownership: Ownership,
    pub application_references: Option<u64>,
    /// Durable running jobs; not a claim that an expired claim stopped remote IO.
    pub inflight_jobs: Option<u32>,
    pub retained: bool,
    pub cleanup_pending: bool,
    pub accounted_bytes: Option<u64>,
    pub confirmations: Vec<ConfirmationLevel>,
    /// No live provider probe or observation-freshness guarantee is performed.
    pub observation_current: bool,
    pub confirmed_absent: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Pending,
    Running,
    Done,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Submit,
    Poll,
    Unpin,
    Reconcile,
    Unknown,
}

#[derive(Clone, Serialize)]
pub struct JobEvidence {
    pub id: String,
    pub allocation: String,
    pub cid: String,
    pub target_id: Option<String>,
    pub lease_id: Option<String>,
    pub expected_generation: Option<i64>,
    pub expected_remote_epoch: Option<i64>,
    pub state: JobState,
    pub operation: Operation,
    pub claim_until: Option<i64>,
    pub historical_route: Option<RouteIdentity>,
    pub invocation_epoch: Option<i64>,
    pub submitted_calls: Option<i32>,
    pub submission_effect: SideEffect,
}

#[derive(Clone, Serialize)]
pub struct SubmissionEvidence {
    pub id: String,
    pub job_id: String,
    pub allocation: String,
    pub expected_cid: String,
    pub route: RouteIdentity,
    pub remote_epoch: i64,
    pub claim_until: i64,
    pub effect: SubmitEffect,
    pub inflight: bool,
    pub needs_attention: bool,
    pub resources: Vec<SubmittedResource>,
}

#[derive(Clone, Serialize)]
pub struct SubmittedResource {
    pub resource_type: ResourceType,
    pub cid: String,
    pub original_cid: String,
    pub resource_id: String,
    pub ownership: Ownership,
    pub status: ObservedResourceStatus,
}

#[derive(Clone, Serialize)]
pub struct UsageEvidence {
    pub allocation: String,
    pub reserved_bytes: u64,
    pub reserved_pins: u64,
}
