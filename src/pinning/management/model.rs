//! Owned, non-secret DTOs. No type containing caller text implements Debug.

pub use crate::store::pinning::management::native::{NativeFrame, NativeKey, NativeRow};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SafeError {
    InvalidInput,
    LimitExceeded,
    DuplicateIdentity,
    InconsistentSnapshot,
    MissingExactObject,
    MissingTarget,
    MissingRoute,
    UnreadableObject,
    StoredByteProofUnavailable,
    SafetyProhibition,
    RequiresExplicitReapply,
    InvalidActionScope,
    LeaseNotActive,
    IntentExpired,
    HistoricalRouteUnavailable,
    AllocationStopped,
    IncompleteInventory,
    SnapshotChanged,
    ConfirmationMismatch,
    CandidateConfirmationRequired,
    PlanBlocked,
    IdempotencyConflict,
    UnsupportedSchemaVersion,
    UnknownEvidence,
    ConfirmationExpired,
    DatabaseScopeMismatch,
}

impl std::fmt::Display for SafeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "management validation: {self:?}")
    }
}

impl std::error::Error for SafeError {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicVersionId {
    /// Explicit S3 null version, never a selector for the current key.
    Null,
    Version(String),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactObject {
    pub bucket: String,
    pub key: String,
    pub public_version: PublicVersionId,
    /// object_versions.id, including non-public versions of an unversioned key.
    pub private_version_id: String,
    pub object_id: String,
    /// Already parsed/canonicalized by the parent CID adapter; not an ETag hash.
    pub cid: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionState {
    None,
    Accepted,
    Skipped,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SelectionEvidence {
    ExplicitObject,
    TrustedManifest {
        batch_id: String,
        entry_path: String,
        batch_tuple: NativeRow,
        entry_tuple: NativeRow,
    },
    /// No inferred ZIP ownership, even if a prefix or old log looks plausible.
    LegacyZipCandidate,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectSnapshot {
    pub identity: ExactObject,
    pub logical_bytes: u64,
    pub readable: Option<bool>,
    pub read_evidence: Option<ReadEvidence>,
    pub permitted: Option<bool>,
    /// Parent's per-object, stored-byte safety decision for exact route revisions.
    pub allowed_routes: Vec<RouteIdentity>,
    pub control_revision: Option<String>,
    pub config_revision: Option<String>,
    pub decision: Option<DecisionState>,
    pub evidence: SelectionEvidence,
}

/// Public derived facts; original tuples and endpoint/read configuration remain
/// in the complete trusted DB frame, not this ordinary wire representation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadEvidence {
    pub identity: ExactObject,
    pub node_identity: String,
    pub stored_bytes: Option<u64>,
    pub proof: super::evidence::ReadProof,
    pub observed_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub idle_timeout_millis: u64,
}
impl From<&super::evidence::ReadAttestation> for ReadEvidence {
    fn from(proof: &super::evidence::ReadAttestation) -> Self {
        let stored_bytes = attested_stored_bytes(proof);
        Self {
            identity: proof.identity.clone(),
            node_identity: proof.source.node_identity.clone(),
            stored_bytes,
            proof: proof.proof,
            observed_at: proof.observed_at,
            expires_at: proof.expires_at,
            idle_timeout_millis: proof.source.idle_timeout_millis,
        }
    }
}
impl ReadEvidence {
    pub(crate) fn matches_attestation(&self, proof: &super::evidence::ReadAttestation) -> bool {
        self.identity == proof.identity
            && self.node_identity == proof.source.node_identity
            && self.stored_bytes == attested_stored_bytes(proof)
            && self.proof == proof.proof
            && self.observed_at == proof.observed_at
            && self.expires_at == proof.expires_at
            && self.idle_timeout_millis == proof.source.idle_timeout_millis
    }
}
pub(crate) fn attested_stored_bytes(proof: &super::evidence::ReadAttestation) -> Option<u64> {
    if proof.proof == super::evidence::ReadProof::CompleteLocalDirectoryDagBlocksV1 {
        proof
            .directory_receipt
            .as_ref()
            .filter(|receipt| {
                receipt.valid_shape()
                    && receipt.cid() == proof.identity.cid
                    && receipt.node_identity() == proof.source.node_identity
                    && Some(receipt.stored_block_bytes()) == proof.stored_bytes
            })
            .map(|receipt| receipt.stored_block_bytes())
    } else {
        proof.stored_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceType {
    PsaRequest,
    HostedFile,
    RpcPin,
    ClusterPin,
}

/// Stable identity plus historical invocation revision, not a URL/config alias.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteIdentity {
    pub provider_id: String,
    pub backend: String,
    pub scope: String,
    pub storage_domain: String,
    pub api_profile: String,
    pub strategy: String,
    pub resource_type: ResourceType,
    pub credential_revision: u64,
    pub endpoint_revision: u64,
    /// Every original execution field, including opaque secret reference,
    /// cleanup and Stage 6 confirmation. None is diagnostic/unknown only.
    pub original: Option<crate::pinning::identity::ProviderRouteSnapshot>,
}

impl Ord for RouteIdentity {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            &self.provider_id,
            &self.backend,
            &self.scope,
            &self.storage_domain,
            &self.api_profile,
            &self.strategy,
            self.resource_type,
            self.credential_revision,
            self.endpoint_revision,
        )
            .cmp(&(
                &other.provider_id,
                &other.backend,
                &other.scope,
                &other.storage_domain,
                &other.api_profile,
                &other.strategy,
                other.resource_type,
                other.credential_revision,
                other.endpoint_revision,
            ))
            .then_with(|| {
                serde_json::to_string(&self.original)
                    .expect("route serializes")
                    .cmp(&serde_json::to_string(&other.original).expect("route serializes"))
            })
    }
}
impl PartialOrd for RouteIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CleanupMode {
    Retain,
    Managed,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSnapshot {
    pub identity: RouteIdentity,
    /// Full original routing contract; public projections never fence execution.
    pub original: Option<crate::pinning::identity::ProviderRouteSnapshot>,
    pub available: bool,
    pub allocation_allowed: bool,
    pub cleanup: CleanupMode,
    pub exclusive_management: bool,
}

/// Allocation domain/CID key; opaque provider resource id is captured separately.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteKey {
    pub backend: String,
    pub scope: String,
    pub resource_type: ResourceType,
    pub cid: String,
}

impl RouteIdentity {
    pub fn from_original(original: crate::pinning::identity::ProviderRouteSnapshot) -> Self {
        let resource_type = match original.resource_type() {
            crate::pinning::identity::RemoteResourceType::PsaRequest => ResourceType::PsaRequest,
            crate::pinning::identity::RemoteResourceType::HostedFile => ResourceType::HostedFile,
            crate::pinning::identity::RemoteResourceType::RpcPin => ResourceType::RpcPin,
            crate::pinning::identity::RemoteResourceType::ClusterPin => ResourceType::ClusterPin,
        };
        Self {
            provider_id: original.provider_id.clone(),
            backend: original.backend.clone(),
            scope: original.scope.clone(),
            storage_domain: original.storage_domain.clone(),
            api_profile: original.api_profile.clone(),
            strategy: original.strategy.clone(),
            resource_type,
            credential_revision: original.credential_revision,
            endpoint_revision: original.endpoint_revision,
            original: Some(original),
        }
    }
    pub fn remote_key(&self, cid: &str) -> RemoteKey {
        RemoteKey {
            backend: self.backend.clone(),
            scope: self.scope.clone(),
            resource_type: self.resource_type,
            cid: cid.to_owned(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SideEffect {
    NotSubmitted,
    Rejected,
    Unknown,
    InFlight,
    Present,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Ownership {
    ApplicationCreated,
    ExternalExisting,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ConfirmationLevel {
    Accepted,
    RemoteSubmitted,
    RemotePinned,
    GatewayVerified,
    ContentVerified,
    ReplicationComplete,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSnapshot {
    pub key: RemoteKey,
    pub route: RouteIdentity,
    pub resource_id: Option<String>,
    pub epoch: i64,
    pub effect: SideEffect,
    pub ownership: Ownership,
    pub application_references: Option<u64>,
    pub inflight_operations: Option<u32>,
    pub retained: bool,
    pub cleanup_pending: bool,
    /// Ledger occupancy, not a claim about physical blocks or invoice pricing.
    pub accounted_bytes: Option<u64>,
    /// Independent evidence dimensions; enum ordering never proves implication.
    pub confirmations: Vec<ConfirmationLevel>,
    pub confirmed_replicas: Option<u32>,
    pub observation_current: Option<bool>,
    pub replica_started_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl RemoteSnapshot {
    pub(crate) fn has_unknown_responsibility(&self) -> bool {
        self.effect == SideEffect::Unknown
            || self.application_references.is_none()
            || self.inflight_operations.is_none()
            || self.accounted_bytes.is_none()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerDomainSnapshot {
    pub backend: String,
    pub scope: String,
    pub resource_type: ResourceType,
    pub frontier: Vec<NativeKey>,
    /// Authoritative local ledger inventory, not a remote-query absence proof.
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderMode {
    One,
    All,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirements {
    pub mode: ProviderMode,
    /// Full frozen requirement set, not just currently available providers.
    pub provider_ids: Vec<String>,
    pub levels: Vec<ConfirmationLevel>,
    pub min_replicas: u32,
    pub max_age_secs: Option<u32>,
    pub policy_revision: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LeaseState {
    Active,
    Expired,
    Cancelled,
    Unknown,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseSnapshot {
    pub id: String,
    pub object_id: String,
    pub generation: i64,
    pub started_at: i64,
    pub expires_at: i64,
    pub state: LeaseState,
    pub native_tuple: Option<NativeRow>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TargetState {
    Pending,
    MissingRequirements,
    Confirmed,
    Blocked,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetSnapshot {
    pub id: String,
    /// Exact SQL-native image of every pin_lease_targets column.
    pub native_tuple: NativeRow,
    pub frozen_contract: Option<NativeRow>,
    pub lease: LeaseSnapshot,
    pub cid: String,
    pub logical_bytes: u64,
    pub route: Option<RouteIdentity>,
    /// Full birth order, including unknown and unavailable slots. Operational
    /// allocations are resolved only by exact committed R26 point receipts.
    pub frozen_candidates: Vec<FrozenCandidate>,
    pub requirements: Option<Requirements>,
    pub state: TargetState,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCandidate {
    pub slot: String,
    pub allocation: String,
    pub route: Option<RouteIdentity>,
    pub receipt_id: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderInventory {
    pub provider_id: String,
    pub frontier: Vec<NativeKey>,
    /// Parent attests authoritative, complete inventory, including orphan ledger.
    pub complete: bool,
    /// Full membership can be known while old allocation ownership is unknown.
    /// Retirement retains that uncertainty; it never clears its responsibilities.
    pub attribution_complete: bool,
    pub allocation_stopped: bool,
    /// Includes leases without targets and expired/cancelled responsibilities.
    pub leases: Vec<LeaseSnapshot>,
    pub root_reference_ids: Vec<NativeKey>,
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// Database-clock seconds. Advancing time alone does not change identity.
    pub observed_at: i64,
    pub database_now: Option<chrono::DateTime<chrono::Utc>>,
    /// Full global bounded image, original precision, membership and PK frontier.
    /// None is allowed only for IO-free prototype/unit fixtures, never DB apply.
    pub native_frame: Option<NativeFrame>,
    pub configuration: Option<ConfigurationBinding>,
    pub objects: Vec<ObjectSnapshot>,
    pub routes: Vec<RouteSnapshot>,
    pub targets: Vec<TargetSnapshot>,
    pub remotes: Vec<RemoteSnapshot>,
    /// Outstanding pin_resource_history obligations, not just the latest epoch.
    pub historical_remotes: Vec<RemoteSnapshot>,
    /// Exact rows whose execution identity/epoch cannot be safely projected.
    /// Membership is retained; absence of a DTO never means absence of debt.
    pub unknown_resources: Vec<NativeKey>,
    pub ledger_domains: Vec<LedgerDomainSnapshot>,
    pub inventories: Vec<ProviderInventory>,
    /// Public projections of exact durable ZIP root references. Receipt contents
    /// remain private in native_frame; these projections are not authority.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_references: Vec<RootReferenceSnapshot>,
    /// Trusted durable receipts, atomically recorded by the future executor.
    pub applied: Vec<AppliedItem>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub started_at: i64,
    pub expires_at: i64,
    pub requirements: Requirements,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Create(Intent),
    Renew {
        expires_at: i64,
    },
    Cancel,
    Reapply(Intent),
    Backfill(Intent),
    Retry,
    /// Add destination execution; source responsibility and TTL remain intact.
    Migrate {
        destination: Box<RouteIdentity>,
    },
    Reconcile,
    /// Default retain: only stop allocations; never delete routes or resources.
    Retire,
    /// Additive explicit disposition. Historical `retire` still decodes as
    /// default Retain; an executor parameter cannot select another disposition.
    RetireWith {
        selection: RetirementSelection,
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RetirementSelection {
    Retain,
    /// Complete exact prior migration results; retirement never implies source
    /// cleanup or invents migration authority for an unselected object.
    Migrate {
        targets: Vec<RetirementMigration>,
    },
    ControlledCleanup {
        resources: Vec<RetirementResource>,
    },
    /// Logical release of only these exact root references. Never unpin/GC,
    /// delete root history, or infer provider ownership from an inventory.
    ReleaseRootReferences {
        references: Vec<NativeKey>,
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootReferenceSnapshot {
    pub key: NativeKey,
    /// Exact ZIP batch owner, not the retiring provider's identity.
    pub owner: String,
    pub reference_tuple: NativeRow,
    pub build_tuple: NativeRow,
    pub batch_tuple: NativeRow,
    /// Derived from the durable private verification_receipt, never a caller
    /// claim that substitutes for rechecking the complete native frame.
    pub verification_recorded: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementMigration {
    pub plan_id: String,
    pub item_id: String,
    pub lease_id: String,
    pub source_target_id: String,
    pub cid: String,
    pub destination: RouteIdentity,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementResource {
    pub key: RemoteKey,
    pub route: RouteIdentity,
    pub epoch: i64,
    pub resource_id: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TargetSelection {
    New(RouteIdentity),
    Existing {
        target_id: String,
        /// Exact readable target content when its lease owner is an archive.
        /// Never discovered from a key prefix or substituted current version.
        source: Option<ExactObject>,
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Subject {
    Object {
        identity: ExactObject,
        target: TargetSelection,
    },
    Provider {
        route: RouteIdentity,
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemRequest {
    pub item_id: String,
    pub action: Action,
    pub subject: Subject,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanRequest {
    /// Non-secret operator namespace. Uniqueness is not a safety proof.
    pub plan_id: String,
    pub items: Vec<ItemRequest>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyId(pub(crate) String);

impl IdempotencyId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoteDirective {
    NoWrite,
    /// Scheduling possibility only, never a network permission or completed POST.
    SubmitWithFence,
    ReconcileHistorical,
    ObserveExisting,
    WaitForInflight,
    ReuseConfirmed,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStep {
    pub source: RemoteDirective,
    pub destination: Option<RemoteDirective>,
    pub preserve_source_responsibility: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementStep {
    pub stop_new_allocations: bool,
    pub needs_attention: bool,
    pub leases: usize,
    pub retained: usize,
    pub unknown: usize,
    pub inflight: usize,
    pub cleanup_pending: usize,
    pub root_references: usize,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PlannedStep {
    Object(ObjectStep),
    Retire(RetirementStep),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PlanStatus {
    Ready,
    Candidate,
    Blocked(SafeError),
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectCapture {
    pub owner_identity: ExactObject,
    pub object: ObjectSnapshot,
    pub target: Option<TargetSnapshot>,
    pub route: RouteSnapshot,
    pub ledger_domain: LedgerDomainSnapshot,
    pub remote: Option<RemoteSnapshot>,
    pub remote_route: Option<RouteSnapshot>,
    pub destination_route: Option<RouteSnapshot>,
    pub destination_ledger_domain: Option<LedgerDomainSnapshot>,
    pub destination_remote: Option<RemoteSnapshot>,
    pub destination_remote_route: Option<RouteSnapshot>,
    pub historical_remotes: Vec<RemoteSnapshot>,
    pub historical_routes: Vec<RouteSnapshot>,
    pub requirement_routes: Vec<RouteSnapshot>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementCapture {
    pub inventory: ProviderInventory,
    pub routes: Vec<RouteSnapshot>,
    pub targets: Vec<TargetSnapshot>,
    pub remotes: Vec<RemoteSnapshot>,
    pub historical_remotes: Vec<RemoteSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_references: Vec<RootReferenceSnapshot>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Capture {
    Object(Box<ObjectCapture>),
    Provider(Box<RetirementCapture>),
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanEntry {
    pub id: IdempotencyId,
    pub request: ItemRequest,
    pub status: PlanStatus,
    pub capture: Option<Capture>,
    pub step: Option<PlannedStep>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedItem {
    /// Complete original binding, not just an ID, UUID, or hash receipt.
    pub binding: PlanEntry,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Confirmation {
    /// Exact candidate rows reviewed independently of the inferred old ZIP.
    pub candidate_item_ids: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationBinding {
    pub routes: Vec<crate::pinning::identity::ProviderRouteSnapshot>,
    pub policies: Vec<crate::pinning::config::ValidatedPolicy>,
    pub limits: std::collections::BTreeMap<String, crate::pinning::config::ProviderLimits>,
    pub zip_rules: Option<crate::pinning::zip_policy::ValidatedZipOutputRules>,
    pub source_node: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Summary {
    pub items: usize,
    pub candidates: usize,
    pub blocked: usize,
    pub already_applied: usize,
    pub logical_targets: usize,
    pub unique_cids: usize,
    pub unique_resources: usize,
    pub possible_new_transfers: usize,
    pub logical_intent_bytes: u64,
    pub accounted_remote_bytes: u64,
    pub unknown_resources: usize,
    pub inflight_resources: usize,
    pub cleanup_pending_resources: usize,
    /// Always zero: cancelling references is not a physical space release.
    pub physical_release_bytes: u64,
}

#[derive(Clone, PartialEq, Eq)]
pub enum RevalidationOutcome {
    Checked(PlannedStep),
    /// No permission to execute anything again, even if current state has changed.
    AlreadyApplied,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CheckedItem {
    pub id: IdempotencyId,
    pub outcome: RevalidationOutcome,
}
