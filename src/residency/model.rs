use chrono::{DateTime, Utc};

use crate::error::{AppError, AppResult};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum KuboTier {
    Hot,
    Cold,
}

impl KuboTier {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Cold => "cold",
        }
    }

    pub fn from_db_str(value: &str) -> AppResult<Self> {
        match value {
            "hot" => Ok(Self::Hot),
            "cold" => Ok(Self::Cold),
            _ => Err(corrupt("unknown Kubo tier")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageClass {
    Standard,
    StandardIa,
}

impl StorageClass {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::StandardIa => "STANDARD_IA",
        }
    }

    pub fn from_db_str(value: &str) -> AppResult<Self> {
        match value {
            "STANDARD" => Ok(Self::Standard),
            "STANDARD_IA" => Ok(Self::StandardIa),
            _ => Err(corrupt("unknown storage class")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationState {
    Pending,
    Verified,
    Failed,
}

impl VerificationState {
    pub fn from_db_str(value: &str) -> AppResult<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "verified" => Ok(Self::Verified),
            "failed" => Ok(Self::Failed),
            _ => Err(corrupt("unknown residency verification state")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalVerification {
    Pending,
    Verified {
        node_identity: String,
        receipt: String,
    },
}

impl PhysicalVerification {
    pub fn verified(node_identity: impl Into<String>, receipt: impl Into<String>) -> Self {
        Self::Verified {
            node_identity: node_identity.into(),
            receipt: receipt.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReferenceReason {
    RetainedVersion,
    TransitionStaging,
    TransitionCleanupHold,
}

impl ReferenceReason {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::RetainedVersion => "retained_version",
            Self::TransitionStaging => "transition_staging",
            Self::TransitionCleanupHold => "transition_cleanup_hold",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionResidencyIdentity {
    pub version_row_id: String,
    pub object_id: String,
    pub cid: String,
}

impl VersionResidencyIdentity {
    pub fn new(
        version_row_id: impl Into<String>,
        object_id: impl Into<String>,
        cid: impl Into<String>,
    ) -> Self {
        Self {
            version_row_id: version_row_id.into(),
            object_id: object_id.into(),
            cid: cid.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ResidencyLocation {
    pub tier: KuboTier,
    pub cid: String,
}

impl ResidencyLocation {
    pub fn new(tier: KuboTier, cid: impl Into<String>) -> Self {
        Self {
            tier,
            cid: cid.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalResidencySnapshot {
    pub location: ResidencyLocation,
    pub verification_state: VerificationState,
    pub node_identity: Option<String>,
    pub verification_receipt: Option<String>,
    pub verified_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedVersionResidency {
    pub identity: VersionResidencyIdentity,
    pub primary: ResidencyLocation,
    pub storage_class: StorageClass,
    pub revision: i64,
    pub physical: PhysicalResidencySnapshot,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReferenceSummary {
    pub retained_versions: u64,
    pub transition_staging_holds: u64,
    pub transition_cleanup_holds: u64,
    pub active_lease_targets: u64,
    pub active_lease_owners: u64,
}

impl ReferenceSummary {
    /// This is an observability signal only. It never authorizes physical deletion.
    pub fn has_known_references(&self) -> bool {
        self.retained_versions > 0
            || self.transition_staging_holds > 0
            || self.transition_cleanup_holds > 0
            || self.active_lease_targets > 0
            || self.active_lease_owners > 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidencyBackfillCursor {
    pub version_row_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedResidencyBackfill {
    pub worker_id: String,
    pub claim_epoch: i64,
    pub cursor: Option<ResidencyBackfillCursor>,
    pub lease_until: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingHotResidency {
    pub identity: VersionResidencyIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingHotResidencyPage {
    pub items: Vec<PendingHotResidency>,
    pub next_cursor: Option<ResidencyBackfillCursor>,
    pub complete: bool,
}

pub const MAX_RESIDENCY_BACKFILL_PAGE_SIZE: u64 = 1_000;

pub(crate) fn corrupt(message: &str) -> AppError {
    AppError::Internal(format!("invalid residency state: {message}"))
}
