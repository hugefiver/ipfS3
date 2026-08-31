use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalLifecycleConfiguration {
    pub schema_version: u8,
    pub rules: Vec<CanonicalLifecycleRule>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalLifecycleRule {
    pub id: Option<String>,
    pub status: LifecycleRuleStatus,
    pub selector: CanonicalRuleSelector,
    pub expiration: Option<CurrentExpiration>,
    pub noncurrent_version_expiration: Option<NoncurrentExpiration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleRuleStatus {
    Enabled,
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanonicalRuleSelector {
    LegacyPrefix { prefix: String },
    Modern { filter: CanonicalFilter },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanonicalFilter {
    All,
    Prefix {
        prefix: String,
    },
    Tag {
        tag: CanonicalTag,
    },
    ObjectSizeGreaterThan {
        bytes: i64,
    },
    ObjectSizeLessThan {
        bytes: i64,
    },
    And {
        prefix: Option<String>,
        tags: Vec<CanonicalTag>,
        object_size_greater_than: Option<i64>,
        object_size_less_than: Option<i64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct CanonicalTag {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CurrentExpiration {
    Date { utc_midnight: DateTime<Utc> },
    Days { days: u32 },
    ExpiredObjectDeleteMarker,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NoncurrentExpiration {
    pub noncurrent_days: u32,
    pub newer_noncurrent_versions: Option<u16>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuleIdentity {
    Id(String),
    Ordinal(u16),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleActionKind {
    ExpireCurrent,
    ExpireNoncurrent,
    DeleteExpiredMarker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionTargetIdentity {
    pub bucket: String,
    pub key: String,
    pub version_row_id: String,
    pub public_version_id: crate::store::object_version::PublicVersionId,
    pub kind: crate::store::object_version::VersionKind,
    pub object_id: Option<String>,
    pub sequence: i64,
}

#[derive(Clone, Debug)]
pub struct LifecycleCandidate {
    pub target: VersionTargetIdentity,
    pub is_latest: bool,
    pub size: i64,
    pub lifecycle_age_started_at: DateTime<Utc>,
    pub became_noncurrent_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct LifecycleCandidatePage {
    pub candidates: Vec<LifecycleCandidate>,
    pub next_cursor: Option<LifecycleScanCursor>,
    pub cycle_complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LifecycleScanSource {
    Current,
    Noncurrent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LifecycleScanCursor {
    pub source: LifecycleScanSource,
    pub bucket: String,
    pub key: String,
    pub sequence: i64,
    pub version_row_id: String,
}

#[derive(Clone, Debug)]
pub struct ClaimedLifecycleScan {
    pub bucket: String,
    pub config_revision: i64,
    pub canonical_json: String,
    pub cursor: Option<LifecycleScanCursor>,
    pub lease_epoch: i64,
    pub database_now: DateTime<Utc>,
    pub lease_until: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct NewLifecycleAction {
    pub idempotency_key: String,
    pub bucket: String,
    pub config_revision: i64,
    pub rule_identity: RuleIdentity,
    pub action_kind: LifecycleActionKind,
    pub target: VersionTargetIdentity,
    pub due_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ClaimedLifecycleAction {
    pub action: crate::store::entities::lifecycle_action::Model,
    pub claim_epoch: i64,
    pub worker_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuardedLifecycleExecutionResult {
    Applied(crate::store::object_version::DeleteVersionResult),
    AlreadySatisfied,
    Stale,
}
