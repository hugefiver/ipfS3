use super::{ClusterResourceKey, RestOperation, VerificationCode, VerificationFailure};
use chrono::{DateTime, Utc};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaOutcome {
    Complete,
    Insufficient,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerReplicaState {
    Pinned,
    Remote,
    Missing,
    Error,
    Pending,
    Unpinned,
    Stale,
}

/// A revision binding, not a claimed monotonic consensus revision. v1.1.6 has no
/// read transaction/revision token. Compare both sides of the global status read.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationBinding {
    pub peers: BTreeSet<String>,
    pub created_at: DateTime<Utc>,
    pub replication_factor_min: i32,
    pub replication_factor_max: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaEvidence {
    pub resource: ClusterResourceKey,
    pub allocation: AllocationBinding,
    pub required_replicas: usize,
    pub pinned_peers: BTreeSet<String>,
    /// Only allocated Cluster peers; unallocated statuses never count.
    pub peer_states: BTreeMap<String, PeerReplicaState>,
    pub started_at: DateTime<Utc>,
    pub global_received_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

/// Immutable historical evidence with a live, monotonic freshness check. No
/// Deserialize or constructor accepting cached DTOs/timestamps can mint proof.
#[derive(Debug, Clone)]
pub struct ReplicaObservation {
    pub(super) evidence: Option<ReplicaEvidence>,
    pub(super) failure: Option<VerificationFailure>,
    pub(super) started: Instant,
    pub(super) max_age: Duration,
    pub(super) current: Arc<AtomicBool>,
}

impl ReplicaObservation {
    pub fn outcome(&self) -> ReplicaOutcome {
        if self.failure().is_some() {
            return ReplicaOutcome::Unknown;
        }
        match &self.evidence {
            Some(e) if e.pinned_peers.len() >= e.required_replicas => ReplicaOutcome::Complete,
            Some(_) => ReplicaOutcome::Insufficient,
            None => ReplicaOutcome::Unknown,
        }
    }

    /// Historical evidence remains readable after expiry; counts alone are NOT
    /// current confirmation. Call outcome() immediately at the integration gate.
    pub fn evidence(&self) -> Option<&ReplicaEvidence> {
        self.evidence.as_ref()
    }

    pub fn failure(&self) -> Option<VerificationFailure> {
        self.failure.or_else(|| {
            (!self.current.load(Ordering::Acquire) || self.started.elapsed() >= self.max_age).then(
                || {
                    VerificationFailure::new(
                        RestOperation::Verify,
                        VerificationCode::StaleObservation,
                    )
                },
            )
        })
    }
}
