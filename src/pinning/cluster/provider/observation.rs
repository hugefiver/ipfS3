use super::super::{ClusterResourceKey, ReplicaObservation, ReplicaOutcome};
use super::{ClusterProtocol, ClusterStrategy};
use crate::pinning::identity::Ownership;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterFailureCode {
    InvalidConfiguration,
    InvalidCid,
    Protocol,
    Transport,
    IdleTimeout,
    ControlTimeout,
    Cancelled,
    HttpStatus,
    RedirectRejected,
    CidMismatch,
    InvalidCar,
    DirectoryRequiresCar,
    SourceFailure,
    IncompleteUpload,
    ResourceLimit,
    InvalidReference,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Cluster write failed: {code:?} (HTTP {http_status:?})")]
pub struct ClusterWriteFailure {
    pub code: ClusterFailureCode,
    pub http_status: Option<u16>,
}
impl ClusterWriteFailure {
    pub(super) fn new(code: ClusterFailureCode) -> Self {
        Self {
            code,
            http_status: None,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterWriteEffect {
    NotSubmitted,
    Observed,
    Unknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterResourceStatus {
    DispatchIntent,
    Reported,
    PinAccepted,
    UnpinAccepted,
}

/// Historical resource/route footprint, including requested and unexpected CIDs.
/// No endpoint strings, credentials, object names or upstream free text are exposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterObservedResource {
    pub resource: ClusterResourceKey,
    pub protocol: ClusterProtocol,
    pub strategy: ClusterStrategy,
    pub status: ClusterResourceStatus,
    pub ownership: Ownership,
    pub reported_allocations: Option<std::collections::BTreeSet<String>>,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}
impl ClusterObservedResource {
    pub fn ledger_key(&self) -> crate::pinning::identity::CanonicalResourceKey {
        crate::pinning::identity::CanonicalResourceKey {
            backend: self.resource.pinset.backend.clone(),
            scope: self.resource.pinset.scope.clone(),
            resource_type: crate::pinning::identity::RemoteResourceType::ClusterPin,
            resource_id: self.resource.cid.clone(),
        }
    }
}
#[derive(Debug)]
pub struct ClusterWriteReceipt {
    pub resource: ClusterResourceKey,
    pub accepted_at: chrono::DateTime<chrono::Utc>,
}
#[derive(Debug)]
pub struct ClusterWriteObservation {
    pub result: Result<ClusterWriteReceipt, ClusterWriteFailure>,
    pub effect: ClusterWriteEffect,
    pub resources: Vec<ClusterObservedResource>,
    pub replicas: Option<ReplicaObservation>,
    pub http_status: Option<u16>,
}
impl ClusterWriteObservation {
    pub fn replication_outcome(&self) -> ReplicaOutcome {
        match (&self.result, &self.replicas) {
            (Ok(receipt), Some(proof))
                if proof
                    .evidence()
                    .is_some_and(|e| e.resource == receipt.resource) =>
            {
                proof.outcome()
            }
            _ => ReplicaOutcome::Unknown,
        }
    }
}
/// Parent ledger must authorize deletion of this exact historical pinset/route/CID.
#[derive(Debug, Clone)]
pub struct ClusterRemoteRef {
    pub resource: ClusterResourceKey,
    pub protocol: ClusterProtocol,
    pub strategy: ClusterStrategy,
}
