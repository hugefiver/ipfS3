use super::{
    AllocationBinding, ClusterRestProfile, PeerReplicaState, ReplicaEvidence, ReplicaObservation,
    RestOperation, VerificationCode, VerificationFailure,
    dto::{GlobalPinInfo, PinAllocation, Version},
    peer,
    transport::Transport,
};
use chrono::{DateTime, Utc};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::time::Instant;

pub struct ClusterReplicaVerifier {
    transport: Transport,
    // Supersession only within this resource, not unrelated concurrent CIDs.
    // Weak entries do not keep observations alive or retain an unbounded CID cache.
    current: Mutex<BTreeMap<String, Weak<AtomicBool>>>,
}

impl ClusterReplicaVerifier {
    pub fn new(profile: ClusterRestProfile) -> Result<Self, VerificationFailure> {
        Ok(Self {
            transport: Transport::new(profile)?,
            current: Mutex::new(BTreeMap::new()),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_resolver(
        profile: ClusterRestProfile,
        resolver: Arc<dyn reqwest::dns::Resolve>,
    ) -> Result<Self, VerificationFailure> {
        Ok(Self {
            transport: Transport::with_resolver(profile, resolver)?,
            current: Mutex::new(BTreeMap::new()),
        })
    }

    /// Always issues fresh read-only REST queries. Never consumes Proxy pin/ls
    /// results, previous proofs or caller-supplied observations.
    pub async fn observe(&self, cid: &str) -> ReplicaObservation {
        let started = Instant::now();
        let started_at = Utc::now();
        let limits = self.transport.profile.limits;
        let current = Arc::new(AtomicBool::new(true));
        if let Ok(resource) = self.transport.profile.resource_key(cid) {
            let mut registry = self
                .current
                .lock()
                .expect("verification registry lock poisoned");
            registry.retain(|_, value| value.strong_count() > 0);
            if let Some(previous) = registry
                .insert(resource.cid, Arc::downgrade(&current))
                .and_then(|weak| weak.upgrade())
            {
                previous.store(false, Ordering::Release);
            }
        }
        let mut result = tokio::time::timeout(limits.total_timeout, self.read(cid, started_at))
            .await
            .unwrap_or_else(|_| {
                Err(failure(
                    RestOperation::Verify,
                    VerificationCode::TotalTimeout,
                ))
            });
        // Synchronous bounded JSON decoding/evaluation also belongs to the total
        // budget; it cannot rely on an async timer being polled during CPU work.
        if result.is_ok() && started.elapsed() >= limits.total_timeout {
            result = Err(failure(
                RestOperation::Verify,
                VerificationCode::TotalTimeout,
            ));
        }
        match result {
            Ok(evidence) => ReplicaObservation {
                evidence: Some(evidence),
                failure: None,
                started,
                max_age: limits.max_observation_age,
                current,
            },
            Err(error) => ReplicaObservation {
                evidence: None,
                failure: Some(error),
                started,
                max_age: limits.max_observation_age,
                current,
            },
        }
    }

    async fn read(
        &self,
        cid: &str,
        started_at: DateTime<Utc>,
    ) -> Result<ReplicaEvidence, VerificationFailure> {
        let resource = self.transport.profile.resource_key(cid)?;
        let version: Version = self
            .transport
            .get(RestOperation::Version, "/version", false)
            .await?;
        if !release_version(&version.version) {
            return Err(failure(
                RestOperation::Version,
                VerificationCode::UnsupportedVersion,
            ));
        }
        // Route only parsed CID text, never paths accepted as CID aliases.
        let route_cid = ::cid::Cid::try_from(cid)
            .map_err(|_| failure(RestOperation::Verify, VerificationCode::InvalidCid))?
            .to_string();
        let path = format!("/allocations/{route_cid}");
        let before: PinAllocation = self
            .transport
            .get(RestOperation::Allocations, &path, false)
            .await?;
        check_cid(&before.cid, &resource.cid, RestOperation::Allocations)?;
        let binding = allocation_binding(before)?;
        let global: GlobalPinInfo = self
            .transport
            .get(
                RestOperation::GlobalPins,
                &format!("/pins/{route_cid}"),
                true,
            )
            .await?;
        let global_received_at = Utc::now();
        check_cid(&global.cid, &resource.cid, RestOperation::GlobalPins)?;
        if peer::set(global.allocations, RestOperation::GlobalPins)? != binding.peers
            || global.created != binding.created_at
        {
            return Err(failure(
                RestOperation::GlobalPins,
                VerificationCode::AllocationChanged,
            ));
        }
        let after: PinAllocation = self
            .transport
            .get(RestOperation::Allocations, &path, false)
            .await?;
        check_cid(&after.cid, &resource.cid, RestOperation::Allocations)?;
        if allocation_binding(after)? != binding {
            return Err(failure(
                RestOperation::Allocations,
                VerificationCode::AllocationChanged,
            ));
        }
        let mut peer_states = BTreeMap::new();
        let mut pinned_peers = BTreeSet::new();
        let mut peer_map = BTreeMap::new();
        for (id, info) in global.peer_map {
            let id = peer::canonical(&id, RestOperation::GlobalPins)?;
            if peer_map.insert(id, info).is_some() {
                return Err(failure(
                    RestOperation::GlobalPins,
                    VerificationCode::MalformedResponse,
                ));
            }
        }
        for peer in &binding.peers {
            let state = match peer_map.get(peer) {
                None => PeerReplicaState::Missing,
                Some(info) if info.has_error => PeerReplicaState::Error,
                Some(info)
                    if info.timestamp < binding.created_at
                        || info.timestamp > global_received_at =>
                {
                    PeerReplicaState::Stale
                }
                Some(info) => match info.status.as_str() {
                    "pinned" => PeerReplicaState::Pinned,
                    "remote" | "sharded" => PeerReplicaState::Remote,
                    "cluster_error" | "pin_error" | "unpin_error" | "error" => {
                        PeerReplicaState::Error
                    }
                    "pinning" | "unpinning" | "pin_queued" | "unpin_queued" | "queued" => {
                        PeerReplicaState::Pending
                    }
                    "unpinned" | "unexpectedly_unpinned" => PeerReplicaState::Unpinned,
                    _ => {
                        return Err(failure(
                            RestOperation::GlobalPins,
                            VerificationCode::MalformedResponse,
                        ));
                    }
                },
            };
            if state == PeerReplicaState::Pinned {
                pinned_peers.insert(peer.clone());
            }
            peer_states.insert(peer.clone(), state);
        }
        Ok(ReplicaEvidence {
            resource,
            allocation: binding,
            required_replicas: self.transport.profile.min_replicas,
            pinned_peers,
            peer_states,
            started_at,
            global_received_at,
            finished_at: Utc::now(),
        })
    }
}

fn failure(operation: RestOperation, code: VerificationCode) -> VerificationFailure {
    VerificationFailure::new(operation, code)
}

fn check_cid(
    value: &str,
    canonical: &str,
    operation: RestOperation,
) -> Result<(), VerificationFailure> {
    let parsed = ::cid::Cid::try_from(value)
        .map_err(|_| failure(operation, VerificationCode::MalformedResponse))?;
    if ::cid::Cid::new_v1(parsed.codec(), *parsed.hash()).to_string() != canonical {
        return Err(failure(operation, VerificationCode::CidMismatch));
    }
    Ok(())
}

fn allocation_binding(pin: PinAllocation) -> Result<AllocationBinding, VerificationFailure> {
    if pin.timestamp.timestamp() <= 0 || pin.timestamp > Utc::now() {
        return Err(failure(
            RestOperation::Allocations,
            VerificationCode::MalformedResponse,
        ));
    }
    Ok(AllocationBinding {
        peers: peer::set(pin.allocations, RestOperation::Allocations)?,
        created_at: pin.timestamp,
        replication_factor_min: pin.replication_factor_min,
        replication_factor_max: pin.replication_factor_max,
    })
}

fn release_version(version: &str) -> bool {
    version == "1.1.6"
        || version.strip_prefix("1.1.6+").is_some_and(|meta| {
            !meta.is_empty()
                && meta.split('.').all(|part| {
                    !part.is_empty() && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                })
        })
}
