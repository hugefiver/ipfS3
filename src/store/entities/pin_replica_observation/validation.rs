use crate::pinning::{
    identity::{ConfirmationRequirement, ProviderRouteSnapshot, RemoteResourceType},
    provider::{
        ObservedResource, ObservedResourceStatus, PinObservation, RemotePinStatus,
        canonical_resource_cid,
    },
};
use chrono::{DateTime, Utc};

pub fn requirement(route: &ProviderRouteSnapshot) -> Option<(usize, i64)> {
    match route.confirmation {
        Some(ConfirmationRequirement::ClusterReplicas {
            min_replicas,
            max_age_secs,
        }) if min_replicas > 0 && max_age_secs > 0 => {
            Some((min_replicas as usize, max_age_secs as i64))
        }
        _ => None,
    }
}

pub fn evidence_valid(
    resource: &ObservedResource,
    route: &ProviderRouteSnapshot,
    expected: &str,
    now: DateTime<Utc>,
) -> bool {
    let Some((_, age)) = requirement(route) else {
        return false;
    };
    let Some(e) = resource.replica_evidence.as_ref() else {
        return false;
    };
    let Some(cid) = canonical_resource_cid(expected).ok() else {
        return false;
    };
    if resource.resource_type != RemoteResourceType::ClusterPin
        || canonical_resource_cid(&resource.cid).ok().as_ref() != Some(&cid)
        || e.resource.cid != cid
        || e.resource.pinset.backend != route.backend
        || e.resource.pinset.scope != route.scope
        || e.required_replicas == 0
        || e.peer_states
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            != e.allocation.peers
        || e.pinned_peers
            != e.peer_states
                .iter()
                .filter(|(_, state)| **state == crate::pinning::cluster::PeerReplicaState::Pinned)
                .map(|(peer, _)| peer.clone())
                .collect()
        || e.allocation.created_at > e.started_at
        || e.started_at > e.global_received_at
        || e.global_received_at > e.finished_at
        || e.finished_at > now
        || now.signed_duration_since(e.started_at).num_milliseconds() >= age * 1000
    {
        return false;
    }
    let rf = e.allocation.replication_factor_min;
    let rf_max = e.allocation.replication_factor_max;
    // RF is allocation history, not this reader's confirmation threshold.
    (rf == -1 && rf_max == -1) || (rf > 0 && rf_max >= rf)
}

pub fn outcome(
    observation: &PinObservation,
    route: &ProviderRouteSnapshot,
    expected: &str,
    request: &str,
    now: DateTime<Utc>,
) -> &'static str {
    let [resource] = observation.resources.as_slice() else {
        return "Unknown";
    };
    if resource.request_id != request || !evidence_valid(resource, route, expected, now) {
        return "Unknown";
    }
    let e = resource.replica_evidence.as_ref().expect("validated");
    let min = requirement(route)
        .expect("validated")
        .0
        .max(e.required_replicas);
    if e.pinned_peers.len() < min {
        return "Insufficient";
    }
    if resource.status == ObservedResourceStatus::ReplicasVerified
        && observation.result.as_ref().is_ok_and(|remote| {
            remote.status == RemotePinStatus::Pinned
                && remote.request_id == request
                && canonical_resource_cid(&remote.cid).ok() == canonical_resource_cid(expected).ok()
        })
    {
        "Complete"
    } else {
        "Unknown"
    }
}

/// The Cluster RF and this lease's birth min are separate from the min used by
/// the observing adapter. A stricter sibling lease cannot lower or raise ours.
pub fn birth_threshold(
    resource: &ObservedResource,
    route: &ProviderRouteSnapshot,
) -> Option<usize> {
    let min = requirement(route)?.0;
    resource.replica_evidence.as_ref()?;
    Some(min)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pinning::{
        cluster::{
            AllocationBinding, ClusterPinset, ClusterResourceKey, PeerReplicaState, ReplicaEvidence,
        },
        identity::{CleanupMode, Ownership},
    };

    fn sample(
        min: u32,
        rf: i32,
        rf_max: i32,
    ) -> (ObservedResource, ProviderRouteSnapshot, DateTime<Utc>) {
        let now = Utc::now();
        let cid = canonical_resource_cid("QmYwAPJzv5CZsnAzt8auVZRnGiRAz1F7TaqzM9zECsFcdX").unwrap();
        let route = ProviderRouteSnapshot {
            provider_id: "provider".into(),
            backend: "cluster".into(),
            scope: "pinset".into(),
            storage_domain: "backup".into(),
            credential_revision: 1,
            endpoint_revision: 1,
            secret_ref: None,
            api_profile: "cluster-rest".into(),
            strategy: "cid".into(),
            cleanup: CleanupMode::Retain,
            confirmation: Some(ConfirmationRequirement::ClusterReplicas {
                min_replicas: min,
                max_age_secs: 2,
            }),
        };
        let resource = ObservedResource {
            resource_type: RemoteResourceType::ClusterPin,
            cid: cid.clone(),
            request_id: cid.clone(),
            status: ObservedResourceStatus::ReplicasVerified,
            ownership: Ownership::Unknown,
            replica_evidence: Some(ReplicaEvidence {
                resource: ClusterResourceKey {
                    pinset: ClusterPinset {
                        backend: "cluster".into(),
                        scope: "pinset".into(),
                    },
                    cid,
                },
                allocation: AllocationBinding {
                    peers: ["a".into(), "b".into()].into(),
                    created_at: now - chrono::Duration::seconds(1),
                    replication_factor_min: rf,
                    replication_factor_max: rf_max,
                },
                required_replicas: min as usize,
                pinned_peers: ["a".into(), "b".into()].into(),
                peer_states: [
                    ("a".into(), PeerReplicaState::Pinned),
                    ("b".into(), PeerReplicaState::Pinned),
                ]
                .into(),
                started_at: now,
                global_received_at: now,
                finished_at: now,
            }),
        };
        (resource, route, now)
    }

    #[test]
    fn valid_rf_does_not_raise_or_lower_birth_min() {
        for (min, rf) in [(1, 2), (3, 2), (1, -1), (3, -1)] {
            let (resource, route, now) = sample(min, rf, rf);
            assert!(evidence_valid(&resource, &route, &resource.cid, now));
            assert_eq!(birth_threshold(&resource, &route), Some(min as usize));
        }
    }

    #[test]
    fn unknown_or_malformed_rf_and_zero_reader_min_stay_invalid() {
        for (rf, max) in [(0, 0), (-2, -2), (-1, 2), (2, -1), (3, 2)] {
            let (resource, route, now) = sample(1, rf, max);
            assert!(
                !evidence_valid(&resource, &route, &resource.cid, now),
                "RF={rf}/{max}"
            );
        }
        let (mut resource, route, now) = sample(1, 2, 2);
        resource
            .replica_evidence
            .as_mut()
            .unwrap()
            .required_replicas = 0;
        assert!(!evidence_valid(&resource, &route, &resource.cid, now));
    }

    #[test]
    fn independent_min_preserves_freshness_cid_pinset_and_peer_state_checks() {
        let (resource, route, now) = sample(1, 2, 2);
        assert!(!evidence_valid(
            &resource,
            &route,
            &resource.cid,
            now + chrono::Duration::seconds(2)
        ));
        assert!(!evidence_valid(&resource, &route, "not-a-cid", now));
        for corruption in ["pinset", "peers", "pinned", "time", "cid"] {
            let mut resource = resource.clone();
            let evidence = resource.replica_evidence.as_mut().unwrap();
            match corruption {
                "pinset" => evidence.resource.pinset.scope = "other".into(),
                "peers" => {
                    evidence.peer_states.remove("b");
                }
                "pinned" => {
                    evidence.pinned_peers.remove("b");
                }
                "time" => evidence.finished_at = now + chrono::Duration::seconds(1),
                _ => evidence.resource.cid = "other".into(),
            }
            assert!(
                !evidence_valid(&resource, &route, &resource.cid, now),
                "{corruption}"
            );
        }
    }
}
