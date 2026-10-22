//! Replica freshness evaluated only over the captured resource-family originals.
use super::*;
use crate::{
    pinning::provider::ObservedResource,
    store::entities::pin_lease_requirement::contract::read_route_compatible,
};

pub(super) fn project(
    frame: &NativeFrame,
    remote: &mut RemoteSnapshot,
    now: chrono::DateTime<chrono::Utc>,
    work: &SharedDecodeBudget,
) -> Result<()> {
    let Some(route) = remote
        .route
        .original
        .as_ref()
        .filter(|route| route.resource_type() == RemoteResourceType::ClusterPin)
    else {
        return Ok(());
    };
    remote.observation_current = Some(false);
    let allocation = table(frame, "remote_pins")?
        .iter()
        .find(|row| {
            value(row, "epoch").and_then(|epoch| epoch.parse::<i64>().ok()) == Some(remote.epoch)
                && value(row, "cid")
                    .and_then(|cid| canonical(cid).ok())
                    .as_deref()
                    == Some(&remote.key.cid)
                && value(row, "provider").is_some_and(|provider| {
                    frame
                        .original_value(
                            "remote_pin_ledger",
                            &[provider, value(row, "cid").unwrap_or("")],
                            "route",
                        )
                        .and_then(|raw| decode::<ProviderRouteSnapshot>(raw, work).ok())
                        .as_deref()
                        == Some(route)
                })
        })
        .and_then(|row| value(row, "provider"));
    let Some(allocation) = allocation else {
        return Ok(());
    };
    let Some(registered) = frame.row("pin_provider_routes", &[allocation]) else {
        return Ok(());
    };
    let registered =
        super::super::read::original_row(frame, "pin_provider_routes", registered, work)?;
    let registered = decode::<ProviderRouteSnapshot>(
        value(&registered, "snapshot").ok_or(SnapshotError::InvalidEvidence)?,
        work,
    )?;
    if !read_route_compatible(route, &registered) {
        return Ok(());
    }
    let mut candidates = table(frame, "pin_replica_observations")?
        .iter()
        .filter(|row| {
            matches!(value(row, "projectable"), Some("1" | "true"))
                && value(row, "expected_cid")
                    .and_then(|cid| canonical(cid).ok())
                    .as_deref()
                    == Some(&remote.key.cid)
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|row| {
        std::cmp::Reverse(
            value(row, "sequence")
                .and_then(|raw| raw.parse::<i64>().ok())
                .unwrap_or(-1),
        )
    });
    for row in candidates {
        let original =
            super::super::read::original_row(frame, "pin_replica_observations", row, work)?;
        let row = &original;
        let provider = value(row, "provider").ok_or(SnapshotError::InvalidEvidence)?;
        if provider != allocation {
            let key = [("sequence".into(), text(row, "sequence")?)].into();
            if super::super::legacy::resolve(
                frame,
                "pin_replica_observations",
                &key,
                provider,
                work,
            )?
            .is_none_or(|point| point.operational_key != allocation)
            {
                continue;
            }
        }
        // Only the latest scoped, projectable receipt participates. A newer
        // invalid observation must not revive an older successful receipt.
        let recorded = decode::<ProviderRouteSnapshot>(
            value(row, "route").ok_or(SnapshotError::InvalidEvidence)?,
            work,
        )?;
        let epoch = super::super::read::field::<i64>(row, "remote_epoch")?;
        if value(row, "phase") != Some("poll")
            || !matches!(value(row, "outcome"), Some("Complete" | "Insufficient"))
            || !read_route_compatible(route, &recorded)
            || table(frame, "pin_resource_history")?.iter().any(|history| {
                value(history, "provider") == Some(allocation)
                    && value(history, "cid") == Some(&remote.key.cid)
                    && value(history, "epoch")
                        .and_then(|raw| raw.parse::<i64>().ok())
                        .is_some_and(|release| release >= epoch)
            })
        {
            return Ok(());
        }
        let current = frame
            .row("remote_pins", &[allocation, &remote.key.cid])
            .ok_or(SnapshotError::InvalidEvidence)?;
        if super::super::read::field::<i64>(current, "epoch")? < epoch
            || !matches!(value(current, "status"), Some("pinned" | "pinning"))
        {
            return Ok(());
        }
        let resources = decode::<Vec<ObservedResource>>(
            value(row, "resources").ok_or(SnapshotError::InvalidEvidence)?,
            work,
        )?;
        let [resource] = resources.as_slice() else {
            return Ok(());
        };
        if value(current, "request_id") != Some(&resource.request_id)
            || !valid(resource, route, &remote.key.cid, now)
            || !valid(resource, &recorded, &remote.key.cid, now)
        {
            return Ok(());
        }
        let proof = resource
            .replica_evidence
            .as_ref()
            .ok_or(SnapshotError::InvalidEvidence)?;
        let Some(ConfirmationRequirement::ClusterReplicas { min_replicas, .. }) =
            route.confirmation
        else {
            return Ok(());
        };
        if proof.pinned_peers.len() < min_replicas as usize {
            return Ok(());
        }
        remote.observation_current = Some(true);
        remote.replica_started_at = Some(proof.started_at);
        remote.confirmed_replicas = Some(
            u32::try_from(proof.pinned_peers.len()).map_err(|_| SnapshotError::InvalidEvidence)?,
        );
        remote
            .confirmations
            .push(ConfirmationLevel::ReplicationComplete);
        return Ok(());
    }
    Ok(())
}

fn valid(
    resource: &ObservedResource,
    route: &ProviderRouteSnapshot,
    cid: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let Some(ConfirmationRequirement::ClusterReplicas {
        min_replicas,
        max_age_secs,
    }) = route.confirmation.as_ref()
    else {
        return false;
    };
    if *min_replicas == 0 || *max_age_secs == 0 {
        return false;
    }
    let Some(proof) = &resource.replica_evidence else {
        return false;
    };
    resource.resource_type == RemoteResourceType::ClusterPin
        && canonical(&resource.cid).ok().as_deref() == Some(cid)
        && proof.resource.cid == cid
        && proof.resource.pinset.backend == route.backend
        && proof.resource.pinset.scope == route.scope
        && proof.required_replicas > 0
        && proof.peer_states.keys().cloned().collect::<BTreeSet<_>>() == proof.allocation.peers
        && proof.pinned_peers
            == proof
                .peer_states
                .iter()
                .filter(|(_, state)| **state == crate::pinning::cluster::PeerReplicaState::Pinned)
                .map(|(peer, _)| peer.clone())
                .collect()
        && proof.allocation.created_at <= proof.started_at
        && proof.started_at <= proof.global_received_at
        && proof.global_received_at <= proof.finished_at
        && proof.finished_at <= now
        && now
            .signed_duration_since(proof.started_at)
            .num_milliseconds()
            < i64::from(*max_age_secs) * 1000
        && ((proof.allocation.replication_factor_min == -1
            && proof.allocation.replication_factor_max == -1)
            || (proof.allocation.replication_factor_min > 0
                && proof.allocation.replication_factor_max
                    >= proof.allocation.replication_factor_min))
}
