use super::*;
use crate::store::{entities::pin_replica_observation::receipt, pinning::legacy_migration};

pub(super) async fn project(
    tx: &DatabaseTransaction,
    frame: &NativeFrame,
    remote: &mut RemoteSnapshot,
    now: chrono::DateTime<chrono::Utc>,
    work: &SharedDecodeBudget,
) -> Result<()> {
    let Some(route) = remote.route.original.as_ref() else {
        return Ok(());
    };
    if route.resource_type() != RemoteResourceType::ClusterPin {
        return Ok(());
    }
    let allocation = frame
        .table("remote_pins")
        .ok_or(SnapshotError::InvalidEvidence)?
        .rows
        .iter()
        .find(|row| {
            value(row, "epoch").and_then(|value| value.parse::<i64>().ok()) == Some(remote.epoch)
                && value(row, "cid")
                    .and_then(|cid| canonical(cid).ok())
                    .as_deref()
                    == Some(&remote.key.cid)
                && frame
                    .original_value(
                        "remote_pin_ledger",
                        &[
                            value(row, "provider").unwrap_or(""),
                            value(row, "cid").unwrap_or(""),
                        ],
                        "route",
                    )
                    .and_then(|raw| decode::<ProviderRouteSnapshot>(raw, work).ok())
                    .as_deref()
                    == Some(route)
        })
        .and_then(|row| value(row, "provider"));
    let Some(allocation) = allocation else {
        return Ok(());
    };
    let complete = receipt::fresh_complete(tx, allocation, &remote.key.cid, route, now)
        .await
        .map_err(|_| SnapshotError::InvalidEvidence)?;
    remote.observation_current = Some(complete);
    if !complete {
        return Ok(());
    }
    let mut rows = table(frame, "pin_replica_observations")?
        .iter()
        .filter(|row| {
            matches!(value(row, "projectable"), Some("1" | "true"))
                && value(row, "expected_cid")
                    .and_then(|cid| canonical(cid).ok())
                    .as_deref()
                    == Some(&remote.key.cid)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| {
        std::cmp::Reverse(
            value(row, "sequence")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(-1),
        )
    });
    for row in rows {
        let provider = value(row, "provider").ok_or(SnapshotError::InvalidEvidence)?;
        let key = [("sequence".into(), text(row, "sequence")?)].into();
        let point = legacy_migration::resolve_legacy_binding_with_work(
            tx,
            "pin_replica_observations",
            &key,
            provider,
            DecodeWork::shared(work),
        )
        .await
        .map_err(|error| {
            if error == crate::store::pinning::management::ManagementError::Budget {
                SnapshotError::LimitExceeded
            } else {
                SnapshotError::InvalidEvidence
            }
        })?;
        if provider != allocation && point.is_none_or(|point| point.operational_key != allocation) {
            continue;
        }
        // fresh_complete validated this exact latest scoped immutable receipt,
        // both original freshness limits, releases, epoch and request identity.
        let resources = decode::<Vec<crate::pinning::provider::ObservedResource>>(
            value(row, "resources").ok_or(SnapshotError::InvalidEvidence)?,
            work,
        )?;
        let [resource] = resources.as_slice() else {
            return Err(SnapshotError::InvalidEvidence);
        };
        let proof = resource
            .replica_evidence
            .as_ref()
            .ok_or(SnapshotError::InvalidEvidence)?;
        let Some(ConfirmationRequirement::ClusterReplicas { max_age_secs, .. }) =
            route.confirmation
        else {
            return Err(SnapshotError::InvalidEvidence);
        };
        if proof.started_at > now
            || now.signed_duration_since(proof.started_at)
                >= chrono::Duration::seconds(i64::from(max_age_secs))
        {
            remote.observation_current = Some(false);
            return Ok(());
        }
        remote.replica_started_at = Some(proof.started_at);
        remote.confirmed_replicas = Some(
            u32::try_from(proof.pinned_peers.len()).map_err(|_| SnapshotError::InvalidEvidence)?,
        );
        remote
            .confirmations
            .push(ConfirmationLevel::ReplicationComplete);
        return Ok(());
    }
    Err(SnapshotError::InvalidEvidence)
}
