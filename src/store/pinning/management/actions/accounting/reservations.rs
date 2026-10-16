use super::*;
use crate::pinning::{
    management::{Action, Capture},
    provider::canonical_resource_cid,
};

/// No charge is recalculated. Directory sharing requires a persisted compatible
/// management birth/migration receipt in this same unreleased resource lifetime.
/// Historical ordinary file reservations retain their existing numeric check;
/// a proved directory basis can never be borrowed by FileCleanEof or vice versa.
pub(crate) fn check_rows_with_work(
    frame: records::FrozenRows<'_>,
    provider: &str,
    cid: &str,
    expected: Count,
    work: records::SharedDecodeBudget,
) -> Result<()> {
    let context = records::DecodeWork::shared(&work);
    context.copy(cid)?;
    let cid = canonical_resource_cid(cid).map_err(|_| ManagementError::InvalidEvidence)?;
    let remotes = frame
        .table("remote_pins")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
        .filter(|row| {
            native::value(row, "provider") == Some(provider)
                && native::value(row, "cid")
                    .and_then(|value| canonical_resource_cid(value).ok())
                    .as_deref()
                    == Some(&cid)
        })
        .collect::<Vec<_>>();
    if remotes.is_empty() {
        return Ok(());
    }
    if remotes.len() != 1 {
        return Err(ManagementError::InvalidEvidence);
    }
    let remote = remotes[0];
    let epoch = native::value(remote, "epoch")
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(ManagementError::InvalidEvidence)?;
    if native::value(remote, "cid_size").and_then(|value| value.parse::<u64>().ok())
        != Some(expected.bytes)
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let targets = frame
        .table("pin_lease_targets")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
        .filter(|row| {
            native::value(row, "provider") == Some(provider)
                && native::value(row, "cid")
                    .and_then(|value| canonical_resource_cid(value).ok())
                    .as_deref()
                    == Some(&cid)
        })
        .collect::<Vec<_>>();
    context.with(|work| work.nodes(1 + targets.len()))?;
    let leases = targets
        .iter()
        .filter_map(|row| native::value(row, "lease_id"))
        .collect::<Vec<_>>();
    let actions = records::frozen_actions_for_leases(frame, &leases, context)?;
    let mut proved = false;
    for target in &targets {
        let id = native::value(target, "id").ok_or(ManagementError::InvalidEvidence)?;
        let lease = native::value(target, "lease_id").ok_or(ManagementError::InvalidEvidence)?;
        let mut target_proved = false;
        for trusted in &actions {
            let binding = trusted.binding();
            let action = trusted.action_view();
            if !action.targets.iter().any(|target| target == id)
                || !action
                    .receipt
                    .intent_ids
                    .iter()
                    .any(|intent| intent == lease)
            {
                continue;
            }
            let Some(Capture::Object(capture)) = &action.receipt.binding.capture else {
                continue;
            };
            if capture.object.identity.cid != cid {
                continue;
            }
            let Some(summary) = &capture.object.read_evidence else {
                continue;
            };
            let count = count(summary)?;
            let before = binding.frozen_rows()?;
            binding.with_decode_work(|work| trusted_proof_with_work(before, summary, work))?;
            let mut original_provider = provider.to_owned();
            let origin = match &action.receipt.binding.request.action {
                Action::Create(_) | Action::Backfill(_) | Action::Reapply(_) => {
                    let decision = action
                        .decision
                        .as_ref()
                        .ok_or(ManagementError::InvalidEvidence)?;
                    if decision.id != lease
                        || decision.owner != capture.owner_identity
                        || decision.stored_bytes != count.bytes
                    {
                        continue;
                    }
                    let mut origin = None;
                    for job in &action.jobs {
                        if !(job.target_id.as_deref() == Some(id)
                            || job.directive == records::JobDirective::ReadOnlyObserve
                                && job.lease_id.as_deref() == Some(lease))
                            || canonical_resource_cid(&job.cid).ok().as_deref() != Some(&cid)
                        {
                            continue;
                        }
                        trusted.validate_job(job)?;
                        if !same_allocation(
                            frame,
                            id,
                            provider,
                            &job.provider,
                            &job.cid,
                            &job.route,
                            context,
                        )? {
                            continue;
                        }
                        original_provider = job.provider.clone();
                        origin = Some(job.remote_epoch);
                        break;
                    }
                    if origin.is_some() {
                        origin
                    } else {
                        // A compatible already-confirmed resource may need no
                        // new job. Its original captured physical lifetime is
                        // retained in the full trusted before image, not guessed
                        // from current counters or a caller's applied claims.
                        before
                            .table("remote_pins")
                            .ok_or(ManagementError::Schema)?
                            .rows
                            .iter()
                            .find(|row| {
                                native::value(row, "provider") == Some(provider)
                                    && native::value(row, "cid")
                                        .and_then(|cid| canonical_resource_cid(cid).ok())
                                        .as_deref()
                                        == Some(&cid)
                            })
                            .and_then(|row| {
                                native::value(row, "epoch").and_then(|epoch| epoch.parse().ok())
                            })
                    }
                }
                Action::Migrate { .. } => {
                    let mut origin = None;
                    for point in &action.migration_points {
                        if point.destination_target_id != id || point.cid != cid {
                            continue;
                        }
                        trusted.validate_point(point)?;
                        if !same_allocation(
                            frame,
                            id,
                            provider,
                            &point.destination_allocation,
                            &point.destination_resource_key.values[1],
                            &point.destination_route,
                            context,
                        )? {
                            continue;
                        }
                        original_provider = point.destination_allocation.clone();
                        origin = Some(point.destination_epoch);
                        break;
                    }
                    origin
                }
                _ => None,
            };
            let Some(origin) = origin else { continue };
            if origin > epoch || released(frame, provider, &original_provider, &cid, origin, epoch)?
            {
                continue;
            }
            if count != expected {
                return Err(ManagementError::InvalidEvidence);
            }
            if native::value(target, "logical_size").and_then(|value| value.parse::<u64>().ok())
                != Some(count.bytes)
            {
                return Err(ManagementError::InvalidEvidence);
            }
            target_proved = true;
            proved = true;
        }
        if expected.basis == Basis::DirectoryDagBlocksV1 && !target_proved {
            return Err(ManagementError::InvalidEvidence);
        }
    }
    if expected.basis == Basis::DirectoryDagBlocksV1 && !proved {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}

fn same_allocation(
    frame: records::FrozenRows<'_>,
    target: &str,
    current: &str,
    original: &str,
    cid: &str,
    route: &crate::pinning::identity::ProviderRouteSnapshot,
    work: records::DecodeWork<'_>,
) -> Result<bool> {
    if current == original {
        return Ok(true);
    }
    work.copy(&[("id", target), ("provider", original), ("cid", cid)])?;
    let target = [("id".into(), target.to_owned())].into();
    let resource = [
        ("provider".into(), original.to_owned()),
        ("cid".into(), cid.to_owned()),
    ]
    .into();
    let target = work.with(|work| {
        super::super::points::movement_in_rows_with_work(
            frame,
            "pin_lease_targets",
            &target,
            original,
            route,
            work,
        )
    })?;
    let resource = work.with(|work| {
        super::super::points::movement_in_rows_with_work(
            frame,
            "remote_pins",
            &resource,
            original,
            route,
            work,
        )
    })?;
    Ok(target.is_some_and(|point| point.allocation == current)
        && resource.is_some_and(|point| point.allocation == current))
}

fn released(
    frame: records::FrozenRows<'_>,
    provider: &str,
    original: &str,
    cid: &str,
    first: i64,
    last: i64,
) -> Result<bool> {
    for row in frame
        .table("pin_resource_history")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
    {
        if ![Some(provider), Some(original)].contains(&native::value(row, "provider"))
            || native::value(row, "cid")
                .and_then(|value| canonical_resource_cid(value).ok())
                .as_deref()
                != Some(cid)
        {
            continue;
        }
        let epoch = native::value(row, "epoch")
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or(ManagementError::InvalidEvidence)?;
        if epoch < first || epoch > last {
            continue;
        }
        // Same boundary as the existing native release_history consumer. A saved
        // lifetime is not a grant to reuse its basis for a replacement resource.
        return Ok(true);
    }
    Ok(false)
}
