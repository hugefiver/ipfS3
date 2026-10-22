//! The retryable-park proof over the same complete resource-family originals.
use super::*;
use crate::store::{
    entities::pin_job,
    pinning::{jobs, ledger::submission::ResourceEvidence},
};

pub(super) fn retryable(
    rows: &read::Rows,
    job: &pin_job::Model,
    now: chrono::DateTime<chrono::Utc>,
    work: &SharedDecodeBudget,
) -> Result<bool> {
    if job.operation != "submit"
        || job.state != "running"
        || job.submit_phase.as_deref() != Some("recovering")
        || job.locked_until.is_some()
        || job.expected_remote_epoch.is_some()
    {
        return Ok(false);
    }
    let Some(target) = rows
        .targets
        .iter()
        .filter(|target| {
            target.provider == job.provider
                && target.cid == job.cid
                && matches!(
                    target.state.as_str(),
                    "waiting" | "submitted" | "pinned" | "degraded"
                )
                && rows
                    .leases
                    .iter()
                    .any(|lease| lease.id == target.lease_id && lease.state == "active")
        })
        .min_by(|a, b| (&a.created_at, &a.id).cmp(&(&b.created_at, &b.id)))
    else {
        return Ok(false);
    };
    let Some(lease) = rows.leases.iter().find(|lease| lease.id == target.lease_id) else {
        return Ok(false);
    };
    if job.target_id.as_ref() != Some(&target.id)
        || job.lease_id.as_ref() != Some(&lease.id)
        || job.expected_generation != Some(lease.generation)
        || lease.expires_at <= now
    {
        return Ok(false);
    }
    let Some(history) = rows.submit_history.iter().find(|row| row.job_id == job.id) else {
        return Ok(false);
    };
    let Some(invocation) = rows.invocations.iter().find(|row| row.job_id == job.id) else {
        return Ok(false);
    };
    let route = decode::<ProviderRouteSnapshot>(&invocation.route, work)?;
    let Some((api, strategy)) = crate::pinning::worker::invocation_route_for_snapshot(&route)
    else {
        return Ok(false);
    };
    if history.effect != "not_created"
        || !matches!(history.state.as_str(), "blocked" | "needs_attention")
        || history.api != api
        || history.strategy != strategy
        || history.submit_calls <= 0
        || history.submit_calls >= jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS
    {
        return Ok(false);
    }
    let Some(remote) = rows
        .remotes
        .iter()
        .find(|row| row.provider == job.provider && row.cid == job.cid)
    else {
        return Ok(false);
    };
    let Some(ledger) = rows
        .ledgers
        .iter()
        .find(|row| row.provider == job.provider && row.cid == job.cid)
    else {
        return Ok(false);
    };
    if remote.status != "reserved"
        || remote.request_id.is_some()
        || remote.epoch != invocation.remote_epoch
        || ledger.effect != "not_created"
        || ledger
            .route
            .as_deref()
            .map(|raw| decode::<ProviderRouteSnapshot>(raw, work))
            .transpose()?
            .as_deref()
            != Some(route.as_ref())
        || rows.history.iter().any(|row| {
            row.provider == job.provider
                && row.cid == job.cid
                && row.epoch >= invocation.remote_epoch
        })
    {
        return Ok(false);
    }
    if let Some(receipt) = rows
        .submissions
        .iter()
        .filter(|row| row.job_id == job.id)
        .max_by_key(|row| row.submit_call)
    {
        if receipt.needs_attention
            || receipt.outcome != "not_submitted"
            || receipt.effect != "not_created"
            || receipt.submit_call != history.submit_calls
            || receipt.remote_epoch != invocation.remote_epoch
            || receipt.observed_at.is_none()
            || !decode::<Vec<ResourceEvidence>>(&receipt.resources, work)?.is_empty()
            || receipt.expected_cid != job.cid
            || receipt.provider != job.provider
            || decode::<ProviderRouteSnapshot>(&receipt.route, work)?.as_ref() != route.as_ref()
        {
            return Ok(false);
        }
    } else if crate::store::pinning::ledger::submission::is_rpc(&route) {
        return Ok(false);
    }
    Ok(!rows.jobs.iter().any(|other| {
        other.id != job.id
            && other.provider == job.provider
            && other.cid == job.cid
            && matches!(other.state.as_str(), "pending" | "running")
            && (matches!(other.operation.as_str(), "submit" | "unpin")
                || other.locked_until.is_some())
    }))
}
