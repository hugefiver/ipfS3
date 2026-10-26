//! A live successor is derived from native invocation/submission/desired-target
//! evidence. The stored seed, its route and original epoch are never rewritten.
use super::*;
use crate::store::pinning::jobs;

/// A receipt candidate is not a parent grant. Require the actual original
/// Submit plus both immutable invocation locators and the native request-bound
/// Poll. Live lease/resource/release fences remain in `current_resource`.
#[cfg(test)]
pub(crate) async fn original_submit_for_poll<C: ConnectionTrait>(
    db: &C,
    poll: &pin_job::Model,
    seed: &records::JobAuthority,
    invocation: &pin_invocation_route::Model,
    view: records::BindingView<'_>,
) -> Result<bool> {
    original_submit_for_poll_catalog(ReceiptCatalog::full(db), poll, seed, invocation, view).await
}

pub(super) async fn original_submit_for_poll_catalog(
    catalog: ReceiptCatalog<'_>,
    poll: &pin_job::Model,
    seed: &records::JobAuthority,
    invocation: &pin_invocation_route::Model,
    view: records::BindingView<'_>,
) -> Result<bool> {
    let db = &catalog;
    let work = view.decode_work();
    let Some(original) =
        records::current_model::<pin_job::Entity, _>(db, &[&seed.job_id], work).await?
    else {
        return Ok(false);
    };
    if original.operation != "submit" {
        return Ok(false);
    }
    validate_job_resolved_catalog(catalog, &original, seed, view.decode_work()).await?;
    let current = records::current_model::<pin_job::Entity, _>(db, &[&poll.id], work)
        .await?
        .ok_or(ManagementError::Confirmation)?;
    validate_job_resolved_catalog(catalog, &current, seed, view.decode_work()).await?;
    let source =
        records::current_model::<pin_invocation_route::Entity, _>(db, &[&original.id], work)
            .await?
            .ok_or(ManagementError::Confirmation)?;
    if source.remote_epoch < seed.remote_epoch
        || source.remote_epoch > invocation.remote_epoch
        || view.decode_route(&source.route)?.as_ref() != &seed.route
    {
        return Err(ManagementError::Confirmation);
    }
    let Some(history) =
        records::current_model::<jobs::history::Entity, _>(db, &[&seed.job_id], work).await?
    else {
        return Ok(false);
    };
    if history.submit_calls <= 0 || !history_route_matches(&history, &seed.route) {
        return Ok(false);
    }
    let provider = operational_resource_catalog(catalog, seed, view.decode_work()).await?;
    let Some(remote) =
        records::current_model::<remote_pin::Entity, _>(db, &[&provider, &seed.cid], work).await?
    else {
        return Ok(false);
    };
    let Some(request) = remote.request_id.as_deref() else {
        return Ok(false);
    };
    jobs::poll_names_request_catalog(catalog, poll, request, view.decode_work()).await
}

pub(super) async fn current_resource_catalog(
    catalog: ReceiptCatalog<'_>,
    job: &pin_job::Model,
    authority: &records::JobAuthority,
    work: records::DecodeWork<'_>,
) -> Result<bool> {
    let db = &catalog;
    let provider = operational_resource_catalog(catalog, authority, work).await?;
    let Some(remote) =
        records::current_model::<remote_pin::Entity, _>(db, &[&provider, &authority.cid], work)
            .await?
    else {
        return Ok(false);
    };
    let Some(invocation) =
        records::current_model::<pin_invocation_route::Entity, _>(db, &[&job.id], work).await?
    else {
        return Ok(false);
    };
    let row = records::current_model::<crate::store::entities::remote_pin_ledger::Entity, _>(
        db,
        &[&provider, &authority.cid],
        work,
    )
    .await?;
    let Some(raw_route) = row.as_ref().and_then(|row| row.route.as_deref()) else {
        return Ok(false);
    };
    let ledger_route: ProviderRouteSnapshot = work.with(|work| work.typed(raw_route))?;
    let invocation_route: ProviderRouteSnapshot =
        work.with(|work| work.typed(&invocation.route))?;
    if remote.epoch < authority.remote_epoch
        || invocation.remote_epoch < authority.remote_epoch
        || invocation.remote_epoch > remote.epoch
        || ledger_route != authority.route
        || invocation_route != authority.route
        || authority.resource_id.is_some() && authority.resource_id != remote.request_id
    {
        return Ok(false);
    }
    if authority.directive != records::JobDirective::Submit {
        return Ok(remote.epoch == authority.remote_epoch
            && invocation.remote_epoch == authority.remote_epoch);
    }
    let releases = crate::store::pinning::legacy_migration::release_history_catalog(
        catalog,
        &provider,
        &authority.cid,
        work,
    )
    .await?;
    if releases
        .iter()
        .any(|release| release.epoch >= authority.remote_epoch && release.epoch <= remote.epoch)
    {
        return Ok(false);
    }
    if !jobs::check_target_job_generation_with_work(db, job, work).await? {
        return Ok(false);
    }
    if job.operation == "poll" {
        // This exact durable Poll ID binds the original request, target and
        // generation; current namespace/epoch alone is not continuity evidence.
        let Some(request) = remote.request_id.as_deref() else {
            return Ok(false);
        };
        return jobs::poll_continuity_catalog(catalog, job, request, work).await;
    }
    let current = records::current_model::<pin_job::Entity, _>(db, &[&job.id], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    let history = records::current_model::<jobs::history::Entity, _>(db, &[&job.id], work).await?;
    if crate::pinning::worker::invocation_route_for_snapshot(&authority.route).is_none()
        || history.as_ref().is_some_and(|history| {
            !history_route_matches(history, &authority.route)
                || history.state != "active"
                || history.submit_calls <= 0
        })
    {
        return Ok(false);
    }
    if current.submit_phase.as_deref() == Some("recovering") {
        return Ok(false);
    }
    if current.submit_phase.as_deref() == Some("recovery_backoff") {
        let Some(history) = history else {
            return Ok(false);
        };
        let Some(claim) = job.locked_until else {
            return Ok(false);
        };
        let now = crate::store::database_clock::database_now(db)
            .await
            .map_err(database)?;
        if job.operation != "submit"
            || current.operation != "submit"
            || current.provider != job.provider
            || current.cid != job.cid
            || current.lease_id != job.lease_id
            || current.target_id != job.target_id
            || current.expected_generation != job.expected_generation
            || current.lease_id != authority.lease_id
            || current.target_id != authority.target_id
            || current.expected_generation != authority.lease_generation
            || current.expected_remote_epoch != job.expected_remote_epoch
            || current.state != "running"
            || authority.resource_id.is_some()
            || remote.status != "reserved"
            || remote.request_id.is_some()
            || row.as_ref().is_none_or(|row| row.effect != "not_created")
            || history.effect != "not_created"
            || history.submit_calls >= jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS
            || now.signed_duration_since(history.started_at) >= chrono::Duration::hours(1)
        {
            return Ok(false);
        }
        // Compare the caller's current token with native DB timestamp precision;
        // the old dispatch token is independently cross-proven by its two facts.
        for _ in 0..2 {
            work.copy(&(&job.id, claim, now))?;
        }
        if records::bounded_projection(
            db,
            "FROM pin_jobs WHERE id=$1 AND state='running' AND locked_until=$2 AND locked_until>$3",
            &[("id", "id")],
            "id",
            vec![job.id.clone().into(), claim.into(), now.into()],
            (1, native::MAX_BYTES),
            work,
        )
        .await?
        .0
        .is_empty()
        {
            return Ok(false);
        }
        return ledger::submission::rejected_non_rpc_call_with_work(
            db,
            &current,
            &invocation,
            history.submit_calls,
            work,
        )
        .await;
    }
    if remote.epoch == authority.remote_epoch && invocation.remote_epoch == authority.remote_epoch {
        return Ok(true);
    }
    if remote.status != "reserved" || remote.request_id.is_some() {
        return Ok(false);
    }
    // Before the existing native epoch CAS, only an exact fresh ready source or
    // its durable not-created submission proof may proceed. No bare missing
    // invocation/history is interpreted as evidence that a POST never occurred.
    if invocation.remote_epoch != remote.epoch {
        return Ok(match history {
            None => {
                current.submit_phase.as_deref() == Some("ready")
                    && current.attempts == 0
                    && matches!(current.state.as_str(), "pending" | "running")
                    && authority.resource_id.is_none()
                    && row.is_some_and(|row| {
                        matches!(row.effect.as_str(), "reserved" | "not_created")
                    })
            }
            Some(history) => history.effect == "not_created" && history.submit_calls < 8,
        });
    }
    // The existing claimed prepare transaction has persisted the successor
    // invocation and exact calling/history footprint before provider IO.
    Ok(history.is_some_and(|history| {
        matches!(history.effect.as_str(), "unknown" | "not_created")
            && current.submit_phase.as_deref() == Some("calling")
            && current.state == "running"
            && current.locked_until.is_some()
    }))
}

fn history_route_matches(history: &jobs::history::Model, route: &ProviderRouteSnapshot) -> bool {
    crate::pinning::worker::invocation_route_for_snapshot(route)
        .is_some_and(|(api, strategy)| history.api == api && history.strategy == strategy)
}
