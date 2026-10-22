use super::*;
use crate::{
    pinning::management::{Action, ObjectCapture, ObjectStep, RemoteDirective, SideEffect},
    store::{
        entities::{pin_job, pin_lease, pin_lease_target, remote_pin},
        pinning::{
            jobs::{self, NewPinJob},
            leases, ledger,
        },
    },
};
use sea_orm::DatabaseTransaction;

pub(super) async fn execute(
    tx: &DatabaseTransaction,
    capture: &ObjectCapture,
    step: &ObjectStep,
    action: &Action,
    before: records::FrozenRows<'_>,
    result: &mut records::CommittedAction,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let target = capture
        .target
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    work.copy(&(&target.lease.id, &target.id))?;
    result.receipt.intent_ids.push(target.lease.id.clone());
    result.targets.push(target.id.clone());
    if matches!(action, Action::Reconcile)
        || matches!(
            step.source,
            RemoteDirective::ReconcileHistorical | RemoteDirective::ObserveExisting
        )
    {
        observe_capture(tx, capture, before, result, work).await?;
    } else if step.source == RemoteDirective::WaitForInflight {
        result.state = records::ActionState::Waiting;
        // A durable readonly observation can wait for the original in-flight
        // invocation; it can never become its replacement Submit/Unpin.
        observe_capture(tx, capture, before, result, work).await?;
    } else if step.source == RemoteDirective::ReuseConfirmed {
        let model = records::current_model::<pin_lease_target::Entity, _>(tx, &[&target.id], work)
            .await?
            .ok_or(ManagementError::Changed)?;
        let remote = records::current_model::<remote_pin::Entity, _>(
            tx,
            &[&model.provider, &model.cid],
            work,
        )
        .await?
        .ok_or(ManagementError::Changed)?;
        let now = crate::store::database_clock::database_now(tx)
            .await
            .map_err(|_| ManagementError::Database)?;
        if remote.request_id.is_none() || !matches!(remote.status.as_str(), "pinned" | "pinning") {
            return Err(ManagementError::Changed);
        }
        if !crate::store::entities::pin_lease_requirement::contract::target_confirmed_with_work(
            tx,
            &target.lease.id,
            &model.provider,
            &model.cid,
            now,
            work.budget().clone(),
        )
        .await?
        {
            return Err(ManagementError::Changed);
        }
        leases::project_confirmed_target_from_remote(tx, &model, &remote, now, work)
            .await
            .map_err(database)?;
        result.state = records::ActionState::RemoteComplete;
    } else if step.source == RemoteDirective::SubmitWithFence {
        let model = records::current_model::<pin_lease_target::Entity, _>(tx, &[&target.id], work)
            .await?
            .ok_or(ManagementError::Changed)?;
        if let Some(remote) = records::current_model::<remote_pin::Entity, _>(
            tx,
            &[&model.provider, &model.cid],
            work,
        )
        .await?
            && remote.status == "failed"
            && let Some(request) = &remote.request_id
        {
            let now = crate::store::database_clock::database_now(tx)
                .await
                .map_err(|_| ManagementError::Database)?;
            let outcome =
                leases::prepare_confirmed_failed_remote_resubmit(tx, &remote, request, now, work)
                    .await
                    .map_err(database)?;
            if !matches!(
                outcome,
                leases::FailedRemoteResubmitDecision::Prepared { .. }
            ) {
                return Err(ManagementError::Changed);
            }
        } else {
            schedule_target(tx, capture, &model, before, result, work).await?;
        }
    } else {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}

pub(super) async fn schedule_target(
    tx: &DatabaseTransaction,
    capture: &ObjectCapture,
    target: &pin_lease_target::Model,
    before: records::FrozenRows<'_>,
    result: &mut records::CommittedAction,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    if matches!(target.state.as_str(), "quota_waiting" | "quota_blocked") {
        result.state = records::ActionState::NeedsAttention;
        return Ok(());
    }
    let remote =
        records::current_model::<remote_pin::Entity, _>(tx, &[&target.provider, &target.cid], work)
            .await?
            .ok_or(ManagementError::Changed)?;
    let row = ledger::get_with_work(tx, &target.provider, &target.cid, work)
        .await
        .map_err(database)?
        .ok_or(ManagementError::InvalidEvidence)?;
    let route = work.with(|work| {
        work.typed(
            row.route
                .as_deref()
                .ok_or(ManagementError::InvalidEvidence)?,
        )
    })?;
    let captured = capture
        .remote
        .as_ref()
        .filter(|remote| remote.route.original.as_ref() == Some(&route))
        .or_else(|| {
            capture
                .destination_remote
                .as_ref()
                .filter(|remote| remote.route.original.as_ref() == Some(&route))
        });
    let old = before.row("remote_pins", &[&target.provider, &target.cid]);
    let history = before
        .table("pin_resource_history")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
        .any(|row| {
            native::value(row, "provider") == Some(&target.provider)
                && native::value(row, "cid") == Some(&target.cid)
        });
    let safe_new = !history
        && (old.is_none()
            || matches!(before, records::FrozenRows::Execution(_))
                && capture.remote.is_none()
                && row.effect == "reserved");
    let safe_rejected = captured.is_some_and(|remote| {
        matches!(
            remote.effect,
            SideEffect::NotSubmitted | SideEffect::Rejected
        ) && remote.inflight_operations == Some(0)
            && !remote.retained
            && !remote.cleanup_pending
    });
    let desired = records::current_model::<pin_lease::Entity, _>(tx, &[&target.lease_id], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    if desired.state != "active" || desired.expires_at <= now {
        return Err(ManagementError::Expired);
    }
    if row.effect == "confirmed"
        && crate::store::entities::pin_lease_requirement::contract::target_confirmed_with_work(
            tx,
            &desired.id,
            &target.provider,
            &target.cid,
            now,
            work.budget().clone(),
        )
        .await?
    {
        leases::project_confirmed_target_from_remote(tx, target, &remote, now, work)
            .await
            .map_err(database)?;
        result.state = records::ActionState::RemoteComplete;
    } else if (safe_new || safe_rejected)
        && remote.status == "reserved"
        && remote.request_id.is_none()
    {
        for _ in 0..2 {
            work.copy(&(&target.provider, &target.cid))?;
        }
        let parked=records::current_models::<pin_job::Entity,_>(tx,"FROM pin_jobs WHERE provider=$1 AND cid=$2 AND operation='submit' AND state='running' AND locked_until IS NULL","id",vec![target.provider.clone().into(),target.cid.clone().into()],native::MAX_ROWS,work).await?;
        if !parked.is_empty() {
            if parked.len() != 1 {
                return Err(ManagementError::Changed);
            }
            let resumed = if before.row("pin_leases", &[&target.lease_id]).is_none()
                && result.decision.is_some()
                && result.receipt.intent_ids.contains(&target.lease_id)
                && matches!(
                    result.receipt.binding.request.action,
                    crate::pinning::management::Action::Create(_)
                        | crate::pinning::management::Action::Backfill(_)
                        | crate::pinning::management::Action::Reapply(_)
                ) {
                jobs::resume_management_submit_after_allocation(
                    tx, &parked[0], &route, before, target, now, work,
                )
                .await
                .map_err(database)?
            } else {
                jobs::resume_management_submit_in_transaction(tx, &parked[0], &route, now, work)
                    .await
                    .map_err(database)?
            };
            if !resumed {
                return Err(ManagementError::Changed);
            }
            result.state = records::ActionState::Queued;
            return Ok(());
        }
        let owner =
            jobs::canonical_desired_target_with_work(tx, &target.provider, &target.cid, work)
                .await?
                .ok_or(ManagementError::Changed)?;
        let owner_lease =
            records::current_model::<pin_lease::Entity, _>(tx, &[&owner.lease_id], work)
                .await?
                .ok_or(ManagementError::Changed)?;
        work.copy(&(&target.provider, &target.cid, &owner_lease.id, &owner.id))?;
        let NewPinJob::Target(job) = jobs::submit_job(
            &target.provider,
            &target.cid,
            &owner_lease.id,
            &owner.id,
            owner_lease.generation,
            now,
        ) else {
            unreachable!()
        };
        work.copy(&job.id)?;
        let id = job.id.clone();
        jobs::ensure_submit_with_work(tx, job, now, work)
            .await
            .map_err(database)?;
        let exists = tx
            .query_one(sea_orm::Statement::from_sql_and_values(
                tx.get_database_backend(),
                "SELECT COUNT(*) AS n FROM pin_jobs WHERE id=$1",
                [id.into()],
            ))
            .await
            .map_err(|_| ManagementError::Database)?
            .ok_or(ManagementError::Database)?
            .try_get::<i64>("", "n")
            .map_err(|_| ManagementError::Database)?;
        if exists != 0 {
            result.state = records::ActionState::Queued;
        } else {
            result.state = records::ActionState::Waiting;
        }
    } else {
        work.copy(&(&remote.provider, &remote.cid, &remote.request_id))?;
        observe(
            tx,
            &target.provider,
            &target.cid,
            remote.epoch,
            &route,
            &desired.id,
            capture,
            result,
            native::NativeKey {
                table: "remote_pins".into(),
                values: vec![remote.provider.clone(), remote.cid.clone()],
            },
            remote.request_id.clone(),
            work,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn observe_capture(
    tx: &DatabaseTransaction,
    capture: &ObjectCapture,
    before: records::FrozenRows<'_>,
    result: &mut records::CommittedAction,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let target = capture
        .target
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let provider =
        native::value(&target.native_tuple, "provider").ok_or(ManagementError::InvalidEvidence)?;
    let cid = native::value(&target.native_tuple, "cid").ok_or(ManagementError::InvalidEvidence)?;
    let remote = records::current_model::<remote_pin::Entity, _>(tx, &[provider, cid], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    let route = capture
        .remote_route
        .as_ref()
        .and_then(|route| route.original.as_ref())
        .or(capture.route.original.as_ref())
        .ok_or(ManagementError::InvalidEvidence)?;
    work.copy(&(&remote.provider, &remote.cid, &remote.request_id))?;
    observe(
        tx,
        provider,
        cid,
        remote.epoch,
        route,
        &target.lease.id,
        capture,
        result,
        native::NativeKey {
            table: "remote_pins".into(),
            values: vec![remote.provider.clone(), remote.cid.clone()],
        },
        remote.request_id.clone(),
        work,
    )
    .await?;
    for historical in &capture.historical_remotes {
        let original = historical
            .route
            .original
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?;
        let key = historical_key(before, historical, work)?;
        // Historical observation is fenced to its captured epoch and immutable
        // route even if the current epoch has since changed. C consumes it as
        // readonly debt evidence, not current-resource mutation authority.
        work.copy(&(&key, &historical.resource_id))?;
        observe(
            tx,
            &key.values[0],
            &key.values[1],
            historical.epoch,
            original,
            &target.lease.id,
            capture,
            result,
            key.clone(),
            historical.resource_id.clone(),
            work,
        )
        .await?;
    }
    Ok(())
}

fn historical_key(
    frame: records::FrozenRows<'_>,
    remote: &crate::pinning::management::RemoteSnapshot,
    work: records::DecodeWork<'_>,
) -> Result<native::NativeKey> {
    let mut found = None;
    for row in frame
        .table("pin_resource_history")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
    {
        let (Some(provider), Some(cid), Some(epoch)) =
            (row.cell("provider"), row.cell("cid"), row.cell("epoch"))
        else {
            return Err(ManagementError::InvalidEvidence);
        };
        if epoch.parse::<i64>().ok() != Some(remote.epoch) {
            continue;
        }
        work.copy(cid)?;
        if crate::pinning::provider::canonical_resource_cid(cid)
            .ok()
            .as_deref()
            != Some(&remote.key.cid)
        {
            continue;
        }
        let encoded = row.cell("ledger").ok_or(ManagementError::InvalidEvidence)?;
        let envelope: serde_json::Value = work.with(|work| work.typed(encoded))?;
        let original = match envelope
            .get("ledger")
            .and_then(|ledger| ledger.get("route"))
            .and_then(serde_json::Value::as_str)
        {
            Some(route) => Some(work.with(|work| {
                work.typed::<crate::pinning::identity::ProviderRouteSnapshot>(route)
            })?),
            None => None,
        };
        if original.as_ref() != remote.route.original.as_ref()
            || envelope
                .get("request_id")
                .and_then(serde_json::Value::as_str)
                != remote.resource_id.as_deref()
        {
            continue;
        }
        if found.is_some() {
            return Err(ManagementError::InvalidEvidence);
        }
        work.copy(&("pin_resource_history", provider, cid, epoch))?;
        found = Some(native::NativeKey {
            table: "pin_resource_history".into(),
            values: vec![provider.into(), cid.into(), epoch.into()],
        });
    }
    found.ok_or(ManagementError::InvalidEvidence)
}

#[allow(clippy::too_many_arguments)]
async fn observe(
    tx: &DatabaseTransaction,
    provider: &str,
    cid: &str,
    epoch: i64,
    route: &crate::pinning::identity::ProviderRouteSnapshot,
    lease: &str,
    capture: &ObjectCapture,
    result: &mut records::CommittedAction,
    resource_key: native::NativeKey,
    resource_id: Option<String>,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    if result.jobs.iter().any(|job| {
        job.directive == records::JobDirective::ReadOnlyObserve
            && job.provider == provider
            && job.cid == cid
            && job.remote_epoch == epoch
            && job.route == *route
    }) {
        return Ok(());
    }
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    work.with(|work| {
        work.nodes(1)?;
        work.bytes("management-observe:".len() + 36)
    })?;
    let id = format!("management-observe:{}", uuid::Uuid::new_v4());
    job_authority::enqueue_observation(tx, &id, (provider, cid), epoch, route, now, work).await?;
    let lease = records::current_model::<pin_lease::Entity, _>(tx, &[lease], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    let contract =
        records::current_model::<crate::store::entities::pin_lease_requirement::Entity, _>(
            tx,
            &[&lease.id],
            work,
        )
        .await?
        .map(|row| row.contract);
    work.copy(&(
        provider,
        cid,
        route,
        &capture.owner_identity,
        &capture.object.identity,
    ))?;
    result.jobs.push(records::JobAuthority {
        job_id: id,
        directive: records::JobDirective::ReadOnlyObserve,
        provider: provider.into(),
        cid: cid.into(),
        remote_epoch: epoch,
        route: route.clone(),
        resource_key,
        resource_id,
        lease_id: Some(lease.id),
        lease_generation: Some(lease.generation),
        target_id: None,
        owner: Some(capture.owner_identity.clone()),
        source: Some(capture.object.identity.clone()),
        lease_source: Some(lease.source),
        started_at: Some(lease.created_at),
        expires_at: Some(lease.expires_at),
        policy_id: Some(lease.policy_id),
        provider_mode: Some(lease.provider_mode),
        content_mode: Some(lease.content_mode),
        lease_contract: contract,
    });
    result.state = records::ActionState::Queued;
    Ok(())
}
