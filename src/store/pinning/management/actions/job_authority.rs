//! Worker-facing typed authority: exact applied action, job/source, immutable
//! route, lease generation and remote epoch. No queue-wide confirmation permit.
use super::execution::cell;
use super::*;
use crate::store::pinning::management::admission::ManagedCall;
use crate::{
    error::{AppError, AppResult},
    pinning::{
        identity::ProviderRouteSnapshot,
        management::{Capture, PlanEntry},
    },
    store::{
        entities::{object_version, pin_invocation_route, pin_job, pin_lease, remote_pin},
        pinning::ledger,
    },
};
use chrono::{DateTime, Utc};
#[cfg(test)]
use sea_orm::{ColumnTrait, QueryFilter};
use sea_orm::{ConnectionTrait, DatabaseTransaction, EntityTrait, Set};
mod completion;
mod current_scope;
pub(crate) use completion::CompletionReader;
pub(crate) mod park_scope;
#[cfg(test)]
#[path = "job_authority/renew_tests.rs"]
mod renew_tests;
pub(crate) mod renewal;
pub(crate) mod source;
pub(crate) mod successor;
#[cfg(test)]
#[path = "job_authority/worker_facts.rs"]
mod worker_facts;

/// Immutable dispatch identity. Receipt appends must not resolve a newer
/// invocation by job ID after an awaited provider call or claim renewal.
pub(crate) struct InvocationCapture {
    pub(crate) job: pin_job::Model,
    pub(crate) invocation: pin_invocation_route::Model,
    pub(crate) route: ProviderRouteSnapshot,
    pub(crate) submit_call: Option<i32>,
    pub(crate) started_at: DateTime<Utc>,
}

impl InvocationCapture {
    pub(crate) async fn capture_with_work<C: ConnectionTrait>(
        db: &C,
        job: &pin_job::Model,
        call: ManagedCall,
        work: records::DecodeWork<'_>,
    ) -> AppResult<Self> {
        let invocation =
            records::current_model::<pin_invocation_route::Entity, _>(db, &[&job.id], work)
                .await
                .map_err(safe)?
                .ok_or_else(|| safe(ManagementError::InvalidEvidence))?;
        let route = work
            .with(|work| work.typed(&invocation.route))
            .map_err(safe)?;
        if job.locked_until.is_none() {
            return Err(AppError::Internal("dispatch claim is missing".into()));
        }
        let submit_call = if matches!(call, ManagedCall::Submit)
            && !ledger::submission::is_rpc(&route)
        {
            Some(
                records::current_model::<crate::store::pinning::jobs::history::Entity, _>(
                    db,
                    &[&job.id],
                    work,
                )
                .await
                .map_err(safe)?
                .ok_or_else(|| AppError::Internal("dispatch submission history is missing".into()))?
                .submit_calls,
            )
        } else {
            None
        };
        work.copy(&[
            Some(job.id.as_str()),
            Some(job.operation.as_str()),
            Some(job.provider.as_str()),
            Some(job.cid.as_str()),
            job.lease_id.as_deref(),
            job.target_id.as_deref(),
            Some(job.state.as_str()),
            job.submit_phase.as_deref(),
        ])
        .map_err(safe)?;
        work.with(|work| work.nodes(10)).map_err(safe)?;
        Ok(Self {
            job: pin_job::Model {
                id: job.id.clone(),
                operation: job.operation.clone(),
                provider: job.provider.clone(),
                cid: job.cid.clone(),
                lease_id: job.lease_id.clone(),
                target_id: job.target_id.clone(),
                expected_generation: job.expected_generation,
                expected_remote_epoch: job.expected_remote_epoch,
                state: job.state.clone(),
                attempts: job.attempts,
                next_attempt_at: job.next_attempt_at,
                locked_until: job.locked_until,
                submit_phase: job.submit_phase.clone(),
                last_error: None,
                created_at: job.created_at,
                updated_at: job.updated_at,
            },
            invocation,
            route,
            submit_call,
            started_at: Utc::now(),
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum JobPermit {
    Submit(records::JobAuthority),
    ReadOnly(records::JobAuthority),
    ExistingCleanup(records::JobAuthority),
    /// Late facts may enter the existing ledger, never project/resurrect a lease.
    LedgerOnly(records::JobAuthority),
}

/// Caller invokes this under its normal claim/lifecycle/remote transaction at
/// job birth/claim/heartbeat/before IO. None preserves an ordinary job's existing
/// authority; it does not mean a missing management binding is authorized.
pub async fn check_before_call<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
) -> AppResult<Option<JobPermit>> {
    check_before_call_with_work(db, job, records::SharedDecodeBudget::default())
        .await
        .map_err(safe)
}
pub(crate) async fn check_before_call_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    work: records::SharedDecodeBudget,
) -> Result<Option<JobPermit>> {
    let authority = lookup_with_work(db, job, work.clone()).await?;
    check_with_authority(db, job, authority, work).await
}

async fn check_with_authority<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    authority: Option<records::JobAuthority>,
    work: records::SharedDecodeBudget,
) -> Result<Option<JobPermit>> {
    let Some(authority) = authority else {
        return Ok(None);
    };
    if authority.directive == records::JobDirective::ReadOnlyObserve {
        // Historical/late observation is allowed as readonly ledger evidence,
        // never current lease projection, resubmit or compensation authority.
        return Ok(Some(JobPermit::ReadOnly(authority)));
    }
    let now = crate::store::database_clock::database_now(db)
        .await
        .map_err(database)?;
    if !successor::current_resource_with_work(
        db,
        job,
        &authority,
        records::DecodeWork::shared(&work),
    )
    .await?
        || !current_lease(db, &authority, now, records::DecodeWork::shared(&work)).await?
        || authority.directive == records::JobDirective::Submit
            && !source::historical_fresh(db, job, &authority, now, work.clone()).await?
    {
        return Ok(Some(JobPermit::LedgerOnly(authority)));
    }
    let permit = match authority.directive {
        records::JobDirective::Submit => {
            if !matches!(job.operation.as_str(), "submit" | "poll") {
                return Err(ManagementError::Confirmation);
            }
            JobPermit::Submit(authority)
        }
        records::JobDirective::ReadOnlyObserve => {
            if job.operation != "reconcile" {
                return Err(ManagementError::Confirmation);
            }
            JobPermit::ReadOnly(authority)
        }
        records::JobDirective::ExistingCleanup => {
            if job.operation != "unpin"
                || !ledger::cleanup_allowed_with_work(
                    db,
                    &job.provider,
                    &job.cid,
                    records::DecodeWork::shared(&work),
                )
                .await
                .map_err(database)?
            {
                return Ok(Some(JobPermit::LedgerOnly(authority)));
            }
            JobPermit::ExistingCleanup(authority)
        }
    };
    Ok(Some(permit))
}

/// Current runtime context is supplied by the worker after bounded source-node
/// IO outside the transaction. Only trusted proofs/native rows are read here.
#[cfg(test)]
pub(crate) async fn fresh_source_allowed<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    config: Option<&ManagementConfig>,
    idle_timeout_millis: Option<u64>,
) -> AppResult<bool> {
    fresh_source_allowed_with_work(
        db,
        job,
        config,
        idle_timeout_millis,
        records::SharedDecodeBudget::default(),
    )
    .await
}
#[cfg(test)]
pub(crate) async fn fresh_source_allowed_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    config: Option<&ManagementConfig>,
    idle_timeout_millis: Option<u64>,
    work: records::SharedDecodeBudget,
) -> AppResult<bool> {
    fresh_source_allowed_view_with_work(
        db,
        job,
        config.map(ManagementConfig::view),
        idle_timeout_millis,
        work,
    )
    .await
}
pub(crate) async fn fresh_source_allowed_view_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    config: Option<crate::pinning::management::local_config::ManagementConfigView<'_>>,
    idle_timeout_millis: Option<u64>,
    work: records::SharedDecodeBudget,
) -> AppResult<bool> {
    let authority = lookup_with_work(db, job, work.clone())
        .await
        .map_err(safe)?;
    fresh_source_from_authority(db, authority.as_ref(), config, idle_timeout_millis, work).await
}

async fn fresh_source_from_authority<C: ConnectionTrait>(
    db: &C,
    authority: Option<&records::JobAuthority>,
    config: Option<crate::pinning::management::local_config::ManagementConfigView<'_>>,
    idle_timeout_millis: Option<u64>,
    work: records::SharedDecodeBudget,
) -> AppResult<bool> {
    let Some(authority) = authority else {
        return Ok(true);
    };
    if authority.directive != records::JobDirective::Submit {
        return Ok(true);
    }
    let Some(config) = config else {
        return Ok(false);
    };
    let now = crate::store::database_clock::database_now(db).await?;
    source::current(db, authority, config, idle_timeout_millis, now, work)
        .await
        .map_err(safe)
}

pub(crate) async fn lock_source_in_transaction_with_work(
    tx: &DatabaseTransaction,
    job: &pin_job::Model,
    work: records::SharedDecodeBudget,
) -> AppResult<()> {
    crate::store::pinning::management::admission::lock_sqlite_writer_in_caller_transaction(tx)
        .await?;
    if let Some(authority) = lookup_with_work(tx, job, work).await.map_err(safe)?
        && authority.directive == records::JobDirective::Submit
    {
        source::lock(tx, &authority).await.map_err(safe)?;
    }
    Ok(())
}

/// Acquire only at the tail, after the caller's ordered lifecycle/remote locks.
/// Same-CID proof owners and empty manifest sets must stay
/// stable through final projection, not just the selected source's row.
pub(crate) async fn fence_worker_authority_with_work<'a>(
    tx: &'a DatabaseTransaction,
    job: &'a pin_job::Model,
    work: records::SharedDecodeBudget,
) -> AppResult<CompletionReader<'a>> {
    records::locator::fence_authority(tx).await.map_err(safe)?;
    records::DecodeWork::shared(&work)
        .copy(&(&job.provider, &job.cid))
        .map_err(safe)?;
    let actions = crate::store::pinning::management::admission::authority::actions_for_job(
        tx,
        job,
        work.clone(),
    )
    .await
    .map_err(safe)?;
    if let Some(authority) = lookup_from_actions(tx, job, &actions, work.clone())
        .await
        .map_err(safe)?
        && authority.directive == records::JobDirective::Submit
    {
        current_scope::fence(tx, job, &authority)
            .await
            .map_err(safe)?;
    }
    Ok(CompletionReader::new(tx, job, actions, work))
}

/// Call on result publication too. Management readonly/expired/stale work may
/// append facts with the existing protocol, but may not update a lease or turn
/// a readonly reconcile into a Submit/Unpin.
pub async fn projection_allowed<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
) -> AppResult<bool> {
    projection_allowed_with_work(db, job, records::SharedDecodeBudget::default()).await
}
pub(crate) async fn projection_allowed_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    work: records::SharedDecodeBudget,
) -> AppResult<bool> {
    Ok(
        match check_before_call_with_work(db, job, work)
            .await
            .map_err(safe)?
        {
            None => true,
            Some(JobPermit::Submit(_) | JobPermit::ExistingCleanup(_)) => true,
            Some(JobPermit::ReadOnly(_) | JobPermit::LedgerOnly(_)) => false,
        },
    )
}

pub async fn cleanup_settlement_allowed<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
) -> AppResult<bool> {
    cleanup_settlement_allowed_with_work(db, job, records::SharedDecodeBudget::default()).await
}
pub(crate) async fn cleanup_settlement_allowed_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    work: records::SharedDecodeBudget,
) -> AppResult<bool> {
    Ok(matches!(
        check_before_call_with_work(db, job, work)
            .await
            .map_err(safe)?,
        Some(JobPermit::ExistingCleanup(_))
    ) && job.operation == "unpin")
}

pub async fn lookup<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
) -> Result<Option<records::JobAuthority>> {
    Box::pin(lookup_with_work(
        db,
        job,
        records::SharedDecodeBudget::default(),
    ))
    .await
}
pub(crate) async fn lookup_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    work: records::SharedDecodeBudget,
) -> Result<Option<records::JobAuthority>> {
    let actions = Box::pin(
        crate::store::pinning::management::admission::authority::actions_for_job(
            db,
            job,
            work.clone(),
        ),
    )
    .await?;
    lookup_from_actions(db, job, &actions, work).await
}

async fn lookup_from_actions<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    actions: &[records::TrustedAppliedAction],
    work: records::SharedDecodeBudget,
) -> Result<Option<records::JobAuthority>> {
    let mut found = None;
    for trusted in actions {
        let action = trusted.action_view();
        for authority in &action.jobs {
            if authority.job_id != job.id {
                continue;
            }
            trusted.validate_job(authority)?;
            validate_job_resolved_with_work(db, job, authority, trusted.binding().decode_work())
                .await?;
            let invocation = records::current_model::<pin_invocation_route::Entity, _>(
                db,
                &[&job.id],
                trusted.binding().decode_work(),
            )
            .await?
            .ok_or(ManagementError::Confirmation)?;
            if invocation.remote_epoch < authority.remote_epoch
                || trusted.binding().decode_route(&invocation.route)?.as_ref() != &authority.route
            {
                return Err(ManagementError::Confirmation);
            }
            trusted.binding().copy(authority)?;
            meet(&mut found, authority.clone())?;
        }
    }
    // Poll successors must carry the exact original target/source/generation
    // and invocation. There is no global alias or job-ID-prefix admission.
    if job.operation == "poll" {
        for trusted in actions {
            let action = trusted.action_view();
            for original in &action.jobs {
                if original.job_id == job.id
                    || original.directive != records::JobDirective::Submit
                    || original.lease_id != job.lease_id
                    || original.lease_generation != job.expected_generation
                    || original.target_id != job.target_id
                    || original.cid != job.cid
                {
                    continue;
                }
                trusted.validate_job(original)?;
                let invocation = records::current_model::<pin_invocation_route::Entity, _>(
                    db,
                    &[&job.id],
                    trusted.binding().decode_work(),
                )
                .await?
                .ok_or(ManagementError::Confirmation)?;
                if invocation.remote_epoch < original.remote_epoch
                    || trusted.binding().decode_route(&invocation.route)?.as_ref()
                        != &original.route
                {
                    return Err(ManagementError::Confirmation);
                }
                if !successor::original_submit_for_poll(
                    db,
                    job,
                    original,
                    &invocation,
                    trusted.binding(),
                )
                .await?
                {
                    return Err(ManagementError::Confirmation);
                }
                trusted.binding().copy(&(original, &job.id))?;
                let mut successor = original.clone();
                successor.job_id = job.id.clone();
                // Stable seed equality is preserved for C's execution scope.
                // Invocation/reference progression is a derived live permit,
                // not a rewrite of the immutable applied authority.
                validate_job_resolved_with_work(
                    db,
                    job,
                    &successor,
                    trusted.binding().decode_work(),
                )
                .await?;
                meet(&mut found, successor)?;
            }
        }
    }
    // An orphaned management lease/job is fail-closed, not an ordinary source.
    if found.is_none()
        && let Some(lease_id) = &job.lease_id
    {
        let lease = records::current_model::<pin_lease::Entity, _>(
            db,
            &[lease_id],
            records::DecodeWork::shared(&work),
        )
        .await?;
        if lease.is_some_and(|lease| lease.source.starts_with("management:")) {
            return Err(ManagementError::Confirmation);
        }
    }
    // A name may deny an orphan; it never grants authority. Remote-only readonly
    // jobs have no lease ID and must not fall through to ordinary reconciliation.
    if found.is_none() && job.id.starts_with("management-") {
        return Err(ManagementError::Confirmation);
    }
    Ok(found)
}

pub(super) async fn birth(
    tx: &DatabaseTransaction,
    job: &pin_job::Model,
    entry: &PlanEntry,
    directive: records::JobDirective,
    before: records::FrozenRows<'_>,
    intents: &[String],
    work: records::DecodeWork<'_>,
) -> Result<records::JobAuthority> {
    if before.row("pin_jobs", &[&job.id]).is_some()
        && let Some(authority) = lookup_with_work(tx, job, work.budget().clone()).await?
    {
        if authority.directive != directive {
            return Err(ManagementError::Confirmation);
        }
        return Ok(authority);
    }
    let capture = match &entry.capture {
        Some(Capture::Object(capture)) => capture,
        _ => return Err(ManagementError::Confirmation),
    };
    let invocation =
        records::current_model::<pin_invocation_route::Entity, _>(tx, &[&job.id], work)
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
    let route = work.with(|work| work.typed(&invocation.route))?;
    let remote =
        records::current_model::<remote_pin::Entity, _>(tx, &[&job.provider, &job.cid], work)
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
    work.copy(&(
        &job.id,
        &job.provider,
        &job.cid,
        &job.provider,
        &job.cid,
        &job.target_id,
    ))?;
    if directive == records::JobDirective::ExistingCleanup {
        return Ok(records::JobAuthority {
            job_id: job.id.clone(),
            directive,
            provider: job.provider.clone(),
            cid: job.cid.clone(),
            remote_epoch: invocation.remote_epoch,
            route,
            resource_key: native::NativeKey {
                table: "remote_pins".into(),
                values: vec![job.provider.clone(), job.cid.clone()],
            },
            resource_id: remote.request_id,
            lease_id: None,
            lease_generation: None,
            target_id: None,
            owner: None,
            source: None,
            lease_source: None,
            started_at: None,
            expires_at: None,
            policy_id: None,
            provider_mode: None,
            content_mode: None,
            lease_contract: None,
        });
    }
    let lease_id = job
        .lease_id
        .as_ref()
        .or_else(|| {
            (directive == records::JobDirective::ReadOnlyObserve && intents.len() == 1)
                .then(|| &intents[0])
        })
        .or_else(|| capture.target.as_ref().map(|target| &target.lease.id))
        .ok_or(ManagementError::Confirmation)?;
    let lease = records::current_model::<pin_lease::Entity, _>(tx, &[lease_id], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    let (owner, source) = if lease.owner_object_id == capture.owner_identity.object_id {
        work.copy(&(&capture.owner_identity, &capture.object.identity))?;
        (
            capture.owner_identity.clone(),
            capture.object.identity.clone(),
        )
    } else {
        if before.row("pin_jobs", &[&job.id]).is_none() {
            return Err(ManagementError::Confirmation);
        }
        let versions = before
            .table("object_versions")
            .ok_or(ManagementError::Schema)?;
        let mut candidates = versions
            .rows
            .iter()
            .filter(|row| row.cell("object_id") == Some(&lease.owner_object_id));
        let version = candidates.next().ok_or(ManagementError::InvalidEvidence)?;
        if candidates.next().is_some() {
            return Err(ManagementError::InvalidEvidence);
        }
        let object = before
            .row("objects", &[&lease.owner_object_id])
            .ok_or(ManagementError::Changed)?;
        work.copy(&(cell(object, "cid")?, &job.cid))?;
        let cid = crate::pinning::provider::canonical_resource_cid(cell(object, "cid")?)
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if crate::pinning::provider::canonical_resource_cid(&job.cid)
            .ok()
            .as_ref()
            != Some(&cid)
        {
            return Err(ManagementError::InvalidEvidence);
        }
        work.copy(&(
            cell(version, "bucket")?,
            cell(version, "key")?,
            version.cell("version_id"),
            cell(version, "id")?,
            &lease.owner_object_id,
        ))?;
        let owner = crate::pinning::management::ExactObject {
            bucket: cell(version, "bucket")?.into(),
            key: cell(version, "key")?.into(),
            public_version: version.cell("version_id").map(str::to_owned).map_or(
                crate::pinning::management::PublicVersionId::Null,
                crate::pinning::management::PublicVersionId::Version,
            ),
            private_version_id: cell(version, "id")?.into(),
            object_id: lease.owner_object_id.clone(),
            cid,
        };
        work.copy(&owner)?;
        (owner.clone(), owner)
    };
    let contract =
        records::current_model::<crate::store::entities::pin_lease_requirement::Entity, _>(
            tx,
            &[&lease.id],
            work,
        )
        .await?
        .map(|row| row.contract);
    Ok(records::JobAuthority {
        job_id: job.id.clone(),
        directive,
        provider: job.provider.clone(),
        cid: job.cid.clone(),
        remote_epoch: invocation.remote_epoch,
        route,
        resource_key: native::NativeKey {
            table: "remote_pins".into(),
            values: vec![job.provider.clone(), job.cid.clone()],
        },
        resource_id: remote.request_id,
        lease_id: Some(lease.id),
        lease_generation: Some(lease.generation),
        target_id: job.target_id.clone(),
        owner: Some(owner),
        source: Some(source),
        lease_source: Some(lease.source),
        started_at: Some(lease.created_at),
        expires_at: Some(lease.expires_at),
        policy_id: Some(lease.policy_id),
        provider_mode: Some(lease.provider_mode),
        content_mode: Some(lease.content_mode),
        lease_contract: contract,
    })
}

pub(super) async fn enqueue_observation(
    tx: &DatabaseTransaction,
    id: &str,
    (provider, cid): (&str, &str),
    epoch: i64,
    route: &ProviderRouteSnapshot,
    now: DateTime<Utc>,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    work.copy(&(id, provider, cid))?;
    pin_job::Entity::insert(pin_job::ActiveModel {
        id: Set(id.into()),
        operation: Set("reconcile".into()),
        provider: Set(provider.into()),
        cid: Set(cid.into()),
        lease_id: Set(None),
        target_id: Set(None),
        expected_generation: Set(None),
        expected_remote_epoch: Set(Some(epoch)),
        state: Set("pending".into()),
        attempts: Set(0),
        next_attempt_at: Set(now),
        locked_until: Set(None),
        submit_phase: Set(None),
        last_error: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(tx)
    .await
    .map_err(|_| ManagementError::Database)?;
    work.copy(&(id, route))?;
    pin_invocation_route::Entity::insert(pin_invocation_route::ActiveModel {
        job_id: Set(id.into()),
        route: Set(serde_json::to_string(route).map_err(|_| ManagementError::InvalidEvidence)?),
        remote_epoch: Set(epoch),
    })
    .exec(tx)
    .await
    .map_err(|_| ManagementError::Database)?;
    crate::store::pinning::jobs::validate_inserted_job_times(tx, id, now, now, work)
        .await
        .map_err(database)?;
    Ok(())
}

pub(crate) async fn validate_executed_source<'a, 'r>(
    tx: &DatabaseTransaction,
    binding: impl Into<records::BindingView<'a>>,
    action: &records::CommittedAction,
    before: impl Into<records::FrozenRows<'r>>,
) -> Result<()> {
    let binding = binding.into();
    let before = before.into();
    if matches!(action.receipt.binding.capture, Some(Capture::Provider(_))) {
        return Ok(());
    }
    for authority in action
        .jobs
        .iter()
        .filter(|authority| authority.directive == records::JobDirective::Submit)
    {
        let now = crate::store::database_clock::database_now(tx)
            .await
            .map_err(database)?;
        if !source::birth_from_rows(tx, binding, action, authority, before, now).await? {
            return Err(ManagementError::Confirmation);
        }
    }
    Ok(())
}

fn validate_job(job: &pin_job::Model, authority: &records::JobAuthority) -> Result<()> {
    validate_job_provider(job, authority, &authority.provider)
}
fn validate_job_provider(
    job: &pin_job::Model,
    authority: &records::JobAuthority,
    provider: &str,
) -> Result<()> {
    let operation = match authority.directive {
        records::JobDirective::Submit => matches!(job.operation.as_str(), "submit" | "poll"),
        records::JobDirective::ReadOnlyObserve => job.operation == "reconcile",
        records::JobDirective::ExistingCleanup => job.operation == "unpin",
    };
    if !operation
        || job.provider != provider
        || job.cid != authority.cid
        || job
            .expected_remote_epoch
            .is_some_and(|epoch| epoch != authority.remote_epoch)
        || job
            .lease_id
            .as_ref()
            .is_some_and(|id| Some(id) != authority.lease_id.as_ref())
        || job
            .expected_generation
            .is_some_and(|generation| Some(generation) != authority.lease_generation)
        || job.target_id != authority.target_id
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}

pub(crate) fn validate_frozen_authority<'a>(
    binding: impl Into<records::BindingView<'a>>,
    action: &records::CommittedAction,
    authority: &records::JobAuthority,
) -> Result<()> {
    let binding = binding.into();
    validate_authority_at_boundary(binding, action, authority, binding.frozen_rows()?)
}

pub(super) fn validate_authority_at_boundary(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    authority: &records::JobAuthority,
    before: records::FrozenRows<'_>,
) -> Result<()> {
    if !binding
        .confirmation
        .bindings
        .contains(&action.receipt.binding)
        || !action.receipt.job_ids.contains(&authority.job_id)
    {
        return Err(ManagementError::Confirmation);
    }
    if !matches!(
        authority.resource_key.table.as_str(),
        "remote_pins" | "pin_resource_history"
    ) || authority.resource_key.values.first() != Some(&authority.provider)
        || authority.resource_key.values.get(1) != Some(&authority.cid)
        || (authority.resource_key.table == "pin_resource_history"
            && (authority.directive != records::JobDirective::ReadOnlyObserve
                || authority
                    .resource_key
                    .values
                    .get(2)
                    .and_then(|epoch| epoch.parse::<i64>().ok())
                    != Some(authority.remote_epoch)))
    {
        return Err(ManagementError::Confirmation);
    }
    match &action.receipt.binding.capture {
        Some(Capture::Object(capture)) => {
            let cancellation = matches!(
                action.receipt.binding.request.action,
                crate::pinning::management::Action::Cancel
            );
            let full_cancel_scope =
                cancellation && cancel_resource_in_frame(binding, capture, authority)?;
            if authority.directive == records::JobDirective::ExistingCleanup {
                if !full_cancel_scope || authority.lease_id.is_some() {
                    return Err(ManagementError::Confirmation);
                }
                return validate_resource_proof(binding, action, authority);
            }
            let shared = authority.owner.as_ref() != Some(&capture.owner_identity)
                || authority
                    .lease_id
                    .as_ref()
                    .is_none_or(|id| !action.receipt.intent_ids.contains(id));
            if shared {
                validate_shared_source(binding, action, authority, before)?;
                return validate_resource_proof_at(binding, authority, before);
            }
            if authority.owner.as_ref() != Some(&capture.owner_identity)
                || authority.source.as_ref() != Some(&capture.object.identity)
                || authority
                    .lease_id
                    .as_ref()
                    .is_none_or(|id| !action.receipt.intent_ids.contains(id))
                || !full_cancel_scope
                    && !capture
                        .requirement_routes
                        .iter()
                        .chain(&capture.historical_routes)
                        .chain([&capture.route])
                        .chain(capture.destination_route.iter())
                        .chain(capture.remote_route.iter())
                        .chain(capture.destination_remote_route.iter())
                        .any(|route| route.original.as_ref() == Some(&authority.route))
                || !full_cancel_scope
                    && crate::pinning::provider::canonical_resource_cid(&authority.cid)
                        .ok()
                        .as_deref()
                        != Some(&capture.object.identity.cid)
            {
                return Err(ManagementError::Confirmation);
            }
            if authority.directive == records::JobDirective::ReadOnlyObserve {
                validate_resource_proof(binding, action, authority)?;
            }
        }
        Some(Capture::Provider(capture)) => {
            if matches!(
                action.receipt.binding.request.action,
                crate::pinning::management::Action::RetireWith {
                    selection: crate::pinning::management::RetirementSelection::Migrate { .. }
                }
            ) {
                return super::execution::validate_retirement_job(binding, action, authority);
            }
            if authority.directive != records::JobDirective::ExistingCleanup
                || !capture.mutation_scope.remotes.iter().any(|remote| {
                    remote.epoch == authority.remote_epoch
                        && crate::pinning::provider::canonical_resource_cid(&authority.cid)
                            .ok()
                            .as_deref()
                            == Some(&remote.key.cid)
                        && remote.route.original.as_ref() == Some(&authority.route)
                        && remote.resource_id == authority.resource_id
                })
            {
                return Err(ManagementError::Confirmation);
            }
        }
        None => return Err(ManagementError::Confirmation),
    }
    Ok(())
}

async fn current_lease<C: ConnectionTrait>(
    db: &C,
    authority: &records::JobAuthority,
    now: DateTime<Utc>,
    work: records::DecodeWork<'_>,
) -> Result<bool> {
    let Some(id) = &authority.lease_id else {
        return Ok(authority.directive == records::JobDirective::ExistingCleanup);
    };
    let lease = records::current_model::<pin_lease::Entity, _>(db, &[id], work).await?;
    let Some(lease) = lease else { return Ok(false) };
    let contract =
        records::current_model::<crate::store::entities::pin_lease_requirement::Entity, _>(
            db,
            &[id],
            work,
        )
        .await?
        .map(|row| row.contract);
    if authority.policy_id.as_deref() != Some(&lease.policy_id)
        || authority.provider_mode.as_deref() != Some(&lease.provider_mode)
        || authority.content_mode.as_deref() != Some(&lease.content_mode)
        || authority.lease_contract != contract
    {
        return Ok(false);
    }
    let Some(owner) = &authority.owner else {
        return Ok(false);
    };
    work.copy(&authority.cid)?;
    if !identity_present(db, owner, work).await?
        || authority.source.as_ref().is_none_or(|source| {
            source.cid
                != crate::pinning::provider::canonical_resource_cid(&authority.cid)
                    .unwrap_or_default()
        })
    {
        return Ok(false);
    }
    if let Some(source) = &authority.source
        && !identity_present(db, source, work).await?
    {
        return Ok(false);
    }
    let exact =
        records::current_model::<object_version::Entity, _>(db, &[&owner.private_version_id], work)
            .await?;
    let Some(exact) = exact else { return Ok(false) };
    Ok(exact.object_id.as_deref() == Some(owner.object_id.as_str())
        && exact.bucket == owner.bucket
        && exact.key == owner.key
        && exact.version_id.as_deref()
            == match &owner.public_version {
                crate::pinning::management::PublicVersionId::Null => None,
                crate::pinning::management::PublicVersionId::Version(version) => {
                    Some(version.as_str())
                }
            }
        && lease.owner_object_id == owner.object_id
        && authority.lease_source.as_deref() == Some(&lease.source)
        && authority.lease_generation == Some(lease.generation)
        && authority.started_at == Some(lease.created_at)
        && authority.expires_at == Some(lease.expires_at)
        && lease.state == "active"
        && now < lease.expires_at)
}

async fn identity_present<C: ConnectionTrait>(
    db: &C,
    identity: &crate::pinning::management::ExactObject,
    work: records::DecodeWork<'_>,
) -> Result<bool> {
    let version = records::current_model::<object_version::Entity, _>(
        db,
        &[&identity.private_version_id],
        work,
    )
    .await?;
    let object = records::current_model::<crate::store::entities::object::Entity, _>(
        db,
        &[&identity.object_id],
        work,
    )
    .await?;
    if let Some(object) = &object {
        work.copy(&object.cid)?;
    }
    Ok(version.is_some_and(|version| {
        version.object_id.as_deref() == Some(identity.object_id.as_str())
            && version.bucket == identity.bucket
            && version.key == identity.key
            && version.version_id.as_deref()
                == match &identity.public_version {
                    crate::pinning::management::PublicVersionId::Null => None,
                    crate::pinning::management::PublicVersionId::Version(version) => {
                        Some(version.as_str())
                    }
                }
    }) && object.is_some_and(|object| {
        object.bucket == identity.bucket
            && object.key == identity.key
            && crate::pinning::provider::canonical_resource_cid(&object.cid)
                .ok()
                .as_ref()
                == Some(&identity.cid)
    }))
}

pub(crate) fn safe(error: ManagementError) -> AppError {
    if error == ManagementError::Budget {
        AppError::ManagementBudget
    } else {
        AppError::InvalidPinningRequest(
            "management job authority is missing, stale, or invalid".into(),
        )
    }
}

fn meet(found: &mut Option<records::JobAuthority>, candidate: records::JobAuthority) -> Result<()> {
    if found
        .as_ref()
        .is_some_and(|original| *original != candidate)
    {
        return Err(ManagementError::InvalidEvidence);
    }
    *found = Some(candidate);
    Ok(())
}
#[cfg(test)]
async fn validate_job_resolved<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    authority: &records::JobAuthority,
) -> Result<()> {
    validate_job_resolved_with_work(
        db,
        job,
        authority,
        records::DecodeWork::shared(&records::SharedDecodeBudget::default()),
    )
    .await
}
pub(crate) async fn validate_job_resolved_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    authority: &records::JobAuthority,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    if job.provider != authority.provider {
        work.copy(&[("id", &job.id)])?;
        let key = [("id".into(), job.id.clone())].into();
        let binding = crate::store::pinning::legacy_migration::resolve_legacy_binding_with_work(
            db,
            "pin_jobs",
            &key,
            &authority.provider,
            work,
        )
        .await?
        .ok_or(ManagementError::Confirmation)?;
        if binding.operational_key != job.provider
            || binding.historical_route.as_ref() != Some(&authority.route)
        {
            return Err(ManagementError::Confirmation);
        }
        return validate_job_provider(job, authority, &job.provider);
    }
    validate_job(job, authority)
}
async fn operational_resource_with_work<C: ConnectionTrait>(
    db: &C,
    authority: &records::JobAuthority,
    work: records::DecodeWork<'_>,
) -> Result<String> {
    work.copy(&[("provider", &authority.provider), ("cid", &authority.cid)])?;
    let mut key = std::collections::BTreeMap::from([
        ("provider".into(), authority.provider.clone()),
        ("cid".into(), authority.cid.clone()),
    ]);
    if authority.resource_key.table == "pin_resource_history" {
        work.copy(&("epoch", authority.remote_epoch))?;
        key.insert("epoch".into(), authority.remote_epoch.to_string());
    }
    let binding = crate::store::pinning::legacy_migration::resolve_legacy_binding_with_work(
        db,
        &authority.resource_key.table,
        &key,
        &authority.provider,
        work,
    )
    .await?;
    if let Some(binding) = binding {
        if binding.historical_route.as_ref() != Some(&authority.route) {
            return Err(ManagementError::Confirmation);
        }
        return Ok(binding.operational_key);
    }
    work.copy(&authority.provider)?;
    Ok(authority.provider.clone())
}
fn validate_shared_source(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    authority: &records::JobAuthority,
    before: records::FrozenRows<'_>,
) -> Result<()> {
    if authority.directive != records::JobDirective::Submit
        || !matches!(
            action.receipt.binding.request.action,
            crate::pinning::management::Action::Create(_)
                | crate::pinning::management::Action::Backfill(_)
                | crate::pinning::management::Action::Reapply(_)
        )
    {
        return Err(ManagementError::Confirmation);
    }
    let Some(Capture::Object(capture)) = &action.receipt.binding.capture else {
        return Err(ManagementError::Confirmation);
    };
    if crate::pinning::provider::canonical_resource_cid(&authority.cid)
        .ok()
        .as_deref()
        != Some(&capture.object.identity.cid)
    {
        return Err(ManagementError::Confirmation);
    }
    let job = before
        .row("pin_jobs", &[&authority.job_id])
        .ok_or(ManagementError::Confirmation)?;
    let lease_id = authority
        .lease_id
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let owner = authority
        .owner
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let lease = before
        .row("pin_leases", &[lease_id])
        .ok_or(ManagementError::Confirmation)?;
    let version = before
        .row("object_versions", &[&owner.private_version_id])
        .ok_or(ManagementError::Confirmation)?;
    let object = before
        .row("objects", &[&owner.object_id])
        .ok_or(ManagementError::Confirmation)?;
    let invocation = before
        .row("pin_invocation_routes", &[&authority.job_id])
        .ok_or(ManagementError::Confirmation)?;
    if native::value(job, "provider") != Some(&authority.provider)
        || native::value(job, "cid") != Some(&authority.cid)
        || native::value(job, "lease_id") != Some(lease_id)
        || native::value(job, "expected_generation").and_then(|value| value.parse().ok())
            != authority.lease_generation
        || native::value(job, "target_id") != authority.target_id.as_deref()
        || native::value(lease, "owner_object_id") != Some(&owner.object_id)
        || native::value(lease, "source") != authority.lease_source.as_deref()
        || native::value(lease, "created_at").and_then(|time| native::parse_time(time).ok())
            != authority.started_at
        || native::value(lease, "expires_at").and_then(|time| native::parse_time(time).ok())
            != authority.expires_at
        || native::value(lease, "policy_id") != authority.policy_id.as_deref()
        || native::value(lease, "provider_mode") != authority.provider_mode.as_deref()
        || native::value(lease, "content_mode") != authority.content_mode.as_deref()
        || before
            .row("pin_lease_requirements", &[lease_id])
            .and_then(|row| native::value(row, "contract"))
            != authority.lease_contract.as_deref()
        || native::value(version, "object_id") != Some(&owner.object_id)
        || native::value(version, "bucket") != Some(&owner.bucket)
        || native::value(version, "key") != Some(&owner.key)
        || native::value(version, "version_id")
            != match &owner.public_version {
                crate::pinning::management::PublicVersionId::Null => None,
                crate::pinning::management::PublicVersionId::Version(version) => {
                    Some(version.as_str())
                }
            }
        || native::value(object, "cid")
            .and_then(|cid| crate::pinning::provider::canonical_resource_cid(cid).ok())
            .as_ref()
            != Some(&owner.cid)
        || !shared_source_in_frame(before, capture, authority)?
    {
        return Err(ManagementError::Confirmation);
    }
    if native::value(invocation, "remote_epoch").and_then(|value| value.parse().ok())
        != Some(authority.remote_epoch)
        || binding.decode_route(cell(invocation, "route")?)?.as_ref() != &authority.route
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}
fn shared_source_in_frame(
    before: records::FrozenRows<'_>,
    capture: &crate::pinning::management::ObjectCapture,
    authority: &records::JobAuthority,
) -> Result<bool> {
    let owner = authority
        .owner
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let source = authority
        .source
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    if source == owner {
        return Ok(true);
    }
    if owner != &capture.owner_identity || source != &capture.object.identity {
        return Ok(false);
    }
    let version = before
        .row("object_versions", &[&source.private_version_id])
        .ok_or(ManagementError::Confirmation)?;
    let object = before
        .row("objects", &[&source.object_id])
        .ok_or(ManagementError::Confirmation)?;
    let target = before
        .row(
            "pin_lease_targets",
            &[authority
                .target_id
                .as_deref()
                .ok_or(ManagementError::Confirmation)?],
        )
        .ok_or(ManagementError::Confirmation)?;
    Ok(
        native::value(version, "object_id") == Some(&source.object_id)
            && native::value(version, "bucket") == Some(&source.bucket)
            && native::value(version, "key") == Some(&source.key)
            && native::value(version, "version_id")
                == match &source.public_version {
                    crate::pinning::management::PublicVersionId::Null => None,
                    crate::pinning::management::PublicVersionId::Version(version) => {
                        Some(version.as_str())
                    }
                }
            && native::value(object, "cid")
                .and_then(|cid| crate::pinning::provider::canonical_resource_cid(cid).ok())
                .as_ref()
                == Some(&source.cid)
            && native::value(target, "lease_id") == authority.lease_id.as_deref()
            && native::value(target, "provider") == Some(&authority.provider)
            && native::value(target, "cid") == Some(&authority.cid),
    )
}
fn validate_resource_proof(
    binding: records::BindingView<'_>,
    _action: &records::CommittedAction,
    authority: &records::JobAuthority,
) -> Result<()> {
    let frame = binding.frozen_rows()?;
    validate_resource_proof_at(binding, authority, frame)
}

fn validate_resource_proof_at(
    binding: records::BindingView<'_>,
    authority: &records::JobAuthority,
    frame: records::FrozenRows<'_>,
) -> Result<()> {
    if authority.resource_key.table == "remote_pins" {
        let remote = frame
            .row("remote_pins", &[&authority.provider, &authority.cid])
            .ok_or(ManagementError::Confirmation)?;
        let ledger = frame
            .row("remote_pin_ledger", &[&authority.provider, &authority.cid])
            .ok_or(ManagementError::Confirmation)?;
        let original_epoch = native::value(remote, "epoch")
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or(ManagementError::Confirmation)?;
        let mut closures = 0usize;
        for entry in binding.confirmation.bindings {
            if matches!(
                entry.request.action,
                crate::pinning::management::Action::Cancel
            ) && let Some(Capture::Object(capture)) = &entry.capture
                && cancel_resource_in_frame(binding, capture, authority)?
            {
                closures += 1;
            }
        }
        let cancellation = closures > 0
            && matches!(
                authority.directive,
                records::JobDirective::ReadOnlyObserve | records::JobDirective::ExistingCleanup
            );
        let creations=binding.confirmation.bindings.iter().filter(|entry|matches!(entry.request.action,crate::pinning::management::Action::Create(_)|crate::pinning::management::Action::Backfill(_)|crate::pinning::management::Action::Reapply(_))&&matches!(&entry.capture,Some(Capture::Object(capture)) if crate::pinning::provider::canonical_resource_cid(&authority.cid).ok().as_deref()==Some(&capture.object.identity.cid)&&capture.requirement_routes.iter().chain(capture.remote_route.iter()).any(|route|route.original.as_ref()==Some(&authority.route)))).count();
        let observation =
            authority.directive == records::JobDirective::ReadOnlyObserve && creations > 0;
        let maximum_delta = if observation {
            closures + creations
        } else {
            closures
        };
        let epoch_matches = authority.remote_epoch == original_epoch
            || (cancellation || observation)
                && authority.remote_epoch >= original_epoch
                && original_epoch
                    .checked_add(maximum_delta as i64)
                    .is_some_and(|maximum| authority.remote_epoch <= maximum);
        if !epoch_matches
            || native::value(remote, "request_id") != authority.resource_id.as_deref()
            || binding.decode_route(cell(ledger, "route")?)?.as_ref() != &authority.route
        {
            return Err(ManagementError::Confirmation);
        }
    } else {
        let epoch = authority.remote_epoch.to_string();
        let envelope: HistoryEnvelope = binding.decode(
            frame
                .original_value(
                    "pin_resource_history",
                    &[&authority.provider, &authority.cid, &epoch],
                    "ledger",
                )
                .ok_or(ManagementError::Confirmation)?,
        )?;
        if envelope.request_id != authority.resource_id
            || binding.decode_route(&envelope.ledger.route)?.as_ref() != &authority.route
        {
            return Err(ManagementError::Confirmation);
        }
    }
    Ok(())
}

fn cancel_resource_in_frame(
    binding: records::BindingView<'_>,
    capture: &crate::pinning::management::ObjectCapture,
    authority: &records::JobAuthority,
) -> Result<bool> {
    let Some(target) = &capture.target else {
        return Ok(false);
    };
    let frame = binding.frozen_rows()?;
    let lease = frame
        .row("pin_leases", &[&target.lease.id])
        .ok_or(ManagementError::Confirmation)?;
    if native::value(lease, "owner_object_id") != Some(&capture.owner_identity.object_id) {
        return Err(ManagementError::Confirmation);
    }
    let attached = frame
        .table("pin_lease_targets")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
        .any(|row| {
            native::value(row, "lease_id") == Some(&target.lease.id)
                && native::value(row, "provider") == Some(&authority.provider)
                && native::value(row, "cid") == Some(&authority.cid)
        });
    let route = frame
        .row("remote_pin_ledger", &[&authority.provider, &authority.cid])
        .and_then(|row| native::value(row, "route"))
        .map(|encoded| binding.decode_route(encoded))
        .transpose()?;
    Ok(attached && route.as_deref() == Some(&authority.route))
}
#[derive(serde::Deserialize)]
struct HistoryEnvelope {
    request_id: Option<String>,
    ledger: HistoryRoute,
}
#[derive(serde::Deserialize)]
struct HistoryRoute {
    route: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn reference_epoch_and_poll_successor_preserve_source_but_release_ends_permission() {
        use sea_orm::{ActiveModelTrait, IntoActiveModel, TransactionTrait};
        let fixture = super::super::tests::fixture("epoch-poll").await;
        let applied = super::super::apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding,
        )
        .await
        .unwrap();
        let original = applied.items[0]
            .jobs
            .iter()
            .find(|job| job.directive == records::JobDirective::Submit)
            .unwrap()
            .clone();
        let tx = fixture.db.begin().await.unwrap();
        let remote =
            remote_pin::Entity::find_by_id((original.provider.clone(), original.cid.clone()))
                .one(&tx)
                .await
                .unwrap()
                .unwrap();
        let mut remote = remote.into_active_model();
        remote.epoch = Set(original.remote_epoch + 1);
        remote.update(&tx).await.unwrap();
        let now = crate::store::database_clock::database_now(&tx)
            .await
            .unwrap();
        let submit = pin_job::Entity::find_by_id(original.job_id.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let mut active = submit.clone().into_active_model();
        active.state = Set("running".into());
        active.locked_until = Set(Some(now + chrono::Duration::minutes(1)));
        active.submit_phase = Set(Some("ready".into()));
        let model = active.update(&tx).await.unwrap();
        let claimed = crate::store::pinning::jobs::ClaimedPinJob {
            model,
            reclaimed: false,
            previous_state: "pending".into(),
            object_id: original.owner.as_ref().map(|owner| owner.object_id.clone()),
        };
        assert!(matches!(
            check_before_call(&tx, &claimed.model).await.unwrap(),
            Some(JobPermit::Submit(_))
        ));
        let mut recovering = claimed.model.clone().into_active_model();
        recovering.submit_phase = Set(Some("recovering".into()));
        recovering.update(&tx).await.unwrap();
        assert!(
            matches!(
                check_before_call(&tx, &claimed.model).await.unwrap(),
                Some(JobPermit::LedgerOnly(_))
            ),
            "a missing history on recovery is not a zero-POST proof"
        );
        let mut ready = claimed.model.clone().into_active_model();
        ready.submit_phase = Set(Some("ready".into()));
        ready.update(&tx).await.unwrap();
        assert!(matches!(
            crate::store::pinning::jobs::prepare_submit_call(&tx, &claimed, now)
                .await
                .unwrap(),
            crate::store::pinning::jobs::SubmitCallDecision::ReadyToCall
        ));
        crate::store::pinning::jobs::record_submit_invocation(
            &tx,
            &claimed,
            &original.route.api_profile,
            &original.route.strategy,
            now,
        )
        .await
        .unwrap();
        assert!(lookup(&tx, &claimed.model).await.unwrap().as_ref() == Some(&original));
        assert!(matches!(
            check_before_call(&tx, &claimed.model).await.unwrap(),
            Some(JobPermit::Submit(_))
        ));
        let history = crate::store::pinning::jobs::submission_history(&tx, &original.job_id)
            .await
            .unwrap()
            .unwrap();
        let mut blocked = history.clone().into_active_model();
        blocked.state = Set("needs_attention".into());
        blocked.update(&tx).await.unwrap();
        assert!(matches!(
            check_before_call(&tx, &claimed.model).await.unwrap(),
            Some(JobPermit::LedgerOnly(_))
        ));
        let mut active = history.into_active_model();
        active.state = Set("active".into());
        active.update(&tx).await.unwrap();
        let remote =
            remote_pin::Entity::find_by_id((original.provider.clone(), original.cid.clone()))
                .one(&tx)
                .await
                .unwrap()
                .unwrap();
        let mut active = remote.into_active_model();
        active.request_id = Set(Some("exact-poll-successor".into()));
        active.status = Set("queued".into());
        active.update(&tx).await.unwrap();
        let poll = crate::store::pinning::jobs::poll_job(
            &original.provider,
            &original.cid,
            original.lease_id.as_deref().unwrap(),
            original.target_id.as_deref().unwrap(),
            original.lease_generation.unwrap(),
            "exact-poll-successor",
            now,
        );
        let crate::store::pinning::jobs::NewPinJob::Target(target) = &poll else {
            unreachable!()
        };
        let id = target.id.clone();
        crate::store::pinning::jobs::insert_new_job(&tx, poll, now)
            .await
            .unwrap();
        let job = pin_job::Entity::find_by_id(id)
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let successor = lookup(&tx, &job).await.unwrap().unwrap();
        assert!(
            successor.owner == original.owner
                && successor.source == original.source
                && successor.lease_source == original.lease_source
                && successor.lease_contract == original.lease_contract
        );
        assert_eq!(successor.remote_epoch, original.remote_epoch);
        let mut stable_seed = original.clone();
        stable_seed.job_id = job.id.clone();
        assert!(
            stable_seed == successor,
            "C's strict seed membership must not be widened"
        );
        assert!(matches!(
            check_before_call(&tx, &job).await.unwrap(),
            Some(JobPermit::Submit(_))
        ));
        let submit = pin_job::Entity::find_by_id(original.job_id.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        // This is an old Submit observation, not a new POST to the now-queued resource.
        assert!(matches!(
            check_before_call(&tx, &submit).await.unwrap(),
            Some(JobPermit::LedgerOnly(_))
        ));
        crate::store::entities::pin_resource_history::Entity::insert(crate::store::entities::pin_resource_history::ActiveModel {provider:Set(original.provider.clone()),cid:Set(original.cid.clone()),epoch:Set(original.remote_epoch),ledger:Set(serde_json::json!({"confirmed_absent":true,"request_id":original.resource_id,"ledger":{"route":serde_json::to_string(&original.route).unwrap()}}).to_string())}).exec(&tx).await.unwrap();
        assert!(matches!(
            check_before_call(&tx, &submit).await.unwrap(),
            Some(JobPermit::LedgerOnly(_))
        ));
        assert!(lookup(&tx, &job).await.unwrap().as_ref() == Some(&stable_seed));
        assert!(matches!(
            check_before_call(&tx, &job).await.unwrap(),
            Some(JobPermit::LedgerOnly(_))
        ));
        tx.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn compatible_shared_authority_meets_and_conflicting_generation_epoch_route_fail_closed()
    {
        let fixture = super::super::tests::fixture("compatible-meet").await;
        let applied = super::super::apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding,
        )
        .await
        .unwrap();
        let authority = applied.items[0].jobs[0].clone();
        let mut found = None;
        meet(&mut found, authority.clone()).unwrap();
        meet(&mut found, authority.clone()).unwrap();
        assert!(found.as_ref() == Some(&authority));
        for changed in [0, 1, 2, 3] {
            let mut conflicting = authority.clone();
            match changed {
                0 => {
                    conflicting.lease_generation = conflicting
                        .lease_generation
                        .map(|generation| generation + 1)
                }
                1 => conflicting.remote_epoch += 1,
                2 => conflicting.route.scope = "different-account".into(),
                _ => conflicting.lease_source = Some("different-original-source".into()),
            }
            assert!(meet(&mut found, conflicting).is_err());
            assert!(found.as_ref() == Some(&authority));
        }
    }

    #[tokio::test]
    async fn shared_native_submit_indexes_only_its_request_bound_poll_and_preserves_all_seed_fields()
     {
        shared_poll_seed_integrity(true).await;
    }

    #[tokio::test]
    async fn shared_native_poll_missing_original_submit_or_history_fails_closed() {
        shared_poll_seed_integrity(false).await;
    }

    async fn shared_poll_seed_integrity(include_unrelated_action: bool) {
        use sea_orm::{ActiveModelTrait, IntoActiveModel, TransactionTrait};
        let mut fixture =
            super::super::tests::fixture_with_automatic("shared-poll-index", true).await;
        let native_source = pin_job::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let now = crate::store::database_clock::database_now(&fixture.db)
            .await
            .unwrap();
        // A controlled rejected native source, not an unknown reservation. These
        // durable fence rows do not stand in for provider IO evidence.
        fixture.db.execute_unprepared("UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='rejected'").await.unwrap();
        fixture
            .db
            .execute_unprepared("UPDATE remote_pin_ledger SET effect='not_created'")
            .await
            .unwrap();
        fixture.db.execute(sea_orm::Statement::from_sql_and_values(fixture.db.get_database_backend(),"INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','not_created','blocked',1,0,$2)",[native_source.id.clone().into(),now.into()])).await.unwrap();
        let parked = pin_job::Entity::find_by_id(native_source.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert!(
            crate::store::pinning::jobs::management_retryable_park(
                &fixture.db,
                &parked,
                &fixture.config.pinning.providers[0].route_snapshot()
            )
            .await
            .unwrap()
        );
        let mut request = (*fixture.binding.request).clone();
        request.plan_id = "shared-rejected-source".into();
        let snapshot = crate::pinning::management::db_snapshot::collect_with_config(
            &fixture.db,
            &fixture.config,
            &request,
        )
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
        let plan = crate::pinning::management::dry_run(&request, &snapshot).unwrap();
        let ack = plan
            .items()
            .iter()
            .filter(|entry| entry.status == crate::pinning::management::PlanStatus::Candidate)
            .map(|entry| entry.request.item_id.clone())
            .collect();
        fixture.binding = records::confirm(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            (&request, &snapshot, &plan),
            ack,
            chrono::Duration::minutes(5),
        )
        .await
        .unwrap();
        let applied = super::super::apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding,
        )
        .await
        .unwrap();
        let action = &applied.items[0];
        let seed = action
            .jobs
            .iter()
            .find(|authority| {
                authority.directive == records::JobDirective::Submit
                    && authority
                        .lease_id
                        .as_ref()
                        .is_some_and(|lease| !action.receipt.intent_ids.contains(lease))
            })
            .unwrap()
            .clone();
        assert_eq!(seed.job_id, native_source.id);
        assert_eq!(seed.lease_id, native_source.lease_id);
        assert_eq!(seed.target_id, native_source.target_id);
        assert_eq!(seed.lease_generation, native_source.expected_generation);
        let unrelated = if include_unrelated_action {
            let mut unrelated_request = (*fixture.binding.request).clone();
            unrelated_request.plan_id = "poll-unrelated".into();
            unrelated_request.items[0].item_id = "foreign".into();
            unrelated_request.items[0].action = crate::pinning::management::Action::Reconcile;
            let crate::pinning::management::Subject::Object {
                target: selection, ..
            } = &mut unrelated_request.items[0].subject
            else {
                unreachable!()
            };
            *selection = crate::pinning::management::TargetSelection::Existing {
                target_id: action.targets[0].clone(),
                source: None,
            };
            let unrelated_snapshot = crate::pinning::management::db_snapshot::collect_with_config(
                &fixture.db,
                &fixture.config,
                &unrelated_request,
            )
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
            let unrelated_plan =
                crate::pinning::management::dry_run(&unrelated_request, &unrelated_snapshot)
                    .unwrap();
            let unrelated_ack = unrelated_plan
                .items()
                .iter()
                .filter(|entry| entry.status == crate::pinning::management::PlanStatus::Candidate)
                .map(|entry| entry.request.item_id.clone())
                .collect();
            let unrelated_binding = records::confirm(
                &fixture.db,
                &fixture.config,
                &fixture.auth,
                (&unrelated_request, &unrelated_snapshot, &unrelated_plan),
                unrelated_ack,
                chrono::Duration::minutes(5),
            )
            .await
            .unwrap();
            let unrelated = super::super::apply_cancellable(
                &fixture.db,
                &fixture.config,
                &fixture.auth,
                &unrelated_binding,
                crate::store::pinning::management::transaction::CancellationToken::new(),
            )
            .await
            .unwrap_or_else(|failure| {
                panic!(
                    "unrelated formal action failed: {:?}, transaction={:?}",
                    failure.failure.cause, failure.transaction
                )
            });
            assert!(
                !unrelated.items[0]
                    .receipt
                    .intent_ids
                    .contains(seed.lease_id.as_ref().unwrap())
            );
            assert!(!unrelated.items[0].receipt.job_ids.contains(&seed.job_id));
            Some((unrelated_binding, unrelated))
        } else {
            None
        };
        let tx = fixture.db.begin().await.unwrap();
        let now = crate::store::database_clock::database_now(&tx)
            .await
            .unwrap();
        let source = pin_job::Entity::find_by_id(seed.job_id.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let mut running = source.clone().into_active_model();
        running.state = Set("running".into());
        running.locked_until = Set(Some(now + chrono::Duration::minutes(1)));
        running.submit_phase = Set(Some("ready".into()));
        let source = running.update(&tx).await.unwrap();
        let claimed = crate::store::pinning::jobs::ClaimedPinJob {
            model: source.clone(),
            reclaimed: false,
            previous_state: "pending".into(),
            object_id: seed.owner.as_ref().map(|owner| owner.object_id.clone()),
        };
        assert!(matches!(
            crate::store::pinning::jobs::prepare_submit_call(&tx, &claimed, now)
                .await
                .unwrap(),
            crate::store::pinning::jobs::SubmitCallDecision::ReadyToCall
        ));
        crate::store::pinning::jobs::record_submit_invocation(
            &tx,
            &claimed,
            &seed.route.api_profile,
            &seed.route.strategy,
            now,
        )
        .await
        .unwrap();
        // Native invocation is real; the queued request below is a fence fixture,
        // not a POST/PIN proof. C's controlled PSA worker regression covers IO.
        let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let mut queued = remote.into_active_model();
        queued.request_id = Set(Some("shared-original-request".into()));
        queued.status = Set("queued".into());
        queued.update(&tx).await.unwrap();
        let new_poll = crate::store::pinning::jobs::poll_job(
            &seed.provider,
            &seed.cid,
            seed.lease_id.as_deref().unwrap(),
            seed.target_id.as_deref().unwrap(),
            seed.lease_generation.unwrap(),
            "shared-original-request",
            now,
        );
        let crate::store::pinning::jobs::NewPinJob::Target(target) = &new_poll else {
            unreachable!()
        };
        let poll_id = target.id.clone();
        crate::store::pinning::jobs::insert_new_job(&tx, new_poll, now)
            .await
            .unwrap();
        let poll = pin_job::Entity::find_by_id(poll_id)
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert!(!action.receipt.job_ids.contains(&poll.id));
        assert!(
            !action
                .receipt
                .intent_ids
                .contains(seed.lease_id.as_ref().unwrap())
        );
        let mut expected = seed.clone();
        expected.job_id = poll.id.clone();
        assert!(lookup(&tx, &poll).await.unwrap().as_ref() == Some(&expected));
        assert!(
            matches!(check_before_call(&tx,&poll).await.unwrap(),Some(JobPermit::Submit(authority)) if authority==expected)
        );
        let reader =
            fence_worker_authority_with_work(&tx, &poll, records::SharedDecodeBudget::default())
                .await
                .unwrap();
        assert!(reader.projection_allowed().await.unwrap());
        let backfill = tx.begin().await.unwrap();
        backfill
            .execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
            .await
            .unwrap();
        assert!(reader.projection_allowed().await.is_err());
        assert!(reader.fresh_source_allowed(None, None).await.is_err());
        backfill.rollback().await.unwrap();
        if let Some((unrelated_binding, unrelated)) = &unrelated {
            // A real unrelated action retains its full private native document, but
            // its distinct lease/job memberships cannot admit this request-bound Poll.
            let unrelated_record =
                records::lookup_committed(&tx, &fixture.auth, unrelated_binding, "foreign")
                    .await
                    .unwrap()
                    .unwrap();
            assert!(unrelated_record == unrelated.items[0]);
            assert!(!unrelated_record.receipt.job_ids.contains(&poll.id));
            let indexed = crate::store::pinning::management::admission::authority::actions_for_job(
                &tx,
                &poll,
                records::SharedDecodeBudget::default(),
            )
            .await
            .unwrap();
            assert!(
                !indexed
                    .iter()
                    .any(|candidate| candidate.action_view().receipt.binding.id
                        == unrelated_record.receipt.binding.id)
            );
            assert!(lookup(&tx, &poll).await.unwrap().as_ref() == Some(&expected));
        }
        let mut wrong_generation = poll.clone();
        wrong_generation.expected_generation = Some(seed.lease_generation.unwrap() + 1);
        assert!(matches!(
            lookup(&tx, &wrong_generation).await,
            Ok(None) | Err(ManagementError::Confirmation | ManagementError::InvalidEvidence)
        ));
        let invocation = pin_invocation_route::Entity::find_by_id(poll.id.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let mut wrong_route = seed.route.clone();
        wrong_route.scope = "foreign-account".into();
        let mut changed = invocation.clone().into_active_model();
        changed.route = Set(serde_json::to_string(&wrong_route).unwrap());
        changed.update(&tx).await.unwrap();
        assert!(lookup(&tx, &poll).await.is_err());
        assert!(reader.projection_allowed().await.is_err());
        let mut restored = invocation.into_active_model();
        restored.route = Set(serde_json::to_string(&seed.route).unwrap());
        restored.update(&tx).await.unwrap();
        let invocation = pin_invocation_route::Entity::find_by_id(poll.id.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let mut changed = invocation.clone().into_active_model();
        changed.remote_epoch = Set(seed.remote_epoch - 1);
        changed.update(&tx).await.unwrap();
        assert!(lookup(&tx, &poll).await.is_err());
        let mut changed = invocation.clone().into_active_model();
        changed.remote_epoch = Set(invocation.remote_epoch + 1);
        changed.update(&tx).await.unwrap();
        assert!(
            matches!(check_before_call(&tx,&poll).await.unwrap(),Some(JobPermit::LedgerOnly(authority)) if authority==expected)
        );
        assert!(!reader.projection_allowed().await.unwrap());
        let mut restored = invocation.clone().into_active_model();
        restored.remote_epoch = Set(invocation.remote_epoch);
        restored.update(&tx).await.unwrap();
        let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let mut changed = remote.clone().into_active_model();
        changed.request_id = Set(Some("replacement-request".into()));
        changed.update(&tx).await.unwrap();
        assert!(matches!(
            lookup(&tx, &poll).await,
            Ok(None) | Err(ManagementError::Confirmation | ManagementError::InvalidEvidence)
        ));
        let mut restored = remote.into_active_model();
        restored.request_id = Set(Some("shared-original-request".into()));
        restored.update(&tx).await.unwrap();
        let lease = pin_lease::Entity::find_by_id(seed.lease_id.clone().unwrap())
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let mut changed = lease.clone().into_active_model();
        changed.source = Set("changed-native-source".into());
        changed.update(&tx).await.unwrap();
        assert!(
            lookup(&tx, &poll).await.unwrap().as_ref() == Some(&expected),
            "historical seed is immutable, not a new source grant"
        );
        assert!(
            matches!(check_before_call(&tx,&poll).await.unwrap(),Some(JobPermit::LedgerOnly(authority)) if authority==expected)
        );
        assert!(!reader.projection_allowed().await.unwrap());
        let original_source = lease.source.clone();
        let mut restored = lease.into_active_model();
        restored.source = Set(original_source);
        restored.update(&tx).await.unwrap();
        let mut changed = source.clone().into_active_model();
        changed.lease_id = Set(Some(action.receipt.intent_ids[0].clone()));
        changed.update(&tx).await.unwrap();
        assert!(matches!(
            lookup(&tx, &poll).await,
            Ok(None) | Err(ManagementError::Confirmation | ManagementError::InvalidEvidence)
        ));
        let original_lease = source.lease_id.clone();
        let mut restored = source.into_active_model();
        restored.lease_id = Set(original_lease);
        restored.update(&tx).await.unwrap();
        assert!(reader.projection_allowed().await.unwrap());
        let missing_history = tx.begin().await.unwrap();
        crate::store::pinning::jobs::history::Entity::delete_by_id(seed.job_id.clone())
            .exec(&missing_history)
            .await
            .unwrap();
        assert!(lookup(&missing_history, &poll).await.is_err());
        assert!(check_before_call(&missing_history, &poll).await.is_err());
        assert!(projection_allowed(&missing_history, &poll).await.is_err());
        assert!(reader.projection_allowed().await.is_err());
        assert!(reader.fresh_source_allowed(None, None).await.is_err());
        assert!(
            reader
                .projection_and_source_allowed(None, None)
                .await
                .is_err()
        );
        missing_history.rollback().await.unwrap();
        pin_job::Entity::delete_by_id(seed.job_id.clone())
            .exec(&tx)
            .await
            .unwrap();
        assert!(lookup(&tx, &poll).await.is_err());
        assert!(check_before_call(&tx, &poll).await.is_err());
        assert!(projection_allowed(&tx, &poll).await.is_err());
        assert!(reader.projection_allowed().await.is_err());
        assert!(reader.fresh_source_allowed(None, None).await.is_err());
        assert!(
            reader
                .projection_and_source_allowed(None, None)
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
    }
}
