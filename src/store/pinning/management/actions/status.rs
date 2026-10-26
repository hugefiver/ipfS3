//! Current status is evidence, not a renewed confirmation or lease capability.
use super::*;
use crate::{
    pinning::{
        identity::RemoteResourceType,
        management::{Action, Capture},
    },
    store::entities::{pin_job, pin_lease, pin_lease_requirement, pin_lease_target, remote_pin},
};
use sea_orm::{ConnectionTrait, DatabaseTransaction};
mod observations;
mod recorded;
mod retirement;
pub use recorded::{RecordedItemStatus, RecordedRetirementStatus, recorded_with_work};
pub use retirement::{RetirementStatusLookup, lookup_with_work as lookup_retirement_with_work};
#[cfg(test)]
mod retirement_tests;

#[derive(serde::Deserialize)]
struct StatusContract {
    provider_mode: String,
    candidates:
        std::collections::BTreeMap<String, Option<crate::pinning::identity::ProviderRouteSnapshot>>,
    management_requirements: Option<crate::pinning::management::Requirements>,
}

pub(super) async fn fence_business_expiry<'a>(
    tx: &DatabaseTransaction,
    binding: records::BindingView<'_>,
    items: impl Iterator<Item = &'a records::CommittedAction>,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    for item in items {
        let action = &item.receipt.binding.request.action;
        if matches!(
            action,
            Action::Cancel | Action::Reconcile | Action::Retire | Action::RetireWith { .. }
        ) {
            continue;
        }
        // An unknown/inflight Retry/Migrate can only create readonly directives.
        if matches!(action, Action::Retry | Action::Migrate { .. })
            && !item.jobs.is_empty()
            && item
                .jobs
                .iter()
                .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
            && item.migration_points.is_empty()
        {
            continue;
        }
        if let Some(Capture::Object(capture)) = &item.receipt.binding.capture {
            if matches!(action,Action::Create(_)|Action::Backfill(_)|Action::Reapply(_)) {
                let summary=capture.object.read_evidence.as_ref().ok_or(ManagementError::InvalidEvidence)?;
                let proof=work.with(|work|accounting::trusted_proof_with_work(binding.frozen_rows()?,summary,work))?;
                if proof.identity!=capture.object.identity||!proof.proof.has_stored_byte_count(){return Err(ManagementError::InvalidEvidence);}
                if proof.observed_at>now||proof.expires_at<=now{return Err(ManagementError::Expired);}
            }else if let Some(proof)=&capture.object.read_evidence&&proof.proof==crate::pinning::management::evidence::ReadProof::CompleteLocalDirectoryDagBlocksV1
                &&(proof.observed_at>now||proof.expires_at<=now){return Err(ManagementError::Expired);}
            if let Some(target) = &capture.target {
                let tuple = target
                    .lease
                    .native_tuple
                    .as_ref()
                    .ok_or(ManagementError::InvalidEvidence)?;
                if native::parse_time(
                    native::value(tuple, "expires_at").ok_or(ManagementError::InvalidEvidence)?,
                )? <= now
                {
                    return Err(ManagementError::Expired);
                }
            }
        }
        for id in &item.receipt.intent_ids {
            let lease = records::current_model::<pin_lease::Entity, _>(tx, &[id], work)
                .await?
                .ok_or(ManagementError::Changed)?;
            if lease.state != "active" || lease.expires_at <= now {
                return Err(ManagementError::Expired);
            }
        }
    }
    Ok(())
}

/// Current non-retirement status. Use `lookup_retirement_with_work` for a
/// retirement action's bounded, connection-level responsibility scan.
pub async fn lookup<C: ConnectionTrait>(
    db: &C,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    item_id: &str,
) -> Result<Option<records::CommittedAction>> {
    lookup_with_work(
        db,
        auth,
        binding,
        item_id,
        records::SharedDecodeBudget::default(),
    )
    .await
}
pub async fn lookup_with_work<C: ConnectionTrait>(
    db: &C,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    item_id: &str,
    work: records::SharedDecodeBudget,
) -> Result<Option<records::CommittedAction>> {
    let Some(trusted) =
        records::lookup_trusted_with_work(db, auth, binding, item_id, work.clone()).await?
    else {
        return Ok(None);
    };
    // Current retirement responsibility requires an outer connection scan, not
    // a transaction-local or frozen-state fallback.
    if is_retirement(trusted.action_view()) {
        return Err(ManagementError::InvalidEvidence);
    }
    trusted.binding().copy(trusted.action_view())?;
    let mut action = trusted.action_view().clone();
    action.state = current_state_with_work(db, &action, work).await?;
    Ok(Some(action))
}

/// Evaluates only bounded current rows. Retirement executors already assign a
/// committed state; dynamic responsibility belongs to the outer status scan.
pub(crate) async fn current_state_with_work<C: ConnectionTrait>(
    db: &C,
    action: &records::CommittedAction,
    work: records::SharedDecodeBudget,
) -> Result<records::ActionState> {
    current_state_catalog(
        crate::store::pinning::legacy_migration::ReceiptCatalog::full(db),
        action,
        work,
    )
    .await
}

pub(crate) async fn current_state_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    action: &records::CommittedAction,
    work: records::SharedDecodeBudget,
) -> Result<records::ActionState> {
    let db = &catalog;
    use records::ActionState::*;
    let context = records::DecodeWork::shared(&work);
    if matches!(action.receipt.binding.request.action, Action::Cancel) {
        return Ok(Accepted);
    }
    if is_retirement(action) {
        return Ok(action.state);
    }
    if observations::needs_attention(db, &action.jobs, work.clone()).await? {
        return Ok(NeedsAttention);
    }
    let now = crate::store::database_clock::database_now(db)
        .await
        .map_err(|_| ManagementError::Database)?;
    let mut complete = !action.receipt.intent_ids.is_empty();
    let mut replica_required = false;
    for id in &action.receipt.intent_ids {
        let lease = records::current_model::<pin_lease::Entity, _>(db, &[id], context)
            .await?
            .ok_or(ManagementError::Changed)?;
        if lease.state != "active" || lease.expires_at <= now {
            return Ok(NeedsAttention);
        }
        let contract =
            records::current_model::<pin_lease_requirement::Entity, _>(db, &[id], context)
                .await?
                .ok_or(ManagementError::InvalidEvidence)?;
        let birth: StatusContract = context.with(|work| work.typed(&contract.contract))?;
        replica_required |= birth
            .management_requirements
            .as_ref()
            .is_some_and(|requirements| {
                requirements
                    .levels
                    .contains(&crate::pinning::management::ConfirmationLevel::ReplicationComplete)
            })
            || !birth.candidates.is_empty()
                && birth.candidates.values().all(|route| {
                    route.as_ref().is_some_and(|route| {
                        route.resource_type() == RemoteResourceType::ClusterPin
                    })
                });
        let Some((any, all, _)) =
            pin_lease_requirement::contract::completion_catalog(catalog, id, now, work.clone())
                .await?
        else {
            return Err(ManagementError::InvalidEvidence);
        };
        complete &= if birth.provider_mode == "all" {
            all
        } else {
            any
        };
    }
    // The completion consumer verifies every frozen slot/CID, actual target,
    // epoch/route ledger evidence and original Cluster threshold/freshness.
    if matches!(action.receipt.binding.request.action, Action::Reconcile) {
        return Ok(ReadOnly);
    }
    if complete {
        return Ok(if replica_required {
            ReplicationComplete
        } else {
            RemoteComplete
        });
    }
    if !action.jobs.is_empty()
        && action
            .jobs
            .iter()
            .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
    {
        return Ok(ReadOnly);
    }
    for authority in &action.jobs {
        if let Some(job) =
            records::current_model::<pin_job::Entity, _>(db, &[&authority.job_id], context).await?
            && matches!(job.state.as_str(), "pending" | "retry" | "running")
        {
            return Ok(Queued);
        }
    }
    for id in &action.targets {
        let target = records::current_model::<pin_lease_target::Entity, _>(db, &[id], context)
            .await?
            .ok_or(ManagementError::Changed)?;
        if matches!(
            target.state.as_str(),
            "quota_waiting" | "quota_blocked" | "failed"
        ) {
            return Ok(NeedsAttention);
        }
        if let Some(remote) = records::current_model::<remote_pin::Entity, _>(
            db,
            &[&target.provider, &target.cid],
            context,
        )
        .await?
            && matches!(remote.status.as_str(), "queued" | "pinning")
        {
            return Ok(Waiting);
        }
    }
    Ok(Accepted)
}

fn is_retirement(action: &records::CommittedAction) -> bool {
    matches!(
        action.receipt.binding.request.action,
        Action::Retire | Action::RetireWith { .. }
    )
}
