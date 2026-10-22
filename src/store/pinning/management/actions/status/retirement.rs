//! A retirement barrier is durable; its outstanding responsibility is current.
use super::*;
use crate::pinning::{
    identity::ProviderRouteSnapshot,
    management::{
        RetirementSelection, Subject,
        retirement::{CurrentRetirementScan, RunLimits},
    },
};
use std::{fs::File, path::Path};

/// Owns one verified action and its task-local scan. Progress is not a status
/// result, and a completed scan cannot be reused for a later current lookup.
pub struct RetirementStatusLookup {
    scan: CurrentRetirementScan,
    original: Option<records::TrustedAppliedAction>,
    work: records::SharedDecodeBudget,
}

/// Verifies the original committed action before starting a fresh current scan.
/// The local artifact adapter owns the protected path/reservation and cleanup;
/// neither may be taken from an imported plan. No confirmation TTL is renewed.
#[allow(clippy::too_many_arguments)]
pub async fn lookup_with_work(
    db: &DatabaseConnection,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    item_id: &str,
    path: &Path,
    reservation: File,
    limits: &RunLimits,
    work: records::SharedDecodeBudget,
) -> Result<Option<RetirementStatusLookup>> {
    let mut tx = ManagementTransaction::begin(
        db,
        transaction::FINAL_WRITE_LIMIT.min(limits.elapsed),
        limits.cancellation.clone(),
    )
    .await
    .map_err(TransactionFailure::management_error)?;
    let original = tx
        .run_with(
            (auth, binding, item_id, work.clone()),
            |tx, _, (auth, binding, item_id, work)| {
                Box::pin(async move {
                    let Some(trusted) =
                        records::lookup_trusted_with_work(tx, auth, binding, item_id, work.clone())
                            .await?
                    else {
                        return Ok(None);
                    };
                    let action = trusted.action_view();
                    if !is_retirement(action) {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    let Subject::Provider { route } = &action.receipt.binding.request.subject
                    else {
                        return Err(ManagementError::InvalidEvidence);
                    };
                    let configuration = trusted
                        .binding()
                        .confirmation
                        .configuration
                        .as_ref()
                        .ok_or(ManagementError::InvalidEvidence)?;
                    let context = records::DecodeWork::shared(work);
                    context.copy(&(route, configuration))?;
                    let (route, configuration) = (route.clone(), configuration.clone());
                    Ok(Some((trusted, route, configuration)))
                })
            },
        )
        .await
        .map_err(TransactionFailure::management_error)?;
    tx.rollback()
        .await
        .map_err(TransactionFailure::management_error)?;
    let Some((original, route, configuration)) = original else {
        return Ok(None);
    };
    let scan =
        CurrentRetirementScan::start(db, route, configuration, path, reservation, limits).await?;
    Ok(Some(RetirementStatusLookup {
        scan,
        original: Some(original),
        work,
    }))
}

impl RetirementStatusLookup {
    pub async fn close(&self) -> Result<()> {
        self.scan.close().await
    }
    /// None is bounded, resumable progress. Resume with fresh invocation limits;
    /// source drift, cancellation and budget errors do not authorize a write.
    pub async fn advance(
        &mut self,
        db: &DatabaseConnection,
        limits: &RunLimits,
    ) -> Result<Option<records::CommittedAction>> {
        let Some((fence, state)) = self.scan.prepare_action_status(db, limits).await? else {
            return Ok(None);
        };
        let original = self
            .original
            .take()
            .ok_or(ManagementError::InvalidEvidence)?;
        let mut tx = ManagementTransaction::begin(
            db,
            transaction::FINAL_WRITE_LIMIT.min(limits.elapsed),
            limits.cancellation.clone(),
        )
        .await
        .map_err(TransactionFailure::management_error)?;
        let result = tx
            .run_with((limits, self.work.clone()), move |tx, _, (limits, work)| {
                Box::pin(async move {
                    fence.current_status(tx, limits).await?;
                    let context = records::DecodeWork::shared(work);
                    context.copy(original.record_key())?;
                    let keys = [original.record_key().clone()];
                    let current =
                        records::actions_at_keys_with_work(tx, &keys, work.clone()).await?;
                    let current = current.first().ok_or(ManagementError::Changed)?;
                    if current.facts_identity()? != original.facts_identity()?
                        || current.action_view() != original.action_view()
                        || current.binding().principal != original.binding().principal
                        || current.binding().database_identity
                            != original.binding().database_identity
                    {
                        return Err(ManagementError::Changed);
                    }
                    context.copy(current.action_view())?;
                    let mut action = current.action_view().clone();
                    action.state = if state == records::ActionState::NeedsAttention
                        || observations::needs_attention(tx, &action.jobs, work.clone()).await?
                    {
                        records::ActionState::NeedsAttention
                    } else {
                        destinations(
                            tx,
                            &action.receipt.binding.request.action,
                            &action.targets,
                            state,
                            work.clone(),
                        )
                        .await?
                    };
                    fence.current_status(tx, limits).await?;
                    Ok(action)
                })
            })
            .await
            .map_err(TransactionFailure::management_error)?;
        tx.rollback()
            .await
            .map_err(TransactionFailure::management_error)?;
        Ok(Some(result))
    }
}

pub(super) async fn destinations<C: ConnectionTrait>(
    db: &C,
    action: &Action,
    targets: &[String],
    state: records::ActionState,
    work: records::SharedDecodeBudget,
) -> Result<records::ActionState> {
    use records::ActionState::NeedsAttention;
    if !matches!(
        action,
        Action::RetireWith {
            selection: RetirementSelection::Migrate { .. },
        }
    ) {
        return Ok(state);
    }
    let context = records::DecodeWork::shared(&work);
    let now = crate::store::database_clock::database_now(db)
        .await
        .map_err(|_| ManagementError::Database)?;
    for id in targets {
        let Some(target) =
            records::current_model::<pin_lease_target::Entity, _>(db, &[id], context).await?
        else {
            return Ok(NeedsAttention);
        };
        let Some(lease) =
            records::current_model::<pin_lease::Entity, _>(db, &[&target.lease_id], context)
                .await?
        else {
            return Ok(NeedsAttention);
        };
        if target.state != "pinned" || lease.state != "active" || lease.expires_at <= now {
            return Ok(NeedsAttention);
        }
        let key = [target.provider.as_str(), target.cid.as_str()];
        let Some(remote) =
            records::current_model::<remote_pin::Entity, _>(db, &key, context).await?
        else {
            return Ok(NeedsAttention);
        };
        let Some(ledger) = records::current_model::<
            crate::store::entities::remote_pin_ledger::Entity,
            _,
        >(db, &key, context)
        .await?
        else {
            return Ok(NeedsAttention);
        };
        let route: ProviderRouteSnapshot = context.with(|work| {
            work.typed(
                ledger
                    .route
                    .as_deref()
                    .ok_or(ManagementError::InvalidEvidence)?,
            )
        })?;
        if remote.status != "pinned"
            && !(route.resource_type() == RemoteResourceType::ClusterPin
                && remote.status == "pinning")
        {
            return Ok(NeedsAttention);
        }
        if !pin_lease_requirement::contract::target_confirmed_with_work(
            db,
            &target.lease_id,
            &target.provider,
            &target.cid,
            now,
            work.clone(),
        )
        .await?
        {
            return Ok(NeedsAttention);
        }
    }
    Ok(state)
}
