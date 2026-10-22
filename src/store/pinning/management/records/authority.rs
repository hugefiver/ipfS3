//! Exact immutable local confirmation authority, without fresh execution checks.
use super::*;

/// Resolve the complete original plan and its explicit archive dependencies.
/// `request` selects a local original; it cannot supply confirmation or results.
/// Expiry and current configuration are deliberately left to apply, after exact
/// committed-receipt replay has been attempted.
pub async fn load_confirmed_authority<C: ConnectionTrait>(
    db: &C,
    authorization: &LocalAuthorization,
    request: &PlanRequest,
    work: SharedDecodeBudget,
) -> Result<ConfirmedBinding> {
    authorization.check(db).await?;
    crate::pinning::management::validate_request(request)
        .map_err(|_| ManagementError::Confirmation)?;
    let source = evidence::Evidence::load_with_work(
        db,
        authorization.database_scope(),
        authorization.database_identity(),
        std::slice::from_ref(&request.plan_id),
        work,
    )
    .await?;
    let root = source
        .archive_members(authorization.database_scope(), &request.plan_id)
        .next()
        .ok_or(ManagementError::Confirmation)?;
    let binding = source.read_binding(root)?;
    if binding.database_scope != authorization.database_scope()
        || binding.database_identity != authorization.database_identity()
        || binding.principal != authorization.principal()
        || &*binding.request != request
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(binding)
}

pub struct CheckpointEvidence<'a> {
    pub origin: &'a crate::pinning::management::retirement::SourceCheckpoint,
    pub inventories: &'a [crate::pinning::management::retirement::RetirementInventoryRef],
    pub configuration: &'a crate::pinning::management::ConfigurationBinding,
}

/// Read the original committed confirmation and its exact recorded self-transition.
/// Unrelated live changes do not renew expiry or grant authority to apply.
pub async fn load_checkpoint_confirmation(
    db: &sea_orm::DatabaseConnection,
    authorization: &LocalAuthorization,
    request: &PlanRequest,
    evidence: CheckpointEvidence<'_>,
    limits: &crate::pinning::management::retirement::RunLimits,
    work: SharedDecodeBudget,
) -> Result<Option<ConfirmedBinding>> {
    let CheckpointEvidence {
        origin,
        inventories,
        configuration,
    } = evidence;
    use crate::store::pinning::management::transaction::{
        ManagementTransaction, TransactionFailure,
    };
    let mut transaction =
        ManagementTransaction::begin_read_only(db, limits.remaining(), limits.cancellation.clone())
            .await
            .map_err(TransactionFailure::management_error)?;
    let result = transaction.run_with(
        (authorization, request, origin, inventories, configuration, &work),
        |tx, _, (auth, request, origin, inventories, configuration, work)| Box::pin(async move {
            DecodeWork::shared(work).with(|session|session.check())?;
            auth.check(tx).await?;
            DecodeWork::shared(work).index_cells(1,0)?;
            let exists = tx.query_one(Statement::from_sql_and_values(
                tx.get_database_backend(),
                "SELECT 1 AS present FROM pin_management_records WHERE database_scope=$1 AND plan_id=$2 AND kind='action' LIMIT 1",
                [auth.database_scope().into(), request.plan_id.clone().into()],
            )).await.map_err(|_|ManagementError::Database)?.is_some();
            if !exists { return Ok(None); }
            let binding = load_confirmed_authority(tx, auth, request, std::sync::Arc::clone(work)).await?;
            if binding.review().planning_view().configuration.as_ref() != Some(*configuration) {
                return Err(ManagementError::Changed);
            }
            inventory_review::check_checkpoint_confirmation(tx, &binding, origin, inventories).await?;
            Ok(Some(binding))
        }),
    ).await.map_err(TransactionFailure::management_error);
    transaction
        .rollback()
        .await
        .map_err(TransactionFailure::management_error)?;
    result
}
