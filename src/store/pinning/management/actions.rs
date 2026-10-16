//! Formal Stage 7 Store apply. Provider IO is deliberately outside this module.
use super::{ManagementError, Result, native, records};
use crate::pinning::management::{RevalidationOutcome, local_config::ManagementConfig};
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseBackend, DatabaseConnection, IsolationLevel,
    TransactionTrait,
};

pub(crate) mod accounting;
pub(crate) mod execution;
mod executor;
#[cfg(test)]
mod fault_injection;
mod fence;
mod intents;
pub mod job_authority;
mod migration;
pub mod points;
pub mod provider_points;
mod recovery;
mod retirement;
mod roots;
pub mod status;
#[cfg(test)]
pub(crate) mod tests;

#[derive(Clone, PartialEq, Eq)]
pub struct ApplyResult {
    pub replayed: bool,
    pub items: Vec<records::CommittedAction>,
}

/// A new write transaction per invocation; never reuse a dry-run RR snapshot.
/// Commit-response failure is resolved by the exact durable receipts first.
pub async fn apply(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
) -> Result<ApplyResult> {
    apply_detailed(db, config, auth, binding)
        .await
        .map_err(|failure| failure.cause)
}

async fn apply_attempt(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    failure_state: &mut records::ActionState,
    work: records::SharedDecodeBudget,
) -> Result<ApplyResult> {
    let tx = if db.get_database_backend() == DatabaseBackend::Postgres {
        db.begin_with_config(
            Some(IsolationLevel::ReadCommitted),
            Some(AccessMode::ReadWrite),
        )
        .await
    } else {
        db.begin().await
    }
    .map_err(|_| ManagementError::Database)?;
    let result = apply_in_transaction_with_work(&tx, config, auth, binding, work.clone()).await?;
    if result.replayed {
        tx.rollback().await.map_err(|_| ManagementError::Database)?;
        return Ok(result);
    }
    *failure_state = records::ActionState::NeedsAttention;
    let committed = tx.commit().await;
    #[cfg(test)]
    let committed = if committed.is_ok() && fault_injection::take(auth, binding) {
        db.clone()
            .close()
            .await
            .map_err(|_| ManagementError::Database)?;
        Err(sea_orm::DbErr::Custom("commit response unavailable".into()))
    } else {
        committed
    };
    match committed {
        Ok(()) => Ok(result),
        Err(_) => match replay(db, auth, binding, work).await? {
            Some(items) => Ok(ApplyResult {
                replayed: true,
                items,
            }),
            None => {
                *failure_state = records::ActionState::Stopped;
                Err(ManagementError::Database)
            }
        },
    }
}

/// Execute and verify the complete confirmed batch in the caller's transaction.
/// The caller owns commit/rollback; durable receipts cannot be supplied by callers.
pub async fn apply_in_transaction(
    tx: &sea_orm::DatabaseTransaction,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
) -> Result<ApplyResult> {
    let batch = tx.begin().await.map_err(|_| ManagementError::Database)?;
    match apply_in_transaction_with_work(
        &batch,
        config,
        auth,
        binding,
        records::SharedDecodeBudget::default(),
    )
    .await
    {
        Ok(result) => {
            batch
                .commit()
                .await
                .map_err(|_| ManagementError::Database)?;
            Ok(result)
        }
        Err(error) => {
            batch
                .rollback()
                .await
                .map_err(|_| ManagementError::Database)?;
            Err(error)
        }
    }
}
async fn apply_in_transaction_with_work(
    tx: &sea_orm::DatabaseTransaction,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    work: records::SharedDecodeBudget,
) -> Result<ApplyResult> {
    fence::write_intent(tx).await?;
    // TTL/current config only govern new business. A committed exact result is
    // still replayable after expiry or a subsequent legitimate state transition.
    if let Some(items) = replay(tx, auth, binding, work.clone()).await? {
        return Ok(ApplyResult {
            replayed: true,
            items,
        });
    }
    let before = binding.view().frozen_rows()?;
    if binding.archived_rows().catalog() != native::current_catalog() {
        return Err(ManagementError::Changed);
    }
    fence::lock(
        tx,
        before,
        config,
        binding,
        records::DecodeWork::shared(&work),
    )
    .await?;
    let checked = records::revalidate_with_work(tx, config, auth, binding, work.clone()).await?;
    if checked.len() != binding.confirmation.bindings.len()
        || checked
            .iter()
            .any(|item| !matches!(item.outcome, RevalidationOutcome::Checked(_)))
    {
        return Err(ManagementError::Confirmation);
    }
    records::DecodeWork::shared(&work).with(|work| work.nodes(1 + checked.len() * 2))?;
    let mut written = Vec::with_capacity(checked.len());
    let mut execution =
        execution::ExecutionBatch::with_work(tx, config, auth, binding, before, work.clone())?;
    execution.use_review(binding.clone())?;
    for entry in &binding.confirmation.bindings {
        let executed = execution.execute_entry(entry).await?;
        let encoded =
            records::write_executed_action_in_transaction(auth, binding, &executed, before).await?;
        written.push((executed, encoded));
    }
    // Check DB-clock deadline at the publication boundary too. Business time
    // passing during an allocation does not silently extend confirmed authority.
    binding.check_deadline(tx, auth).await?;
    status::fence_business_expiry(
        tx,
        binding.view().with_work(&work),
        written.iter().map(|(executed, _)| executed.action_view()),
        records::DecodeWork::shared(&work),
    )
    .await?;
    for (executed, original) in &mut written {
        let state =
            status::current_state_with_work(tx, executed.action_view(), work.clone()).await?;
        if let Some(updated) =
            records::update_executed_state(auth, binding, executed, original, state).await?
        {
            *original = updated;
        }
        executed.set_state(state);
    }
    super::schema::validate(tx).await?;
    super::catalog::validate(tx, native::current_catalog()).await?;
    native::check_raw_inventory(tx).await?;
    execution.validate_final(&written).await?;
    binding.check_deadline(tx, auth).await?;
    status::fence_business_expiry(
        tx,
        binding.view().with_work(&work),
        written.iter().map(|(executed, _)| executed.action_view()),
        records::DecodeWork::shared(&work),
    )
    .await?;
    records::DecodeWork::shared(&work).with(|work| work.check())?;
    Ok(ApplyResult {
        replayed: false,
        items: written
            .into_iter()
            .map(|(executed, _)| executed.into_action())
            .collect(),
    })
}

/// Stopped denotes a known rollback/no committed receipt. NeedsAttention denotes
/// an unresolved commit response: requery exact receipts, never auto-execute.
pub struct ApplyFailure {
    pub cause: ManagementError,
    pub items: Vec<StoppedItem>,
}
pub struct StoppedItem {
    pub item_id: String,
    pub state: records::ActionState,
}
pub async fn apply_detailed(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
) -> std::result::Result<ApplyResult, ApplyFailure> {
    apply_detailed_with_session(
        db,
        config,
        auth,
        binding,
        records::SharedDecodeBudget::default(),
    )
    .await
}
pub async fn apply_detailed_with_session(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedPlan,
    work: records::SharedDecodeBudget,
) -> std::result::Result<ApplyResult, ApplyFailure> {
    let mut state = records::ActionState::Stopped;
    apply_attempt(db, config, auth, binding, &mut state, work)
        .await
        .map_err(|cause| ApplyFailure {
            cause,
            items: binding
                .request
                .items
                .iter()
                .map(|item| StoppedItem {
                    item_id: item.item_id.clone(),
                    state,
                })
                .collect(),
        })
}

async fn replay<C: ConnectionTrait>(
    db: &C,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    work: records::SharedDecodeBudget,
) -> Result<Option<Vec<records::CommittedAction>>> {
    auth.check(db).await?;
    if binding.request.items.len() != binding.confirmation.bindings.len() {
        return Err(ManagementError::Confirmation);
    }
    let mut items = Vec::new();
    for entry in &binding.confirmation.bindings {
        if let Some(receipt) = records::lookup_trusted_with_work(
            db,
            auth,
            binding,
            &entry.request.item_id,
            work.clone(),
        )
        .await?
        {
            records::DecodeWork::shared(&work).copy(receipt.action_view())?;
            items.push(receipt.action_view().clone());
        }
    }
    if items.is_empty() {
        return Ok(None);
    }
    // Formal apply never commits a prefix. A partially published manual record set
    // is not authority to finish an expired or drifting remainder.
    if items.len() != binding.request.items.len() {
        return Err(ManagementError::Confirmation);
    }
    Ok(Some(items))
}

pub(super) fn database(error: crate::error::AppError) -> ManagementError {
    match error {
        crate::error::AppError::ManagementBudget => ManagementError::Budget,
        crate::error::AppError::InvalidPinningRequest(_) => ManagementError::Changed,
        _ => ManagementError::Database,
    }
}
