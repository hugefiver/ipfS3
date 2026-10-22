//! Formal Stage 7 Store apply. Provider IO is deliberately outside this module.
use super::{
    ManagementError, Result, native, records,
    transaction::{self, CancellationToken, ManagementTransaction, TransactionFailure},
};
use crate::pinning::management::{RevalidationOutcome, local_config::ManagementConfig};
#[cfg(test)]
use sea_orm::TransactionTrait;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection};

pub(crate) mod accounting;
pub(crate) mod execution;
mod executor;
#[cfg(test)]
mod fault_injection;
mod fence;
mod intents;
pub mod job_authority;
mod local_scope;
mod migration;
pub mod points;
pub mod provider_points;
mod recovery;
mod retirement;
mod roots;
mod seal;
mod sql;
#[cfg(test)]
mod sql_tests;
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
    (failure_state, transaction_failure): (
        &mut records::ActionState,
        &mut Option<TransactionFailure>,
    ),
    work: records::SharedDecodeBudget,
    cancel: CancellationToken,
) -> Result<ApplyResult> {
    if let Some(items) = recover_receipts(db, auth, binding, work.clone()).await? {
        return Ok(ApplyResult {
            replayed: true,
            items,
        });
    }
    records::prepare_retirement_apply(db, binding).await?;
    let mut tx = ManagementTransaction::begin(db, transaction::FINAL_WRITE_LIMIT, cancel)
        .await
        .map_err(|failure| failed_transaction(failure, failure_state, transaction_failure))?;
    let result = apply_bounded_with_work(&mut tx, config, auth, binding, work.clone())
        .await
        .map_err(|failure| failed_transaction(failure, failure_state, transaction_failure))?;
    if result.replayed {
        tx.rollback()
            .await
            .map_err(|failure| failed_transaction(failure, failure_state, transaction_failure))?;
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
        Err(TransactionFailure {
            cause: transaction::StopCause::Database,
            rollback_confirmed: false,
        })
    } else {
        committed
    };
    match committed {
        Ok(()) => Ok(result),
        Err(failure) if failure.rollback_confirmed => Err(failed_transaction(
            failure,
            failure_state,
            transaction_failure,
        )),
        Err(failure) => {
            *transaction_failure = Some(failure);
            match recover_receipts(db, auth, binding, work).await? {
                Some(items) => Ok(ApplyResult {
                    replayed: true,
                    items,
                }),
                None => Err(ManagementError::Database),
            }
        }
    }
}

/// Execute in a bounded management transaction. On error the entire transaction
/// is rolled back and acknowledged, not merely a nested savepoint.
pub async fn apply_in_transaction(
    tx: &mut ManagementTransaction,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
) -> std::result::Result<ApplyResult, TransactionFailure> {
    apply_bounded_with_work(
        tx,
        config,
        auth,
        binding,
        records::SharedDecodeBudget::default(),
    )
    .await
}

async fn apply_bounded_with_work(
    tx: &mut ManagementTransaction,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    work: records::SharedDecodeBudget,
) -> std::result::Result<ApplyResult, TransactionFailure> {
    tx.run_with(
        (config, auth, binding, work),
        |tx, deadline, (config, auth, binding, work)| {
            Box::pin(async move {
                apply_in_transaction_with_work(tx, config, auth, binding, work.clone(), deadline)
                    .await
            })
        },
    )
    .await
}
async fn apply_in_transaction_with_work(
    tx: &sea_orm::DatabaseTransaction,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    work: records::SharedDecodeBudget,
    deadline: transaction::Deadline,
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
    deadline.business_expiry(tx, binding.expires_at).await?;
    let before = binding.view().frozen_rows()?;
    if binding.archived_rows().catalog() != native::current_catalog() {
        return Err(ManagementError::Changed);
    }
    let scope = local_scope::capture(tx, config, binding, work.clone()).await?;
    fence::lock(
        tx,
        before,
        config,
        binding,
        &scope,
        records::DecodeWork::shared(&work),
    )
    .await?;
    let checked =
        records::revalidate_scoped_with_work(tx, config, auth, binding, Some(&scope), work.clone())
            .await?;
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
    execution.use_scope(scope)?;
    for entry in &binding.confirmation.bindings {
        let executed = Box::pin(execution.execute_entry(entry)).await?;
        let encoded =
            records::write_executed_action_in_transaction(auth, binding, &executed, before).await?;
        seal::executed(
            tx,
            auth,
            binding,
            &executed,
            records::DecodeWork::shared(&work),
        )
        .await?;
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
    super::schema::validate(tx).await?;
    super::catalog::validate(tx, native::current_catalog()).await?;
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

/// Stopped denotes acknowledged rollback or no business SQL started.
/// NeedsAttention denotes unresolved cleanup or commit: requery exact receipts,
/// never auto-execute based only on their momentary absence.
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
    apply_cancellable_with_session(db, config, auth, binding, work, CancellationToken::new())
        .await
        .map_err(|failure| failure.failure)
}

/// The transaction failure retains deadline/cancel/database distinctions and
/// whether rollback actually completed. Await this result after cancelling.
pub struct CancellableApplyFailure {
    pub failure: ApplyFailure,
    pub transaction: Option<TransactionFailure>,
}

pub async fn apply_cancellable(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedPlan,
    cancel: CancellationToken,
) -> std::result::Result<ApplyResult, CancellableApplyFailure> {
    apply_cancellable_with_session(
        db,
        config,
        auth,
        binding,
        records::SharedDecodeBudget::default(),
        cancel,
    )
    .await
}

pub async fn apply_cancellable_with_session(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedPlan,
    work: records::SharedDecodeBudget,
    cancel: CancellationToken,
) -> std::result::Result<ApplyResult, CancellableApplyFailure> {
    let mut state = records::ActionState::Stopped;
    let mut transaction_failure = None;
    apply_attempt(
        db,
        config,
        auth,
        binding,
        (&mut state, &mut transaction_failure),
        work,
        cancel,
    )
    .await
    .map_err(|cause| CancellableApplyFailure {
        transaction: transaction_failure,
        failure: ApplyFailure {
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
        },
    })
}

fn failed_transaction(
    failure: TransactionFailure,
    state: &mut records::ActionState,
    detail: &mut Option<TransactionFailure>,
) -> ManagementError {
    *state = if failure.rollback_confirmed {
        records::ActionState::Stopped
    } else {
        records::ActionState::NeedsAttention
    };
    *detail = Some(failure);
    failure.management_error()
}

async fn recover_receipts(
    db: &DatabaseConnection,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    work: records::SharedDecodeBudget,
) -> Result<Option<Vec<records::CommittedAction>>> {
    // Receipt recovery has a separate read-only budget, never authority to retry
    // business writes after the final-write deadline.
    let mut recovery =
        ManagementTransaction::begin(db, transaction::FINAL_WRITE_LIMIT, CancellationToken::new())
            .await
            .map_err(TransactionFailure::management_error)?;
    let (auth, binding) = (auth.clone(), binding.clone());
    let result = recovery
        .run(move |tx, _| Box::pin(async move { replay(tx, &auth, &binding, work).await }))
        .await
        .map_err(TransactionFailure::management_error)?;
    recovery
        .rollback()
        .await
        .map_err(TransactionFailure::management_error)?;
    Ok(result)
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
