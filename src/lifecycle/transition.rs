//! Durable STANDARD -> STANDARD_IA coordination. Network I/O never owns a DB transaction.
use std::future::Future;

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, TransactionTrait};
use tokio_util::sync::CancellationToken;

use crate::{
    config::ValidatedLifecycleConfig,
    error::{AppError, AppResult, TierError},
    kubo::tier_copy::stream_copy_verified,
    lifecycle::model::ClaimedLifecycleAction,
    residency::router::TierClients,
    store::{
        database_clock::database_now,
        entities::lifecycle_transition,
        import::ownership::{
            StandardMutationGuard, clear_lifecycle_mutation_if_owned, lock_bucket_for_ownership,
            try_admit_lifecycle_mutation,
        },
        lifecycle_action::{
            FAILURE_INTERNAL_DEPENDENCY, TRANSITION_SETTLEMENT_REQUIRED, lock_claim_for_execution,
            renew_claim, retry_at, schedule_retry, wait_for_hot_verification_in_transaction,
            wait_for_mutation_dependency, waiting_for_dependency,
        },
        lifecycle_transition::{
            self as saga_store, TransitionPrepareResult, TransitionPublishResult,
        },
    },
};

/// Internal worker entry point. No public lifecycle configuration capability is enabled here.
pub(crate) async fn execute(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    clients: &TierClients<'_>,
    config: &ValidatedLifecycleConfig,
    cancel: &CancellationToken,
) -> AppResult<()> {
    if cancel.is_cancelled() {
        return Ok(());
    }
    if recover_published(db, claim, config, cancel).await? {
        return Ok(());
    }
    if claim.action.last_error_redacted.as_deref() == Some(TRANSITION_SETTLEMENT_REQUIRED) {
        return settle_failure(db, claim, config).await;
    }
    let guard = match admit_claimed(db, claim).await {
        Ok(Some(guard)) => guard,
        Ok(None) => {
            if wait_for_mutation_dependency(db, claim, config.max_attempts).await? {
                return Ok(());
            }
            return settle_failure(db, claim, config).await;
        }
        Err(_) => return settle_failure(db, claim, config).await,
    };
    let result = execute_admitted(db, claim, clients, config, cancel, &guard).await;
    match result {
        // Drop cancels the in-flight future and its stream. Do not issue any
        // write from an expired epoch, even an apparently harmless terminal write.
        Err(AppError::StaleContentMutation) => Ok(()),
        Err(_) if cancel.is_cancelled() => Ok(()),
        Err(_) => settle_failure(db, claim, config).await,
        Ok(()) => Ok(()),
    }
}

/// Cleanup has no Kubo dependency, including through the legacy worker wrapper.
pub(super) async fn recover_published(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    config: &ValidatedLifecycleConfig,
    cancel: &CancellationToken,
) -> AppResult<bool> {
    // Publication is irreversible. Neither current policy nor target existence
    // nor cold health can revoke this durable cleanup responsibility.
    let saga = lifecycle_transition::Entity::find()
        .filter(lifecycle_transition::Column::ActionId.eq(&claim.action.id))
        .one(db)
        .await?;
    let Some(saga) = saga.filter(|saga| saga.publication_receipt.is_some()) else {
        return Ok(false);
    };
    let guard = recovery_guard(claim, saga.ownership_generation);
    let recovery = async {
        let txn = db.begin().await?;
        saga_store::cleanup(&txn, claim, &guard, &saga).await?;
        txn.commit().await?;
        AppResult::Ok(())
    }
    .await;
    if recovery.is_err() && !cancel.is_cancelled() {
        settle_failure(db, claim, config).await?;
    }
    Ok(true)
}

async fn admit_claimed(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
) -> AppResult<Option<StandardMutationGuard>> {
    let txn = db.begin().await?;
    if lock_claim_for_execution(&txn, claim).await?.is_none() {
        return Ok(None);
    }
    // The shared admission helper retains its existing bucket-first order inside
    // this claim-first transaction. Recheck after waiting for that bucket; any
    // tentative ownership write is rolled back if the claim expired meanwhile.
    let guard = try_admit_lifecycle_mutation(
        &txn,
        &claim.action.bucket,
        &claim.action.object_key,
        &claim.action.id,
        claim.claim_epoch,
        database_now(&txn).await?,
    )
    .await?;
    if lock_claim_for_execution(&txn, claim).await?.is_none() {
        return Ok(None);
    }
    txn.commit().await?;
    Ok(guard)
}

async fn execute_admitted(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    clients: &TierClients<'_>,
    config: &ValidatedLifecycleConfig,
    cancel: &CancellationToken,
    guard: &StandardMutationGuard,
) -> AppResult<()> {
    if waiting_for_dependency(&claim.action) && claim.action.attempts >= config.max_attempts {
        return settle_failure(db, claim, config).await;
    }
    // Revalidate before contacting a tier, including when cold is unconfigured.
    let txn = db.begin().await?;
    if lock_claim_for_execution(&txn, claim).await?.is_none() {
        return Err(AppError::StaleContentMutation);
    }
    lock_bucket_for_ownership(&txn, &claim.action.bucket).await?;
    match super::revalidation::revalidate_transition(&txn, claim).await? {
        None => {
            saga_store::settle_cancelled(&txn, claim, guard, None).await?;
            txn.commit().await?;
            return Ok(());
        }
        Some(candidate) if !candidate.hot_residency_verified => {
            if !wait_for_hot_verification_in_transaction(&txn, claim).await? {
                return Err(AppError::StaleContentMutation);
            }
            clear_lifecycle_mutation_if_owned(
                &txn,
                &claim.action.bucket,
                &claim.action.object_key,
                &claim.action.id,
                claim.claim_epoch,
                database_now(&txn).await?,
            )
            .await?;
            txn.commit().await?;
            return Ok(());
        }
        Some(_) => txn.commit().await?,
    }
    if claim.action.attempts > config.max_attempts {
        return settle_failure(db, claim, config).await;
    }
    let cold = clients
        .cold
        .ok_or(AppError::Tier(TierError::ColdNotConfigured))?;
    let (source_node, destination_node) = io_with_lease(db, claim, config, cancel, async {
        Ok((
            clients.hot.local_node_identity().await?,
            cold.local_node_identity().await?,
        ))
    })
    .await?;
    let txn = db.begin().await?;
    let prepared = saga_store::prepare(&txn, claim, guard, &source_node, &destination_node).await?;
    let mut saga = match prepared {
        TransitionPrepareResult::Prepared(saga) => *saga,
        TransitionPrepareResult::Stale => {
            saga_store::settle_cancelled(&txn, claim, guard, None).await?;
            txn.commit().await?;
            return Ok(());
        }
        TransitionPrepareResult::DependencyWaiting => {
            if !wait_for_hot_verification_in_transaction(&txn, claim).await? {
                return Err(AppError::StaleContentMutation);
            }
            clear_lifecycle_mutation_if_owned(
                &txn,
                &claim.action.bucket,
                &claim.action.object_key,
                &claim.action.id,
                claim.claim_epoch,
                database_now(&txn).await?,
            )
            .await?;
            txn.commit().await?;
            return Ok(());
        }
    };
    txn.commit().await?;
    if saga.checkpoint == "prepare" {
        io_with_lease(
            db,
            claim,
            config,
            cancel,
            stream_copy_verified(
                clients.hot,
                cold,
                &saga.source_cid,
                Some(&saga.expected_source_node_identity),
                Some(&saga.expected_destination_node_identity),
                cancel,
            ),
        )
        .await?;
        let txn = db.begin().await?;
        saga = saga_store::record_copy(&txn, claim, &saga)
            .await?
            .ok_or(AppError::StaleContentMutation)?;
        txn.commit().await?;
    }
    // Always reverify after restart, including copy/verify checkpoint replay.
    // A durable checkpoint alone does not prove a replacement node has bytes.
    let verification = io_with_lease(db, claim, config, cancel, async {
        let source_node = clients.hot.local_node_identity().await?;
        if source_node != saga.expected_source_node_identity {
            return Err(AppError::Tier(TierError::NodeIdentityMismatch));
        }
        let receipt = cold.verify_local_residency(&saga.destination_cid).await?;
        if receipt.node_identity != saga.expected_destination_node_identity {
            return Err(AppError::Tier(TierError::NodeIdentityMismatch));
        }
        Ok(receipt)
    })
    .await;
    let receipt = match verification {
        Ok(receipt) => receipt,
        Err(AppError::StaleContentMutation) => return Err(AppError::StaleContentMutation),
        Err(_) => {
            // A copy checkpoint records completed I/O, not eternal durability
            // at the destination. Repair lost local blocks with the same DAG;
            // stream_copy_verified rechecks both node bindings before importing.
            io_with_lease(
                db,
                claim,
                config,
                cancel,
                stream_copy_verified(
                    clients.hot,
                    cold,
                    &saga.source_cid,
                    Some(&saga.expected_source_node_identity),
                    Some(&saga.expected_destination_node_identity),
                    cancel,
                ),
            )
            .await?
        }
    };
    let txn = db.begin().await?;
    saga_store::record_verified(&txn, claim, &saga, &receipt)
        .await?
        .ok_or(AppError::StaleContentMutation)?;
    txn.commit().await?;
    let txn = db.begin().await?;
    let published = saga_store::publish(&txn, claim, guard, &receipt).await?;
    match published {
        TransitionPublishResult::Published(published)
        | TransitionPublishResult::AlreadyPublished(published) => {
            txn.commit().await?;
            let txn = db.begin().await?;
            saga_store::cleanup(&txn, claim, guard, &published).await?;
            txn.commit().await?;
        }
        TransitionPublishResult::Stale => {
            saga_store::settle_cancelled(&txn, claim, guard, Some(&saga)).await?;
            txn.commit().await?;
        }
        TransitionPublishResult::DependencyWaiting => {
            if !wait_for_hot_verification_in_transaction(&txn, claim).await? {
                return Err(AppError::StaleContentMutation);
            }
            clear_lifecycle_mutation_if_owned(
                &txn,
                &claim.action.bucket,
                &claim.action.object_key,
                &claim.action.id,
                claim.claim_epoch,
                database_now(&txn).await?,
            )
            .await?;
            txn.commit().await?;
        }
    }
    Ok(())
}

fn recovery_guard(claim: &ClaimedLifecycleAction, generation: i64) -> StandardMutationGuard {
    StandardMutationGuard {
        bucket: claim.action.bucket.clone(),
        key: claim.action.object_key.clone(),
        mutation_id: format!("lifecycle:{}:{}", claim.action.id, claim.claim_epoch),
        expected_generation: generation,
        mutation_prefix: None,
    }
}

pub(super) async fn settle_failure(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    config: &ValidatedLifecycleConfig,
) -> AppResult<()> {
    let txn = db.begin().await?;
    let Some(action) = lock_claim_for_execution(&txn, claim).await? else {
        return Ok(());
    };
    lock_bucket_for_ownership(&txn, &action.bucket).await?;
    let Some(action) = lock_claim_for_execution(&txn, claim).await? else {
        return Ok(());
    };
    let saga = lifecycle_transition::Entity::find()
        .filter(lifecycle_transition::Column::ActionId.eq(&action.id))
        .one(&txn)
        .await?;
    let published = saga
        .as_ref()
        .is_some_and(|saga| saga.publication_receipt.is_some());
    let exhausted = action.attempts >= config.max_attempts
        || action.last_error_redacted.as_deref() == Some(TRANSITION_SETTLEMENT_REQUIRED);
    if !published {
        // A failed network operation may have raced a rule DELETE, tag change
        // or exact version delete. Revalidate under the same final fences even
        // when there will be no publication and the error exhausted the budget.
        let stale = super::revalidation::revalidate_transition(&txn, claim)
            .await?
            .is_none();
        if exhausted || stale {
            let guard = recovery_guard(
                claim,
                saga.as_ref().map_or(1, |saga| saga.ownership_generation),
            );
            let settled = if exhausted {
                saga_store::settle_exhausted(&txn, claim, &guard, saga.as_ref(), stale).await?
            } else {
                saga_store::settle_cancelled(&txn, claim, &guard, saga.as_ref()).await?
            };
            if !settled {
                return Err(AppError::StaleContentMutation);
            }
            txn.commit().await?;
            tracing::warn!(bucket = %action.bucket, key = %action.object_key, exhausted, stale,
                failure_class = "transition_cancelled");
            return Ok(());
        }
    }
    if lock_claim_for_execution(&txn, claim).await?.is_none() {
        return Ok(());
    }
    let now = database_now(&txn).await?;
    let due = retry_at(
        now,
        action.attempts,
        config.base_backoff_secs,
        config.max_backoff_secs,
    )?;
    if !schedule_retry(&txn, claim, now, due, FAILURE_INTERNAL_DEPENDENCY).await? {
        return Err(AppError::StaleContentMutation);
    }
    clear_lifecycle_mutation_if_owned(
        &txn,
        &action.bucket,
        &action.object_key,
        &action.id,
        claim.claim_epoch,
        now,
    )
    .await?;
    txn.commit().await?;
    tracing::warn!(bucket = %action.bucket, key = %action.object_key, failure_class = "transition_dependency");
    Ok(())
}

/// One task owns both the I/O future and heartbeat. Dropping/aborting it cannot
/// leave a detached heartbeat renewing a dead worker's lease.
async fn io_with_lease<T>(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    config: &ValidatedLifecycleConfig,
    cancel: &CancellationToken,
    io: impl Future<Output = AppResult<T>>,
) -> AppResult<T> {
    let interval = config
        .action_lease
        .to_std()
        .map_err(|_| AppError::Internal("invalid transition lease".into()))?
        / 3;
    let mut heartbeat = tokio::time::interval(interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(io);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(AppError::StaleContentMutation),
            _ = heartbeat.tick() => {
                if !renew_claim(db, claim, config.action_lease).await? { return Err(AppError::StaleContentMutation); }
            }
            result = &mut io => return result,
        }
    }
}

#[cfg(test)]
mod tests;
