use chrono::{DateTime, Utc};
use sea_orm::{
    ActiveValue::Set,
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait,
    PaginatorTrait, QueryFilter, QuerySelect,
    sea_query::{Expr, OnConflict},
};
use serde::{Deserialize, Serialize};

pub use crate::kubo::LocalResidencyVerificationReceipt as TierCopyReceipt;

use crate::{
    error::{AppError, AppResult},
    lifecycle::model::{ClaimedLifecycleAction, VersionLifecycleCandidate},
    residency::{
        KuboTier, ReferenceReason, ResidencyLocation, StorageClass, VersionResidencyIdentity,
    },
    store::{
        database_clock::database_now,
        entities::{
            import_destination, lifecycle_action, lifecycle_transition, object, object_version,
            physical_residency, residency_reference, version_residency,
        },
        import::ownership::{
            StandardMutationGuard, clear_lifecycle_mutation_if_owned,
            complete_standard_mutation_in_transaction, has_overlapping_standard_prefix_mutation,
            lock_bucket_for_ownership, verify_standard_mutation_guard,
        },
        lifecycle_action::{
            FAILURE_CANCELLED_STALE, FAILURE_INTERNAL_DEPENDENCY, lock_claim_for_execution,
            mark_cancelled, mark_succeeded, retry_at, schedule_retry,
        },
        pinning::leases::lock_publication_lifecycle_frontier,
        residency::{
            attach_transition_hold_in_transaction, lock_residency_frontier,
            release_transition_hold_in_transaction,
        },
    },
};

const TRANSITION_CURRENT: &str = "transition_current";
const TRANSITION_NONCURRENT: &str = "transition_noncurrent";
const OWNER_VERSION: &str = "version";
const OWNER_TRANSITION: &str = "transition";
const RETAINED_VERSION: &str = "retained_version";
pub(crate) const TRANSITION_ATTEMPTS_EXHAUSTED: &str = "transition_attempts_exhausted";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedLifecycleTransition {
    pub source_residency_revision: i64,
    pub expected_source_node_identity: String,
    pub expected_destination_node_identity: String,
    pub ownership_generation: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransitionPrepareResult {
    Prepared(Box<lifecycle_transition::Model>),
    Stale,
    DependencyWaiting,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransitionPublishResult {
    Published(lifecycle_transition::Model),
    AlreadyPublished(lifecycle_transition::Model),
    Stale,
    DependencyWaiting,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableVerificationReceipt {
    version: u8,
    saga_id: String,
    action_id: String,
    claim_epoch: i64,
    source_residency_revision: i64,
    source_node_identity: String,
    destination_node_identity: String,
    cid: String,
    tier_receipt: TierCopyReceipt,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionPublicationReceipt {
    pub version: u8,
    pub saga_id: String,
    pub action_id: String,
    pub claim_epoch: i64,
    pub source_residency_revision: i64,
    pub published_residency_revision: i64,
    pub destination_node_identity: String,
    pub cid: String,
    pub published_at: DateTime<Utc>,
}

/// Revalidates and prepares a transition while preserving the global lock order:
/// lifecycle claim first, bucket ownership second, then version/residency rows.
///
/// `DependencyWaiting` is intentionally distinct from `Stale`: a still-winning
/// STANDARD target whose hot residency awaits verification must be retried without
/// consuming the ordinary failure budget.
pub async fn prepare(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
    source_node_identity: &str,
    destination_node_identity: &str,
) -> AppResult<TransitionPrepareResult> {
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(TransitionPrepareResult::Stale);
    };
    validate_transition_action(&action)?;
    validate_guard_target(claim, guard)?;
    validate_node_pair(source_node_identity, destination_node_identity)?;
    let existing = lock_optional_saga_for_action(txn, claim).await?;
    if let Some(existing) = existing.as_ref()
        && matches!(existing.checkpoint.as_str(), "publish" | "cleanup")
        && existing.publication_receipt.is_some()
    {
        return Ok(TransitionPrepareResult::Prepared(Box::new(
            existing.clone(),
        )));
    }
    validate_guard_binding(claim, guard)?;

    lock_bucket_for_ownership(txn, &action.bucket).await?;
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(TransitionPrepareResult::Stale);
    };
    verify_standard_mutation_guard(txn, guard, &action.bucket, &action.object_key, &[]).await?;

    let Some(candidate) = shared_revalidate_transition(txn, claim).await? else {
        return Ok(TransitionPrepareResult::Stale);
    };
    validate_candidate_snapshot(&action, &candidate)?;
    if !candidate.hot_residency_verified {
        return Ok(TransitionPrepareResult::DependencyWaiting);
    }

    lock_transition_frontier(
        txn,
        &action.bucket,
        &action.object_key,
        &candidate.target.version_row_id,
        candidate.target.object_id.as_deref().unwrap_or_default(),
        existing.as_ref().map(|saga| saga.source_cid.as_str()),
        false,
    )
    .await?;
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(TransitionPrepareResult::Stale);
    }

    let source = load_verified_source(txn, &candidate, source_node_identity).await?;
    let prepared = PreparedLifecycleTransition {
        source_residency_revision: source.revision,
        expected_source_node_identity: source_node_identity.to_owned(),
        expected_destination_node_identity: destination_node_identity.to_owned(),
        ownership_generation: guard.expected_generation,
    };
    let saga = insert_prepared_after_claim(txn, action, claim, prepared).await?;
    attach_transition_hold_in_transaction(
        txn,
        &saga.id,
        ReferenceReason::TransitionStaging,
        &saga_identity(&saga),
        &ResidencyLocation::new(KuboTier::Hot, saga.source_cid.clone()),
    )
    .await?;
    Ok(TransitionPrepareResult::Prepared(Box::new(saga)))
}

/// Backwards-compatible schema-layer primitive retained for existing callers.
/// New E2 orchestration should call [`prepare`] so eligibility, bucket ownership,
/// and staging-reference installation occur in one transaction.
pub async fn insert_prepared_in_transaction(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    prepared: PreparedLifecycleTransition,
) -> AppResult<Option<lifecycle_transition::Model>> {
    validate_prepared(&prepared)?;
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(None);
    };
    validate_transition_action(&action)?;
    let existing = lock_optional_saga_for_action(txn, claim).await?;
    lock_bucket_for_ownership(txn, &action.bucket).await?;
    lock_transition_frontier(
        txn,
        &action.bucket,
        &action.object_key,
        action.target_version_row_id.as_deref().unwrap_or_default(),
        action.target_object_id.as_deref().unwrap_or_default(),
        existing.as_ref().map(|saga| saga.source_cid.as_str()),
        false,
    )
    .await?;
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(None);
    }
    validate_prepared_source(txn, &action, &prepared).await?;
    insert_prepared_after_claim(txn, action, claim, prepared)
        .await
        .map(Some)
}

/// Records completion of external CAR copy I/O. This is deliberately not a
/// verification receipt and cannot authorize publication.
pub async fn record_copy(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    saga: &lifecycle_transition::Model,
) -> AppResult<Option<lifecycle_transition::Model>> {
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(None);
    }
    let stored = lock_saga(txn, claim, saga).await?;
    if !matches!(stored.checkpoint.as_str(), "prepare" | "copy" | "verify")
        || stored.settlement_kind.is_some()
    {
        return Err(invalid_state("copy checkpoint"));
    }
    lock_bucket_for_ownership(txn, &stored.bucket).await?;
    if current_lifecycle_mutation_guard_in_transaction(txn, claim)
        .await?
        .is_none()
    {
        return Ok(None);
    }
    lock_transition_frontier_for_saga(txn, &stored, false).await?;
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(None);
    }
    if matches!(stored.checkpoint.as_str(), "copy" | "verify") {
        return Ok(Some(stored));
    }

    ensure_cold_pending(txn, &stored).await?;
    attach_transition_hold_in_transaction(
        txn,
        &stored.id,
        ReferenceReason::TransitionStaging,
        &saga_identity(&stored),
        &ResidencyLocation::new(KuboTier::Cold, stored.destination_cid.clone()),
    )
    .await?;
    update_checkpoint(txn, &stored.id, "prepare", "copy", None, None).await?;
    Ok(Some(reload_saga(txn, &stored.id).await?))
}

/// Stores typed destination verification evidence, wrapped with the current
/// claim epoch and immutable saga/source snapshot. A receipt from a previous
/// claimant can be overwritten only after the new claimant independently
/// re-verifies the same destination.
pub async fn record_verified(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    saga: &lifecycle_transition::Model,
    receipt: &TierCopyReceipt,
) -> AppResult<Option<lifecycle_transition::Model>> {
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(None);
    }
    let stored = lock_saga(txn, claim, saga).await?;
    if !matches!(stored.checkpoint.as_str(), "copy" | "verify") || stored.settlement_kind.is_some()
    {
        return Err(invalid_state("verification checkpoint"));
    }
    lock_bucket_for_ownership(txn, &stored.bucket).await?;
    if current_lifecycle_mutation_guard_in_transaction(txn, claim)
        .await?
        .is_none()
    {
        return Ok(None);
    }
    lock_transition_frontier_for_saga(txn, &stored, false).await?;
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(None);
    }
    validate_tier_receipt(&stored, receipt)?;
    let durable = durable_verification(&stored, claim, receipt);
    let durable_json = serialize_receipt(&durable)?;
    let tier_json = serialize_receipt(receipt)?;
    apply_cold_verification(txn, &stored, receipt, &tier_json).await?;

    let now = database_now(txn).await?;
    let updated = lifecycle_transition::Entity::update_many()
        .col_expr(
            lifecycle_transition::Column::Checkpoint,
            Expr::value("verify"),
        )
        .col_expr(
            lifecycle_transition::Column::VerificationReceipt,
            Expr::value(Some(durable_json)),
        )
        .col_expr(lifecycle_transition::Column::UpdatedAt, Expr::value(now))
        .filter(lifecycle_transition::Column::Id.eq(&stored.id))
        .filter(lifecycle_transition::Column::ActionId.eq(&claim.action.id))
        .filter(lifecycle_transition::Column::Checkpoint.is_in(["copy", "verify"]))
        .filter(lifecycle_transition::Column::PublicationReceipt.is_null())
        .filter(lifecycle_transition::Column::SettlementKind.is_null())
        .exec(txn)
        .await?;
    if updated.rows_affected != 1 {
        return Err(invalid_state("verification compare-and-set"));
    }
    Ok(Some(reload_saga(txn, &stored.id).await?))
}

/// Performs the final E2 publication fence. The claim is locked before the
/// bucket, shared lifecycle eligibility is rerun, the current ownership guard
/// is checked, and cold primary/class/reference plus publication receipt are
/// committed atomically.
pub async fn publish(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
    receipt: &TierCopyReceipt,
) -> AppResult<TransitionPublishResult> {
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(TransitionPublishResult::Stale);
    };
    validate_transition_action(&action)?;
    validate_guard_binding(claim, guard)?;
    let saga = lock_saga_for_action(txn, claim).await?;
    if matches!(saga.checkpoint.as_str(), "publish" | "cleanup")
        && saga.publication_receipt.is_some()
    {
        return Ok(TransitionPublishResult::AlreadyPublished(saga));
    }
    if saga.checkpoint != "verify" || saga.settlement_kind.is_some() {
        return Err(invalid_state("publish checkpoint"));
    }

    lock_bucket_for_ownership(txn, &action.bucket).await?;
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(TransitionPublishResult::Stale);
    };
    verify_standard_mutation_guard(txn, guard, &action.bucket, &action.object_key, &[]).await?;
    let Some(candidate) = shared_revalidate_transition(txn, claim).await? else {
        return Ok(TransitionPublishResult::Stale);
    };
    validate_candidate_snapshot(&action, &candidate)?;
    if !candidate.hot_residency_verified {
        return Ok(TransitionPublishResult::DependencyWaiting);
    }
    lock_transition_frontier_for_saga(txn, &saga, false).await?;
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(TransitionPublishResult::Stale);
    }
    validate_source_snapshot(txn, &saga, &candidate).await?;
    validate_current_verification(&saga, claim, receipt)?;

    let published = publish_residency(txn, claim, &saga, receipt).await?;
    Ok(TransitionPublishResult::Published(published))
}

/// Settles post-publication cleanup. Only this saga's logical transition holds
/// are released; Kubo pins and shared physical rows are never removed.
pub async fn cleanup(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
    saga: &lifecycle_transition::Model,
) -> AppResult<bool> {
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(false);
    }
    validate_guard_target(claim, guard)?;
    let stored = lock_saga(txn, claim, saga).await?;
    if stored.checkpoint == "cleanup"
        && stored.settlement_kind.as_deref() == Some("cleanup_complete")
    {
        return Ok(true);
    }
    if stored.checkpoint != "publish" || stored.publication_receipt.is_none() {
        return Err(invalid_state("cleanup checkpoint"));
    }
    validate_publication_receipt(&stored, claim)?;

    lock_bucket_for_ownership(txn, &stored.bucket).await?;
    lock_transition_frontier_for_saga(txn, &stored, true).await?;
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(false);
    }
    release_all_transition_holds(txn, &stored).await?;
    let now = database_now(txn).await?;
    let updated = lifecycle_transition::Entity::update_many()
        .col_expr(
            lifecycle_transition::Column::Checkpoint,
            Expr::value("cleanup"),
        )
        .col_expr(
            lifecycle_transition::Column::SettlementKind,
            Expr::value(Some("cleanup_complete".to_owned())),
        )
        .col_expr(
            lifecycle_transition::Column::CompletedAt,
            Expr::value(Some(now)),
        )
        .col_expr(lifecycle_transition::Column::UpdatedAt, Expr::value(now))
        .filter(lifecycle_transition::Column::Id.eq(&stored.id))
        .filter(lifecycle_transition::Column::Checkpoint.eq("publish"))
        .filter(lifecycle_transition::Column::PublicationReceipt.is_not_null())
        .filter(lifecycle_transition::Column::SettlementKind.is_null())
        .exec(txn)
        .await?;
    if updated.rows_affected != 1 || !mark_succeeded(txn, claim, now).await? {
        return Err(invalid_state("cleanup settlement fence"));
    }
    clear_owned_guard(txn, claim, &stored, guard, now).await?;
    Ok(true)
}

/// Cancels an unpublished transition and releases only its own staging holds.
pub async fn settle_cancelled(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
    saga: Option<&lifecycle_transition::Model>,
) -> AppResult<bool> {
    settle_unpublished(txn, claim, guard, saga, FAILURE_CANCELLED_STALE, None).await
}

/// Exhaustion cancels the unpublished intent, not its durable cleanup owner.
/// Saga settlement, own-reference release, terminal action, diagnosis and guard
/// cleanup share one transaction. A rollback leaves a claimable responsibility.
pub(crate) async fn settle_exhausted(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
    saga: Option<&lifecycle_transition::Model>,
    stale: bool,
) -> AppResult<bool> {
    let failure = if stale {
        FAILURE_CANCELLED_STALE
    } else {
        FAILURE_INTERNAL_DEPENDENCY
    };
    settle_unpublished(
        txn,
        claim,
        guard,
        saga,
        failure,
        Some(TRANSITION_ATTEMPTS_EXHAUSTED),
    )
    .await
}

async fn settle_unpublished(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
    saga: Option<&lifecycle_transition::Model>,
    failure_class: &str,
    diagnostic: Option<&str>,
) -> AppResult<bool> {
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(false);
    };
    validate_guard_target(claim, guard)?;
    let stored = match saga {
        Some(saga) => Some(lock_saga(txn, claim, saga).await?),
        None => lock_optional_saga_for_action(txn, claim).await?,
    };
    lock_bucket_for_ownership(txn, &action.bucket).await?;
    if let Some(stored) = stored.as_ref() {
        lock_transition_frontier_for_saga(txn, stored, true).await?;
    } else {
        lock_transition_frontier(
            txn,
            &action.bucket,
            &action.object_key,
            action.target_version_row_id.as_deref().unwrap_or_default(),
            action.target_object_id.as_deref().unwrap_or_default(),
            None,
            true,
        )
        .await?;
    }
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(false);
    }
    if let Some(stored) = stored.as_ref() {
        if matches!(stored.checkpoint.as_str(), "publish" | "cleanup")
            || stored.publication_receipt.is_some()
        {
            return Err(invalid_state("published transition cancellation"));
        }
        release_all_transition_holds(txn, stored).await?;
    }
    let now = database_now(txn).await?;
    if let Some(stored) = stored.as_ref() {
        let updated = lifecycle_transition::Entity::update_many()
            .col_expr(
                lifecycle_transition::Column::SettlementKind,
                Expr::value(Some("cancelled".to_owned())),
            )
            .col_expr(
                lifecycle_transition::Column::CompletedAt,
                Expr::value(Some(now)),
            )
            .col_expr(lifecycle_transition::Column::UpdatedAt, Expr::value(now))
            .filter(lifecycle_transition::Column::Id.eq(&stored.id))
            .filter(lifecycle_transition::Column::Checkpoint.is_in(["prepare", "copy", "verify"]))
            .filter(lifecycle_transition::Column::PublicationReceipt.is_null())
            .filter(lifecycle_transition::Column::SettlementKind.is_null())
            .exec(txn)
            .await?;
        if updated.rows_affected != 1 {
            return Err(invalid_state("cancellation settlement fence"));
        }
    }
    if !mark_cancelled(txn, claim, now, failure_class).await? {
        return Err(invalid_state("cancellation claim fence"));
    }
    if let Some(diagnostic) = diagnostic {
        let updated = lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LastErrorRedacted,
                Expr::value(Some(diagnostic.to_owned())),
            )
            .filter(lifecycle_action::Column::Id.eq(&claim.action.id))
            .filter(lifecycle_action::Column::ClaimEpoch.eq(claim.claim_epoch))
            .filter(lifecycle_action::Column::State.eq("cancelled"))
            .exec(txn)
            .await?;
        if updated.rows_affected != 1 {
            return Err(invalid_state("exhaustion diagnostic fence"));
        }
    }
    clear_owned_guard_for_action(txn, claim, guard, now).await?;
    Ok(true)
}

/// Releases the current ownership admission and schedules a bounded retry while
/// preserving the durable saga and its logical holds for recovery.
pub async fn settle_retry(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
    base_backoff_secs: u64,
    max_backoff_secs: u64,
    failure_class: &str,
) -> AppResult<bool> {
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(false);
    };
    validate_guard_target(claim, guard)?;
    let saga = lock_optional_saga_for_action(txn, claim).await?;
    lock_bucket_for_ownership(txn, &action.bucket).await?;
    if let Some(saga) = saga.as_ref() {
        lock_transition_frontier_for_saga(txn, saga, true).await?;
    } else {
        lock_transition_frontier(
            txn,
            &action.bucket,
            &action.object_key,
            action.target_version_row_id.as_deref().unwrap_or_default(),
            action.target_object_id.as_deref().unwrap_or_default(),
            None,
            true,
        )
        .await?;
    }
    if lock_claim_for_execution(txn, claim).await?.is_none() {
        return Ok(false);
    }
    let now = database_now(txn).await?;
    let next_attempt_at = retry_at(now, action.attempts, base_backoff_secs, max_backoff_secs)?;
    if !schedule_retry(txn, claim, now, next_attempt_at, failure_class).await? {
        return Err(invalid_state("retry claim fence"));
    }
    clear_owned_guard_for_action(txn, claim, guard, now).await?;
    Ok(true)
}

/// Deletes only a saga whose settlement was already durably recorded by a fenced E2 path.
pub async fn delete_settled_in_transaction(
    txn: &DatabaseTransaction,
    transition_id: &str,
) -> AppResult<bool> {
    if transition_id.is_empty() {
        return Err(AppError::InvalidArgument(
            "lifecycle transition identity is empty".to_owned(),
        ));
    }
    let Some(transition) = lifecycle_transition::Entity::find_by_id(transition_id)
        .one(txn)
        .await?
    else {
        return Ok(false);
    };
    let expected_action_state = match transition.settlement_kind.as_deref() {
        Some("cancelled")
            if transition.completed_at.is_some()
                && matches!(
                    transition.checkpoint.as_str(),
                    "prepare" | "copy" | "verify"
                )
                && transition.publication_receipt.is_none() =>
        {
            "cancelled"
        }
        Some("cleanup_complete")
            if transition.completed_at.is_some()
                && transition.checkpoint == "cleanup"
                && transition
                    .verification_receipt
                    .as_deref()
                    .is_some_and(|v| !v.is_empty())
                && transition
                    .publication_receipt
                    .as_deref()
                    .is_some_and(|v| !v.is_empty()) =>
        {
            "succeeded"
        }
        _ => return Err(invalid_state("transition deletion settlement")),
    };
    let action_query = lifecycle_action::Entity::find_by_id(&transition.action_id);
    let action = if txn.get_database_backend() == DatabaseBackend::Postgres {
        action_query.lock_exclusive().one(txn).await?
    } else {
        action_query.one(txn).await?
    };
    if !action
        .is_some_and(|action| action.state == expected_action_state && action.finished_at.is_some())
    {
        return Err(invalid_state("transition deletion terminal action"));
    }
    let holds = residency_reference::Entity::find()
        .filter(residency_reference::Column::OwnerKind.eq(OWNER_TRANSITION))
        .filter(
            residency_reference::Column::OwnerId
                .is_in([transition.id.clone(), transition.action_id.clone()]),
        )
        .count(txn)
        .await?;
    if holds != 0 {
        return Err(invalid_state("transition deletion with residency holds"));
    }
    let deleted = lifecycle_transition::Entity::delete_by_id(transition.id)
        .exec(txn)
        .await?;
    if deleted.rows_affected != 1 {
        return Err(AppError::Internal(
            "settled lifecycle transition delete lost its row".to_owned(),
        ));
    }
    Ok(true)
}

async fn insert_prepared_after_claim(
    txn: &DatabaseTransaction,
    action: lifecycle_action::Model,
    claim: &ClaimedLifecycleAction,
    prepared: PreparedLifecycleTransition,
) -> AppResult<lifecycle_transition::Model> {
    validate_prepared(&prepared)?;
    let target_version_row_id = required(action.target_version_row_id.clone(), "target version")?;
    let target_public_version_id =
        required(action.target_public_version_id.clone(), "public version")?;
    let target_object_id = required(action.target_object_id.clone(), "target object")?;
    let target_sequence = action.target_sequence.ok_or_else(|| {
        AppError::InvalidArgument("transition action has no target sequence".to_owned())
    })?;
    let residency = version_residency::Entity::find_by_id(&target_version_row_id)
        .one(txn)
        .await?
        .ok_or_else(|| invalid_state("transition source residency missing"))?;
    if residency.object_id != target_object_id
        || residency.primary_tier != "hot"
        || residency.storage_class != "STANDARD"
        || residency.revision != prepared.source_residency_revision
    {
        return Err(invalid_state("transition source residency snapshot"));
    }
    let now = database_now(txn).await?;
    lifecycle_transition::Entity::insert(lifecycle_transition::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        action_id: Set(action.id),
        action_kind: Set(action.action_kind),
        bucket: Set(action.bucket),
        object_key: Set(action.object_key),
        config_revision: Set(action.config_revision),
        rule_id: Set(action.rule_id),
        target_version_row_id: Set(target_version_row_id),
        target_public_version_id: Set(target_public_version_id),
        target_object_id: Set(target_object_id),
        target_sequence: Set(target_sequence),
        source_tier: Set("hot".to_owned()),
        destination_tier: Set("cold".to_owned()),
        source_cid: Set(residency.cid.clone()),
        destination_cid: Set(residency.cid),
        source_residency_revision: Set(prepared.source_residency_revision),
        expected_source_node_identity: Set(prepared.expected_source_node_identity.clone()),
        expected_destination_node_identity: Set(prepared
            .expected_destination_node_identity
            .clone()),
        ownership_generation: Set(prepared.ownership_generation),
        checkpoint: Set("prepare".to_owned()),
        verification_receipt: Set(None),
        publication_receipt: Set(None),
        settlement_kind: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
        completed_at: Set(None),
    })
    .on_conflict(
        OnConflict::column(lifecycle_transition::Column::ActionId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(txn)
    .await?;
    let stored = lock_saga_for_action(txn, claim).await?;
    validate_replayed_snapshot(&stored, &prepared)?;
    Ok(stored)
}

async fn publish_residency(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    saga: &lifecycle_transition::Model,
    receipt: &TierCopyReceipt,
) -> AppResult<lifecycle_transition::Model> {
    let identity = saga_identity(saga);
    let hot = ResidencyLocation::new(KuboTier::Hot, saga.source_cid.clone());
    let cold = ResidencyLocation::new(KuboTier::Cold, saga.destination_cid.clone());

    let cold_row =
        physical_residency::Entity::find_by_id(("cold".to_owned(), saga.destination_cid.clone()))
            .one(txn)
            .await?
            .ok_or_else(|| invalid_state("verified cold residency missing"))?;
    let tier_json = serialize_receipt(receipt)?;
    if cold_row.verification_state != "verified"
        || cold_row.node_identity.as_deref()
            != Some(saga.expected_destination_node_identity.as_str())
        || cold_row.verification_receipt.as_deref() != Some(tier_json.as_str())
        || cold_row.verified_at.is_none()
    {
        return Err(invalid_state("cold verification binding"));
    }
    let current = version_residency::Entity::find_by_id(&identity.version_row_id)
        .one(txn)
        .await?
        .ok_or_else(|| invalid_state("source residency disappeared"))?;
    if current.object_id != identity.object_id
        || current.cid != identity.cid
        || current.primary_tier != "hot"
        || current.storage_class != "STANDARD"
        || current.revision != saga.source_residency_revision
    {
        return Err(invalid_state("source residency publication fence"));
    }
    let published_revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| AppError::Internal("residency revision exhausted".to_owned()))?;
    let now = database_now(txn).await?;

    attach_transition_hold_in_transaction(
        txn,
        &saga.id,
        ReferenceReason::TransitionCleanupHold,
        &identity,
        &hot,
    )
    .await?;
    residency_reference::Entity::insert(residency_reference::ActiveModel {
        owner_kind: Set(OWNER_VERSION.to_owned()),
        owner_id: Set(identity.version_row_id.clone()),
        reason: Set(RETAINED_VERSION.to_owned()),
        version_row_id: Set(identity.version_row_id.clone()),
        object_id: Set(identity.object_id.clone()),
        tier: Set("cold".to_owned()),
        cid: Set(identity.cid.clone()),
        created_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            residency_reference::Column::OwnerKind,
            residency_reference::Column::OwnerId,
            residency_reference::Column::Reason,
            residency_reference::Column::Tier,
            residency_reference::Column::Cid,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(txn)
    .await?;
    let removed_hot = residency_reference::Entity::delete_many()
        .filter(residency_reference::Column::OwnerKind.eq(OWNER_VERSION))
        .filter(residency_reference::Column::OwnerId.eq(&identity.version_row_id))
        .filter(residency_reference::Column::Reason.eq(RETAINED_VERSION))
        .filter(residency_reference::Column::VersionRowId.eq(&identity.version_row_id))
        .filter(residency_reference::Column::ObjectId.eq(&identity.object_id))
        .filter(residency_reference::Column::Tier.eq("hot"))
        .filter(residency_reference::Column::Cid.eq(&identity.cid))
        .exec(txn)
        .await?;
    if removed_hot.rows_affected != 1 {
        return Err(invalid_state("hot retained reference publication fence"));
    }
    let moved = version_residency::Entity::update_many()
        .col_expr(version_residency::Column::PrimaryTier, Expr::value("cold"))
        .col_expr(
            version_residency::Column::StorageClass,
            Expr::value("STANDARD_IA"),
        )
        .col_expr(
            version_residency::Column::Revision,
            Expr::value(published_revision),
        )
        .col_expr(version_residency::Column::UpdatedAt, Expr::value(now))
        .filter(version_residency::Column::VersionRowId.eq(&identity.version_row_id))
        .filter(version_residency::Column::ObjectId.eq(&identity.object_id))
        .filter(version_residency::Column::Cid.eq(&identity.cid))
        .filter(version_residency::Column::PrimaryTier.eq("hot"))
        .filter(version_residency::Column::StorageClass.eq("STANDARD"))
        .filter(version_residency::Column::Revision.eq(saga.source_residency_revision))
        .exec(txn)
        .await?;
    if moved.rows_affected != 1 {
        return Err(invalid_state(
            "version residency publication compare-and-set",
        ));
    }
    let released_hot = release_transition_hold_in_transaction(
        txn,
        &saga.id,
        ReferenceReason::TransitionStaging,
        &hot,
    )
    .await?;
    let released_cold = release_transition_hold_in_transaction(
        txn,
        &saga.id,
        ReferenceReason::TransitionStaging,
        &cold,
    )
    .await?;
    if !released_hot || !released_cold {
        return Err(invalid_state("publication staging references"));
    }

    let publication = TransitionPublicationReceipt {
        version: 1,
        saga_id: saga.id.clone(),
        action_id: saga.action_id.clone(),
        claim_epoch: claim.claim_epoch,
        source_residency_revision: saga.source_residency_revision,
        published_residency_revision: published_revision,
        destination_node_identity: saga.expected_destination_node_identity.clone(),
        cid: saga.destination_cid.clone(),
        published_at: now,
    };
    let publication_json = serialize_receipt(&publication)?;
    let updated = lifecycle_transition::Entity::update_many()
        .col_expr(
            lifecycle_transition::Column::Checkpoint,
            Expr::value("publish"),
        )
        .col_expr(
            lifecycle_transition::Column::PublicationReceipt,
            Expr::value(Some(publication_json)),
        )
        .col_expr(lifecycle_transition::Column::UpdatedAt, Expr::value(now))
        .filter(lifecycle_transition::Column::Id.eq(&saga.id))
        .filter(lifecycle_transition::Column::Checkpoint.eq("verify"))
        .filter(lifecycle_transition::Column::VerificationReceipt.is_not_null())
        .filter(lifecycle_transition::Column::PublicationReceipt.is_null())
        .filter(lifecycle_transition::Column::SettlementKind.is_null())
        .exec(txn)
        .await?;
    if updated.rows_affected != 1 {
        return Err(invalid_state("publication receipt compare-and-set"));
    }
    reload_saga(txn, &saga.id).await
}

async fn load_verified_source(
    txn: &DatabaseTransaction,
    candidate: &VersionLifecycleCandidate,
    expected_node: &str,
) -> AppResult<version_residency::Model> {
    let residency = version_residency::Entity::find_by_id(&candidate.target.version_row_id)
        .one(txn)
        .await?
        .ok_or_else(|| invalid_state("transition source residency missing"))?;
    if residency.object_id != candidate.target.object_id.as_deref().unwrap_or_default()
        || residency.primary_tier != "hot"
        || residency.storage_class != "STANDARD"
        || residency.revision <= 0
    {
        return Err(invalid_state("transition source residency is not STANDARD"));
    }
    let physical =
        physical_residency::Entity::find_by_id(("hot".to_owned(), residency.cid.clone()))
            .one(txn)
            .await?
            .ok_or_else(|| invalid_state("transition source physical residency missing"))?;
    if physical.verification_state != "verified"
        || physical.node_identity.as_deref() != Some(expected_node)
        || physical
            .verification_receipt
            .as_deref()
            .is_none_or(str::is_empty)
        || physical.verified_at.is_none()
    {
        return Err(invalid_state("transition source node verification"));
    }
    Ok(residency)
}

async fn validate_prepared_source(
    txn: &DatabaseTransaction,
    action: &lifecycle_action::Model,
    prepared: &PreparedLifecycleTransition,
) -> AppResult<()> {
    let version_id = required(action.target_version_row_id.clone(), "target version")?;
    let object_id = required(action.target_object_id.clone(), "target object")?;
    let source = version_residency::Entity::find_by_id(version_id)
        .one(txn)
        .await?
        .ok_or_else(|| invalid_state("transition source residency missing"))?;
    if source.object_id != object_id
        || source.primary_tier != "hot"
        || source.storage_class != "STANDARD"
        || source.revision != prepared.source_residency_revision
    {
        return Err(invalid_state("transition source residency revision"));
    }
    let physical = physical_residency::Entity::find_by_id(("hot".to_owned(), source.cid))
        .one(txn)
        .await?
        .ok_or_else(|| invalid_state("transition source physical residency missing"))?;
    if physical.verification_state != "verified"
        || physical.node_identity.as_deref()
            != Some(prepared.expected_source_node_identity.as_str())
        || physical
            .verification_receipt
            .as_deref()
            .is_none_or(str::is_empty)
        || physical.verified_at.is_none()
    {
        return Err(invalid_state("transition source node verification"));
    }
    Ok(())
}

async fn validate_source_snapshot(
    txn: &DatabaseTransaction,
    saga: &lifecycle_transition::Model,
    candidate: &VersionLifecycleCandidate,
) -> AppResult<()> {
    let source = load_verified_source(txn, candidate, &saga.expected_source_node_identity).await?;
    if source.revision != saga.source_residency_revision
        || source.cid != saga.source_cid
        || source.object_id != saga.target_object_id
    {
        return Err(invalid_state("immutable source residency snapshot"));
    }
    Ok(())
}

async fn ensure_cold_pending(
    txn: &DatabaseTransaction,
    saga: &lifecycle_transition::Model,
) -> AppResult<()> {
    let now = database_now(txn).await?;
    physical_residency::Entity::insert(physical_residency::ActiveModel {
        tier: Set("cold".to_owned()),
        cid: Set(saga.destination_cid.clone()),
        node_identity: Set(None),
        verification_state: Set("pending".to_owned()),
        verification_receipt: Set(None),
        verified_at: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            physical_residency::Column::Tier,
            physical_residency::Column::Cid,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(txn)
    .await?;
    let existing =
        physical_residency::Entity::find_by_id(("cold".to_owned(), saga.destination_cid.clone()))
            .one(txn)
            .await?
            .ok_or_else(|| invalid_state("cold copy staging attach"))?;
    if existing.verification_state == "verified"
        && existing.node_identity.as_deref()
            != Some(saga.expected_destination_node_identity.as_str())
    {
        return Err(invalid_state("cold node identity conflict"));
    }
    Ok(())
}

async fn apply_cold_verification(
    txn: &DatabaseTransaction,
    saga: &lifecycle_transition::Model,
    receipt: &TierCopyReceipt,
    receipt_json: &str,
) -> AppResult<()> {
    ensure_cold_pending(txn, saga).await?;
    let now = database_now(txn).await?;
    let updated = physical_residency::Entity::update_many()
        .col_expr(
            physical_residency::Column::VerificationState,
            Expr::value("verified"),
        )
        .col_expr(
            physical_residency::Column::NodeIdentity,
            Expr::value(Some(receipt.node_identity.clone())),
        )
        .col_expr(
            physical_residency::Column::VerificationReceipt,
            Expr::value(Some(receipt_json.to_owned())),
        )
        .col_expr(
            physical_residency::Column::VerifiedAt,
            Expr::value(Some(now)),
        )
        .col_expr(physical_residency::Column::UpdatedAt, Expr::value(now))
        .filter(physical_residency::Column::Tier.eq("cold"))
        .filter(physical_residency::Column::Cid.eq(&saga.destination_cid))
        .filter(physical_residency::Column::VerificationState.is_in(["pending", "failed"]))
        .exec(txn)
        .await?;
    if updated.rows_affected == 1 {
        return Ok(());
    }
    let existing =
        physical_residency::Entity::find_by_id(("cold".to_owned(), saga.destination_cid.clone()))
            .one(txn)
            .await?
            .ok_or_else(|| invalid_state("cold verification residency missing"))?;
    if existing.verification_state == "verified"
        && existing.node_identity.as_deref() == Some(receipt.node_identity.as_str())
        && existing.verification_receipt.as_deref() == Some(receipt_json)
        && existing.verified_at.is_some()
    {
        Ok(())
    } else {
        Err(invalid_state("conflicting cold verification receipt"))
    }
}

async fn release_all_transition_holds(
    txn: &DatabaseTransaction,
    saga: &lifecycle_transition::Model,
) -> AppResult<()> {
    residency_reference::Entity::delete_many()
        .filter(residency_reference::Column::OwnerKind.eq(OWNER_TRANSITION))
        .filter(residency_reference::Column::OwnerId.eq(&saga.id))
        .exec(txn)
        .await?;
    Ok(())
}

async fn clear_owned_guard(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    saga: &lifecycle_transition::Model,
    guard: &StandardMutationGuard,
    now: DateTime<Utc>,
) -> AppResult<()> {
    if guard.bucket == saga.bucket && guard.key == saga.object_key {
        clear_owned_guard_for_action(txn, claim, guard, now).await?;
    }
    Ok(())
}

async fn clear_owned_guard_for_action(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let exact_current_guard =
        guard.mutation_id == format!("lifecycle:{}:{}", claim.action.id, claim.claim_epoch);
    if exact_current_guard {
        match verify_standard_mutation_guard(
            txn,
            guard,
            &claim.action.bucket,
            &claim.action.object_key,
            &[],
        )
        .await
        {
            Ok(()) => complete_standard_mutation_in_transaction(txn, guard, now).await?,
            Err(AppError::StaleContentMutation) => {}
            Err(error) => return Err(error),
        }
    }
    // A recovery guard describes the new claim epoch, while the durable row may
    // still contain this action's older epoch. The broad same-action CAS is safe
    // through the current epoch and deliberately leaves unrelated/newer owners.
    clear_lifecycle_mutation_if_owned(
        txn,
        &claim.action.bucket,
        &claim.action.object_key,
        &claim.action.id,
        claim.claim_epoch,
        now,
    )
    .await?;
    Ok(())
}

/// Reconstructs the current lifecycle mutation guard from the authoritative
/// destination row. The caller must hold the bucket ownership lock. A reclaim
/// may have a different generation than the saga's immutable first admission;
/// only the action/current-epoch token and authoritative generation are used.
pub async fn current_lifecycle_mutation_guard_in_transaction(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
) -> AppResult<Option<StandardMutationGuard>> {
    let expected_mutation_id = format!("lifecycle:{}:{}", claim.action.id, claim.claim_epoch);
    let query = import_destination::Entity::find_by_id((
        claim.action.bucket.clone(),
        claim.action.object_key.clone(),
    ));
    let destination = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(txn).await?
    } else {
        query.one(txn).await?
    };
    let Some(destination) = destination else {
        return Ok(None);
    };
    if destination.generation <= 0
        || destination.owner_job_id.is_some()
        || destination.mutation_id.as_deref() != Some(expected_mutation_id.as_str())
        || destination.mutation_prefix.is_some()
    {
        return Ok(None);
    }
    let guard = StandardMutationGuard {
        bucket: destination.bucket,
        key: destination.key,
        mutation_id: expected_mutation_id,
        expected_generation: destination.generation,
        mutation_prefix: None,
    };
    match verify_standard_mutation_guard(
        txn,
        &guard,
        &claim.action.bucket,
        &claim.action.object_key,
        &[],
    )
    .await
    {
        Ok(()) => {}
        Err(AppError::StaleContentMutation) => return Ok(None),
        Err(error) => return Err(error),
    }
    if has_overlapping_standard_prefix_mutation(txn, &claim.action.bucket, &claim.action.object_key)
        .await?
    {
        return Ok(None);
    }
    Ok(Some(guard))
}

/// Locks the exact transition mutation frontier in the same order as version
/// deletion/publication: version row, immutable object owner, active lease
/// lifecycle, then residency/reference/physical rows in stable tier/CID order.
/// The caller must already hold the claim, saga (when present), and bucket lock.
async fn lock_transition_frontier(
    txn: &DatabaseTransaction,
    bucket: &str,
    key: &str,
    version_row_id: &str,
    object_id: &str,
    expected_cid: Option<&str>,
    allow_deleted_target: bool,
) -> AppResult<()> {
    if bucket.is_empty() || key.is_empty() || version_row_id.is_empty() || object_id.is_empty() {
        return Err(invalid_state("transition frontier identity"));
    }

    let version_query = object_version::Entity::find_by_id(version_row_id);
    let version = if txn.get_database_backend() == DatabaseBackend::Postgres {
        version_query.lock_exclusive().one(txn).await?
    } else {
        version_query.one(txn).await?
    };
    if let Some(version) = version.as_ref()
        && (version.bucket != bucket
            || version.key != key
            || version.kind != "object"
            || version.object_id.as_deref() != Some(object_id))
    {
        return Err(invalid_state("transition frontier version binding"));
    }

    let owner_query = object::Entity::find_by_id(object_id);
    let owner = if txn.get_database_backend() == DatabaseBackend::Postgres {
        owner_query.lock_exclusive().one(txn).await?
    } else {
        owner_query.one(txn).await?
    };
    if let Some(owner) = owner.as_ref()
        && (owner.bucket != bucket
            || owner.key != key
            || expected_cid.is_some_and(|cid| owner.cid != cid))
    {
        return Err(invalid_state("transition frontier object binding"));
    }
    if !allow_deleted_target && (version.is_none() || owner.is_none()) {
        return Err(invalid_state("transition frontier target missing"));
    }

    lock_publication_lifecycle_frontier(txn, &[object_id.to_owned()], &[]).await?;

    let cid = match expected_cid {
        Some(cid) if !cid.is_empty() => Some(cid.to_owned()),
        Some(_) => return Err(invalid_state("transition frontier CID")),
        None => version_residency::Entity::find_by_id(version_row_id)
            .one(txn)
            .await?
            .map(|residency| residency.cid),
    };
    let locations = cid
        .as_ref()
        .map(|cid| {
            vec![
                ResidencyLocation::new(KuboTier::Hot, cid.clone()),
                ResidencyLocation::new(KuboTier::Cold, cid.clone()),
            ]
        })
        .unwrap_or_default();
    lock_residency_frontier(txn, &[version_row_id.to_owned()], &locations).await?;

    if !allow_deleted_target {
        let residency = version_residency::Entity::find_by_id(version_row_id)
            .one(txn)
            .await?
            .ok_or_else(|| invalid_state("transition frontier residency missing"))?;
        if residency.object_id != object_id
            || cid.as_deref() != Some(residency.cid.as_str())
            || owner
                .as_ref()
                .is_none_or(|owner| owner.cid != residency.cid)
        {
            return Err(invalid_state("transition frontier residency binding"));
        }
    }
    Ok(())
}

async fn lock_transition_frontier_for_saga(
    txn: &DatabaseTransaction,
    saga: &lifecycle_transition::Model,
    allow_deleted_target: bool,
) -> AppResult<()> {
    lock_transition_frontier(
        txn,
        &saga.bucket,
        &saga.object_key,
        &saga.target_version_row_id,
        &saga.target_object_id,
        Some(&saga.source_cid),
        allow_deleted_target,
    )
    .await
}

async fn lock_saga(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    saga: &lifecycle_transition::Model,
) -> AppResult<lifecycle_transition::Model> {
    if saga.action_id != claim.action.id {
        return Err(invalid_state("saga action binding"));
    }
    let query = lifecycle_transition::Entity::find_by_id(&saga.id);
    let stored = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(txn).await?
    } else {
        query.one(txn).await?
    }
    .ok_or_else(|| invalid_state("transition saga missing"))?;
    if stored.action_id != claim.action.id {
        return Err(invalid_state("saga action binding"));
    }
    Ok(stored)
}

async fn lock_saga_for_action(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
) -> AppResult<lifecycle_transition::Model> {
    lock_optional_saga_for_action(txn, claim)
        .await?
        .ok_or_else(|| invalid_state("transition saga missing"))
}

async fn lock_optional_saga_for_action(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
) -> AppResult<Option<lifecycle_transition::Model>> {
    let query = lifecycle_transition::Entity::find()
        .filter(lifecycle_transition::Column::ActionId.eq(&claim.action.id));
    let saga = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(txn).await?
    } else {
        query.one(txn).await?
    };
    Ok(saga)
}

async fn reload_saga(
    txn: &DatabaseTransaction,
    saga_id: &str,
) -> AppResult<lifecycle_transition::Model> {
    lifecycle_transition::Entity::find_by_id(saga_id)
        .one(txn)
        .await?
        .ok_or_else(|| invalid_state("transition saga disappeared"))
}

async fn update_checkpoint(
    txn: &DatabaseTransaction,
    saga_id: &str,
    expected: &str,
    next: &str,
    verification_receipt: Option<String>,
    publication_receipt: Option<String>,
) -> AppResult<()> {
    let now = database_now(txn).await?;
    let updated = lifecycle_transition::Entity::update_many()
        .col_expr(lifecycle_transition::Column::Checkpoint, Expr::value(next))
        .col_expr(
            lifecycle_transition::Column::VerificationReceipt,
            Expr::value(verification_receipt),
        )
        .col_expr(
            lifecycle_transition::Column::PublicationReceipt,
            Expr::value(publication_receipt),
        )
        .col_expr(lifecycle_transition::Column::UpdatedAt, Expr::value(now))
        .filter(lifecycle_transition::Column::Id.eq(saga_id))
        .filter(lifecycle_transition::Column::Checkpoint.eq(expected))
        .filter(lifecycle_transition::Column::SettlementKind.is_null())
        .exec(txn)
        .await?;
    if updated.rows_affected != 1 {
        return Err(invalid_state("transition checkpoint compare-and-set"));
    }
    Ok(())
}

fn validate_transition_action(action: &lifecycle_action::Model) -> AppResult<()> {
    if !matches!(
        action.action_kind.as_str(),
        TRANSITION_CURRENT | TRANSITION_NONCURRENT
    ) || action.target_type != "version"
        || action
            .target_version_row_id
            .as_deref()
            .is_none_or(str::is_empty)
        || action
            .target_public_version_id
            .as_deref()
            .is_none_or(str::is_empty)
        || action.target_object_id.as_deref().is_none_or(str::is_empty)
        || action.target_sequence.is_none()
    {
        return Err(AppError::InvalidArgument(
            "lifecycle action is not a content-version transition".to_owned(),
        ));
    }
    Ok(())
}

fn validate_candidate_snapshot(
    action: &lifecycle_action::Model,
    candidate: &VersionLifecycleCandidate,
) -> AppResult<()> {
    let role_matches = match action.action_kind.as_str() {
        TRANSITION_CURRENT => candidate.is_latest,
        TRANSITION_NONCURRENT => !candidate.is_latest,
        _ => false,
    };
    if !role_matches
        || candidate.target.bucket != action.bucket
        || candidate.target.key != action.object_key
        || Some(candidate.target.version_row_id.as_str()) != action.target_version_row_id.as_deref()
        || Some(candidate.target.public_version_id.as_s3_str())
            != action.target_public_version_id.as_deref()
        || candidate.target.object_id.as_deref() != action.target_object_id.as_deref()
        || Some(candidate.target.sequence) != action.target_sequence
        || candidate.primary_storage_class != Some(StorageClass::Standard)
    {
        return Err(invalid_state("shared revalidation candidate snapshot"));
    }
    Ok(())
}

fn validate_guard_binding(
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
) -> AppResult<()> {
    validate_guard_target(claim, guard)?;
    if guard.mutation_prefix.is_some()
        || guard.mutation_id != format!("lifecycle:{}:{}", claim.action.id, claim.claim_epoch)
    {
        return Err(AppError::StaleContentMutation);
    }
    Ok(())
}

fn validate_guard_target(
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
) -> AppResult<()> {
    if guard.bucket != claim.action.bucket
        || guard.key != claim.action.object_key
        || guard.expected_generation <= 0
    {
        return Err(AppError::StaleContentMutation);
    }
    Ok(())
}

fn validate_node_pair(source: &str, destination: &str) -> AppResult<()> {
    if source.is_empty() || destination.is_empty() || source == destination {
        return Err(AppError::InvalidArgument(
            "transition requires distinct source and destination nodes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_tier_receipt(
    saga: &lifecycle_transition::Model,
    receipt: &TierCopyReceipt,
) -> AppResult<()> {
    if receipt.cid != saga.destination_cid
        || receipt.node_identity != saga.expected_destination_node_identity
    {
        return Err(invalid_state("tier copy receipt binding"));
    }
    Ok(())
}

fn durable_verification(
    saga: &lifecycle_transition::Model,
    claim: &ClaimedLifecycleAction,
    receipt: &TierCopyReceipt,
) -> DurableVerificationReceipt {
    DurableVerificationReceipt {
        version: 1,
        saga_id: saga.id.clone(),
        action_id: saga.action_id.clone(),
        claim_epoch: claim.claim_epoch,
        source_residency_revision: saga.source_residency_revision,
        source_node_identity: saga.expected_source_node_identity.clone(),
        destination_node_identity: saga.expected_destination_node_identity.clone(),
        cid: saga.destination_cid.clone(),
        tier_receipt: receipt.clone(),
    }
}

fn validate_current_verification(
    saga: &lifecycle_transition::Model,
    claim: &ClaimedLifecycleAction,
    receipt: &TierCopyReceipt,
) -> AppResult<()> {
    validate_tier_receipt(saga, receipt)?;
    let stored: DurableVerificationReceipt = serde_json::from_str(
        saga.verification_receipt
            .as_deref()
            .ok_or_else(|| invalid_state("verification receipt missing"))?,
    )
    .map_err(|_| invalid_state("verification receipt encoding"))?;
    if stored != durable_verification(saga, claim, receipt) {
        return Err(invalid_state("verification receipt epoch or saga binding"));
    }
    Ok(())
}

fn validate_publication_receipt(
    saga: &lifecycle_transition::Model,
    claim: &ClaimedLifecycleAction,
) -> AppResult<()> {
    let stored: TransitionPublicationReceipt = serde_json::from_str(
        saga.publication_receipt
            .as_deref()
            .ok_or_else(|| invalid_state("publication receipt missing"))?,
    )
    .map_err(|_| invalid_state("publication receipt encoding"))?;
    let expected_revision = saga
        .source_residency_revision
        .checked_add(1)
        .ok_or_else(|| invalid_state("publication receipt revision"))?;
    if stored.version != 1
        || stored.saga_id != saga.id
        || stored.action_id != saga.action_id
        || stored.claim_epoch <= 0
        || stored.claim_epoch > claim.claim_epoch
        || stored.source_residency_revision != saga.source_residency_revision
        || stored.published_residency_revision != expected_revision
        || stored.destination_node_identity != saga.expected_destination_node_identity
        || stored.cid != saga.destination_cid
    {
        return Err(invalid_state("publication receipt binding"));
    }
    Ok(())
}

fn validate_prepared(prepared: &PreparedLifecycleTransition) -> AppResult<()> {
    if prepared.source_residency_revision <= 0
        || prepared.ownership_generation <= 0
        || prepared.expected_source_node_identity.is_empty()
        || prepared.expected_destination_node_identity.is_empty()
        || prepared.expected_source_node_identity == prepared.expected_destination_node_identity
    {
        return Err(AppError::InvalidArgument(
            "invalid prepared lifecycle transition".to_owned(),
        ));
    }
    Ok(())
}

fn validate_replayed_snapshot(
    stored: &lifecycle_transition::Model,
    prepared: &PreparedLifecycleTransition,
) -> AppResult<()> {
    // ownership_generation describes the immutable first admission. A reclaim
    // may legitimately install a new generation; publication uses the fresh
    // StandardMutationGuard rather than rewriting the saga snapshot.
    if stored.source_residency_revision != prepared.source_residency_revision
        || stored.expected_source_node_identity != prepared.expected_source_node_identity
        || stored.expected_destination_node_identity != prepared.expected_destination_node_identity
    {
        return Err(AppError::Internal(
            "transition action already has a different immutable saga snapshot".to_owned(),
        ));
    }
    Ok(())
}

fn saga_identity(saga: &lifecycle_transition::Model) -> VersionResidencyIdentity {
    VersionResidencyIdentity::new(
        saga.target_version_row_id.clone(),
        saga.target_object_id.clone(),
        saga.source_cid.clone(),
    )
}

fn serialize_receipt(value: &impl Serialize) -> AppResult<String> {
    serde_json::to_string(value)
        .map_err(|_| AppError::Internal("failed to serialize transition receipt".to_owned()))
}

fn invalid_state(subject: &str) -> AppError {
    AppError::Internal(format!("invalid lifecycle transition state: {subject}"))
}

fn required(value: Option<String>, field: &str) -> AppResult<String> {
    value.filter(|value| !value.is_empty()).ok_or_else(|| {
        AppError::InvalidArgument(format!("transition action has no {field} identity"))
    })
}

async fn shared_revalidate_transition(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
) -> AppResult<Option<VersionLifecycleCandidate>> {
    crate::lifecycle::revalidation::revalidate_transition(txn, claim).await
}

#[cfg(test)]
mod tests;
