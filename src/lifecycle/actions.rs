use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    EntityTrait, PaginatorTrait, QueryFilter, QuerySelect, TransactionError, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    lifecycle::{
        config::from_canonical_json,
        evaluator::{LifecycleEvaluationContext, evaluate_candidate},
        model::{
            ClaimedLifecycleAction, GuardedLifecycleExecutionResult, LifecycleActionKind,
            LifecycleCandidate, RuleIdentity, VersionTargetIdentity,
        },
    },
    store::{
        database_clock::database_now,
        entities::{bucket_lifecycle_config, lifecycle_action, object, object_version},
        import::ownership::{
            StandardMutationGuard, clear_lifecycle_mutation_if_owned,
            complete_standard_mutation_in_transaction, lock_bucket_for_ownership,
            try_admit_lifecycle_mutation, verify_standard_mutation_guard,
        },
        lifecycle_action::{
            FAILURE_ADMISSION_TEMPORARILY_UNAVAILABLE, FAILURE_CANCELLED_STALE,
            FAILURE_DATABASE_CONTENTION, FAILURE_INTERNAL_DEPENDENCY,
            MAX_LIFECYCLE_ACTION_ATTEMPTS, lock_claim_for_execution, mark_cancelled,
            mark_failed_safe, mark_succeeded, retry_at, schedule_retry,
        },
        object_version::{
            BucketVersioningState, ExactCurrentMarkerDeleteResult, PublicVersionId, VersionKind,
            delete_current_sole_marker, lock_version_by_id, public_version_id, version_kind,
        },
        pinning::publication::{
            delete_enabled_current_in_transaction, delete_exact_in_transaction,
            delete_suspended_current_in_transaction, delete_unversioned_current_in_transaction,
        },
        pinning::tags::list_object_tags,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleAdmissionClassification {
    Stale,
    Temporary,
    Other,
}

#[derive(Clone, Debug)]
pub(crate) enum LifecycleAdmissionResult {
    Admitted(StandardMutationGuard),
    Stale,
    Temporary,
}

/// Admits the exact key that a lifecycle worker plans to mutate. Task 8 owns
/// retry and terminal action handling; this helper reports stale and temporary
/// admission outcomes without exposing raw database details.
pub(crate) async fn admit_lifecycle_expiration(
    db: &DatabaseConnection,
    target: &VersionTargetIdentity,
    action_id: &str,
    claim_epoch: i64,
    now: DateTime<Utc>,
) -> AppResult<LifecycleAdmissionResult> {
    let result =
        try_admit_lifecycle_mutation(db, &target.bucket, &target.key, action_id, claim_epoch, now)
            .await;
    match result {
        Ok(Some(guard)) => Ok(LifecycleAdmissionResult::Admitted(guard)),
        Ok(None) => Ok(LifecycleAdmissionResult::Temporary),
        Err(error) => match classify_lifecycle_admission_error(&error) {
            LifecycleAdmissionClassification::Stale => Ok(LifecycleAdmissionResult::Stale),
            LifecycleAdmissionClassification::Temporary => Ok(LifecycleAdmissionResult::Temporary),
            LifecycleAdmissionClassification::Other => Err(error),
        },
    }
}

/// Executes one claimed lifecycle action. Admission is intentionally separate
/// from the final mutation transaction, but every policy read and every
/// terminal state write is claim-fenced inside that final transaction.
pub(crate) async fn execute_claimed_lifecycle_action(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    max_attempts: i64,
    base_backoff_secs: u64,
    max_backoff_secs: u64,
) -> AppResult<()> {
    validate_execution_settings(max_attempts, base_backoff_secs, max_backoff_secs)?;
    let target = match target_from_action(&claim.action) {
        Ok(target) => target,
        Err(_) => {
            return fail_safe(
                db,
                claim,
                FAILURE_INTERNAL_DEPENDENCY,
                "invalid_action_identity",
            )
            .await;
        }
    };
    let admission_now = match database_now(db).await {
        Ok(now) => now,
        Err(error) if is_temporary_execution_error(&error) => {
            return retry_or_fail_safe(
                db,
                claim,
                max_attempts,
                base_backoff_secs,
                max_backoff_secs,
                FAILURE_DATABASE_CONTENTION,
            )
            .await;
        }
        Err(_) => {
            return fail_safe(
                db,
                claim,
                FAILURE_INTERNAL_DEPENDENCY,
                "internal_dependency",
            )
            .await;
        }
    };
    let admission = match admit_lifecycle_expiration(
        db,
        &target,
        &claim.action.id,
        claim.claim_epoch,
        admission_now,
    )
    .await
    {
        Ok(admission) => admission,
        Err(error) if is_temporary_execution_error(&error) => {
            return retry_or_fail_safe(
                db,
                claim,
                max_attempts,
                base_backoff_secs,
                max_backoff_secs,
                FAILURE_DATABASE_CONTENTION,
            )
            .await;
        }
        Err(_) => {
            return fail_safe(
                db,
                claim,
                FAILURE_INTERNAL_DEPENDENCY,
                "internal_dependency",
            )
            .await;
        }
    };
    match admission {
        LifecycleAdmissionResult::Admitted(guard) => {
            match execute_final_transaction(db, claim, &target, &guard).await {
                Ok(()) => Ok(()),
                Err(error) => {
                    let error = transaction_error_into_app(error);
                    if is_temporary_execution_error(&error) {
                        settle_post_admission_failure(
                            db,
                            claim,
                            &target,
                            &guard,
                            max_attempts,
                            base_backoff_secs,
                            max_backoff_secs,
                            FAILURE_DATABASE_CONTENTION,
                            true,
                        )
                        .await
                    } else {
                        settle_post_admission_failure(
                            db,
                            claim,
                            &target,
                            &guard,
                            max_attempts,
                            base_backoff_secs,
                            max_backoff_secs,
                            FAILURE_INTERNAL_DEPENDENCY,
                            false,
                        )
                        .await
                    }
                }
            }
        }
        LifecycleAdmissionResult::Stale => {
            cancel_without_guard(db, claim, FAILURE_CANCELLED_STALE).await
        }
        LifecycleAdmissionResult::Temporary => {
            retry_or_fail_safe(
                db,
                claim,
                max_attempts,
                base_backoff_secs,
                max_backoff_secs,
                FAILURE_ADMISSION_TEMPORARILY_UNAVAILABLE,
            )
            .await
        }
    }
}

fn validate_execution_settings(
    max_attempts: i64,
    base_backoff_secs: u64,
    max_backoff_secs: u64,
) -> AppResult<()> {
    if max_attempts <= 0 || max_attempts > MAX_LIFECYCLE_ACTION_ATTEMPTS {
        return Err(AppError::InvalidArgument(
            "lifecycle action max attempts is out of range".to_owned(),
        ));
    }
    if base_backoff_secs == 0 || max_backoff_secs < base_backoff_secs {
        return Err(AppError::InvalidArgument(
            "lifecycle action retry backoff is invalid".to_owned(),
        ));
    }
    Ok(())
}

async fn execute_final_transaction(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    target: &VersionTargetIdentity,
    guard: &StandardMutationGuard,
) -> Result<(), TransactionError<AppError>> {
    let claim = claim.clone();
    let target = target.clone();
    let guard = guard.clone();
    db.transaction(move |txn| {
        Box::pin(async move {
            // This must remain the first transaction operation: a reclaimed or
            // expired epoch is forbidden from even reading lifecycle policy.
            let Some(locked_action) = lock_claim_for_execution(txn, &claim).await? else {
                // Admission can race an administrative action-row removal or a
                // lease reclaim. Clear only our still-current admission token;
                // a newer admission makes this a benign stale no-op.
                let now = database_now(txn).await?;
                lock_bucket_for_ownership(txn, &target.bucket).await?;
                match complete_standard_mutation_in_transaction(txn, &guard, now).await {
                    Ok(()) | Err(AppError::StaleContentMutation) => {}
                    Err(error) => return Err(error),
                }
                return Ok(());
            };
            let now = database_now(txn).await?;
            if !same_action_definition(&locked_action, &claim.action) {
                lock_bucket_for_ownership(txn, &target.bucket).await?;
                return cancel_guarded_in_transaction(txn, &claim, &guard, now).await;
            }

            lock_bucket_for_ownership(txn, &target.bucket).await?;
            match verify_standard_mutation_guard(txn, &guard, &target.bucket, &target.key, &[])
                .await
            {
                Ok(()) => {}
                Err(AppError::StaleContentMutation) => {
                    return Err(AppError::StaleContentMutation);
                }
                Err(error) => return Err(error),
            }
            #[cfg(test)]
            test_hooks::fail_after_admission(&claim.action.id).await?;

            let Some(configuration_row) = lock_lifecycle_configuration(txn, &target.bucket).await?
            else {
                return cancel_guarded_in_transaction(txn, &claim, &guard, now).await;
            };
            if configuration_row.revision != locked_action.config_revision {
                return cancel_guarded_in_transaction(txn, &claim, &guard, now).await;
            }
            let Some(canonical_json) = configuration_row.canonical_json else {
                return cancel_guarded_in_transaction(txn, &claim, &guard, now).await;
            };
            let configuration = from_canonical_json(&canonical_json)?;

            let action_kind = action_kind_from_db(&locked_action.action_kind)?;
            let Some(candidate) = revalidate_candidate(txn, &target, action_kind).await? else {
                return cancel_guarded_in_transaction(txn, &claim, &guard, now).await;
            };
            let tags = match target.object_id.as_deref() {
                Some(object_id) => list_object_tags(txn, object_id).await?,
                None => Vec::new(),
            };
            let public_version_count = object_version::Entity::find()
                .filter(object_version::Column::Bucket.eq(&target.bucket))
                .filter(object_version::Column::Key.eq(&target.key))
                .count(txn)
                .await?;
            let newer_noncurrent_count = object_version::Entity::find()
                .filter(object_version::Column::Bucket.eq(&target.bucket))
                .filter(object_version::Column::Key.eq(&target.key))
                .filter(object_version::Column::IsLatest.eq(false))
                .filter(object_version::Column::Sequence.gt(target.sequence))
                .count(txn)
                .await?;
            let context = LifecycleEvaluationContext {
                candidate: &candidate,
                tags: &tags,
                public_version_count,
                newer_noncurrent_count,
                config_revision: configuration_row.revision,
                configuration: &configuration,
                database_now: now,
            };
            let expected = evaluate_candidate(&context)?;
            if !expected
                .as_ref()
                .is_some_and(|expected| action_matches_expected(&locked_action, expected))
            {
                return cancel_guarded_in_transaction(txn, &claim, &guard, now).await;
            }

            match execute_lifecycle_delete_guarded(txn, &target, action_kind, &guard, now).await? {
                GuardedLifecycleExecutionResult::Applied(_)
                | GuardedLifecycleExecutionResult::AlreadySatisfied => {
                    if !mark_succeeded(txn, &claim, now).await? {
                        return Err(AppError::StaleContentMutation);
                    }
                }
                GuardedLifecycleExecutionResult::Stale => {
                    cancel_guarded_in_transaction(txn, &claim, &guard, now).await?;
                }
            }
            Ok(())
        })
    })
    .await
}

async fn lock_lifecycle_configuration<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
) -> AppResult<Option<bucket_lifecycle_config::Model>> {
    let query = bucket_lifecycle_config::Entity::find_by_id(bucket_name.to_owned());
    if db.get_database_backend() == DatabaseBackend::Postgres {
        return Ok(query.lock_exclusive().one(db).await?);
    }
    Ok(query.one(db).await?)
}

async fn revalidate_candidate<C: ConnectionTrait>(
    db: &C,
    target: &VersionTargetIdentity,
    action_kind: LifecycleActionKind,
) -> AppResult<Option<LifecycleCandidate>> {
    let Some(selected) = lock_version_by_id(db, &target.version_row_id).await? else {
        return Ok(None);
    };
    if !target_matches_row(&selected, target)?
        || selected.is_latest != (action_kind != LifecycleActionKind::ExpireNoncurrent)
    {
        return Ok(None);
    }
    match (action_kind, target.kind) {
        (LifecycleActionKind::ExpireCurrent, VersionKind::Object)
        | (LifecycleActionKind::DeleteExpiredMarker, VersionKind::DeleteMarker)
        | (LifecycleActionKind::ExpireNoncurrent, _) => {}
        _ => return Ok(None),
    }

    let size = match target.kind {
        VersionKind::Object => {
            let object_id = target.object_id.as_deref().ok_or_else(|| {
                AppError::Internal("lifecycle object target is invalid".to_owned())
            })?;
            let Some(locked_object) = lock_object_for_lifecycle(db, object_id).await? else {
                return Ok(None);
            };
            if locked_object.bucket != target.bucket
                || locked_object.key != target.key
                || locked_object.is_latest != selected.is_latest
            {
                return Ok(None);
            }
            locked_object.size
        }
        VersionKind::DeleteMarker => {
            if selected.is_latest {
                let current_object_count = object::Entity::find()
                    .filter(object::Column::Bucket.eq(&target.bucket))
                    .filter(object::Column::Key.eq(&target.key))
                    .filter(object::Column::IsLatest.eq(true))
                    .count(db)
                    .await?;
                if current_object_count != 0 {
                    return Ok(None);
                }
            }
            0
        }
    };
    Ok(Some(LifecycleCandidate {
        target: target.clone(),
        is_latest: selected.is_latest,
        size,
        lifecycle_age_started_at: selected.lifecycle_age_started_at,
        became_noncurrent_at: selected.became_noncurrent_at,
    }))
}

async fn lock_object_for_lifecycle<C: ConnectionTrait>(
    db: &C,
    object_id: &str,
) -> AppResult<Option<object::Model>> {
    let query = object::Entity::find_by_id(object_id.to_owned());
    if db.get_database_backend() == DatabaseBackend::Postgres {
        return Ok(query.lock_exclusive().one(db).await?);
    }
    Ok(query.one(db).await?)
}

async fn cancel_guarded_in_transaction<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleAction,
    guard: &StandardMutationGuard,
    now: DateTime<Utc>,
) -> AppResult<()> {
    complete_standard_mutation_in_transaction(db, guard, now).await?;
    cancel_without_guard_in_transaction(db, claim, now, FAILURE_CANCELLED_STALE).await
}

async fn cancel_without_guard_in_transaction<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleAction,
    now: DateTime<Utc>,
    failure_class: &str,
) -> AppResult<()> {
    if !mark_cancelled(db, claim, now, failure_class).await? {
        return Err(AppError::StaleContentMutation);
    }
    log_action_diagnostic(claim, failure_class);
    Ok(())
}

async fn cancel_without_guard(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    failure_class: &str,
) -> AppResult<()> {
    let claim = claim.clone();
    let failure_class = failure_class.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            let Some(_) = lock_claim_for_execution(txn, &claim).await? else {
                return Ok(());
            };
            let now = database_now(txn).await?;
            cancel_without_guard_in_transaction(txn, &claim, now, &failure_class).await
        })
    })
    .await
    .map_err(transaction_error_into_app)
}

/// Settles the exact admission token after the final execution transaction
/// rolled back. The guard clear and the fresh claim-fenced terminal outcome
/// commit together, so a retry or failed-safe row never strands its own token.
/// A stale guard is deliberately not cleared: it belongs to a newer mutation.
#[allow(clippy::too_many_arguments)]
async fn settle_post_admission_failure(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    target: &VersionTargetIdentity,
    guard: &StandardMutationGuard,
    max_attempts: i64,
    base_backoff_secs: u64,
    max_backoff_secs: u64,
    failure_class: &str,
    retry: bool,
) -> AppResult<()> {
    let claim = claim.clone();
    let target = target.clone();
    let guard = guard.clone();
    let failure_class = failure_class.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            let locked_action = lock_claim_for_execution(txn, &claim).await?;
            let now = database_now(txn).await?;
            lock_bucket_for_ownership(txn, &target.bucket).await?;
            match complete_standard_mutation_in_transaction(txn, &guard, now).await {
                Ok(()) | Err(AppError::StaleContentMutation) => {}
                Err(error) => return Err(error),
            }

            let Some(locked_action) = locked_action else {
                return Ok(());
            };
            if retry && locked_action.attempts < max_attempts {
                let next_attempt_at = retry_at(
                    now,
                    locked_action.attempts,
                    base_backoff_secs,
                    max_backoff_secs,
                )?;
                if !schedule_retry(txn, &claim, now, next_attempt_at, &failure_class).await? {
                    return Err(AppError::StaleContentMutation);
                }
            } else if !mark_failed_safe(txn, &claim, now, &failure_class).await? {
                return Err(AppError::StaleContentMutation);
            }
            log_action_diagnostic(&claim, &failure_class);
            Ok(())
        })
    })
    .await
    .map_err(transaction_error_into_app)
}

async fn retry_or_fail_safe(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    max_attempts: i64,
    base_backoff_secs: u64,
    max_backoff_secs: u64,
    failure_class: &str,
) -> AppResult<()> {
    let claim = claim.clone();
    let failure_class = failure_class.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            let Some(locked_action) = lock_claim_for_execution(txn, &claim).await? else {
                return Ok(());
            };
            let now = database_now(txn).await?;
            if locked_action.attempts >= max_attempts {
                lock_bucket_for_ownership(txn, &locked_action.bucket).await?;
                clear_lifecycle_mutation_if_owned(
                    txn,
                    &locked_action.bucket,
                    &locked_action.object_key,
                    &locked_action.id,
                    claim.claim_epoch,
                    now,
                )
                .await?;
                if !mark_failed_safe(txn, &claim, now, &failure_class).await? {
                    return Err(AppError::StaleContentMutation);
                }
            } else {
                let next_attempt_at = retry_at(
                    now,
                    locked_action.attempts,
                    base_backoff_secs,
                    max_backoff_secs,
                )?;
                if !schedule_retry(txn, &claim, now, next_attempt_at, &failure_class).await? {
                    return Err(AppError::StaleContentMutation);
                }
            }
            log_action_diagnostic(&claim, &failure_class);
            Ok(())
        })
    })
    .await
    .map_err(transaction_error_into_app)
}

async fn fail_safe(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    failure_class: &str,
    diagnostic_class: &str,
) -> AppResult<()> {
    let claim = claim.clone();
    let failure_class = failure_class.to_owned();
    let diagnostic_class = diagnostic_class.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            let Some(_) = lock_claim_for_execution(txn, &claim).await? else {
                return Ok(());
            };
            let now = database_now(txn).await?;
            lock_bucket_for_ownership(txn, &claim.action.bucket).await?;
            clear_lifecycle_mutation_if_owned(
                txn,
                &claim.action.bucket,
                &claim.action.object_key,
                &claim.action.id,
                claim.claim_epoch,
                now,
            )
            .await?;
            if !mark_failed_safe(txn, &claim, now, &failure_class).await? {
                return Err(AppError::StaleContentMutation);
            }
            log_action_diagnostic(&claim, &diagnostic_class);
            Ok(())
        })
    })
    .await
    .map_err(transaction_error_into_app)
}

fn log_action_diagnostic(claim: &ClaimedLifecycleAction, failure_class: &str) {
    tracing::warn!(
        action_id = %claim.action.id,
        bucket = %claim.action.bucket,
        key = %claim.action.object_key,
        public_version_id = %claim.action.target_public_version_id,
        revision = claim.action.config_revision,
        failure_class,
        "lifecycle action terminal outcome"
    );
}

fn is_temporary_execution_error(error: &AppError) -> bool {
    matches!(error, AppError::StaleContentMutation)
        || matches!(error, AppError::Database(message) if is_temporary_database_contention(message))
}

fn transaction_error_into_app(error: TransactionError<AppError>) -> AppError {
    match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => error.into(),
    }
}

fn same_action_definition(left: &lifecycle_action::Model, right: &lifecycle_action::Model) -> bool {
    left.id == right.id
        && left.idempotency_key == right.idempotency_key
        && left.bucket == right.bucket
        && left.object_key == right.object_key
        && left.config_revision == right.config_revision
        && left.rule_id == right.rule_id
        && left.action_kind == right.action_kind
        && left.target_version_row_id == right.target_version_row_id
        && left.target_public_version_id == right.target_public_version_id
        && left.target_object_id == right.target_object_id
        && left.target_sequence == right.target_sequence
        && left.due_at == right.due_at
}

fn target_from_action(action: &lifecycle_action::Model) -> AppResult<VersionTargetIdentity> {
    let action_kind = action_kind_from_db(&action.action_kind)?;
    let kind = match action_kind {
        LifecycleActionKind::ExpireCurrent => VersionKind::Object,
        LifecycleActionKind::DeleteExpiredMarker => VersionKind::DeleteMarker,
        LifecycleActionKind::ExpireNoncurrent => match action.target_object_id {
            Some(_) => VersionKind::Object,
            None => VersionKind::DeleteMarker,
        },
    };
    let object_id = action.target_object_id.clone();
    if action.bucket.is_empty()
        || action.object_key.is_empty()
        || action.target_version_row_id.is_empty()
        || action.target_sequence < 0
        || matches!(kind, VersionKind::Object) != object_id.is_some()
    {
        return Err(AppError::Internal(
            "invalid lifecycle action identity".to_owned(),
        ));
    }
    Ok(VersionTargetIdentity {
        bucket: action.bucket.clone(),
        key: action.object_key.clone(),
        version_row_id: action.target_version_row_id.clone(),
        public_version_id: PublicVersionId::parse_s3(&action.target_public_version_id)
            .map_err(|_| AppError::Internal("invalid lifecycle action identity".to_owned()))?,
        kind,
        object_id,
        sequence: action.target_sequence,
    })
}

fn action_kind_from_db(value: &str) -> AppResult<LifecycleActionKind> {
    match value {
        "expire_current" => Ok(LifecycleActionKind::ExpireCurrent),
        "expire_noncurrent" => Ok(LifecycleActionKind::ExpireNoncurrent),
        "delete_expired_marker" => Ok(LifecycleActionKind::DeleteExpiredMarker),
        _ => Err(AppError::Internal(
            "invalid lifecycle action kind".to_owned(),
        )),
    }
}

fn action_matches_expected(
    action: &lifecycle_action::Model,
    expected: &crate::lifecycle::model::NewLifecycleAction,
) -> bool {
    action.bucket == expected.bucket
        && action.config_revision == expected.config_revision
        && action.rule_id == persisted_rule_identity(&expected.rule_identity)
        && action.action_kind == persisted_action_kind(expected.action_kind)
        && action.target_version_row_id == expected.target.version_row_id
        && action.target_public_version_id == expected.target.public_version_id.as_s3_str()
        && action.target_object_id == expected.target.object_id
        && action.target_sequence == expected.target.sequence
        && action.due_at == expected.due_at
}

fn persisted_rule_identity(rule_identity: &RuleIdentity) -> String {
    match rule_identity {
        RuleIdentity::Id(id) => format!("id:{id}"),
        RuleIdentity::Ordinal(ordinal) => format!("ordinal:{ordinal}"),
    }
}

fn persisted_action_kind(action_kind: LifecycleActionKind) -> &'static str {
    match action_kind {
        LifecycleActionKind::ExpireCurrent => "expire_current",
        LifecycleActionKind::ExpireNoncurrent => "expire_noncurrent",
        LifecycleActionKind::DeleteExpiredMarker => "delete_expired_marker",
    }
}

/// Classifies admission failures without relying on the database error's raw
/// text outside the temporary-contention cases Task 8 can safely retry.
fn classify_lifecycle_admission_error(error: &AppError) -> LifecycleAdmissionClassification {
    match error {
        AppError::StaleContentMutation => LifecycleAdmissionClassification::Stale,
        AppError::Database(message) if is_temporary_database_contention(message) => {
            LifecycleAdmissionClassification::Temporary
        }
        _ => LifecycleAdmissionClassification::Other,
    }
}

/// Executes one already-admitted current lifecycle action inside the caller's
/// transaction. Lifecycle action claim and terminal-state persistence remain
/// intentionally outside this primitive for Task 8 to compose atomically.
pub(crate) async fn execute_lifecycle_delete_guarded(
    txn: &DatabaseTransaction,
    target: &VersionTargetIdentity,
    action_kind: LifecycleActionKind,
    guard: &StandardMutationGuard,
    now: DateTime<Utc>,
) -> AppResult<GuardedLifecycleExecutionResult> {
    if target.bucket != guard.bucket || target.key != guard.key {
        return Ok(GuardedLifecycleExecutionResult::Stale);
    }

    lock_bucket_for_ownership(txn, &target.bucket).await?;
    match verify_standard_mutation_guard(txn, guard, &target.bucket, &target.key, &[]).await {
        Ok(()) => {}
        Err(AppError::StaleContentMutation) => return Ok(GuardedLifecycleExecutionResult::Stale),
        Err(error) => return Err(error),
    }
    let versioning_state = crate::store::bucket::lock_versioning_state(txn, &target.bucket).await?;
    let Some(selected) = lock_version_by_id(txn, &target.version_row_id).await? else {
        return Ok(GuardedLifecycleExecutionResult::Stale);
    };
    if !target_matches_row(&selected, target)? {
        return Ok(GuardedLifecycleExecutionResult::Stale);
    }

    let result = match action_kind {
        LifecycleActionKind::ExpireCurrent => {
            if target.kind != VersionKind::Object || !selected.is_latest {
                return Ok(GuardedLifecycleExecutionResult::Stale);
            }
            let deleted = match versioning_state {
                BucketVersioningState::Unversioned => {
                    delete_unversioned_current_in_transaction(txn, &target.bucket, &target.key, now)
                        .await?
                }
                BucketVersioningState::Enabled => {
                    delete_enabled_current_in_transaction(txn, &target.bucket, &target.key, now)
                        .await?
                }
                BucketVersioningState::Suspended => {
                    delete_suspended_current_in_transaction(txn, &target.bucket, &target.key, now)
                        .await?
                }
            };
            GuardedLifecycleExecutionResult::Applied(deleted)
        }
        LifecycleActionKind::DeleteExpiredMarker => {
            if target.kind != VersionKind::DeleteMarker || !selected.is_latest {
                return Ok(GuardedLifecycleExecutionResult::Stale);
            }
            match delete_current_sole_marker(txn, &selected).await? {
                ExactCurrentMarkerDeleteResult::Deleted(deleted) => {
                    GuardedLifecycleExecutionResult::Applied(deleted)
                }
                ExactCurrentMarkerDeleteResult::AlreadySatisfied => {
                    GuardedLifecycleExecutionResult::AlreadySatisfied
                }
                ExactCurrentMarkerDeleteResult::Stale => {
                    return Ok(GuardedLifecycleExecutionResult::Stale);
                }
            }
        }
        LifecycleActionKind::ExpireNoncurrent => {
            if selected.is_latest {
                return Ok(GuardedLifecycleExecutionResult::Stale);
            }
            GuardedLifecycleExecutionResult::Applied(
                delete_exact_in_transaction(
                    txn,
                    &target.bucket,
                    &target.key,
                    target.public_version_id.clone(),
                    now,
                )
                .await?,
            )
        }
    };
    complete_standard_mutation_in_transaction(txn, guard, now).await?;
    Ok(result)
}

fn target_matches_row(
    selected: &crate::store::entities::object_version::Model,
    target: &VersionTargetIdentity,
) -> AppResult<bool> {
    Ok(selected.bucket == target.bucket
        && selected.key == target.key
        && selected.sequence == target.sequence
        && selected.object_id == target.object_id
        && version_kind(selected)? == target.kind
        && public_version_id(selected)? == target.public_version_id)
}

fn is_temporary_database_contention(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database is busy")
        || message.contains("deadlock detected")
        || message.contains("could not serialize access")
        || message.contains("serialization failure")
        || message.contains("sqlstate 40p01")
        || message.contains("sqlstate 40001")
        || message.contains("code: 40p01")
        || message.contains("code: 40001")
}

#[cfg(test)]
mod test_hooks {
    use std::sync::{LazyLock, Mutex};

    use crate::{
        error::{AppError, AppResult},
        store::lifecycle_action::FAILURE_DATABASE_CONTENTION,
    };

    static NEXT_POST_ADMISSION_FAILURE: LazyLock<Mutex<Option<String>>> =
        LazyLock::new(|| Mutex::new(None));

    pub struct FailureScope {
        action_id: String,
    }

    pub fn fail_next_temporary_post_admission(action_id: &str) -> FailureScope {
        *NEXT_POST_ADMISSION_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(action_id.to_owned());
        FailureScope {
            action_id: action_id.to_owned(),
        }
    }

    pub async fn fail_after_admission(action_id: &str) -> AppResult<()> {
        let mut failure = NEXT_POST_ADMISSION_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if failure.as_deref() == Some(action_id) {
            *failure = None;
            return Err(AppError::Database(format!(
                "{FAILURE_DATABASE_CONTENTION}: database is locked"
            )));
        }
        Ok(())
    }

    impl Drop for FailureScope {
        fn drop(&mut self) {
            let mut failure = NEXT_POST_ADMISSION_FAILURE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if failure.as_deref() == Some(self.action_id.as_str()) {
                *failure = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use chrono::{Duration, TimeZone, Utc};
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait, PaginatorTrait,
        QueryFilter, TransactionTrait,
    };
    use tracing_subscriber::fmt::MakeWriter;

    use super::{
        admit_lifecycle_expiration, execute_claimed_lifecycle_action, fail_safe,
        is_temporary_execution_error, log_action_diagnostic, retry_or_fail_safe,
        settle_post_admission_failure,
    };
    use crate::{
        error::AppError,
        lifecycle::{
            config::canonical_json,
            model::{
                CanonicalFilter, CanonicalLifecycleConfiguration, CanonicalLifecycleRule,
                CanonicalRuleSelector, CanonicalTag, ClaimedLifecycleAction, CurrentExpiration,
                LifecycleActionKind, LifecycleRuleStatus, NewLifecycleAction, NoncurrentExpiration,
                RuleIdentity, VersionTargetIdentity,
            },
        },
        pinning::{policy::PublicationPolicy, tags::ObjectTag},
        store::{
            bucket,
            database_clock::database_now,
            entities::{
                bucket_lifecycle_config, import_destination, lifecycle_action, object, object_tag,
                object_version,
            },
            import::ownership::{
                admit_content_mutation, complete_standard_mutation_in_transaction,
                lock_bucket_for_ownership,
            },
            lifecycle_action::{
                MAX_LIFECYCLE_ACTION_ATTEMPTS, claim_due, idempotency_key, insert_idempotent,
            },
            lifecycle_config::{delete_configuration, put_configuration},
            object_version::{
                BucketVersioningState, PublicVersionId, VersionKind, install_delete_marker,
                remove_and_promote,
            },
            pinning::publication::{
                PinTargetSpec, PublicationObject, PublicationRequest, publish_object,
            },
            pinning::tags::replace_object_tags,
            run_migrations,
        },
    };

    const WORKER: &str = "lifecycle-action-execution-test";

    #[derive(Clone)]
    struct LogCapture(Arc<Mutex<Vec<u8>>>);

    struct LogWriter(LogCapture);

    impl Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for LogCapture {
        type Writer = LogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            LogWriter(self.clone())
        }
    }

    fn at(year: i32, month: u32, day: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
            .single()
            .unwrap()
    }

    fn all() -> CanonicalRuleSelector {
        CanonicalRuleSelector::Modern {
            filter: CanonicalFilter::All,
        }
    }

    fn current_rule(id: &str, selector: CanonicalRuleSelector) -> CanonicalLifecycleRule {
        CanonicalLifecycleRule {
            id: Some(id.to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector,
            expiration: Some(CurrentExpiration::Date {
                utc_midnight: at(2000, 1, 1),
            }),
            noncurrent_version_expiration: None,
        }
    }

    fn noncurrent_rule(id: &str, newer_noncurrent_versions: Option<u16>) -> CanonicalLifecycleRule {
        CanonicalLifecycleRule {
            id: Some(id.to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector: all(),
            expiration: None,
            noncurrent_version_expiration: Some(NoncurrentExpiration {
                noncurrent_days: 1,
                newer_noncurrent_versions,
            }),
        }
    }

    fn configuration(rules: Vec<CanonicalLifecycleRule>) -> CanonicalLifecycleConfiguration {
        CanonicalLifecycleConfiguration {
            schema_version: 1,
            rules,
        }
    }

    async fn setup() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        run_migrations(&db).await.unwrap();
        bucket::create(&db, "bucket", None).await.unwrap();
        db
    }

    async fn configure(
        db: &DatabaseConnection,
        rules: Vec<CanonicalLifecycleRule>,
    ) -> (i64, CanonicalLifecycleConfiguration) {
        let configuration = configuration(rules);
        let json = canonical_json(&configuration).unwrap();
        let revision = put_configuration(db, "bucket", &json).await.unwrap();
        (revision, configuration)
    }

    async fn replace_configuration_without_revising(
        db: &DatabaseConnection,
        configuration: &CanonicalLifecycleConfiguration,
    ) {
        bucket_lifecycle_config::Entity::update_many()
            .col_expr(
                bucket_lifecycle_config::Column::CanonicalJson,
                sea_orm::sea_query::Expr::value(Some(canonical_json(configuration).unwrap())),
            )
            .filter(bucket_lifecycle_config::Column::Bucket.eq("bucket"))
            .exec(db)
            .await
            .unwrap();
    }

    async fn publish(
        db: &DatabaseConnection,
        id: &str,
        key: &str,
        size: i64,
        tags: Vec<ObjectTag>,
    ) -> object_version::Model {
        let object = PublicationObject::from_put(
            id.to_owned(),
            "bucket",
            key,
            format!("cid-{id}"),
            size,
            None,
            None,
            false,
            None,
            None,
            Utc::now(),
        );
        let request = PublicationRequest {
            object_target: PinTargetSpec {
                cid: object.cid.clone(),
                logical_size: object.logical_size,
            },
            object,
            tags: tags.clone(),
            policy: PublicationPolicy {
                tags,
                leases: Vec::new(),
            },
        };
        publish_object(db, request, &Default::default())
            .await
            .unwrap();
        object_version::Entity::find()
            .filter(object_version::Column::ObjectId.eq(id))
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    fn target(version: &object_version::Model) -> VersionTargetIdentity {
        VersionTargetIdentity {
            bucket: version.bucket.clone(),
            key: version.key.clone(),
            version_row_id: version.id.clone(),
            public_version_id: match version.version_id.as_deref() {
                Some(version_id) => PublicVersionId::parse_s3(version_id).unwrap(),
                None => PublicVersionId::Null,
            },
            kind: match version.kind.as_str() {
                "object" => VersionKind::Object,
                "delete_marker" => VersionKind::DeleteMarker,
                other => panic!("unexpected version kind {other}"),
            },
            object_id: version.object_id.clone(),
            sequence: version.sequence,
        }
    }

    async fn claim(
        db: &DatabaseConnection,
        revision: i64,
        rule_id: &str,
        action_kind: LifecycleActionKind,
        target: VersionTargetIdentity,
    ) -> ClaimedLifecycleAction {
        let now = database_now(db).await.unwrap();
        let due_at = if action_kind == LifecycleActionKind::ExpireNoncurrent {
            let stored = object_version::Entity::find_by_id(target.version_row_id.clone())
                .one(db)
                .await
                .unwrap()
                .unwrap();
            crate::lifecycle::evaluator::next_utc_midnight_after_full_days(
                stored.became_noncurrent_at.unwrap(),
                1,
            )
            .unwrap()
        } else {
            at(2000, 1, 1)
        };
        let mut action = NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: "bucket".to_owned(),
            config_revision: revision,
            rule_identity: RuleIdentity::Id(rule_id.to_owned()),
            action_kind,
            target,
            due_at,
        };
        action.idempotency_key = idempotency_key(&action).unwrap();
        assert!(insert_idempotent(db, action, now).await.unwrap());
        claim_due(db, WORKER, Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap()
    }

    async fn state(db: &DatabaseConnection, claim: &ClaimedLifecycleAction) -> Option<String> {
        lifecycle_action::Entity::find_by_id(claim.action.id.clone())
            .one(db)
            .await
            .unwrap()
            .map(|action| action.state)
    }

    async fn assert_standard_guard_settled(db: &DatabaseConnection, key: &str) {
        let destination =
            import_destination::Entity::find_by_id(("bucket".to_owned(), key.to_owned()))
                .one(db)
                .await
                .unwrap()
                .unwrap();
        assert!(
            destination.mutation_id.is_none() && destination.mutation_prefix.is_none(),
            "lifecycle execution must settle its standard mutation guard"
        );
    }

    async fn assert_subsequent_standard_mutation_can_be_admitted(
        db: &DatabaseConnection,
        target: &VersionTargetIdentity,
    ) {
        let now = database_now(db).await.unwrap();
        let guard = match super::admit_lifecycle_expiration(
            db,
            target,
            "subsequent-admission-test",
            1,
            now,
        )
        .await
        .unwrap()
        {
            super::LifecycleAdmissionResult::Admitted(guard) => guard,
            outcome => panic!("expected a fresh standard mutation admission, got {outcome:?}"),
        };
        let bucket = target.bucket.clone();
        db.transaction(move |txn| {
            Box::pin(async move {
                lock_bucket_for_ownership(txn, &bucket).await?;
                complete_standard_mutation_in_transaction(txn, &guard, database_now(txn).await?)
                    .await
            })
        })
        .await
        .unwrap();
    }

    async fn execute(db: &DatabaseConnection, claim: &ClaimedLifecycleAction) {
        execute_claimed_lifecycle_action(db, claim, MAX_LIFECYCLE_ACTION_ATTEMPTS, 1, 60)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn lifecycle_action_execution_applies_current_and_claim_fences_terminal_success() {
        let db = setup().await;
        let (revision, _) = configure(&db, vec![current_rule("current", all())]).await;
        let version = publish(&db, "current-owner", "current", 7, vec![]).await;
        let claim = claim(
            &db,
            revision,
            "current",
            LifecycleActionKind::ExpireCurrent,
            target(&version),
        )
        .await;

        execute(&db, &claim).await;

        assert_eq!(state(&db, &claim).await.as_deref(), Some("succeeded"));
        assert!(
            object_version::Entity::find_by_id(version.id)
                .one(&db)
                .await
                .unwrap()
                .is_none(),
            "unversioned expiration removes the selected index row"
        );
        assert_standard_guard_settled(&db, "current").await;
    }

    #[tokio::test]
    async fn lifecycle_action_execution_cancels_replaced_deleted_missing_disabled_and_changed_filters()
     {
        let cases = [
            "config_replace",
            "config_delete",
            "missing_rule",
            "disabled_rule",
            "prefix",
            "size",
            "tag",
        ];
        for case in cases {
            let db = setup().await;
            let selector = match case {
                "prefix" => CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::Prefix {
                        prefix: "logs/".to_owned(),
                    },
                },
                "size" => CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::ObjectSizeGreaterThan { bytes: 5 },
                },
                "tag" => CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::Tag {
                        tag: CanonicalTag {
                            key: "class".to_owned(),
                            value: "cold".to_owned(),
                        },
                    },
                },
                _ => all(),
            };
            let (revision, _) = configure(&db, vec![current_rule("current", selector)]).await;
            let version = publish(
                &db,
                &format!("owner-{case}"),
                "logs/object",
                7,
                vec![ObjectTag::new("class", "cold")],
            )
            .await;
            let claim = claim(
                &db,
                revision,
                "current",
                LifecycleActionKind::ExpireCurrent,
                target(&version),
            )
            .await;

            match case {
                "config_replace" => {
                    configure(&db, vec![current_rule("replacement", all())]).await;
                }
                "config_delete" => {
                    delete_configuration(&db, "bucket").await.unwrap();
                }
                "missing_rule" => {
                    replace_configuration_without_revising(
                        &db,
                        &configuration(vec![current_rule("other", all())]),
                    )
                    .await;
                }
                "disabled_rule" => {
                    let mut disabled = current_rule("current", all());
                    disabled.status = LifecycleRuleStatus::Disabled;
                    replace_configuration_without_revising(&db, &configuration(vec![disabled]))
                        .await;
                }
                "prefix" => {
                    replace_configuration_without_revising(
                        &db,
                        &configuration(vec![current_rule(
                            "current",
                            CanonicalRuleSelector::Modern {
                                filter: CanonicalFilter::Prefix {
                                    prefix: "other/".to_owned(),
                                },
                            },
                        )]),
                    )
                    .await;
                }
                "size" => {
                    object::Entity::update_many()
                        .col_expr(object::Column::Size, sea_orm::sea_query::Expr::value(5_i64))
                        .filter(object::Column::Id.eq(format!("owner-{case}")))
                        .exec(&db)
                        .await
                        .unwrap();
                }
                "tag" => {
                    replace_object_tags(
                        &db,
                        &format!("owner-{case}"),
                        &[ObjectTag::new("class", "hot")],
                    )
                    .await
                    .unwrap();
                }
                _ => unreachable!(),
            }

            execute(&db, &claim).await;
            assert_eq!(
                state(&db, &claim).await.as_deref(),
                Some("cancelled"),
                "{case}"
            );
            assert!(
                object_version::Entity::find_by_id(version.id)
                    .one(&db)
                    .await
                    .unwrap()
                    .is_some(),
                "{case} must not mutate the revalidated target"
            );
            assert_standard_guard_settled(&db, "logs/object").await;
        }
    }

    #[tokio::test]
    async fn lifecycle_action_execution_cancels_removed_reclaimed_and_mismatched_action_rows() {
        for case in ["removed", "object", "sequence"] {
            let db = setup().await;
            let (revision, _) = configure(&db, vec![current_rule("current", all())]).await;
            let version = publish(&db, &format!("owner-{case}"), "object", 7, vec![]).await;
            let action_target = target(&version);
            let claim = claim(
                &db,
                revision,
                "current",
                LifecycleActionKind::ExpireCurrent,
                action_target.clone(),
            )
            .await;
            match case {
                "removed" => {
                    lifecycle_action::Entity::delete_by_id(claim.action.id.clone())
                        .exec(&db)
                        .await
                        .unwrap();
                }
                "object" => {
                    lifecycle_action::Entity::update_many()
                        .col_expr(
                            lifecycle_action::Column::TargetObjectId,
                            sea_orm::sea_query::Expr::value(Some("different-owner")),
                        )
                        .filter(lifecycle_action::Column::Id.eq(claim.action.id.clone()))
                        .exec(&db)
                        .await
                        .unwrap();
                }
                "sequence" => {
                    lifecycle_action::Entity::update_many()
                        .col_expr(
                            lifecycle_action::Column::TargetSequence,
                            sea_orm::sea_query::Expr::value(version.sequence + 1),
                        )
                        .filter(lifecycle_action::Column::Id.eq(claim.action.id.clone()))
                        .exec(&db)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            execute(&db, &claim).await;
            assert_eq!(
                state(&db, &claim).await.as_deref(),
                (case != "removed").then_some("cancelled"),
                "{case} must not receive an unfenced terminal write"
            );
            assert_standard_guard_settled(&db, "object").await;
            if case != "removed" {
                assert_subsequent_standard_mutation_can_be_admitted(&db, &action_target).await;
            }
        }
    }

    #[tokio::test]
    async fn lifecycle_action_execution_deletes_exact_noncurrent_content_and_marker_only() {
        let db = setup().await;
        bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
            .await
            .unwrap();
        let (revision, _) = configure(&db, vec![noncurrent_rule("noncurrent", None)]).await;
        let old = publish(
            &db,
            "noncurrent-content",
            "content",
            7,
            vec![ObjectTag::new("class", "cold")],
        )
        .await;
        let _current = publish(&db, "noncurrent-current", "content", 9, vec![]).await;
        object_version::Entity::update_many()
            .col_expr(
                object_version::Column::BecameNoncurrentAt,
                sea_orm::sea_query::Expr::value(Some(Utc::now() - Duration::days(3))),
            )
            .filter(object_version::Column::Id.eq(old.id.clone()))
            .exec(&db)
            .await
            .unwrap();
        let claim = claim(
            &db,
            revision,
            "noncurrent",
            LifecycleActionKind::ExpireNoncurrent,
            target(&old),
        )
        .await;

        execute(&db, &claim).await;

        assert_eq!(state(&db, &claim).await.as_deref(), Some("succeeded"));
        assert!(
            object_version::Entity::find_by_id(old.id)
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            object_tag::Entity::find()
                .filter(object_tag::Column::ObjectId.eq("noncurrent-content"))
                .count(&db)
                .await
                .unwrap(),
            0,
            "exact content deletion ends only the selected internal object's tags"
        );
    }

    #[tokio::test]
    async fn lifecycle_action_execution_cancels_current_replacement_promotion_exact_delete_marker_count_and_conflict()
     {
        let cases = [
            "current_replacement",
            "target_exact_delete",
            "conflicting_winner",
        ];
        for case in cases {
            let db = setup().await;
            let rules = if case == "conflicting_winner" {
                vec![
                    current_rule("a-winner", all()),
                    current_rule("z-loser", all()),
                ]
            } else {
                vec![current_rule("current", all())]
            };
            let (revision, _) = configure(&db, rules).await;
            let version = publish(&db, &format!("owner-{case}"), "object", 7, vec![]).await;
            let claim = claim(
                &db,
                revision,
                if case == "conflicting_winner" {
                    "z-loser"
                } else {
                    "current"
                },
                LifecycleActionKind::ExpireCurrent,
                target(&version),
            )
            .await;
            match case {
                "current_replacement" => {
                    bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
                        .await
                        .unwrap();
                    publish(&db, "replacement", "object", 9, vec![]).await;
                }
                "target_exact_delete" => {
                    object_version::Entity::delete_by_id(version.id.clone())
                        .exec(&db)
                        .await
                        .unwrap();
                }
                "conflicting_winner" => {}
                _ => unreachable!(),
            }
            execute(&db, &claim).await;
            assert_eq!(
                state(&db, &claim).await.as_deref(),
                Some("cancelled"),
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn lifecycle_action_execution_deletes_exact_noncurrent_marker_without_touching_content() {
        let db = setup().await;
        bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
            .await
            .unwrap();
        let (revision, _) = configure(&db, vec![noncurrent_rule("noncurrent", None)]).await;
        let content = publish(
            &db,
            "marker-content-owner",
            "marker-history",
            7,
            vec![ObjectTag::new("class", "cold")],
        )
        .await;
        let marker_id = db
            .transaction(|txn| {
                Box::pin(async move {
                    install_delete_marker(
                        txn,
                        BucketVersioningState::Enabled,
                        "bucket",
                        "marker-history",
                        database_now(txn).await?,
                    )
                    .await
                })
            })
            .await
            .unwrap();
        let _current = publish(&db, "marker-new-current", "marker-history", 9, vec![]).await;
        let marker = object_version::Entity::find()
            .filter(object_version::Column::VersionId.eq(marker_id))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        object_version::Entity::update_many()
            .col_expr(
                object_version::Column::BecameNoncurrentAt,
                sea_orm::sea_query::Expr::value(Some(Utc::now() - Duration::days(3))),
            )
            .filter(object_version::Column::Id.eq(marker.id.clone()))
            .exec(&db)
            .await
            .unwrap();
        let tags_before =
            crate::store::pinning::tags::list_object_tags(&db, "marker-content-owner")
                .await
                .unwrap();
        let claim = claim(
            &db,
            revision,
            "noncurrent",
            LifecycleActionKind::ExpireNoncurrent,
            target(&marker),
        )
        .await;

        execute(&db, &claim).await;

        assert_eq!(state(&db, &claim).await.as_deref(), Some("succeeded"));
        assert!(
            object_version::Entity::find_by_id(marker.id)
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            crate::store::pinning::tags::list_object_tags(&db, "marker-content-owner")
                .await
                .unwrap(),
            tags_before,
            "an exact noncurrent marker delete has no object/tag/lease ownership side effects"
        );
        assert!(
            object_version::Entity::find_by_id(content.id)
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn lifecycle_action_execution_requires_current_sole_marker_and_treats_exact_race_as_success()
     {
        for case in ["not_sole", "already_satisfied"] {
            let db = setup().await;
            bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
                .await
                .unwrap();
            let (revision, _) = configure(&db, vec![current_rule("marker", all())]).await;
            let content = publish(
                &db,
                &format!("marker-owner-{case}"),
                "marker",
                7,
                vec![ObjectTag::new("class", "cold")],
            )
            .await;
            let marker_id = db
                .transaction(|txn| {
                    Box::pin(async move {
                        install_delete_marker(
                            txn,
                            BucketVersioningState::Enabled,
                            "bucket",
                            "marker",
                            database_now(txn).await?,
                        )
                        .await
                    })
                })
                .await
                .unwrap();
            let marker = object_version::Entity::find()
                .filter(object_version::Column::VersionId.eq(marker_id))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            if case == "already_satisfied" {
                db.transaction(move |txn| {
                    Box::pin(async move {
                        remove_and_promote(txn, &content).await?;
                        Ok::<(), AppError>(())
                    })
                })
                .await
                .unwrap();
                db.execute_unprepared(&format!(
                    "CREATE TRIGGER lifecycle_exact_marker_race BEFORE DELETE ON object_versions \
                     WHEN OLD.id = '{}' BEGIN DELETE FROM object_versions WHERE id = OLD.id; \
                     SELECT RAISE(IGNORE); END;",
                    marker.id
                ))
                .await
                .unwrap();
            }
            let tags_before =
                crate::store::pinning::tags::list_object_tags(&db, &format!("marker-owner-{case}"))
                    .await
                    .unwrap();
            let claim = claim(
                &db,
                revision,
                "marker",
                LifecycleActionKind::DeleteExpiredMarker,
                target(&marker),
            )
            .await;

            execute(&db, &claim).await;

            assert_eq!(
                state(&db, &claim).await.as_deref(),
                Some(if case == "not_sole" {
                    "cancelled"
                } else {
                    "succeeded"
                }),
                "{case}"
            );
            assert_eq!(
                crate::store::pinning::tags::list_object_tags(
                    &db,
                    &format!("marker-owner-{case}"),
                )
                .await
                .unwrap(),
                tags_before,
                "marker execution never ends content ownership"
            );
            assert_standard_guard_settled(&db, "marker").await;
        }
    }

    #[tokio::test]
    async fn lifecycle_action_execution_cancels_promoted_and_newer_count_invalidated_noncurrent_targets()
     {
        for case in ["promoted", "newer_count"] {
            let db = setup().await;
            bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
                .await
                .unwrap();
            let (revision, _) = configure(
                &db,
                vec![noncurrent_rule(
                    "noncurrent",
                    (case == "newer_count").then_some(1),
                )],
            )
            .await;
            let first = publish(&db, &format!("{case}-first"), "history", 7, vec![]).await;
            let second = publish(&db, &format!("{case}-second"), "history", 8, vec![]).await;
            let third = publish(&db, &format!("{case}-third"), "history", 9, vec![]).await;
            let fourth = (case == "newer_count")
                .then(|| publish(&db, "newer-count-fourth", "history", 10, vec![]));
            if let Some(fourth) = fourth {
                fourth.await;
            }
            object_version::Entity::update_many()
                .col_expr(
                    object_version::Column::BecameNoncurrentAt,
                    sea_orm::sea_query::Expr::value(Some(Utc::now() - Duration::days(3))),
                )
                .filter(object_version::Column::Key.eq("history"))
                .filter(object_version::Column::IsLatest.eq(false))
                .exec(&db)
                .await
                .unwrap();
            let claim = claim(
                &db,
                revision,
                "noncurrent",
                LifecycleActionKind::ExpireNoncurrent,
                target(&first),
            )
            .await;
            if case == "promoted" {
                db.transaction(move |txn| {
                    Box::pin(async move {
                        remove_and_promote(txn, &third).await?;
                        remove_and_promote(txn, &second).await?;
                        Ok::<(), AppError>(())
                    })
                })
                .await
                .unwrap();
            } else {
                db.transaction(move |txn| {
                    Box::pin(async move {
                        remove_and_promote(txn, &third).await?;
                        Ok::<(), AppError>(())
                    })
                })
                .await
                .unwrap();
            }

            execute(&db, &claim).await;

            assert_eq!(
                state(&db, &claim).await.as_deref(),
                Some("cancelled"),
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn lifecycle_action_execution_retries_only_temporary_failures_with_documented_backoff_and_fails_safe()
     {
        let db = setup().await;
        let (revision, _) = configure(&db, vec![current_rule("current", all())]).await;
        let version = publish(&db, "retry-owner", "retry", 7, vec![]).await;
        let claim = claim(
            &db,
            revision,
            "current",
            LifecycleActionKind::ExpireCurrent,
            target(&version),
        )
        .await;

        assert!(is_temporary_execution_error(&AppError::Database(
            "database is locked".to_owned(),
        )));
        assert!(is_temporary_execution_error(&AppError::Database(
            "SQLSTATE 40001".to_owned(),
        )));
        assert!(!is_temporary_execution_error(&AppError::Database(
            "raw provider body credential=secret".to_owned(),
        )));
        retry_or_fail_safe(
            &db,
            &claim,
            MAX_LIFECYCLE_ACTION_ATTEMPTS,
            2,
            60,
            crate::store::lifecycle_action::FAILURE_DATABASE_CONTENTION,
        )
        .await
        .unwrap();
        let retried = lifecycle_action::Entity::find_by_id(claim.action.id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.state, "pending");
        assert_eq!(
            retried.failure_class.as_deref(),
            Some("database_contention")
        );
        assert_eq!(
            retried.next_attempt_at - retried.updated_at,
            Duration::seconds(2)
        );

        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::State,
                sea_orm::sea_query::Expr::value("claimed"),
            )
            .col_expr(
                lifecycle_action::Column::ClaimedBy,
                sea_orm::sea_query::Expr::value(Some(WORKER)),
            )
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                sea_orm::sea_query::Expr::value(Some(
                    database_now(&db).await.unwrap() + Duration::seconds(30),
                )),
            )
            .col_expr(
                lifecycle_action::Column::Attempts,
                sea_orm::sea_query::Expr::value(MAX_LIFECYCLE_ACTION_ATTEMPTS),
            )
            .filter(lifecycle_action::Column::Id.eq(claim.action.id.clone()))
            .exec(&db)
            .await
            .unwrap();
        retry_or_fail_safe(
            &db,
            &claim,
            MAX_LIFECYCLE_ACTION_ATTEMPTS,
            2,
            60,
            crate::store::lifecycle_action::FAILURE_DATABASE_CONTENTION,
        )
        .await
        .unwrap();
        assert_eq!(state(&db, &claim).await.as_deref(), Some("failed_safe"));
    }

    #[tokio::test]
    async fn lifecycle_action_execution_retries_a_guard_superseded_by_another_same_key_action() {
        let db = setup().await;
        let (revision, _) = configure(&db, vec![current_rule("current", all())]).await;
        let version = publish(&db, "superseded-guard-owner", "same-key", 7, vec![]).await;
        let target = target(&version);
        let claim = claim(
            &db,
            revision,
            "current",
            LifecycleActionKind::ExpireCurrent,
            target.clone(),
        )
        .await;
        let now = database_now(&db).await.unwrap();
        let stale_guard = match admit_lifecycle_expiration(
            &db,
            &target,
            &claim.action.id,
            claim.claim_epoch,
            now,
        )
        .await
        .unwrap()
        {
            super::LifecycleAdmissionResult::Admitted(guard) => guard,
            other => panic!("expected first admission, got {other:?}"),
        };
        let newer_guard = admit_content_mutation(
            &db,
            &target.bucket,
            &target.key,
            None,
            crate::import::SupersedeReason::PutObject,
            now,
        )
        .await
        .unwrap();

        settle_post_admission_failure(
            &db,
            &claim,
            &target,
            &stale_guard,
            MAX_LIFECYCLE_ACTION_ATTEMPTS,
            1,
            60,
            crate::store::lifecycle_action::FAILURE_DATABASE_CONTENTION,
            true,
        )
        .await
        .unwrap();

        let action = lifecycle_action::Entity::find_by_id(claim.action.id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(action.state, "pending");
        assert!(action.claimed_by.is_none());
        assert!(action.lease_until.is_none());
        assert!(action.next_attempt_at > now);
        let destination =
            import_destination::Entity::find_by_id((target.bucket.clone(), target.key.clone()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            destination.mutation_id.as_deref(),
            Some(newer_guard.mutation_id.as_str())
        );

        db.transaction(move |txn| {
            Box::pin(async move {
                lock_bucket_for_ownership(txn, &newer_guard.bucket).await?;
                complete_standard_mutation_in_transaction(txn, &newer_guard, now).await
            })
        })
        .await
        .unwrap();
        assert_standard_guard_settled(&db, "same-key").await;
    }

    #[tokio::test]
    async fn lifecycle_admission_never_supersedes_an_inflight_user_mutation() {
        let db = setup().await;
        let version = publish(&db, "user-guard-owner", "user-wins", 7, vec![]).await;
        let target = target(&version);
        let now = database_now(&db).await.unwrap();
        let user_guard = admit_content_mutation(
            &db,
            &target.bucket,
            &target.key,
            None,
            crate::import::SupersedeReason::PutObject,
            now,
        )
        .await
        .unwrap();

        assert!(matches!(
            admit_lifecycle_expiration(&db, &target, "blocked-lifecycle-test", 1, now)
                .await
                .unwrap(),
            super::LifecycleAdmissionResult::Temporary
        ));
        let destination =
            import_destination::Entity::find_by_id((target.bucket.clone(), target.key.clone()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            destination.mutation_id.as_deref(),
            Some(user_guard.mutation_id.as_str())
        );

        db.transaction(move |txn| {
            Box::pin(async move {
                lock_bucket_for_ownership(txn, &user_guard.bucket).await?;
                complete_standard_mutation_in_transaction(txn, &user_guard, now).await
            })
        })
        .await
        .unwrap();
        assert_standard_guard_settled(&db, "user-wins").await;
    }

    #[tokio::test]
    async fn lifecycle_action_reclaims_an_orphaned_post_admission_token_with_a_new_epoch() {
        let db = setup().await;
        let (revision, _) = configure(&db, vec![current_rule("current", all())]).await;
        let version = publish(&db, "crashed-admission-owner", "crash-reclaim", 7, vec![]).await;
        let target = target(&version);
        let first = claim(
            &db,
            revision,
            "current",
            LifecycleActionKind::ExpireCurrent,
            target.clone(),
        )
        .await;
        let now = database_now(&db).await.unwrap();
        assert!(matches!(
            admit_lifecycle_expiration(&db, &target, &first.action.id, first.claim_epoch, now,)
                .await
                .unwrap(),
            super::LifecycleAdmissionResult::Admitted(_)
        ));

        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                sea_orm::sea_query::Expr::value(Some(now - Duration::seconds(1))),
            )
            .col_expr(
                lifecycle_action::Column::Attempts,
                sea_orm::sea_query::Expr::value(MAX_LIFECYCLE_ACTION_ATTEMPTS),
            )
            .filter(lifecycle_action::Column::Id.eq(first.action.id.clone()))
            .exec(&db)
            .await
            .unwrap();
        let second = claim_due(&db, "post-crash-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(second.claim_epoch, first.claim_epoch + 1);
        assert_eq!(second.action.attempts, MAX_LIFECYCLE_ACTION_ATTEMPTS + 1);

        execute(&db, &second).await;

        assert_eq!(state(&db, &second).await.as_deref(), Some("succeeded"));
        assert!(
            object_version::Entity::find_by_id(version.id)
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert_standard_guard_settled(&db, "crash-reclaim").await;
    }

    #[tokio::test]
    async fn lifecycle_terminal_failure_clears_only_its_owned_admission_token() {
        for newer_user_guard in [false, true] {
            let db = setup().await;
            let key = if newer_user_guard {
                "terminal-preserves-user"
            } else {
                "terminal-clears-owned"
            };
            let (revision, _) = configure(&db, vec![current_rule("current", all())]).await;
            let version = publish(&db, &format!("owner-{key}"), key, 7, vec![]).await;
            let target = target(&version);
            let claim = claim(
                &db,
                revision,
                "current",
                LifecycleActionKind::ExpireCurrent,
                target.clone(),
            )
            .await;
            let now = database_now(&db).await.unwrap();
            assert!(matches!(
                admit_lifecycle_expiration(&db, &target, &claim.action.id, claim.claim_epoch, now,)
                    .await
                    .unwrap(),
                super::LifecycleAdmissionResult::Admitted(_)
            ));
            let user_guard = if newer_user_guard {
                Some(
                    admit_content_mutation(
                        &db,
                        &target.bucket,
                        &target.key,
                        None,
                        crate::import::SupersedeReason::PutObject,
                        now,
                    )
                    .await
                    .unwrap(),
                )
            } else {
                None
            };

            fail_safe(
                &db,
                &claim,
                crate::store::lifecycle_action::FAILURE_INTERNAL_DEPENDENCY,
                "internal_dependency",
            )
            .await
            .unwrap();

            assert_eq!(state(&db, &claim).await.as_deref(), Some("failed_safe"));
            let destination =
                import_destination::Entity::find_by_id((target.bucket.clone(), target.key.clone()))
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(
                destination.mutation_id.as_deref(),
                user_guard.as_ref().map(|guard| guard.mutation_id.as_str())
            );
            if let Some(user_guard) = user_guard {
                db.transaction(move |txn| {
                    Box::pin(async move {
                        lock_bucket_for_ownership(txn, &user_guard.bucket).await?;
                        complete_standard_mutation_in_transaction(txn, &user_guard, now).await
                    })
                })
                .await
                .unwrap();
            }
            assert_standard_guard_settled(&db, key).await;
        }
    }

    #[tokio::test]
    async fn lifecycle_action_execution_stale_epoch_cannot_terminally_complete_after_reclaim() {
        let db = setup().await;
        let (revision, _) = configure(&db, vec![current_rule("current", all())]).await;
        let version = publish(&db, "epoch-owner", "epoch", 7, vec![]).await;
        let first = claim(
            &db,
            revision,
            "current",
            LifecycleActionKind::ExpireCurrent,
            target(&version),
        )
        .await;
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                sea_orm::sea_query::Expr::value(Some(
                    database_now(&db).await.unwrap() - Duration::seconds(1),
                )),
            )
            .filter(lifecycle_action::Column::Id.eq(first.action.id.clone()))
            .exec(&db)
            .await
            .unwrap();
        let second = claim_due(&db, "new-epoch-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();

        execute(&db, &first).await;
        assert_eq!(state(&db, &first).await.as_deref(), Some("claimed"));
        assert_eq!(second.claim_epoch, first.claim_epoch + 1);
        execute(&db, &second).await;
        assert_eq!(state(&db, &second).await.as_deref(), Some("succeeded"));
    }

    #[tokio::test]
    async fn lifecycle_action_execution_terminal_store_failure_rolls_back_mutation_and_success() {
        let db = setup().await;
        let (revision, _) = configure(&db, vec![current_rule("current", all())]).await;
        let version = publish(&db, "terminal-failure-owner", "terminal", 7, vec![]).await;
        let action_target = target(&version);
        let claim = claim(
            &db,
            revision,
            "current",
            LifecycleActionKind::ExpireCurrent,
            action_target.clone(),
        )
        .await;
        let _scope =
            crate::store::lifecycle_action::test_hooks::fail_next_succeeded(&claim.action.id);

        execute(&db, &claim).await;

        assert!(
            object_version::Entity::find_by_id(version.id)
                .one(&db)
                .await
                .unwrap()
                .is_some(),
            "terminal persistence failure rolls the version mutation back"
        );
        assert_eq!(state(&db, &claim).await.as_deref(), Some("failed_safe"));
        assert_standard_guard_settled(&db, "terminal").await;
        assert_subsequent_standard_mutation_can_be_admitted(&db, &action_target).await;
    }

    #[tokio::test]
    async fn lifecycle_action_execution_temporary_post_admission_failure_settles_guard_before_retry()
     {
        let db = setup().await;
        let (revision, _) = configure(&db, vec![current_rule("current", all())]).await;
        let version = publish(&db, "temporary-failure-owner", "temporary", 7, vec![]).await;
        let action_target = target(&version);
        let claim = claim(
            &db,
            revision,
            "current",
            LifecycleActionKind::ExpireCurrent,
            action_target.clone(),
        )
        .await;
        let _scope = super::test_hooks::fail_next_temporary_post_admission(&claim.action.id);

        execute(&db, &claim).await;

        assert_eq!(state(&db, &claim).await.as_deref(), Some("pending"));
        assert_standard_guard_settled(&db, "temporary").await;
        assert_subsequent_standard_mutation_can_be_admitted(&db, &action_target).await;
    }

    #[test]
    fn lifecycle_action_execution_diagnostics_are_allowlisted_and_redacted() {
        let capture = LogCapture(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(capture.clone())
            .finish();
        let claim = ClaimedLifecycleAction {
            worker_id: "worker".to_owned(),
            claim_epoch: 2,
            action: lifecycle_action::Model {
                id: "action-id".to_owned(),
                idempotency_key: "idempotency".to_owned(),
                bucket: "bucket".to_owned(),
                object_key: "key".to_owned(),
                config_revision: 7,
                rule_id: "id:rule".to_owned(),
                action_kind: "expire_current".to_owned(),
                target_version_row_id: "internal-object-uuid".to_owned(),
                target_public_version_id: "public-version-id".to_owned(),
                target_object_id: Some("internal-object-uuid".to_owned()),
                target_sequence: 1,
                due_at: at(2000, 1, 1),
                state: "claimed".to_owned(),
                attempts: 1,
                next_attempt_at: at(2000, 1, 1),
                claim_epoch: 2,
                lease_until: Some(at(2030, 1, 1)),
                claimed_by: Some("worker".to_owned()),
                failure_class: None,
                last_error_redacted: None,
                created_at: at(2000, 1, 1),
                updated_at: at(2000, 1, 1),
                finished_at: None,
            },
        };

        tracing::subscriber::with_default(subscriber, || {
            log_action_diagnostic(&claim, "database_contention");
        });

        let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        for required in [
            "action-id",
            "bucket",
            "key",
            "public-version-id",
            "revision=7",
            "failure_class=\"database_contention\"",
        ] {
            assert!(
                log.contains(required),
                "missing diagnostic field {required}: {log}"
            );
        }
        for forbidden in [
            "SELECT *",
            "internal-object-uuid",
            "raw-provider-response",
            "credential=secret",
            "sse-c-key",
            "wrapped-key",
            "request-body",
        ] {
            assert!(
                !log.contains(forbidden),
                "leaked diagnostic value {forbidden}: {log}"
            );
        }
    }
}
