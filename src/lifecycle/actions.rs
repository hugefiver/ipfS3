use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    EntityTrait, PaginatorTrait, QueryFilter, QuerySelect, TransactionError, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    lifecycle::{
        config::from_canonical_json,
        evaluator::{
            LifecycleEvaluationContext, evaluate_candidate, next_utc_midnight_after_full_days,
        },
        model::{
            CanonicalFilter, CanonicalRuleSelector, ClaimedLifecycleAction,
            GuardedLifecycleExecutionResult, LifecycleActionKind, LifecycleCandidate,
            LifecycleRuleStatus, LifecycleTargetIdentity, MultipartUploadTargetIdentity,
            RuleIdentity, VersionLifecycleCandidate, VersionTargetIdentity,
        },
    },
    store::{
        database_clock::database_now,
        entities::{
            bucket_lifecycle_config, lifecycle_action, multipart_upload, object, object_version,
        },
        import::ownership::{
            StandardMutationGuard, clear_lifecycle_mutation_if_owned,
            complete_standard_mutation_in_transaction, lock_bucket_for_ownership,
            try_admit_lifecycle_mutation, verify_standard_mutation_guard,
        },
        lifecycle_action::{
            FAILURE_ADMISSION_TEMPORARILY_UNAVAILABLE, FAILURE_CANCELLED_STALE,
            FAILURE_DATABASE_CONTENTION, FAILURE_INTERNAL_DEPENDENCY,
            MAX_LIFECYCLE_ACTION_ATTEMPTS, action_kind_from_db, lock_claim_for_execution,
            mark_cancelled, mark_failed_safe, mark_succeeded, retry_at, schedule_retry,
            target_from_action,
        },
        multipart::{
            AbortExactIncompleteUploadResult, abort_exact_incomplete_upload_in_transaction,
        },
        object_version::{
            BucketVersioningState, ExactCurrentMarkerDeleteResult, VersionKind,
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
    let target = match target {
        LifecycleTargetIdentity::Version(target) => target,
        LifecycleTargetIdentity::MultipartUpload(target) => {
            let transaction_claim = claim.clone();
            let result = db
                .transaction(move |txn| {
                    Box::pin(async move {
                        execute_multipart_in_transaction(txn, &transaction_claim, &target).await
                    })
                })
                .await;
            return match result {
                Ok(()) => Ok(()),
                Err(error) => {
                    if is_temporary_execution_error(&transaction_error_into_app(error)) {
                        retry_or_fail_safe(
                            db,
                            claim,
                            max_attempts,
                            base_backoff_secs,
                            max_backoff_secs,
                            FAILURE_DATABASE_CONTENTION,
                        )
                        .await
                    } else {
                        fail_safe(
                            db,
                            claim,
                            FAILURE_INTERNAL_DEPENDENCY,
                            "internal_dependency",
                        )
                        .await
                    }
                }
            };
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

async fn execute_multipart_in_transaction<C: ConnectionTrait>(
    txn: &C,
    claim: &ClaimedLifecycleAction,
    target: &MultipartUploadTargetIdentity,
) -> AppResult<()> {
    let Some(locked_action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(());
    };
    let now = database_now(txn).await?;
    if !same_action_definition(&locked_action, &claim.action) {
        return cancel_without_guard_in_transaction(txn, claim, now, FAILURE_CANCELLED_STALE).await;
    }
    lock_bucket_for_ownership(txn, &target.bucket).await?;
    let Some(configuration_row) = lock_lifecycle_configuration(txn, &target.bucket).await? else {
        return cancel_without_guard_in_transaction(txn, claim, now, FAILURE_CANCELLED_STALE).await;
    };
    if configuration_row.revision != locked_action.config_revision {
        return cancel_without_guard_in_transaction(txn, claim, now, FAILURE_CANCELLED_STALE).await;
    }
    let Some(json) = configuration_row.canonical_json else {
        return cancel_without_guard_in_transaction(txn, claim, now, FAILURE_CANCELLED_STALE).await;
    };
    let configuration = from_canonical_json(&json)?;
    // Revalidate the named rule, not the evaluator's current winner: another
    // eligible rule does not revoke this revision-scoped action's entitlement.
    let rule = if let Some(id) = locked_action.rule_id.strip_prefix("id:") {
        configuration
            .rules
            .iter()
            .find(|rule| rule.id.as_deref() == Some(id))
    } else if let Some(ordinal) = locked_action.rule_id.strip_prefix("ordinal:") {
        ordinal
            .parse::<u16>()
            .ok()
            .filter(|parsed| parsed.to_string() == ordinal)
            .and_then(|ordinal| configuration.rules.get(usize::from(ordinal)))
            .filter(|rule| rule.id.is_none())
    } else {
        None
    };
    let eligible_abort = rule
        .filter(|rule| rule.status == LifecycleRuleStatus::Enabled)
        .filter(|rule| match &rule.selector {
            CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::All,
            } => true,
            CanonicalRuleSelector::LegacyPrefix { prefix }
            | CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::Prefix { prefix },
            } => target.key.starts_with(prefix),
            _ => false,
        })
        .and_then(|rule| rule.abort_incomplete_multipart_upload.as_ref());
    let Some(abort) = eligible_abort else {
        return cancel_without_guard_in_transaction(txn, claim, now, FAILURE_CANCELLED_STALE).await;
    };
    let query = multipart_upload::Entity::find_by_id(target.upload_id.clone());
    let upload = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(txn).await?
    } else {
        query.one(txn).await?
    };
    if upload.is_some_and(|upload| {
        upload.bucket != target.bucket
            || upload.key != target.key
            || upload.created_at != target.initiated_at
    }) {
        return cancel_without_guard_in_transaction(txn, claim, now, FAILURE_CANCELLED_STALE).await;
    }
    let Ok(due_at) =
        next_utc_midnight_after_full_days(target.initiated_at, abort.days_after_initiation)
    else {
        return cancel_without_guard_in_transaction(txn, claim, now, FAILURE_CANCELLED_STALE).await;
    };
    if locked_action.due_at != due_at || now < due_at {
        return cancel_without_guard_in_transaction(txn, claim, now, FAILURE_CANCELLED_STALE).await;
    }
    // Absence is successful only after policy and due-time revalidation; the
    // action must retain its audit outcome even when completion removed the upload.
    match abort_exact_incomplete_upload_in_transaction(txn, target).await? {
        AbortExactIncompleteUploadResult::Applied
        | AbortExactIncompleteUploadResult::AlreadySatisfied => {
            if !mark_succeeded(txn, claim, now).await? {
                return Err(AppError::StaleContentMutation);
            }
            Ok(())
        }
        AbortExactIncompleteUploadResult::Stale => {
            cancel_without_guard_in_transaction(txn, claim, now, FAILURE_CANCELLED_STALE).await
        }
    }
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
        (LifecycleActionKind::AbortIncompleteMultipartUpload, _) => {
            return Err(AppError::Internal(
                "multipart lifecycle action reached version revalidation".to_owned(),
            ));
        }
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
    Ok(Some(LifecycleCandidate::Version(
        VersionLifecycleCandidate {
            target: target.clone(),
            is_latest: selected.is_latest,
            size,
            lifecycle_age_started_at: selected.lifecycle_age_started_at,
            became_noncurrent_at: selected.became_noncurrent_at,
        },
    )))
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
                if locked_action.target_type == "version" {
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
                }
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
            let Some(locked_action) = lock_claim_for_execution(txn, &claim).await? else {
                return Ok(());
            };
            let now = database_now(txn).await?;
            if locked_action.target_type == "version" {
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
            }
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
    if claim.action.target_type == "version" {
        tracing::warn!(
            action_id = %claim.action.id,
            bucket = %claim.action.bucket,
            key = %claim.action.object_key,
            public_version_id = %claim.action.target_public_version_id.as_deref().unwrap_or("<invalid>"),
            revision = claim.action.config_revision,
            failure_class,
            "lifecycle action terminal outcome"
        );
    } else {
        tracing::warn!(
            action_id = %claim.action.id,
            bucket = %claim.action.bucket,
            key = %claim.action.object_key,
            revision = claim.action.config_revision,
            failure_class,
            "lifecycle action terminal outcome"
        );
    }
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
        && left.target_type == right.target_type
        && left.target_version_row_id == right.target_version_row_id
        && left.target_public_version_id == right.target_public_version_id
        && left.target_object_id == right.target_object_id
        && left.target_sequence == right.target_sequence
        && left.target_upload_id == right.target_upload_id
        && left.target_upload_created_at == right.target_upload_created_at
        && left.due_at == right.due_at
}

fn action_matches_expected(
    action: &lifecycle_action::Model,
    expected: &crate::lifecycle::model::NewLifecycleAction,
) -> bool {
    let target_matches = match &expected.target {
        LifecycleTargetIdentity::Version(target) => {
            action.object_key == target.key
                && action.target_type == "version"
                && action.target_version_row_id.as_deref() == Some(target.version_row_id.as_str())
                && action.target_public_version_id.as_deref()
                    == Some(target.public_version_id.as_s3_str())
                && action.target_object_id == target.object_id
                && action.target_sequence == Some(target.sequence)
                && action.target_upload_id.is_none()
                && action.target_upload_created_at.is_none()
        }
        LifecycleTargetIdentity::MultipartUpload(target) => {
            action.object_key == target.key
                && action.target_type == "multipart_upload"
                && action.target_version_row_id.is_none()
                && action.target_public_version_id.is_none()
                && action.target_object_id.is_none()
                && action.target_sequence.is_none()
                && action.target_upload_id.as_deref() == Some(target.upload_id.as_str())
                && action.target_upload_created_at == Some(target.initiated_at)
        }
    };
    target_matches
        && action.bucket == expected.bucket
        && action.config_revision == expected.config_revision
        && action.rule_id == persisted_rule_identity(&expected.rule_identity)
        && action.action_kind == persisted_action_kind(expected.action_kind)
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
        LifecycleActionKind::AbortIncompleteMultipartUpload => "abort_incomplete_multipart_upload",
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
        LifecycleActionKind::AbortIncompleteMultipartUpload => {
            return Err(AppError::Internal(
                "multipart lifecycle action reached version deletion".to_owned(),
            ));
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
                LifecycleActionKind, LifecycleRuleStatus, LifecycleTargetIdentity,
                NewLifecycleAction, NoncurrentExpiration, RuleIdentity, VersionTargetIdentity,
            },
        },
        pinning::{policy::PublicationPolicy, tags::ObjectTag},
        store::{
            bucket,
            database_clock::database_now,
            entities::{
                bucket_lifecycle_config, import_destination, lifecycle_action, multipart_upload,
                object, object_tag, object_version,
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

    use crate::lifecycle::model::{
        AbortIncompleteMultipartUploadAction, MultipartUploadTargetIdentity,
    };
    use crate::store::{lifecycle_action as action_store, multipart};
    use sea_orm::sea_query::Expr;

    fn abort_rule() -> CanonicalLifecycleRule {
        CanonicalLifecycleRule {
            id: Some("abort".to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector: all(),
            expiration: None,
            noncurrent_version_expiration: None,
            abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUploadAction {
                days_after_initiation: 1,
            }),
        }
    }

    async fn multipart_fixture() -> (DatabaseConnection, ClaimedLifecycleAction) {
        let db = setup().await;
        let (revision, _) = configure(&db, vec![abort_rule()]).await;
        multipart::create_upload(
            &db,
            "upload",
            "mpu-object",
            "bucket",
            "logs/key",
            "none",
            None,
            None,
            None,
            None,
            &[],
            None,
            false,
        )
        .await
        .unwrap();
        let initiated_at = database_now(&db).await.unwrap() - Duration::days(3);
        multipart_upload::Entity::update_many()
            .col_expr(
                multipart_upload::Column::CreatedAt,
                Expr::value(initiated_at),
            )
            .exec(&db)
            .await
            .unwrap();
        multipart::upsert_part(&db, "upload", 1, "part-cid", 5, "part-cid")
            .await
            .unwrap();
        let action = NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: "bucket".to_owned(),
            config_revision: revision,
            rule_identity: RuleIdentity::Id("abort".to_owned()),
            action_kind: LifecycleActionKind::AbortIncompleteMultipartUpload,
            target: LifecycleTargetIdentity::MultipartUpload(MultipartUploadTargetIdentity {
                bucket: "bucket".to_owned(),
                key: "logs/key".to_owned(),
                upload_id: "upload".to_owned(),
                initiated_at,
            }),
            due_at: crate::lifecycle::evaluator::next_utc_midnight_after_full_days(initiated_at, 1)
                .unwrap(),
        };
        insert_idempotent(&db, action, database_now(&db).await.unwrap())
            .await
            .unwrap();
        let claim = claim_due(&db, WORKER, Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        (db, claim)
    }

    async fn stored_action(
        db: &DatabaseConnection,
        claim: &ClaimedLifecycleAction,
    ) -> lifecycle_action::Model {
        lifecycle_action::Entity::find_by_id(claim.action.id.clone())
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    async fn assert_upload_present(db: &DatabaseConnection, present: bool) {
        assert_eq!(
            multipart_upload::Entity::find_by_id("upload")
                .one(db)
                .await
                .unwrap()
                .is_some(),
            present
        );
        assert_eq!(
            multipart::list_parts(db, "upload").await.unwrap().len(),
            usize::from(present)
        );
    }

    async fn change_action(
        db: &DatabaseConnection,
        claim: &ClaimedLifecycleAction,
        column: lifecycle_action::Column,
        value: impl Into<sea_orm::Value>,
    ) {
        lifecycle_action::Entity::update_many()
            .col_expr(column, Expr::value(value))
            .filter(lifecycle_action::Column::Id.eq(&claim.action.id))
            .exec(db)
            .await
            .unwrap();
    }

    async fn reclaim_multipart(
        db: &DatabaseConnection,
        claim: &ClaimedLifecycleAction,
    ) -> ClaimedLifecycleAction {
        let past = database_now(db).await.unwrap() - Duration::seconds(1);
        if stored_action(db, claim).await.state == "claimed" {
            change_action(db, claim, lifecycle_action::Column::LeaseUntil, Some(past)).await;
        }
        change_action(db, claim, lifecycle_action::Column::NextAttemptAt, past).await;
        claim_due(db, "replacement", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap()
    }

    async fn seed_foreign_token(
        db: &DatabaseConnection,
        claim: &ClaimedLifecycleAction,
    ) -> import_destination::Model {
        // Even a token with the apparent lifecycle owner is foreign to MPU execution.
        import_destination::Entity::insert(import_destination::ActiveModel {
            bucket: sea_orm::Set("bucket".to_owned()),
            key: sea_orm::Set("logs/key".to_owned()),
            generation: sea_orm::Set(42),
            owner_job_id: sea_orm::Set(None),
            mutation_id: sea_orm::Set(Some(format!(
                "lifecycle:{}:{}",
                claim.action.id, claim.claim_epoch
            ))),
            mutation_prefix: sea_orm::Set(None),
            updated_at: sea_orm::Set(database_now(db).await.unwrap()),
        })
        .exec(db)
        .await
        .unwrap();
        foreign_token(db).await
    }

    async fn foreign_token(db: &DatabaseConnection) -> import_destination::Model {
        import_destination::Entity::find_by_id(("bucket".to_owned(), "logs/key".to_owned()))
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    struct QueryRecorder<'a, C> {
        db: &'a C,
        queries: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl<C: ConnectionTrait> ConnectionTrait for QueryRecorder<'_, C> {
        fn get_database_backend(&self) -> sea_orm::DatabaseBackend {
            self.db.get_database_backend()
        }
        async fn execute(
            &self,
            statement: sea_orm::Statement,
        ) -> Result<sea_orm::ExecResult, sea_orm::DbErr> {
            self.queries.lock().unwrap().push(statement.sql.clone());
            self.db.execute(statement).await
        }
        async fn execute_unprepared(
            &self,
            sql: &str,
        ) -> Result<sea_orm::ExecResult, sea_orm::DbErr> {
            self.queries.lock().unwrap().push(sql.to_owned());
            self.db.execute_unprepared(sql).await
        }
        async fn query_one(
            &self,
            statement: sea_orm::Statement,
        ) -> Result<Option<sea_orm::QueryResult>, sea_orm::DbErr> {
            self.queries.lock().unwrap().push(statement.sql.clone());
            self.db.query_one(statement).await
        }
        async fn query_all(
            &self,
            statement: sea_orm::Statement,
        ) -> Result<Vec<sea_orm::QueryResult>, sea_orm::DbErr> {
            self.queries.lock().unwrap().push(statement.sql.clone());
            self.db.query_all(statement).await
        }
    }

    #[tokio::test]
    async fn multipart_execution_locks_claim_before_policy_and_never_takes_standard_token() {
        let (db, claim) = multipart_fixture().await;
        let txn = db.begin().await.unwrap();
        let recorder = QueryRecorder {
            db: &txn,
            queries: Mutex::new(Vec::new()),
        };
        let mut stale = claim.clone();
        stale.claim_epoch += 1;
        let LifecycleTargetIdentity::MultipartUpload(target) =
            action_store::target_from_action(&claim.action).unwrap()
        else {
            unreachable!()
        };
        super::execute_multipart_in_transaction(&recorder, &stale, &target)
            .await
            .unwrap();
        let queries = recorder.queries.into_inner().unwrap();
        assert!(
            queries[0].contains("lifecycle_actions"),
            "claim must be the first read: {queries:?}"
        );
        assert_eq!(
            queries.len(),
            1,
            "rejected epoch must not read database time or target state"
        );
        txn.commit().await.unwrap();
        let user_guard = admit_content_mutation(
            &db,
            "bucket",
            "logs/key",
            None,
            crate::import::SupersedeReason::PutObject,
            database_now(&db).await.unwrap(),
        )
        .await
        .unwrap();
        let token = foreign_token(&db).await;
        assert_eq!(
            token.mutation_id.as_deref(),
            Some(user_guard.mutation_id.as_str())
        );
        execute(&db, &stale).await;
        assert_eq!(stored_action(&db, &claim).await, claim.action);
        assert_upload_present(&db, true).await;
        execute(&db, &claim).await;
        assert_eq!(state(&db, &claim).await.as_deref(), Some("succeeded"));
        assert_upload_present(&db, false).await;
        assert_eq!(foreign_token(&db).await, token);
    }

    #[tokio::test]
    async fn multipart_execution_applies_and_claim_fences_terminal_success() {
        let (db, claim) = multipart_fixture().await;
        db.execute_unprepared("CREATE TRIGGER invalidate_abort_epoch AFTER DELETE ON multipart_uploads BEGIN UPDATE lifecycle_actions SET claim_epoch = claim_epoch + 1 WHERE target_upload_id = OLD.upload_id; END;").await.unwrap();
        execute(&db, &claim).await;
        assert_upload_present(&db, true).await;
        let pending = stored_action(&db, &claim).await;
        assert_eq!(
            pending.state, "pending",
            "losing the terminal fence must roll back the delete"
        );
        assert_eq!(
            pending.claim_epoch, claim.claim_epoch,
            "the trigger's mutation must also roll back"
        );
        db.execute_unprepared("DROP TRIGGER invalidate_abort_epoch")
            .await
            .unwrap();
        let claim = reclaim_multipart(&db, &claim).await;
        let txn = db.begin().await.unwrap();
        let recorder = QueryRecorder {
            db: &txn,
            queries: Mutex::new(Vec::new()),
        };
        let LifecycleTargetIdentity::MultipartUpload(target) =
            action_store::target_from_action(&claim.action).unwrap()
        else {
            unreachable!()
        };
        super::execute_multipart_in_transaction(&recorder, &claim, &target)
            .await
            .unwrap();
        let queries = recorder.queries.into_inner().unwrap();
        assert!(queries[0].contains("lifecycle_actions"));
        assert!(queries[1].contains("strftime"));
        let bucket_lock = queries
            .iter()
            .position(|sql| sql.contains("buckets"))
            .unwrap();
        let policy = queries
            .iter()
            .position(|sql| sql.contains("bucket_lifecycle_configs"))
            .unwrap();
        let mutation = queries
            .iter()
            .position(|sql| sql.starts_with("DELETE") && sql.contains("multipart_uploads"))
            .unwrap();
        let target_read = queries
            .iter()
            .position(|sql| sql.starts_with("SELECT") && sql.contains("multipart_uploads"))
            .expect("exact identity must be revalidated before mutation");
        assert!(policy < target_read && target_read < mutation);
        let terminal_write = queries
            .iter()
            .position(|sql| sql.starts_with("UPDATE") && sql.contains("lifecycle_actions"))
            .unwrap();
        assert!(
            bucket_lock > 1
                && bucket_lock < policy
                && policy < mutation
                && mutation < terminal_write,
            "{queries:?}"
        );
        assert!(
            queries
                .iter()
                .all(|sql| !sql.contains("import_destinations")),
            "MPU must never inspect standard admission state: {queries:?}"
        );
        txn.commit().await.unwrap();
        let terminal = stored_action(&db, &claim).await;
        assert_eq!(terminal.state, "succeeded");
        assert!(
            terminal.finished_at.is_some()
                && terminal.lease_until.is_none()
                && terminal.claimed_by.is_none()
        );
        assert_upload_present(&db, false).await;
        assert!(
            !action_store::mark_cancelled(
                &db,
                &claim,
                database_now(&db).await.unwrap(),
                action_store::FAILURE_CANCELLED_STALE
            )
            .await
            .unwrap()
        );
        execute(&db, &claim).await;
        assert_eq!(stored_action(&db, &claim).await, terminal);
    }

    #[tokio::test]
    async fn multipart_execution_missing_target_is_success_but_stale_identity_is_cancelled() {
        for case in ["missing", "bucket", "key", "initiation"] {
            let (db, claim) = multipart_fixture().await;
            match case {
                "missing" => multipart::delete_upload(&db, "upload").await.unwrap(),
                "bucket" => {
                    bucket::create(&db, "other", None).await.unwrap();
                    multipart_upload::Entity::update_many()
                        .col_expr(multipart_upload::Column::Bucket, Expr::value("other"))
                        .exec(&db)
                        .await
                        .unwrap();
                }
                "key" => {
                    multipart_upload::Entity::update_many()
                        .col_expr(multipart_upload::Column::Key, Expr::value("other"))
                        .exec(&db)
                        .await
                        .unwrap();
                }
                "initiation" => {
                    multipart_upload::Entity::update_many()
                        .col_expr(
                            multipart_upload::Column::CreatedAt,
                            Expr::value(database_now(&db).await.unwrap()),
                        )
                        .exec(&db)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            execute(&db, &claim).await;
            let row = stored_action(&db, &claim).await;
            assert_eq!(
                row.state,
                if case == "missing" {
                    "succeeded"
                } else {
                    "cancelled"
                },
                "{case}"
            );
            if case != "missing" {
                assert_eq!(
                    row.failure_class.as_deref(),
                    Some(action_store::FAILURE_CANCELLED_STALE)
                );
            }
            assert_upload_present(&db, case != "missing").await;
        }
    }

    #[tokio::test]
    async fn multipart_execution_revalidates_revision_rule_status_selector_due_and_target() {
        for case in [
            "replace",
            "delete",
            "disabled",
            "missing_rule",
            "no_abort",
            "prefix",
            "days",
            "due",
            "not_due",
            "action_identity",
            "ordinal",
            "bad_ordinal",
            "legacy",
            "modern",
            "named_not_winner",
        ] {
            let (db, mut claim) = multipart_fixture().await;
            let mut rule = abort_rule();
            match case {
                "replace" => {
                    configure(&db, vec![rule.clone()]).await;
                }
                "delete" => {
                    delete_configuration(&db, "bucket").await.unwrap();
                }
                "disabled" => rule.status = LifecycleRuleStatus::Disabled,
                "missing_rule" => rule.id = Some("different".to_owned()),
                "no_abort" => {
                    rule.abort_incomplete_multipart_upload = None;
                    rule.expiration = Some(CurrentExpiration::Days { days: 1 });
                }
                "prefix" => {
                    rule.selector = CanonicalRuleSelector::LegacyPrefix {
                        prefix: "other/".to_owned(),
                    }
                }
                "days" => {
                    rule.abort_incomplete_multipart_upload
                        .as_mut()
                        .unwrap()
                        .days_after_initiation = 2
                }
                "due" => {
                    claim.action.due_at -= Duration::seconds(1);
                    change_action(
                        &db,
                        &claim,
                        lifecycle_action::Column::DueAt,
                        claim.action.due_at,
                    )
                    .await;
                }
                "not_due" => {
                    let initiated = database_now(&db).await.unwrap();
                    claim.action.target_upload_created_at = Some(initiated);
                    claim.action.due_at =
                        crate::lifecycle::evaluator::next_utc_midnight_after_full_days(
                            initiated, 1,
                        )
                        .unwrap();
                    change_action(
                        &db,
                        &claim,
                        lifecycle_action::Column::TargetUploadCreatedAt,
                        Some(initiated),
                    )
                    .await;
                    change_action(
                        &db,
                        &claim,
                        lifecycle_action::Column::DueAt,
                        claim.action.due_at,
                    )
                    .await;
                    multipart_upload::Entity::update_many()
                        .col_expr(multipart_upload::Column::CreatedAt, Expr::value(initiated))
                        .exec(&db)
                        .await
                        .unwrap();
                }
                "action_identity" => {
                    change_action(&db, &claim, lifecycle_action::Column::ObjectKey, "changed").await
                }
                "ordinal" | "bad_ordinal" => {
                    rule.id = None;
                    claim.action.rule_id = if case == "ordinal" {
                        "ordinal:0"
                    } else {
                        "ordinal:00"
                    }
                    .to_owned();
                    change_action(
                        &db,
                        &claim,
                        lifecycle_action::Column::RuleId,
                        claim.action.rule_id.clone(),
                    )
                    .await;
                }
                "legacy" => {
                    rule.selector = CanonicalRuleSelector::LegacyPrefix {
                        prefix: "logs/".to_owned(),
                    }
                }
                "modern" => {
                    rule.selector = CanonicalRuleSelector::Modern {
                        filter: CanonicalFilter::Prefix {
                            prefix: "logs/".to_owned(),
                        },
                    }
                }
                "named_not_winner" => {}
                _ => unreachable!(),
            }
            if !matches!(case, "replace" | "delete") {
                let mut rules = vec![rule];
                if case == "named_not_winner" {
                    let mut other = abort_rule();
                    other.id = Some("a-earlier".to_owned());
                    rules.insert(0, other);
                }
                replace_configuration_without_revising(&db, &configuration(rules)).await;
            }
            execute(&db, &claim).await;
            let succeeds = matches!(case, "ordinal" | "legacy" | "modern" | "named_not_winner");
            let row = stored_action(&db, &claim).await;
            assert_eq!(
                row.state,
                if succeeds { "succeeded" } else { "cancelled" },
                "{case}"
            );
            if !succeeds {
                assert_eq!(
                    row.failure_class.as_deref(),
                    Some(action_store::FAILURE_CANCELLED_STALE),
                    "{case}"
                );
            }
            assert_upload_present(&db, !succeeds).await;
        }
    }

    #[tokio::test]
    async fn multipart_execution_cancels_when_canonical_due_time_is_not_representable() {
        let (db, claim) = multipart_fixture().await;
        let mut rule = abort_rule();
        rule.abort_incomplete_multipart_upload
            .as_mut()
            .unwrap()
            .days_after_initiation = i32::MAX as u32;
        replace_configuration_without_revising(&db, &configuration(vec![rule])).await;

        execute(&db, &claim).await;

        let terminal = stored_action(&db, &claim).await;
        assert_eq!(terminal.state, "cancelled");
        assert_eq!(
            terminal.failure_class.as_deref(),
            Some(action_store::FAILURE_CANCELLED_STALE)
        );
        assert!(terminal.finished_at.is_some());
        assert!(terminal.lease_until.is_none() && terminal.claimed_by.is_none());
        assert_upload_present(&db, true).await;
    }

    #[tokio::test]
    async fn multipart_terminal_failure_never_clears_foreign_standard_token() {
        for case in [
            "malformed",
            "malformed_type",
            "invalid_policy",
            "retry_exhausted",
        ] {
            let (db, mut claim) = multipart_fixture().await;
            let token = seed_foreign_token(&db, &claim).await;
            match case {
                "malformed" => claim.action.target_upload_id = None,
                "malformed_type" => claim.action.target_type = "version".to_owned(),
                "invalid_policy" => {
                    bucket_lifecycle_config::Entity::update_many()
                        .col_expr(
                            bucket_lifecycle_config::Column::CanonicalJson,
                            Expr::value(Some("{\"schema_version\":1,\"rules\":[]}")),
                        )
                        .exec(&db)
                        .await
                        .unwrap();
                }
                "retry_exhausted" => {
                    change_action(
                        &db,
                        &claim,
                        lifecycle_action::Column::Attempts,
                        MAX_LIFECYCLE_ACTION_ATTEMPTS,
                    )
                    .await;
                    retry_or_fail_safe(
                        &db,
                        &claim,
                        MAX_LIFECYCLE_ACTION_ATTEMPTS,
                        1,
                        60,
                        action_store::FAILURE_DATABASE_CONTENTION,
                    )
                    .await
                    .unwrap();
                }
                _ => unreachable!(),
            }
            if case != "retry_exhausted" {
                execute(&db, &claim).await;
            }
            let row = stored_action(&db, &claim).await;
            assert_eq!(row.state, "failed_safe", "{case}");
            assert_eq!(
                row.last_error_redacted.as_deref(),
                Some(action_store::REDACTED_LIFECYCLE_ACTION_ERROR)
            );
            assert_upload_present(&db, true).await;
            assert_eq!(foreign_token(&db).await, token, "{case}");
        }
    }

    #[tokio::test]
    async fn multipart_temporary_failure_retries_then_succeeds() {
        let (db, claim) = multipart_fixture().await;
        db.execute_unprepared("CREATE TRIGGER temporary_abort BEFORE DELETE ON multipart_uploads BEGIN SELECT RAISE(FAIL, 'database is locked'); END;").await.unwrap();
        execute(&db, &claim).await;
        let pending = stored_action(&db, &claim).await;
        assert_eq!(pending.state, "pending");
        assert_eq!(
            pending.failure_class.as_deref(),
            Some(action_store::FAILURE_DATABASE_CONTENTION)
        );
        assert_eq!(
            pending.next_attempt_at - pending.updated_at,
            Duration::seconds(1)
        );
        assert_upload_present(&db, true).await;
        db.execute_unprepared("DROP TRIGGER temporary_abort")
            .await
            .unwrap();
        let next = reclaim_multipart(&db, &claim).await;
        execute(&db, &next).await;
        assert_eq!(state(&db, &next).await.as_deref(), Some("succeeded"));
        assert_upload_present(&db, false).await;
    }

    #[tokio::test]
    async fn multipart_terminal_write_failure_rolls_back_abort() {
        let (db, claim) = multipart_fixture().await;
        let _scope = action_store::test_hooks::fail_next_succeeded(&claim.action.id).temporarily();
        execute(&db, &claim).await;
        assert_upload_present(&db, true).await;
        assert_eq!(state(&db, &claim).await.as_deref(), Some("pending"));
        let next = reclaim_multipart(&db, &claim).await;
        assert_eq!(next.action.attempts, 2);
        execute(&db, &next).await;
        assert_eq!(state(&db, &next).await.as_deref(), Some("succeeded"));
        assert_upload_present(&db, false).await;
    }

    #[tokio::test]
    async fn multipart_stale_epoch_cannot_abort_or_terminalize() {
        let (db, first) = multipart_fixture().await;
        let second = reclaim_multipart(&db, &first).await;
        execute(&db, &first).await;
        assert_eq!(stored_action(&db, &first).await, second.action);
        assert_upload_present(&db, true).await;
        let now = database_now(&db).await.unwrap();
        assert!(
            !action_store::mark_succeeded(&db, &first, now)
                .await
                .unwrap()
        );
        assert!(
            !action_store::mark_cancelled(&db, &first, now, action_store::FAILURE_CANCELLED_STALE)
                .await
                .unwrap()
        );
        assert!(
            !action_store::mark_failed_safe(
                &db,
                &first,
                now,
                action_store::FAILURE_INTERNAL_DEPENDENCY
            )
            .await
            .unwrap()
        );
        execute(&db, &second).await;
        assert_eq!(state(&db, &second).await.as_deref(), Some("succeeded"));
        assert_upload_present(&db, false).await;
    }

    #[tokio::test]
    async fn multipart_final_recovery_claim_is_bounded() {
        for crash_again in [false, true] {
            let (db, first) = multipart_fixture().await;
            change_action(
                &db,
                &first,
                lifecycle_action::Column::Attempts,
                MAX_LIFECYCLE_ACTION_ATTEMPTS,
            )
            .await;
            let recovery = reclaim_multipart(&db, &first).await;
            assert_eq!(recovery.action.attempts, MAX_LIFECYCLE_ACTION_ATTEMPTS + 1);
            assert_eq!(recovery.claim_epoch, first.claim_epoch + 1);
            let token = seed_foreign_token(&db, &recovery).await;
            if crash_again {
                change_action(
                    &db,
                    &recovery,
                    lifecycle_action::Column::LeaseUntil,
                    Some(database_now(&db).await.unwrap() - Duration::seconds(1)),
                )
                .await;
                for _ in 0..2 {
                    assert!(
                        claim_due(&db, "last", Duration::seconds(30), 1)
                            .await
                            .unwrap()
                            .is_empty()
                    );
                }
                let terminal = stored_action(&db, &recovery).await;
                assert_eq!(terminal.state, "failed_safe");
                assert_eq!(terminal.claim_epoch, recovery.claim_epoch);
            } else {
                execute(&db, &recovery).await;
                assert_eq!(state(&db, &recovery).await.as_deref(), Some("succeeded"));
            }
            assert_upload_present(&db, crash_again).await;
            assert_eq!(foreign_token(&db).await, token);
        }
    }

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
            abort_incomplete_multipart_upload: None,
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
            abort_incomplete_multipart_upload: None,
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
            target: LifecycleTargetIdentity::Version(target),
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
    async fn multipart_missing_configuration_cancels_and_never_touches_content_token() {
        let db = setup().await;
        crate::store::multipart::create_upload(
            &db,
            "fail-closed-upload",
            "fail-closed-object",
            "bucket",
            "fail-closed-key",
            "none",
            None,
            None,
            None,
            None,
            &[],
            None,
            false,
        )
        .await
        .unwrap();
        let upload = multipart_upload::Entity::find_by_id("fail-closed-upload")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let now = database_now(&db).await.unwrap();
        let action_id = uuid::Uuid::new_v4().to_string();
        lifecycle_action::Entity::insert(lifecycle_action::ActiveModel {
            id: sea_orm::Set(action_id.clone()),
            idempotency_key: sea_orm::Set("fail-closed-multipart-key".to_owned()),
            bucket: sea_orm::Set("bucket".to_owned()),
            object_key: sea_orm::Set("fail-closed-key".to_owned()),
            config_revision: sea_orm::Set(1),
            rule_id: sea_orm::Set("id:abort".to_owned()),
            action_kind: sea_orm::Set("abort_incomplete_multipart_upload".to_owned()),
            target_type: sea_orm::Set("multipart_upload".to_owned()),
            target_version_row_id: sea_orm::Set(None),
            target_public_version_id: sea_orm::Set(None),
            target_object_id: sea_orm::Set(None),
            target_sequence: sea_orm::Set(None),
            target_upload_id: sea_orm::Set(Some(upload.upload_id.clone())),
            target_upload_created_at: sea_orm::Set(Some(upload.created_at)),
            due_at: sea_orm::Set(now - Duration::seconds(1)),
            state: sea_orm::Set("pending".to_owned()),
            attempts: sea_orm::Set(0),
            next_attempt_at: sea_orm::Set(now - Duration::seconds(1)),
            claim_epoch: sea_orm::Set(0),
            lease_until: sea_orm::Set(None),
            claimed_by: sea_orm::Set(None),
            failure_class: sea_orm::Set(None),
            last_error_redacted: sea_orm::Set(None),
            created_at: sea_orm::Set(now),
            updated_at: sea_orm::Set(now),
            finished_at: sea_orm::Set(None),
        })
        .exec(&db)
        .await
        .unwrap();
        let claim = claim_due(&db, WORKER, Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let untouched_token = format!("lifecycle:{}:{}", action_id, claim.claim_epoch);
        import_destination::Entity::insert(import_destination::ActiveModel {
            bucket: sea_orm::Set("bucket".to_owned()),
            key: sea_orm::Set("fail-closed-key".to_owned()),
            generation: sea_orm::Set(1),
            owner_job_id: sea_orm::Set(None),
            mutation_id: sea_orm::Set(Some(untouched_token.clone())),
            mutation_prefix: sea_orm::Set(None),
            updated_at: sea_orm::Set(now),
        })
        .exec(&db)
        .await
        .unwrap();

        execute(&db, &claim).await;

        assert_eq!(state(&db, &claim).await.as_deref(), Some("cancelled"));
        let terminal = lifecycle_action::Entity::find_by_id(claim.action.id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.failure_class.as_deref(), Some("cancelled_stale"));
        assert_eq!(
            terminal.last_error_redacted.as_deref(),
            Some("lifecycle action failed")
        );
        assert!(
            multipart_upload::Entity::find_by_id(upload.upload_id)
                .one(&db)
                .await
                .unwrap()
                .is_some(),
            "an MPU action without an active policy must not mutate its upload"
        );
        let destination = import_destination::Entity::find_by_id((
            "bucket".to_owned(),
            "fail-closed-key".to_owned(),
        ))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            destination.mutation_id.as_deref(),
            Some(untouched_token.as_str()),
            "an MPU action never owns or clears a standard content-mutation token"
        );
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
                target_type: "version".to_owned(),
                target_version_row_id: Some("internal-object-uuid".to_owned()),
                target_public_version_id: Some("public-version-id".to_owned()),
                target_object_id: Some("internal-object-uuid".to_owned()),
                target_sequence: Some(1),
                target_upload_id: None,
                target_upload_created_at: None,
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
