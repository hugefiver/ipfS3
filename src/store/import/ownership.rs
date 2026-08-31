use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use sea_orm::sea_query::{Condition, Expr};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, Statement, TransactionError, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    import::{ImportClaim, ImportFailure, SupersedeReason},
    store::{
        entities::{
            bucket, import_destination, import_job, import_job_target, import_prefix_claim,
        },
        import::{
            jobs::{NewImportJob, SubmitImportOutcome, find_idempotent, insert_queued},
            lease_clock,
        },
    },
};

const STATE_QUEUED: &str = "queued";
const STATE_RUNNING: &str = "running";
const STATE_COMPLETED: &str = "completed";
const STATE_FAILED: &str = "failed";
const STATE_SUPERSEDED: &str = "superseded";
const KIND_ARCHIVE: &str = "archive";
const KIND_EXTRACTED: &str = "extracted";
const MAX_OWNERSHIP_TRANSACTION_RETRIES: usize = 3;
const POSTGRES_BUCKET_OWNERSHIP_LOCK_SQL: &str =
    "SELECT name FROM buckets WHERE name = $1 FOR NO KEY UPDATE";
/// Bounds ownership discovery and release while preserving the global ascending job-ID lock order.
const OWNERSHIP_BATCH_SIZE: u64 = 128;

enum IdempotencyPreflightOutcome {
    Match(Box<import_job::Model>),
    Conflict,
    Missing,
}

#[derive(Clone, Debug)]
pub struct ExpectedImportTarget {
    pub bucket: String,
    pub key: String,
    pub generation: i64,
}

#[derive(Clone, Debug)]
pub struct ImportPublicationGuard {
    pub job_id: String,
    pub worker_id: String,
    pub claim_epoch: i64,
    pub targets: Vec<ExpectedImportTarget>,
}

/// Durable fence returned by a standard content-mutation admission.
///
/// The token lives on the archive/exact destination row until the operation's
/// final publication or delete transaction validates and clears it. A newer
/// overlapping admission clears the token, making the older operation stale.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandardMutationGuard {
    pub bucket: String,
    pub key: String,
    pub mutation_id: String,
    pub expected_generation: i64,
    pub mutation_prefix: Option<String>,
}

/// Acquires the per-bucket ownership serialization fence.
///
/// Call this as the first database operation in every ownership-changing
/// transaction. PostgreSQL locks the bucket row; SQLite's no-op update obtains
/// write intent before any ownership reads can establish an old snapshot.
pub async fn lock_bucket_for_ownership<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
) -> AppResult<()> {
    if txn.get_database_backend() == DatabaseBackend::Postgres {
        let row = txn
            .query_one(postgres_bucket_ownership_lock_statement(bucket_name))
            .await?;
        if row.is_some() {
            return Ok(());
        }
    } else {
        let updated = bucket::Entity::update_many()
            .col_expr(
                bucket::Column::CreatedAt,
                Expr::col(bucket::Column::CreatedAt).into(),
            )
            .filter(bucket::Column::Name.eq(bucket_name))
            .exec(txn)
            .await?;
        if updated.rows_affected == 1 {
            return Ok(());
        }
    }
    Err(AppError::NoSuchBucket(bucket_name.to_owned()))
}

fn postgres_bucket_ownership_lock_statement(bucket_name: &str) -> Statement {
    Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        POSTGRES_BUCKET_OWNERSHIP_LOCK_SQL,
        [bucket_name.to_owned().into()],
    )
}

/// Resolves a tokenized submission while holding the bucket ownership fence.
///
/// The short transaction ends before callers perform any network authorization.
pub async fn preflight_idempotent_submission(
    db: &DatabaseConnection,
    bucket_name: &str,
    key: &str,
    token: &str,
    request_fingerprint: &str,
) -> AppResult<Option<import_job::Model>> {
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let bucket_name = bucket_name.to_owned();
        let key = key.to_owned();
        let token = token.to_owned();
        let request_fingerprint = request_fingerprint.to_owned();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    let existing = find_idempotent(txn, &bucket_name, &key, &token).await?;
                    match existing {
                        Some(existing) if existing.request_fingerprint == request_fingerprint => {
                            Ok(IdempotencyPreflightOutcome::Match(Box::new(existing)))
                        }
                        Some(_) => Ok(IdempotencyPreflightOutcome::Conflict),
                        None => Ok(IdempotencyPreflightOutcome::Missing),
                    }
                })
            })
            .await;
        match outcome {
            Ok(IdempotencyPreflightOutcome::Match(existing)) => return Ok(Some(*existing)),
            Ok(IdempotencyPreflightOutcome::Conflict) => {
                return Err(AppError::ImportIdempotencyConflict);
            }
            Ok(IdempotencyPreflightOutcome::Missing) => return Ok(None),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("ownership idempotency preflight retry loop always returns or errors")
}

/// Atomically creates an owned import job, or replays an identical tokenized
/// request without changing existing ownership.
pub async fn submit(
    db: &DatabaseConnection,
    request: NewImportJob,
    now: DateTime<Utc>,
) -> AppResult<SubmitImportOutcome> {
    let bucket_name = request.bucket.clone();
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let request = request.clone();
        let bucket_name = bucket_name.clone();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    submit_in_transaction(txn, request, now).await
                })
            })
            .await;
        match outcome {
            Ok(outcome) => return Ok(outcome),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("ownership submit retry loop always returns or errors")
}

async fn submit_in_transaction<C: ConnectionTrait>(
    txn: &C,
    request: NewImportJob,
    now: DateTime<Utc>,
) -> AppResult<SubmitImportOutcome> {
    if let Some(token) = request.client_token.as_deref()
        && let Some(existing) = find_idempotent(txn, &request.bucket, &request.key, token).await?
    {
        return if existing.request_fingerprint == request.request_fingerprint {
            Ok(SubmitImportOutcome::Replayed(existing))
        } else {
            Err(AppError::ImportIdempotencyConflict)
        };
    }

    let job = insert_queued(txn, request, now).await?;
    let expected_generation =
        claim_primary_destination(txn, &job.id, &job.bucket, &job.key, now).await?;
    insert_target(
        txn,
        &job.id,
        &job.bucket,
        &job.key,
        expected_generation,
        KIND_ARCHIVE,
    )
    .await?;
    if let Some(prefix) = job.decompress_prefix.as_deref() {
        install_prefix_claim(txn, &job.id, &job.bucket, prefix, now).await?;
    }
    Ok(SubmitImportOutcome::Created(job))
}

/// Claims the primary exact target after the caller has acquired its bucket
/// ownership lock. The target generation is monotonically increasing.
pub async fn claim_primary_destination<C: ConnectionTrait>(
    txn: &C,
    job_id: &str,
    bucket_name: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<i64> {
    let previous = find_destination_for_update(txn, bucket_name, key).await?;
    let exact_targets = vec![(
        key.to_owned(),
        previous
            .as_ref()
            .and_then(|destination| destination.owner_job_id.clone()),
    )];
    supersede_conflicts_in_order(
        txn,
        bucket_name,
        &exact_targets,
        None,
        Some(job_id),
        SupersedeReason::NewImport,
        now,
    )
    .await?;
    invalidate_standard_mutations_in_order(
        txn,
        bucket_name,
        &BTreeSet::from([key.to_owned()]),
        None,
        now,
    )
    .await?;
    upsert_destination_owner(txn, previous, bucket_name, key, Some(job_id), now).await
}

/// Installs a literal prefix claim after the caller has acquired the bucket
/// ownership lock. It supersedes owners of any overlapping literal prefix and
/// any exact destination below this prefix.
pub async fn install_prefix_claim<C: ConnectionTrait>(
    txn: &C,
    job_id: &str,
    bucket_name: &str,
    prefix: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let claim_order = next_prefix_claim_order(txn, bucket_name).await?;
    supersede_conflicts_in_order(
        txn,
        bucket_name,
        &[],
        Some(prefix),
        Some(job_id),
        SupersedeReason::NewImport,
        now,
    )
    .await?;
    invalidate_standard_mutations_in_order(txn, bucket_name, &BTreeSet::new(), Some(prefix), now)
        .await?;

    let existing = import_prefix_claim::Entity::find_by_id((
        job_id.to_owned(),
        bucket_name.to_owned(),
        prefix.to_owned(),
    ))
    .one(txn)
    .await?;
    if existing.is_none() {
        import_prefix_claim::Entity::insert(import_prefix_claim::ActiveModel {
            job_id: Set(job_id.to_owned()),
            bucket: Set(bucket_name.to_owned()),
            prefix: Set(prefix.to_owned()),
            claim_order: Set(claim_order),
        })
        .exec(txn)
        .await?;
    }
    Ok(())
}

/// Claims a decompressed output only while the exact worker lease is still
/// active. A reclaimed or expired attempt cannot create a new target.
pub async fn claim_extracted_target(
    db: &DatabaseConnection,
    claim: &ImportClaim,
    bucket_name: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<i64> {
    let claim = claim.clone();
    let bucket_name = bucket_name.to_owned();
    let key = key.to_owned();
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let claim = claim.clone();
        let bucket_name = bucket_name.clone();
        let key = key.clone();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    verify_extraction_claim(txn, &claim, &bucket_name, &key).await?;
                    let existing = import_job_target::Entity::find_by_id((
                        claim.job_id.clone(),
                        bucket_name.clone(),
                        key.clone(),
                    ));
                    let existing = if txn.get_database_backend() == DatabaseBackend::Postgres {
                        existing.lock_exclusive().one(txn).await?
                    } else {
                        existing.one(txn).await?
                    };
                    if let Some(existing) = existing {
                        let destination =
                            find_destination_for_update(txn, &bucket_name, &key).await?;
                        if existing.kind == KIND_EXTRACTED
                            && matches!(destination, Some(ref destination)
                                if destination.generation == existing.expected_generation
                                    && destination.owner_job_id.as_deref()
                                        == Some(claim.job_id.as_str()))
                        {
                            return Ok(existing.expected_generation);
                        }
                        return Err(AppError::StaleImportOwnership);
                    }
                    let generation =
                        claim_primary_destination(txn, &claim.job_id, &bucket_name, &key, now)
                            .await?;
                    insert_target(
                        txn,
                        &claim.job_id,
                        &bucket_name,
                        &key,
                        generation,
                        KIND_EXTRACTED,
                    )
                    .await?;
                    Ok(generation)
                })
            })
            .await;
        match outcome {
            Ok(generation) => return Ok(generation),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("extracted target retry loop always returns or errors")
}

/// Releases a failed extracted output while retaining its bumped generation.
/// The exact worker lease, prefix claim, target generation, and destination
/// ownership are all rechecked under the bucket lock.
pub async fn release_extracted_target(
    db: &DatabaseConnection,
    claim: &ImportClaim,
    bucket_name: &str,
    key: &str,
    generation: i64,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let claim = claim.clone();
    let bucket_name = bucket_name.to_owned();
    let key = key.to_owned();
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let claim = claim.clone();
        let bucket_name = bucket_name.clone();
        let key = key.clone();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    verify_extraction_claim(txn, &claim, &bucket_name, &key).await?;
                    let target = import_job_target::Entity::find_by_id((
                        claim.job_id.clone(),
                        bucket_name.clone(),
                        key.clone(),
                    ));
                    let target = if txn.get_database_backend() == DatabaseBackend::Postgres {
                        target.lock_exclusive().one(txn).await?
                    } else {
                        target.one(txn).await?
                    };
                    if !matches!(target, Some(ref target) if target.expected_generation == generation && target.kind == KIND_EXTRACTED)
                    {
                        return Err(AppError::StaleImportOwnership);
                    }
                    let destination = find_destination_for_update(txn, &bucket_name, &key).await?;
                    if !matches!(destination, Some(ref destination) if destination.generation == generation && destination.owner_job_id.as_deref() == Some(claim.job_id.as_str()))
                    {
                        return Err(AppError::StaleImportOwnership);
                    }
                    import_job_target::Entity::delete_by_id((
                        claim.job_id.clone(),
                        bucket_name.clone(),
                        key.clone(),
                    ))
                    .exec(txn)
                    .await?;
                    let updated = import_destination::Entity::update_many()
                        .col_expr(
                            import_destination::Column::OwnerJobId,
                            Expr::value(Option::<String>::None),
                        )
                        .col_expr(import_destination::Column::UpdatedAt, Expr::value(now))
                        .filter(import_destination::Column::Bucket.eq(&bucket_name))
                        .filter(import_destination::Column::Key.eq(&key))
                        .filter(import_destination::Column::Generation.eq(generation))
                        .filter(import_destination::Column::OwnerJobId.eq(&claim.job_id))
                        .exec(txn)
                        .await?;
                    if updated.rows_affected != 1 {
                        return Err(AppError::StaleImportOwnership);
                    }
                    Ok(())
                })
            })
            .await;
        match outcome {
            Ok(()) => return Ok(()),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("extracted target release retry loop always returns or errors")
}

/// Clears attempt-local decompressed outputs before a newly claimed ZIP import
/// performs any source or Kubo work. Archive ownership and the literal prefix
/// claim remain intact.
pub async fn reset_extracted_targets_for_attempt(
    db: &DatabaseConnection,
    claim: &ImportClaim,
    bucket_name: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    loop {
        let released = reset_extracted_target_batch(db, claim, bucket_name, now).await?;
        if released == 0 {
            return Ok(());
        }
        #[cfg(test)]
        pause_after_reset_batch_for_test(&claim.job_id).await;
        // The completed batch owns no transaction or database lock here. Give
        // the worker's lease-renewal future a chance to run before reacquiring
        // the bucket for the next batch.
        tokio::task::yield_now().await;
    }
}

async fn reset_extracted_target_batch(
    db: &DatabaseConnection,
    claim: &ImportClaim,
    bucket_name: &str,
    now: DateTime<Utc>,
) -> AppResult<usize> {
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let claim = claim.clone();
        let bucket_name = bucket_name.to_owned();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    verify_decompression_attempt(txn, &claim, &bucket_name).await?;
                    let query = import_job_target::Entity::find()
                        .filter(import_job_target::Column::JobId.eq(&claim.job_id))
                        .filter(import_job_target::Column::Bucket.eq(&bucket_name))
                        .filter(import_job_target::Column::Kind.eq(KIND_EXTRACTED))
                        .order_by_asc(import_job_target::Column::Key)
                        .limit(OWNERSHIP_BATCH_SIZE);
                    let targets = if txn.get_database_backend() == DatabaseBackend::Postgres {
                        query.lock_exclusive().all(txn).await?
                    } else {
                        query.all(txn).await?
                    };
                    let released_count = targets.len();
                    for target in targets {
                        let destination =
                            find_destination_for_update(txn, &bucket_name, &target.key).await?;
                        if !matches!(destination, Some(ref destination)
                            if destination.generation == target.expected_generation
                                && destination.owner_job_id.as_deref()
                                    == Some(claim.job_id.as_str()))
                        {
                            return Err(AppError::StaleImportOwnership);
                        }
                        let released = import_destination::Entity::update_many()
                            .col_expr(
                                import_destination::Column::OwnerJobId,
                                Expr::value(Option::<String>::None),
                            )
                            .col_expr(import_destination::Column::UpdatedAt, Expr::value(now))
                            .filter(import_destination::Column::Bucket.eq(&bucket_name))
                            .filter(import_destination::Column::Key.eq(&target.key))
                            .filter(
                                import_destination::Column::Generation
                                    .eq(target.expected_generation),
                            )
                            .filter(import_destination::Column::OwnerJobId.eq(&claim.job_id))
                            .exec(txn)
                            .await?;
                        if released.rows_affected != 1 {
                            return Err(AppError::StaleImportOwnership);
                        }
                        let deleted = import_job_target::Entity::delete_many()
                            .filter(import_job_target::Column::JobId.eq(&claim.job_id))
                            .filter(import_job_target::Column::Bucket.eq(&bucket_name))
                            .filter(import_job_target::Column::Key.eq(&target.key))
                            .filter(import_job_target::Column::Kind.eq(KIND_EXTRACTED))
                            .filter(
                                import_job_target::Column::ExpectedGeneration
                                    .eq(target.expected_generation),
                            )
                            .exec(txn)
                            .await?;
                        if deleted.rows_affected != 1 {
                            return Err(AppError::StaleImportOwnership);
                        }
                    }
                    // Fence immediately before this one batch commits. A later
                    // batch independently re-locks and verifies the claim.
                    verify_decompression_attempt(txn, &claim, &bucket_name).await?;
                    Ok(released_count)
                })
            })
            .await;
        match outcome {
            Ok(released) => return Ok(released),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("extracted target batch retry loop always returns or errors")
}

#[cfg(test)]
mod reset_test_gate {
    use std::sync::{Arc, LazyLock};

    use tokio::sync::{Mutex, Notify};

    pub static AFTER_BATCH: LazyLock<Mutex<Option<Arc<BatchGate>>>> =
        LazyLock::new(|| Mutex::new(None));

    pub struct BatchGate {
        pub job_id: String,
        pub arrived: Notify,
        pub resume: Notify,
    }
}

#[cfg(test)]
async fn pause_after_reset_batch_for_test(job_id: &str) {
    let gate = {
        let mut configured = reset_test_gate::AFTER_BATCH.lock().await;
        if configured
            .as_ref()
            .is_some_and(|gate| gate.job_id == job_id)
        {
            configured.take()
        } else {
            None
        }
    };
    if let Some(gate) = gate {
        gate.arrived.notify_one();
        gate.resume.notified().await;
    }
}

/// Admits one standard content mutation in a bucket-first transaction.
pub async fn admit_content_mutation<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    excluding_job: Option<&str>,
    reason: SupersedeReason,
    now: DateTime<Utc>,
) -> AppResult<StandardMutationGuard> {
    let mut guards = admit_content_mutations(
        db,
        bucket_name,
        &[key.to_owned()],
        excluding_job,
        reason,
        now,
    )
    .await?;
    guards.pop().ok_or_else(|| {
        AppError::Internal("content admission returned no mutation guard".to_owned())
    })
}

/// Tries to admit a lifecycle mutation without superseding an in-flight import
/// or standard content mutation. The bucket lock makes the conflict check and
/// token installation atomic across gateway processes.
pub async fn try_admit_lifecycle_mutation<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    action_id: &str,
    claim_epoch: i64,
    now: DateTime<Utc>,
) -> AppResult<Option<StandardMutationGuard>> {
    if action_id.is_empty() || action_id.contains(':') || claim_epoch <= 0 {
        return Err(AppError::Internal(
            "invalid lifecycle mutation owner".to_owned(),
        ));
    }
    let bucket_name = bucket_name.to_owned();
    let key = key.to_owned();
    let action_id = action_id.to_owned();
    let mutation_id = format!("lifecycle:{action_id}:{claim_epoch}");
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let bucket_name = bucket_name.clone();
        let key = key.clone();
        let action_id = action_id.clone();
        let mutation_id = mutation_id.clone();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    let destination = find_destination_for_update(txn, &bucket_name, &key).await?;
                    if destination
                        .as_ref()
                        .is_some_and(|destination| destination.owner_job_id.is_some())
                        || !prefix_owner_jobs_for_key(txn, &bucket_name, &key, None, None)
                            .await?
                            .is_empty()
                        || has_overlapping_standard_prefix_mutation(txn, &bucket_name, &key).await?
                    {
                        return Ok(None);
                    }
                    if let Some(destination) = destination.as_ref()
                        && let Some(active_mutation_id) = destination.mutation_id.as_deref()
                    {
                        if active_mutation_id == mutation_id {
                            if destination.mutation_prefix.is_some() {
                                return Ok(None);
                            }
                            return Ok(Some(StandardMutationGuard {
                                bucket: bucket_name,
                                key,
                                mutation_id,
                                expected_generation: destination.generation,
                                mutation_prefix: None,
                            }));
                        }
                        let older_same_action =
                            lifecycle_mutation_epoch(active_mutation_id, &action_id)
                                .is_some_and(|epoch| epoch > 0 && epoch < claim_epoch);
                        if !older_same_action || destination.mutation_prefix.is_some() {
                            return Ok(None);
                        }
                    }
                    upsert_standard_mutation(
                        txn,
                        destination,
                        &bucket_name,
                        &key,
                        &mutation_id,
                        None,
                        now,
                    )
                    .await
                    .map(Some)
                })
            })
            .await;
        match outcome {
            Ok(guard) => return Ok(guard),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("lifecycle admission retry loop always returns or errors")
}

/// Clears only an admission token owned by this lifecycle action at this or an
/// older claim epoch. The caller must hold the bucket ownership fence.
pub async fn clear_lifecycle_mutation_if_owned<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    key: &str,
    action_id: &str,
    claim_epoch: i64,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    if action_id.is_empty() || action_id.contains(':') || claim_epoch <= 0 {
        return Err(AppError::Internal(
            "invalid lifecycle mutation owner".to_owned(),
        ));
    }
    let Some(destination) = find_destination_for_update(txn, bucket_name, key).await? else {
        return Ok(false);
    };
    let Some(active_mutation_id) = destination.mutation_id.as_deref() else {
        return Ok(false);
    };
    let owned = destination.mutation_prefix.is_none()
        && lifecycle_mutation_epoch(active_mutation_id, action_id)
            .is_some_and(|epoch| epoch > 0 && epoch <= claim_epoch);
    if !owned {
        return Ok(false);
    }
    let cleared = import_destination::Entity::update_many()
        .col_expr(
            import_destination::Column::MutationId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            import_destination::Column::MutationPrefix,
            Expr::value(Option::<String>::None),
        )
        .col_expr(import_destination::Column::UpdatedAt, Expr::value(now))
        .filter(import_destination::Column::Bucket.eq(bucket_name))
        .filter(import_destination::Column::Key.eq(key))
        .filter(import_destination::Column::MutationId.eq(active_mutation_id))
        .exec(txn)
        .await?;
    if cleared.rows_affected != 1 {
        return Err(AppError::Database(
            "stale lifecycle mutation cleanup compare-and-set".to_owned(),
        ));
    }
    Ok(true)
}

fn lifecycle_mutation_epoch(token: &str, action_id: &str) -> Option<i64> {
    token
        .strip_prefix("lifecycle:")?
        .strip_prefix(action_id)?
        .strip_prefix(':')?
        .parse::<i64>()
        .ok()
}

/// Admits all unique content keys with one bucket lock. This is the Task 5
/// DeleteObjects entry point and deliberately delegates to the no-transaction
/// single-key helper instead of nesting transactions.
pub async fn admit_content_mutations<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    bucket_name: &str,
    keys: &[String],
    excluding_job: Option<&str>,
    reason: SupersedeReason,
    now: DateTime<Utc>,
) -> AppResult<Vec<StandardMutationGuard>> {
    let bucket_name = bucket_name.to_owned();
    let keys = keys.iter().cloned().collect::<BTreeSet<_>>();
    let mutation_ids = keys
        .iter()
        .map(|key| (key.clone(), uuid::Uuid::new_v4().to_string()))
        .collect::<BTreeMap<_, _>>();
    let excluding_job = excluding_job.map(str::to_owned);
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let bucket_name = bucket_name.clone();
        let keys = keys.clone();
        let mutation_ids = mutation_ids.clone();
        let excluding_job = excluding_job.clone();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    admit_content_mutations_in_transaction(
                        txn,
                        &bucket_name,
                        &keys,
                        &mutation_ids,
                        excluding_job.as_deref(),
                        reason,
                        now,
                    )
                    .await
                })
            })
            .await;
        match outcome {
            Ok(guards) => return Ok(guards),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("content admission retry loop always returns or errors")
}

/// Admits a standard prefix mutation in a bucket-first transaction.
pub async fn admit_prefix_mutation<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    bucket_name: &str,
    prefix: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let bucket_name = bucket_name.to_owned();
    let prefix = prefix.to_owned();
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let bucket_name = bucket_name.clone();
        let prefix = prefix.clone();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    admit_prefix_mutation_in_transaction(txn, &bucket_name, &prefix, now).await
                })
            })
            .await;
        match outcome {
            Ok(()) => return Ok(()),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("prefix admission retry loop always returns or errors")
}

/// Admits an archive key and a decompression output prefix atomically.
pub async fn admit_content_and_prefix_mutation<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    prefix: &str,
    reason: SupersedeReason,
    now: DateTime<Utc>,
) -> AppResult<StandardMutationGuard> {
    let bucket_name = bucket_name.to_owned();
    let key = key.to_owned();
    let prefix = prefix.to_owned();
    let mutation_id = uuid::Uuid::new_v4().to_string();
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let bucket_name = bucket_name.clone();
        let key = key.clone();
        let prefix = prefix.clone();
        let mutation_id = mutation_id.clone();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    admit_content_and_prefix_mutation_in_transaction(
                        txn,
                        &bucket_name,
                        &key,
                        &prefix,
                        &mutation_id,
                        reason,
                        now,
                    )
                    .await
                })
            })
            .await;
        match outcome {
            Ok(guard) => return Ok(guard),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("combined admission retry loop always returns or errors")
}

#[cfg(test)]
async fn admit_content_mutation_in_transaction<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    key: &str,
    excluding_job: Option<&str>,
    reason: SupersedeReason,
    now: DateTime<Utc>,
) -> AppResult<StandardMutationGuard> {
    let mutation_ids = BTreeMap::from([(key.to_owned(), uuid::Uuid::new_v4().to_string())]);
    admit_content_mutations_in_transaction(
        txn,
        bucket_name,
        &BTreeSet::from([key.to_owned()]),
        &mutation_ids,
        excluding_job,
        reason,
        now,
    )
    .await?
    .pop()
    .ok_or_else(|| AppError::Internal("content admission returned no mutation guard".to_owned()))
}

async fn admit_content_mutations_in_transaction<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    keys: &BTreeSet<String>,
    mutation_ids: &BTreeMap<String, String>,
    excluding_job: Option<&str>,
    reason: SupersedeReason,
    now: DateTime<Utc>,
) -> AppResult<Vec<StandardMutationGuard>> {
    let mut destinations = Vec::with_capacity(keys.len());
    let mut exact_targets = Vec::with_capacity(keys.len());
    for key in keys {
        let destination = find_destination_for_update(txn, bucket_name, key).await?;
        exact_targets.push((
            key.clone(),
            destination
                .as_ref()
                .and_then(|destination| destination.owner_job_id.clone()),
        ));
        destinations.push((key.clone(), destination));
    }
    supersede_conflicts_in_order(
        txn,
        bucket_name,
        &exact_targets,
        None,
        excluding_job,
        reason,
        now,
    )
    .await?;
    invalidate_standard_mutations_in_order(txn, bucket_name, keys, None, now).await?;
    let mut guards = Vec::with_capacity(destinations.len());
    for (key, _) in destinations {
        let destination = find_destination_for_update(txn, bucket_name, &key).await?;
        let mutation_id = mutation_ids.get(&key).ok_or_else(|| {
            AppError::Internal("content admission is missing a mutation token".to_owned())
        })?;
        guards.push(
            upsert_standard_mutation(txn, destination, bucket_name, &key, mutation_id, None, now)
                .await?,
        );
    }
    Ok(guards)
}

async fn admit_prefix_mutation_in_transaction<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    prefix: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    supersede_conflicts_in_order(
        txn,
        bucket_name,
        &[],
        Some(prefix),
        None,
        SupersedeReason::DecompressZip,
        now,
    )
    .await?;
    invalidate_standard_mutations_in_order(txn, bucket_name, &BTreeSet::new(), Some(prefix), now)
        .await
}

async fn admit_content_and_prefix_mutation_in_transaction<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    key: &str,
    prefix: &str,
    mutation_id: &str,
    reason: SupersedeReason,
    now: DateTime<Utc>,
) -> AppResult<StandardMutationGuard> {
    let destination = find_destination_for_update(txn, bucket_name, key).await?;
    let exact_targets = vec![(
        key.to_owned(),
        destination
            .as_ref()
            .and_then(|destination| destination.owner_job_id.clone()),
    )];
    supersede_conflicts_in_order(
        txn,
        bucket_name,
        &exact_targets,
        Some(prefix),
        None,
        reason,
        now,
    )
    .await?;
    invalidate_standard_mutations_in_order(
        txn,
        bucket_name,
        &BTreeSet::from([key.to_owned()]),
        Some(prefix),
        now,
    )
    .await?;
    let destination = find_destination_for_update(txn, bucket_name, key).await?;
    upsert_standard_mutation(
        txn,
        destination,
        bucket_name,
        key,
        mutation_id,
        Some(prefix),
        now,
    )
    .await
}

/// Supersedes all active jobs in an already bucket-locked transaction.
pub async fn supersede_bucket<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    now: DateTime<Utc>,
) -> AppResult<u64> {
    let mut after_job_id: Option<String> = None;
    let mut count = 0_u64;
    loop {
        let mut query = import_job::Entity::find()
            .filter(import_job::Column::Bucket.eq(bucket_name))
            .filter(import_job::Column::State.is_in([STATE_QUEUED, STATE_RUNNING]))
            .order_by_asc(import_job::Column::Id)
            .limit(OWNERSHIP_BATCH_SIZE);
        if let Some(after_job_id) = after_job_id.as_deref() {
            query = query.filter(import_job::Column::Id.gt(after_job_id));
        }
        let jobs = if txn.get_database_backend() == DatabaseBackend::Postgres {
            query.lock_exclusive().all(txn).await?
        } else {
            query.all(txn).await?
        };
        let Some(last_job_id) = jobs.last().map(|job| job.id.clone()) else {
            break;
        };
        count = count.saturating_add(jobs.len() as u64);
        for job in jobs {
            supersede_job_in_transaction(txn, &job.id, SupersedeReason::DeleteBucket, now).await?;
        }
        after_job_id = Some(last_job_id);
    }
    Ok(count)
}

/// Fences and terminally fails a worker claim, releasing every ownership row
/// in the same bucket-locked transaction.
pub async fn fail_claimed(
    db: &DatabaseConnection,
    claim: &ImportClaim,
    bucket_name: &str,
    failure: &ImportFailure,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let claim = claim.clone();
    let bucket_name = bucket_name.to_owned();
    let failure = failure.clone();
    for retry in 0..=MAX_OWNERSHIP_TRANSACTION_RETRIES {
        let claim = claim.clone();
        let bucket_name = bucket_name.clone();
        let failure = failure.clone();
        let outcome = db
            .transaction(move |txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, &bucket_name).await?;
                    verify_active_claim(txn, &claim, &bucket_name).await?;
                    let updated = import_job::Entity::update_many()
                        .col_expr(import_job::Column::State, Expr::value(STATE_FAILED))
                        .col_expr(
                            import_job::Column::LockedBy,
                            Expr::value(Option::<String>::None),
                        )
                        .col_expr(
                            import_job::Column::LockedUntil,
                            Expr::value(Option::<DateTime<Utc>>::None),
                        )
                        .col_expr(
                            import_job::Column::FailureCode,
                            Expr::value(Some(failure.code.as_str().to_owned())),
                        )
                        .col_expr(
                            import_job::Column::FailureMessage,
                            Expr::value(Some(failure.message.clone())),
                        )
                        .col_expr(import_job::Column::UpdatedAt, Expr::value(now))
                        .col_expr(import_job::Column::CompletedAt, Expr::value(Some(now)))
                        .filter(active_claim_filter(&claim, txn.get_database_backend()))
                        .exec(txn)
                        .await?;
                    if updated.rows_affected != 1 {
                        return Err(AppError::StaleImportOwnership);
                    }
                    release_job_ownership(txn, &claim.job_id, now).await
                })
            })
            .await;
        match outcome {
            Ok(()) => return Ok(()),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < MAX_OWNERSHIP_TRANSACTION_RETRIES =>
            {
                ownership_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("fail retry loop always returns or errors")
}

/// Marks a single active job superseded after the caller acquired its bucket
/// ownership lock. Reasons are intentionally not persisted because the schema
/// stores no redaction-safe reason column.
pub async fn supersede_job_in_transaction<C: ConnectionTrait>(
    txn: &C,
    job_id: &str,
    _reason: SupersedeReason,
    now: DateTime<Utc>,
) -> AppResult<()> {
    supersede_job_with_destination_invalidation(txn, job_id, None, now).await
}

async fn supersede_job_with_destination_invalidation<C: ConnectionTrait>(
    txn: &C,
    job_id: &str,
    invalidation_prefix: Option<(&str, &str)>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let Some(job) = find_job_for_update(txn, job_id).await? else {
        return Err(AppError::NoSuchImportJob);
    };
    if !matches!(job.state.as_str(), STATE_QUEUED | STATE_RUNNING) {
        return Ok(());
    }
    let updated = import_job::Entity::update_many()
        .col_expr(import_job::Column::State, Expr::value(STATE_SUPERSEDED))
        .col_expr(
            import_job::Column::LockedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            import_job::Column::LockedUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(import_job::Column::UpdatedAt, Expr::value(now))
        .col_expr(import_job::Column::CompletedAt, Expr::value(Some(now)))
        .filter(import_job::Column::Id.eq(job_id))
        .filter(import_job::Column::State.is_in([STATE_QUEUED, STATE_RUNNING]))
        .exec(txn)
        .await?;
    if updated.rows_affected != 1 {
        return Err(AppError::StaleImportOwnership);
    }
    release_job_ownership_with_invalidation(txn, job_id, invalidation_prefix, now).await
}

/// Verifies the ownership guard after the archive bucket is locked, before any
/// publication-side mutation. The guard target set must be both durable and
/// exactly equal to the objects this publication will write.
pub(crate) async fn verify_publication_guard<C: ConnectionTrait>(
    txn: &C,
    guard: &ImportPublicationGuard,
    archive_bucket: &str,
    publication_targets: &[(String, String)],
) -> AppResult<()> {
    let expected = target_map(&guard.targets, archive_bucket)?;
    let publication = publication_target_set(publication_targets, archive_bucket)?;
    if expected.keys().cloned().collect::<BTreeSet<_>>() != publication {
        return Err(AppError::StaleImportOwnership);
    }

    for ((bucket_name, key), generation) in &expected {
        let Some(destination) = find_destination_for_update(txn, bucket_name, key).await? else {
            return Err(AppError::StaleImportOwnership);
        };
        if destination.owner_job_id.as_deref() != Some(guard.job_id.as_str())
            || destination.generation != *generation
        {
            return Err(AppError::StaleImportOwnership);
        }
    }

    let query =
        import_job::Entity::find().filter(active_guard_filter(guard, txn.get_database_backend()));
    let job = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(txn).await?
    } else {
        query.one(txn).await?
    };
    let Some(job) = job else {
        return Err(AppError::StaleImportOwnership);
    };
    if job.bucket != archive_bucket {
        return Err(AppError::StaleImportOwnership);
    }
    let query = import_job_target::Entity::find()
        .filter(import_job_target::Column::JobId.eq(&guard.job_id))
        .order_by_asc(import_job_target::Column::Bucket)
        .order_by_asc(import_job_target::Column::Key);
    let durable_targets = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().all(txn).await?
    } else {
        query.all(txn).await?
    };
    let durable = durable_targets
        .into_iter()
        .map(|target| ((target.bucket, target.key), target.expected_generation))
        .collect::<BTreeMap<_, _>>();
    if durable != expected {
        return Err(AppError::StaleImportOwnership);
    }
    Ok(())
}

/// Verifies a standard mutation token after the bucket lock is held and before
/// any publication/delete side effect is written.
pub(crate) async fn verify_standard_mutation_guard<C: ConnectionTrait>(
    txn: &C,
    guard: &StandardMutationGuard,
    archive_bucket: &str,
    archive_key: &str,
    entry_keys: &[String],
) -> AppResult<()> {
    if guard.bucket != archive_bucket || guard.key != archive_key || guard.expected_generation < 1 {
        return Err(AppError::StaleContentMutation);
    }
    match guard.mutation_prefix.as_deref() {
        Some(prefix) if entry_keys.iter().all(|key| key.starts_with(prefix)) => {}
        Some(_) => return Err(AppError::StaleContentMutation),
        None if entry_keys.is_empty() => {}
        None => return Err(AppError::StaleContentMutation),
    }

    let destination = find_destination_for_update(txn, archive_bucket, archive_key)
        .await?
        .ok_or(AppError::StaleContentMutation)?;
    if destination.generation != guard.expected_generation
        || destination.owner_job_id.is_some()
        || destination.mutation_id.as_deref() != Some(guard.mutation_id.as_str())
        || destination.mutation_prefix != guard.mutation_prefix
    {
        return Err(AppError::StaleContentMutation);
    }
    Ok(())
}

/// Clears a verified standard mutation token as the last authorization write
/// in the same transaction as publication/delete.
pub(crate) async fn complete_standard_mutation_in_transaction<C: ConnectionTrait>(
    txn: &C,
    guard: &StandardMutationGuard,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let cleared = import_destination::Entity::update_many()
        .col_expr(
            import_destination::Column::MutationId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            import_destination::Column::MutationPrefix,
            Expr::value(Option::<String>::None),
        )
        .col_expr(import_destination::Column::UpdatedAt, Expr::value(now))
        .filter(import_destination::Column::Bucket.eq(&guard.bucket))
        .filter(import_destination::Column::Key.eq(&guard.key))
        .filter(import_destination::Column::Generation.eq(guard.expected_generation))
        .filter(import_destination::Column::OwnerJobId.is_null())
        .filter(import_destination::Column::MutationId.eq(&guard.mutation_id))
        .exec(txn)
        .await?;
    if cleared.rows_affected != 1 {
        return Err(AppError::StaleContentMutation);
    }
    Ok(())
}

/// Completes an already verified guard and releases all destination/prefix/
/// target claims. It deliberately repeats the lease fence in the state update.
pub(crate) async fn complete_publication_in_transaction<C: ConnectionTrait>(
    txn: &C,
    guard: &ImportPublicationGuard,
    final_cid: &str,
    logical_size: i64,
    now: DateTime<Utc>,
) -> AppResult<()> {
    if logical_size < 0 {
        return Err(AppError::StaleImportOwnership);
    }
    let updated = import_job::Entity::update_many()
        .col_expr(import_job::Column::State, Expr::value(STATE_COMPLETED))
        .col_expr(
            import_job::Column::FinalCid,
            Expr::value(Some(final_cid.to_owned())),
        )
        .col_expr(
            import_job::Column::LogicalSize,
            Expr::value(Some(logical_size)),
        )
        .col_expr(
            import_job::Column::LockedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            import_job::Column::LockedUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            import_job::Column::FailureCode,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            import_job::Column::FailureMessage,
            Expr::value(Option::<String>::None),
        )
        .col_expr(import_job::Column::UpdatedAt, Expr::value(now))
        .col_expr(import_job::Column::CompletedAt, Expr::value(Some(now)))
        .filter(active_guard_filter(guard, txn.get_database_backend()))
        .exec(txn)
        .await?;
    if updated.rows_affected != 1 {
        return Err(AppError::StaleImportOwnership);
    }
    release_job_ownership(txn, &guard.job_id, now).await
}

pub(crate) async fn release_job_ownership<C: ConnectionTrait>(
    txn: &C,
    job_id: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    release_job_ownership_with_invalidation(txn, job_id, None, now).await
}

async fn release_job_ownership_with_invalidation<C: ConnectionTrait>(
    txn: &C,
    job_id: &str,
    invalidation_prefix: Option<(&str, &str)>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut after_destination: Option<(String, String)> = None;
    loop {
        let mut query = import_destination::Entity::find()
            .filter(import_destination::Column::OwnerJobId.eq(job_id))
            .order_by_asc(import_destination::Column::Bucket)
            .order_by_asc(import_destination::Column::Key)
            .limit(OWNERSHIP_BATCH_SIZE);
        if let Some((after_bucket, after_key)) = after_destination.as_ref() {
            query = query.filter(
                Condition::any()
                    .add(import_destination::Column::Bucket.gt(after_bucket))
                    .add(
                        Condition::all()
                            .add(import_destination::Column::Bucket.eq(after_bucket))
                            .add(import_destination::Column::Key.gt(after_key)),
                    ),
            );
        }
        let destinations = if txn.get_database_backend() == DatabaseBackend::Postgres {
            query.lock_exclusive().all(txn).await?
        } else {
            query.all(txn).await?
        };
        let Some(last_destination) = destinations.last() else {
            break;
        };
        after_destination = Some((
            last_destination.bucket.clone(),
            last_destination.key.clone(),
        ));
        for destination in destinations {
            let mut update = import_destination::Entity::update_many()
                .col_expr(
                    import_destination::Column::OwnerJobId,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(import_destination::Column::UpdatedAt, Expr::value(now));
            if invalidation_prefix.is_some_and(|(bucket_name, prefix)| {
                destination.bucket == bucket_name && destination.key.starts_with(prefix)
            }) {
                let next_generation = destination.generation.checked_add(1).ok_or_else(|| {
                    AppError::Internal("import destination generation exhausted".to_owned())
                })?;
                update = update.col_expr(
                    import_destination::Column::Generation,
                    Expr::value(next_generation),
                );
            }
            update
                .filter(import_destination::Column::Bucket.eq(&destination.bucket))
                .filter(import_destination::Column::Key.eq(&destination.key))
                .filter(import_destination::Column::OwnerJobId.eq(job_id))
                .exec(txn)
                .await?;
        }
    }
    import_prefix_claim::Entity::delete_many()
        .filter(import_prefix_claim::Column::JobId.eq(job_id))
        .exec(txn)
        .await?;
    import_job_target::Entity::delete_many()
        .filter(import_job_target::Column::JobId.eq(job_id))
        .exec(txn)
        .await?;
    Ok(())
}

async fn verify_active_claim<C: ConnectionTrait>(
    txn: &C,
    claim: &ImportClaim,
    bucket_name: &str,
) -> AppResult<()> {
    let query =
        import_job::Entity::find().filter(active_claim_filter(claim, txn.get_database_backend()));
    let job = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(txn).await?
    } else {
        query.one(txn).await?
    };
    let Some(job) = job else {
        return Err(AppError::StaleImportOwnership);
    };
    if job.bucket != bucket_name {
        return Err(AppError::StaleImportOwnership);
    }
    Ok(())
}

async fn verify_extraction_claim<C: ConnectionTrait>(
    txn: &C,
    claim: &ImportClaim,
    bucket_name: &str,
    key: &str,
) -> AppResult<()> {
    verify_active_claim(txn, claim, bucket_name).await?;
    let job = find_job_for_update(txn, &claim.job_id)
        .await?
        .ok_or(AppError::StaleImportOwnership)?;
    let Some(prefix) = job.decompress_prefix else {
        return Ok(());
    };
    if !key.starts_with(&prefix) {
        return Err(AppError::StaleImportOwnership);
    }
    let prefix_claim = import_prefix_claim::Entity::find_by_id((
        claim.job_id.clone(),
        bucket_name.to_owned(),
        prefix,
    ));
    let prefix_claim = if txn.get_database_backend() == DatabaseBackend::Postgres {
        prefix_claim.lock_exclusive().one(txn).await?
    } else {
        prefix_claim.one(txn).await?
    };
    if prefix_claim.is_none() {
        return Err(AppError::StaleImportOwnership);
    }
    Ok(())
}

async fn verify_decompression_attempt<C: ConnectionTrait>(
    txn: &C,
    claim: &ImportClaim,
    bucket_name: &str,
) -> AppResult<()> {
    verify_active_claim(txn, claim, bucket_name).await?;
    let job = find_job_for_update(txn, &claim.job_id)
        .await?
        .ok_or(AppError::StaleImportOwnership)?;
    let prefix = job
        .decompress_prefix
        .ok_or(AppError::StaleImportOwnership)?;
    let prefix_claim = import_prefix_claim::Entity::find_by_id((
        claim.job_id.clone(),
        bucket_name.to_owned(),
        prefix,
    ));
    let prefix_claim = if txn.get_database_backend() == DatabaseBackend::Postgres {
        prefix_claim.lock_exclusive().one(txn).await?
    } else {
        prefix_claim.one(txn).await?
    };
    if prefix_claim.is_none() {
        return Err(AppError::StaleImportOwnership);
    }
    Ok(())
}

async fn insert_target<C: ConnectionTrait>(
    txn: &C,
    job_id: &str,
    bucket_name: &str,
    key: &str,
    expected_generation: i64,
    kind: &str,
) -> AppResult<()> {
    import_job_target::Entity::insert(import_job_target::ActiveModel {
        job_id: Set(job_id.to_owned()),
        bucket: Set(bucket_name.to_owned()),
        key: Set(key.to_owned()),
        expected_generation: Set(expected_generation),
        kind: Set(kind.to_owned()),
    })
    .exec(txn)
    .await?;
    Ok(())
}

async fn upsert_destination_owner<C: ConnectionTrait>(
    txn: &C,
    existing: Option<import_destination::Model>,
    bucket_name: &str,
    key: &str,
    owner_job_id: Option<&str>,
    now: DateTime<Utc>,
) -> AppResult<i64> {
    let generation = match existing {
        Some(destination) => {
            let next = destination.generation.checked_add(1).ok_or_else(|| {
                AppError::Internal("import destination generation exhausted".to_owned())
            })?;
            import_destination::Entity::update_many()
                .col_expr(import_destination::Column::Generation, Expr::value(next))
                .col_expr(
                    import_destination::Column::OwnerJobId,
                    Expr::value(owner_job_id.map(str::to_owned)),
                )
                .col_expr(
                    import_destination::Column::MutationId,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(
                    import_destination::Column::MutationPrefix,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(import_destination::Column::UpdatedAt, Expr::value(now))
                .filter(import_destination::Column::Bucket.eq(bucket_name))
                .filter(import_destination::Column::Key.eq(key))
                .exec(txn)
                .await?;
            next
        }
        None => {
            import_destination::Entity::insert(import_destination::ActiveModel {
                bucket: Set(bucket_name.to_owned()),
                key: Set(key.to_owned()),
                generation: Set(1),
                owner_job_id: Set(owner_job_id.map(str::to_owned)),
                mutation_id: Set(None),
                mutation_prefix: Set(None),
                updated_at: Set(now),
            })
            .exec(txn)
            .await?;
            1
        }
    };
    Ok(generation)
}

async fn upsert_standard_mutation<C: ConnectionTrait>(
    txn: &C,
    existing: Option<import_destination::Model>,
    bucket_name: &str,
    key: &str,
    mutation_id: &str,
    mutation_prefix: Option<&str>,
    now: DateTime<Utc>,
) -> AppResult<StandardMutationGuard> {
    let generation = match existing {
        Some(destination) => {
            let next = destination.generation.checked_add(1).ok_or_else(|| {
                AppError::Internal("import destination generation exhausted".to_owned())
            })?;
            let updated = import_destination::Entity::update_many()
                .col_expr(import_destination::Column::Generation, Expr::value(next))
                .col_expr(
                    import_destination::Column::OwnerJobId,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(
                    import_destination::Column::MutationId,
                    Expr::value(Some(mutation_id.to_owned())),
                )
                .col_expr(
                    import_destination::Column::MutationPrefix,
                    Expr::value(mutation_prefix.map(str::to_owned)),
                )
                .col_expr(import_destination::Column::UpdatedAt, Expr::value(now))
                .filter(import_destination::Column::Bucket.eq(bucket_name))
                .filter(import_destination::Column::Key.eq(key))
                .filter(import_destination::Column::Generation.eq(destination.generation))
                .exec(txn)
                .await?;
            if updated.rows_affected != 1 {
                return Err(AppError::Database(
                    "stale standard mutation admission compare-and-set".to_owned(),
                ));
            }
            next
        }
        None => {
            import_destination::Entity::insert(import_destination::ActiveModel {
                bucket: Set(bucket_name.to_owned()),
                key: Set(key.to_owned()),
                generation: Set(1),
                owner_job_id: Set(None),
                mutation_id: Set(Some(mutation_id.to_owned())),
                mutation_prefix: Set(mutation_prefix.map(str::to_owned)),
                updated_at: Set(now),
            })
            .exec(txn)
            .await?;
            1
        }
    };
    Ok(StandardMutationGuard {
        bucket: bucket_name.to_owned(),
        key: key.to_owned(),
        mutation_id: mutation_id.to_owned(),
        expected_generation: generation,
        mutation_prefix: mutation_prefix.map(str::to_owned),
    })
}

/// Invalidates only active standard tokens and reads them in bounded,
/// canonical key order. Prefix comparisons happen in Rust so `%`, `_`, and
/// case retain literal semantics on both SQLite and PostgreSQL.
async fn invalidate_standard_mutations_in_order<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    exact_keys: &BTreeSet<String>,
    mutation_prefix: Option<&str>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut after_key: Option<String> = None;
    loop {
        let mut query = import_destination::Entity::find()
            .filter(import_destination::Column::Bucket.eq(bucket_name))
            .filter(import_destination::Column::MutationId.is_not_null())
            .order_by_asc(import_destination::Column::Key)
            .limit(OWNERSHIP_BATCH_SIZE);
        if let Some(after_key) = after_key.as_deref() {
            query = query.filter(import_destination::Column::Key.gt(after_key));
        }
        let active = if txn.get_database_backend() == DatabaseBackend::Postgres {
            query.lock_exclusive().all(txn).await?
        } else {
            query.all(txn).await?
        };
        let Some(last_key) = active.last().map(|destination| destination.key.clone()) else {
            break;
        };
        for destination in active {
            let exact_overlap = exact_keys.iter().any(|key| {
                destination.key == *key
                    || destination
                        .mutation_prefix
                        .as_deref()
                        .is_some_and(|prefix| key.starts_with(prefix))
            });
            let prefix_overlap = mutation_prefix.is_some_and(|prefix| {
                destination.key.starts_with(prefix)
                    || destination
                        .mutation_prefix
                        .as_deref()
                        .is_some_and(|active_prefix| prefixes_overlap(active_prefix, prefix))
            });
            if !exact_overlap && !prefix_overlap {
                continue;
            }
            let mutation_id = destination.mutation_id.ok_or_else(|| {
                AppError::Internal("active standard mutation has no token".to_owned())
            })?;
            let cleared = import_destination::Entity::update_many()
                .col_expr(
                    import_destination::Column::MutationId,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(
                    import_destination::Column::MutationPrefix,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(import_destination::Column::UpdatedAt, Expr::value(now))
                .filter(import_destination::Column::Bucket.eq(bucket_name))
                .filter(import_destination::Column::Key.eq(&destination.key))
                .filter(import_destination::Column::MutationId.eq(mutation_id))
                .exec(txn)
                .await?;
            if cleared.rows_affected != 1 {
                return Err(AppError::Database(
                    "stale standard mutation invalidation compare-and-set".to_owned(),
                ));
            }
        }
        after_key = Some(last_key);
    }
    Ok(())
}

async fn has_overlapping_standard_prefix_mutation<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    key: &str,
) -> AppResult<bool> {
    let mut after_key: Option<String> = None;
    loop {
        let mut query = import_destination::Entity::find()
            .filter(import_destination::Column::Bucket.eq(bucket_name))
            .filter(import_destination::Column::MutationId.is_not_null())
            .filter(import_destination::Column::MutationPrefix.is_not_null())
            .order_by_asc(import_destination::Column::Key)
            .limit(OWNERSHIP_BATCH_SIZE);
        if let Some(after_key) = after_key.as_deref() {
            query = query.filter(import_destination::Column::Key.gt(after_key));
        }
        let active = if txn.get_database_backend() == DatabaseBackend::Postgres {
            query.lock_exclusive().all(txn).await?
        } else {
            query.all(txn).await?
        };
        let Some(last_key) = active.last().map(|destination| destination.key.clone()) else {
            return Ok(false);
        };
        if active.iter().any(|destination| {
            destination
                .mutation_prefix
                .as_deref()
                .is_some_and(|prefix| key.starts_with(prefix))
        }) {
            return Ok(true);
        }
        after_key = Some(last_key);
    }
}

async fn find_destination_for_update<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    key: &str,
) -> AppResult<Option<import_destination::Model>> {
    let query = import_destination::Entity::find_by_id((bucket_name.to_owned(), key.to_owned()));
    let destination = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(txn).await?
    } else {
        query.one(txn).await?
    };
    Ok(destination)
}

async fn supersede_conflicts_in_order<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    exact_targets: &[(String, Option<String>)],
    mutation_prefix: Option<&str>,
    excluding_job: Option<&str>,
    _reason: SupersedeReason,
    now: DateTime<Utc>,
) -> AppResult<()> {
    // Each source is an ascending, keyset-paginated stream. Keeping only the smallest batch from
    // their union makes every job lock globally ascending without materializing bucket history.
    let mut after_job_id: Option<String> = None;
    loop {
        let mut conflicting_jobs = BTreeMap::<String, bool>::new();
        for (key, exact_owner) in exact_targets {
            for job_id in prefix_owner_jobs_for_key(
                txn,
                bucket_name,
                key,
                after_job_id.as_deref(),
                excluding_job,
            )
            .await?
            {
                insert_conflicting_job(
                    &mut conflicting_jobs,
                    job_id,
                    false,
                    after_job_id.as_deref(),
                    excluding_job,
                );
            }
            if let Some(exact_owner) = exact_owner.as_deref() {
                insert_conflicting_job(
                    &mut conflicting_jobs,
                    exact_owner.to_owned(),
                    false,
                    after_job_id.as_deref(),
                    excluding_job,
                );
            }
        }
        if let Some(prefix) = mutation_prefix {
            for job_id in prefix_owner_jobs_for_prefix(
                txn,
                bucket_name,
                prefix,
                after_job_id.as_deref(),
                excluding_job,
            )
            .await?
            {
                insert_conflicting_job(
                    &mut conflicting_jobs,
                    job_id,
                    true,
                    after_job_id.as_deref(),
                    excluding_job,
                );
            }
            for job_id in destination_owner_jobs_for_prefix(
                txn,
                bucket_name,
                prefix,
                after_job_id.as_deref(),
                excluding_job,
            )
            .await?
            {
                insert_conflicting_job(
                    &mut conflicting_jobs,
                    job_id,
                    true,
                    after_job_id.as_deref(),
                    excluding_job,
                );
            }
        }
        let Some(last_job_id) = conflicting_jobs.last_key_value().map(|(id, _)| id.clone()) else {
            break;
        };
        for (job_id, invalidate_destinations) in conflicting_jobs {
            let invalidation_prefix = if invalidate_destinations {
                Some((
                    bucket_name,
                    mutation_prefix.ok_or_else(|| {
                        AppError::Internal(
                            "prefix ownership conflict is missing its literal prefix".to_owned(),
                        )
                    })?,
                ))
            } else {
                None
            };
            supersede_job_with_destination_invalidation(txn, &job_id, invalidation_prefix, now)
                .await?;
        }
        after_job_id = Some(last_job_id);
    }
    Ok(())
}

fn insert_conflicting_job(
    conflicting_jobs: &mut BTreeMap<String, bool>,
    job_id: String,
    invalidate_destinations: bool,
    after_job_id: Option<&str>,
    excluding_job: Option<&str>,
) {
    if excluding_job == Some(job_id.as_str())
        || after_job_id.is_some_and(|after_job_id| job_id.as_str() <= after_job_id)
    {
        return;
    }
    conflicting_jobs
        .entry(job_id)
        .and_modify(|invalidate| *invalidate |= invalidate_destinations)
        .or_insert(invalidate_destinations);
    if conflicting_jobs.len() > OWNERSHIP_BATCH_SIZE as usize {
        conflicting_jobs.pop_last();
    }
}

async fn prefix_owner_jobs_for_key<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    key: &str,
    after_job_id: Option<&str>,
    excluding_job: Option<&str>,
) -> AppResult<Vec<String>> {
    query_owner_job_ids(
        txn,
        "SELECT DISTINCT c.job_id AS job_id \
         FROM import_prefix_claims c \
         JOIN import_jobs j ON j.id = c.job_id \
         WHERE c.bucket = ?1 \
           AND j.state IN ('queued', 'running') \
           AND c.prefix = substr(?2, 1, length(c.prefix)) \
           AND (?3 IS NULL OR c.job_id > ?3) \
           AND (?4 IS NULL OR c.job_id <> ?4) \
         ORDER BY c.job_id LIMIT ?5",
        "SELECT DISTINCT c.job_id AS job_id \
         FROM import_prefix_claims c \
         JOIN import_jobs j ON j.id = c.job_id \
         WHERE c.bucket = $1 \
           AND j.state IN ('queued', 'running') \
           AND c.prefix = substring($2::text FROM 1 FOR char_length(c.prefix)) \
           AND ($3::text IS NULL OR c.job_id > $3) \
           AND ($4::text IS NULL OR c.job_id <> $4) \
         ORDER BY c.job_id LIMIT $5",
        bucket_name,
        key,
        after_job_id,
        excluding_job,
    )
    .await
}

async fn prefix_owner_jobs_for_prefix<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    prefix: &str,
    after_job_id: Option<&str>,
    excluding_job: Option<&str>,
) -> AppResult<Vec<String>> {
    query_owner_job_ids(
        txn,
        "SELECT DISTINCT c.job_id AS job_id \
         FROM import_prefix_claims c \
         JOIN import_jobs j ON j.id = c.job_id \
         WHERE c.bucket = ?1 \
           AND j.state IN ('queued', 'running') \
           AND (c.prefix = substr(?2, 1, length(c.prefix)) \
                OR ?2 = substr(c.prefix, 1, length(?2))) \
           AND (?3 IS NULL OR c.job_id > ?3) \
           AND (?4 IS NULL OR c.job_id <> ?4) \
         ORDER BY c.job_id LIMIT ?5",
        "SELECT DISTINCT c.job_id AS job_id \
         FROM import_prefix_claims c \
         JOIN import_jobs j ON j.id = c.job_id \
         WHERE c.bucket = $1 \
           AND j.state IN ('queued', 'running') \
           AND (c.prefix = substring($2::text FROM 1 FOR char_length(c.prefix)) \
                OR $2::text = substring(c.prefix FROM 1 FOR char_length($2::text))) \
           AND ($3::text IS NULL OR c.job_id > $3) \
           AND ($4::text IS NULL OR c.job_id <> $4) \
         ORDER BY c.job_id LIMIT $5",
        bucket_name,
        prefix,
        after_job_id,
        excluding_job,
    )
    .await
}

async fn destination_owner_jobs_for_prefix<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    prefix: &str,
    after_job_id: Option<&str>,
    excluding_job: Option<&str>,
) -> AppResult<Vec<String>> {
    query_owner_job_ids(
        txn,
        "SELECT DISTINCT d.owner_job_id AS job_id \
         FROM import_destinations d \
         JOIN import_jobs j ON j.id = d.owner_job_id \
         WHERE d.bucket = ?1 \
           AND d.owner_job_id IS NOT NULL \
           AND j.state IN ('queued', 'running') \
           AND ?2 = substr(d.key, 1, length(?2)) \
           AND (?3 IS NULL OR d.owner_job_id > ?3) \
           AND (?4 IS NULL OR d.owner_job_id <> ?4) \
         ORDER BY d.owner_job_id LIMIT ?5",
        "SELECT DISTINCT d.owner_job_id AS job_id \
         FROM import_destinations d \
         JOIN import_jobs j ON j.id = d.owner_job_id \
         WHERE d.bucket = $1 \
           AND d.owner_job_id IS NOT NULL \
           AND j.state IN ('queued', 'running') \
           AND $2::text = substring(d.key FROM 1 FOR char_length($2::text)) \
           AND ($3::text IS NULL OR d.owner_job_id > $3) \
           AND ($4::text IS NULL OR d.owner_job_id <> $4) \
         ORDER BY d.owner_job_id LIMIT $5",
        bucket_name,
        prefix,
        after_job_id,
        excluding_job,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn query_owner_job_ids<C: ConnectionTrait>(
    txn: &C,
    sqlite_sql: &str,
    postgres_sql: &str,
    bucket_name: &str,
    literal: &str,
    after_job_id: Option<&str>,
    excluding_job: Option<&str>,
) -> AppResult<Vec<String>> {
    let backend = txn.get_database_backend();
    let sql = match backend {
        DatabaseBackend::Sqlite => sqlite_sql,
        DatabaseBackend::Postgres => postgres_sql,
        _ => {
            return Err(AppError::Internal(
                "import ownership requires SQLite or PostgreSQL".to_owned(),
            ));
        }
    };
    let rows = txn
        .query_all(Statement::from_sql_and_values(
            backend,
            sql,
            vec![
                bucket_name.to_owned().into(),
                literal.to_owned().into(),
                after_job_id.map(str::to_owned).into(),
                excluding_job.map(str::to_owned).into(),
                (OWNERSHIP_BATCH_SIZE as i64).into(),
            ],
        ))
        .await?;
    rows.into_iter()
        .map(|row| {
            row.try_get::<String>("", "job_id")
                .map_err(|error| AppError::Database(error.to_string()))
        })
        .collect()
}

async fn next_prefix_claim_order<C: ConnectionTrait>(txn: &C, bucket_name: &str) -> AppResult<i64> {
    let backend = txn.get_database_backend();
    let sql = match backend {
        DatabaseBackend::Sqlite => {
            "SELECT MAX(claim_order) AS max_claim_order FROM import_prefix_claims WHERE bucket = ?1"
        }
        DatabaseBackend::Postgres => {
            "SELECT MAX(claim_order) AS max_claim_order FROM import_prefix_claims WHERE bucket = $1"
        }
        _ => {
            return Err(AppError::Internal(
                "import ownership requires SQLite or PostgreSQL".to_owned(),
            ));
        }
    };
    let row = txn
        .query_one(Statement::from_sql_and_values(
            backend,
            sql,
            [bucket_name.to_owned().into()],
        ))
        .await?
        .ok_or_else(|| AppError::Internal("prefix claim aggregate returned no row".to_owned()))?;
    let maximum = row
        .try_get::<Option<i64>>("", "max_claim_order")
        .map_err(|error| AppError::Database(error.to_string()))?;
    Ok(maximum.map_or(0, |maximum| maximum.saturating_add(1)))
}

async fn find_job_for_update<C: ConnectionTrait>(
    txn: &C,
    job_id: &str,
) -> AppResult<Option<import_job::Model>> {
    let query = import_job::Entity::find_by_id(job_id.to_owned());
    let job = if txn.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(txn).await?
    } else {
        query.one(txn).await?
    };
    Ok(job)
}

fn prefixes_overlap(left: &str, right: &str) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn target_map(
    targets: &[ExpectedImportTarget],
    archive_bucket: &str,
) -> AppResult<BTreeMap<(String, String), i64>> {
    let mut output = BTreeMap::new();
    for target in targets {
        if target.bucket != archive_bucket || target.generation < 1 {
            return Err(AppError::StaleImportOwnership);
        }
        let location = (target.bucket.clone(), target.key.clone());
        if let Some(previous) = output.insert(location, target.generation)
            && previous != target.generation
        {
            return Err(AppError::StaleImportOwnership);
        }
    }
    Ok(output)
}

fn publication_target_set(
    targets: &[(String, String)],
    archive_bucket: &str,
) -> AppResult<BTreeSet<(String, String)>> {
    if targets
        .iter()
        .any(|(bucket_name, _)| bucket_name != archive_bucket)
    {
        return Err(AppError::StaleImportOwnership);
    }
    Ok(targets.iter().cloned().collect())
}

fn active_claim_filter(
    claim: &ImportClaim,
    backend: DatabaseBackend,
) -> sea_orm::sea_query::Condition {
    sea_orm::sea_query::Condition::all()
        .add(import_job::Column::Id.eq(&claim.job_id))
        .add(import_job::Column::State.eq(STATE_RUNNING))
        .add(import_job::Column::LockedBy.eq(&claim.worker_id))
        .add(import_job::Column::ClaimEpoch.eq(claim.claim_epoch))
        .add(lease_clock::active_lease(backend))
}

fn active_guard_filter(
    guard: &ImportPublicationGuard,
    backend: DatabaseBackend,
) -> sea_orm::sea_query::Condition {
    sea_orm::sea_query::Condition::all()
        .add(import_job::Column::Id.eq(&guard.job_id))
        .add(import_job::Column::State.eq(STATE_RUNNING))
        .add(import_job::Column::LockedBy.eq(&guard.worker_id))
        .add(import_job::Column::ClaimEpoch.eq(guard.claim_epoch))
        .add(lease_clock::active_lease(backend))
}

fn is_retryable_transaction_conflict(error: &AppError) -> bool {
    let AppError::Database(message) = error else {
        return false;
    };
    let message = message.to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database is busy")
        || message.contains("duplicate key value violates unique constraint")
        || message.contains("unique violation")
        || message.contains("sqlstate 23505")
        || message.contains("code: 23505")
        || message.contains("code: 1555")
        || message.contains("code: 2067")
        || message.contains("deadlock detected")
        || message.contains("could not serialize access")
        || message.contains("serialization failure")
        || message.contains("sqlstate 40p01")
        || message.contains("code: 40p01")
        || message.contains("sqlstate 40001")
        || message.contains("code: 40001")
}

async fn ownership_retry_delay(retry: usize) {
    let milliseconds = 10_u64.checked_shl(retry.min(2) as u32).unwrap_or(40);
    tokio::time::sleep(Duration::from_millis(milliseconds)).await;
}

fn transaction_error_into_app(error: TransactionError<AppError>) -> AppError {
    match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => error.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::LazyLock};

    use chrono::{DateTime, Duration, Utc};
    use sea_orm::{
        ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait, PaginatorTrait,
        QueryFilter, TransactionTrait,
    };

    use super::*;
    use crate::{
        import::{ImportFailureCode, ImportSource},
        pinning::tags::ObjectTag,
        store::{
            self,
            entities::{
                bucket, import_destination, import_job, import_job_target, import_prefix_claim,
            },
            import::jobs::{NewImportJob, SubmitImportOutcome, claim_due},
        },
    };

    static TEST_EPOCH: LazyLock<DateTime<Utc>> = LazyLock::new(|| Utc::now() + Duration::days(1));

    fn time(seconds: i64) -> DateTime<Utc> {
        *TEST_EPOCH + Duration::seconds(seconds)
    }

    async fn setup() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        db
    }

    async fn setup_file_backed(
        name: &str,
    ) -> (tempfile::TempDir, DatabaseConnection, DatabaseConnection) {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join(name);
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let first = connect_file_database(&database_url).await;
        store::run_migrations(&first).await.unwrap();
        crate::store::bucket::create(&first, "bucket", None)
            .await
            .unwrap();
        let second = connect_file_database(&database_url).await;
        (directory, first, second)
    }

    async fn connect_file_database(database_url: &str) -> DatabaseConnection {
        let mut options = ConnectOptions::new(database_url.to_owned());
        options.max_connections(1).min_connections(1);
        store::apply_sqlite_busy_timeout(&mut options);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        db
    }

    fn request(
        id: &str,
        key: &str,
        token: Option<&str>,
        decompress_prefix: Option<&str>,
    ) -> NewImportJob {
        NewImportJob {
            id: id.to_owned(),
            bucket: "bucket".to_owned(),
            key: key.to_owned(),
            source: ImportSource::Cid(
                "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_owned(),
            ),
            request_fingerprint: format!("sha256:{id}"),
            client_token: token.map(str::to_owned),
            object_content_type: Some("application/octet-stream".to_owned()),
            metadata: HashMap::new(),
            tags: vec![ObjectTag::new("fixture", "true")],
            decompress_prefix: decompress_prefix.map(str::to_owned),
        }
    }

    async fn job(db: &DatabaseConnection, id: &str) -> import_job::Model {
        import_job::Entity::find_by_id(id.to_owned())
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    async fn destination(db: &DatabaseConnection, key: &str) -> import_destination::Model {
        import_destination::Entity::find_by_id(("bucket".to_owned(), key.to_owned()))
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    async fn claim_one(
        db: &DatabaseConnection,
        worker_id: &str,
        now: DateTime<Utc>,
    ) -> ImportClaim {
        claim_due(db, worker_id, now, now + Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap()
            .claim
    }

    #[tokio::test]
    async fn atomic_submission_creates_an_owned_archive_target() {
        let db = setup().await;
        let submitted = submit(&db, request("job-1", "archive.zip", None, None), time(0))
            .await
            .unwrap();
        assert!(matches!(submitted, SubmitImportOutcome::Created(_)));

        let stored = job(&db, "job-1").await;
        let owned = destination(&db, "archive.zip").await;
        let target = import_job_target::Entity::find_by_id((
            "job-1".to_owned(),
            "bucket".to_owned(),
            "archive.zip".to_owned(),
        ))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(stored.state, STATE_QUEUED);
        assert_eq!(
            (owned.generation, owned.owner_job_id.as_deref()),
            (1, Some("job-1"))
        );
        assert_eq!(
            (target.expected_generation, target.kind.as_str()),
            (1, KIND_ARCHIVE)
        );
    }

    #[test]
    fn postgres_bucket_ownership_fence_uses_no_key_update_sql_shape() {
        let statement = postgres_bucket_ownership_lock_statement("bucket");
        let rendered = format!("{statement:?}");

        assert_eq!(
            POSTGRES_BUCKET_OWNERSHIP_LOCK_SQL,
            "SELECT name FROM buckets WHERE name = $1 FOR NO KEY UPDATE"
        );
        assert!(rendered.contains(POSTGRES_BUCKET_OWNERSHIP_LOCK_SQL));
        assert!(rendered.contains("bucket"), "bucket must be a bound value");
        assert!(!POSTGRES_BUCKET_OWNERSHIP_LOCK_SQL.contains(" FOR UPDATE"));
    }

    #[tokio::test]
    async fn token_replay_is_unchanged_conflict_is_rejected_and_untokened_submit_supersedes() {
        let db = setup().await;
        let mut first = request("job-1", "key", Some("token"), None);
        first.request_fingerprint = "sha256:same".to_owned();
        submit(&db, first, time(0)).await.unwrap();
        let before = destination(&db, "key").await;

        let mut replay = request("job-replay", "key", Some("token"), None);
        replay.request_fingerprint = "sha256:same".to_owned();
        let replayed = submit(&db, replay, time(1)).await.unwrap();
        assert!(matches!(replayed, SubmitImportOutcome::Replayed(ref row) if row.id == "job-1"));
        assert_eq!(destination(&db, "key").await, before);
        assert!(
            import_job::Entity::find_by_id("job-replay".to_owned())
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );

        let mut mismatch = request("job-mismatch", "key", Some("token"), None);
        mismatch.request_fingerprint = "sha256:different".to_owned();
        assert!(matches!(
            submit(&db, mismatch, time(2)).await,
            Err(AppError::ImportIdempotencyConflict)
        ));
        assert_eq!(destination(&db, "key").await, before);

        submit(&db, request("job-2", "key", None, None), time(3))
            .await
            .unwrap();
        assert_eq!(job(&db, "job-1").await.state, STATE_SUPERSEDED);
        let owned = destination(&db, "key").await;
        assert_eq!(
            (owned.generation, owned.owner_job_id.as_deref()),
            (2, Some("job-2"))
        );
    }

    #[tokio::test]
    async fn idempotency_preflight_is_bucket_fenced_and_read_only() {
        let db = setup().await;
        let mut first = request("job-1", "key", Some("token"), None);
        first.request_fingerprint = "sha256:same".to_owned();
        submit(&db, first, time(0)).await.unwrap();
        let destination_before = destination(&db, "key").await;

        let replay = preflight_idempotent_submission(&db, "bucket", "key", "token", "sha256:same")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.id, "job-1");
        assert_eq!(destination(&db, "key").await, destination_before);

        assert!(matches!(
            preflight_idempotent_submission(&db, "bucket", "key", "token", "sha256:different",)
                .await,
            Err(AppError::ImportIdempotencyConflict)
        ));
        assert!(
            preflight_idempotent_submission(&db, "bucket", "key", "another-token", "sha256:same",)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(destination(&db, "key").await, destination_before);

        let superseded = db
            .transaction(|txn| {
                Box::pin(async move {
                    lock_bucket_for_ownership(txn, "bucket").await?;
                    supersede_bucket(txn, "bucket", time(1)).await
                })
            })
            .await
            .unwrap();
        assert_eq!(superseded, 1);
        assert_eq!(job(&db, "job-1").await.state, STATE_SUPERSEDED);
        crate::store::bucket::delete(&db, "bucket").await.unwrap();
        let retained_before = job(&db, "job-1").await;
        assert!(matches!(
            preflight_idempotent_submission(
                &db,
                "bucket",
                "key",
                "token",
                "sha256:same",
            )
            .await,
            Err(AppError::NoSuchBucket(ref bucket)) if bucket == "bucket"
        ));
        assert_eq!(job(&db, "job-1").await, retained_before);
    }

    #[tokio::test]
    async fn exact_generations_are_monotonic_and_admission_releases_owners() {
        let db = setup().await;
        submit(&db, request("first", "key", None, None), time(0))
            .await
            .unwrap();
        submit(&db, request("second", "key", None, None), time(1))
            .await
            .unwrap();
        assert_eq!(job(&db, "first").await.state, STATE_SUPERSEDED);
        assert_eq!(destination(&db, "key").await.generation, 2);

        admit_content_mutation(
            &db,
            "bucket",
            "key",
            None,
            SupersedeReason::PutObject,
            time(2),
        )
        .await
        .unwrap();
        let released = destination(&db, "key").await;
        assert_eq!((released.generation, released.owner_job_id), (3, None));
        assert_eq!(job(&db, "second").await.state, STATE_SUPERSEDED);

        submit(&db, request("third", "key", None, None), time(3))
            .await
            .unwrap();
        let reowned = destination(&db, "key").await;
        assert_eq!(
            (reowned.generation, reowned.owner_job_id.as_deref()),
            (4, Some("third"))
        );
    }

    #[tokio::test]
    async fn prefixes_are_literal_and_empty_prefix_overlaps_everything() {
        let db = setup().await;
        submit(
            &db,
            request("literal", "archive-a", None, Some("literal%_")),
            time(0),
        )
        .await
        .unwrap();
        submit(
            &db,
            request("inside", "literal%_entry", None, None),
            time(1),
        )
        .await
        .unwrap();
        assert_eq!(job(&db, "literal").await.state, STATE_SUPERSEDED);

        submit(
            &db,
            request("not-like", "archive-b", None, Some("a_b%")),
            time(2),
        )
        .await
        .unwrap();
        submit(&db, request("not-inside", "axbZ", None, None), time(3))
            .await
            .unwrap();
        assert_eq!(job(&db, "not-like").await.state, STATE_QUEUED);

        submit(&db, request("empty", "archive-c", None, Some("")), time(4))
            .await
            .unwrap();
        submit(&db, request("everywhere", "unrelated", None, None), time(5))
            .await
            .unwrap();
        assert_eq!(job(&db, "empty").await.state, STATE_SUPERSEDED);
        assert!(prefixes_overlap("", "anything"));
        assert!(prefixes_overlap("a/", "a/b/"));
        assert!(!prefixes_overlap("a/", "b/"));
    }

    #[tokio::test]
    async fn literal_prefix_conflict_queries_treat_percent_underscore_and_case_as_data() {
        let db = setup().await;
        submit(
            &db,
            request("literal-owner", "archive-a", None, Some("literal%_/")),
            time(0),
        )
        .await
        .unwrap();
        submit(
            &db,
            request("wildcard-lookalike", "literalXX/file", None, None),
            time(1),
        )
        .await
        .unwrap();
        assert_eq!(job(&db, "literal-owner").await.state, STATE_QUEUED);
        submit(
            &db,
            request("literal-match", "literal%_/file", None, None),
            time(2),
        )
        .await
        .unwrap();
        assert_eq!(job(&db, "literal-owner").await.state, STATE_SUPERSEDED);
        admit_prefix_mutation(&db, "bucket", "literal%_/", time(3))
            .await
            .unwrap();
        assert_eq!(job(&db, "literal-match").await.state, STATE_SUPERSEDED);
        assert_eq!(job(&db, "wildcard-lookalike").await.state, STATE_QUEUED);

        submit(
            &db,
            request("case-owner", "archive-b", None, Some("Case/")),
            time(4),
        )
        .await
        .unwrap();
        submit(
            &db,
            request("case-lookalike", "case/file", None, None),
            time(5),
        )
        .await
        .unwrap();
        assert_eq!(job(&db, "case-owner").await.state, STATE_QUEUED);
        submit(&db, request("case-match", "Case/file", None, None), time(6))
            .await
            .unwrap();
        assert_eq!(job(&db, "case-owner").await.state, STATE_SUPERSEDED);
        admit_prefix_mutation(&db, "bucket", "Case/", time(7))
            .await
            .unwrap();
        assert_eq!(job(&db, "case-match").await.state, STATE_SUPERSEDED);
        assert_eq!(job(&db, "case-lookalike").await.state, STATE_QUEUED);
    }

    #[tokio::test]
    async fn prefix_admission_ignores_ownerless_history_and_invalidates_active_exact_destinations()
    {
        let db = setup().await;
        for index in 0..300 {
            import_destination::Entity::insert(import_destination::ActiveModel {
                bucket: Set("bucket".to_owned()),
                key: Set(format!("unrelated-history/{index:03}")),
                generation: Set(7),
                owner_job_id: Set(None),
                mutation_id: Set(None),
                mutation_prefix: Set(None),
                updated_at: Set(time(-1)),
            })
            .exec(&db)
            .await
            .unwrap();
        }
        import_destination::Entity::insert(import_destination::ActiveModel {
            bucket: Set("bucket".to_owned()),
            key: Set("scope/ownerless-history".to_owned()),
            generation: Set(41),
            owner_job_id: Set(None),
            mutation_id: Set(None),
            mutation_prefix: Set(None),
            updated_at: Set(time(-1)),
        })
        .exec(&db)
        .await
        .unwrap();
        submit(
            &db,
            request("active-owner", "scope/active", None, None),
            time(0),
        )
        .await
        .unwrap();

        admit_prefix_mutation(&db, "bucket", "scope/", time(1))
            .await
            .unwrap();

        assert_eq!(job(&db, "active-owner").await.state, STATE_SUPERSEDED);
        let active = destination(&db, "scope/active").await;
        assert_eq!((active.generation, active.owner_job_id), (2, None));
        let ownerless = destination(&db, "scope/ownerless-history").await;
        assert_eq!((ownerless.generation, ownerless.owner_job_id), (41, None));
        assert_eq!(
            destination(&db, "unrelated-history/299").await.generation,
            7
        );
    }

    #[tokio::test]
    async fn empty_prefix_processes_more_than_owner_batch_size_with_exact_terminal_state() {
        let db = setup().await;
        let mut job_ids = Vec::new();
        for index in 0..133 {
            let job_id = format!("batch-job-{index:03}");
            let key = format!("batch-key-{index:03}");
            submit(&db, request(&job_id, &key, None, None), time(0))
                .await
                .unwrap();
            job_ids.push((job_id, key));
        }

        admit_prefix_mutation(&db, "bucket", "", time(1))
            .await
            .unwrap();

        for (job_id, key) in job_ids {
            assert_eq!(job(&db, &job_id).await.state, STATE_SUPERSEDED);
            let released = destination(&db, &key).await;
            assert_eq!((released.generation, released.owner_job_id), (2, None));
            assert_no_job_targets(&db, &job_id).await;
        }
        assert_eq!(prefix_claim_count(&db).await, 0);
    }

    #[tokio::test]
    async fn prefix_admission_invalidates_more_than_one_standard_mutation_batch() {
        let db = setup().await;
        let mut keys = Vec::new();
        for index in 0..(OWNERSHIP_BATCH_SIZE + 1) {
            let key = format!("scope/active-standard-{index:03}");
            admit_content_mutation(
                &db,
                "bucket",
                &key,
                None,
                SupersedeReason::PutObject,
                time(0),
            )
            .await
            .unwrap();
            keys.push(key);
        }

        admit_prefix_mutation(&db, "bucket", "scope/", time(1))
            .await
            .unwrap();

        for key in keys {
            let invalidated = destination(&db, &key).await;
            assert!(invalidated.mutation_id.is_none(), "{key}");
            assert!(invalidated.mutation_prefix.is_none(), "{key}");
        }
    }

    #[tokio::test]
    async fn prefix_overlap_query_matches_ancestors_and_descendants_only() {
        let db = setup().await;
        submit(
            &db,
            request("ancestor", "archive-a", None, Some("tree/")),
            time(0),
        )
        .await
        .unwrap();
        submit(
            &db,
            request("descendant", "archive-b", None, Some("tree/branch/")),
            time(1),
        )
        .await
        .unwrap();
        assert_eq!(job(&db, "ancestor").await.state, STATE_SUPERSEDED);

        submit(
            &db,
            request(
                "existing-descendant",
                "archive-c",
                None,
                Some("forest/branch/"),
            ),
            time(2),
        )
        .await
        .unwrap();
        submit(
            &db,
            request("case-sibling", "archive-d", None, Some("Forest/")),
            time(3),
        )
        .await
        .unwrap();

        admit_prefix_mutation(&db, "bucket", "forest/", time(4))
            .await
            .unwrap();

        assert_eq!(
            job(&db, "existing-descendant").await.state,
            STATE_SUPERSEDED
        );
        assert_eq!(job(&db, "case-sibling").await.state, STATE_QUEUED);
        assert_eq!(job(&db, "descendant").await.state, STATE_QUEUED);
    }

    #[tokio::test]
    async fn extracted_claim_and_terminal_failure_are_epoch_fenced_and_release_claims() {
        let db = setup().await;
        submit(
            &db,
            request("job", "archive.zip", None, Some("out/")),
            time(0),
        )
        .await
        .unwrap();
        let first = claim_one(&db, "worker-1", time(0)).await;
        let second = claim_one(&db, "worker-2", time(30)).await;
        assert_eq!(second.claim_epoch, first.claim_epoch + 1);

        for stale in [
            claim_extracted_target(&db, &first, "bucket", "out/file.txt", time(30))
                .await
                .map(|_| ()),
            fail_claimed(
                &db,
                &first,
                "bucket",
                &ImportFailure {
                    code: ImportFailureCode::PublicationFailed,
                    message: "redacted failure".to_owned(),
                    retryable: false,
                },
                time(30),
            )
            .await,
        ] {
            assert!(matches!(stale, Err(AppError::StaleImportOwnership)));
        }

        let generation = claim_extracted_target(&db, &second, "bucket", "out/file.txt", time(31))
            .await
            .unwrap();
        assert_eq!(generation, 1);
        fail_claimed(
            &db,
            &second,
            "bucket",
            &ImportFailure {
                code: ImportFailureCode::PublicationFailed,
                message: "redacted failure".to_owned(),
                retryable: false,
            },
            time(31),
        )
        .await
        .unwrap();
        assert_eq!(job(&db, "job").await.state, STATE_FAILED);
        let released = destination(&db, "out/file.txt").await;
        assert_eq!((released.generation, released.owner_job_id), (1, None));
        assert!(
            import_job_target::Entity::find()
                .filter(import_job_target::Column::JobId.eq("job"))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            import_prefix_claim::Entity::find()
                .filter(import_prefix_claim::Column::JobId.eq("job"))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn expired_reclaim_reset_releases_only_prior_extracted_targets() {
        let db = setup().await;
        submit(
            &db,
            request("reset-reclaim", "archive.zip", None, Some("out/")),
            time(0),
        )
        .await
        .unwrap();
        let first = claim_one(&db, "worker-1", time(0)).await;
        let old_generation = claim_extracted_target(&db, &first, "bucket", "out/a.txt", time(1))
            .await
            .unwrap();
        let second = claim_one(&db, "worker-2", time(30)).await;
        assert_eq!(second.claim_epoch, first.claim_epoch + 1);

        assert!(matches!(
            reset_extracted_targets_for_attempt(&db, &first, "bucket", time(30)).await,
            Err(AppError::StaleImportOwnership)
        ));
        reset_extracted_targets_for_attempt(&db, &second, "bucket", time(30))
            .await
            .unwrap();

        let old_destination = destination(&db, "out/a.txt").await;
        assert_eq!(
            (old_destination.generation, old_destination.owner_job_id),
            (old_generation, None)
        );
        assert!(
            import_job_target::Entity::find_by_id((
                "reset-reclaim".to_owned(),
                "bucket".to_owned(),
                "out/a.txt".to_owned(),
            ))
            .one(&db)
            .await
            .unwrap()
            .is_none()
        );
        let archive_target = import_job_target::Entity::find_by_id((
            "reset-reclaim".to_owned(),
            "bucket".to_owned(),
            "archive.zip".to_owned(),
        ))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(archive_target.kind, KIND_ARCHIVE);
        assert_eq!(
            destination(&db, "archive.zip")
                .await
                .owner_job_id
                .as_deref(),
            Some("reset-reclaim")
        );
        assert!(
            import_prefix_claim::Entity::find_by_id((
                "reset-reclaim".to_owned(),
                "bucket".to_owned(),
                "out/".to_owned(),
            ))
            .one(&db)
            .await
            .unwrap()
            .is_some()
        );
        let current_generation =
            claim_extracted_target(&db, &second, "bucket", "out/b.txt", time(31))
                .await
                .unwrap();
        assert_eq!(current_generation, 1);
    }

    #[tokio::test]
    async fn multi_batch_reset_commits_before_renewal_and_resumes_under_extended_lease() {
        let (_directory, first, second) = setup_file_backed("reset-batch-renewal.sqlite").await;
        let now = Utc::now();
        submit(
            &first,
            request("reset-batches", "archive.zip", None, Some("out/")),
            now,
        )
        .await
        .unwrap();
        let claimed = claim_due(&first, "worker", now, now + Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let seed = first.begin().await.unwrap();
        lock_bucket_for_ownership(&seed, "bucket").await.unwrap();
        for index in 0..(OWNERSHIP_BATCH_SIZE + 1) {
            let key = format!("out/{index:03}.txt");
            import_destination::Entity::insert(import_destination::ActiveModel {
                bucket: Set("bucket".to_owned()),
                key: Set(key.clone()),
                generation: Set(1),
                owner_job_id: Set(Some("reset-batches".to_owned())),
                mutation_id: Set(None),
                mutation_prefix: Set(None),
                updated_at: Set(now),
            })
            .exec(&seed)
            .await
            .unwrap();
            import_job_target::Entity::insert(import_job_target::ActiveModel {
                job_id: Set("reset-batches".to_owned()),
                bucket: Set("bucket".to_owned()),
                key: Set(key),
                expected_generation: Set(1),
                kind: Set(KIND_EXTRACTED.to_owned()),
            })
            .exec(&seed)
            .await
            .unwrap();
        }
        seed.commit().await.unwrap();
        let original_deadline = Utc::now() + Duration::seconds(2);
        assert!(
            crate::store::import::jobs::renew_claim(
                &first,
                &claimed.job.id,
                &claimed.claim.worker_id,
                claimed.claim.claim_epoch,
                Utc::now(),
                original_deadline,
            )
            .await
            .unwrap()
        );
        let gate = std::sync::Arc::new(reset_test_gate::BatchGate {
            job_id: claimed.job.id.clone(),
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        *reset_test_gate::AFTER_BATCH.lock().await = Some(gate.clone());
        let reset_db = first.clone();
        let reset_claim = claimed.claim.clone();
        let mut reset = tokio::spawn(async move {
            reset_extracted_targets_for_attempt(&reset_db, &reset_claim, "bucket", Utc::now()).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), gate.arrived.notified())
            .await
            .expect("reset must pause after its first full batch");

        let renewal_db = second.clone();
        let renewal_claim = claimed.claim.clone();
        let extended_deadline = Utc::now() + Duration::seconds(5);
        let mut renewal = tokio::spawn(async move {
            crate::store::import::jobs::renew_claim(
                &renewal_db,
                &renewal_claim.job_id,
                &renewal_claim.worker_id,
                renewal_claim.claim_epoch,
                Utc::now(),
                extended_deadline,
            )
            .await
        });
        let renewal_while_paused =
            tokio::time::timeout(std::time::Duration::from_millis(500), &mut renewal).await;
        let remaining = (original_deadline - Utc::now())
            .to_std()
            .unwrap_or(std::time::Duration::ZERO);
        tokio::time::sleep(remaining + std::time::Duration::from_millis(75)).await;
        gate.resume.notify_one();
        let reset_result = tokio::time::timeout(std::time::Duration::from_secs(3), &mut reset)
            .await
            .expect("reset must finish after the gate resumes")
            .unwrap();
        if renewal_while_paused.is_err() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), &mut renewal).await;
        }
        assert!(
            matches!(renewal_while_paused, Ok(Ok(Ok(true)))),
            "renewal must complete while reset is paused between committed batches"
        );
        reset_result.unwrap();

        assert_eq!(
            import_job_target::Entity::find()
                .filter(import_job_target::Column::JobId.eq("reset-batches"))
                .filter(import_job_target::Column::Kind.eq(KIND_EXTRACTED))
                .count(&first)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            import_destination::Entity::find()
                .filter(import_destination::Column::OwnerJobId.eq("reset-batches"))
                .filter(import_destination::Column::Key.ne("archive.zip"))
                .count(&first)
                .await
                .unwrap(),
            0
        );
        for index in 0..(OWNERSHIP_BATCH_SIZE + 1) {
            let row = destination(&first, &format!("out/{index:03}.txt")).await;
            assert_eq!((row.generation, row.owner_job_id), (1, None));
        }
        let archive_target = import_job_target::Entity::find_by_id((
            "reset-batches".to_owned(),
            "bucket".to_owned(),
            "archive.zip".to_owned(),
        ))
        .one(&first)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            (
                archive_target.kind.as_str(),
                archive_target.expected_generation
            ),
            (KIND_ARCHIVE, 1)
        );
        let archive_destination = destination(&first, "archive.zip").await;
        assert_eq!(archive_destination.generation, 1);
        assert_eq!(
            archive_destination.owner_job_id.as_deref(),
            Some("reset-batches")
        );
        assert!(
            import_prefix_claim::Entity::find_by_id((
                "reset-batches".to_owned(),
                "bucket".to_owned(),
                "out/".to_owned(),
            ))
            .one(&first)
            .await
            .unwrap()
            .is_some()
        );
        assert!(reset_test_gate::AFTER_BATCH.lock().await.is_none());
    }

    #[tokio::test]
    async fn terminal_failure_waiting_on_bucket_lock_rechecks_database_clock_after_expiry() {
        let (_directory, first, second) = setup_file_backed("terminal-failure-expiry.sqlite").await;
        let now = Utc::now();
        submit(
            &first,
            request("terminal-expiry", "terminal-expiry-key", None, None),
            now,
        )
        .await
        .unwrap();
        let lease_until = now + Duration::milliseconds(200);
        let claim = claim_due(&first, "worker", now, lease_until, 1)
            .await
            .unwrap()
            .pop()
            .unwrap()
            .claim;
        let holder = first.begin().await.unwrap();
        lock_bucket_for_ownership(&holder, "bucket").await.unwrap();
        let mut failure = tokio::spawn(async move {
            fail_claimed(
                &second,
                &claim,
                "bucket",
                &ImportFailure {
                    code: ImportFailureCode::PublicationFailed,
                    message: "terminal".to_owned(),
                    retryable: false,
                },
                now,
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut failure)
                .await
                .is_err(),
            "terminal failure must be waiting on the held bucket lock"
        );
        let remaining = (lease_until - Utc::now())
            .to_std()
            .unwrap_or(std::time::Duration::ZERO);
        tokio::time::sleep(remaining + std::time::Duration::from_millis(75)).await;
        holder.rollback().await.unwrap();

        assert!(matches!(
            failure.await.unwrap(),
            Err(AppError::StaleImportOwnership)
        ));
        let row = job(&first, "terminal-expiry").await;
        assert_eq!(row.state, STATE_RUNNING);
        assert_eq!(row.failure_code, None);
        assert_eq!(row.completed_at, None);
        assert_eq!(
            destination(&first, "terminal-expiry-key")
                .await
                .owner_job_id
                .as_deref(),
            Some("terminal-expiry")
        );
        assert_eq!(
            import_job_target::Entity::find()
                .filter(import_job_target::Column::JobId.eq("terminal-expiry"))
                .count(&first)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn superseding_a_job_releases_destinations_but_retains_generation() {
        let db = setup().await;
        submit(&db, request("job", "key", None, Some("out/")), time(0))
            .await
            .unwrap();
        let transaction = db.begin().await.unwrap();
        lock_bucket_for_ownership(&transaction, "bucket")
            .await
            .unwrap();
        supersede_job_in_transaction(&transaction, "job", SupersedeReason::PutObject, time(1))
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(job(&db, "job").await.state, STATE_SUPERSEDED);
        let released = destination(&db, "key").await;
        assert_eq!((released.generation, released.owner_job_id), (1, None));
        assert!(
            import_job_target::Entity::find()
                .filter(import_job_target::Column::JobId.eq("job"))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[derive(Clone, Copy)]
    enum ExactRaceOrder {
        SubmitFirst,
        AdmissionFirst,
    }

    #[derive(Clone, Copy)]
    enum PrefixRaceOrder {
        ClaimFirst,
        AdmissionFirst,
    }

    #[derive(Clone, Copy)]
    enum EmptyPrefixRaceOrder {
        SubmitFirst,
        AdmissionFirst,
    }

    #[derive(Clone, Copy)]
    enum BucketDeleteRaceOrder {
        SubmitFirst,
        DeleteFirst,
    }

    #[tokio::test]
    async fn file_backed_sqlite_submit_first_then_exact_admission_is_fenced() {
        run_sqlite_exact_order(ExactRaceOrder::SubmitFirst).await;
    }

    #[tokio::test]
    async fn file_backed_sqlite_exact_admission_first_then_submit_is_fenced() {
        run_sqlite_exact_order(ExactRaceOrder::AdmissionFirst).await;
    }

    async fn run_sqlite_exact_order(order: ExactRaceOrder) {
        let (_directory, first, second) = setup_file_backed("submit-admit.sqlite").await;
        submit(&first, request("old", "key", None, None), time(0))
            .await
            .unwrap();
        let transaction = first.begin().await.unwrap();
        lock_bucket_for_ownership(&transaction, "bucket")
            .await
            .unwrap();
        let (started, started_at_lock) = tokio::sync::oneshot::channel();
        let competing = tokio::spawn(async move {
            let transaction = second.begin().await.unwrap();
            started.send(()).unwrap();
            let outcome = async {
                lock_bucket_for_ownership(&transaction, "bucket").await?;
                match order {
                    ExactRaceOrder::SubmitFirst => admit_content_mutation_in_transaction(
                        &transaction,
                        "bucket",
                        "key",
                        None,
                        SupersedeReason::PutObject,
                        time(1),
                    )
                    .await
                    .map(|_| ()),
                    ExactRaceOrder::AdmissionFirst => submit_in_transaction(
                        &transaction,
                        request("new", "key", None, None),
                        time(1),
                    )
                    .await
                    .map(|_| ()),
                }
            }
            .await;
            match outcome {
                Ok(()) => transaction.commit().await.unwrap(),
                Err(_) => transaction.rollback().await.unwrap(),
            }
            outcome
        });
        started_at_lock.await.unwrap();
        match order {
            ExactRaceOrder::SubmitFirst => {
                submit_in_transaction(&transaction, request("new", "key", None, None), time(1))
                    .await
                    .unwrap();
            }
            ExactRaceOrder::AdmissionFirst => {
                admit_content_mutation_in_transaction(
                    &transaction,
                    "bucket",
                    "key",
                    None,
                    SupersedeReason::PutObject,
                    time(1),
                )
                .await
                .unwrap();
            }
        }
        transaction.commit().await.unwrap();
        competing.await.unwrap().unwrap();

        let final_destination = destination(&first, "key").await;
        assert_eq!(job(&first, "old").await.state, STATE_SUPERSEDED);
        assert_eq!(final_destination.generation, 3);
        assert_no_job_targets(&first, "old").await;
        assert_eq!(prefix_claim_count(&first).await, 0);
        match order {
            ExactRaceOrder::SubmitFirst => {
                assert_eq!(job(&first, "new").await.state, STATE_SUPERSEDED);
                assert_eq!(final_destination.owner_job_id, None);
                assert_no_job_targets(&first, "new").await;
            }
            ExactRaceOrder::AdmissionFirst => {
                assert_eq!(job(&first, "new").await.state, STATE_QUEUED);
                assert_eq!(final_destination.owner_job_id.as_deref(), Some("new"));
                assert_target(&first, "new", "key", 3).await;
            }
        }
    }

    #[tokio::test]
    async fn file_backed_sqlite_extracted_claim_first_then_prefix_admission_is_fenced() {
        run_sqlite_prefix_order(PrefixRaceOrder::ClaimFirst).await;
    }

    #[tokio::test]
    async fn file_backed_sqlite_prefix_admission_first_rejects_claim_after_bucket_fence() {
        run_sqlite_prefix_order(PrefixRaceOrder::AdmissionFirst).await;
    }

    async fn run_sqlite_prefix_order(order: PrefixRaceOrder) {
        let (_directory, first, second) = setup_file_backed("prefix-claim.sqlite").await;
        submit(
            &first,
            request("job", "archive.zip", None, Some("out/")),
            time(0),
        )
        .await
        .unwrap();
        let claim = claim_one(&first, "worker", time(0)).await;
        let transaction = first.begin().await.unwrap();
        lock_bucket_for_ownership(&transaction, "bucket")
            .await
            .unwrap();
        let (started, started_at_lock) = tokio::sync::oneshot::channel();
        let competing_claim = claim.clone();
        let competing = tokio::spawn(async move {
            let transaction = second.begin().await.unwrap();
            started.send(()).unwrap();
            let outcome = async {
                lock_bucket_for_ownership(&transaction, "bucket").await?;
                match order {
                    PrefixRaceOrder::ClaimFirst => admit_prefix_mutation_in_transaction(
                        &transaction,
                        "bucket",
                        "out/",
                        time(1),
                    )
                    .await
                    .map(|_| None),
                    PrefixRaceOrder::AdmissionFirst => claim_extracted_target_after_lock(
                        &transaction,
                        &competing_claim,
                        "out/file",
                        time(1),
                    )
                    .await
                    .map(Some),
                }
            }
            .await;
            match outcome {
                Ok(_) => transaction.commit().await.unwrap(),
                Err(_) => transaction.rollback().await.unwrap(),
            }
            outcome
        });
        started_at_lock.await.unwrap();
        match order {
            PrefixRaceOrder::ClaimFirst => {
                assert_eq!(
                    claim_extracted_target_after_lock(&transaction, &claim, "out/file", time(1))
                        .await
                        .unwrap(),
                    1
                );
            }
            PrefixRaceOrder::AdmissionFirst => {
                admit_prefix_mutation_in_transaction(&transaction, "bucket", "out/", time(1))
                    .await
                    .unwrap();
            }
        }
        transaction.commit().await.unwrap();
        let competing = competing.await.unwrap();

        assert_eq!(job(&first, "job").await.state, STATE_SUPERSEDED);
        assert_no_prefix_claims(&first, "job").await;
        assert_no_job_targets(&first, "job").await;
        assert_eq!(prefix_claim_count(&first).await, 0);
        match order {
            PrefixRaceOrder::ClaimFirst => {
                assert_eq!(competing.unwrap(), None);
                let output = destination(&first, "out/file").await;
                assert_eq!((output.generation, output.owner_job_id), (2, None));
            }
            PrefixRaceOrder::AdmissionFirst => {
                assert!(matches!(competing, Err(AppError::StaleImportOwnership)));
                assert_no_destination(&first, "out/file").await;
            }
        }
    }

    #[tokio::test]
    async fn file_backed_sqlite_empty_prefix_submit_first_is_fenced() {
        run_sqlite_empty_prefix_order(EmptyPrefixRaceOrder::SubmitFirst).await;
    }

    #[tokio::test]
    async fn file_backed_sqlite_empty_prefix_admission_first_is_fenced() {
        run_sqlite_empty_prefix_order(EmptyPrefixRaceOrder::AdmissionFirst).await;
    }

    async fn run_sqlite_empty_prefix_order(order: EmptyPrefixRaceOrder) {
        let (_directory, first, second) = setup_file_backed("empty-prefix.sqlite").await;
        submit(
            &first,
            request("prefix-owner", "archive.zip", None, Some("")),
            time(0),
        )
        .await
        .unwrap();
        let transaction = first.begin().await.unwrap();
        lock_bucket_for_ownership(&transaction, "bucket")
            .await
            .unwrap();
        let (started, started_at_lock) = tokio::sync::oneshot::channel();
        let competing = tokio::spawn(async move {
            let transaction = second.begin().await.unwrap();
            started.send(()).unwrap();
            let outcome = async {
                lock_bucket_for_ownership(&transaction, "bucket").await?;
                match order {
                    EmptyPrefixRaceOrder::SubmitFirst => {
                        admit_prefix_mutation_in_transaction(&transaction, "bucket", "", time(1))
                            .await
                            .map(|_| ())
                    }
                    EmptyPrefixRaceOrder::AdmissionFirst => submit_in_transaction(
                        &transaction,
                        request("new", "whole-bucket/key", None, None),
                        time(1),
                    )
                    .await
                    .map(|_| ()),
                }
            }
            .await;
            match outcome {
                Ok(()) => transaction.commit().await.unwrap(),
                Err(_) => transaction.rollback().await.unwrap(),
            }
            outcome
        });
        started_at_lock.await.unwrap();
        match order {
            EmptyPrefixRaceOrder::SubmitFirst => {
                submit_in_transaction(
                    &transaction,
                    request("new", "whole-bucket/key", None, None),
                    time(1),
                )
                .await
                .unwrap();
            }
            EmptyPrefixRaceOrder::AdmissionFirst => {
                admit_prefix_mutation_in_transaction(&transaction, "bucket", "", time(1))
                    .await
                    .unwrap();
            }
        }
        transaction.commit().await.unwrap();
        competing.await.unwrap().unwrap();

        assert_eq!(job(&first, "prefix-owner").await.state, STATE_SUPERSEDED);
        assert_no_prefix_claims(&first, "prefix-owner").await;
        assert_no_job_targets(&first, "prefix-owner").await;
        assert_eq!(prefix_claim_count(&first).await, 0);
        let key = destination(&first, "whole-bucket/key").await;
        match order {
            EmptyPrefixRaceOrder::SubmitFirst => {
                assert_eq!(job(&first, "new").await.state, STATE_SUPERSEDED);
                assert_eq!((key.generation, key.owner_job_id), (2, None));
                assert_no_job_targets(&first, "new").await;
            }
            EmptyPrefixRaceOrder::AdmissionFirst => {
                assert_eq!(job(&first, "new").await.state, STATE_QUEUED);
                assert_eq!(
                    (key.generation, key.owner_job_id.as_deref()),
                    (1, Some("new"))
                );
                assert_target(&first, "new", "whole-bucket/key", 1).await;
            }
        }
    }

    #[tokio::test]
    async fn file_backed_sqlite_submit_first_then_bucket_delete_is_fenced() {
        run_sqlite_bucket_delete_order(BucketDeleteRaceOrder::SubmitFirst).await;
    }

    #[tokio::test]
    async fn file_backed_sqlite_bucket_delete_first_rejects_submit_after_bucket_fence() {
        run_sqlite_bucket_delete_order(BucketDeleteRaceOrder::DeleteFirst).await;
    }

    async fn run_sqlite_bucket_delete_order(order: BucketDeleteRaceOrder) {
        let (_directory, first, second) = setup_file_backed("delete-seam.sqlite").await;
        submit(&first, request("old", "old-key", None, None), time(0))
            .await
            .unwrap();
        let transaction = first.begin().await.unwrap();
        lock_bucket_for_ownership(&transaction, "bucket")
            .await
            .unwrap();
        let (started, started_at_lock) = tokio::sync::oneshot::channel();
        let competing = tokio::spawn(async move {
            let transaction = second.begin().await.unwrap();
            started.send(()).unwrap();
            let outcome = async {
                lock_bucket_for_ownership(&transaction, "bucket").await?;
                match order {
                    BucketDeleteRaceOrder::SubmitFirst => {
                        delete_bucket_after_lock(&transaction, time(1)).await
                    }
                    BucketDeleteRaceOrder::DeleteFirst => submit_in_transaction(
                        &transaction,
                        request("new", "new-key", None, None),
                        time(1),
                    )
                    .await
                    .map(|_| ()),
                }
            }
            .await;
            match outcome {
                Ok(()) => transaction.commit().await.unwrap(),
                Err(_) => transaction.rollback().await.unwrap(),
            }
            outcome
        });
        started_at_lock.await.unwrap();
        match order {
            BucketDeleteRaceOrder::SubmitFirst => {
                submit_in_transaction(&transaction, request("new", "new-key", None, None), time(1))
                    .await
                    .unwrap();
            }
            BucketDeleteRaceOrder::DeleteFirst => {
                delete_bucket_after_lock(&transaction, time(1))
                    .await
                    .unwrap();
            }
        }
        transaction.commit().await.unwrap();
        let competing = competing.await.unwrap();

        assert!(
            bucket::Entity::find_by_id("bucket".to_owned())
                .one(&first)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(job(&first, "old").await.state, STATE_SUPERSEDED);
        assert_no_job_targets(&first, "old").await;
        assert_eq!(prefix_claim_count(&first).await, 0);
        match order {
            BucketDeleteRaceOrder::SubmitFirst => {
                competing.unwrap();
                assert_eq!(job(&first, "new").await.state, STATE_SUPERSEDED);
                assert_no_job_targets(&first, "new").await;
            }
            BucketDeleteRaceOrder::DeleteFirst => {
                assert!(matches!(competing, Err(AppError::NoSuchBucket(name)) if name == "bucket"));
                assert!(
                    import_job::Entity::find_by_id("new".to_owned())
                        .one(&first)
                        .await
                        .unwrap()
                        .is_none()
                );
            }
        }
        assert!(
            import_destination::Entity::find()
                .one(&first)
                .await
                .unwrap()
                .is_none()
        );
        assert_no_job_targets(&first, "old").await;
    }

    async fn claim_extracted_target_after_lock<C: ConnectionTrait>(
        transaction: &C,
        claim: &ImportClaim,
        key: &str,
        now: DateTime<Utc>,
    ) -> AppResult<i64> {
        verify_active_claim(transaction, claim, "bucket").await?;
        let generation =
            claim_primary_destination(transaction, &claim.job_id, "bucket", key, now).await?;
        insert_target(
            transaction,
            &claim.job_id,
            "bucket",
            key,
            generation,
            KIND_EXTRACTED,
        )
        .await?;
        Ok(generation)
    }

    async fn delete_bucket_after_lock<C: ConnectionTrait>(
        transaction: &C,
        now: DateTime<Utc>,
    ) -> AppResult<()> {
        supersede_bucket(transaction, "bucket", now).await?;
        bucket::Entity::delete_by_id("bucket".to_owned())
            .exec(transaction)
            .await?;
        Ok(())
    }

    async fn assert_no_destination(db: &DatabaseConnection, key: &str) {
        assert!(
            import_destination::Entity::find_by_id(("bucket".to_owned(), key.to_owned()))
                .one(db)
                .await
                .unwrap()
                .is_none()
        );
    }

    async fn assert_target(db: &DatabaseConnection, job_id: &str, key: &str, generation: i64) {
        let target = import_job_target::Entity::find_by_id((
            job_id.to_owned(),
            "bucket".to_owned(),
            key.to_owned(),
        ))
        .one(db)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(target.expected_generation, generation);
    }

    async fn assert_no_job_targets(db: &DatabaseConnection, job_id: &str) {
        assert!(
            import_job_target::Entity::find()
                .filter(import_job_target::Column::JobId.eq(job_id))
                .one(db)
                .await
                .unwrap()
                .is_none()
        );
    }

    async fn assert_no_prefix_claims(db: &DatabaseConnection, job_id: &str) {
        assert!(
            import_prefix_claim::Entity::find()
                .filter(import_prefix_claim::Column::JobId.eq(job_id))
                .one(db)
                .await
                .unwrap()
                .is_none()
        );
    }

    async fn prefix_claim_count(db: &DatabaseConnection) -> u64 {
        import_prefix_claim::Entity::find()
            .filter(import_prefix_claim::Column::Bucket.eq("bucket"))
            .count(db)
            .await
            .unwrap()
    }
}
