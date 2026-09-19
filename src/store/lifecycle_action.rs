use chrono::{DateTime, Duration, SecondsFormat, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, TransactionError, TransactionTrait,
    sea_query::{Condition, Expr, LockBehavior, LockType, OnConflict, Query, SimpleExpr},
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    error::{AppError, AppResult},
    lifecycle::model::{
        ClaimedLifecycleAction, LifecycleActionKind, LifecycleTargetIdentity,
        MultipartUploadTargetIdentity, NewLifecycleAction, RuleIdentity, VersionTargetIdentity,
    },
    store::{
        database_clock::database_now,
        entities::{lifecycle_action, lifecycle_transition},
        import::ownership::{clear_lifecycle_mutation_if_owned, lock_bucket_for_ownership},
        object_version::VersionKind,
    },
};

pub const MAX_LIFECYCLE_ACTION_ATTEMPTS: i64 = 8;
pub const MAX_LIFECYCLE_ACTION_CLAIM_LIMIT: u64 = 1_000;
pub const MAX_LIFECYCLE_ACTION_LEASE_SECONDS: i64 = 86_400;
pub const FAILURE_DATABASE_CONTENTION: &str = "database_contention";
pub const FAILURE_ADMISSION_TEMPORARILY_UNAVAILABLE: &str = "admission_temporarily_unavailable";
pub const FAILURE_INTERNAL_DEPENDENCY: &str = "internal_dependency";
pub const FAILURE_CANCELLED_STALE: &str = "cancelled_stale";
pub const TRANSITION_SETTLEMENT_REQUIRED: &str = "transition_settlement_required";
const WAITING_FOR_TRANSITION: &str = "waiting_for_transition";
const WAITING_FOR_MUTATION: &str = "waiting_for_mutation";
const TRANSITION_RECHECK_SECONDS: i64 = 30;
pub const REDACTED_LIFECYCLE_ACTION_ERROR: &str = "lifecycle action failed";

const STATE_PENDING: &str = "pending";
const STATE_CLAIMED: &str = "claimed";
const STATE_SUCCEEDED: &str = "succeeded";
const STATE_CANCELLED: &str = "cancelled";
const STATE_FAILED_SAFE: &str = "failed_safe";
const MAX_SQLITE_ACTION_CLAIM_RETRIES: usize = 4;

#[derive(Serialize)]
struct CanonicalVersionActionIdempotency<'a> {
    bucket: &'a str,
    config_revision: i64,
    rule_identity: &'a str,
    action_kind: &'a str,
    target_version_row_id: &'a str,
    target_public_version_id: &'a str,
    target_object_id: Option<&'a str>,
    target_sequence: i64,
    due_at: String,
}

#[derive(Serialize)]
struct CanonicalMultipartActionIdempotency<'a> {
    bucket: &'a str,
    config_revision: i64,
    rule_identity: &'a str,
    action_kind: &'a str,
    target_upload_id: &'a str,
    target_upload_created_at: String,
    due_at: String,
}

struct ValidatedAction {
    idempotency_key: String,
    rule_id: String,
    action_kind: String,
}

/// Returns the durable, lowercase SHA-256 idempotency key for one exact lifecycle action.
pub fn idempotency_key(action: &NewLifecycleAction) -> AppResult<String> {
    Ok(validate_action(action)?.idempotency_key)
}

/// Returns the canonical serialized identity used to derive an action's durable idempotency key.
/// The version representation is the byte-for-byte Phase A representation.
pub(crate) fn canonical_action_bytes(action: &NewLifecycleAction) -> AppResult<Vec<u8>> {
    validate_action_identity(action)?;
    let rule_id = persisted_rule_identity(&action.rule_identity);
    let action_kind = persisted_action_kind(action.action_kind);
    match &action.target {
        LifecycleTargetIdentity::Version(target) => {
            let public_version_id = target.public_version_id.as_s3_str();
            let canonical = CanonicalVersionActionIdempotency {
                bucket: &action.bucket,
                config_revision: action.config_revision,
                rule_identity: &rule_id,
                action_kind,
                target_version_row_id: &target.version_row_id,
                target_public_version_id: public_version_id,
                target_object_id: target.object_id.as_deref(),
                target_sequence: target.sequence,
                due_at: action.due_at.to_rfc3339_opts(SecondsFormat::Nanos, true),
            };
            serde_json::to_vec(&canonical)
        }
        LifecycleTargetIdentity::MultipartUpload(target) => {
            let canonical = CanonicalMultipartActionIdempotency {
                bucket: &action.bucket,
                config_revision: action.config_revision,
                rule_identity: &rule_id,
                action_kind,
                target_upload_id: &target.upload_id,
                target_upload_created_at: target
                    .initiated_at
                    .to_rfc3339_opts(SecondsFormat::Nanos, true),
                due_at: action.due_at.to_rfc3339_opts(SecondsFormat::Nanos, true),
            };
            serde_json::to_vec(&canonical)
        }
    }
    .map_err(|_| {
        AppError::Internal("failed to serialize lifecycle action idempotency identity".to_owned())
    })
}

/// Inserts a new action exactly once. Replays conflict only on the idempotency key; every other
/// database constraint remains observable to the caller.
pub async fn insert_idempotent<C: ConnectionTrait>(
    db: &C,
    action: NewLifecycleAction,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let validated = validate_action(&action)?;
    if !action.idempotency_key.is_empty() && action.idempotency_key != validated.idempotency_key {
        return Err(AppError::InvalidArgument(
            "lifecycle action idempotency key does not match its canonical identity".to_owned(),
        ));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let (
        object_key,
        target_type,
        target_version_row_id,
        target_public_version_id,
        target_object_id,
        target_sequence,
        target_upload_id,
        target_upload_created_at,
    ) = match action.target {
        LifecycleTargetIdentity::Version(target) => (
            target.key,
            "version".to_owned(),
            Some(target.version_row_id),
            Some(target.public_version_id.as_s3_str().to_owned()),
            target.object_id,
            Some(target.sequence),
            None,
            None,
        ),
        LifecycleTargetIdentity::MultipartUpload(target) => (
            target.key,
            "multipart_upload".to_owned(),
            None,
            None,
            None,
            None,
            Some(target.upload_id),
            Some(target.initiated_at),
        ),
    };
    let inserted = lifecycle_action::Entity::insert(lifecycle_action::ActiveModel {
        id: Set(id),
        idempotency_key: Set(validated.idempotency_key),
        bucket: Set(action.bucket),
        object_key: Set(object_key),
        config_revision: Set(action.config_revision),
        rule_id: Set(validated.rule_id),
        action_kind: Set(validated.action_kind),
        target_type: Set(target_type),
        target_version_row_id: Set(target_version_row_id),
        target_public_version_id: Set(target_public_version_id),
        target_object_id: Set(target_object_id),
        target_sequence: Set(target_sequence),
        target_upload_id: Set(target_upload_id),
        target_upload_created_at: Set(target_upload_created_at),
        due_at: Set(action.due_at),
        state: Set(STATE_PENDING.to_owned()),
        attempts: Set(0),
        next_attempt_at: Set(now),
        claim_epoch: Set(0),
        lease_until: Set(None),
        claimed_by: Set(None),
        failure_class: Set(None),
        last_error_redacted: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
        finished_at: Set(None),
    })
    .on_conflict(
        OnConflict::column(lifecycle_action::Column::IdempotencyKey)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    Ok(inserted == 1)
}

/// Claims due work (or expired leases) in stable `(due_at, id)` order using a database-owned
/// clock. Reclaiming increments the epoch fence and normally increments the saturating attempt
/// counter; expiration dependency probes retain their refunded attempt.
pub async fn claim_due(
    db: &DatabaseConnection,
    worker_id: &str,
    lease_for: Duration,
    limit: u64,
) -> AppResult<Vec<ClaimedLifecycleAction>> {
    claim_due_with_max_attempts(
        db,
        worker_id,
        lease_for,
        MAX_LIFECYCLE_ACTION_ATTEMPTS,
        limit,
    )
    .await
}

/// Claims due work using the validated worker-specific attempt cap. Generic expired claims at the
/// cap receive one final recovery claim. Transition work also receives a final E2 settlement claim;
/// an outstanding saga can reclaim that settlement claim after a crash, and a published saga
/// remains claimable until cleanup succeeds.
pub async fn claim_due_with_max_attempts(
    db: &DatabaseConnection,
    worker_id: &str,
    lease_for: Duration,
    max_attempts: i64,
    limit: u64,
) -> AppResult<Vec<ClaimedLifecycleAction>> {
    validate_claim_request(worker_id, lease_for, limit)?;
    if !(1..=MAX_LIFECYCLE_ACTION_ATTEMPTS).contains(&max_attempts) {
        return Err(AppError::InvalidArgument(format!(
            "lifecycle max attempts must be in 1..={MAX_LIFECYCLE_ACTION_ATTEMPTS}"
        )));
    }
    let worker_id = worker_id.to_owned();

    for attempt in 0..=MAX_SQLITE_ACTION_CLAIM_RETRIES {
        let worker_id = worker_id.clone();
        let result = db
            .transaction(move |txn| {
                Box::pin(async move {
                    claim_due_in_transaction(txn, &worker_id, lease_for, max_attempts, limit).await
                })
            })
            .await;
        match result {
            Ok(claims) => return Ok(claims),
            Err(error)
                if db.get_database_backend() == DatabaseBackend::Sqlite
                    && is_sqlite_contention(&error.to_string())
                    && attempt < MAX_SQLITE_ACTION_CLAIM_RETRIES =>
            {
                sqlite_claim_retry_delay(attempt).await;
            }
            Err(error) => return Err(normalize_transaction_error(error)),
        }
    }
    unreachable!("SQLite lifecycle action claim retry loop always returns or errors")
}

/// Locks the bucket before the current active action row on PostgreSQL (SQLite
/// acquires write intent). This is the shared execution/renewal lock hierarchy.
/// Callers use this immediately before execution to reject a reclaimed or expired worker token.
pub async fn lock_claim_for_execution<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleAction,
) -> AppResult<Option<lifecycle_action::Model>> {
    // Bucket -> action -> destination/lease, including renewal and cleanup.
    // Bucket deletion cascades action rows, so action -> bucket would deadlock
    // against deletion (and any bucket-owned configuration transaction).
    match lock_bucket_for_ownership(db, &claim.action.bucket).await {
        Ok(()) => {}
        Err(AppError::NoSuchBucket(_)) => return Ok(None),
        Err(error) => return Err(error),
    }
    let query = lifecycle_action::Entity::find().filter(active_claim_condition(claim));
    let action = if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    };
    let Some(action) = action else {
        return Ok(None);
    };
    // Read the clock after acquiring the row lock so time spent waiting cannot
    // authorize execution with a lease that expired while the lock was held elsewhere.
    let now = database_now(db).await?;
    Ok(action
        .lease_until
        .filter(|until| *until > now)
        .map(|_| action))
}

/// Extends an active claim from the database clock. The action identity, worker,
/// and epoch are a compare-and-set fence, and an already expired lease cannot be
/// revived by renewal.
pub async fn renew_claim<C>(
    db: &C,
    claim: &ClaimedLifecycleAction,
    lease_for: Duration,
) -> AppResult<bool>
where
    C: ConnectionTrait + TransactionTrait,
{
    validate_lease_duration(lease_for)?;
    let claim = claim.clone();
    let result = db
        .transaction(move |txn| {
            let claim = claim.clone();
            Box::pin(async move { renew_claim_in_transaction(txn, &claim, lease_for).await })
        })
        .await;
    result.map_err(normalize_transaction_error)
}

async fn renew_claim_in_transaction<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleAction,
    lease_for: Duration,
) -> AppResult<bool> {
    if lock_claim_for_execution(db, claim).await?.is_none() {
        return Ok(false);
    }
    #[cfg(test)]
    test_hooks::pause_after_renewal_claim_lock(&claim.action.id).await;
    // This sample must remain after the row lock. Sampling before a contended
    // PostgreSQL lock can authorize renewal using time at which the lease was
    // valid even though it expired while the UPDATE waited.
    let now = database_now(db).await?;
    let lease_until = now.checked_add_signed(lease_for).ok_or_else(|| {
        AppError::InvalidArgument(
            "lifecycle action lease is outside the database timestamp range".to_owned(),
        )
    })?;
    let updated = lifecycle_action::Entity::update_many()
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Some(lease_until)),
        )
        .col_expr(lifecycle_action::Column::UpdatedAt, Expr::value(now))
        .filter(active_claim_condition(claim))
        .filter(lifecycle_action::Column::LeaseUntil.gt(now))
        .exec(db)
        .await?;
    if updated.rows_affected == 1 && claim.action.target_type == "version" {
        crate::store::import::ownership::renew_lifecycle_mutation_in_transaction(
            db,
            &claim.action.bucket,
            &claim.action.object_key,
            &claim.action.id,
            claim.claim_epoch,
        )
        .await?;
    }
    Ok(updated.rows_affected == 1)
}

async fn acquire_sqlite_action_write_intent<C: ConnectionTrait>(
    db: &C,
    action_id: &str,
) -> AppResult<()> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        return Ok(());
    }
    lifecycle_action::Entity::update_many()
        .col_expr(
            lifecycle_action::Column::UpdatedAt,
            Expr::col(lifecycle_action::Column::UpdatedAt).into(),
        )
        .filter(lifecycle_action::Column::Id.eq(action_id))
        .exec(db)
        .await?;
    Ok(())
}

pub async fn mark_succeeded<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleAction,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    before_terminal_store_write(&claim.action.id, STATE_SUCCEEDED).await?;
    let updated = lifecycle_action::Entity::update_many()
        .col_expr(
            lifecycle_action::Column::State,
            Expr::value(STATE_SUCCEEDED),
        )
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            lifecycle_action::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            lifecycle_action::Column::FailureClass,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            lifecycle_action::Column::LastErrorRedacted,
            Expr::value(Option::<String>::None),
        )
        .col_expr(lifecycle_action::Column::UpdatedAt, Expr::value(now))
        .col_expr(lifecycle_action::Column::FinishedAt, Expr::value(Some(now)))
        .filter(active_claim_condition(claim))
        .exec(db)
        .await?;
    Ok(updated.rows_affected == 1)
}

/// Computes the documented deterministic retry delay without exposing database
/// errors or allowing an unbounded shift.
pub fn retry_delay_secs(attempts: i64, base_backoff_secs: u64, max_backoff_secs: u64) -> u64 {
    let attempt_index = u32::try_from((attempts - 1).max(0)).unwrap_or(62).min(62);
    max_backoff_secs
        .min(base_backoff_secs.saturating_mul(1_u64.checked_shl(attempt_index).unwrap_or(u64::MAX)))
}

/// Adds the deterministic retry delay to a database-owned timestamp.
pub fn retry_at(
    now: DateTime<Utc>,
    attempts: i64,
    base_backoff_secs: u64,
    max_backoff_secs: u64,
) -> AppResult<DateTime<Utc>> {
    let delay = i64::try_from(retry_delay_secs(
        attempts,
        base_backoff_secs,
        max_backoff_secs,
    ))
    .map_err(|_| AppError::InvalidArgument("lifecycle retry delay is out of range".to_owned()))?;
    now.checked_add_signed(Duration::seconds(delay))
        .ok_or_else(|| {
            AppError::InvalidArgument(
                "lifecycle retry time is outside the database range".to_owned(),
            )
        })
}

pub async fn mark_cancelled<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleAction,
    now: DateTime<Utc>,
    failure_class: &str,
) -> AppResult<bool> {
    terminal_failure_update(db, claim, now, STATE_CANCELLED, failure_class).await
}

pub async fn mark_failed_safe<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleAction,
    now: DateTime<Utc>,
    failure_class: &str,
) -> AppResult<bool> {
    terminal_failure_update(db, claim, now, STATE_FAILED_SAFE, failure_class).await
}

pub(crate) fn waiting_for_transition(action: &lifecycle_action::Model) -> bool {
    action.action_kind == "expire_current"
        && action.failure_class.as_deref() == Some(WAITING_FOR_TRANSITION)
}

pub(crate) fn waiting_for_dependency(action: &lifecycle_action::Model) -> bool {
    waiting_for_transition(action)
        || (action.target_type == "version"
            && action.failure_class.as_deref() == Some(WAITING_FOR_MUTATION))
}

fn consumed_attempts(action: &lifecycle_action::Model) -> i64 {
    action
        .attempts
        .saturating_sub(i64::from(!waiting_for_dependency(action)))
}

/// Park the same durable action for an ownership dependency. Return false only
/// if its ordinary failure budget was already spent; callers retain their
/// existing terminal/saga-settlement path in that case.
pub(crate) async fn wait_for_mutation_dependency(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    max_attempts: i64,
) -> AppResult<bool> {
    let claim = claim.clone();
    db.transaction(move |txn| {
        Box::pin(async move {
            let Some(action) = lock_claim_for_execution(txn, &claim).await? else {
                return Ok(true);
            };
            if consumed_attempts(&action) >= max_attempts {
                return Ok(false);
            }
            park_dependency_in_transaction(txn, &claim, &action, WAITING_FOR_MUTATION).await?;
            Ok(true)
        })
    })
    .await
    .map_err(normalize_transaction_error)
}

/// Keep the same durable identity while transition temporarily wins. This is a
/// dependency, not an execution failure. Rechecks (including crashed/reclaimed
/// rechecks) do not consume the ordinary failure budget. The caller must prove
/// this expiration is otherwise still the winner and settle its ownership guard
/// in this same transaction.
pub(crate) async fn wait_for_transition_in_transaction(
    txn: &sea_orm::DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
) -> AppResult<bool> {
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(false);
    };
    if action.action_kind != "expire_current" {
        return Err(AppError::Internal(
            "invalid transition dependency action".into(),
        ));
    }
    park_dependency_in_transaction(txn, claim, &action, WAITING_FOR_TRANSITION).await
}

async fn park_dependency_in_transaction(
    txn: &sea_orm::DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
    action: &lifecycle_action::Model,
    reason: &str,
) -> AppResult<bool> {
    let now = database_now(txn).await?;
    let attempts = consumed_attempts(action);
    let updated = lifecycle_action::Entity::update_many()
        .col_expr(lifecycle_action::Column::State, Expr::value(STATE_PENDING))
        .col_expr(lifecycle_action::Column::Attempts, Expr::value(attempts))
        .col_expr(
            lifecycle_action::Column::NextAttemptAt,
            Expr::value(now + Duration::seconds(TRANSITION_RECHECK_SECONDS)),
        )
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            lifecycle_action::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            lifecycle_action::Column::FailureClass,
            Expr::value(Some(reason.to_owned())),
        )
        .col_expr(
            lifecycle_action::Column::LastErrorRedacted,
            Expr::value(Option::<String>::None),
        )
        .col_expr(lifecycle_action::Column::UpdatedAt, Expr::value(now))
        .filter(active_claim_condition(claim))
        .filter(lifecycle_action::Column::LeaseUntil.gt(now))
        .exec(txn)
        .await?;
    Ok(updated.rows_affected == 1)
}

/// Returns a transition claim to the queue while its hot source is awaiting
/// verification. The claim-time attempt is refunded without entering the
/// expiration-specific `waiting_for_transition` mode.
pub(crate) async fn wait_for_hot_verification_in_transaction(
    txn: &sea_orm::DatabaseTransaction,
    claim: &ClaimedLifecycleAction,
) -> AppResult<bool> {
    let Some(action) = lock_claim_for_execution(txn, claim).await? else {
        return Ok(false);
    };
    if !is_transition_action(&action) {
        return Err(AppError::Internal(
            "invalid hot verification dependency action".into(),
        ));
    }
    let now = database_now(txn).await?;
    let next_attempt_at = now
        .checked_add_signed(Duration::seconds(TRANSITION_RECHECK_SECONDS))
        .ok_or_else(|| {
            AppError::Internal(
                "lifecycle transition recheck time is outside the database range".into(),
            )
        })?;
    let updated = lifecycle_action::Entity::update_many()
        .col_expr(lifecycle_action::Column::State, Expr::value(STATE_PENDING))
        .col_expr(
            lifecycle_action::Column::Attempts,
            Expr::value(consumed_attempts(&action)),
        )
        .col_expr(
            lifecycle_action::Column::NextAttemptAt,
            Expr::value(next_attempt_at),
        )
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            lifecycle_action::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            lifecycle_action::Column::FailureClass,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            lifecycle_action::Column::LastErrorRedacted,
            Expr::value(Option::<String>::None),
        )
        .col_expr(lifecycle_action::Column::UpdatedAt, Expr::value(now))
        .filter(active_claim_condition(claim))
        .filter(lifecycle_action::Column::LeaseUntil.gt(now))
        .exec(txn)
        .await?;
    Ok(updated.rows_affected == 1)
}

pub async fn schedule_retry<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleAction,
    now: DateTime<Utc>,
    next_attempt_at: DateTime<Utc>,
    failure_class: &str,
) -> AppResult<bool> {
    validate_failure_class(failure_class)?;
    if next_attempt_at <= now {
        return Err(AppError::InvalidArgument(
            "lifecycle retry must be scheduled after the database time".to_owned(),
        ));
    }
    let updated = lifecycle_action::Entity::update_many()
        .col_expr(lifecycle_action::Column::State, Expr::value(STATE_PENDING))
        // A dependency probe is not charged at claim time. An actual execution
        // failure leaves dependency mode and uses the ordinary bounded budget.
        .col_expr(
            lifecycle_action::Column::Attempts,
            Expr::value(claim.action.attempts + i64::from(waiting_for_dependency(&claim.action))),
        )
        .col_expr(
            lifecycle_action::Column::NextAttemptAt,
            Expr::value(next_attempt_at),
        )
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            lifecycle_action::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            lifecycle_action::Column::FailureClass,
            Expr::value(Some(failure_class.to_owned())),
        )
        .col_expr(
            lifecycle_action::Column::LastErrorRedacted,
            Expr::value(Some(REDACTED_LIFECYCLE_ACTION_ERROR.to_owned())),
        )
        .col_expr(lifecycle_action::Column::UpdatedAt, Expr::value(now))
        .col_expr(
            lifecycle_action::Column::FinishedAt,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .filter(active_claim_condition(claim))
        .exec(db)
        .await?;
    Ok(updated.rows_affected == 1)
}

async fn claim_due_in_transaction<C: ConnectionTrait>(
    db: &C,
    worker_id: &str,
    lease_for: Duration,
    max_attempts: i64,
    limit: u64,
) -> AppResult<Vec<ClaimedLifecycleAction>> {
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        // SQLite has one writer, not row locks. Obtain write intent before
        // candidate discovery so two deferred read snapshots cannot deadlock
        // while both attempt to upgrade to the bucket write fence.
        db.execute_unprepared("UPDATE lifecycle_actions SET claim_epoch = claim_epoch WHERE 0")
            .await?;
    }
    let candidates = due_candidates(db, database_now(db).await?, limit).await?;
    let mut locked_buckets = std::collections::BTreeSet::new();
    for bucket in candidates
        .iter()
        .map(|candidate| candidate.bucket.clone())
        .collect::<std::collections::BTreeSet<_>>()
    {
        match lock_bucket_for_ownership(db, &bucket).await {
            Ok(()) => {
                locked_buckets.insert(bucket);
            }
            Err(AppError::NoSuchBucket(_)) => {}
            Err(error) => return Err(error),
        }
    }
    // Candidate discovery is not authorization. After all bucket locks are held,
    // re-read due rows and sample the engine clock before installing a lease.
    let now = database_now(db).await?;
    let lease_until = now.checked_add_signed(lease_for).ok_or_else(|| {
        AppError::InvalidArgument(
            "lifecycle action lease is outside the database timestamp range".to_owned(),
        )
    })?;
    let mut claimed = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if !locked_buckets.contains(&candidate.bucket) {
            continue;
        }
        let query =
            lifecycle_action::Entity::find_by_id(candidate.id).filter(due_claim_condition(now));
        let candidate = if db.get_database_backend() == DatabaseBackend::Postgres {
            query
                .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
                .one(db)
                .await?
        } else {
            query.one(db).await?
        };
        let Some(candidate) = candidate else {
            continue;
        };
        let transition = is_transition_action(&candidate);
        let transition_saga = if transition {
            transition_saga_state(db, &candidate.id).await?
        } else {
            TransitionSagaState::None
        };
        let published_cleanup = transition_saga == TransitionSagaState::Published;
        // Once the charged final recovery claim has been issued, reclaims exist
        // only so E2 can atomically settle the saga/guard. They remain eligible
        // past the ordinary execution cap, with the counter saturating safely.
        let final_saga_settlement = transition_saga == TransitionSagaState::Outstanding
            && candidate.attempts > max_attempts;
        if candidate.attempts >= max_attempts {
            // Free dependency probes preserve prior ordinary attempts, not an
            // exemption from the current worker's cap. Unlike an ordinary
            // recovery claim, a probe does not increment attempts, so granting
            // this exception would allow unlimited recovery claims at the cap.
            let one_recovery_claim =
                !waiting_for_dependency(&candidate) && candidate.attempts == max_attempts;
            let one_recovery_claim =
                one_recovery_claim && (candidate.state == STATE_CLAIMED || transition);
            if !one_recovery_claim && !published_cleanup && !final_saga_settlement {
                fail_safe_exhausted(db, &candidate, now).await?;
                continue;
            }
        }
        if let Some(claim) = claim_candidate(db, candidate, worker_id, now, lease_until).await? {
            claimed.push(claim);
        }
    }
    Ok(claimed)
}

async fn due_candidates<C: ConnectionTrait>(
    db: &C,
    now: DateTime<Utc>,
    limit: u64,
) -> AppResult<Vec<lifecycle_action::Model>> {
    let query = lifecycle_action::Entity::find()
        .filter(due_claim_condition(now))
        .order_by_asc(lifecycle_action::Column::DueAt)
        .order_by_asc(lifecycle_action::Column::Id)
        .limit(limit);
    Ok(query.all(db).await?)
}

async fn claim_candidate<C: ConnectionTrait>(
    db: &C,
    candidate: lifecycle_action::Model,
    worker_id: &str,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
) -> AppResult<Option<ClaimedLifecycleAction>> {
    acquire_sqlite_action_write_intent(db, &candidate.id).await?;
    let settlement_recovery = candidate.state == STATE_FAILED_SAFE
        || candidate.last_error_redacted.as_deref() == Some(TRANSITION_SETTLEMENT_REQUIRED);
    let attempts = candidate
        .attempts
        .saturating_add(i64::from(!waiting_for_dependency(&candidate)));
    let claim_epoch = candidate
        .claim_epoch
        .checked_add(1)
        .ok_or_else(|| AppError::Database("lifecycle action claim epoch overflow".to_owned()))?;
    let mut update = lifecycle_action::Entity::update_many()
        .col_expr(lifecycle_action::Column::State, Expr::value(STATE_CLAIMED))
        .col_expr(lifecycle_action::Column::Attempts, Expr::value(attempts))
        .col_expr(
            lifecycle_action::Column::ClaimEpoch,
            Expr::value(claim_epoch),
        )
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Some(lease_until)),
        )
        .col_expr(
            lifecycle_action::Column::ClaimedBy,
            Expr::value(Some(worker_id.to_owned())),
        )
        .col_expr(
            lifecycle_action::Column::FinishedAt,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(lifecycle_action::Column::UpdatedAt, Expr::value(now))
        .filter(lifecycle_action::Column::Id.eq(candidate.id.clone()))
        .filter(lifecycle_action::Column::Attempts.eq(candidate.attempts))
        .filter(lifecycle_action::Column::ClaimEpoch.eq(candidate.claim_epoch))
        .filter(due_claim_condition(now));
    if settlement_recovery {
        update = update.col_expr(
            lifecycle_action::Column::LastErrorRedacted,
            Expr::value(Some(TRANSITION_SETTLEMENT_REQUIRED.to_owned())),
        );
    }
    let updated = update.exec(db).await?;
    if updated.rows_affected != 1 {
        return Ok(None);
    }
    let action = lifecycle_action::Entity::find_by_id(candidate.id)
        .one(db)
        .await?
        .ok_or_else(|| AppError::Internal("claimed lifecycle action disappeared".to_owned()))?;
    Ok(Some(ClaimedLifecycleAction {
        claim_epoch: action.claim_epoch,
        worker_id: worker_id.to_owned(),
        action,
    }))
}

async fn fail_safe_exhausted<C: ConnectionTrait>(
    db: &C,
    candidate: &lifecycle_action::Model,
    now: DateTime<Utc>,
) -> AppResult<()> {
    if candidate.target_type == "version" {
        lock_bucket_for_ownership(db, &candidate.bucket).await?;
        if candidate.claim_epoch > 0 {
            clear_lifecycle_mutation_if_owned(
                db,
                &candidate.bucket,
                &candidate.object_key,
                &candidate.id,
                candidate.claim_epoch,
                now,
            )
            .await?;
        }
    }
    let failure_class = candidate
        .failure_class
        .as_deref()
        .filter(|class| is_allowed_failure_class(class))
        .unwrap_or(FAILURE_INTERNAL_DEPENDENCY);
    lifecycle_action::Entity::update_many()
        .col_expr(
            lifecycle_action::Column::State,
            Expr::value(STATE_FAILED_SAFE),
        )
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            lifecycle_action::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            lifecycle_action::Column::FailureClass,
            Expr::value(Some(failure_class.to_owned())),
        )
        .col_expr(
            lifecycle_action::Column::LastErrorRedacted,
            Expr::value(Some(REDACTED_LIFECYCLE_ACTION_ERROR.to_owned())),
        )
        .col_expr(lifecycle_action::Column::UpdatedAt, Expr::value(now))
        .col_expr(lifecycle_action::Column::FinishedAt, Expr::value(Some(now)))
        .filter(lifecycle_action::Column::Id.eq(candidate.id.clone()))
        .filter(lifecycle_action::Column::Attempts.eq(candidate.attempts))
        .filter(lifecycle_action::Column::ClaimEpoch.eq(candidate.claim_epoch))
        .filter(due_claim_condition(now))
        .exec(db)
        .await?;
    Ok(())
}

async fn terminal_failure_update<C: ConnectionTrait>(
    db: &C,
    claim: &ClaimedLifecycleAction,
    now: DateTime<Utc>,
    terminal_state: &str,
    failure_class: &str,
) -> AppResult<bool> {
    validate_failure_class(failure_class)?;
    let updated = lifecycle_action::Entity::update_many()
        .col_expr(lifecycle_action::Column::State, Expr::value(terminal_state))
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            lifecycle_action::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            lifecycle_action::Column::FailureClass,
            Expr::value(Some(failure_class.to_owned())),
        )
        .col_expr(
            lifecycle_action::Column::LastErrorRedacted,
            Expr::value(Some(REDACTED_LIFECYCLE_ACTION_ERROR.to_owned())),
        )
        .col_expr(lifecycle_action::Column::UpdatedAt, Expr::value(now))
        .col_expr(lifecycle_action::Column::FinishedAt, Expr::value(Some(now)))
        .filter(active_claim_condition(claim))
        .exec(db)
        .await?;
    Ok(updated.rows_affected == 1)
}

fn validate_action(action: &NewLifecycleAction) -> AppResult<ValidatedAction> {
    validate_action_identity(action)?;
    let rule_id = persisted_rule_identity(&action.rule_identity);
    let action_kind = persisted_action_kind(action.action_kind).to_owned();
    let bytes = canonical_action_bytes(action)?;
    Ok(ValidatedAction {
        idempotency_key: hex::encode(Sha256::digest(bytes)),
        rule_id,
        action_kind,
    })
}

fn validate_action_identity(action: &NewLifecycleAction) -> AppResult<()> {
    if action.bucket.is_empty() || action.config_revision <= 0 {
        return Err(AppError::InvalidArgument(
            "invalid lifecycle action target identity".to_owned(),
        ));
    }
    match (&action.target, action.action_kind) {
        (
            LifecycleTargetIdentity::Version(target),
            LifecycleActionKind::ExpireCurrent
            | LifecycleActionKind::ExpireNoncurrent
            | LifecycleActionKind::TransitionCurrent
            | LifecycleActionKind::TransitionNoncurrent
            | LifecycleActionKind::DeleteExpiredMarker,
        ) => {
            if target.bucket != action.bucket
                || target.key.is_empty()
                || target.version_row_id.is_empty()
                || target.sequence < 0
            {
                return Err(AppError::InvalidArgument(
                    "invalid lifecycle action target identity".to_owned(),
                ));
            }
            validate_target(target)?;
            if matches!(
                action.action_kind,
                LifecycleActionKind::TransitionCurrent | LifecycleActionKind::TransitionNoncurrent
            ) && target.kind != VersionKind::Object
            {
                return Err(AppError::InvalidArgument(
                    "lifecycle transition target must be a content version".to_owned(),
                ));
            }
            if target.public_version_id.as_s3_str().is_empty() {
                return Err(AppError::InvalidArgument(
                    "lifecycle action public version ID must not be empty".to_owned(),
                ));
            }
            Ok(())
        }
        (
            LifecycleTargetIdentity::MultipartUpload(target),
            LifecycleActionKind::AbortIncompleteMultipartUpload,
        ) if target.bucket == action.bucket
            && !target.key.is_empty()
            && !target.upload_id.is_empty() =>
        {
            Ok(())
        }
        _ => Err(AppError::InvalidArgument(
            "invalid lifecycle action target identity".to_owned(),
        )),
    }
}

/// Decodes and validates the exact persisted target shape without trusting database constraints.
pub(crate) fn target_from_action(
    action: &lifecycle_action::Model,
) -> AppResult<LifecycleTargetIdentity> {
    if action.bucket.is_empty() || action.object_key.is_empty() || action.config_revision <= 0 {
        return Err(invalid_persisted_action_identity());
    }
    let action_kind = action_kind_from_db(&action.action_kind)?;
    match action.target_type.as_str() {
        "version" => {
            if action_kind == LifecycleActionKind::AbortIncompleteMultipartUpload
                || action.target_upload_id.is_some()
                || action.target_upload_created_at.is_some()
            {
                return Err(invalid_persisted_action_identity());
            }
            let version_row_id = action
                .target_version_row_id
                .clone()
                .filter(|value| !value.is_empty())
                .ok_or_else(invalid_persisted_action_identity)?;
            let public_version_id = action
                .target_public_version_id
                .as_deref()
                .filter(|value| !value.is_empty())
                .ok_or_else(invalid_persisted_action_identity)?;
            let sequence = action
                .target_sequence
                .filter(|sequence| *sequence >= 0)
                .ok_or_else(invalid_persisted_action_identity)?;
            let kind = match action_kind {
                LifecycleActionKind::ExpireCurrent => VersionKind::Object,
                LifecycleActionKind::TransitionCurrent
                | LifecycleActionKind::TransitionNoncurrent => VersionKind::Object,
                LifecycleActionKind::DeleteExpiredMarker => VersionKind::DeleteMarker,
                LifecycleActionKind::ExpireNoncurrent => {
                    if action.target_object_id.is_some() {
                        VersionKind::Object
                    } else {
                        VersionKind::DeleteMarker
                    }
                }
                LifecycleActionKind::AbortIncompleteMultipartUpload => {
                    return Err(invalid_persisted_action_identity());
                }
            };
            let target = VersionTargetIdentity {
                bucket: action.bucket.clone(),
                key: action.object_key.clone(),
                version_row_id,
                public_version_id: crate::store::object_version::PublicVersionId::parse_s3(
                    public_version_id,
                )
                .map_err(|_| invalid_persisted_action_identity())?,
                kind,
                object_id: action.target_object_id.clone(),
                sequence,
            };
            if matches!(target.kind, VersionKind::Object) != target.object_id.is_some() {
                return Err(invalid_persisted_action_identity());
            }
            Ok(LifecycleTargetIdentity::Version(target))
        }
        "multipart_upload" => {
            if action_kind != LifecycleActionKind::AbortIncompleteMultipartUpload
                || action.target_version_row_id.is_some()
                || action.target_public_version_id.is_some()
                || action.target_object_id.is_some()
                || action.target_sequence.is_some()
            {
                return Err(invalid_persisted_action_identity());
            }
            let upload_id = action
                .target_upload_id
                .clone()
                .filter(|value| !value.is_empty())
                .ok_or_else(invalid_persisted_action_identity)?;
            let initiated_at = action
                .target_upload_created_at
                .ok_or_else(invalid_persisted_action_identity)?;
            Ok(LifecycleTargetIdentity::MultipartUpload(
                MultipartUploadTargetIdentity {
                    bucket: action.bucket.clone(),
                    key: action.object_key.clone(),
                    upload_id,
                    initiated_at,
                },
            ))
        }
        _ => Err(invalid_persisted_action_identity()),
    }
}

fn invalid_persisted_action_identity() -> AppError {
    AppError::Internal("invalid lifecycle action identity".to_owned())
}

pub(crate) fn action_kind_from_db(value: &str) -> AppResult<LifecycleActionKind> {
    match value {
        "expire_current" => Ok(LifecycleActionKind::ExpireCurrent),
        "expire_noncurrent" => Ok(LifecycleActionKind::ExpireNoncurrent),
        "transition_current" => Ok(LifecycleActionKind::TransitionCurrent),
        "transition_noncurrent" => Ok(LifecycleActionKind::TransitionNoncurrent),
        "delete_expired_marker" => Ok(LifecycleActionKind::DeleteExpiredMarker),
        "abort_incomplete_multipart_upload" => {
            Ok(LifecycleActionKind::AbortIncompleteMultipartUpload)
        }
        _ => Err(AppError::Internal(
            "invalid lifecycle action kind".to_owned(),
        )),
    }
}

fn validate_target(target: &VersionTargetIdentity) -> AppResult<()> {
    match (target.kind, target.object_id.as_deref()) {
        (VersionKind::Object, Some(object_id)) if !object_id.is_empty() => Ok(()),
        (VersionKind::DeleteMarker, None) => Ok(()),
        _ => Err(AppError::InvalidArgument(
            "lifecycle action target kind and object ID are inconsistent".to_owned(),
        )),
    }
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
        LifecycleActionKind::TransitionCurrent => "transition_current",
        LifecycleActionKind::TransitionNoncurrent => "transition_noncurrent",
        LifecycleActionKind::DeleteExpiredMarker => "delete_expired_marker",
        LifecycleActionKind::AbortIncompleteMultipartUpload => "abort_incomplete_multipart_upload",
    }
}

fn validate_claim_request(worker_id: &str, lease_for: Duration, limit: u64) -> AppResult<()> {
    if worker_id.trim().is_empty() {
        return Err(AppError::InvalidArgument(
            "lifecycle worker ID must not be empty".to_owned(),
        ));
    }
    validate_lease_duration(lease_for)?;
    if limit == 0 || limit > MAX_LIFECYCLE_ACTION_CLAIM_LIMIT {
        return Err(AppError::InvalidArgument(format!(
            "lifecycle action claim limit must be between 1 and {MAX_LIFECYCLE_ACTION_CLAIM_LIMIT}"
        )));
    }
    Ok(())
}

fn validate_lease_duration(lease_for: Duration) -> AppResult<()> {
    if lease_for <= Duration::zero()
        || lease_for > Duration::seconds(MAX_LIFECYCLE_ACTION_LEASE_SECONDS)
    {
        return Err(AppError::InvalidArgument(format!(
            "lifecycle action lease must be between 1 second and {MAX_LIFECYCLE_ACTION_LEASE_SECONDS} seconds"
        )));
    }
    Ok(())
}

fn due_claim_condition(now: DateTime<Utc>) -> Condition {
    Condition::any()
        .add(
            Condition::all()
                .add(lifecycle_action::Column::State.eq(STATE_PENDING))
                .add(lifecycle_action::Column::NextAttemptAt.lte(now))
                .add(lifecycle_action::Column::DueAt.lte(now)),
        )
        .add(
            Condition::all()
                .add(lifecycle_action::Column::State.eq(STATE_CLAIMED))
                .add(lifecycle_action::Column::LeaseUntil.lte(now))
                .add(
                    Condition::any()
                        .add(lifecycle_action::Column::LastErrorRedacted.is_null())
                        .add(
                            lifecycle_action::Column::LastErrorRedacted
                                .ne(TRANSITION_SETTLEMENT_REQUIRED),
                        )
                        .add(unfinished_transition_saga_exists()),
                ),
        )
        .add(
            Condition::all()
                .add(lifecycle_action::Column::State.eq(STATE_FAILED_SAFE))
                .add(
                    Condition::any()
                        .add(lifecycle_action::Column::ActionKind.eq("transition_current"))
                        .add(lifecycle_action::Column::ActionKind.eq("transition_noncurrent")),
                )
                .add(unfinished_transition_saga_exists()),
        )
}

fn unfinished_transition_saga_exists() -> SimpleExpr {
    Expr::exists(
        Query::select()
            .column(lifecycle_transition::Column::Id)
            .from(lifecycle_transition::Entity)
            .and_where(
                Expr::col((
                    lifecycle_transition::Entity,
                    lifecycle_transition::Column::ActionId,
                ))
                .equals((lifecycle_action::Entity, lifecycle_action::Column::Id)),
            )
            .and_where(
                Expr::col((
                    lifecycle_transition::Entity,
                    lifecycle_transition::Column::SettlementKind,
                ))
                .is_null(),
            )
            .and_where(
                Expr::col((
                    lifecycle_transition::Entity,
                    lifecycle_transition::Column::CompletedAt,
                ))
                .is_null(),
            )
            .to_owned(),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransitionSagaState {
    None,
    Outstanding,
    Published,
}

async fn transition_saga_state<C: ConnectionTrait>(
    db: &C,
    action_id: &str,
) -> AppResult<TransitionSagaState> {
    let saga = lifecycle_transition::Entity::find()
        .filter(lifecycle_transition::Column::ActionId.eq(action_id))
        .one(db)
        .await?;
    Ok(match saga {
        None => TransitionSagaState::None,
        Some(saga)
            if saga.settlement_kind.is_none()
                && saga.completed_at.is_none()
                && saga.publication_receipt.is_some()
                && matches!(saga.checkpoint.as_str(), "publish" | "cleanup") =>
        {
            TransitionSagaState::Published
        }
        Some(saga) if saga.settlement_kind.is_none() && saga.completed_at.is_none() => {
            TransitionSagaState::Outstanding
        }
        Some(_) => TransitionSagaState::None,
    })
}

fn is_transition_action(action: &lifecycle_action::Model) -> bool {
    matches!(
        action.action_kind.as_str(),
        "transition_current" | "transition_noncurrent"
    )
}

fn active_claim_condition(claim: &ClaimedLifecycleAction) -> Condition {
    Condition::all()
        .add(lifecycle_action::Column::Id.eq(claim.action.id.clone()))
        .add(lifecycle_action::Column::State.eq(STATE_CLAIMED))
        .add(lifecycle_action::Column::ClaimedBy.eq(claim.worker_id.clone()))
        .add(lifecycle_action::Column::ClaimEpoch.eq(claim.claim_epoch))
}

fn validate_failure_class(failure_class: &str) -> AppResult<()> {
    if is_allowed_failure_class(failure_class) {
        return Ok(());
    }
    Err(AppError::InvalidArgument(
        "invalid lifecycle action failure class".to_owned(),
    ))
}

fn is_allowed_failure_class(failure_class: &str) -> bool {
    matches!(
        failure_class,
        FAILURE_DATABASE_CONTENTION
            | FAILURE_ADMISSION_TEMPORARILY_UNAVAILABLE
            | FAILURE_INTERNAL_DEPENDENCY
            | FAILURE_CANCELLED_STALE
    )
}

async fn before_terminal_store_write(action_id: &str, state: &str) -> AppResult<()> {
    #[cfg(test)]
    test_hooks::fail_before_terminal_store_write(action_id, state).await?;
    #[cfg(not(test))]
    let _ = (action_id, state);
    Ok(())
}

fn is_sqlite_contention(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("database is locked") || error.contains("database is busy")
}

async fn sqlite_claim_retry_delay(attempt: usize) {
    let milliseconds = 1_u64.checked_shl(attempt.min(4) as u32).unwrap_or(16);
    tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
}

fn normalize_transaction_error(error: TransactionError<AppError>) -> AppError {
    match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => error.into(),
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};
    use std::time::Duration;

    use crate::{
        error::{AppError, AppResult},
        store::lifecycle_action::STATE_SUCCEEDED,
    };

    #[derive(Debug)]
    struct PendingTerminalFailure {
        state: String,
        temporary: bool,
    }

    static NEXT_TERMINAL_FAILURE: LazyLock<Mutex<HashMap<String, PendingTerminalFailure>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    static NEXT_RENEWAL_PAUSE: LazyLock<Mutex<HashMap<String, Duration>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    pub struct FailureScope {
        action_id: String,
    }

    pub struct RenewalPauseScope {
        action_id: String,
    }

    pub fn fail_next_succeeded(action_id: &str) -> FailureScope {
        NEXT_TERMINAL_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                action_id.to_owned(),
                PendingTerminalFailure {
                    state: STATE_SUCCEEDED.to_owned(),
                    temporary: false,
                },
            );
        FailureScope {
            action_id: action_id.to_owned(),
        }
    }

    pub fn pause_next_renewal(action_id: &str, duration: Duration) -> RenewalPauseScope {
        NEXT_RENEWAL_PAUSE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(action_id.to_owned(), duration);
        RenewalPauseScope {
            action_id: action_id.to_owned(),
        }
    }

    impl FailureScope {
        pub fn temporarily(self) -> Self {
            NEXT_TERMINAL_FAILURE
                .lock()
                .unwrap()
                .get_mut(&self.action_id)
                .unwrap()
                .temporary = true;
            self
        }
    }

    pub async fn fail_before_terminal_store_write(action_id: &str, state: &str) -> AppResult<()> {
        let mut failure = NEXT_TERMINAL_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if failure
            .get(action_id)
            .is_some_and(|pending| pending.state == state)
        {
            let temporary = failure.remove(action_id).unwrap().temporary;
            return Err(AppError::Database(
                if temporary {
                    "database is locked"
                } else {
                    "injected lifecycle terminal-store failure"
                }
                .to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn pause_after_renewal_claim_lock(action_id: &str) {
        let duration = NEXT_RENEWAL_PAUSE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(action_id);
        if let Some(duration) = duration {
            tokio::time::sleep(duration).await;
        }
    }

    impl Drop for FailureScope {
        fn drop(&mut self) {
            let mut failure = NEXT_TERMINAL_FAILURE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            failure.remove(&self.action_id);
        }
    }

    impl Drop for RenewalPauseScope {
        fn drop(&mut self) {
            NEXT_RENEWAL_PAUSE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.action_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, EntityTrait, PaginatorTrait, QueryFilter,
        QueryOrder, sea_query::Expr,
    };
    use sha2::Digest as _;

    use super::{
        MAX_LIFECYCLE_ACTION_ATTEMPTS, STATE_CLAIMED, STATE_FAILED_SAFE,
        TRANSITION_SETTLEMENT_REQUIRED, canonical_action_bytes, claim_due,
        claim_due_with_max_attempts, idempotency_key, insert_idempotent, lock_claim_for_execution,
        mark_cancelled, mark_failed_safe, mark_succeeded, renew_claim, schedule_retry,
    };
    use crate::{
        lifecycle::model::{
            LifecycleActionKind, LifecycleTargetIdentity, NewLifecycleAction, RuleIdentity,
            VersionTargetIdentity,
        },
        store::{
            bucket, connect_database,
            database_clock::database_now,
            entities::{
                import_destination, lifecycle_action, lifecycle_transition, physical_residency,
            },
            object_version::{PublicVersionId, VersionKind},
            run_migrations,
        },
    };

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        run_migrations(&db).await.unwrap();
        bucket::create(&db, "bucket", None).await.unwrap();
        db
    }

    fn action(row: &str, key: &str, due_at: chrono::DateTime<chrono::Utc>) -> NewLifecycleAction {
        NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: "bucket".to_owned(),
            config_revision: 1,
            rule_identity: RuleIdentity::Id("expire".to_owned()),
            action_kind: LifecycleActionKind::ExpireCurrent,
            target: LifecycleTargetIdentity::Version(VersionTargetIdentity {
                bucket: "bucket".to_owned(),
                key: key.to_owned(),
                version_row_id: row.to_owned(),
                public_version_id: PublicVersionId::Opaque(
                    "00000000-0000-4000-8000-000000000001".to_owned(),
                ),
                kind: VersionKind::Object,
                object_id: Some(format!("object-{row}")),
                sequence: 1,
            }),
            due_at,
        }
    }

    async fn insert_transition_saga(
        db: &sea_orm::DatabaseConnection,
        action: &lifecycle_action::Model,
        checkpoint: &str,
    ) {
        let now = database_now(db).await.unwrap();
        physical_residency::Entity::insert(physical_residency::ActiveModel {
            tier: sea_orm::Set("hot".to_owned()),
            cid: sea_orm::Set("transition-cid".to_owned()),
            node_identity: sea_orm::Set(Some("hot-node".to_owned())),
            verification_state: sea_orm::Set("verified".to_owned()),
            verification_receipt: sea_orm::Set(Some("hot-receipt".to_owned())),
            verified_at: sea_orm::Set(Some(now)),
            created_at: sea_orm::Set(now),
            updated_at: sea_orm::Set(now),
        })
        .exec(db)
        .await
        .unwrap();
        lifecycle_transition::Entity::insert(lifecycle_transition::ActiveModel {
            id: sea_orm::Set(format!("saga-{}", action.id)),
            action_id: sea_orm::Set(action.id.clone()),
            action_kind: sea_orm::Set(action.action_kind.clone()),
            bucket: sea_orm::Set(action.bucket.clone()),
            object_key: sea_orm::Set(action.object_key.clone()),
            config_revision: sea_orm::Set(action.config_revision),
            rule_id: sea_orm::Set(action.rule_id.clone()),
            target_version_row_id: sea_orm::Set(action.target_version_row_id.clone().unwrap()),
            target_public_version_id: sea_orm::Set(
                action.target_public_version_id.clone().unwrap(),
            ),
            target_object_id: sea_orm::Set(action.target_object_id.clone().unwrap()),
            target_sequence: sea_orm::Set(action.target_sequence.unwrap()),
            source_tier: sea_orm::Set("hot".to_owned()),
            destination_tier: sea_orm::Set("cold".to_owned()),
            source_cid: sea_orm::Set("transition-cid".to_owned()),
            destination_cid: sea_orm::Set("transition-cid".to_owned()),
            source_residency_revision: sea_orm::Set(1),
            expected_source_node_identity: sea_orm::Set("hot-node".to_owned()),
            expected_destination_node_identity: sea_orm::Set("cold-node".to_owned()),
            ownership_generation: sea_orm::Set(1),
            checkpoint: sea_orm::Set(checkpoint.to_owned()),
            verification_receipt: sea_orm::Set(
                matches!(checkpoint, "verify" | "publish" | "cleanup")
                    .then(|| "verification-receipt".to_owned()),
            ),
            publication_receipt: sea_orm::Set(
                matches!(checkpoint, "publish" | "cleanup")
                    .then(|| "publication-receipt".to_owned()),
            ),
            settlement_kind: sea_orm::Set(None),
            created_at: sea_orm::Set(now),
            updated_at: sea_orm::Set(now),
            completed_at: sea_orm::Set(None),
        })
        .exec(db)
        .await
        .unwrap();
    }

    fn stored_version_action() -> lifecycle_action::Model {
        let now = chrono::DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        lifecycle_action::Model {
            id: "stored-version-action".to_owned(),
            idempotency_key: "stored-version-key".to_owned(),
            bucket: "bucket".to_owned(),
            object_key: "key".to_owned(),
            config_revision: 7,
            rule_id: "id:expire".to_owned(),
            action_kind: "expire_current".to_owned(),
            target_type: "version".to_owned(),
            target_version_row_id: Some("version-row".to_owned()),
            target_public_version_id: Some("00000000-0000-4000-8000-000000000001".to_owned()),
            target_object_id: Some("object-id".to_owned()),
            target_sequence: Some(1),
            target_upload_id: None,
            target_upload_created_at: None,
            due_at: now,
            state: "pending".to_owned(),
            attempts: 0,
            next_attempt_at: now,
            claim_epoch: 0,
            lease_until: None,
            claimed_by: None,
            failure_class: None,
            last_error_redacted: None,
            created_at: now,
            updated_at: now,
            finished_at: None,
        }
    }

    fn stored_multipart_action() -> lifecycle_action::Model {
        let initiated_at = chrono::DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let due_at = chrono::DateTime::parse_from_rfc3339("2026-09-03T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        lifecycle_action::Model {
            id: "stored-multipart-action".to_owned(),
            idempotency_key: "stored-multipart-key".to_owned(),
            bucket: "bucket".to_owned(),
            object_key: "prefix/key".to_owned(),
            config_revision: 7,
            rule_id: "id:abort".to_owned(),
            action_kind: "abort_incomplete_multipart_upload".to_owned(),
            target_type: "multipart_upload".to_owned(),
            target_version_row_id: None,
            target_public_version_id: None,
            target_object_id: None,
            target_sequence: None,
            target_upload_id: Some("upload-1".to_owned()),
            target_upload_created_at: Some(initiated_at),
            due_at,
            state: "pending".to_owned(),
            attempts: 0,
            next_attempt_at: due_at,
            claim_epoch: 0,
            lease_until: None,
            claimed_by: None,
            failure_class: None,
            last_error_redacted: None,
            created_at: initiated_at,
            updated_at: initiated_at,
            finished_at: None,
        }
    }

    #[test]
    fn stored_polymorphic_target_decoder_returns_only_the_discriminated_identity() {
        match super::target_from_action(&stored_version_action()).unwrap() {
            LifecycleTargetIdentity::Version(target) => {
                assert_eq!(target.bucket, "bucket");
                assert_eq!(target.key, "key");
                assert_eq!(target.version_row_id, "version-row");
                assert_eq!(target.object_id.as_deref(), Some("object-id"));
                assert_eq!(target.sequence, 1);
            }
            other => panic!("expected a version target, got {other:?}"),
        }

        match super::target_from_action(&stored_multipart_action()).unwrap() {
            LifecycleTargetIdentity::MultipartUpload(target) => {
                assert_eq!(target.bucket, "bucket");
                assert_eq!(target.key, "prefix/key");
                assert_eq!(target.upload_id, "upload-1");
                assert_eq!(
                    target.initiated_at,
                    chrono::DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
                        .unwrap()
                        .with_timezone(&chrono::Utc)
                );
            }
            other => panic!("expected a multipart target, got {other:?}"),
        }
    }

    #[test]
    fn stored_polymorphic_target_decoder_rejects_hybrid_missing_and_mismatched_rows() {
        let mut cases = Vec::new();

        let mut action = stored_version_action();
        action.target_upload_id = Some("hybrid-upload".to_owned());
        cases.push(("version with upload ID", action));

        let mut action = stored_version_action();
        action.target_upload_created_at = Some(action.created_at);
        cases.push(("version with upload initiation", action));

        let mut action = stored_multipart_action();
        action.target_version_row_id = Some("hybrid-version".to_owned());
        cases.push(("multipart with version row", action));

        let mut action = stored_multipart_action();
        action.target_public_version_id = Some("null".to_owned());
        cases.push(("multipart with public version", action));

        let mut action = stored_multipart_action();
        action.target_object_id = Some("hybrid-object".to_owned());
        cases.push(("multipart with object ID", action));

        let mut action = stored_multipart_action();
        action.target_sequence = Some(0);
        cases.push(("multipart with sequence", action));

        let mut action = stored_version_action();
        action.target_version_row_id = None;
        cases.push(("version missing row ID", action));

        let mut action = stored_version_action();
        action.target_version_row_id = Some(String::new());
        cases.push(("version with empty row ID", action));

        let mut action = stored_version_action();
        action.target_public_version_id = None;
        cases.push(("version missing public ID", action));

        let mut action = stored_version_action();
        action.target_public_version_id = Some(String::new());
        cases.push(("version with empty public ID", action));

        let mut action = stored_version_action();
        action.target_sequence = None;
        cases.push(("version missing sequence", action));

        let mut action = stored_version_action();
        action.target_sequence = Some(-1);
        cases.push(("version with negative sequence", action));

        let mut action = stored_version_action();
        action.target_object_id = None;
        cases.push(("current expiration missing object ID", action));

        let mut action = stored_version_action();
        action.action_kind = "delete_expired_marker".to_owned();
        cases.push(("delete-marker expiration carrying object ID", action));

        let mut action = stored_multipart_action();
        action.target_upload_id = None;
        cases.push(("multipart missing upload ID", action));

        let mut action = stored_multipart_action();
        action.target_upload_created_at = None;
        cases.push(("multipart missing initiation", action));

        let mut action = stored_multipart_action();
        action.action_kind = "expire_current".to_owned();
        cases.push(("version kind with multipart target", action));

        let mut action = stored_version_action();
        action.action_kind = "abort_incomplete_multipart_upload".to_owned();
        cases.push(("abort kind with version target", action));

        let mut action = stored_multipart_action();
        action.target_type = "unknown".to_owned();
        cases.push(("unknown target discriminator", action));

        let mut action = stored_multipart_action();
        action.action_kind = "unknown".to_owned();
        cases.push(("unknown action kind", action));

        let mut action = stored_multipart_action();
        action.config_revision = 0;
        cases.push(("zero revision", action));

        let mut action = stored_multipart_action();
        action.config_revision = -1;
        cases.push(("negative revision", action));

        let mut action = stored_multipart_action();
        action.target_upload_id = Some(String::new());
        cases.push(("empty upload ID", action));

        let mut action = stored_multipart_action();
        action.bucket.clear();
        cases.push(("empty bucket", action));

        let mut action = stored_multipart_action();
        action.object_key.clear();
        cases.push(("empty object key", action));

        for (case, action) in cases {
            assert!(
                matches!(
                    super::target_from_action(&action),
                    Err(crate::error::AppError::Internal(_))
                ),
                "persisted action decoder accepted {case}"
            );
        }
    }

    #[test]
    fn phase_a_version_idempotency_bytes_are_frozen() {
        let canonical = super::CanonicalVersionActionIdempotency {
            bucket: "bucket",
            config_revision: 7,
            rule_identity: "id:expire",
            action_kind: "expire_current",
            target_version_row_id: "version-row",
            target_public_version_id: "00000000-0000-4000-8000-000000000001",
            target_object_id: Some("object-id"),
            target_sequence: 1,
            due_at: "2026-09-01T00:00:00.000000000Z".to_owned(),
        };
        let bytes = serde_json::to_vec(&canonical).unwrap();
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            "{\"bucket\":\"bucket\",\"config_revision\":7,\"rule_identity\":\"id:expire\",\"action_kind\":\"expire_current\",\"target_version_row_id\":\"version-row\",\"target_public_version_id\":\"00000000-0000-4000-8000-000000000001\",\"target_object_id\":\"object-id\",\"target_sequence\":1,\"due_at\":\"2026-09-01T00:00:00.000000000Z\"}"
        );
        assert_eq!(
            hex::encode(sha2::Sha256::digest(&bytes)),
            "d9c49eddf8319d6726c99c8162ff9b72150f6d57f23ad121be9a263647f55578"
        );

        let action = NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: "bucket".to_owned(),
            config_revision: 7,
            rule_identity: RuleIdentity::Id("expire".to_owned()),
            action_kind: LifecycleActionKind::ExpireCurrent,
            target: LifecycleTargetIdentity::Version(VersionTargetIdentity {
                bucket: "bucket".to_owned(),
                key: "key".to_owned(),
                version_row_id: "version-row".to_owned(),
                public_version_id: PublicVersionId::Opaque(
                    "00000000-0000-4000-8000-000000000001".to_owned(),
                ),
                kind: VersionKind::Object,
                object_id: Some("object-id".to_owned()),
                sequence: 1,
            }),
            due_at: chrono::DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        };
        assert_eq!(
            idempotency_key(&action).unwrap(),
            "d9c49eddf8319d6726c99c8162ff9b72150f6d57f23ad121be9a263647f55578"
        );
        assert_eq!(canonical_action_bytes(&action).unwrap(), bytes);
    }

    #[test]
    fn transition_action_identities_are_distinct_and_content_only() {
        let due_at = chrono::DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let mut current = action("version-row", "key", due_at);
        current.config_revision = 7;
        current.rule_identity = RuleIdentity::Id("transition".to_owned());
        current.action_kind = LifecycleActionKind::TransitionCurrent;

        let current_bytes = canonical_action_bytes(&current).unwrap();
        assert_eq!(
            std::str::from_utf8(&current_bytes).unwrap(),
            "{\"bucket\":\"bucket\",\"config_revision\":7,\"rule_identity\":\"id:transition\",\"action_kind\":\"transition_current\",\"target_version_row_id\":\"version-row\",\"target_public_version_id\":\"00000000-0000-4000-8000-000000000001\",\"target_object_id\":\"object-version-row\",\"target_sequence\":1,\"due_at\":\"2026-09-01T00:00:00.000000000Z\"}"
        );

        let mut noncurrent = current.clone();
        noncurrent.action_kind = LifecycleActionKind::TransitionNoncurrent;
        let noncurrent_bytes = canonical_action_bytes(&noncurrent).unwrap();
        assert_ne!(current_bytes, noncurrent_bytes);
        assert_ne!(
            idempotency_key(&current).unwrap(),
            idempotency_key(&noncurrent).unwrap()
        );

        for action_kind in [
            LifecycleActionKind::TransitionCurrent,
            LifecycleActionKind::TransitionNoncurrent,
        ] {
            let mut marker = current.clone();
            marker.action_kind = action_kind;
            let LifecycleTargetIdentity::Version(target) = &mut marker.target else {
                unreachable!()
            };
            target.kind = VersionKind::DeleteMarker;
            target.object_id = None;
            assert!(matches!(
                canonical_action_bytes(&marker),
                Err(crate::error::AppError::InvalidArgument(_))
            ));
        }
    }

    #[test]
    fn stored_transition_decoder_requires_a_content_version_shape() {
        for (persisted, expected) in [
            ("transition_current", LifecycleActionKind::TransitionCurrent),
            (
                "transition_noncurrent",
                LifecycleActionKind::TransitionNoncurrent,
            ),
        ] {
            assert_eq!(super::action_kind_from_db(persisted).unwrap(), expected);
            let mut stored = stored_version_action();
            stored.action_kind = persisted.to_owned();
            let LifecycleTargetIdentity::Version(target) =
                super::target_from_action(&stored).unwrap()
            else {
                panic!("transition must decode as a version target")
            };
            assert_eq!(target.kind, VersionKind::Object);

            stored.target_object_id = None;
            assert!(matches!(
                super::target_from_action(&stored),
                Err(crate::error::AppError::Internal(_))
            ));
        }
    }

    #[tokio::test]
    async fn generic_claims_include_transition_actions_for_e2() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();

        let mut expiration = action("expiration-row", "expiration", now - Duration::seconds(1));
        expiration.idempotency_key = idempotency_key(&expiration).unwrap();
        assert!(insert_idempotent(&db, expiration, now).await.unwrap());

        let mut transition = action("transition-row", "transition", now - Duration::seconds(1));
        transition.action_kind = LifecycleActionKind::TransitionCurrent;
        transition.idempotency_key = idempotency_key(&transition).unwrap();
        assert!(insert_idempotent(&db, transition, now).await.unwrap());

        let claimed = claim_due(&db, "worker", Duration::seconds(30), 10)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);
        assert!(
            claimed
                .iter()
                .any(|claim| claim.action.action_kind == "expire_current")
        );
        assert!(
            claimed
                .iter()
                .any(|claim| claim.action.action_kind == "transition_current")
        );
    }

    #[tokio::test]
    async fn claim_renewal_uses_database_time_and_cannot_revive_an_expired_claim() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut value = action("renew-row", "renew", now - Duration::seconds(1));
        value.idempotency_key = idempotency_key(&value).unwrap();
        insert_idempotent(&db, value, now).await.unwrap();
        let claim = claim_due(&db, "renew-worker", Duration::seconds(2), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let original_lease = claim.action.lease_until.unwrap();

        assert!(
            renew_claim(&db, &claim, Duration::seconds(30))
                .await
                .unwrap()
        );
        let renewed = lifecycle_action::Entity::find_by_id(&claim.action.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(renewed.lease_until.unwrap() > original_lease);

        let mut wrong_worker = claim.clone();
        wrong_worker.worker_id = "wrong-worker".to_owned();
        assert!(
            !renew_claim(&db, &wrong_worker, Duration::seconds(30))
                .await
                .unwrap()
        );

        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(
                    database_now(&db).await.unwrap() - Duration::seconds(1),
                )),
            )
            .filter(lifecycle_action::Column::Id.eq(&claim.action.id))
            .exec(&db)
            .await
            .unwrap();
        assert!(
            !renew_claim(&db, &claim, Duration::seconds(30))
                .await
                .unwrap()
        );

        let reclaimed = claim_due(&db, "replacement-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(reclaimed.claim_epoch, claim.claim_epoch + 1);
        assert!(
            !renew_claim(&db, &claim, Duration::seconds(30))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn claim_renewal_rechecks_database_time_after_obtaining_the_claim_lock() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut value = action("renew-lock-row", "renew-lock", now - Duration::seconds(1));
        value.idempotency_key = idempotency_key(&value).unwrap();
        insert_idempotent(&db, value, now).await.unwrap();
        let claim = claim_due(&db, "renew-worker", Duration::milliseconds(100), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let _pause = super::test_hooks::pause_next_renewal(
            &claim.action.id,
            std::time::Duration::from_millis(250),
        );

        assert!(
            !renew_claim(&db, &claim, Duration::seconds(30))
                .await
                .unwrap(),
            "the post-lock database clock must observe expiration"
        );
    }

    #[tokio::test]
    async fn published_transition_cleanup_remains_claimable_beyond_the_attempt_budget() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut value = action("published-row", "published", now - Duration::seconds(1));
        value.action_kind = LifecycleActionKind::TransitionCurrent;
        value.idempotency_key = idempotency_key(&value).unwrap();
        insert_idempotent(&db, value, now).await.unwrap();
        let stored = lifecycle_action::Entity::find()
            .filter(lifecycle_action::Column::TargetVersionRowId.eq("published-row"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        insert_transition_saga(&db, &stored, "publish").await;
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::Attempts,
                Expr::value(MAX_LIFECYCLE_ACTION_ATTEMPTS),
            )
            .filter(lifecycle_action::Column::Id.eq(&stored.id))
            .exec(&db)
            .await
            .unwrap();

        for epoch in 1..=3 {
            let claim = claim_due(&db, "cleanup-worker", Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .expect("published cleanup must remain claimable");
            assert_eq!(claim.action.attempts, MAX_LIFECYCLE_ACTION_ATTEMPTS + epoch);
            assert_eq!(claim.claim_epoch, epoch);
            lifecycle_action::Entity::update_many()
                .col_expr(lifecycle_action::Column::State, Expr::value("pending"))
                .col_expr(
                    lifecycle_action::Column::LeaseUntil,
                    Expr::value(Option::<chrono::DateTime<chrono::Utc>>::None),
                )
                .col_expr(
                    lifecycle_action::Column::ClaimedBy,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(
                    lifecycle_action::Column::NextAttemptAt,
                    Expr::value(now - Duration::seconds(1)),
                )
                .filter(lifecycle_action::Column::Id.eq(&stored.id))
                .exec(&db)
                .await
                .unwrap();
        }

        lifecycle_action::Entity::update_many()
            .col_expr(lifecycle_action::Column::Attempts, Expr::value(i64::MAX))
            .filter(lifecycle_action::Column::Id.eq(&stored.id))
            .exec(&db)
            .await
            .unwrap();
        let overflow_fenced = claim_due(&db, "cleanup-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .expect("published cleanup must not overflow the attempt counter");
        assert_eq!(overflow_fenced.action.attempts, i64::MAX);
    }

    #[tokio::test]
    async fn legacy_failed_safe_transition_sagas_are_claimed_for_settlement_only() {
        for checkpoint in ["prepare", "copy", "verify", "publish"] {
            let db = setup().await;
            let now = database_now(&db).await.unwrap();
            let mut value = action(
                &format!("legacy-{checkpoint}-row"),
                &format!("legacy-{checkpoint}"),
                now - Duration::seconds(1),
            );
            value.action_kind = LifecycleActionKind::TransitionCurrent;
            value.idempotency_key = idempotency_key(&value).unwrap();
            insert_idempotent(&db, value, now).await.unwrap();
            let original = claim_due(&db, "legacy-worker", Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
            insert_transition_saga(&db, &original.action, checkpoint).await;
            assert!(
                mark_failed_safe(&db, &original, now, super::FAILURE_INTERNAL_DEPENDENCY)
                    .await
                    .unwrap()
            );

            let recovery = claim_due_with_max_attempts(
                &db,
                "settlement-worker",
                Duration::seconds(30),
                MAX_LIFECYCLE_ACTION_ATTEMPTS,
                1,
            )
            .await
            .unwrap()
            .pop()
            .unwrap_or_else(|| panic!("{checkpoint} saga must be recoverable"));
            assert_eq!(recovery.action.id, original.action.id);
            assert_eq!(recovery.action.state, STATE_CLAIMED);
            assert_eq!(recovery.action.attempts, original.action.attempts + 1);
            assert_eq!(recovery.claim_epoch, original.claim_epoch + 1);
            assert_eq!(recovery.worker_id, "settlement-worker");
            assert_eq!(
                recovery.action.last_error_redacted.as_deref(),
                Some(TRANSITION_SETTLEMENT_REQUIRED),
                "the claim must advertise cleanup-only responsibility"
            );
            assert!(recovery.action.finished_at.is_none());
            assert!(
                recovery.action.lease_until.unwrap() > database_now(&db).await.unwrap(),
                "recovery uses a live lease derived from the database clock"
            );
            assert!(
                !mark_succeeded(&db, &original, database_now(&db).await.unwrap())
                    .await
                    .unwrap(),
                "the pre-recovery epoch must remain fenced"
            );
        }
    }

    #[tokio::test]
    async fn terminal_actions_without_unfinished_transition_sagas_remain_terminal() {
        for terminal_case in [
            "ordinary_failed_safe",
            "transition_failed_safe_without_saga",
            "failed_safe_mismatched_saga",
            "succeeded_with_saga",
            "cancelled_with_saga",
            "failed_safe_with_settled_saga",
        ] {
            let db = setup().await;
            let now = database_now(&db).await.unwrap();
            let mut value = action(
                &format!("{terminal_case}-row"),
                terminal_case,
                now - Duration::seconds(1),
            );
            if terminal_case != "ordinary_failed_safe" {
                value.action_kind = LifecycleActionKind::TransitionCurrent;
            }
            value.idempotency_key = idempotency_key(&value).unwrap();
            insert_idempotent(&db, value, now).await.unwrap();
            let claim = claim_due(&db, "terminal-worker", Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
            if matches!(
                terminal_case,
                "failed_safe_mismatched_saga"
                    | "succeeded_with_saga"
                    | "cancelled_with_saga"
                    | "failed_safe_with_settled_saga"
            ) {
                insert_transition_saga(&db, &claim.action, "prepare").await;
            }
            match terminal_case {
                "succeeded_with_saga" => {
                    assert!(mark_succeeded(&db, &claim, now).await.unwrap());
                }
                "cancelled_with_saga" => {
                    assert!(
                        mark_cancelled(&db, &claim, now, super::FAILURE_CANCELLED_STALE)
                            .await
                            .unwrap()
                    );
                }
                _ => {
                    assert!(
                        mark_failed_safe(&db, &claim, now, super::FAILURE_INTERNAL_DEPENDENCY,)
                            .await
                            .unwrap()
                    );
                }
            }
            if terminal_case == "failed_safe_mismatched_saga" {
                lifecycle_action::Entity::update_many()
                    .col_expr(
                        lifecycle_action::Column::ActionKind,
                        Expr::value("expire_current"),
                    )
                    .filter(lifecycle_action::Column::Id.eq(&claim.action.id))
                    .exec(&db)
                    .await
                    .unwrap();
            }
            if terminal_case == "failed_safe_with_settled_saga" {
                lifecycle_transition::Entity::update_many()
                    .col_expr(
                        lifecycle_transition::Column::SettlementKind,
                        Expr::value(Some("cancelled".to_owned())),
                    )
                    .col_expr(
                        lifecycle_transition::Column::CompletedAt,
                        Expr::value(Some(now)),
                    )
                    .filter(lifecycle_transition::Column::ActionId.eq(&claim.action.id))
                    .exec(&db)
                    .await
                    .unwrap();
            }

            assert!(
                claim_due(&db, "must-not-reopen", Duration::seconds(30), 1)
                    .await
                    .unwrap()
                    .is_empty(),
                "{terminal_case} must remain terminal"
            );
        }
    }

    #[tokio::test]
    async fn sqlite_legacy_settlement_claim_has_one_owner_per_epoch_across_connections() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lifecycle-settlement-claim.db");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            path.display().to_string().replace('\\', "/")
        );
        let primary = connect_database(&database_url).await.unwrap();
        run_migrations(&primary).await.unwrap();
        bucket::create(&primary, "bucket", None).await.unwrap();
        let now = database_now(&primary).await.unwrap();
        let mut value = action(
            "concurrent-legacy-row",
            "concurrent-legacy",
            now - Duration::seconds(1),
        );
        value.action_kind = LifecycleActionKind::TransitionCurrent;
        value.idempotency_key = idempotency_key(&value).unwrap();
        insert_idempotent(&primary, value, now).await.unwrap();
        let original = claim_due(&primary, "original-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        insert_transition_saga(&primary, &original.action, "prepare").await;
        assert!(
            mark_failed_safe(&primary, &original, now, super::FAILURE_INTERNAL_DEPENDENCY,)
                .await
                .unwrap()
        );

        let first_barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let first_left_db = connect_database(&database_url).await.unwrap();
        let first_right_db = connect_database(&database_url).await.unwrap();
        let left_barrier = first_barrier.clone();
        let first_left = tokio::spawn(async move {
            left_barrier.wait().await;
            claim_due(&first_left_db, "first-left", Duration::seconds(30), 1).await
        });
        let right_barrier = first_barrier.clone();
        let first_right = tokio::spawn(async move {
            right_barrier.wait().await;
            claim_due(&first_right_db, "first-right", Duration::seconds(30), 1).await
        });
        first_barrier.wait().await;
        let first_claims = [
            first_left.await.unwrap().unwrap(),
            first_right.await.unwrap().unwrap(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        assert_eq!(first_claims.len(), 1, "only one recovery owner may win");
        let recovery = first_claims.into_iter().next().unwrap();
        assert_eq!(recovery.claim_epoch, original.claim_epoch + 1);
        assert!(matches!(
            recovery.worker_id.as_str(),
            "first-left" | "first-right"
        ));
        assert_eq!(
            recovery.action.last_error_redacted.as_deref(),
            Some(TRANSITION_SETTLEMENT_REQUIRED)
        );

        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(
                    database_now(&primary).await.unwrap() - Duration::seconds(1),
                )),
            )
            .filter(lifecycle_action::Column::Id.eq(&recovery.action.id))
            .exec(&primary)
            .await
            .unwrap();

        let second_barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let second_left_db = connect_database(&database_url).await.unwrap();
        let second_right_db = connect_database(&database_url).await.unwrap();
        let left_barrier = second_barrier.clone();
        let second_left = tokio::spawn(async move {
            left_barrier.wait().await;
            claim_due(&second_left_db, "second-left", Duration::seconds(30), 1).await
        });
        let right_barrier = second_barrier.clone();
        let second_right = tokio::spawn(async move {
            right_barrier.wait().await;
            claim_due(&second_right_db, "second-right", Duration::seconds(30), 1).await
        });
        second_barrier.wait().await;
        let second_claims = [
            second_left.await.unwrap().unwrap(),
            second_right.await.unwrap().unwrap(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        assert_eq!(
            second_claims.len(),
            1,
            "only one expired-lease takeover may win"
        );
        let takeover = second_claims.into_iter().next().unwrap();
        assert_eq!(takeover.claim_epoch, recovery.claim_epoch + 1);
        assert!(matches!(
            takeover.worker_id.as_str(),
            "second-left" | "second-right"
        ));
        assert_eq!(
            takeover.action.last_error_redacted.as_deref(),
            Some(TRANSITION_SETTLEMENT_REQUIRED)
        );
        assert!(
            !mark_succeeded(&primary, &recovery, database_now(&primary).await.unwrap(),)
                .await
                .unwrap(),
            "the expired recovery epoch must not settle after takeover"
        );
        let stored = lifecycle_action::Entity::find_by_id(&takeover.action.id)
            .one(&primary)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.claim_epoch, takeover.claim_epoch);
        assert_eq!(
            stored.claimed_by.as_deref(),
            Some(takeover.worker_id.as_str())
        );
        assert_eq!(stored.state, STATE_CLAIMED);
    }

    #[tokio::test]
    async fn crashed_legacy_settlement_claim_retains_marker_until_the_saga_is_settled() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut value = action(
            "legacy-reclaim-row",
            "legacy-reclaim",
            now - Duration::seconds(1),
        );
        value.action_kind = LifecycleActionKind::TransitionCurrent;
        value.idempotency_key = idempotency_key(&value).unwrap();
        insert_idempotent(&db, value, now).await.unwrap();
        let original = claim_due(&db, "original-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        insert_transition_saga(&db, &original.action, "copy").await;
        assert!(
            mark_failed_safe(&db, &original, now, super::FAILURE_INTERNAL_DEPENDENCY)
                .await
                .unwrap()
        );
        let recovery = claim_due(&db, "recovery-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .expect("legacy failed-safe saga must be recovered");
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(
                    database_now(&db).await.unwrap() - Duration::seconds(1),
                )),
            )
            .filter(lifecycle_action::Column::Id.eq(&recovery.action.id))
            .exec(&db)
            .await
            .unwrap();

        let reclaimed = claim_due(&db, "replacement-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .expect("crashed settlement cleanup must remain recoverable");
        assert_eq!(reclaimed.claim_epoch, recovery.claim_epoch + 1);
        assert_eq!(reclaimed.action.attempts, recovery.action.attempts + 1);
        assert_eq!(reclaimed.worker_id, "replacement-worker");
        assert_eq!(
            reclaimed.action.last_error_redacted.as_deref(),
            Some(TRANSITION_SETTLEMENT_REQUIRED)
        );
        assert!(
            !mark_failed_safe(
                &db,
                &recovery,
                database_now(&db).await.unwrap(),
                super::FAILURE_INTERNAL_DEPENDENCY,
            )
            .await
            .unwrap(),
            "the crashed recovery epoch must remain fenced"
        );

        lifecycle_transition::Entity::update_many()
            .col_expr(
                lifecycle_transition::Column::SettlementKind,
                Expr::value(Some("cancelled".to_owned())),
            )
            .col_expr(
                lifecycle_transition::Column::CompletedAt,
                Expr::value(Some(database_now(&db).await.unwrap())),
            )
            .filter(lifecycle_transition::Column::ActionId.eq(&reclaimed.action.id))
            .exec(&db)
            .await
            .unwrap();
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(
                    database_now(&db).await.unwrap() - Duration::seconds(1),
                )),
            )
            .filter(lifecycle_action::Column::Id.eq(&reclaimed.action.id))
            .exec(&db)
            .await
            .unwrap();
        assert!(
            claim_due(&db, "settled-must-not-reclaim", Duration::seconds(30), 1)
                .await
                .unwrap()
                .is_empty(),
            "settled cleanup work must not be reclaimed"
        );
    }

    #[tokio::test]
    async fn transition_hot_verification_wait_refunds_the_claim_attempt_without_expiration_mode() {
        use sea_orm::TransactionTrait;

        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut value = action("hot-wait-row", "hot-wait", now - Duration::seconds(1));
        value.action_kind = LifecycleActionKind::TransitionCurrent;
        value.idempotency_key = idempotency_key(&value).unwrap();
        insert_idempotent(&db, value, now).await.unwrap();
        let claim = claim_due(&db, "hot-wait-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(claim.action.attempts, 1);

        let before = database_now(&db).await.unwrap();
        let txn = db.begin().await.unwrap();
        assert!(
            super::wait_for_hot_verification_in_transaction(&txn, &claim)
                .await
                .unwrap()
        );
        txn.commit().await.unwrap();
        let after = database_now(&db).await.unwrap();
        let stored = lifecycle_action::Entity::find_by_id(&claim.action.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, "pending");
        assert_eq!(stored.attempts, 0);
        assert!(stored.failure_class.is_none());
        assert!(!super::waiting_for_transition(&stored));
        assert!(stored.next_attempt_at >= before + Duration::seconds(30));
        assert!(stored.next_attempt_at <= after + Duration::seconds(30));
    }

    #[tokio::test]
    async fn unpublished_transition_gets_one_final_recovery_claim_at_the_attempt_cap() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut value = action(
            "final-transition-row",
            "final-transition",
            now - Duration::seconds(1),
        );
        value.action_kind = LifecycleActionKind::TransitionCurrent;
        value.idempotency_key = idempotency_key(&value).unwrap();
        insert_idempotent(&db, value, now).await.unwrap();
        let stored = lifecycle_action::Entity::find()
            .filter(lifecycle_action::Column::TargetVersionRowId.eq("final-transition-row"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        insert_transition_saga(&db, &stored, "prepare").await;
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::Attempts,
                Expr::value(MAX_LIFECYCLE_ACTION_ATTEMPTS),
            )
            .filter(lifecycle_action::Column::TargetVersionRowId.eq("final-transition-row"))
            .exec(&db)
            .await
            .unwrap();

        let recovery = claim_due(&db, "recovery-worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .expect("transition must receive a final E2 settlement claim");
        assert_eq!(recovery.action.attempts, MAX_LIFECYCLE_ACTION_ATTEMPTS + 1);

        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(
                    database_now(&db).await.unwrap() - Duration::seconds(1),
                )),
            )
            .filter(lifecycle_action::Column::Id.eq(&recovery.action.id))
            .exec(&db)
            .await
            .unwrap();
        let settlement_reclaim = claim_due(&db, "must-not-drop-saga", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .expect("final E2 settlement must remain reclaimable after a crash");
        assert_eq!(
            settlement_reclaim.action.attempts,
            MAX_LIFECYCLE_ACTION_ATTEMPTS + 2
        );
        assert_eq!(settlement_reclaim.claim_epoch, recovery.claim_epoch + 1);
    }

    #[tokio::test]
    async fn dependency_probe_failure_returns_to_the_ordinary_attempt_budget() {
        use sea_orm::TransactionTrait;
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut value = action("waiting-row", "waiting", now - Duration::seconds(1));
        value.idempotency_key = idempotency_key(&value).unwrap();
        insert_idempotent(&db, value, now).await.unwrap();
        let first = claim_due(&db, "worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let txn = db.begin().await.unwrap();
        assert!(
            super::wait_for_transition_in_transaction(&txn, &first)
                .await
                .unwrap()
        );
        txn.commit().await.unwrap();
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::NextAttemptAt,
                Expr::value(now - Duration::seconds(1)),
            )
            .filter(lifecycle_action::Column::Id.eq(&first.action.id))
            .exec(&db)
            .await
            .unwrap();
        let probe = claim_due(&db, "worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(probe.action.attempts, 0);
        let now = database_now(&db).await.unwrap();
        assert!(
            schedule_retry(
                &db,
                &probe,
                now,
                now + Duration::seconds(1),
                super::FAILURE_DATABASE_CONTENTION
            )
            .await
            .unwrap()
        );
        let stored = lifecycle_action::Entity::find_by_id(&first.action.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.attempts, 1);
        assert!(!super::waiting_for_transition(&stored));
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::NextAttemptAt,
                Expr::value(now - Duration::seconds(1)),
            )
            .filter(lifecycle_action::Column::Id.eq(&first.action.id))
            .exec(&db)
            .await
            .unwrap();
        let next = claim_due(&db, "worker", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(next.action.attempts, 2);
    }

    #[tokio::test]
    async fn lifecycle_claim_actions_are_idempotent_stably_ordered_reclaimed_and_fenced() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut first = action("row-1", "one", now - Duration::seconds(3));
        first.idempotency_key = idempotency_key(&first).unwrap();
        assert!(insert_idempotent(&db, first.clone(), now).await.unwrap());
        assert!(
            !insert_idempotent(&db, first.clone(), now).await.unwrap(),
            "replaying the exact action must not create a second durable row"
        );

        let mut second = action("row-2", "two", now - Duration::seconds(2));
        second.idempotency_key = idempotency_key(&second).unwrap();
        let mut third = action("row-3", "three", now - Duration::seconds(1));
        third.idempotency_key = idempotency_key(&third).unwrap();
        assert!(insert_idempotent(&db, second, now).await.unwrap());
        assert!(insert_idempotent(&db, third, now).await.unwrap());

        let expected = lifecycle_action::Entity::find()
            .order_by_asc(lifecycle_action::Column::DueAt)
            .order_by_asc(lifecycle_action::Column::Id)
            .all(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        let claimed = claim_due(&db, "worker-a", Duration::seconds(30), 3)
            .await
            .unwrap();
        assert_eq!(
            claimed
                .iter()
                .map(|claim| claim.action.id.as_str())
                .collect::<Vec<_>>(),
            expected.iter().map(String::as_str).collect::<Vec<_>>(),
            "due work has stable (due_at, id) order"
        );
        assert!(claimed.iter().all(|claim| {
            claim.action.state == "claimed"
                && claim.action.attempts == 1
                && claim.claim_epoch == 1
                && claim.action.claimed_by.as_deref() == Some("worker-a")
        }));

        let first_claim = claimed[0].clone();
        let db_now = database_now(&db).await.unwrap();
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(db_now - Duration::seconds(1))),
            )
            .col_expr(
                lifecycle_action::Column::FailureClass,
                Expr::value(Some("database_contention")),
            )
            .filter(lifecycle_action::Column::Id.eq(first_claim.action.id.clone()))
            .exec(&db)
            .await
            .unwrap();
        let reclaimed = claim_due(&db, "worker-b", Duration::seconds(30), 1)
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), 1);
        let reclaim = &reclaimed[0];
        assert_eq!(reclaim.action.id, first_claim.action.id);
        assert_eq!(reclaim.action.attempts, 2);
        assert_eq!(reclaim.claim_epoch, 2);
        assert_eq!(
            reclaim.action.failure_class.as_deref(),
            Some("database_contention")
        );
        assert!(
            lock_claim_for_execution(&db, &first_claim)
                .await
                .unwrap()
                .is_none()
        );

        let fence_now = database_now(&db).await.unwrap();
        assert!(!mark_succeeded(&db, &first_claim, fence_now).await.unwrap());
        assert!(
            !schedule_retry(
                &db,
                &first_claim,
                fence_now,
                fence_now + Duration::seconds(1),
                "database_contention",
            )
            .await
            .unwrap()
        );
        assert!(
            !mark_cancelled(&db, &first_claim, fence_now, "cancelled_stale")
                .await
                .unwrap()
        );
        assert!(
            !mark_failed_safe(&db, &first_claim, fence_now, "internal_dependency")
                .await
                .unwrap()
        );
        assert!(
            lock_claim_for_execution(&db, reclaim)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn lifecycle_claim_max_attempts_fails_safe_without_another_claim() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let configured_max_attempts = 2;
        let mut pending = action("row-limit", "limit", now - Duration::seconds(1));
        pending.idempotency_key = idempotency_key(&pending).unwrap();
        insert_idempotent(&db, pending, now).await.unwrap();
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::Attempts,
                Expr::value(configured_max_attempts),
            )
            .filter(lifecycle_action::Column::TargetVersionRowId.eq("row-limit"))
            .exec(&db)
            .await
            .unwrap();

        assert!(
            claim_due_with_max_attempts(
                &db,
                "worker-limit",
                Duration::seconds(30),
                configured_max_attempts,
                1,
            )
            .await
            .unwrap()
            .is_empty()
        );
        let stored = lifecycle_action::Entity::find()
            .filter(lifecycle_action::Column::TargetVersionRowId.eq("row-limit"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, "failed_safe");
        assert_eq!(stored.attempts, configured_max_attempts);
        assert!(stored.finished_at.is_some());
        assert!(stored.claimed_by.is_none());
        assert!(stored.lease_until.is_none());
    }

    #[tokio::test]
    async fn lifecycle_claim_final_recovery_crash_clears_only_its_owned_token_without_reclaiming() {
        for token_case in ["owned", "user", "other_action", "newer_epoch"] {
            let db = setup().await;
            let now = database_now(&db).await.unwrap();
            let mut pending = action("row-final-crash", "final-crash", now - Duration::seconds(1));
            pending.idempotency_key = idempotency_key(&pending).unwrap();
            insert_idempotent(&db, pending, now).await.unwrap();
            let stored = lifecycle_action::Entity::find()
                .filter(lifecycle_action::Column::TargetVersionRowId.eq("row-final-crash"))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            let claim_epoch = 2_i64;
            let token = match token_case {
                "owned" => format!("lifecycle:{}:{claim_epoch}", stored.id),
                "user" => "user-mutation-token".to_owned(),
                "other_action" => format!("lifecycle:{}:{claim_epoch}", uuid::Uuid::new_v4()),
                "newer_epoch" => format!("lifecycle:{}:{}", stored.id, claim_epoch + 1),
                _ => unreachable!(),
            };
            lifecycle_action::Entity::update_many()
                .col_expr(lifecycle_action::Column::State, Expr::value(STATE_CLAIMED))
                .col_expr(
                    lifecycle_action::Column::Attempts,
                    Expr::value(MAX_LIFECYCLE_ACTION_ATTEMPTS + 1),
                )
                .col_expr(
                    lifecycle_action::Column::ClaimEpoch,
                    Expr::value(claim_epoch),
                )
                .col_expr(
                    lifecycle_action::Column::ClaimedBy,
                    Expr::value(Some("crashed-recovery-worker")),
                )
                .col_expr(
                    lifecycle_action::Column::LeaseUntil,
                    Expr::value(Some(now - Duration::seconds(1))),
                )
                .filter(lifecycle_action::Column::Id.eq(stored.id.clone()))
                .exec(&db)
                .await
                .unwrap();
            import_destination::Entity::insert(import_destination::ActiveModel {
                bucket: sea_orm::Set("bucket".to_owned()),
                key: sea_orm::Set("final-crash".to_owned()),
                generation: sea_orm::Set(1),
                owner_job_id: sea_orm::Set(None),
                mutation_id: sea_orm::Set(Some(token.clone())),
                mutation_prefix: sea_orm::Set(None),
                updated_at: sea_orm::Set(now),
            })
            .exec(&db)
            .await
            .unwrap();

            assert!(
                claim_due(&db, "must-not-reclaim", Duration::seconds(30), 1)
                    .await
                    .unwrap()
                    .is_empty()
            );
            let terminal = lifecycle_action::Entity::find_by_id(stored.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(terminal.state, STATE_FAILED_SAFE, "{token_case}");
            assert_eq!(
                terminal.attempts,
                MAX_LIFECYCLE_ACTION_ATTEMPTS + 1,
                "{token_case}"
            );
            let destination = import_destination::Entity::find_by_id((
                "bucket".to_owned(),
                "final-crash".to_owned(),
            ))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                destination.mutation_id.as_deref(),
                (token_case != "owned").then_some(token.as_str()),
                "{token_case}"
            );
        }
    }

    #[tokio::test]
    async fn multipart_exhausted_claim_never_clears_a_content_mutation_token() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut exhausted = stored_multipart_action();
        exhausted.id = uuid::Uuid::new_v4().to_string();
        exhausted.due_at = now - Duration::seconds(2);
        exhausted.state = STATE_CLAIMED.to_owned();
        exhausted.attempts = MAX_LIFECYCLE_ACTION_ATTEMPTS + 1;
        exhausted.next_attempt_at = now - Duration::seconds(2);
        exhausted.claim_epoch = 2;
        exhausted.lease_until = Some(now - Duration::seconds(1));
        exhausted.claimed_by = Some("crashed-multipart-worker".to_owned());
        exhausted.created_at = now - Duration::seconds(3);
        exhausted.updated_at = now - Duration::seconds(2);
        lifecycle_action::Entity::insert(lifecycle_action::ActiveModel::from(exhausted.clone()))
            .exec(&db)
            .await
            .unwrap();
        let untouched_token = format!("lifecycle:{}:{}", exhausted.id, exhausted.claim_epoch);
        import_destination::Entity::insert(import_destination::ActiveModel {
            bucket: sea_orm::Set(exhausted.bucket.clone()),
            key: sea_orm::Set(exhausted.object_key.clone()),
            generation: sea_orm::Set(1),
            owner_job_id: sea_orm::Set(None),
            mutation_id: sea_orm::Set(Some(untouched_token.clone())),
            mutation_prefix: sea_orm::Set(None),
            updated_at: sea_orm::Set(now),
        })
        .exec(&db)
        .await
        .unwrap();

        assert!(
            claim_due(&db, "must-not-reclaim-multipart", Duration::seconds(30), 1)
                .await
                .unwrap()
                .is_empty()
        );

        let terminal = lifecycle_action::Entity::find_by_id(exhausted.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.state, STATE_FAILED_SAFE);
        let destination =
            import_destination::Entity::find_by_id((exhausted.bucket, exhausted.object_key))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            destination.mutation_id.as_deref(),
            Some(untouched_token.as_str()),
            "multipart exhaustion must not perform version-token cleanup"
        );
    }

    #[tokio::test]
    async fn lifecycle_claim_empty_present_rule_id_is_distinct_from_ordinal_zero() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let due_at = now - Duration::seconds(1);
        let mut present_empty_id = action("empty-rule-row", "empty-rule", due_at);
        present_empty_id.rule_identity = RuleIdentity::Id(String::new());
        present_empty_id.idempotency_key = idempotency_key(&present_empty_id).unwrap();
        let mut absent_id_ordinal_zero = present_empty_id.clone();
        absent_id_ordinal_zero.rule_identity = RuleIdentity::Ordinal(0);
        absent_id_ordinal_zero.idempotency_key = idempotency_key(&absent_id_ordinal_zero).unwrap();

        assert_ne!(
            present_empty_id.idempotency_key, absent_id_ordinal_zero.idempotency_key,
            "present empty IDs and absent IDs must remain separate idempotency namespaces"
        );
        assert!(insert_idempotent(&db, present_empty_id, now).await.unwrap());
        assert!(
            insert_idempotent(&db, absent_id_ordinal_zero, now)
                .await
                .unwrap()
        );

        let claimed = claim_due(&db, "worker-empty-id", Duration::seconds(30), 2)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);
        let rule_ids = claimed
            .iter()
            .map(|claim| claim.action.rule_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            rule_ids,
            std::collections::BTreeSet::from(["id:", "ordinal:0"])
        );
    }

    #[tokio::test]
    async fn lifecycle_claim_sqlite_duplicate_insert_is_unique_under_busy_contention() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lifecycle-action-contention.db");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            path.display().to_string().replace('\\', "/")
        );
        let primary = connect_database(&database_url).await.unwrap();
        run_migrations(&primary).await.unwrap();
        bucket::create(&primary, "bucket", None).await.unwrap();
        let replica = connect_database(&database_url).await.unwrap();
        let now = database_now(&primary).await.unwrap();
        let duplicate = action("row-contention", "contention", now - Duration::seconds(1));
        let left = duplicate.clone();
        let right = duplicate;

        let (left, right) = tokio::join!(
            insert_idempotent(&primary, left, now),
            insert_idempotent(&replica, right, now),
        );
        let inserted = [left.unwrap(), right.unwrap()]
            .into_iter()
            .filter(|inserted| *inserted)
            .count();
        assert_eq!(
            inserted, 1,
            "busy SQLite writers converge on one idempotency row"
        );
        assert_eq!(
            lifecycle_action::Entity::find()
                .count(&primary)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn lifecycle_claim_validates_marker_targets_and_redacts_failure_classes() {
        let db = setup().await;
        let now = database_now(&db).await.unwrap();
        let mut invalid_marker = action("marker-row", "marker", now - Duration::seconds(1));
        let LifecycleTargetIdentity::Version(target) = &mut invalid_marker.target else {
            unreachable!()
        };
        target.kind = VersionKind::DeleteMarker;
        assert!(matches!(
            insert_idempotent(&db, invalid_marker, now).await,
            Err(crate::error::AppError::InvalidArgument(_))
        ));

        let mut valid = action("redaction-row", "redaction", now - Duration::seconds(1));
        valid.idempotency_key = idempotency_key(&valid).unwrap();
        insert_idempotent(&db, valid, now).await.unwrap();
        let claim = claim_due(&db, "worker-redaction", Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(matches!(
            schedule_retry(
                &db,
                &claim,
                now,
                now + Duration::seconds(1),
                "raw backend error: credential=secret",
            )
            .await,
            Err(crate::error::AppError::InvalidArgument(_))
        ));
        let complete_now = database_now(&db).await.unwrap();
        assert!(
            mark_failed_safe(&db, &claim, complete_now, "internal_dependency")
                .await
                .unwrap()
        );
        let stored = lifecycle_action::Entity::find_by_id(claim.action.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.last_error_redacted.as_deref(),
            Some(super::REDACTED_LIFECYCLE_ACTION_ERROR)
        );
        assert_eq!(stored.failure_class.as_deref(), Some("internal_dependency"));
    }
}
