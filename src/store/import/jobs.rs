use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use chrono::{DateTime, Utc};
use sea_orm::sea_query::{Condition, Expr};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    import::{ImportClaim, ImportFailure, ImportPhase, ImportProgress, ImportSource, ImportState},
    pinning::decision::ExtensionDecision,
    pinning::tags::{ObjectTag, validate_tag_set},
    store::{entities::import_job, import::lease_clock},
};

const STATE_QUEUED: &str = "queued";
const STATE_RUNNING: &str = "running";
const STATE_COMPLETED: &str = "completed";
const STATE_FAILED: &str = "failed";
const STATE_SUPERSEDED: &str = "superseded";
const PHASE_QUEUED: &str = "queued";
const MAX_SQLITE_CLAIM_RETRIES: usize = 4;

#[derive(Clone, Debug)]
pub struct NewImportJob {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub source: ImportSource,
    pub request_fingerprint: String,
    pub client_token: Option<String>,
    pub object_content_type: Option<String>,
    pub metadata: HashMap<String, String>,
    pub tags: Vec<ObjectTag>,
    pub decompress_prefix: Option<String>,
}

pub enum SubmitImportOutcome {
    Created(import_job::Model),
    Replayed(import_job::Model),
}

pub struct ClaimedImportJob {
    pub job: import_job::Model,
    pub claim: ImportClaim,
}

#[allow(dead_code)]
pub(crate) async fn find_idempotent<C: ConnectionTrait>(
    txn: &C,
    bucket: &str,
    key: &str,
    client_token: &str,
) -> AppResult<Option<import_job::Model>> {
    Ok(import_job::Entity::find()
        .filter(import_job::Column::Bucket.eq(bucket))
        .filter(import_job::Column::Key.eq(key))
        .filter(import_job::Column::ClientToken.eq(client_token))
        .one(txn)
        .await?)
}

#[allow(dead_code)]
pub(crate) async fn insert_queued<C: ConnectionTrait>(
    txn: &C,
    request: NewImportJob,
    now: DateTime<Utc>,
) -> AppResult<import_job::Model> {
    insert_queued_with_decision(txn, request, None, now).await
}

pub(crate) async fn insert_queued_with_decision<C: ConnectionTrait>(
    txn: &C,
    request: NewImportJob,
    decision: Option<&ExtensionDecision>,
    now: DateTime<Utc>,
) -> AppResult<import_job::Model> {
    let pin_decision_json = decision
        .map(|decision| {
            decision
                .replay_policy(request.tags.clone())
                .map_err(|_| AppError::InvalidImportRequest)?;
            if decision.origin.request_id != request.id {
                return Err(AppError::InvalidImportRequest);
            }
            serde_json::to_string(decision).map_err(|_| {
                AppError::Internal("failed to serialize import pin decision".to_owned())
            })
        })
        .transpose()?;
    let (source_type, source_value) = canonical_source(request.source)?;
    let metadata_json = deterministic_metadata_json(&request.metadata)?;
    let tags_json = deterministic_tags_json(request.tags)?;
    let id = request.id.clone();

    import_job::Entity::insert(import_job::ActiveModel {
        id: Set(request.id),
        bucket: Set(request.bucket),
        key: Set(request.key),
        source_type: Set(source_type),
        source_value: Set(source_value),
        request_fingerprint: Set(request.request_fingerprint),
        client_token: Set(request.client_token),
        object_content_type: Set(request.object_content_type),
        metadata_json: Set(metadata_json),
        tags_json: Set(tags_json),
        pin_decision_json: Set(pin_decision_json),
        decompress_prefix: Set(request.decompress_prefix),
        state: Set(STATE_QUEUED.to_owned()),
        phase: Set(PHASE_QUEUED.to_owned()),
        attempts: Set(0),
        next_attempt_at: Set(now),
        locked_by: Set(None),
        locked_until: Set(None),
        claim_epoch: Set(0),
        providers_observed: Set(0),
        pin_nodes_processed: Set(0),
        pin_bytes_processed: Set(0),
        downloaded_bytes: Set(0),
        download_total: Set(None),
        ipfs_add_bytes: Set(0),
        logical_size: Set(None),
        entries_processed: Set(0),
        entries_succeeded: Set(0),
        entries_failed: Set(0),
        decompressed_bytes: Set(0),
        final_cid: Set(None),
        failure_code: Set(None),
        failure_message: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
        completed_at: Set(None),
    })
    .exec(txn)
    .await?;

    import_job::Entity::find_by_id(id)
        .one(txn)
        .await?
        .ok_or_else(|| AppError::Internal("inserted import job disappeared".to_owned()))
}

/// Claims due queued work and expired running leases in stable due-time order.
///
/// Each candidate is conditionally updated, so competing workers can observe the same candidate
/// but only one receives its newly incremented claim epoch.
pub async fn claim_due(
    db: &DatabaseConnection,
    worker_id: &str,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: u64,
) -> AppResult<Vec<ClaimedImportJob>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if lease_until <= now {
        return Err(AppError::Internal(
            "import claim lease must expire in the future".to_owned(),
        ));
    }

    let worker_id = worker_id.to_owned();
    for attempt in 0..=MAX_SQLITE_CLAIM_RETRIES {
        let worker_id = worker_id.clone();
        let result = db
            .transaction(move |txn| {
                Box::pin(async move {
                    claim_due_in_transaction(txn, &worker_id, now, lease_until, limit).await
                })
            })
            .await;
        match result {
            Ok(claimed) => return Ok(claimed),
            Err(error)
                if is_sqlite_contention(&error.to_string())
                    && attempt < MAX_SQLITE_CLAIM_RETRIES =>
            {
                sqlite_claim_retry_delay(attempt).await;
            }
            Err(error) => return Err(AppError::Database(error.to_string())),
        }
    }
    unreachable!("SQLite claim retry loop always returns or errors")
}

async fn claim_due_in_transaction<C: ConnectionTrait>(
    db: &C,
    worker_id: &str,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: u64,
) -> Result<Vec<ClaimedImportJob>, sea_orm::DbErr> {
    let candidates = import_job::Entity::find()
        .filter(due_claim_condition(now))
        .order_by_asc(import_job::Column::NextAttemptAt)
        .order_by_asc(import_job::Column::CreatedAt)
        .order_by_asc(import_job::Column::Id)
        .limit(limit)
        .all(db)
        .await?;
    let mut claimed = Vec::with_capacity(candidates.len());
    for (due_rank, candidate) in candidates_in_lock_order(candidates) {
        if let Some(claimed_job) =
            claim_candidate(db, candidate, worker_id, now, lease_until).await?
        {
            claimed.push((due_rank, claimed_job));
        }
    }
    claimed.sort_by_key(|(due_rank, _)| *due_rank);
    Ok(claimed
        .into_iter()
        .map(|(_, claimed_job)| claimed_job)
        .collect())
}

/// Every transaction that can update more than one import job does so by ascending job ID.
/// The rank preserves the independently defined due-time ordering for the caller's result.
fn candidates_in_lock_order(candidates: Vec<import_job::Model>) -> Vec<(usize, import_job::Model)> {
    let mut ranked = candidates.into_iter().enumerate().collect::<Vec<_>>();
    ranked.sort_by(|(_, left), (_, right)| left.id.cmp(&right.id));
    ranked
}

async fn claim_candidate<C: ConnectionTrait>(
    db: &C,
    candidate: import_job::Model,
    worker_id: &str,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
) -> Result<Option<ClaimedImportJob>, sea_orm::DbErr> {
    let mut claim_update = import_job::Entity::update_many()
        .col_expr(import_job::Column::State, Expr::value(STATE_RUNNING))
        .col_expr(
            import_job::Column::Attempts,
            Expr::col(import_job::Column::Attempts).add(1),
        )
        .col_expr(
            import_job::Column::ClaimEpoch,
            Expr::col(import_job::Column::ClaimEpoch).add(1),
        )
        .col_expr(
            import_job::Column::LockedBy,
            Expr::value(Some(worker_id.to_owned())),
        )
        .col_expr(
            import_job::Column::LockedUntil,
            Expr::value(Some(lease_until)),
        )
        .col_expr(import_job::Column::UpdatedAt, Expr::value(now));
    // Download and add counters describe one URL transfer attempt. A running
    // lease can be reclaimed without passing through `retry`, so reset them as
    // part of every newly fenced URL claim as well.
    if candidate.source_type == "url" {
        claim_update = claim_update
            .col_expr(import_job::Column::DownloadedBytes, Expr::value(0))
            .col_expr(
                import_job::Column::DownloadTotal,
                Expr::value(Option::<i64>::None),
            )
            .col_expr(import_job::Column::IpfsAddBytes, Expr::value(0));
    }
    let updated = claim_update
        .filter(import_job::Column::Id.eq(candidate.id.clone()))
        .filter(due_claim_condition(now))
        .filter(import_job::Column::Attempts.lt(i32::MAX))
        .filter(import_job::Column::ClaimEpoch.lt(i64::MAX))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Ok(None);
    }

    let job = import_job::Entity::find_by_id(candidate.id)
        .one(db)
        .await?
        .ok_or_else(|| {
            sea_orm::DbErr::RecordNotFound("claimed import job disappeared".to_owned())
        })?;
    let attempt = u32::try_from(job.attempts).map_err(|_| {
        sea_orm::DbErr::Custom("persisted import attempt is outside range".to_owned())
    })?;
    Ok(Some(ClaimedImportJob {
        claim: ImportClaim {
            job_id: job.id.clone(),
            worker_id: worker_id.to_owned(),
            attempt,
            claim_epoch: job.claim_epoch,
            locked_until: lease_until,
        },
        job,
    }))
}

pub async fn renew_claim<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    worker_id: &str,
    claim_epoch: i64,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
) -> AppResult<bool> {
    if lease_until <= now {
        return Ok(false);
    }
    let backend = db.get_database_backend();
    let updated = import_job::Entity::update_many()
        .col_expr(
            import_job::Column::LockedUntil,
            Expr::value(Some(lease_until)),
        )
        .col_expr(import_job::Column::UpdatedAt, Expr::value(now))
        .filter(active_claim_condition(
            job_id,
            worker_id,
            claim_epoch,
            backend,
        ))
        .filter(lease_clock::lease_end_is_future(backend, lease_until))
        .exec(db)
        .await?;
    Ok(updated.rows_affected == 1)
}

pub async fn update_phase<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    worker_id: &str,
    claim_epoch: i64,
    state: ImportState,
    phase: ImportPhase,
    now: DateTime<Utc>,
) -> AppResult<()> {
    if state != ImportState::Running {
        return Err(AppError::Internal(
            "import phase updates require a running state".to_owned(),
        ));
    }
    let updated = import_job::Entity::update_many()
        .col_expr(import_job::Column::State, Expr::value(state.as_str()))
        .col_expr(import_job::Column::Phase, Expr::value(phase.as_str()))
        .col_expr(import_job::Column::UpdatedAt, Expr::value(now))
        .filter(active_claim_condition(
            job_id,
            worker_id,
            claim_epoch,
            db.get_database_backend(),
        ))
        .exec(db)
        .await?;
    require_active_claim(updated.rows_affected)
}

pub async fn update_progress<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    worker_id: &str,
    claim_epoch: i64,
    attempt: u32,
    progress: &ImportProgress,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let attempt = u32_to_i32(attempt, "attempt")?;
    let providers_observed = i64::from(progress.providers_observed);
    let pin_nodes_processed = u64_to_i64(progress.pin_nodes_processed, "pin node progress")?;
    let pin_bytes_processed = u64_to_i64(progress.pin_bytes_processed, "pin byte progress")?;
    let downloaded_bytes = u64_to_i64(progress.downloaded_bytes, "download progress")?;
    let ipfs_add_bytes = u64_to_i64(progress.ipfs_add_bytes, "IPFS add progress")?;
    let entries_processed = u64_to_i64(progress.entries_processed, "entry progress")?;
    let entries_succeeded = u64_to_i64(progress.entries_succeeded, "successful entry progress")?;
    let entries_failed = u64_to_i64(progress.entries_failed, "failed entry progress")?;
    let decompressed_bytes = u64_to_i64(progress.decompressed_bytes, "decompression progress")?;
    let download_total = progress
        .download_total
        .map(|value| u64_to_i64(value, "download total"))
        .transpose()?;
    let logical_size = progress
        .logical_size
        .map(|value| u64_to_i64(value, "logical size"))
        .transpose()?;

    let updated = import_job::Entity::update_many()
        .col_expr(
            import_job::Column::ProvidersObserved,
            monotonic_counter(import_job::Column::ProvidersObserved, providers_observed),
        )
        .col_expr(
            import_job::Column::PinNodesProcessed,
            monotonic_counter(import_job::Column::PinNodesProcessed, pin_nodes_processed),
        )
        .col_expr(
            import_job::Column::PinBytesProcessed,
            monotonic_counter(import_job::Column::PinBytesProcessed, pin_bytes_processed),
        )
        .col_expr(
            import_job::Column::DownloadedBytes,
            monotonic_counter(import_job::Column::DownloadedBytes, downloaded_bytes),
        )
        .col_expr(
            import_job::Column::DownloadTotal,
            monotonic_optional(import_job::Column::DownloadTotal, download_total),
        )
        .col_expr(
            import_job::Column::IpfsAddBytes,
            monotonic_counter(import_job::Column::IpfsAddBytes, ipfs_add_bytes),
        )
        .col_expr(
            import_job::Column::LogicalSize,
            monotonic_optional(import_job::Column::LogicalSize, logical_size),
        )
        .col_expr(
            import_job::Column::EntriesProcessed,
            monotonic_counter(import_job::Column::EntriesProcessed, entries_processed),
        )
        .col_expr(
            import_job::Column::EntriesSucceeded,
            monotonic_counter(import_job::Column::EntriesSucceeded, entries_succeeded),
        )
        .col_expr(
            import_job::Column::EntriesFailed,
            monotonic_counter(import_job::Column::EntriesFailed, entries_failed),
        )
        .col_expr(
            import_job::Column::DecompressedBytes,
            monotonic_counter(import_job::Column::DecompressedBytes, decompressed_bytes),
        )
        .col_expr(import_job::Column::UpdatedAt, Expr::value(now))
        .filter(active_claim_condition(
            job_id,
            worker_id,
            claim_epoch,
            db.get_database_backend(),
        ))
        .filter(import_job::Column::Attempts.eq(attempt))
        .exec(db)
        .await?;
    require_active_claim(updated.rows_affected)
}

#[allow(clippy::too_many_arguments)]
pub async fn retry<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    worker_id: &str,
    claim_epoch: i64,
    attempt: u32,
    next_attempt_at: DateTime<Utc>,
    failure: &ImportFailure,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let attempt = u32_to_i32(attempt, "attempt")?;
    let updated = import_job::Entity::update_many()
        .col_expr(import_job::Column::State, Expr::value(STATE_QUEUED))
        .col_expr(import_job::Column::Phase, Expr::value(PHASE_QUEUED))
        .col_expr(
            import_job::Column::NextAttemptAt,
            Expr::value(next_attempt_at),
        )
        .col_expr(
            import_job::Column::LockedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            import_job::Column::LockedUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(import_job::Column::DownloadedBytes, Expr::value(0))
        .col_expr(
            import_job::Column::DownloadTotal,
            Expr::value(Option::<i64>::None),
        )
        .col_expr(import_job::Column::IpfsAddBytes, Expr::value(0))
        .col_expr(
            import_job::Column::FailureCode,
            Expr::value(Some(failure.code.as_str().to_owned())),
        )
        .col_expr(
            import_job::Column::FailureMessage,
            Expr::value(Some(failure.message.clone())),
        )
        .col_expr(import_job::Column::UpdatedAt, Expr::value(now))
        .filter(active_claim_condition(
            job_id,
            worker_id,
            claim_epoch,
            db.get_database_backend(),
        ))
        .filter(import_job::Column::Attempts.eq(attempt))
        .exec(db)
        .await?;
    require_active_claim(updated.rows_affected)
}

pub async fn get_for_path<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    bucket: &str,
    key: &str,
) -> AppResult<Option<import_job::Model>> {
    Ok(import_job::Entity::find()
        .filter(import_job::Column::Id.eq(job_id))
        .filter(import_job::Column::Bucket.eq(bucket))
        .filter(import_job::Column::Key.eq(key))
        .one(db)
        .await?)
}

/// Removes only terminal jobs whose durable completion timestamp predates `cutoff`.
/// Cascading foreign keys remove per-job results, targets, and prefix claims; destination
/// generations survive because their owner reference uses `ON DELETE SET NULL`.
pub async fn delete_terminal_before<C: ConnectionTrait>(
    db: &C,
    cutoff: DateTime<Utc>,
) -> AppResult<u64> {
    let deleted = import_job::Entity::delete_many()
        .filter(import_job::Column::State.is_in([STATE_COMPLETED, STATE_FAILED, STATE_SUPERSEDED]))
        .filter(import_job::Column::CompletedAt.is_not_null())
        .filter(import_job::Column::CompletedAt.lt(cutoff))
        .exec(db)
        .await?;
    Ok(deleted.rows_affected)
}

#[allow(dead_code)]
fn canonical_source(source: ImportSource) -> AppResult<(String, String)> {
    match source {
        ImportSource::Cid(value) => {
            let cid = value
                .parse::<cid::Cid>()
                .map_err(|_| AppError::InvalidImportRequest)?;
            Ok(("cid".to_owned(), cid.to_string()))
        }
        ImportSource::Url(value) => Ok(("url".to_owned(), value.to_string())),
    }
}

#[allow(dead_code)]
fn deterministic_metadata_json(metadata: &HashMap<String, String>) -> AppResult<String> {
    let ordered = metadata.iter().collect::<BTreeMap<_, _>>();
    serde_json::to_string(&ordered)
        .map_err(|_| AppError::Internal("failed to serialize import metadata".to_owned()))
}

#[allow(dead_code)]
fn deterministic_tags_json(mut tags: Vec<ObjectTag>) -> AppResult<String> {
    validate_tag_set(&tags).map_err(|_| AppError::InvalidImportRequest)?;
    tags.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| left.value.cmp(&right.value))
    });
    serde_json::to_string(&tags)
        .map_err(|_| AppError::Internal("failed to serialize import tags".to_owned()))
}

fn due_claim_condition(now: DateTime<Utc>) -> Condition {
    Condition::any()
        .add(
            Condition::all()
                .add(import_job::Column::State.eq(STATE_QUEUED))
                .add(import_job::Column::NextAttemptAt.lte(now))
                .add(import_job::Column::LockedBy.is_null())
                .add(import_job::Column::LockedUntil.is_null()),
        )
        .add(
            Condition::all()
                .add(import_job::Column::State.eq(STATE_RUNNING))
                .add(import_job::Column::LockedUntil.lte(now)),
        )
}

fn active_claim_condition(
    job_id: &str,
    worker_id: &str,
    claim_epoch: i64,
    backend: DatabaseBackend,
) -> Condition {
    Condition::all()
        .add(import_job::Column::Id.eq(job_id))
        .add(import_job::Column::LockedBy.eq(worker_id))
        .add(import_job::Column::ClaimEpoch.eq(claim_epoch))
        .add(import_job::Column::State.eq(STATE_RUNNING))
        .add(lease_clock::active_lease(backend))
}

fn monotonic_counter(column: import_job::Column, candidate: i64) -> sea_orm::sea_query::SimpleExpr {
    Expr::case(
        Condition::all().add(column.gt(candidate)),
        Expr::col(column),
    )
    .finally(Expr::value(candidate))
    .into()
}

fn monotonic_optional(
    column: import_job::Column,
    candidate: Option<i64>,
) -> sea_orm::sea_query::SimpleExpr {
    let Some(candidate) = candidate else {
        return Expr::col(column).into();
    };
    Expr::case(
        Condition::any()
            .add(column.is_null())
            .add(column.lt(candidate)),
        Expr::value(candidate),
    )
    .finally(Expr::col(column))
    .into()
}

fn u64_to_i64(value: u64, field: &str) -> AppResult<i64> {
    i64::try_from(value)
        .map_err(|_| AppError::Internal(format!("import {field} exceeds database range")))
}

fn u32_to_i32(value: u32, field: &str) -> AppResult<i32> {
    i32::try_from(value)
        .map_err(|_| AppError::Internal(format!("import {field} exceeds database range")))
}

fn require_active_claim(rows_affected: u64) -> AppResult<()> {
    if rows_affected == 1 {
        Ok(())
    } else {
        Err(AppError::StaleImportOwnership)
    }
}

fn is_sqlite_contention(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("database is locked") || error.contains("database is busy")
}

async fn sqlite_claim_retry_delay(attempt: usize) {
    let milliseconds = 1_u64.checked_shl(attempt.min(4) as u32).unwrap_or(16);
    tokio::time::sleep(Duration::from_millis(milliseconds)).await;
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::LazyLock};

    use chrono::{DateTime, Duration, Utc};
    use sea_orm::{
        ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
        EntityTrait, PaginatorTrait, Set, Statement, TransactionTrait,
    };

    use super::*;
    use crate::{
        error::AppError,
        import::{
            ImportFailure, ImportFailureCode, ImportPhase, ImportProgress, ImportSource,
            ImportState,
        },
        pinning::tags::ObjectTag,
        store::entities::{import_destination, import_job, import_job_result},
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
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        db
    }

    async fn setup_file_backed(name: &str) -> (tempfile::TempDir, DatabaseConnection) {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join(name);
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        crate::store::apply_sqlite_busy_timeout(&mut options);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        (directory, db)
    }

    async fn hold_job_write_lock(
        db: &DatabaseConnection,
        id: &str,
    ) -> sea_orm::DatabaseTransaction {
        let holder = db.begin().await.unwrap();
        holder
            .execute(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!(
                    "UPDATE import_jobs SET updated_at = updated_at WHERE id = '{}'",
                    id.replace('\'', "''")
                ),
            ))
            .await
            .unwrap();
        holder
    }

    async fn wait_past(lease_until: DateTime<Utc>) {
        let remaining = (lease_until - Utc::now())
            .to_std()
            .unwrap_or(std::time::Duration::ZERO);
        tokio::time::sleep(remaining + std::time::Duration::from_millis(75)).await;
    }

    fn request(id: &str, key: &str) -> NewImportJob {
        NewImportJob {
            id: id.to_owned(),
            bucket: "bucket".to_owned(),
            key: key.to_owned(),
            source: ImportSource::Cid(
                "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_owned(),
            ),
            request_fingerprint: format!("fingerprint-{id}"),
            client_token: None,
            object_content_type: Some("application/octet-stream".to_owned()),
            metadata: HashMap::from([
                ("zebra".to_owned(), "last".to_owned()),
                ("apple".to_owned(), "first".to_owned()),
            ]),
            tags: vec![
                ObjectTag::new("zebra", "last"),
                ObjectTag::new("apple", "first"),
            ],
            decompress_prefix: None,
        }
    }

    fn url_request(id: &str, key: &str) -> NewImportJob {
        let mut request = request(id, key);
        request.source =
            ImportSource::Url(url::Url::parse("https://downloads.example.test/object").unwrap());
        request
    }

    async fn persisted(db: &DatabaseConnection, id: &str) -> import_job::Model {
        import_job::Entity::find_by_id(id.to_owned())
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    async fn claim_one(
        db: &DatabaseConnection,
        worker_id: &str,
        now: DateTime<Utc>,
    ) -> ClaimedImportJob {
        claim_due(db, worker_id, now, now + Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap()
    }

    #[tokio::test]
    async fn insert_serializes_canonical_source_metadata_and_tags_deterministically() {
        let db = setup().await;
        let now = time(0);

        let inserted = insert_queued(&db, request("job-1", "key"), now)
            .await
            .unwrap();

        assert_eq!(inserted.source_type, "cid");
        assert_eq!(
            inserted.source_value,
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
        );
        assert_eq!(
            inserted.metadata_json,
            r#"{"apple":"first","zebra":"last"}"#
        );
        assert_eq!(
            inserted.tags_json,
            r#"[{"key":"apple","value":"first"},{"key":"zebra","value":"last"}]"#
        );
        assert_eq!(inserted.state, "queued");
        assert_eq!(inserted.phase, "queued");
        assert_eq!(inserted.attempts, 0);
        assert_eq!(inserted.claim_epoch, 0);
        assert_eq!(inserted.downloaded_bytes, 0);
        assert_eq!(inserted.download_total, None);
        assert_eq!(inserted.ipfs_add_bytes, 0);
    }

    #[tokio::test]
    async fn decision_from_another_job_cannot_create_a_partial_import_admission() {
        use crate::{
            config::PinningConfig,
            pinning::{
                config::ValidatedPinningConfig,
                decision::DecisionOrigin,
                policy::{PinPolicyEvaluator, PublicationContext},
            },
            store::{entities::import_destination, import::ownership},
        };
        let db = setup().await;
        let request = request("job-1", "key");
        let config = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
        let (_, decision) = PinPolicyEvaluator::new(&config)
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "bucket",
                    key: "key",
                    tags: &request.tags,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("test", "different-job"),
            )
            .unwrap();
        assert!(matches!(
            ownership::submit_decided(&db, request, decision, time(0)).await,
            Err(AppError::InvalidImportRequest)
        ));
        assert_eq!(import_job::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(
            import_destination::Entity::find().count(&db).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn due_claims_are_fairly_ordered_limited_and_epoch_fenced_on_reclaim() {
        let db = setup().await;
        let now = time(0);
        for (id, at) in [
            ("job-c", time(-2)),
            ("job-b", time(-1)),
            ("job-a", time(-1)),
        ] {
            insert_queued(&db, request(id, id), at).await.unwrap();
        }

        let first = claim_due(&db, "worker-1", now, time(30), 2).await.unwrap();
        assert_eq!(
            first
                .iter()
                .map(|claim| claim.job.id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-c", "job-a"],
        );
        assert_eq!(
            first
                .iter()
                .map(|claim| (claim.claim.attempt, claim.claim.claim_epoch))
                .collect::<Vec<_>>(),
            vec![(1, 1), (1, 1)],
        );

        let only = claim_one(&db, "worker-2", time(31)).await;
        assert_eq!(only.job.id, "job-c");
        assert_eq!(only.claim.attempt, 2);
        assert_eq!(only.claim.claim_epoch, 2);
        assert_eq!(only.job.locked_by.as_deref(), Some("worker-2"));
        assert_eq!(only.job.locked_until, Some(time(61)));

        assert_eq!(persisted(&db, "job-c").await.claim_epoch, 2);
        assert_eq!(persisted(&db, "job-c").await.attempts, 2);
    }

    #[tokio::test]
    async fn claim_due_locks_by_ascending_id_but_returns_due_order() {
        let db = setup().await;
        let now = time(0);
        insert_queued(&db, request("job-z", "later-id"), time(-2))
            .await
            .unwrap();
        insert_queued(&db, request("job-a", "earlier-id"), time(-1))
            .await
            .unwrap();

        let candidates = import_job::Entity::find()
            .filter(due_claim_condition(now))
            .order_by_asc(import_job::Column::NextAttemptAt)
            .order_by_asc(import_job::Column::CreatedAt)
            .order_by_asc(import_job::Column::Id)
            .limit(2)
            .all(&db)
            .await
            .unwrap();
        let lock_order = candidates_in_lock_order(candidates)
            .into_iter()
            .map(|(_, candidate)| candidate.id)
            .collect::<Vec<_>>();
        assert_eq!(lock_order, ["job-a", "job-z"]);

        let claimed = claim_due(&db, "worker", now, time(30), 2).await.unwrap();
        assert_eq!(
            claimed
                .iter()
                .map(|claim| claim.job.id.as_str())
                .collect::<Vec<_>>(),
            ["job-z", "job-a"],
        );
    }

    #[tokio::test]
    async fn concurrent_claimants_use_conditional_updates_to_produce_one_epoch() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("import-job-claims.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(4);
        let db = Database::connect(options).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        let now = time(0);
        insert_queued(&db, request("job-1", "key"), now)
            .await
            .unwrap();

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first_db = db.clone();
        let first_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            claim_due(&first_db, "worker-1", now, time(30), 1).await
        });
        let second_db = db.clone();
        let second = tokio::spawn(async move {
            barrier.wait().await;
            claim_due(&second_db, "worker-2", now, time(30), 1).await
        });

        let (first, second) = tokio::join!(first, second);
        let claims = [first.unwrap().unwrap(), second.unwrap().unwrap()];
        assert_eq!(claims.iter().map(Vec::len).sum::<usize>(), 1);
        let returned_epochs = claims
            .iter()
            .flat_map(|claimed| claimed.iter())
            .map(|claimed| (claimed.job.id.as_str(), claimed.claim.claim_epoch))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(returned_epochs.len(), 1);
        let row = persisted(&db, "job-1").await;
        assert_eq!(row.attempts, 1);
        assert_eq!(row.claim_epoch, 1);
        assert_eq!(row.state, "running");
    }

    #[tokio::test]
    async fn lease_expiring_at_reclaim_time_is_reclaimable_and_fences_old_claim() {
        let db = setup().await;
        let now = time(0);
        insert_queued(&db, request("job-1", "key"), now)
            .await
            .unwrap();
        let first = claim_one(&db, "worker-1", now).await;

        assert!(
            renew_claim(
                &db,
                "job-1",
                "worker-1",
                first.claim.claim_epoch,
                time(10),
                time(40)
            )
            .await
            .unwrap()
        );
        assert!(
            renew_claim(
                &db,
                "job-1",
                "worker-1",
                first.claim.claim_epoch,
                time(40),
                time(70)
            )
            .await
            .unwrap()
        );

        let reclaimed = claim_one(&db, "worker-2", time(70)).await;
        assert_eq!(reclaimed.claim.claim_epoch, first.claim.claim_epoch + 1);

        assert!(
            !renew_claim(
                &db,
                "job-1",
                "worker-1",
                first.claim.claim_epoch,
                time(70),
                time(71)
            )
            .await
            .unwrap()
        );
        for stale in [
            update_phase(
                &db,
                "job-1",
                "worker-1",
                first.claim.claim_epoch,
                ImportState::Running,
                ImportPhase::Downloading,
                time(40),
            )
            .await,
            update_progress(
                &db,
                "job-1",
                "worker-1",
                first.claim.claim_epoch,
                first.claim.attempt,
                &ImportProgress::default(),
                time(40),
            )
            .await,
            retry(
                &db,
                "job-1",
                "worker-1",
                first.claim.claim_epoch,
                first.claim.attempt,
                time(50),
                &ImportFailure {
                    code: ImportFailureCode::SourceUnreachable,
                    message: "transient source failure".to_owned(),
                    retryable: true,
                },
                time(40),
            )
            .await,
        ] {
            assert!(matches!(stale, Err(AppError::StaleImportOwnership)));
        }
    }

    #[tokio::test]
    async fn sqlite_execution_clock_fences_renew_phase_progress_and_retry_after_lock_wait() {
        let (_directory, db) = setup_file_backed("import-job-execution-clock.sqlite").await;

        let now = Utc::now();
        insert_queued(&db, request("renew-expiry", "renew-expiry"), now)
            .await
            .unwrap();
        let lease_until = now + Duration::milliseconds(200);
        let claim = claim_due(&db, "worker", now, lease_until, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let holder = hold_job_write_lock(&db, "renew-expiry").await;
        let worker_db = db.clone();
        let worker_claim = claim.claim.clone();
        let mut operation = tokio::spawn(async move {
            renew_claim(
                &worker_db,
                &worker_claim.job_id,
                &worker_claim.worker_id,
                worker_claim.claim_epoch,
                now,
                now + Duration::seconds(30),
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut operation)
                .await
                .is_err(),
            "renew must be blocked by the real SQLite writer"
        );
        wait_past(lease_until).await;
        holder.rollback().await.unwrap();
        assert!(!operation.await.unwrap().unwrap());
        assert_eq!(
            persisted(&db, "renew-expiry").await.locked_until,
            Some(lease_until)
        );

        let (_directory, db) = setup_file_backed("import-job-phase-clock.sqlite").await;
        let now = Utc::now();
        insert_queued(&db, request("phase-expiry", "phase-expiry"), now)
            .await
            .unwrap();
        let lease_until = now + Duration::milliseconds(200);
        let claim = claim_due(&db, "worker", now, lease_until, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let holder = hold_job_write_lock(&db, "phase-expiry").await;
        let worker_db = db.clone();
        let worker_claim = claim.claim.clone();
        let mut operation = tokio::spawn(async move {
            update_phase(
                &worker_db,
                &worker_claim.job_id,
                &worker_claim.worker_id,
                worker_claim.claim_epoch,
                ImportState::Running,
                ImportPhase::Downloading,
                now,
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut operation)
                .await
                .is_err(),
            "phase update must be blocked by the real SQLite writer"
        );
        wait_past(lease_until).await;
        holder.rollback().await.unwrap();
        assert!(matches!(
            operation.await.unwrap(),
            Err(AppError::StaleImportOwnership)
        ));
        assert_eq!(persisted(&db, "phase-expiry").await.phase, PHASE_QUEUED);

        let (_directory, db) = setup_file_backed("import-job-progress-clock.sqlite").await;
        let now = Utc::now();
        insert_queued(&db, request("progress-expiry", "progress-expiry"), now)
            .await
            .unwrap();
        let lease_until = now + Duration::milliseconds(200);
        let claim = claim_due(&db, "worker", now, lease_until, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let holder = hold_job_write_lock(&db, "progress-expiry").await;
        let worker_db = db.clone();
        let worker_claim = claim.claim.clone();
        let mut operation = tokio::spawn(async move {
            update_progress(
                &worker_db,
                &worker_claim.job_id,
                &worker_claim.worker_id,
                worker_claim.claim_epoch,
                worker_claim.attempt,
                &ImportProgress {
                    downloaded_bytes: 9,
                    ..ImportProgress::default()
                },
                now,
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut operation)
                .await
                .is_err(),
            "progress update must be blocked by the real SQLite writer"
        );
        wait_past(lease_until).await;
        holder.rollback().await.unwrap();
        assert!(matches!(
            operation.await.unwrap(),
            Err(AppError::StaleImportOwnership)
        ));
        assert_eq!(persisted(&db, "progress-expiry").await.downloaded_bytes, 0);

        let (_directory, db) = setup_file_backed("import-job-retry-clock.sqlite").await;
        let now = Utc::now();
        insert_queued(&db, request("retry-expiry", "retry-expiry"), now)
            .await
            .unwrap();
        let lease_until = now + Duration::milliseconds(200);
        let claim = claim_due(&db, "worker", now, lease_until, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let holder = hold_job_write_lock(&db, "retry-expiry").await;
        let worker_db = db.clone();
        let worker_claim = claim.claim.clone();
        let mut operation = tokio::spawn(async move {
            retry(
                &worker_db,
                &worker_claim.job_id,
                &worker_claim.worker_id,
                worker_claim.claim_epoch,
                worker_claim.attempt,
                now + Duration::seconds(30),
                &ImportFailure {
                    code: ImportFailureCode::SourceUnreachable,
                    message: "retryable".to_owned(),
                    retryable: true,
                },
                now,
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut operation)
                .await
                .is_err(),
            "retry update must be blocked by the real SQLite writer"
        );
        wait_past(lease_until).await;
        holder.rollback().await.unwrap();
        assert!(matches!(
            operation.await.unwrap(),
            Err(AppError::StaleImportOwnership)
        ));
        let row = persisted(&db, "retry-expiry").await;
        assert_eq!(row.state, STATE_RUNNING);
        assert_eq!(row.failure_code, None);
    }

    #[tokio::test]
    async fn expired_url_lease_reclaim_resets_attempt_local_transfer_progress() {
        let db = setup().await;
        let now = time(0);
        insert_queued(&db, url_request("url-reclaim", "url-key"), now)
            .await
            .unwrap();
        let first = claim_one(&db, "worker-1", now).await;
        update_progress(
            &db,
            "url-reclaim",
            "worker-1",
            first.claim.claim_epoch,
            first.claim.attempt,
            &ImportProgress {
                providers_observed: 2,
                pin_nodes_processed: 3,
                pin_bytes_processed: 4,
                downloaded_bytes: 5,
                download_total: Some(9),
                ipfs_add_bytes: 5,
                ..ImportProgress::default()
            },
            time(1),
        )
        .await
        .unwrap();

        let reclaimed = claim_one(&db, "worker-2", time(30)).await;
        assert_eq!(reclaimed.job.source_type, "url");
        assert_eq!(reclaimed.claim.attempt, 2);
        assert_eq!(reclaimed.job.downloaded_bytes, 0);
        assert_eq!(reclaimed.job.download_total, None);
        assert_eq!(reclaimed.job.ipfs_add_bytes, 0);
        assert_eq!(reclaimed.job.providers_observed, 2);
        assert_eq!(reclaimed.job.pin_nodes_processed, 3);
        assert_eq!(reclaimed.job.pin_bytes_processed, 4);

        let row = persisted(&db, "url-reclaim").await;
        assert_eq!(row.downloaded_bytes, 0);
        assert_eq!(row.download_total, None);
        assert_eq!(row.ipfs_add_bytes, 0);
        assert_eq!(row.providers_observed, 2);
        assert_eq!(row.pin_nodes_processed, 3);
    }

    #[tokio::test]
    async fn progress_is_monotonic_per_attempt_and_rejects_u64_overflow() {
        let db = setup().await;
        let now = time(0);
        insert_queued(&db, request("job-1", "key"), now)
            .await
            .unwrap();
        let claimed = claim_one(&db, "worker", now).await;
        let progress = ImportProgress {
            providers_observed: 3,
            pin_nodes_processed: 4,
            pin_bytes_processed: 5,
            downloaded_bytes: 6,
            download_total: Some(10),
            ipfs_add_bytes: 7,
            logical_size: Some(8),
            entries_processed: 9,
            entries_succeeded: 8,
            entries_failed: 1,
            decompressed_bytes: 10,
        };
        update_progress(
            &db,
            "job-1",
            "worker",
            claimed.claim.claim_epoch,
            claimed.claim.attempt,
            &progress,
            time(1),
        )
        .await
        .unwrap();
        update_progress(
            &db,
            "job-1",
            "worker",
            claimed.claim.claim_epoch,
            claimed.claim.attempt,
            &ImportProgress::default(),
            time(2),
        )
        .await
        .unwrap();

        let row = persisted(&db, "job-1").await;
        assert_eq!(row.providers_observed, 3);
        assert_eq!(row.pin_nodes_processed, 4);
        assert_eq!(row.downloaded_bytes, 6);
        assert_eq!(row.download_total, Some(10));
        assert_eq!(row.logical_size, Some(8));
        assert_eq!(row.decompressed_bytes, 10);

        let overflow = ImportProgress {
            downloaded_bytes: u64::MAX,
            ..progress
        };
        assert!(
            update_progress(
                &db,
                "job-1",
                "worker",
                claimed.claim.claim_epoch,
                claimed.claim.attempt,
                &overflow,
                time(3),
            )
            .await
            .is_err()
        );
        assert_eq!(persisted(&db, "job-1").await.downloaded_bytes, 6);
    }

    #[tokio::test]
    async fn retry_releases_the_claim_resets_url_progress_and_retains_destination_ownership() {
        let db = setup().await;
        let now = time(0);
        insert_queued(&db, request("job-1", "key"), now)
            .await
            .unwrap();
        let claimed = claim_one(&db, "worker", now).await;
        update_progress(
            &db,
            "job-1",
            "worker",
            claimed.claim.claim_epoch,
            claimed.claim.attempt,
            &ImportProgress {
                downloaded_bytes: 12,
                download_total: Some(20),
                ipfs_add_bytes: 11,
                ..ImportProgress::default()
            },
            time(1),
        )
        .await
        .unwrap();
        import_destination::Entity::insert(import_destination::ActiveModel {
            bucket: Set("bucket".to_owned()),
            key: Set("key".to_owned()),
            generation: Set(1),
            owner_job_id: Set(Some("job-1".to_owned())),
            mutation_id: Set(None),
            mutation_prefix: Set(None),
            updated_at: Set(now),
        })
        .exec(&db)
        .await
        .unwrap();

        retry(
            &db,
            "job-1",
            "worker",
            claimed.claim.claim_epoch,
            claimed.claim.attempt,
            time(30),
            &ImportFailure {
                code: ImportFailureCode::SourceUnreachable,
                message: "transient source failure".to_owned(),
                retryable: true,
            },
            time(2),
        )
        .await
        .unwrap();

        let row = persisted(&db, "job-1").await;
        assert_eq!(row.state, "queued");
        assert_eq!(row.phase, "queued");
        assert_eq!(row.locked_by, None);
        assert_eq!(row.locked_until, None);
        assert_eq!(row.downloaded_bytes, 0);
        assert_eq!(row.download_total, None);
        assert_eq!(row.ipfs_add_bytes, 0);
        assert_eq!(row.failure_code.as_deref(), Some("source_unreachable"));
        assert_eq!(
            row.failure_message.as_deref(),
            Some("transient source failure")
        );
        assert_eq!(
            import_destination::Entity::find_by_id(("bucket".to_owned(), "key".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .owner_job_id
                .as_deref(),
            Some("job-1")
        );
    }

    #[tokio::test]
    async fn path_lookup_requires_all_three_identifiers() {
        let db = setup().await;
        insert_queued(&db, request("job-1", "key"), time(0))
            .await
            .unwrap();

        assert!(
            get_for_path(&db, "job-1", "bucket", "key")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            get_for_path(&db, "job-1", "other-bucket", "key")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            get_for_path(&db, "job-1", "bucket", "other-key")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn terminal_retention_deletes_only_old_terminal_jobs_and_cascades_children() {
        let db = setup().await;
        let now = time(100);
        for id in [
            "old-completed",
            "old-failed",
            "old-superseded",
            "recent-completed",
            "queued",
            "running",
        ] {
            insert_queued(&db, request(id, id), time(0)).await.unwrap();
        }
        db.execute_unprepared(
            "UPDATE import_jobs SET state = 'completed', completed_at = '2026-07-29T00:00:00Z' WHERE id = 'old-completed'; \
             UPDATE import_jobs SET state = 'failed', completed_at = '2026-07-29T00:00:00Z' WHERE id = 'old-failed'; \
             UPDATE import_jobs SET state = 'superseded', completed_at = '2026-07-29T00:00:00Z' WHERE id = 'old-superseded'; \
             UPDATE import_jobs SET state = 'completed', completed_at = '2026-07-29T00:01:39Z' WHERE id = 'recent-completed'; \
             UPDATE import_jobs SET state = 'running' WHERE id = 'running'",
        )
        .await
        .unwrap();
        import_job_result::Entity::insert(import_job_result::ActiveModel {
            job_id: Set("old-completed".to_owned()),
            sequence: Set(0),
            key: Set("key".to_owned()),
            cid: Set(Some("bafy-result".to_owned())),
            size: Set(Some(1)),
            error_code: Set(None),
            error_message: Set(None),
        })
        .exec(&db)
        .await
        .unwrap();
        import_destination::Entity::insert(import_destination::ActiveModel {
            bucket: Set("bucket".to_owned()),
            key: Set("old-completed".to_owned()),
            generation: Set(1),
            owner_job_id: Set(Some("old-completed".to_owned())),
            mutation_id: Set(None),
            mutation_prefix: Set(None),
            updated_at: Set(now),
        })
        .exec(&db)
        .await
        .unwrap();

        let cutoff = DateTime::parse_from_rfc3339("2026-07-29T00:01:39Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(delete_terminal_before(&db, cutoff).await.unwrap(), 3);
        for id in ["old-completed", "old-failed", "old-superseded"] {
            assert!(
                import_job::Entity::find_by_id(id.to_owned())
                    .one(&db)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        for id in ["recent-completed", "queued", "running"] {
            assert!(
                import_job::Entity::find_by_id(id.to_owned())
                    .one(&db)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert!(
            import_job_result::Entity::find_by_id(("old-completed".to_owned(), 0))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            import_destination::Entity::find_by_id((
                "bucket".to_owned(),
                "old-completed".to_owned()
            ))
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .generation,
            1,
        );
    }
}
