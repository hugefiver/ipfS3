use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, TransactionError, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    pinning::{
        config::{ProviderLimitMap, ProviderMode},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::{ContentMode, ObjectTag},
    },
    store::{
        entities::{
            bucket, import_job_result, multipart_upload, object, pin_lease, pin_lease_target,
            remote_pin,
        },
        import::ownership::{
            ImportPublicationGuard, StandardMutationGuard, complete_publication_in_transaction,
            complete_standard_mutation_in_transaction, lock_bucket_for_ownership,
            verify_publication_guard, verify_standard_mutation_guard,
        },
        multipart::{CommitCompletedUploadError, ReconciledCommitOutcome},
        object::LatestObjectRow,
    },
};

use super::{jobs, leases, quota, tags};

const MAX_TRANSACTION_RETRIES: usize = 3;
const LEASE_ACTIVE: &str = "active";
const TARGET_WAITING: &str = "waiting";
const TARGET_QUOTA_WAITING: &str = "quota_waiting";
const TARGET_QUOTA_BLOCKED: &str = "quota_blocked";
const REMOTE_FAILED: &str = "failed";

#[cfg(test)]
pub(crate) mod test_gates {
    use std::sync::{Arc, LazyLock};

    use tokio::sync::{Mutex, Notify};

    pub static IMPORT_COMPLETION_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    pub static IMPORT_BEFORE_COMPLETION: LazyLock<Mutex<Option<Arc<ImportCompletionGate>>>> =
        LazyLock::new(|| Mutex::new(None));

    pub struct ImportCompletionGate {
        pub job_id: String,
        pub arrived: Notify,
        pub resume: Notify,
    }
}

#[derive(Debug, Clone)]
pub struct PublicationObject {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub cid: String,
    pub logical_size: i64,
    pub content_type: Option<String>,
    pub etag: String,
    pub metadata: Option<serde_json::Value>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub sse_c_key_fingerprint: Option<String>,
    pub multipart: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinTargetSpec {
    pub cid: String,
    pub logical_size: i64,
}

#[derive(Debug, Clone)]
pub struct PublicationRequest {
    pub object: PublicationObject,
    pub tags: Vec<ObjectTag>,
    pub policy: PublicationPolicy,
    pub object_target: PinTargetSpec,
}

#[derive(Debug, Clone)]
pub struct ZipPublicationRequest {
    pub archive: PublicationRequest,
    pub entries: Vec<PublicationObject>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationResult {
    pub object_id: String,
}

#[allow(clippy::too_many_arguments)]
impl PublicationObject {
    pub fn from_put(
        id: String,
        bucket: &str,
        key: &str,
        cid: String,
        logical_size: i64,
        content_type: Option<String>,
        metadata: Option<serde_json::Value>,
        encrypted: bool,
        key_wrap: Option<String>,
        sse_c_key_fingerprint: Option<String>,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            etag: cid.clone(),
            cid,
            logical_size,
            content_type,
            metadata,
            encrypted,
            key_wrap,
            sse_c_key_fingerprint,
            multipart: false,
            created_at,
        }
    }

    fn latest_row(&self) -> LatestObjectRow {
        LatestObjectRow {
            id: self.id.clone(),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            cid: self.cid.clone(),
            size: self.logical_size,
            content_type: self.content_type.clone(),
            etag: self.etag.clone(),
            metadata: self.metadata.clone(),
            encrypted: self.encrypted,
            key_wrap: self.key_wrap.clone(),
            sse_c_key_fingerprint: self.sse_c_key_fingerprint.clone(),
            multipart: self.multipart,
            created_at: self.created_at,
        }
    }
}

pub async fn publish_object(
    db: &DatabaseConnection,
    request: PublicationRequest,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    run_publication_with_retries(
        db,
        request,
        Vec::new(),
        None,
        None,
        None,
        Vec::new(),
        None,
        limits,
    )
    .await
}

/// Publishes an admitted standard exact-key mutation only while its durable
/// admission token is still current.
pub async fn publish_standard_object(
    db: &DatabaseConnection,
    request: PublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    run_publication_with_retries(
        db,
        request,
        Vec::new(),
        None,
        Some(guard),
        None,
        Vec::new(),
        None,
        limits,
    )
    .await
}

pub async fn publish_completed_upload(
    db: &DatabaseConnection,
    upload_id: &str,
    request: PublicationRequest,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries(db, upload_id, request, Vec::new(), None, limits).await
}

pub async fn publish_standard_completed_upload(
    db: &DatabaseConnection,
    upload_id: &str,
    request: PublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries(db, upload_id, request, Vec::new(), Some(guard), limits)
        .await
}

pub async fn publish_zip(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    run_publication_with_retries(
        db,
        request.archive,
        request.entries,
        None,
        None,
        None,
        Vec::new(),
        None,
        limits,
    )
    .await
}

/// Publishes an admitted standard archive and its extracted entries only while
/// the archive's durable prefix token remains current.
pub async fn publish_standard_zip(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    run_publication_with_retries(
        db,
        request.archive,
        request.entries,
        None,
        Some(guard),
        None,
        Vec::new(),
        None,
        limits,
    )
    .await
}

/// Publishes an import archive only if its destination generations, durable
/// targets, and worker lease still match inside the publication transaction.
pub async fn publish_import_object(
    db: &DatabaseConnection,
    request: PublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    run_publication_with_retries(
        db,
        request,
        Vec::new(),
        None,
        None,
        Some(guard),
        result_rows,
        Some(now),
        limits,
    )
    .await
}

/// Publishes an import archive and all successful ZIP entries atomically under
/// one import ownership guard.
pub async fn publish_import_zip(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    run_publication_with_retries(
        db,
        request.archive,
        request.entries,
        None,
        None,
        Some(guard),
        result_rows,
        Some(now),
        limits,
    )
    .await
}

pub async fn publish_completed_zip(
    db: &DatabaseConnection,
    upload_id: &str,
    request: ZipPublicationRequest,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries(
        db,
        upload_id,
        request.archive,
        request.entries,
        None,
        limits,
    )
    .await
}

pub async fn publish_standard_completed_zip(
    db: &DatabaseConnection,
    upload_id: &str,
    request: ZipPublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries(
        db,
        upload_id,
        request.archive,
        request.entries,
        Some(guard),
        limits,
    )
    .await
}

pub async fn reconcile_completed_publication(
    db: &DatabaseConnection,
    upload_id: &str,
    expected_archive: &PublicationObject,
) -> ReconciledCommitOutcome {
    let object = match object::Entity::find_by_id(expected_archive.id.clone())
        .one(db)
        .await
    {
        Ok(Some(object)) => object,
        Ok(None) => return ReconciledCommitOutcome::NotCommitted,
        Err(error) => return ReconciledCommitOutcome::Unknown(error.into()),
    };
    let upload = match multipart_upload::Entity::find_by_id(upload_id.to_owned())
        .one(db)
        .await
    {
        Ok(upload) => upload,
        Err(error) => return ReconciledCommitOutcome::Unknown(error.into()),
    };
    crate::store::multipart::classify_completion_attempt_state(
        &expected_archive.latest_row(),
        Some(&object),
        upload.as_ref(),
    )
}

pub async fn delete_latest_with_leases(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    delete_latest_attempt(db, bucket, key, None, now)
        .await
        .map_err(transaction_error_into_app)
}

pub async fn delete_latest_with_leases_guarded(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    guard: StandardMutationGuard,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    for retry in 0..=MAX_TRANSACTION_RETRIES {
        match delete_latest_attempt(db, bucket, key, Some(guard.clone()), now).await {
            Ok(deleted) => return Ok(deleted),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error) && retry < MAX_TRANSACTION_RETRIES =>
            {
                publication_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("guarded delete retry loop exhausted without returning")
}

async fn delete_latest_attempt(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    guard: Option<StandardMutationGuard>,
    now: DateTime<Utc>,
) -> Result<bool, TransactionError<AppError>> {
    let bucket = bucket.to_owned();
    let key = key.to_owned();
    db.transaction(|txn| {
        Box::pin(async move {
            if let Some(guard) = guard.as_ref() {
                lock_bucket_for_ownership(txn, &bucket).await?;
                verify_standard_mutation_guard(txn, guard, &bucket, &key, &[]).await?;
            }
            let latest_query = object::Entity::find()
                .filter(object::Column::Bucket.eq(bucket.clone()))
                .filter(object::Column::Key.eq(key.clone()))
                .filter(object::Column::IsLatest.eq(true));
            let latest = if txn.get_database_backend() == DatabaseBackend::Postgres {
                latest_query.lock_exclusive().one(txn).await?
            } else {
                latest_query.one(txn).await?
            };
            let Some(latest) = latest else {
                if let Some(guard) = guard.as_ref() {
                    complete_standard_mutation_in_transaction(txn, guard, now).await?;
                }
                return Ok(false);
            };
            let updated = object::Entity::update_many()
                .col_expr(object::Column::IsLatest, false.into())
                .filter(object::Column::Id.eq(&latest.id))
                .filter(object::Column::IsLatest.eq(true))
                .exec(txn)
                .await?;
            if updated.rows_affected != 1 {
                return Err(AppError::Database(
                    "latest object changed during delete".to_owned(),
                ));
            }
            leases::end_active_leases_for_object(txn, &latest.id, now).await?;
            if let Some(guard) = guard.as_ref() {
                complete_standard_mutation_in_transaction(txn, guard, now).await?;
            }
            Ok(true)
        })
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_publication_with_retries(
    db: &DatabaseConnection,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    upload_id: Option<String>,
    standard_guard: Option<StandardMutationGuard>,
    import_guard: Option<ImportPublicationGuard>,
    result_rows: Vec<import_job_result::ActiveModel>,
    import_now: Option<DateTime<Utc>>,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    for retry in 0..=MAX_TRANSACTION_RETRIES {
        match publication_attempt(
            db,
            request.clone(),
            entries.clone(),
            upload_id.clone(),
            standard_guard.clone(),
            import_guard.clone(),
            result_rows.clone(),
            import_now,
            limits.clone(),
        )
        .await
        {
            Ok(result) => return Ok(result),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error) && retry < MAX_TRANSACTION_RETRIES =>
            {
                publication_retry_delay(retry).await;
            }
            Err(error) => return Err(transaction_error_into_app(error)),
        }
    }
    unreachable!("publication retry loop exhausted without returning")
}

async fn run_completed_publication_with_retries(
    db: &DatabaseConnection,
    upload_id: &str,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    standard_guard: Option<StandardMutationGuard>,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    let completion_attempt_id = request.object.id.clone();
    for retry in 0..=MAX_TRANSACTION_RETRIES {
        match publication_attempt(
            db,
            request.clone(),
            entries.clone(),
            Some(upload_id.to_owned()),
            standard_guard.clone(),
            None,
            Vec::new(),
            None,
            limits.clone(),
        )
        .await
        {
            Ok(result) => return Ok(result),
            Err(TransactionError::Transaction(source)) => {
                if is_retryable_transaction_conflict(&source) && retry < MAX_TRANSACTION_RETRIES {
                    publication_retry_delay(retry).await;
                    continue;
                }
                return Err(CommitCompletedUploadError::RolledBack {
                    completion_attempt_id,
                    source,
                });
            }
            Err(TransactionError::Connection(source)) => {
                return Err(CommitCompletedUploadError::OutcomeUnknown {
                    completion_attempt_id,
                    source: source.into(),
                });
            }
        }
    }
    unreachable!("completed publication retry loop exhausted without returning")
}

#[allow(clippy::too_many_arguments)]
async fn publication_attempt(
    db: &DatabaseConnection,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    upload_id: Option<String>,
    standard_guard: Option<StandardMutationGuard>,
    import_guard: Option<ImportPublicationGuard>,
    result_rows: Vec<import_job_result::ActiveModel>,
    import_now: Option<DateTime<Utc>>,
    limits: ProviderLimitMap,
) -> Result<PublicationResult, TransactionError<AppError>> {
    db.transaction(|txn| {
        Box::pin(async move {
            publish_in_transaction(
                txn,
                request,
                entries,
                upload_id.as_deref(),
                standard_guard.as_ref(),
                import_guard.as_ref(),
                result_rows,
                import_now,
                &limits,
            )
            .await
        })
    })
    .await
}

async fn acquire_sqlite_publication_write_intent<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
) -> AppResult<()> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        return Ok(());
    }

    bucket::Entity::update_many()
        .col_expr(
            bucket::Column::Owner,
            Expr::col(bucket::Column::Owner).into(),
        )
        .filter(bucket::Column::Name.eq(bucket_name))
        .exec(db)
        .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn publish_in_transaction<C: ConnectionTrait>(
    db: &C,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    upload_id: Option<&str>,
    standard_guard: Option<&StandardMutationGuard>,
    import_guard: Option<&ImportPublicationGuard>,
    mut result_rows: Vec<import_job_result::ActiveModel>,
    import_now: Option<DateTime<Utc>>,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    validate_request(&request)?;
    for entry in &entries {
        if entry.logical_size < 0 {
            return Err(invalid_publication(
                "logical object size cannot be negative",
            ));
        }
    }
    let attachment_pairs = publication_attachment_pairs(&request, &entries, limits)?;
    if standard_guard.is_some() && import_guard.is_some() {
        return Err(AppError::Internal(
            "publication cannot have both standard and import guards".to_owned(),
        ));
    }
    if let Some(guard) = standard_guard {
        lock_bucket_for_ownership(db, &request.object.bucket).await?;
        let entry_keys = entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>();
        verify_standard_mutation_guard(
            db,
            guard,
            &request.object.bucket,
            &request.object.key,
            &entry_keys,
        )
        .await?;
    } else if let Some(guard) = import_guard {
        lock_bucket_for_ownership(db, &request.object.bucket).await?;
        let publication_targets = publication_locations(&request.object, &entries);
        verify_publication_guard(db, guard, &request.object.bucket, &publication_targets).await?;
    } else {
        acquire_sqlite_publication_write_intent(db, &request.object.bucket).await?;
    }
    let publication_time = import_now.unwrap_or_else(Utc::now);
    let object_id = request.object.id.clone();

    let previous_owner_ids =
        lock_previous_publication_owners(db, &request.object, &entries).await?;
    leases::lock_publication_lifecycle_frontier(db, &previous_owner_ids, &attachment_pairs).await?;
    let attachment_providers: Vec<_> = attachment_pairs
        .iter()
        .map(|(provider, _)| provider.clone())
        .collect();
    quota::lock_publication_usage_rows(db, &attachment_providers).await?;

    for object in ordered_publication_objects(&request.object, &entries) {
        write_object_and_end_previous(db, object, publication_time).await?;
    }
    tags::replace_object_tags(db, &object_id, &request.tags).await?;

    create_publication_leases(db, &request, &entries, limits, publication_time).await?;

    if let Some(upload_id) = upload_id {
        crate::store::multipart::delete_matching_upload_in_transaction(
            db,
            upload_id,
            &request.object.bucket,
            &request.object.key,
        )
        .await?;
    }
    if let Some(guard) = standard_guard {
        complete_standard_mutation_in_transaction(db, guard, Utc::now()).await?;
    }
    if let Some(guard) = import_guard {
        for result in &mut result_rows {
            result.job_id = Set(guard.job_id.clone());
        }
        if !result_rows.is_empty() {
            import_job_result::Entity::insert_many(result_rows)
                .exec(db)
                .await?;
        }
        #[cfg(test)]
        pause_before_import_completion_for_test(&guard.job_id).await;
        complete_publication_in_transaction(
            db,
            guard,
            &request.object.cid,
            request.object.logical_size,
            Utc::now(),
        )
        .await?;
    }
    Ok(PublicationResult { object_id })
}

#[cfg(test)]
async fn pause_before_import_completion_for_test(job_id: &str) {
    let gate = test_gates::IMPORT_BEFORE_COMPLETION.lock().await.clone();
    if let Some(gate) = gate.filter(|gate| gate.job_id == job_id) {
        gate.arrived.notify_one();
        gate.resume.notified().await;
    }
}

fn publication_locations(
    archive: &PublicationObject,
    entries: &[PublicationObject],
) -> Vec<(String, String)> {
    let mut locations = Vec::with_capacity(entries.len() + 1);
    locations.push((archive.bucket.clone(), archive.key.clone()));
    locations.extend(
        entries
            .iter()
            .map(|entry| (entry.bucket.clone(), entry.key.clone())),
    );
    locations
}

fn ordered_publication_objects<'a>(
    archive: &'a PublicationObject,
    entries: &'a [PublicationObject],
) -> Vec<&'a PublicationObject> {
    let mut objects = Vec::with_capacity(entries.len() + 1);
    objects.push(archive);
    objects.extend(entries);
    // Stable location-only ordering closes cross-location lock cycles while preserving the ZIP
    // contract that a later successful entry wins when duplicate entry keys are present.
    objects.sort_by(|left, right| (&left.bucket, &left.key).cmp(&(&right.bucket, &right.key)));
    objects
}

async fn lock_previous_publication_owners<C: ConnectionTrait>(
    db: &C,
    archive: &PublicationObject,
    entries: &[PublicationObject],
) -> AppResult<Vec<String>> {
    let mut locations = BTreeSet::from([(archive.bucket.clone(), archive.key.clone())]);
    locations.extend(
        entries
            .iter()
            .map(|entry| (entry.bucket.clone(), entry.key.clone())),
    );

    let mut owner_ids = Vec::new();
    for (bucket, key) in locations {
        let query = object::Entity::find()
            .filter(object::Column::Bucket.eq(bucket))
            .filter(object::Column::Key.eq(key))
            .filter(object::Column::IsLatest.eq(true));
        let previous = if db.get_database_backend() == DatabaseBackend::Postgres {
            query.lock_exclusive().one(db).await?
        } else {
            query.one(db).await?
        };
        if let Some(previous) = previous {
            owner_ids.push(previous.id);
        }
    }
    owner_ids.sort();
    owner_ids.dedup();
    Ok(owner_ids)
}

fn publication_attachment_pairs(
    request: &PublicationRequest,
    entries: &[PublicationObject],
    limits: &ProviderLimitMap,
) -> AppResult<Vec<(String, String)>> {
    let mut pairs = BTreeSet::new();
    for intent in &request.policy.leases {
        let providers = ordered_enabled_providers(intent, limits)?;
        for target in target_specs(request, entries, intent.content_mode) {
            for provider in &providers {
                pairs.insert((provider.clone(), target.cid.clone()));
            }
        }
    }
    Ok(pairs.into_iter().collect())
}

fn validate_request(request: &PublicationRequest) -> AppResult<()> {
    if request.tags != request.policy.tags {
        return Err(invalid_publication(
            "publication tags do not match evaluated policy tags",
        ));
    }
    if request.object.logical_size < 0 || request.object_target.logical_size < 0 {
        return Err(invalid_publication(
            "logical object size cannot be negative",
        ));
    }
    if request.object_target.cid != request.object.cid
        || request.object_target.logical_size != request.object.logical_size
    {
        return Err(invalid_publication(
            "object target must identify the published object",
        ));
    }
    Ok(())
}

async fn write_object_and_end_previous<C: ConnectionTrait>(
    db: &C,
    publication: &PublicationObject,
    now: DateTime<Utc>,
) -> AppResult<()> {
    if publication.logical_size < 0 {
        return Err(invalid_publication(
            "logical object size cannot be negative",
        ));
    }
    if let Some(previous_id) =
        crate::store::object::write_latest_in_transaction(db, publication.latest_row()).await?
    {
        leases::end_active_leases_for_object(db, &previous_id, now).await?;
    }
    Ok(())
}

async fn create_publication_leases<C: ConnectionTrait>(
    db: &C,
    request: &PublicationRequest,
    entries: &[PublicationObject],
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut affected_remotes = BTreeSet::new();
    let mut failed_user_touches = BTreeSet::new();
    for intent in &request.policy.leases {
        let lease_id = lease_id(&request.object.id, intent.source);
        insert_lease(db, &lease_id, &request.object.id, intent, now).await?;
        let targets = target_specs(request, entries, intent.content_mode);
        for target in targets {
            assign_target_providers(
                db,
                &lease_id,
                intent,
                &target,
                limits,
                now,
                &mut affected_remotes,
                &mut failed_user_touches,
            )
            .await?;
        }
    }

    for (provider, cid) in failed_user_touches {
        leases::reset_failed_remote_retry_on_user_touch(db, &provider, &cid, now).await?;
        ensure_failed_attachment_reconcile(db, &provider, &cid, now).await?;
    }
    for (provider, cid) in affected_remotes {
        project_desired_remote_targets(db, &provider, &cid, now).await?;
    }
    Ok(())
}

async fn insert_lease<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    owner_object_id: &str,
    intent: &LeaseIntent,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let seconds = i64::try_from(intent.duration.as_seconds())
        .map_err(|_| invalid_publication("lease duration is too large"))?;
    let expires_at = now
        .checked_add_signed(TimeDelta::seconds(seconds))
        .ok_or_else(|| invalid_publication("lease expiry is out of range"))?;
    pin_lease::Entity::insert(pin_lease::ActiveModel {
        id: Set(lease_id.to_owned()),
        owner_object_id: Set(owner_object_id.to_owned()),
        source: Set(source_name(intent.source).to_owned()),
        policy_id: Set(intent.policy_id.clone()),
        provider_mode: Set(provider_mode_name(intent.provider_mode).to_owned()),
        content_mode: Set(content_mode_name(intent.content_mode).to_owned()),
        created_at: Set(now),
        last_touched_at: Set(now),
        expires_at: Set(expires_at),
        generation: Set(1),
        state: Set(LEASE_ACTIVE.to_owned()),
    })
    .exec(db)
    .await?;
    Ok(())
}

fn target_specs(
    request: &PublicationRequest,
    entries: &[PublicationObject],
    content_mode: ContentMode,
) -> Vec<PinTargetSpec> {
    match content_mode {
        ContentMode::Object => vec![request.object_target.clone()],
        ContentMode::Decompressed => {
            let mut unique = BTreeMap::new();
            for entry in entries {
                unique
                    .entry(entry.cid.clone())
                    .or_insert(entry.logical_size);
            }
            unique
                .into_iter()
                .map(|(cid, logical_size)| PinTargetSpec { cid, logical_size })
                .collect()
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn assign_target_providers<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    intent: &LeaseIntent,
    target: &PinTargetSpec,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
    affected_remotes: &mut BTreeSet<(String, String)>,
    failed_user_touches: &mut BTreeSet<(String, String)>,
) -> AppResult<()> {
    let providers = ordered_enabled_providers(intent, limits)?;
    match intent.provider_mode {
        ProviderMode::All => {
            for provider in providers {
                reserve_and_insert_target(
                    db,
                    lease_id,
                    &provider,
                    target,
                    limits,
                    now,
                    affected_remotes,
                    failed_user_touches,
                )
                .await?;
            }
        }
        ProviderMode::One => {
            let mut first_unavailable = None;
            for provider in providers {
                let outcome = quota::reserve_unique(
                    db,
                    &provider,
                    &target.cid,
                    target.logical_size,
                    limits,
                    now,
                )
                .await?;
                if matches!(
                    outcome,
                    quota::ReservationOutcome::Reserved | quota::ReservationOutcome::Reused
                ) {
                    let was_failed = remote_is_failed(db, &provider, &target.cid).await?;
                    insert_target(db, lease_id, &provider, target, &outcome, now).await?;
                    record_reserved_target(
                        db,
                        &provider,
                        target,
                        was_failed,
                        affected_remotes,
                        failed_user_touches,
                    )
                    .await?;
                    return Ok(());
                }
                if first_unavailable.is_none() {
                    first_unavailable = Some((provider, outcome));
                }
            }
            let Some((provider, outcome)) = first_unavailable else {
                return Err(invalid_publication("pinning lease has no enabled provider"));
            };
            insert_target(db, lease_id, &provider, target, &outcome, now).await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn reserve_and_insert_target<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    provider: &str,
    target: &PinTargetSpec,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
    affected_remotes: &mut BTreeSet<(String, String)>,
    failed_user_touches: &mut BTreeSet<(String, String)>,
) -> AppResult<()> {
    let outcome =
        quota::reserve_unique(db, provider, &target.cid, target.logical_size, limits, now).await?;
    insert_target(db, lease_id, provider, target, &outcome, now).await?;
    if matches!(
        outcome,
        quota::ReservationOutcome::Reserved | quota::ReservationOutcome::Reused
    ) {
        let was_failed = remote_is_failed(db, provider, &target.cid).await?;
        record_reserved_target(
            db,
            provider,
            target,
            was_failed,
            affected_remotes,
            failed_user_touches,
        )
        .await?;
    }
    Ok(())
}

async fn ensure_failed_attachment_reconcile<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(());
    };
    if remote.status != REMOTE_FAILED {
        return Ok(());
    }
    let jobs::NewPinJob::Remote(job) = jobs::reconcile_job(
        provider,
        cid,
        remote.epoch,
        remote.next_retry_at.unwrap_or(now),
    ) else {
        unreachable!("reconcile constructor is remote scoped")
    };
    jobs::ensure_or_reactivate_reconcile_job(db, job, now).await?;
    Ok(())
}

async fn insert_target<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    provider: &str,
    target: &PinTargetSpec,
    outcome: &quota::ReservationOutcome,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let state = match outcome {
        quota::ReservationOutcome::Reserved | quota::ReservationOutcome::Reused => TARGET_WAITING,
        quota::ReservationOutcome::QuotaWaiting { .. } => TARGET_QUOTA_WAITING,
        quota::ReservationOutcome::QuotaBlocked => TARGET_QUOTA_BLOCKED,
    };
    pin_lease_target::Entity::insert(pin_lease_target::ActiveModel {
        id: Set(leases::target_id(lease_id, provider, &target.cid)),
        lease_id: Set(lease_id.to_owned()),
        cid: Set(target.cid.clone()),
        logical_size: Set(target.logical_size),
        provider: Set(provider.to_owned()),
        state: Set(state.to_owned()),
        created_at: Set(now),
        last_touched_at: Set(now),
    })
    .exec(db)
    .await?;
    Ok(())
}

async fn record_reserved_target<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    target: &PinTargetSpec,
    was_failed: bool,
    affected_remotes: &mut BTreeSet<(String, String)>,
    failed_user_touches: &mut BTreeSet<(String, String)>,
) -> AppResult<()> {
    quota::refresh_remote_max_active_touch(db, provider, &target.cid).await?;
    let pair = (provider.to_owned(), target.cid.clone());
    affected_remotes.insert(pair.clone());
    if was_failed {
        failed_user_touches.insert(pair);
    }
    Ok(())
}

async fn project_desired_remote_targets<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let target_ids = pin_lease_target::Entity::find()
        .inner_join(pin_lease::Entity)
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::Cid.eq(cid))
        .filter(pin_lease_target::Column::State.is_in([
            "waiting",
            "submitted",
            "pinned",
            "degraded",
        ]))
        .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
        .order_by_asc(pin_lease_target::Column::CreatedAt)
        .order_by_asc(pin_lease_target::Column::Id)
        .all(db)
        .await?
        .into_iter()
        .map(|target| target.id)
        .collect::<Vec<_>>();
    for target_id in target_ids {
        leases::project_target_from_remote(db, &target_id, now).await?;
    }
    Ok(())
}

async fn remote_is_failed<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<bool> {
    Ok(
        remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await?
            .is_some_and(|remote| remote.status == REMOTE_FAILED),
    )
}

fn ordered_enabled_providers(
    intent: &LeaseIntent,
    limits: &ProviderLimitMap,
) -> AppResult<Vec<String>> {
    let mut providers = BTreeSet::new();
    for provider in &intent.providers {
        let provider_limits = limits
            .get(provider)
            .ok_or_else(|| invalid_publication("pinning lease references an unknown provider"))?;
        if provider_limits.enabled {
            providers.insert(provider.clone());
        }
    }
    let mut providers = providers.into_iter().collect::<Vec<_>>();
    providers.sort_by(|left, right| {
        limits[left]
            .priority
            .cmp(&limits[right].priority)
            .then_with(|| left.cmp(right))
    });
    if providers.is_empty() {
        return Err(invalid_publication("pinning lease has no enabled provider"));
    }
    Ok(providers)
}

fn lease_id(object_id: &str, source: LeaseSource) -> String {
    format!("lease:{object_id}:{}", source_name(source))
}

fn source_name(source: LeaseSource) -> &'static str {
    match source {
        LeaseSource::Automatic => "automatic",
        LeaseSource::Manual => "manual",
    }
}

fn provider_mode_name(mode: ProviderMode) -> &'static str {
    match mode {
        ProviderMode::One => "one",
        ProviderMode::All => "all",
    }
}

fn content_mode_name(mode: ContentMode) -> &'static str {
    match mode {
        ContentMode::Object => "full",
        ContentMode::Decompressed => "decompressed",
    }
}

fn invalid_publication(message: &str) -> AppError {
    AppError::InvalidPinningRequest(message.to_owned())
}

fn is_retryable_transaction_conflict(error: &AppError) -> bool {
    let AppError::Database(message) = error else {
        return false;
    };
    let message = message.to_ascii_lowercase();

    let stale_lifecycle_compare_and_set =
        message.starts_with("stale ") && message.ends_with(" compare-and-set");
    let sqlite_write_conflict =
        message.contains("database is locked") || message.contains("database is busy");
    let unique_conflict = message.contains("unique constraint failed")
        || message.contains("duplicate key value violates unique constraint")
        || message.contains("unique violation")
        || message.contains("sqlstate 23505")
        || message.contains("code: 23505")
        || message.contains("code: 1555")
        || message.contains("code: 2067");
    let postgres_transaction_conflict = message.contains("deadlock detected")
        || message.contains("could not serialize access")
        || message.contains("serialization failure")
        || message.contains("sqlstate 40p01")
        || message.contains("code: 40p01")
        || message.contains("sqlstate 40001")
        || message.contains("code: 40001");

    stale_lifecycle_compare_and_set
        || sqlite_write_conflict
        || unique_conflict
        || postgres_transaction_conflict
}

async fn publication_retry_delay(retry: usize) {
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
mod tests;
