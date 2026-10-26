use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set, TransactionError, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    lifecycle::model::MultipartUploadTargetIdentity,
    pinning::{
        config::{ProviderLimitMap, ProviderMode},
        decision::ExtensionDecision,
        identity::ProviderRouteSnapshot,
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::{ContentMode, ObjectTag},
    },
    store::{
        entities::{
            bucket, import_job_result, multipart_upload, object, object_version, pin_lease,
            pin_lease_target, remote_pin,
        },
        import::ownership::{
            ImportPublicationGuard, StandardMutationGuard, complete_publication_in_transaction,
            complete_standard_mutation_in_transaction, lock_bucket_for_ownership,
            verify_publication_guard, verify_standard_mutation_guard,
        },
        multipart::{
            AbortExactIncompleteUploadResult, CommitCompletedUploadError, ReconciledCommitOutcome,
        },
        object::LatestObjectRow,
        object_version::{
            BucketVersioningState, DeleteVersionResult, NULL_VERSION_ID, PublicVersionId,
            VersionKind, VersionSelector,
        },
    },
};

use super::{jobs, leases, ledger, quota, tags};

pub(crate) mod application;
mod hot_receipt;
mod reservations;
use reservations::PublicationDatabase;
pub(in crate::store::pinning) use reservations::PublicationTransaction;
pub mod management;
mod zip;
mod zip_v2;

pub use crate::store::zip::execution as v2_execution;
pub use zip::{
    CapturedZipPublication, LegacyZipAuthority, LegacyZipInputProof, ZipExecutionSettlement,
    ZipPublicationAuthority, bind_legacy_import_input, decode_zip_capture_json,
    publish_captured_zip, read_legacy_import_input, serialize_zip_capture,
};
pub use zip_v2::{ZipV2Publication, ZipV2PublicationResult, ZipV2Success, publish_zip_v2};

use hot_receipt::HotPublicationReceipt;

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

/// One successful manifest path bound to the immutable object written by this publication.
/// Failed manifest paths have no corresponding entry or binding.
#[derive(Debug, Clone)]
pub struct ZipBatchEntry {
    pub path: String,
    pub object_id: String,
}

/// Optional durable ZIP batch finalization. The terminal result must be safe
/// serialized JSON, not a raw upload/extraction error or untrusted diagnostic.
#[derive(Debug, Clone)]
pub struct ZipBatchPublication {
    pub batch_id: String,
    pub entries: Vec<ZipBatchEntry>,
    pub source_published: bool,
    pub root_outcome: crate::store::zip::RootOutcome,
    pub terminal_result: String,
}

/// The exact signed Complete request and response representation to seal with
/// the archive version in its publication transaction.
#[derive(Debug, Clone)]
pub struct ZipCompletedReplay {
    pub owner: String,
    pub parts: Vec<s3s::dto::CompletedPart>,
    pub response_xml: String,
    pub server_side_encryption: Option<String>,
    pub pin_warning: Option<String>,
}

impl ZipBatchPublication {
    fn validate(
        &self,
        archive: &PublicationObject,
        entries: &[PublicationObject],
    ) -> AppResult<()> {
        if self.batch_id.is_empty() || !self.source_published || self.entries.len() != entries.len()
        {
            return Err(invalid_publication(
                "ZIP batch must bind every published entry and archive",
            ));
        }
        let mut object_ids = BTreeSet::new();
        for entry in entries {
            if entry.id == archive.id || !object_ids.insert(entry.id.as_str()) {
                return Err(invalid_publication("ZIP batch object IDs must be unique"));
            }
        }
        let mut paths = BTreeSet::new();
        let mut bound_ids = BTreeSet::new();
        for binding in &self.entries {
            if binding.path.is_empty()
                || !paths.insert(binding.path.as_str())
                || !object_ids.contains(binding.object_id.as_str())
                || !bound_ids.insert(binding.object_id.as_str())
            {
                return Err(invalid_publication(
                    "ZIP batch bindings must match unique published objects and paths",
                ));
            }
        }
        Ok(())
    }
}

pub use crate::store::object_version::PublicationResult;

#[derive(Debug)]
pub enum ReconciledPublicationOutcome {
    Committed(PublicationResult),
    NotCommitted,
    Unknown(AppError),
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

/// Stage 3 opt-in path. The decision is captured before upload/MPU/import
/// side effects and stored with the exact content version on successful publish.
/// A changed accepted provider revision is a hard error, never a fresh interpretation of tags.
pub struct DecidedPublish<'a> {
    pub decision: &'a ExtensionDecision,
    pub config: &'a crate::pinning::config::ValidatedPinningConfig,
    pub mode: crate::config::OptionalPinControlMode,
    pub limits: &'a ProviderLimitMap,
}

impl DecidedPublish<'_> {
    fn validate(&self, request: &PublicationRequest) -> AppResult<()> {
        self.decision
            .verify_revision(self.config, self.mode)
            .map_err(invalid_publication)?;
        self.decision
            .validate_policy(&request.policy)
            .map_err(invalid_publication)?;
        Ok(())
    }

    fn snapshot(&self) -> AppResult<DecidedSnapshot> {
        self.decision
            .verify_revision(self.config, self.mode)
            .map_err(invalid_publication)?;
        let mut routes = BTreeMap::new();
        let mut allocatable = BTreeSet::new();
        for name in self
            .decision
            .effective_intents
            .iter()
            .flat_map(|intent| &intent.providers)
        {
            let provider = self
                .config
                .providers
                .iter()
                .find(|provider| &provider.name == name)
                .ok_or_else(|| invalid_publication("captured pinning provider is unavailable"))?;
            if provider.identity.allocation_key() != *name {
                return Err(invalid_publication(
                    "captured provider allocation key changed",
                ));
            }
            let route = provider.route_snapshot();
            if self.limits.get(name).is_some_and(|limit| limit.enabled) {
                allocatable.insert(name.clone());
            }
            if routes
                .insert(name.clone(), route.clone())
                .is_some_and(|old| old != route)
            {
                return Err(invalid_publication("captured provider route is ambiguous"));
            }
        }
        Ok(DecidedSnapshot {
            decision: self.decision.clone(),
            routes,
            allocatable,
        })
    }
}

#[derive(Clone)]
struct DecidedSnapshot {
    decision: ExtensionDecision,
    routes: BTreeMap<String, ProviderRouteSnapshot>,
    allocatable: BTreeSet<String>,
}

/// Cheap fail-fast check before a durable completion/import starts source or Kubo I/O.
/// The publication transaction repeats it under locks; this read is not the fence.
pub async fn preflight_decided_routes(
    db: &DatabaseConnection,
    captured: DecidedPublish<'_>,
) -> AppResult<()> {
    let snapshot = captured.snapshot()?;
    ledger::verify_birth_candidate_routes(db, &snapshot.routes, &snapshot.allocatable, false).await
}

/// Fail fast on an unavailable original transport before ZIP source I/O. Frozen
/// policy/TTL/limits are not reinterpreted; publication repeats the full route
/// fence under its existing locks with exact final bindings.
pub(crate) async fn preflight_captured_zip_routes(
    db: &DatabaseConnection,
    capture: &crate::pinning::zip_policy::CapturedZipIntent,
    current: &crate::pinning::config::ValidatedPinningConfig,
    manifest: Option<&crate::zip::batch::FinalZipManifest>,
) -> AppResult<()> {
    let mut routes = BTreeMap::new();
    let mut flags = BTreeMap::new();
    let mut allocatable = BTreeSet::new();
    for frozen in capture.io_provider_candidates(manifest) {
        let unavailable =
            || invalid_publication("captured pinning configuration revision is unavailable");
        let provider = current
            .providers
            .iter()
            .find(|provider| provider.name == frozen.config_name)
            .ok_or_else(unavailable)?;
        if provider.validate_route_snapshot(&frozen.route).is_err()
            || (frozen.enabled && (!provider.limits.enabled || provider.identity.retired))
        {
            return Err(unavailable());
        }
        if routes
            .insert(frozen.config_name.clone(), frozen.route.clone())
            .is_some_and(|old| old != frozen.route)
            || flags
                .insert(frozen.config_name.clone(), frozen.enabled)
                .is_some_and(|old| old != frozen.enabled)
        {
            return Err(invalid_publication("captured provider route is ambiguous"));
        }
        if frozen.enabled {
            allocatable.insert(frozen.config_name.clone());
        }
    }
    ledger::verify_birth_candidate_routes(db, &routes, &allocatable, false).await
}

pub async fn publish_decided_object(
    db: &DatabaseConnection,
    request: PublicationRequest,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    captured.validate(&request)?;
    run_publication_with_retries_and_hot_receipt(
        db.into(),
        request,
        Vec::new(),
        None,
        None,
        None,
        None,
        Vec::new(),
        None,
        None,
        captured.limits,
        Some(captured.snapshot()?),
    )
    .await
}

/// Guarded standard writes without a separate hot verification receipt retain
/// the same admission fence as the existing publication path.
pub async fn publish_decided_standard_object(
    db: &DatabaseConnection,
    request: PublicationRequest,
    guard: StandardMutationGuard,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    captured.validate(&request)?;
    require_decision_origin(captured.decision, &guard.mutation_id)?;
    run_publication_with_retries_and_hot_receipt(
        db.into(),
        request,
        Vec::new(),
        None,
        None,
        Some(guard),
        None,
        Vec::new(),
        None,
        None,
        captured.limits,
        Some(captured.snapshot()?),
    )
    .await
}

/// Guarded S3 PUT/Copy route. A bad capture is rejected before the hot receipt
/// and guard are consumed; a valid decision is committed with the exact version.
pub async fn publish_decided_standard_object_with_hot_receipt(
    db: &DatabaseConnection,
    request: PublicationRequest,
    guard: StandardMutationGuard,
    receipt: crate::kubo::LocalResidencyVerificationReceipt,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    captured.validate(&request)?;
    require_decision_origin(captured.decision, &guard.mutation_id)?;
    let receipt = match HotPublicationReceipt::validate(receipt, &request.object.cid) {
        Ok(receipt) => receipt,
        Err(error) => {
            release_failed_mutation(db, Some(&guard), true).await;
            return Err(error);
        }
    };
    run_publication_with_retries_and_hot_receipt(
        db.into(),
        request,
        Vec::new(),
        None,
        None,
        Some(guard),
        None,
        Vec::new(),
        None,
        Some(receipt),
        captured.limits,
        Some(captured.snapshot()?),
    )
    .await
}

pub async fn publish_decided_completed_upload(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: PublicationRequest,
    guard: Option<StandardMutationGuard>,
    captured: DecidedPublish<'_>,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    let completion_attempt_id = request.object.id.clone();
    captured
        .validate(&request)
        .and_then(|()| require_decision_origin(captured.decision, &upload_target.upload_id))
        .map_err(|source| CommitCompletedUploadError::RolledBack {
            completion_attempt_id: completion_attempt_id.clone(),
            source,
        })?;
    let snapshot =
        captured
            .snapshot()
            .map_err(|source| CommitCompletedUploadError::RolledBack {
                completion_attempt_id,
                source,
            })?;
    run_completed_publication_with_retries_and_decision(
        db.into(),
        upload_target,
        request,
        Vec::new(),
        None,
        guard,
        captured.limits,
        Some(snapshot),
        None,
    )
    .await
}

pub async fn publish_decided_import_object(
    db: &DatabaseConnection,
    request: PublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    captured.validate(&request)?;
    require_decision_origin(captured.decision, &guard.job_id)?;
    run_publication_with_retries_and_hot_receipt(
        db.into(),
        request,
        Vec::new(),
        None,
        None,
        None,
        Some(guard),
        result_rows,
        Some(now),
        None,
        captured.limits,
        Some(captured.snapshot()?),
    )
    .await
}

/// Direct ZIP uses the archive's captured decision; extracted entries do not
/// silently inherit/execute raw pin tags. Ownership guards retain their old behavior.
pub async fn publish_decided_zip(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    publish_decided_zip_with_batch(db, request, guard, None, captured).await
}

/// Finalizes the prepared manifest in the very same transaction that writes
/// archive, entries, pin leases and the standard mutation completion.
pub async fn publish_decided_zip_batch(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    batch: ZipBatchPublication,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    publish_decided_zip_with_batch(db, request, guard, Some(batch), captured).await
}

async fn publish_decided_zip_with_batch(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    batch: Option<ZipBatchPublication>,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    captured
        .decision
        .validate_policy(&request.archive.policy)
        .map_err(invalid_publication)?;
    if let Some(guard) = &guard {
        require_decision_origin(captured.decision, &guard.mutation_id)?;
    }
    if let Some(batch) = &batch {
        batch.validate(&request.archive.object, &request.entries)?;
    }
    run_publication_with_retries_and_hot_receipt(
        db.into(),
        request.archive,
        request.entries,
        batch,
        None,
        guard,
        None,
        Vec::new(),
        None,
        None,
        captured.limits,
        Some(zip_decided_snapshot(captured.decision)?),
    )
    .await
}

pub async fn publish_decided_completed_zip(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    captured: DecidedPublish<'_>,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    publish_decided_completed_zip_with_batch(
        db,
        upload_target,
        request,
        guard,
        None,
        None,
        captured,
    )
    .await
}

/// MPU completion and batch publication share the original commit/unknown-outcome
/// classification. On OutcomeUnknown, reconcile the upload AND batch before retrying.
pub async fn publish_decided_completed_zip_batch(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    batch: ZipBatchPublication,
    replay: ZipCompletedReplay,
    captured: DecidedPublish<'_>,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    publish_decided_completed_zip_with_batch(
        db,
        upload_target,
        request,
        guard,
        Some(batch),
        Some(replay),
        captured,
    )
    .await
}

async fn publish_decided_completed_zip_with_batch(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    batch: Option<ZipBatchPublication>,
    replay: Option<ZipCompletedReplay>,
    captured: DecidedPublish<'_>,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    let completion_attempt_id = request.archive.object.id.clone();
    captured
        .decision
        .validate_policy(&request.archive.policy)
        .map_err(invalid_publication)
        .and_then(|()| require_decision_origin(captured.decision, &upload_target.upload_id))
        .and_then(|()| {
            if let Some(batch) = &batch {
                batch.validate(&request.archive.object, &request.entries)?;
            }
            Ok(())
        })
        .map_err(|source| CommitCompletedUploadError::RolledBack {
            completion_attempt_id: completion_attempt_id.clone(),
            source,
        })?;
    let snapshot = zip_decided_snapshot(captured.decision).map_err(|source| {
        CommitCompletedUploadError::RolledBack {
            completion_attempt_id,
            source,
        }
    })?;
    run_completed_publication_with_retries_and_decision(
        db.into(),
        upload_target,
        request.archive,
        request.entries,
        batch,
        guard,
        captured.limits,
        Some(snapshot),
        replay,
    )
    .await
}

pub async fn publish_decided_import_zip(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    publish_decided_import_zip_with_batch(db, request, guard, result_rows, now, None, captured)
        .await
}

/// Import completion, result rows, and batch finalization are one atomic write.
pub async fn publish_decided_import_zip_batch(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    batch: ZipBatchPublication,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    publish_decided_import_zip_with_batch(
        db,
        request,
        guard,
        result_rows,
        now,
        Some(batch),
        captured,
    )
    .await
}

async fn publish_decided_import_zip_with_batch(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    batch: Option<ZipBatchPublication>,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    captured
        .decision
        .validate_policy(&request.archive.policy)
        .map_err(invalid_publication)?;
    require_decision_origin(captured.decision, &guard.job_id)?;
    if let Some(batch) = &batch {
        batch.validate(&request.archive.object, &request.entries)?;
    }
    run_publication_with_retries_and_hot_receipt(
        db.into(),
        request.archive,
        request.entries,
        batch,
        None,
        None,
        Some(guard),
        result_rows,
        Some(now),
        None,
        captured.limits,
        Some(zip_decided_snapshot(captured.decision)?),
    )
    .await
}

/// For durable admission the origin request ID is the admission's opaque
/// mutation ID / upload ID / import job ID. Never reuse another admission's capture.
fn require_decision_origin(
    decision: &ExtensionDecision,
    expected_request_id: &str,
) -> AppResult<()> {
    if decision.origin.request_id != expected_request_id {
        return Err(invalid_publication(
            "captured pin decision does not belong to this admission",
        ));
    }
    Ok(())
}

fn zip_decided_snapshot(decision: &ExtensionDecision) -> AppResult<DecidedSnapshot> {
    decision.validate_snapshot().map_err(invalid_publication)?;
    Ok(DecidedSnapshot {
        decision: decision.clone(),
        routes: BTreeMap::new(),
        allocatable: BTreeSet::new(),
    })
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

/// Publishes a standard object after a caller has proved that its CID is
/// recursively pinned and complete on the hot Kubo node.
pub async fn publish_standard_object_with_hot_receipt(
    db: &DatabaseConnection,
    request: PublicationRequest,
    guard: StandardMutationGuard,
    receipt: crate::kubo::LocalResidencyVerificationReceipt,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    let receipt = match HotPublicationReceipt::validate(receipt, &request.object.cid) {
        Ok(receipt) => receipt,
        Err(error) => {
            release_failed_mutation(db, Some(&guard), true).await;
            return Err(error);
        }
    };
    run_publication_with_retries_and_hot_receipt(
        db.into(),
        request,
        Vec::new(),
        None,
        None,
        Some(guard),
        None,
        Vec::new(),
        None,
        Some(receipt),
        limits,
        None,
    )
    .await
}

pub async fn publish_completed_upload(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: PublicationRequest,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries(db, upload_target, request, Vec::new(), None, limits)
        .await
}

pub async fn publish_standard_completed_upload(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: PublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries(
        db,
        upload_target,
        request,
        Vec::new(),
        Some(guard),
        limits,
    )
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
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries(
        db,
        upload_target,
        request.archive,
        request.entries,
        None,
        limits,
    )
    .await
}

pub async fn publish_standard_completed_zip(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries(
        db,
        upload_target,
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
) -> ReconciledPublicationOutcome {
    let object = match object::Entity::find_by_id(expected_archive.id.clone())
        .one(db)
        .await
    {
        Ok(Some(object)) => object,
        Ok(None) => return ReconciledPublicationOutcome::NotCommitted,
        Err(error) => return ReconciledPublicationOutcome::Unknown(error.into()),
    };
    let upload = match multipart_upload::Entity::find_by_id(upload_id.to_owned())
        .one(db)
        .await
    {
        Ok(upload) => upload,
        Err(error) => return ReconciledPublicationOutcome::Unknown(error.into()),
    };
    match crate::store::multipart::classify_completion_attempt_state(
        &expected_archive.latest_row(),
        Some(&object),
        upload.as_ref(),
    ) {
        ReconciledCommitOutcome::Committed => {
            match publication_result_for_internal_object(db, &object).await {
                Ok(result) => ReconciledPublicationOutcome::Committed(result),
                Err(error) => ReconciledPublicationOutcome::Unknown(error),
            }
        }
        ReconciledCommitOutcome::NotCommitted => ReconciledPublicationOutcome::NotCommitted,
        ReconciledCommitOutcome::Unknown(error) => ReconciledPublicationOutcome::Unknown(error),
    }
}

async fn publication_result_for_internal_object<C: ConnectionTrait>(
    db: &C,
    object: &object::Model,
) -> AppResult<PublicationResult> {
    let versions = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&object.id))
        .all(db)
        .await?;
    let [version] = versions.as_slice() else {
        return Err(AppError::Internal(
            "committed object does not have exactly one version index row".to_owned(),
        ));
    };
    if version.bucket != object.bucket || version.key != object.key || version.kind != "object" {
        return Err(AppError::Internal(
            "committed object version index does not match the internal object".to_owned(),
        ));
    }
    let version_id = match version.version_id.as_ref() {
        Some(version_id) => Some(version_id.clone()),
        None => match crate::store::bucket::get_versioning_state(db, &object.bucket).await? {
            BucketVersioningState::Unversioned => None,
            BucketVersioningState::Enabled | BucketVersioningState::Suspended => {
                Some(NULL_VERSION_ID.to_owned())
            }
        },
    };
    Ok(PublicationResult {
        object_id: object.id.clone(),
        version_id,
    })
}

pub async fn delete_version_with_leases_guarded(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    selector: VersionSelector,
    guard: StandardMutationGuard,
    now: DateTime<Utc>,
) -> AppResult<DeleteVersionResult> {
    let result = async {
        for retry in 0..=MAX_TRANSACTION_RETRIES {
            match delete_version_attempt(db, bucket, key, selector.clone(), guard.clone(), now)
                .await
            {
                Ok(deleted) => return Ok(deleted),
                Err(TransactionError::Transaction(error))
                    if is_retryable_transaction_conflict(&error)
                        && retry < MAX_TRANSACTION_RETRIES =>
                {
                    publication_retry_delay(retry).await;
                }
                Err(error) => return Err(transaction_error_into_app(error)),
            }
        }
        unreachable!("guarded version delete retry loop exhausted without returning")
    }
    .await;
    release_failed_mutation(db, Some(&guard), result.is_err()).await;
    result
}

async fn release_failed_mutation(
    db: &DatabaseConnection,
    guard: Option<&StandardMutationGuard>,
    failed: bool,
) {
    if failed && let Some(guard) = guard {
        match tokio::time::timeout(
            Duration::from_secs(5),
            crate::store::import::ownership::release_standard_mutation(db, guard),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, "failed mutation cleanup deferred to lease expiry")
            }
            Err(_) => tracing::warn!("failed mutation cleanup timed out; deferred to lease expiry"),
        }
    }
}

async fn delete_version_attempt(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    selector: VersionSelector,
    guard: StandardMutationGuard,
    now: DateTime<Utc>,
) -> Result<DeleteVersionResult, TransactionError<AppError>> {
    let bucket = bucket.to_owned();
    let key = key.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            lock_bucket_for_ownership(txn, &bucket).await?;
            verify_standard_mutation_guard(txn, &guard, &bucket, &key, &[]).await?;
            let state = crate::store::bucket::get_versioning_state(txn, &bucket).await?;
            if state == BucketVersioningState::Unversioned
                && matches!(&selector, VersionSelector::Exact(_))
            {
                return Err(AppError::InvalidArgument(
                    "version IDs are unavailable for an unversioned bucket".to_owned(),
                ));
            }

            let _ = crate::store::object_version::lock_current_row(txn, &bucket, &key).await?;
            let result = match selector {
                VersionSelector::Current => match state {
                    BucketVersioningState::Unversioned => {
                        delete_unversioned_current_in_transaction(txn, &bucket, &key, now).await?
                    }
                    BucketVersioningState::Enabled => {
                        delete_enabled_current_in_transaction(txn, &bucket, &key, now).await?
                    }
                    BucketVersioningState::Suspended => {
                        delete_suspended_current_in_transaction(txn, &bucket, &key, now).await?
                    }
                },
                VersionSelector::Exact(version_id) => {
                    delete_exact_in_transaction(txn, &bucket, &key, version_id, now).await?
                }
            };
            complete_standard_mutation_in_transaction(txn, &guard, now).await?;
            Ok(result)
        })
    })
    .await
}

pub(crate) async fn delete_unversioned_current_in_transaction<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<DeleteVersionResult> {
    let null_slot =
        crate::store::object_version::lock_indexed_version(db, bucket, key, &PublicVersionId::Null)
            .await?;
    let current = lock_current_object_projection(db, bucket, key).await?;
    if let (Some((row, _)), Some(current)) = (&null_slot, &current)
        && row.object_id.as_deref() != Some(current.id.as_str())
    {
        return Err(AppError::Internal(
            "unversioned null index does not match the current object projection".to_owned(),
        ));
    }
    if let Some(current) = current {
        leases::lock_publication_lifecycle_frontier(db, std::slice::from_ref(&current.id), &[])
            .await?;
        crate::store::object::set_only_latest(db, bucket, key, None).await?;
        leases::end_active_leases_for_object(db, &current.id, now).await?;
        tags::replace_object_tags(db, &current.id, &[]).await?;
    }
    crate::store::object_version::remove_null_slot(db, bucket, key).await?;
    Ok(DeleteVersionResult {
        version_id: None,
        deleted_delete_marker: false,
        created_delete_marker: false,
    })
}

pub(crate) async fn delete_enabled_current_in_transaction<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<DeleteVersionResult> {
    let version_id = crate::store::object_version::install_delete_marker(
        db,
        BucketVersioningState::Enabled,
        bucket,
        key,
        now,
    )
    .await?;
    Ok(DeleteVersionResult {
        version_id: Some(version_id),
        deleted_delete_marker: false,
        created_delete_marker: true,
    })
}

pub(crate) async fn delete_suspended_current_in_transaction<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<DeleteVersionResult> {
    let null_slot =
        crate::store::object_version::lock_indexed_version(db, bucket, key, &PublicVersionId::Null)
            .await?;
    let _ = crate::store::object_version::allocate_next_sequence(db, bucket, key).await?;
    let _ = lock_current_object_projection(db, bucket, key).await?;
    if let Some((_, resolved)) = &null_slot
        && let Some(object) = &resolved.object
    {
        let locked = lock_object_by_id(db, &object.id).await?;
        if locked != *object {
            return Err(AppError::Internal(
                "null version object changed during delete".to_owned(),
            ));
        }
        leases::lock_publication_lifecycle_frontier(db, std::slice::from_ref(&object.id), &[])
            .await?;
        leases::end_active_leases_for_object(db, &object.id, now).await?;
        tags::replace_object_tags(db, &object.id, &[]).await?;
    }
    let version_id = crate::store::object_version::install_delete_marker(
        db,
        BucketVersioningState::Suspended,
        bucket,
        key,
        now,
    )
    .await?;
    Ok(DeleteVersionResult {
        version_id: Some(version_id),
        deleted_delete_marker: false,
        created_delete_marker: true,
    })
}

/// Permanently deletes one exact public version inside a caller-owned
/// transaction. Lifecycle execution calls this only after it has locked and
/// revalidated the exact internal version-row identity; content ownership is
/// ended for that selected object only, while delete markers touch no content
/// ownership records.
pub(crate) async fn delete_exact_in_transaction<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
    version_id: PublicVersionId,
    now: DateTime<Utc>,
) -> AppResult<DeleteVersionResult> {
    let requested_version_id = version_id.as_s3_str().to_owned();
    let (selected, resolved) =
        crate::store::object_version::lock_indexed_version(db, bucket, key, &version_id)
            .await?
            .ok_or_else(|| AppError::NoSuchVersion {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                version_id: requested_version_id.clone(),
            })?;
    let _ = lock_current_object_projection(db, bucket, key).await?;
    if let Some(object) = &resolved.object {
        let locked = lock_object_by_id(db, &object.id).await?;
        if locked != *object {
            return Err(AppError::Internal(
                "selected version object changed during delete".to_owned(),
            ));
        }
        leases::lock_publication_lifecycle_frontier(db, std::slice::from_ref(&object.id), &[])
            .await?;
        leases::end_active_leases_for_object(db, &object.id, now).await?;
        tags::replace_object_tags(db, &object.id, &[]).await?;
    }
    let deleted_delete_marker = resolved.kind == VersionKind::DeleteMarker;
    let _ = crate::store::object_version::remove_and_promote(db, &selected).await?;
    Ok(DeleteVersionResult {
        version_id: Some(requested_version_id),
        deleted_delete_marker,
        created_delete_marker: false,
    })
}

#[cfg(test)]
pub async fn delete_latest_with_leases(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let bucket = bucket.to_owned();
    let key = key.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            acquire_sqlite_publication_write_intent(txn, &bucket).await?;
            let latest = lock_current_object_projection(txn, &bucket, &key).await?;
            let Some(latest) = latest else {
                return Ok(false);
            };
            leases::lock_publication_lifecycle_frontier(txn, std::slice::from_ref(&latest.id), &[])
                .await?;
            crate::store::object::set_only_latest(txn, &bucket, &key, None).await?;
            leases::end_active_leases_for_object(txn, &latest.id, now).await?;
            tags::replace_object_tags(txn, &latest.id, &[]).await?;
            Ok(true)
        })
    })
    .await
    .map_err(transaction_error_into_app)
}

#[allow(clippy::too_many_arguments)]
async fn run_publication_with_retries(
    db: &DatabaseConnection,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    upload_target: Option<MultipartUploadTargetIdentity>,
    standard_guard: Option<StandardMutationGuard>,
    import_guard: Option<ImportPublicationGuard>,
    result_rows: Vec<import_job_result::ActiveModel>,
    import_now: Option<DateTime<Utc>>,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    run_publication_with_retries_and_hot_receipt(
        db.into(),
        request,
        entries,
        None,
        upload_target,
        standard_guard,
        import_guard,
        result_rows,
        import_now,
        None,
        limits,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_publication_with_retries_and_hot_receipt(
    db: PublicationDatabase<'_>,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    batch: Option<ZipBatchPublication>,
    upload_target: Option<MultipartUploadTargetIdentity>,
    standard_guard: Option<StandardMutationGuard>,
    import_guard: Option<ImportPublicationGuard>,
    result_rows: Vec<import_job_result::ActiveModel>,
    import_now: Option<DateTime<Utc>>,
    hot_receipt: Option<HotPublicationReceipt>,
    limits: &ProviderLimitMap,
    decision: Option<DecidedSnapshot>,
) -> AppResult<PublicationResult> {
    let session = super::management::records::SharedDecodeBudget::default();
    let result = async {
        for retry in 0..=MAX_TRANSACTION_RETRIES {
            match publication_attempt(
                db,
                request.clone(),
                entries.clone(),
                batch.clone(),
                upload_target.clone(),
                standard_guard.clone(),
                import_guard.clone(),
                result_rows.clone(),
                import_now,
                hot_receipt.clone(),
                limits.clone(),
                decision.clone(),
                None,
                &session,
            )
            .await
            {
                Ok(result) => return Ok(result),
                Err(TransactionError::Transaction(error))
                    if is_retryable_transaction_conflict(&error)
                        && retry < MAX_TRANSACTION_RETRIES =>
                {
                    publication_retry_delay(retry).await;
                }
                Err(error) => return Err(transaction_error_into_app(error)),
            }
        }
        unreachable!("publication retry loop exhausted without returning")
    }
    .await;
    release_failed_mutation(&db, standard_guard.as_ref(), result.is_err()).await;
    result
}

async fn run_completed_publication_with_retries(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    standard_guard: Option<StandardMutationGuard>,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries_and_decision(
        db.into(),
        upload_target,
        request,
        entries,
        None,
        standard_guard,
        limits,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_completed_publication_with_retries_and_decision(
    db: PublicationDatabase<'_>,
    upload_target: &MultipartUploadTargetIdentity,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    batch: Option<ZipBatchPublication>,
    standard_guard: Option<StandardMutationGuard>,
    limits: &ProviderLimitMap,
    decision: Option<DecidedSnapshot>,
    replay: Option<ZipCompletedReplay>,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    let session = super::management::records::SharedDecodeBudget::default();
    let completion_attempt_id = request.object.id.clone();
    let result = async {
        for retry in 0..=MAX_TRANSACTION_RETRIES {
            match publication_attempt(
                db,
                request.clone(),
                entries.clone(),
                batch.clone(),
                Some(upload_target.clone()),
                standard_guard.clone(),
                None,
                Vec::new(),
                None,
                None,
                limits.clone(),
                decision.clone(),
                replay.clone(),
                &session,
            )
            .await
            {
                Ok(result) => return Ok(result),
                Err(TransactionError::Transaction(source)) => {
                    if is_retryable_transaction_conflict(&source) && retry < MAX_TRANSACTION_RETRIES
                    {
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
    .await;
    release_failed_mutation(&db, standard_guard.as_ref(), result.is_err()).await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn publication_attempt(
    db: PublicationDatabase<'_>,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    batch: Option<ZipBatchPublication>,
    upload_target: Option<MultipartUploadTargetIdentity>,
    standard_guard: Option<StandardMutationGuard>,
    import_guard: Option<ImportPublicationGuard>,
    result_rows: Vec<import_job_result::ActiveModel>,
    import_now: Option<DateTime<Utc>>,
    hot_receipt: Option<HotPublicationReceipt>,
    limits: ProviderLimitMap,
    decision: Option<DecidedSnapshot>,
    replay: Option<ZipCompletedReplay>,
    session: &super::management::records::SharedDecodeBudget,
) -> Result<PublicationResult, TransactionError<AppError>> {
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        return Box::pin(reservations::attempt(
            db,
            request,
            entries,
            batch,
            upload_target,
            standard_guard,
            import_guard,
            result_rows,
            import_now,
            hot_receipt,
            limits,
            decision,
            replay,
            session,
        ))
        .await;
    }
    db.transaction(|txn| {
        Box::pin(async move {
            publish_in_transaction_with_batch(
                txn,
                request,
                entries,
                batch.as_ref(),
                upload_target.as_ref(),
                standard_guard.as_ref(),
                import_guard.as_ref(),
                result_rows,
                import_now,
                hot_receipt.as_ref(),
                &limits,
                decision.as_ref(),
                replay.as_ref(),
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
async fn publish_in_transaction_with_batch(
    db: &DatabaseTransaction,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    batch: Option<&ZipBatchPublication>,
    upload_target: Option<&MultipartUploadTargetIdentity>,
    standard_guard: Option<&StandardMutationGuard>,
    import_guard: Option<&ImportPublicationGuard>,
    result_rows: Vec<import_job_result::ActiveModel>,
    _import_now: Option<DateTime<Utc>>,
    hot_receipt: Option<&HotPublicationReceipt>,
    limits: &ProviderLimitMap,
    decided: Option<&DecidedSnapshot>,
    replay: Option<&ZipCompletedReplay>,
) -> AppResult<PublicationResult> {
    publish_with_reservations(
        &mut reservations::Borrowed(db),
        request,
        entries,
        batch,
        upload_target,
        standard_guard,
        import_guard,
        result_rows,
        _import_now,
        hot_receipt,
        limits,
        decided,
        replay,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn publish_with_reservations<
    R: reservations::Reservations<Connection = DatabaseTransaction>,
>(
    reservations: &mut R,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    batch: Option<&ZipBatchPublication>,
    upload_target: Option<&MultipartUploadTargetIdentity>,
    standard_guard: Option<&StandardMutationGuard>,
    import_guard: Option<&ImportPublicationGuard>,
    mut result_rows: Vec<import_job_result::ActiveModel>,
    _import_now: Option<DateTime<Utc>>,
    hot_receipt: Option<&HotPublicationReceipt>,
    limits: &ProviderLimitMap,
    decided: Option<&DecidedSnapshot>,
    replay: Option<&ZipCompletedReplay>,
) -> AppResult<PublicationResult> {
    let db = reservations.db();
    validate_request(&request)?;
    if let Some(batch) = batch {
        batch.validate(&request.object, &entries)?;
        return zip::publish_legacy_in_transaction(
            db,
            request,
            entries,
            batch,
            upload_target,
            standard_guard,
            import_guard,
            result_rows,
            decided,
            replay,
        )
        .await;
    }
    if !entries.is_empty() {
        return Err(invalid_publication(
            "ZIP publication requires its durable captured admission; needs_attention",
        ));
    }
    if let Some(decision) = decided.map(|snapshot| &snapshot.decision) {
        decision
            .validate_policy(&request.policy)
            .map_err(invalid_publication)?;
    }
    if standard_guard.is_some() && import_guard.is_some() {
        return Err(AppError::Internal(
            "publication cannot have both standard and import guards".to_owned(),
        ));
    }
    if let Some(upload_target) = upload_target
        && (upload_target.bucket != request.object.bucket
            || upload_target.key != request.object.key)
    {
        return Err(AppError::NoSuchUpload(upload_target.upload_id.clone()));
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
    } else if upload_target.is_some() {
        lock_bucket_for_ownership(db, &request.object.bucket).await?;
    } else {
        acquire_sqlite_publication_write_intent(db, &request.object.bucket).await?;
    }
    let session = reservations.runtime_work().cloned().unwrap_or_default();
    let work = super::management::records::DecodeWork::shared(&session);
    if let Some(tx) = reservations.runtime() {
        tx.check_schema(work).await?;
    }
    let versioning_state =
        crate::store::bucket::lock_versioning_state(db, &request.object.bucket).await?;
    let object_id = request.object.id.clone();

    let previous_owner_ids =
        lock_publication_version_frontier(db, versioning_state, &request.object, &entries).await?;
    let attachment_pairs =
        publication_attachment_pairs(reservations, &request, &entries, limits, work).await?;
    leases::lock_publication_lifecycle_frontier(db, &previous_owner_ids, &attachment_pairs).await?;
    let attachment_providers: Vec<_> = attachment_pairs
        .iter()
        .map(|(provider, _)| provider.clone())
        .collect();
    quota::lock_publication_usage_rows(db, &attachment_providers).await?;
    if let Some(snapshot) = decided {
        match reservations.runtime() {
            Some(tx) => {
                ledger::verify_runtime_birth_routes(
                    tx,
                    &snapshot.routes,
                    &snapshot.allocatable,
                    work,
                )
                .await?
            }
            None => {
                ledger::verify_birth_candidate_routes(
                    db,
                    &snapshot.routes,
                    &snapshot.allocatable,
                    true,
                )
                .await?
            }
        }
    }
    crate::store::residency::prepare_hot_publication_frontier(
        db,
        ordered_publication_objects(&request.object, &entries)
            .into_iter()
            .map(|object| object.cid.clone())
            .collect(),
    )
    .await?;
    if let Some(receipt) = hot_receipt {
        crate::store::residency::apply_hot_publication_verification(
            db,
            &request.object.cid,
            receipt.verification(),
        )
        .await?;
    }
    let publication_time = crate::store::database_clock::database_now(db).await?;

    let publication_result = write_object_version_and_update_lifecycle(
        db,
        &request.object,
        versioning_state,
        publication_time,
    )
    .await?;
    tags::replace_object_tags(db, &object_id, &request.tags).await?;

    if let Some(decision) = decided.map(|snapshot| &snapshot.decision) {
        super::decision::write_for_object_in_transaction(db, &object_id, decision).await?;
    }

    create_publication_leases(reservations, &request, &entries, limits, publication_time).await?;
    let db = reservations.db();

    if let Some(upload_target) = upload_target {
        match crate::store::multipart::abort_exact_incomplete_upload_in_transaction(
            db,
            upload_target,
        )
        .await?
        {
            AbortExactIncompleteUploadResult::Applied => {}
            AbortExactIncompleteUploadResult::AlreadySatisfied
            | AbortExactIncompleteUploadResult::Stale => {
                return Err(AppError::NoSuchUpload(upload_target.upload_id.clone()));
            }
        }
    }
    if let Some(guard) = standard_guard {
        complete_standard_mutation_in_transaction(db, guard, publication_time).await?;
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
            publication_time,
        )
        .await?;
    }
    if let Some(tx) = reservations.runtime() {
        tx.check_schema(work).await?;
    }
    Ok(publication_result)
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
    // ZIP winner orchestration belongs to the captured core; this helper also
    // provides stable location ordering for ordinary publication frontiers.
    objects.sort_by(|left, right| (&left.bucket, &left.key).cmp(&(&right.bucket, &right.key)));
    objects
}

async fn lock_publication_version_frontier<C: ConnectionTrait>(
    db: &C,
    state: BucketVersioningState,
    archive: &PublicationObject,
    entries: &[PublicationObject],
) -> AppResult<Vec<String>> {
    zip::lock_version_frontier(
        db,
        &ordered_publication_objects(archive, entries),
        state,
        None,
    )
    .await
}

async fn lock_current_object_projection<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
) -> AppResult<Option<object::Model>> {
    let query = object::Entity::find()
        .filter(object::Column::Bucket.eq(bucket_name))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true));
    Ok(if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    })
}

async fn lock_null_object_id<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
) -> AppResult<Option<String>> {
    let query = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(bucket_name))
        .filter(object_version::Column::Key.eq(key))
        .filter(object_version::Column::VersionId.is_null());
    let row = if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    };
    Ok(row.and_then(|row| row.object_id))
}

async fn lock_object_by_id<C: ConnectionTrait>(
    db: &C,
    object_id: &str,
) -> AppResult<object::Model> {
    let query = object::Entity::find_by_id(object_id.to_owned());
    let object = if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    };
    object.ok_or_else(|| {
        AppError::Internal("object version index references a missing internal object".to_owned())
    })
}

async fn publication_attachment_pairs<R: reservations::Reservations>(
    reservations: &R,
    request: &PublicationRequest,
    entries: &[PublicationObject],
    limits: &ProviderLimitMap,
    work: super::management::records::DecodeWork<'_>,
) -> AppResult<Vec<(String, String)>> {
    let db = reservations.db();
    let mut pairs = BTreeSet::new();
    for intent in &request.policy.leases {
        let providers = ordered_enabled_providers(intent, limits)?;
        for target in target_specs(request, entries, intent.content_mode) {
            for provider in &providers {
                let cid = if reservations.runtime().is_some() {
                    ledger::allocation_cid_with_work(
                        &reservations.catalog(work),
                        provider,
                        &target.cid,
                        work,
                    )
                    .await?
                } else {
                    ledger::allocation_cid(db, provider, &target.cid).await?
                };
                pairs.insert((provider.clone(), cid));
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

async fn write_object_version_and_update_lifecycle<C: ConnectionTrait>(
    db: &C,
    object: &PublicationObject,
    state: BucketVersioningState,
    now: DateTime<Utc>,
) -> AppResult<PublicationResult> {
    if object.logical_size < 0 {
        return Err(invalid_publication(
            "logical object size cannot be negative",
        ));
    }

    let displaced_object_id = match state {
        BucketVersioningState::Unversioned => {
            lock_current_object_projection(db, &object.bucket, &object.key)
                .await?
                .map(|object| object.id)
        }
        BucketVersioningState::Enabled => None,
        BucketVersioningState::Suspended => {
            lock_null_object_id(db, &object.bucket, &object.key).await?
        }
    };
    let mut immutable_object = object.latest_row();
    immutable_object.created_at = now;
    let inserted =
        crate::store::object::insert_immutable_in_transaction(db, immutable_object).await?;
    if let Some(displaced_object_id) = displaced_object_id {
        leases::end_active_leases_for_object(db, &displaced_object_id, now).await?;
        tags::replace_object_tags(db, &displaced_object_id, &[]).await?;
    }
    let public_version_id =
        crate::store::object_version::install_content_version(db, state, &inserted, now).await?;
    let version_id = match state {
        BucketVersioningState::Unversioned => None,
        BucketVersioningState::Enabled | BucketVersioningState::Suspended => {
            Some(public_version_id)
        }
    };
    Ok(PublicationResult {
        object_id: object.id.clone(),
        version_id,
    })
}

async fn create_publication_leases<R: reservations::Reservations>(
    reservations: &mut R,
    request: &PublicationRequest,
    entries: &[PublicationObject],
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut affected_remotes = BTreeSet::new();
    let mut failed_user_touches = BTreeSet::new();
    for intent in &request.policy.leases {
        let lease_id = lease_id(&request.object.id, intent.source);
        let targets = target_specs(request, entries, intent.content_mode);
        insert_lease(
            reservations.db(),
            &lease_id,
            &request.object.id,
            intent,
            &targets,
            now,
        )
        .await?;
        // Decompressed entries can have distinct object CID spellings but the
        // same provider resource. Keep one target/reference per lease/resource.
        let mut assigned_targets = BTreeMap::new();
        for target in targets {
            let work = reservations.runtime_work().cloned().unwrap_or_default();
            assign_target_providers_core(
                reservations,
                &lease_id,
                intent,
                &target,
                limits,
                now,
                &mut assigned_targets,
                &mut affected_remotes,
                &mut failed_user_touches,
                super::management::records::DecodeWork::shared(&work),
            )
            .await?;
        }
    }

    settle_publication_remotes(
        reservations,
        affected_remotes,
        failed_user_touches,
        now,
        None,
    )
    .await
}

async fn insert_lease<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    owner_object_id: &str,
    intent: &LeaseIntent,
    targets: &[PinTargetSpec],
    now: DateTime<Utc>,
) -> AppResult<()> {
    let seconds = i64::try_from(intent.duration.as_seconds())
        .map_err(|_| invalid_publication("lease duration is too large"))?;
    let expires_at = now
        .checked_add_signed(TimeDelta::seconds(seconds))
        .ok_or_else(|| invalid_publication("lease expiry is out of range"))?;
    insert_absolute_lease(
        db,
        lease_id,
        owner_object_id,
        source_name(intent.source),
        intent,
        targets,
        now,
        expires_at,
        now,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_absolute_lease<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    owner_object_id: &str,
    source: &str,
    intent: &LeaseIntent,
    targets: &[PinTargetSpec],
    started_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    now: DateTime<Utc>,
    requirements: Option<&crate::pinning::management::Requirements>,
) -> AppResult<()> {
    let work = super::management::records::SharedDecodeBudget::default();
    insert_absolute_lease_with_work(
        db,
        lease_id,
        owner_object_id,
        source,
        intent,
        targets,
        started_at,
        expires_at,
        now,
        requirements,
        super::management::records::DecodeWork::shared(&work),
    )
    .await
}
#[allow(clippy::too_many_arguments)]
async fn insert_absolute_lease_with_work<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    owner_object_id: &str,
    source: &str,
    intent: &LeaseIntent,
    targets: &[PinTargetSpec],
    started_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    now: DateTime<Utc>,
    requirements: Option<&crate::pinning::management::Requirements>,
    work: super::management::records::DecodeWork<'_>,
) -> AppResult<()> {
    if expires_at <= started_at {
        return Err(invalid_publication("invalid absolute lease interval"));
    }
    pin_lease::Entity::insert(pin_lease::ActiveModel {
        id: Set(lease_id.to_owned()),
        owner_object_id: Set(owner_object_id.to_owned()),
        source: Set(source.to_owned()),
        policy_id: Set(intent.policy_id.clone()),
        provider_mode: Set(provider_mode_name(intent.provider_mode).to_owned()),
        content_mode: Set(content_mode_name(intent.content_mode).to_owned()),
        created_at: Set(started_at),
        last_touched_at: Set(now),
        expires_at: Set(expires_at),
        generation: Set(1),
        state: Set(LEASE_ACTIVE.to_owned()),
    })
    .exec(db)
    .await?;
    if let Some(requirements) = requirements {
        for target in targets {
            work.copy(&target.cid)
                .map_err(super::management::admission::work_error)?;
        }
        crate::store::entities::pin_lease_requirement::contract::insert_management_with_work(
            db,
            lease_id,
            owner_object_id,
            provider_mode_name(intent.provider_mode),
            content_mode_name(intent.content_mode),
            targets.iter().map(|target| target.cid.clone()),
            &intent.providers,
            started_at,
            requirements,
            work,
        )
        .await?;
    } else {
        crate::store::entities::pin_lease_requirement::contract::insert(
            db,
            lease_id,
            owner_object_id,
            provider_mode_name(intent.provider_mode),
            content_mode_name(intent.content_mode),
            targets.iter().map(|target| target.cid.clone()),
            &intent.providers,
            started_at,
        )
        .await?;
    }
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
async fn assign_target_providers_with_work<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    intent: &LeaseIntent,
    target: &PinTargetSpec,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
    assigned_targets: &mut BTreeMap<(String, String), quota::ReservationOutcome>,
    affected_remotes: &mut BTreeSet<(String, String)>,
    failed_user_touches: &mut BTreeSet<(String, String)>,
    work: super::management::records::DecodeWork<'_>,
) -> AppResult<()> {
    assign_target_providers_core(
        &mut reservations::Borrowed(db),
        lease_id,
        intent,
        target,
        limits,
        now,
        assigned_targets,
        affected_remotes,
        failed_user_touches,
        work,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn assign_target_providers_core<R: reservations::Reservations>(
    reservations: &mut R,
    lease_id: &str,
    intent: &LeaseIntent,
    target: &PinTargetSpec,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
    assigned_targets: &mut BTreeMap<(String, String), quota::ReservationOutcome>,
    affected_remotes: &mut BTreeSet<(String, String)>,
    failed_user_touches: &mut BTreeSet<(String, String)>,
    work: super::management::records::DecodeWork<'_>,
) -> AppResult<()> {
    let providers = ordered_enabled_providers(intent, limits)?;
    match intent.provider_mode {
        ProviderMode::All => {
            for provider in providers {
                let target = PinTargetSpec {
                    cid: ledger::allocation_cid_with_work(
                        &reservations.catalog(work),
                        &provider,
                        &target.cid,
                        work,
                    )
                    .await?,
                    logical_size: target.logical_size,
                };
                let pair = (provider.clone(), target.cid.clone());
                if assigned_targets.contains_key(&pair) {
                    continue;
                }
                let outcome = reserve_and_insert_target_core(
                    reservations,
                    lease_id,
                    &provider,
                    &target,
                    limits,
                    now,
                    affected_remotes,
                    failed_user_touches,
                    work,
                )
                .await?;
                assigned_targets.insert(pair, outcome);
            }
        }
        ProviderMode::One => {
            let mut first_unavailable = None;
            for provider in providers {
                let target = PinTargetSpec {
                    cid: ledger::allocation_cid_with_work(
                        &reservations.catalog(work),
                        &provider,
                        &target.cid,
                        work,
                    )
                    .await?,
                    logical_size: target.logical_size,
                };
                let pair = (provider.clone(), target.cid.clone());
                if let Some(outcome) = assigned_targets.get(&pair) {
                    if matches!(
                        outcome,
                        quota::ReservationOutcome::Reserved | quota::ReservationOutcome::Reused
                    ) {
                        return Ok(());
                    }
                    if first_unavailable.is_none() {
                        first_unavailable = Some((provider, target, outcome.clone()));
                    }
                    // A raw alias may still be a different resource on a later
                    // non-RPC provider. Preserve that provider's fallback chance.
                    continue;
                }
                let outcome = reservations
                    .reserve(&provider, &target, limits, now, work)
                    .await?;
                if matches!(
                    outcome,
                    quota::ReservationOutcome::Reserved | quota::ReservationOutcome::Reused
                ) {
                    let was_failed =
                        remote_is_failed(reservations.db(), &provider, &target.cid).await?;
                    insert_reserved_target(
                        reservations,
                        lease_id,
                        &provider,
                        &target,
                        &outcome,
                        now,
                        work,
                    )
                    .await?;
                    record_reserved_target(
                        reservations.db(),
                        &provider,
                        &target,
                        was_failed,
                        affected_remotes,
                        failed_user_touches,
                    )
                    .await?;
                    assigned_targets.insert(pair, outcome);
                    return Ok(());
                }
                if first_unavailable.is_none() {
                    first_unavailable = Some((provider, target, outcome));
                }
            }
            let Some((provider, target, outcome)) = first_unavailable else {
                return Err(invalid_publication("pinning lease has no enabled provider"));
            };
            let pair = (provider.clone(), target.cid.clone());
            if let std::collections::btree_map::Entry::Vacant(entry) = assigned_targets.entry(pair)
            {
                insert_reserved_target(
                    reservations,
                    lease_id,
                    &provider,
                    &target,
                    &outcome,
                    now,
                    work,
                )
                .await?;
                entry.insert(outcome);
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn reserve_and_insert_target_with_work<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    provider: &str,
    target: &PinTargetSpec,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
    affected_remotes: &mut BTreeSet<(String, String)>,
    failed_user_touches: &mut BTreeSet<(String, String)>,
    work: super::management::records::DecodeWork<'_>,
) -> AppResult<quota::ReservationOutcome> {
    reserve_and_insert_target_core(
        &mut reservations::Borrowed(db),
        lease_id,
        provider,
        target,
        limits,
        now,
        affected_remotes,
        failed_user_touches,
        work,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn reserve_and_insert_target_core<R: reservations::Reservations>(
    reservations: &mut R,
    lease_id: &str,
    provider: &str,
    target: &PinTargetSpec,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
    affected_remotes: &mut BTreeSet<(String, String)>,
    failed_user_touches: &mut BTreeSet<(String, String)>,
    work: super::management::records::DecodeWork<'_>,
) -> AppResult<quota::ReservationOutcome> {
    let outcome = reservations
        .reserve(provider, target, limits, now, work)
        .await?;
    insert_reserved_target(
        reservations,
        lease_id,
        provider,
        target,
        &outcome,
        now,
        work,
    )
    .await?;
    if matches!(
        outcome,
        quota::ReservationOutcome::Reserved | quota::ReservationOutcome::Reused
    ) {
        let was_failed = remote_is_failed(reservations.db(), provider, &target.cid).await?;
        record_reserved_target(
            reservations.db(),
            provider,
            target,
            was_failed,
            affected_remotes,
            failed_user_touches,
        )
        .await?;
    }
    Ok(outcome)
}

async fn ensure_failed_attachment_reconcile_core<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
    runtime: Option<(
        &crate::store::RuntimeTransaction,
        super::management::records::DecodeWork<'_>,
    )>,
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
    match runtime {
        Some((tx, work)) => jobs::ensure_runtime_reconcile(tx, job, now, work).await?,
        None => jobs::ensure_or_reactivate_reconcile_job(db, job, now).await?,
    };
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_reserved_target<R: reservations::Reservations>(
    reservations: &R,
    lease_id: &str,
    provider: &str,
    target: &PinTargetSpec,
    outcome: &quota::ReservationOutcome,
    now: DateTime<Utc>,
    work: super::management::records::DecodeWork<'_>,
) -> AppResult<()> {
    if reservations.runtime().is_some() {
        super::management::admission::check_provider_catalog(
            reservations.catalog(work),
            provider,
            work,
        )
        .await?;
    }
    insert_target(reservations.db(), lease_id, provider, target, outcome, now).await
}

async fn settle_publication_remotes<R: reservations::Reservations>(
    reservations: &R,
    affected: BTreeSet<(String, String)>,
    failed: BTreeSet<(String, String)>,
    now: DateTime<Utc>,
    mut maintenance: Option<&mut crate::store::zip::publication_lease::PublicationLease<'_>>,
) -> AppResult<()> {
    let session = reservations.runtime_work().cloned().unwrap_or_default();
    let work = super::management::records::DecodeWork::shared(&session);
    let runtime = reservations.runtime().map(|tx| (tx, work));
    let db = reservations.db();
    for (provider, cid) in failed {
        if let Some(lease) = maintenance.as_deref_mut() {
            lease.checkpoint(db).await?;
        }
        leases::reset_failed_publication_retry(
            db,
            &provider,
            &cid,
            now,
            maintenance.as_deref_mut(),
            runtime,
        )
        .await?;
        ensure_failed_attachment_reconcile_core(db, &provider, &cid, now, runtime).await?;
    }
    for (provider, cid) in affected {
        project_desired_remote_targets_core(
            db,
            &provider,
            &cid,
            now,
            maintenance.as_deref_mut(),
            runtime,
        )
        .await?;
    }
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

async fn project_desired_remote_targets_core<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
    mut maintenance: Option<&mut crate::store::zip::publication_lease::PublicationLease<'_>>,
    runtime: Option<(
        &crate::store::RuntimeTransaction,
        super::management::records::DecodeWork<'_>,
    )>,
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
        if let Some(lease) = maintenance.as_deref_mut() {
            lease.checkpoint(db).await?;
        }
        match runtime {
            Some((tx, work)) => {
                leases::project_runtime_publication_target(
                    tx,
                    &target_id,
                    now,
                    maintenance.as_deref_mut(),
                    work,
                )
                .await?;
            }
            None => {
                leases::project_target_from_remote_with_maintenance(
                    db,
                    &target_id,
                    now,
                    maintenance.as_deref_mut(),
                )
                .await?;
            }
        }
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
        } else if intent.provider_mode == ProviderMode::All {
            return Err(invalid_publication(
                "all-provider intent includes a disabled or retired provider",
            ));
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
mod allocation_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod zip_batch_publication_tests {
    use super::*;
    use crate::{
        config::OptionalPinControlMode,
        import::SupersedeReason,
        pinning::{
            config::{LeaseDuration, ProviderLimits, ProviderMode, ValidatedPinningConfig},
            policy::{LeaseIntent, LeaseSource},
        },
        store::{
            entities::{object, object_version, pin_lease, pin_lease_target},
            import::ownership::admit_content_and_prefix_mutation,
            zip::{self, BatchAdmission, ManifestItem, RootOutcome},
        },
    };
    use sea_orm::{Database, PaginatorTrait};

    async fn setup() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        db
    }

    fn entry(id: &str, key: &str, cid: &str) -> PublicationObject {
        PublicationObject::from_put(
            id.into(),
            "bucket",
            key,
            cid.into(),
            1,
            None,
            None,
            false,
            None,
            None,
            Utc::now(),
        )
    }

    async fn prepare(
        db: &DatabaseConnection,
        archive: &PublicationRequest,
        prefix: &str,
        items: &[ManifestItem],
    ) -> tests::zip_fixture::AdmittedZip {
        let tx = db.begin().await.unwrap();
        lock_bucket_for_ownership(&tx, "bucket").await.unwrap();
        let guard = admit_content_and_prefix_mutation(
            &tx,
            "bucket",
            "archive.zip",
            prefix,
            SupersedeReason::DecompressZip,
            Utc::now(),
        )
        .await
        .unwrap();
        let capture =
            tests::zip_fixture::original_capture(&tx, archive, prefix, &guard.mutation_id).await;
        BatchAdmission::admit_legacy_in_transaction(
            &tx,
            &BatchAdmission {
                id: "batch".into(),
                owner: "principal".into(),
                source: "direct".into(),
                token: "token".into(),
                fingerprint: "fingerprint".into(),
                bucket: "bucket".into(),
                archive_key: "archive.zip".into(),
                input_identity: "digest".into(),
                captured_options: serde_json::json!({"zip_intent": capture}).to_string(),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        zip::prepare_manifest(db, "batch", items).await.unwrap();
        tests::zip_fixture::AdmittedZip {
            capture,
            guard: Some(guard),
            id: "batch".into(),
        }
    }

    fn captured_request<'a>(
        decision: &'a ExtensionDecision,
        config: &'a ValidatedPinningConfig,
        limits: &'a ProviderLimitMap,
    ) -> DecidedPublish<'a> {
        DecidedPublish {
            decision,
            config,
            mode: OptionalPinControlMode::Warn,
            limits,
        }
    }

    fn request(entries: Vec<PublicationObject>) -> ZipPublicationRequest {
        let archive = entry("archive", "archive.zip", "archive-cid");
        ZipPublicationRequest {
            archive: PublicationRequest {
                object_target: PinTargetSpec {
                    cid: archive.cid.clone(),
                    logical_size: archive.logical_size,
                },
                object: archive,
                tags: vec![],
                policy: PublicationPolicy {
                    tags: vec![],
                    leases: vec![],
                },
            },
            entries,
        }
    }

    #[tokio::test]
    async fn zip_publish_failure_rolls_back_object_versions_and_batch() {
        let db = setup().await;
        let admitted = prepare(
            &db,
            &request(vec![]).archive,
            "out/",
            &[ManifestItem::Success {
                path: "a.txt".into(),
                object_key: "out/a.txt".into(),
                cid: "different-cid".into(),
                size: 1,
            }],
        )
        .await;
        let config =
            ValidatedPinningConfig::from_raw(&crate::config::PinningConfig::default(), |_| None)
                .unwrap();
        let decision = admitted.capture.source_decision().clone();
        let limits = ProviderLimitMap::new();
        let result = publish_decided_zip_batch(
            &db,
            request(vec![entry("entry", "out/a.txt", "entry-cid")]),
            admitted.guard,
            ZipBatchPublication {
                batch_id: "batch".into(),
                entries: vec![ZipBatchEntry {
                    path: "a.txt".into(),
                    object_id: "entry".into(),
                }],
                source_published: true,
                root_outcome: RootOutcome::Disabled,
                terminal_result: "{}".into(),
            },
            captured_request(&decision, &config, &limits),
        )
        .await;
        assert!(matches!(
            result,
            Err(AppError::Internal(message)) if message.contains("stale ZIP batch")
        ));
        assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
        let snapshot = zip::snapshot(&db, "batch").await.unwrap().unwrap();
        assert_eq!(snapshot.batch.state, "open");
        assert!(snapshot.entries[0].version_row_id.is_none());
    }

    #[tokio::test]
    async fn zip_publish_error_rolls_back_pin_leases_and_guard_settlement() {
        let db = setup().await;
        let mut zip_request = request(vec![entry("entry", "out/a.txt", "entry-cid")]);
        zip_request.archive.policy.leases.push(LeaseIntent {
            source: LeaseSource::Automatic,
            policy_id: "test".into(),
            provider_mode: ProviderMode::All,
            providers: vec!["pinata".into()],
            content_mode: ContentMode::Decompressed,
            duration: LeaseDuration::parse("1h").unwrap(),
        });
        let admitted = prepare(
            &db,
            &zip_request.archive,
            "out/",
            &[ManifestItem::Success {
                path: "a.txt".into(),
                object_key: "out/a.txt".into(),
                cid: "wrong-cid".into(),
                size: 1,
            }],
        )
        .await;
        let guard = admitted.guard.unwrap();
        let limits = ProviderLimitMap::from([(
            "pinata".into(),
            ProviderLimits {
                priority: 1,
                max_bytes: 1_000,
                max_pins: 100,
                enabled: true,
            },
        )]);
        let batch = ZipBatchPublication {
            batch_id: "batch".into(),
            entries: vec![ZipBatchEntry {
                path: "a.txt".into(),
                object_id: "entry".into(),
            }],
            source_published: true,
            root_outcome: RootOutcome::Disabled,
            terminal_result: "{}".into(),
        };
        let error = publication_attempt(
            (&db).into(),
            zip_request.archive,
            zip_request.entries,
            Some(batch),
            None,
            Some(guard.clone()),
            None,
            vec![],
            None,
            None,
            limits,
            None,
            None,
            &Default::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            TransactionError::Transaction(AppError::Internal(message))
                if message.contains("stale ZIP batch")
        ));
        assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(
            pin_lease_target::Entity::find().count(&db).await.unwrap(),
            0
        );
        let tx = db.begin().await.unwrap();
        verify_standard_mutation_guard(&tx, &guard, "bucket", "archive.zip", &["out/a.txt".into()])
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(
            zip::snapshot(&db, "batch")
                .await
                .unwrap()
                .unwrap()
                .batch
                .state,
            "open"
        );
    }

    #[tokio::test]
    async fn zip_batch_binds_each_written_object_not_latest_key() {
        let db = setup().await;
        crate::store::bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
            .await
            .unwrap();
        prepare(
            &db,
            &request(vec![]).archive,
            "",
            &[
                ManifestItem::Success {
                    path: "first".into(),
                    object_key: "same.txt".into(),
                    cid: "first-cid".into(),
                    size: 1,
                },
                ManifestItem::Success {
                    path: "second".into(),
                    object_key: "same.txt".into(),
                    cid: "second-cid".into(),
                    size: 1,
                },
            ],
        )
        .await;
        let limits = ProviderLimitMap::new();
        // This is the bottom metadata boundary, not a shared ZIP producer:
        // both immutable histories are real ordinary publications, and their
        // exact bindings must survive the current key moving to the second.
        publish_object(&db, request(vec![]).archive, &limits)
            .await
            .unwrap();
        for written in [
            entry("first", "same.txt", "first-cid"),
            entry("second", "same.txt", "second-cid"),
        ] {
            publish_object(
                &db,
                PublicationRequest {
                    object_target: PinTargetSpec {
                        cid: written.cid.clone(),
                        logical_size: written.logical_size,
                    },
                    object: written,
                    tags: vec![],
                    policy: PublicationPolicy {
                        tags: vec![],
                        leases: vec![],
                    },
                },
                &limits,
            )
            .await
            .unwrap();
        }
        assert_eq!(
            crate::store::object::get_latest(&db, "bucket", "same.txt")
                .await
                .unwrap()
                .id,
            "second"
        );
        let tx = db.begin().await.unwrap();
        lock_bucket_for_ownership(&tx, "bucket").await.unwrap();
        let bindings = [
            zip::binding_for_published_object(&tx, "first", "first")
                .await
                .unwrap(),
            zip::binding_for_published_object(&tx, "second", "second")
                .await
                .unwrap(),
        ];
        zip::publish(
            &tx,
            "batch",
            &bindings,
            true,
            "{\"ok\":true}",
            RootOutcome::Disabled,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let snapshot = zip::snapshot(&db, "batch").await.unwrap().unwrap();
        assert_eq!(snapshot.batch.state, "published");
        assert!(snapshot.batch.source_published);
        for (path, object_id) in [("first", "first"), ("second", "second")] {
            let version = object_version::Entity::find()
                .filter(object_version::Column::ObjectId.eq(object_id))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            let manifest = snapshot
                .entries
                .iter()
                .find(|item| item.path == path)
                .unwrap();
            assert_eq!(
                manifest.version_row_id.as_deref(),
                Some(version.id.as_str())
            );
            assert_eq!(version.is_latest, path == "second");
        }
    }
}
