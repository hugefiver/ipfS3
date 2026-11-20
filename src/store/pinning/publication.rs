use std::collections::BTreeSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    EntityTrait, QueryFilter, QuerySelect, Set, TransactionError, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    lifecycle::model::MultipartUploadTargetIdentity,
    pinning::{
        config::ProviderLimitMap,
        decision::ExtensionDecision,
        policy::PublicationPolicy,
        tags::{ContentMode, ObjectTag},
    },
    store::{
        entities::{bucket, import_job_result, multipart_upload, object, object_version},
        import::ownership::{
            ImportPublicationGuard, StandardMutationGuard, admit_content_mutation_in_transaction,
            complete_publication_in_transaction, complete_standard_mutation_in_transaction,
            lock_bucket_for_ownership, verify_publication_guard, verify_standard_mutation_guard,
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

use super::{resources, tags};
mod hot_receipt;
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

/// An authenticated control capture resolved against the current publication policy.
pub struct DecidedPublish<'a> {
    pub decision: &'a ExtensionDecision,
    pub config: &'a crate::pinning::config::ValidatedPinningConfig,
    pub mode: crate::config::OptionalPinControlMode,
    pub limits: &'a ProviderLimitMap,
}

impl DecidedPublish<'_> {
    fn validate(&self, request: &PublicationRequest) -> AppResult<()> {
        let current = crate::pinning::policy::PinPolicyEvaluator::with_mode(self.config, self.mode)
            .resolve_captured_publication(
                crate::pinning::policy::PublicationContext {
                    bucket: &request.object.bucket,
                    key: &request.object.key,
                    tags: &request.policy.tags,
                    is_decompress_zip: false,
                },
                self.decision,
            )?;
        if current != request.policy {
            return Err(invalid_publication(
                "publication does not match current policy",
            ));
        }
        Ok(())
    }

    fn snapshot(&self, policy: &PublicationPolicy) -> AppResult<DecidedSnapshot> {
        let mut limits = self.limits.clone();
        for name in policy.leases.iter().flat_map(|intent| &intent.providers) {
            let provider = self
                .config
                .providers
                .iter()
                .find(|provider| &provider.name == name)
                .ok_or_else(|| invalid_publication("current pinning provider is unavailable"))?;
            let limit = limits
                .get_mut(name)
                .ok_or_else(|| invalid_publication("current provider limits unavailable"))?;
            limit.enabled &= !provider.identity.retired;
        }
        Ok(DecidedSnapshot {
            decision: self.decision.clone(),
            limits,
            zip_current: None,
        })
    }
}

#[derive(Clone)]
struct DecidedSnapshot {
    zip_current: Option<(
        crate::pinning::config::ValidatedPinningConfig,
        crate::pinning::zip_policy::ValidatedZipOutputRules,
        crate::config::OptionalPinControlMode,
    )>,
    decision: ExtensionDecision,
    limits: ProviderLimitMap,
}
pub fn preflight_decided_routes(
    captured: DecidedPublish<'_>,
    policy: &PublicationPolicy,
) -> AppResult<()> {
    captured.snapshot(policy).map(|_| ())
}
pub(crate) fn preflight_captured_zip_routes(
    capture: &crate::pinning::zip_policy::CapturedZipIntent,
    current: &crate::pinning::config::ValidatedPinningConfig,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
    mode: crate::config::OptionalPinControlMode,
    manifest: Option<&crate::zip::batch::FinalZipManifest>,
) -> AppResult<()> {
    let selection = capture
        .resolve_current_with_rules(current, rules, mode)
        .map_err(|error| invalid_publication(&error.to_string()))?;
    for frozen in selection.io_provider_candidates(manifest) {
        let unavailable =
            || invalid_publication("captured pinning configuration revision is unavailable");
        let provider = current
            .providers
            .iter()
            .find(|provider| provider.name == frozen.config_name)
            .ok_or_else(unavailable)?;
        if !provider
            .route_snapshot()
            .execution_compatible(&frozen.route)
            || (frozen.enabled && (!provider.limits.enabled || provider.identity.retired))
        {
            return Err(unavailable());
        }
    }
    Ok(())
}
pub async fn publish_decided_object(
    db: &DatabaseConnection,
    request: PublicationRequest,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    captured.validate(&request)?;
    let snapshot = captured.snapshot(&request.policy)?;
    run_publication_with_retries_and_hot_receipt(
        db,
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
        Some(snapshot),
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
    let snapshot = captured.snapshot(&request.policy)?;
    require_decision_origin(captured.decision, &guard.mutation_id)?;
    run_publication_with_retries_and_hot_receipt(
        db,
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
        Some(snapshot),
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
    let snapshot = captured.snapshot(&request.policy)?;
    require_decision_origin(captured.decision, &guard.mutation_id)?;
    let receipt = match HotPublicationReceipt::validate(receipt, &request.object.cid) {
        Ok(receipt) => receipt,
        Err(error) => {
            release_failed_mutation(db, Some(&guard), true).await;
            return Err(error);
        }
    };
    run_publication_with_retries_and_hot_receipt(
        db,
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
        Some(snapshot),
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
    let snapshot = captured.snapshot(&request.policy).map_err(|source| {
        CommitCompletedUploadError::RolledBack {
            completion_attempt_id,
            source,
        }
    })?;
    run_completed_publication_with_retries_and_decision(
        db,
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
    let snapshot = captured.snapshot(&request.policy)?;
    require_decision_origin(captured.decision, &guard.job_id)?;
    run_publication_with_retries_and_hot_receipt(
        db,
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
        Some(snapshot),
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
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> AppResult<PublicationResult> {
    publish_decided_zip_with_batch(db, request, guard, None, captured, rules).await
}

/// Finalizes the prepared manifest in the very same transaction that writes
/// archive, entries, pin references and the standard mutation completion.
pub async fn publish_decided_zip_batch(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    batch: ZipBatchPublication,
    captured: DecidedPublish<'_>,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> AppResult<PublicationResult> {
    publish_decided_zip_with_batch(db, request, guard, Some(batch), captured, rules).await
}

async fn publish_decided_zip_with_batch(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    batch: Option<ZipBatchPublication>,
    captured: DecidedPublish<'_>,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> AppResult<PublicationResult> {
    captured
        .decision
        .validate_snapshot()
        .map_err(invalid_publication)?;
    if let Some(guard) = &guard {
        require_decision_origin(captured.decision, &guard.mutation_id)?;
    }
    if let Some(batch) = &batch {
        batch.validate(&request.archive.object, &request.entries)?;
    }
    run_publication_with_retries_and_hot_receipt(
        db,
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
        Some(zip_decided_snapshot(&captured, rules)?),
    )
    .await
}

pub async fn publish_decided_completed_zip(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    captured: DecidedPublish<'_>,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    publish_decided_completed_zip_with_batch(
        db,
        upload_target,
        request,
        guard,
        None,
        None,
        captured,
        rules,
    )
    .await
}

/// MPU completion and batch publication share the original commit/unknown-outcome
/// classification. On OutcomeUnknown, reconcile the upload AND batch before retrying.
#[allow(clippy::too_many_arguments)]
pub async fn publish_decided_completed_zip_batch(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    batch: ZipBatchPublication,
    replay: ZipCompletedReplay,
    captured: DecidedPublish<'_>,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    publish_decided_completed_zip_with_batch(
        db,
        upload_target,
        request,
        guard,
        Some(batch),
        Some(replay),
        captured,
        rules,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn publish_decided_completed_zip_with_batch(
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    batch: Option<ZipBatchPublication>,
    replay: Option<ZipCompletedReplay>,
    captured: DecidedPublish<'_>,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    let completion_attempt_id = request.archive.object.id.clone();
    captured
        .decision
        .validate_snapshot()
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
    let snapshot = zip_decided_snapshot(&captured, rules).map_err(|source| {
        CommitCompletedUploadError::RolledBack {
            completion_attempt_id,
            source,
        }
    })?;
    run_completed_publication_with_retries_and_decision(
        db,
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
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> AppResult<PublicationResult> {
    publish_decided_import_zip_with_batch(
        db,
        request,
        guard,
        result_rows,
        now,
        None,
        captured,
        rules,
    )
    .await
}

/// Import completion, result rows, and batch finalization are one atomic write.
#[allow(clippy::too_many_arguments)]
pub async fn publish_decided_import_zip_batch(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    batch: ZipBatchPublication,
    captured: DecidedPublish<'_>,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> AppResult<PublicationResult> {
    publish_decided_import_zip_with_batch(
        db,
        request,
        guard,
        result_rows,
        now,
        Some(batch),
        captured,
        rules,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn publish_decided_import_zip_with_batch(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    batch: Option<ZipBatchPublication>,
    captured: DecidedPublish<'_>,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> AppResult<PublicationResult> {
    captured
        .decision
        .validate_snapshot()
        .map_err(invalid_publication)?;
    require_decision_origin(captured.decision, &guard.job_id)?;
    if let Some(batch) = &batch {
        batch.validate(&request.archive.object, &request.entries)?;
    }
    run_publication_with_retries_and_hot_receipt(
        db,
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
        Some(zip_decided_snapshot(&captured, rules)?),
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

fn zip_decided_snapshot(
    captured: &DecidedPublish<'_>,
    rules: &crate::pinning::zip_policy::ValidatedZipOutputRules,
) -> AppResult<DecidedSnapshot> {
    let decision = captured.decision;
    decision.validate_snapshot().map_err(invalid_publication)?;
    Ok(DecidedSnapshot {
        decision: decision.clone(),
        limits: captured.limits.clone(),
        zip_current: Some((captured.config.clone(), rules.clone(), captured.mode)),
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
        db,
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

pub(crate) async fn delete_version_authorized(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    selector: VersionSelector,
    needs_existing_tags: bool,
    authorize: impl Fn(Option<&[ObjectTag]>) -> AppResult<()> + Send + Sync,
    now: DateTime<Utc>,
) -> AppResult<DeleteVersionResult> {
    for retry in 0..=MAX_TRANSACTION_RETRIES {
        let txn = db.begin().await?;
        let result = async {
            lock_bucket_for_ownership(&txn, bucket).await?;
            let state = crate::store::bucket::get_versioning_state(&txn, bucket).await?;
            let current = crate::store::object_version::lock_current_row(&txn, bucket, key).await?;
            if needs_existing_tags {
                // Tagging locks the same version row before writing tags. In a suspended
                // bucket a simple delete displaces the null slot, even if it is not current.
                let object_id = match (&selector, state) {
                    (VersionSelector::Exact(_), BucketVersioningState::Unversioned) => None,
                    (VersionSelector::Exact(version), _) => {
                        locked_delete_target_object_id(&txn, bucket, key, version).await?
                    }
                    (VersionSelector::Current, BucketVersioningState::Suspended) => {
                        locked_delete_target_object_id(&txn, bucket, key, &PublicVersionId::Null)
                            .await?
                    }
                    (VersionSelector::Current, BucketVersioningState::Unversioned) => {
                        lock_current_object_projection(&txn, bucket, key)
                            .await?
                            .map(|object| object.id)
                    }
                    (VersionSelector::Current, BucketVersioningState::Enabled) => {
                        current.and_then(|row| row.object_id)
                    }
                };
                let existing_tags = match object_id {
                    Some(id) => tags::list_object_tags(&txn, &id).await?,
                    None => Vec::new(),
                };
                authorize(Some(&existing_tags))?;
            } else {
                authorize(None)?;
            }

            // Admission can supersede active imports, so it must be covered by the
            // same authorization and rollback as the version mutation.
            let guard = admit_content_mutation_in_transaction(
                &txn,
                bucket,
                key,
                None,
                crate::import::SupersedeReason::DeleteObject,
                now,
            )
            .await?;
            let deleted =
                delete_version_in_transaction(&txn, bucket, key, selector.clone(), state, now)
                    .await?;
            complete_standard_mutation_in_transaction(&txn, &guard, now).await?;
            Ok(deleted)
        }
        .await;
        match result {
            Ok(deleted) => {
                txn.commit().await?;
                return Ok(deleted);
            }
            Err(error) => {
                txn.rollback().await?;
                if !is_retryable_transaction_conflict(&error) || retry == MAX_TRANSACTION_RETRIES {
                    return Err(error);
                }
                publication_retry_delay(retry).await;
            }
        }
    }
    unreachable!("authorized version delete retry loop exhausted without returning")
}

async fn locked_delete_target_object_id(
    txn: &DatabaseTransaction,
    bucket: &str,
    key: &str,
    version: &PublicVersionId,
) -> AppResult<Option<String>> {
    Ok(
        crate::store::object_version::lock_indexed_version(txn, bucket, key, version)
            .await?
            .and_then(|(row, _)| row.object_id),
    )
}

pub async fn delete_version_guarded(
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
            let (bucket, key) = (bucket.as_str(), key.as_str());
            lock_bucket_for_ownership(txn, bucket).await?;
            verify_standard_mutation_guard(txn, &guard, bucket, key, &[]).await?;
            let state = crate::store::bucket::get_versioning_state(txn, bucket).await?;
            let _ = crate::store::object_version::lock_current_row(txn, bucket, key).await?;
            let result =
                delete_version_in_transaction(txn, bucket, key, selector, state, now).await?;
            complete_standard_mutation_in_transaction(txn, &guard, now).await?;
            Ok(result)
        })
    })
    .await
}

async fn delete_version_in_transaction(
    txn: &DatabaseTransaction,
    bucket: &str,
    key: &str,
    selector: VersionSelector,
    state: BucketVersioningState,
    now: DateTime<Utc>,
) -> AppResult<DeleteVersionResult> {
    match (selector, state) {
        (VersionSelector::Current, BucketVersioningState::Unversioned) => {
            delete_unversioned_current_in_transaction(txn, bucket, key, now).await
        }
        (VersionSelector::Current, BucketVersioningState::Enabled) => {
            delete_enabled_current_in_transaction(txn, bucket, key, now).await
        }
        (VersionSelector::Current, BucketVersioningState::Suspended) => {
            delete_suspended_current_in_transaction(txn, bucket, key, now).await
        }
        (VersionSelector::Exact(_), BucketVersioningState::Unversioned) => {
            Err(AppError::InvalidArgument(
                "version IDs are unavailable for an unversioned bucket".to_owned(),
            ))
        }
        (VersionSelector::Exact(version), _) => {
            delete_exact_in_transaction(txn, bucket, key, version, now).await
        }
    }
}

pub(crate) async fn delete_unversioned_current_in_transaction(
    db: &DatabaseTransaction,
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
        crate::store::object::set_only_latest(db, bucket, key, None).await?;
        resources::release_owner(db, &current.id, now).await?;
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

pub(crate) async fn delete_suspended_current_in_transaction(
    db: &DatabaseTransaction,
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
        resources::release_owner(db, &object.id, now).await?;
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
pub(crate) async fn delete_exact_in_transaction(
    db: &DatabaseTransaction,
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
        resources::release_owner(db, &object.id, now).await?;
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
        db,
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
    db: &DatabaseConnection,
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
    release_failed_mutation(db, standard_guard.as_ref(), result.is_err()).await;
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
        db,
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
    db: &DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    batch: Option<ZipBatchPublication>,
    standard_guard: Option<StandardMutationGuard>,
    limits: &ProviderLimitMap,
    decision: Option<DecidedSnapshot>,
    replay: Option<ZipCompletedReplay>,
) -> Result<PublicationResult, CommitCompletedUploadError> {
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
    release_failed_mutation(db, standard_guard.as_ref(), result.is_err()).await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn publication_attempt(
    db: &DatabaseConnection,
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
) -> Result<PublicationResult, TransactionError<AppError>> {
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
    mut result_rows: Vec<import_job_result::ActiveModel>,
    _import_now: Option<DateTime<Utc>>,
    hot_receipt: Option<&HotPublicationReceipt>,
    limits: &ProviderLimitMap,
    decided: Option<&DecidedSnapshot>,
    replay: Option<&ZipCompletedReplay>,
) -> AppResult<PublicationResult> {
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
            limits,
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
            .replay_policy(request.policy.tags.clone())
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
        verify_standard_mutation_guard(db, guard, &request.object.bucket, &request.object.key, &[])
            .await?;
    } else if let Some(guard) = import_guard {
        lock_bucket_for_ownership(db, &request.object.bucket).await?;
        let publication_targets = [(request.object.bucket.clone(), request.object.key.clone())];
        verify_publication_guard(db, guard, &request.object.bucket, &publication_targets).await?;
    } else if upload_target.is_some() {
        lock_bucket_for_ownership(db, &request.object.bucket).await?;
    } else {
        acquire_sqlite_publication_write_intent(db, &request.object.bucket).await?;
    }
    let versioning_state =
        crate::store::bucket::lock_versioning_state(db, &request.object.bucket).await?;
    let object_id = request.object.id.clone();

    crate::store::object_version::lock_current_row(db, &request.object.bucket, &request.object.key)
        .await?;
    crate::store::residency::prepare_hot_publication_frontier(db, vec![request.object.cid.clone()])
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

    let limits = decided.map_or(limits, |snapshot| &snapshot.limits);
    for intent in &request.policy.leases {
        if intent.content_mode == ContentMode::Object {
            resources::attach(
                db,
                &object_id,
                &request.object_target,
                intent,
                limits,
                publication_time,
            )
            .await?;
        }
    }
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

async fn write_object_version_and_update_lifecycle(
    db: &DatabaseTransaction,
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
        resources::release_owner(db, &displaced_object_id, now).await?;
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
            entities::{object, object_version},
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
        let capture = tests::zip_fixture::original_capture(archive, prefix, &guard.mutation_id);
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
            &crate::pinning::zip_policy::ValidatedZipOutputRules::compile(&[], &config).unwrap(),
        )
        .await;
        assert!(matches!(
            result,
            Err(AppError::Internal(message)) if message.contains("stale ZIP batch")
        ));
        assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(
            tests::sql_count(&db, "SELECT COUNT(*) n FROM pin_refs").await,
            0
        );
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
        let decided = tests::zip_fixture::current_snapshot(
            &db,
            &batch.batch_id,
            &zip_request.archive,
            &limits,
        )
        .await;
        let error = publication_attempt(
            &db,
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
            Some(decided),
            None,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&error, TransactionError::Transaction(AppError::Internal(_))),
            "{error:?}"
        );
        assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(
            tests::sql_count(&db, "SELECT COUNT(*) n FROM pin_refs").await,
            0
        );
        assert_eq!(
            tests::sql_count(&db, "SELECT COUNT(*) n FROM pin_resources").await,
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
