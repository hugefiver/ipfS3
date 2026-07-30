use chrono::Utc;
use sea_orm::EntityTrait;

use crate::{
    error::AppError,
    import::{
        ImportClaim, ImportExecutionError, ImportFailureCode,
        execution_error::{ensure_active, map_publication_error, terminal_failure},
        pipeline::{ImportArtifact, JobCancellation},
    },
    pinning::{policy::PublicationContext, tags::ObjectTag},
    state::AppState,
    store::{
        entities::{import_job, import_job_target},
        import::{
            ownership::{ExpectedImportTarget, ImportPublicationGuard},
            results::{self, ZipResultRecord},
        },
        pinning::publication::{
            PinTargetSpec, PublicationObject, PublicationRequest, PublicationResult,
            ZipPublicationRequest,
        },
    },
    zip::response::ExtractedEntry,
};

#[derive(Clone, Debug)]
pub(crate) struct ZipPublicationEntry {
    pub entry: ExtractedEntry,
    pub target: ExpectedImportTarget,
}

pub(crate) async fn publish_zip_import(
    state: &AppState,
    job: &import_job::Model,
    claim: &ImportClaim,
    artifact: &ImportArtifact,
    entries: &[ZipPublicationEntry],
    records: &[ZipResultRecord],
    cancellation: &JobCancellation,
) -> Result<PublicationResult, ImportExecutionError> {
    ensure_active(cancellation)?;
    let tags: Vec<ObjectTag> = serde_json::from_str(&job.tags_json).map_err(|_| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "persisted import tags are invalid",
        )
    })?;
    let metadata: serde_json::Value = serde_json::from_str(&job.metadata_json).map_err(|_| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "persisted import metadata is invalid",
        )
    })?;
    let policy = state
        .pinning
        .policy()
        .evaluate_publication(PublicationContext {
            bucket: &job.bucket,
            key: &job.key,
            tags: &tags,
            is_decompress_zip: true,
        })
        .map_err(AppError::from)
        .map_err(|error| map_publication_error(error, cancellation))?;
    let archive_size = i64::try_from(artifact.logical_size).map_err(|_| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "import archive size is outside the supported range",
        )
    })?;
    let archive_target = import_job_target::Entity::find_by_id((
        job.id.clone(),
        job.bucket.clone(),
        job.key.clone(),
    ))
    .one(state.store.db())
    .await
    .map_err(AppError::from)
    .map_err(|error| map_publication_error(error, cancellation))?
    .filter(|target| target.kind == "archive")
    .ok_or(ImportExecutionError::Superseded)?;

    let mut targets = Vec::with_capacity(entries.len().saturating_add(1));
    targets.push(ExpectedImportTarget {
        bucket: archive_target.bucket,
        key: archive_target.key,
        generation: archive_target.expected_generation,
    });
    targets.extend(entries.iter().map(|entry| entry.target.clone()));
    targets.sort_by(|left, right| {
        left.bucket
            .cmp(&right.bucket)
            .then_with(|| left.key.cmp(&right.key))
    });
    let guard = ImportPublicationGuard {
        job_id: claim.job_id.clone(),
        worker_id: claim.worker_id.clone(),
        claim_epoch: claim.claim_epoch,
        targets,
    };
    let now = Utc::now();
    let archive_object = PublicationObject::from_put(
        uuid::Uuid::new_v4().to_string(),
        &job.bucket,
        &job.key,
        artifact.cid.clone(),
        archive_size,
        artifact.object_content_type.clone(),
        Some(metadata),
        false,
        None,
        None,
        now,
    );
    let request = ZipPublicationRequest {
        archive: PublicationRequest {
            object: archive_object,
            tags: policy.tags.clone(),
            policy,
            object_target: PinTargetSpec {
                cid: artifact.cid.clone(),
                logical_size: archive_size,
            },
        },
        entries: entries
            .iter()
            .map(|entry| {
                PublicationObject::from_put(
                    uuid::Uuid::new_v4().to_string(),
                    &job.bucket,
                    &entry.entry.key,
                    entry.entry.cid.clone(),
                    entry.entry.size,
                    None,
                    None,
                    false,
                    None,
                    None,
                    now,
                )
            })
            .collect(),
    };
    let result_rows =
        results::zip_publication_rows(&job.id, &job.key, &artifact.cid, archive_size, records)
            .map_err(|error| map_publication_error(error, cancellation))?;
    crate::store::pinning::publication::publish_import_zip(
        state.store.db(),
        request,
        guard,
        result_rows,
        now,
        state.pinning.provider_limits(),
    )
    .await
    .map_err(|error| map_publication_error(error, cancellation))
}
