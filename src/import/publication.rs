use chrono::Utc;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};

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
        entities::{import_job, import_job_result, import_job_target},
        import::ownership::{ExpectedImportTarget, ImportPublicationGuard},
        pinning::publication::{PinTargetSpec, PublicationObject, PublicationRequest},
    },
};

pub(crate) mod zip;

pub(crate) async fn publish_direct(
    state: &AppState,
    job: &import_job::Model,
    claim: &ImportClaim,
    artifact: &ImportArtifact,
    cancellation: &JobCancellation,
) -> Result<(), ImportExecutionError> {
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
            is_decompress_zip: false,
        })
        .map_err(AppError::from)
        .map_err(|error| map_publication_error(error, cancellation))?;
    let logical_size = i64::try_from(artifact.logical_size).map_err(|_| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "import object size is outside the supported range",
        )
    })?;
    let targets = import_job_target::Entity::find()
        .filter(import_job_target::Column::JobId.eq(&job.id))
        .all(state.store.db())
        .await
        .map_err(AppError::from)
        .map_err(|error| map_publication_error(error, cancellation))?
        .into_iter()
        .map(|target| ExpectedImportTarget {
            bucket: target.bucket,
            key: target.key,
            generation: target.expected_generation,
        })
        .collect();
    let guard = ImportPublicationGuard {
        job_id: claim.job_id.clone(),
        worker_id: claim.worker_id.clone(),
        claim_epoch: claim.claim_epoch,
        targets,
    };
    let request = PublicationRequest {
        object: PublicationObject::from_put(
            uuid::Uuid::new_v4().to_string(),
            &job.bucket,
            &job.key,
            artifact.cid.clone(),
            logical_size,
            artifact.object_content_type.clone(),
            Some(metadata),
            false,
            None,
            None,
            Utc::now(),
        ),
        tags: policy.tags.clone(),
        policy,
        object_target: PinTargetSpec {
            cid: artifact.cid.clone(),
            logical_size,
        },
    };
    let result_rows = vec![import_job_result::ActiveModel {
        job_id: Set(job.id.clone()),
        sequence: Set(0),
        key: Set(job.key.clone()),
        cid: Set(Some(artifact.cid.clone())),
        size: Set(Some(logical_size)),
        error_code: Set(None),
        error_message: Set(None),
    }];
    crate::store::pinning::publication::publish_import_object(
        state.store.db(),
        request,
        guard,
        result_rows,
        Utc::now(),
        state.pinning.provider_limits(),
    )
    .await
    .map_err(|error| map_publication_error(error, cancellation))?;
    Ok(())
}
