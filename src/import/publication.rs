use chrono::Utc;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};

use crate::{
    error::AppError,
    import::{
        ImportClaim, ImportExecutionError, ImportFailureCode,
        execution_error::{ensure_active, map_publication_error, terminal_failure},
        pipeline::{ImportArtifact, JobCancellation},
    },
    pinning::{
        decision::ExtensionDecision,
        policy::{PublicationContext, PublicationPolicy},
        tags::{ObjectTag, PinControl},
    },
    state::AppState,
    store::{
        entities::{import_job, import_job_result, import_job_target},
        import::ownership::{ExpectedImportTarget, ImportPublicationGuard},
        pinning::publication::{
            DecidedPublish, PinTargetSpec, PublicationObject, PublicationRequest,
        },
    },
};

pub(crate) mod zip;

/// Called before source I/O and again at publication. A historical job without
/// a captured decision can retain its original ordinary/automatic behavior, but
/// its reserved tags can never turn into newly executable pin instructions.
pub(crate) fn resolved_policy(
    state: &AppState,
    job: &import_job::Model,
    cancellation: &JobCancellation,
) -> Result<(PublicationPolicy, Option<ExtensionDecision>), ImportExecutionError> {
    let tags: Vec<ObjectTag> = serde_json::from_str(&job.tags_json).map_err(|_| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "persisted import tags are invalid",
        )
    })?;
    if let Some(json) = &job.pin_decision_json {
        let decision: ExtensionDecision = serde_json::from_str(json).map_err(|_| {
            terminal_failure(
                ImportFailureCode::PublicationFailed,
                "persisted import pin decision is invalid",
            )
        })?;
        if decision.origin.request_id != job.id {
            return Err(terminal_failure(
                ImportFailureCode::PublicationFailed,
                "persisted import pin decision is invalid",
            ));
        }
        decision
            .verify_revision(
                state.pinning.effective_config(),
                state.pinning.control_mode(),
            )
            .map_err(|_| {
                terminal_failure(
                    ImportFailureCode::PublicationFailed,
                    "captured pinning configuration is unavailable",
                )
            })?;
        let policy = decision.replay_policy(tags).map_err(|_| {
            terminal_failure(
                ImportFailureCode::PublicationFailed,
                "persisted import pin decision is invalid",
            )
        })?;
        return Ok((policy, Some(decision)));
    }
    if !matches!(PinControl::from_tags(&tags), Ok(PinControl::Absent)) {
        return Err(terminal_failure(
            ImportFailureCode::PublicationFailed,
            "legacy import pin intent has no captured decision",
        ));
    }
    let policy = state
        .pinning
        .policy()
        .evaluate_publication(PublicationContext {
            bucket: &job.bucket,
            key: &job.key,
            tags: &tags,
            is_decompress_zip: job.decompress_prefix.is_some(),
        })
        .map_err(AppError::from)
        .map_err(|error| map_publication_error(error, cancellation))?;
    Ok((policy, None))
}

pub(crate) fn decided_publish<'a>(
    state: &'a AppState,
    decision: &'a ExtensionDecision,
) -> DecidedPublish<'a> {
    DecidedPublish {
        decision,
        config: state.pinning.effective_config(),
        mode: state.pinning.control_mode(),
        limits: state.pinning.provider_limits(),
    }
}

pub(crate) async fn publish_direct(
    state: &AppState,
    job: &import_job::Model,
    claim: &ImportClaim,
    artifact: &ImportArtifact,
    cancellation: &JobCancellation,
) -> Result<(), ImportExecutionError> {
    ensure_active(cancellation)?;
    let (policy, decision) = resolved_policy(state, job, cancellation)?;
    let metadata: serde_json::Value = serde_json::from_str(&job.metadata_json).map_err(|_| {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "persisted import metadata is invalid",
        )
    })?;
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
    if let Some(decision) = &decision {
        crate::store::pinning::publication::publish_decided_import_object(
            state.store.db(),
            request,
            guard,
            result_rows,
            Utc::now(),
            decided_publish(state, decision),
        )
        .await
    } else {
        crate::store::pinning::publication::publish_import_object(
            state.store.db(),
            request,
            guard,
            result_rows,
            Utc::now(),
            state.pinning.provider_limits(),
        )
        .await
    }
    .map_err(|error| map_publication_error(error, cancellation))?;
    Ok(())
}
