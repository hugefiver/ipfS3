//! Application publication retains the original Store through every retry.
use super::*;
use crate::store::Store;

pub(crate) async fn publish_object(
    store: &Store,
    request: PublicationRequest,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    run_publication_with_retries_and_hot_receipt(
        PublicationDatabase::Store(store),
        request,
        Vec::new(),
        None,
        None,
        None,
        None,
        Vec::new(),
        None,
        None,
        limits,
        None,
    )
    .await
}

pub(crate) async fn publish_decided_standard_object(
    store: &Store,
    request: PublicationRequest,
    guard: StandardMutationGuard,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    captured.validate(&request)?;
    require_decision_origin(captured.decision, &guard.mutation_id)?;
    run_publication_with_retries_and_hot_receipt(
        PublicationDatabase::Store(store),
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

pub(crate) async fn publish_decided_standard_object_with_hot_receipt(
    store: &Store,
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
            release_failed_mutation(store.db(), Some(&guard), true).await;
            return Err(error);
        }
    };
    run_publication_with_retries_and_hot_receipt(
        PublicationDatabase::Store(store),
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

pub(crate) async fn publish_decided_completed_upload(
    store: &Store,
    upload_target: &MultipartUploadTargetIdentity,
    request: PublicationRequest,
    guard: Option<StandardMutationGuard>,
    captured: DecidedPublish<'_>,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    let snapshot = captured
        .validate(&request)
        .and_then(|()| require_decision_origin(captured.decision, &upload_target.upload_id))
        .and_then(|()| captured.snapshot())
        .map_err(|source| CommitCompletedUploadError::RolledBack {
            completion_attempt_id: request.object.id.clone(),
            source,
        })?;
    run_completed_publication_with_retries_and_decision(
        PublicationDatabase::Store(store),
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

pub(crate) async fn publish_standard_completed_upload(
    store: &Store,
    upload_target: &MultipartUploadTargetIdentity,
    request: PublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries_and_decision(
        PublicationDatabase::Store(store),
        upload_target,
        request,
        Vec::new(),
        None,
        Some(guard),
        limits,
        None,
        None,
    )
    .await
}

pub(crate) async fn publish_decided_completed_zip(
    store: &Store,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: Option<StandardMutationGuard>,
    captured: DecidedPublish<'_>,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    let snapshot = captured
        .decision
        .validate_policy(&request.archive.policy)
        .map_err(invalid_publication)
        .and_then(|()| require_decision_origin(captured.decision, &upload_target.upload_id))
        .and_then(|()| zip_decided_snapshot(captured.decision))
        .map_err(|source| CommitCompletedUploadError::RolledBack {
            completion_attempt_id: request.archive.object.id.clone(),
            source,
        })?;
    run_completed_publication_with_retries_and_decision(
        PublicationDatabase::Store(store),
        upload_target,
        request.archive,
        request.entries,
        None,
        guard,
        captured.limits,
        Some(snapshot),
        None,
    )
    .await
}

pub(crate) async fn publish_standard_completed_zip(
    store: &Store,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError> {
    run_completed_publication_with_retries_and_decision(
        PublicationDatabase::Store(store),
        upload_target,
        request.archive,
        request.entries,
        None,
        Some(guard),
        limits,
        None,
        None,
    )
    .await
}

pub(crate) async fn publish_decided_import_object(
    store: &Store,
    request: PublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    captured: DecidedPublish<'_>,
) -> AppResult<PublicationResult> {
    captured.validate(&request)?;
    require_decision_origin(captured.decision, &guard.job_id)?;
    run_publication_with_retries_and_hot_receipt(
        PublicationDatabase::Store(store),
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

pub(crate) use super::zip::publish_captured_zip_store as publish_captured_zip;
