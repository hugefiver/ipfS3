use std::{sync::Arc, time::Duration};

use s3s::S3Error;
use tokio_util::sync::CancellationToken;

use crate::{
    error::AppError,
    import::{
        ImportClaim, ImportExecutionError, ImportFailure, ImportFailureCode, ImportPhase,
        execution_error::{KuboOperation, map_kubo_error, terminal_failure},
        pipeline::{
            ImportArtifact, ImportExecutionObserver, JobCancellation, NoopImportExecutionObserver,
        },
        progress::ProgressReporter,
        publication::zip::publish_zip_import,
    },
    state::AppState,
    store::{entities::import_job, pinning::publication::PublicationResult},
    zip::extract::{
        MAX_DECOMPRESSED_ARCHIVE_BYTES, ObservedExtractionError, extract_zip_stream_observed,
    },
};

mod observer;

use observer::ImportExtractionObserver;

pub async fn decompress_import(
    state: &Arc<AppState>,
    job: &import_job::Model,
    artifact: ImportArtifact,
    claim: &ImportClaim,
    cancel: CancellationToken,
) -> Result<PublicationResult, ImportExecutionError> {
    let cancellation = JobCancellation {
        shutdown: cancel,
        ownership_lost: CancellationToken::new(),
    };
    let reporter = ProgressReporter::start(
        state.clone(),
        job,
        claim.clone(),
        cancellation.clone(),
        Duration::from_secs(1),
    );
    let execution_observer = NoopImportExecutionObserver;
    let result = decompress_import_with_context(
        state,
        job,
        artifact,
        claim,
        &cancellation,
        &reporter,
        MAX_DECOMPRESSED_ARCHIVE_BYTES,
        &execution_observer,
    )
    .await;
    let reporter_result = reporter.finish(&cancellation).await;
    match (result, reporter_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn decompress_import_with_context(
    state: &Arc<AppState>,
    job: &import_job::Model,
    artifact: ImportArtifact,
    claim: &ImportClaim,
    cancellation: &JobCancellation,
    reporter: &ProgressReporter,
    max_decompressed_bytes: u64,
    execution_observer: &dyn ImportExecutionObserver,
) -> Result<PublicationResult, ImportExecutionError> {
    let target_prefix = job.decompress_prefix.as_deref().ok_or_else(|| {
        terminal_failure(
            ImportFailureCode::InvalidArchive,
            "combined import is missing its ZIP target prefix",
        )
    })?;
    let target_prefix =
        crate::zip::sanitize::normalize_target_prefix(target_prefix).map_err(|_| {
            terminal_failure(
                ImportFailureCode::InvalidArchive,
                "persisted ZIP target prefix is invalid",
            )
        })?;
    reporter
        .phase(ImportPhase::Decompressing, cancellation)
        .await?;
    let stream = tokio::select! {
        biased;
        _ = cancellation.shutdown.cancelled() => return Err(ImportExecutionError::Interrupted),
        _ = cancellation.ownership_lost.cancelled() => return Err(ImportExecutionError::Superseded),
        result = crate::kubo::cat::stream_cat(&state.kubo, &artifact.cid, None) => {
            result.map_err(|error| map_kubo_error(error, KuboOperation::Cat, cancellation))?
        }
    };
    let mut observer =
        ImportExtractionObserver::new(state, &job.bucket, &job.key, claim, cancellation, reporter);
    let outcome = {
        let extraction = extract_zip_stream_observed(
            state,
            &target_prefix,
            Box::pin(stream),
            max_decompressed_bytes,
            &mut observer,
        );
        tokio::pin!(extraction);
        tokio::select! {
            biased;
            _ = cancellation.shutdown.cancelled() => return Err(ImportExecutionError::Interrupted),
            _ = cancellation.ownership_lost.cancelled() => return Err(ImportExecutionError::Superseded),
            result = &mut extraction => match result {
                Ok(outcome) => outcome,
                Err(ObservedExtractionError::Observer(error)) => return Err(error),
                Err(ObservedExtractionError::Limit(_)) => {
                    return Err(terminal_failure(
                        ImportFailureCode::DecompressionLimitExceeded,
                        "ZIP archive exceeds a decompression resource limit",
                    ));
                }
                Err(ObservedExtractionError::Archive(error)) => {
                    return Err(map_archive_error(error, cancellation));
                }
            }
        }
    };
    debug_assert_eq!(outcome.entries.len(), observer.successful.len());
    debug_assert!(
        outcome.failures.len() >= observer.records.len().saturating_sub(outcome.entries.len())
    );

    tokio::select! {
        biased;
        _ = cancellation.shutdown.cancelled() => return Err(ImportExecutionError::Interrupted),
        _ = cancellation.ownership_lost.cancelled() => return Err(ImportExecutionError::Superseded),
        _ = execution_observer.before_publication(&job.id) => {}
    }
    reporter
        .phase(ImportPhase::Publishing, cancellation)
        .await?;
    publish_zip_import(
        state,
        job,
        claim,
        &artifact,
        &observer.successful,
        &observer.records,
        cancellation,
    )
    .await
}

fn map_archive_error(error: S3Error, cancellation: &JobCancellation) -> ImportExecutionError {
    if cancellation.shutdown.is_cancelled() {
        return ImportExecutionError::Interrupted;
    }
    if cancellation.ownership_lost.is_cancelled() {
        return ImportExecutionError::Superseded;
    }
    if error.code().as_str() == "InternalError" {
        return map_kubo_error(
            AppError::kubo_rpc_detail("Kubo archive response stream failed"),
            KuboOperation::Cat,
            cancellation,
        );
    }
    ImportExecutionError::Terminal(ImportFailure {
        code: ImportFailureCode::InvalidArchive,
        message: "ZIP archive is invalid".to_owned(),
        retryable: false,
    })
}

#[cfg(test)]
mod tests;
