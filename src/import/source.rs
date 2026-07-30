use std::pin::Pin;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    import::{
        ImportExecutionError, ImportFailureCode, ImportPhase,
        downloader::DownloadError,
        execution_error::{
            KuboOperation, map_download_error, map_kubo_error, map_url_stream_add_error,
            terminal_failure,
        },
        pipeline::{ImportArtifact, ImportCoordinator, JobCancellation},
        progress::ProgressReporter,
    },
    state::AppState,
    store::entities::import_job,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum UrlStreamError {
    #[error("URL download failed")]
    Download(#[source] DownloadError),
    #[error("URL download phase transition failed")]
    Phase(#[source] ImportExecutionError),
}

pub(crate) async fn execute_cid(
    coordinator: &ImportCoordinator,
    state: &AppState,
    job: &import_job::Model,
    cancellation: &JobCancellation,
    network_cancel: &CancellationToken,
    reporter: &ProgressReporter,
) -> Result<ImportArtifact, ImportExecutionError> {
    let canonical_cid = cid::Cid::try_from(job.source_value.as_str())
        .map_err(|_| {
            terminal_failure(
                ImportFailureCode::CidNotFound,
                "persisted source CID is invalid",
            )
        })?
        .to_string();
    reporter
        .phase(ImportPhase::DiscoveringProviders, cancellation)
        .await?;
    crate::kubo::routing::find_providers(
        &state.kubo,
        &canonical_cid,
        coordinator.config().raw.max_provider_records,
        reporter.kubo.clone(),
        network_cancel.clone(),
    )
    .await
    .map_err(|error| map_kubo_error(error, KuboOperation::Discover, cancellation))?;

    reporter
        .phase(ImportPhase::PinningLocal, cancellation)
        .await?;
    crate::kubo::pin::pin_add_with_progress(
        &state.kubo,
        &canonical_cid,
        reporter.kubo.clone(),
        network_cancel.clone(),
    )
    .await
    .map_err(|error| map_kubo_error(error, KuboOperation::Pin, cancellation))?;

    reporter
        .phase(ImportPhase::Inspecting, cancellation)
        .await?;
    let logical_size =
        crate::kubo::cat::inspect_file(&state.kubo, &canonical_cid, network_cancel.clone())
            .await
            .map_err(|error| map_kubo_error(error, KuboOperation::Inspect, cancellation))?;
    reporter.logical_size(logical_size, cancellation).await?;

    Ok(ImportArtifact {
        cid: canonical_cid,
        logical_size,
        object_content_type: None,
    })
}

pub(crate) async fn execute_url(
    coordinator: &ImportCoordinator,
    state: &AppState,
    job: &import_job::Model,
    cancellation: &JobCancellation,
    network_cancel: &CancellationToken,
    reporter: &ProgressReporter,
) -> Result<ImportArtifact, ImportExecutionError> {
    let source = Url::parse(&job.source_value).map_err(|_| {
        terminal_failure(
            ImportFailureCode::SourceUnreachable,
            "persisted source URL is invalid",
        )
    })?;
    reporter
        .phase(ImportPhase::Downloading, cancellation)
        .await?;
    let download = coordinator
        .downloader()
        .open(&source, reporter.download.clone(), network_cancel.clone())
        .await
        .map_err(|error| map_download_error(error, cancellation))?;
    reporter
        .download_total(download.total, cancellation)
        .await?;
    let content_type = download.content_type;

    let body = phase_at_download_eof(download.body, reporter, cancellation);
    let added = crate::kubo::add::stream_add_with_progress(
        &state.kubo,
        body,
        1,
        reporter.kubo.clone(),
        network_cancel.clone(),
    )
    .await
    .map_err(|error| map_url_stream_add_error(error, cancellation))?;

    reporter
        .phase(ImportPhase::PinningLocal, cancellation)
        .await?;
    crate::kubo::pin::pin_add_with_progress(
        &state.kubo,
        &added.cid,
        reporter.kubo.clone(),
        network_cancel.clone(),
    )
    .await
    .map_err(|error| map_kubo_error(error, KuboOperation::Pin, cancellation))?;

    let logical_size = *reporter.download.borrow();
    reporter.logical_size(logical_size, cancellation).await?;
    Ok(ImportArtifact {
        cid: added.cid,
        logical_size,
        object_content_type: content_type,
    })
}

fn phase_at_download_eof<'a>(
    mut body: Pin<Box<dyn Stream<Item = Result<Bytes, DownloadError>> + Send>>,
    reporter: &'a ProgressReporter,
    cancellation: &'a JobCancellation,
) -> impl Stream<Item = Result<Bytes, UrlStreamError>> + Send + 'a {
    async_stream::stream! {
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(bytes) => yield Ok(bytes),
                Err(error) => {
                    yield Err(UrlStreamError::Download(error));
                    return;
                }
            }
        }
        if let Err(error) = reporter.phase(ImportPhase::AddingToIpfs, cancellation).await {
            yield Err(UrlStreamError::Phase(error));
        }
    }
}
