//! Source I/O only for a claimed ZIP v2 import. The worker owns authorization,
//! claim fencing, durable binding, replay and publication; none occur here.

use std::time::Duration;

use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    import::{
        downloader::DownloadError,
        pipeline::{AbortOnDropTask, ImportCoordinator},
    },
    kubo::{KuboProgress, add::StreamAddError},
    state::AppState,
    zip::extract::ZipExtractionLimits,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedZipSource {
    /// Locally pinned, complete UnixFS file in the hot Kubo node.
    pub cid: String,
    pub size: u64,
    /// Lowercase hex digest of the *entire* source, not the CID or ZIP prefix.
    pub sha256: String,
}

/// Static, redacted failure codes. Never attach the URL, Kubo response, token,
/// or transport's free-form diagnostic to this error or to a job receipt.
#[derive(Clone, PartialEq, Eq, thiserror::Error)]
pub enum V2SourceError {
    #[error("invalid_source_descriptor")]
    InvalidDescriptor,
    #[error("missing_expected_sha256")]
    MissingExpectedSha256,
    #[error("sha256_mismatch")]
    Sha256Mismatch { measured_sha256: String },
    #[error("source_denied")]
    SourceDenied,
    #[error("source_redirected")]
    SourceRedirected,
    #[error("source_http_error")]
    SourceHttpError,
    #[error("source_too_large")]
    SourceTooLarge,
    #[error("source_incomplete")]
    SourceIncomplete,
    #[error("source_transport")]
    SourceTransport,
    #[error("source_stalled")]
    SourceStalled,
    #[error("cid_unavailable")]
    CidUnavailable,
    #[error("kubo_transport")]
    KuboTransport,
    #[error("canceled")]
    Canceled,
    #[error("deadline_exceeded")]
    DeadlineExceeded,
}

impl std::fmt::Debug for V2SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("V2SourceError").field(&self.code()).finish()
    }
}

impl V2SourceError {
    pub fn measured_sha256(&self) -> Option<&str> {
        match self {
            Self::Sha256Mismatch { measured_sha256 } => Some(measured_sha256),
            _ => None,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidDescriptor => "invalid_source_descriptor",
            Self::MissingExpectedSha256 => "missing_expected_sha256",
            Self::Sha256Mismatch { .. } => "sha256_mismatch",
            Self::SourceDenied => "source_denied",
            Self::SourceRedirected => "source_redirected",
            Self::SourceHttpError => "source_http_error",
            Self::SourceTooLarge => "source_too_large",
            Self::SourceIncomplete => "source_incomplete",
            Self::SourceTransport => "source_transport",
            Self::SourceStalled => "source_stalled",
            Self::CidUnavailable => "cid_unavailable",
            Self::KuboTransport => "kubo_transport",
            Self::Canceled => "canceled",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::SourceTransport
                | Self::SourceStalled
                | Self::SourceIncomplete
                | Self::KuboTransport
                | Self::Canceled
        )
    }
}

fn download_error(error: DownloadError) -> V2SourceError {
    match error {
        DownloadError::Redirect => V2SourceError::SourceRedirected,
        DownloadError::TooLarge => V2SourceError::SourceTooLarge,
        DownloadError::Stalled => V2SourceError::SourceStalled,
        DownloadError::Canceled => V2SourceError::Canceled,
        DownloadError::NotAllowed | DownloadError::TlsCertificate => V2SourceError::SourceDenied,
        DownloadError::HttpStatus(status) if (500..=599).contains(&status) => {
            V2SourceError::SourceTransport
        }
        DownloadError::Dns | DownloadError::Connect | DownloadError::TlsTransport => {
            V2SourceError::SourceTransport
        }
        DownloadError::HttpStatus(_) | DownloadError::InvalidResponse => {
            V2SourceError::SourceHttpError
        }
    }
}

fn download_stream_error(error: DownloadError) -> V2SourceError {
    match error {
        DownloadError::InvalidResponse => V2SourceError::SourceIncomplete,
        error => download_error(error),
    }
}

fn kubo_error(error: crate::error::AppError) -> V2SourceError {
    match error {
        crate::error::AppError::KuboRpc {
            status: Some(400..=499),
            ..
        } => V2SourceError::CidUnavailable,
        _ => V2SourceError::KuboTransport,
    }
}

fn progress_drain() -> (mpsc::Sender<KuboProgress>, AbortOnDropTask) {
    let (sender, mut receiver) = mpsc::channel(32);
    let drain = AbortOnDropTask::new(tokio::spawn(async move {
        while receiver.recv().await.is_some() {}
    }));
    (sender, drain)
}

/// Call only with the private descriptor retrieved after `execution::claim`.
/// No DB or source-object lookup is performed. `limits` supplies the optional
/// processing deadline; compressed input bytes use the import download bound.
pub async fn fetch_v2_zip_source(
    coordinator: &ImportCoordinator,
    state: &AppState,
    source_descriptor: &str,
    expected_sha256: Option<&str>,
    cancel: CancellationToken,
    limits: &ZipExtractionLimits,
) -> Result<VerifiedZipSource, V2SourceError> {
    let (kind, value): (&str, &str) =
        serde_json::from_str(source_descriptor).map_err(|_| V2SourceError::InvalidDescriptor)?;
    if expected_sha256.is_some_and(|sha| {
        sha.len() != 64
            || !sha
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }) {
        return Err(V2SourceError::InvalidDescriptor);
    }
    let deadline = Duration::from_secs(coordinator.config().raw.job_timeout_secs);
    let deadline = limits
        .processing_deadline_secs()
        .map_or(deadline, |secs| deadline.min(Duration::from_secs(secs)));
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(V2SourceError::Canceled),
        result = tokio::time::timeout(deadline, fetch_inner(coordinator, state, kind, value, expected_sha256, cancel.clone())) => {
            result.map_err(|_| V2SourceError::DeadlineExceeded)?
        }
    }
}

async fn fetch_inner(
    coordinator: &ImportCoordinator,
    state: &AppState,
    kind: &str,
    value: &str,
    expected_sha256: Option<&str>,
    cancel: CancellationToken,
) -> Result<VerifiedZipSource, V2SourceError> {
    let max_bytes = coordinator
        .config()
        .raw
        .max_download_bytes
        .min(state.pinning.zip_extraction_limits().max_archive_bytes())
        .min(i64::MAX as u64);
    match kind {
        "url" => {
            let expected = expected_sha256.ok_or(V2SourceError::MissingExpectedSha256)?;
            let url = Url::parse(value).map_err(|_| V2SourceError::InvalidDescriptor)?;
            let (progress, _receiver) = tokio::sync::watch::channel(0);
            let download = coordinator
                .downloader()
                .open(&url, progress, cancel.clone())
                .await
                .map_err(download_error)?;
            if download.total.is_some_and(|total| total > max_bytes) {
                return Err(V2SourceError::SourceTooLarge);
            }
            let total = download.total;
            let (complete, measured) = oneshot::channel();
            let body = async_stream::stream! {
                let mut source = download.body;
                let mut hasher = Sha256::new();
                let mut size = 0_u64;
                while let Some(frame) = source.next().await {
                    let bytes = match frame {
                        Ok(bytes) => bytes,
                        Err(error) => { yield Err(download_stream_error(error)); return; }
                    };
                    size = match size.checked_add(bytes.len() as u64) {
                        Some(next) if next <= max_bytes => next,
                        _ => { yield Err(V2SourceError::SourceTooLarge); return; }
                    };
                    hasher.update(&bytes);
                    yield Ok(bytes);
                }
                if total.is_some_and(|declared| declared != size) {
                    yield Err(V2SourceError::SourceIncomplete);
                    return;
                }
                let sha256 = hex::encode(hasher.finalize());
                if sha256 != expected {
                     yield Err(V2SourceError::Sha256Mismatch { measured_sha256: sha256 });
                    return;
                }
                let _ = complete.send((size, sha256));
            };
            let (progress, _drain) = progress_drain();
            let added = crate::kubo::add::stream_add_with_progress(
                &state.kubo,
                body,
                1,
                progress,
                cancel.clone(),
            )
            .await
            .map_err(|error| match error {
                StreamAddError::Source(error) => error,
                StreamAddError::Kubo(_) => V2SourceError::KuboTransport,
                StreamAddError::Canceled => V2SourceError::Canceled,
            })?;
            // Both the source stream's clean EOF and the upload body's complete
            // drain are necessary: a Kubo root response alone proves neither.
            let (size, sha256) = measured
                .await
                .map_err(|_| V2SourceError::SourceIncomplete)?;
            if cancel.is_cancelled() {
                return Err(V2SourceError::Canceled);
            }
            let cid = cid::Cid::try_from(added.cid.as_str())
                .map_err(|_| V2SourceError::KuboTransport)?
                .to_string();
            let (progress, _drain) = progress_drain();
            crate::kubo::pin::pin_add_with_progress(&state.kubo, &cid, progress, cancel.clone())
                .await
                .map_err(kubo_error)?;
            if cancel.is_cancelled() {
                return Err(V2SourceError::Canceled);
            }
            Ok(VerifiedZipSource { cid, size, sha256 })
        }
        "cid" => {
            let cid = cid::Cid::try_from(value)
                .map_err(|_| V2SourceError::InvalidDescriptor)?
                .to_string();
            let (progress, _drain) = progress_drain();
            crate::kubo::pin::pin_add_with_progress(&state.kubo, &cid, progress, cancel.clone())
                .await
                .map_err(kubo_error)?;
            let size = crate::kubo::cat::inspect_file(&state.kubo, &cid, cancel.clone())
                .await
                .map_err(kubo_error)?;
            if size > max_bytes {
                return Err(V2SourceError::SourceTooLarge);
            }
            let stream = tokio::select! {
                _ = cancel.cancelled() => return Err(V2SourceError::Canceled),
                stream = crate::kubo::cat::stream_cat(&state.kubo, &cid, None) => stream.map_err(kubo_error)?,
            };
            tokio::pin!(stream);
            let mut hasher = Sha256::new();
            let mut measured_size = 0_u64;
            loop {
                let next = tokio::select! {
                    _ = cancel.cancelled() => return Err(V2SourceError::Canceled),
                    next = stream.next() => next,
                };
                let Some(chunk) = next else { break };
                let bytes = chunk.map_err(|_| V2SourceError::KuboTransport)?;
                measured_size = measured_size
                    .checked_add(bytes.len() as u64)
                    .filter(|next| *next <= max_bytes)
                    .ok_or(V2SourceError::SourceTooLarge)?;
                hasher.update(&bytes);
            }
            if measured_size != size {
                return Err(V2SourceError::SourceIncomplete);
            }
            let sha256 = hex::encode(hasher.finalize());
            if expected_sha256.is_some_and(|expected| expected != sha256) {
                return Err(V2SourceError::Sha256Mismatch {
                    measured_sha256: sha256,
                });
            }
            if cancel.is_cancelled() {
                return Err(V2SourceError::Canceled);
            }
            Ok(VerifiedZipSource { cid, size, sha256 })
        }
        _ => Err(V2SourceError::InvalidDescriptor),
    }
}
