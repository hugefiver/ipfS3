use crate::{
    error::AppError,
    import::{
        ImportExecutionError, ImportFailure, ImportFailureCode, downloader::DownloadError,
        pipeline::JobCancellation, source::UrlStreamError,
    },
    kubo::add::StreamAddError,
};

pub(crate) fn ensure_active(cancellation: &JobCancellation) -> Result<(), ImportExecutionError> {
    if cancellation.shutdown.is_cancelled() {
        Err(ImportExecutionError::Interrupted)
    } else if cancellation.ownership_lost.is_cancelled() {
        Err(ImportExecutionError::Superseded)
    } else {
        Ok(())
    }
}

pub(crate) fn cancellation_error(cancellation: &JobCancellation) -> ImportExecutionError {
    if cancellation.shutdown.is_cancelled() {
        ImportExecutionError::Interrupted
    } else {
        ImportExecutionError::Superseded
    }
}

pub(crate) fn map_download_error(
    error: DownloadError,
    cancellation: &JobCancellation,
) -> ImportExecutionError {
    match error {
        DownloadError::Canceled if cancellation.shutdown.is_cancelled() => {
            ImportExecutionError::Interrupted
        }
        DownloadError::Canceled if cancellation.ownership_lost.is_cancelled() => {
            ImportExecutionError::Superseded
        }
        error => failure_to_execution(error.into_import_failure()),
    }
}

#[cfg(test)]
pub(crate) fn map_stream_add_error(
    error: StreamAddError<DownloadError>,
    cancellation: &JobCancellation,
) -> ImportExecutionError {
    match error {
        StreamAddError::Source(error) => map_download_error(error, cancellation),
        StreamAddError::Canceled => map_add_canceled(cancellation),
        StreamAddError::Kubo(error) => map_kubo_error(error, KuboOperation::Add, cancellation),
    }
}

pub(crate) fn map_url_stream_add_error(
    error: StreamAddError<UrlStreamError>,
    cancellation: &JobCancellation,
) -> ImportExecutionError {
    match error {
        StreamAddError::Source(UrlStreamError::Download(error)) => {
            map_download_error(error, cancellation)
        }
        StreamAddError::Source(UrlStreamError::Phase(error)) => error,
        StreamAddError::Canceled => map_add_canceled(cancellation),
        StreamAddError::Kubo(error) => map_kubo_error(error, KuboOperation::Add, cancellation),
    }
}

fn map_add_canceled(cancellation: &JobCancellation) -> ImportExecutionError {
    if cancellation.shutdown.is_cancelled() {
        ImportExecutionError::Interrupted
    } else if cancellation.ownership_lost.is_cancelled() {
        ImportExecutionError::Superseded
    } else {
        ImportExecutionError::Retryable(ImportFailure {
            code: ImportFailureCode::KuboAddFailed,
            message: "Kubo add was canceled".to_owned(),
            retryable: true,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum KuboOperation {
    Discover,
    Add,
    Pin,
    Inspect,
    Cat,
}

pub(crate) fn map_kubo_error(
    error: AppError,
    operation: KuboOperation,
    cancellation: &JobCancellation,
) -> ImportExecutionError {
    if cancellation.shutdown.is_cancelled() || cancellation.ownership_lost.is_cancelled() {
        return cancellation_error(cancellation);
    }
    if matches!(error, AppError::StaleImportOwnership) {
        return ImportExecutionError::Superseded;
    }
    let status = match &error {
        AppError::KuboRpc { status, .. } => *status,
        _ => None,
    };
    let terminal_client_error = status.is_some_and(|status| (400..500).contains(&status));
    let (code, message) = match operation {
        KuboOperation::Discover => (
            ImportFailureCode::CidNotFound,
            "CID provider discovery failed",
        ),
        KuboOperation::Add => (ImportFailureCode::KuboAddFailed, "Kubo add failed"),
        KuboOperation::Pin => (
            ImportFailureCode::KuboPinFailed,
            "Kubo recursive pin failed",
        ),
        KuboOperation::Inspect => {
            if status == Some(404) {
                (ImportFailureCode::CidNotFound, "CID was not found")
            } else {
                (ImportFailureCode::CidNotFile, "CID is not a file")
            }
        }
        KuboOperation::Cat => {
            if status == Some(404) {
                (ImportFailureCode::CidNotFound, "archive CID was not found")
            } else {
                (ImportFailureCode::CidNotFile, "archive CID stream failed")
            }
        }
    };
    failure_to_execution(ImportFailure {
        code,
        message: message.to_owned(),
        retryable: !terminal_client_error,
    })
}

pub(crate) fn map_publication_error(
    error: AppError,
    cancellation: &JobCancellation,
) -> ImportExecutionError {
    if cancellation.shutdown.is_cancelled() || cancellation.ownership_lost.is_cancelled() {
        return cancellation_error(cancellation);
    }
    if matches!(error, AppError::StaleImportOwnership) {
        return ImportExecutionError::Superseded;
    }
    let retryable = matches!(error, AppError::Database(_) | AppError::Internal(_));
    failure_to_execution(ImportFailure {
        code: ImportFailureCode::PublicationFailed,
        message: "import publication failed".to_owned(),
        retryable,
    })
}

fn failure_to_execution(failure: ImportFailure) -> ImportExecutionError {
    if failure.retryable {
        ImportExecutionError::Retryable(failure)
    } else {
        ImportExecutionError::Terminal(failure)
    }
}

pub(crate) fn terminal_failure(code: ImportFailureCode, message: &str) -> ImportExecutionError {
    ImportExecutionError::Terminal(ImportFailure {
        code,
        message: message.to_owned(),
        retryable: false,
    })
}
