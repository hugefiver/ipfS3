use s3s::s3_error;
use s3s::{S3Error, S3ErrorCode};

pub const INTERNAL_STORAGE_BACKEND_ERROR: &str = "internal storage backend error";

/// Private provenance attached to a successful Kubo response whose body fails
/// after headers have been accepted. Its display text is safe for response
/// streams, while callers can still distinguish it from ordinary I/O failures.
#[derive(Debug, thiserror::Error)]
#[error("internal storage backend error")]
pub(crate) struct KuboStreamError;

pub(crate) fn kubo_stream_error() -> std::io::Error {
    std::io::Error::other(KuboStreamError)
}

pub(crate) fn has_kubo_stream_provenance(error: &(dyn std::error::Error + 'static)) -> bool {
    fn contains_kubo_stream_error(error: &(dyn std::error::Error + 'static), depth: u8) -> bool {
        if depth == 0 {
            return false;
        }
        if error.is::<KuboStreamError>() {
            return true;
        }
        if let Some(io_error) = error.downcast_ref::<std::io::Error>()
            && let Some(source) = io_error.get_ref()
            && contains_kubo_stream_error(source, depth - 1)
        {
            return true;
        }
        error
            .source()
            .is_some_and(|source| contains_kubo_stream_error(source, depth - 1))
    }

    contains_kubo_stream_error(error, 32)
}

fn invalid_parameter_value(error: &AppError) -> S3Error {
    let mut s3_error = S3Error::with_message(
        S3ErrorCode::Custom("InvalidParameterValue".into()),
        error.to_string(),
    );
    s3_error.set_status_code(http::StatusCode::BAD_REQUEST);
    s3_error
}

/// Application-level errors. Converted to S3Error at the S3 handler boundary.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("bucket not found: {0}")]
    NoSuchBucket(String),

    #[error("key not found: {0}")]
    NoSuchKey(String),

    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    #[error("bucket not empty: {0}")]
    BucketNotEmpty(String),

    #[error("multipart upload not found: {0}")]
    NoSuchUpload(String),

    #[error("invalid part: {0}")]
    InvalidPart(String),

    #[error("invalid part order")]
    InvalidPartOrder,

    #[error("entity too small")]
    EntityTooSmall,

    #[error("invalid range")]
    InvalidRange,

    #[error("invalid decompress-zip parameter: {0}")]
    InvalidZipParameter(String),

    #[error("invalid zip entry: {0}")]
    InvalidZipEntry(String),

    #[error("invalid pinning request: {0}")]
    InvalidPinningRequest(String),

    #[error("zip entry escapes target prefix: {0}")]
    ZipSlip(String),

    #[error("unsupported zip entry: {0}")]
    UnsupportedZipEntry(String),

    #[error("zip archive rejected: {0}")]
    ZipArchiveRejected(String),

    #[error("access denied: {0}")]
    AccessDenied(String),

    /// Kubo failures retain diagnostic detail for controlled inspection, but
    /// their Display output must be safe for ordinary logs and S3 responses.
    #[error("kubo rpc failure")]
    KuboRpc { status: Option<u16>, detail: String },

    #[error("database error: {0}")]
    Database(String),

    #[error("crypto error: {0}")]
    Crypto(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<AppError> for S3Error {
    fn from(e: AppError) -> Self {
        match &e {
            AppError::NoSuchBucket(_) => s3_error!(NoSuchBucket, "{}", e),
            AppError::NoSuchKey(_) => s3_error!(NoSuchKey, "{}", e),
            AppError::BucketAlreadyExists(_) => s3_error!(BucketAlreadyOwnedByYou, "{}", e),
            AppError::BucketNotEmpty(_) => s3_error!(BucketNotEmpty, "{}", e),
            AppError::NoSuchUpload(_) => s3_error!(NoSuchUpload, "{}", e),
            AppError::InvalidPart(_) => s3_error!(InvalidPart, "{}", e),
            AppError::InvalidPartOrder => s3_error!(InvalidPartOrder, "{}", e),
            AppError::EntityTooSmall => s3_error!(EntityTooSmall, "{}", e),
            AppError::InvalidRange => s3_error!(InvalidRange, "{}", e),
            AppError::InvalidZipParameter(_) => s3_error!(InvalidArgument, "{}", e),
            AppError::InvalidPinningRequest(_) => s3_error!(InvalidArgument, "{}", e),
            AppError::InvalidZipEntry(_)
            | AppError::ZipSlip(_)
            | AppError::UnsupportedZipEntry(_)
            | AppError::ZipArchiveRejected(_) => invalid_parameter_value(&e),
            AppError::AccessDenied(_) => s3_error!(AccessDenied, "{}", e),
            AppError::Database(_) => s3_error!(InternalError, "internal database error"),
            AppError::KuboRpc { .. } => s3_error!(InternalError, "internal storage backend error"),
            _ => s3_error!(InternalError, "{}", e),
        }
    }
}

/// Convenience type alias.
pub type AppResult<T> = Result<T, AppError>;

impl From<sea_orm::DbErr> for AppError {
    fn from(e: sea_orm::DbErr) -> Self {
        AppError::Database(e.to_string())
    }
}

impl From<reqwest::Error> for AppError {
    fn from(e: reqwest::Error) -> Self {
        AppError::KuboRpc {
            status: e.status().map(|status| status.as_u16()),
            detail: e.to_string(),
        }
    }
}

impl AppError {
    pub(crate) fn kubo_rpc_status(status: http::StatusCode) -> Self {
        Self::KuboRpc {
            status: Some(status.as_u16()),
            detail: format!("Kubo returned HTTP {}", status.as_u16()),
        }
    }

    pub(crate) fn kubo_rpc_detail(detail: impl Into<String>) -> Self {
        Self::KuboRpc {
            status: None,
            detail: detail.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zip_validation_errors_map_to_client_errors() {
        let err: S3Error = AppError::ZipSlip("../escape.txt".to_string()).into();
        assert_eq!(err.code().as_str(), "InvalidParameterValue");
        assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));

        let err: S3Error = AppError::InvalidZipParameter("bad prefix".to_string()).into();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));

        for error in [
            AppError::InvalidZipEntry("bad.txt".to_string()),
            AppError::UnsupportedZipEntry("encrypted.txt".to_string()),
            AppError::ZipArchiveRejected("archive is corrupt".to_string()),
        ] {
            let err: S3Error = error.into();
            assert_eq!(err.code().as_str(), "InvalidParameterValue");
            assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));
        }
    }

    #[test]
    fn database_errors_map_to_a_stable_generic_internal_error() {
        let err: S3Error = AppError::Database("private driver and query details".to_owned()).into();

        assert_eq!(err.code().as_str(), "InternalError");
        assert_eq!(err.message(), Some("internal database error"));
    }

    #[test]
    fn kubo_rpc_errors_map_to_a_stable_generic_internal_error() {
        let kubo_error = AppError::kubo_rpc_detail("private daemon details");
        assert_eq!(kubo_error.to_string(), "kubo rpc failure");

        let err: S3Error = kubo_error.into();

        assert_eq!(err.code().as_str(), "InternalError");
        assert_eq!(err.message(), Some(INTERNAL_STORAGE_BACKEND_ERROR));
    }
}
