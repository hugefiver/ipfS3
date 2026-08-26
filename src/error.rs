use std::time::SystemTime;

use chrono::{DateTime, Utc};
use http::{HeaderMap, HeaderValue};
use s3s::dto::{Timestamp, TimestampFormat};
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

fn import_route_error(code: &str, status: http::StatusCode, message: &'static str) -> S3Error {
    let mut s3_error = S3Error::with_message(S3ErrorCode::Custom(code.into()), message);
    s3_error.set_status_code(status);
    s3_error
}

fn delete_marker_error(version_id: &str, created_at: DateTime<Utc>, current: bool) -> S3Error {
    let result = (|| -> Result<S3Error, ()> {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-delete-marker", HeaderValue::from_static("true"));
        headers.insert(
            "x-amz-version-id",
            HeaderValue::from_str(version_id).map_err(|_| ())?,
        );

        let (code, status) = if current {
            (S3ErrorCode::NoSuchKey, http::StatusCode::NOT_FOUND)
        } else {
            let mut formatted = Vec::new();
            Timestamp::from(SystemTime::from(created_at))
                .format(TimestampFormat::HttpDate, &mut formatted)
                .map_err(|_| ())?;
            headers.insert(
                http::header::LAST_MODIFIED,
                HeaderValue::from_bytes(&formatted).map_err(|_| ())?,
            );
            (
                S3ErrorCode::MethodNotAllowed,
                http::StatusCode::METHOD_NOT_ALLOWED,
            )
        };

        let mut error = S3Error::with_message(code, "delete marker");
        error.set_status_code(status);
        error.set_headers(headers);
        Ok(error)
    })();

    result.unwrap_or_else(|()| {
        let mut error = s3_error!(InternalError, "internal error");
        error.set_status_code(http::StatusCode::INTERNAL_SERVER_ERROR);
        error
    })
}

/// Application-level errors. Converted to S3Error at the S3 handler boundary.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("bucket not found: {0}")]
    NoSuchBucket(String),

    #[error("key not found: {0}")]
    NoSuchKey(String),

    #[error("version not found")]
    NoSuchVersion {
        bucket: String,
        key: String,
        version_id: String,
    },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("delete marker")]
    DeleteMarker {
        version_id: String,
        created_at: DateTime<Utc>,
        current: bool,
    },

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

    #[error("invalid import request")]
    InvalidImportRequest,

    #[error("import source URL is not allowed")]
    ImportUrlDenied,

    #[error("import job not found")]
    NoSuchImportJob,

    #[error("import idempotency token conflicts with an existing job")]
    ImportIdempotencyConflict,

    #[error("ipfs3 import is disabled")]
    ImportDisabled,

    #[error("stale import ownership")]
    StaleImportOwnership,

    #[error("stale content mutation")]
    StaleContentMutation,

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
            AppError::NoSuchVersion { .. } => {
                let mut error = s3_error!(NoSuchVersion, "version not found");
                error.set_status_code(http::StatusCode::NOT_FOUND);
                error
            }
            AppError::InvalidArgument(_) => {
                let mut error = s3_error!(InvalidArgument, "{}", e);
                error.set_status_code(http::StatusCode::BAD_REQUEST);
                error
            }
            AppError::DeleteMarker {
                version_id,
                created_at,
                current,
            } => delete_marker_error(version_id, *created_at, *current),
            AppError::BucketAlreadyExists(_) => s3_error!(BucketAlreadyOwnedByYou, "{}", e),
            AppError::BucketNotEmpty(_) => s3_error!(BucketNotEmpty, "{}", e),
            AppError::NoSuchUpload(_) => s3_error!(NoSuchUpload, "{}", e),
            AppError::InvalidPart(_) => s3_error!(InvalidPart, "{}", e),
            AppError::InvalidPartOrder => s3_error!(InvalidPartOrder, "{}", e),
            AppError::EntityTooSmall => s3_error!(EntityTooSmall, "{}", e),
            AppError::InvalidRange => s3_error!(InvalidRange, "{}", e),
            AppError::InvalidZipParameter(_) => s3_error!(InvalidArgument, "{}", e),
            AppError::InvalidPinningRequest(_) => s3_error!(InvalidArgument, "{}", e),
            AppError::InvalidImportRequest => import_route_error(
                "InvalidArgument",
                http::StatusCode::BAD_REQUEST,
                "invalid import request",
            ),
            AppError::ImportUrlDenied => import_route_error(
                "AccessDenied",
                http::StatusCode::FORBIDDEN,
                "import source URL is not allowed",
            ),
            AppError::NoSuchImportJob => import_route_error(
                "NoSuchImportJob",
                http::StatusCode::NOT_FOUND,
                "import job not found",
            ),
            AppError::ImportIdempotencyConflict => import_route_error(
                "IdempotentParameterMismatch",
                http::StatusCode::CONFLICT,
                "import idempotency token conflicts with an existing job",
            ),
            AppError::ImportDisabled => import_route_error(
                "NotImplemented",
                http::StatusCode::NOT_IMPLEMENTED,
                "ipfs3 import is disabled",
            ),
            AppError::StaleImportOwnership => {
                s3_error!(InternalError, "internal import ownership error")
            }
            AppError::StaleContentMutation => import_route_error(
                "OperationAborted",
                http::StatusCode::CONFLICT,
                "content mutation was superseded by a newer operation",
            ),
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
    use chrono::{TimeZone, Utc};

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

    #[test]
    fn import_errors_are_stable_and_redacted() {
        let cases = [
            (
                AppError::InvalidImportRequest,
                "InvalidArgument",
                http::StatusCode::BAD_REQUEST,
                "invalid import request",
            ),
            (
                AppError::ImportUrlDenied,
                "AccessDenied",
                http::StatusCode::FORBIDDEN,
                "import source URL is not allowed",
            ),
            (
                AppError::NoSuchImportJob,
                "NoSuchImportJob",
                http::StatusCode::NOT_FOUND,
                "import job not found",
            ),
            (
                AppError::ImportIdempotencyConflict,
                "IdempotentParameterMismatch",
                http::StatusCode::CONFLICT,
                "import idempotency token conflicts with an existing job",
            ),
            (
                AppError::ImportDisabled,
                "NotImplemented",
                http::StatusCode::NOT_IMPLEMENTED,
                "ipfs3 import is disabled",
            ),
        ];

        for (app_error, code, status, message) in cases {
            let s3_error: S3Error = app_error.into();
            assert_eq!(s3_error.code().as_str(), code);
            assert_eq!(s3_error.status_code(), Some(status));
            assert_eq!(s3_error.message(), Some(message));
            assert!(!s3_error.to_string().contains("secret"));
        }
    }

    #[test]
    fn stale_content_mutation_maps_to_stable_operation_aborted_conflict() {
        let error: S3Error = AppError::StaleContentMutation.into();

        assert_eq!(error.code().as_str(), "OperationAborted");
        assert_eq!(error.status_code(), Some(http::StatusCode::CONFLICT));
        assert_eq!(
            error.message(),
            Some("content mutation was superseded by a newer operation")
        );
    }

    #[test]
    fn version_errors_are_redacted_and_distinct() {
        let no_such_version: S3Error = AppError::NoSuchVersion {
            bucket: "private-bucket".to_owned(),
            key: "private-key".to_owned(),
            version_id: "private-version".to_owned(),
        }
        .into();
        assert_eq!(no_such_version.code().as_str(), "NoSuchVersion");
        assert_eq!(
            no_such_version.status_code(),
            Some(http::StatusCode::NOT_FOUND)
        );
        assert_eq!(no_such_version.message(), Some("version not found"));
        assert!(!no_such_version.to_string().contains("private"));

        let invalid_argument: S3Error =
            AppError::InvalidArgument("version ID is malformed".to_owned()).into();
        assert_eq!(invalid_argument.code().as_str(), "InvalidArgument");
        assert_eq!(
            invalid_argument.status_code(),
            Some(http::StatusCode::BAD_REQUEST)
        );
        assert_ne!(invalid_argument.code(), no_such_version.code());
    }

    #[test]
    fn current_marker_error_is_404_with_required_headers() {
        let error: S3Error = AppError::DeleteMarker {
            version_id: "public-version".to_owned(),
            created_at: Utc.with_ymd_and_hms(2015, 10, 21, 7, 28, 0).unwrap(),
            current: true,
        }
        .into();

        assert_eq!(error.code().as_str(), "NoSuchKey");
        assert_eq!(error.status_code(), Some(http::StatusCode::NOT_FOUND));
        let headers = error.headers().expect("delete marker headers");
        assert_eq!(headers.len(), 2);
        assert_eq!(headers["x-amz-delete-marker"], "true");
        assert_eq!(headers["x-amz-version-id"], "public-version");
        assert!(headers.get(http::header::LAST_MODIFIED).is_none());
    }

    #[test]
    fn explicit_marker_error_is_405_with_rfc1123_last_modified() {
        let error: S3Error = AppError::DeleteMarker {
            version_id: "public-version".to_owned(),
            created_at: Utc.with_ymd_and_hms(2015, 10, 21, 7, 28, 0).unwrap(),
            current: false,
        }
        .into();

        assert_eq!(error.code().as_str(), "MethodNotAllowed");
        assert_eq!(
            error.status_code(),
            Some(http::StatusCode::METHOD_NOT_ALLOWED)
        );
        let headers = error.headers().expect("delete marker headers");
        assert_eq!(headers.len(), 3);
        assert_eq!(headers["x-amz-delete-marker"], "true");
        assert_eq!(headers["x-amz-version-id"], "public-version");
        assert_eq!(
            headers[http::header::LAST_MODIFIED],
            "Wed, 21 Oct 2015 07:28:00 GMT"
        );
    }
}
