use s3s::S3Error;
use s3s::s3_error;

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

    #[error("access denied: {0}")]
    AccessDenied(String),

    #[error("kubo rpc error: {0}")]
    KuboRpc(String),

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
            AppError::AccessDenied(_) => s3_error!(AccessDenied, "{}", e),
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
        AppError::KuboRpc(e.to_string())
    }
}
