use crate::error::{AppError, AppResult};
use chrono::Utc;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set, TransactionTrait,
};
use serde_json::Value as JsonValue;

use super::entities::{multipart_part, multipart_upload};

#[allow(clippy::too_many_arguments)]
pub async fn create_upload<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    object_id: &str,
    bucket: &str,
    key: &str,
    encryption_mode: &str,
    key_wrap: Option<&str>,
    content_type: Option<&str>,
    metadata: Option<JsonValue>,
    decompress_zip_target: Option<&str>,
    decompress_zip_result: bool,
) -> AppResult<()> {
    let model = multipart_upload::ActiveModel {
        upload_id: Set(upload_id.to_owned()),
        object_id: Set(object_id.to_owned()),
        bucket: Set(bucket.to_owned()),
        key: Set(key.to_owned()),
        created_at: Set(Utc::now()),
        encryption_mode: Set(encryption_mode.to_owned()),
        key_wrap: Set(key_wrap.map(|s| s.to_owned())),
        content_type: Set(content_type.map(|s| s.to_owned())),
        metadata: Set(metadata),
        decompress_zip_target: Set(decompress_zip_target.map(str::to_owned)),
        decompress_zip_result: Set(decompress_zip_result),
    };

    multipart_upload::Entity::insert(model).exec(db).await?;
    Ok(())
}

pub async fn get_upload<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
) -> AppResult<multipart_upload::Model> {
    multipart_upload::Entity::find_by_id(upload_id.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| AppError::NoSuchUpload(upload_id.to_owned()))
}

pub async fn delete_upload<C: ConnectionTrait>(db: &C, upload_id: &str) -> AppResult<()> {
    let result = multipart_upload::Entity::delete_by_id(upload_id.to_owned())
        .exec(db)
        .await?;

    if result.rows_affected == 0 {
        return Err(AppError::NoSuchUpload(upload_id.to_owned()));
    }

    Ok(())
}

pub async fn upsert_part<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    part_number: i32,
    cid: &str,
    size: i64,
    etag: &str,
) -> AppResult<()> {
    let model = multipart_part::ActiveModel {
        upload_id: Set(upload_id.to_owned()),
        part_number: Set(part_number),
        cid: Set(cid.to_owned()),
        size: Set(size),
        etag: Set(etag.to_owned()),
        uploaded_at: Set(Utc::now()),
    };

    multipart_part::Entity::insert(model)
        .on_conflict(
            OnConflict::columns([
                multipart_part::Column::UploadId,
                multipart_part::Column::PartNumber,
            ])
            .update_columns([
                multipart_part::Column::Cid,
                multipart_part::Column::Size,
                multipart_part::Column::Etag,
                multipart_part::Column::UploadedAt,
            ])
            .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn list_parts<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
) -> AppResult<Vec<multipart_part::Model>> {
    let parts = multipart_part::Entity::find()
        .filter(multipart_part::Column::UploadId.eq(upload_id))
        .order_by_asc(multipart_part::Column::PartNumber)
        .all(db)
        .await?;
    Ok(parts)
}

pub async fn get_part<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    part_number: i32,
) -> AppResult<multipart_part::Model> {
    multipart_part::Entity::find_by_id((upload_id.to_owned(), part_number))
        .one(db)
        .await?
        .ok_or_else(|| AppError::InvalidPart(format!("{upload_id}/{part_number}")))
}

#[derive(Debug, thiserror::Error)]
pub enum CommitCompletedUploadError {
    #[error("completion attempt {completion_attempt_id} transaction rolled back: {source}")]
    RolledBack {
        completion_attempt_id: String,
        #[source]
        source: AppError,
    },
    #[error("completion attempt {completion_attempt_id} commit outcome is unknown: {source}")]
    OutcomeUnknown {
        completion_attempt_id: String,
        #[source]
        source: AppError,
    },
}

#[derive(Debug)]
pub enum ReconciledCommitOutcome {
    Committed,
    NotCommitted,
    Unknown(AppError),
}

pub async fn commit_completed_upload<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    upload_id: &str,
    attempt: crate::store::object::LatestObjectRow,
) -> Result<(), CommitCompletedUploadError> {
    let completion_attempt_id = attempt.id.clone();
    const MAX_RETRIES: usize = 3;
    for retry in 0..=MAX_RETRIES {
        let upload_id = upload_id.to_owned();
        let attempt = attempt.clone();
        let result = db
            .transaction(|txn| {
                Box::pin(async move {
                    crate::store::object::write_latest_in_transaction(txn, attempt).await?;
                    let deleted = multipart_upload::Entity::delete_by_id(upload_id.clone())
                        .exec(txn)
                        .await?;
                    if deleted.rows_affected != 1 {
                        return Err(sea_orm::DbErr::RecordNotFound(format!(
                            "multipart upload not found: {upload_id}"
                        )));
                    }
                    Ok::<_, sea_orm::DbErr>(())
                })
            })
            .await;

        match result {
            Ok(()) => return Ok(()),
            Err(sea_orm::TransactionError::Transaction(db_err)) => {
                let message = db_err.to_string().to_lowercase();
                if (message.contains("unique") || message.contains("constraint"))
                    && retry < MAX_RETRIES
                {
                    continue;
                }
                return Err(CommitCompletedUploadError::RolledBack {
                    completion_attempt_id,
                    source: AppError::from(db_err),
                });
            }
            Err(sea_orm::TransactionError::Connection(db_err)) => {
                return Err(CommitCompletedUploadError::OutcomeUnknown {
                    completion_attempt_id,
                    source: AppError::from(db_err),
                });
            }
        }
    }
    unreachable!("retry loop exhausted without returning")
}

pub(crate) fn classify_completion_attempt_state(
    expected: &crate::store::object::LatestObjectRow,
    object: Option<&crate::store::entities::object::Model>,
    upload: Option<&multipart_upload::Model>,
) -> ReconciledCommitOutcome {
    let Some(row) = object else {
        return ReconciledCommitOutcome::NotCommitted;
    };
    let exact = row.id == expected.id
        && row.bucket == expected.bucket
        && row.key == expected.key
        && row.cid == expected.cid
        && row.size == expected.size
        && row.content_type == expected.content_type
        && row.etag == expected.etag
        && row.metadata == expected.metadata
        && row.encrypted == expected.encrypted
        && row.key_wrap == expected.key_wrap
        && row.multipart == expected.multipart
        && row.is_latest;
    if exact && upload.is_none() {
        ReconciledCommitOutcome::Committed
    } else {
        ReconciledCommitOutcome::Unknown(AppError::Internal(format!(
            "mixed completion-attempt state for completion_attempt_id={} upload_id={}",
            expected.id,
            upload.map_or("absent", |row| row.upload_id.as_str()),
        )))
    }
}

pub async fn reconcile_completion_attempt<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    expected: &crate::store::object::LatestObjectRow,
) -> ReconciledCommitOutcome {
    let object = match crate::store::entities::object::Entity::find_by_id(expected.id.clone())
        .one(db)
        .await
    {
        Ok(Some(object)) => object,
        Ok(None) => return ReconciledCommitOutcome::NotCommitted,
        Err(error) => return ReconciledCommitOutcome::Unknown(AppError::from(error)),
    };
    let upload = match multipart_upload::Entity::find_by_id(upload_id.to_owned())
        .one(db)
        .await
    {
        Ok(upload) => upload,
        Err(error) => return ReconciledCommitOutcome::Unknown(AppError::from(error)),
    };
    classify_completion_attempt_state(expected, Some(&object), upload.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, EntityTrait, Set, Statement};

    fn completion_attempt(id: &str) -> crate::store::object::LatestObjectRow {
        crate::store::object::LatestObjectRow {
            id: id.to_owned(),
            bucket: "test-bucket".to_owned(),
            key: "archive.zip".to_owned(),
            cid: "QmRoot".to_owned(),
            size: 7,
            content_type: Some("application/zip".to_owned()),
            etag: "QmRoot".to_owned(),
            metadata: Some(serde_json::json!({"source": "multipart"})),
            encrypted: false,
            key_wrap: None,
            multipart: true,
            created_at: Utc::now(),
        }
    }

    fn stored_attempt(
        attempt: &crate::store::object::LatestObjectRow,
    ) -> crate::store::entities::object::Model {
        crate::store::entities::object::Model {
            id: attempt.id.clone(),
            bucket: attempt.bucket.clone(),
            key: attempt.key.clone(),
            cid: attempt.cid.clone(),
            size: attempt.size,
            content_type: attempt.content_type.clone(),
            etag: attempt.etag.clone(),
            metadata: attempt.metadata.clone(),
            encrypted: attempt.encrypted,
            key_wrap: attempt.key_wrap.clone(),
            multipart: attempt.multipart,
            is_latest: true,
            created_at: attempt.created_at,
        }
    }

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "test-bucket", None)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn create_upload_persists_decompress_metadata() {
        let db = setup().await;

        create_upload(
            &db,
            "upload-1",
            "object-1",
            "test-bucket",
            "archive.zip",
            "none",
            None,
            Some("application/zip"),
            None,
            Some("prefix/"),
            false,
        )
        .await
        .unwrap();

        let upload = get_upload(&db, "upload-1").await.unwrap();
        assert_eq!(upload.decompress_zip_target.as_deref(), Some("prefix/"));
        assert!(!upload.decompress_zip_result);
    }

    async fn seed_upload_and_part(db: &sea_orm::DatabaseConnection) -> multipart_part::Model {
        create_upload(
            db,
            "upload-1",
            "object-1",
            "test-bucket",
            "archive.zip",
            "none",
            None,
            Some("application/zip"),
            None,
            None,
            true,
        )
        .await
        .unwrap();

        multipart_part::Entity::insert(multipart_part::ActiveModel {
            upload_id: Set("upload-1".to_owned()),
            part_number: Set(1),
            cid: Set("QmOld".to_owned()),
            size: Set(3),
            etag: Set("QmOld".to_owned()),
            uploaded_at: Set(Utc::now()),
        })
        .exec(db)
        .await
        .unwrap();

        get_part(db, "upload-1", 1).await.unwrap()
    }

    #[tokio::test]
    async fn upsert_part_replaces_all_mutable_fields_atomically() {
        let db = setup().await;
        let original = seed_upload_and_part(&db).await;

        upsert_part(&db, "upload-1", 1, "QmNew", 7, "QmNew")
            .await
            .unwrap();

        let parts = list_parts(&db, "upload-1").await.unwrap();
        assert_eq!(parts.len(), 1);
        let replacement = &parts[0];
        assert_eq!(replacement.cid, "QmNew");
        assert_eq!(replacement.size, 7);
        assert_eq!(replacement.etag, "QmNew");
        assert!(replacement.uploaded_at >= original.uploaded_at);
    }

    #[tokio::test]
    async fn upsert_part_failure_preserves_previous_row() {
        let db = setup().await;
        let original = seed_upload_and_part(&db).await;
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_part_upsert BEFORE UPDATE ON multipart_parts \
             BEGIN SELECT RAISE(FAIL, 'forced part upsert failure'); END;",
        ))
        .await
        .unwrap();

        let error = upsert_part(&db, "upload-1", 1, "QmNew", 7, "QmNew")
            .await
            .unwrap_err();

        assert!(matches!(error, AppError::Database(_)));
        assert_eq!(get_part(&db, "upload-1", 1).await.unwrap(), original);
    }

    #[tokio::test]
    async fn commit_completed_upload_requires_exactly_one_upload_row() {
        let db = setup().await;
        crate::store::object::upsert(
            &db,
            "old-object",
            "test-bucket",
            "archive.zip",
            "QmOld",
            3,
            Some("text/plain"),
            "QmOld",
            None,
            false,
            None,
            false,
        )
        .await
        .unwrap();
        let attempt = completion_attempt("attempt-missing-upload");

        let error = commit_completed_upload(&db, "missing-upload", attempt.clone())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CommitCompletedUploadError::RolledBack {
                ref completion_attempt_id,
                ..
            } if completion_attempt_id == "attempt-missing-upload"
        ));
        assert_eq!(
            crate::store::object::get_latest(&db, "test-bucket", "archive.zip")
                .await
                .unwrap()
                .id,
            "old-object"
        );
        assert!(
            crate::store::entities::object::Entity::find_by_id("attempt-missing-upload")
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unknown_commit_exact_attempt_and_missing_upload_is_committed() {
        let attempt = completion_attempt("attempt-1");
        let object = stored_attempt(&attempt);

        assert!(matches!(
            classify_completion_attempt_state(&attempt, Some(&object), None),
            ReconciledCommitOutcome::Committed
        ));
    }

    #[test]
    fn unknown_commit_ignores_database_normalized_created_at() {
        let attempt = completion_attempt("attempt-1");
        let mut object = stored_attempt(&attempt);
        object.created_at = attempt.created_at + chrono::Duration::nanoseconds(1);

        assert!(matches!(
            classify_completion_attempt_state(&attempt, Some(&object), None),
            ReconciledCommitOutcome::Committed
        ));
    }

    #[test]
    fn unknown_commit_missing_attempt_is_not_committed_even_when_upload_is_absent() {
        let attempt = completion_attempt("attempt-1");
        let upload = multipart_upload::Model {
            upload_id: "upload-1".to_owned(),
            object_id: "encryption-object-1".to_owned(),
            bucket: "test-bucket".to_owned(),
            key: "archive.zip".to_owned(),
            created_at: Utc::now(),
            encryption_mode: "none".to_owned(),
            key_wrap: None,
            content_type: Some("application/zip".to_owned()),
            metadata: None,
            decompress_zip_target: None,
            decompress_zip_result: true,
        };

        assert!(matches!(
            classify_completion_attempt_state(&attempt, None, Some(&upload)),
            ReconciledCommitOutcome::NotCommitted
        ));
        assert!(matches!(
            classify_completion_attempt_state(&attempt, None, None),
            ReconciledCommitOutcome::NotCommitted
        ));
    }

    #[test]
    fn unknown_commit_present_but_mismatched_attempt_is_unknown() {
        let attempt = completion_attempt("attempt-1");
        let mut mismatched = stored_attempt(&attempt);
        mismatched.cid = "QmOther".to_owned();
        let upload = multipart_upload::Model {
            upload_id: "upload-1".to_owned(),
            object_id: "encryption-object-1".to_owned(),
            bucket: "test-bucket".to_owned(),
            key: "archive.zip".to_owned(),
            created_at: Utc::now(),
            encryption_mode: "none".to_owned(),
            key_wrap: None,
            content_type: Some("application/zip".to_owned()),
            metadata: None,
            decompress_zip_target: None,
            decompress_zip_result: true,
        };

        assert!(matches!(
            classify_completion_attempt_state(&attempt, Some(&mismatched), None),
            ReconciledCommitOutcome::Unknown(AppError::Internal(_))
        ));
        let exact = stored_attempt(&attempt);
        assert!(matches!(
            classify_completion_attempt_state(&attempt, Some(&exact), Some(&upload)),
            ReconciledCommitOutcome::Unknown(AppError::Internal(_))
        ));
    }
}
