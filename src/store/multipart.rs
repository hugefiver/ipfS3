use crate::error::{AppError, AppResult};
use crate::lifecycle::model::MultipartUploadTargetIdentity;
use chrono::Utc;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, TransactionError, TransactionTrait,
};
use serde_json::Value as JsonValue;

use super::entities::{multipart_part, multipart_upload};
use crate::pinning::decision::ExtensionDecision;
use crate::pinning::tags::ObjectTag;

#[allow(clippy::too_many_arguments)]
pub async fn create_upload<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    object_id: &str,
    bucket: &str,
    key: &str,
    encryption_mode: &str,
    key_wrap: Option<&str>,
    sse_c_key_fingerprint: Option<&str>,
    content_type: Option<&str>,
    metadata: Option<JsonValue>,
    tags: &[ObjectTag],
    decompress_zip_target: Option<&str>,
    decompress_zip_result: bool,
) -> AppResult<()> {
    create_upload_with_decision(
        db,
        upload_id,
        object_id,
        bucket,
        key,
        encryption_mode,
        key_wrap,
        sse_c_key_fingerprint,
        content_type,
        metadata,
        tags,
        decompress_zip_target,
        decompress_zip_result,
        None,
    )
    .await
}

/// Call inside the initiating transaction; the decision and upload are one row
/// and cannot be independently committed or overwritten at completion.
#[allow(clippy::too_many_arguments)]
pub async fn create_upload_with_decision<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    object_id: &str,
    bucket: &str,
    key: &str,
    encryption_mode: &str,
    key_wrap: Option<&str>,
    sse_c_key_fingerprint: Option<&str>,
    content_type: Option<&str>,
    metadata: Option<JsonValue>,
    tags: &[ObjectTag],
    decompress_zip_target: Option<&str>,
    decompress_zip_result: bool,
    decision: Option<&ExtensionDecision>,
) -> AppResult<()> {
    let pin_decision_json = decision
        .map(|decision| {
            decision
                .validate_snapshot()
                .map_err(|_| AppError::Internal("invalid multipart pin decision".to_owned()))?;
            decision.replay_policy(tags.to_vec()).map_err(|_| {
                AppError::Internal("multipart pin decision tags mismatch".to_owned())
            })?;
            if decision.origin.request_id != upload_id {
                return Err(AppError::Internal(
                    "multipart pin decision upload mismatch".to_owned(),
                ));
            }
            serde_json::to_value(decision).map_err(|_| {
                AppError::Internal("failed to serialize multipart pin decision".to_owned())
            })
        })
        .transpose()?;
    let created_at = crate::store::database_clock::database_now(db).await?;
    let model = multipart_upload::ActiveModel {
        upload_id: Set(upload_id.to_owned()),
        object_id: Set(object_id.to_owned()),
        bucket: Set(bucket.to_owned()),
        key: Set(key.to_owned()),
        created_at: Set(created_at),
        encryption_mode: Set(encryption_mode.to_owned()),
        key_wrap: Set(key_wrap.map(|s| s.to_owned())),
        sse_c_key_fingerprint: Set(sse_c_key_fingerprint.map(|s| s.to_owned())),
        content_type: Set(content_type.map(|s| s.to_owned())),
        metadata: Set(metadata),
        tags_json: Set(crate::store::pinning::tags::tags_to_json(tags)
            .map_err(|_| AppError::Internal("failed to serialize multipart tags".to_owned()))?),
        pin_decision_json: Set(pin_decision_json),
        decompress_zip_target: Set(decompress_zip_target.map(str::to_owned)),
        decompress_zip_result: Set(decompress_zip_result),
    };

    multipart_upload::Entity::insert(model).exec(db).await?;
    Ok(())
}

pub fn decision_from_upload(
    upload: &multipart_upload::Model,
) -> AppResult<Option<ExtensionDecision>> {
    let Some(snapshot) = &upload.pin_decision_json else {
        return Ok(None);
    };
    let decision: ExtensionDecision = serde_json::from_value(snapshot.clone())
        .map_err(|_| AppError::Internal("invalid persisted multipart pin decision".to_owned()))?;
    decision
        .validate_snapshot()
        .map_err(|_| AppError::Internal("invalid persisted multipart pin decision".to_owned()))?;
    if decision.origin.request_id != upload.upload_id {
        return Err(AppError::Internal(
            "multipart pin decision upload mismatch".to_owned(),
        ));
    }
    Ok(Some(decision))
}

pub async fn claim_sse_c_key_fingerprint<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    candidate: &str,
) -> AppResult<multipart_upload::Model> {
    multipart_upload::Entity::update_many()
        .col_expr(
            multipart_upload::Column::SseCKeyFingerprint,
            candidate.to_owned().into(),
        )
        .filter(multipart_upload::Column::UploadId.eq(upload_id))
        .filter(multipart_upload::Column::EncryptionMode.eq("sse_c"))
        .filter(multipart_upload::Column::SseCKeyFingerprint.is_null())
        .exec(db)
        .await?;

    get_upload(db, upload_id).await
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortExactIncompleteUploadResult {
    Applied,
    AlreadySatisfied,
    Stale,
}

/// Deletes an exact incomplete upload and lets the foreign key cascade remove its parts.
///
/// The caller must already hold the ownership lock for `target.bucket` in `txn`.
pub async fn abort_exact_incomplete_upload_in_transaction<C: ConnectionTrait>(
    txn: &C,
    target: &MultipartUploadTargetIdentity,
) -> AppResult<AbortExactIncompleteUploadResult> {
    let deleted = multipart_upload::Entity::delete_many()
        .filter(multipart_upload::Column::UploadId.eq(&target.upload_id))
        .filter(multipart_upload::Column::Bucket.eq(&target.bucket))
        .filter(multipart_upload::Column::Key.eq(&target.key))
        .filter(multipart_upload::Column::CreatedAt.eq(target.initiated_at))
        .exec(txn)
        .await?;
    if deleted.rows_affected == 1 {
        return Ok(AbortExactIncompleteUploadResult::Applied);
    }

    let upload_exists = multipart_upload::Entity::find_by_id(target.upload_id.clone())
        .one(txn)
        .await?
        .is_some();
    Ok(if upload_exists {
        AbortExactIncompleteUploadResult::Stale
    } else {
        AbortExactIncompleteUploadResult::AlreadySatisfied
    })
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

pub async fn upsert_part_for_active_upload(
    db: &DatabaseConnection,
    target: &MultipartUploadTargetIdentity,
    part_number: i32,
    cid: &str,
    size: i64,
    etag: &str,
) -> AppResult<()> {
    let target = target.clone();
    let cid = cid.to_owned();
    let etag = etag.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            crate::store::import::ownership::lock_bucket_for_ownership(txn, &target.bucket).await?;

            let query = multipart_upload::Entity::find_by_id(target.upload_id.clone());
            let upload = if txn.get_database_backend() == DatabaseBackend::Postgres {
                query.lock_exclusive().one(txn).await?
            } else {
                query.one(txn).await?
            };
            let Some(upload) = upload else {
                return Err(AppError::NoSuchUpload(target.upload_id.clone()));
            };
            if upload.bucket != target.bucket
                || upload.key != target.key
                || upload.created_at != target.initiated_at
            {
                return Err(AppError::NoSuchUpload(target.upload_id.clone()));
            }

            upsert_part(txn, &target.upload_id, part_number, &cid, size, &etag).await
        })
    })
    .await
    .map_err(transaction_error_into_app)
}

fn transaction_error_into_app(error: TransactionError<AppError>) -> AppError {
    match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => error.into(),
    }
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
        && row.sse_c_key_fingerprint == expected.sse_c_key_fingerprint
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, SecondsFormat};
    use sea_orm::{
        ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, DbErr, EntityTrait,
        ExecResult, QueryResult, Set, Statement, TransactionTrait,
    };

    use crate::lifecycle::model::MultipartUploadTargetIdentity;

    struct ControlledClockConnection {
        inner: DatabaseConnection,
        now: DateTime<Utc>,
    }

    #[async_trait::async_trait]
    impl ConnectionTrait for ControlledClockConnection {
        fn get_database_backend(&self) -> DatabaseBackend {
            self.inner.get_database_backend()
        }

        async fn execute(&self, statement: Statement) -> Result<ExecResult, DbErr> {
            self.inner.execute(statement).await
        }

        async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
            self.inner.execute_unprepared(sql).await
        }

        async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
            if statement.sql == "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now') AS now" {
                let now = self.now.to_rfc3339_opts(SecondsFormat::Millis, true);
                return self
                    .inner
                    .query_one(Statement::from_string(
                        DatabaseBackend::Sqlite,
                        format!("SELECT '{now}' AS now"),
                    ))
                    .await;
            }
            self.inner.query_one(statement).await
        }

        async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
            self.inner.query_all(statement).await
        }
    }

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
            sse_c_key_fingerprint: None,
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
            sse_c_key_fingerprint: attempt.sse_c_key_fingerprint.clone(),
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
    async fn captured_decision_round_trips_with_upload_and_legacy_remains_unknown() {
        use crate::config::{OptionalPinControlMode, PinningConfig};
        use crate::pinning::config::ValidatedPinningConfig;
        use crate::pinning::decision::{DecisionEffect, DecisionOrigin};
        use crate::pinning::policy::{PinPolicyEvaluator, PublicationContext};

        let db = setup().await;
        let tags = vec![
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("private", "do-not-leak"),
        ];
        let config = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
        let (_, decision) = PinPolicyEvaluator::with_mode(&config, OptionalPinControlMode::Warn)
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: "test-bucket",
                    key: "archive.zip",
                    tags: &tags,
                    is_decompress_zip: false,
                },
                DecisionOrigin::new("test", "upload-captured"),
            )
            .unwrap();
        let txn = db.begin().await.unwrap();
        create_upload_with_decision(
            &txn,
            "upload-captured",
            "object",
            "test-bucket",
            "archive.zip",
            "none",
            None,
            None,
            None,
            None,
            &tags,
            None,
            true,
            Some(&decision),
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        let row = get_upload(&db, "upload-captured").await.unwrap();
        let replayed = decision_from_upload(&row).unwrap().unwrap();
        assert_eq!(replayed, decision);
        assert_eq!(replayed.effect, DecisionEffect::Skipped);
        assert_eq!(replayed.origin.request_id, row.upload_id);
        assert!(
            !row.pin_decision_json
                .unwrap()
                .to_string()
                .contains("do-not-leak")
        );

        create_upload(
            &db,
            "legacy",
            "object-2",
            "test-bucket",
            "archive.zip",
            "none",
            None,
            None,
            None,
            None,
            &tags,
            None,
            true,
        )
        .await
        .unwrap();
        assert!(
            decision_from_upload(&get_upload(&db, "legacy").await.unwrap())
                .unwrap()
                .is_none()
        );
    }

    async fn abort_with_bucket_lock(
        db: &DatabaseConnection,
        target: &MultipartUploadTargetIdentity,
    ) -> AbortExactIncompleteUploadResult {
        let transaction = db.begin().await.unwrap();
        crate::store::import::ownership::lock_bucket_for_ownership(&transaction, &target.bucket)
            .await
            .unwrap();
        let result = abort_exact_incomplete_upload_in_transaction(&transaction, target)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        result
    }

    #[tokio::test]
    async fn create_upload_uses_database_clock_and_parts_do_not_reset_it() {
        let db = setup().await;
        let database_time = DateTime::parse_from_rfc3339("2041-02-03T04:05:06.789Z")
            .unwrap()
            .with_timezone(&Utc);
        let controlled = ControlledClockConnection {
            inner: db.clone(),
            now: database_time,
        };

        create_upload(
            &controlled,
            "clock-upload",
            "clock-object",
            "test-bucket",
            "clock.bin",
            "none",
            None,
            None,
            None,
            None,
            &[],
            None,
            false,
        )
        .await
        .unwrap();
        let upload = get_upload(&db, "clock-upload").await.unwrap();
        assert_eq!(upload.created_at, database_time);
        let target = MultipartUploadTargetIdentity {
            bucket: upload.bucket,
            key: upload.key,
            upload_id: upload.upload_id,
            initiated_at: upload.created_at,
        };

        upsert_part_for_active_upload(&db, &target, 1, "QmFirst", 3, "QmFirst")
            .await
            .unwrap();
        upsert_part_for_active_upload(&db, &target, 1, "QmReplacement", 7, "QmReplacement")
            .await
            .unwrap();

        assert_eq!(
            get_upload(&db, "clock-upload").await.unwrap().created_at,
            database_time
        );
    }

    #[tokio::test]
    async fn abort_exact_upload_applies_cascades_and_distinguishes_missing_from_stale() {
        let db = setup().await;
        crate::store::bucket::create(&db, "other-bucket", None)
            .await
            .unwrap();
        seed_upload_and_part(&db).await;
        let upload = get_upload(&db, "upload-1").await.unwrap();
        let target = MultipartUploadTargetIdentity {
            bucket: upload.bucket,
            key: upload.key,
            upload_id: upload.upload_id,
            initiated_at: upload.created_at,
        };

        let mut missing = target.clone();
        missing.upload_id = "missing-upload".to_owned();
        assert_eq!(
            abort_with_bucket_lock(&db, &missing).await,
            AbortExactIncompleteUploadResult::AlreadySatisfied
        );

        let mut wrong_bucket = target.clone();
        wrong_bucket.bucket = "other-bucket".to_owned();
        let mut wrong_key = target.clone();
        wrong_key.key = "other-key".to_owned();
        let mut wrong_initiated_at = target.clone();
        wrong_initiated_at.initiated_at += chrono::Duration::seconds(1);
        for stale in [wrong_bucket, wrong_key, wrong_initiated_at] {
            assert_eq!(
                abort_with_bucket_lock(&db, &stale).await,
                AbortExactIncompleteUploadResult::Stale
            );
            assert!(get_upload(&db, "upload-1").await.is_ok());
            assert_eq!(list_parts(&db, "upload-1").await.unwrap().len(), 1);
        }

        assert_eq!(
            abort_with_bucket_lock(&db, &target).await,
            AbortExactIncompleteUploadResult::Applied
        );
        assert!(matches!(
            get_upload(&db, "upload-1").await,
            Err(AppError::NoSuchUpload(_))
        ));
        assert!(list_parts(&db, "upload-1").await.unwrap().is_empty());
        assert_eq!(
            abort_with_bucket_lock(&db, &target).await,
            AbortExactIncompleteUploadResult::AlreadySatisfied
        );
    }

    #[tokio::test]
    async fn late_store_part_after_abort_fails_without_resurrecting_upload() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("multipart-late-part.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let first = crate::store::connect_database(&database_url).await.unwrap();
        first
            .execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&first).await.unwrap();
        crate::store::bucket::create(&first, "test-bucket", None)
            .await
            .unwrap();
        let second = crate::store::connect_database(&database_url).await.unwrap();
        second
            .execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        create_upload(
            &first,
            "late-part-upload",
            "late-part-object",
            "test-bucket",
            "late.bin",
            "none",
            None,
            None,
            None,
            None,
            &[],
            None,
            false,
        )
        .await
        .unwrap();
        let upload = get_upload(&first, "late-part-upload").await.unwrap();
        let target = MultipartUploadTargetIdentity {
            bucket: upload.bucket,
            key: upload.key,
            upload_id: upload.upload_id,
            initiated_at: upload.created_at,
        };
        let abort_transaction = first.begin().await.unwrap();
        crate::store::import::ownership::lock_bucket_for_ownership(
            &abort_transaction,
            &target.bucket,
        )
        .await
        .unwrap();

        let late_target = target.clone();
        let late_part = tokio::spawn(async move {
            upsert_part_for_active_upload(&second, &late_target, 1, "QmLate", 4, "QmLate").await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !late_part.is_finished(),
            "late part must wait for the abort bucket lock"
        );

        assert_eq!(
            abort_exact_incomplete_upload_in_transaction(&abort_transaction, &target)
                .await
                .unwrap(),
            AbortExactIncompleteUploadResult::Applied
        );
        abort_transaction.commit().await.unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(10), late_part)
            .await
            .expect("late part must observe the committed abort")
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::NoSuchUpload(ref upload_id) if upload_id == "late-part-upload"
        ));
        assert!(matches!(
            get_upload(&first, "late-part-upload").await,
            Err(AppError::NoSuchUpload(_))
        ));
        assert!(
            list_parts(&first, "late-part-upload")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn create_upload_persists_sse_c_key_fingerprint() {
        let db = setup().await;

        create_upload(
            &db,
            "upload-sse-c",
            "object-sse-c",
            "test-bucket",
            "archive.zip",
            "sse_c",
            None,
            Some("v1:hmac-sha256:fixture"),
            Some("application/zip"),
            None,
            &[],
            None,
            true,
        )
        .await
        .unwrap();

        let upload = get_upload(&db, "upload-sse-c").await.unwrap();
        assert_eq!(
            upload.sse_c_key_fingerprint.as_deref(),
            Some("v1:hmac-sha256:fixture")
        );
    }

    #[tokio::test]
    async fn create_upload_round_trips_normalized_tags() {
        let db = setup().await;
        let tags = vec![
            crate::pinning::tags::ObjectTag::new("team", "R&D"),
            crate::pinning::tags::ObjectTag::new("space", "hello world"),
        ];

        create_upload(
            &db,
            "upload-tags",
            "object-tags",
            "test-bucket",
            "archive.zip",
            "none",
            None,
            None,
            Some("application/zip"),
            None,
            &tags,
            None,
            true,
        )
        .await
        .unwrap();

        let upload = get_upload(&db, "upload-tags").await.unwrap();
        assert_eq!(
            crate::store::pinning::tags::tags_from_json(&upload.tags_json).unwrap(),
            tags
        );
    }

    #[tokio::test]
    async fn create_upload_rejects_invalid_tags_without_leaking_values() {
        let db = setup().await;
        let tags = vec![crate::pinning::tags::ObjectTag::new(
            "ipfs-s3:unknown",
            "sensitive-value",
        )];

        let error = create_upload(
            &db,
            "upload-invalid-tags",
            "object-invalid-tags",
            "test-bucket",
            "archive.zip",
            "none",
            None,
            None,
            Some("application/zip"),
            None,
            &tags,
            None,
            true,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, AppError::Internal(ref message) if message == "failed to serialize multipart tags")
        );
        assert!(!format!("{error:?}").contains("sensitive-value"));
        assert!(matches!(
            get_upload(&db, "upload-invalid-tags").await,
            Err(AppError::NoSuchUpload(_))
        ));
    }

    #[tokio::test]
    async fn concurrent_sse_c_key_fingerprint_claims_keep_one_legacy_winner() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("multipart-claim.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = sea_orm::ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        let db = Database::connect(options).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "test-bucket", None)
            .await
            .unwrap();
        create_upload(
            &db,
            "legacy-upload",
            "legacy-object",
            "test-bucket",
            "archive.zip",
            "sse_c",
            None,
            None,
            Some("application/zip"),
            None,
            &[],
            None,
            true,
        )
        .await
        .unwrap();

        let (first, second) = tokio::join!(
            claim_sse_c_key_fingerprint(&db, "legacy-upload", "fp-a"),
            claim_sse_c_key_fingerprint(&db, "legacy-upload", "fp-b"),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        let winner = get_upload(&db, "legacy-upload").await.unwrap();

        assert!(matches!(
            winner.sse_c_key_fingerprint.as_deref(),
            Some("fp-a" | "fp-b")
        ));
        assert_eq!(
            first.sse_c_key_fingerprint, winner.sse_c_key_fingerprint,
            "first claimant must reload the database winner"
        );
        assert_eq!(
            second.sse_c_key_fingerprint, winner.sse_c_key_fingerprint,
            "second claimant must reload the database winner"
        );
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
            None,
            Some("application/zip"),
            None,
            &[],
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
            None,
            Some("application/zip"),
            None,
            &[],
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
            sse_c_key_fingerprint: None,
            content_type: Some("application/zip".to_owned()),
            metadata: None,
            tags_json: serde_json::json!([]),
            pin_decision_json: None,
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
            sse_c_key_fingerprint: None,
            content_type: Some("application/zip".to_owned()),
            metadata: None,
            tags_json: serde_json::json!([]),
            pin_decision_json: None,
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
