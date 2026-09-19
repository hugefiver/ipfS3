use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::{AppError, AppResult},
    lifecycle::model::{
        LifecycleCandidate, LifecycleCandidatePage, LifecycleScanCursor, LifecycleScanSource,
        MultipartUploadTargetIdentity, VersionLifecycleCandidate, VersionTargetIdentity,
    },
    store::{
        entities::{multipart_upload, object, object_version},
        object_version::{public_version_id, version_kind},
    },
};

pub const MAX_LIFECYCLE_SCAN_PAGE_SIZE: u64 = 1_000;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCursor {
    source: LifecycleScanSource,
    bucket: String,
    key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version_row_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    multipart_created_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    multipart_upload_id: Option<String>,
}

impl From<&LifecycleScanCursor> for StoredCursor {
    fn from(cursor: &LifecycleScanCursor) -> Self {
        Self {
            source: cursor.source.clone(),
            bucket: cursor.bucket.clone(),
            key: cursor.key.clone(),
            sequence: cursor.sequence,
            version_row_id: cursor.version_row_id.clone(),
            multipart_created_at: cursor.multipart_created_at,
            multipart_upload_id: cursor.multipart_upload_id.clone(),
        }
    }
}

impl From<StoredCursor> for LifecycleScanCursor {
    fn from(cursor: StoredCursor) -> Self {
        Self {
            source: cursor.source,
            bucket: cursor.bucket,
            key: cursor.key,
            sequence: cursor.sequence,
            version_row_id: cursor.version_row_id,
            multipart_created_at: cursor.multipart_created_at,
            multipart_upload_id: cursor.multipart_upload_id,
        }
    }
}

fn invalid_cursor() -> AppError {
    AppError::InvalidArgument("invalid lifecycle scan cursor".to_owned())
}

fn validate_cursor(cursor: &LifecycleScanCursor, bucket: &str) -> AppResult<()> {
    if bucket.is_empty()
        || cursor.bucket.is_empty()
        || cursor.bucket != bucket
        || cursor.key.is_empty()
    {
        return Err(invalid_cursor());
    }

    let valid_shape = match &cursor.source {
        LifecycleScanSource::Current | LifecycleScanSource::Noncurrent => {
            matches!(cursor.sequence, Some(sequence) if sequence > 0)
                && matches!(&cursor.version_row_id, Some(version_row_id) if !version_row_id.is_empty())
                && cursor.multipart_created_at.is_none()
                && cursor.multipart_upload_id.is_none()
        }
        LifecycleScanSource::Multipart => {
            cursor.sequence.is_none()
                && cursor.version_row_id.is_none()
                && cursor.multipart_created_at.is_some()
                && matches!(&cursor.multipart_upload_id, Some(upload_id) if !upload_id.is_empty())
        }
    };
    if valid_shape {
        Ok(())
    } else {
        Err(invalid_cursor())
    }
}

pub fn encode_cursor(cursor: &LifecycleScanCursor) -> String {
    let json = serde_json::to_vec(&StoredCursor::from(cursor))
        .expect("lifecycle scan cursors always serialize");
    URL_SAFE_NO_PAD.encode(json)
}

pub fn decode_cursor(value: &str, bucket: &str) -> AppResult<LifecycleScanCursor> {
    let encoded = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| invalid_cursor())?;
    let cursor = serde_json::from_slice::<StoredCursor>(&encoded)
        .map(LifecycleScanCursor::from)
        .map_err(|_| invalid_cursor())?;
    validate_cursor(&cursor, bucket)?;
    Ok(cursor)
}

fn version_tuple_after(cursor: &LifecycleScanCursor) -> Condition {
    let sequence = cursor
        .sequence
        .expect("validated version lifecycle cursor has a sequence");
    let version_row_id = cursor
        .version_row_id
        .clone()
        .expect("validated version lifecycle cursor has a row ID");
    Condition::any()
        .add(object_version::Column::Key.gt(cursor.key.clone()))
        .add(
            Condition::all()
                .add(object_version::Column::Key.eq(cursor.key.clone()))
                .add(
                    Condition::any()
                        .add(object_version::Column::Sequence.gt(sequence))
                        .add(
                            Condition::all()
                                .add(object_version::Column::Sequence.eq(sequence))
                                .add(object_version::Column::Id.gt(version_row_id)),
                        ),
                ),
        )
}

fn multipart_tuple_after(cursor: &LifecycleScanCursor) -> Condition {
    let created_at = cursor
        .multipart_created_at
        .expect("validated multipart lifecycle cursor has a creation timestamp");
    let upload_id = cursor
        .multipart_upload_id
        .clone()
        .expect("validated multipart lifecycle cursor has an upload ID");
    Condition::any()
        .add(multipart_upload::Column::Key.gt(cursor.key.clone()))
        .add(
            Condition::all()
                .add(multipart_upload::Column::Key.eq(cursor.key.clone()))
                .add(
                    Condition::any()
                        .add(multipart_upload::Column::CreatedAt.gt(created_at))
                        .add(
                            Condition::all()
                                .add(multipart_upload::Column::CreatedAt.eq(created_at))
                                .add(multipart_upload::Column::UploadId.gt(upload_id)),
                        ),
                ),
        )
}

fn cursor_for(source: LifecycleScanSource, candidate: &LifecycleCandidate) -> LifecycleScanCursor {
    match candidate {
        LifecycleCandidate::Version(candidate) => LifecycleScanCursor {
            source,
            bucket: candidate.target.bucket.clone(),
            key: candidate.target.key.clone(),
            sequence: Some(candidate.target.sequence),
            version_row_id: Some(candidate.target.version_row_id.clone()),
            multipart_created_at: None,
            multipart_upload_id: None,
        },
        LifecycleCandidate::MultipartUpload(target) => LifecycleScanCursor {
            source,
            bucket: target.bucket.clone(),
            key: target.key.clone(),
            sequence: None,
            version_row_id: None,
            multipart_created_at: Some(target.initiated_at),
            multipart_upload_id: Some(target.upload_id.clone()),
        },
    }
}

pub(crate) async fn version_evaluation_facts<C: ConnectionTrait>(
    db: &C,
    row: &object_version::Model,
    joined_object: Option<&object::Model>,
) -> AppResult<(
    crate::store::object_version::BucketVersioningState,
    Option<crate::residency::model::StorageClass>,
    bool,
)> {
    use crate::residency::model::{KuboTier, StorageClass, VerificationState};
    use crate::store::{bucket, entities::version_residency, residency};

    let state = bucket::get_versioning_state(db, &row.bucket).await?;
    if version_kind(row)? == crate::store::object_version::VersionKind::DeleteMarker {
        return Ok((state, None, false));
    }
    let object = joined_object
        .ok_or_else(|| AppError::Internal("lifecycle content version has no object".to_owned()))?;
    if row.object_id.as_deref() != Some(&object.id)
        || row.bucket != object.bucket
        || row.key != object.key
    {
        return Err(AppError::Internal(
            "lifecycle content owner mismatch".to_owned(),
        ));
    }
    // Same legacy branch as residency backfill: a live content owner without a
    // residency record is pending hot, never proof of verified content. Known
    // residency records must resolve strictly; broken cold records cannot fall back.
    if version_residency::Entity::find_by_id(&row.id)
        .one(db)
        .await?
        .is_none()
    {
        return Ok((state, Some(StorageClass::Standard), false));
    }
    let resolved = residency::resolve_version_residency(db, &row.id).await?;
    Ok((
        state,
        Some(resolved.storage_class),
        resolved.primary.tier == KuboTier::Hot
            && resolved.physical.verification_state == VerificationState::Verified,
    ))
}

async fn candidate_from_row<C: ConnectionTrait>(
    db: &C,
    row: object_version::Model,
    joined_object: Option<object::Model>,
) -> AppResult<LifecycleCandidate> {
    let kind = version_kind(&row)?;
    let public_version_id = public_version_id(&row)?;
    let (bucket_versioning_state, primary_storage_class, hot_residency_verified) =
        version_evaluation_facts(db, &row, joined_object.as_ref()).await?;
    let (object_id, size) = match &kind {
        crate::store::object_version::VersionKind::Object => {
            let object_id = row.object_id.as_ref().ok_or_else(invalid_cursor)?;
            let object = joined_object.ok_or_else(|| {
                AppError::Internal("object version index references a missing object".to_owned())
            })?;
            if object.id != *object_id || object.bucket != row.bucket || object.key != row.key {
                return Err(AppError::Internal(
                    "object version index references a mismatched object".to_owned(),
                ));
            }
            (Some(object_id.clone()), object.size)
        }
        crate::store::object_version::VersionKind::DeleteMarker => {
            if joined_object.is_some() {
                return Err(AppError::Internal(
                    "delete marker version index references an object".to_owned(),
                ));
            }
            (None, 0)
        }
    };
    Ok(LifecycleCandidate::Version(VersionLifecycleCandidate {
        target: VersionTargetIdentity {
            bucket: row.bucket,
            key: row.key,
            version_row_id: row.id,
            public_version_id,
            kind,
            object_id,
            sequence: row.sequence,
        },
        is_latest: row.is_latest,
        bucket_versioning_state,
        primary_storage_class,
        hot_residency_verified,
        size,
        lifecycle_age_started_at: row.lifecycle_age_started_at,
        became_noncurrent_at: row.became_noncurrent_at,
    }))
}

async fn source_page<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    source: LifecycleScanSource,
    cursor: Option<&LifecycleScanCursor>,
    limit: u64,
) -> AppResult<Vec<LifecycleCandidate>> {
    match source {
        LifecycleScanSource::Current | LifecycleScanSource::Noncurrent => {
            let is_latest = source == LifecycleScanSource::Current;
            let mut query = object_version::Entity::find()
                .find_also_related(object::Entity)
                .filter(object_version::Column::Bucket.eq(bucket))
                .filter(object_version::Column::IsLatest.eq(is_latest));
            if let Some(cursor) = cursor {
                query = query.filter(version_tuple_after(cursor));
            }
            let rows = query
                .order_by_asc(object_version::Column::Key)
                .order_by_asc(object_version::Column::Sequence)
                .order_by_asc(object_version::Column::Id)
                .limit(limit)
                .all(db)
                .await?;
            let mut candidates = Vec::with_capacity(rows.len());
            for (row, joined_object) in rows {
                candidates.push(candidate_from_row(db, row, joined_object).await?);
            }
            Ok(candidates)
        }
        LifecycleScanSource::Multipart => {
            let mut query = multipart_upload::Entity::find()
                .filter(multipart_upload::Column::Bucket.eq(bucket));
            if let Some(cursor) = cursor {
                query = query.filter(multipart_tuple_after(cursor));
            }
            let rows = query
                .order_by_asc(multipart_upload::Column::Key)
                .order_by_asc(multipart_upload::Column::CreatedAt)
                .order_by_asc(multipart_upload::Column::UploadId)
                .limit(limit)
                .all(db)
                .await?;
            Ok(rows
                .into_iter()
                .map(|row| {
                    LifecycleCandidate::MultipartUpload(MultipartUploadTargetIdentity {
                        bucket: row.bucket,
                        key: row.key,
                        upload_id: row.upload_id,
                        initiated_at: row.created_at,
                    })
                })
                .collect())
        }
    }
}

async fn source_has_after<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    source: LifecycleScanSource,
    cursor: Option<&LifecycleScanCursor>,
) -> AppResult<bool> {
    match source {
        LifecycleScanSource::Current | LifecycleScanSource::Noncurrent => {
            let is_latest = source == LifecycleScanSource::Current;
            let mut query = object_version::Entity::find()
                .filter(object_version::Column::Bucket.eq(bucket))
                .filter(object_version::Column::IsLatest.eq(is_latest));
            if let Some(cursor) = cursor {
                query = query.filter(version_tuple_after(cursor));
            }
            Ok(query.one(db).await?.is_some())
        }
        LifecycleScanSource::Multipart => {
            let mut query = multipart_upload::Entity::find()
                .filter(multipart_upload::Column::Bucket.eq(bucket));
            if let Some(cursor) = cursor {
                query = query.filter(multipart_tuple_after(cursor));
            }
            Ok(query.one(db).await?.is_some())
        }
    }
}

async fn source_exhausted_after<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    source: LifecycleScanSource,
    candidates: &[LifecycleCandidate],
    limit: u64,
) -> AppResult<bool> {
    if candidates.len() < limit as usize {
        return Ok(true);
    }
    let cursor = candidates
        .last()
        .map(|candidate| cursor_for(source.clone(), candidate));
    Ok(!source_has_after(db, bucket, source, cursor.as_ref()).await?)
}

pub async fn scan_candidate_page<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    cursor: Option<&LifecycleScanCursor>,
    limit: u64,
) -> AppResult<LifecycleCandidatePage> {
    if limit == 0 || limit > MAX_LIFECYCLE_SCAN_PAGE_SIZE {
        return Err(AppError::InvalidArgument(
            "lifecycle scan page limit must be between 1 and 1000".to_owned(),
        ));
    }
    if let Some(cursor) = cursor {
        validate_cursor(cursor, bucket)?;
    }

    let mut candidates = Vec::with_capacity(limit as usize);
    let mut last_source = None;
    let mut cycle_complete = false;

    let sources = [
        LifecycleScanSource::Current,
        LifecycleScanSource::Noncurrent,
        LifecycleScanSource::Multipart,
    ];
    let start_index = match cursor.map(|cursor| &cursor.source) {
        None | Some(LifecycleScanSource::Current) => 0,
        Some(LifecycleScanSource::Noncurrent) => 1,
        Some(LifecycleScanSource::Multipart) => 2,
    };

    for (index, source) in sources.iter().enumerate().skip(start_index) {
        let remaining = limit - candidates.len() as u64;
        if remaining == 0 {
            let mut later_source_has_candidates = false;
            for later_source in sources.iter().skip(index) {
                if source_has_after(db, bucket, later_source.clone(), None).await? {
                    later_source_has_candidates = true;
                    break;
                }
            }
            cycle_complete = !later_source_has_candidates;
            break;
        }

        let source_cursor = cursor.filter(|cursor| cursor.source == *source);
        let page = source_page(db, bucket, source.clone(), source_cursor, remaining).await?;
        let exhausted =
            source_exhausted_after(db, bucket, source.clone(), &page, remaining).await?;
        if !page.is_empty() {
            last_source = Some(source.clone());
        }
        candidates.extend(page);

        if !exhausted {
            break;
        }
        if *source == LifecycleScanSource::Multipart {
            cycle_complete = true;
            break;
        }
    }

    let next_cursor = match (last_source, candidates.last()) {
        (Some(source), Some(candidate)) => Some(cursor_for(source, candidate)),
        _ => None,
    };
    Ok(LifecycleCandidatePage {
        candidates,
        next_cursor,
        cycle_complete,
    })
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use chrono::{TimeZone, Utc};
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, EntityTrait, PaginatorTrait, QueryFilter, Set,
    };

    use super::{decode_cursor, encode_cursor, scan_candidate_page};
    use crate::{
        error::AppError,
        lifecycle::model::{LifecycleCandidate, LifecycleScanCursor, LifecycleScanSource},
        store::{
            bucket,
            entities::{multipart_upload, object, object_version},
        },
    };

    fn timestamp(sequence: i64) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 26, 0, 0, sequence as u32)
            .single()
            .unwrap()
    }

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        bucket::create(&db, "bucket", None).await.unwrap();
        bucket::create(&db, "other", None).await.unwrap();
        db
    }

    async fn insert_candidate(
        db: &sea_orm::DatabaseConnection,
        id: &str,
        key: &str,
        sequence: i64,
        is_latest: bool,
        object_id: Option<&str>,
        version_id: Option<String>,
    ) {
        let now = timestamp(sequence);
        if let Some(object_id) = object_id {
            object::Entity::insert(object::ActiveModel {
                id: Set(object_id.to_owned()),
                bucket: Set("bucket".to_owned()),
                key: Set(key.to_owned()),
                cid: Set(format!("cid-{object_id}")),
                size: Set(sequence * 10),
                content_type: Set(None),
                etag: Set(format!("cid-{object_id}")),
                metadata: Set(None),
                encrypted: Set(false),
                key_wrap: Set(None),
                sse_c_key_fingerprint: Set(None),
                multipart: Set(false),
                is_latest: Set(is_latest),
                created_at: Set(now),
            })
            .exec(db)
            .await
            .unwrap();
        }
        object_version::Entity::insert(object_version::ActiveModel {
            id: Set(id.to_owned()),
            bucket: Set("bucket".to_owned()),
            key: Set(key.to_owned()),
            version_id: Set(version_id),
            kind: Set(if object_id.is_some() {
                "object".to_owned()
            } else {
                "delete_marker".to_owned()
            }),
            object_id: Set(object_id.map(str::to_owned)),
            sequence: Set(sequence),
            is_latest: Set(is_latest),
            lifecycle_age_started_at: Set(now),
            became_noncurrent_at: Set((!is_latest).then_some(now)),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(db)
        .await
        .unwrap();
    }

    async fn insert_upload(
        db: &sea_orm::DatabaseConnection,
        upload_id: &str,
        bucket: &str,
        key: &str,
        created_at: chrono::DateTime<Utc>,
    ) {
        multipart_upload::Entity::insert(multipart_upload::ActiveModel {
            upload_id: Set(upload_id.to_owned()),
            object_id: Set(format!("object-{upload_id}")),
            bucket: Set(bucket.to_owned()),
            key: Set(key.to_owned()),
            created_at: Set(created_at),
            encryption_mode: Set("plain".to_owned()),
            key_wrap: Set(None),
            sse_c_key_fingerprint: Set(None),
            content_type: Set(None),
            metadata: Set(None),
            tags_json: Set(serde_json::json!([])),
            decompress_zip_target: Set(None),
            decompress_zip_result: Set(false),
        })
        .exec(db)
        .await
        .unwrap();
    }

    fn encoded_json(value: &str) -> String {
        URL_SAFE_NO_PAD.encode(value)
    }

    fn cursor_json(cursor: &LifecycleScanCursor) -> serde_json::Value {
        let json = URL_SAFE_NO_PAD.decode(encode_cursor(cursor)).unwrap();
        serde_json::from_slice(&json).unwrap()
    }

    fn cursor_signature(cursor: &LifecycleScanCursor) -> String {
        let cursor = cursor_json(cursor);
        let source = cursor["source"].as_str().unwrap();
        let identity = match source {
            "Current" | "Noncurrent" => cursor["version_row_id"].as_str().unwrap(),
            "Multipart" => cursor["multipart_upload_id"].as_str().unwrap(),
            source => panic!("unexpected lifecycle scan source {source}"),
        };
        format!("{source}:{identity}")
    }

    fn opaque_version() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    #[test]
    fn lifecycle_scan_cursor_decodes_phase_a_and_rejects_mixed_multipart_shapes() {
        let phase_a_json = r#"{"source":"Current","bucket":"bucket","key":"key","sequence":1,"version_row_id":"row"}"#;
        let phase_a = decode_cursor(&encoded_json(phase_a_json), "bucket").unwrap();
        let reencoded = URL_SAFE_NO_PAD.decode(encode_cursor(&phase_a)).unwrap();
        assert_eq!(std::str::from_utf8(&reencoded).unwrap(), phase_a_json);

        let phase_a_noncurrent = encoded_json(
            r#"{"source":"Noncurrent","bucket":"bucket","key":"key","sequence":2,"version_row_id":"row-2"}"#,
        );
        assert!(decode_cursor(&phase_a_noncurrent, "bucket").is_ok());

        let valid_multipart = encoded_json(
            r#"{"source":"Multipart","bucket":"bucket","key":"key","multipart_created_at":"2026-09-01T00:00:00Z","multipart_upload_id":"upload-1"}"#,
        );
        let invalid = [
            encoded_json(
                r#"{"source":"Current","bucket":"bucket","key":"key","version_row_id":"row"}"#,
            ),
            encoded_json(r#"{"source":"Noncurrent","bucket":"bucket","key":"key","sequence":1}"#),
            encoded_json(
                r#"{"source":"Current","bucket":"bucket","key":"key","sequence":1,"version_row_id":"row","multipart_created_at":"2026-09-01T00:00:00Z","multipart_upload_id":"upload-1"}"#,
            ),
            encoded_json(
                r#"{"source":"Multipart","bucket":"bucket","key":"key","multipart_upload_id":"upload-1"}"#,
            ),
            encoded_json(
                r#"{"source":"Multipart","bucket":"bucket","key":"key","multipart_created_at":"2026-09-01T00:00:00Z"}"#,
            ),
            encoded_json(
                r#"{"source":"Multipart","bucket":"bucket","key":"key","sequence":1,"version_row_id":"row","multipart_created_at":"2026-09-01T00:00:00Z","multipart_upload_id":"upload-1"}"#,
            ),
            encoded_json(
                r#"{"source":"Multipart","bucket":"bucket","key":"","multipart_created_at":"2026-09-01T00:00:00Z","multipart_upload_id":"upload-1"}"#,
            ),
            encoded_json(
                r#"{"source":"Multipart","bucket":"bucket","key":"key","multipart_created_at":"2026-09-01T00:00:00Z","multipart_upload_id":""}"#,
            ),
            encoded_json(
                r#"{"source":"Multipart","bucket":"","key":"key","multipart_created_at":"2026-09-01T00:00:00Z","multipart_upload_id":"upload-1"}"#,
            ),
        ];
        for invalid in invalid {
            assert!(matches!(
                decode_cursor(&invalid, "bucket"),
                Err(AppError::InvalidArgument(_))
            ));
        }
        assert!(matches!(
            decode_cursor(&valid_multipart, "other"),
            Err(AppError::InvalidArgument(_))
        ));
        assert!(matches!(
            decode_cursor(&valid_multipart, ""),
            Err(AppError::InvalidArgument(_))
        ));
        assert!(decode_cursor(&valid_multipart, "bucket").is_ok());
    }

    #[tokio::test]
    async fn lifecycle_scan_pages_current_noncurrent_multipart_without_duplicates_or_loops() {
        let db = setup().await;
        insert_candidate(
            &db,
            "current-row",
            "current-key",
            1,
            true,
            Some("current-object"),
            None,
        )
        .await;
        insert_candidate(
            &db,
            "noncurrent-row",
            "noncurrent-key",
            2,
            false,
            Some("noncurrent-object"),
            Some(opaque_version()),
        )
        .await;
        insert_upload(&db, "active-upload", "bucket", "upload-key", timestamp(3)).await;
        insert_upload(
            &db,
            "other-bucket-upload",
            "other",
            "other-key",
            timestamp(4),
        )
        .await;
        object::Entity::insert(object::ActiveModel {
            id: Set("published-multipart".to_owned()),
            bucket: Set("bucket".to_owned()),
            key: Set("published-not-active".to_owned()),
            cid: Set("cid-published-multipart".to_owned()),
            size: Set(10),
            content_type: Set(None),
            etag: Set("cid-published-multipart".to_owned()),
            metadata: Set(None),
            encrypted: Set(false),
            key_wrap: Set(None),
            sse_c_key_fingerprint: Set(None),
            multipart: Set(true),
            is_latest: Set(true),
            created_at: Set(timestamp(5)),
        })
        .exec(&db)
        .await
        .unwrap();

        let mut cursor = None;
        let mut signatures = Vec::new();
        let mut cycle_complete = false;
        for _ in 0..8 {
            let page = scan_candidate_page(&db, "bucket", cursor.as_ref(), 1)
                .await
                .unwrap();
            assert_eq!(page.candidates.len(), 1, "each bounded page must advance");
            let next = page
                .next_cursor
                .expect("a nonempty lifecycle page must return a cursor");
            signatures.push(cursor_signature(&next));
            cycle_complete = page.cycle_complete;
            cursor = Some(next);
            if cycle_complete {
                break;
            }
        }

        assert!(cycle_complete, "bounded traversal must terminate");
        assert_eq!(
            signatures,
            [
                "Current:current-row",
                "Noncurrent:noncurrent-row",
                "Multipart:active-upload",
            ]
        );
        assert_eq!(
            signatures.len(),
            signatures
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            "a source transition must not duplicate or loop"
        );
        let combined = scan_candidate_page(&db, "bucket", None, 3).await.unwrap();
        assert!(combined.cycle_complete);
        assert_eq!(combined.candidates.len(), 3);
        assert!(
            matches!(&combined.candidates[0], LifecycleCandidate::Version(candidate) if candidate.is_latest)
        );
        assert!(
            matches!(&combined.candidates[1], LifecycleCandidate::Version(candidate) if !candidate.is_latest)
        );
        assert!(
            matches!(&combined.candidates[2], LifecycleCandidate::MultipartUpload(target) if target.upload_id == "active-upload")
        );
    }

    #[tokio::test]
    async fn multipart_scan_uses_strict_key_created_upload_tuple_order() {
        let db = setup().await;
        let early = timestamp(1);
        let middle = timestamp(2);
        let late = timestamp(3);
        insert_upload(&db, "upload-b", "bucket", "a-key", middle).await;
        insert_upload(&db, "upload-a", "bucket", "a-key", middle).await;
        insert_upload(&db, "upload-z", "bucket", "a-key", late).await;
        insert_upload(&db, "upload-0", "bucket", "b-key", early).await;

        let mut cursor = None;
        let mut tuples = Vec::new();
        let mut cycle_complete = false;
        for _ in 0..8 {
            let page = scan_candidate_page(&db, "bucket", cursor.as_ref(), 1)
                .await
                .unwrap();
            assert_eq!(page.candidates.len(), 1, "each multipart page must advance");
            let next = page
                .next_cursor
                .expect("a multipart candidate must produce a cursor");
            let json = cursor_json(&next);
            assert_eq!(json["source"], "Multipart");
            tuples.push((
                json["key"].as_str().unwrap().to_owned(),
                json["multipart_created_at"]
                    .as_str()
                    .unwrap()
                    .parse::<chrono::DateTime<Utc>>()
                    .unwrap(),
                json["multipart_upload_id"].as_str().unwrap().to_owned(),
            ));
            cycle_complete = page.cycle_complete;
            cursor = Some(next);
            if cycle_complete {
                break;
            }
        }

        assert!(cycle_complete, "multipart traversal must terminate");
        assert_eq!(
            tuples,
            [
                ("a-key".to_owned(), middle, "upload-a".to_owned()),
                ("a-key".to_owned(), middle, "upload-b".to_owned()),
                ("a-key".to_owned(), late, "upload-z".to_owned()),
                ("b-key".to_owned(), early, "upload-0".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn multipart_cursor_race_defers_before_cursor_insert_until_fresh_cycle() {
        let db = setup().await;
        let initiated_at = timestamp(1);
        insert_upload(&db, "upload-b", "bucket", "same-key", initiated_at).await;
        insert_upload(&db, "upload-d", "bucket", "same-key", initiated_at).await;

        let first = scan_candidate_page(&db, "bucket", None, 1).await.unwrap();
        assert_eq!(first.candidates.len(), 1);
        assert!(!first.cycle_complete);
        let first_cursor = first.next_cursor.unwrap();
        assert_eq!(cursor_signature(&first_cursor), "Multipart:upload-b");

        insert_upload(&db, "upload-a", "bucket", "same-key", initiated_at).await;
        insert_upload(&db, "upload-c", "bucket", "same-key", initiated_at).await;

        let mut current_cycle = vec![cursor_signature(&first_cursor)];
        let mut cursor = Some(first_cursor);
        let mut cycle_complete = false;
        for _ in 0..6 {
            let page = scan_candidate_page(&db, "bucket", cursor.as_ref(), 1)
                .await
                .unwrap();
            assert_eq!(page.candidates.len(), 1, "resumed scan must advance");
            let next = page.next_cursor.unwrap();
            current_cycle.push(cursor_signature(&next));
            cycle_complete = page.cycle_complete;
            cursor = Some(next);
            if cycle_complete {
                break;
            }
        }
        assert!(cycle_complete);
        assert_eq!(
            current_cycle,
            [
                "Multipart:upload-b",
                "Multipart:upload-c",
                "Multipart:upload-d",
            ],
            "an insert before the stored cursor waits while one after it is visible"
        );

        let mut fresh_cycle = Vec::new();
        let mut cursor = None;
        let mut fresh_complete = false;
        for _ in 0..8 {
            let page = scan_candidate_page(&db, "bucket", cursor.as_ref(), 1)
                .await
                .unwrap();
            assert_eq!(page.candidates.len(), 1, "fresh scan must advance");
            let next = page.next_cursor.unwrap();
            fresh_cycle.push(cursor_signature(&next));
            fresh_complete = page.cycle_complete;
            cursor = Some(next);
            if fresh_complete {
                break;
            }
        }
        assert!(fresh_complete);
        assert_eq!(
            fresh_cycle,
            [
                "Multipart:upload-a",
                "Multipart:upload-b",
                "Multipart:upload-c",
                "Multipart:upload-d",
            ]
        );
    }

    #[test]
    fn lifecycle_scan_cursor_round_trips_and_rejects_invalid_input() {
        let cursor = LifecycleScanCursor {
            source: LifecycleScanSource::Current,
            bucket: "bucket".to_owned(),
            key: "a/key".to_owned(),
            sequence: Some(7),
            version_row_id: Some("version-row".to_owned()),
            multipart_created_at: None,
            multipart_upload_id: None,
        };
        let encoded = encode_cursor(&cursor);
        assert!(!encoded.contains('='));
        assert_eq!(decode_cursor(&encoded, "bucket").unwrap(), cursor);

        for invalid in [
            "%%%",
            "not-base64",
            &URL_SAFE_NO_PAD.encode("not json"),
            &URL_SAFE_NO_PAD.encode(
                r#"{"source":"unknown","bucket":"bucket","key":"key","sequence":1,"version_row_id":"row"}"#,
            ),
            &URL_SAFE_NO_PAD.encode(
                r#"{"source":"Current","bucket":"bucket","key":"key","sequence":0,"version_row_id":"row"}"#,
            ),
            &URL_SAFE_NO_PAD.encode(
                r#"{"source":"Current","bucket":"bucket","key":"","sequence":1,"version_row_id":"row"}"#,
            ),
            &URL_SAFE_NO_PAD.encode(
                r#"{"source":"Current","bucket":"bucket","key":"key","sequence":1,"version_row_id":""}"#,
            ),
        ] {
            assert!(matches!(
                decode_cursor(invalid, "bucket"),
                Err(AppError::InvalidArgument(_))
            ));
        }

        let wrong_bucket = LifecycleScanCursor {
            bucket: "other".to_owned(),
            ..cursor
        };
        assert!(matches!(
            decode_cursor(&encode_cursor(&wrong_bucket), "bucket"),
            Err(AppError::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn lifecycle_scan_pages_current_then_noncurrent_without_duplicates_or_loops() {
        let db = setup().await;
        insert_candidate(
            &db,
            "current-null",
            "z-current",
            1,
            true,
            Some("current-null-object"),
            None,
        )
        .await;
        insert_candidate(
            &db,
            "marker-noncurrent",
            "a-history",
            1,
            false,
            None,
            Some(opaque_version()),
        )
        .await;
        insert_candidate(
            &db,
            "object-noncurrent",
            "a-history",
            2,
            false,
            Some("object-noncurrent-object"),
            Some(opaque_version()),
        )
        .await;
        object::Entity::insert(object::ActiveModel {
            id: Set("legacy-unindexed".to_owned()),
            bucket: Set("bucket".to_owned()),
            key: Set("legacy-hidden".to_owned()),
            cid: Set("cid-legacy".to_owned()),
            size: Set(99),
            content_type: Set(None),
            etag: Set("cid-legacy".to_owned()),
            metadata: Set(None),
            encrypted: Set(false),
            key_wrap: Set(None),
            sse_c_key_fingerprint: Set(None),
            multipart: Set(false),
            is_latest: Set(false),
            created_at: Set(timestamp(10)),
        })
        .exec(&db)
        .await
        .unwrap();

        let mut cursor = None;
        let mut seen = Vec::new();
        for _ in 0..4 {
            let page = scan_candidate_page(&db, "bucket", cursor.as_ref(), 1)
                .await
                .unwrap();
            seen.extend(page.candidates.iter().map(|candidate| {
                let crate::lifecycle::model::LifecycleCandidate::Version(candidate) = candidate
                else {
                    panic!("version scan returned a multipart candidate");
                };
                candidate.target.version_row_id.clone()
            }));
            cursor = page.next_cursor;
            if page.cycle_complete {
                break;
            }
        }

        assert_eq!(
            seen,
            ["current-null", "marker-noncurrent", "object-noncurrent"]
        );
        assert_eq!(
            seen.len(),
            seen.iter().collect::<std::collections::BTreeSet<_>>().len()
        );
        assert!(!seen.contains(&"legacy-unindexed".to_owned()));

        let marker = scan_candidate_page(&db, "bucket", None, 3)
            .await
            .unwrap()
            .candidates
            .into_iter()
            .find(|candidate| match candidate {
                crate::lifecycle::model::LifecycleCandidate::Version(candidate) => {
                    candidate.target.version_row_id == "marker-noncurrent"
                }
                crate::lifecycle::model::LifecycleCandidate::MultipartUpload(_) => false,
            })
            .unwrap();
        let crate::lifecycle::model::LifecycleCandidate::Version(marker) = marker else {
            panic!("version scan returned a multipart candidate");
        };
        assert_eq!(marker.size, 0);
        assert!(marker.target.object_id.is_none());
    }

    #[tokio::test]
    async fn lifecycle_scan_cursor_race_never_duplicates_and_fresh_cycle_finds_earlier_insert() {
        let db = setup().await;
        insert_candidate(
            &db,
            "before-cursor",
            "b-key",
            1,
            false,
            Some("before-cursor-object"),
            Some(opaque_version()),
        )
        .await;
        insert_candidate(
            &db,
            "after-cursor",
            "d-key",
            1,
            false,
            Some("after-cursor-object"),
            Some(opaque_version()),
        )
        .await;

        let first = scan_candidate_page(&db, "bucket", None, 1).await.unwrap();
        let crate::lifecycle::model::LifecycleCandidate::Version(first_candidate) =
            &first.candidates[0]
        else {
            panic!("version scan returned a multipart candidate");
        };
        assert_eq!(first_candidate.target.version_row_id, "before-cursor");
        let cursor = first.next_cursor.unwrap();

        insert_candidate(
            &db,
            "new-before-cursor",
            "a-key",
            1,
            false,
            Some("new-before-cursor-object"),
            Some(opaque_version()),
        )
        .await;
        insert_candidate(
            &db,
            "new-after-cursor",
            "c-key",
            1,
            false,
            Some("new-after-cursor-object"),
            Some(opaque_version()),
        )
        .await;

        let mut seen = vec!["before-cursor".to_owned()];
        let mut next = Some(cursor);
        for _ in 0..3 {
            let page = scan_candidate_page(&db, "bucket", next.as_ref(), 1)
                .await
                .unwrap();
            seen.extend(page.candidates.iter().map(|candidate| {
                let crate::lifecycle::model::LifecycleCandidate::Version(candidate) = candidate
                else {
                    panic!("version scan returned a multipart candidate");
                };
                candidate.target.version_row_id.clone()
            }));
            next = page.next_cursor;
            if page.cycle_complete {
                break;
            }
        }
        assert_eq!(seen, ["before-cursor", "new-after-cursor", "after-cursor"]);
        assert_eq!(
            seen.len(),
            seen.iter().collect::<std::collections::BTreeSet<_>>().len()
        );

        let fresh = scan_candidate_page(&db, "bucket", None, 10).await.unwrap();
        assert!(fresh.candidates.iter().any(|candidate| {
            let crate::lifecycle::model::LifecycleCandidate::Version(candidate) = candidate else {
                return false;
            };
            candidate.target.version_row_id == "new-before-cursor"
        }));
    }

    #[tokio::test]
    async fn lifecycle_scan_rejects_zero_or_unbounded_page_limits() {
        let db = setup().await;
        for limit in [0, 1_001] {
            assert!(matches!(
                scan_candidate_page(&db, "bucket", None, limit).await,
                Err(AppError::InvalidArgument(_))
            ));
        }
        assert_eq!(
            object_version::Entity::find()
                .filter(object_version::Column::Bucket.eq("bucket"))
                .count(&db)
                .await
                .unwrap(),
            0
        );
    }
}
