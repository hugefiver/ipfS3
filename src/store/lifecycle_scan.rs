use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::{AppError, AppResult},
    lifecycle::model::{
        LifecycleCandidate, LifecycleCandidatePage, LifecycleScanCursor, LifecycleScanSource,
        VersionTargetIdentity,
    },
    store::{
        entities::{object, object_version},
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
    sequence: i64,
    version_row_id: String,
}

impl From<&LifecycleScanCursor> for StoredCursor {
    fn from(cursor: &LifecycleScanCursor) -> Self {
        Self {
            source: cursor.source.clone(),
            bucket: cursor.bucket.clone(),
            key: cursor.key.clone(),
            sequence: cursor.sequence,
            version_row_id: cursor.version_row_id.clone(),
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
        || cursor.sequence <= 0
        || cursor.version_row_id.is_empty()
    {
        return Err(invalid_cursor());
    }
    Ok(())
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

fn tuple_after(cursor: &LifecycleScanCursor) -> Condition {
    Condition::any()
        .add(object_version::Column::Key.gt(cursor.key.clone()))
        .add(
            Condition::all()
                .add(object_version::Column::Key.eq(cursor.key.clone()))
                .add(
                    Condition::any()
                        .add(object_version::Column::Sequence.gt(cursor.sequence))
                        .add(
                            Condition::all()
                                .add(object_version::Column::Sequence.eq(cursor.sequence))
                                .add(object_version::Column::Id.gt(cursor.version_row_id.clone())),
                        ),
                ),
        )
}

fn source_filter(source: LifecycleScanSource) -> bool {
    matches!(source, LifecycleScanSource::Current)
}

fn cursor_for(source: LifecycleScanSource, candidate: &LifecycleCandidate) -> LifecycleScanCursor {
    LifecycleScanCursor {
        source,
        bucket: candidate.target.bucket.clone(),
        key: candidate.target.key.clone(),
        sequence: candidate.target.sequence,
        version_row_id: candidate.target.version_row_id.clone(),
    }
}

fn candidate_from_row(
    row: object_version::Model,
    joined_object: Option<object::Model>,
) -> AppResult<LifecycleCandidate> {
    let kind = version_kind(&row)?;
    let public_version_id = public_version_id(&row)?;
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
    Ok(LifecycleCandidate {
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
        size,
        lifecycle_age_started_at: row.lifecycle_age_started_at,
        became_noncurrent_at: row.became_noncurrent_at,
    })
}

async fn source_page<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    source: LifecycleScanSource,
    cursor: Option<&LifecycleScanCursor>,
    limit: u64,
) -> AppResult<Vec<LifecycleCandidate>> {
    let mut query = object_version::Entity::find()
        .find_also_related(object::Entity)
        .filter(object_version::Column::Bucket.eq(bucket))
        .filter(object_version::Column::IsLatest.eq(source_filter(source)));
    if let Some(cursor) = cursor {
        query = query.filter(tuple_after(cursor));
    }
    let rows = query
        .order_by_asc(object_version::Column::Key)
        .order_by_asc(object_version::Column::Sequence)
        .order_by_asc(object_version::Column::Id)
        .limit(limit)
        .all(db)
        .await?;
    rows.into_iter()
        .map(|(row, joined_object)| candidate_from_row(row, joined_object))
        .collect()
}

async fn source_has_after<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    source: LifecycleScanSource,
    cursor: Option<&LifecycleScanCursor>,
) -> AppResult<bool> {
    let mut query = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(bucket))
        .filter(object_version::Column::IsLatest.eq(source_filter(source)));
    if let Some(cursor) = cursor {
        query = query.filter(tuple_after(cursor));
    }
    Ok(query.one(db).await?.is_some())
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

    if !matches!(
        cursor.map(|cursor| &cursor.source),
        Some(LifecycleScanSource::Noncurrent)
    ) {
        let current_cursor = cursor.filter(|cursor| cursor.source == LifecycleScanSource::Current);
        let current = source_page(
            db,
            bucket,
            LifecycleScanSource::Current,
            current_cursor,
            limit,
        )
        .await?;
        let current_exhausted =
            source_exhausted_after(db, bucket, LifecycleScanSource::Current, &current, limit)
                .await?;
        if !current.is_empty() {
            last_source = Some(LifecycleScanSource::Current);
        }
        candidates.extend(current);

        if current_exhausted {
            let remaining = limit - candidates.len() as u64;
            if remaining > 0 {
                let noncurrent =
                    source_page(db, bucket, LifecycleScanSource::Noncurrent, None, remaining)
                        .await?;
                cycle_complete = source_exhausted_after(
                    db,
                    bucket,
                    LifecycleScanSource::Noncurrent,
                    &noncurrent,
                    remaining,
                )
                .await?;
                if !noncurrent.is_empty() {
                    last_source = Some(LifecycleScanSource::Noncurrent);
                }
                candidates.extend(noncurrent);
            } else {
                cycle_complete =
                    !source_has_after(db, bucket, LifecycleScanSource::Noncurrent, None).await?;
            }
        }
    } else {
        let noncurrent =
            source_page(db, bucket, LifecycleScanSource::Noncurrent, cursor, limit).await?;
        cycle_complete = source_exhausted_after(
            db,
            bucket,
            LifecycleScanSource::Noncurrent,
            &noncurrent,
            limit,
        )
        .await?;
        if !noncurrent.is_empty() {
            last_source = Some(LifecycleScanSource::Noncurrent);
        }
        candidates.extend(noncurrent);
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
        lifecycle::model::{LifecycleScanCursor, LifecycleScanSource},
        store::{
            bucket,
            entities::{object, object_version},
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

    fn opaque_version() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    #[test]
    fn lifecycle_scan_cursor_round_trips_and_rejects_invalid_input() {
        let cursor = LifecycleScanCursor {
            source: LifecycleScanSource::Current,
            bucket: "bucket".to_owned(),
            key: "a/key".to_owned(),
            sequence: 7,
            version_row_id: "version-row".to_owned(),
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
            seen.extend(
                page.candidates
                    .iter()
                    .map(|candidate| candidate.target.version_row_id.clone()),
            );
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
            .find(|candidate| candidate.target.version_row_id == "marker-noncurrent")
            .unwrap();
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
        assert_eq!(first.candidates[0].target.version_row_id, "before-cursor");
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
            seen.extend(
                page.candidates
                    .iter()
                    .map(|candidate| candidate.target.version_row_id.clone()),
            );
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
        assert!(
            fresh
                .candidates
                .iter()
                .any(|candidate| candidate.target.version_row_id == "new-before-cursor")
        );
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
