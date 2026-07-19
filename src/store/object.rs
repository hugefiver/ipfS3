use crate::error::{AppError, AppResult};
use chrono::Utc;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, TransactionTrait,
};
use serde_json::Value as JsonValue;

use super::entities::object;

#[derive(Debug, Clone, PartialEq)]
pub struct LatestObjectRow {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub cid: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub etag: String,
    pub metadata: Option<JsonValue>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub multipart: bool,
    pub created_at: chrono::DateTime<Utc>,
}

pub(crate) async fn write_latest_in_transaction<C: ConnectionTrait>(
    db: &C,
    row: LatestObjectRow,
) -> Result<(), sea_orm::DbErr> {
    let bucket = row.bucket.clone();
    let key = row.key.clone();
    object::Entity::update_many()
        .col_expr(object::Column::IsLatest, false.into())
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .exec(db)
        .await?;

    object::Entity::insert(object::ActiveModel {
        id: Set(row.id),
        bucket: Set(row.bucket),
        key: Set(row.key),
        cid: Set(row.cid),
        size: Set(row.size),
        content_type: Set(row.content_type),
        etag: Set(row.etag),
        metadata: Set(row.metadata),
        encrypted: Set(row.encrypted),
        key_wrap: Set(row.key_wrap),
        multipart: Set(row.multipart),
        is_latest: Set(true),
        created_at: Set(row.created_at),
    })
    .exec(db)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    id: &str,
    bucket: &str,
    key: &str,
    cid: &str,
    size: i64,
    content_type: Option<&str>,
    etag: &str,
    metadata: Option<JsonValue>,
    encrypted: bool,
    key_wrap: Option<&str>,
    multipart: bool,
) -> AppResult<()> {
    // Retry on unique-constraint violations caused by concurrent upserts on
    // the same (bucket, key). S3 semantics are last-writer-wins; the partial
    // unique index `idx_objects_latest` may cause one transaction to fail if
    // two concurrent writes race. A small retry window resolves this.
    let row = LatestObjectRow {
        id: id.to_owned(),
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        cid: cid.to_owned(),
        size,
        content_type: content_type.map(str::to_owned),
        etag: etag.to_owned(),
        metadata,
        encrypted,
        key_wrap: key_wrap.map(str::to_owned),
        multipart,
        created_at: Utc::now(),
    };
    const MAX_RETRIES: usize = 3;
    for attempt in 0..=MAX_RETRIES {
        let result = db
            .transaction(|txn| {
                let row = row.clone();
                Box::pin(async move { write_latest_in_transaction(txn, row).await })
            })
            .await;

        match result {
            Ok(()) => return Ok(()),
            Err(sea_orm::TransactionError::Transaction(db_err)) => {
                let msg = db_err.to_string().to_lowercase();
                if (msg.contains("unique") || msg.contains("constraint")) && attempt < MAX_RETRIES {
                    // Retry — a concurrent upsert likely marked the old row
                    // non-latest between our UPDATE and INSERT.
                    continue;
                }
                return Err(AppError::from(db_err));
            }
            Err(sea_orm::TransactionError::Connection(db_err)) => {
                return Err(AppError::from(db_err));
            }
        }
    }
    unreachable!("retry loop exhausted without returning")
}

pub async fn get_latest<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
) -> AppResult<object::Model> {
    object::Entity::find()
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .one(db)
        .await?
        .ok_or_else(|| AppError::NoSuchKey(format!("{bucket}/{key}")))
}

pub async fn delete_latest_if_present<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
) -> AppResult<bool> {
    let result = object::Entity::update_many()
        .col_expr(object::Column::IsLatest, false.into())
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .exec(db)
        .await?;

    Ok(result.rows_affected > 0)
}

pub async fn delete_latest<C: ConnectionTrait>(db: &C, bucket: &str, key: &str) -> AppResult<()> {
    if !delete_latest_if_present(db, bucket, key).await? {
        return Err(AppError::NoSuchKey(format!("{bucket}/{key}")));
    }

    Ok(())
}

pub async fn list<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    prefix: Option<&str>,
    continuation_token: Option<&str>,
    max_keys: u64,
) -> AppResult<Vec<object::Model>> {
    let mut query = object::Entity::find()
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::IsLatest.eq(true));

    if let Some(pfx) = prefix {
        query = query.filter(object::Column::Key.starts_with(pfx));
    }

    if let Some(token) = continuation_token
        && !token.is_empty()
    {
        query = query.filter(object::Column::Key.gt(token));
    }

    let objects = query
        .order_by_asc(object::Column::Key)
        .limit(max_keys)
        .all(db)
        .await?;

    Ok(objects)
}

#[allow(dead_code)]
pub async fn count<C: ConnectionTrait>(db: &C, bucket: &str) -> AppResult<u64> {
    let count = object::Entity::find()
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::IsLatest.eq(true))
        .count(db)
        .await?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::Database;

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "test-bucket", None)
            .await
            .unwrap();
        db
    }

    fn latest_row(id: &str, cid: &str) -> LatestObjectRow {
        LatestObjectRow {
            id: id.to_owned(),
            bucket: "test-bucket".to_owned(),
            key: "archive.zip".to_owned(),
            cid: cid.to_owned(),
            size: 7,
            content_type: Some("application/zip".to_owned()),
            etag: cid.to_owned(),
            metadata: Some(serde_json::json!({"source": "multipart"})),
            encrypted: true,
            key_wrap: Some("wrapped-key".to_owned()),
            multipart: true,
            created_at: Utc::now(),
        }
    }

    async fn seed_latest(db: &sea_orm::DatabaseConnection, id: &str, key: &str) {
        let mut row = latest_row(id, &format!("Qm{id}"));
        row.key = key.to_owned();
        write_latest_in_transaction(db, row).await.unwrap();
    }

    #[tokio::test]
    async fn write_latest_in_transaction_replaces_latest_and_preserves_all_fields() {
        let db = setup().await;
        let first = latest_row("object-1", "QmOld");
        write_latest_in_transaction(&db, first.clone())
            .await
            .unwrap();

        let second = latest_row("object-2", "QmNew");
        write_latest_in_transaction(&db, second.clone())
            .await
            .unwrap();

        let latest = get_latest(&db, "test-bucket", "archive.zip").await.unwrap();
        assert_eq!(latest.id, second.id);
        assert_eq!(latest.bucket, second.bucket);
        assert_eq!(latest.key, second.key);
        assert_eq!(latest.cid, second.cid);
        assert_eq!(latest.size, second.size);
        assert_eq!(latest.content_type, second.content_type);
        assert_eq!(latest.etag, second.etag);
        assert_eq!(latest.metadata, second.metadata);
        assert_eq!(latest.encrypted, second.encrypted);
        assert_eq!(latest.key_wrap, second.key_wrap);
        assert_eq!(latest.multipart, second.multipart);
        assert_eq!(latest.created_at, second.created_at);
        assert!(latest.is_latest);

        let old = object::Entity::find_by_id("object-1")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(!old.is_latest);
    }

    #[tokio::test]
    async fn delete_latest_if_present_is_idempotent_and_hides_latest() {
        let db = setup().await;
        seed_latest(&db, "object-1", "archive.zip").await;

        assert!(
            delete_latest_if_present(&db, "test-bucket", "archive.zip")
                .await
                .unwrap()
        );
        assert!(
            !delete_latest_if_present(&db, "test-bucket", "archive.zip")
                .await
                .unwrap()
        );
        assert!(matches!(
            get_latest(&db, "test-bucket", "archive.zip").await,
            Err(AppError::NoSuchKey(path)) if path == "test-bucket/archive.zip"
        ));
    }

    #[tokio::test]
    async fn delete_latest_missing_remains_no_such_key() {
        let db = setup().await;

        assert!(matches!(
            delete_latest(&db, "test-bucket", "missing").await,
            Err(AppError::NoSuchKey(path)) if path == "test-bucket/missing"
        ));
    }
}
