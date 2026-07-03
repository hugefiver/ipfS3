use crate::error::{AppError, AppResult};
use chrono::Utc;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, TransactionTrait,
};
use serde_json::Value as JsonValue;

use super::entities::object;

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
    const MAX_RETRIES: usize = 3;
    for attempt in 0..=MAX_RETRIES {
        let metadata_clone = metadata.clone();
        let result = db
            .transaction(|txn| {
                let id = id.to_owned();
                let bucket = bucket.to_owned();
                let key = key.to_owned();
                let cid = cid.to_owned();
                let content_type = content_type.map(|s| s.to_owned());
                let etag = etag.to_owned();
                let key_wrap = key_wrap.map(|s| s.to_owned());
                let metadata = metadata_clone.clone();
                Box::pin(async move {
                    object::Entity::update_many()
                        .col_expr(object::Column::IsLatest, false.into())
                        .filter(object::Column::Bucket.eq(&bucket))
                        .filter(object::Column::Key.eq(&key))
                        .filter(object::Column::IsLatest.eq(true))
                        .exec(txn)
                        .await?;

                    let model = object::ActiveModel {
                        id: Set(id),
                        bucket: Set(bucket),
                        key: Set(key),
                        cid: Set(cid),
                        size: Set(size),
                        content_type: Set(content_type),
                        etag: Set(etag),
                        metadata: Set(metadata),
                        encrypted: Set(encrypted),
                        key_wrap: Set(key_wrap),
                        multipart: Set(multipart),
                        is_latest: Set(true),
                        created_at: Set(Utc::now()),
                    };
                    object::Entity::insert(model).exec(txn).await?;
                    Ok::<_, sea_orm::DbErr>(())
                })
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

pub async fn delete_latest<C: ConnectionTrait>(db: &C, bucket: &str, key: &str) -> AppResult<()> {
    let result = object::Entity::update_many()
        .col_expr(object::Column::IsLatest, false.into())
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .exec(db)
        .await?;

    if result.rows_affected == 0 {
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
