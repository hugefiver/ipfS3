use crate::error::{AppError, AppResult};
use chrono::Utc;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};
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

pub async fn insert_part<C: ConnectionTrait>(
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

    multipart_part::Entity::insert(model).exec(db).await?;
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

pub async fn delete_part<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    part_number: i32,
) -> AppResult<()> {
    multipart_part::Entity::delete_many()
        .filter(multipart_part::Column::UploadId.eq(upload_id))
        .filter(multipart_part::Column::PartNumber.eq(part_number))
        .exec(db)
        .await?;
    Ok(())
}
