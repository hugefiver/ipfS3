use crate::error::{AppError, AppResult};
use chrono::Utc;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, Set};

use super::entities::bucket;

pub async fn create<C: ConnectionTrait>(db: &C, name: &str, owner: Option<&str>) -> AppResult<()> {
    // Pre-check existence to avoid relying on backend-specific error strings.
    if exists(db, name).await? {
        return Err(AppError::BucketAlreadyExists(name.to_owned()));
    }

    let model = bucket::ActiveModel {
        name: Set(name.to_owned()),
        created_at: Set(Utc::now()),
        owner: Set(owner.map(|s| s.to_owned())),
    };

    // Concurrent inserts may still race past the exists() check. If the unique
    // constraint fires, normalize it to BucketAlreadyExists; any other error
    // propagates as-is.
    match bucket::Entity::insert(model).exec(db).await {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("unique")
                || msg.contains("duplicate")
                || msg.contains("primary key")
                || msg.contains("constraint")
            {
                Err(AppError::BucketAlreadyExists(name.to_owned()))
            } else {
                Err(AppError::from(e))
            }
        }
    }
}

pub async fn exists<C: ConnectionTrait>(db: &C, name: &str) -> AppResult<bool> {
    let count = bucket::Entity::find()
        .filter(bucket::Column::Name.eq(name))
        .count(db)
        .await?;
    Ok(count > 0)
}

pub async fn delete<C: ConnectionTrait>(db: &C, name: &str) -> AppResult<()> {
    use super::entities::object;

    // Check if bucket has latest objects
    let has_objects = object::Entity::find()
        .filter(object::Column::Bucket.eq(name))
        .filter(object::Column::IsLatest.eq(true))
        .count(db)
        .await?
        > 0;

    if has_objects {
        return Err(AppError::BucketNotEmpty(name.to_owned()));
    }

    let result = bucket::Entity::delete_by_id(name.to_owned())
        .exec(db)
        .await?;

    if result.rows_affected == 0 {
        return Err(AppError::NoSuchBucket(name.to_owned()));
    }

    Ok(())
}

pub async fn list<C: ConnectionTrait>(db: &C) -> AppResult<Vec<bucket::Model>> {
    let buckets = bucket::Entity::find().all(db).await?;
    Ok(buckets)
}

#[allow(dead_code)]
pub async fn get<C: ConnectionTrait>(db: &C, name: &str) -> AppResult<bucket::Model> {
    bucket::Entity::find_by_id(name.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| AppError::NoSuchBucket(name.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::run_migrations;
    use sea_orm::Database;

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();
        db
    }

    #[tokio::test]
    async fn test_bucket_crud() {
        let db = setup().await;

        // create
        create(&db, "test-bucket", Some("alice")).await.unwrap();

        // duplicate create
        let err = create(&db, "test-bucket", None).await.unwrap_err();
        assert!(matches!(err, AppError::BucketAlreadyExists(_)));

        // exists
        assert!(exists(&db, "test-bucket").await.unwrap());
        assert!(!exists(&db, "nonexistent").await.unwrap());

        // list
        let buckets = list(&db).await.unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name, "test-bucket");

        // get
        let bucket = get(&db, "test-bucket").await.unwrap();
        assert_eq!(bucket.owner.as_deref(), Some("alice"));

        // get nonexistent
        let err = get(&db, "nonexistent").await.unwrap_err();
        assert!(matches!(err, AppError::NoSuchBucket(_)));

        // delete
        delete(&db, "test-bucket").await.unwrap();

        // delete nonexistent
        let err = delete(&db, "test-bucket").await.unwrap_err();
        assert!(matches!(err, AppError::NoSuchBucket(_)));

        // exists after delete
        assert!(!exists(&db, "test-bucket").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_nonempty_bucket_fails() {
        let db = setup().await;

        create(&db, "my-bucket", None).await.unwrap();

        // Insert an object into the bucket
        crate::store::object::upsert(
            &db,
            "obj-1",
            "my-bucket",
            "file.txt",
            "bafy-test",
            1024,
            Some("text/plain"),
            "etag-abc",
            None,
            false,
            None,
            false,
        )
        .await
        .unwrap();

        let err = delete(&db, "my-bucket").await.unwrap_err();
        assert!(matches!(err, AppError::BucketNotEmpty(_)));
    }
}
