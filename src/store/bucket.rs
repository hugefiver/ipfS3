use crate::error::{AppError, AppResult};
use chrono::Utc;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    QuerySelect, Set, TransactionError, TransactionTrait, sea_query::Expr,
};

use super::{
    entities::{bucket, import_job, multipart_upload, object_version},
    object_version::BucketVersioningState,
};

pub async fn create<C: ConnectionTrait>(db: &C, name: &str, owner: Option<&str>) -> AppResult<()> {
    // Pre-check existence to avoid relying on backend-specific error strings.
    if exists(db, name).await? {
        return Err(AppError::BucketAlreadyExists(name.to_owned()));
    }

    let model = bucket::ActiveModel {
        name: Set(name.to_owned()),
        created_at: Set(Utc::now()),
        owner: Set(owner.map(|s| s.to_owned())),
        versioning_status: Set(None),
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

pub async fn delete<C: ConnectionTrait + TransactionTrait>(db: &C, name: &str) -> AppResult<()> {
    let name = name.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            use super::entities::object;

            super::import::ownership::lock_bucket_for_ownership(txn, &name).await?;

            let has_multipart_uploads = multipart_upload::Entity::find()
                .filter(multipart_upload::Column::Bucket.eq(&name))
                .count(txn)
                .await?
                > 0;
            if has_multipart_uploads {
                return Err(AppError::BucketNotEmpty(name));
            }

            let has_active_imports = import_job::Entity::find()
                .filter(import_job::Column::Bucket.eq(&name))
                .filter(import_job::Column::State.is_in(["queued", "running"]))
                .count(txn)
                .await?
                > 0;
            if has_active_imports {
                return Err(AppError::BucketNotEmpty(name));
            }

            let has_objects = object::Entity::find()
                .filter(object::Column::Bucket.eq(&name))
                .filter(object::Column::IsLatest.eq(true))
                .count(txn)
                .await?
                > 0;
            if has_objects {
                return Err(AppError::BucketNotEmpty(name));
            }

            let has_versions = object_version::Entity::find()
                .filter(object_version::Column::Bucket.eq(&name))
                .count(txn)
                .await?
                > 0;
            if has_versions {
                return Err(AppError::BucketNotEmpty(name));
            }

            // A durable receipt protects an unfinished transition from bucket
            // cascades. Once atomically settled it is no longer a live owner;
            // remove only settled receipts through their guarded deletion API.
            // These action rows are terminal and cannot be claimed, so acquiring
            // their deletion locks here cannot invert an active action's order.
            let transitions = super::entities::lifecycle_transition::Entity::find()
                .filter(super::entities::lifecycle_transition::Column::Bucket.eq(&name))
                .all(txn)
                .await?;
            if transitions.iter().any(|saga| saga.completed_at.is_none()) {
                return Err(AppError::BucketNotEmpty(name));
            }
            for saga in transitions {
                super::lifecycle_transition::delete_settled_in_transaction(txn, &saga.id).await?;
            }

            super::import::ownership::supersede_bucket(txn, &name, Utc::now()).await?;
            let result = bucket::Entity::delete_by_id(name.clone()).exec(txn).await?;
            if result.rows_affected != 1 {
                return Err(AppError::NoSuchBucket(name));
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => error.into(),
    })
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

pub async fn get_versioning_state<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
) -> AppResult<BucketVersioningState> {
    let bucket = get(db, bucket_name).await?;
    BucketVersioningState::from_db_value(bucket.versioning_status.as_deref())
}

/// Locks the bucket row before its versioning state is consumed by a caller-owned transaction.
/// PostgreSQL uses `FOR UPDATE`; SQLite takes its serialized write intent with a no-op update.
pub async fn lock_versioning_state<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
) -> AppResult<BucketVersioningState> {
    if db.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        let bucket = bucket::Entity::find_by_id(bucket_name.to_owned())
            .lock_exclusive()
            .one(db)
            .await?
            .ok_or_else(|| AppError::NoSuchBucket(bucket_name.to_owned()))?;
        return BucketVersioningState::from_db_value(bucket.versioning_status.as_deref());
    }

    let locked = bucket::Entity::update_many()
        .col_expr(
            bucket::Column::CreatedAt,
            Expr::col(bucket::Column::CreatedAt).into(),
        )
        .filter(bucket::Column::Name.eq(bucket_name))
        .exec(db)
        .await?;
    if locked.rows_affected != 1 {
        return Err(AppError::NoSuchBucket(bucket_name.to_owned()));
    }

    get_versioning_state(db, bucket_name).await
}

pub async fn set_versioning_state(
    db: &DatabaseConnection,
    bucket_name: &str,
    state: BucketVersioningState,
) -> AppResult<()> {
    let status = state.as_db_value().ok_or_else(|| {
        AppError::InvalidArgument("versioning status must be Enabled or Suspended".to_owned())
    })?;
    let bucket_name = bucket_name.to_owned();

    db.transaction(move |txn| {
        Box::pin(async move {
            let current = lock_versioning_state(txn, &bucket_name).await?;
            if current == state {
                return Ok(());
            }

            let updated = bucket::Entity::update_many()
                .col_expr(bucket::Column::VersioningStatus, Expr::value(status))
                .filter(bucket::Column::Name.eq(&bucket_name))
                .exec(txn)
                .await?;
            if updated.rows_affected != 1 {
                return Err(AppError::NoSuchBucket(bucket_name));
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => error.into(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        import::ImportSource,
        pinning::tags::ObjectTag,
        store::{
            entities::{import_job, object, object_version},
            import::{jobs::NewImportJob, ownership::submit},
            run_migrations,
        },
    };
    use sea_orm::{ColumnTrait, ConnectionTrait, Database, EntityTrait, QueryFilter, Set};

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        run_migrations(&db).await.unwrap();
        db
    }

    async fn insert_object(db: &sea_orm::DatabaseConnection, bucket: &str, id: &str, key: &str) {
        crate::store::object::upsert(
            db,
            id,
            bucket,
            key,
            &format!("bafy-{id}"),
            7,
            Some("application/octet-stream"),
            &format!("bafy-{id}"),
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    }

    fn import_request(id: &str, bucket: &str, key: &str) -> NewImportJob {
        NewImportJob {
            id: id.to_owned(),
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            source: ImportSource::Cid(
                "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_owned(),
            ),
            request_fingerprint: format!("sha256:{id}"),
            client_token: None,
            object_content_type: Some("application/octet-stream".to_owned()),
            metadata: HashMap::new(),
            tags: vec![ObjectTag::new("fixture", "true")],
            decompress_prefix: None,
        }
    }

    async fn assert_not_empty(db: &sea_orm::DatabaseConnection, bucket: &str) {
        assert!(matches!(
            delete(db, bucket).await,
            Err(AppError::BucketNotEmpty(name)) if name == bucket
        ));
        assert!(exists(db, bucket).await.unwrap());
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
            None,
            false,
        )
        .await
        .unwrap();

        let err = delete(&db, "my-bucket").await.unwrap_err();
        assert!(matches!(err, AppError::BucketNotEmpty(_)));
    }

    #[tokio::test]
    async fn delete_bucket_blocks_active_multipart() {
        let db = setup().await;
        create(&db, "multipart-bucket", None).await.unwrap();
        crate::store::multipart::create_upload(
            &db,
            "upload-1",
            "multipart-object",
            "multipart-bucket",
            "large.bin",
            "plain",
            None,
            None,
            Some("application/octet-stream"),
            None,
            &[],
            None,
            false,
        )
        .await
        .unwrap();

        assert_not_empty(&db, "multipart-bucket").await;
    }

    #[tokio::test]
    async fn delete_bucket_blocks_active_import() {
        let db = setup().await;
        create(&db, "import-bucket", None).await.unwrap();
        submit(
            &db,
            import_request("active-import", "import-bucket", "future.bin"),
            Utc::now(),
        )
        .await
        .unwrap();

        assert_not_empty(&db, "import-bucket").await;
        let job = import_job::Entity::find_by_id("active-import")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.state, "queued");
    }

    #[tokio::test]
    async fn delete_bucket_blocks_current_unversioned_object() {
        let db = setup().await;
        create(&db, "unversioned-bucket", None).await.unwrap();
        insert_object(&db, "unversioned-bucket", "unversioned-current", "key").await;

        assert_not_empty(&db, "unversioned-bucket").await;

        crate::store::object::set_only_latest(&db, "unversioned-bucket", "key", None)
            .await
            .unwrap();
        let now = Utc::now();
        object_version::Entity::insert(object_version::ActiveModel {
            id: Set("hidden-null".to_owned()),
            bucket: Set("unversioned-bucket".to_owned()),
            key: Set("key".to_owned()),
            version_id: Set(None),
            kind: Set("object".to_owned()),
            object_id: Set(Some("unversioned-current".to_owned())),
            sequence: Set(1),
            is_latest: Set(true),
            lifecycle_age_started_at: Set(now),
            became_noncurrent_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&db)
        .await
        .unwrap();

        assert_not_empty(&db, "unversioned-bucket").await;
    }

    #[tokio::test]
    async fn delete_bucket_blocks_retained_public_object_and_marker() {
        let db = setup().await;
        create(&db, "versioned-bucket", None).await.unwrap();
        set_versioning_state(&db, "versioned-bucket", BucketVersioningState::Enabled)
            .await
            .unwrap();
        insert_object(&db, "versioned-bucket", "retained-object", "key").await;
        crate::store::object::set_only_latest(&db, "versioned-bucket", "key", None)
            .await
            .unwrap();
        let now = Utc::now();
        object_version::Entity::insert(object_version::ActiveModel {
            id: Set("retained-version-row".to_owned()),
            bucket: Set("versioned-bucket".to_owned()),
            key: Set("key".to_owned()),
            version_id: Set(Some(uuid::Uuid::new_v4().to_string())),
            kind: Set("object".to_owned()),
            object_id: Set(Some("retained-object".to_owned())),
            sequence: Set(1),
            is_latest: Set(false),
            lifecycle_age_started_at: Set(now),
            became_noncurrent_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&db)
        .await
        .unwrap();

        assert_not_empty(&db, "versioned-bucket").await;
        object_version::Entity::delete_by_id("retained-version-row")
            .exec(&db)
            .await
            .unwrap();
        object::Entity::delete_by_id("retained-object")
            .exec(&db)
            .await
            .unwrap();

        object_version::Entity::insert(object_version::ActiveModel {
            id: Set("retained-marker-row".to_owned()),
            bucket: Set("versioned-bucket".to_owned()),
            key: Set("key".to_owned()),
            version_id: Set(Some(uuid::Uuid::new_v4().to_string())),
            kind: Set("delete_marker".to_owned()),
            object_id: Set(None),
            sequence: Set(2),
            is_latest: Set(true),
            lifecycle_age_started_at: Set(now),
            became_noncurrent_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&db)
        .await
        .unwrap();

        assert_not_empty(&db, "versioned-bucket").await;
        object_version::Entity::delete_by_id("retained-marker-row")
            .exec(&db)
            .await
            .unwrap();
        delete(&db, "versioned-bucket").await.unwrap();
        assert!(!exists(&db, "versioned-bucket").await.unwrap());
    }

    #[tokio::test]
    async fn delete_bucket_allows_legacy_hidden_nonlatest_cleanup() {
        let db = setup().await;
        create(&db, "legacy-bucket", None).await.unwrap();
        insert_object(&db, "legacy-bucket", "legacy-hidden", "key").await;
        crate::store::object::set_only_latest(&db, "legacy-bucket", "key", None)
            .await
            .unwrap();

        delete(&db, "legacy-bucket").await.unwrap();

        assert!(!exists(&db, "legacy-bucket").await.unwrap());
        assert_eq!(
            object::Entity::find()
                .filter(object::Column::Id.eq("legacy-hidden"))
                .count(&db)
                .await
                .unwrap(),
            0
        );
    }
}
