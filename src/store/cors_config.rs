use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    QuerySelect, Set, TransactionError, TransactionTrait,
    sea_query::{Expr, OnConflict},
};

use crate::error::{AppError, AppResult};

use super::{
    database_clock::database_now,
    entities::{bucket, bucket_cors_config},
};

/// Returns the stored CORS configuration without checking whether the bucket exists.
pub async fn get_optional_configuration<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
) -> AppResult<Option<String>> {
    Ok(
        bucket_cors_config::Entity::find_by_id(bucket_name.to_owned())
            .one(db)
            .await?
            .map(|configuration| configuration.canonical_json),
    )
}

/// Atomically replaces a bucket's complete CORS configuration.
pub async fn put_configuration(
    db: &DatabaseConnection,
    bucket_name: &str,
    canonical_json: &str,
) -> AppResult<()> {
    let bucket_name = bucket_name.to_owned();
    let canonical_json = canonical_json.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            lock_bucket(txn, &bucket_name).await?;
            let now = database_now(txn).await?;
            let previous = bucket_cors_config::Entity::find_by_id(bucket_name.clone())
                .one(txn)
                .await?;
            before_write(WriteOperation::Put).await?;
            let model = bucket_cors_config::ActiveModel {
                bucket: Set(bucket_name),
                canonical_json: Set(canonical_json),
                created_at: Set(previous.map_or(now, |configuration| configuration.created_at)),
                updated_at: Set(now),
            };
            bucket_cors_config::Entity::insert(model)
                .on_conflict(
                    OnConflict::column(bucket_cors_config::Column::Bucket)
                        .update_columns([
                            bucket_cors_config::Column::CanonicalJson,
                            bucket_cors_config::Column::UpdatedAt,
                        ])
                        .to_owned(),
                )
                .exec(txn)
                .await?;
            Ok(())
        })
    })
    .await
    .map_err(normalize_transaction_error)
}

/// Physically removes a bucket's CORS configuration. Deleting an absent row succeeds.
pub async fn delete_configuration(db: &DatabaseConnection, bucket_name: &str) -> AppResult<()> {
    let bucket_name = bucket_name.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            lock_bucket(txn, &bucket_name).await?;
            database_now(txn).await?;
            before_write(WriteOperation::Delete).await?;
            bucket_cors_config::Entity::delete_by_id(bucket_name)
                .exec(txn)
                .await?;
            Ok(())
        })
    })
    .await
    .map_err(normalize_transaction_error)
}

#[derive(Clone, Copy)]
enum WriteOperation {
    Put,
    Delete,
}

async fn lock_bucket<C: ConnectionTrait>(db: &C, bucket_name: &str) -> AppResult<bucket::Model> {
    if db.get_database_backend() == DatabaseBackend::Postgres {
        return bucket::Entity::find_by_id(bucket_name.to_owned())
            .lock_exclusive()
            .one(db)
            .await?
            .ok_or_else(|| AppError::NoSuchBucket(bucket_name.to_owned()));
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
    bucket::Entity::find_by_id(bucket_name.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| AppError::NoSuchBucket(bucket_name.to_owned()))
}

async fn before_write(operation: WriteOperation) -> AppResult<()> {
    #[cfg(test)]
    if test_hooks::should_fail(operation) {
        return Err(AppError::Database(
            "injected CORS configuration write failure".to_owned(),
        ));
    }
    #[cfg(not(test))]
    let _ = operation;
    Ok(())
}

fn normalize_transaction_error(error: TransactionError<AppError>) -> AppError {
    match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => error.into(),
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::future::Future;

    #[derive(Clone, Copy, Eq, PartialEq)]
    pub enum WriteOperation {
        Put,
        Delete,
    }

    tokio::task_local! {
        static FAILURE: WriteOperation;
    }

    pub async fn with_failure<T>(operation: WriteOperation, future: impl Future<Output = T>) -> T {
        FAILURE.scope(operation, future).await
    }

    pub(super) fn should_fail(operation: super::WriteOperation) -> bool {
        let requested = match operation {
            super::WriteOperation::Put => WriteOperation::Put,
            super::WriteOperation::Delete => WriteOperation::Delete,
        };
        FAILURE
            .try_with(|failure| *failure == requested)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait};
    use tokio::sync::Barrier;

    use super::{delete_configuration, get_optional_configuration, put_configuration, test_hooks};
    use crate::{
        error::AppError,
        store::{connect_database, entities::bucket_cors_config, run_migrations},
    };

    const OLD: &str = r#"{"rules":[{"id":"old","origin":"https://old.example"}]}"#;
    const FIRST: &str = r#"{"rules":[{"id":"first","origin":"https://first.example"}]}"#;
    const SECOND: &str = r#"{"rules":[{"id":"second","origin":"https://second.example"}]}"#;
    const RACE_TIMEOUT: Duration = Duration::from_secs(10);

    struct SqliteFixture {
        _directory: tempfile::TempDir,
        url: String,
        db: DatabaseConnection,
    }

    async fn open_sqlite_connection(url: &str) -> DatabaseConnection {
        let db = connect_database(url).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        db
    }

    async fn fixture() -> SqliteFixture {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cors-config.db");
        let url = format!(
            "sqlite://{}?mode=rwc",
            path.display().to_string().replace('\\', "/")
        );
        let db = open_sqlite_connection(&url).await;
        run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", Some("owner"))
            .await
            .unwrap();
        SqliteFixture {
            _directory: directory,
            url,
            db,
        }
    }

    fn assert_snapshot(snapshot: Option<String>) {
        assert!(matches!(
            snapshot.as_deref(),
            None | Some(OLD) | Some(FIRST) | Some(SECOND)
        ));
        if let Some(snapshot) = snapshot {
            assert!(serde_json::from_str::<serde_json::Value>(&snapshot).is_ok());
        }
    }

    async fn observe_during_race(
        db: DatabaseConnection,
        start: std::sync::Arc<Barrier>,
    ) -> Vec<Option<String>> {
        start.wait().await;
        let mut snapshots = Vec::new();
        for _ in 0..16 {
            snapshots.push(get_optional_configuration(&db, "bucket").await.unwrap());
            tokio::task::yield_now().await;
        }
        snapshots
    }

    #[tokio::test]
    async fn optional_read_never_checks_bucket_existence() {
        let fixture = fixture().await;

        assert_eq!(
            get_optional_configuration(&fixture.db, "missing")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn put_replaces_the_complete_document_and_retains_created_at() {
        let fixture = fixture().await;
        put_configuration(&fixture.db, "bucket", OLD).await.unwrap();
        let first = bucket_cors_config::Entity::find_by_id("bucket")
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();

        put_configuration(&fixture.db, "bucket", FIRST)
            .await
            .unwrap();
        let second = bucket_cors_config::Entity::find_by_id("bucket")
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(second.canonical_json, FIRST);
        assert_eq!(second.created_at, first.created_at);
        assert!(second.updated_at >= first.updated_at);
    }

    #[tokio::test]
    async fn put_and_delete_missing_buckets_return_no_such_bucket() {
        let fixture = fixture().await;

        assert!(matches!(
            put_configuration(&fixture.db, "missing", FIRST).await,
            Err(AppError::NoSuchBucket(name)) if name == "missing"
        ));
        assert!(matches!(
            delete_configuration(&fixture.db, "missing").await,
            Err(AppError::NoSuchBucket(name)) if name == "missing"
        ));
    }

    #[tokio::test]
    async fn delete_is_physical_and_idempotent() {
        let fixture = fixture().await;
        put_configuration(&fixture.db, "bucket", OLD).await.unwrap();

        delete_configuration(&fixture.db, "bucket").await.unwrap();
        delete_configuration(&fixture.db, "bucket").await.unwrap();

        assert_eq!(
            get_optional_configuration(&fixture.db, "bucket")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            bucket_cors_config::Entity::find()
                .count(&fixture.db)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn injected_writes_roll_back_to_the_old_complete_document() {
        let fixture = fixture().await;
        put_configuration(&fixture.db, "bucket", OLD).await.unwrap();

        let put_result = test_hooks::with_failure(
            test_hooks::WriteOperation::Put,
            put_configuration(&fixture.db, "bucket", FIRST),
        )
        .await;
        assert!(matches!(put_result, Err(AppError::Database(_))));
        assert_eq!(
            get_optional_configuration(&fixture.db, "bucket")
                .await
                .unwrap()
                .as_deref(),
            Some(OLD)
        );

        let delete_result = test_hooks::with_failure(
            test_hooks::WriteOperation::Delete,
            delete_configuration(&fixture.db, "bucket"),
        )
        .await;
        assert!(matches!(delete_result, Err(AppError::Database(_))));
        assert_eq!(
            get_optional_configuration(&fixture.db, "bucket")
                .await
                .unwrap()
                .as_deref(),
            Some(OLD)
        );
    }

    #[tokio::test]
    async fn put_put_race_exposes_only_complete_json_snapshots() {
        let fixture = fixture().await;
        put_configuration(&fixture.db, "bucket", OLD).await.unwrap();
        let left_db = open_sqlite_connection(&fixture.url).await;
        let right_db = open_sqlite_connection(&fixture.url).await;
        let reader_db = open_sqlite_connection(&fixture.url).await;
        let start = std::sync::Arc::new(Barrier::new(3));

        let left_start = start.clone();
        let left = tokio::spawn(async move {
            left_start.wait().await;
            put_configuration(&left_db, "bucket", FIRST).await
        });
        let right_start = start.clone();
        let right = tokio::spawn(async move {
            right_start.wait().await;
            put_configuration(&right_db, "bucket", SECOND).await
        });
        let reader = tokio::spawn(observe_during_race(reader_db, start));

        let (left, right, mut snapshots) = tokio::time::timeout(RACE_TIMEOUT, async {
            (
                left.await.unwrap(),
                right.await.unwrap(),
                reader.await.unwrap(),
            )
        })
        .await
        .expect("put/put race completed within the bounded timeout");
        left.unwrap();
        right.unwrap();
        snapshots.push(
            get_optional_configuration(&fixture.db, "bucket")
                .await
                .unwrap(),
        );
        for snapshot in snapshots {
            assert_snapshot(snapshot);
        }
        assert!(matches!(
            get_optional_configuration(&fixture.db, "bucket")
                .await
                .unwrap()
                .as_deref(),
            Some(FIRST) | Some(SECOND)
        ));
    }

    #[tokio::test]
    async fn put_delete_race_exposes_only_complete_json_snapshots() {
        let fixture = fixture().await;
        put_configuration(&fixture.db, "bucket", OLD).await.unwrap();
        let put_db = open_sqlite_connection(&fixture.url).await;
        let delete_db = open_sqlite_connection(&fixture.url).await;
        let reader_db = open_sqlite_connection(&fixture.url).await;
        let start = std::sync::Arc::new(Barrier::new(3));

        let put_start = start.clone();
        let put = tokio::spawn(async move {
            put_start.wait().await;
            put_configuration(&put_db, "bucket", FIRST).await
        });
        let delete_start = start.clone();
        let delete = tokio::spawn(async move {
            delete_start.wait().await;
            delete_configuration(&delete_db, "bucket").await
        });
        let reader = tokio::spawn(observe_during_race(reader_db, start));

        let (put, delete, mut snapshots) = tokio::time::timeout(RACE_TIMEOUT, async {
            (
                put.await.unwrap(),
                delete.await.unwrap(),
                reader.await.unwrap(),
            )
        })
        .await
        .expect("put/delete race completed within the bounded timeout");
        put.unwrap();
        delete.unwrap();
        snapshots.push(
            get_optional_configuration(&fixture.db, "bucket")
                .await
                .unwrap(),
        );
        for snapshot in snapshots {
            assert_snapshot(snapshot);
        }
        assert!(matches!(
            get_optional_configuration(&fixture.db, "bucket")
                .await
                .unwrap()
                .as_deref(),
            None | Some(FIRST)
        ));
    }

    #[tokio::test]
    async fn delete_put_race_exposes_only_complete_json_snapshots() {
        let fixture = fixture().await;
        put_configuration(&fixture.db, "bucket", OLD).await.unwrap();
        let delete_db = open_sqlite_connection(&fixture.url).await;
        let put_db = open_sqlite_connection(&fixture.url).await;
        let reader_db = open_sqlite_connection(&fixture.url).await;
        let start = std::sync::Arc::new(Barrier::new(3));

        let delete_start = start.clone();
        let delete = tokio::spawn(async move {
            delete_start.wait().await;
            delete_configuration(&delete_db, "bucket").await
        });
        let put_start = start.clone();
        let put = tokio::spawn(async move {
            put_start.wait().await;
            put_configuration(&put_db, "bucket", FIRST).await
        });
        let reader = tokio::spawn(observe_during_race(reader_db, start));

        let (delete, put, mut snapshots) = tokio::time::timeout(RACE_TIMEOUT, async {
            (
                delete.await.unwrap(),
                put.await.unwrap(),
                reader.await.unwrap(),
            )
        })
        .await
        .expect("delete/put race completed within the bounded timeout");
        delete.unwrap();
        put.unwrap();
        snapshots.push(
            get_optional_configuration(&fixture.db, "bucket")
                .await
                .unwrap(),
        );
        for snapshot in snapshots {
            assert_snapshot(snapshot);
        }
        assert!(matches!(
            get_optional_configuration(&fixture.db, "bucket")
                .await
                .unwrap()
                .as_deref(),
            None | Some(FIRST)
        ));
    }
}
