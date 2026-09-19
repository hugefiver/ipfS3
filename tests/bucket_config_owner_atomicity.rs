use std::{sync::Arc, time::Duration};

use chrono::Utc;
use ipfs_s3_gateway::{
    error::AppError,
    store::{
        self,
        entities::{bucket, bucket_cors_config, bucket_lifecycle_config},
    },
};
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DatabaseConnection, EntityTrait, Set, TransactionTrait,
};
use tokio::sync::Barrier;

const BUCKET: &str = "owner-atomicity";
const OLD_CORS: &str = r#"{"rules":[{"id":"old"}]}"#;
const NEW_CORS: &str = r#"{"rules":[{"id":"new"}]}"#;
const OLD_LIFECYCLE: &str = r#"{"rules":[{"id":"old"}]}"#;
const NEW_LIFECYCLE: &str = r#"{"rules":[{"id":"new"}]}"#;
const RACE_TIMEOUT: Duration = Duration::from_secs(10);

struct Fixture {
    _directory: tempfile::TempDir,
    url: String,
    db: DatabaseConnection,
}

async fn open_connection(url: &str) -> DatabaseConnection {
    let db = store::connect_database(url).await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    db
}

async fn fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bucket-config-owner.db");
    let url = format!(
        "sqlite://{}?mode=rwc",
        path.display().to_string().replace('\\', "/")
    );
    let db = open_connection(&url).await;
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, BUCKET, Some("old-owner"))
        .await
        .unwrap();
    Fixture {
        _directory: directory,
        url,
        db,
    }
}

async fn replace_bucket(
    txn: &sea_orm::DatabaseTransaction,
    owner: &str,
) -> Result<(), sea_orm::DbErr> {
    bucket::Entity::delete_by_id(BUCKET).exec(txn).await?;
    bucket::ActiveModel {
        name: Set(BUCKET.to_owned()),
        created_at: Set(Utc::now()),
        owner: Set(Some(owner.to_owned())),
        versioning_status: Set(None),
    }
    .insert(txn)
    .await?;
    Ok(())
}

fn assert_owner_mismatch<T>(result: Result<T, AppError>) {
    assert!(matches!(
        result,
        Err(AppError::AccessDenied(message)) if message == "expected bucket owner mismatch"
    ));
}

#[tokio::test]
async fn cors_stale_owner_write_waiting_on_bucket_lock_cannot_modify_recreated_bucket() {
    let fixture = fixture().await;
    store::cors_config::put_configuration(&fixture.db, BUCKET, OLD_CORS)
        .await
        .unwrap();

    let replacement = fixture.db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&replacement, BUCKET)
        .await
        .unwrap();

    let waiter_db = open_connection(&fixture.url).await;
    let started = Arc::new(Barrier::new(2));
    let waiter_started = started.clone();
    let stale_write = tokio::spawn(async move {
        waiter_started.wait().await;
        store::cors_config::put_configuration_for_owner(
            &waiter_db,
            BUCKET,
            NEW_CORS,
            Some("old-owner"),
        )
        .await
    });
    started.wait().await;

    replace_bucket(&replacement, "new-owner").await.unwrap();
    bucket_cors_config::ActiveModel {
        bucket: Set(BUCKET.to_owned()),
        canonical_json: Set(NEW_CORS.to_owned()),
        created_at: Set(Utc::now()),
        updated_at: Set(Utc::now()),
    }
    .insert(&replacement)
    .await
    .unwrap();
    replacement.commit().await.unwrap();

    let result = tokio::time::timeout(RACE_TIMEOUT, stale_write)
        .await
        .expect("stale CORS write must finish after replacement commits")
        .unwrap();
    assert_owner_mismatch(result);
    assert_eq!(
        store::cors_config::get_optional_configuration(&fixture.db, BUCKET)
            .await
            .unwrap()
            .as_deref(),
        Some(NEW_CORS)
    );
}

#[tokio::test]
async fn lifecycle_owner_and_configuration_read_share_one_snapshot_during_recreation() {
    let fixture = fixture().await;
    store::lifecycle_config::put_configuration(&fixture.db, BUCKET, OLD_LIFECYCLE)
        .await
        .unwrap();

    let replacement = fixture.db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&replacement, BUCKET)
        .await
        .unwrap();

    let reader_db = open_connection(&fixture.url).await;
    let started = Arc::new(Barrier::new(2));
    let reader_started = started.clone();
    let stale_read = tokio::spawn(async move {
        reader_started.wait().await;
        store::lifecycle_config::get_configuration_for_owner(&reader_db, BUCKET, Some("old-owner"))
            .await
    });
    started.wait().await;

    replace_bucket(&replacement, "new-owner").await.unwrap();
    bucket_lifecycle_config::ActiveModel {
        bucket: Set(BUCKET.to_owned()),
        canonical_json: Set(Some(NEW_LIFECYCLE.to_owned())),
        revision: Set(1),
        scan_cursor: Set(None),
        scan_lease_epoch: Set(0),
        scan_lease_until: Set(None),
        created_at: Set(Utc::now()),
        updated_at: Set(Utc::now()),
        last_scanned_at: Set(None),
    }
    .insert(&replacement)
    .await
    .unwrap();

    let result = tokio::time::timeout(RACE_TIMEOUT, stale_read)
        .await
        .expect("lifecycle snapshot read must not wait for an uncommitted replacement")
        .unwrap()
        .unwrap();
    assert_eq!(result, OLD_LIFECYCLE);

    replacement.commit().await.unwrap();
    assert_eq!(
        store::lifecycle_config::get_configuration(&fixture.db, BUCKET)
            .await
            .unwrap(),
        NEW_LIFECYCLE
    );
}

#[tokio::test]
async fn cors_owner_and_configuration_read_share_one_snapshot_during_recreation() {
    let fixture = fixture().await;
    store::cors_config::put_configuration(&fixture.db, BUCKET, OLD_CORS)
        .await
        .unwrap();

    let replacement = fixture.db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&replacement, BUCKET)
        .await
        .unwrap();

    let reader_db = open_connection(&fixture.url).await;
    let started = Arc::new(Barrier::new(2));
    let reader_started = started.clone();
    let read = tokio::spawn(async move {
        reader_started.wait().await;
        store::cors_config::get_optional_configuration_for_owner(
            &reader_db,
            BUCKET,
            Some("old-owner"),
        )
        .await
    });
    started.wait().await;

    replace_bucket(&replacement, "new-owner").await.unwrap();
    bucket_cors_config::ActiveModel {
        bucket: Set(BUCKET.to_owned()),
        canonical_json: Set(NEW_CORS.to_owned()),
        created_at: Set(Utc::now()),
        updated_at: Set(Utc::now()),
    }
    .insert(&replacement)
    .await
    .unwrap();

    let result = tokio::time::timeout(RACE_TIMEOUT, read)
        .await
        .expect("CORS snapshot read must not wait for an uncommitted replacement")
        .unwrap()
        .unwrap();
    assert_eq!(result.as_deref(), Some(OLD_CORS));

    replacement.commit().await.unwrap();
    assert_eq!(
        store::cors_config::get_optional_configuration(&fixture.db, BUCKET)
            .await
            .unwrap()
            .as_deref(),
        Some(NEW_CORS)
    );
}

#[tokio::test]
async fn lifecycle_stale_owner_delete_waiting_on_bucket_lock_cannot_modify_recreated_bucket() {
    let fixture = fixture().await;
    store::lifecycle_config::put_configuration(&fixture.db, BUCKET, OLD_LIFECYCLE)
        .await
        .unwrap();

    let replacement = fixture.db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&replacement, BUCKET)
        .await
        .unwrap();

    let waiter_db = open_connection(&fixture.url).await;
    let started = Arc::new(Barrier::new(2));
    let waiter_started = started.clone();
    let stale_delete = tokio::spawn(async move {
        waiter_started.wait().await;
        store::lifecycle_config::delete_configuration_for_owner(
            &waiter_db,
            BUCKET,
            Some("old-owner"),
        )
        .await
    });
    started.wait().await;

    replace_bucket(&replacement, "new-owner").await.unwrap();
    bucket_lifecycle_config::ActiveModel {
        bucket: Set(BUCKET.to_owned()),
        canonical_json: Set(Some(NEW_LIFECYCLE.to_owned())),
        revision: Set(1),
        scan_cursor: Set(None),
        scan_lease_epoch: Set(0),
        scan_lease_until: Set(None),
        created_at: Set(Utc::now()),
        updated_at: Set(Utc::now()),
        last_scanned_at: Set(None),
    }
    .insert(&replacement)
    .await
    .unwrap();
    replacement.commit().await.unwrap();

    let result = tokio::time::timeout(RACE_TIMEOUT, stale_delete)
        .await
        .expect("stale lifecycle delete must finish after replacement commits")
        .unwrap();
    assert_owner_mismatch(result);
    assert_eq!(
        store::lifecycle_config::get_configuration(&fixture.db, BUCKET)
            .await
            .unwrap(),
        NEW_LIFECYCLE
    );
}

#[tokio::test]
async fn owner_aware_configuration_apis_preserve_owner_and_missing_bucket_semantics() {
    let fixture = fixture().await;

    store::cors_config::put_configuration_for_owner(
        &fixture.db,
        BUCKET,
        OLD_CORS,
        Some("old-owner"),
    )
    .await
    .unwrap();
    assert_eq!(
        store::cors_config::get_optional_configuration_for_owner(
            &fixture.db,
            BUCKET,
            Some("old-owner"),
        )
        .await
        .unwrap()
        .as_deref(),
        Some(OLD_CORS)
    );
    assert_owner_mismatch(
        store::cors_config::delete_configuration_for_owner(
            &fixture.db,
            BUCKET,
            Some("other-owner"),
        )
        .await,
    );

    store::lifecycle_config::put_configuration_for_owner(&fixture.db, BUCKET, OLD_LIFECYCLE, None)
        .await
        .unwrap();
    assert_owner_mismatch(
        store::lifecycle_config::get_configuration_for_owner(
            &fixture.db,
            BUCKET,
            Some("other-owner"),
        )
        .await,
    );

    assert!(matches!(
        store::cors_config::get_optional_configuration_for_owner(
            &fixture.db,
            "missing",
            Some("old-owner"),
        )
        .await,
        Err(AppError::NoSuchBucket(name)) if name == "missing"
    ));
    assert!(matches!(
        store::lifecycle_config::delete_configuration_for_owner(
            &fixture.db,
            "missing",
            None,
        )
        .await,
        Err(AppError::NoSuchBucket(name)) if name == "missing"
    ));
}
