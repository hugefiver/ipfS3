use super::*;

#[path = "../../../../../tests/support/baseline_postgres.rs"]
mod postgres;

async fn limit(db: &DatabaseConnection, bytes: Option<i64>) {
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE buckets SET quota_bytes=$1 WHERE name='bucket'",
        [bytes.into()],
    ))
    .await
    .unwrap();
}

async fn used(db: &DatabaseConnection) -> u64 {
    sql_count(db, "SELECT used_bytes n FROM buckets WHERE name='bucket'").await
}

async fn put(db: &DatabaseConnection, key: &str, size: i64) -> AppResult<PublicationResult> {
    publish_object(
        db,
        request(
            object(&uuid::Uuid::new_v4().to_string(), key, "bafy-shared", size),
            vec![],
            vec![],
        ),
        &limits(),
    )
    .await
}

async fn compete(first: &DatabaseConnection, second: &DatabaseConnection) {
    limit(first, Some(5)).await;
    let (a, b) = tokio::join!(put(first, "a", 5), put(second, "b", 5));
    assert!(matches!(
        (a, b),
        (Ok(_), Err(AppError::QuotaExceeded)) | (Err(AppError::QuotaExceeded), Ok(_))
    ));
    assert_eq!(used(first).await, 5);
    assert_eq!(
        object_version::Entity::find().count(first).await.unwrap(),
        1
    );
}

#[tokio::test]
async fn quota_follows_addressable_versions_across_suspension_and_exact_deletion() {
    let db = setup().await;
    put(&db, "key", 7).await.unwrap();
    limit(&db, Some(0)).await;
    put(&db, "key", 5).await.unwrap();
    put(&db, "key", 5).await.unwrap();
    assert!(matches!(
        put(&db, "new", 1).await,
        Err(AppError::QuotaExceeded)
    ));
    limit(&db, None).await;
    set_versioning(&db, BucketVersioningState::Enabled).await;
    let kept = put(&db, "key", 5).await.unwrap().version_id.unwrap();
    assert_eq!(used(&db).await, 10);
    guarded_delete(&db, "key", VersionSelector::Current, Utc::now())
        .await
        .unwrap();
    assert_eq!(used(&db).await, 10);
    set_versioning(&db, BucketVersioningState::Suspended).await;
    limit(&db, Some(10)).await;
    put(&db, "key", 4).await.unwrap();
    assert_eq!(used(&db).await, 9);
    guarded_delete(&db, "key", VersionSelector::Current, Utc::now())
        .await
        .unwrap();
    assert_eq!(used(&db).await, 5);
    guarded_delete(
        &db,
        "key",
        VersionSelector::Exact(PublicVersionId::Opaque(kept)),
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(used(&db).await, 0);
}

#[tokio::test]
async fn zip_settles_source_fresh_entries_and_replacements_together() {
    let db = setup().await;
    put(&db, "z", 10).await.unwrap();
    let batch = || ZipPublicationRequest {
        archive: request(
            object("archive", "archive.zip", "bafy-archive", 2),
            vec![],
            vec![],
        ),
        entries: vec![
            object("fresh", "a", "bafy-new", 7),
            object("replaced", "z", "bafy-old", 2),
        ],
    };
    limit(&db, Some(10)).await;
    assert!(matches!(
        zip_fixture::publish(&db, batch(), &limits()).await,
        Err(AppError::QuotaExceeded)
    ));
    assert_eq!(used(&db).await, 10);
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 1);
    assert_eq!(
        crate::store::object::get_latest(&db, "bucket", "z")
            .await
            .unwrap()
            .size,
        10
    );
    limit(&db, Some(11)).await;
    zip_fixture::publish(&db, batch(), &limits()).await.unwrap();
    assert_eq!(used(&db).await, 11);
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 3);
}

#[tokio::test]
async fn concurrent_gateways_cannot_both_take_the_last_bytes() {
    let (_dir, first, second) = setup_independent_file_backed("quota.sqlite").await;
    compete(&first, &second).await;
}

#[tokio::test]
#[ignore = "requires an isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_serializes_competing_bucket_writes() {
    postgres::with_schema(|db| async move {
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        compete(&db, &db).await;
    })
    .await;
}
