//! Coordinator-owned real PostgreSQL check; no shared Docker lifecycle changes.
use futures_util::FutureExt;
use ipfs_s3_gateway::{
    error::AppError,
    import::SupersedeReason,
    store::{
        self,
        entities::{bucket, standard_mutation_lease},
        import::ownership::*,
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, Database, DatabaseBackend, EntityTrait, QueryFilter, Statement,
    TransactionTrait,
};

async fn isolated_pg<F, Fut>(work: F)
where
    F: FnOnce(sea_orm::DatabaseConnection, sea_orm::DatabaseConnection) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("set IPFS_S3_TEST_POSTGRES_URL");
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("b1_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut scoped = url::Url::parse(&url).unwrap();
    scoped
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let first = Database::connect(scoped.as_str()).await.unwrap();
    let mut options = sea_orm::ConnectOptions::new(scoped.to_string());
    options.max_connections(1).min_connections(1);
    let second = Database::connect(options).await.unwrap();
    store::run_migrations(&first).await.unwrap();
    let result = std::panic::AssertUnwindSafe(work(first.clone(), second.clone()))
        .catch_unwind()
        .await;
    second.close().await.unwrap();
    first.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore = "requires coordinator-provided IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_mutation_lease_rechecks_after_bucket_lock_and_recovers_across_instances() {
    isolated_pg(|first, second| async move {
    let name = format!("b1-{}", uuid::Uuid::new_v4());
    store::bucket::create(&first, &name, None).await.unwrap();
    let guard = admit_content_and_prefix_mutation(
        &first,
        &name,
        "archive",
        "out%_/",
        SupersedeReason::DecompressZip,
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    renew_standard_mutation(&second, &guard).await.unwrap();
    assert!(
        try_admit_lifecycle_mutation(&second, &name, "out%_/key", "action", 1, chrono::Utc::now())
            .await
            .unwrap()
            .is_none()
    );

    let pid: i32 = second
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT pg_backend_pid() AS pid",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "pid")
        .unwrap();
    let lock = first.begin().await.unwrap();
    lock_bucket_for_ownership(&lock, &name).await.unwrap();
    let waiter_db = second.clone();
    let old = guard.clone();
    let waiter = tokio::spawn(async move { renew_standard_mutation(&waiter_db, &old).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let row = first.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
                "SELECT wait_event_type = 'Lock' AS blocked FROM pg_stat_activity WHERE pid = $1", [pid.into()])).await.unwrap().unwrap();
            if row.try_get::<Option<bool>>("", "blocked").unwrap() == Some(true) { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("second instance must actually wait on bucket lock");
    lock.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "UPDATE standard_mutation_leases SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE bucket = $1", [name.clone().into()])).await.unwrap();
    lock.commit().await.unwrap();
    assert!(matches!(
        waiter.await.unwrap(),
        Err(AppError::StaleContentMutation)
    ));
    assert!(
        try_admit_lifecycle_mutation(&second, &name, "out%_/key", "action", 1, chrono::Utc::now())
            .await
            .unwrap()
            .is_some()
    );
    let replacement = admit_content_mutation(
        &second,
        &name,
        "archive",
        None,
        SupersedeReason::PutObject,
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    assert!(!release_standard_mutation(&first, &guard).await.unwrap());
    renew_standard_mutation(&first, &replacement).await.unwrap();
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .filter(standard_mutation_lease::Column::Bucket.eq(&name))
            .filter(standard_mutation_lease::Column::Key.eq("archive"))
            .one(&first)
            .await
            .unwrap()
            .unwrap()
            .mutation_id,
        replacement.mutation_id
    );
    bucket::Entity::delete_by_id(name)
        .exec(&first)
        .await
        .unwrap();
    }).await;
}

#[tokio::test]
#[ignore = "requires coordinator-provided IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_versioning_does_not_upgrade_bucket_lock_behind_scanner_fk() {
    isolated_pg(|first, second| async move {
        use ipfs_s3_gateway::lifecycle::model::*;
        use ipfs_s3_gateway::store::object_version::{PublicVersionId, VersionKind};
        store::bucket::create(&first, "bucket", None).await.unwrap();
        first.execute_unprepared("INSERT INTO bucket_lifecycle_configs (bucket, revision, scan_lease_epoch, created_at, updated_at) VALUES ('bucket', 1, 0, clock_timestamp(), clock_timestamp())").await.unwrap();
        let owner = first.begin().await.unwrap();
        lock_bucket_for_ownership(&owner, "bucket").await.unwrap();
        owner.execute_unprepared("UPDATE bucket_lifecycle_configs SET revision = revision WHERE bucket = 'bucket'").await.unwrap();
        let scanner = second.begin().await.unwrap();
        let mut action = NewLifecycleAction {
            idempotency_key: String::new(), bucket: "bucket".into(), config_revision: 1,
            rule_identity: RuleIdentity::Id("scan".into()), action_kind: LifecycleActionKind::ExpireCurrent,
            target: LifecycleTargetIdentity::Version(VersionTargetIdentity { bucket: "bucket".into(), key: "key".into(), version_row_id: "version".into(), public_version_id: PublicVersionId::Null, object_id: Some("object".into()), sequence: 1, kind: VersionKind::Object }),
            due_at: chrono::Utc::now(),
        };
        action.idempotency_key = store::lifecycle_action::idempotency_key(&action).unwrap();
        store::lifecycle_action::insert_idempotent(&scanner, action, chrono::Utc::now()).await.unwrap();
        let pid: i32 = scanner.query_one(Statement::from_string(DatabaseBackend::Postgres, "SELECT pg_backend_pid() AS pid")).await.unwrap().unwrap().try_get("", "pid").unwrap();
        let scan_task = tokio::spawn(async move {
            let result = scanner.execute_unprepared("UPDATE bucket_lifecycle_configs SET scan_cursor = 'finished' WHERE bucket = 'bucket'").await;
            scanner.commit().await.unwrap();
            result
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let blocked: bool = first.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres, "SELECT wait_event_type = 'Lock' AS blocked FROM pg_stat_activity WHERE pid = $1", [pid.into()])).await.unwrap().unwrap().try_get::<Option<bool>>("", "blocked").unwrap().unwrap_or(false);
                if blocked { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        // Both prerequisites of the reported cycle are now observed: the real
        // action FK holds KEY SHARE, and scan waits on the owner's config row.
        owner.execute_unprepared("SET LOCAL lock_timeout = '250ms'").await.unwrap();
        let versioning = store::bucket::lock_versioning_state(&owner, "bucket").await;
        owner.rollback().await.unwrap();
        let scan_result = scan_task.await.unwrap();
        assert!(versioning.is_ok(), "versioning must not upgrade to UPDATE or depend on a deadlock retry: {versioning:?}");
        scan_result.unwrap();
    }).await;
}
