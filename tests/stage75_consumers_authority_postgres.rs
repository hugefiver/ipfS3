//! Read-only ordinary-consumer checks against a parent-owned initialized PG scope.
use ipfs_s3_gateway::{
    pinning::identity::ProviderRouteSnapshot,
    store::{
        entities::pin_provider_route,
        pinning::management::{
            native::{self, NativeKey, meter::Budget},
            records::locator::{self, AuthoritySelector},
        },
    },
};
use sea_orm::{
    AccessMode, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, EntityTrait,
    IsolationLevel, Statement, TransactionTrait,
};

#[tokio::test]
#[ignore = "requires initialized parent-owned exclusive IPFS_S3_STAGE7_APPLY_DATABASE_URL"]
async fn registered_route_and_domain_read_only_closures_preserve_original_seals() {
    let url = std::env::var("IPFS_S3_STAGE7_APPLY_DATABASE_URL").unwrap();
    let mut options = ConnectOptions::new(url);
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    let tx = db
        .begin_with_config(
            Some(IsolationLevel::RepeatableRead),
            Some(AccessMode::ReadOnly),
        )
        .await
        .unwrap();
    let scope = tx.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT current_schema()::text AS schema, current_schemas(false)=ARRAY[current_schema()::name] AS exclusive, nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AS owned FROM pg_namespace WHERE oid=current_schema()::regnamespace"))
        .await.unwrap().unwrap();
    assert!(
        scope
            .try_get::<String>("", "schema")
            .unwrap()
            .starts_with("stage7_")
    );
    assert!(scope.try_get::<bool>("", "exclusive").unwrap());
    assert!(scope.try_get::<bool>("", "owned").unwrap());
    let registered = pin_provider_route::Entity::find()
        .one(&tx)
        .await
        .unwrap()
        .expect("the initialized fixture contains a formally registered route");
    let route: ProviderRouteSnapshot = serde_json::from_str(&registered.snapshot).unwrap();
    let source = NativeKey {
        table: "pin_provider_routes".into(),
        values: vec![registered.provider.clone()],
    };
    for selector in [
        AuthoritySelector::RegisteredRoute(registered.provider.clone()),
        AuthoritySelector::RegisteredDomain(locator::canonical_domain(&route).unwrap()),
    ] {
        let view = locator::discover_authority(&tx, &[selector], &mut Budget::default())
            .await
            .unwrap();
        assert_eq!(view.originals.len(), 1);
        let original = view.originals.get(&source).unwrap();
        assert_eq!(
            native::value(original, "provider"),
            Some(registered.provider.as_str())
        );
        let original_route: ProviderRouteSnapshot =
            serde_json::from_str(native::value(original, "snapshot").unwrap()).unwrap();
        assert!(original_route == route);
        assert_eq!(view.proof.seals.len(), 1);
        assert_eq!(view.proof.seals[0].source, source);
    }
    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires initialized parent-owned exclusive IPFS_S3_STAGE7_APPLY_DATABASE_URL"]
async fn authority_fence_reports_lock_contention_without_losing_reusability() {
    use ipfs_s3_gateway::store::pinning::management::ManagementError;
    use std::time::{Duration, Instant};

    let url = std::env::var("IPFS_S3_STAGE7_APPLY_DATABASE_URL").unwrap();
    let mut options = ConnectOptions::new(url);
    options
        .max_connections(2)
        .min_connections(2)
        .sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    let holder = db.begin().await.unwrap();
    let scope = holder.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT current_schema()::text AS schema, current_schemas(false)=ARRAY[current_schema()::name] AS exclusive, nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AS owned FROM pg_namespace WHERE oid=current_schema()::regnamespace"))
        .await.unwrap().unwrap();
    assert!(
        scope
            .try_get::<String>("", "schema")
            .unwrap()
            .starts_with("stage7_")
    );
    assert!(scope.try_get::<bool>("", "exclusive").unwrap());
    assert!(scope.try_get::<bool>("", "owned").unwrap());
    holder
        .execute_unprepared("SET LOCAL lock_timeout='1s'")
        .await
        .unwrap();
    // The maintenance lock remains compatible with ordinary reads and writes,
    // but not with the final authority membership barrier.
    holder
        .execute_unprepared(
            "LOCK TABLE pin_management_pending_scope IN SHARE UPDATE EXCLUSIVE MODE",
        )
        .await
        .unwrap();
    let held = holder.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT COUNT(*) AS n FROM pg_locks WHERE pid=pg_backend_pid() AND relation='pin_management_pending_scope'::regclass AND mode='ShareUpdateExclusiveLock' AND granted"))
        .await.unwrap().unwrap();
    assert_eq!(held.try_get::<i64>("", "n").unwrap(), 1);

    let contender = db.begin().await.unwrap();
    let started = Instant::now();
    let result = locator::fence_authority(&contender).await;
    let elapsed = started.elapsed();
    contender.rollback().await.unwrap();
    holder.rollback().await.unwrap();
    assert_eq!(result, Err(ManagementError::Changed));
    assert!(elapsed < Duration::from_secs(3));

    let retry = db.begin().await.unwrap();
    locator::fence_authority(&retry).await.unwrap();
    retry.rollback().await.unwrap();
    db.close().await.unwrap();
}
