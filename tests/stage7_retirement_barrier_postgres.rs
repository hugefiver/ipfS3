//! Run only in the parent-registered Stage7 isolated schema; never creates,
//! drops, resets or repurposes an existing PostgreSQL scope.
#[path = "support/stage7_retirement_barrier.rs"]
mod support;
use ipfs_s3_gateway::{
    error::AppError,
    pinning::provider::canonical_resource_cid,
    store::{
        self,
        entities::{pin_provider_usage, remote_pin},
        pinning::{
            ledger,
            management::records,
            quota::{self, ReservationOutcome},
        },
    },
};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryOrder, Statement,
    TransactionTrait,
};
use std::time::Duration;

async fn backend_pid(db: &impl ConnectionTrait) -> i32 {
    db.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT pg_backend_pid() AS pid",
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "pid")
    .unwrap()
}

async fn wait_until_blocked_by(db: &DatabaseConnection, birth_pid: i32, retire_pid: i32) {
    loop {
        let blocked = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT $2 = ANY(pg_blocking_pids($1)) AS blocked",
                [birth_pid.into(), retire_pid.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<bool>("", "blocked")
            .unwrap();
        if blocked {
            return;
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
#[ignore = "requires parent-registered isolated Stage7 PostgreSQL scope"]
async fn postgres_birth_retire_both_orders_absent_first_insert_and_native_same_count_phantom() {
    tokio::time::timeout(Duration::from_secs(60), retirement_barrier())
        .await
        .expect("PostgreSQL retirement barrier test must finish without a lock wait cycle");
}

async fn retirement_barrier() {
    let url = std::env::var("IPFS_S3_STAGE7_RETIREMENT_DATABASE_URL")
        .expect("parent must select an isolated Stage7 PostgreSQL scope");
    let db = store::connect_database(&url)
        .await
        .unwrap_or_else(|_| panic!("isolated PostgreSQL unavailable"));
    assert_eq!(db.get_database_backend(), DatabaseBackend::Postgres);
    let schema = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT current_schema() AS scope",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "scope")
        .unwrap();
    assert!(
        schema.starts_with("stage7_"),
        "refuse non-Stage7 resource scope"
    );
    // No cleanup of old Unknown/shared state: require the selected fixture scope
    // to be empty before the normal registered migrator/bootstrap runs.
    let existing = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT to_regclass(current_schema() || '.pin_jobs')::text AS jobs",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<Option<String>>("", "jobs")
        .unwrap();
    if existing.is_some() {
        assert!(db.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT 1 AS occupied WHERE EXISTS(SELECT 1 FROM pin_jobs) OR EXISTS(SELECT 1 FROM remote_pins) OR EXISTS(SELECT 1 FROM buckets) OR EXISTS(SELECT 1 FROM pin_management_retirements)"))
            .await.unwrap().is_none(), "preserve prior scope; parent must select an empty isolated fixture");
    }
    let raw = support::foundation::config("http://127.0.0.1:1");
    let config = support::foundation::management_config(&raw);
    support::foundation::setup(&db, &config).await;
    let provider = config.pinning.providers[0].identity.allocation_key();
    let cid = canonical_resource_cid(support::foundation::CID).unwrap();
    let (auth, binding, _) =
        support::confirmed_retirement(&db, &raw, &config, "pg-before-birth").await;
    let birth = db.begin().await.unwrap();
    let now = store::database_clock::database_now(&birth).await.unwrap();
    assert_eq!(
        quota::reserve_unique(
            &birth,
            &provider,
            &cid,
            10,
            &config.pinning.provider_limits,
            now
        )
        .await
        .unwrap(),
        ReservationOutcome::Reserved
    );
    let retire = db.begin().await.unwrap();
    assert!(
        retire
            .execute_unprepared("LOCK TABLE pin_management_retirements IN EXCLUSIVE MODE NOWAIT")
            .await
            .is_err(),
        "birth's SHARE covers an empty retirement domain until commit"
    );
    retire.rollback().await.unwrap();
    birth.commit().await.unwrap();
    let retire = db.begin().await.unwrap();
    assert!(
        records::revalidate_confirmed_in_transaction(&retire, &config, &auth, &binding)
            .await
            .is_err(),
        "a committed first birth invalidates the original absence, not just its row count"
    );
    retire.rollback().await.unwrap();
    support::retire(&db, &raw, &config, "pg-after-birth").await;

    let mut other_raw = raw.clone();
    other_raw.pinning_identity.providers[0].scope = "empty-second-domain".into();
    other_raw.pinning_identity.providers[0].provider_id = "second-domain".into();
    let other = support::foundation::management_config(&other_raw);
    let key = other.pinning.providers[0].identity.allocation_key();
    ledger::register_route(&db, &key, &other.pinning.providers[0].identity)
        .await
        .unwrap();
    let (auth, binding, plan) =
        support::confirmed_retirement(&db, &other_raw, &other, "pg-retire-first").await;
    let retire = db.begin().await.unwrap();
    support::write_retirement(&retire, &other, &auth, &binding, &plan).await;
    let usage_before = pin_provider_usage::Entity::find()
        .order_by_asc(pin_provider_usage::Column::Provider)
        .all(&retire)
        .await
        .unwrap();
    let remotes_before = remote_pin::Entity::find()
        .order_by_asc(remote_pin::Column::Provider)
        .order_by_asc(remote_pin::Column::Cid)
        .all(&retire)
        .await
        .unwrap();
    assert!(!usage_before.iter().any(|row| row.provider == key));
    assert!(!remotes_before.iter().any(|row| row.provider == key));
    let retire_pid = backend_pid(&retire).await;
    let birth = db.begin().await.unwrap();
    let birth_pid = backend_pid(&birth).await;
    assert_ne!(birth_pid, retire_pid);
    {
        let reservation =
            quota::reserve_unique(&birth, &key, &cid, 10, &other.pinning.provider_limits, now);
        tokio::pin!(reservation);
        // The complete retirement fence may block the early usage write. Poll
        // both futures so commit never depends on that blocked write finishing.
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = &mut reservation => panic!("birth must wait for the retirement writer before admission: {result:?}"),
                () = wait_until_blocked_by(&db, birth_pid, retire_pid) => {},
            }
        }).await.expect("birth backend must demonstrably wait on the retirement backend");
        retire.commit().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(10), &mut reservation)
            .await
            .expect("birth must recheck admission after retirement commits");
        assert!(
            matches!(result, Err(AppError::InvalidPinningRequest(_))),
            "committed retirement must deny birth through admission, not a database timeout: {result:?}"
        );
    }
    birth.rollback().await.unwrap();
    assert_eq!(
        pin_provider_usage::Entity::find()
            .order_by_asc(pin_provider_usage::Column::Provider)
            .all(&db)
            .await
            .unwrap(),
        usage_before,
        "rejected birth must not leave usage rows or change accounting"
    );
    assert_eq!(
        remote_pin::Entity::find()
            .order_by_asc(remote_pin::Column::Provider)
            .order_by_asc(remote_pin::Column::Cid)
            .all(&db)
            .await
            .unwrap(),
        remotes_before,
        "rejected birth must not leave an orphan first remote"
    );
    let birth = db.begin().await.unwrap();
    assert!(
        quota::reserve_unique(&birth, &key, &cid, 10, &other.pinning.provider_limits, now)
            .await
            .is_err()
    );
    birth.rollback().await.unwrap();

    let mut third_raw = raw.clone();
    third_raw.pinning_identity.providers[0].scope = "same-count-domain".into();
    third_raw.pinning_identity.providers[0].provider_id = "same-count-provider".into();
    let third = support::foundation::management_config(&third_raw);
    let third_key = third.pinning.providers[0].identity.allocation_key();
    ledger::register_route(&db, &third_key, &third.pinning.providers[0].identity)
        .await
        .unwrap();
    let (auth, binding, _) =
        support::confirmed_retirement(&db, &third_raw, &third, "pg-same-count").await;
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE pin_provider_routes SET display_name='same-count-phantom' WHERE provider=$1",
        [third_key.into()],
    ))
    .await
    .unwrap();
    let retire = db.begin().await.unwrap();
    assert!(
        records::revalidate_confirmed_in_transaction(&retire, &third, &auth, &binding)
            .await
            .is_err()
    );
    retire.rollback().await.unwrap();
    db.close().await.unwrap();
}
#[path = "support/stage7_management.rs"]
pub mod foundation;
