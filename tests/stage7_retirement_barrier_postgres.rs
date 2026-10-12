//! Run only in the parent-registered Stage7 isolated schema; never creates,
//! drops, resets or repurposes an existing PostgreSQL scope.
#[path = "support/stage7_retirement_barrier.rs"]
mod support;
use ipfs_s3_gateway::{pinning::provider::canonical_resource_cid, store::{self, pinning::{ledger, management::records, quota::{self, ReservationOutcome}}}};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};

#[tokio::test]
#[ignore = "requires parent-registered isolated Stage7 PostgreSQL scope"]
async fn postgres_birth_retire_both_orders_absent_first_insert_and_native_same_count_phantom() {
    let url = std::env::var("IPFS_S3_STAGE7_RETIREMENT_DATABASE_URL").expect("parent must select an isolated Stage7 PostgreSQL scope");
    let db = store::connect_database(&url).await.unwrap_or_else(|_| panic!("isolated PostgreSQL unavailable"));
    assert_eq!(db.get_database_backend(), DatabaseBackend::Postgres);
    let schema = db.query_one(Statement::from_string(DatabaseBackend::Postgres, "SELECT current_schema() AS scope")).await.unwrap().unwrap()
        .try_get::<String>("", "scope").unwrap();
    assert!(schema.starts_with("stage7_"), "refuse non-Stage7 resource scope");
    // No cleanup of old Unknown/shared state: require the selected fixture scope
    // to be empty before the normal registered migrator/bootstrap runs.
    let existing = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT to_regclass(current_schema() || '.pin_jobs')::text AS jobs")).await.unwrap().unwrap().try_get::<Option<String>>("", "jobs").unwrap();
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
    let (auth, binding, _) = support::confirmed_retirement(&db, &raw, &config, "pg-before-birth").await;
    let birth = db.begin().await.unwrap();
    let now = store::database_clock::database_now(&birth).await.unwrap();
    assert_eq!(quota::reserve_unique(&birth, &provider, &cid, 10, &config.pinning.provider_limits, now).await.unwrap(), ReservationOutcome::Reserved);
    let retire = db.begin().await.unwrap();
    assert!(retire.execute_unprepared("LOCK TABLE pin_management_retirements IN EXCLUSIVE MODE NOWAIT").await.is_err(),
        "birth's SHARE covers an empty retirement domain until commit");
    retire.rollback().await.unwrap();
    birth.commit().await.unwrap();
    let retire = db.begin().await.unwrap();
    assert!(records::revalidate_confirmed_in_transaction(&retire, &config, &auth, &binding).await.is_err(),
        "a committed first birth invalidates the original absence, not just its row count");
    retire.rollback().await.unwrap();
    support::retire(&db, &raw, &config, "pg-after-birth").await;

    let mut other_raw = raw.clone();
    other_raw.pinning_identity.providers[0].scope = "empty-second-domain".into();
    other_raw.pinning_identity.providers[0].provider_id = "second-domain".into();
    let other = support::foundation::management_config(&other_raw);
    let key = other.pinning.providers[0].identity.allocation_key();
    ledger::register_route(&db, &key, &other.pinning.providers[0].identity).await.unwrap();
    let (auth, binding, plan) = support::confirmed_retirement(&db, &other_raw, &other, "pg-retire-first").await;
    let retire = db.begin().await.unwrap();
    support::write_retirement(&retire, &other, &auth, &binding, &plan).await;
    let birth = db.begin().await.unwrap();
    assert!(quota::reserve_unique(&birth, &key, &cid, 10, &other.pinning.provider_limits, now).await.is_err(),
        "retirement writer excludes the absent first remote insertion");
    birth.rollback().await.unwrap();
    retire.commit().await.unwrap();
    let birth = db.begin().await.unwrap();
    assert!(quota::reserve_unique(&birth, &key, &cid, 10, &other.pinning.provider_limits, now).await.is_err());
    birth.rollback().await.unwrap();

    let mut third_raw = raw.clone();
    third_raw.pinning_identity.providers[0].scope = "same-count-domain".into();
    third_raw.pinning_identity.providers[0].provider_id = "same-count-provider".into();
    let third = support::foundation::management_config(&third_raw);
    let third_key = third.pinning.providers[0].identity.allocation_key();
    ledger::register_route(&db, &third_key, &third.pinning.providers[0].identity).await.unwrap();
    let (auth, binding, _) = support::confirmed_retirement(&db, &third_raw, &third, "pg-same-count").await;
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "UPDATE pin_provider_routes SET display_name='same-count-phantom' WHERE provider=$1", [third_key.into()])).await.unwrap();
    let retire = db.begin().await.unwrap();
    assert!(records::revalidate_confirmed_in_transaction(&retire, &third, &auth, &binding).await.is_err());
    retire.rollback().await.unwrap();
    db.close().await.unwrap();
}
