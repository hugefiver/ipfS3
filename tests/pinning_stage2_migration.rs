use chrono::Utc;
use ipfs_s3_gateway::{
    pinning::{
        config::{ProviderLimitMap, ProviderLimits},
        identity::{CleanupMode, ProviderIdentity},
    },
    store::{
        self,
        entities::{pin_job, remote_pin},
        migrations as m,
        pinning::{ledger, quota},
    },
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, EntityTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};

async fn migrate_prior_schema(db: &DatabaseConnection) {
    // Run the real predecessors in order, never create Stage 2 tables and drop
    // them to approximate an upgrade from the previous release.
    let predecessors: Vec<Box<dyn MigrationTrait>> = vec![
        Box::new(m::m20250701_000001_init::Migration),
        Box::new(m::m20260707_000001_decompress_zip::Migration),
        Box::new(m::m20260720_000001_sse_c_key_fingerprint::Migration),
        Box::new(m::m20260721_000001_multi_provider_pinning::Migration),
        Box::new(m::m20260729_000001_ipfs3_import::Migration),
        Box::new(m::m20260729_000002_postgres_utc_timestamps::Migration),
        Box::new(m::m20260730_000001_standard_mutation_fence::Migration),
        Box::new(m::m20260813_000001_postgres_json_columns::Migration),
        Box::new(m::m20260825_000001_object_versioning::Migration),
        Box::new(m::m20260826_000001_lifecycle_expiration::Migration),
        Box::new(m::m20260831_000001_bucket_cors::Migration),
        Box::new(m::m20260901_000001_lifecycle_abort_multipart::Migration),
        Box::new(m::m20260912_000001_residency_references::Migration),
        Box::new(m::m20260912_000002_lifecycle_transition::Migration),
        Box::new(m::m20260919_000001_standard_mutation_lease::Migration),
        Box::new(m::m20260920_000001_pin_submit_history::Migration),
    ];
    let manager = SchemaManager::new(db);
    for predecessor in predecessors {
        predecessor.up(&manager).await.unwrap();
    }
    assert!(!manager.has_table("remote_pin_ledger").await.unwrap());
}

fn limits(provider: &str) -> ProviderLimitMap {
    [(
        provider.to_owned(),
        ProviderLimits {
            priority: 1,
            max_bytes: 1000,
            max_pins: 10,
            enabled: true,
        },
    )]
    .into()
}

fn route_identity(provider_id: &str) -> ProviderIdentity {
    ProviderIdentity {
        provider_id: provider_id.into(),
        display_name: provider_id.into(),
        backend: "noop".into(),
        scope: "new-lifetime".into(),
        storage_domain: "remote".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        retired: false,
        cleanup: CleanupMode::Retain,
    }
}

async fn exercise_upgrade(db: &DatabaseConnection, with_occupied_remote: bool) {
    migrate_prior_schema(db).await;
    db.execute_unprepared("INSERT INTO remote_pins (provider,cid,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES ('old-alias','bafy-absent',50,'absent',4,0,'2026-09-19T00:00:00Z')").await.unwrap();
    db.execute_unprepared("INSERT INTO pin_provider_usage (provider,reserved_bytes,reserved_pins) VALUES ('old-alias',0,0)").await.unwrap();
    db.execute_unprepared("INSERT INTO pin_submit_history (job_id,api,strategy,effect,state,started_at) VALUES ('historical-submit','psa','cid','created','settled','2026-09-19T00:00:00Z')").await.unwrap();
    db.execute_unprepared("INSERT INTO pin_jobs (id,operation,provider,cid,expected_remote_epoch,state,next_attempt_at,submit_phase) VALUES ('old-unpin','unpin','old-alias','bafy-absent',4,'pending','2026-09-19T00:00:00Z',NULL)").await.unwrap();
    if with_occupied_remote {
        db.execute_unprepared("INSERT INTO remote_pins (provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES ('old-alias','bafy-occupied','old-request',75,'pinned',7,0,'2026-09-19T00:00:00Z')").await.unwrap();
    }

    m::m20260920_000002_pin_identity_ledger::Migration
        .up(&SchemaManager::new(db))
        .await
        .unwrap();
    let absent = ledger::get(db, "old-alias", "bafy-absent")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(absent.effect, "absent");
    assert_eq!(absent.ownership, "unknown");
    assert!(absent.route.is_none());
    assert!(
        !ledger::cleanup_allowed(db, "old-alias", "bafy-absent")
            .await
            .unwrap()
    );
    let job = pin_job::Entity::find_by_id("old-unpin")
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "running");
    assert!(job.locked_until.is_none());
    assert_eq!(
        job.last_error.as_deref(),
        Some("historical identity unavailable; needs_attention")
    );
    assert!(
        store::pinning::jobs::claim_due_jobs(db, Utc::now(), chrono::Duration::seconds(30), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store::pinning::jobs::submission_history(db, "historical-submit")
            .await
            .unwrap()
            .unwrap()
            .effect,
        "created"
    );
    let usage = quota::read_usage(db, "old-alias").await.unwrap().unwrap();
    assert_eq!(
        (usage.reserved_bytes, usage.reserved_pins),
        if with_occupied_remote {
            (75, 1)
        } else {
            (0, 0)
        }
    );

    let explicit_identity = route_identity("explicit");
    let explicit_provider = explicit_identity.allocation_key();
    ledger::register_route(db, &explicit_provider, &explicit_identity)
        .await
        .unwrap();

    if with_occupied_remote {
        let occupied = ledger::get(db, "old-alias", "bafy-occupied")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(occupied.effect, "unknown");
        assert_eq!(occupied.ownership, "unknown");
        assert!(occupied.route.is_none());
        assert!(
            quota::reserve_unique(
                db,
                &explicit_provider,
                "bafy-new",
                30,
                &limits(&explicit_provider),
                Utc::now()
            )
            .await
            .is_err()
        );
        assert!(
            remote_pin::Entity::find_by_id((explicit_provider.clone(), "bafy-new".to_owned()))
                .one(db)
                .await
                .unwrap()
                .is_none()
        );
        return;
    }

    let explicit = quota::reserve_unique(
        db,
        &explicit_provider,
        "bafy-new",
        30,
        &limits(&explicit_provider),
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(explicit, quota::ReservationOutcome::Reserved);
    // A new lifetime of the old CID may be allocated only after a current
    // route is registered; its historical lifetime never acquires that route.
    ledger::register_route(db, "old-alias", &route_identity("legacy:old-alias"))
        .await
        .unwrap();
    assert_eq!(
        quota::reserve_unique(
            db,
            "old-alias",
            "bafy-absent",
            50,
            &limits("old-alias"),
            Utc::now()
        )
        .await
        .unwrap(),
        quota::ReservationOutcome::Reserved
    );
    let current = ledger::get(db, "old-alias", "bafy-absent")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.effect, "reserved");
    assert_eq!(current.ownership, "unknown");
    assert!(current.route.is_some());
    assert_eq!(
        remote_pin::Entity::find_by_id(("old-alias".to_owned(), "bafy-absent".to_owned()))
            .one(db)
            .await
            .unwrap()
            .unwrap()
            .epoch,
        5
    );
}

#[tokio::test]
async fn sqlite_absent_history_does_not_block_new_allocation() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    exercise_upgrade(&db, false).await;
}

#[tokio::test]
async fn sqlite_occupied_history_still_quarantines_explicit_allocations() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    exercise_upgrade(&db, true).await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; uses only its own UUID schema"]
async fn postgres_absent_history_does_not_block_new_allocation() {
    postgres_upgrade(false).await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; uses only its own UUID schema"]
async fn postgres_occupied_history_still_quarantines_explicit_allocations() {
    postgres_upgrade(true).await;
}

async fn postgres_upgrade(with_occupied_remote: bool) {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("isolated test PostgreSQL URL");
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("stage2_migration_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut scoped = url::Url::parse(&url).unwrap();
    scoped
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let db = store::connect_database(scoped.as_str()).await.unwrap();
    let result = std::panic::AssertUnwindSafe(exercise_upgrade(&db, with_occupied_remote))
        .catch_unwind()
        .await;
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
