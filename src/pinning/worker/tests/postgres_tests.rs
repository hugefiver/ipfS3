use std::{future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use chrono::Utc;
use futures_util::FutureExt;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend,
    DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, TransactionTrait,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::{Fixture, ScriptProvider};
use crate::pinning::worker;
use crate::{
    config::{PinningConfig, ProviderConfig},
    pinning::{config::ValidatedPinningConfig, coordinator::PinningCoordinator},
    store::{
        Store,
        entities::{pin_job, pin_lease_target, pin_provider_usage, remote_pin, remote_pin_ledger},
        pinning::{jobs, ledger},
    },
};

async fn isolated<F, Fut>(body: F)
where
    F: FnOnce(Fixture) -> Fut,
    Fut: Future<Output = ()>,
{
    with_postgres_fixture(|fixture| async move {
        tokio::time::timeout(Duration::from_secs(5), body(fixture))
            .await
            .expect("worker writer did not settle");
    })
    .await;
}

async fn with_postgres_fixture<F, Fut>(body: F)
where
    F: FnOnce(Fixture) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("explicit PostgreSQL tests require IPFS_S3_TEST_POSTGRES_URL");
    let mut control_options = ConnectOptions::new(url.clone());
    control_options.min_connections(1).max_connections(1);
    let control = Database::connect(control_options)
        .await
        .unwrap_or_else(|_| panic!("authorized test PostgreSQL must be reachable"));
    assert_eq!(control.get_database_backend(), DatabaseBackend::Postgres);
    let schema = format!("worker_fences_{}", uuid::Uuid::new_v4().simple());
    control
        .execute_unprepared(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();

    let mut scoped_db = None;
    let outcome = AssertUnwindSafe(async {
        let mut options = ConnectOptions::new(url);
        options.min_connections(1).max_connections(4);
        options.set_schema_search_path(schema.clone());
        let db = Database::connect(options)
            .await
            .unwrap_or_else(|_| panic!("worker test schema connection failed"));
        scoped_db = Some(db.clone());
        let fixture = fixture(db).await;
        // All worker futures are awaited inline: cancellation leaves no detached
        // worker handles, and the pool is drained before schema cleanup.
        body(fixture).await;
    })
    .catch_unwind()
    .await;

    let closed = match scoped_db {
        Some(db) => db.close().await,
        None => Ok(()),
    };
    assert!(closed.is_ok(), "worker test pool did not close");
    let dropped = control
        .execute_unprepared(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await;
    let control_closed = control.close().await;
    assert!(dropped.is_ok(), "owned worker test schema cleanup failed");
    assert!(control_closed.is_ok(), "worker control pool did not close");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn fixture(db: DatabaseConnection) -> Fixture {
    crate::store::run_migrations(&db).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO buckets (name) VALUES ('bucket');
         INSERT INTO objects (id,bucket,key,cid,size,etag)
         VALUES ('object-1','bucket','key','bafy-worker',100,'bafy-worker');
         INSERT INTO pin_leases
         (id,owner_object_id,source,policy_id,provider_mode,content_mode,
          created_at,last_touched_at,expires_at,generation,state)
         VALUES ('lease-1','object-1','test','policy','all','full',
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP + INTERVAL '1 hour',1,'active');
         INSERT INTO pin_lease_targets
         (id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at)
         VALUES ('target-1','lease-1','bafy-worker',100,'noop','waiting',
                 CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         INSERT INTO remote_pins
         (provider,cid,cid_size,status,epoch,failure_attempts,last_touched_at)
         VALUES ('noop','bafy-worker',100,'reserved',1,0,CURRENT_TIMESTAMP);
         INSERT INTO pin_provider_usage (provider,reserved_bytes,reserved_pins)
         VALUES ('noop',100,1)",
    )
    .await
    .unwrap();
    let raw = PinningConfig {
        providers: vec![ProviderConfig {
            name: "noop".to_owned(),
            kind: "noop".to_owned(),
            token_env: None,
            endpoint: None,
            api: None,
            strategy: None,
            upload_endpoint: None,
            cluster: None,
            enabled: true,
            priority: 1,
            max_bytes: 10_000,
            max_pins: 100,
            requests_per_second: None,
        }],
        ..Default::default()
    };
    let validated = ValidatedPinningConfig::from_raw(&raw, |_| None).unwrap();
    let mut coordinator = PinningCoordinator::build(validated).unwrap();
    let mut provider = ScriptProvider::new([]);
    Arc::get_mut(&mut provider).unwrap().route = ("noop", "cid");
    PinningCoordinator::replace_provider_for_test(&mut coordinator, "noop", provider.clone());
    Fixture {
        store: Store::new(db),
        coordinator,
        provider,
        _database_directory: None,
    }
}

async fn job(fixture: &Fixture, id: &str) -> pin_job::Model {
    pin_job::Entity::find_by_id(id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap()
}

async fn claim_one(fixture: &Fixture) -> jobs::ClaimedPinJob {
    let mut claims = jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap();
    assert_eq!(claims.len(), 1);
    claims.pop().unwrap()
}

async fn reclaim(fixture: &Fixture, claimed: &jobs::ClaimedPinJob) -> jobs::ClaimedPinJob {
    let mut claims = jobs::claim_due_jobs(
        fixture.store.db(),
        claimed.model.locked_until.unwrap() + chrono::Duration::seconds(1),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap();
    assert_eq!(claims.len(), 1);
    let successor = claims.pop().unwrap();
    assert_eq!(successor.model.id, claimed.model.id);
    assert_ne!(successor.model.locked_until, claimed.model.locked_until);
    successor
}

async fn resource_state(
    fixture: &Fixture,
) -> (
    remote_pin::Model,
    pin_provider_usage::Model,
    pin_lease_target::Model,
) {
    let db = fixture.store.db();
    (
        remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        pin_provider_usage::Entity::find_by_id("noop")
            .one(db)
            .await
            .unwrap()
            .unwrap(),
        pin_lease_target::Entity::find_by_id("target-1")
            .one(db)
            .await
            .unwrap()
            .unwrap(),
    )
}

fn assert_no_provider_calls(fixture: &Fixture) {
    use std::sync::atomic::Ordering::SeqCst;
    assert_eq!(fixture.provider.submits.load(SeqCst), 0);
    assert_eq!(fixture.provider.gets.load(SeqCst), 0);
    assert_eq!(fixture.provider.finds.load(SeqCst), 0);
    assert_eq!(fixture.provider.unpins.load(SeqCst), 0);
}

async fn enqueue_reconcile(fixture: &Fixture) -> jobs::ClaimedPinJob {
    jobs::enqueue_job(
        fixture.store.db(),
        jobs::reconcile_job("noop", "bafy-worker", 1, Utc::now()),
    )
    .await
    .unwrap();
    claim_one(fixture).await
}

async fn current_reconciles(fixture: &Fixture, epoch: i64) -> Vec<pin_job::Model> {
    pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("reconcile"))
        .filter(pin_job::Column::ExpectedRemoteEpoch.eq(epoch))
        .all(fixture.store.db())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_identity_park_rechecks_restored_route_and_exact_claim() {
    isolated(|fixture| async move {
        fixture.enqueue_poll("identity-request").await;
        let claimed = claim_one(&fixture).await;
        let db = fixture.store.db();
        let deny = "INSERT INTO remote_pin_ledger(provider,cid,ownership,effect)
                    VALUES ('noop','bafy-worker','unknown','confirmed')";
        db.execute_unprepared(deny).await.unwrap();
        let observation = worker::admission::worker_observation::observe_claimed_job(
            db,
            &claimed.model,
            fixture.coordinator.provider_identity("noop"),
            worker::SharedDecodeBudget::default(),
        )
        .await
        .unwrap();
        assert_eq!(observation.ordinary, ledger::JobAdmission::RouteDenied);
        db.execute_unprepared(
            "DELETE FROM remote_pin_ledger WHERE provider='noop' AND cid='bafy-worker'",
        )
        .await
        .unwrap();
        let before = resource_state(&fixture).await;
        assert!(
            !worker::park_identity_if_current(
                &fixture.store,
                &fixture.coordinator,
                &claimed,
                worker::SharedDecodeBudget::default(),
            )
            .await
            .unwrap()
        );
        let restored = job(&fixture, &claimed.model.id).await;
        assert_eq!(restored, claimed.model);

        db.execute_unprepared(deny).await.unwrap();
        let successor = reclaim(&fixture, &claimed).await;
        let ledger_before = ledger_row(&fixture).await;
        assert!(
            worker::park_identity_if_current(
                &fixture.store,
                &fixture.coordinator,
                &claimed,
                worker::SharedDecodeBudget::default(),
            )
            .await
            .unwrap()
        );
        assert_eq!(job(&fixture, &claimed.model.id).await, successor.model);
        assert_eq!(ledger_row(&fixture).await, ledger_before);
        assert_eq!(resource_state(&fixture).await, before);
        assert_no_provider_calls(&fixture);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_missing_provider_worker_retries_and_finishes_ordinary_jobs_without_quota_drift() {
    with_postgres_fixture(|fixture| async move {
        fixture.enqueue_poll("unavailable-request").await;
        let db = fixture.store.db();
        for new_job in [
            jobs::unpin_job("noop", "bafy-worker", 1, Utc::now()),
            jobs::reconcile_job("noop", "bafy-worker", 1, Utc::now()),
        ] {
            jobs::enqueue_job(db, new_job).await.unwrap();
        }
        let before = resource_state(&fixture).await;
        assert_eq!((before.1.reserved_bytes, before.1.reserved_pins), (100, 1));
        let mut missing = PinningCoordinator::disabled_for_test();
        PinningCoordinator::configure_worker_for_test(&mut missing, |settings| {
            settings.max_attempts = 2;
        });
        let global = Arc::new(Semaphore::new(3));
        for pass in 1..=3 {
            let rows = pin_job::Entity::find().all(db).await.unwrap();
            assert_eq!(rows.len(), 3);
            let due = rows.iter().map(|row| row.next_attempt_at).max().unwrap();
            let claims = jobs::claim_due_jobs(db, due, chrono::Duration::seconds(30), 3)
                .await
                .unwrap();
            assert_eq!(claims.len(), 3);
            for claimed in claims {
                let id = claimed.model.id.clone();
                tokio::time::timeout(
                    Duration::from_secs(5),
                    worker::execute_claimed_job(&fixture.store, &missing, &global, claimed),
                )
                .await
                .expect("worker dispatch did not settle")
                .unwrap();
                let row = job(&fixture, &id).await;
                assert_eq!(row.state, if pass < 3 { "pending" } else { "done" });
                assert_eq!(row.attempts, pass.min(2));
                assert_eq!(row.locked_until, None);
            }
            assert_eq!(resource_state(&fixture).await, before);
        }
        assert_no_provider_calls(&fixture);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_missing_provider_submit_keeps_durable_recovery_and_reservation() {
    isolated(|fixture| async move {
        let seed = fixture.store.db().begin().await.unwrap();
        jobs::enqueue_job(
            &seed,
            jobs::submit_job("noop", "bafy-worker", "lease-1", "target-1", 1, Utc::now()),
        )
        .await
        .unwrap();
        seed.execute_unprepared("INSERT INTO pin_submit_history (job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) SELECT id,'psa','cid','unknown','active',0,0,created_at FROM pin_jobs WHERE operation='submit' ON CONFLICT(job_id) DO NOTHING").await.unwrap();
        seed.commit().await.unwrap();
        let claimed = claim_one(&fixture).await;
        let before = resource_state(&fixture).await;
        let history = jobs::submission_history(fixture.store.db(), &claimed.model.id)
            .await
            .unwrap();
        worker::execute_claimed_job(
            &fixture.store,
            &PinningCoordinator::disabled_for_test(),
            &Arc::new(Semaphore::new(1)),
            claimed.clone(),
        )
        .await
        .unwrap();
        let row = job(&fixture, &claimed.model.id).await;
        assert_eq!(row.state, "pending");
        assert_eq!(row.submit_phase.as_deref(), Some("recovering"));
        assert_eq!(row.attempts, 1);
        assert_eq!(row.locked_until, None);
        assert!(row.next_attempt_at > claimed.model.next_attempt_at);
        assert_eq!(
            jobs::submission_history(fixture.store.db(), &row.id)
                .await
                .unwrap(),
            history
        );
        assert_eq!(resource_state(&fixture).await, before);
        assert_no_provider_calls(&fixture);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_unavailable_provider_rechecks_claim_and_current_remote_epoch() {
    isolated(|fixture| async move {
        let claimed = enqueue_reconcile(&fixture).await;
        let successor = reclaim(&fixture, &claimed).await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET epoch=2 WHERE provider='noop' AND cid='bafy-worker'",
            )
            .await
            .unwrap();
        let before = resource_state(&fixture).await;
        let missing = PinningCoordinator::disabled_for_test();
        worker::handle_unavailable_provider(&fixture.store, &missing, &claimed)
            .await
            .unwrap();
        assert_eq!(job(&fixture, &claimed.model.id).await, successor.model);
        assert!(current_reconciles(&fixture, 2).await.is_empty());
        worker::handle_unavailable_provider(&fixture.store, &missing, &successor)
            .await
            .unwrap();
        let old = job(&fixture, &claimed.model.id).await;
        assert_eq!(old.state, "done");
        assert_eq!(old.attempts, 0);
        assert_eq!(old.locked_until, None);
        let current = current_reconciles(&fixture, 2).await;
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].state, "pending");
        assert_eq!(current[0].attempts, 0);
        assert_eq!(resource_state(&fixture).await, before);
        assert_no_provider_calls(&fixture);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_reconcile_absence_hint_restores_current_work() {
    isolated(|fixture| async move {
        let claimed = enqueue_reconcile(&fixture).await;
        let before = resource_state(&fixture).await;
        let db = fixture.store.db();
        remote_pin::Entity::delete_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .exec(db)
            .await
            .unwrap();
        let hint = worker::admission::worker_observation::observe_reconcile(
            db,
            &claimed.model,
            worker::SharedDecodeBudget::default(),
        )
        .await
        .unwrap();
        assert!(hint.snapshot.is_none());
        before
            .0
            .clone()
            .into_active_model()
            .insert(db)
            .await
            .unwrap();
        worker::finish_and_reconcile(&fixture.store, &claimed, Utc::now())
            .await
            .unwrap();
        let current = current_reconciles(&fixture, 1).await;
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].id, claimed.model.id);
        assert_eq!(current[0].state, "pending");
        assert_eq!(current[0].locked_until, None);
        assert_eq!(resource_state(&fixture).await, before);
        assert_no_provider_calls(&fixture);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_reconcile_stale_epoch_hint_uses_writer_current_epoch_and_retains_live_work() {
    isolated(|fixture| async move {
        let claimed = enqueue_reconcile(&fixture).await;
        let db = fixture.store.db();
        db.execute_unprepared(
            "UPDATE remote_pins SET epoch=2 WHERE provider='noop' AND cid='bafy-worker'",
        )
        .await
        .unwrap();
        let hint = worker::admission::worker_observation::observe_reconcile(
            db,
            &claimed.model,
            worker::SharedDecodeBudget::default(),
        )
        .await
        .unwrap();
        assert_eq!(hint.snapshot.unwrap().remote.epoch, 2);
        db.execute_unprepared(
            "UPDATE remote_pins SET epoch=3 WHERE provider='noop' AND cid='bafy-worker'",
        )
        .await
        .unwrap();
        jobs::enqueue_job(
            db,
            jobs::reconcile_job("noop", "bafy-worker", 3, Utc::now()),
        )
        .await
        .unwrap();
        let live = claim_one(&fixture).await;
        assert_eq!(live.model.expected_remote_epoch, Some(3));
        let before = resource_state(&fixture).await;
        worker::finish_and_reconcile(&fixture.store, &claimed, Utc::now())
            .await
            .unwrap();
        let old = job(&fixture, &claimed.model.id).await;
        assert_eq!(old.state, "done");
        assert_eq!(old.locked_until, None);
        assert!(current_reconciles(&fixture, 2).await.is_empty());
        assert_eq!(
            current_reconciles(&fixture, 3).await,
            vec![live.model.clone()]
        );
        worker::finish_and_reconcile(&fixture.store, &claimed, Utc::now())
            .await
            .unwrap();
        assert_eq!(job(&fixture, &live.model.id).await, live.model);
        assert_eq!(resource_state(&fixture).await, before);
        assert_no_provider_calls(&fixture);
    })
    .await;
}

async fn denied_cleanup(fixture: &Fixture) -> jobs::ClaimedPinJob {
    fixture
        .store
        .db()
        .execute_unprepared(
            "UPDATE remote_pins SET status='failed',request_id='failed-request'
         WHERE provider='noop' AND cid='bafy-worker';
         INSERT INTO remote_pin_ledger(provider,cid,ownership,effect)
         VALUES ('noop','bafy-worker','unknown','confirmed')",
        )
        .await
        .unwrap();
    let (cleanup, _) = worker::admission::worker_observation::observe_failed_cleanup(
        fixture.store.db(),
        "noop",
        "bafy-worker",
        worker::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert!(!cleanup);
    enqueue_reconcile(fixture).await
}

async fn ledger_row(fixture: &Fixture) -> remote_pin_ledger::Model {
    remote_pin_ledger::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_false_cleanup_stale_claim_cannot_retain_or_complete_successor() {
    isolated(|fixture| async move {
        let claimed = denied_cleanup(&fixture).await;
        let successor = reclaim(&fixture, &claimed).await;
        let before = resource_state(&fixture).await;
        let ledger_before = ledger_row(&fixture).await;
        let global = Arc::new(Semaphore::new(1));
        let cancellation = CancellationToken::new();
        worker::reconcile_failed(
            &fixture.store,
            &fixture.coordinator,
            &global,
            claimed.clone(),
            &cancellation,
        )
        .await
        .unwrap();
        assert_eq!(job(&fixture, &claimed.model.id).await, successor.model);
        assert_eq!(ledger_row(&fixture).await, ledger_before);
        assert_eq!(resource_state(&fixture).await, before);

        // The current claim can record retention, but unknown ownership never
        // authorizes a remote DELETE or releases the reservation.
        worker::reconcile_failed(
            &fixture.store,
            &fixture.coordinator,
            &global,
            successor,
            &cancellation,
        )
        .await
        .unwrap();
        assert_eq!(job(&fixture, &claimed.model.id).await.state, "done");
        let mut retained = ledger_before;
        retained.effect = "retained".to_owned();
        assert_eq!(ledger_row(&fixture).await, retained);
        assert_eq!(resource_state(&fixture).await, before);
        assert_no_provider_calls(&fixture);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_false_cleanup_stale_epoch_cannot_retain_new_lifetime() {
    isolated(|fixture| async move {
        let claimed = denied_cleanup(&fixture).await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET epoch=2,request_id='current-request'
             WHERE provider='noop' AND cid='bafy-worker'",
            )
            .await
            .unwrap();
        let before = resource_state(&fixture).await;
        let ledger_before = ledger_row(&fixture).await;
        worker::reconcile_failed(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(1)),
            claimed.clone(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let old = job(&fixture, &claimed.model.id).await;
        assert_eq!(old.state, "done");
        assert_eq!(old.locked_until, None);
        let current = current_reconciles(&fixture, 2).await;
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].state, "pending");
        assert_eq!(current[0].locked_until, None);
        assert_eq!(ledger_row(&fixture).await, ledger_before);
        assert_eq!(resource_state(&fixture).await, before);
        assert_no_provider_calls(&fixture);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL; run explicitly with --ignored"]
async fn postgres_ordinary_unpin_ack_releases_exact_reservation_once() {
    use std::sync::atomic::Ordering::SeqCst;

    use super::Script;
    use crate::{
        pinning::provider::RemotePinStatus,
        store::{
            entities::{object, pin_lease, pin_resource_history},
            pinning::leases,
        },
    };

    with_postgres_fixture(|fixture| async move {
        let db = fixture.store.db();
        let identity = fixture.coordinator.provider_identity("noop").unwrap();
        ledger::register_route(db, "noop", identity).await.unwrap();
        let reservation = db.begin().await.unwrap();
        ledger::capture_reservation(&reservation, "noop", "bafy-worker")
            .await
            .unwrap();
        reservation.commit().await.unwrap();
        db.execute_unprepared(
            "UPDATE remote_pins SET status='pinned',request_id='release-once'
             WHERE provider='noop' AND cid='bafy-worker';
             UPDATE pin_lease_targets SET state='pinned' WHERE id='target-1';
             INSERT INTO remote_pins
             (provider,cid,cid_size,status,epoch,failure_attempts,last_touched_at)
             VALUES ('noop','bafy-unrelated',100,'absent',1,0,CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        ledger::observe(
            db,
            "noop",
            "bafy-worker",
            RemotePinStatus::Pinned,
            ledger::Ownership::ApplicationCreated,
            Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(
            leases::cancel_lease(db, "lease-1", Utc::now())
                .await
                .unwrap(),
            leases::GenerationDecision::Current
        );
        jobs::enqueue_job(
            db,
            jobs::reconcile_job(
                "noop",
                "bafy-unrelated",
                1,
                Utc::now() + chrono::Duration::hours(1),
            ),
        )
        .await
        .unwrap();
        let unrelated =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-unrelated".to_owned()))
                .one(db)
                .await
                .unwrap()
                .unwrap();
        let unrelated_job = pin_job::Entity::find()
            .filter(pin_job::Column::Cid.eq("bafy-unrelated"))
            .one(db)
            .await
            .unwrap()
            .unwrap();
        let objects = object::Entity::find().all(db).await.unwrap();
        let leases = pin_lease::Entity::find().all(db).await.unwrap();
        let before = resource_state(&fixture).await;
        assert_eq!(before.0.status, "pinned");
        assert_eq!(before.0.request_id.as_deref(), Some("release-once"));
        assert_eq!((before.1.reserved_bytes, before.1.reserved_pins), (100, 1));
        assert_eq!(before.2.state, "released");
        assert!(
            leases::remote_work_snapshot(db, "noop", "bafy-worker")
                .await
                .unwrap()
                .unwrap()
                .desired
                .is_empty()
        );
        assert!(
            ledger::cleanup_allowed(db, "noop", "bafy-worker")
                .await
                .unwrap()
        );
        let mut archived_ledger = ledger_row(&fixture).await;
        assert_eq!(archived_ledger.ownership, "application_created");
        assert_eq!(archived_ledger.effect, "confirmed");
        assert_eq!(
            archived_ledger.route.as_deref(),
            Some(
                serde_json::to_string(&identity.route_snapshot())
                    .unwrap()
                    .as_str()
            )
        );
        assert!(
            pin_resource_history::Entity::find()
                .all(db)
                .await
                .unwrap()
                .is_empty()
        );

        let claimed = claim_one(&fixture).await;
        assert_eq!(claimed.model.operation, "unpin");
        assert_eq!(claimed.model.provider, "noop");
        assert_eq!(claimed.model.cid, "bafy-worker");
        assert_eq!(claimed.model.expected_remote_epoch, Some(before.0.epoch));
        assert_eq!(
            worker::current_unpin_request(&fixture.store, &claimed.model)
                .await
                .unwrap()
                .as_deref(),
            Some("release-once")
        );
        assert!(
            worker::job_authority::lookup(db, &claimed.model)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            ledger::job_route_matches(db, &claimed.model, Some(identity))
                .await
                .unwrap()
        );
        fixture
            .provider
            .scripts
            .lock()
            .await
            .push_back(Script::Unpin(Ok(())));
        let global = Arc::new(Semaphore::new(1));
        tokio::time::timeout(
            Duration::from_secs(5),
            worker::execute_claimed_job(
                &fixture.store,
                &fixture.coordinator,
                &global,
                claimed.clone(),
            ),
        )
        .await
        .expect("worker dispatch did not settle")
        .unwrap();

        let released = resource_state(&fixture).await;
        assert_eq!(released.0.status, "absent");
        assert_eq!(released.0.request_id, None);
        assert_eq!(released.0.epoch, before.0.epoch);
        assert_eq!(
            (released.1.reserved_bytes, released.1.reserved_pins),
            (0, 0)
        );
        assert_eq!(released.2, before.2);
        archived_ledger.effect = "cleanup_pending".to_owned();
        let history = pin_resource_history::Entity::find().all(db).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].provider, "noop");
        assert_eq!(history[0].cid, "bafy-worker");
        assert_eq!(history[0].epoch, before.0.epoch);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&history[0].ledger).unwrap(),
            serde_json::json!({
                "ledger": archived_ledger,
                "request_id": "release-once",
                "status_before_release": "pinned",
                "confirmed_absent": true,
            })
        );
        let mut absent_ledger = archived_ledger;
        absent_ledger.effect = "absent".to_owned();
        assert_eq!(ledger_row(&fixture).await, absent_ledger);
        let completed = job(&fixture, &claimed.model.id).await;
        assert_eq!(completed.state, "done");
        assert_eq!(completed.locked_until, None);
        assert_eq!(completed.attempts, 0);

        tokio::time::timeout(
            Duration::from_secs(5),
            worker::execute_claimed_job(&fixture.store, &fixture.coordinator, &global, claimed),
        )
        .await
        .expect("worker stale replay did not settle")
        .unwrap();
        assert_eq!(resource_state(&fixture).await, released);
        assert_eq!(ledger_row(&fixture).await, absent_ledger);
        assert_eq!(
            pin_resource_history::Entity::find().all(db).await.unwrap(),
            history
        );
        assert_eq!(job(&fixture, &completed.id).await, completed);
        assert_eq!(job(&fixture, &unrelated_job.id).await, unrelated_job);
        assert_eq!(pin_job::Entity::find().all(db).await.unwrap().len(), 2);
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-unrelated".to_owned()))
                .one(db)
                .await
                .unwrap()
                .unwrap(),
            unrelated
        );
        assert_eq!(remote_pin::Entity::find().all(db).await.unwrap().len(), 2);
        assert_eq!(
            pin_provider_usage::Entity::find().all(db).await.unwrap(),
            vec![released.1]
        );
        assert_eq!(pin_lease::Entity::find().all(db).await.unwrap(), leases);
        assert_eq!(object::Entity::find().all(db).await.unwrap(), objects);
        assert_eq!(fixture.provider.unpins.load(SeqCst), 1);
        assert_eq!(fixture.provider.submits.load(SeqCst), 0);
        assert_eq!(fixture.provider.gets.load(SeqCst), 0);
        assert_eq!(fixture.provider.finds.load(SeqCst), 0);
        assert!(fixture.provider.scripts.lock().await.is_empty());
    })
    .await;
}
