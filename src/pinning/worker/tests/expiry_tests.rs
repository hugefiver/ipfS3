use super::*;
use crate::pinning::worker::expire_worker_leases;

const TARGETS: i64 = 1980;

#[tokio::test]
async fn expired_submit_yields_to_live_sibling_before_recording_any_call() {
    let fixture = fixture([Script::Submit(Ok(remote("live", RemotePinStatus::Pinned)))]).await;
    let db = fixture.store.db();
    fixture.register_fixture_routes().await;
    let leases::TargetProjection::Waiting { submit_job_id } =
        leases::project_target_from_remote(db, "target-1", Utc::now())
            .await
            .unwrap()
    else {
        panic!("expected Submit")
    };
    db.execute_unprepared(
        "INSERT INTO pin_leases (id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state)
         SELECT 'lease-2',owner_object_id,'manual',policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state
         FROM pin_leases WHERE id='lease-1';
         INSERT INTO pin_lease_targets (id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at)
         SELECT 'target-2','lease-2',cid,logical_size,provider,state,created_at,last_touched_at FROM pin_lease_targets WHERE id='target-1';
         UPDATE pin_leases SET expires_at='2000-01-01T00:00:00+00:00' WHERE id='lease-1'"
    ).await.unwrap();

    fixture.run_one_due().await;
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
    assert!(
        jobs::submission_history(db, &submit_job_id)
            .await
            .unwrap()
            .is_none()
    );
    fixture.run_one_due().await; // Reconcile the same reservation.
    let next = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("submit"))
        .filter(pin_job::Column::State.eq("pending"))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.target_id.as_deref(), Some("target-2"));
    fixture.run_one_due().await;
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
    assert_eq!(
        jobs::submission_history(db, &next.id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        1
    );
}

#[tokio::test]
async fn submit_expiring_at_the_rate_gate_never_enters_the_provider() {
    let fixture = fixture([]).await;
    fixture.register_fixture_routes().await;
    let db = fixture.store.db();
    let leases::TargetProjection::Waiting { submit_job_id } =
        leases::project_target_from_remote(db, "target-1", Utc::now())
            .await
            .unwrap()
    else {
        panic!("expected Submit")
    };
    let runtime = fixture.coordinator.provider_runtime("noop").unwrap();
    let gate = runtime.next_request_at.lock().await;
    let expire_prepared = async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while jobs::submission_history(db, &submit_job_id)
                .await
                .unwrap()
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Submit never reached preparation");
        db.execute_unprepared(
            "UPDATE pin_leases SET expires_at='2000-01-01T00:00:00Z' WHERE id='lease-1'",
        )
        .await
        .unwrap();
        drop(gate);
    };
    tokio::join!(fixture.run_one_due(), expire_prepared);
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
    let job = pin_job::Entity::find_by_id(submit_job_id)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "done");
}

#[tokio::test]
async fn explicitly_extended_manual_lease_can_dispatch_its_new_generation() {
    let fixture = fixture([Script::Submit(Ok(remote(
        "renewed",
        RemotePinStatus::Pinned,
    )))])
    .await;
    fixture.register_fixture_routes().await;
    let db = fixture.store.db();
    db.execute_unprepared("UPDATE pin_leases SET source='manual', expires_at='2000-01-01T00:00:00+00:00' WHERE id='lease-1'").await.unwrap();
    let now = Utc::now();
    let result = leases::renew_manual_lease(
        db,
        "object-1",
        "lease-1",
        now + chrono::Duration::hours(1),
        now,
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        leases::ManualLeaseRenewalOutcome::Extended { generation: 2 }
    );
    // Renewal schedules both Reconcile and the generation-2 Submit for the same time.
    fixture.run_one_due().await;
    fixture.run_one_due().await;
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
    let submit_job_id = jobs::stable_submit_id("noop", "bafy-worker", "target-1", 2);
    assert_eq!(
        jobs::submission_history(db, &submit_job_id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        1
    );
}

async fn seed_due_lease(fixture: &Fixture) {
    fixture
        .submit_owned("request-shared", RemotePinStatus::Pinned)
        .await;
    let db = fixture.store.db();
    db.execute_unprepared(&format!(
        "INSERT INTO pin_leases (id,owner_object_id,source,policy_id,provider_mode,content_mode,
                                 created_at,last_touched_at,expires_at,generation,state)
         SELECT 'lease-bulk',owner_object_id,'manual',policy_id,provider_mode,'decompressed',
                '2000-01-01T00:00:00+00:00',last_touched_at,'2000-01-02T00:00:00+00:00',7,'active'
         FROM pin_leases WHERE id='lease-1';
         WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<{TARGETS})
         INSERT INTO pin_lease_targets (id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at)
         SELECT printf('bulk-%04d',n),'lease-bulk',CASE WHEN n=1 THEN t.cid ELSE printf('bafy-bulk-%04d',n) END,
                t.logical_size,t.provider,t.state,t.created_at,t.last_touched_at
         FROM seq CROSS JOIN pin_lease_targets t WHERE t.id='target-1';
         INSERT INTO remote_pins (provider,cid,cid_size,request_id,status,epoch,failure_attempts,last_touched_at)
         SELECT provider,cid,logical_size,'request-'||id,'pinned',1,0,last_touched_at
         FROM pin_lease_targets WHERE lease_id='lease-bulk' AND cid!='bafy-worker';
         INSERT INTO remote_pin_ledger (provider,cid,route,ownership,effect,remote_pinned_at)
         SELECT t.provider,t.cid,l.route,l.ownership,l.effect,l.remote_pinned_at
         FROM pin_lease_targets t JOIN remote_pin_ledger l ON l.provider=t.provider AND l.cid='bafy-worker'
         WHERE t.lease_id='lease-bulk' AND t.cid!='bafy-worker';
         UPDATE pin_provider_usage SET reserved_bytes=100*{TARGETS},reserved_pins={TARGETS} WHERE provider='noop'"
    )).await.unwrap();
}

#[tokio::test]
async fn large_expired_lease_resumes_after_cursor_reset() {
    let fixture = fixture_with_noop_limits([], 1_000_000, 2000).await;
    seed_due_lease(&fixture).await;
    let db = fixture.store.db();
    let now = Utc::now();
    let mut cursor = String::new();
    let released = async || {
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("lease-bulk"))
            .filter(pin_lease_target::Column::State.eq("released"))
            .count(db)
            .await
            .unwrap() as i64
    };
    expire_worker_leases(&fixture.store, now, &mut cursor, 1)
        .await
        .unwrap();
    let mut done = released().await;
    assert!(
        (1..TARGETS).contains(&done),
        "first expiry pass released {done}/{TARGETS}"
    );

    cursor.clear();
    while done < TARGETS {
        expire_worker_leases(&fixture.store, now, &mut cursor, 16)
            .await
            .unwrap();
        let next = released().await;
        assert!(next > done, "expiry stopped after {done} released targets");
        done = next;
    }
    let lease = pin_lease::Entity::find_by_id("lease-bulk")
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((lease.state.as_str(), lease.generation), ("expired", 8));

    let unpins = pin_job::Entity::find().filter(pin_job::Column::Operation.eq("unpin"));
    assert_eq!(unpins.count(db).await.unwrap() as i64, TARGETS - 1);
    let matching = db.query_one(sea_orm::Statement::from_string(db.get_database_backend(),
        "SELECT COUNT(DISTINCT t.cid) AS n FROM pin_lease_targets t
         JOIN remote_pins r ON r.provider=t.provider AND r.cid=t.cid
         JOIN pin_jobs j ON j.provider=r.provider AND j.cid=r.cid AND j.expected_remote_epoch=r.epoch
         JOIN pin_invocation_routes i ON i.job_id=j.id AND i.remote_epoch=r.epoch
         WHERE t.lease_id='lease-bulk' AND t.cid!='bafy-worker' AND j.operation='unpin' AND j.state='pending'"
    )).await.unwrap().unwrap();
    assert_eq!(matching.try_get::<i64>("", "n").unwrap(), TARGETS - 1);
    let shared = pin_lease_target::Entity::find_by_id("target-1")
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(shared.state, "pinned");
}
