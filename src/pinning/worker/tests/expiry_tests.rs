use super::*;
use crate::pinning::worker::expire_worker_leases;

const TARGETS: i64 = 1980;

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
