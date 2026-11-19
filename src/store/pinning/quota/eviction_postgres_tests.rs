use chrono::{Duration, TimeZone, Utc};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    EntityTrait, QueryFilter, Statement, TransactionTrait,
};

use super::{
    DateTimeUtc, EvictionSnapshot, ProviderLimitMap, ProviderLimits, ReservationOutcome,
    apply_eviction, discover_waiter_eviction, read_usage, reserve_unique,
};
use crate::store::{
    entities::{pin_job, pin_lease_target, remote_pin},
    pinning::leases::{
        GenerationDecision, RemoteDeleteCompletion, cancel_lease, complete_remote_delete,
        touch_target_reference,
    },
};

mod isolated_postgres {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/postgres_pinning_stage3.rs"
    ));
}

const PROVIDER: &str = "pinata";
const OLD_CID: &str = "bafy-eviction-pg-old";
const NEW_CID: &str = "bafy-eviction-pg-new";
const WAITER_CID: &str = "bafy-eviction-pg-waiter";
const LIMITS: ProviderLimits = ProviderLimits {
    priority: 1,
    max_bytes: 100,
    max_pins: 2,
    enabled: true,
};

fn time(seconds: i64) -> DateTimeUtc {
    Utc.with_ymd_and_hms(2026, 7, 21, 0, 0, 0).single().unwrap() + Duration::seconds(seconds)
}

async fn seed(db: &DatabaseConnection) {
    let limits = ProviderLimitMap::from([(PROVIDER.to_owned(), LIMITS)]);
    let tx = db.begin().await.unwrap();
    for (cid, touched_at) in [(OLD_CID, time(1)), (NEW_CID, time(2))] {
        assert_eq!(
            reserve_unique(&tx, PROVIDER, cid, 40, &limits, touched_at)
                .await
                .unwrap(),
            ReservationOutcome::Reserved
        );
    }
    tx.commit().await.unwrap();
    db.execute_unprepared(
        "INSERT INTO buckets (name) VALUES ('bucket');
         UPDATE remote_pins SET status='pinned', request_id='request-' || cid;",
    )
    .await
    .unwrap();
    for (id, cid, created_at, state) in [
        ("old", OLD_CID, time(1), "pinned"),
        ("new", NEW_CID, time(2), "pinned"),
        ("waiter", WAITER_CID, time(3), "quota_waiting"),
    ] {
        let timestamp = created_at.to_rfc3339();
        let expires_at = (created_at + Duration::hours(1)).to_rfc3339();
        db.execute_unprepared(&format!(
            "INSERT INTO objects (id, bucket, key, cid, size, etag)
             VALUES ('{id}', 'bucket', '{id}', '{cid}', 40, '{cid}');
             INSERT INTO pin_leases
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode,
                  created_at, last_touched_at, expires_at, generation, state)
             VALUES ('{id}', '{id}', 'automatic', 'policy', 'all', 'object',
                     '{timestamp}', '{timestamp}', '{expires_at}', 1, 'active');
             INSERT INTO pin_lease_targets
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at)
             VALUES ('{id}', '{id}', '{cid}', 40, '{PROVIDER}', '{state}',
                     '{timestamp}', '{timestamp}');"
        ))
        .await
        .unwrap();
    }
}

async fn target(db: &DatabaseConnection, id: &str) -> pin_lease_target::Model {
    pin_lease_target::Entity::find_by_id(id)
        .one(db)
        .await
        .unwrap()
        .unwrap()
}

async fn unpins<C: ConnectionTrait>(db: &C) -> Vec<pin_job::Model> {
    pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(PROVIDER))
        .filter(pin_job::Column::Operation.eq("unpin"))
        .all(db)
        .await
        .unwrap()
}

async fn discover(db: &DatabaseConnection) -> EvictionSnapshot {
    discover_waiter_eviction(db, PROVIDER, &LIMITS, time(4))
        .await
        .unwrap()
        .expect("the full provider must yield a victim for the waiter")
}

async fn assert_no_eviction(
    db: &DatabaseConnection,
    snapshot: &EvictionSnapshot,
    now: DateTimeUtc,
) {
    let old = target(db, "old").await;
    let newer = target(db, "new").await;
    let jobs = unpins(db).await;
    let tx = db.begin().await.unwrap();
    let evicted = apply_eviction(&tx, snapshot, now, None).await.unwrap();
    tx.commit().await.unwrap();

    assert!(evicted.is_empty());
    assert_eq!(target(db, "old").await, old);
    assert_eq!(target(db, "new").await, newer);
    assert_eq!(unpins(db).await, jobs);
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_waiter_eviction_commits_with_unselected_remote_locked() {
    isolated_postgres::isolated(true, |db| async move {
        seed(&db).await;
        let schema: String = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT current_schema() AS schema",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "schema")
            .unwrap();
        let mut options = ConnectOptions::new(std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap());
        options.set_schema_search_path(schema).max_connections(1);
        let peer = Database::connect(options).await.unwrap();
        peer.execute_unprepared("SET statement_timeout TO '15s'; SET lock_timeout TO '2s'")
            .await
            .unwrap();

        let holder = db.begin().await.unwrap();
        holder
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT cid FROM remote_pins WHERE provider=$1 AND cid=$2 FOR UPDATE",
                [PROVIDER.into(), NEW_CID.into()],
            ))
            .await
            .unwrap()
            .expect("the unselected remote must exist and be locked");

        let snapshot = discover(&peer).await;
        let tx = peer.begin().await.unwrap();
        let evicted = apply_eviction(&tx, &snapshot, time(6), None)
            .await
            .expect("eviction must not wait for the unselected remote");
        tx.commit().await.unwrap();
        holder.rollback().await.unwrap();
        peer.close().await.unwrap();

        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].cid, OLD_CID);
        assert_eq!(evicted[0].target_id, "old");
        assert_eq!(target(&db, "old").await.state, "evicted");
        assert_eq!(target(&db, "new").await.state, "pinned");
        let jobs = unpins(&db).await;
        let [unpin] = jobs.as_slice() else {
            panic!("expected exactly one Unpin, got {jobs:?}");
        };
        let remote = remote_pin::Entity::find_by_id((PROVIDER.to_owned(), OLD_CID.to_owned()))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unpin.cid, OLD_CID);
        assert_eq!(unpin.state, "pending");
        assert_eq!(unpin.expected_remote_epoch, Some(remote.epoch));
    })
    .await;
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_waiter_eviction_abandons_snapshot_after_demand_cancellation() {
    isolated_postgres::isolated(true, |db| async move {
        seed(&db).await;
        let snapshot = discover(&db).await;
        let change = db.begin().await.unwrap();
        assert_eq!(
            cancel_lease(&change, "waiter", time(5)).await.unwrap(),
            GenerationDecision::Current
        );
        change.commit().await.unwrap();
        assert_eq!(target(&db, "waiter").await.state, "released");

        assert_no_eviction(&db, &snapshot, time(6)).await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_waiter_eviction_abandons_snapshot_after_victim_touch() {
    isolated_postgres::isolated(true, |db| async move {
        seed(&db).await;
        let snapshot = discover(&db).await;
        let change = db.begin().await.unwrap();
        assert_eq!(
            touch_target_reference(&change, "old", time(5))
                .await
                .unwrap(),
            GenerationDecision::Current
        );
        change.commit().await.unwrap();
        assert_eq!(target(&db, "old").await.last_touched_at, time(5));

        assert_no_eviction(&db, &snapshot, time(6)).await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_waiter_eviction_abandons_snapshot_after_confirmed_headroom() {
    isolated_postgres::isolated(true, |db| async move {
        seed(&db).await;
        let snapshot = discover(&db).await;
        let cancel = db.begin().await.unwrap();
        assert_eq!(
            cancel_lease(&cancel, "new", time(5)).await.unwrap(),
            GenerationDecision::Current
        );
        cancel.commit().await.unwrap();

        let usage = read_usage(&db, PROVIDER).await.unwrap().unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (80, 2));
        let jobs = unpins(&db).await;
        let [unpin] = jobs.as_slice() else {
            panic!("expected an Unpin from the newer lease cancellation, got {jobs:?}");
        };
        assert_eq!(unpin.cid, NEW_CID);
        assert_eq!(unpin.state, "pending");
        assert_no_eviction(&db, &snapshot, time(6)).await;

        let confirm = db.begin().await.unwrap();
        assert_eq!(
            complete_remote_delete(
                &confirm,
                PROVIDER,
                NEW_CID,
                unpin.expected_remote_epoch.unwrap(),
                time(7),
            )
            .await
            .unwrap(),
            RemoteDeleteCompletion::Released
        );
        confirm.commit().await.unwrap();
        let usage = read_usage(&db, PROVIDER).await.unwrap().unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (40, 1));
        assert_eq!(target(&db, "waiter").await.state, "quota_waiting");

        assert_no_eviction(&db, &snapshot, time(8)).await;
    })
    .await;
}
