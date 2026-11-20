use super::*;

mod dispatch_batch;
mod pg_batch;

#[path = "../../../../tests/support/baseline_postgres.rs"]
mod postgres;
use crate::{
    pinning::{
        config::{LeaseDuration, ProviderLimitMap, ProviderLimits, ProviderMode},
        policy::{LeaseIntent, LeaseSource, ManualLeaseMutation},
        tags::ContentMode,
    },
    store::pinning::publication::PinTargetSpec,
};
use sea_orm::{Database, DatabaseConnection, TransactionTrait};

async fn setup() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys=ON")
        .await
        .unwrap();
    db.execute_unprepared(include_str!("schema.sql"))
        .await
        .unwrap();
    db
}

fn time(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap()
}

fn target(seed: u8, size: i64) -> PinTargetSpec {
    let multihash = cid::multihash::Multihash::wrap(0x12, &[seed; 32]).unwrap();
    PinTargetSpec {
        cid: cid::Cid::new_v0(multihash).unwrap().to_string(),
        logical_size: size,
    }
}

fn limits() -> ProviderLimitMap {
    [("alpha", 0), ("beta", 1)]
        .into_iter()
        .map(|(name, priority)| {
            (
                name.to_owned(),
                ProviderLimits {
                    priority,
                    max_pins: 3,
                    max_bytes: 10,
                    enabled: true,
                },
            )
        })
        .collect()
}

fn intent(source: LeaseSource, mode: ProviderMode, names: &[&str]) -> LeaseIntent {
    LeaseIntent {
        source,
        provider_mode: mode,
        providers: names.iter().map(|name| name.to_string()).collect(),
        policy_id: "default".into(),
        content_mode: ContentMode::Object,
        duration: LeaseDuration::parse("1m").unwrap(),
    }
}

async fn add(
    db: &DatabaseConnection,
    owner: &str,
    target: &PinTargetSpec,
    intent: &LeaseIntent,
    now: DateTime<Utc>,
) {
    let tx = db.begin().await.unwrap();
    attach(&tx, owner, target, intent, &limits(), now)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn next(db: &DatabaseConnection, provider: &str, t: i64) -> Option<Claim> {
    claim(db, provider, &limits()[provider], time(t), time(t + 10))
        .await
        .unwrap()
}

#[tokio::test]
async fn zip_batch_shares_resources_and_keeps_each_owner_across_chunks() {
    let db = setup().await;
    let policy = intent(
        LeaseSource::Automatic,
        ProviderMode::All,
        &["alpha", "beta"],
    );
    let mut requests: Vec<_> = (0..140)
        .map(|n| (format!("file-{n}"), target(n, 8), policy.clone()))
        .collect();
    requests.push(requests[0].clone());
    requests.push(("copied-file".into(), target(0, 8), policy));
    let tx = db.begin().await.unwrap();
    attach_many(&tx, &requests, &limits(), time(0))
        .await
        .unwrap();
    release_owner(&tx, "file-0", time(1)).await.unwrap();
    tx.commit().await.unwrap();
    let count = db.query_one(stmt(&db,"SELECT (SELECT COUNT(*) FROM pin_resources) AS resources, (SELECT COUNT(*) FROM pin_refs) AS refs",vec![])).await.unwrap().unwrap();
    assert_eq!(count.try_get::<i64>("", "resources").unwrap(), 280);
    assert_eq!(count.try_get::<i64>("", "refs").unwrap(), 280);
    assert!(next(&db, "beta", 1).await.is_some());
}

#[tokio::test]
async fn definitive_failure_moves_only_current_one_refs_and_exhausts_candidates() {
    exercise_one_failover(setup().await).await;
}

async fn exercise_one_failover(db: DatabaseConnection) {
    let file = target(12, 3);
    let mut candidates = limits();
    let mut too_small = candidates["alpha"].clone();
    too_small.priority = 1;
    too_small.max_bytes = file.logical_size - 1;
    candidates.insert("too-small".into(), too_small);
    let mut no_slots = candidates["alpha"].clone();
    no_slots.priority = 2;
    no_slots.max_pins = 0;
    candidates.insert("no-slots".into(), no_slots);
    candidates.get_mut("beta").unwrap().priority = 3;
    let one = intent(
        LeaseSource::Manual,
        ProviderMode::One,
        &["alpha", "too-small", "no-slots", "beta"],
    );
    let tx = db.begin().await.unwrap();
    attach(&tx, "switch", &file, &one, &candidates, time(0))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    add(
        &db,
        "stay",
        &file,
        &intent(LeaseSource::Automatic, ProviderMode::All, &["alpha"]),
        time(0),
    )
    .await;
    let first = next(&db, "alpha", 1).await.unwrap();
    settle(&db, &first, Outcome::Failed, time(2)).await.unwrap();
    let fixed = db
        .query_one(stmt(
            &db,
            "SELECT provider FROM pin_refs WHERE owner_object_id='stay'",
            vec![],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixed.try_get::<String>("", "provider").unwrap(), "alpha");
    let tx = db.begin().await.unwrap();
    release_owner(&tx, "stay", time(3)).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        db.query_one(stmt(
            &db,
            "SELECT cid FROM pin_resources WHERE provider='alpha' AND cid=$1",
            vec![first.cid.clone().into()]
        ))
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        manual(&db, "switch", time(3))
            .await
            .unwrap()
            .unwrap()
            .expires_at,
        time(60)
    );
    let second = next(&db, "beta", 3).await.unwrap();
    assert_eq!(second.cid, first.cid);
    assert_eq!(second.source_cid, file.cid);
    settle(&db, &second, Outcome::Failed, time(4))
        .await
        .unwrap();
    assert!(next(&db, "alpha", 5).await.is_none());
    assert!(next(&db, "beta", 5).await.is_none());
}

#[tokio::test]
async fn unknown_and_rejected_with_handle_do_not_submit_to_another_provider() {
    let db = setup().await;
    let one = intent(LeaseSource::Manual, ProviderMode::One, &["alpha", "beta"]);
    add(&db, "lost", &target(13, 2), &one, time(0)).await;
    let lost = next(&db, "alpha", 0).await.unwrap();
    settle(&db, &lost, Outcome::Unknown, time(1)).await.unwrap();
    add(&db, "rejected", &target(14, 2), &one, time(1)).await;
    let rejected = next(&db, "alpha", 1).await.unwrap();
    settle(
        &db,
        &rejected,
        Outcome::Rejected("negative-ack-handle".into()),
        time(2),
    )
    .await
    .unwrap();
    assert!(next(&db, "beta", 3).await.is_none());
    expire(&db, time(100), 20).await.unwrap();
    let cleanup = next(&db, "alpha", 100).await.unwrap();
    assert_eq!(cleanup.handle.as_deref(), Some("negative-ack-handle"));
    settle(&db, &cleanup, Outcome::Deleted, time(100))
        .await
        .unwrap();
    assert!(next(&db, "alpha", 100).await.is_none());
}

#[tokio::test]
async fn shared_canonical_resource_obeys_manual_renewal_and_last_expiry() {
    let db = setup().await;
    let file = target(1, 8);
    let equivalent = PinTargetSpec {
        cid: crate::pinning::provider::canonical_cid(&file.cid).unwrap(),
        logical_size: file.logical_size,
    };
    add(
        &db,
        "file-a",
        &equivalent,
        &intent(LeaseSource::Automatic, ProviderMode::One, &["alpha"]),
        time(0),
    )
    .await;
    add(
        &db,
        "file-b",
        &file,
        &intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]),
        time(0),
    )
    .await;
    let first = next(&db, "alpha", 0).await.unwrap();
    assert_eq!(first.cid, equivalent.cid);
    assert_eq!(first.source_cid, file.cid);
    assert!(next(&db, "alpha", 1).await.is_none());
    settle(
        &db,
        &first,
        Outcome::Submitted("remote-handle".into()),
        time(1),
    )
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    replace_manual(
        &tx,
        "file-b",
        &ManualLeaseMutation::Renew {
            retain_until: time(90),
        },
        time(15),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(expire(&db, time(60), 20).await.unwrap(), 1);
    assert!(next(&db, "alpha", 60).await.is_none());
    assert_eq!(
        manual(&db, "file-b", time(60))
            .await
            .unwrap()
            .unwrap()
            .expires_at,
        time(90)
    );
    expire(&db, time(90), 20).await.unwrap();
    let remove = next(&db, "alpha", 90).await.unwrap();
    assert_eq!(remove.action, Action::Delete);
    assert_eq!(remove.handle.as_deref(), Some("remote-handle"));
    settle(&db, &remove, Outcome::Deleted, time(91))
        .await
        .unwrap();
    let cleaned=db.query_one(stmt(&db,"SELECT status, action, handle, claim_token FROM pin_resources WHERE provider=$1 AND cid=$2",vec!["alpha".into(),remove.cid.clone().into()])).await.unwrap().unwrap();
    assert_eq!(cleaned.try_get::<String>("", "status").unwrap(), "cleaned");
    for field in ["action", "handle", "claim_token"] {
        assert!(
            cleaned
                .try_get::<Option<String>>("", field)
                .unwrap()
                .is_none()
        );
    }
    assert!(next(&db, "alpha", 92).await.is_none());
    add(
        &db,
        "fresh",
        &equivalent,
        &intent(LeaseSource::Manual, ProviderMode::One, &["alpha", "beta"]),
        time(93),
    )
    .await;
    let fresh = next(&db, "alpha", 93).await.unwrap();
    assert_eq!(fresh.source_cid, equivalent.cid);
    assert_eq!(
        (fresh.action, fresh.attempt, fresh.cid),
        (Action::Submit, 1, remove.cid)
    );
}

#[tokio::test]
async fn lost_post_never_reposts_and_late_ack_tracks_demand_reversals() {
    let db = setup().await;
    let file = target(2, 8);
    let request = intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]);
    add(&db, "original", &file, &request, time(0)).await;
    let post = next(&db, "alpha", 0).await.unwrap();
    let tx = db.begin().await.unwrap();
    replace_manual(&tx, "original", &ManualLeaseMutation::Cancel, time(1))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(next(&db, "alpha", 11).await.is_none());
    // Even with fresh demand, a lost POST occupies capacity and cannot be repeated.
    add(&db, "temporary", &file, &request, time(12)).await;
    add(&db, "unrelated", &target(3, 7), &request, time(12)).await;
    assert!(next(&db, "alpha", 12).await.is_none());
    let tx = db.begin().await.unwrap();
    release_owner(&tx, "temporary", time(13)).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        settle_dispatch(
            &db,
            &post,
            Outcome::Submitted("first-upload".into()),
            time(14)
        )
        .await
        .unwrap()
        .due_delete
    );
    let remove = next(&db, "alpha", 15).await.unwrap();
    assert_eq!(remove.handle.as_deref(), Some("first-upload"));
    add(&db, "new-owner", &file, &request, time(16)).await;
    settle(&db, &remove, Outcome::Deleted, time(17))
        .await
        .unwrap();
    assert!(
        !settle_dispatch(
            &db,
            &post,
            Outcome::Submitted("stale-upload".into()),
            time(18)
        )
        .await
        .unwrap()
        .applied
    );
    let tx = db.begin().await.unwrap();
    release_owner(&tx, "unrelated", time(18)).await.unwrap();
    tx.commit().await.unwrap();
    let repin = next(&db, "alpha", 18).await.unwrap();
    assert_eq!((repin.action, repin.cid), (Action::Submit, post.cid));
    assert!(repin.handle.is_none());
}

#[tokio::test]
async fn provider_choice_and_dispatch_charge_real_bytes_once() {
    let db = setup().await;
    let both = intent(
        LeaseSource::Automatic,
        ProviderMode::All,
        &["alpha", "beta"],
    );
    let one = intent(
        LeaseSource::Automatic,
        ProviderMode::One,
        &["beta", "alpha"],
    );
    let large = target(4, 8);
    add(&db, "primary", &large, &one, time(0)).await;
    let a = next(&db, "alpha", 0).await.unwrap();
    add(&db, "shared", &large, &one, time(1)).await;
    assert!(next(&db, "beta", 1).await.is_none());
    add(&db, "spill", &target(5, 4), &one, time(1)).await;
    let b = next(&db, "beta", 1).await.unwrap();
    assert!(next(&db, "alpha", 1).await.is_none());
    add(&db, "both", &target(6, 2), &both, time(2)).await;
    let second_a = next(&db, "alpha", 2).await.unwrap();
    let second_b = next(&db, "beta", 2).await.unwrap();
    assert_eq!(second_a.cid, second_b.cid);
    assert_ne!(a.cid, b.cid);
    add(
        &db,
        "waiting",
        &target(7, 1),
        &intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]),
        time(2),
    )
    .await;
    assert!(next(&db, "alpha", 2).await.is_none());
    settle(&db, &a, Outcome::Failed, time(2)).await.unwrap();
    assert!(next(&db, "alpha", 2).await.is_some());
}

#[tokio::test]
#[ignore = "requires an isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_claim_and_expiry_skip_a_locked_resource() {
    postgres::with_schema(|db| async move {
        db.execute_unprepared(include_str!("schema.sql"))
            .await
            .unwrap();
        let demand = intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]);
        add(&db, "one", &target(1, 5), &demand, time(0)).await;
        add(&db, "two", &target(2, 5), &demand, time(0)).await;
        let tx = db.begin().await.unwrap();
        let locked: String = tx
            .query_one(stmt(
                &tx,
                "SELECT cid FROM pin_resources ORDER BY cid LIMIT 1 FOR UPDATE",
                vec![],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "cid")
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let other = next(&db, "alpha", 0).await.unwrap();
            assert_ne!(other.cid, locked);
            settle(&db, &other, Outcome::Submitted("handle".into()), time(1))
                .await
                .unwrap();
            assert_eq!(expire(&db, time(61), 10).await.unwrap(), 1);
            let remove = next(&db, "alpha", 61).await.unwrap();
            assert_eq!(remove.handle.as_deref(), Some("handle"));
            settle(&db, &remove, Outcome::Deleted, time(62))
                .await
                .unwrap();
        })
        .await
        .expect("a locked sibling must not block the next resource");
        tx.rollback().await.unwrap();
        assert_eq!(expire(&db, time(62), 10).await.unwrap(), 1);
        let cleaned = db
            .query_one(stmt(&db, "SELECT status,action FROM pin_resources", vec![]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cleaned.try_get::<String>("", "status").unwrap(), "cleaned");
        assert!(
            cleaned
                .try_get::<Option<String>>("", "action")
                .unwrap()
                .is_none()
        );
        exercise_one_failover(db).await;
    })
    .await;
}

#[tokio::test]
async fn rejected_submit_retries_at_deadline_without_reviving_cancelled_demand() {
    let db = setup().await;
    add(
        &db,
        "retry",
        &target(8, 4),
        &intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]),
        time(0),
    )
    .await;
    let first = next(&db, "alpha", 0).await.unwrap();
    settle(&db, &first, Outcome::RetryAt(time(5)), time(1))
        .await
        .unwrap();
    assert!(next(&db, "alpha", 4).await.is_none());
    let second = next(&db, "alpha", 5).await.unwrap();
    assert_eq!(second.attempt, 2);
    assert_ne!(first.token, second.token);
    settle(&db, &first, Outcome::Submitted("stale".into()), time(5))
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    replace_manual(&tx, "retry", &ManualLeaseMutation::Cancel, time(6))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    settle(&db, &second, Outcome::RetryAt(time(15)), time(6))
        .await
        .unwrap();
    assert!(next(&db, "alpha", 15).await.is_none());
    assert!(
        db.query_one(stmt(&db, "SELECT cid FROM pin_resources", vec![]))
            .await
            .unwrap()
            .is_none()
    );
}
