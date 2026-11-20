use super::*;
use std::collections::BTreeMap;

#[tokio::test]
async fn sqlite_prefetch_is_discardable_and_one_transaction_claims_only_free_slots() {
    let db = setup().await;
    let mut limits = limits();
    let alpha = limits.get_mut("alpha").unwrap();
    alpha.max_pins = 64;
    alpha.max_bytes = 64;
    let demand = intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]);
    let requests = (0..64)
        .map(|n| (format!("owner-{n}"), target(n, 1), demand.clone()))
        .collect::<Vec<_>>();
    let tx = db.begin().await.unwrap();
    attach_many(&tx, &requests, &limits, time(0)).await.unwrap();
    tx.commit().await.unwrap();
    let page = candidate_page(&db, "alpha", &limits["alpha"], None, 64, time(0))
        .await
        .unwrap();
    assert_eq!(page.len(), 64);
    let budgets = BTreeMap::from([("alpha".into(), 32)]);
    let first = claim_batch(&db, &page, &limits, &budgets, 16, time(1), time(10))
        .await
        .unwrap();
    assert_eq!(first.claims.len(), 16);
    assert_eq!(first.consumed, (0..16).collect::<Vec<_>>());
    drop(page);
    let page = candidate_page(&db, "alpha", &limits["alpha"], None, 64, time(11))
        .await
        .unwrap();
    let single = BTreeMap::from([("alpha".into(), 1)]);
    let second = claim_batch(&db, &page, &limits, &single, 16, time(11), time(21))
        .await
        .unwrap();
    assert_eq!(second.claims.len(), 1);
    assert_eq!(second.consumed.len(), 17);
    let row = db.query_one(stmt(&db,"SELECT COUNT(CASE WHEN status='unknown' THEN 1 END) AS lost,
        COUNT(CASE WHEN status='waiting' AND attempt=0 AND claim_token IS NULL THEN 1 END) AS untouched FROM pin_resources",vec![])).await.unwrap().unwrap();
    assert_eq!(row.try_get::<i64>("", "lost").unwrap(), 16);
    assert_eq!(row.try_get::<i64>("", "untouched").unwrap(), 47);
    assert!(
        !settle_dispatch(
            &db,
            &first.claims[0],
            Outcome::Submitted("late".into()),
            time(12)
        )
        .await
        .unwrap()
        .due_delete
    );
    let row = db
        .query_one(stmt(
            &db,
            "SELECT status FROM pin_resources WHERE handle='late'",
            vec![],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<String>("", "status").unwrap(), "accepted");
}

#[tokio::test]
async fn sqlite_skip_oversize_page_and_recheck_cached_demand_and_due_time() {
    let db = setup().await;
    let limits = limits();
    let budgets = BTreeMap::from([("alpha".into(), 3)]);
    let demand = intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]);
    add(&db, "occupied", &target(0, 7), &demand, time(0)).await;
    let occupied = next(&db, "alpha", 0).await.unwrap();
    settle(
        &db,
        &occupied,
        Outcome::Submitted("occupied".into()),
        time(0),
    )
    .await
    .unwrap();
    for (n, size) in [(1, 4), (2, 5), (3, 1), (4, 1), (5, 2)] {
        let file = target(n, size);
        add(&db, &format!("owner-{n}"), &file, &demand, time(0)).await;
        db.execute(stmt(
            &db,
            "UPDATE pin_resources SET next_at=$1 WHERE source_cid=$2",
            vec![i64::from(n).into(), file.cid.into()],
        ))
        .await
        .unwrap();
    }
    let head = candidate_page(&db, "alpha", &limits["alpha"], None, 2, time(1))
        .await
        .unwrap();
    let skipped = claim_batch(&db, &head, &limits, &budgets, 3, time(1), time(20))
        .await
        .unwrap();
    assert!(skipped.claims.is_empty());
    assert!(skipped.blocked.is_empty());
    assert_eq!(skipped.consumed, [0, 1]);
    let tail = candidate_page(&db, "alpha", &limits["alpha"], head.last(), 4, time(1))
        .await
        .unwrap();
    assert_eq!(tail.len(), 3);
    db.execute(stmt(
        &db,
        "UPDATE pin_refs SET expires_at=0 WHERE owner_object_id='owner-3'",
        vec![],
    ))
    .await
    .unwrap();
    db.execute(stmt(
        &db,
        "UPDATE pin_resources SET next_at=$1 WHERE cid=$2",
        vec![
            time(15).timestamp_micros().into(),
            tail[1].cid.clone().into(),
        ],
    ))
    .await
    .unwrap();
    let claimed = claim_batch(&db, &tail, &limits, &budgets, 3, time(2), time(20))
        .await
        .unwrap();
    assert_eq!(claimed.claims.len(), 1);
    assert_eq!(claimed.claims[0].source_cid, target(5, 2).cid);
    assert_eq!(claimed.consumed, [0, 1, 2]);
}

#[tokio::test]
async fn sqlite_independent_connections_prefetch_under_writer_and_share_one_quota() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("claims.db").display()
    );
    let first = Database::connect(&url).await.unwrap();
    first
        .execute_unprepared(include_str!("../schema.sql"))
        .await
        .unwrap();
    let second = Database::connect(&url).await.unwrap();
    let limits = limits();
    let demand = intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]);
    for n in 1..=3 {
        add(
            &first,
            &format!("owner-{n}"),
            &target(n, 5),
            &demand,
            time(0),
        )
        .await;
    }
    let writer = first.begin().await.unwrap();
    sqlite_writer(&writer).await.unwrap();
    let page = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        assert!(next(&second, "beta", 0).await.is_none());
        candidate_page(&second, "alpha", &limits["alpha"], None, 3, time(0))
            .await
            .unwrap()
    })
    .await
    .expect("idle claims and prefetch only need a reader");
    writer.rollback().await.unwrap();
    let budget = BTreeMap::from([("alpha".into(), 3)]);
    let (a, b) = tokio::join!(
        claim_batch(&first, &page, &limits, &budget, 1, time(0), time(10)),
        claim_batch(&second, &page, &limits, &budget, 1, time(0), time(10)),
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!((a.claims.len(), b.claims.len()), (1, 1));
    assert_ne!(a.claims[0].cid, b.claims[0].cid);
    assert!(next(&second, "alpha", 0).await.is_none());
    first.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_full_bytes_park_the_route_unless_a_zero_byte_submit_fits() {
    let db = setup().await;
    let limits = limits();
    let demand = intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]);
    let budget = BTreeMap::from([("alpha".into(), 2)]);
    add(&db, "full", &target(1, 10), &demand, time(0)).await;
    let full = next(&db, "alpha", 0).await.unwrap();
    settle(&db, &full, Outcome::Submitted("full".into()), time(0))
        .await
        .unwrap();
    add(&db, "wait", &target(2, 1), &demand, time(0)).await;
    let page = candidate_page(&db, "alpha", &limits["alpha"], None, 1, time(0))
        .await
        .unwrap();
    let saturated = claim_batch(&db, &page, &limits, &budget, 2, time(0), time(10))
        .await
        .unwrap();
    assert!(saturated.claims.is_empty());
    assert!(saturated.blocked.contains("alpha"));
    let empty = target(3, 0);
    add(&db, "zero", &empty, &demand, time(0)).await;
    let next = claim(&db, "alpha", &limits["alpha"], time(0), time(10))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.source_cid, empty.cid);
}

#[tokio::test]
async fn sqlite_new_due_delete_preempts_cached_submit_even_when_disabled() {
    let db = setup().await;
    let mut limits = limits();
    let budgets = BTreeMap::from([("alpha".into(), 1)]);
    let demand = intent(LeaseSource::Manual, ProviderMode::One, &["alpha"]);
    add(&db, "retired", &target(0, 1), &demand, time(0)).await;
    let old = next(&db, "alpha", 0).await.unwrap();
    settle(&db, &old, Outcome::Submitted("delete-me".into()), time(0))
        .await
        .unwrap();
    add(&db, "waiting", &target(1, 2), &demand, time(0)).await;
    let cached = candidate_page(&db, "alpha", &limits["alpha"], None, 32, time(0))
        .await
        .unwrap();
    assert_eq!(cached.len(), 1);
    let tx = db.begin().await.unwrap();
    release_owner(&tx, "retired", time(1)).await.unwrap();
    tx.commit().await.unwrap();
    limits.get_mut("alpha").unwrap().enabled = false;
    let claimed = claim_batch(&db, &cached, &limits, &budgets, 1, time(2), time(20))
        .await
        .unwrap();
    assert_eq!(claimed.claims.len(), 1);
    assert_eq!(claimed.claims[0].action, Action::Delete);
    assert_eq!(claimed.claims[0].handle.as_deref(), Some("delete-me"));
    assert!(claimed.consumed.is_empty());
}
