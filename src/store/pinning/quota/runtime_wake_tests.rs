use super::*;
use crate::store::{
    Store,
    entities::pin_job,
    pinning::management::{
        native::meter::Budget, records::ReadSession, transaction::CancellationToken,
    },
};
use sea_orm::{ConnectOptions, Database, PaginatorTrait, TransactionTrait};

fn limits() -> ProviderLimits {
    ProviderLimits {
        priority: 1,
        max_bytes: 100,
        max_pins: 10,
        enabled: true,
    }
}

async fn fixture() -> Store {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options.max_connections(1);
    fixture_with_options(options).await
}

async fn fixture_with_options(options: ConnectOptions) -> Store {
    let db = Database::connect(options).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO buckets (name) VALUES ('bucket');
         INSERT INTO objects (id,bucket,key,cid,size,etag)
         VALUES ('object','bucket','key','bafy-object',7,'bafy-object');
         INSERT INTO pin_provider_usage (provider,reserved_bytes,reserved_pins)
         VALUES ('pinata',0,0)",
    )
    .await
    .unwrap();
    super::super::ledger::register_route(
        &db,
        "pinata",
        &crate::pinning::identity::ProviderIdentity::legacy("pinata", "noop", None, "noop", "cid"),
    )
    .await
    .unwrap();
    Store::new(db)
}

async fn waiter(store: &Store, id: &str, cid: &str, now: DateTimeUtc) {
    let created = now.to_rfc3339();
    let expires = (now + chrono::Duration::hours(1)).to_rfc3339();
    store
        .db()
        .execute_unprepared(&format!(
            "INSERT INTO pin_leases
         (id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,
          last_touched_at,expires_at,generation,state)
         VALUES ('lease-{id}','object','source-{id}','policy','all','object',
                 '{created}','{created}','{expires}',1,'active');
         INSERT INTO pin_lease_targets
         (id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at)
         VALUES ('{id}','lease-{id}','{cid}',7,'pinata','quota_waiting','{created}','{created}')"
        ))
        .await
        .unwrap();
}

async fn set_operator_debt(store: &Store, cid: &str) {
    let route =
        crate::pinning::identity::ProviderIdentity::legacy("pinata", "ipfs", None, "kubo", "cid")
            .route_snapshot();
    remote_pin_ledger::Entity::insert(remote_pin_ledger::ActiveModel {
        provider: Set("pinata".into()),
        cid: Set(cid.into()),
        route: Set(Some(serde_json::to_string(&route).unwrap())),
        ownership: Set("unknown".into()),
        effect: Set("unknown".into()),
        ..Default::default()
    })
    .exec(store.db())
    .await
    .unwrap();
}

async fn assert_unallocated(store: &Store, target: &pin_lease_target::Model) {
    let usage = read_usage(store.db(), "pinata").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (0, 0));
    assert_eq!(
        remote_pin::Entity::find().count(store.db()).await.unwrap(),
        0
    );
    assert_eq!(
        remote_pin_ledger::Entity::find()
            .count(store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(pin_job::Entity::find().count(store.db()).await.unwrap(), 0);
    assert_eq!(
        pin_lease_target::Entity::find_by_id(target.id.clone())
            .one(store.db())
            .await
            .unwrap()
            .unwrap(),
        *target
    );
}

#[tokio::test]
async fn runtime_wake_batches_fifo_deduplicates_and_reuses_clone_qualification() {
    let store = fixture().await;
    let now = Utc::now();
    for id in [
        "f", "e", "d", "c", "b", "a", "inactive", "expired", "cooldown", "oversize", "terminal",
    ] {
        let cid = if id == "b" {
            "bafy-a".into()
        } else {
            format!("bafy-{id}")
        };
        waiter(&store, id, &cid, now).await;
    }
    store
        .db()
        .execute_unprepared(&format!(
            "UPDATE pin_leases SET state='cancelled' WHERE id='lease-inactive';
         UPDATE pin_leases SET expires_at=created_at WHERE id='lease-expired';
         UPDATE pin_lease_targets SET state='quota_blocked' WHERE id='b';
         UPDATE pin_lease_targets SET state='quota_blocked',logical_size=101 WHERE id='oversize';
         UPDATE pin_lease_targets SET state='error' WHERE id='terminal';
         UPDATE pin_lease_targets SET last_touched_at='{}' WHERE id='cooldown'",
            (now + chrono::Duration::hours(1)).to_rfc3339()
        ))
        .await
        .unwrap();

    let mut qualification_sql = Vec::new();
    for (owner, expected, unique) in [
        (store.clone(), vec!["a", "b", "c", "d"], 3),
        (store.clone(), vec!["e", "f"], 5),
    ] {
        let session = SharedDecodeBudget::default();
        let work = DecodeWork::shared(&session);
        owner.qualify_runtime_schema(work).await.unwrap();
        qualification_sql.push(session.lock().unwrap().task_usage().unwrap().sql);
        let WaiterWakeOutcome::Woken(woken) =
            wake_runtime_provider_waiters(&owner, "pinata", &limits(), now, work, None)
                .await
                .unwrap()
        else {
            panic!("valid waiters must be admitted");
        };
        assert_eq!(woken, expected);
        let usage = read_usage(store.db(), "pinata").await.unwrap().unwrap();
        assert_eq!(
            (usage.reserved_bytes, usage.reserved_pins),
            (unique * 7, unique)
        );
        assert_eq!(
            pin_job::Entity::find().count(store.db()).await.unwrap(),
            unique as u64
        );
    }
    assert!(
        qualification_sql[0] > qualification_sql[1],
        "cold qualification must be charged; the clone reuses its Store proof"
    );
    for id in ["inactive", "expired", "cooldown"] {
        assert_eq!(
            pin_lease_target::Entity::find_by_id(id)
                .one(store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "quota_waiting"
        );
    }
}

#[tokio::test]
async fn runtime_wake_shared_budget_exhaustion_after_reservation_rolls_back_every_write() {
    let store = fixture().await;
    let now = Utc::now();
    waiter(&store, "head", "bafy-head", now).await;
    waiter(&store, "tail", "bafy-tail", now).await;
    let original = pin_lease_target::Entity::find_by_id("head")
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    store
        .qualify_runtime_schema(DecodeWork::shared(&SharedDecodeBudget::default()))
        .await
        .unwrap();

    // Allow one complete wake and the next reservation, but not its projection.
    let baseline = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&baseline);
    let mut candidates =
        discover_runtime_quota_wake_candidates(&store, "pinata", &limits(), now, work)
            .await
            .unwrap();
    candidates.truncate(1);
    let tx = store.begin_runtime(work).await.unwrap();
    let WaiterWakeOutcome::Woken(woken) = leases::apply_quota_wake_candidates(
        tx.db(),
        "pinata",
        &limits(),
        now,
        candidates,
        Some((&tx, work)),
        None,
    )
    .await
    .unwrap() else {
        panic!("the valid queue head must be admitted");
    };
    assert_eq!(woken, ["head"]);
    assert_eq!(
        reserve_runtime_waiter(
            &tx,
            "pinata",
            "bafy-tail",
            7,
            &[("pinata".into(), limits())].into(),
            now,
            work
        )
        .await
        .unwrap(),
        Some(ReservationOutcome::Reserved)
    );
    let reservation_bytes = baseline.lock().unwrap().task_usage().unwrap().raw_bytes;
    tx.rollback().await.unwrap();
    let original_tail = pin_lease_target::Entity::find_by_id("tail")
        .one(store.db())
        .await
        .unwrap()
        .unwrap();

    let prior_work = 4096;
    let session = std::sync::Arc::new(std::sync::Mutex::new(ReadSession::for_task(
        Budget::default(),
        u64::MAX,
        reservation_bytes + prior_work,
        CancellationToken::new(),
    )));
    let work = DecodeWork::shared(&session);
    work.with(|session| session.consume(0, prior_work as usize))
        .unwrap();
    let candidates = discover_runtime_quota_wake_candidates(&store, "pinata", &limits(), now, work)
        .await
        .unwrap();
    let tx = store.begin_runtime(work).await.unwrap();
    let result = leases::apply_quota_wake_candidates(
        tx.db(),
        "pinata",
        &limits(),
        now,
        candidates,
        Some((&tx, work)),
        None,
    )
    .await;
    assert!(
        matches!(result, Err(AppError::ManagementBudget)),
        "{result:?}"
    );
    assert_eq!(
        remote_pin::Entity::find().count(tx.db()).await.unwrap(),
        2,
        "both reservations completed before exhaustion"
    );
    assert_eq!(
        remote_pin_ledger::Entity::find()
            .count(tx.db())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        pin_job::Entity::find().count(tx.db()).await.unwrap(),
        1,
        "the earlier projection inserted a job"
    );
    assert_eq!(
        read_usage(tx.db(), "pinata")
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        14
    );
    tx.rollback().await.unwrap();
    assert_unallocated(&store, &original).await;
    assert_unallocated(&store, &original_tail).await;
}

#[tokio::test]
async fn runtime_wake_discovers_past_full_debt_pages_without_the_sqlite_wal_writer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("quota-wake.sqlite");
    let mut options = ConnectOptions::new(format!(
        "sqlite://{}?mode=rwc",
        path.display().to_string().replace('\\', "/")
    ));
    options.max_connections(2);
    let store = fixture_with_options(options).await;
    store
        .db()
        .execute_unprepared("PRAGMA journal_mode = WAL")
        .await
        .unwrap();
    let now = Utc::now();
    for id in ["a", "b", "c", "d", "e", "f", "g", "h", "i"] {
        waiter(&store, id, "bafy-unresolved", now).await;
    }
    waiter(&store, "tail-blocked", "bafy-tail-blocked", now).await;
    waiter(&store, "tail-waiting", "bafy-tail-waiting", now).await;
    store
        .db()
        .execute_unprepared(
            "UPDATE pin_lease_targets SET state='quota_blocked' WHERE id IN ('a','tail-blocked')",
        )
        .await
        .unwrap();
    set_operator_debt(&store, "bafy-unresolved").await;
    assert_eq!(
        super::super::ledger::submission::admission_barrier(store.db(), "pinata")
            .await
            .unwrap(),
        super::super::ledger::submission::AdmissionBarrier::Operator
    );
    let session = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&session);
    let writer = store.db().begin().await.unwrap();
    writer
        .execute_unprepared("UPDATE objects SET etag='uncommitted' WHERE id='object'")
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let found = discover_runtime_quota_wake_candidates(&store, "pinata", &limits(), now, work)
            .await
            .unwrap();
        assert_eq!(
            found
                .iter()
                .map(|candidate| candidate.target.id.as_str())
                .collect::<Vec<_>>(),
            ["tail-blocked", "tail-waiting"]
        );
        let read = store.begin_runtime(work).await.unwrap();
        let first = leases::find_quota_waiter_for_eviction("pinata", now, read.catalog(work), work)
            .await
            .unwrap()
            .unwrap();
        read.rollback().await.unwrap();
        assert_eq!(first.target.id, "tail-waiting");
    })
    .await
    .expect("discovery must finish while another connection owns the WAL writer");
    writer.rollback().await.unwrap();

    let WaiterWakeOutcome::Woken(woken) =
        wake_runtime_provider_waiters(&store, "pinata", &limits(), now, work, None)
            .await
            .unwrap()
    else {
        panic!("provider debt does not make a valid waiter malformed");
    };
    assert_eq!(woken, ["tail-blocked", "tail-waiting"]);
    let usage = read_usage(store.db(), "pinata").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (14, 2));
    assert_eq!(pin_job::Entity::find().count(store.db()).await.unwrap(), 2);

    remote_pin_ledger::Entity::delete_by_id(("pinata".to_string(), "bafy-unresolved".to_string()))
        .exec(store.db())
        .await
        .unwrap();
    let WaiterWakeOutcome::Woken(woken) =
        wake_runtime_provider_waiters(&store, "pinata", &limits(), now, work, None)
            .await
            .unwrap()
    else {
        panic!("valid waiters")
    };
    assert_eq!(woken, ["a", "b", "c", "d"]);
    let usage = read_usage(store.db(), "pinata").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (21, 3));
}

#[tokio::test]
async fn eviction_discovers_a_victim_while_another_connection_holds_the_sqlite_writer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("eviction.sqlite");
    let mut options = ConnectOptions::new(format!(
        "sqlite://{}?mode=rwc",
        path.display().to_string().replace('\\', "/")
    ));
    options.max_connections(2);
    let store = fixture_with_options(options).await;
    store
        .db()
        .execute_unprepared("PRAGMA journal_mode = WAL")
        .await
        .unwrap();
    let now = Utc::now();
    let limit = ProviderLimits {
        max_pins: 1,
        ..limits()
    };
    let limits = ProviderLimitMap::from([("pinata".to_owned(), limit.clone())]);
    reserve_unique(store.db(), "pinata", "bafy-victim", 7, &limits, now)
        .await
        .unwrap();
    waiter(&store, "victim", "bafy-victim", now).await;
    store
        .db()
        .execute_unprepared("UPDATE pin_lease_targets SET state='waiting' WHERE id='victim'")
        .await
        .unwrap();
    waiter(&store, "demand", "bafy-demand", now).await;

    let writer = store.db().begin().await.unwrap();
    writer
        .execute_unprepared("UPDATE objects SET etag='uncommitted' WHERE id='object'")
        .await
        .unwrap();
    let selected = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        discover_waiter_eviction(store.db(), "pinata", &limit, now),
    )
    .await
    .expect("victim discovery must finish while the WAL writer is held")
    .unwrap();
    assert!(selected.is_some());
    writer.rollback().await.unwrap();
}

#[tokio::test]
async fn runtime_wake_rechecks_waiter_versions_and_debt_after_discovery() {
    let store = fixture().await;
    let now = Utc::now();
    for (id, cid) in [("a", "bafy-a"), ("b", "bafy-b"), ("c", "bafy-c")] {
        waiter(&store, id, cid, now).await;
    }
    let budget = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&budget);
    for change in [
        "UPDATE pin_leases SET generation=2 WHERE id='lease-a'".to_owned(),
        format!(
            "UPDATE pin_lease_targets SET last_touched_at='{}' WHERE id='b'",
            (now + chrono::Duration::hours(1)).to_rfc3339()
        ),
    ] {
        let candidates =
            discover_runtime_quota_wake_candidates(&store, "pinata", &limits(), now, work)
                .await
                .unwrap();
        store.db().execute_unprepared(&change).await.unwrap();
        let tx = store.begin_runtime(work).await.unwrap();
        let outcome = leases::apply_quota_wake_candidates(
            tx.db(),
            "pinata",
            &limits(),
            now,
            candidates,
            Some((&tx, work)),
            None,
        )
        .await
        .unwrap();
        assert!(matches!(outcome, WaiterWakeOutcome::Woken(woken) if woken.is_empty()));
        tx.commit().await.unwrap();
        assert_eq!(
            remote_pin::Entity::find().count(store.db()).await.unwrap(),
            0
        );
    }

    let candidates = discover_runtime_quota_wake_candidates(&store, "pinata", &limits(), now, work)
        .await
        .unwrap();
    set_operator_debt(&store, "bafy-a").await;
    let tx = store.begin_runtime(work).await.unwrap();
    let outcome = leases::apply_quota_wake_candidates(
        tx.db(),
        "pinata",
        &limits(),
        now,
        candidates,
        Some((&tx, work)),
        None,
    )
    .await
    .unwrap();
    assert!(matches!(outcome, WaiterWakeOutcome::Woken(woken) if woken == ["c"]));
    tx.commit().await.unwrap();
    let usage = read_usage(store.db(), "pinata").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    for id in ["a", "b"] {
        let target = pin_lease_target::Entity::find_by_id(id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target.state, "quota_waiting");
    }
}

#[tokio::test]
async fn cold_runtime_rejects_unready_database_before_a_candidate_scan() {
    let store = fixture().await;
    store
        .db()
        .execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
        .await
        .unwrap();
    let session = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&session);
    assert!(
        wake_runtime_provider_waiters(&store, "pinata", &limits(), Utc::now(), work, None)
            .await
            .is_err()
    );
    assert_eq!(pin_job::Entity::find().count(store.db()).await.unwrap(), 0);
}
