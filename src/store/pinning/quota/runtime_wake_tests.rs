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
    for id in ["f", "e", "d", "c", "b", "a", "inactive", "cooldown"] {
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
         UPDATE pin_lease_targets SET last_touched_at='{}' WHERE id='cooldown'",
            (now + chrono::Duration::hours(1)).to_rfc3339()
        ))
        .await
        .unwrap();

    let generic_started = std::time::Instant::now();
    let tx = store.db().begin().await.unwrap();
    assert_eq!(
        wake_provider_waiters(&tx, "pinata", &limits(), now)
            .await
            .unwrap(),
        ["a", "b", "c", "d"]
    );
    tx.rollback().await.unwrap();
    let generic_elapsed = generic_started.elapsed();

    let mut begin_sql = Vec::new();
    for (owner, expected, unique) in [
        (store.clone(), vec!["a", "b", "c", "d"], 3),
        (store.clone(), vec!["e", "f"], 5),
    ] {
        let session = SharedDecodeBudget::default();
        let work = DecodeWork::shared(&session);
        let started = std::time::Instant::now();
        let tx = owner.begin_runtime(work).await.unwrap();
        begin_sql.push(session.lock().unwrap().task_usage().unwrap().sql);
        assert_eq!(
            wake_runtime_provider_waiters(&tx, "pinata", &limits(), now, work)
                .await
                .unwrap(),
            expected
        );
        work.with(|session| session.check()).unwrap();
        tx.commit().await.unwrap();
        let used = session.lock().unwrap().task_usage().unwrap();
        eprintln!(
            "runtime wake {}: {:?}, begin SQL={}, total SQL={}, rows={}, bytes={}; generic first batch {:?}",
            begin_sql.len(),
            started.elapsed(),
            begin_sql.last().unwrap(),
            used.sql,
            used.rows,
            used.raw_bytes,
            generic_elapsed
        );
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
        begin_sql[0] > begin_sql[1],
        "cold qualification must be charged; the clone reuses its Store proof"
    );
    for id in ["inactive", "cooldown"] {
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
    store
        .db()
        .execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET last_touched_at='{}' WHERE id='tail'",
            (now + chrono::Duration::hours(1)).to_rfc3339()
        ))
        .await
        .unwrap();
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
    let tx = store.begin_runtime(work).await.unwrap();
    assert_eq!(
        wake_runtime_provider_waiters(&tx, "pinata", &limits(), now, work)
            .await
            .unwrap(),
        ["head"]
    );
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
        ReservationOutcome::Reserved
    );
    let reservation_bytes = baseline.lock().unwrap().task_usage().unwrap().raw_bytes;
    tx.rollback().await.unwrap();
    store
        .db()
        .execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET last_touched_at='{}' WHERE id='tail'",
            now.to_rfc3339()
        ))
        .await
        .unwrap();
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
    let tx = store.begin_runtime(work).await.unwrap();
    work.with(|session| session.consume(0, prior_work as usize))
        .unwrap();
    let result = wake_runtime_provider_waiters(&tx, "pinata", &limits(), now, work).await;
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
async fn runtime_wake_operator_debt_preserves_the_queued_admission() {
    let store = fixture().await;
    let now = Utc::now();
    waiter(&store, "head", "bafy-head", now).await;
    let route =
        crate::pinning::identity::ProviderIdentity::legacy("pinata", "ipfs", None, "kubo", "cid")
            .route_snapshot();
    remote_pin_ledger::Entity::insert(remote_pin_ledger::ActiveModel {
        provider: Set("pinata".into()),
        cid: Set("bafy-unresolved".into()),
        route: Set(Some(serde_json::to_string(&route).unwrap())),
        ownership: Set("unknown".into()),
        effect: Set("unknown".into()),
        ..Default::default()
    })
    .exec(store.db())
    .await
    .unwrap();
    assert_eq!(
        super::super::ledger::submission::admission_barrier(store.db(), "pinata")
            .await
            .unwrap(),
        super::super::ledger::submission::AdmissionBarrier::Operator
    );
    let session = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&session);
    let tx = store.begin_runtime(work).await.unwrap();
    assert!(
        wake_runtime_provider_waiters(&tx, "pinata", &limits(), now, work)
            .await
            .unwrap()
            .is_empty()
    );
    work.with(|session| session.check()).unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id("head")
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "quota_waiting"
    );
    let usage = read_usage(store.db(), "pinata").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (0, 0));
    assert_eq!(
        remote_pin::Entity::find().count(store.db()).await.unwrap(),
        0
    );
    assert_eq!(pin_job::Entity::find().count(store.db()).await.unwrap(), 0);
}

#[tokio::test]
async fn runtime_wake_rechecks_live_readiness_before_even_an_empty_candidate_scan() {
    let store = fixture().await;
    store
        .qualify_runtime_schema(DecodeWork::shared(&SharedDecodeBudget::default()))
        .await
        .unwrap();
    store
        .db()
        .execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
        .await
        .unwrap();
    let session = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&session);
    let tx = store.begin_runtime(work).await.unwrap();
    assert!(
        wake_runtime_provider_waiters(&tx, "pinata", &limits(), Utc::now(), work)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(pin_job::Entity::find().count(store.db()).await.unwrap(), 0);
}
