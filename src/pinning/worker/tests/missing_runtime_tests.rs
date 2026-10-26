use super::*;
use crate::store::{
    entities::{pin_invocation_route, pin_provider_route},
    pinning::{ledger, management::records::SharedDecodeBudget},
};

async fn claim(fixture: &Fixture) -> jobs::ClaimedPinJob {
    jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
}

async fn execute(fixture: &Fixture, claimed: jobs::ClaimedPinJob) {
    super::super::execute_claimed_job(
        &fixture.store,
        &PinningCoordinator::disabled_for_test(),
        &Arc::new(Semaphore::new(2)),
        claimed,
    )
    .await
    .unwrap();
    assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn missing_runtime_entry_parks_captured_identity_without_runtime_before_exhaustion() {
    let fixture = fixture([]).await;
    let db = fixture.store.db();
    ledger::register_route(
        db,
        "noop",
        fixture.coordinator.provider_identity("noop").unwrap(),
    )
    .await
    .unwrap();
    ledger::capture_reservation(db, "noop", "bafy-worker")
        .await
        .unwrap();
    fixture.enqueue_poll("captured-runtime").await;
    db.execute_unprepared("UPDATE pin_jobs SET attempts=8")
        .await
        .unwrap();
    let claimed = claim(&fixture).await;
    let id = claimed.model.id.clone();
    execute(&fixture, claimed).await;
    let row = pin_job::Entity::find_by_id(id)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "running");
    assert_eq!(row.attempts, 8);
    assert!(row.locked_until.is_none());
    assert!(row.last_error.is_some());
}

#[tokio::test]
async fn missing_runtime_entry_rejects_registered_poll_without_capture_and_unknown_domain() {
    for registered in [true, false] {
        let fixture = fixture([]).await;
        let db = fixture.store.db();
        if registered {
            ledger::register_route(
                db,
                "noop",
                fixture.coordinator.provider_identity("noop").unwrap(),
            )
            .await
            .unwrap();
            ledger::capture_reservation(db, "noop", "bafy-worker")
                .await
                .unwrap();
            fixture.enqueue_poll("missing-capture").await;
            pin_invocation_route::Entity::delete_many()
                .exec(db)
                .await
                .unwrap();
        } else {
            jobs::enqueue_job(
                db,
                jobs::reconcile_job("domain:unknown", "bafy-worker", 1, Utc::now()),
            )
            .await
            .unwrap();
        }
        let claimed = claim(&fixture).await;
        let id = claimed.model.id.clone();
        execute(&fixture, claimed).await;
        let row = pin_job::Entity::find_by_id(id)
            .one(db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, "running");
        assert_eq!(row.attempts, 0);
        assert!(row.locked_until.is_none());
        assert!(row.last_error.is_some());
        if registered {
            assert!(
                pin_provider_route::Entity::find_by_id("noop".to_owned())
                    .one(db)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }
}

#[tokio::test]
async fn missing_runtime_entry_propagates_invalid_registered_route_without_settlement() {
    use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};
    let fixture = fixture([]).await;
    let db = fixture.store.db();
    let identity = fixture.coordinator.provider_identity("noop").unwrap();
    ledger::register_route(db, "noop", identity).await.unwrap();
    ledger::capture_reservation(db, "noop", "bafy-worker")
        .await
        .unwrap();
    fixture.enqueue_poll("registered-drift").await;
    let claimed = claim(&fixture).await;
    let mut registered = pin_provider_route::Entity::find_by_id("noop".to_owned())
        .one(db)
        .await
        .unwrap()
        .unwrap()
        .into_active_model();
    let mut drifted = identity.clone();
    drifted.credential_revision += 1;
    registered.snapshot = Set(serde_json::to_string(&drifted.route_snapshot()).unwrap());
    registered.update(db).await.unwrap();
    let result = super::super::execute_claimed_job(
        &fixture.store,
        &PinningCoordinator::disabled_for_test(),
        &Arc::new(Semaphore::new(2)),
        claimed.clone(),
    )
    .await;
    assert!(result.is_err());
    let row = pin_job::Entity::find_by_id(claimed.model.id.clone())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row, claimed.model);
    assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn missing_runtime_entry_stale_claim_cannot_retry_or_park_successor() {
    let fixture = fixture([]).await;
    fixture.enqueue_poll("stale-claim").await;
    let claimed = claim(&fixture).await;
    let successors = jobs::claim_due_jobs(
        fixture.store.db(),
        claimed.model.locked_until.unwrap() + chrono::Duration::seconds(1),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap();
    assert_eq!(successors.len(), 1);
    let before = pin_job::Entity::find_by_id(claimed.model.id.clone())
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    execute(&fixture, claimed).await;
    let after = pin_job::Entity::find_by_id(before.id.clone())
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after, before);
}

#[tokio::test]
async fn missing_runtime_entry_stale_reconcile_finishes_and_enqueues_current_epoch_atomically() {
    let fixture = fixture([]).await;
    jobs::enqueue_job(
        fixture.store.db(),
        jobs::reconcile_job("noop", "bafy-worker", 1, Utc::now()),
    )
    .await
    .unwrap();
    let claimed = claim(&fixture).await;
    let id = claimed.model.id.clone();
    fixture
        .store
        .db()
        .execute_unprepared("UPDATE remote_pins SET epoch=2")
        .await
        .unwrap();
    execute(&fixture, claimed).await;
    let old = pin_job::Entity::find_by_id(id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.state, "done");
    assert_eq!(old.attempts, 0);
    assert!(old.locked_until.is_none());
    let current = pin_job::Entity::find()
        .filter(pin_job::Column::ExpectedRemoteEpoch.eq(2))
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.state, "pending");
    assert_eq!(current.operation, "reconcile");
}

#[tokio::test]
async fn missing_runtime_entry_cancelled_claim_is_untouched() {
    let fixture = fixture([]).await;
    fixture.enqueue_poll("cancelled-runtime").await;
    let claimed = claim(&fixture).await;
    let before = claimed.model.clone();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    super::super::execute_claimed_job_with_cancellation(
        &fixture.store,
        &PinningCoordinator::disabled_for_test(),
        &Arc::new(Semaphore::new(2)),
        claimed,
        &cancellation,
    )
    .await
    .unwrap();
    assert_eq!(
        pin_job::Entity::find_by_id(before.id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn missing_runtime_candidate_preserves_exhausted_admission_work_and_releases_transaction() {
    let fixture = fixture([]).await;
    fixture.enqueue_poll("budget-runtime").await;
    let claimed = claim(&fixture).await;
    let work = SharedDecodeBudget::default();
    work.lock()
        .unwrap()
        .nodes(crate::pinning::management::wire::MAX_WIRE_NODES + 1)
        .unwrap_err();
    let result = super::super::unavailable::settle_missing_runtime(
        &fixture.store,
        &PinningCoordinator::disabled_for_test(),
        &claimed,
        work,
    )
    .await;
    assert!(result.is_err());
    let tx = fixture.store.db().begin().await.unwrap();
    tx.execute_unprepared("UPDATE pin_jobs SET attempts=attempts WHERE id IS NOT NULL")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        pin_job::Entity::find_by_id(claimed.model.id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap(),
        claimed.model
    );
}
