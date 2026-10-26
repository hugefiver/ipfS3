use super::*;
use crate::{
    pinning::worker::{current_unpin_request, prepare_unpin_writer},
    store::{
        entities::remote_pin_ledger,
        pinning::management::{actions::job_authority, records::SharedDecodeBudget},
    },
};
use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};

mod fixture;
mod fresh_reads;
mod runtime_tests;

#[tokio::test]
async fn missing_runtime_managed_cleanup_falls_back_with_closed_transaction_and_shared_debt() {
    let fixture = fixture::confirmed_cleanup().await;
    let work = SharedDecodeBudget::default();
    let before = work.lock().unwrap().usage();
    let effects = NativeEffects::read(&fixture.store, &fixture.claimed.model.id).await;
    let result = crate::pinning::worker::unavailable::settle_missing_runtime(
        &fixture.store,
        &PinningCoordinator::disabled_for_test(),
        &fixture.claimed,
        work.clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        crate::pinning::worker::unavailable::MissingRuntime::ExistingAdmission
    ));
    let after = work.lock().unwrap().usage();
    assert_ne!(before, after);
    let tx = fixture.store.db().begin().await.unwrap();
    tx.execute_unprepared("UPDATE pin_jobs SET attempts=attempts WHERE id IS NOT NULL")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        NativeEffects::read(&fixture.store, &fixture.claimed.model.id).await,
        effects
    );
    assert_eq!(
        pin_job::Entity::find_by_id(fixture.claimed.model.id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap(),
        fixture.claimed.model
    );
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn managed_unpin_writer_parks_revoked_cleanup_without_recording_ordinary_retention() {
    let fixture = fixture::confirmed_cleanup().await;
    let store = &fixture.store;
    let claimed = &fixture.claimed;
    let authority = job_authority::check_before_call_store(
        store,
        &claimed.model,
        SharedDecodeBudget::default(),
    )
    .await
    .expect("confirmed cleanup must have valid attributed authority");
    let Some(job_authority::JobPermit::ExistingCleanup(initial)) = authority else {
        panic!("fixture must start with ExistingCleanup, not LedgerOnly or ordinary authority")
    };
    assert!(initial == fixture.authority);
    let request = current_unpin_request(store, &claimed.model)
        .await
        .unwrap()
        .expect("cleanup must observe a current no-reference unpin request");
    assert_eq!(Some(request.as_str()), initial.resource_id.as_deref());

    // Native evidence changes after the request observation, without modifying
    // any receipt, locator, seal, route, job identity, epoch or quota row.
    let tx = store.db().begin().await.unwrap();
    let ledger = remote_pin_ledger::Entity::find_by_id((
        claimed.model.provider.clone(),
        claimed.model.cid.clone(),
    ))
    .one(&tx)
    .await
    .unwrap()
    .unwrap();
    let mut unknown = ledger.into_active_model();
    unknown.ownership = Set("unknown".into());
    unknown.update(&tx).await.unwrap();
    tx.commit().await.unwrap();

    let fresh = job_authority::check_before_call_store(
        store,
        &claimed.model,
        SharedDecodeBudget::default(),
    )
    .await
    .expect("ownership downgrade must remain valid evidence, not a schema/attribution error");
    let Some(job_authority::JobPermit::LedgerOnly(revoked)) = fresh else {
        panic!("fresh authority must successfully downgrade ExistingCleanup to LedgerOnly")
    };
    assert!(revoked == initial);
    let before = NativeEffects::read(store, &claimed.model.id).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let cleanup = before_wall_deadline(
        deadline,
        prepare_unpin_writer(store, claimed, claimed.model.expected_remote_epoch.unwrap()),
    )
    .await
    .expect("unpin writer did not finish within its original wall guard")
    .unwrap();
    assert!(!cleanup, "revoked cleanup must not proceed to DELETE");

    let job = pin_job::Entity::find_by_id(claimed.model.id.clone())
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        job.state, "running",
        "revoked managed work needs attention, not ordinary completion"
    );
    assert!(job.locked_until.is_none());
    assert!(job.last_error.is_some());
    assert_eq!(job.attempts, claimed.model.attempts);
    assert_eq!(NativeEffects::read(store, &claimed.model.id).await, before);
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
}

#[derive(Debug, PartialEq)]
struct NativeEffects {
    ledger: Vec<remote_pin_ledger::Model>,
    remotes: Vec<remote_pin::Model>,
    usage: Vec<pin_provider_usage::Model>,
    leases: Vec<pin_lease::Model>,
    targets: Vec<pin_lease_target::Model>,
    other_jobs: Vec<pin_job::Model>,
}

impl NativeEffects {
    async fn read(store: &Store, own_job: &str) -> Self {
        let db = store.db();
        Self {
            ledger: remote_pin_ledger::Entity::find().all(db).await.unwrap(),
            remotes: remote_pin::Entity::find().all(db).await.unwrap(),
            usage: pin_provider_usage::Entity::find().all(db).await.unwrap(),
            leases: pin_lease::Entity::find().all(db).await.unwrap(),
            targets: pin_lease_target::Entity::find().all(db).await.unwrap(),
            other_jobs: pin_job::Entity::find()
                .filter(pin_job::Column::Id.ne(own_job))
                .order_by_asc(pin_job::Column::Id)
                .all(db)
                .await
                .unwrap(),
        }
    }
}

#[tokio::test]
async fn ordinary_unpin_without_cleanup_authority_keeps_retained_done_and_quota() {
    let fixture = super::fixture([]).await;
    fixture
        .store
        .db()
        .execute_unprepared(
            "UPDATE pin_leases SET state='cancelled',generation=generation+1; \
         UPDATE pin_lease_targets SET state='released'; \
         UPDATE remote_pins SET status='pinned',request_id='ordinary-retained'; \
         INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) \
         VALUES('noop','bafy-worker','unknown','confirmed')",
        )
        .await
        .unwrap();
    jobs::enqueue_job(
        fixture.store.db(),
        jobs::unpin_job("noop", "bafy-worker", 1, Utc::now()),
    )
    .await
    .unwrap();
    let claimed = jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert!(
        job_authority::check_before_call_store(
            &fixture.store,
            &claimed.model,
            SharedDecodeBudget::default(),
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        current_unpin_request(&fixture.store, &claimed.model)
            .await
            .unwrap()
            .as_deref(),
        Some("ordinary-retained")
    );
    let before = NativeEffects::read(&fixture.store, &claimed.model.id).await;
    assert!(
        !prepare_unpin_writer(&fixture.store, &claimed, 1)
            .await
            .unwrap()
    );
    let job = pin_job::Entity::find_by_id(claimed.model.id.clone())
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "done");
    assert!(job.locked_until.is_none());
    assert_eq!(job.attempts, claimed.model.attempts);
    let mut after = NativeEffects::read(&fixture.store, &claimed.model.id).await;
    assert_eq!(after.ledger.len(), 1);
    assert_eq!(after.ledger[0].effect, "retained");
    after.ledger[0].effect.clone_from(&before.ledger[0].effect);
    assert_eq!(after, before);
    assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
}
