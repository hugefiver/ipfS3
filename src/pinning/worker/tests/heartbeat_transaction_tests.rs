use super::*;
use std::{future::Future, pin::Pin, task::Poll};

use crate::pinning::worker::{ClaimWait, claim_heartbeat_period, wait_with_claim_heartbeat};
use crate::store::pinning::management::records::{DecodeWork, SharedDecodeBudget};

struct ObservedDrop<F> {
    future: Pin<Box<F>>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}

impl<F: Future> Future for ObservedDrop<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(context)
    }
}

impl<F> Drop for ObservedDrop<F> {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    assert!(
        std::future::poll_fn(|context| Poll::Ready(future.as_mut().poll(context)))
            .await
            .is_pending()
    );
}

async fn cancel_ready_output_before_renewal(
    store: &crate::store::Store,
    claimed: &mut jobs::ClaimedPinJob,
    lock_for: chrono::Duration,
    deadline: std::time::Instant,
) {
    let original = claimed.model.clone();
    assert!(Utc::now() + lock_for * 2 > original.locked_until.unwrap());
    let work = SharedDecodeBudget::default();
    let gate = before_wall_deadline(deadline, store.begin_runtime(DecodeWork::shared(&work)))
        .await
        .expect("the preceding cancellation must release the connection")
        .unwrap();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = semaphore.clone().acquire_owned().await.unwrap();
    let ready = Notify::new();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel = CancellationToken::new();
    let monitored = ObservedDrop {
        future: Box::pin(async {
            ready.notified().await;
            permit
        }),
        dropped: dropped.clone(),
    };
    let mut wait = Box::pin(wait_with_claim_heartbeat(
        store, claimed, lock_for, &cancel, monitored,
    ));
    assert_pending(wait.as_mut()).await;
    tokio::time::advance(claim_heartbeat_period(lock_for).unwrap()).await;
    assert_pending(wait.as_mut()).await;

    // The independent transaction keeps renewal queued after monitored Ready.
    ready.notify_one();
    assert_pending(wait.as_mut()).await;
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(semaphore.available_permits(), 0);
    cancel.cancel();
    assert_pending(wait.as_mut()).await;
    assert_eq!(semaphore.available_permits(), 1);

    before_wall_deadline(deadline, gate.rollback())
        .await
        .expect("the renewal gate must release within the original deadline")
        .unwrap();
    let result = before_wall_deadline(deadline, &mut wait)
        .await
        .expect("cancellation must drain the queued renewal")
        .unwrap();
    drop(wait);
    assert!(matches!(result, ClaimWait::Cancelled));
    let durable = before_wall_deadline(
        deadline,
        pin_job::Entity::find_by_id(original.id).one(store.db()),
    )
    .await
    .expect("the drained renewal must release its connection")
    .unwrap()
    .unwrap();
    assert_eq!(durable.locked_until, claimed.model.locked_until);
    assert!(durable.locked_until > original.locked_until);
    assert_eq!(durable.attempts, original.attempts);
}

enum Completion {
    Rollback,
    Cancel,
    LoseClaim,
}

async fn queued_renewal_with_transaction(completion: Completion) {
    let clock = PausedClockHold::start().await;
    let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
    options
        .max_connections(1)
        .min_connections(1)
        .test_before_acquire(false);
    let db = Database::connect(options).await.unwrap();
    let fixture = fixture_with_database([], db, 10_000, 100, None, "1s").await;
    fixture.enqueue_poll("transaction-heartbeat").await;
    let lock_for = chrono::Duration::seconds(30);
    let mut claimed = jobs::claim_due_jobs(fixture.store.db(), Utc::now(), lock_for, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let original = claimed.model.clone();
    assert!(Utc::now() + lock_for * 2 > original.locked_until.unwrap());

    let entered = Notify::new();
    let release = Notify::new();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rolled_back = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel = CancellationToken::new();
    let monitored = ObservedDrop {
        future: Box::pin(async {
            let work = SharedDecodeBudget::default();
            let txn = fixture
                .store
                .begin_runtime(DecodeWork::shared(&work))
                .await
                .unwrap();
            if matches!(completion, Completion::LoseClaim) {
                use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};
                let mut stolen = original.clone().into_active_model();
                stolen.locked_until = Set(Some(original.locked_until.unwrap() + lock_for * 4));
                stolen.update(&txn).await.unwrap();
            }
            entered.notify_one();
            release.notified().await;
            if matches!(completion, Completion::LoseClaim) {
                txn.commit().await.unwrap();
            } else {
                txn.rollback().await.unwrap();
                rolled_back.store(true, Ordering::SeqCst);
            }
            true
        }),
        dropped: dropped.clone(),
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut wait = Box::pin(wait_with_claim_heartbeat(
        &fixture.store,
        &mut claimed,
        lock_for,
        &cancel,
        monitored,
    ));
    before_wall_deadline(deadline, async {
        tokio::select! {
            biased;
            _ = entered.notified() => {},
            _ = &mut wait => panic!("monitored transaction must wait for release"),
        }
    })
    .await
    .expect("monitored future did not acquire the sole connection");

    tokio::time::advance(claim_heartbeat_period(lock_for).unwrap()).await;
    // The biased monitored branch is Pending at its unreleased Notify. The tick
    // is due, and renewal cannot be a stamp no-op (UTC did not advance with
    // Tokio). Polling the helper now therefore polls renewal Pending while the
    // RuntimeTransaction owns the only pool connection.
    assert!(Utc::now() + lock_for * 2 > original.locked_until.unwrap());
    assert_pending(wait.as_mut()).await;
    assert!(!dropped.load(Ordering::SeqCst));
    assert!(!rolled_back.load(Ordering::SeqCst));

    if matches!(completion, Completion::Cancel) {
        cancel.cancel();
    } else {
        release.notify_one();
    }
    let result = before_wall_deadline(deadline, &mut wait)
        .await
        .expect("queued renewal must not stop monitored progress or cancellation");
    drop(wait);
    assert!(dropped.load(Ordering::SeqCst));

    let durable = before_wall_deadline(
        deadline,
        pin_job::Entity::find_by_id(original.id.clone()).one(fixture.store.db()),
    )
    .await
    .expect("the monitored future must release its transaction before draining renewal")
    .unwrap()
    .unwrap();
    assert_eq!(durable.attempts, original.attempts);
    match completion {
        Completion::Rollback => {
            assert!(matches!(result.unwrap(), ClaimWait::Ready(true)));
            assert!(rolled_back.load(Ordering::SeqCst));
            assert_eq!(durable.locked_until, claimed.model.locked_until);
            assert!(durable.locked_until > original.locked_until);
        }
        Completion::Cancel => {
            assert!(matches!(result.unwrap(), ClaimWait::Cancelled));
            assert!(!rolled_back.load(Ordering::SeqCst));
            assert_eq!(durable.locked_until, claimed.model.locked_until);
            assert!(durable.locked_until > original.locked_until);
            cancel_ready_output_before_renewal(
                &fixture.store,
                &mut claimed,
                lock_for * 2,
                deadline,
            )
            .await;
        }
        Completion::LoseClaim => {
            assert!(
                result.is_err(),
                "a queued renewal CAS failure must win over Ready"
            );
            assert_eq!(
                durable.locked_until,
                Some(original.locked_until.unwrap() + lock_for * 4)
            );
            assert_eq!(claimed.model.locked_until, original.locked_until);
        }
    }
    assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
    clock.finish().await;
}

#[tokio::test(start_paused = true)]
async fn queued_claim_renewal_allows_monitored_transaction_to_rollback() {
    queued_renewal_with_transaction(Completion::Rollback).await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_monitored_transaction_before_draining_queued_renewal() {
    queued_renewal_with_transaction(Completion::Cancel).await;
}

#[tokio::test(start_paused = true)]
async fn queued_renewal_claim_loss_wins_over_monitored_ready() {
    queued_renewal_with_transaction(Completion::LoseClaim).await;
}
