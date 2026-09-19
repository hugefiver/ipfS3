use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Duration as ChronoDuration;
use sea_orm::TransactionTrait;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    error::{AppError, AppResult},
    kubo::{KuboClient, LocalResidencyVerificationReceipt},
    residency::{ClaimedResidencyBackfill, ResidencyBackfillCursor},
    store::{
        Store,
        residency::{
            checkpoint_residency_backfill_in_transaction, claim_residency_backfill,
            mark_hot_verified_in_transaction, pending_hot_residency_page,
            release_residency_backfill_in_transaction,
        },
    },
};

// One item plus one row of look-ahead is enough to advance a stable cursor and
// identify the end of a pass without loading a large batch before external IO.
const PAGE_SIZE: u64 = 2;
const CLAIM_LEASE: ChronoDuration = ChronoDuration::minutes(30);
const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct ResidencyBackfillWorkerHandle {
    cancellation: CancellationToken,
    join: JoinHandle<()>,
}

impl ResidencyBackfillWorkerHandle {
    pub async fn shutdown(self, grace: Duration) {
        self.cancellation.cancel();
        let mut join = self.join;
        if tokio::time::timeout(grace, &mut join).await.is_err() {
            join.abort();
            let _ = join.await;
        }
    }
}

/// Starts the durable hot-residency verification backfill.
///
/// Startup is non-blocking. Kubo verification runs outside database
/// transactions, and the returned handle owns all in-flight work.
pub fn start_worker(
    store: Store,
    kubo: KuboClient,
    parent: CancellationToken,
) -> ResidencyBackfillWorkerHandle {
    start_worker_with_verifier(
        store,
        Arc::new(KuboResidencyVerifier(kubo)),
        parent,
        WorkerSettings {
            worker_id: format!("residency-backfill-{}", uuid::Uuid::new_v4()),
            claim_lease: CLAIM_LEASE,
            page_size: PAGE_SIZE,
            idle_poll_interval: IDLE_POLL_INTERVAL,
        },
    )
}

#[async_trait]
trait ResidencyVerifier: Send + Sync + 'static {
    async fn verify_local_residency(
        &self,
        cid: &str,
    ) -> AppResult<LocalResidencyVerificationReceipt>;
}

struct KuboResidencyVerifier(KuboClient);

#[async_trait]
impl ResidencyVerifier for KuboResidencyVerifier {
    async fn verify_local_residency(
        &self,
        cid: &str,
    ) -> AppResult<LocalResidencyVerificationReceipt> {
        self.0.verify_local_residency(cid).await
    }
}

#[derive(Clone)]
struct WorkerSettings {
    worker_id: String,
    claim_lease: ChronoDuration,
    page_size: u64,
    idle_poll_interval: Duration,
}

fn start_worker_with_verifier<V: ResidencyVerifier>(
    store: Store,
    verifier: Arc<V>,
    parent: CancellationToken,
    settings: WorkerSettings,
) -> ResidencyBackfillWorkerHandle {
    let cancellation = parent.child_token();
    let worker_cancellation = cancellation.clone();
    let join = tokio::spawn(async move {
        run_worker(store, verifier, worker_cancellation, settings).await;
    });
    ResidencyBackfillWorkerHandle { cancellation, join }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessOutcome {
    Progress,
    Idle,
    Contended,
    Cancelled,
}

async fn run_worker<V: ResidencyVerifier>(
    store: Store,
    verifier: Arc<V>,
    cancellation: CancellationToken,
    settings: WorkerSettings,
) {
    loop {
        let outcome = process_one(&store, verifier.as_ref(), &cancellation, &settings).await;
        match outcome {
            Ok(ProcessOutcome::Progress) => continue,
            Ok(ProcessOutcome::Cancelled) => break,
            Ok(ProcessOutcome::Idle | ProcessOutcome::Contended) => {}
            Err(_) => tracing::error!(failure = "residency_backfill_iteration"),
        }

        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = tokio::time::sleep(settings.idle_poll_interval) => {}
        }
    }
}

async fn process_one<V: ResidencyVerifier>(
    store: &Store,
    verifier: &V,
    cancellation: &CancellationToken,
    settings: &WorkerSettings,
) -> AppResult<ProcessOutcome> {
    let claim = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Ok(ProcessOutcome::Cancelled),
        result = claim_residency_backfill(
            store.db(),
            &settings.worker_id,
            settings.claim_lease,
        ) => result?,
    };
    let Some(claim) = claim else {
        return Ok(ProcessOutcome::Contended);
    };

    if cancellation.is_cancelled() {
        release_claim(store, &claim).await?;
        return Ok(ProcessOutcome::Cancelled);
    }

    let page =
        pending_hot_residency_page(store.db(), claim.cursor.as_ref(), settings.page_size).await?;
    let Some(item) = page.items.first() else {
        let checkpointed = checkpoint_claim(store, &claim, None, true).await?;
        return Ok(if checkpointed {
            ProcessOutcome::Idle
        } else {
            ProcessOutcome::Contended
        });
    };

    let receipt = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            release_claim(store, &claim).await?;
            return Ok(ProcessOutcome::Cancelled);
        }
        result = verifier.verify_local_residency(&item.identity.cid) => result,
    };

    let next_cursor = ResidencyBackfillCursor {
        version_row_id: item.identity.version_row_id.clone(),
    };
    let completes_pass = page.complete && page.items.len() == 1;

    let receipt = match receipt {
        Ok(receipt) if receipt.cid == item.identity.cid && !receipt.node_identity.is_empty() => {
            receipt
        }
        Ok(_) => {
            tracing::warn!(failure = "residency_verification_receipt_mismatch");
            let checkpointed =
                checkpoint_claim(store, &claim, Some(&next_cursor), completes_pass).await?;
            return Ok(checkpoint_outcome(checkpointed, completes_pass));
        }
        Err(_) => {
            tracing::warn!(failure = "residency_verification");
            let checkpointed =
                checkpoint_claim(store, &claim, Some(&next_cursor), completes_pass).await?;
            return Ok(checkpoint_outcome(checkpointed, completes_pass));
        }
    };

    let serialized_receipt = serde_json::to_string(&receipt).map_err(|_| {
        AppError::Internal("local residency receipt serialization failed".to_owned())
    })?;
    let txn = store.db().begin().await?;
    let marked = mark_hot_verified_in_transaction(
        &txn,
        &item.identity,
        &receipt.node_identity,
        &serialized_receipt,
    )
    .await;
    if marked.is_err() {
        txn.rollback().await?;
        tracing::warn!(failure = "residency_verification_owner_revalidation");
        let checkpointed =
            checkpoint_claim(store, &claim, Some(&next_cursor), completes_pass).await?;
        return Ok(checkpoint_outcome(checkpointed, completes_pass));
    }

    // Owner/version rows are locked by mark_hot_verified first. The durable
    // claim row is deliberately fenced second to preserve global lock order.
    let checkpointed = checkpoint_residency_backfill_in_transaction(
        &txn,
        &claim,
        Some(&next_cursor),
        completes_pass,
    )
    .await?;
    if !checkpointed {
        txn.rollback().await?;
        return Ok(ProcessOutcome::Contended);
    }
    txn.commit().await?;
    Ok(checkpoint_outcome(true, completes_pass))
}

fn checkpoint_outcome(checkpointed: bool, completes_pass: bool) -> ProcessOutcome {
    if checkpointed && completes_pass {
        ProcessOutcome::Idle
    } else if checkpointed {
        ProcessOutcome::Progress
    } else {
        ProcessOutcome::Contended
    }
}

async fn checkpoint_claim(
    store: &Store,
    claim: &ClaimedResidencyBackfill,
    next_cursor: Option<&ResidencyBackfillCursor>,
    complete: bool,
) -> AppResult<bool> {
    let txn = store.db().begin().await?;
    let checkpointed =
        checkpoint_residency_backfill_in_transaction(&txn, claim, next_cursor, complete).await?;
    if checkpointed {
        txn.commit().await?;
    } else {
        txn.rollback().await?;
    }
    Ok(checkpointed)
}

async fn release_claim(store: &Store, claim: &ClaimedResidencyBackfill) -> AppResult<()> {
    let txn = store.db().begin().await?;
    let released = release_residency_backfill_in_transaction(&txn, claim).await?;
    if released {
        txn.commit().await?;
    } else {
        txn.rollback().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use sea_orm::{ActiveModelTrait, ActiveValue::Set, Database, EntityTrait, TransactionTrait};
    use tokio::sync::{Mutex, Notify};

    use super::*;
    use crate::{
        residency::{PhysicalVerification, VerificationState, VersionResidencyIdentity},
        store::{
            entities::{object, object_version, physical_residency, residency_backfill},
            residency::{attach_hot_in_transaction, resolve_version_residency},
            run_migrations,
        },
    };

    const CID_A: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    const CID_B: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";
    const CID_C: &str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3did5lntuhkvelu7m7q4b6mhy";

    fn test_settings(worker_id: &str) -> WorkerSettings {
        WorkerSettings {
            worker_id: worker_id.to_owned(),
            claim_lease: ChronoDuration::seconds(5),
            page_size: PAGE_SIZE,
            idle_poll_interval: Duration::from_millis(10),
        }
    }

    async fn test_store() -> Store {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        Store::new(db)
    }

    async fn seed_pending(store: &Store, suffix: &str, cid: &str) {
        let now = crate::store::database_clock::database_now(store.db())
            .await
            .unwrap();
        let object_id = format!("object-{suffix}");
        let version_id = format!("version-{suffix}");
        let key = format!("key-{suffix}");
        object::ActiveModel {
            id: Set(object_id.clone()),
            bucket: Set("bucket".to_owned()),
            key: Set(key.clone()),
            cid: Set(cid.to_owned()),
            size: Set(1),
            content_type: Set(None),
            etag: Set(cid.to_owned()),
            metadata: Set(None),
            encrypted: Set(false),
            key_wrap: Set(None),
            sse_c_key_fingerprint: Set(None),
            multipart: Set(false),
            is_latest: Set(true),
            created_at: Set(now),
        }
        .insert(store.db())
        .await
        .unwrap();
        object_version::ActiveModel {
            id: Set(version_id.clone()),
            bucket: Set("bucket".to_owned()),
            key: Set(key),
            version_id: Set(None),
            kind: Set("object".to_owned()),
            object_id: Set(Some(object_id.clone())),
            sequence: Set(1),
            is_latest: Set(true),
            lifecycle_age_started_at: Set(now),
            became_noncurrent_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(store.db())
        .await
        .unwrap();
        let txn = store.db().begin().await.unwrap();
        attach_hot_in_transaction(
            &txn,
            &VersionResidencyIdentity::new(version_id, object_id, cid),
            &PhysicalVerification::Pending,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    async fn wait_verified(store: &Store, version_id: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if resolve_version_residency(store.db(), version_id)
                    .await
                    .is_ok_and(|residency| {
                        residency.physical.verification_state == VerificationState::Verified
                    })
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn wait_pass_complete(store: &Store) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = residency_backfill::Entity::find_by_id("hot_verification")
                    .one(store.db())
                    .await
                    .unwrap()
                    .unwrap();
                if state.completed && state.claimed_by.is_none() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    struct ScriptedVerifier {
        failures: Mutex<HashMap<String, usize>>,
        calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ResidencyVerifier for ScriptedVerifier {
        async fn verify_local_residency(
            &self,
            cid: &str,
        ) -> AppResult<LocalResidencyVerificationReceipt> {
            self.calls.lock().await.push(cid.to_owned());
            let mut failures = self.failures.lock().await;
            if failures.get_mut(cid).is_some_and(|remaining| {
                if *remaining == 0 {
                    false
                } else {
                    *remaining -= 1;
                    true
                }
            }) {
                return Err(AppError::Internal("injected verifier failure".to_owned()));
            }
            Ok(LocalResidencyVerificationReceipt {
                node_identity: "test-node".to_owned(),
                cid: cid.to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn failed_complete_pass_yields_before_retrying() {
        let store = test_store().await;
        seed_pending(&store, "a", CID_A).await;
        let verifier = ScriptedVerifier {
            failures: Mutex::new(HashMap::from([(CID_A.to_owned(), usize::MAX)])),
            calls: Mutex::new(Vec::new()),
        };
        assert_eq!(
            process_one(
                &store,
                &verifier,
                &CancellationToken::new(),
                &test_settings("worker-a"),
            )
            .await
            .unwrap(),
            ProcessOutcome::Idle,
            "an offline hot node must not cause a tight completed-pass retry loop",
        );
    }

    #[tokio::test]
    async fn retries_failures_fairly_and_revisits_completed_scans_for_new_writes() {
        let store = test_store().await;
        seed_pending(&store, "a", CID_A).await;
        seed_pending(&store, "b", CID_B).await;
        let verifier = Arc::new(ScriptedVerifier {
            failures: Mutex::new(HashMap::from([(CID_A.to_owned(), 1)])),
            calls: Mutex::new(Vec::new()),
        });
        let handle = start_worker_with_verifier(
            store.clone(),
            verifier.clone(),
            CancellationToken::new(),
            test_settings("worker-a"),
        );

        wait_verified(&store, "version-a").await;
        wait_verified(&store, "version-b").await;
        wait_pass_complete(&store).await;
        assert_eq!(
            &verifier.calls.lock().await[..3],
            &[CID_A.to_owned(), CID_B.to_owned(), CID_A.to_owned()],
            "a failed item must not block b and must be retried on the next pass"
        );

        // This ID sorts before the previous cursor, proving a completed pass is
        // restarted rather than treated as a permanent terminal state.
        seed_pending(&store, "0", CID_C).await;
        wait_verified(&store, "version-0").await;
        handle.shutdown(Duration::from_secs(1)).await;
    }

    struct GateVerifier {
        started: AtomicBool,
        started_notify: Notify,
        released: AtomicBool,
        release: Notify,
    }

    impl GateVerifier {
        async fn wait_started(&self) {
            let notified = self.started_notify.notified();
            if !self.started.load(Ordering::Acquire) {
                notified.await;
            }
        }

        fn release(&self) {
            self.released.store(true, Ordering::Release);
            self.release.notify_waiters();
        }
    }

    #[async_trait]
    impl ResidencyVerifier for GateVerifier {
        async fn verify_local_residency(
            &self,
            cid: &str,
        ) -> AppResult<LocalResidencyVerificationReceipt> {
            self.started.store(true, Ordering::Release);
            self.started_notify.notify_waiters();
            while !self.released.load(Ordering::Acquire) {
                self.release.notified().await;
            }
            Ok(LocalResidencyVerificationReceipt {
                node_identity: "test-node".to_owned(),
                cid: cid.to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn deletion_during_verification_cannot_write_a_stale_receipt() {
        let store = test_store().await;
        seed_pending(&store, "a", CID_A).await;
        let verifier = Arc::new(GateVerifier {
            started: AtomicBool::new(false),
            started_notify: Notify::new(),
            released: AtomicBool::new(false),
            release: Notify::new(),
        });
        let handle = start_worker_with_verifier(
            store.clone(),
            verifier.clone(),
            CancellationToken::new(),
            test_settings("worker-a"),
        );
        verifier.wait_started().await;

        let now = crate::store::database_clock::database_now(store.db())
            .await
            .unwrap();
        let guard = crate::store::import::ownership::admit_content_mutation(
            store.db(),
            "bucket",
            "key-a",
            None,
            crate::import::SupersedeReason::DeleteObject,
            now,
        )
        .await
        .unwrap();
        crate::store::pinning::publication::delete_version_with_leases_guarded(
            store.db(),
            "bucket",
            "key-a",
            crate::store::object_version::VersionSelector::Current,
            guard,
            now,
        )
        .await
        .unwrap();
        verifier.release();
        wait_pass_complete(&store).await;

        let physical = physical_residency::Entity::find_by_id(("hot".to_owned(), CID_A.to_owned()))
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(physical.verification_state, "pending");
        assert!(physical.verification_receipt.is_none());
        handle.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn expired_epoch_cannot_commit_a_verification_receipt() {
        let store = test_store().await;
        seed_pending(&store, "a", CID_A).await;
        let verifier = Arc::new(GateVerifier {
            started: AtomicBool::new(false),
            started_notify: Notify::new(),
            released: AtomicBool::new(false),
            release: Notify::new(),
        });
        let mut settings = test_settings("worker-a");
        settings.claim_lease = ChronoDuration::milliseconds(50);
        let worker_store = store.clone();
        let worker_verifier = verifier.clone();
        let worker = tokio::spawn(async move {
            process_one(
                &worker_store,
                worker_verifier.as_ref(),
                &CancellationToken::new(),
                &settings,
            )
            .await
        });
        verifier.wait_started().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let replacement =
            claim_residency_backfill(store.db(), "worker-b", ChronoDuration::seconds(5))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(replacement.claim_epoch, 2);

        verifier.release();
        assert_eq!(
            worker.await.unwrap().unwrap(),
            ProcessOutcome::Contended,
            "the old worker must lose its receipt write when the epoch changes"
        );
        let physical = physical_residency::Entity::find_by_id(("hot".to_owned(), CID_A.to_owned()))
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(physical.verification_state, "pending");
        let state = residency_backfill::Entity::find_by_id("hot_verification")
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.claim_epoch, 2);
        assert_eq!(state.claimed_by.as_deref(), Some("worker-b"));

        let txn = store.db().begin().await.unwrap();
        assert!(
            release_residency_backfill_in_transaction(&txn, &replacement)
                .await
                .unwrap()
        );
        txn.commit().await.unwrap();
    }

    struct ActiveGuard(Arc<AtomicUsize>);

    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }

    struct NeverVerifier {
        active: Arc<AtomicUsize>,
        started: Notify,
    }

    #[async_trait]
    impl ResidencyVerifier for NeverVerifier {
        async fn verify_local_residency(
            &self,
            _cid: &str,
        ) -> AppResult<LocalResidencyVerificationReceipt> {
            self.active.fetch_add(1, Ordering::AcqRel);
            let _guard = ActiveGuard(self.active.clone());
            self.started.notify_waiters();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_and_joins_in_flight_verification() {
        let store = test_store().await;
        seed_pending(&store, "a", CID_A).await;
        let active = Arc::new(AtomicUsize::new(0));
        let verifier = Arc::new(NeverVerifier {
            active: active.clone(),
            started: Notify::new(),
        });
        let handle = start_worker_with_verifier(
            store.clone(),
            verifier.clone(),
            CancellationToken::new(),
            test_settings("worker-a"),
        );
        let notified = verifier.started.notified();
        if active.load(Ordering::Acquire) == 0 {
            notified.await;
        }

        handle.shutdown(Duration::from_secs(1)).await;
        assert_eq!(active.load(Ordering::Acquire), 0);
        let state = residency_backfill::Entity::find_by_id("hot_verification")
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(state.claimed_by.is_none());
    }
}
