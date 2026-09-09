use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use sea_orm::{DatabaseConnection, TransactionTrait};
use tokio::{
    sync::{Mutex, Notify},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::ValidatedLifecycleConfig,
    error::AppResult,
    lifecycle::{
        actions::execute_claimed_lifecycle_action,
        evaluator::schedule_claimed_scan_page,
        model::{ClaimedLifecycleAction, ClaimedLifecycleScan},
    },
    store::{
        Store,
        lifecycle_config::{claim_next_scan, finish_scan_page_in_transaction},
    },
};

pub struct LifecycleWorkerHandle {
    cancellation: CancellationToken,
    join: JoinHandle<()>,
}

impl LifecycleWorkerHandle {
    pub async fn shutdown(self, grace: Duration) {
        self.cancellation.cancel();
        let mut join = self.join;
        if tokio::time::timeout(grace, &mut join).await.is_err() {
            join.abort();
            let _ = join.await;
        }
    }

    #[doc(hidden)]
    pub fn abort_for_test(self) -> JoinHandle<()> {
        self.join.abort();
        self.join
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct LifecycleWorkerTestControl {
    pub worker_id: String,
    pub after_claim: Option<Arc<LifecycleAfterClaimGate>>,
}

#[doc(hidden)]
pub struct LifecycleAfterClaimGate {
    expected_worker_id: String,
    claimed: Mutex<Option<ClaimedLifecycleAction>>,
    arrived: Notify,
    resume: Notify,
    released: AtomicBool,
}

impl LifecycleAfterClaimGate {
    pub fn new(expected_worker_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            expected_worker_id: expected_worker_id.into(),
            claimed: Mutex::new(None),
            arrived: Notify::new(),
            resume: Notify::new(),
            released: AtomicBool::new(false),
        })
    }

    pub async fn wait_claim(&self) -> ClaimedLifecycleAction {
        loop {
            let notified = self.arrived.notified();
            if let Some(claim) = self.claimed.lock().await.clone() {
                return claim;
            }
            notified.await;
        }
    }

    pub fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.resume.notify_waiters();
    }

    async fn after_claim(&self, worker_id: &str, claim: &ClaimedLifecycleAction) {
        assert_eq!(worker_id, self.expected_worker_id);
        let mut claimed = self.claimed.lock().await;
        assert!(claimed.is_none(), "test control accepts one claimed action");
        *claimed = Some(claim.clone());
        drop(claimed);
        self.arrived.notify_waiters();

        loop {
            let notified = self.resume.notified();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

pub fn start_worker(
    store: Store,
    config: ValidatedLifecycleConfig,
    parent: CancellationToken,
) -> LifecycleWorkerHandle {
    let cancellation = parent.child_token();
    let worker_cancellation = cancellation.clone();
    let worker_id = format!("lifecycle-worker-{}", uuid::Uuid::new_v4());
    let join = tokio::spawn(async move {
        run_worker(store, config, worker_cancellation, worker_id, None).await;
    });
    LifecycleWorkerHandle { cancellation, join }
}

#[doc(hidden)]
pub fn start_worker_for_test(
    store: Store,
    config: ValidatedLifecycleConfig,
    parent: CancellationToken,
    control: LifecycleWorkerTestControl,
) -> LifecycleWorkerHandle {
    let cancellation = parent.child_token();
    let worker_cancellation = cancellation.clone();
    let join = tokio::spawn(async move {
        run_worker(
            store,
            config,
            worker_cancellation,
            control.worker_id,
            control.after_claim,
        )
        .await;
    });
    LifecycleWorkerHandle { cancellation, join }
}

async fn run_worker(
    store: Store,
    config: ValidatedLifecycleConfig,
    cancellation: CancellationToken,
    worker_id: String,
    after_claim: Option<Arc<LifecycleAfterClaimGate>>,
) {
    let mut actions = JoinSet::new();
    let mut poll = tokio::time::interval(config.poll_interval);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            joined = actions.join_next(), if !actions.is_empty() => {
                log_join_result(joined, "action_task");
            }
            _ = poll.tick() => {
                if cancellation.is_cancelled() {
                    break;
                }
                while let Some(joined) = actions.try_join_next() {
                    log_join_result(Some(joined), "action_task");
                }
                if !scan_one_page(&store, &config, &cancellation).await {
                    break;
                }
                if !claim_and_start_actions(
                    &store,
                    &config,
                    &worker_id,
                    &cancellation,
                    after_claim.as_deref(),
                    &mut actions,
                )
                .await
                {
                    break;
                }
            }
        }
    }

    while let Some(joined) = actions.join_next().await {
        log_join_result(Some(joined), "action_task_drain");
    }
}

fn log_join_result(joined: Option<Result<(), tokio::task::JoinError>>, failure: &'static str) {
    if matches!(joined, Some(Err(_))) {
        tracing::error!(failure);
    }
}

async fn scan_one_page(
    store: &Store,
    config: &ValidatedLifecycleConfig,
    cancellation: &CancellationToken,
) -> bool {
    let claim = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return false,
        result = claim_next_scan(store.db(), config.scan_lease) => result,
    };
    let claim = match claim {
        Ok(Some(claim)) => claim,
        Ok(None) => return true,
        Err(_) => {
            tracing::error!(failure = "scan_claim");
            return true;
        }
    };

    let completed = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return false,
        result = schedule_and_finish_scan_page(
            store.db(),
            &claim,
            config.scan_page_size,
            #[cfg(test)]
            async |_| Ok(()),
        ) => result,
    };
    match completed {
        Ok(true) => {
            scan_page_finished_for_test().await;
            true
        }
        Ok(false) => true,
        Err(_) => {
            tracing::error!(failure = "scan_finish");
            true
        }
    }
}

async fn schedule_and_finish_scan_page(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleScan,
    page_limit: u64,
    #[cfg(test)] after_schedule: impl AsyncFnOnce(&sea_orm::DatabaseTransaction) -> AppResult<()>,
) -> AppResult<bool> {
    // Dropping this future before commit drops the transaction and rolls the page back.
    let txn = db.begin().await?;
    let result = async {
        let page = schedule_claimed_scan_page(&txn, claim, page_limit).await?;
        #[cfg(test)]
        after_schedule(&txn).await?;
        finish_scan_page_in_transaction(&txn, claim, page.next_cursor.as_ref(), page.cycle_complete)
            .await
    }
    .await;
    match result {
        Ok(true) => {
            txn.commit().await?;
            Ok(true)
        }
        result => {
            // A false fence is not a successful transaction: none of its inserts may escape.
            txn.rollback().await?;
            result
        }
    }
}

async fn claim_and_start_actions(
    store: &Store,
    config: &ValidatedLifecycleConfig,
    worker_id: &str,
    cancellation: &CancellationToken,
    after_claim: Option<&LifecycleAfterClaimGate>,
    actions: &mut JoinSet<()>,
) -> bool {
    let available = config.worker_concurrency.saturating_sub(actions.len());
    if available == 0 {
        return true;
    }
    let limit = u64::try_from(available).expect("validated lifecycle worker concurrency fits u64");
    let claims = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return false,
        result = crate::store::lifecycle_action::claim_due_with_max_attempts(
            store.db(),
            worker_id,
            config.action_lease,
            config.max_attempts,
            limit,
        ) => result,
    };
    let claims = match claims {
        Ok(claims) => claims,
        Err(_) => {
            tracing::error!(failure = "action_claim");
            return true;
        }
    };

    for claim in claims {
        if cancellation.is_cancelled() {
            return false;
        }
        if let Some(gate) = after_claim {
            gate.after_claim(worker_id, &claim).await;
        }
        let store = store.clone();
        let config = config.clone();
        actions.spawn(async move {
            pause_before_action_execution_for_test().await;
            if execute_claimed_lifecycle_action(
                store.db(),
                &claim,
                config.max_attempts,
                config.base_backoff_secs,
                config.max_backoff_secs,
            )
            .await
            .is_err()
            {
                tracing::error!(failure = "action_execute");
            }
        });
    }
    true
}

#[cfg(test)]
async fn pause_before_action_execution_for_test() {
    let gate = test_hooks::ACTION_GATE.lock().unwrap().clone();
    if let Some(gate) = gate {
        gate.started.notify_one();
        gate.resume.notified().await;
    }
}

#[cfg(not(test))]
async fn pause_before_action_execution_for_test() {}

#[cfg(test)]
async fn scan_page_finished_for_test() {
    if let Some(gate) = test_hooks::SCAN_GATE.lock().unwrap().clone() {
        gate.completed.notify_one();
    }
}

#[cfg(not(test))]
async fn scan_page_finished_for_test() {}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::{Arc, Mutex};

    use tokio::sync::Notify;

    pub static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    pub(super) static ACTION_GATE: Mutex<Option<Arc<ActionGate>>> = Mutex::new(None);
    pub(super) static SCAN_GATE: Mutex<Option<Arc<ScanGate>>> = Mutex::new(None);

    pub struct ActionGate {
        pub started: Notify,
        pub resume: Notify,
    }

    impl ActionGate {
        pub fn new() -> Self {
            Self {
                started: Notify::new(),
                resume: Notify::new(),
            }
        }
    }

    pub struct ScanGate {
        pub completed: Notify,
    }

    impl ScanGate {
        pub fn new() -> Self {
            Self {
                completed: Notify::new(),
            }
        }
    }

    enum InstalledGate {
        Action,
        Scan,
    }

    pub struct GateScope(InstalledGate);

    impl Drop for GateScope {
        fn drop(&mut self) {
            match self.0 {
                InstalledGate::Action => *ACTION_GATE.lock().unwrap() = None,
                InstalledGate::Scan => *SCAN_GATE.lock().unwrap() = None,
            }
        }
    }

    pub fn install_action_gate(gate: Arc<ActionGate>) -> GateScope {
        *ACTION_GATE.lock().unwrap() = Some(gate);
        GateScope(InstalledGate::Action)
    }

    pub fn install_scan_gate(gate: Arc<ScanGate>) -> GateScope {
        *SCAN_GATE.lock().unwrap() = Some(gate);
        GateScope(InstalledGate::Scan)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use chrono::Duration as ChronoDuration;
    use sea_orm::{
        ColumnTrait, ConnectOptions, Database, EntityTrait, QueryFilter, sea_query::Expr,
    };
    use tokio_util::sync::CancellationToken;

    use super::{
        LifecycleAfterClaimGate, LifecycleWorkerTestControl, start_worker, start_worker_for_test,
        test_hooks,
    };
    use crate::{
        config::{LifecycleWorkerConfig, ValidatedLifecycleConfig},
        lifecycle::{
            config::canonical_json,
            model::{
                AbortIncompleteMultipartUploadAction, CanonicalFilter,
                CanonicalLifecycleConfiguration, CanonicalLifecycleRule, CanonicalRuleSelector,
                CurrentExpiration, LifecycleActionKind, LifecycleRuleStatus, NewLifecycleAction,
                RuleIdentity, VersionTargetIdentity,
            },
        },
        store::{
            Store, bucket,
            database_clock::database_now,
            entities::{
                bucket_lifecycle_config, lifecycle_action, multipart_part, multipart_upload,
            },
            lifecycle_action::{claim_due, idempotency_key, insert_idempotent},
            lifecycle_config::put_configuration,
            multipart,
            object_version::{PublicVersionId, VersionKind},
            run_migrations,
        },
    };

    const ACTION_START_TIMEOUT: Duration = Duration::from_secs(5);

    fn worker_config() -> ValidatedLifecycleConfig {
        LifecycleWorkerConfig {
            poll_interval_ms: 1,
            scan_page_size: 1,
            scan_lease_secs: 30,
            action_lease_secs: 30,
            worker_concurrency: 1,
            max_attempts: 8,
            base_backoff_secs: 1,
            max_backoff_secs: 60,
        }
        .validate()
        .unwrap()
    }

    async fn setup_store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("lifecycle-worker.sqlite")
                .display()
                .to_string()
                .replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(1).min_connections(1);
        let db = Database::connect(options).await.unwrap();
        sea_orm::ConnectionTrait::execute_unprepared(&db, "PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        run_migrations(&db).await.unwrap();
        bucket::create(&db, "bucket", None).await.unwrap();
        (directory, Store::new(db))
    }

    async fn insert_due_action(store: &Store) -> String {
        let now = database_now(store.db()).await.unwrap();
        let mut action = NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: "bucket".to_owned(),
            config_revision: 1,
            rule_identity: RuleIdentity::Id("expire".to_owned()),
            action_kind: LifecycleActionKind::ExpireCurrent,
            target: crate::lifecycle::model::LifecycleTargetIdentity::Version(
                VersionTargetIdentity {
                    bucket: "bucket".to_owned(),
                    key: "object".to_owned(),
                    version_row_id: "missing-version".to_owned(),
                    public_version_id: PublicVersionId::Null,
                    kind: VersionKind::Object,
                    object_id: Some("missing-object".to_owned()),
                    sequence: 1,
                },
            ),
            due_at: now,
        };
        action.idempotency_key = idempotency_key(&action).unwrap();
        let idempotency_key = action.idempotency_key.clone();
        assert!(insert_idempotent(store.db(), action, now).await.unwrap());
        lifecycle_action::Entity::find()
            .filter(lifecycle_action::Column::IdempotencyKey.eq(idempotency_key))
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .id
    }

    async fn action_state(store: &Store, id: &str) -> String {
        lifecycle_action::Entity::find_by_id(id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .state
    }

    fn configuration() -> CanonicalLifecycleConfiguration {
        CanonicalLifecycleConfiguration {
            schema_version: 1,
            rules: vec![CanonicalLifecycleRule {
                id: Some("expire".to_owned()),
                status: LifecycleRuleStatus::Enabled,
                selector: CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::All,
                },
                expiration: Some(CurrentExpiration::Days { days: 1 }),
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: None,
            }],
        }
    }

    fn multipart_configuration() -> CanonicalLifecycleConfiguration {
        CanonicalLifecycleConfiguration {
            schema_version: 1,
            rules: vec![CanonicalLifecycleRule {
                id: Some("abort".to_owned()),
                status: LifecycleRuleStatus::Enabled,
                selector: CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::All,
                },
                expiration: None,
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUploadAction {
                    days_after_initiation: 1,
                }),
            }],
        }
    }

    async fn insert_due_multipart_upload(store: &Store, upload_id: &str) {
        multipart::create_upload(
            store.db(),
            upload_id,
            &format!("object-{upload_id}"),
            "bucket",
            "multipart-object",
            "none",
            None,
            None,
            Some("application/octet-stream"),
            None,
            &[],
            None,
            false,
        )
        .await
        .unwrap();
        multipart::upsert_part(store.db(), upload_id, 1, "cid-part-1", 5, "etag-part-1")
            .await
            .unwrap();
        multipart::upsert_part(store.db(), upload_id, 2, "cid-part-2", 7, "etag-part-2")
            .await
            .unwrap();

        let initiated_at = database_now(store.db()).await.unwrap() - ChronoDuration::days(3);
        let updated = multipart_upload::Entity::update_many()
            .col_expr(
                multipart_upload::Column::CreatedAt,
                Expr::value(initiated_at),
            )
            .filter(multipart_upload::Column::UploadId.eq(upload_id))
            .exec(store.db())
            .await
            .unwrap();
        assert_eq!(updated.rows_affected, 1);
    }

    async fn configure_multipart_abort(store: &Store) {
        put_configuration(
            store.db(),
            "bucket",
            &canonical_json(&multipart_configuration()).unwrap(),
        )
        .await
        .unwrap();
    }

    async fn assert_multipart_rows(store: &Store, upload_id: &str, expected_parts: usize) {
        assert!(
            multipart_upload::Entity::find_by_id(upload_id)
                .one(store.db())
                .await
                .unwrap()
                .is_some(),
            "multipart upload must still exist"
        );
        let parts = multipart_part::Entity::find()
            .filter(multipart_part::Column::UploadId.eq(upload_id))
            .all(store.db())
            .await
            .unwrap();
        assert_eq!(parts.len(), expected_parts);
    }

    async fn wait_for_multipart_action_state(
        store: &Store,
        upload_id: &str,
        expected_state: &str,
    ) -> lifecycle_action::Model {
        let waited = tokio::time::timeout(ACTION_START_TIMEOUT, async {
            loop {
                if let Some(action) = lifecycle_action::Entity::find()
                    .filter(lifecycle_action::Column::TargetUploadId.eq(upload_id))
                    .one(store.db())
                    .await
                    .unwrap()
                    && action.state == expected_state
                {
                    return action;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        match waited {
            Ok(action) => action,
            Err(_) => {
                let actual = lifecycle_action::Entity::find()
                    .filter(lifecycle_action::Column::TargetUploadId.eq(upload_id))
                    .one(store.db())
                    .await
                    .unwrap()
                    .map(|action| action.state);
                panic!("multipart action must reach {expected_state}; observed state: {actual:?}");
            }
        }
    }

    #[tokio::test]
    async fn scan_max_days_advances_cursor_and_schedules_later_candidates() {
        let (_directory, store) = setup_store().await;
        let mut rules = multipart_configuration();
        let mut future = rules.rules[0].clone();
        future.id = Some("far-future".to_owned());
        future.abort_incomplete_multipart_upload = Some(AbortIncompleteMultipartUploadAction {
            days_after_initiation: i32::MAX as u32,
        });
        rules.rules[0].selector = CanonicalRuleSelector::LegacyPrefix {
            prefix: "b-".to_owned(),
        };
        rules.rules.insert(0, future);
        put_configuration(store.db(), "bucket", &canonical_json(&rules).unwrap())
            .await
            .unwrap();
        for key in ["a-future", "b-due", "c-future"] {
            insert_due_multipart_upload(&store, key).await;
            multipart_upload::Entity::update_many()
                .col_expr(multipart_upload::Column::Key, Expr::value(key))
                .filter(multipart_upload::Column::UploadId.eq(key))
                .exec(store.db())
                .await
                .unwrap();
        }
        let cancellation = CancellationToken::new();
        let mut config = worker_config();
        config.scan_page_size = 2;
        assert!(super::scan_one_page(&store, &config, &cancellation).await);
        let saved = bucket_lifecycle_config::Entity::find_by_id("bucket")
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        let cursor = crate::store::lifecycle_scan::decode_cursor(
            saved
                .scan_cursor
                .as_deref()
                .expect("page must advance its cursor"),
            "bucket",
        )
        .unwrap();
        assert_eq!(cursor.key, "b-due");
        assert!(saved.last_scanned_at.is_some());
        assert!(saved.scan_lease_until.is_none());
        let actions = lifecycle_action::Entity::find()
            .all(store.db())
            .await
            .unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].target_upload_id.as_deref(), Some("b-due"));
        assert!(super::scan_one_page(&store, &config, &cancellation).await);
        let saved = bucket_lifecycle_config::Entity::find_by_id("bucket")
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(
            saved.scan_cursor.is_none(),
            "the next page completes normally"
        );
        assert_eq!(
            lifecycle_action::Entity::find()
                .all(store.db())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn scan_finish_fence_failure_rolls_back_scheduled_actions() {
        for invalidation in ["epoch", "revision", "expired"] {
            let (_directory, store) = setup_store().await;
            configure_multipart_abort(&store).await;
            insert_due_multipart_upload(&store, "rollback-upload").await;
            let claim = crate::store::lifecycle_config::claim_next_scan(
                store.db(),
                ChronoDuration::seconds(30),
            )
            .await
            .unwrap()
            .unwrap();
            let before = bucket_lifecycle_config::Entity::find_by_id("bucket")
                .one(store.db())
                .await
                .unwrap()
                .unwrap();
            let completed =
                super::schedule_and_finish_scan_page(store.db(), &claim, 1, async |db| {
                    assert_eq!(
                        lifecycle_action::Entity::find().all(db).await?.len(),
                        1,
                        "invalidation must happen after the action insert"
                    );
                    let update = bucket_lifecycle_config::Entity::update_many();
                    let update = match invalidation {
                        "epoch" => update.col_expr(
                            bucket_lifecycle_config::Column::ScanLeaseEpoch,
                            Expr::value(claim.lease_epoch + 1),
                        ),
                        "revision" => update
                            .col_expr(
                                bucket_lifecycle_config::Column::Revision,
                                Expr::value(claim.config_revision + 1),
                            )
                            .col_expr(
                                bucket_lifecycle_config::Column::CanonicalJson,
                                Expr::value(canonical_json(&configuration()).unwrap()),
                            ),
                        "expired" => update.col_expr(
                            bucket_lifecycle_config::Column::ScanLeaseUntil,
                            Expr::value(claim.database_now - ChronoDuration::seconds(1)),
                        ),
                        _ => unreachable!(),
                    };
                    update
                        .filter(bucket_lifecycle_config::Column::Bucket.eq("bucket"))
                        .exec(db)
                        .await?;
                    Ok(())
                })
                .await
                .unwrap();
            assert!(!completed, "{invalidation}");
            assert!(
                lifecycle_action::Entity::find()
                    .all(store.db())
                    .await
                    .unwrap()
                    .is_empty(),
                "failed {invalidation} fence must roll back the action insert"
            );
            let after = bucket_lifecycle_config::Entity::find_by_id("bucket")
                .one(store.db())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                after, before,
                "the entire simulated page transaction rolls back"
            );
        }
    }

    #[tokio::test]
    async fn scan_stale_claim_preserves_committed_replacement() {
        let (_directory, store) = setup_store().await;
        configure_multipart_abort(&store).await;
        insert_due_multipart_upload(&store, "stale-upload").await;
        let claim = crate::store::lifecycle_config::claim_next_scan(
            store.db(),
            ChronoDuration::seconds(30),
        )
        .await
        .unwrap()
        .unwrap();
        put_configuration(
            store.db(),
            "bucket",
            &canonical_json(&configuration()).unwrap(),
        )
        .await
        .unwrap();
        let replacement = bucket_lifecycle_config::Entity::find_by_id("bucket")
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(
            !super::schedule_and_finish_scan_page(store.db(), &claim, 1, async |db| {
                assert_eq!(lifecycle_action::Entity::find().all(db).await?.len(), 1);
                Ok(())
            })
            .await
            .unwrap()
        );
        assert!(
            lifecycle_action::Entity::find()
                .all(store.db())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            bucket_lifecycle_config::Entity::find_by_id("bucket")
                .one(store.db())
                .await
                .unwrap()
                .unwrap(),
            replacement
        );
    }

    #[tokio::test]
    async fn scan_cancelled_after_scheduling_does_not_commit_a_partial_page() {
        let (_directory, store) = setup_store().await;
        configure_multipart_abort(&store).await;
        insert_due_multipart_upload(&store, "cancel-upload").await;
        let claim = crate::store::lifecycle_config::claim_next_scan(
            store.db(),
            ChronoDuration::seconds(30),
        )
        .await
        .unwrap()
        .unwrap();
        let before = bucket_lifecycle_config::Entity::find_by_id("bucket")
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        let cancellation = CancellationToken::new();
        let process = super::schedule_and_finish_scan_page(store.db(), &claim, 1, async |db| {
            assert_eq!(lifecycle_action::Entity::find().all(db).await?.len(), 1);
            cancellation.cancel();
            std::future::pending::<crate::error::AppResult<()>>().await
        });
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {},
            result = process => panic!("page should be cancelled: {result:?}"),
        }
        assert!(
            lifecycle_action::Entity::find()
                .all(store.db())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            bucket_lifecycle_config::Entity::find_by_id("bucket")
                .one(store.db())
                .await
                .unwrap()
                .unwrap(),
            before
        );
    }

    #[tokio::test]
    async fn lifecycle_worker_starts_and_fences_a_scan_page() {
        let _lock = test_hooks::TEST_LOCK.lock().await;
        let (_directory, store) = setup_store().await;
        put_configuration(
            store.db(),
            "bucket",
            &canonical_json(&configuration()).unwrap(),
        )
        .await
        .unwrap();
        let gate = Arc::new(test_hooks::ScanGate::new());
        let _scope = test_hooks::install_scan_gate(gate.clone());
        let scanned = gate.completed.notified();

        let handle = start_worker(store.clone(), worker_config(), CancellationToken::new());
        tokio::time::timeout(Duration::from_secs(1), scanned)
            .await
            .expect("worker must finish one scan page");
        handle.shutdown(Duration::from_secs(1)).await;

        let config = bucket_lifecycle_config::Entity::find_by_id("bucket")
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(config.last_scanned_at.is_some());
        assert!(config.scan_lease_until.is_none());
    }

    #[tokio::test]
    async fn lifecycle_worker_stops_before_claiming_after_cancellation() {
        let (_directory, store) = setup_store().await;
        let action_id = insert_due_action(&store).await;
        let parent = CancellationToken::new();
        parent.cancel();

        let handle = start_worker(store.clone(), worker_config(), parent);
        handle.shutdown(Duration::from_secs(1)).await;

        assert_eq!(action_state(&store, &action_id).await, "pending");
    }

    #[tokio::test]
    async fn lifecycle_worker_drains_in_flight_actions_before_shutdown() {
        let _lock = test_hooks::TEST_LOCK.lock().await;
        let (_directory, store) = setup_store().await;
        let action_id = insert_due_action(&store).await;
        let gate = Arc::new(test_hooks::ActionGate::new());
        let _scope = test_hooks::install_action_gate(gate.clone());
        let started = gate.started.notified();

        let handle = start_worker(store.clone(), worker_config(), CancellationToken::new());
        tokio::time::timeout(ACTION_START_TIMEOUT, started)
            .await
            .expect("worker must start the claimed action");
        let shutdown = tokio::spawn(handle.shutdown(Duration::from_secs(1)));
        assert!(
            !shutdown.is_finished(),
            "shutdown must drain the already-started action"
        );
        gate.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown must complete after the action drains")
            .unwrap();

        assert_ne!(action_state(&store, &action_id).await, "claimed");
    }

    #[tokio::test]
    async fn lifecycle_worker_forced_shutdown_leaves_claimed_action_reclaimable() {
        let _lock = test_hooks::TEST_LOCK.lock().await;
        let (_directory, store) = setup_store().await;
        let action_id = insert_due_action(&store).await;
        let gate = Arc::new(test_hooks::ActionGate::new());
        let _scope = test_hooks::install_action_gate(gate.clone());
        let started = gate.started.notified();

        let handle = start_worker(store.clone(), worker_config(), CancellationToken::new());
        tokio::time::timeout(ACTION_START_TIMEOUT, started)
            .await
            .expect("worker must start the claimed action");
        handle.shutdown(Duration::ZERO).await;

        let claimed = lifecycle_action::Entity::find_by_id(action_id.clone())
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.state, "claimed");
        assert!(claimed.lease_until.is_some());

        let now = database_now(store.db()).await.unwrap();
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(now - ChronoDuration::seconds(1))),
            )
            .filter(lifecycle_action::Column::Id.eq(action_id.clone()))
            .exec(store.db())
            .await
            .unwrap();
        let reclaimed = claim_due(
            store.db(),
            "replacement-worker",
            ChronoDuration::seconds(30),
            1,
        )
        .await
        .unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].action.id, action_id);
    }

    #[tokio::test]
    async fn multipart_worker_claims_and_executes_abort_with_existing_generic_worker() {
        let _lock = test_hooks::TEST_LOCK.lock().await;
        let (_directory, store) = setup_store().await;
        configure_multipart_abort(&store).await;
        insert_due_multipart_upload(&store, "worker-upload").await;
        let gate = Arc::new(test_hooks::ActionGate::new());
        let _scope = test_hooks::install_action_gate(gate.clone());
        let started = gate.started.notified();

        let handle = start_worker(store.clone(), worker_config(), CancellationToken::new());
        tokio::time::timeout(ACTION_START_TIMEOUT, started)
            .await
            .expect("generic worker must claim the multipart action");
        assert_multipart_rows(&store, "worker-upload", 2).await;
        gate.resume.notify_one();

        let terminal = wait_for_multipart_action_state(&store, "worker-upload", "succeeded").await;
        handle.shutdown(Duration::from_secs(1)).await;
        assert_eq!(terminal.action_kind, "abort_incomplete_multipart_upload");
        assert_eq!(terminal.target_type, "multipart_upload");
        assert!(
            multipart_upload::Entity::find_by_id("worker-upload")
                .one(store.db())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            multipart_part::Entity::find()
                .filter(multipart_part::Column::UploadId.eq("worker-upload"))
                .all(store.db())
                .await
                .unwrap()
                .is_empty(),
            "successful abort must cascade to multipart parts"
        );
    }

    #[tokio::test]
    async fn multipart_worker_reclaims_expired_claim_after_abort_and_succeeds() {
        let _lock = test_hooks::TEST_LOCK.lock().await;
        let (_directory, store) = setup_store().await;
        configure_multipart_abort(&store).await;
        insert_due_multipart_upload(&store, "reclaimed-upload").await;
        let after_claim = LifecycleAfterClaimGate::new("worker-a");
        let handle = start_worker_for_test(
            store.clone(),
            worker_config(),
            CancellationToken::new(),
            LifecycleWorkerTestControl {
                worker_id: "worker-a".to_owned(),
                after_claim: Some(after_claim.clone()),
            },
        );

        let claim = tokio::time::timeout(ACTION_START_TIMEOUT, after_claim.wait_claim())
            .await
            .expect("worker-a must claim the multipart action");
        handle.abort_for_test().await.unwrap_err();
        assert_eq!(claim.action.target_type, "multipart_upload");
        assert_eq!(
            claim.action.target_upload_id.as_deref(),
            Some("reclaimed-upload")
        );
        assert_multipart_rows(&store, "reclaimed-upload", 2).await;

        let now = database_now(store.db()).await.unwrap();
        let expired = lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(now - ChronoDuration::seconds(1))),
            )
            .filter(lifecycle_action::Column::Id.eq(claim.action.id.clone()))
            .exec(store.db())
            .await
            .unwrap();
        assert_eq!(expired.rows_affected, 1);

        let gate = Arc::new(test_hooks::ActionGate::new());
        let _scope = test_hooks::install_action_gate(gate.clone());
        let started = gate.started.notified();
        let replacement = start_worker(store.clone(), worker_config(), CancellationToken::new());
        tokio::time::timeout(ACTION_START_TIMEOUT, started)
            .await
            .expect("generic replacement worker must reclaim the expired multipart action");
        assert_multipart_rows(&store, "reclaimed-upload", 2).await;
        gate.resume.notify_one();

        let terminal =
            wait_for_multipart_action_state(&store, "reclaimed-upload", "succeeded").await;
        replacement.shutdown(Duration::from_secs(1)).await;
        assert_eq!(terminal.id, claim.action.id);
        assert!(terminal.claim_epoch > claim.claim_epoch);
        assert_eq!(terminal.state, "succeeded");
        assert!(
            multipart_upload::Entity::find_by_id("reclaimed-upload")
                .one(store.db())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            multipart_part::Entity::find()
                .filter(multipart_part::Column::UploadId.eq("reclaimed-upload"))
                .all(store.db())
                .await
                .unwrap()
                .is_empty()
        );
    }
}
