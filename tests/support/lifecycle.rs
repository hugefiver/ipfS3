//! Deterministic signed-S3 lifecycle acceptance support.
//!
//! The production worker remains the execution boundary.  This harness controls
//! only durable test state and worker lifetime; S3 requests still traverse the
//! in-process service and object content still traverses mock Kubo.

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectOptions, ConnectionTrait, Database, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, Set,
};
use tokio::sync::{Barrier, Mutex, Notify};
use tokio_util::sync::CancellationToken;

use ipfs_s3_gateway::{
    config::{LifecycleWorkerConfig, ValidatedLifecycleConfig},
    lifecycle::{
        evaluator::schedule_claimed_scan_page,
        model::ClaimedLifecycleAction,
        worker::{LifecycleWorkerHandle, start_worker},
    },
    state::AppState,
    store::{
        Store,
        database_clock::database_now,
        entities::{lifecycle_action, object_version},
        lifecycle_action::claim_due,
        lifecycle_config::{claim_next_scan, finish_scan_page},
    },
};

use super::decompress::{
    KuboHarness, KuboScript, ObservedHttpRequest, S3ServerHandle, S3TestEndpoint,
    start_kubo_harness, start_s3_server,
};
use super::residency::assert_hot_standard_residency_invariant;

const WORKER_WAIT: Duration = Duration::from_secs(10);
const WORKER_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

pub struct LifecycleHarness {
    pub endpoint: String,
    pub bucket: String,
    pub owner: String,
    pub state: Arc<AppState>,
    pub kubo: wiremock::MockServer,
    pub cold_kubo: Option<wiremock::MockServer>,
    worker: Option<LifecycleWorkerHandle>,
    cancellation: CancellationToken,
    s3_server: Option<S3ServerHandle>,
    _database_directory: tempfile::TempDir,
}

impl S3TestEndpoint for LifecycleHarness {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}

pub async fn start_lifecycle_harness(script: KuboScript) -> LifecycleHarness {
    start_lifecycle_harness_inner(script, false).await
}

pub async fn start_lifecycle_harness_with_cold(script: KuboScript) -> LifecycleHarness {
    start_lifecycle_harness_inner(script, true).await
}

async fn start_lifecycle_harness_inner(
    script: KuboScript,
    enable_cold_kubo: bool,
) -> LifecycleHarness {
    let KuboHarness { server: kubo, .. } = start_kubo_harness(script).await;
    let cold_kubo = if enable_cold_kubo {
        Some(wiremock::MockServer::start().await)
    } else {
        None
    };
    let database_directory = tempfile::tempdir().expect("create lifecycle SQLite directory");
    let database_path = database_directory.path().join(format!(
        "lifecycle-{}.sqlite",
        uuid::Uuid::new_v4().simple()
    ));
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database_path.display().to_string().replace('\\', "/")
    );
    let mut options = ConnectOptions::new(database_url);
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options)
        .await
        .expect("connect isolated lifecycle SQLite database");
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .expect("enable lifecycle SQLite foreign keys");
    ipfs_s3_gateway::store::run_migrations(&db)
        .await
        .expect("run lifecycle test migrations");

    let bucket = "lifecycle-test-bucket".to_owned();
    let owner = "lifecycle-test-owner".to_owned();
    ipfs_s3_gateway::store::bucket::create(&db, &bucket, Some(&owner))
        .await
        .expect("create lifecycle test bucket");
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: cold_kubo
            .as_ref()
            .map(|server| ipfs_s3_gateway::kubo::KuboClient::new(server.uri())),
        store: Store::new(db),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64))
            .expect("create lifecycle test master key"),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let observed_http = Arc::new(Mutex::new(Vec::<ObservedHttpRequest>::new()));
    let s3_server = start_s3_server(state.clone(), observed_http).await;

    LifecycleHarness {
        endpoint: s3_server.endpoint.clone(),
        bucket,
        owner,
        state,
        kubo,
        cold_kubo,
        worker: None,
        cancellation: CancellationToken::new(),
        s3_server: Some(s3_server),
        _database_directory: database_directory,
    }
}

#[cfg(test)]
#[path = "lifecycle_transition_api.rs"]
mod lifecycle_transition_api;

impl LifecycleHarness {
    /// Overrides the immutable lifecycle timestamps for one known public version.
    /// This is a test clock control; all scan, claim, and execution decisions still
    /// use the database clock in production code.
    pub async fn set_database_times(
        &self,
        version_row_id: &str,
        lifecycle_age_started_at: DateTime<Utc>,
        became_noncurrent_at: Option<DateTime<Utc>>,
    ) {
        let version = object_version::Entity::find_by_id(version_row_id.to_owned())
            .one(self.state.store.db())
            .await
            .expect("load lifecycle version clock target")
            .expect("lifecycle version clock target exists");
        let mut active = version.into_active_model();
        active.lifecycle_age_started_at = Set(lifecycle_age_started_at);
        active.became_noncurrent_at = Set(became_noncurrent_at);
        active
            .update(self.state.store.db())
            .await
            .expect("update lifecycle version clock target");
    }

    /// Claims, schedules, and finishes exactly one public lifecycle scan page.
    pub async fn run_one_scan_page(&self) -> usize {
        let claim = claim_next_scan(self.state.store.db(), ChronoDuration::seconds(30))
            .await
            .expect("claim lifecycle scan page")
            .expect("lifecycle configuration is scan-claimable");
        let page = schedule_claimed_scan_page(self.state.store.db(), &claim, 1_000)
            .await
            .expect("schedule lifecycle scan page");
        assert!(
            finish_scan_page(
                self.state.store.db(),
                &claim,
                page.next_cursor.as_ref(),
                page.cycle_complete,
            )
            .await
            .expect("finish lifecycle scan page"),
            "lifecycle scan claim must retain its fence"
        );
        page.candidates.len()
    }

    /// Claims one due durable action without invoking any private lifecycle API.
    pub async fn claim_one_action(&self) -> ClaimedLifecycleAction {
        let mut claims = claim_due(
            self.state.store.db(),
            "lifecycle-acceptance-claim",
            ChronoDuration::seconds(30),
            1,
        )
        .await
        .expect("claim lifecycle action");
        assert_eq!(claims.len(), 1, "exactly one lifecycle action must be due");
        claims.pop().expect("one claimed lifecycle action")
    }

    /// Lets a fresh production worker reclaim and execute a previously claimed
    /// action after expiring only that durable claim lease.
    pub async fn execute_claim(
        &mut self,
        claim: &ClaimedLifecycleAction,
    ) -> lifecycle_action::Model {
        let database_now = database_now(self.state.store.db())
            .await
            .expect("read lifecycle database clock");
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                sea_orm::sea_query::Expr::value(Some(database_now - ChronoDuration::seconds(1))),
            )
            .filter(lifecycle_action::Column::Id.eq(&claim.action.id))
            .exec(self.state.store.db())
            .await
            .expect("expire lifecycle action claim");

        let terminal = Arc::new(Notify::new());
        let barrier = Arc::new(Barrier::new(2));
        let db = self.state.store.db().clone();
        let action_id = claim.action.id.clone();
        let observer_terminal = terminal.clone();
        let observer_barrier = barrier.clone();
        let observer = tokio::spawn(async move {
            observer_barrier.wait().await;
            loop {
                let action = lifecycle_action::Entity::find_by_id(action_id.clone())
                    .one(&db)
                    .await
                    .expect("observe lifecycle action terminal state")
                    .expect("observed lifecycle action exists");
                if matches!(
                    action.state.as_str(),
                    "succeeded" | "cancelled" | "failed_safe"
                ) {
                    observer_terminal.notify_one();
                    return action;
                }
                tokio::task::yield_now().await;
            }
        });
        let notified = terminal.notified();
        self.restart_worker();
        barrier.wait().await;
        tokio::time::timeout(WORKER_WAIT, notified)
            .await
            .expect("lifecycle worker did not settle the reclaimed action");
        let action = observer.await.expect("join lifecycle action observer");
        self.stop_worker().await;
        action
    }

    /// Starts a fresh production worker. Tokio's first interval tick is immediate,
    /// while the subsequent interval leaves the test observer time to stop after
    /// its one intended reclaimed action.
    pub fn restart_worker(&mut self) {
        assert!(self.worker.is_none(), "lifecycle worker is already running");
        if self.cancellation.is_cancelled() {
            self.cancellation = CancellationToken::new();
        }
        self.worker = Some(start_worker(
            self.state.store.clone(),
            lifecycle_worker_config(),
            self.cancellation.clone(),
        ));
    }

    /// Persists a claim, then cancels the worker parent as a deterministic
    /// process-stop boundary. A later `execute_claim` must reclaim the lease.
    pub async fn stop_after_claim(&mut self) -> ClaimedLifecycleAction {
        let claim = self.claim_one_action().await;
        self.cancellation.cancel();
        self.stop_worker().await;
        claim
    }

    pub async fn action_rows(&self) -> Vec<lifecycle_action::Model> {
        lifecycle_action::Entity::find()
            .order_by_asc(lifecycle_action::Column::DueAt)
            .order_by_asc(lifecycle_action::Column::Id)
            .all(self.state.store.db())
            .await
            .expect("list lifecycle actions")
    }

    pub async fn version_rows(&self, key: &str) -> Vec<object_version::Model> {
        object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq(&self.bucket))
            .filter(object_version::Column::Key.eq(key))
            .order_by_asc(object_version::Column::Sequence)
            .all(self.state.store.db())
            .await
            .expect("list lifecycle public versions")
    }

    pub async fn assert_no_pin_removal(&self) {
        let requests = self
            .kubo
            .received_requests()
            .await
            .expect("read lifecycle Kubo request log");
        assert!(
            requests
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/rm"),
            "lifecycle must not call Kubo pin removal"
        );
    }

    pub async fn shutdown(mut self) {
        self.stop_worker().await;
        if let Some(server) = self.s3_server.take() {
            server.shutdown().await;
        }
        assert_hot_standard_residency_invariant(self.state.store.db()).await;
    }

    async fn stop_worker(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.shutdown(WORKER_SHUTDOWN_GRACE).await;
        }
    }
}

fn lifecycle_worker_config() -> ValidatedLifecycleConfig {
    LifecycleWorkerConfig {
        poll_interval_ms: 1_000,
        scan_page_size: 1_000,
        scan_lease_secs: 30,
        action_lease_secs: 30,
        worker_concurrency: 1,
        max_attempts: 8,
        base_backoff_secs: 1,
        max_backoff_secs: 60,
    }
    .validate()
    .expect("validate lifecycle acceptance worker config")
}
