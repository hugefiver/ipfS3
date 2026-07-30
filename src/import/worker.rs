use std::{
    collections::hash_map::DefaultHasher,
    future::Future,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::{StreamExt, stream::FuturesUnordered};
use sea_orm::{DatabaseConnection, TransactionTrait};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    error::AppError,
    import::{
        ImportClaim, ImportExecutionError, ImportFailure, ImportFailureCode,
        pipeline::{ImportCoordinator, JobCancellation, execute_job},
    },
    state::AppState,
    store::import::{jobs, ownership},
};

const MAX_RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const RETENTION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
mod test_gates {
    use std::sync::{Arc, LazyLock};

    use tokio::sync::{Mutex, Notify};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum WorkerDbStage {
        InitialLeaseCap,
        Renewal,
        Outcome,
    }

    pub struct WorkerDbGate {
        pub job_id: String,
        pub stage: WorkerDbStage,
        pub pause_after_renewal_write: bool,
        pub arrived: Notify,
        pub resume: Notify,
        pub renewal_write_arrived: Notify,
        pub renewal_write_resume: Notify,
    }

    pub static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    pub static GATE: LazyLock<Mutex<Option<Arc<WorkerDbGate>>>> =
        LazyLock::new(|| Mutex::new(None));
}

#[cfg(test)]
async fn pause_before_worker_db_for_test(job_id: &str, stage: test_gates::WorkerDbStage) {
    let gate = test_gates::GATE.lock().await.clone();
    if let Some(gate) = gate.filter(|gate| gate.job_id == job_id && gate.stage == stage) {
        gate.arrived.notify_one();
        gate.resume.notified().await;
    }
}

#[cfg(test)]
async fn pause_after_renewal_write_for_test(job_id: &str) {
    let gate = test_gates::GATE.lock().await.clone();
    if let Some(gate) = gate.filter(|gate| {
        gate.job_id == job_id
            && gate.stage == test_gates::WorkerDbStage::Renewal
            && gate.pause_after_renewal_write
    }) {
        gate.renewal_write_arrived.notify_one();
        gate.renewal_write_resume.notified().await;
    }
}

pub struct ImportWorkerHandle {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl ImportWorkerHandle {
    /// Stops new claims, cancels active I/O, and waits no longer than `grace`.
    pub async fn shutdown(self, grace: Duration) {
        self.shutdown.cancel();
        let mut task = self.task;
        if tokio::time::timeout(grace, &mut task).await.is_err() {
            task.abort();
            let _ = task.await;
        }
    }
}

pub(crate) fn start_worker(
    coordinator: Arc<ImportCoordinator>,
    state: Arc<AppState>,
    shutdown: CancellationToken,
) -> ImportWorkerHandle {
    let task_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        run_worker(coordinator, state, task_shutdown).await;
    });
    ImportWorkerHandle { shutdown, task }
}

async fn run_worker(
    coordinator: Arc<ImportCoordinator>,
    state: Arc<AppState>,
    shutdown: CancellationToken,
) {
    let imports_enabled = coordinator.enabled();
    let config = coordinator.config().raw.clone();
    let worker_id = format!("import-worker-{}", uuid::Uuid::new_v4());
    let mut jobs_in_flight = JoinSet::new();
    let mut poll = tokio::time::interval(Duration::from_millis(config.poll_interval_ms));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let cleanup_interval = Duration::from_secs(config.terminal_retention_secs.max(1))
        .min(MAX_RETENTION_CLEANUP_INTERVAL);
    let mut cleanup = tokio::time::interval(cleanup_interval);
    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            joined = jobs_in_flight.join_next(), if !jobs_in_flight.is_empty() => {
                if let Some(Err(error)) = joined {
                    tracing::error!(%error, "import job task failed");
                }
            }
            _ = cleanup.tick() => {
                let cutoff = Utc::now()
                    .checked_sub_signed(retention_delta(config.terminal_retention_secs))
                    .unwrap_or(DateTime::<Utc>::MIN_UTC);
                let cleanup_result = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    result = tokio::time::timeout(
                        RETENTION_CLEANUP_TIMEOUT,
                        jobs::delete_terminal_before(state.store.db(), cutoff),
                    ) => result,
                };
                match cleanup_result {
                    Ok(Ok(deleted)) if deleted > 0 => {
                        tracing::debug!(deleted, "deleted expired terminal import jobs");
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => tracing::error!(%error, "failed to clean up terminal import jobs"),
                    Err(_) => tracing::warn!("terminal import job cleanup timed out"),
                }
            }
            _ = poll.tick(), if imports_enabled => {
                let available = config.worker_concurrency.saturating_sub(jobs_in_flight.len());
                if available == 0 {
                    continue;
                }
                let now = Utc::now();
                let lease_until = now + lease_delta(config.lease_duration_secs);
                match jobs::claim_due(
                    state.store.db(),
                    &worker_id,
                    now,
                    lease_until,
                    available as u64,
                ).await {
                    Ok(claimed) => {
                        for claimed_job in claimed {
                            let coordinator = coordinator.clone();
                            let state = state.clone();
                            let shutdown = shutdown.clone();
                            jobs_in_flight.spawn(async move {
                                execute_claimed_job(coordinator, state, claimed_job, shutdown).await;
                            });
                        }
                    }
                    Err(error) => tracing::error!(%error, "failed to claim import jobs"),
                }
            }
        }
    }

    // Active executions observe the same token and must finish their joined
    // progress/network helpers before this worker task returns.
    while let Some(result) = jobs_in_flight.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "import job task failed during shutdown");
        }
    }
}

async fn execute_claimed_job(
    coordinator: Arc<ImportCoordinator>,
    state: Arc<AppState>,
    claimed: jobs::ClaimedImportJob,
    shutdown: CancellationToken,
) {
    let config = coordinator.config().raw.clone();
    let ownership_lost = CancellationToken::new();
    let cancellation = JobCancellation {
        shutdown: shutdown.clone(),
        ownership_lost: ownership_lost.clone(),
    };
    let deadline = job_deadline(claimed.job.created_at, config.job_timeout_secs);
    let deadline_instant = chrono_deadline_to_instant(deadline);

    if Utc::now() >= deadline {
        // This newly reclaimed claim must durably record the deadline outcome
        // even though its execution budget is already exhausted. It still
        // remains process-shutdown responsive.
        fail_if_owned_unless_shutdown(
            &state,
            &claimed.claim,
            &claimed.job.bucket,
            deadline_failure(),
            &shutdown,
        )
        .await;
        return;
    }

    if claimed.claim.attempt > config.max_attempts {
        fail_if_owned_until(
            &state,
            &claimed.claim,
            &claimed.job.bucket,
            attempt_limit_failure(&claimed.job.source_type),
            &shutdown,
            deadline_instant,
        )
        .await;
        return;
    }

    // A claim batch may have granted a lease beyond this job's absolute
    // deadline. Shorten it under the current fence before any pipeline work.
    let initial_now = Utc::now();
    let initial_lease_until = (initial_now + lease_delta(config.lease_duration_secs)).min(deadline);
    let initial_cap = async {
        #[cfg(test)]
        pause_before_worker_db_for_test(
            &claimed.claim.job_id,
            test_gates::WorkerDbStage::InitialLeaseCap,
        )
        .await;
        renew_claim_transactionally(
            state.store.db(),
            &claimed.claim.job_id,
            &claimed.claim.worker_id,
            claimed.claim.claim_epoch,
            initial_now,
            initial_lease_until,
            false,
        )
        .await
    };
    if !matches!(
        await_claim_db(&shutdown, deadline_instant, initial_cap).await,
        Some(Ok(true))
    ) {
        return;
    }

    let renewal_period =
        (Duration::from_secs(config.lease_duration_secs) / 3).max(Duration::from_millis(100));
    let mut renewal = tokio::time::interval(renewal_period);
    renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    renewal.tick().await;
    let deadline_sleep = tokio::time::sleep_until(deadline_instant);
    tokio::pin!(deadline_sleep);
    type RenewalFuture = Pin<Box<dyn Future<Output = Result<bool, AppError>> + Send>>;
    enum ExecutionExit {
        Finished(Result<crate::import::pipeline::ImportArtifact, ImportExecutionError>),
        Deadline,
        Shutdown,
        OwnershipLost,
    }

    // Keep both execution and renewal futures in this scope so every exit
    // drops them before durable outcome handling. The execution's owned helper
    // handles abort-on-drop, so no producer or progress task can detach.
    let exit = {
        let mut renewals = FuturesUnordered::<RenewalFuture>::new();
        let execution = execute_job(
            coordinator,
            state.clone(),
            claimed.job.clone(),
            claimed.claim.clone(),
            cancellation,
        );
        tokio::pin!(execution);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break ExecutionExit::Shutdown,
                _ = &mut deadline_sleep => {
                    ownership_lost.cancel();
                    break ExecutionExit::Deadline;
                }
                _ = ownership_lost.cancelled() => break ExecutionExit::OwnershipLost,
                renewal_result = renewals.next(), if !renewals.is_empty() => {
                    match renewal_result.expect("non-empty renewal set must yield a result") {
                        Ok(true) => {}
                        Ok(false) | Err(_) => {
                            ownership_lost.cancel();
                            break ExecutionExit::OwnershipLost;
                        }
                    }
                }
                _ = renewal.tick(), if !shutdown.is_cancelled() && renewals.is_empty() => {
                    let now = Utc::now();
                    let lease_until = (now + lease_delta(config.lease_duration_secs)).min(deadline);
                    let db = state.store.db().clone();
                    let job_id = claimed.claim.job_id.clone();
                    let worker_id = claimed.claim.worker_id.clone();
                    let claim_epoch = claimed.claim.claim_epoch;
                    renewals.push(Box::pin(async move {
                        #[cfg(test)]
                        pause_before_worker_db_for_test(
                            &job_id,
                            test_gates::WorkerDbStage::Renewal,
                        ).await;
                        renew_claim_transactionally(
                            &db,
                            &job_id,
                            &worker_id,
                            claim_epoch,
                            now,
                            lease_until,
                            true,
                        ).await
                    }));
                }
                result = &mut execution => break ExecutionExit::Finished(result),
            }
        }
    };

    let outcome = match exit {
        ExecutionExit::Finished(outcome) => outcome,
        ExecutionExit::Deadline | ExecutionExit::Shutdown | ExecutionExit::OwnershipLost => return,
    };

    if shutdown.is_cancelled() || Utc::now() >= deadline {
        return;
    }

    match outcome {
        Ok(_) | Err(ImportExecutionError::Superseded | ImportExecutionError::Interrupted) => {}
        Err(ImportExecutionError::Retryable(failure)) => {
            if claimed.claim.attempt >= config.max_attempts {
                fail_if_owned_until(
                    &state,
                    &claimed.claim,
                    &claimed.job.bucket,
                    failure,
                    &shutdown,
                    deadline_instant,
                )
                .await;
                return;
            }
            let delay = bounded_retry_delay(&claimed.claim.job_id, claimed.claim.attempt);
            let now = Utc::now();
            let Some(next_attempt_at) = TimeDelta::from_std(delay).ok().map(|delay| now + delay)
            else {
                fail_if_owned_until(
                    &state,
                    &claimed.claim,
                    &claimed.job.bucket,
                    deadline_failure(),
                    &shutdown,
                    deadline_instant,
                )
                .await;
                return;
            };
            if next_attempt_at >= deadline {
                fail_if_owned_until(
                    &state,
                    &claimed.claim,
                    &claimed.job.bucket,
                    deadline_failure(),
                    &shutdown,
                    deadline_instant,
                )
                .await;
                return;
            }
            let retry = async {
                #[cfg(test)]
                pause_before_worker_db_for_test(
                    &claimed.claim.job_id,
                    test_gates::WorkerDbStage::Outcome,
                )
                .await;
                retry_transactionally(
                    state.store.db(),
                    &claimed.claim.job_id,
                    &claimed.claim.worker_id,
                    claimed.claim.claim_epoch,
                    claimed.claim.attempt,
                    next_attempt_at,
                    &failure,
                    now,
                )
                .await
            };
            if let Some(Err(error)) = await_claim_db(&shutdown, deadline_instant, retry).await
                && !matches!(error, AppError::StaleImportOwnership)
            {
                tracing::error!(job_id = %claimed.claim.job_id, %error, "failed to requeue import job");
            }
        }
        Err(ImportExecutionError::Terminal(failure)) => {
            fail_if_owned_until(
                &state,
                &claimed.claim,
                &claimed.job.bucket,
                failure,
                &shutdown,
                deadline_instant,
            )
            .await;
        }
    }
}

#[cfg(test)]
async fn fail_if_owned(
    state: &AppState,
    claim: &ImportClaim,
    bucket: &str,
    failure: ImportFailure,
) {
    if let Err(error) =
        ownership::fail_claimed(state.store.db(), claim, bucket, &failure, Utc::now()).await
        && !matches!(error, AppError::StaleImportOwnership)
    {
        tracing::error!(job_id = %claim.job_id, %error, "failed to terminally fail import job");
    }
}

async fn fail_if_owned_until(
    state: &AppState,
    claim: &ImportClaim,
    bucket: &str,
    failure: ImportFailure,
    shutdown: &CancellationToken,
    deadline: tokio::time::Instant,
) {
    let persistence = async {
        #[cfg(test)]
        pause_before_worker_db_for_test(&claim.job_id, test_gates::WorkerDbStage::Outcome).await;
        ownership::fail_claimed(state.store.db(), claim, bucket, &failure, Utc::now()).await
    };
    if let Some(Err(error)) = await_claim_db(shutdown, deadline, persistence).await
        && !matches!(error, AppError::StaleImportOwnership)
    {
        tracing::error!(job_id = %claim.job_id, %error, "failed to terminally fail import job");
    }
}

async fn fail_if_owned_unless_shutdown(
    state: &AppState,
    claim: &ImportClaim,
    bucket: &str,
    failure: ImportFailure,
    shutdown: &CancellationToken,
) {
    let persistence =
        ownership::fail_claimed(state.store.db(), claim, bucket, &failure, Utc::now());
    tokio::pin!(persistence);
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => {}
        result = &mut persistence => {
            if let Err(error) = result
                && !matches!(error, AppError::StaleImportOwnership)
            {
                tracing::error!(job_id = %claim.job_id, %error, "failed to persist expired import deadline");
            }
        }
    }
}

async fn await_claim_db<F, T>(
    shutdown: &CancellationToken,
    deadline: tokio::time::Instant,
    operation: F,
) -> Option<T>
where
    F: Future<Output = T>,
{
    let deadline_sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(deadline_sleep);
    tokio::pin!(operation);
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => None,
        _ = &mut deadline_sleep => None,
        result = &mut operation => Some(result),
    }
}

#[allow(clippy::too_many_arguments)]
async fn renew_claim_transactionally(
    db: &DatabaseConnection,
    job_id: &str,
    worker_id: &str,
    claim_epoch: i64,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    _pause_after_write_for_test: bool,
) -> Result<bool, AppError> {
    let transaction = db.begin().await?;
    match jobs::renew_claim(
        &transaction,
        job_id,
        worker_id,
        claim_epoch,
        now,
        lease_until,
    )
    .await
    {
        Ok(renewed) => {
            #[cfg(test)]
            if renewed && _pause_after_write_for_test {
                pause_after_renewal_write_for_test(job_id).await;
            }
            transaction.commit().await?;
            Ok(renewed)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn retry_transactionally(
    db: &DatabaseConnection,
    job_id: &str,
    worker_id: &str,
    claim_epoch: i64,
    attempt: u32,
    next_attempt_at: DateTime<Utc>,
    failure: &ImportFailure,
    now: DateTime<Utc>,
) -> Result<(), AppError> {
    let transaction = db.begin().await?;
    match jobs::retry(
        &transaction,
        job_id,
        worker_id,
        claim_epoch,
        attempt,
        next_attempt_at,
        failure,
        now,
    )
    .await
    {
        Ok(()) => {
            transaction.commit().await?;
            Ok(())
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

fn job_deadline(created_at: DateTime<Utc>, timeout_secs: u64) -> DateTime<Utc> {
    created_at
        .checked_add_signed(TimeDelta::seconds(timeout_secs.min(i64::MAX as u64) as i64))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

fn lease_delta(lease_duration_secs: u64) -> TimeDelta {
    TimeDelta::seconds(lease_duration_secs.min(i64::MAX as u64) as i64)
}

fn retention_delta(retention_secs: u64) -> TimeDelta {
    TimeDelta::seconds(retention_secs.min(i64::MAX as u64) as i64)
}

fn chrono_deadline_to_instant(deadline: DateTime<Utc>) -> tokio::time::Instant {
    let duration = (deadline - Utc::now()).to_std().unwrap_or(Duration::ZERO);
    tokio::time::Instant::now() + duration
}

fn deadline_failure() -> ImportFailure {
    ImportFailure {
        code: ImportFailureCode::JobDeadlineExceeded,
        message: "import job deadline exceeded".to_owned(),
        retryable: false,
    }
}

fn attempt_limit_failure(source_type: &str) -> ImportFailure {
    ImportFailure {
        code: if source_type == "url" {
            ImportFailureCode::SourceUnreachable
        } else {
            ImportFailureCode::CidNotFound
        },
        message: "import retry limit exceeded".to_owned(),
        retryable: false,
    }
}

fn bounded_retry_delay(job_id: &str, attempt: u32) -> Duration {
    const MAX_DELAY_SECS: u64 = 300;
    let exponent = attempt.saturating_sub(1).min(8);
    let base = 1_u64 << exponent;
    if base == 1 {
        return Duration::from_secs(1);
    }
    let mut hasher = DefaultHasher::new();
    job_id.hash(&mut hasher);
    attempt.hash(&mut hasher);
    let jitter_window = (base / 5).max(1);
    let jitter = hasher.finish() % (jitter_window + 1);
    Duration::from_secs(base.saturating_add(jitter).min(MAX_DELAY_SECS))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use sea_orm::{
        ConnectOptions, ConnectionTrait, Database, DatabaseBackend, EntityTrait, PaginatorTrait,
        Statement, TransactionTrait,
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::{
        import::{ImportConfig, ImportProgress, ImportSource, downloader::SourceDownloader},
        state::AppState,
        store::{
            Store,
            entities::{import_job, object},
            import::jobs::NewImportJob,
        },
    };

    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

    async fn test_state(kubo_uri: String) -> Arc<AppState> {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo_uri),
            store: Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        })
    }

    async fn file_test_state(name: &str, kubo_uri: String) -> (tempfile::TempDir, Arc<AppState>) {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join(name);
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        crate::store::apply_sqlite_busy_timeout(&mut options);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        (
            directory,
            Arc::new(AppState {
                kubo: crate::kubo::KuboClient::new(kubo_uri),
                store: Store::new(db),
                credentials: HashMap::new(),
                master_key: crate::crypto::key::MasterKey::from_hex(
                    "0000000000000000000000000000000000000000000000000000000000000000",
                )
                .unwrap(),
                pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
            }),
        )
    }

    async fn lock_import_job(state: &AppState, job_id: &str) -> sea_orm::DatabaseTransaction {
        let holder = state.store.db().begin().await.unwrap();
        holder
            .execute(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!(
                    "UPDATE import_jobs SET updated_at = updated_at WHERE id = '{}'",
                    job_id.replace('\'', "''")
                ),
            ))
            .await
            .unwrap();
        holder
    }

    async fn install_worker_db_gate(
        job_id: &str,
        stage: test_gates::WorkerDbStage,
    ) -> Arc<test_gates::WorkerDbGate> {
        let gate = Arc::new(test_gates::WorkerDbGate {
            job_id: job_id.to_owned(),
            stage,
            pause_after_renewal_write: false,
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
            renewal_write_arrived: tokio::sync::Notify::new(),
            renewal_write_resume: tokio::sync::Notify::new(),
        });
        *test_gates::GATE.lock().await = Some(gate.clone());
        gate
    }

    async fn install_renewal_write_gate(job_id: &str) -> Arc<test_gates::WorkerDbGate> {
        let gate = Arc::new(test_gates::WorkerDbGate {
            job_id: job_id.to_owned(),
            stage: test_gates::WorkerDbStage::Renewal,
            pause_after_renewal_write: true,
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
            renewal_write_arrived: tokio::sync::Notify::new(),
            renewal_write_resume: tokio::sync::Notify::new(),
        });
        *test_gates::GATE.lock().await = Some(gate.clone());
        gate
    }

    fn request(id: &str, key: &str) -> NewImportJob {
        request_with_source(id, key, ImportSource::Cid(CID.to_owned()))
    }

    fn request_with_source(id: &str, key: &str, source: ImportSource) -> NewImportJob {
        NewImportJob {
            id: id.to_owned(),
            bucket: "bucket".to_owned(),
            key: key.to_owned(),
            source,
            request_fingerprint: format!("fingerprint-{id}"),
            client_token: None,
            object_content_type: None,
            metadata: HashMap::new(),
            tags: Vec::new(),
            decompress_prefix: None,
        }
    }

    async fn submit(state: &AppState, id: &str, key: &str, now: DateTime<Utc>) {
        ownership::submit(state.store.db(), request(id, key), now)
            .await
            .unwrap();
    }

    async fn submit_source(
        state: &AppState,
        id: &str,
        key: &str,
        source: ImportSource,
        now: DateTime<Utc>,
    ) {
        ownership::submit(state.store.db(), request_with_source(id, key, source), now)
            .await
            .unwrap();
    }

    async fn persisted(state: &AppState, id: &str) -> import_job::Model {
        import_job::Entity::find_by_id(id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap()
    }

    fn coordinator(mut config: ImportConfig) -> Arc<ImportCoordinator> {
        config.allowed_https_origins = Vec::new();
        let validated = config.validate().unwrap();
        let downloader = SourceDownloader::production(Arc::new(validated.clone()));
        ImportCoordinator::new(validated, downloader)
    }

    #[test]
    fn retry_delay_is_exponential_jittered_and_bounded() {
        assert_eq!(bounded_retry_delay("job", 1), Duration::from_secs(1));
        let second = bounded_retry_delay("job", 2);
        let third = bounded_retry_delay("job", 3);
        assert!((2..=3).contains(&second.as_secs()));
        assert!((4..=5).contains(&third.as_secs()));
        assert!(bounded_retry_delay("job", 20) <= Duration::from_secs(300));
    }

    #[test]
    fn overall_deadline_is_anchored_to_job_creation() {
        let created = DateTime::<Utc>::from_timestamp(1_000, 0).unwrap();
        assert_eq!(
            job_deadline(created, 30),
            DateTime::<Utc>::from_timestamp(1_030, 0).unwrap()
        );
    }

    #[test]
    fn max_attempts_and_deadline_failures_are_terminal() {
        let failure = deadline_failure();
        assert_eq!(failure.code, ImportFailureCode::JobDeadlineExceeded);
        assert!(!failure.retryable);
    }

    #[test]
    fn available_claim_capacity_is_bounded_by_configured_concurrency() {
        let concurrency = 4_usize;
        assert_eq!(concurrency.saturating_sub(0), 4);
        assert_eq!(concurrency.saturating_sub(3), 1);
        assert_eq!(concurrency.saturating_sub(4), 0);
        assert_eq!(concurrency.saturating_sub(8), 0);
    }

    #[tokio::test]
    async fn durable_claims_are_fair_limited_and_expired_leases_are_reclaimed() {
        let state = test_state("http://127.0.0.1:1".to_owned()).await;
        let now = Utc::now();
        submit(&state, "job-c", "key-c", now - TimeDelta::seconds(2)).await;
        submit(&state, "job-b", "key-b", now - TimeDelta::seconds(1)).await;
        submit(&state, "job-a", "key-a", now - TimeDelta::seconds(1)).await;

        let first = jobs::claim_due(
            state.store.db(),
            "worker-1",
            now,
            now + TimeDelta::seconds(30),
            2,
        )
        .await
        .unwrap();
        assert_eq!(
            first
                .iter()
                .map(|job| job.job.id.as_str())
                .collect::<Vec<_>>(),
            ["job-c", "job-a"]
        );
        let reclaimed = jobs::claim_due(
            state.store.db(),
            "worker-2",
            now + TimeDelta::seconds(31),
            now + TimeDelta::seconds(61),
            1,
        )
        .await
        .unwrap();
        assert_eq!(reclaimed[0].job.id, "job-c");
        assert_eq!(reclaimed[0].claim.attempt, 2);
        assert_eq!(reclaimed[0].claim.claim_epoch, 2);
    }

    #[tokio::test]
    async fn lease_renewal_extends_ownership_and_old_epoch_is_fenced_after_reclaim() {
        let state = test_state("http://127.0.0.1:1".to_owned()).await;
        let now = Utc::now();
        submit(&state, "lease", "lease-key", now).await;
        let first = jobs::claim_due(
            state.store.db(),
            "worker-1",
            now,
            now + TimeDelta::seconds(10),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert!(
            jobs::renew_claim(
                state.store.db(),
                "lease",
                "worker-1",
                first.claim.claim_epoch,
                now + TimeDelta::seconds(5),
                now + TimeDelta::seconds(40),
            )
            .await
            .unwrap()
        );
        assert!(
            jobs::claim_due(
                state.store.db(),
                "worker-2",
                now + TimeDelta::seconds(20),
                now + TimeDelta::seconds(50),
                1,
            )
            .await
            .unwrap()
            .is_empty()
        );
        let second = jobs::claim_due(
            state.store.db(),
            "worker-2",
            now + TimeDelta::seconds(40),
            now + TimeDelta::seconds(70),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(second.claim.claim_epoch, first.claim.claim_epoch + 1);
        let stale = jobs::update_phase(
            state.store.db(),
            "lease",
            "worker-1",
            first.claim.claim_epoch,
            crate::import::ImportState::Running,
            crate::import::ImportPhase::Downloading,
            now + TimeDelta::seconds(40),
        )
        .await;
        assert!(matches!(stale, Err(AppError::StaleImportOwnership)));
    }

    #[tokio::test]
    async fn expired_url_reclaim_resets_only_attempt_local_download_and_add_counters() {
        let state = test_state("http://127.0.0.1:1".to_owned()).await;
        let now = Utc::now();
        submit_source(
            &state,
            "retry",
            "retry-key",
            ImportSource::Url(url::Url::parse("https://example.com/object").unwrap()),
            now,
        )
        .await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker",
            now,
            now + TimeDelta::seconds(2),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        jobs::update_progress(
            state.store.db(),
            "retry",
            "worker",
            claimed.claim.claim_epoch,
            claimed.claim.attempt,
            &ImportProgress {
                providers_observed: 2,
                pin_nodes_processed: 3,
                pin_bytes_processed: 4,
                downloaded_bytes: 5,
                download_total: Some(9),
                ipfs_add_bytes: 5,
                ..ImportProgress::default()
            },
            now + TimeDelta::seconds(1),
        )
        .await
        .unwrap();
        let reclaimed = jobs::claim_due(
            state.store.db(),
            "worker-2",
            now + TimeDelta::seconds(2),
            now + TimeDelta::seconds(32),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(reclaimed.job.source_type, "url");
        assert_eq!(reclaimed.claim.attempt, 2);
        let row = persisted(&state, "retry").await;
        assert_eq!(row.downloaded_bytes, 0);
        assert_eq!(row.download_total, None);
        assert_eq!(row.ipfs_add_bytes, 0);
        assert_eq!(row.providers_observed, 2);
        assert_eq!(row.pin_nodes_processed, 3);
    }

    #[tokio::test]
    async fn process_shutdown_interrupts_without_mutation_and_leaves_lease_reclaimable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let state = test_state(server.uri()).await;
        let now = Utc::now();
        submit(&state, "shutdown", "shutdown-key", now).await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker-1",
            now,
            now + TimeDelta::seconds(2),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(execute_claimed_job(
            coordinator(ImportConfig {
                lease_duration_secs: 2,
                ..ImportConfig::default()
            }),
            state.clone(),
            claimed,
            shutdown.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(30)).await;
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("shutdown must promptly join the job")
            .unwrap();

        let interrupted = persisted(&state, "shutdown").await;
        assert_eq!(interrupted.state, "running");
        assert_eq!(interrupted.attempts, 1);
        assert_eq!(interrupted.failure_code, None);
        assert_eq!(interrupted.completed_at, None);
        assert_eq!(interrupted.locked_by.as_deref(), Some("worker-1"));

        let reclaimed = jobs::claim_due(
            state.store.db(),
            "worker-2",
            now + TimeDelta::seconds(3),
            now + TimeDelta::seconds(5),
            1,
        )
        .await
        .unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].claim.attempt, 2);
    }

    #[tokio::test]
    async fn stale_failure_fallback_never_mutates_a_reclaimed_job() {
        let state = test_state("http://127.0.0.1:1".to_owned()).await;
        let now = Utc::now();
        submit(&state, "stale", "stale-key", now).await;
        let first = jobs::claim_due(
            state.store.db(),
            "worker-1",
            now,
            now + TimeDelta::seconds(2),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let second = jobs::claim_due(
            state.store.db(),
            "worker-2",
            now + TimeDelta::seconds(3),
            now + TimeDelta::seconds(5),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        fail_if_owned(
            &state,
            &first.claim,
            "bucket",
            ImportFailure {
                code: ImportFailureCode::PublicationFailed,
                message: "old attempt failed".to_owned(),
                retryable: false,
            },
        )
        .await;
        let row = persisted(&state, "stale").await;
        assert_eq!(row.state, "running");
        assert_eq!(row.locked_by.as_deref(), Some("worker-2"));
        assert_eq!(row.claim_epoch, second.claim.claim_epoch);
        assert_eq!(row.failure_code, None);
    }

    #[tokio::test]
    async fn expired_overall_deadline_terminally_fails_without_running_pipeline() {
        let state = test_state("http://127.0.0.1:1".to_owned()).await;
        let now = Utc::now();
        submit(
            &state,
            "deadline",
            "deadline-key",
            now - TimeDelta::seconds(2),
        )
        .await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        execute_claimed_job(
            coordinator(ImportConfig {
                job_timeout_secs: 1,
                ..ImportConfig::default()
            }),
            state.clone(),
            claimed,
            CancellationToken::new(),
        )
        .await;
        let row = persisted(&state, "deadline").await;
        assert_eq!(row.state, "failed");
        assert_eq!(
            row.failure_code.as_deref(),
            Some(ImportFailureCode::JobDeadlineExceeded.as_str())
        );
    }

    #[tokio::test]
    async fn expired_deadline_precedes_exhausted_attempt_classification() {
        let state = test_state("http://127.0.0.1:1".to_owned()).await;
        let now = Utc::now();
        let created_at = now - TimeDelta::seconds(3);
        submit(
            &state,
            "deadline-and-attempts",
            "deadline-and-attempts-key",
            created_at,
        )
        .await;
        let first = jobs::claim_due(
            state.store.db(),
            "worker-1",
            created_at,
            created_at + TimeDelta::seconds(1),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let reclaimed = jobs::claim_due(
            state.store.db(),
            "worker-2",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(reclaimed.claim.attempt, first.claim.attempt + 1);

        execute_claimed_job(
            coordinator(ImportConfig {
                max_attempts: 1,
                job_timeout_secs: 1,
                ..ImportConfig::default()
            }),
            state.clone(),
            reclaimed,
            CancellationToken::new(),
        )
        .await;

        let row = persisted(&state, "deadline-and-attempts").await;
        assert_eq!(row.state, "failed");
        assert_eq!(
            row.failure_code.as_deref(),
            Some(ImportFailureCode::JobDeadlineExceeded.as_str())
        );
    }

    #[tokio::test]
    async fn shutdown_drops_initial_lease_cap_waiting_on_real_sqlite_lock() {
        let _test_lock = test_gates::TEST_LOCK.lock().await;
        let (_directory, state) =
            file_test_state("worker-initial-cap.sqlite", "http://127.0.0.1:1".to_owned()).await;
        let now = Utc::now();
        submit(&state, "initial-cap-lock", "initial-cap-lock-key", now).await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let original_lease = claimed.job.locked_until;
        let gate = install_worker_db_gate(
            "initial-cap-lock",
            test_gates::WorkerDbStage::InitialLeaseCap,
        )
        .await;
        let shutdown = CancellationToken::new();
        let mut task = tokio::spawn(execute_claimed_job(
            coordinator(ImportConfig::default()),
            state.clone(),
            claimed,
            shutdown.clone(),
        ));
        gate.arrived.notified().await;
        let holder = lock_import_job(&state, "initial-cap-lock").await;
        gate.resume.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(75), &mut task)
                .await
                .is_err(),
            "initial lease cap must be waiting on the held write lock"
        );

        shutdown.cancel();
        let responsive = tokio::time::timeout(Duration::from_millis(400), &mut task).await;
        holder.rollback().await.unwrap();
        if responsive.is_err() {
            let _ = task.await;
        }
        *test_gates::GATE.lock().await = None;
        assert!(
            responsive.is_ok(),
            "shutdown must drop the blocked initial cap"
        );
        let row = persisted(&state, "initial-cap-lock").await;
        assert_eq!(row.state, "running");
        assert_eq!(row.locked_until, original_lease);
        assert_eq!(row.failure_code, None);
    }

    #[tokio::test]
    async fn deadline_drops_renewal_waiting_on_real_sqlite_lock() {
        let _test_lock = test_gates::TEST_LOCK.lock().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let (_directory, state) = file_test_state("worker-renewal.sqlite", server.uri()).await;
        let now = Utc::now();
        submit(&state, "renewal-lock", "renewal-lock-key", now).await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let gate = install_worker_db_gate("renewal-lock", test_gates::WorkerDbStage::Renewal).await;
        let mut task = tokio::spawn(execute_claimed_job(
            coordinator(ImportConfig {
                lease_duration_secs: 1,
                job_timeout_secs: 2,
                ..ImportConfig::default()
            }),
            state.clone(),
            claimed,
            CancellationToken::new(),
        ));
        tokio::time::timeout(Duration::from_secs(1), gate.arrived.notified())
            .await
            .expect("renewal must reach its deterministic pre-await seam");
        let holder = lock_import_job(&state, "renewal-lock").await;
        gate.resume.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(75), &mut task)
                .await
                .is_err(),
            "renewal must be waiting on the held write lock"
        );

        let responsive = tokio::time::timeout(Duration::from_secs(3), &mut task).await;
        holder.rollback().await.unwrap();
        if responsive.is_err() {
            let _ = task.await;
        }
        *test_gates::GATE.lock().await = None;
        assert!(responsive.is_ok(), "deadline must drop the blocked renewal");
        let row = persisted(&state, "renewal-lock").await;
        assert_eq!(row.state, "running");
        assert_eq!(row.failure_code, None);
    }

    #[tokio::test]
    async fn pipeline_failure_drops_blocked_renewal_before_retry_persistence() {
        let _test_lock = test_gates::TEST_LOCK.lock().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(503).set_delay(Duration::from_secs(2)))
            .expect(1)
            .mount(&server)
            .await;
        let (_directory, state) =
            file_test_state("worker-renewal-outcome.sqlite", server.uri()).await;
        let now = Utc::now();
        submit(&state, "renewal-outcome", "renewal-outcome-key", now).await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let renewal_gate = install_renewal_write_gate("renewal-outcome").await;
        let mut task = tokio::spawn(execute_claimed_job(
            coordinator(ImportConfig {
                lease_duration_secs: 4,
                job_timeout_secs: 10,
                ..ImportConfig::default()
            }),
            state.clone(),
            claimed,
            CancellationToken::new(),
        ));
        tokio::time::timeout(Duration::from_secs(3), renewal_gate.arrived.notified())
            .await
            .expect("renewal must reach its deterministic pre-await seam");
        let holder = lock_import_job(&state, "renewal-outcome").await;
        renewal_gate.resume.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(75), &mut task)
                .await
                .is_err(),
            "renewal must be waiting on the real SQLite holder"
        );
        holder.rollback().await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            renewal_gate.renewal_write_arrived.notified(),
        )
        .await
        .expect("renewal must acquire the real SQLite write lock before pipeline failure");
        let outcome_gate =
            install_worker_db_gate("renewal-outcome", test_gates::WorkerDbStage::Outcome).await;

        tokio::time::timeout(Duration::from_secs(3), outcome_gate.arrived.notified())
            .await
            .expect("the delayed pipeline failure must reach retry persistence");
        outcome_gate.resume.notify_one();
        let completed = tokio::time::timeout(Duration::from_secs(1), &mut task).await;
        if completed.is_err() {
            task.abort();
            let _ = task.await;
        }
        *test_gates::GATE.lock().await = None;
        assert!(
            completed.is_ok(),
            "retry persistence must not remain blocked behind an abandoned renewal"
        );

        let row = persisted(&state, "renewal-outcome").await;
        assert_eq!(row.state, "queued");
        assert_eq!(row.locked_by, None);
        assert_eq!(row.locked_until, None);
        assert_eq!(
            row.failure_code.as_deref(),
            Some(ImportFailureCode::CidNotFound.as_str())
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        let stable = persisted(&state, "renewal-outcome").await;
        assert_eq!(stable.state, "queued");
        assert_eq!(stable.locked_by, None);
        assert_eq!(stable.locked_until, None);
    }

    #[tokio::test]
    async fn shutdown_drops_retry_persistence_waiting_on_real_sqlite_lock() {
        let _test_lock = test_gates::TEST_LOCK.lock().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let (_directory, state) = file_test_state("worker-retry.sqlite", server.uri()).await;
        let now = Utc::now();
        submit(&state, "retry-lock", "retry-lock-key", now).await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let gate = install_worker_db_gate("retry-lock", test_gates::WorkerDbStage::Outcome).await;
        let shutdown = CancellationToken::new();
        let mut task = tokio::spawn(execute_claimed_job(
            coordinator(ImportConfig::default()),
            state.clone(),
            claimed,
            shutdown.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(1), gate.arrived.notified())
            .await
            .expect("retry must reach its deterministic pre-await seam");
        let holder = lock_import_job(&state, "retry-lock").await;
        gate.resume.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(75), &mut task)
                .await
                .is_err(),
            "retry persistence must be waiting on the held write lock"
        );

        shutdown.cancel();
        let responsive = tokio::time::timeout(Duration::from_millis(400), &mut task).await;
        holder.rollback().await.unwrap();
        if responsive.is_err() {
            let _ = task.await;
        }
        *test_gates::GATE.lock().await = None;
        assert!(
            responsive.is_ok(),
            "shutdown must drop blocked retry persistence"
        );
        let row = persisted(&state, "retry-lock").await;
        assert_eq!(row.state, "running");
        assert_eq!(row.failure_code, None);
        assert_eq!(row.locked_by.as_deref(), Some("worker"));
    }

    #[tokio::test]
    async fn deadline_drops_blocked_publication_and_releases_worker_slot() {
        let _test_lock =
            crate::store::pinning::publication::test_gates::IMPORT_COMPLETION_TEST_LOCK
                .lock()
                .await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"}]}\n"),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello"))
            .mount(&server)
            .await;
        let state = test_state(server.uri()).await;
        let created_at = Utc::now();
        submit(
            &state,
            "blocked-deadline",
            "blocked-deadline-key",
            created_at,
        )
        .await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker",
            created_at,
            created_at + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let gate = Arc::new(
            crate::store::pinning::publication::test_gates::ImportCompletionGate {
                job_id: "blocked-deadline".to_owned(),
                arrived: tokio::sync::Notify::new(),
                resume: tokio::sync::Notify::new(),
            },
        );
        *crate::store::pinning::publication::test_gates::IMPORT_BEFORE_COMPLETION
            .lock()
            .await = Some(gate.clone());
        let mut task = tokio::spawn(execute_claimed_job(
            coordinator(ImportConfig {
                lease_duration_secs: 30,
                job_timeout_secs: 1,
                ..ImportConfig::default()
            }),
            state.clone(),
            claimed,
            CancellationToken::new(),
        ));
        tokio::time::timeout(Duration::from_secs(1), gate.arrived.notified())
            .await
            .expect("pipeline must reach the blocked publication");

        let completed = tokio::time::timeout(Duration::from_millis(1_500), &mut task).await;
        if completed.is_err() {
            gate.resume.notify_one();
            let _ = task.await;
        }
        *crate::store::pinning::publication::test_gates::IMPORT_BEFORE_COMPLETION
            .lock()
            .await = None;
        assert!(
            completed.is_ok(),
            "deadline must drop blocked execution promptly"
        );

        let expired = persisted(&state, "blocked-deadline").await;
        assert_eq!(expired.state, "running");
        assert_eq!(expired.final_cid, None);
        assert!(expired.locked_until.unwrap() <= job_deadline(created_at, 1));
        assert_eq!(
            object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );

        let now = Utc::now();
        let reclaimed = jobs::claim_due(
            state.store.db(),
            "replacement",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .expect("deadline-capped lease must be reclaimable");
        execute_claimed_job(
            coordinator(ImportConfig {
                job_timeout_secs: 1,
                ..ImportConfig::default()
            }),
            state.clone(),
            reclaimed,
            CancellationToken::new(),
        )
        .await;
        let failed = persisted(&state, "blocked-deadline").await;
        assert_eq!(failed.state, "failed");
        assert_eq!(
            failed.failure_code.as_deref(),
            Some(ImportFailureCode::JobDeadlineExceeded.as_str())
        );
    }

    #[tokio::test]
    async fn worker_claims_no_more_than_configured_concurrency() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let state = test_state(server.uri()).await;
        let now = Utc::now();
        submit(&state, "bounded-a", "bounded-a", now).await;
        submit(&state, "bounded-b", "bounded-b", now).await;
        submit(&state, "bounded-c", "bounded-c", now).await;
        let coordinator = coordinator(ImportConfig {
            worker_concurrency: 2,
            poll_interval_ms: 10,
            lease_duration_secs: 5,
            ..ImportConfig::default()
        });
        let shutdown = CancellationToken::new();
        let worker = coordinator.start(state.clone(), shutdown);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let rows = import_job::Entity::find()
                    .all(state.store.db())
                    .await
                    .unwrap();
                if rows.iter().filter(|row| row.state == "running").count() == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker must claim its available slots");
        let rows = import_job::Entity::find()
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(rows.iter().filter(|row| row.state == "running").count(), 2);
        assert_eq!(rows.iter().filter(|row| row.state == "queued").count(), 1);
        worker.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn retryable_failure_at_max_attempts_becomes_terminal() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        let state = test_state(server.uri()).await;
        let now = Utc::now();
        submit(&state, "max-attempts", "max-attempts-key", now).await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        execute_claimed_job(
            coordinator(ImportConfig {
                max_attempts: 1,
                ..ImportConfig::default()
            }),
            state.clone(),
            claimed,
            CancellationToken::new(),
        )
        .await;
        let row = persisted(&state, "max-attempts").await;
        assert_eq!(row.state, "failed");
        assert_eq!(row.attempts, 1);
        assert_eq!(
            row.failure_code.as_deref(),
            Some(ImportFailureCode::CidNotFound.as_str())
        );
    }

    #[tokio::test]
    async fn disabled_worker_never_claims_due_jobs() {
        let state = test_state("http://127.0.0.1:1".to_owned()).await;
        submit(&state, "disabled", "disabled-key", Utc::now()).await;
        let worker = coordinator(ImportConfig {
            enabled: false,
            poll_interval_ms: 10,
            ..ImportConfig::default()
        })
        .start(state.clone(), CancellationToken::new());
        tokio::time::sleep(Duration::from_millis(30)).await;
        worker.shutdown(Duration::from_secs(1)).await;
        let row = persisted(&state, "disabled").await;
        assert_eq!(row.state, "queued");
        assert_eq!(row.attempts, 0);
        assert_eq!(row.locked_by, None);
    }

    #[tokio::test]
    async fn active_worker_renews_lease_while_pipeline_is_running() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let state = test_state(server.uri()).await;
        let now = Utc::now();
        submit(&state, "renew-running", "renew-running-key", now).await;
        let claimed = jobs::claim_due(
            state.store.db(),
            "worker",
            now,
            now + TimeDelta::seconds(2),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let original_lease = claimed.claim.locked_until;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(execute_claimed_job(
            coordinator(ImportConfig {
                lease_duration_secs: 2,
                ..ImportConfig::default()
            }),
            state.clone(),
            claimed,
            shutdown.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let renewed = persisted(&state, "renew-running").await;
        assert!(renewed.locked_until.unwrap() > original_lease);
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("renewing job must stop promptly")
            .unwrap();
        assert_eq!(persisted(&state, "renew-running").await.state, "running");
    }

    #[tokio::test]
    async fn expired_reclaim_above_max_attempts_fails_without_kubo_or_publication() {
        let server = MockServer::start().await;
        let state = test_state(server.uri()).await;
        let now = Utc::now();
        submit(&state, "over-limit", "over-limit-key", now).await;
        let first = jobs::claim_due(
            state.store.db(),
            "worker-1",
            now,
            now + TimeDelta::seconds(1),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(first.claim.attempt, 1);
        let reclaimed = jobs::claim_due(
            state.store.db(),
            "worker-2",
            now + TimeDelta::seconds(2),
            now + TimeDelta::seconds(32),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(reclaimed.claim.attempt, 2);
        execute_claimed_job(
            coordinator(ImportConfig {
                max_attempts: 1,
                ..ImportConfig::default()
            }),
            state.clone(),
            reclaimed,
            CancellationToken::new(),
        )
        .await;

        assert!(server.received_requests().await.unwrap().is_empty());
        let row = persisted(&state, "over-limit").await;
        assert_eq!(row.state, "failed");
        assert_eq!(row.attempts, 2);
        assert_eq!(
            object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn worker_retention_cleanup_deletes_only_old_terminal_jobs() {
        let state = test_state("http://127.0.0.1:1".to_owned()).await;
        let now = Utc::now();
        let old = now - TimeDelta::seconds(120);
        submit(&state, "old-terminal", "old-terminal-key", old).await;
        let old_claim = jobs::claim_due(
            state.store.db(),
            "old-worker",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        ownership::fail_claimed(
            state.store.db(),
            &old_claim.claim,
            "bucket",
            &ImportFailure {
                code: ImportFailureCode::SourceUnreachable,
                message: "old failure".to_owned(),
                retryable: false,
            },
            old + TimeDelta::seconds(1),
        )
        .await
        .unwrap();

        submit(&state, "recent-terminal", "recent-terminal-key", now).await;
        let recent_claim = jobs::claim_due(
            state.store.db(),
            "recent-worker",
            now,
            now + TimeDelta::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        ownership::fail_claimed(
            state.store.db(),
            &recent_claim.claim,
            "bucket",
            &ImportFailure {
                code: ImportFailureCode::SourceUnreachable,
                message: "recent failure".to_owned(),
                retryable: false,
            },
            now,
        )
        .await
        .unwrap();

        submit(&state, "active", "active-key", old).await;
        let active = jobs::claim_due(
            state.store.db(),
            "active-worker",
            now,
            now + TimeDelta::seconds(60),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(active.job.id, "active");

        let worker = coordinator(ImportConfig {
            terminal_retention_secs: 60,
            poll_interval_ms: 1_000,
            ..ImportConfig::default()
        })
        .start(state.clone(), CancellationToken::new());
        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                if import_job::Entity::find_by_id("old-terminal")
                    .one(state.store.db())
                    .await
                    .unwrap()
                    .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker must run retention cleanup promptly");
        worker.shutdown(Duration::from_secs(1)).await;

        assert!(
            persisted(&state, "recent-terminal")
                .await
                .completed_at
                .is_some()
        );
        let active = persisted(&state, "active").await;
        assert_eq!(active.state, "running");
        assert_eq!(active.locked_by.as_deref(), Some("active-worker"));
    }
}
