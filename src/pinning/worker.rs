use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, TransactionTrait};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    error::{AppError, AppResult},
    pinning::{
        config::ProviderMode,
        coordinator::{PinningCoordinator, ProviderHealth, ProviderRuntime},
        provider::{
            FindPin, PinningProvider, ProviderError, ProviderErrorClass, RemotePin,
            RemotePinStatus, SubmitPin,
        },
    },
    store::{
        Store,
        entities::{object, pin_job, pin_lease, pin_lease_target, remote_pin},
        pinning::{
            jobs::{self, ClaimedPinJob, NewPinJob, SubmitCallDecision},
            leases::{
                self, FailedRemoteResubmitDecision, MAX_FAILED_REQUEST_ATTEMPTS,
                NoRequestRemoteCompletion, RemoteDeleteCompletion, RemoteStatusApplyResult,
                RemoteStatusOrigin, RemoteStatusUpdate, RemoteWorkSnapshot,
            },
            quota as store_quota,
        },
    },
};

const STATE_DONE: &str = "done";
const STATUS_RESERVED: &str = "reserved";
const STATUS_QUEUED: &str = "queued";
const STATUS_PINNING: &str = "pinning";
const STATUS_PINNED: &str = "pinned";
const STATUS_FAILED: &str = "failed";
const STATUS_ABSENT: &str = "absent";

#[derive(Clone, Default)]
struct ProviderOccupancy {
    state: Arc<StdMutex<ProviderOccupancyState>>,
}

struct ProviderOccupancyState {
    in_flight: BTreeMap<String, usize>,
    last_served_ticket: BTreeMap<String, u64>,
    next_ticket: u64,
}

struct ProviderOccupancySnapshot {
    in_flight: BTreeMap<String, usize>,
    last_served_ticket: BTreeMap<String, u64>,
}

struct ProviderOccupancyGuard {
    occupancy: ProviderOccupancy,
    provider: String,
}

impl ProviderOccupancy {
    fn enter(&self, provider: String) -> ProviderOccupancyGuard {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state.in_flight.entry(provider.clone()).or_default() += 1;
        drop(state);
        ProviderOccupancyGuard {
            occupancy: self.clone(),
            provider,
        }
    }

    fn snapshot(&self) -> ProviderOccupancySnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ProviderOccupancySnapshot {
            in_flight: state.in_flight.clone(),
            last_served_ticket: state.last_served_ticket.clone(),
        }
    }

    fn record_selected<'a>(&self, providers: impl IntoIterator<Item = &'a str>) -> AppResult<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for provider in providers {
            if state.next_ticket == u64::MAX {
                compact_service_tickets(&mut state)?;
            }
            let ticket = state.next_ticket;
            state.next_ticket = ticket.checked_add(1).ok_or_else(|| {
                AppError::Internal("provider scheduler ticket space exhausted".to_owned())
            })?;
            state.last_served_ticket.insert(provider.to_owned(), ticket);
        }
        Ok(())
    }
}

impl Default for ProviderOccupancyState {
    fn default() -> Self {
        Self {
            in_flight: BTreeMap::new(),
            last_served_ticket: BTreeMap::new(),
            next_ticket: 1,
        }
    }
}

fn compact_service_tickets(state: &mut ProviderOccupancyState) -> AppResult<()> {
    let mut providers = state
        .last_served_ticket
        .iter()
        .map(|(provider, ticket)| (provider.clone(), *ticket))
        .collect::<Vec<_>>();
    providers.sort_by(
        |(left_provider, left_ticket), (right_provider, right_ticket)| {
            left_ticket
                .cmp(right_ticket)
                .then_with(|| left_provider.cmp(right_provider))
        },
    );
    state.last_served_ticket.clear();
    let mut next_ticket = 1_u64;
    for (provider, _) in providers {
        state.last_served_ticket.insert(provider, next_ticket);
        next_ticket = next_ticket.checked_add(1).ok_or_else(|| {
            AppError::Internal("provider scheduler ticket space exhausted".to_owned())
        })?;
    }
    state.next_ticket = next_ticket;
    Ok(())
}

impl Drop for ProviderOccupancyGuard {
    fn drop(&mut self) {
        let mut state = self
            .occupancy
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remove = match state.in_flight.get_mut(&self.provider) {
            Some(count) => {
                *count = count.saturating_sub(1);
                *count == 0
            }
            None => false,
        };
        if remove {
            state.in_flight.remove(&self.provider);
        }
    }
}

#[cfg(test)]
struct ReconcileAfterSnapshotGate {
    expected_epoch: i64,
    arrived: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[cfg(test)]
static RECONCILE_AFTER_SNAPSHOT: std::sync::LazyLock<
    tokio::sync::Mutex<Option<Arc<ReconcileAfterSnapshotGate>>>,
> = std::sync::LazyLock::new(|| tokio::sync::Mutex::new(None));

#[cfg(test)]
struct ObservationAfterStatusCommitGate {
    job_id: String,
    request_id: String,
    fail_once: std::sync::atomic::AtomicBool,
    arrived: tokio::sync::Notify,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExhaustedCoordinationPhase {
    BeforeCoordination,
    AfterScopeRead,
    AfterCoordination,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum ExhaustedCoordinationInterruption {
    Continue,
    Database,
    Crash,
}

#[cfg(test)]
struct ExhaustedCoordinationGate {
    job_id: String,
    phase: ExhaustedCoordinationPhase,
    interruption: ExhaustedCoordinationInterruption,
    fail_once: std::sync::atomic::AtomicBool,
    arrived: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[cfg(test)]
type ObservationAfterStatusCommitGates =
    BTreeMap<(String, String), Arc<ObservationAfterStatusCommitGate>>;

#[cfg(test)]
static OBSERVATION_AFTER_STATUS_COMMIT: std::sync::LazyLock<
    tokio::sync::Mutex<ObservationAfterStatusCommitGates>,
> = std::sync::LazyLock::new(|| tokio::sync::Mutex::new(BTreeMap::new()));

#[cfg(test)]
static EXHAUSTED_COORDINATION_GATE: std::sync::LazyLock<
    tokio::sync::Mutex<BTreeMap<String, Arc<ExhaustedCoordinationGate>>>,
> = std::sync::LazyLock::new(|| tokio::sync::Mutex::new(BTreeMap::new()));

#[cfg(test)]
async fn pause_reconcile_after_snapshot(expected_epoch: i64) {
    let gate = RECONCILE_AFTER_SNAPSHOT.lock().await.clone();
    if let Some(gate) = gate.filter(|gate| gate.expected_epoch == expected_epoch) {
        gate.arrived.notify_one();
        gate.resume.notified().await;
    }
}

#[cfg(test)]
async fn fail_once_after_observation_status_commit(
    job_id: &str,
    request_id: &str,
) -> AppResult<()> {
    let gate = OBSERVATION_AFTER_STATUS_COMMIT
        .lock()
        .await
        .get(&(job_id.to_owned(), request_id.to_owned()))
        .cloned();
    if let Some(gate) = gate.filter(|gate| {
        gate.fail_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }) {
        gate.arrived.notify_one();
        return Err(AppError::Database(
            "test coordination failure after durable pin status projection".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
async fn interrupt_exhausted_coordination(
    job_id: &str,
    phase: ExhaustedCoordinationPhase,
) -> AppResult<()> {
    let gate = EXHAUSTED_COORDINATION_GATE
        .lock()
        .await
        .get(job_id)
        .cloned();
    let Some(gate) = gate.filter(|gate| {
        gate.job_id == job_id
            && gate.phase == phase
            && gate
                .fail_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
    }) else {
        return Ok(());
    };
    gate.arrived.notify_one();
    gate.resume.notified().await;
    match gate.interruption {
        ExhaustedCoordinationInterruption::Continue => Ok(()),
        ExhaustedCoordinationInterruption::Database => Err(AppError::Database(
            "test transient exhausted one coordination failure".to_owned(),
        )),
        ExhaustedCoordinationInterruption::Crash => Err(AppError::Internal(
            "test interruption in exhausted one coordination".to_owned(),
        )),
    }
}

pub struct PinningWorkerHandle {
    cancellation: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl PinningWorkerHandle {
    pub async fn shutdown(self, grace: Duration) {
        self.cancellation.cancel();
        let mut join = self.join;
        if tokio::time::timeout(grace, &mut join).await.is_err() {
            join.abort();
            let _ = join.await;
        }
    }
}

pub(crate) fn start(
    coordinator: Arc<PinningCoordinator>,
    store: Store,
    parent: CancellationToken,
) -> PinningWorkerHandle {
    let cancellation = parent.child_token();
    let worker_cancellation = cancellation.clone();
    let join = tokio::spawn(async move {
        run_worker(coordinator, Arc::new(store), worker_cancellation).await;
    });
    PinningWorkerHandle { cancellation, join }
}

#[derive(Clone)]
struct JobLogContext {
    job_id: String,
    provider: String,
    cid: String,
    lease_id: Option<String>,
    target_id: Option<String>,
}

impl JobLogContext {
    fn from_model(model: &crate::store::entities::pin_job::Model) -> Self {
        Self {
            job_id: model.id.clone(),
            provider: model.provider.clone(),
            cid: model.cid.clone(),
            lease_id: model.lease_id.clone(),
            target_id: model.target_id.clone(),
        }
    }

    fn log_task_failure(&self, error: &tokio::task::JoinError, message: &'static str) {
        tracing::error!(
            job_id = %self.job_id,
            provider = %self.provider,
            cid = %self.cid,
            lease_id = ?self.lease_id,
            target_id = ?self.target_id,
            error = %error,
            "{message}"
        );
    }
}

fn handle_join_result(
    completed: Result<(tokio::task::Id, ()), tokio::task::JoinError>,
    job_contexts: &mut std::collections::HashMap<tokio::task::Id, JobLogContext>,
    failure_message: &'static str,
) {
    match completed {
        Ok((id, ())) => {
            job_contexts.remove(&id);
        }
        Err(error) => {
            let id = error.id();
            match job_contexts.remove(&id) {
                Some(context) => context.log_task_failure(&error, failure_message),
                None => tracing::error!(
                    error = %error,
                    "{failure_message} with no recorded job context"
                ),
            }
        }
    }
}

async fn run_worker(
    coordinator: Arc<PinningCoordinator>,
    store: Arc<Store>,
    cancellation: CancellationToken,
) {
    let settings = coordinator.settings().clone();
    let global = Arc::new(Semaphore::new(settings.worker_concurrency));
    let mut interval = tokio::time::interval(settings.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut in_flight = JoinSet::new();
    let mut job_contexts: std::collections::HashMap<tokio::task::Id, JobLogContext> =
        std::collections::HashMap::new();
    let provider_occupancy = ProviderOccupancy::default();

    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            completed = in_flight.join_next_with_id(), if !in_flight.is_empty() => {
                if let Some(completed) = completed {
                    handle_join_result(completed, &mut job_contexts, "pinning worker task failed");
                }
            }
            _ = interval.tick() => {
                if cancellation.is_cancelled() {
                    break;
                }
                while let Some(completed) = in_flight.try_join_next_with_id() {
                    handle_join_result(completed, &mut job_contexts, "pinning worker task failed");
                }
                let available = settings.worker_concurrency.saturating_sub(in_flight.len());
                let scan = scan_and_claim(
                    &coordinator,
                    &store,
                    &cancellation,
                    &provider_occupancy,
                    Utc::now(),
                    u64::try_from(available).unwrap_or(u64::MAX),
                );
                let scan_result = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    result = scan => result,
                };
                match scan_result {
                    Ok(claimed) => {
                        for job in claimed {
                            if cancellation.is_cancelled() {
                                break;
                            }
                            let coordinator = coordinator.clone();
                            let store = store.clone();
                            let global = global.clone();
                            let cancellation = cancellation.clone();
                            let context = JobLogContext::from_model(&job.model);
                            let task_context = context.clone();
                            let occupancy =
                                provider_occupancy.enter(job.model.provider.clone());
                            let handle = in_flight.spawn(async move {
                                let _occupancy = occupancy;
                                if let Err(error) = execute_claimed_job_with_cancellation(
                                    &store,
                                    &coordinator,
                                    &global,
                                    job,
                                    &cancellation,
                                )
                                .await
                                {
                                    tracing::error!(
                                        job_id = %task_context.job_id,
                                        provider = %task_context.provider,
                                        cid = %task_context.cid,
                                        lease_id = ?task_context.lease_id,
                                        target_id = ?task_context.target_id,
                                        error = %error,
                                        "pinning job execution failed"
                                    );
                                }
                            });
                            job_contexts.insert(handle.id(), context);
                        }
                    }
                    Err(error) => tracing::error!(error = %error, "pinning worker scan failed"),
                }
            }
        }
    }

    let drain = async {
        while let Some(result) = in_flight.join_next_with_id().await {
            handle_join_result(
                result,
                &mut job_contexts,
                "pinning worker task failed during drain",
            );
        }
    };
    if tokio::time::timeout(settings.shutdown_grace, drain)
        .await
        .is_err()
    {
        in_flight.abort_all();
        while in_flight.join_next().await.is_some() {}
    }
}

async fn scan_and_claim(
    coordinator: &PinningCoordinator,
    store: &Store,
    cancellation: &CancellationToken,
    provider_occupancy: &ProviderOccupancy,
    now: DateTime<Utc>,
    claim_limit: u64,
) -> AppResult<Vec<ClaimedPinJob>> {
    let txn = store.db().begin().await?;
    let expired = leases::expire_due_leases(&txn, now).await?;
    txn.commit().await?;
    if !expired.is_empty() {
        tracing::info!(
            expired_lease_count = expired.len(),
            "pinning leases expired"
        );
    }
    coordinate_quota_waiters(coordinator, store, now).await?;
    if cancellation.is_cancelled() {
        return Ok(Vec::new());
    }
    if claim_limit == 0 {
        return Ok(Vec::new());
    }
    let configured_priorities = coordinator
        .provider_limits()
        .iter()
        .map(|(provider, limits)| (provider.clone(), limits.priority))
        .collect();
    let occupancy = provider_occupancy.snapshot();
    let claimed = jobs::claim_due_jobs_fair(
        store.db(),
        now,
        coordinator.settings().lock_for,
        claim_limit,
        &occupancy.in_flight,
        &occupancy.last_served_ticket,
        &configured_priorities,
    )
    .await?;
    provider_occupancy.record_selected(claimed.iter().map(|job| job.model.provider.as_str()))?;
    Ok(claimed)
}

#[cfg(test)]
async fn execute_claimed_job(
    store: &Store,
    coordinator: &PinningCoordinator,
    global: &Arc<Semaphore>,
    claimed: ClaimedPinJob,
) -> AppResult<()> {
    let cancellation = CancellationToken::new();
    execute_claimed_job_with_cancellation(store, coordinator, global, claimed, &cancellation).await
}

async fn execute_claimed_job_with_cancellation(
    store: &Store,
    coordinator: &PinningCoordinator,
    global: &Arc<Semaphore>,
    claimed: ClaimedPinJob,
    cancellation: &CancellationToken,
) -> AppResult<()> {
    validate_operation_scope(&claimed.model)?;
    if claimed.reclaimed {
        recover_durable_observation_coordination(
            store,
            coordinator,
            &claimed.model.provider,
            &claimed.model.cid,
            Utc::now(),
        )
        .await?;
    }
    if claimed.model.operation != "submit"
        && claimed.model.attempts >= coordinator.settings().max_attempts as i32
    {
        return park_exhausted_ordinary_job(store, coordinator, claimed).await;
    }
    let claimed_state = if claimed.reclaimed {
        "reclaimed"
    } else {
        "running"
    };
    transition(&claimed, &claimed.previous_state, claimed_state, None);
    if coordinator.provider(&claimed.model.provider).is_none()
        || coordinator
            .provider_runtime(&claimed.model.provider)
            .is_none()
    {
        return handle_unavailable_provider(store, coordinator, &claimed).await;
    }
    match claimed.model.operation.as_str() {
        "submit" => execute_submit(store, coordinator, global, claimed, cancellation).await,
        "poll" => execute_poll(store, coordinator, global, claimed, cancellation).await,
        "unpin" => execute_unpin(store, coordinator, global, claimed, cancellation).await,
        "reconcile" => execute_reconcile(store, coordinator, global, claimed, cancellation).await,
        operation => Err(AppError::Internal(format!(
            "unknown pin job operation: {operation}"
        ))),
    }
}

async fn handle_unavailable_provider(
    store: &Store,
    coordinator: &PinningCoordinator,
    claimed: &ClaimedPinJob,
) -> AppResult<()> {
    let error = terminal_health_error();
    if claimed.model.operation == "submit" {
        let old_phase = claimed.model.submit_phase.as_deref().unwrap_or("running");
        jobs::retry_submit_recovery(
            store.db(),
            claimed,
            Utc::now(),
            coordinator.settings().max_backoff,
            "provider unavailable",
        )
        .await?;
        coordinate_claimed_one_target(store, coordinator, &claimed.model, Utc::now()).await?;
        transition(claimed, old_phase, "provider_unavailable_wait", None);
        return Ok(());
    }
    retry_ordinary_job(store, coordinator, claimed, &error, None).await
}

fn validate_operation_scope(job: &pin_job::Model) -> AppResult<()> {
    let target_scope = job.lease_id.is_some()
        && job.target_id.is_some()
        && job.expected_generation.is_some()
        && job.expected_remote_epoch.is_none();
    let remote_scope = job.lease_id.is_none()
        && job.target_id.is_none()
        && job.expected_generation.is_none()
        && job.expected_remote_epoch.is_some();
    match job.operation.as_str() {
        "submit" if target_scope && job.submit_phase.is_some() => Ok(()),
        "poll" if target_scope && job.submit_phase.is_none() => Ok(()),
        "unpin" | "reconcile" if remote_scope && job.submit_phase.is_none() => Ok(()),
        "submit" | "poll" | "unpin" | "reconcile" => Err(AppError::Internal(format!(
            "invalid persisted scope for pin job operation `{}`",
            job.operation
        ))),
        operation => Err(AppError::Internal(format!(
            "unknown pin job operation: {operation}"
        ))),
    }
}

async fn execute_submit(
    store: &Store,
    coordinator: &PinningCoordinator,
    global: &Arc<Semaphore>,
    mut claimed: ClaimedPinJob,
    cancellation: &CancellationToken,
) -> AppResult<()> {
    if claimed.reclaimed || claimed.model.submit_phase.as_deref() == Some("recovering") {
        return recover_submit(store, coordinator, global, &mut claimed, cancellation).await;
    }

    let Some(slot) = acquire_provider_slot(
        coordinator,
        store,
        &mut claimed,
        coordinator.settings().lock_for,
        cancellation,
    )
    .await?
    else {
        return Ok(());
    };
    renew_claim_once(store, &mut claimed, coordinator.settings().lock_for).await?;
    let context = load_submit_context(store, &claimed.model).await?;
    let now = Utc::now();
    let txn = store.db().begin().await?;
    let decision = jobs::prepare_submit_call(&txn, &claimed, now).await?;
    txn.commit().await?;
    if decision == SubmitCallDecision::NoLongerDesired {
        transition(&claimed, "running", "done_current_reconcile", None);
        return Ok(());
    }
    let context = context.ok_or_else(|| {
        AppError::Internal("current Submit target has no immutable owner context".to_owned())
    })?;
    let request = SubmitPin {
        cid: claimed.model.cid.clone(),
        name: format!("{}/{}", context.owner.bucket, context.owner.key),
        metadata: BTreeMap::from([
            ("gateway_job_id".to_owned(), claimed.model.id.clone()),
            ("gateway_lease_id".to_owned(), context.lease.id.clone()),
            ("gateway_target_id".to_owned(), context.target.id.clone()),
        ]),
    };
    transition(
        &claimed,
        claimed.model.submit_phase.as_deref().unwrap_or("running"),
        "calling",
        None,
    );

    let preflight_job = claimed.model.clone();
    let result = match provider_call_with_heartbeat(
        slot,
        global,
        store,
        &mut claimed,
        coordinator.settings().lock_for,
        cancellation,
        move || async move { jobs::check_target_job_generation(store.db(), &preflight_job).await },
        move |provider| async move { provider.submit(request).await },
    )
    .await?
    {
        ProviderCallOutcome::Cancelled => return Ok(()),
        ProviderCallOutcome::StaleClaim => return Ok(()),
        ProviderCallOutcome::PreflightRejected => {
            finish_and_reconcile(store, &claimed, Utc::now()).await?;
            return Ok(());
        }
        ProviderCallOutcome::Completed(result) => result,
    };
    match result {
        Ok(remote) if valid_remote(&remote, &claimed.model.cid, None) => {
            apply_observation(
                store,
                coordinator,
                &claimed,
                remote,
                RemoteStatusOrigin::Adopt,
            )
            .await
        }
        Ok(_) => {
            mark_runtime_degraded(coordinator, &claimed.model.provider).await;
            let error = protocol_error();
            if !mark_submit_recovering(store, &claimed, &error).await? {
                return Ok(());
            }
            recover_submit(store, coordinator, global, &mut claimed, cancellation).await
        }
        Err(error) => {
            if !mark_submit_recovering(store, &claimed, &error).await? {
                return Ok(());
            }
            if matches!(
                error.class,
                ProviderErrorClass::Ambiguous
                    | ProviderErrorClass::Transient
                    | ProviderErrorClass::Protocol
            ) {
                recover_submit(store, coordinator, global, &mut claimed, cancellation).await
            } else {
                retry_submit_recovery(store, coordinator, &claimed, &error).await
            }
        }
    }
}

async fn mark_submit_recovering(
    store: &Store,
    claimed: &ClaimedPinJob,
    error: &ProviderError,
) -> AppResult<bool> {
    let now = Utc::now();
    let txn = store.db().begin().await?;
    if !jobs::fence_job_claim(&txn, &claimed.model.id, claimed_lock(claimed)?).await? {
        txn.rollback().await?;
        transition(claimed, "calling", "stale_claim_discarded", None);
        return Ok(false);
    }
    if error.class != ProviderErrorClass::Quota {
        leases::mark_all_mode_target_degraded_for_retry(&txn, &claimed.model, now).await?;
    }
    jobs::mark_submit_recovering_after_call(&txn, claimed, now, provider_error_label(error.class))
        .await?;
    txn.commit().await?;
    transition(claimed, "calling", "recovering", None);
    Ok(true)
}

async fn recover_submit(
    store: &Store,
    coordinator: &PinningCoordinator,
    global: &Arc<Semaphore>,
    claimed: &mut ClaimedPinJob,
    cancellation: &CancellationToken,
) -> AppResult<()> {
    let query = FindPin::for_job(&claimed.model.cid, &claimed.model.id);
    let Some(slot) = acquire_provider_slot(
        coordinator,
        store,
        claimed,
        coordinator.settings().lock_for,
        cancellation,
    )
    .await?
    else {
        return Ok(());
    };
    let result = match provider_call_with_heartbeat(
        slot,
        global,
        store,
        claimed,
        coordinator.settings().lock_for,
        cancellation,
        || async { Ok(true) },
        move |provider| async move { provider.find(query).await },
    )
    .await?
    {
        ProviderCallOutcome::Cancelled => return Ok(()),
        ProviderCallOutcome::StaleClaim => return Ok(()),
        ProviderCallOutcome::PreflightRejected => {
            unreachable!("unconditional recovery Find preflight")
        }
        ProviderCallOutcome::Completed(result) => result,
    };
    match result {
        Ok(found) => {
            let matching: Vec<_> = found
                .into_iter()
                .filter(|remote| valid_remote(remote, &claimed.model.cid, None))
                .collect();
            match matching.as_slice() {
                [remote] => {
                    apply_observation(
                        store,
                        coordinator,
                        claimed,
                        remote.clone(),
                        RemoteStatusOrigin::Adopt,
                    )
                    .await
                }
                [] => {
                    let txn = store.db().begin().await?;
                    if !jobs::fence_job_claim(&txn, &claimed.model.id, claimed_lock(claimed)?)
                        .await?
                    {
                        txn.rollback().await?;
                        transition(claimed, "recovering", "stale_claim_discarded", None);
                        return Ok(());
                    }
                    leases::mark_all_mode_target_degraded_for_retry(
                        &txn,
                        &claimed.model,
                        Utc::now(),
                    )
                    .await?;
                    let decision = jobs::record_submit_recovery_no_match(
                        &txn,
                        claimed,
                        Utc::now(),
                        coordinator.settings().base_backoff,
                    )
                    .await?;
                    txn.commit().await?;
                    transition(
                        claimed,
                        "recovering",
                        match decision {
                            jobs::SubmitRecoveryDecision::RetryScheduled { .. } => {
                                "recovery_backoff"
                            }
                            jobs::SubmitRecoveryDecision::NoLongerDesired { .. } => {
                                "done_current_reconcile"
                            }
                        },
                        None,
                    );
                    Ok(())
                }
                _ => {
                    mark_runtime_degraded(coordinator, &claimed.model.provider).await;
                    retry_submit_recovery(store, coordinator, claimed, &protocol_error()).await
                }
            }
        }
        Err(error) => retry_submit_recovery(store, coordinator, claimed, &error).await,
    }
}

async fn retry_submit_recovery(
    store: &Store,
    coordinator: &PinningCoordinator,
    claimed: &ClaimedPinJob,
    error: &ProviderError,
) -> AppResult<()> {
    let delay = provider_retry_delay(
        &claimed.model,
        error,
        coordinator.settings().base_backoff,
        coordinator.settings().max_backoff,
    );
    let now = Utc::now();
    let txn = store.db().begin().await?;
    if !jobs::fence_job_claim(&txn, &claimed.model.id, claimed_lock(claimed)?).await? {
        txn.rollback().await?;
        transition(claimed, "recovering", "stale_claim_discarded", None);
        return Ok(());
    }
    let evicted = if error.class == ProviderErrorClass::Quota {
        quota_coordination_hook(&txn, coordinator, &claimed.model, now).await?
    } else {
        leases::mark_all_mode_target_degraded_for_retry(&txn, &claimed.model, now).await?;
        Vec::new()
    };
    jobs::retry_submit_recovery(&txn, claimed, now, delay, provider_error_label(error.class))
        .await?;
    txn.commit().await?;
    if !evicted.is_empty() {
        coordinate_quota_evicted_one_targets(store, coordinator, &evicted, now).await?;
    }
    if error.class == ProviderErrorClass::Quota {
        tracing::info!(
            provider = %claimed.model.provider,
            cid = %claimed.model.cid,
            job_id = %claimed.model.id,
            eviction_count = evicted.len(),
            "pinning provider quota response coordinated"
        );
    }
    if matches!(
        error.class,
        ProviderErrorClass::Authentication | ProviderErrorClass::Terminal
    ) {
        coordinate_claimed_one_target(store, coordinator, &claimed.model, Utc::now()).await?;
    }
    transition(claimed, "recovering", "recovery_wait", None);
    Ok(())
}

async fn execute_poll(
    store: &Store,
    coordinator: &PinningCoordinator,
    global: &Arc<Semaphore>,
    mut claimed: ClaimedPinJob,
    cancellation: &CancellationToken,
) -> AppResult<()> {
    let Some(request_id) = current_poll_request(store, &claimed.model).await? else {
        finish_and_reconcile(store, &claimed, Utc::now()).await?;
        return Ok(());
    };
    let Some(slot) = acquire_provider_slot(
        coordinator,
        store,
        &mut claimed,
        coordinator.settings().lock_for,
        cancellation,
    )
    .await?
    else {
        return Ok(());
    };
    if current_poll_request(store, &claimed.model)
        .await?
        .as_deref()
        != Some(request_id.as_str())
    {
        finish_and_reconcile(store, &claimed, Utc::now()).await?;
        return Ok(());
    }

    let expected_request_id = request_id.clone();
    let preflight_job = claimed.model.clone();
    let preflight_request_id = request_id.clone();
    let result = match provider_call_with_heartbeat(
        slot,
        global,
        store,
        &mut claimed,
        coordinator.settings().lock_for,
        cancellation,
        move || async move {
            Ok(current_poll_request(store, &preflight_job)
                .await?
                .as_deref()
                == Some(preflight_request_id.as_str()))
        },
        move |provider| async move { provider.get(&request_id).await },
    )
    .await?
    {
        ProviderCallOutcome::Cancelled => return Ok(()),
        ProviderCallOutcome::StaleClaim => return Ok(()),
        ProviderCallOutcome::PreflightRejected => {
            finish_and_reconcile(store, &claimed, Utc::now()).await?;
            return Ok(());
        }
        ProviderCallOutcome::Completed(result) => result,
    };
    match result {
        Ok(remote)
            if valid_remote(
                &remote,
                &claimed.model.cid,
                Some(expected_request_id.as_str()),
            ) =>
        {
            apply_observation(
                store,
                coordinator,
                &claimed,
                remote,
                RemoteStatusOrigin::ExistingRequest,
            )
            .await
        }
        Ok(_) => {
            mark_runtime_degraded(coordinator, &claimed.model.provider).await;
            retry_ordinary_job(
                store,
                coordinator,
                &claimed,
                &protocol_error(),
                Some(&expected_request_id),
            )
            .await
        }
        Err(error) => {
            retry_ordinary_job(
                store,
                coordinator,
                &claimed,
                &error,
                Some(&expected_request_id),
            )
            .await
        }
    }
}

async fn current_poll_request(store: &Store, job: &pin_job::Model) -> AppResult<Option<String>> {
    if !jobs::check_target_job_generation(store.db(), job).await? {
        return Ok(None);
    }
    let Some(remote) = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(store.db())
        .await?
    else {
        return Ok(None);
    };
    if !matches!(remote.status.as_str(), STATUS_QUEUED | STATUS_PINNING) {
        return Ok(None);
    }
    let Some(request_id) = remote.request_id else {
        return Ok(None);
    };
    let NewPinJob::Target(expected_poll) = jobs::poll_job(
        &job.provider,
        &job.cid,
        job.lease_id.as_deref().expect("validated scope"),
        job.target_id.as_deref().expect("validated scope"),
        job.expected_generation.expect("validated scope"),
        &request_id,
        job.next_attempt_at,
    ) else {
        unreachable!("poll is target scoped")
    };
    Ok((expected_poll.id == job.id).then_some(request_id))
}

async fn apply_observation(
    store: &Store,
    coordinator: &PinningCoordinator,
    claimed: &ClaimedPinJob,
    remote: RemotePin,
    origin: RemoteStatusOrigin,
) -> AppResult<()> {
    let now = Utc::now();
    let failure_reason = (remote.status == RemotePinStatus::Failed).then(|| {
        remote
            .failure_reason
            .as_deref()
            .unwrap_or("remote pin failed")
    });
    let update = RemoteStatusUpdate {
        provider: &claimed.model.provider,
        cid: &claimed.model.cid,
        request_id: &remote.request_id,
        origin,
        status: remote.status,
        error_class: (remote.status == RemotePinStatus::Failed).then_some("remote_failed"),
        error_text: failure_reason,
        now,
    };
    let result = match persist_observation_status_phase(store, claimed, update).await? {
        PersistObservationResult::Applied(result) => result,
        PersistObservationResult::StaleClaim => {
            transition(claimed, "running", "stale_claim_discarded", None);
            return Ok(());
        }
    };
    if let RemoteStatusApplyResult::Applied {
        previous_status,
        current_status,
        ..
    } = &result
    {
        audit_status_projection(claimed, previous_status, current_status, &remote.request_id);
        #[cfg(test)]
        if let Err(error) =
            fail_once_after_observation_status_commit(&claimed.model.id, &remote.request_id).await
        {
            audit_follow_up_failure(claimed, "one_coordination", &error);
            return Err(error);
        }
    }
    let job_outcome;
    match result {
        RemoteStatusApplyResult::StaleRequest => {
            let finalized: AppResult<()> = async {
                let txn = store.db().begin().await?;
                complete_claimed_if_live(&txn, claimed, now).await?;
                ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now)
                    .await?;
                txn.commit().await?;
                Ok(())
            }
            .await;
            if let Err(error) = finalized {
                audit_follow_up_failure(claimed, "job_finalization", &error);
                return Err(error);
            }
            job_outcome = "stale_current_reconcile";
        }
        RemoteStatusApplyResult::Applied { affected, .. } => {
            if remote.status == RemotePinStatus::Failed {
                if let Err(error) =
                    coordinate_failed_one_outcomes(store, coordinator, &affected, now).await
                {
                    audit_follow_up_failure(claimed, "one_coordination", &error);
                    return Err(error);
                }
                let retry_result: AppResult<()> = async {
                    let txn = store.db().begin().await?;
                    leases::ensure_failed_remote_retry(
                        &txn,
                        &claimed.model.provider,
                        &claimed.model.cid,
                        now,
                    )
                    .await?;
                    txn.commit().await?;
                    Ok(())
                }
                .await;
                if let Err(error) = retry_result {
                    audit_follow_up_failure(claimed, "failed_remote_retry", &error);
                    return Err(error);
                }
            } else if remote.status == RemotePinStatus::Pinned
                && let Err(error) = converge_pinned_one_outcomes(store, &affected, now).await
            {
                audit_follow_up_failure(claimed, "one_convergence", &error);
                return Err(error);
            }

            let finalized: AppResult<&'static str> = async {
                let txn = store.db().begin().await?;
                let outcome = if claimed.model.operation == "poll"
                    && matches!(
                        remote.status,
                        RemotePinStatus::Queued | RemotePinStatus::Pinning
                    ) {
                    let snapshot = leases::remote_work_snapshot(
                        &txn,
                        &claimed.model.provider,
                        &claimed.model.cid,
                    )
                    .await?;
                    let same_owner = snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.desired.first())
                        .is_some_and(|owner| {
                            Some(owner.target_id.as_str()) == claimed.model.target_id.as_deref()
                                && Some(owner.generation) == claimed.model.expected_generation
                        });
                    if same_owner {
                        jobs::reschedule_poll_job(
                            &txn,
                            &claimed.model.id,
                            claimed_lock(claimed)?,
                            now,
                            coordinator.settings().poll_interval,
                        )
                        .await?;
                        "poll_rescheduled"
                    } else {
                        complete_claimed_if_live(&txn, claimed, now).await?;
                        "poll_handoff"
                    }
                } else {
                    complete_claimed_if_live(&txn, claimed, now).await?;
                    if affected.is_empty() {
                        "done_current_reconcile"
                    } else {
                        "done"
                    }
                };
                if affected.is_empty() {
                    ensure_current_reconcile(
                        &txn,
                        &claimed.model.provider,
                        &claimed.model.cid,
                        now,
                    )
                    .await?;
                }
                txn.commit().await?;
                Ok(outcome)
            }
            .await;
            match finalized {
                Ok(outcome) => job_outcome = outcome,
                Err(error) => {
                    audit_follow_up_failure(claimed, "job_finalization", &error);
                    return Err(error);
                }
            }
        }
    }
    transition(claimed, "running", job_outcome, Some(&remote.request_id));
    Ok(())
}

enum PersistObservationResult {
    Applied(RemoteStatusApplyResult),
    StaleClaim,
}

async fn persist_observation_status_phase(
    store: &Store,
    claimed: &ClaimedPinJob,
    update: RemoteStatusUpdate<'_>,
) -> AppResult<PersistObservationResult> {
    let txn = store.db().begin().await?;
    if !jobs::fence_job_claim(&txn, &claimed.model.id, claimed_lock(claimed)?).await? {
        txn.rollback().await?;
        return Ok(PersistObservationResult::StaleClaim);
    }
    let result = if claimed.model.operation == "reconcile" {
        leases::apply_reconcile_remote_status(
            &txn,
            claimed
                .model
                .expected_remote_epoch
                .expect("validated remote scope"),
            update,
        )
        .await?
    } else {
        leases::apply_worker_remote_status(&txn, update).await?
    };
    txn.commit().await?;
    #[cfg(test)]
    {
        leases::record_worker_transaction_boundary_for_test(
            &claimed.model.provider,
            &claimed.model.cid,
        )
        .await;
    }
    Ok(PersistObservationResult::Applied(result))
}

async fn execute_unpin(
    store: &Store,
    coordinator: &PinningCoordinator,
    global: &Arc<Semaphore>,
    mut claimed: ClaimedPinJob,
    cancellation: &CancellationToken,
) -> AppResult<()> {
    let expected_epoch = claimed
        .model
        .expected_remote_epoch
        .expect("validated scope");
    let Some(request_id) = current_unpin_request(store, &claimed.model).await? else {
        finish_and_reconcile(store, &claimed, Utc::now()).await?;
        return Ok(());
    };
    let Some(slot) = acquire_provider_slot(
        coordinator,
        store,
        &mut claimed,
        coordinator.settings().lock_for,
        cancellation,
    )
    .await?
    else {
        return Ok(());
    };
    if current_unpin_request(store, &claimed.model)
        .await?
        .as_deref()
        != Some(request_id.as_str())
    {
        finish_and_reconcile(store, &claimed, Utc::now()).await?;
        return Ok(());
    }

    let request_for_call = request_id.clone();
    let preflight_job = claimed.model.clone();
    let preflight_request_id = request_id.clone();
    let result = match provider_call_with_heartbeat(
        slot,
        global,
        store,
        &mut claimed,
        coordinator.settings().lock_for,
        cancellation,
        move || async move {
            Ok(current_unpin_request(store, &preflight_job)
                .await?
                .as_deref()
                == Some(preflight_request_id.as_str()))
        },
        move |provider| async move { provider.unpin(&request_for_call).await },
    )
    .await?
    {
        ProviderCallOutcome::Cancelled => return Ok(()),
        ProviderCallOutcome::StaleClaim => return Ok(()),
        ProviderCallOutcome::PreflightRejected => {
            finish_and_reconcile(store, &claimed, Utc::now()).await?;
            return Ok(());
        }
        ProviderCallOutcome::Completed(result) => result,
    };
    match result {
        Ok(())
        | Err(ProviderError {
            class: ProviderErrorClass::NotFound,
            ..
        }) => {
            let now = Utc::now();
            let txn = store.db().begin().await?;
            if !jobs::fence_job_claim(&txn, &claimed.model.id, claimed_lock(&claimed)?).await? {
                txn.rollback().await?;
                transition(
                    &claimed,
                    "running",
                    "stale_claim_discarded",
                    Some(&request_id),
                );
                return Ok(());
            }
            let completion = leases::complete_remote_delete(
                &txn,
                &claimed.model.provider,
                &claimed.model.cid,
                expected_epoch,
                now,
            )
            .await?;
            complete_claimed_if_live(&txn, &claimed, now).await?;
            txn.commit().await?;
            let new_state = match completion {
                RemoteDeleteCompletion::Released => {
                    wake_provider_waiters_after_release(
                        store,
                        coordinator,
                        &claimed.model.provider,
                        now,
                    )
                    .await?;
                    "released"
                }
                RemoteDeleteCompletion::Compensated { .. } => "compensated",
                RemoteDeleteCompletion::ReconcileRequired { .. } => "reconcile_required",
            };
            transition(&claimed, "running", new_state, Some(&request_id));
            Ok(())
        }
        Err(error) => {
            retry_ordinary_job(store, coordinator, &claimed, &error, Some(&request_id)).await
        }
    }
}

async fn current_unpin_request(store: &Store, job: &pin_job::Model) -> AppResult<Option<String>> {
    let Some(snapshot) = leases::remote_work_snapshot(store.db(), &job.provider, &job.cid).await?
    else {
        return Ok(None);
    };
    if snapshot.remote.epoch != job.expected_remote_epoch.expect("validated scope")
        || !snapshot.desired.is_empty()
    {
        return Ok(None);
    }
    Ok(snapshot.remote.request_id)
}

async fn execute_reconcile(
    store: &Store,
    coordinator: &PinningCoordinator,
    global: &Arc<Semaphore>,
    claimed: ClaimedPinJob,
    cancellation: &CancellationToken,
) -> AppResult<()> {
    let expected_epoch = claimed
        .model
        .expected_remote_epoch
        .expect("validated scope");
    let Some(snapshot) =
        leases::remote_work_snapshot(store.db(), &claimed.model.provider, &claimed.model.cid)
            .await?
    else {
        complete_claimed_if_live(store.db(), &claimed, Utc::now()).await?;
        return Ok(());
    };
    if snapshot.remote.epoch != expected_epoch {
        finish_and_reconcile(store, &claimed, Utc::now()).await?;
        return Ok(());
    }

    #[cfg(test)]
    pause_reconcile_after_snapshot(expected_epoch).await;

    if snapshot.desired.is_empty() {
        return reconcile_without_desired(store, coordinator, &claimed, snapshot).await;
    }
    match (
        snapshot.remote.status.as_str(),
        snapshot.remote.request_id.clone(),
    ) {
        (STATUS_RESERVED, None) => {
            let now = Utc::now();
            let txn = store.db().begin().await?;
            let projected = leases::project_reconcile_target_from_remote(
                &txn,
                &snapshot.desired[0].target_id,
                expected_epoch,
                now,
            )
            .await?;
            complete_claimed_if_live(&txn, &claimed, now).await?;
            if projected.is_none() {
                ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now)
                    .await?;
            }
            txn.commit().await?;
            transition(
                &claimed,
                "running",
                if projected.is_some() {
                    "submit_ready"
                } else {
                    "stale_current_reconcile"
                },
                None,
            );
            Ok(())
        }
        (STATUS_QUEUED | STATUS_PINNING | STATUS_PINNED, Some(request_id)) => {
            let status = persisted_status(&snapshot.remote.status)?;
            apply_observation(
                store,
                coordinator,
                &claimed,
                RemotePin {
                    request_id,
                    cid: claimed.model.cid.clone(),
                    status,
                    raw_status: snapshot.remote.status,
                    failure_reason: None,
                },
                RemoteStatusOrigin::ExistingRequest,
            )
            .await
        }
        (STATUS_FAILED, Some(_)) => {
            reconcile_failed(store, coordinator, global, claimed, cancellation).await
        }
        (STATUS_ABSENT, None) => {
            let current = complete_reconcile_at_expected_epoch(store, &claimed, Utc::now()).await?;
            transition(
                &claimed,
                "running",
                if current {
                    "done"
                } else {
                    "stale_current_reconcile"
                },
                None,
            );
            Ok(())
        }
        _ => {
            retry_ordinary_job(
                store,
                coordinator,
                &claimed,
                &protocol_error(),
                snapshot.remote.request_id.as_deref(),
            )
            .await
        }
    }
}

async fn reconcile_without_desired(
    store: &Store,
    coordinator: &PinningCoordinator,
    claimed: &ClaimedPinJob,
    snapshot: RemoteWorkSnapshot,
) -> AppResult<()> {
    let now = Utc::now();
    let txn = store.db().begin().await?;
    if snapshot.remote.request_id.is_some() {
        let epoch_current = leases::guard_reconcile_remote_epoch(
            &txn,
            &claimed.model.provider,
            &claimed.model.cid,
            claimed
                .model
                .expected_remote_epoch
                .expect("validated scope"),
        )
        .await?;
        if epoch_current {
            ensure_current_unpin(&txn, &snapshot.remote, now).await?;
        } else {
            ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now)
                .await?;
        }
        complete_claimed_if_live(&txn, claimed, now).await?;
        txn.commit().await?;
        transition(
            claimed,
            "running",
            if epoch_current {
                "unpin_ready"
            } else {
                "stale_current_reconcile"
            },
            snapshot.remote.request_id.as_deref(),
        );
        return Ok(());
    }
    let completion = leases::complete_no_request_remote_absence(
        &txn,
        &claimed.model.provider,
        &claimed.model.cid,
        claimed
            .model
            .expected_remote_epoch
            .expect("validated scope"),
        now,
    )
    .await?;
    let released = completion == NoRequestRemoteCompletion::Released;
    let outcome_state = match completion {
        NoRequestRemoteCompletion::Released => {
            complete_claimed_if_live(&txn, claimed, now).await?;
            "released"
        }
        NoRequestRemoteCompletion::Wait { next_check_at } => {
            let minimum = now + chrono_duration(Duration::from_secs(1))?;
            jobs::reschedule_reconcile_job(
                &txn,
                &claimed.model.id,
                claimed_lock(claimed)?,
                next_check_at.max(minimum),
            )
            .await?;
            "reconcile_wait"
        }
        NoRequestRemoteCompletion::Stale => {
            complete_claimed_if_live(&txn, claimed, now).await?;
            ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now)
                .await?;
            "stale_current_reconcile"
        }
    };
    txn.commit().await?;
    if released {
        wake_provider_waiters_after_release(store, coordinator, &claimed.model.provider, now)
            .await?;
    }
    transition(claimed, "running", outcome_state, None);
    Ok(())
}

async fn reconcile_failed(
    store: &Store,
    coordinator: &PinningCoordinator,
    global: &Arc<Semaphore>,
    mut claimed: ClaimedPinJob,
    cancellation: &CancellationToken,
) -> AppResult<()> {
    let now = Utc::now();
    let initial =
        leases::remote_work_snapshot(store.db(), &claimed.model.provider, &claimed.model.cid)
            .await?
            .ok_or_else(|| AppError::Internal("failed remote disappeared".to_owned()))?;
    let request_id =
        initial.remote.request_id.clone().ok_or_else(|| {
            AppError::Internal("failed remote has no request identity".to_owned())
        })?;
    let failure_reason = initial
        .remote
        .last_error_text
        .as_deref()
        .unwrap_or("remote pin failed");

    let applied = match persist_observation_status_phase(
        store,
        &claimed,
        RemoteStatusUpdate {
            provider: &claimed.model.provider,
            cid: &claimed.model.cid,
            request_id: &request_id,
            origin: RemoteStatusOrigin::ExistingRequest,
            status: RemotePinStatus::Failed,
            error_class: Some("remote_failed"),
            error_text: Some(failure_reason),
            now,
        },
    )
    .await?
    {
        PersistObservationResult::Applied(applied) => applied,
        PersistObservationResult::StaleClaim => {
            transition(
                &claimed,
                "running",
                "stale_claim_discarded",
                Some(&request_id),
            );
            return Ok(());
        }
    };
    match applied {
        RemoteStatusApplyResult::StaleRequest => {
            let txn = store.db().begin().await?;
            complete_claimed_if_live(&txn, &claimed, now).await?;
            ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now)
                .await?;
            txn.commit().await?;
            transition(
                &claimed,
                "running",
                "stale_current_reconcile",
                Some(&request_id),
            );
            return Ok(());
        }
        RemoteStatusApplyResult::Applied {
            affected,
            previous_status,
            current_status,
            ..
        } => {
            audit_status_projection(&claimed, &previous_status, &current_status, &request_id);
            #[cfg(test)]
            if let Err(error) =
                fail_once_after_observation_status_commit(&claimed.model.id, &request_id).await
            {
                audit_follow_up_failure(&claimed, "one_coordination", &error);
                return Err(error);
            }
            if let Err(error) =
                coordinate_failed_one_outcomes(store, coordinator, &affected, now).await
            {
                audit_follow_up_failure(&claimed, "one_coordination", &error);
                return Err(error);
            }
            let retry_result: AppResult<()> = async {
                let txn = store.db().begin().await?;
                leases::ensure_failed_remote_retry(
                    &txn,
                    &claimed.model.provider,
                    &claimed.model.cid,
                    now,
                )
                .await?;
                txn.commit().await?;
                Ok(())
            }
            .await;
            if let Err(error) = retry_result {
                audit_follow_up_failure(&claimed, "failed_remote_retry", &error);
                return Err(error);
            }
        }
    }

    let snapshot =
        leases::remote_work_snapshot(store.db(), &claimed.model.provider, &claimed.model.cid)
            .await?
            .ok_or_else(|| AppError::Internal("failed remote disappeared".to_owned()))?;
    if snapshot.remote.epoch
        != claimed
            .model
            .expected_remote_epoch
            .expect("validated scope")
        || snapshot.remote.request_id.as_deref() != Some(request_id.as_str())
    {
        finish_and_reconcile(store, &claimed, Utc::now()).await?;
        return Ok(());
    }
    if snapshot.desired.is_empty() {
        return reconcile_without_desired(store, coordinator, &claimed, snapshot).await;
    }
    if !snapshot
        .desired
        .iter()
        .any(|target| target.provider_mode == ProviderMode::All)
    {
        let current = complete_reconcile_at_expected_epoch(store, &claimed, Utc::now()).await?;
        transition(
            &claimed,
            "running",
            if current {
                "done"
            } else {
                "stale_current_reconcile"
            },
            Some(&request_id),
        );
        return Ok(());
    }
    if snapshot.remote.failure_attempts >= MAX_FAILED_REQUEST_ATTEMPTS
        || snapshot.remote.next_retry_at.is_none()
    {
        let current = complete_reconcile_at_expected_epoch(store, &claimed, Utc::now()).await?;
        transition(
            &claimed,
            "running",
            if current {
                "degraded_exhausted"
            } else {
                "stale_current_reconcile"
            },
            Some(&request_id),
        );
        return Ok(());
    }
    let due = snapshot.remote.next_retry_at.expect("checked above");
    let now = Utc::now();
    if due > now {
        let current = reschedule_reconcile_at_expected_epoch(store, &claimed, due, now).await?;
        transition(
            &claimed,
            "running",
            if current {
                "reconcile_wait"
            } else {
                "stale_current_reconcile"
            },
            Some(&request_id),
        );
        return Ok(());
    }

    let Some(slot) = acquire_provider_slot(
        coordinator,
        store,
        &mut claimed,
        coordinator.settings().lock_for,
        cancellation,
    )
    .await?
    else {
        return Ok(());
    };
    if !failed_delete_is_current(store, &claimed.model, &request_id, Utc::now()).await? {
        finish_and_reconcile(store, &claimed, Utc::now()).await?;
        return Ok(());
    }
    let request_for_call = request_id.clone();
    let preflight_job = claimed.model.clone();
    let preflight_request_id = request_id.clone();
    let result = match provider_call_with_heartbeat(
        slot,
        global,
        store,
        &mut claimed,
        coordinator.settings().lock_for,
        cancellation,
        move || async move {
            failed_delete_is_current(store, &preflight_job, &preflight_request_id, Utc::now()).await
        },
        move |provider| async move { provider.unpin(&request_for_call).await },
    )
    .await?
    {
        ProviderCallOutcome::Cancelled => return Ok(()),
        ProviderCallOutcome::StaleClaim => return Ok(()),
        ProviderCallOutcome::PreflightRejected => {
            finish_and_reconcile(store, &claimed, Utc::now()).await?;
            return Ok(());
        }
        ProviderCallOutcome::Completed(result) => result,
    };
    match result {
        Ok(())
        | Err(ProviderError {
            class: ProviderErrorClass::NotFound,
            ..
        }) => {
            let now = Utc::now();
            let txn = store.db().begin().await?;
            if !jobs::fence_job_claim(&txn, &claimed.model.id, claimed_lock(&claimed)?).await? {
                txn.rollback().await?;
                transition(
                    &claimed,
                    "running",
                    "stale_claim_discarded",
                    Some(&request_id),
                );
                return Ok(());
            }
            let decision = leases::prepare_failed_remote_resubmit(
                &txn,
                &claimed.model.provider,
                &claimed.model.cid,
                claimed
                    .model
                    .expected_remote_epoch
                    .expect("validated scope"),
                &request_id,
                now,
            )
            .await?;
            complete_claimed_if_live(&txn, &claimed, now).await?;
            if decision == FailedRemoteResubmitDecision::Stale {
                ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now)
                    .await?;
            }
            txn.commit().await?;
            let outcome = match decision {
                FailedRemoteResubmitDecision::Prepared { .. } => "resubmit_ready",
                FailedRemoteResubmitDecision::Stale => "stale_current_reconcile",
                FailedRemoteResubmitDecision::NoAllModeTarget => "done_no_all_target",
                FailedRemoteResubmitDecision::Exhausted => "degraded_exhausted",
            };
            transition(&claimed, "running", outcome, Some(&request_id));
            Ok(())
        }
        Err(error) => {
            retry_ordinary_job(store, coordinator, &claimed, &error, Some(&request_id)).await
        }
    }
}

async fn failed_delete_is_current(
    store: &Store,
    job: &pin_job::Model,
    request_id: &str,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let Some(snapshot) = leases::remote_work_snapshot(store.db(), &job.provider, &job.cid).await?
    else {
        return Ok(false);
    };
    Ok(
        snapshot.remote.epoch == job.expected_remote_epoch.expect("validated scope")
            && snapshot.remote.status == STATUS_FAILED
            && snapshot.remote.request_id.as_deref() == Some(request_id)
            && snapshot.remote.failure_attempts < MAX_FAILED_REQUEST_ATTEMPTS
            && snapshot.remote.next_retry_at.is_some_and(|due| due <= now)
            && snapshot
                .desired
                .iter()
                .any(|target| target.provider_mode == ProviderMode::All),
    )
}

async fn retry_ordinary_job(
    store: &Store,
    coordinator: &PinningCoordinator,
    claimed: &ClaimedPinJob,
    error: &ProviderError,
    request_id: Option<&str>,
) -> AppResult<()> {
    let delay = provider_retry_delay(
        &claimed.model,
        error,
        coordinator.settings().base_backoff,
        coordinator.settings().max_backoff,
    );
    let now = Utc::now();
    let current = if claimed.model.operation == "reconcile" {
        let txn = store.db().begin().await?;
        let current = leases::guard_reconcile_remote_epoch(
            &txn,
            &claimed.model.provider,
            &claimed.model.cid,
            claimed
                .model
                .expected_remote_epoch
                .expect("validated remote scope"),
        )
        .await?;
        if current {
            jobs::retry_job(
                &txn,
                &claimed.model.id,
                claimed_lock(claimed)?,
                now,
                delay,
                delay,
                coordinator.settings().max_attempts,
                provider_error_label(error.class),
            )
            .await?;
        } else {
            complete_claimed_if_live(&txn, claimed, now).await?;
            ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now)
                .await?;
        }
        txn.commit().await?;
        current
    } else {
        jobs::retry_job(
            store.db(),
            &claimed.model.id,
            claimed_lock(claimed)?,
            now,
            delay,
            delay,
            coordinator.settings().max_attempts,
            provider_error_label(error.class),
        )
        .await?;
        true
    };
    if matches!(
        error.class,
        ProviderErrorClass::Authentication | ProviderErrorClass::Terminal
    ) {
        coordinate_claimed_one_target(store, coordinator, &claimed.model, now).await?;
    }
    transition(
        claimed,
        "running",
        if current {
            "retry_scheduled"
        } else {
            "stale_current_reconcile"
        },
        request_id,
    );
    Ok(())
}

async fn park_exhausted_ordinary_job(
    store: &Store,
    coordinator: &PinningCoordinator,
    mut claimed: ClaimedPinJob,
) -> AppResult<()> {
    let request_id =
        remote_pin::Entity::find_by_id((claimed.model.provider.clone(), claimed.model.cid.clone()))
            .one(store.db())
            .await?
            .and_then(|remote| remote.request_id);
    if claimed.model.operation == "reconcile" {
        let current = complete_reconcile_at_expected_epoch(store, &claimed, Utc::now()).await?;
        transition(
            &claimed,
            "running",
            if current {
                "coordination_required"
            } else {
                "stale_current_reconcile"
            },
            request_id.as_deref(),
        );
        return Ok(());
    }
    if claimed.model.target_id.is_none() {
        complete_claimed_if_live(store.db(), &claimed, Utc::now()).await?;
        transition(
            &claimed,
            "running",
            "coordination_required",
            request_id.as_deref(),
        );
        return Ok(());
    }

    if !extend_exhausted_coordination_claim(store, &mut claimed, coordinator.settings().lock_for)
        .await?
    {
        transition(
            &claimed,
            "running",
            "stale_claim_handoff",
            request_id.as_deref(),
        );
        return Ok(());
    }
    #[cfg(test)]
    {
        leases::record_worker_job_event_for_test(
            &claimed.model.provider,
            &claimed.model.cid,
            leases::test_gates::LifecycleOrderEvent::JobClaimLock(claimed.model.id.clone()),
        )
        .await;
        leases::record_worker_transaction_boundary_for_test(
            &claimed.model.provider,
            &claimed.model.cid,
        )
        .await;
    }
    #[cfg(test)]
    interrupt_exhausted_coordination(
        &claimed.model.id,
        ExhaustedCoordinationPhase::BeforeCoordination,
    )
    .await?;
    let coordination =
        match coordinate_claimed_one_target(store, coordinator, &claimed.model, Utc::now()).await {
            Ok(coordination) => coordination,
            Err(error) => {
                audit_follow_up_failure(&claimed, "one_coordination", &error);
                return Err(error);
            }
        };
    #[cfg(test)]
    leases::record_worker_transaction_boundary_for_test(
        &claimed.model.provider,
        &claimed.model.cid,
    )
    .await;
    if coordination == ClaimedOneCoordination::Stale {
        finish_and_reconcile(store, &claimed, Utc::now()).await?;
        return Ok(());
    }
    #[cfg(test)]
    interrupt_exhausted_coordination(
        &claimed.model.id,
        ExhaustedCoordinationPhase::AfterCoordination,
    )
    .await?;
    jobs::complete_job(
        store.db(),
        &claimed.model.id,
        claimed_lock(&claimed)?,
        Utc::now(),
    )
    .await?;
    #[cfg(test)]
    leases::record_worker_job_event_for_test(
        &claimed.model.provider,
        &claimed.model.cid,
        leases::test_gates::LifecycleOrderEvent::JobComplete(claimed.model.id.clone()),
    )
    .await;
    tracing::warn!(
        provider = %claimed.model.provider,
        cid = %claimed.model.cid,
        lease_id = ?claimed.model.lease_id,
        target_id = ?claimed.model.target_id,
        job_id = %claimed.model.id,
        attempts = claimed.model.attempts,
        "pinning ordinary retry exhausted; coordination hook parked work"
    );
    transition(
        &claimed,
        "running",
        "coordination_required",
        request_id.as_deref(),
    );
    Ok(())
}

async fn extend_exhausted_coordination_claim(
    store: &Store,
    claimed: &mut ClaimedPinJob,
    lock_for: ChronoDuration,
) -> AppResult<bool> {
    let expected = claimed_lock(claimed)?;
    let now = Utc::now();
    let extension = lock_for
        .checked_mul(2)
        .ok_or_else(|| AppError::Internal("worker lock duration overflow".to_owned()))?;
    let minimum = expected
        .checked_add_signed(ChronoDuration::nanoseconds(1))
        .ok_or_else(|| AppError::Internal("worker claim timestamp overflow".to_owned()))?;
    let new_locked_until = (now + extension).max(minimum);
    let Some(renewed) = jobs::renew_job_claim(
        store.db(),
        &claimed.model.id,
        expected,
        new_locked_until,
        now,
    )
    .await?
    else {
        return Ok(false);
    };
    claimed.model.locked_until = Some(renewed);
    Ok(true)
}

async fn finish_and_reconcile(
    store: &Store,
    claimed: &ClaimedPinJob,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let txn = store.db().begin().await?;
    complete_claimed_if_live(&txn, claimed, now).await?;
    ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now).await?;
    txn.commit().await?;
    transition(claimed, "running", "stale_current_reconcile", None);
    Ok(())
}

async fn complete_reconcile_at_expected_epoch(
    store: &Store,
    claimed: &ClaimedPinJob,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let txn = store.db().begin().await?;
    let current = leases::guard_reconcile_remote_epoch(
        &txn,
        &claimed.model.provider,
        &claimed.model.cid,
        claimed
            .model
            .expected_remote_epoch
            .expect("validated remote scope"),
    )
    .await?;
    complete_claimed_if_live(&txn, claimed, now).await?;
    if !current {
        ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now).await?;
    }
    txn.commit().await?;
    Ok(current)
}

async fn reschedule_reconcile_at_expected_epoch(
    store: &Store,
    claimed: &ClaimedPinJob,
    due: DateTime<Utc>,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let txn = store.db().begin().await?;
    let current = leases::guard_reconcile_remote_epoch(
        &txn,
        &claimed.model.provider,
        &claimed.model.cid,
        claimed
            .model
            .expected_remote_epoch
            .expect("validated remote scope"),
    )
    .await?;
    if current {
        jobs::reschedule_reconcile_job(&txn, &claimed.model.id, claimed_lock(claimed)?, due)
            .await?;
    } else {
        complete_claimed_if_live(&txn, claimed, now).await?;
        ensure_current_reconcile(&txn, &claimed.model.provider, &claimed.model.cid, now).await?;
    }
    txn.commit().await?;
    Ok(current)
}

async fn complete_claimed_if_live<C: sea_orm::ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let Some(current) = pin_job::Entity::find_by_id(claimed.model.id.clone())
        .one(db)
        .await?
    else {
        return Ok(());
    };
    if current.state == STATE_DONE {
        return Ok(());
    }
    jobs::complete_job(db, &claimed.model.id, claimed_lock(claimed)?, now).await
}

async fn ensure_current_reconcile<C: sea_orm::ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(());
    };
    let NewPinJob::Remote(job) = jobs::reconcile_job(provider, cid, remote.epoch, now) else {
        unreachable!("reconcile is remote scoped")
    };
    jobs::ensure_or_reactivate_reconcile_job(db, job, now).await?;
    Ok(())
}

async fn ensure_current_unpin<C: sea_orm::ConnectionTrait>(
    db: &C,
    remote: &remote_pin::Model,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let NewPinJob::Remote(job) = jobs::unpin_job(&remote.provider, &remote.cid, remote.epoch, now)
    else {
        unreachable!("unpin is remote scoped")
    };
    jobs::ensure_or_reactivate_unpin_job(db, job, now).await?;
    Ok(())
}

struct SubmitContext {
    lease: pin_lease::Model,
    target: pin_lease_target::Model,
    owner: object::Model,
}

async fn load_submit_context(
    store: &Store,
    job: &pin_job::Model,
) -> AppResult<Option<SubmitContext>> {
    let (Some(lease_id), Some(target_id)) = (job.lease_id.as_deref(), job.target_id.as_deref())
    else {
        return Ok(None);
    };
    let Some(lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(store.db())
        .await?
    else {
        return Ok(None);
    };
    let Some(target) = pin_lease_target::Entity::find_by_id(target_id.to_owned())
        .one(store.db())
        .await?
    else {
        return Ok(None);
    };
    let Some(owner) = object::Entity::find_by_id(lease.owner_object_id.clone())
        .one(store.db())
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(SubmitContext {
        lease,
        target,
        owner,
    }))
}

struct ProviderCallSlot {
    provider: Arc<dyn PinningProvider>,
    runtime: ProviderRuntime,
    _provider_permit: OwnedSemaphorePermit,
}

async fn acquire_provider_slot(
    coordinator: &PinningCoordinator,
    store: &Store,
    claimed: &mut ClaimedPinJob,
    lock_for: ChronoDuration,
    cancellation: &CancellationToken,
) -> AppResult<Option<ProviderCallSlot>> {
    if cancellation.is_cancelled() {
        return Ok(None);
    }
    let provider_name = &claimed.model.provider;
    let provider = coordinator
        .provider(provider_name)
        .ok_or_else(|| AppError::Internal(format!("unknown pinning provider `{provider_name}`")))?;
    let runtime = coordinator
        .provider_runtime(provider_name)
        .cloned()
        .ok_or_else(|| {
            AppError::Internal(format!(
                "missing runtime for pinning provider `{provider_name}`"
            ))
        })?;
    renew_claim_once(store, claimed, lock_for).await?;
    let permit = runtime.concurrency.clone().acquire_owned();
    let ClaimWait::Ready(provider_permit) =
        wait_with_claim_heartbeat(store, claimed, lock_for, cancellation, permit).await?
    else {
        return Ok(None);
    };
    let provider_permit =
        provider_permit.map_err(|_| AppError::Internal("provider semaphore closed".to_owned()))?;
    Ok(Some(ProviderCallSlot {
        provider,
        runtime,
        _provider_permit: provider_permit,
    }))
}

enum ClaimWait<T> {
    Ready(T),
    Cancelled,
}

enum ProviderCallOutcome<T> {
    Completed(Result<T, ProviderError>),
    PreflightRejected,
    StaleClaim,
    Cancelled,
}

async fn wait_with_claim_heartbeat<F>(
    store: &Store,
    claimed: &mut ClaimedPinJob,
    lock_for: ChronoDuration,
    cancellation: &CancellationToken,
    future: F,
) -> AppResult<ClaimWait<F::Output>>
where
    F: Future,
{
    let heartbeat = claim_heartbeat_period(lock_for)?;
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + heartbeat, heartbeat);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut future = Box::pin(future);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(ClaimWait::Cancelled),
            result = &mut future => return Ok(ClaimWait::Ready(result)),
            _ = interval.tick() => renew_claim_once(store, claimed, lock_for).await?,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn provider_call_with_heartbeat<T, P, PFut, F, Fut>(
    slot: ProviderCallSlot,
    global: &Arc<Semaphore>,
    store: &Store,
    claimed: &mut ClaimedPinJob,
    lock_for: ChronoDuration,
    cancellation: &CancellationToken,
    preflight: P,
    call: F,
) -> AppResult<ProviderCallOutcome<T>>
where
    P: FnOnce() -> PFut,
    PFut: Future<Output = AppResult<bool>>,
    F: FnOnce(Arc<dyn PinningProvider>) -> Fut,
    Fut: Future<Output = Result<T, ProviderError>>,
{
    renew_claim_once(store, claimed, lock_for).await?;
    let rate_lock = slot.runtime.next_request_at.clone().lock_owned();
    let ClaimWait::Ready(mut rate_gate) =
        wait_with_claim_heartbeat(store, claimed, lock_for, cancellation, rate_lock).await?
    else {
        return Ok(ProviderCallOutcome::Cancelled);
    };
    if let Some(at) = *rate_gate {
        let ClaimWait::Ready(()) = wait_with_claim_heartbeat(
            store,
            claimed,
            lock_for,
            cancellation,
            tokio::time::sleep_until(at),
        )
        .await?
        else {
            return Ok(ProviderCallOutcome::Cancelled);
        };
    }
    if *slot.runtime.health.read().await == ProviderHealth::Terminal {
        return Ok(ProviderCallOutcome::Completed(Err(terminal_health_error())));
    }

    // Rate waiting is complete before a global HTTP permit is requested. Slow providers can hold
    // only their own permit/start gate while sleeping, never global capacity needed by peers.
    let global_permit = global.clone().acquire_owned();
    let ClaimWait::Ready(global_permit) =
        wait_with_claim_heartbeat(store, claimed, lock_for, cancellation, global_permit).await?
    else {
        return Ok(ProviderCallOutcome::Cancelled);
    };
    let _global_permit =
        global_permit.map_err(|_| AppError::Internal("worker semaphore closed".to_owned()))?;

    let ClaimWait::Ready(current) =
        wait_with_claim_heartbeat(store, claimed, lock_for, cancellation, preflight()).await?
    else {
        return Ok(ProviderCallOutcome::Cancelled);
    };
    if !current? {
        return Ok(ProviderCallOutcome::PreflightRejected);
    }
    // Revalidate the exact durable claim after every wait and the operation preflight,
    // immediately before reserving the rate start and beginning HTTP.
    renew_claim_once(store, claimed, lock_for).await?;
    if *slot.runtime.health.read().await == ProviderHealth::Terminal {
        return Ok(ProviderCallOutcome::Completed(Err(terminal_health_error())));
    }
    if cancellation.is_cancelled() {
        return Ok(ProviderCallOutcome::Cancelled);
    }
    if !slot.runtime.min_request_interval.is_zero() {
        *rate_gate = Some(tokio::time::Instant::now() + slot.runtime.min_request_interval);
    }
    drop(rate_gate);
    let heartbeat = claim_heartbeat_period(lock_for)?;
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + heartbeat, heartbeat);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let provider = slot.provider.clone();
    let mut future = Box::pin(call(provider));
    let result = loop {
        tokio::select! {
            result = &mut future => break result,
            _ = interval.tick() => renew_claim_once(store, claimed, lock_for).await?,
        }
    };
    if !jobs::fence_job_claim(store.db(), &claimed.model.id, claimed_lock(claimed)?).await? {
        return Ok(ProviderCallOutcome::StaleClaim);
    }
    update_provider_health(&slot.runtime, &result).await;
    Ok(ProviderCallOutcome::Completed(result))
}

fn claim_heartbeat_period(lock_for: ChronoDuration) -> AppResult<Duration> {
    Ok(lock_for
        .to_std()
        .map_err(|_| AppError::Internal("negative worker lock duration".to_owned()))?
        .checked_div(3)
        .unwrap_or(Duration::from_millis(100))
        .max(Duration::from_millis(10)))
}

async fn renew_claim_once(
    store: &Store,
    claimed: &mut ClaimedPinJob,
    lock_for: ChronoDuration,
) -> AppResult<()> {
    let expected = claimed_lock(claimed)?;
    let now = Utc::now();
    let extension = lock_for
        .checked_mul(2)
        .ok_or_else(|| AppError::Internal("worker lock duration overflow".to_owned()))?;
    let new_locked_until = now + extension;
    if new_locked_until <= expected {
        return Ok(());
    }
    let Some(renewed) = jobs::renew_job_claim(
        store.db(),
        &claimed.model.id,
        expected,
        new_locked_until,
        now,
    )
    .await?
    else {
        return Err(AppError::Database(format!(
            "pin job claim lost: {}",
            claimed.model.id
        )));
    };
    claimed.model.locked_until = Some(renewed);
    Ok(())
}

async fn update_provider_health<T>(runtime: &ProviderRuntime, result: &Result<T, ProviderError>) {
    let health = match &result {
        Ok(_) => ProviderHealth::Healthy,
        Err(error) => match error.class {
            ProviderErrorClass::Authentication | ProviderErrorClass::Terminal => {
                ProviderHealth::Terminal
            }
            _ => ProviderHealth::Degraded,
        },
    };
    let mut current = runtime.health.write().await;
    if *current != ProviderHealth::Terminal || health == ProviderHealth::Terminal {
        *current = health;
    }
}

fn terminal_health_error() -> ProviderError {
    ProviderError {
        class: ProviderErrorClass::Terminal,
        message: "provider health is terminal".to_owned(),
        retry_after: None,
    }
}

async fn mark_runtime_degraded(coordinator: &PinningCoordinator, provider: &str) {
    if let Some(runtime) = coordinator.provider_runtime(provider) {
        let mut health = runtime.health.write().await;
        if *health != ProviderHealth::Terminal {
            *health = ProviderHealth::Degraded;
        }
    }
}

fn valid_remote(remote: &RemotePin, cid: &str, request_id: Option<&str>) -> bool {
    !remote.request_id.is_empty()
        && remote.cid == cid
        && request_id.is_none_or(|expected| remote.request_id == expected)
}

fn persisted_status(status: &str) -> AppResult<RemotePinStatus> {
    match status {
        STATUS_QUEUED => Ok(RemotePinStatus::Queued),
        STATUS_PINNING => Ok(RemotePinStatus::Pinning),
        STATUS_PINNED => Ok(RemotePinStatus::Pinned),
        STATUS_FAILED => Ok(RemotePinStatus::Failed),
        _ => Err(AppError::Internal(format!(
            "unknown persisted remote pin status `{status}`"
        ))),
    }
}

fn protocol_error() -> ProviderError {
    ProviderError {
        class: ProviderErrorClass::Protocol,
        message: "invalid normalized provider response".to_owned(),
        retry_after: None,
    }
}

fn provider_error_label(class: ProviderErrorClass) -> &'static str {
    match class {
        ProviderErrorClass::Authentication => "provider authentication error",
        ProviderErrorClass::NotFound => "provider request not found",
        ProviderErrorClass::Ambiguous => "provider ambiguous response",
        ProviderErrorClass::RateLimited => "provider rate limited",
        ProviderErrorClass::Quota => "provider quota error",
        ProviderErrorClass::Transient => "provider transient error",
        ProviderErrorClass::Terminal => "provider terminal error",
        ProviderErrorClass::Protocol => "provider protocol error",
    }
}

fn provider_retry_delay(
    job: &pin_job::Model,
    error: &ProviderError,
    base: Duration,
    max: Duration,
) -> Duration {
    let minimum = base.min(max).max(Duration::from_millis(1));
    let maximum = max.max(minimum);
    if let Some(retry_after) = error.retry_after {
        return retry_after.clamp(minimum, maximum);
    }
    if matches!(
        error.class,
        ProviderErrorClass::Authentication
            | ProviderErrorClass::Terminal
            | ProviderErrorClass::Quota
    ) {
        return maximum;
    }
    let mut capped = minimum;
    for _ in 0..job.attempts.max(0) {
        capped = capped.checked_mul(2).unwrap_or(maximum).min(maximum);
    }
    let jitter_bound = (capped.as_nanos() / 4).min(u128::from(u64::MAX));
    let jitter = if jitter_bound == 0 {
        0
    } else {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        job.id.hash(&mut hasher);
        job.attempts.hash(&mut hasher);
        u128::from(hasher.finish()) % (jitter_bound + 1)
    };
    capped
        .checked_add(Duration::from_nanos(jitter as u64))
        .unwrap_or(maximum)
        .min(maximum)
}

fn claimed_lock(claimed: &ClaimedPinJob) -> AppResult<DateTime<Utc>> {
    claimed.model.locked_until.ok_or_else(|| {
        AppError::Internal(format!("pin job `{}` has no claim lock", claimed.model.id))
    })
}

fn chrono_duration(duration: Duration) -> AppResult<ChronoDuration> {
    ChronoDuration::from_std(duration)
        .map_err(|_| AppError::Internal("worker duration exceeds chrono range".to_owned()))
}

async fn coordinate_failed_one_outcomes(
    store: &Store,
    coordinator: &PinningCoordinator,
    affected: &[leases::AffectedLeaseOutcome],
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut coordinated = BTreeSet::new();
    for outcome in affected.iter().filter(|outcome| {
        outcome.provider_mode == ProviderMode::One
            && !outcome.available
            && coordinated.insert(outcome.lease_id.clone())
    }) {
        let Some(lease) = pin_lease::Entity::find_by_id(outcome.lease_id.clone())
            .one(store.db())
            .await?
        else {
            continue;
        };
        let Some(target) = pin_lease_target::Entity::find_by_id(outcome.target_id.clone())
            .one(store.db())
            .await?
        else {
            continue;
        };
        let Some(providers) = coordinator
            .ordered_failover_providers(&lease.policy_id, &target.provider)
            .await
        else {
            tracing::warn!(
                lease_id = %lease.id,
                target_id = %target.id,
                provider = %target.provider,
                "one-mode failover policy is unavailable"
            );
            continue;
        };
        let txn = store.db().begin().await?;
        if let Some(replacement) = leases::fail_one_target(
            &txn,
            &lease.id,
            &target.id,
            &providers,
            coordinator.provider_limits(),
            now,
        )
        .await?
        {
            txn.commit().await?;
            tracing::info!(
                lease_id = %lease.id,
                failed_target_id = %target.id,
                replacement_target_id = %replacement.id,
                replacement_provider = %replacement.provider,
                "one-mode sticky replacement selected"
            );
        } else {
            txn.commit().await?;
        }
    }
    Ok(())
}

async fn recover_durable_observation_coordination(
    store: &Store,
    coordinator: &PinningCoordinator,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(store.db())
        .await?
    else {
        return Ok(());
    };
    if !matches!(remote.status.as_str(), STATUS_FAILED | STATUS_PINNED) {
        return Ok(());
    }
    let affected = leases::current_remote_outcomes(store.db(), provider, cid).await?;
    if remote.status == STATUS_FAILED {
        coordinate_failed_one_outcomes(store, coordinator, &affected, now).await?;
        let txn = store.db().begin().await?;
        leases::ensure_failed_remote_retry(&txn, provider, cid, now).await?;
        txn.commit().await?;
    } else {
        converge_pinned_one_outcomes(store, &affected, now).await?;
    }
    Ok(())
}

async fn converge_pinned_one_outcomes(
    store: &Store,
    affected: &[leases::AffectedLeaseOutcome],
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut coordinated = BTreeSet::new();
    for outcome in affected.iter().filter(|outcome| {
        outcome.provider_mode == ProviderMode::One
            && outcome.available
            && coordinated.insert(outcome.lease_id.clone())
    }) {
        let txn = store.db().begin().await?;
        let cleanup = leases::converge_one_after_replacement(
            &txn,
            &outcome.lease_id,
            &outcome.target_id,
            now,
        )
        .await?;
        for job in cleanup {
            jobs::enqueue_job(&txn, job).await?;
        }
        txn.commit().await?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClaimedOneCoordination {
    Current,
    Stale,
}

async fn coordinate_claimed_one_target(
    store: &Store,
    coordinator: &PinningCoordinator,
    job: &pin_job::Model,
    now: DateTime<Utc>,
) -> AppResult<ClaimedOneCoordination> {
    let Some(target_id) = job.target_id.as_deref() else {
        return Ok(ClaimedOneCoordination::Stale);
    };
    let Some(target) = pin_lease_target::Entity::find_by_id(target_id.to_owned())
        .one(store.db())
        .await?
    else {
        return Ok(ClaimedOneCoordination::Stale);
    };
    let Some(lease) = pin_lease::Entity::find_by_id(target.lease_id.clone())
        .one(store.db())
        .await?
    else {
        return Ok(ClaimedOneCoordination::Stale);
    };
    let preflight_generation_current = job.lease_id.as_deref() == Some(lease.id.as_str())
        && job.expected_generation == Some(lease.generation)
        && target.lease_id == lease.id
        && target.provider == job.provider
        && target.cid == job.cid
        && lease.state == "active";
    if !preflight_generation_current {
        return Ok(ClaimedOneCoordination::Stale);
    }
    #[cfg(test)]
    interrupt_exhausted_coordination(&job.id, ExhaustedCoordinationPhase::AfterScopeRead).await?;
    if lease.provider_mode != "one" {
        return Ok(ClaimedOneCoordination::Current);
    }
    let Some(providers) = coordinator
        .ordered_failover_providers(&lease.policy_id, &target.provider)
        .await
    else {
        return Ok(ClaimedOneCoordination::Current);
    };
    let txn = store.db().begin().await?;
    let outcome =
        leases::fail_one_target_for_job(&txn, job, &providers, coordinator.provider_limits(), now)
            .await?;
    match outcome {
        leases::TargetJobFailoverOutcome::Stale => {
            txn.rollback().await?;
            Ok(ClaimedOneCoordination::Stale)
        }
        leases::TargetJobFailoverOutcome::Current => {
            txn.commit().await?;
            Ok(ClaimedOneCoordination::Current)
        }
    }
}

async fn quota_coordination_hook(
    db: &sea_orm::DatabaseTransaction,
    coordinator: &PinningCoordinator,
    job: &pin_job::Model,
    now: DateTime<Utc>,
) -> AppResult<Vec<leases::QuotaEvictedTarget>> {
    if job.operation != "submit" {
        return Ok(Vec::new());
    }
    let Some(limits) = coordinator.provider_limits().get(&job.provider).cloned() else {
        return Ok(Vec::new());
    };
    let (Some(lease_id), Some(target_id), Some(expected_generation)) = (
        job.lease_id.as_deref(),
        job.target_id.as_deref(),
        job.expected_generation,
    ) else {
        return Ok(Vec::new());
    };
    store_quota::evict_for_provider_quota(
        db,
        &job.provider,
        store_quota::ProviderQuotaWork {
            cid: &job.cid,
            lease_id,
            target_id,
            expected_generation,
        },
        &limits,
        now,
    )
    .await
}

async fn wake_provider_waiters_after_release(
    store: &Store,
    coordinator: &PinningCoordinator,
    provider: &str,
    now: DateTime<Utc>,
) -> AppResult<Vec<String>> {
    let Some(limits) = coordinator.provider_limits().get(provider) else {
        return Ok(Vec::new());
    };
    let txn = store.db().begin().await?;
    let woken = store_quota::wake_provider_waiters(&txn, provider, limits, now).await?;
    txn.commit().await?;
    let retry_delay = chrono_duration(coordinator.settings().interval)?;
    let txn = store.db().begin().await?;
    let requeued =
        leases::requeue_released_all_quota_targets(&txn, provider, retry_delay, now).await?;
    txn.commit().await?;
    tracing::debug!(
        provider,
        waiter_count = woken.len(),
        retry_count = requeued.len(),
        "pinning quota waiters projected after confirmed release"
    );
    Ok(woken)
}

async fn coordinate_quota_waiters(
    coordinator: &PinningCoordinator,
    store: &Store,
    now: DateTime<Utc>,
) -> AppResult<()> {
    recover_quota_evicted_one_targets(store, coordinator, now).await?;
    let retry_delay = chrono_duration(coordinator.settings().interval)?;
    let providers = coordinator
        .provider_limits()
        .iter()
        .map(|(provider, limits)| (provider.clone(), limits.clone()))
        .collect::<Vec<_>>();
    for (provider, limits) in providers {
        let txn = store.db().begin().await?;
        leases::requeue_released_all_quota_targets(&txn, &provider, retry_delay, now).await?;
        txn.commit().await?;

        let txn = store.db().begin().await?;
        let woken = store_quota::wake_provider_waiters(&txn, &provider, &limits, now).await?;
        txn.commit().await?;
        if !woken.is_empty() {
            tracing::debug!(
                provider,
                waiter_count = woken.len(),
                "pinning periodic quota waiter scan projected targets"
            );
            // A newly granted CID cannot itself become an eviction candidate until the next
            // worker interval.
            continue;
        }

        let txn = store.db().begin().await?;
        let evicted = store_quota::evict_for_provider_waiter(&txn, &provider, &limits, now).await?;
        txn.commit().await?;
        coordinate_quota_evicted_one_targets(store, coordinator, &evicted, now).await?;
    }
    Ok(())
}

async fn recover_quota_evicted_one_targets(
    store: &Store,
    coordinator: &PinningCoordinator,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::State.eq("evicted"))
        .order_by_asc(pin_lease_target::Column::CreatedAt)
        .order_by_asc(pin_lease_target::Column::Id)
        .all(store.db())
        .await?;
    let mut pending = Vec::new();
    for target in targets {
        let Some(lease) = pin_lease::Entity::find_by_id(target.lease_id.clone())
            .one(store.db())
            .await?
        else {
            continue;
        };
        if lease.state == "active" && lease.provider_mode == "one" {
            pending.push(leases::QuotaEvictedTarget {
                lease_id: lease.id,
                target_id: target.id,
                provider: target.provider,
                cid: target.cid,
                provider_mode: ProviderMode::One,
            });
        }
    }
    coordinate_quota_evicted_one_targets(store, coordinator, &pending, now).await
}

async fn coordinate_quota_evicted_one_targets(
    store: &Store,
    coordinator: &PinningCoordinator,
    evicted: &[leases::QuotaEvictedTarget],
    now: DateTime<Utc>,
) -> AppResult<()> {
    for target in evicted
        .iter()
        .filter(|target| target.provider_mode == ProviderMode::One)
    {
        let Some(lease) = pin_lease::Entity::find_by_id(target.lease_id.clone())
            .one(store.db())
            .await?
        else {
            continue;
        };
        let Some(providers) = coordinator
            .ordered_failover_providers(&lease.policy_id, &target.provider)
            .await
        else {
            continue;
        };
        let txn = store.db().begin().await?;
        leases::fail_one_target(
            &txn,
            &target.lease_id,
            &target.target_id,
            &providers,
            coordinator.provider_limits(),
            now,
        )
        .await?;
        txn.commit().await?;
    }
    Ok(())
}

fn transition(claimed: &ClaimedPinJob, old_state: &str, new_state: &str, request_id: Option<&str>) {
    let job = &claimed.model;
    tracing::info!(
        provider = %job.provider,
        cid = %job.cid,
        object_id = ?claimed.object_id,
        lease_id = ?job.lease_id,
        target_id = ?job.target_id,
        job_id = %job.id,
        old_state,
        new_state,
        remote_request_id = ?request_id,
        "pinning transition"
    );
}

fn audit_status_projection(
    claimed: &ClaimedPinJob,
    previous_status: &str,
    current_status: &str,
    request_id: &str,
) {
    let job = &claimed.model;
    tracing::info!(
        provider = %job.provider,
        cid = %job.cid,
        object_id = ?claimed.object_id,
        lease_id = ?job.lease_id,
        target_id = ?job.target_id,
        job_id = %job.id,
        phase = "status_projection",
        outcome = "committed",
        old_state = previous_status,
        new_state = current_status,
        remote_request_id = ?Some(request_id),
        "pinning durable phase"
    );
}

fn audit_follow_up_failure(claimed: &ClaimedPinJob, phase: &str, error: &AppError) {
    let error_class = match error {
        AppError::Database(_) => "database",
        AppError::Internal(_) => "internal",
        _ => "application",
    };
    let job = &claimed.model;
    tracing::warn!(
        provider = %job.provider,
        cid = %job.cid,
        object_id = ?claimed.object_id,
        lease_id = ?job.lease_id,
        target_id = ?job.target_id,
        job_id = %job.id,
        phase,
        outcome = "failed",
        error_class,
        "pinning durable follow-up phase"
    );
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap, VecDeque},
        io::Write,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use chrono::{TimeZone, Utc};
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, EntityTrait, PaginatorTrait, QueryFilter,
        QueryOrder, TransactionTrait,
    };
    use tokio::sync::{Mutex, Notify, Semaphore};
    use tokio_util::sync::CancellationToken;

    use crate::{
        config::{PinningConfig, ProviderConfig},
        pinning::{
            config::ValidatedPinningConfig,
            coordinator::PinningCoordinator,
            provider::{
                FindPin, PinningProvider, ProviderError, RemotePin, RemotePinStatus, SubmitPin,
            },
        },
        store::{
            Store,
            entities::{pin_job, pin_lease, pin_lease_target, pin_provider_usage, remote_pin},
            pinning::{jobs, leases},
        },
    };

    enum Script {
        Submit(Result<RemotePin, ProviderError>),
        BlockSubmit(Arc<SubmitBlocker>),
        Get(Result<RemotePin, ProviderError>),
        PanicGet(Arc<Notify>),
        BlockGet(Arc<GetBlocker>),
        Find(Result<Vec<RemotePin>, ProviderError>),
        BlockFind(Arc<FindBlocker>),
        Unpin(Result<(), ProviderError>),
        BlockUnpin(Arc<UnpinBlocker>),
    }

    #[tokio::test]
    async fn successful_join_result_removes_its_recorded_context() {
        let mut in_flight = tokio::task::JoinSet::new();
        let handle = in_flight.spawn(async {});
        let task_id = handle.id();
        let mut contexts = HashMap::from([(
            task_id,
            super::JobLogContext {
                job_id: "success-job".to_owned(),
                provider: "noop".to_owned(),
                cid: "bafy-success".to_owned(),
                lease_id: Some("lease-success".to_owned()),
                target_id: Some("target-success".to_owned()),
            },
        )]);

        let completed = in_flight
            .join_next_with_id()
            .await
            .expect("the completed task must have a JoinSet result");
        super::handle_join_result(completed, &mut contexts, "test worker task failure");

        assert!(
            contexts.is_empty(),
            "a normal JoinSet completion must remove only its matching context"
        );
    }

    struct SubmitBlocker {
        entered: Notify,
        release: Notify,
        result: Mutex<Option<Result<RemotePin, ProviderError>>>,
    }

    struct GetBlocker {
        entered: Notify,
        release: Notify,
        result: RemotePin,
    }

    struct UnpinBlocker {
        entered: Notify,
        release: Notify,
        result: Mutex<Option<Result<(), ProviderError>>>,
    }

    struct FindBlocker {
        entered: Notify,
        release: Notify,
        result: Vec<RemotePin>,
    }

    struct ScriptProvider {
        scripts: Mutex<VecDeque<Script>>,
        submits: AtomicUsize,
        gets: AtomicUsize,
        finds: AtomicUsize,
        unpins: AtomicUsize,
        submitted: Mutex<Vec<SubmitPin>>,
    }

    #[derive(Clone, Default)]
    struct TraceCapture(Arc<std::sync::Mutex<Vec<u8>>>);

    impl Write for TraceCapture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("trace capture poisoned").extend(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TraceCapture {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl TraceCapture {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().expect("trace capture poisoned").clone()).unwrap()
        }
    }

    impl ScriptProvider {
        fn new(scripts: impl IntoIterator<Item = Script>) -> Arc<Self> {
            Arc::new(Self {
                scripts: Mutex::new(scripts.into_iter().collect()),
                submits: AtomicUsize::new(0),
                gets: AtomicUsize::new(0),
                finds: AtomicUsize::new(0),
                unpins: AtomicUsize::new(0),
                submitted: Mutex::new(Vec::new()),
            })
        }

        async fn next(&self) -> Script {
            self.scripts
                .lock()
                .await
                .pop_front()
                .expect("scripted provider call was not expected")
        }
    }

    #[async_trait::async_trait]
    impl PinningProvider for ScriptProvider {
        fn name(&self) -> &str {
            "noop"
        }

        async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
            self.submits.fetch_add(1, Ordering::SeqCst);
            self.submitted.lock().await.push(request);
            match self.next().await {
                Script::Submit(result) => result,
                Script::BlockSubmit(blocker) => {
                    blocker.entered.notify_one();
                    blocker.release.notified().await;
                    blocker
                        .result
                        .lock()
                        .await
                        .take()
                        .expect("blocked Submit result already consumed")
                }
                _ => panic!("expected scripted Submit"),
            }
        }

        async fn get(&self, _request_id: &str) -> Result<RemotePin, ProviderError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            match self.next().await {
                Script::Get(result) => result,
                Script::PanicGet(arrived) => {
                    arrived.notify_one();
                    panic!("scripted Get panic")
                }
                Script::BlockGet(blocker) => {
                    blocker.entered.notify_one();
                    blocker.release.notified().await;
                    Ok(blocker.result.clone())
                }
                _ => panic!("expected scripted Get"),
            }
        }

        async fn find(&self, _query: FindPin) -> Result<Vec<RemotePin>, ProviderError> {
            self.finds.fetch_add(1, Ordering::SeqCst);
            match self.next().await {
                Script::Find(result) => result,
                Script::BlockFind(blocker) => {
                    blocker.entered.notify_one();
                    blocker.release.notified().await;
                    Ok(blocker.result.clone())
                }
                _ => panic!("expected scripted Find"),
            }
        }

        async fn unpin(&self, _request_id: &str) -> Result<(), ProviderError> {
            self.unpins.fetch_add(1, Ordering::SeqCst);
            match self.next().await {
                Script::Unpin(result) => result,
                Script::BlockUnpin(blocker) => {
                    blocker.entered.notify_one();
                    blocker.release.notified().await;
                    blocker
                        .result
                        .lock()
                        .await
                        .take()
                        .expect("blocked Unpin result already consumed")
                }
                _ => panic!("expected scripted Unpin"),
            }
        }
    }

    fn remote(request_id: &str, status: RemotePinStatus) -> RemotePin {
        remote_for("bafy-worker", request_id, status)
    }

    fn remote_for(cid: &str, request_id: &str, status: RemotePinStatus) -> RemotePin {
        RemotePin {
            request_id: request_id.to_owned(),
            cid: cid.to_owned(),
            status,
            raw_status: format!("{status:?}").to_ascii_lowercase(),
            failure_reason: None,
        }
    }

    struct Fixture {
        store: Store,
        coordinator: Arc<PinningCoordinator>,
        provider: Arc<ScriptProvider>,
        _database_directory: Option<tempfile::TempDir>,
    }

    static SHUTDOWN_PAUSED_CLOCK_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    async fn fixture(scripts: impl IntoIterator<Item = Script>) -> Fixture {
        fixture_with_noop_limits(scripts, 10_000, 100).await
    }

    async fn fixture_with_noop_limits(
        scripts: impl IntoIterator<Item = Script>,
        max_bytes: u64,
        max_pins: u64,
    ) -> Fixture {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        fixture_with_database(scripts, db, max_bytes, max_pins, None, "1s").await
    }

    async fn file_backed_fixture(scripts: impl IntoIterator<Item = Script>) -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("pinning-worker.db");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let db = Database::connect(database_url).await.unwrap();
        fixture_with_database(scripts, db, 10_000, 100, Some(directory), "1s").await
    }

    async fn fixture_with_database(
        scripts: impl IntoIterator<Item = Script>,
        db: sea_orm::DatabaseConnection,
        max_bytes: u64,
        max_pins: u64,
        database_directory: Option<tempfile::TempDir>,
        worker_interval: &str,
    ) -> Fixture {
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        db.execute_unprepared(
            "INSERT INTO objects (id, bucket, key, cid, size, etag) \
             VALUES ('object-1', 'bucket', 'key', 'bafy-worker', 100, 'bafy-worker')",
        )
        .await
        .unwrap();
        let now = Utc::now();
        let created = now.to_rfc3339();
        let expires = (now + chrono::Duration::hours(1)).to_rfc3339();
        db.execute_unprepared(&format!(
            "INSERT INTO pin_leases \
             (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
              last_touched_at, expires_at, generation, state) \
             VALUES ('lease-1', 'object-1', 'test', 'policy', 'all', 'full', \
                     '{created}', '{created}', '{expires}', 1, 'active')"
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-1', 'lease-1', 'bafy-worker', 100, 'noop', 'waiting', \
                     '{created}', '{created}')"
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO remote_pins \
             (provider, cid, cid_size, status, epoch, failure_attempts, last_touched_at) \
             VALUES ('noop', 'bafy-worker', 100, 'reserved', 1, 0, '{created}')"
        ))
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO pin_provider_usage \
             (provider, reserved_bytes, reserved_pins) \
             VALUES ('noop', 100, 1)",
        )
        .await
        .unwrap();

        let provider = ScriptProvider::new(scripts);
        let raw = PinningConfig {
            worker_interval: worker_interval.to_owned(),
            worker_concurrency: 2,
            providers: vec![
                ProviderConfig {
                    name: "noop".to_owned(),
                    kind: "noop".to_owned(),
                    token_env: None,
                    endpoint: None,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    enabled: true,
                    priority: 1,
                    max_bytes,
                    max_pins,
                    requests_per_second: None,
                },
                ProviderConfig {
                    name: "fast".to_owned(),
                    kind: "noop".to_owned(),
                    token_env: None,
                    endpoint: None,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    enabled: true,
                    priority: 2,
                    max_bytes: 10_000,
                    max_pins: 100,
                    requests_per_second: None,
                },
                ProviderConfig {
                    name: "third".to_owned(),
                    kind: "noop".to_owned(),
                    token_env: None,
                    endpoint: None,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    enabled: true,
                    priority: 3,
                    max_bytes: 10_000,
                    max_pins: 100,
                    requests_per_second: None,
                },
            ],
            policies: Vec::new(),
        };
        let validated = ValidatedPinningConfig::from_raw(&raw, |_| None).unwrap();
        let mut coordinator = PinningCoordinator::build(validated).unwrap();
        PinningCoordinator::replace_provider_for_test(&mut coordinator, "noop", provider.clone());
        Fixture {
            store: Store::new(db),
            coordinator,
            provider,
            _database_directory: database_directory,
        }
    }

    async fn exhausted_one_poll_fixture(
        cid: &str,
        lease_id: &str,
        target_id: &str,
        request_id: &str,
    ) -> (Fixture, pin_job::Model) {
        let mut fixture = fixture([]).await;
        PinningCoordinator::configure_policy_providers_for_test(
            &mut fixture.coordinator,
            "policy",
            &["noop", "fast"],
        );
        let now = Utc::now();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_provider_usage SET reserved_bytes=200, reserved_pins=2 \
                 WHERE provider='noop'; \
                 INSERT INTO remote_pins \
                 (provider, cid, cid_size, request_id, status, epoch, failure_attempts, last_touched_at) \
                 VALUES ('noop', '{cid}', 100, '{request_id}', 'queued', 1, 0, '{}'); \
                 INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) \
                 VALUES ('{lease_id}', 'object-1', 'exhausted-test', 'policy', 'one', 'full', '{}', \
                         '{}', '{}', 1, 'active'); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES ('{target_id}', '{lease_id}', '{cid}', 100, 'noop', 'submitted', '{}', '{}')",
                now.to_rfc3339(),
                now.to_rfc3339(),
                now.to_rfc3339(),
                (now + chrono::Duration::hours(1)).to_rfc3339(),
                now.to_rfc3339(),
                now.to_rfc3339(),
            ))
            .await
            .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::poll_job("noop", cid, lease_id, target_id, 1, request_id, now),
        )
        .await
        .unwrap();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET attempts=8 WHERE cid='{cid}' AND operation='poll'"
            ))
            .await
            .unwrap();
        let job = pin_job::Entity::find()
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        (fixture, job)
    }

    async fn expire_and_reclaim_exhausted_poll(
        fixture: &Fixture,
        job_id: &str,
    ) -> jobs::ClaimedPinJob {
        let now = Utc::now();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET locked_until='{}' WHERE id='{job_id}'; \
                 UPDATE pin_jobs SET next_attempt_at='{}' \
                 WHERE operation='submit' AND provider='fast' AND state='pending'",
                (now - chrono::Duration::seconds(1)).to_rfc3339(),
                (now + chrono::Duration::hours(1)).to_rfc3339(),
            ))
            .await
            .unwrap();
        let claimed =
            jobs::claim_due_jobs(fixture.store.db(), now, chrono::Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .expect("expired exhausted Poll must be reclaimable");
        assert_eq!(claimed.model.id, job_id);
        assert!(claimed.reclaimed);
        claimed
    }

    fn exhausted_gate(
        job_id: &str,
        phase: super::ExhaustedCoordinationPhase,
        interruption: super::ExhaustedCoordinationInterruption,
    ) -> Arc<super::ExhaustedCoordinationGate> {
        Arc::new(super::ExhaustedCoordinationGate {
            job_id: job_id.to_owned(),
            phase,
            interruption,
            fail_once: std::sync::atomic::AtomicBool::new(true),
            arrived: Notify::new(),
            resume: Notify::new(),
        })
    }

    async fn assert_exact_exhausted_one_failover(
        fixture: &Fixture,
        cid: &str,
        lease_id: &str,
        exhausted_job_id: &str,
    ) {
        assert_eq!(
            pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            2
        );
        let replacements = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
            .filter(pin_lease_target::Column::Provider.eq("fast"))
            .all(fixture.store.db())
            .await
            .unwrap();
        assert_eq!(replacements.len(), 1);
        assert_eq!(replacements[0].state, "waiting");
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), cid.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .epoch,
            1
        );
        assert_eq!(
            remote_pin::Entity::find_by_id(("fast".to_owned(), cid.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .epoch,
            1
        );
        let submits = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("submit"))
            .all(fixture.store.db())
            .await
            .unwrap();
        assert_eq!(submits.len(), 1);
        assert_eq!(submits[0].lease_id.as_deref(), Some(lease_id));
        assert_eq!(
            submits[0].target_id.as_deref(),
            Some(replacements[0].id.as_str())
        );
        assert_eq!(submits[0].expected_generation, Some(2));
        assert_eq!(submits[0].expected_remote_epoch, None);
        assert_eq!(
            pin_job::Entity::find_by_id(exhausted_job_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "done"
        );
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
    }

    async fn assert_exhausted_one_scope_was_not_coordinated(
        fixture: &Fixture,
        cid: &str,
        lease_id: &str,
        expected_generation: i64,
        exhausted_job_id: &str,
    ) {
        assert_eq!(
            pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            expected_generation
        );
        let targets = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
            .all(fixture.store.db())
            .await
            .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].provider, "noop");
        assert_eq!(targets[0].state, "submitted");
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), cid.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .epoch,
            1
        );
        assert!(
            remote_pin::Entity::find_by_id(("fast".to_owned(), cid.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .is_none()
        );
        let noop_usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (noop_usage.reserved_bytes, noop_usage.reserved_pins),
            (200, 2)
        );
        assert!(
            pin_provider_usage::Entity::find_by_id("fast".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            pin_job::Entity::find_by_id(exhausted_job_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "done"
        );
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
    }

    impl Fixture {
        async fn seed_quota_candidate(
            &self,
            cid: &str,
            suffix: &str,
            touched: chrono::DateTime<Utc>,
        ) {
            let touched = touched.to_rfc3339();
            let expires = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
            self.store
                .db()
                .execute_unprepared(&format!(
                    "INSERT INTO pin_leases \
                     (id, owner_object_id, source, policy_id, provider_mode, content_mode, \
                      created_at, last_touched_at, expires_at, generation, state) \
                     VALUES ('lease-{suffix}', 'object-1', 'quota-{suffix}', 'policy', 'all', \
                             'full', '{touched}', '{touched}', '{expires}', 1, 'active'); \
                     INSERT INTO pin_lease_targets \
                     (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                     VALUES ('target-{suffix}', 'lease-{suffix}', '{cid}', 100, 'noop', 'pinned', \
                             '{touched}', '{touched}'); \
                     INSERT INTO remote_pins \
                     (provider, cid, cid_size, request_id, status, epoch, failure_attempts, \
                      last_touched_at) \
                     VALUES ('noop', '{cid}', 100, 'request-{suffix}', 'pinned', 1, 0, \
                             '{touched}')"
                ))
                .await
                .unwrap();
        }

        async fn seed_quota_waiter(&self, cid: &str, suffix: &str, logical_size: i64) {
            let now = Utc::now();
            let created = now.to_rfc3339();
            let expires = (now + chrono::Duration::hours(1)).to_rfc3339();
            self.store
                .db()
                .execute_unprepared(&format!(
                    "INSERT INTO pin_leases \
                     (id, owner_object_id, source, policy_id, provider_mode, content_mode, \
                      created_at, last_touched_at, expires_at, generation, state) \
                     VALUES ('lease-{suffix}', 'object-1', 'quota-{suffix}', 'policy', 'all', \
                             'full', '{created}', '{created}', '{expires}', 1, 'active'); \
                     INSERT INTO pin_lease_targets \
                     (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                     VALUES ('target-{suffix}', 'lease-{suffix}', '{cid}', {logical_size}, 'noop', \
                             'quota_waiting', '{created}', '{created}')"
                ))
                .await
                .unwrap();
        }

        async fn enqueue_submit(&self) {
            jobs::enqueue_job(
                self.store.db(),
                jobs::submit_job("noop", "bafy-worker", "lease-1", "target-1", 1, Utc::now()),
            )
            .await
            .unwrap();
        }

        async fn enqueue_poll(&self, request_id: &str) {
            self.store
                .db()
                .execute_unprepared(&format!(
                    "UPDATE remote_pins SET request_id='{request_id}', status='queued' \
                     WHERE provider='noop' AND cid='bafy-worker'; \
                     UPDATE pin_lease_targets SET state='submitted' WHERE id='target-1'"
                ))
                .await
                .unwrap();
            jobs::enqueue_job(
                self.store.db(),
                jobs::poll_job(
                    "noop",
                    "bafy-worker",
                    "lease-1",
                    "target-1",
                    1,
                    request_id,
                    Utc::now(),
                ),
            )
            .await
            .unwrap();
        }

        async fn run_one_due(&self) {
            let due = pin_job::Entity::find()
                .filter(pin_job::Column::State.eq("pending"))
                .order_by_asc(pin_job::Column::NextAttemptAt)
                .one(self.store.db())
                .await
                .unwrap()
                .expect("expected pending job")
                .next_attempt_at;
            let claimed =
                jobs::claim_due_jobs(self.store.db(), due, chrono::Duration::seconds(30), 1)
                    .await
                    .unwrap()
                    .pop()
                    .expect("expected claimed job");
            super::execute_claimed_job(
                &self.store,
                &self.coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
            .unwrap();
        }

        async fn run_claim_at(&self, now: chrono::DateTime<Utc>) {
            let claimed =
                jobs::claim_due_jobs(self.store.db(), now, chrono::Duration::seconds(30), 1)
                    .await
                    .unwrap()
                    .pop()
                    .expect("expected claimed job");
            super::execute_claimed_job(
                &self.store,
                &self.coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
            .unwrap();
        }

        async fn seed_poll(&self, suffix: &str, due: chrono::DateTime<Utc>) -> String {
            self.seed_poll_for("noop", suffix, due).await
        }

        async fn seed_poll_for(
            &self,
            provider: &str,
            suffix: &str,
            due: chrono::DateTime<Utc>,
        ) -> String {
            let lease_id = format!("lease-{suffix}");
            let target_id = format!("target-{suffix}");
            let cid = format!("bafy-{suffix}");
            let request_id = format!("request-{suffix}");
            let created = due.to_rfc3339();
            let expires = (due + chrono::Duration::hours(1)).to_rfc3339();
            self.store
                .db()
                .execute_unprepared(&format!(
                    "INSERT INTO pin_leases \
                     (id, owner_object_id, source, policy_id, provider_mode, content_mode, \
                      created_at, last_touched_at, expires_at, generation, state) \
                     VALUES ('{lease_id}', 'object-1', '{suffix}', 'policy', 'all', 'full', \
                             '{created}', '{created}', '{expires}', 1, 'active'); \
                     INSERT INTO pin_lease_targets \
                     (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                     VALUES ('{target_id}', '{lease_id}', '{cid}', 100, '{provider}', 'submitted', \
                             '{created}', '{created}'); \
                     INSERT INTO remote_pins \
                     (provider, cid, cid_size, request_id, status, epoch, failure_attempts, \
                      last_touched_at) \
                     VALUES ('{provider}', '{cid}', 100, '{request_id}', 'queued', 1, 0, '{created}')"
                ))
                .await
                .unwrap();
            let jobs::NewPinJob::Target(job) =
                jobs::poll_job(provider, &cid, &lease_id, &target_id, 1, &request_id, due)
            else {
                unreachable!()
            };
            let id = job.id.clone();
            jobs::enqueue_job(self.store.db(), jobs::NewPinJob::Target(job))
                .await
                .unwrap();
            id
        }
    }

    fn job(operation: &str) -> pin_job::Model {
        let now = Utc.with_ymd_and_hms(2026, 7, 22, 0, 0, 0).single().unwrap();
        pin_job::Model {
            id: "job-1".to_owned(),
            operation: operation.to_owned(),
            provider: "pinata".to_owned(),
            cid: "bafy-worker".to_owned(),
            lease_id: None,
            target_id: None,
            expected_generation: None,
            expected_remote_epoch: None,
            state: "running".to_owned(),
            attempts: 0,
            next_attempt_at: now,
            locked_until: Some(now + chrono::Duration::seconds(30)),
            submit_phase: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn persisted_operation_scope_is_validated_before_dispatch() {
        let mut submit = job("submit");
        submit.lease_id = Some("lease-1".to_owned());
        submit.target_id = Some("target-1".to_owned());
        submit.expected_generation = Some(4);
        submit.submit_phase = Some("ready".to_owned());
        assert!(super::validate_operation_scope(&submit).is_ok());

        let mut malformed_submit = submit.clone();
        malformed_submit.expected_remote_epoch = Some(7);
        assert!(super::validate_operation_scope(&malformed_submit).is_err());

        let mut reconcile = job("reconcile");
        reconcile.expected_remote_epoch = Some(7);
        assert!(super::validate_operation_scope(&reconcile).is_ok());

        let mut malformed_reconcile = reconcile.clone();
        malformed_reconcile.target_id = Some("target-1".to_owned());
        assert!(super::validate_operation_scope(&malformed_reconcile).is_err());

        assert!(super::validate_operation_scope(&job("future-operation")).is_err());
    }

    #[test]
    fn retry_after_wins_and_backoff_jitter_is_bounded() {
        let mut model = job("poll");
        model.lease_id = Some("lease-1".to_owned());
        model.target_id = Some("target-1".to_owned());
        model.expected_generation = Some(1);
        model.attempts = 2;
        let retry_after = crate::pinning::provider::ProviderError {
            class: crate::pinning::provider::ProviderErrorClass::RateLimited,
            message: "secret provider body".to_owned(),
            retry_after: Some(std::time::Duration::from_secs(17)),
        };
        assert_eq!(
            super::provider_retry_delay(
                &model,
                &retry_after,
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(300),
            ),
            std::time::Duration::from_secs(17)
        );

        let transient = crate::pinning::provider::ProviderError {
            class: crate::pinning::provider::ProviderErrorClass::Transient,
            message: "not persisted".to_owned(),
            retry_after: None,
        };
        let delay = super::provider_retry_delay(
            &model,
            &transient,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(300),
        );
        assert!(delay >= std::time::Duration::from_secs(4));
        assert!(delay <= std::time::Duration::from_secs(5));
        assert!(!super::provider_error_label(transient.class).contains("not persisted"));
    }

    #[tokio::test]
    async fn retry_after_zero_from_poll_persists_a_positive_bounded_retry() {
        let fixture = fixture([Script::Get(Err(ProviderError {
            class: crate::pinning::provider::ProviderErrorClass::RateLimited,
            message: "zero retry-after raw body".to_owned(),
            retry_after: Some(std::time::Duration::ZERO),
        }))])
        .await;
        fixture.enqueue_poll("request-zero-retry-after").await;
        fixture.run_one_due().await;

        let retry = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.state, "pending");
        assert_eq!(retry.attempts, 1);
        assert_eq!(retry.locked_until, None);
        let delay = retry.next_attempt_at - retry.updated_at;
        assert!(delay >= chrono::Duration::seconds(1), "{delay:?}");
        assert!(delay <= chrono::Duration::seconds(300), "{delay:?}");
    }

    #[tokio::test]
    async fn retry_after_u64_max_from_unpin_is_clamped_before_persistence() {
        let fixture = fixture([Script::Unpin(Err(ProviderError {
            class: crate::pinning::provider::ProviderErrorClass::RateLimited,
            message: "huge retry-after raw body".to_owned(),
            retry_after: Some(std::time::Duration::from_secs(u64::MAX)),
        }))])
        .await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='request-huge-retry-after', status='pinned' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='pinned' WHERE id='target-1'",
            )
            .await
            .unwrap();
        leases::cancel_lease(fixture.store.db(), "lease-1", Utc::now())
            .await
            .unwrap();
        fixture.run_one_due().await;

        let retry = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("unpin"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.state, "pending");
        assert_eq!(retry.attempts, 1);
        assert_eq!(retry.locked_until, None);
        let delay = retry.next_attempt_at - retry.updated_at;
        assert!(delay >= chrono::Duration::seconds(1), "{delay:?}");
        assert!(delay <= chrono::Duration::seconds(300), "{delay:?}");
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
    }

    #[tokio::test]
    async fn retry_after_over_max_from_reconcile_is_clamped_before_persistence() {
        let fixture = fixture([Script::Unpin(Err(ProviderError {
            class: crate::pinning::provider::ProviderErrorClass::RateLimited,
            message: "oversized retry-after raw body".to_owned(),
            retry_after: Some(std::time::Duration::from_secs(301)),
        }))])
        .await;
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE remote_pins SET request_id='failed-retry-after', status='failed', \
                 failure_attempts=1, last_failed_request_id='failed-retry-after', \
                 next_retry_at='{}' WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='degraded' WHERE id='target-1'",
                (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339()
            ))
            .await
            .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 1, Utc::now()),
        )
        .await
        .unwrap();
        fixture.run_one_due().await;

        let retry = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.state, "pending");
        assert_eq!(retry.attempts, 1);
        assert_eq!(retry.locked_until, None);
        let delay = retry.next_attempt_at - retry.updated_at;
        assert!(delay >= chrono::Duration::seconds(1), "{delay:?}");
        assert!(delay <= chrono::Duration::seconds(300), "{delay:?}");
    }

    #[tokio::test]
    async fn missing_provider_ordinary_jobs_retry_then_park_without_releasing_quota() {
        let fixture = fixture([]).await;
        fixture.enqueue_poll("missing-provider-request").await;
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::unpin_job("noop", "bafy-worker", 1, Utc::now()),
        )
        .await
        .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 1, Utc::now()),
        )
        .await
        .unwrap();
        let missing = PinningCoordinator::disabled_for_test();

        for expected_attempt in 1..=8 {
            let due = pin_job::Entity::find()
                .filter(pin_job::Column::State.eq("pending"))
                .order_by_desc(pin_job::Column::NextAttemptAt)
                .one(fixture.store.db())
                .await
                .unwrap()
                .expect("ordinary missing-provider work must remain durable")
                .next_attempt_at;
            let claimed =
                jobs::claim_due_jobs(fixture.store.db(), due, chrono::Duration::seconds(30), 3)
                    .await
                    .unwrap();
            assert_eq!(claimed.len(), 3);
            for job in claimed {
                super::execute_claimed_job(
                    &fixture.store,
                    &missing,
                    &Arc::new(Semaphore::new(2)),
                    job,
                )
                .await
                .unwrap();
            }
            let rows = pin_job::Entity::find()
                .filter(pin_job::Column::Operation.is_in(["poll", "unpin", "reconcile"]))
                .all(fixture.store.db())
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);
            for row in rows {
                assert_eq!(row.state, "pending");
                assert_eq!(row.attempts, expected_attempt);
                assert_eq!(row.locked_until, None);
                assert!(row.next_attempt_at > row.updated_at);
            }
        }

        let final_due = pin_job::Entity::find()
            .filter(pin_job::Column::State.eq("pending"))
            .order_by_desc(pin_job::Column::NextAttemptAt)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .next_attempt_at;
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            final_due,
            chrono::Duration::seconds(30),
            3,
        )
        .await
        .unwrap();
        for job in claimed {
            super::execute_claimed_job(&fixture.store, &missing, &Arc::new(Semaphore::new(2)), job)
                .await
                .unwrap();
        }
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.is_in(["poll", "unpin", "reconcile"]))
                .filter(pin_job::Column::State.eq("done"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            3
        );
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn missing_provider_worker_converges_all_ordinary_operations_without_lock_cycle() {
        let fixture = fixture([]).await;
        fixture.enqueue_poll("missing-worker-request").await;
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::unpin_job("noop", "bafy-worker", 1, Utc::now()),
        )
        .await
        .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 1, Utc::now()),
        )
        .await
        .unwrap();
        let mut missing = PinningCoordinator::disabled_for_test();
        PinningCoordinator::configure_worker_for_test(&mut missing, |settings| {
            settings.interval = std::time::Duration::from_millis(5);
            settings.worker_concurrency = 3;
            settings.claim_limit = 6;
            settings.lock_for = chrono::Duration::milliseconds(50);
            settings.base_backoff = std::time::Duration::from_millis(20);
            settings.max_backoff = std::time::Duration::from_millis(20);
            settings.max_attempts = 2;
            settings.shutdown_grace = std::time::Duration::from_millis(30);
        });
        let db = fixture.store.db().clone();
        let handle = missing.start(Store::new(db.clone()), CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let done = pin_job::Entity::find()
                    .filter(pin_job::Column::Operation.is_in(["poll", "unpin", "reconcile"]))
                    .filter(pin_job::Column::State.eq("done"))
                    .count(&db)
                    .await
                    .unwrap();
                if done == 3 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("missing-provider worker did not durably park ordinary work");
        handle.shutdown(std::time::Duration::from_millis(50)).await;

        let rows = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.is_in(["poll", "unpin", "reconcile"]))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row.state, "done");
            assert_eq!(row.attempts, 2);
            assert_eq!(row.locked_until, None);
        }
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn missing_provider_ambiguous_submit_keeps_stable_recovery_then_finds_on_restore() {
        let fixture = fixture([Script::Find(Ok(vec![remote(
            "accepted-before-provider-removal",
            RemotePinStatus::Queued,
        )]))])
        .await;
        fixture.enqueue_submit().await;
        let expired = Utc::now() - chrono::Duration::seconds(1);
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET state='running', submit_phase='calling', locked_until='{}' \
                 WHERE operation='submit'",
                expired.to_rfc3339()
            ))
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
        let stable_id = claimed.model.id.clone();
        assert!(claimed.reclaimed);
        // Restore the row to pending at the reclaimed lock so a real worker with a missing
        // provider owns the durable unavailable transition.
        jobs::retry_submit_recovery(
            fixture.store.db(),
            &claimed,
            Utc::now(),
            std::time::Duration::from_secs(300),
            "provider unavailable",
        )
        .await
        .unwrap();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET attempts=0, next_attempt_at='{}' WHERE id='{}'",
                Utc::now().to_rfc3339(),
                stable_id
            ))
            .await
            .unwrap();
        let mut missing = PinningCoordinator::disabled_for_test();
        PinningCoordinator::configure_worker_for_test(&mut missing, |settings| {
            settings.interval = std::time::Duration::from_millis(5);
            settings.worker_concurrency = 1;
            settings.claim_limit = 2;
            settings.shutdown_grace = std::time::Duration::from_millis(20);
        });
        let missing_handle = missing.start(
            Store::new(fixture.store.db().clone()),
            CancellationToken::new(),
        );
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let row = pin_job::Entity::find_by_id(stable_id.clone())
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap();
                if row.state == "pending" && row.attempts == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("missing-provider worker did not schedule Submit recovery");
        missing_handle
            .shutdown(std::time::Duration::from_millis(30))
            .await;

        let waiting = pin_job::Entity::find_by_id(stable_id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(waiting.state, "pending");
        assert_eq!(waiting.submit_phase.as_deref(), Some("recovering"));
        assert_eq!(waiting.attempts, 1);
        assert!(waiting.next_attempt_at >= waiting.updated_at + chrono::Duration::seconds(300));
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));

        let restored = jobs::claim_due_jobs(
            fixture.store.db(),
            waiting.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(restored.model.id, stable_id);
        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            restored,
        )
        .await
        .unwrap();
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .request_id
                .as_deref(),
            Some("accepted-before-provider-removal")
        );
    }

    #[tokio::test]
    async fn submit_then_poll_twice_reaches_pinned_with_one_stable_poll() {
        let fixture = fixture([
            Script::Submit(Ok(remote("request-1", RemotePinStatus::Queued))),
            Script::Get(Ok(remote("request-1", RemotePinStatus::Pinning))),
            Script::Get(Ok(remote("request-1", RemotePinStatus::Pinned))),
        ])
        .await;
        tokio::time::pause();
        // SQLx completes SQLite work on helper threads. Keep the current-thread runtime runnable
        // so paused Tokio time cannot auto-advance to the pool timeout before those replies arrive.
        let paused_runtime_keepalive = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        fixture.enqueue_submit().await;

        fixture.run_one_due().await;
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
        let poll = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let poll_id = poll.id.clone();
        assert_eq!(poll.state, "pending");

        fixture.run_one_due().await;
        let same_poll = pin_job::Entity::find_by_id(poll_id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(same_poll.state, "pending");
        assert_eq!(same_poll.attempts, 0);
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 1);
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("poll"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            1
        );

        fixture.run_one_due().await;
        let completed = pin_job::Entity::find_by_id(poll_id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, "done");
        assert_eq!(completed.attempts, 0);
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 2);
        let persisted_remote =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(persisted_remote.request_id.as_deref(), Some("request-1"));
        assert_eq!(persisted_remote.status, "pinned");
        assert_eq!(persisted_remote.failure_attempts, 0);
        let target = pin_lease_target::Entity::find_by_id("target-1".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target.state, "pinned");
        let submitted = fixture.provider.submitted.lock().await;
        assert_eq!(submitted[0].name, "bucket/key");
        assert_eq!(
            submitted[0].metadata,
            std::collections::BTreeMap::from([
                (
                    "gateway_job_id".to_owned(),
                    "submit:noop:bafy-worker:target-1:g1".to_owned(),
                ),
                ("gateway_lease_id".to_owned(), "lease-1".to_owned()),
                ("gateway_target_id".to_owned(), "target-1".to_owned()),
            ])
        );
        paused_runtime_keepalive.abort();
    }

    fn provider_error(
        class: crate::pinning::provider::ProviderErrorClass,
        message: &str,
    ) -> ProviderError {
        ProviderError {
            class,
            message: message.to_owned(),
            retry_after: None,
        }
    }

    #[tokio::test]
    async fn ambiguous_submit_finds_and_adopts_without_second_post() {
        let fixture = fixture([
            Script::Submit(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Ambiguous,
                "raw-conflict-body-must-not-persist",
            ))),
            Script::Find(Ok(vec![remote(
                "accepted-before-response",
                RemotePinStatus::Pinning,
            )])),
        ])
        .await;
        fixture.enqueue_submit().await;
        fixture.run_one_due().await;

        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        let persisted =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            persisted.request_id.as_deref(),
            Some("accepted-before-response")
        );
        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(submit.state, "done");
        assert!(
            submit
                .last_error
                .as_deref()
                .is_none_or(|error| !error.contains("raw-conflict-body"))
        );
    }

    #[tokio::test]
    async fn recovered_all_mode_submit_find_none_degrades_before_backoff() {
        let fixture = fixture([Script::Find(Ok(Vec::new()))]).await;
        fixture.enqueue_submit().await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE pin_jobs SET submit_phase='recovering' WHERE operation='submit'",
            )
            .await
            .unwrap();

        fixture.run_one_due().await;

        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "degraded"
        );
        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (submit.state.as_str(), submit.submit_phase.as_deref()),
            ("pending", Some("recovery_backoff"))
        );
    }

    #[tokio::test]
    async fn recovered_all_mode_submit_multiple_find_matches_degrades_before_retry() {
        let fixture = fixture([Script::Find(Ok(vec![
            remote("ambiguous-a", RemotePinStatus::Queued),
            remote("ambiguous-b", RemotePinStatus::Pinning),
        ]))])
        .await;
        fixture.enqueue_submit().await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE pin_jobs SET submit_phase='recovering' WHERE operation='submit'",
            )
            .await
            .unwrap();

        fixture.run_one_due().await;

        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "degraded"
        );
        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (submit.state.as_str(), submit.submit_phase.as_deref()),
            ("pending", Some("recovering"))
        );
    }

    #[tokio::test]
    async fn conflict_find_none_persists_backoff_before_later_post() {
        let fixture = fixture([
            Script::Submit(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Ambiguous,
                "conflict",
            ))),
            Script::Find(Ok(Vec::new())),
            Script::Submit(Ok(remote("request-after-proof", RemotePinStatus::Pinned))),
        ])
        .await;
        fixture.enqueue_submit().await;
        fixture.run_one_due().await;

        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(submit.state, "pending");
        assert_eq!(submit.submit_phase.as_deref(), Some("recovery_backoff"));
        assert!(submit.next_attempt_at >= submit.updated_at + chrono::Duration::seconds(1));
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);

        fixture.run_one_due().await;
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 2);
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .status,
            "pinned"
        );
    }

    #[tokio::test]
    async fn multiple_recovery_matches_stay_recovering_and_never_post_again() {
        let fixture = fixture([
            Script::Submit(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Ambiguous,
                "conflict",
            ))),
            Script::Find(Ok(vec![
                remote("request-a", RemotePinStatus::Queued),
                remote("request-b", RemotePinStatus::Queued),
            ])),
        ])
        .await;
        fixture.enqueue_submit().await;
        fixture.run_one_due().await;

        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(submit.state, "pending");
        assert_eq!(submit.submit_phase.as_deref(), Some("recovering"));
        assert_eq!(submit.attempts, 1);
        assert_eq!(
            submit.last_error.as_deref(),
            Some("provider protocol error")
        );
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn eight_ambiguous_recovery_finds_hold_quota_then_one_match_converges_without_post() {
        let ambiguous = || {
            Script::Find(Ok(vec![
                remote("ambiguous-a", RemotePinStatus::Queued),
                remote("ambiguous-b", RemotePinStatus::Queued),
            ]))
        };
        let mut scripts: Vec<_> = (0..8).map(|_| ambiguous()).collect();
        scripts.push(Script::Find(Ok(vec![remote(
            "conclusive-match",
            RemotePinStatus::Pinned,
        )])));
        let fixture = fixture(scripts).await;
        fixture.enqueue_submit().await;
        let past = Utc::now() - chrono::Duration::seconds(60);
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET state='running', submit_phase='calling', \
                 locked_until='{}' WHERE operation='submit'",
                past.to_rfc3339()
            ))
            .await
            .unwrap();

        fixture.run_claim_at(Utc::now()).await;
        for _ in 1..8 {
            fixture.run_one_due().await;
        }
        let ambiguous_row = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ambiguous_row.state, "pending");
        assert_eq!(ambiguous_row.submit_phase.as_deref(), Some("recovering"));
        assert_eq!(ambiguous_row.attempts, 8);
        assert!(
            ambiguous_row.next_attempt_at
                >= ambiguous_row.updated_at + chrono::Duration::seconds(300)
        );
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 8);
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("reconcile"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));

        fixture.run_one_due().await;
        let converged = pin_job::Entity::find_by_id(ambiguous_row.id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(converged.state, "done");
        assert_eq!(converged.attempts, 8);
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 9);
        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.request_id.as_deref(), Some("conclusive-match"));
        assert_eq!(remote.status, "pinned");
    }

    #[tokio::test]
    async fn stale_poll_owner_hands_off_to_current_canonical_poll_without_duplicate_post() {
        let blocker = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("handoff-request", RemotePinStatus::Pinning),
        });
        let fixture = fixture([Script::BlockGet(blocker.clone())]).await;
        let now = Utc::now();
        let created = (now + chrono::Duration::milliseconds(1)).to_rfc3339();
        let expires = (now + chrono::Duration::hours(1)).to_rfc3339();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) \
                 VALUES ('lease-handoff', 'object-1', 'copy', 'policy', 'all', 'full', \
                         '{created}', '{created}', '{expires}', 1, 'active'); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES ('target-handoff', 'lease-handoff', 'bafy-worker', 100, 'noop', \
                         'submitted', '{created}', '{created}')"
            ))
            .await
            .unwrap();
        fixture.enqueue_poll("handoff-request").await;
        let due = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .next_attempt_at;
        let claimed =
            jobs::claim_due_jobs(fixture.store.db(), due, chrono::Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
        let old_poll_id = claimed.model.id.clone();
        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("Poll did not enter provider GET");
        leases::cancel_lease(fixture.store.db(), "lease-1", Utc::now())
            .await
            .unwrap();
        blocker.release.notify_one();
        task.await.unwrap().unwrap();

        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 1);
        assert_eq!(
            pin_job::Entity::find_by_id(old_poll_id)
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "done"
        );
        let current_poll = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .filter(pin_job::Column::TargetId.eq("target-handoff"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("current canonical target must own a stable Poll");
        assert_eq!(current_poll.state, "pending");
        assert_eq!(current_poll.expected_generation, Some(1));
        let jobs::NewPinJob::Target(expected) = jobs::poll_job(
            "noop",
            "bafy-worker",
            "lease-handoff",
            "target-handoff",
            1,
            "handoff-request",
            current_poll.next_attempt_at,
        ) else {
            unreachable!()
        };
        assert_eq!(current_poll.id, expected.id);
    }

    #[tokio::test]
    async fn reclaimed_submit_finds_before_stale_target_and_releases_only_via_reconcile() {
        let fixture = fixture([Script::Find(Ok(Vec::new()))]).await;
        fixture.enqueue_submit().await;
        let past = Utc::now() - chrono::Duration::seconds(60);
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET state='running', submit_phase='calling', \
                 locked_until='{}' WHERE operation='submit'",
                past.to_rfc3339()
            ))
            .await
            .unwrap();
        leases::cancel_lease(fixture.store.db(), "lease-1", Utc::now())
            .await
            .unwrap();

        fixture.run_claim_at(Utc::now()).await;
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));

        fixture.run_one_due().await;
        fixture.run_one_due().await;
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (0, 0));
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .status,
            "absent"
        );
    }

    #[tokio::test]
    async fn stale_remote_epoch_skips_unpin_before_provider_call() {
        let fixture = fixture([]).await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='request-old', status='pinned' \
                 WHERE provider='noop' AND cid='bafy-worker'",
            )
            .await
            .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::unpin_job("noop", "bafy-worker", 1, Utc::now()),
        )
        .await
        .unwrap();
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET epoch=2 WHERE provider='noop' AND cid='bafy-worker'",
            )
            .await
            .unwrap();

        fixture.run_one_due().await;
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
        assert!(
            pin_job::Entity::find_by_id("reconcile:noop:bafy-worker:e2".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn reconcile_epoch_bump_between_snapshot_and_apply_cannot_project_old_response() {
        let fixture = fixture([]).await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET epoch=71, request_id='same-request', status='failed', \
                 failure_attempts=0, last_failed_request_id=NULL, next_retry_at=NULL \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='submitted' WHERE id='target-1'",
            )
            .await
            .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 71, Utc::now()),
        )
        .await
        .unwrap();

        let gate = Arc::new(super::ReconcileAfterSnapshotGate {
            expected_epoch: 71,
            arrived: Notify::new(),
            resume: Notify::new(),
        });
        *super::RECONCILE_AFTER_SNAPSHOT.lock().await = Some(gate.clone());
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
        let arrived = gate.arrived.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), arrived)
            .await
            .expect("Reconcile did not pause after the epoch-71 snapshot");
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET epoch=72 \
                 WHERE provider='noop' AND cid='bafy-worker' AND request_id='same-request'",
            )
            .await
            .unwrap();
        gate.resume.notify_one();
        task.await.unwrap().unwrap();
        *super::RECONCILE_AFTER_SNAPSHOT.lock().await = None;

        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.epoch, 72);
        assert_eq!(remote.request_id.as_deref(), Some("same-request"));
        assert_eq!(remote.status, "failed");
        assert_eq!(remote.failure_attempts, 0);
        assert_eq!(remote.last_failed_request_id, None);
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "submitted"
        );
        assert_eq!(
            pin_job::Entity::find_by_id("reconcile:noop:bafy-worker:e71".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "done"
        );
        assert!(
            pin_job::Entity::find_by_id("reconcile:noop:bafy-worker:e72".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn failed_all_remote_forgets_request_resubmits_and_pinned_resets_budget() {
        let fixture = fixture([
            Script::Unpin(Ok(())),
            Script::Submit(Ok(remote("request-2", RemotePinStatus::Pinned))),
        ])
        .await;
        let now = Utc::now();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE remote_pins SET request_id='failed-request-1', status='failed', \
                 failure_attempts=1, last_failed_request_id='failed-request-1', \
                 next_retry_at='{}', last_error_class='remote_failed', \
                 last_error_text='remote pin failed' \
                 WHERE provider='noop' AND cid='bafy-worker'",
                (now - chrono::Duration::seconds(1)).to_rfc3339()
            ))
            .await
            .unwrap();
        fixture
            .store
            .db()
            .execute_unprepared("UPDATE pin_lease_targets SET state='degraded' WHERE id='target-1'")
            .await
            .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 1, now),
        )
        .await
        .unwrap();

        fixture.run_one_due().await;
        let prepared =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 1);
        assert_eq!(prepared.epoch, 2);
        assert_eq!(prepared.status, "reserved");
        assert_eq!(prepared.request_id, None);
        assert_eq!(prepared.failure_attempts, 1);

        fixture.run_one_due().await;
        let pinned = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(pinned.status, "pinned");
        assert_eq!(pinned.failure_attempts, 0);
        assert_eq!(pinned.next_retry_at, None);
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
    }

    #[tokio::test]
    async fn failed_observation_persists_safe_provider_reason_through_reconcile() {
        let mut failed = remote("safe-failed-request", RemotePinStatus::Failed);
        failed.failure_reason = Some("provider reported insufficient storage capacity".to_owned());
        let fixture = fixture([Script::Get(Ok(failed))]).await;

        fixture.enqueue_poll("safe-failed-request").await;
        fixture.run_one_due().await;

        let failed = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, "failed");
        assert_eq!(
            failed.last_error_text.as_deref(),
            Some("provider reported insufficient storage capacity")
        );

        fixture.run_one_due().await;

        let reconciled =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(reconciled.status, "failed");
        assert_eq!(
            reconciled.last_error_text.as_deref(),
            Some("provider reported insufficient storage capacity"),
            "current Reconcile must preserve the terminal provider reason"
        );
    }

    #[tokio::test]
    async fn coordination_one_transient_stays_sticky_then_terminal_selects_next_provider() {
        let mut fixture = fixture([
            Script::Get(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Transient,
                "transient provider detail must stay redacted",
            ))),
            Script::Get(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Terminal,
                "terminal provider detail must stay redacted",
            ))),
        ])
        .await;
        PinningCoordinator::configure_policy_providers_for_test(
            &mut fixture.coordinator,
            "policy",
            &["noop", "fast"],
        );
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE pin_leases SET provider_mode='one' WHERE id='lease-1'; \
                 UPDATE remote_pins SET request_id='sticky-request', status='queued' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='submitted' WHERE id='target-1'",
            )
            .await
            .unwrap();
        fixture.enqueue_poll("sticky-request").await;

        fixture.run_one_due().await;

        assert_eq!(
            pin_lease::Entity::find_by_id("lease-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            1
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq("lease-1"))
                .filter(pin_lease_target::Column::Provider.eq("fast"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0,
            "transient failure must retry the sticky provider in place"
        );
        let retry = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.state, "pending");
        assert_eq!(retry.attempts, 1);
        assert_eq!(retry.expected_generation, Some(1));
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .failure_attempts,
            0,
            "target retry handling must not count a shared failed request"
        );

        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET next_attempt_at='{}' WHERE id='{}'",
                (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(),
                retry.id
            ))
            .await
            .unwrap();
        fixture.run_one_due().await;

        let lease = pin_lease::Entity::find_by_id("lease-1".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.generation, 2);
        assert_eq!(lease.state, "active");
        let replacement = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("lease-1"))
            .filter(pin_lease_target::Column::Provider.eq("fast"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("terminal sticky target must select the next provider");
        assert_eq!(replacement.state, "waiting");
        let replacement_remote =
            remote_pin::Entity::find_by_id(("fast".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(replacement_remote.epoch, 1);
        assert_eq!(replacement_remote.status, "reserved");
        let replacement_submit = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replacement_submit.lease_id.as_deref(), Some("lease-1"));
        assert_eq!(
            replacement_submit.target_id.as_deref(),
            Some(replacement.id.as_str())
        );
        assert_eq!(replacement_submit.expected_generation, Some(2));
        assert_eq!(replacement_submit.expected_remote_epoch, None);
        assert!(
            !jobs::check_target_job_generation(fixture.store.db(), &retry)
                .await
                .unwrap(),
            "the old-provider retry must be generation-stale after failover"
        );
    }

    #[tokio::test]
    async fn coordination_pure_one_failed_observation_preserves_budget_and_splits_lock_phases() {
        let _order_guard = leases::test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let mut fixture = fixture([Script::Get(Ok(remote_for(
            "bafy-order-pure-one",
            "pure-one-failed-request",
            RemotePinStatus::Failed,
        )))])
        .await;
        PinningCoordinator::configure_policy_providers_for_test(
            &mut fixture.coordinator,
            "policy",
            &["noop", "fast"],
        );
        let cid = "bafy-order-pure-one";
        let lease_id = "lease-order-pure-one";
        let target_id = "target-order-pure-one";
        let now = Utc::now();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_provider_usage SET reserved_bytes=200, reserved_pins=2 \
                 WHERE provider='noop'; \
                 INSERT INTO remote_pins \
                 (provider, cid, cid_size, request_id, status, epoch, failure_attempts, last_touched_at) \
                 VALUES ('noop', '{cid}', 100, 'pure-one-failed-request', 'queued', 1, 0, '{}'); \
                 INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) \
                 VALUES ('{lease_id}', 'object-1', 'order-test', 'policy', 'one', 'full', '{}', \
                         '{}', '{}', 1, 'active'); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES ('{target_id}', '{lease_id}', '{cid}', 100, 'noop', 'submitted', '{}', '{}')",
                now.to_rfc3339(),
                now.to_rfc3339(),
                now.to_rfc3339(),
                (now + chrono::Duration::hours(1)).to_rfc3339(),
                now.to_rfc3339(),
                now.to_rfc3339()
            ))
            .await
            .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::poll_job(
                "noop",
                cid,
                lease_id,
                target_id,
                1,
                "pure-one-failed-request",
                now,
            ),
        )
        .await
        .unwrap();
        *leases::test_gates::LIFECYCLE_ORDER_RECORDER.lock().await =
            Some(leases::test_gates::LifecycleOrderRecorder {
                owner_ids: BTreeSet::new(),
                lease_ids: BTreeSet::from([lease_id.to_owned()]),
                target_ids: BTreeSet::from([target_id.to_owned()]),
                remote_pairs: BTreeSet::from([
                    ("fast".to_owned(), cid.to_owned()),
                    ("noop".to_owned(), cid.to_owned()),
                ]),
                record_desired_target_reads: false,
                events: Vec::new(),
            });

        fixture.run_one_due().await;

        let events = leases::test_gates::LIFECYCLE_ORDER_RECORDER
            .lock()
            .await
            .take()
            .unwrap()
            .events;
        let phases = events
            .split(|event| {
                *event == leases::test_gates::LifecycleOrderEvent::WorkerTransactionBoundary
            })
            .collect::<Vec<_>>();
        assert_eq!(
            phases.len(),
            2,
            "status projection and failover must be separated by one committed transaction boundary: {events:?}"
        );
        for phase in phases {
            let mut remote_locked = false;
            for event in phase {
                match event {
                    leases::test_gates::LifecycleOrderEvent::RemoteLock(_, _) => {
                        remote_locked = true;
                    }
                    leases::test_gates::LifecycleOrderEvent::LeaseLock(_)
                    | leases::test_gates::LifecycleOrderEvent::TargetLock(_)
                        if remote_locked =>
                    {
                        panic!(
                            "lifecycle lock reacquired after remote lock in one phase: {events:?}"
                        )
                    }
                    _ => {}
                }
            }
        }
        let failed = remote_pin::Entity::find_by_id(("noop".to_owned(), cid.to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.failure_attempts, 0);
        assert_eq!(failed.last_failed_request_id, None);
        assert_eq!(failed.next_retry_at, None);
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("noop"))
                .filter(pin_job::Column::Cid.eq(cid))
                .filter(pin_job::Column::Operation.eq("reconcile"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0,
            "pure One failure must not create an All retry owner"
        );
        let lease = pin_lease::Entity::find_by_id(lease_id.to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.generation, 2);
        let replacement = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
            .filter(pin_lease_target::Column::Provider.eq("fast"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("pure One failed observation must create a sticky replacement");
        assert_eq!(replacement.state, "waiting");
        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(submit.lease_id.as_deref(), Some(lease_id));
        assert_eq!(submit.target_id.as_deref(), Some(replacement.id.as_str()));
        assert_eq!(submit.expected_generation, Some(2));
        assert_eq!(submit.expected_remote_epoch, None);
    }

    #[tokio::test]
    async fn coordination_pinned_replacement_convergence_splits_canonical_lock_phases() {
        let _order_guard = leases::test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let fixture = fixture([Script::Get(Ok(remote_for(
            "bafy-order-pinned",
            "replacement-pinned",
            RemotePinStatus::Pinned,
        )))])
        .await;
        let now = Utc::now();
        let cid = "bafy-order-pinned";
        let lease_id = "lease-order-pinned";
        let winner_target_id = "target-order-pinned";
        let old_target_id = "target-order-old-fast";
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_provider_usage SET reserved_bytes=200, reserved_pins=2 \
                 WHERE provider='noop'; \
                 INSERT INTO remote_pins \
                 (provider, cid, cid_size, request_id, status, epoch, failure_attempts, last_touched_at) \
                 VALUES \
                 ('noop', '{cid}', 100, 'replacement-pinned', 'queued', 1, 0, '{}'), \
                 ('fast', '{cid}', 100, 'old-fast-request', 'failed', 1, 0, '{}'); \
                 INSERT INTO pin_provider_usage (provider, reserved_bytes, reserved_pins) \
                 VALUES ('fast', 100, 1); \
                 INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) \
                 VALUES ('{lease_id}', 'object-1', 'order-pinned', 'policy', 'one', 'full', '{}', \
                         '{}', '{}', 2, 'active'); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES \
                 ('{old_target_id}', '{lease_id}', '{cid}', 100, 'fast', 'degraded', '{}', '{}'), \
                 ('{winner_target_id}', '{lease_id}', '{cid}', 100, 'noop', 'submitted', '{}', '{}')",
                now.to_rfc3339(),
                now.to_rfc3339(),
                now.to_rfc3339(),
                now.to_rfc3339(),
                (now + chrono::Duration::hours(1)).to_rfc3339(),
                (now - chrono::Duration::seconds(60)).to_rfc3339(),
                now.to_rfc3339(),
                now.to_rfc3339(),
                now.to_rfc3339()
            ))
            .await
            .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::poll_job(
                "noop",
                cid,
                lease_id,
                winner_target_id,
                2,
                "replacement-pinned",
                now,
            ),
        )
        .await
        .unwrap();
        *leases::test_gates::LIFECYCLE_ORDER_RECORDER.lock().await =
            Some(leases::test_gates::LifecycleOrderRecorder {
                owner_ids: BTreeSet::new(),
                lease_ids: BTreeSet::from([lease_id.to_owned()]),
                target_ids: BTreeSet::from([old_target_id.to_owned(), winner_target_id.to_owned()]),
                remote_pairs: BTreeSet::from([
                    ("fast".to_owned(), cid.to_owned()),
                    ("noop".to_owned(), cid.to_owned()),
                ]),
                record_desired_target_reads: false,
                events: Vec::new(),
            });

        fixture.run_one_due().await;

        let events = leases::test_gates::LIFECYCLE_ORDER_RECORDER
            .lock()
            .await
            .take()
            .unwrap()
            .events;
        let phases = events
            .split(|event| {
                *event == leases::test_gates::LifecycleOrderEvent::WorkerTransactionBoundary
            })
            .collect::<Vec<_>>();
        assert_eq!(
            phases.len(),
            2,
            "pinned status and convergence require one committed phase boundary: {events:?}"
        );
        for phase in phases {
            let mut remote_locked = false;
            for event in phase {
                match event {
                    leases::test_gates::LifecycleOrderEvent::RemoteLock(_, _) => {
                        remote_locked = true;
                    }
                    leases::test_gates::LifecycleOrderEvent::LeaseLock(_)
                    | leases::test_gates::LifecycleOrderEvent::TargetLock(_)
                        if remote_locked =>
                    {
                        panic!("pinned path reacquired lifecycle after remote lock: {events:?}")
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(
            pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            3
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id(winner_target_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned"
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id(old_target_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "released"
        );
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), cid.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .epoch,
            1
        );
        assert_eq!(
            remote_pin::Entity::find_by_id(("fast".to_owned(), cid.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .epoch,
            2
        );
        let cleanup = pin_job::Entity::find_by_id(format!("unpin:fast:{cid}:e2"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("convergence must atomically enqueue exact old remote Unpin");
        assert!(cleanup.lease_id.is_none());
        assert!(cleanup.target_id.is_none());
        assert!(cleanup.expected_generation.is_none());
        assert_eq!(cleanup.expected_remote_epoch, Some(2));
    }

    #[tokio::test]
    async fn coordination_reclaimed_submit_recovers_after_projection_commit_interruption() {
        let mut fixture = fixture([
            Script::Submit(Ok(remote("crash-window-failed", RemotePinStatus::Failed))),
            Script::Find(Ok(vec![remote(
                "crash-window-failed",
                RemotePinStatus::Failed,
            )])),
        ])
        .await;
        PinningCoordinator::configure_policy_providers_for_test(
            &mut fixture.coordinator,
            "policy",
            &["noop", "fast"],
        );
        fixture
            .store
            .db()
            .execute_unprepared("UPDATE pin_leases SET provider_mode='one' WHERE id='lease-1'")
            .await
            .unwrap();
        fixture.enqueue_submit().await;
        let initial_job = pin_job::Entity::find()
            .filter(pin_job::Column::State.eq("pending"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let gate = Arc::new(super::ObservationAfterStatusCommitGate {
            job_id: initial_job.id.clone(),
            request_id: "crash-window-failed".to_owned(),
            fail_once: std::sync::atomic::AtomicBool::new(true),
            arrived: Notify::new(),
        });
        let gate_key = (gate.job_id.clone(), gate.request_id.clone());
        super::OBSERVATION_AFTER_STATUS_COMMIT
            .lock()
            .await
            .insert(gate_key.clone(), gate);
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            initial_job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let interruption = super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            claimed,
        )
        .await
        .unwrap_err();
        assert_eq!(
            interruption.to_string(),
            "database error: test coordination failure after durable pin status projection"
        );
        let projected =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(projected.status, "failed");
        assert_eq!(projected.request_id.as_deref(), Some("crash-window-failed"));
        assert_eq!(projected.failure_attempts, 0);
        assert_eq!(projected.last_failed_request_id, None);
        assert_eq!(projected.next_retry_at, None);
        assert_eq!(
            pin_lease::Entity::find_by_id("lease-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            1,
            "interruption occurs before coordination commits"
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq("lease-1"))
                .filter(pin_lease_target::Column::Provider.eq("fast"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );
        let interrupted_job = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("noop"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(interrupted_job.state, "running");

        super::OBSERVATION_AFTER_STATUS_COMMIT
            .lock()
            .await
            .remove(&gate_key);
        let reclaim_at = Utc::now();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET locked_until='{}' WHERE id='{}'",
                (reclaim_at - chrono::Duration::seconds(1)).to_rfc3339(),
                interrupted_job.id
            ))
            .await
            .unwrap();
        let reclaimed = jobs::claim_due_jobs(
            fixture.store.db(),
            reclaim_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .expect("expired observation owner must be reclaimable");
        assert!(reclaimed.reclaimed);
        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            reclaimed,
        )
        .await
        .unwrap();

        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(
            pin_lease::Entity::find_by_id("lease-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            2
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq("lease-1"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            2
        );
        let replacement = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("lease-1"))
            .filter(pin_lease_target::Column::Provider.eq("fast"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replacement.state, "waiting");
        assert_eq!(
            remote_pin::Entity::find_by_id(("fast".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .epoch,
            1
        );
        let replacement_jobs = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .all(fixture.store.db())
            .await
            .unwrap();
        assert_eq!(replacement_jobs.len(), 1);
        assert_eq!(
            replacement_jobs[0].target_id.as_deref(),
            Some(replacement.id.as_str())
        );
        assert_eq!(replacement_jobs[0].expected_generation, Some(2));
        assert_eq!(replacement_jobs[0].expected_remote_epoch, None);
        assert_eq!(
            pin_job::Entity::find_by_id(interrupted_job.id)
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "done"
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("noop"))
                .filter(pin_job::Column::Operation.eq("reconcile"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn coordination_pinned_fallback_reconciles_ambiguous_old_submit_then_not_found_delete() {
        let mut fixture = fixture([
            Script::Find(Ok(vec![remote(
                "accepted-old-request",
                RemotePinStatus::Pinned,
            )])),
            Script::Unpin(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::NotFound,
                "provider NotFound body must stay redacted",
            ))),
        ])
        .await;
        PinningCoordinator::configure_policy_providers_for_test(
            &mut fixture.coordinator,
            "policy",
            &["noop", "fast"],
        );
        fixture.enqueue_submit().await;
        let now = Utc::now();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_leases SET provider_mode='one' WHERE id='lease-1'; \
                 UPDATE pin_jobs SET state='running', submit_phase='calling', locked_until='{}' \
                 WHERE operation='submit'; \
                 INSERT INTO remote_pins \
                 (provider, cid, cid_size, request_id, status, epoch, failure_attempts, last_touched_at) \
                 VALUES ('fast', 'bafy-worker', 100, 'shared-fast', 'pinned', 7, 0, '{}'); \
                 INSERT INTO pin_provider_usage (provider, reserved_bytes, reserved_pins) \
                 VALUES ('fast', 100, 1)",
                (now + chrono::Duration::seconds(60)).to_rfc3339(),
                now.to_rfc3339()
            ))
            .await
            .unwrap();
        let ambiguous_submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();

        super::coordinate_claimed_one_target(
            &fixture.store,
            &fixture.coordinator,
            &ambiguous_submit,
            now,
        )
        .await
        .unwrap();

        assert_eq!(
            pin_lease::Entity::find_by_id("lease-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            3
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "released"
        );
        let fallback = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("lease-1"))
            .filter(pin_lease_target::Column::Provider.eq("fast"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fallback.state, "pinned");
        let old_remote =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(old_remote.epoch, 2);
        assert_eq!(old_remote.request_id, None);
        let reconcile = pin_job::Entity::find_by_id("reconcile:noop:bafy-worker:e2".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("pinned fallback must atomically enqueue old remote Reconcile");
        assert_eq!(reconcile.state, "pending");
        assert!(reconcile.lease_id.is_none());
        assert!(reconcile.target_id.is_none());
        assert!(reconcile.expected_generation.is_none());
        assert_eq!(reconcile.expected_remote_epoch, Some(2));
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("submit"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            1,
            "pinned fallback must not create another Submit"
        );
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);

        fixture.run_one_due().await;
        let waiting_reconcile =
            pin_job::Entity::find_by_id("reconcile:noop:bafy-worker:e2".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(waiting_reconcile.state, "pending");
        assert!(waiting_reconcile.next_attempt_at >= now + chrono::Duration::seconds(60));
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);

        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET locked_until='{}' WHERE id='{}'",
                (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(),
                ambiguous_submit.id
            ))
            .await
            .unwrap();
        fixture.run_claim_at(Utc::now()).await;
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .request_id
                .as_deref(),
            Some("accepted-old-request")
        );

        fixture.run_one_due().await;
        let unpin = pin_job::Entity::find_by_id("unpin:noop:bafy-worker:e2".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("remote Reconcile must create an exact current-epoch Unpin");
        assert_eq!(unpin.state, "pending");
        assert!(unpin.lease_id.is_none());
        assert!(unpin.target_id.is_none());
        assert!(unpin.expected_generation.is_none());
        fixture.run_one_due().await;

        let converged =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(converged.status, "absent");
        assert_eq!(converged.epoch, 2);
        assert_eq!(converged.request_id, None);
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 1);
        let old_usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((old_usage.reserved_bytes, old_usage.reserved_pins), (0, 0));
        let fallback_usage = pin_provider_usage::Entity::find_by_id("fast".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (fallback_usage.reserved_bytes, fallback_usage.reserved_pins),
            (100, 1)
        );
        assert_eq!(
            pin_job::Entity::find_by_id("unpin:noop:bafy-worker:e2".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "done"
        );
    }

    #[tokio::test]
    async fn coordination_shared_failure_fails_over_each_one_and_keeps_one_all_retry_owner() {
        let mut fixture = fixture([Script::Get(Ok(remote(
            "shared-failed-request",
            RemotePinStatus::Failed,
        )))])
        .await;
        PinningCoordinator::configure_policy_providers_for_test(
            &mut fixture.coordinator,
            "policy",
            &["noop", "fast", "third"],
        );
        let now = Utc::now();
        let created = now.to_rfc3339();
        let expires = (now + chrono::Duration::hours(1)).to_rfc3339();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE remote_pins SET request_id='shared-failed-request', status='queued' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='submitted' WHERE id='target-1'; \
                 INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) VALUES \
                 ('lease-one-a', 'object-1', 'one-a', 'policy', 'one', 'full', '{created}', \
                  '{created}', '{expires}', 1, 'active'), \
                 ('lease-one-b', 'object-1', 'one-b', 'policy', 'one', 'full', '{created}', \
                  '{created}', '{expires}', 1, 'active'); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES \
                 ('target-one-a-old', 'lease-one-a', 'bafy-worker', 100, 'noop', 'submitted', \
                  '{created}', '{created}'), \
                 ('target-one-b-old', 'lease-one-b', 'bafy-worker', 100, 'noop', 'submitted', \
                  '{created}', '{created}')"
            ))
            .await
            .unwrap();
        fixture.enqueue_poll("shared-failed-request").await;

        fixture.run_one_due().await;

        let failed_remote =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(failed_remote.status, "failed");
        assert_eq!(failed_remote.failure_attempts, 1);
        assert_eq!(
            failed_remote.last_failed_request_id.as_deref(),
            Some("shared-failed-request")
        );
        assert!(failed_remote.next_retry_at.is_some());

        for (lease_id, old_target_id) in [
            ("lease-one-a", "target-one-a-old"),
            ("lease-one-b", "target-one-b-old"),
        ] {
            let lease = pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(lease.generation, 2);
            assert_eq!(
                pin_lease_target::Entity::find_by_id(old_target_id.to_owned())
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "degraded"
            );
            let replacement = pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .filter(pin_lease_target::Column::Provider.eq("fast"))
                .one(fixture.store.db())
                .await
                .unwrap()
                .expect("each affected one lease must receive its own replacement");
            assert_eq!(replacement.state, "waiting");
        }
        let replacement_remote =
            remote_pin::Entity::find_by_id(("fast".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(replacement_remote.epoch, 2);
        assert_eq!(replacement_remote.status, "reserved");
        let replacement_submits = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .filter(pin_job::Column::Cid.eq("bafy-worker"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .all(fixture.store.db())
            .await
            .unwrap();
        assert_eq!(replacement_submits.len(), 1);
        assert_eq!(replacement_submits[0].expected_generation, Some(2));
        assert!(replacement_submits[0].lease_id.is_some());
        assert!(replacement_submits[0].target_id.is_some());
        assert_eq!(replacement_submits[0].expected_remote_epoch, None);
        let failed_retries = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("noop"))
            .filter(pin_job::Column::Cid.eq("bafy-worker"))
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(1))
            .all(fixture.store.db())
            .await
            .unwrap();
        assert_eq!(failed_retries.len(), 1);
        assert!(failed_retries[0].lease_id.is_none());
        assert!(failed_retries[0].target_id.is_none());
        assert!(failed_retries[0].expected_generation.is_none());

        for _ in 0..3 {
            let remote =
                remote_pin::Entity::find_by_id(("fast".to_owned(), "bafy-worker".to_owned()))
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap();
            if remote.status == "pinned" {
                break;
            }
            fixture.run_one_due().await;
        }

        let replacement_remote =
            remote_pin::Entity::find_by_id(("fast".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(replacement_remote.status, "pinned");
        assert_eq!(replacement_remote.epoch, 2);
        for (lease_id, old_target_id) in [
            ("lease-one-a", "target-one-a-old"),
            ("lease-one-b", "target-one-b-old"),
        ] {
            assert_eq!(
                pin_lease::Entity::find_by_id(lease_id.to_owned())
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap()
                    .generation,
                3
            );
            assert_eq!(
                pin_lease_target::Entity::find_by_id(old_target_id.to_owned())
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "released"
            );
            assert_eq!(
                pin_lease_target::Entity::find()
                    .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                    .filter(pin_lease_target::Column::Provider.eq("fast"))
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "pinned"
            );
        }
        let old_remote =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(old_remote.epoch, 3);
        assert_eq!(old_remote.failure_attempts, 1);
        let current_cleanup = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("noop"))
            .filter(pin_job::Column::Cid.eq("bafy-worker"))
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(3))
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("latest old-provider cleanup must be remote scoped");
        assert!(current_cleanup.lease_id.is_none());
        assert!(current_cleanup.target_id.is_none());
        assert!(current_cleanup.expected_generation.is_none());
    }

    #[tokio::test]
    async fn coordination_all_mode_keeps_pinned_availability_with_independent_failed_retry() {
        let mut fixture = fixture([Script::Submit(Ok(remote(
            "noop-pinned",
            RemotePinStatus::Pinned,
        )))])
        .await;
        let fast = ScriptProvider::new([Script::Get(Ok(remote(
            "fast-failed",
            RemotePinStatus::Failed,
        )))]);
        PinningCoordinator::replace_provider_for_test(
            &mut fixture.coordinator,
            "fast",
            fast.clone(),
        );
        let now = Utc::now();
        let created = now.to_rfc3339();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "INSERT INTO remote_pins \
                 (provider, cid, cid_size, request_id, status, epoch, failure_attempts, last_touched_at) \
                 VALUES ('fast', 'bafy-worker', 100, 'fast-failed', 'queued', 1, 0, '{created}'); \
                 INSERT INTO pin_provider_usage (provider, reserved_bytes, reserved_pins) \
                 VALUES ('fast', 100, 1); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES ('target-all-fast', 'lease-1', 'bafy-worker', 100, 'fast', 'submitted', \
                         '{created}', '{created}')"
            ))
            .await
            .unwrap();
        fixture.enqueue_submit().await;
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::poll_job(
                "fast",
                "bafy-worker",
                "lease-1",
                "target-all-fast",
                1,
                "fast-failed",
                now,
            ),
        )
        .await
        .unwrap();

        fixture.run_one_due().await;
        fixture.run_one_due().await;

        let targets = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("lease-1"))
            .all(fixture.store.db())
            .await
            .unwrap();
        assert!(targets.iter().any(|target| target.state == "pinned"));
        assert_eq!(
            targets
                .iter()
                .find(|target| target.provider == "noop")
                .unwrap()
                .state,
            "pinned"
        );
        assert_eq!(
            targets
                .iter()
                .find(|target| target.provider == "fast")
                .unwrap()
                .state,
            "degraded"
        );
        let failed = remote_pin::Entity::find_by_id(("fast".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.failure_attempts, 1);
        assert!(failed.next_retry_at.is_some());
        let retry = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("fast"))
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(1))
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("failed all target must retain one remote retry");
        assert_eq!(retry.state, "pending");
        assert!(retry.lease_id.is_none());
        assert!(retry.target_id.is_none());
        assert!(retry.expected_generation.is_none());
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fast.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_delete_retry_after_epoch_bump_completes_stale_reconcile_without_retrying_it() {
        let blocker = Arc::new(UnpinBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(Some(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Transient,
                "failed DELETE response body must stay redacted",
            )))),
        });
        let fixture = fixture([Script::BlockUnpin(blocker.clone())]).await;
        let due = Utc::now() - chrono::Duration::seconds(1);
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE remote_pins SET epoch=61, request_id='failed-delete-race', status='failed', \
                 failure_attempts=1, last_failed_request_id='failed-delete-race', \
                 next_retry_at='{}' WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='degraded' WHERE id='target-1'",
                due.to_rfc3339()
            ))
            .await
            .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 61, Utc::now()),
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
        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("failed request DELETE did not enter provider");
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET epoch=62 \
                 WHERE provider='noop' AND cid='bafy-worker' \
                   AND request_id='failed-delete-race'",
            )
            .await
            .unwrap();
        blocker.release.notify_one();
        task.await.unwrap().unwrap();

        let stale = pin_job::Entity::find_by_id("reconcile:noop:bafy-worker:e61".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stale.state, "done");
        assert_eq!(stale.attempts, 0);
        assert_eq!(stale.last_error, None);
        let current = pin_job::Entity::find_by_id("reconcile:noop:bafy-worker:e62".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("stale failed DELETE must ensure current epoch Reconcile");
        assert_eq!(current.state, "pending");
        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.epoch, 62);
        assert_eq!(remote.request_id.as_deref(), Some("failed-delete-race"));
        assert_eq!(remote.failure_attempts, 1);
    }

    #[tokio::test]
    async fn eight_distinct_failed_requests_stop_without_a_due_loop_or_quota_release() {
        async fn reapply_duplicate_failure(
            fixture: &Fixture,
            request_id: &str,
            expected_attempts: i32,
        ) {
            let duplicate = leases::apply_worker_remote_status(
                fixture.store.db(),
                leases::RemoteStatusUpdate {
                    provider: "noop",
                    cid: "bafy-worker",
                    request_id,
                    origin: leases::RemoteStatusOrigin::ExistingRequest,
                    status: RemotePinStatus::Failed,
                    error_class: Some("remote_failed"),
                    error_text: Some("remote pin failed"),
                    now: Utc::now(),
                },
            )
            .await
            .unwrap();
            let leases::RemoteStatusApplyResult::Applied {
                affected,
                failure: Some(progress),
                ..
            } = duplicate
            else {
                panic!("duplicate current failed request must remain applicable")
            };
            assert_eq!(affected.len(), 2);
            assert!(affected.iter().all(|outcome| !outcome.available));
            assert_eq!(progress.attempts, expected_attempts);
            assert!(!progress.newly_counted);
        }

        let mut scripts = vec![Script::Submit(Ok(remote(
            "failed-request-1",
            RemotePinStatus::Failed,
        )))];
        for attempt in 2..=8 {
            scripts.push(Script::Unpin(Ok(())));
            scripts.push(Script::Submit(Ok(remote(
                &format!("failed-request-{attempt}"),
                RemotePinStatus::Failed,
            ))));
        }
        let fixture = fixture(scripts).await;
        let now = Utc::now();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) \
                 VALUES ('lease-all-copy', 'object-1', 'copy', 'policy', 'all', 'full', '{}', '{}', \
                         '{}', 1, 'active'); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES ('target-all-copy', 'lease-all-copy', 'bafy-worker', 100, 'noop', 'waiting', \
                         '{}', '{}')",
                now.to_rfc3339(),
                now.to_rfc3339(),
                (now + chrono::Duration::hours(1)).to_rfc3339(),
                now.to_rfc3339(),
                now.to_rfc3339()
            ))
            .await
            .unwrap();
        fixture.enqueue_submit().await;

        fixture.run_one_due().await;
        reapply_duplicate_failure(&fixture, "failed-request-1", 1).await;
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
        let first_reconciles = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("noop"))
            .filter(pin_job::Column::Cid.eq("bafy-worker"))
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(1))
            .all(fixture.store.db())
            .await
            .unwrap();
        assert_eq!(first_reconciles.len(), 1);
        assert_eq!(first_reconciles[0].state, "pending");
        assert!(first_reconciles[0].lease_id.is_none());
        assert!(first_reconciles[0].target_id.is_none());
        assert!(first_reconciles[0].expected_generation.is_none());
        for attempt in 2..=8 {
            fixture
                .store
                .db()
                .execute_unprepared(&format!(
                    "UPDATE remote_pins SET next_retry_at='{}' \
                     WHERE provider='noop' AND cid='bafy-worker'",
                    (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339()
                ))
                .await
                .unwrap();
            fixture.run_one_due().await;
            assert_eq!(
                fixture.provider.unpins.load(Ordering::SeqCst),
                attempt - 1,
                "each retry cycle must DELETE exactly one failed request"
            );
            fixture.run_one_due().await;
            assert_eq!(
                fixture.provider.submits.load(Ordering::SeqCst),
                attempt,
                "each successful failed-request DELETE must create one canonical replacement Submit"
            );
            let request_id = format!("failed-request-{attempt}");
            reapply_duplicate_failure(&fixture, &request_id, attempt as i32).await;
            let remote =
                remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(remote.epoch, attempt as i64);
            assert_eq!(remote.failure_attempts, attempt as i32);
            let current_reconciles = pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("noop"))
                .filter(pin_job::Column::Cid.eq("bafy-worker"))
                .filter(pin_job::Column::Operation.eq("reconcile"))
                .filter(pin_job::Column::ExpectedRemoteEpoch.eq(attempt as i64))
                .all(fixture.store.db())
                .await
                .unwrap();
            if attempt < 8 {
                assert_eq!(current_reconciles.len(), 1);
                assert_eq!(current_reconciles[0].state, "pending");
                assert!(current_reconciles[0].lease_id.is_none());
                assert!(current_reconciles[0].target_id.is_none());
                assert!(current_reconciles[0].expected_generation.is_none());
            } else {
                assert!(current_reconciles.is_empty());
            }
        }

        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 8);
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 7);
        assert_eq!(remote.status, "failed");
        assert_eq!(remote.request_id.as_deref(), Some("failed-request-8"));
        assert_eq!(remote.failure_attempts, 8);
        assert_eq!(remote.next_retry_at, None);
        let target_states = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::Provider.eq("noop"))
            .filter(pin_lease_target::Column::Cid.eq("bafy-worker"))
            .all(fixture.store.db())
            .await
            .unwrap();
        assert_eq!(target_states.len(), 2);
        assert!(
            target_states
                .iter()
                .all(|target| target.state == "degraded")
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::State.eq("pending"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));

        let recovered = leases::apply_remote_status(
            fixture.store.db(),
            leases::RemoteStatusUpdate {
                provider: "noop",
                cid: "bafy-worker",
                request_id: "failed-request-8",
                origin: leases::RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Pinned,
                error_class: None,
                error_text: None,
                now: Utc::now(),
            },
        )
        .await
        .unwrap();
        let leases::RemoteStatusApplyResult::Applied { affected, .. } = recovered else {
            panic!("current failed request must recover to pinned")
        };
        assert_eq!(affected.len(), 2);
        assert!(affected.iter().all(|outcome| outcome.available));
        let recovered_remote =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(recovered_remote.failure_attempts, 0);
        assert_eq!(recovered_remote.next_retry_at, None);
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::Provider.eq("noop"))
                .filter(pin_lease_target::Column::Cid.eq("bafy-worker"))
                .filter(pin_lease_target::Column::State.eq("pinned"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn accepted_post_crash_reclaims_with_find_and_never_posts_twice() {
        let fixture = fixture([Script::Find(Ok(vec![remote(
            "accepted-before-crash",
            RemotePinStatus::Queued,
        )]))])
        .await;
        fixture.enqueue_submit().await;
        fixture.provider.submits.store(1, Ordering::SeqCst);
        let past = Utc::now() - chrono::Duration::seconds(60);
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET state='running', submit_phase='calling', \
                 locked_until='{}' WHERE operation='submit'",
                past.to_rfc3339()
            ))
            .await
            .unwrap();

        fixture.run_claim_at(Utc::now()).await;
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .request_id
                .as_deref(),
            Some("accepted-before-crash")
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("poll"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn blocked_recovery_find_renews_exact_claim_and_cannot_enable_second_post() {
        let blocker = Arc::new(FindBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: vec![remote("accepted-during-find", RemotePinStatus::Queued)],
        });
        let fixture = fixture([Script::BlockFind(blocker.clone())]).await;
        fixture.enqueue_submit().await;
        let past = Utc::now() - chrono::Duration::seconds(60);
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET state='running', submit_phase='calling', \
                 locked_until='{}' WHERE operation='submit'",
                past.to_rfc3339()
            ))
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
        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("recovery Find did not enter provider");

        let live = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(live.submit_phase.as_deref(), Some("recovering"));
        assert!(live.locked_until.unwrap() > Utc::now() + chrono::Duration::seconds(45));
        assert!(
            jobs::claim_due_jobs(
                fixture.store.db(),
                Utc::now() + chrono::Duration::seconds(40),
                chrono::Duration::seconds(30),
                1,
            )
            .await
            .unwrap()
            .is_empty()
        );
        blocker.release.notify_one();
        task.await.unwrap().unwrap();

        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .request_id
                .as_deref(),
            Some("accepted-during-find")
        );
    }

    #[tokio::test]
    async fn shared_targets_use_one_post_and_project_all_then_later_target_is_immediate() {
        let fixture = fixture([Script::Submit(Ok(remote(
            "shared-request",
            RemotePinStatus::Pinned,
        )))])
        .await;
        let now = Utc::now();
        let created = (now + chrono::Duration::milliseconds(1)).to_rfc3339();
        let expires = (now + chrono::Duration::hours(1)).to_rfc3339();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) \
                 VALUES ('lease-2', 'object-1', 'manual', 'policy', 'one', 'full', \
                         '{created}', '{created}', '{expires}', 1, 'active'); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES ('target-2', 'lease-2', 'bafy-worker', 100, 'noop', 'waiting', \
                         '{created}', '{created}')"
            ))
            .await
            .unwrap();
        fixture.enqueue_submit().await;
        fixture.run_one_due().await;

        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
        for id in ["target-1", "target-2"] {
            assert_eq!(
                pin_lease_target::Entity::find_by_id(id.to_owned())
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "pinned"
            );
        }

        let later = (now + chrono::Duration::milliseconds(2)).to_rfc3339();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) \
                 VALUES ('lease-3', 'object-1', 'copy', 'policy', 'all', 'full', \
                         '{later}', '{later}', '{expires}', 1, 'active'); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES ('target-3', 'lease-3', 'bafy-worker', 100, 'noop', 'waiting', \
                         '{later}', '{later}')"
            ))
            .await
            .unwrap();
        leases::project_target_from_remote(fixture.store.db(), "target-3", Utc::now())
            .await
            .unwrap();
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-3".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned"
        );
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn authentication_marks_terminal_and_future_due_work_does_not_call_provider() {
        let fixture = fixture([Script::Get(Err(provider_error(
            crate::pinning::provider::ProviderErrorClass::Authentication,
            "authorization header must not persist",
        )))])
        .await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='request-auth', status='queued' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='submitted' WHERE id='target-1'",
            )
            .await
            .unwrap();
        let now = Utc::now();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::poll_job(
                "noop",
                "bafy-worker",
                "lease-1",
                "target-1",
                1,
                "request-auth",
                now,
            ),
        )
        .await
        .unwrap();

        fixture.run_one_due().await;
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 1);
        assert_eq!(
            *fixture
                .coordinator
                .provider_runtime("noop")
                .unwrap()
                .health
                .read()
                .await,
            crate::pinning::coordinator::ProviderHealth::Terminal
        );
        let first_retry = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_retry.attempts, 1);
        assert_eq!(
            first_retry.last_error.as_deref(),
            Some("provider authentication error")
        );
        assert!(
            first_retry.next_attempt_at >= first_retry.updated_at + chrono::Duration::minutes(5)
        );

        fixture.run_one_due().await;
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 1);
        let second_retry = pin_job::Entity::find_by_id(first_retry.id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second_retry.attempts, 2);
        assert_eq!(
            second_retry.last_error.as_deref(),
            Some("provider terminal error")
        );
    }

    #[tokio::test]
    async fn terminal_provider_health_is_absorbing_across_late_success_and_protocol_results() {
        let fixture = fixture([]).await;
        let runtime = fixture.coordinator.provider_runtime("noop").unwrap();
        super::update_provider_health::<()>(
            runtime,
            &Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Authentication,
                "auth",
            )),
        )
        .await;
        super::update_provider_health(runtime, &Ok(())).await;
        super::mark_runtime_degraded(&fixture.coordinator, "noop").await;
        assert_eq!(
            *runtime.health.read().await,
            crate::pinning::coordinator::ProviderHealth::Terminal
        );
    }

    #[tokio::test]
    async fn rate_limit_transient_and_malformed_poll_errors_use_durable_redacted_retry() {
        let rate_limited = fixture([Script::Get(Err(ProviderError {
            class: crate::pinning::provider::ProviderErrorClass::RateLimited,
            message: "429 raw body bearer-secret".to_owned(),
            retry_after: Some(std::time::Duration::from_secs(19)),
        }))])
        .await;
        rate_limited.enqueue_poll("request-429").await;
        rate_limited.run_one_due().await;
        let rate_job = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(rate_limited.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rate_job.attempts, 1);
        assert_eq!(
            rate_job.last_error.as_deref(),
            Some("provider rate limited")
        );
        assert_eq!(
            rate_job.next_attempt_at - rate_job.updated_at,
            chrono::Duration::seconds(19)
        );

        let transient = fixture([Script::Get(Err(provider_error(
            crate::pinning::provider::ProviderErrorClass::Transient,
            "transport or 5xx raw response",
        )))])
        .await;
        transient.enqueue_poll("request-5xx").await;
        transient.run_one_due().await;
        let transient_job = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(transient.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(transient_job.attempts, 1);
        assert_eq!(
            transient_job.last_error.as_deref(),
            Some("provider transient error")
        );
        let transient_delay = transient_job.next_attempt_at - transient_job.updated_at;
        assert!(transient_delay >= chrono::Duration::seconds(1));
        assert!(transient_delay <= chrono::Duration::milliseconds(1250));

        let malformed = fixture([Script::Get(Ok(remote(
            "wrong-request-id",
            RemotePinStatus::Queued,
        )))])
        .await;
        malformed.enqueue_poll("request-malformed").await;
        malformed.run_one_due().await;
        let malformed_job = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(malformed.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(malformed_job.attempts, 1);
        assert_eq!(
            malformed_job.last_error.as_deref(),
            Some("provider protocol error")
        );
    }

    #[tokio::test]
    async fn poll_for_forgotten_request_is_stale_before_get_and_cannot_restore_identity() {
        let fixture = fixture([]).await;
        fixture.enqueue_poll("request-old").await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='request-new', epoch=2 \
                 WHERE provider='noop' AND cid='bafy-worker'",
            )
            .await
            .unwrap();

        fixture.run_one_due().await;
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.request_id.as_deref(), Some("request-new"));
        assert_eq!(remote.failure_attempts, 0);
        assert!(
            pin_job::Entity::find_by_id("reconcile:noop:bafy-worker:e2".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn shutdown_blocked_submit_preserves_calling_state_then_reclaims_and_adopts() {
        let _paused_clock_test = SHUTDOWN_PAUSED_CLOCK_TEST_LOCK.lock().await;
        let blocker = Arc::new(SubmitBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(Some(Ok(remote(
                "accepted-after-cancel",
                RemotePinStatus::Queued,
            )))),
        });
        let mut fixture = file_backed_fixture([
            Script::BlockSubmit(blocker.clone()),
            Script::Find(Ok(vec![remote(
                "accepted-after-cancel",
                RemotePinStatus::Queued,
            )])),
            Script::Get(Ok(remote("accepted-after-cancel", RemotePinStatus::Pinned))),
        ])
        .await;
        PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
            settings.lock_for = chrono::Duration::seconds(15);
        });
        fixture.enqueue_submit().await;
        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let provider = fixture.provider.clone();
        let handle = coordinator.start(fixture.store, CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("worker did not enter Submit");
        let live_call = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(live_call.locked_until.unwrap() > Utc::now() + chrono::Duration::seconds(20));
        assert!(
            jobs::claim_due_jobs(
                &db,
                Utc::now() + chrono::Duration::seconds(20),
                chrono::Duration::seconds(30),
                1,
            )
            .await
            .unwrap()
            .is_empty()
        );
        tokio::time::pause();
        let shutdown = tokio::spawn(handle.shutdown(std::time::Duration::from_secs(5)));
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        shutdown.await.unwrap();
        tokio::time::resume();

        let blocked = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(blocked.state, "running");
        assert_eq!(blocked.submit_phase.as_deref(), Some("calling"));
        assert!(
            blocked.locked_until.is_some(),
            "shutdown must not clear the claim lock"
        );
        let remote_before_reclaim =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(remote_before_reclaim.status, "reserved");
        assert_eq!(remote_before_reclaim.request_id, None);
        let usage_before_reclaim = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                usage_before_reclaim.reserved_bytes,
                usage_before_reclaim.reserved_pins
            ),
            (100, 1)
        );
        assert_eq!(provider.submits.load(Ordering::SeqCst), 1);

        tokio::time::pause();
        tokio::time::advance(std::time::Duration::from_secs(31)).await;
        tokio::time::resume();
        let restart_store = Store::new(db);
        let claimed = jobs::claim_due_jobs(
            restart_store.db(),
            blocked.locked_until.unwrap() + chrono::Duration::seconds(1),
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert!(claimed.reclaimed);
        assert_eq!(claimed.model.submit_phase.as_deref(), Some("recovering"));
        super::execute_claimed_job(
            &restart_store,
            &coordinator,
            &Arc::new(Semaphore::new(2)),
            claimed,
        )
        .await
        .unwrap();
        assert_eq!(provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(restart_store.db())
                .await
                .unwrap()
                .unwrap()
                .request_id
                .as_deref(),
            Some("accepted-after-cancel")
        );
        let restart = Fixture {
            store: restart_store,
            coordinator,
            provider,
            _database_directory: None,
        };
        restart.run_one_due().await;
        assert_eq!(restart.provider.gets.load(Ordering::SeqCst), 1);
        let converged =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(restart.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            converged.request_id.as_deref(),
            Some("accepted-after-cancel")
        );
        assert_eq!(converged.status, "pinned");
    }

    #[tokio::test]
    async fn worker_backlog_never_claims_above_capacity_and_fast_provider_still_runs() {
        let slow = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("request-backlog-1", RemotePinStatus::Queued),
        });
        let mut fixture = fixture([Script::BlockGet(slow.clone())]).await;
        let fast = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("request-backlog-fast", RemotePinStatus::Queued),
        });
        let fast_provider = ScriptProvider::new([Script::BlockGet(fast.clone())]);
        PinningCoordinator::replace_provider_for_test(
            &mut fixture.coordinator,
            "fast",
            fast_provider.clone(),
        );
        PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
            settings.interval = std::time::Duration::from_millis(10);
            settings.worker_concurrency = 2;
            settings.claim_limit = 4;
            settings.shutdown_grace = std::time::Duration::from_millis(30);
        });
        PinningCoordinator::configure_provider_runtime_for_test(
            &mut fixture.coordinator,
            "noop",
            1,
            std::time::Duration::ZERO,
        );
        let now = Utc::now() - chrono::Duration::seconds(1);
        fixture.seed_poll("backlog-1", now).await;
        fixture
            .seed_poll("backlog-2", now + chrono::Duration::milliseconds(1))
            .await;
        fixture
            .seed_poll_for(
                "fast",
                "backlog-fast",
                now + chrono::Duration::milliseconds(2),
            )
            .await;
        fixture
            .seed_poll("backlog-3", now + chrono::Duration::milliseconds(3))
            .await;

        let slow_entered = slow.entered.notified();
        let fast_entered = fast.entered.notified();
        let db = fixture.store.db().clone();
        let provider = fixture.provider.clone();
        let coordinator = fixture.coordinator.clone();
        let handle = coordinator.start(fixture.store, CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), slow_entered)
            .await
            .expect("slow provider did not enter its first GET");
        tokio::time::timeout(std::time::Duration::from_millis(250), fast_entered)
            .await
            .expect("fast provider was starved behind the second earlier slow-provider job");

        let running = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .filter(pin_job::Column::State.eq("running"))
            .count(&db)
            .await
            .unwrap();
        assert!(
            running <= 2,
            "claimed/spawned backlog exceeded capacity: {running}"
        );
        assert_eq!(provider.gets.load(Ordering::SeqCst), 1);
        assert_eq!(fast_provider.gets.load(Ordering::SeqCst), 1);
        fast.release.notify_one();
        handle.shutdown(std::time::Duration::from_millis(40)).await;
        let calls_after_shutdown =
            provider.gets.load(Ordering::SeqCst) + fast_provider.gets.load(Ordering::SeqCst);
        assert_eq!(
            provider.gets.load(Ordering::SeqCst) + fast_provider.gets.load(Ordering::SeqCst),
            calls_after_shutdown,
            "cancellation allowed a new provider call"
        );
        assert!(calls_after_shutdown <= 2);
    }

    #[tokio::test]
    async fn three_provider_uneven_backlog_refills_with_an_unoccupied_provider() {
        let slow = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("request-fair-slow-1", RemotePinStatus::Queued),
        });
        let middle = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("request-fair-middle", RemotePinStatus::Queued),
        });
        let middle_second = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("request-fair-middle-2", RemotePinStatus::Queued),
        });
        let third = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("request-fair-third", RemotePinStatus::Queued),
        });
        let mut fixture = fixture([Script::BlockGet(slow.clone())]).await;
        let middle_provider = ScriptProvider::new([
            Script::BlockGet(middle.clone()),
            Script::BlockGet(middle_second.clone()),
        ]);
        let third_provider = ScriptProvider::new([Script::BlockGet(third.clone())]);
        PinningCoordinator::replace_provider_for_test(
            &mut fixture.coordinator,
            "fast",
            middle_provider.clone(),
        );
        PinningCoordinator::replace_provider_for_test(
            &mut fixture.coordinator,
            "third",
            third_provider.clone(),
        );
        PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
            settings.interval = std::time::Duration::from_millis(10);
            settings.worker_concurrency = 2;
            settings.shutdown_grace = std::time::Duration::from_millis(30);
        });
        PinningCoordinator::configure_provider_runtime_for_test(
            &mut fixture.coordinator,
            "noop",
            1,
            std::time::Duration::ZERO,
        );
        let due = Utc::now() - chrono::Duration::seconds(1);
        fixture.seed_poll("fair-slow-1", due).await;
        fixture
            .seed_poll("fair-slow-2", due + chrono::Duration::milliseconds(1))
            .await;
        fixture
            .seed_poll("fair-slow-3", due + chrono::Duration::milliseconds(2))
            .await;
        fixture
            .seed_poll_for(
                "fast",
                "fair-middle",
                due + chrono::Duration::milliseconds(3),
            )
            .await;
        fixture
            .seed_poll_for(
                "fast",
                "fair-middle-2",
                due + chrono::Duration::milliseconds(4),
            )
            .await;
        fixture
            .seed_poll_for(
                "third",
                "fair-third",
                due + chrono::Duration::milliseconds(5),
            )
            .await;

        let slow_entered = slow.entered.notified();
        let middle_entered = middle.entered.notified();
        let third_entered = third.entered.notified();
        let db = fixture.store.db().clone();
        let slow_provider = fixture.provider.clone();
        let coordinator = fixture.coordinator.clone();
        let handle = coordinator.start(fixture.store, CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), slow_entered)
            .await
            .expect("slow provider did not receive the first fair slot");
        tokio::time::timeout(std::time::Duration::from_secs(2), middle_entered)
            .await
            .expect("middle provider did not receive the second fair slot");
        assert_eq!(third_provider.gets.load(Ordering::SeqCst), 0);

        middle.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), third_entered)
            .await
            .expect("completed middle-provider work did not admit the third provider");
        assert_eq!(middle_provider.gets.load(Ordering::SeqCst), 1);
        let running = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .filter(pin_job::Column::State.eq("running"))
            .count(&db)
            .await
            .unwrap();
        assert!(running <= 2);
        assert_eq!(slow_provider.gets.load(Ordering::SeqCst), 1);
        assert_eq!(middle_provider.gets.load(Ordering::SeqCst), 1);
        assert_eq!(third_provider.gets.load(Ordering::SeqCst), 1);

        third.release.notify_one();
        handle.shutdown(std::time::Duration::from_millis(40)).await;
    }

    #[tokio::test]
    async fn recent_service_rotation_ignores_historical_claim_debt_at_capacity_one() {
        let mut fixture = fixture([]).await;
        PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
            settings.worker_concurrency = 1;
        });
        let due = Utc::now() - chrono::Duration::seconds(1);
        for round in 0..2 {
            fixture.seed_poll(&format!("history-a-{round}"), due).await;
            fixture
                .seed_poll_for("fast", &format!("history-b-{round}"), due)
                .await;
            fixture
                .seed_poll_for("removed", &format!("history-unknown-{round}"), due)
                .await;
        }

        let occupancy = super::ProviderOccupancy::default();
        {
            let mut state = occupancy
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.last_served_ticket.insert("noop".to_owned(), 100_000);
            state.next_ticket = 100_001;
        }
        let cancellation = CancellationToken::new();
        let mut served = Vec::new();
        for _ in 0..6 {
            let claimed = super::scan_and_claim(
                &fixture.coordinator,
                &fixture.store,
                &cancellation,
                &occupancy,
                Utc::now(),
                1,
            )
            .await
            .unwrap()
            .pop()
            .expect("one continuously due provider must be selected");
            served.push(claimed.model.provider.clone());
            let guard = occupancy.enter(claimed.model.provider.clone());
            jobs::complete_job(
                fixture.store.db(),
                &claimed.model.id,
                claimed.model.locked_until.unwrap(),
                Utc::now(),
            )
            .await
            .unwrap();
            drop(guard);
        }

        assert_eq!(
            served,
            vec!["fast", "removed", "noop", "fast", "removed", "noop"],
            "recent service must rotate every continuously due provider without repaying a cumulative historical gap"
        );
    }

    #[test]
    fn service_ticket_overflow_compacts_without_wrapping_or_losing_recency() {
        let occupancy = super::ProviderOccupancy::default();
        {
            let mut state = occupancy
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .last_served_ticket
                .insert("oldest".to_owned(), u64::MAX - 2);
            state
                .last_served_ticket
                .insert("newer".to_owned(), u64::MAX - 1);
            state.next_ticket = u64::MAX;
        }

        occupancy.record_selected(["new"]).unwrap();
        let state = occupancy
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(state.last_served_ticket.get("oldest"), Some(&1));
        assert_eq!(state.last_served_ticket.get("newer"), Some(&2));
        assert_eq!(state.last_served_ticket.get("new"), Some(&3));
        assert_eq!(state.next_ticket, 4);
    }

    #[tokio::test]
    async fn panicked_provider_task_releases_the_worker_slot_for_the_next_provider() {
        let mut fixture = fixture([Script::Submit(Ok(remote(
            "never-used",
            RemotePinStatus::Queued,
        )))])
        .await;
        let fast = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("request-after-panic", RemotePinStatus::Queued),
        });
        let fast_provider = ScriptProvider::new([Script::BlockGet(fast.clone())]);
        PinningCoordinator::replace_provider_for_test(
            &mut fixture.coordinator,
            "fast",
            fast_provider.clone(),
        );
        PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
            settings.interval = std::time::Duration::from_millis(10);
            settings.worker_concurrency = 1;
            settings.shutdown_grace = std::time::Duration::from_millis(30);
        });
        let due = Utc::now() - chrono::Duration::seconds(1);
        fixture.seed_poll("panic-first", due).await;
        fixture
            .seed_poll_for(
                "fast",
                "after-panic",
                due + chrono::Duration::milliseconds(1),
            )
            .await;

        let fast_entered = fast.entered.notified();
        let panicking_provider = fixture.provider.clone();
        let coordinator = fixture.coordinator.clone();
        let handle = coordinator.start(fixture.store, CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(10), fast_entered)
            .await
            .expect("JoinError leaked the worker slot and blocked the next provider");
        assert_eq!(panicking_provider.gets.load(Ordering::SeqCst), 1);
        assert_eq!(fast_provider.gets.load(Ordering::SeqCst), 1);
        fast.release.notify_one();
        handle.shutdown(std::time::Duration::from_millis(40)).await;
    }

    #[tokio::test]
    async fn unknown_provider_jobs_get_bounded_progress_without_squeezing_a_healthy_provider() {
        let mut fixture = fixture([]).await;
        let fast = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("request-known-fair", RemotePinStatus::Queued),
        });
        let fast_provider = ScriptProvider::new([Script::BlockGet(fast.clone())]);
        PinningCoordinator::replace_provider_for_test(
            &mut fixture.coordinator,
            "fast",
            fast_provider.clone(),
        );
        PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
            settings.interval = std::time::Duration::from_millis(10);
            settings.worker_concurrency = 2;
            settings.shutdown_grace = std::time::Duration::from_secs(1);
        });
        let due = Utc::now() - chrono::Duration::seconds(1);
        fixture.seed_poll_for("removed", "unknown-1", due).await;
        fixture
            .seed_poll_for(
                "removed",
                "unknown-2",
                due + chrono::Duration::milliseconds(1),
            )
            .await;
        fixture
            .seed_poll_for(
                "fast",
                "known-fair",
                due + chrono::Duration::milliseconds(2),
            )
            .await;

        let fast_entered = fast.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let handle = coordinator.start(fixture.store, CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), fast_entered)
            .await
            .expect("unknown-provider backlog squeezed out the configured healthy provider");
        let unknown = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let jobs = pin_job::Entity::find()
                    .filter(pin_job::Column::Provider.eq("removed"))
                    .order_by_asc(pin_job::Column::NextAttemptAt)
                    .all(&db)
                    .await
                    .unwrap();
                if jobs.len() == 2
                    && jobs.iter().any(|job| job.attempts == 1)
                    && jobs.iter().all(|job| job.state == "pending")
                {
                    break jobs;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unknown-provider work did not durably retry while healthy work was in flight");
        fast.release.notify_one();
        handle.shutdown(std::time::Duration::from_secs(1)).await;

        assert_eq!(unknown.len(), 2);
        assert!(unknown.iter().any(|job| job.attempts == 1));
        assert!(unknown.iter().all(|job| job.attempts <= 1));
        assert!(unknown.iter().all(|job| job.state == "pending"));
        assert_eq!(fast_provider.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn provider_permit_wait_heartbeats_exact_claim_and_cancels_without_http() {
        let blocker = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("unused-blocked", RemotePinStatus::Queued),
        });
        let mut fixture = fixture([Script::BlockGet(blocker.clone())]).await;
        PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
            settings.interval = std::time::Duration::from_secs(1);
            settings.worker_concurrency = 2;
            settings.claim_limit = 4;
            settings.lock_for = chrono::Duration::milliseconds(120);
            settings.shutdown_grace = std::time::Duration::from_millis(30);
        });
        PinningCoordinator::configure_provider_runtime_for_test(
            &mut fixture.coordinator,
            "noop",
            1,
            std::time::Duration::ZERO,
        );
        let due = Utc::now() - chrono::Duration::seconds(1);
        fixture.seed_poll("permit-wait-1", due).await;
        let waiting_job = fixture
            .seed_poll("permit-wait-2", due + chrono::Duration::milliseconds(1))
            .await;

        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let provider = fixture.provider.clone();
        let coordinator = fixture.coordinator.clone();
        let handle = coordinator.start(fixture.store, CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("first provider call did not block");
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;

        let waiting = pin_job::Entity::find_by_id(waiting_job)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(waiting.state, "running");
        assert!(
            waiting.locked_until.unwrap() > Utc::now(),
            "provider-permit waiter did not heartbeat its exact claim"
        );
        assert!(
            jobs::claim_due_jobs(&db, Utc::now(), chrono::Duration::milliseconds(120), 2,)
                .await
                .unwrap()
                .is_empty(),
            "a second claimant reclaimed a live provider-permit waiter"
        );
        handle.shutdown(std::time::Duration::from_millis(40)).await;
        assert_eq!(provider.gets.load(Ordering::SeqCst), 1);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(provider.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rate_wait_heartbeats_without_using_fast_provider_capacity_and_cancels_cleanly() {
        let mut fixture = fixture([]).await;
        let fast_provider = ScriptProvider::new([Script::Get(Ok(remote(
            "unused-fast-rate",
            RemotePinStatus::Queued,
        )))]);
        PinningCoordinator::replace_provider_for_test(
            &mut fixture.coordinator,
            "fast",
            fast_provider.clone(),
        );
        PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
            settings.interval = std::time::Duration::from_secs(1);
            settings.worker_concurrency = 2;
            settings.claim_limit = 4;
            settings.lock_for = chrono::Duration::milliseconds(120);
            settings.shutdown_grace = std::time::Duration::from_millis(30);
        });
        PinningCoordinator::configure_provider_runtime_for_test(
            &mut fixture.coordinator,
            "noop",
            1,
            std::time::Duration::from_secs(1),
        );
        *fixture
            .coordinator
            .provider_runtime("noop")
            .unwrap()
            .next_request_at
            .lock()
            .await = Some(tokio::time::Instant::now() + std::time::Duration::from_millis(700));
        let due = Utc::now() - chrono::Duration::seconds(1);
        let slow_job = fixture.seed_poll("rate-wait", due).await;
        fixture
            .seed_poll_for("fast", "rate-fast", due + chrono::Duration::milliseconds(1))
            .await;

        let db = fixture.store.db().clone();
        let slow_provider = fixture.provider.clone();
        let coordinator = fixture.coordinator.clone();
        let handle = coordinator.start(fixture.store, CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while fast_provider.gets.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fast provider was blocked by another provider's rate wait");
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;

        let waiting = pin_job::Entity::find_by_id(slow_job)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(waiting.state, "running");
        assert!(waiting.locked_until.unwrap() > Utc::now());
        assert!(
            jobs::claim_due_jobs(&db, Utc::now(), chrono::Duration::milliseconds(120), 2,)
                .await
                .unwrap()
                .is_empty()
        );
        handle.shutdown(std::time::Duration::from_millis(40)).await;
        assert_eq!(slow_provider.gets.load(Ordering::SeqCst), 0);
        assert_eq!(fast_provider.gets.load(Ordering::SeqCst), 1);
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert_eq!(slow_provider.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shutdown_cancelled_target_keeps_usage_until_adopted_request_is_unpinned() {
        let _paused_clock_test = SHUTDOWN_PAUSED_CLOCK_TEST_LOCK.lock().await;
        let blocker = Arc::new(SubmitBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(Some(Ok(remote(
                "accepted-cancelled-target",
                RemotePinStatus::Queued,
            )))),
        });
        let delete_blocker = Arc::new(UnpinBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(Some(Ok(()))),
        });
        let mut fixture = file_backed_fixture([
            Script::BlockSubmit(blocker.clone()),
            Script::Find(Ok(vec![remote(
                "accepted-cancelled-target",
                RemotePinStatus::Queued,
            )])),
            Script::BlockUnpin(delete_blocker.clone()),
        ])
        .await;
        PinningCoordinator::configure_worker_for_test(&mut fixture.coordinator, |settings| {
            settings.lock_for = chrono::Duration::seconds(15);
        });
        fixture.enqueue_submit().await;
        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let provider = fixture.provider.clone();
        let handle = coordinator.start(fixture.store, CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("worker did not enter blocked Submit");
        leases::cancel_lease(&db, "lease-1", Utc::now())
            .await
            .unwrap();
        let held = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((held.reserved_bytes, held.reserved_pins), (100, 1));
        tokio::time::pause();
        let shutdown = tokio::spawn(handle.shutdown(std::time::Duration::from_secs(5)));
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        shutdown.await.unwrap();
        tokio::time::resume();
        let held_after_shutdown = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                held_after_shutdown.reserved_bytes,
                held_after_shutdown.reserved_pins
            ),
            (100, 1),
            "shutdown must not release usage before durable recovery and Unpin"
        );

        let restart = Fixture {
            store: Store::new(db),
            coordinator,
            provider,
            _database_directory: None,
        };
        let blocked = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(restart.store.db())
            .await
            .unwrap()
            .unwrap();
        tokio::time::pause();
        tokio::time::advance(std::time::Duration::from_secs(31)).await;
        tokio::time::resume();
        let claimed = jobs::claim_due_jobs(
            restart.store.db(),
            blocked.locked_until.unwrap() + chrono::Duration::seconds(1),
            chrono::Duration::seconds(30),
            1,
        )
        .await;
        let claimed = claimed.unwrap().pop().unwrap();
        assert!(claimed.reclaimed);
        assert_eq!(claimed.model.submit_phase.as_deref(), Some("recovering"));
        super::execute_claimed_job(
            &restart.store,
            &restart.coordinator,
            &Arc::new(Semaphore::new(2)),
            claimed,
        )
        .await
        .unwrap();
        assert_eq!(restart.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(restart.provider.finds.load(Ordering::SeqCst), 1);
        let held_after_adopt = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(restart.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                held_after_adopt.reserved_bytes,
                held_after_adopt.reserved_pins
            ),
            (100, 1)
        );

        let mut unpin = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("unpin"))
            .filter(pin_job::Column::State.eq("pending"))
            .one(restart.store.db())
            .await
            .unwrap();
        if unpin.is_none() {
            restart.run_one_due().await;
            unpin = pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("unpin"))
                .filter(pin_job::Column::State.eq("pending"))
                .one(restart.store.db())
                .await
                .unwrap();
        }
        let unpin = unpin.expect("adopted request with no refs must converge to Unpin");
        let claimed = jobs::claim_due_jobs(
            restart.store.db(),
            unpin.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let entered = delete_blocker.entered.notified();
        let db = restart.store.db().clone();
        let coordinator = restart.coordinator.clone();
        let delete = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("Unpin did not enter provider DELETE");
        let held_before_delete = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(restart.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                held_before_delete.reserved_bytes,
                held_before_delete.reserved_pins
            ),
            (100, 1)
        );
        delete_blocker.release.notify_one();
        delete.await.unwrap().unwrap();
        assert_eq!(restart.provider.unpins.load(Ordering::SeqCst), 1);
        let released = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(restart.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((released.reserved_bytes, released.reserved_pins), (0, 0));
    }

    #[tokio::test]
    async fn exhausted_ordinary_poll_is_parked_without_a_ninth_get_or_due_loop() {
        let fixture = fixture([]).await;
        fixture.enqueue_poll("request-exhausted").await;
        fixture
            .store
            .db()
            .execute_unprepared("UPDATE pin_jobs SET attempts=8 WHERE operation='poll'")
            .await
            .unwrap();

        fixture.run_one_due().await;
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
        let parked = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parked.state, "done");
        assert_eq!(parked.attempts, 8);
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::State.eq("pending"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn exhausted_poll_from_renewed_generation_completes_without_failover() {
        let cid = "bafy-exhausted-renewed-generation";
        let lease_id = "lease-exhausted-renewed-generation";
        let target_id = "target-exhausted-renewed-generation";
        let (fixture, job) = exhausted_one_poll_fixture(
            cid,
            lease_id,
            target_id,
            "request-exhausted-renewed-generation",
        )
        .await;
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_leases SET source='manual' WHERE id='{lease_id}'"
            ))
            .await
            .unwrap();
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(claimed.model.id, job.id);

        let lease_before = pin_lease::Entity::find_by_id(lease_id.to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let renewal = leases::renew_manual_lease(
            fixture.store.db(),
            "object-1",
            lease_id,
            lease_before.expires_at + chrono::Duration::hours(1),
            Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(
            renewal,
            leases::ManualLeaseRenewalOutcome::Extended { generation: 2 }
        );

        let remotes_before = remote_pin::Entity::find()
            .all(fixture.store.db())
            .await
            .unwrap();
        let targets_before = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
            .all(fixture.store.db())
            .await
            .unwrap();
        let usage_before = pin_provider_usage::Entity::find()
            .all(fixture.store.db())
            .await
            .unwrap();
        let current_jobs_before = pin_job::Entity::find()
            .filter(pin_job::Column::Id.ne(&job.id))
            .all(fixture.store.db())
            .await
            .unwrap();
        let current_generation_jobs = current_jobs_before
            .iter()
            .filter(|job| job.expected_generation == Some(2))
            .collect::<Vec<_>>();
        assert_eq!(current_generation_jobs.len(), 1);
        assert_eq!(current_generation_jobs[0].operation, "poll");
        assert_eq!(
            current_generation_jobs[0].target_id.as_deref(),
            Some(target_id)
        );

        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            claimed,
        )
        .await
        .unwrap();

        assert_eq!(
            pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            2,
            "generation-1 exhausted Poll must not mutate the renewed generation-2 lease"
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .all(fixture.store.db())
                .await
                .unwrap(),
            targets_before,
            "stale exhausted Poll must not add or mutate lease targets"
        );
        assert_eq!(
            remote_pin::Entity::find()
                .all(fixture.store.db())
                .await
                .unwrap(),
            remotes_before,
            "stale exhausted Poll must not change remote epochs"
        );
        assert_eq!(
            pin_provider_usage::Entity::find()
                .all(fixture.store.db())
                .await
                .unwrap(),
            usage_before,
            "stale exhausted Poll must not reserve fallback quota"
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Id.ne(&job.id))
                .all(fixture.store.db())
                .await
                .unwrap(),
            current_jobs_before,
            "current-generation work must remain unchanged"
        );
        let completed = pin_job::Entity::find_by_id(job.id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, "done");
        assert_eq!(completed.expected_generation, Some(1));
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn exhausted_poll_renewed_after_scope_read_rolls_back_without_failover() {
        let cid = "bafy-exhausted-renew-race";
        let lease_id = "lease-exhausted-renew-race";
        let target_id = "target-exhausted-renew-race";
        let (fixture, job) =
            exhausted_one_poll_fixture(cid, lease_id, target_id, "request-exhausted-renew-race")
                .await;
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_leases SET source='manual' WHERE id='{lease_id}'"
            ))
            .await
            .unwrap();
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let gate = exhausted_gate(
            &job.id,
            super::ExhaustedCoordinationPhase::AfterScopeRead,
            super::ExhaustedCoordinationInterruption::Continue,
        );
        super::EXHAUSTED_COORDINATION_GATE
            .lock()
            .await
            .insert(job.id.clone(), gate.clone());
        let entered = gate.arrived.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("exhausted Poll did not pause after its scope read");

        let lease_before = pin_lease::Entity::find_by_id(lease_id.to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease_before.generation, 1);
        let renewal = leases::renew_manual_lease(
            fixture.store.db(),
            "object-1",
            lease_id,
            lease_before.expires_at + chrono::Duration::hours(1),
            Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(
            renewal,
            leases::ManualLeaseRenewalOutcome::Extended { generation: 2 }
        );
        let remotes_after_renewal = remote_pin::Entity::find()
            .all(fixture.store.db())
            .await
            .unwrap();
        let targets_after_renewal = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
            .all(fixture.store.db())
            .await
            .unwrap();
        let usage_after_renewal = pin_provider_usage::Entity::find()
            .all(fixture.store.db())
            .await
            .unwrap();
        let current_jobs_after_renewal = pin_job::Entity::find()
            .filter(pin_job::Column::Id.ne(&job.id))
            .all(fixture.store.db())
            .await
            .unwrap();

        gate.resume.notify_one();
        task.await.unwrap().unwrap();
        super::EXHAUSTED_COORDINATION_GATE
            .lock()
            .await
            .remove(&job.id);

        assert_eq!(
            pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            2
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .all(fixture.store.db())
                .await
                .unwrap(),
            targets_after_renewal
        );
        assert_eq!(
            remote_pin::Entity::find()
                .all(fixture.store.db())
                .await
                .unwrap(),
            remotes_after_renewal
        );
        assert_eq!(
            pin_provider_usage::Entity::find()
                .all(fixture.store.db())
                .await
                .unwrap(),
            usage_after_renewal
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Id.ne(&job.id))
                .all(fixture.store.db())
                .await
                .unwrap(),
            current_jobs_after_renewal,
            "the generation-2 Poll/Reconcile work must survive the stale coordination rollback"
        );
        assert_eq!(
            pin_job::Entity::find_by_id(job.id)
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "done"
        );
    }

    #[tokio::test]
    async fn exhausted_poll_rejects_mismatched_lease_and_incorrect_generation_scope() {
        let mismatch_cid = "bafy-exhausted-mismatched-lease";
        let mismatch_lease = "lease-exhausted-mismatched-lease";
        let mismatch_target = "target-exhausted-mismatched-lease";
        let (mismatch_fixture, mismatch_job) = exhausted_one_poll_fixture(
            mismatch_cid,
            mismatch_lease,
            mismatch_target,
            "request-exhausted-mismatched-lease",
        )
        .await;
        let now = Utc::now();
        mismatch_fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) \
                 VALUES ('lease-exhausted-other', 'object-1', 'other-scope', 'policy', 'one', 'full', \
                         '{}', '{}', '{}', 1, 'active'); \
                 UPDATE pin_jobs SET lease_id='lease-exhausted-other' WHERE id='{}'",
                now.to_rfc3339(),
                now.to_rfc3339(),
                (now + chrono::Duration::hours(1)).to_rfc3339(),
                mismatch_job.id,
            ))
            .await
            .unwrap();
        let mismatch_claimed = jobs::claim_due_jobs(
            mismatch_fixture.store.db(),
            mismatch_job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        super::execute_claimed_job(
            &mismatch_fixture.store,
            &mismatch_fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            mismatch_claimed,
        )
        .await
        .unwrap();
        assert_exhausted_one_scope_was_not_coordinated(
            &mismatch_fixture,
            mismatch_cid,
            mismatch_lease,
            1,
            &mismatch_job.id,
        )
        .await;

        let generation_cid = "bafy-exhausted-incorrect-generation";
        let generation_lease = "lease-exhausted-incorrect-generation";
        let generation_target = "target-exhausted-incorrect-generation";
        let (generation_fixture, generation_job) = exhausted_one_poll_fixture(
            generation_cid,
            generation_lease,
            generation_target,
            "request-exhausted-incorrect-generation",
        )
        .await;
        generation_fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET expected_generation=99 WHERE id='{}'",
                generation_job.id
            ))
            .await
            .unwrap();
        let generation_claimed = jobs::claim_due_jobs(
            generation_fixture.store.db(),
            generation_job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        super::execute_claimed_job(
            &generation_fixture.store,
            &generation_fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            generation_claimed,
        )
        .await
        .unwrap();
        assert_exhausted_one_scope_was_not_coordinated(
            &generation_fixture,
            generation_cid,
            generation_lease,
            1,
            &generation_job.id,
        )
        .await;
    }

    #[tokio::test]
    async fn exhausted_one_coordination_defensively_rejects_missing_generation_scope() {
        let cid = "bafy-exhausted-missing-generation";
        let lease_id = "lease-exhausted-missing-generation";
        let target_id = "target-exhausted-missing-generation";
        let (fixture, mut job) = exhausted_one_poll_fixture(
            cid,
            lease_id,
            target_id,
            "request-exhausted-missing-generation",
        )
        .await;
        job.expected_generation = None;

        super::coordinate_claimed_one_target(
            &fixture.store,
            &fixture.coordinator,
            &job,
            Utc::now(),
        )
        .await
        .unwrap();

        assert_eq!(
            pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            1
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .filter(pin_lease_target::Column::Provider.eq("fast"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0,
            "missing expected_generation must never reach failover"
        );
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), cid.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .epoch,
            1
        );
        assert!(
            remote_pin::Entity::find_by_id(("fast".to_owned(), cid.to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .is_none()
        );
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (200, 2));
    }

    #[tokio::test]
    async fn exhausted_one_poll_coordination_db_failure_remains_reclaimable_then_fails_over() {
        let cid = "bafy-exhausted-db-retry";
        let lease_id = "lease-exhausted-db-retry";
        let target_id = "target-exhausted-db-retry";
        let (fixture, job) =
            exhausted_one_poll_fixture(cid, lease_id, target_id, "request-exhausted-db").await;
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let original_lock = claimed.model.locked_until.unwrap();
        let gate = exhausted_gate(
            &job.id,
            super::ExhaustedCoordinationPhase::BeforeCoordination,
            super::ExhaustedCoordinationInterruption::Database,
        );
        super::EXHAUSTED_COORDINATION_GATE
            .lock()
            .await
            .insert(job.id.clone(), gate.clone());
        let entered = gate.arrived.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("exhausted coordination did not reach the injected database failure");
        gate.resume.notify_one();
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(
            error.to_string(),
            "database error: test transient exhausted one coordination failure"
        );
        super::EXHAUSTED_COORDINATION_GATE
            .lock()
            .await
            .remove(&job.id);

        let interrupted = pin_job::Entity::find_by_id(job.id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(interrupted.state, "running");
        assert!(interrupted.locked_until > Some(original_lock));
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .filter(pin_lease_target::Column::Provider.eq("fast"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );

        let reclaimed = expire_and_reclaim_exhausted_poll(&fixture, &job.id).await;
        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            reclaimed,
        )
        .await
        .unwrap();
        assert_exact_exhausted_one_failover(&fixture, cid, lease_id, &job.id).await;
    }

    #[tokio::test]
    async fn exhausted_one_poll_crash_after_claim_extension_reclaims_and_coordinates_once() {
        let cid = "bafy-exhausted-pre-coordinate";
        let lease_id = "lease-exhausted-pre-coordinate";
        let target_id = "target-exhausted-pre-coordinate";
        let (fixture, job) = exhausted_one_poll_fixture(
            cid,
            lease_id,
            target_id,
            "request-exhausted-pre-coordinate",
        )
        .await;
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let original_lock = claimed.model.locked_until.unwrap();
        let gate = exhausted_gate(
            &job.id,
            super::ExhaustedCoordinationPhase::BeforeCoordination,
            super::ExhaustedCoordinationInterruption::Crash,
        );
        super::EXHAUSTED_COORDINATION_GATE
            .lock()
            .await
            .insert(job.id.clone(), gate.clone());
        let entered = gate.arrived.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("exhausted Poll did not pause after claim extension");
        let paused = pin_job::Entity::find_by_id(job.id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        gate.resume.notify_one();
        task.await.unwrap().unwrap_err();
        super::EXHAUSTED_COORDINATION_GATE
            .lock()
            .await
            .remove(&job.id);
        assert_eq!(paused.state, "running");
        assert!(paused.locked_until > Some(original_lock));
        assert_eq!(
            pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            1
        );

        let reclaimed = expire_and_reclaim_exhausted_poll(&fixture, &job.id).await;
        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            reclaimed,
        )
        .await
        .unwrap();
        assert_exact_exhausted_one_failover(&fixture, cid, lease_id, &job.id).await;
    }

    #[tokio::test]
    async fn exhausted_one_poll_crash_after_coordination_reclaims_without_duplicate_mutation() {
        let cid = "bafy-exhausted-post-coordinate";
        let lease_id = "lease-exhausted-post-coordinate";
        let target_id = "target-exhausted-post-coordinate";
        let (fixture, job) = exhausted_one_poll_fixture(
            cid,
            lease_id,
            target_id,
            "request-exhausted-post-coordinate",
        )
        .await;
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let gate = exhausted_gate(
            &job.id,
            super::ExhaustedCoordinationPhase::AfterCoordination,
            super::ExhaustedCoordinationInterruption::Crash,
        );
        super::EXHAUSTED_COORDINATION_GATE
            .lock()
            .await
            .insert(job.id.clone(), gate.clone());
        let entered = gate.arrived.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("exhausted Poll did not pause after coordination commit");
        let paused = pin_job::Entity::find_by_id(job.id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            2
        );
        gate.resume.notify_one();
        task.await.unwrap().unwrap_err();
        super::EXHAUSTED_COORDINATION_GATE
            .lock()
            .await
            .remove(&job.id);
        assert_eq!(paused.state, "running");

        let reclaimed = expire_and_reclaim_exhausted_poll(&fixture, &job.id).await;
        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            reclaimed,
        )
        .await
        .unwrap();
        assert_exact_exhausted_one_failover(&fixture, cid, lease_id, &job.id).await;
    }

    #[tokio::test]
    async fn stale_exhausted_poll_claimant_cannot_coordinate_after_reclaim() {
        let cid = "bafy-exhausted-stale-owner";
        let lease_id = "lease-exhausted-stale-owner";
        let target_id = "target-exhausted-stale-owner";
        let (fixture, job) =
            exhausted_one_poll_fixture(cid, lease_id, target_id, "request-exhausted-stale-owner")
                .await;
        let stale = jobs::claim_due_jobs(
            fixture.store.db(),
            job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let current = expire_and_reclaim_exhausted_poll(&fixture, &job.id).await;

        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            stale,
        )
        .await
        .unwrap();
        assert_eq!(
            pin_lease::Entity::find_by_id(lease_id.to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .generation,
            1
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .filter(pin_lease_target::Column::Provider.eq("fast"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );

        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            current,
        )
        .await
        .unwrap();
        assert_exact_exhausted_one_failover(&fixture, cid, lease_id, &job.id).await;
    }

    #[tokio::test]
    async fn exhausted_one_poll_separates_job_claim_lifecycle_and_completion_transactions() {
        let _order_guard = leases::test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let cid = "bafy-exhausted-lock-order";
        let lease_id = "lease-exhausted-lock-order";
        let target_id = "target-exhausted-lock-order";
        let (fixture, job) =
            exhausted_one_poll_fixture(cid, lease_id, target_id, "request-exhausted-lock-order")
                .await;
        *leases::test_gates::LIFECYCLE_ORDER_RECORDER.lock().await =
            Some(leases::test_gates::LifecycleOrderRecorder {
                owner_ids: BTreeSet::new(),
                lease_ids: BTreeSet::from([lease_id.to_owned()]),
                target_ids: BTreeSet::from([target_id.to_owned()]),
                remote_pairs: BTreeSet::from([
                    ("fast".to_owned(), cid.to_owned()),
                    ("noop".to_owned(), cid.to_owned()),
                ]),
                record_desired_target_reads: false,
                events: Vec::new(),
            });
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            claimed,
        )
        .await
        .unwrap();
        let events = leases::test_gates::LIFECYCLE_ORDER_RECORDER
            .lock()
            .await
            .take()
            .unwrap()
            .events;
        let phases = events
            .split(|event| {
                *event == leases::test_gates::LifecycleOrderEvent::WorkerTransactionBoundary
            })
            .collect::<Vec<_>>();
        assert_eq!(
            phases.len(),
            3,
            "expected job→lifecycle→job phases: {events:?}"
        );
        assert_eq!(
            phases[0],
            [leases::test_gates::LifecycleOrderEvent::JobClaimLock(
                job.id.clone()
            )]
        );
        assert!(phases[1].iter().any(|event| matches!(
            event,
            leases::test_gates::LifecycleOrderEvent::LeaseLock(id) if id == lease_id
        )));
        assert!(phases[1].iter().any(|event| matches!(
            event,
            leases::test_gates::LifecycleOrderEvent::TargetLock(id) if id == target_id
        )));
        assert!(phases[1].iter().any(|event| matches!(
            event,
            leases::test_gates::LifecycleOrderEvent::RemoteLock(_, event_cid) if event_cid == cid
        )));
        assert!(!phases[1].iter().any(|event| matches!(
            event,
            leases::test_gates::LifecycleOrderEvent::JobClaimLock(_)
                | leases::test_gates::LifecycleOrderEvent::JobComplete(_)
        )));
        assert_eq!(
            phases[2],
            [leases::test_gates::LifecycleOrderEvent::JobComplete(
                job.id.clone()
            )]
        );
        assert_exact_exhausted_one_failover(&fixture, cid, lease_id, &job.id).await;
    }

    #[tokio::test]
    async fn blocked_old_poll_cannot_regress_terminal_failed_request() {
        let blocker = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("request-blocked", RemotePinStatus::Queued),
        });
        let fixture = fixture([Script::BlockGet(blocker.clone())]).await;
        fixture.enqueue_poll("request-blocked").await;
        let due = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .next_attempt_at;
        let claimed =
            jobs::claim_due_jobs(fixture.store.db(), due, chrono::Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("Poll did not enter provider GET");

        leases::apply_worker_remote_status(
            fixture.store.db(),
            leases::RemoteStatusUpdate {
                provider: "noop",
                cid: "bafy-worker",
                request_id: "request-blocked",
                origin: leases::RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Failed,
                error_class: Some("remote_failed"),
                error_text: Some("remote pin failed"),
                now: Utc::now(),
            },
        )
        .await
        .unwrap();
        blocker.release.notify_one();
        task.await.unwrap().unwrap();

        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.status, "failed");
        assert_eq!(remote.failure_attempts, 1);
        assert!(remote.next_retry_at.is_some());
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn quota_unpin_transient_retains_usage_and_does_not_wake_waiter() {
        let fixture = fixture_with_noop_limits(
            [Script::Unpin(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Transient,
                "temporary delete failure",
            )))],
            100,
            1,
        )
        .await;
        fixture
            .seed_quota_waiter("bafy-waiter", "waiter", 100)
            .await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='request-quota-delete', status='pinned' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='pinned' WHERE id='target-1'",
            )
            .await
            .unwrap();
        leases::cancel_lease(fixture.store.db(), "lease-1", Utc::now())
            .await
            .unwrap();

        fixture.run_one_due().await;

        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-waiter".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "quota_waiting"
        );
    }

    async fn assert_quota_confirmed_delete_wakes_waiter(
        result: Result<(), crate::pinning::provider::ProviderError>,
    ) {
        let fixture = fixture_with_noop_limits([Script::Unpin(result)], 100, 1).await;
        fixture
            .seed_quota_waiter("bafy-waiter", "waiter", 100)
            .await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='request-quota-delete', status='pinned' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='pinned' WHERE id='target-1'",
            )
            .await
            .unwrap();
        leases::cancel_lease(fixture.store.db(), "lease-1", Utc::now())
            .await
            .unwrap();

        fixture.run_one_due().await;

        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-waiter".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "waiting"
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("noop"))
                .filter(pin_job::Column::Cid.eq("bafy-waiter"))
                .filter(pin_job::Column::Operation.eq("submit"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn quota_confirmed_unpin_release_wakes_and_projects_waiter() {
        assert_quota_confirmed_delete_wakes_waiter(Ok(())).await;
    }

    #[tokio::test]
    async fn quota_not_found_unpin_release_wakes_and_projects_waiter() {
        assert_quota_confirmed_delete_wakes_waiter(Err(provider_error(
            crate::pinning::provider::ProviderErrorClass::NotFound,
            "already absent",
        )))
        .await;
    }

    #[tokio::test]
    async fn quota_confirmed_delete_waits_for_running_submit_before_release_and_wake() {
        let fixture = fixture_with_noop_limits([Script::Unpin(Ok(()))], 100, 1).await;
        fixture
            .seed_quota_waiter("bafy-delete-race-waiter", "delete-race-waiter", 100)
            .await;
        fixture.enqueue_submit().await;
        let locked_until = Utc::now() + chrono::Duration::minutes(1);
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET state='running', submit_phase='calling', \
                 locked_until='{}' WHERE operation='submit'; \
                 UPDATE remote_pins SET request_id='request-delete-race', status='pinned' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='pinned' WHERE id='target-1'",
                locked_until.to_rfc3339(),
            ))
            .await
            .unwrap();
        leases::cancel_lease(fixture.store.db(), "lease-1", Utc::now())
            .await
            .unwrap();

        fixture.run_one_due().await;

        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-delete-race-waiter".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "quota_waiting"
        );
        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.request_id, None);
        assert_eq!(remote.status, "reserved");
        assert_eq!(remote.epoch, 3);
        assert!(
            pin_job::Entity::find_by_id("reconcile:noop:bafy-worker:e3".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .is_some()
        );

        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE pin_jobs SET state='done', locked_until=NULL WHERE operation='submit'",
            )
            .await
            .unwrap();
        fixture
            .run_claim_at(locked_until + chrono::Duration::seconds(1))
            .await;
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-delete-race-waiter".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "waiting"
        );
    }

    #[tokio::test]
    async fn quota_provider_response_evicts_oldest_other_cid_once_per_retry_window() {
        let fixture = fixture_with_noop_limits(
            [
                Script::Submit(Err(provider_error(
                    crate::pinning::provider::ProviderErrorClass::Quota,
                    "provider quota reached",
                ))),
                Script::Find(Err(provider_error(
                    crate::pinning::provider::ProviderErrorClass::Quota,
                    "provider quota reached",
                ))),
                Script::Find(Err(provider_error(
                    crate::pinning::provider::ProviderErrorClass::Quota,
                    "provider quota reached",
                ))),
            ],
            300,
            3,
        )
        .await;
        let oldest = Utc::now() - chrono::Duration::hours(2);
        let older = Utc::now() - chrono::Duration::hours(1);
        let oldest_text = oldest.to_rfc3339();
        let older_text = older.to_rfc3339();
        let expires = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_provider_usage SET reserved_bytes=300, reserved_pins=3 \
                  WHERE provider='noop'; \
                  INSERT INTO pin_leases \
                  (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                   last_touched_at, expires_at, generation, state) \
                  VALUES ('lease-quota-old', 'object-1', 'quota-old', 'policy', 'all', 'full', \
                          '{oldest_text}', '{oldest_text}', '{expires}', 1, 'active'), \
                         ('lease-quota-older', 'object-1', 'quota-older', 'policy', 'all', 'full', \
                          '{older_text}', '{older_text}', '{expires}', 1, 'active'); \
                  INSERT INTO pin_lease_targets \
                  (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                  VALUES ('target-quota-old', 'lease-quota-old', 'bafy-quota-old', 100, 'noop', \
                          'pinned', '{oldest_text}', '{oldest_text}'), \
                         ('target-quota-older', 'lease-quota-older', 'bafy-quota-older', 100, \
                          'noop', 'pinned', '{older_text}', '{older_text}'); \
                  INSERT INTO remote_pins \
                  (provider, cid, cid_size, request_id, status, epoch, failure_attempts, last_touched_at) \
                  VALUES ('noop', 'bafy-quota-old', 100, 'request-quota-old', 'pinned', 1, 0, \
                          '{oldest_text}'), \
                         ('noop', 'bafy-quota-older', 100, 'request-quota-older', 'pinned', 1, 0, \
                          '{older_text}')"
            ))
            .await
            .unwrap();
        fixture.enqueue_submit().await;

        fixture.run_one_due().await;

        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-quota-old".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "evicted"
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "waiting",
            "the quota-error CID must not evict itself"
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("noop"))
                .filter(pin_job::Column::Cid.eq("bafy-quota-old"))
                .filter(pin_job::Column::Operation.eq("unpin"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            1
        );
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (300, 3));

        let current_submit = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("noop"))
            .filter(pin_job::Column::Cid.eq("bafy-worker"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET next_attempt_at='{}' \
                 WHERE operation='unpin' AND cid='bafy-quota-old'",
                (current_submit.next_attempt_at + chrono::Duration::hours(1)).to_rfc3339()
            ))
            .await
            .unwrap();
        fixture.run_claim_at(current_submit.next_attempt_at).await;
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-quota-older".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned",
            "repeated quota observations must not over-evict while one release is pending"
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("unpin"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            1
        );

        let release_at = Utc::now();
        let txn = fixture.store.db().begin().await.unwrap();
        assert_eq!(
            leases::complete_remote_delete(&txn, "noop", "bafy-quota-old", 2, release_at,)
                .await
                .unwrap(),
            leases::RemoteDeleteCompletion::Released
        );
        txn.commit().await.unwrap();
        super::wake_provider_waiters_after_release(
            &fixture.store,
            &fixture.coordinator,
            "noop",
            release_at,
        )
        .await
        .unwrap();
        super::coordinate_quota_waiters(
            &fixture.coordinator,
            &fixture.store,
            release_at + chrono::Duration::seconds(2),
        )
        .await
        .unwrap();
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE pin_jobs SET state='done', locked_until=NULL \
                 WHERE cid='bafy-quota-old'",
            )
            .await
            .unwrap();
        let current_submit = pin_job::Entity::find_by_id(current_submit.id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        fixture.run_claim_at(current_submit.next_attempt_at).await;

        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-quota-older".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "evicted",
            "a later retry window remains eligible after confirmed headroom is consumed"
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-quota-old".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "waiting",
            "the CID that reused released headroom must not be immediately re-evicted"
        );
    }

    #[tokio::test]
    async fn quota_stale_retry_window_cannot_evict_after_the_job_is_reclaimed() {
        let fixture = fixture_with_noop_limits([], 200, 2).await;
        fixture
            .seed_quota_candidate(
                "bafy-stale-window-candidate",
                "stale-window-candidate",
                Utc::now() - chrono::Duration::hours(1),
            )
            .await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE pin_provider_usage SET reserved_bytes=200, reserved_pins=2 \
                 WHERE provider='noop'",
            )
            .await
            .unwrap();
        fixture.enqueue_submit().await;
        let due = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .next_attempt_at;
        let old_claim =
            jobs::claim_due_jobs(fixture.store.db(), due, chrono::Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
        jobs::retry_submit_recovery(
            fixture.store.db(),
            &old_claim,
            Utc::now(),
            std::time::Duration::from_secs(1),
            "provider quota error",
        )
        .await
        .unwrap();
        let replacement_due = pin_job::Entity::find_by_id(old_claim.model.id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .next_attempt_at;
        let replacement_claim = jobs::claim_due_jobs(
            fixture.store.db(),
            replacement_due,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .expect("the next retry window must own a different exact claim");
        assert_ne!(
            old_claim.model.locked_until,
            replacement_claim.model.locked_until
        );

        super::retry_submit_recovery(
            &fixture.store,
            &fixture.coordinator,
            &old_claim,
            &provider_error(
                crate::pinning::provider::ProviderErrorClass::Quota,
                "stale provider quota response",
            ),
        )
        .await
        .unwrap();

        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-stale-window-candidate".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned",
            "a quota response from an old exact claim must not evict in a newer retry window"
        );
    }

    #[tokio::test]
    async fn quota_from_failed_request_delete_never_evicts_capacity() {
        let fixture = fixture_with_noop_limits(
            [Script::Unpin(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Quota,
                "provider quota reached during delete",
            )))],
            200,
            2,
        )
        .await;
        fixture
            .seed_quota_candidate(
                "bafy-delete-quota-candidate",
                "delete-quota-candidate",
                Utc::now() - chrono::Duration::hours(1),
            )
            .await;
        let due = Utc::now() - chrono::Duration::seconds(1);
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_provider_usage SET reserved_bytes=200, reserved_pins=2 \
                 WHERE provider='noop'; \
                 UPDATE pin_lease_targets SET state='degraded' WHERE id='target-1'; \
                 UPDATE remote_pins SET request_id='failed-delete-request', status='failed', \
                    failure_attempts=1, last_failed_request_id='failed-delete-request', \
                    next_retry_at='{}' \
                 WHERE provider='noop' AND cid='bafy-worker'",
                due.to_rfc3339()
            ))
            .await
            .unwrap();
        jobs::enqueue_job(
            fixture.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 1, due),
        )
        .await
        .unwrap();

        fixture.run_one_due().await;

        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 1);
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-delete-quota-candidate".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned",
            "DELETE intent cannot consume capacity and must not trigger quota eviction"
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Cid.eq("bafy-delete-quota-candidate"))
                .filter(pin_job::Column::Operation.eq("unpin"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn stale_blocked_submit_response_cannot_resurrect_released_remote_or_overcommit() {
        let blocker = Arc::new(SubmitBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(Some(Ok(remote(
                "claim-race-request",
                RemotePinStatus::Queued,
            )))),
        });
        let fixture = fixture_with_noop_limits(
            [
                Script::BlockSubmit(blocker.clone()),
                Script::Find(Ok(vec![remote(
                    "claim-race-request",
                    RemotePinStatus::Queued,
                )])),
                Script::Unpin(Ok(())),
            ],
            100,
            1,
        )
        .await;
        fixture
            .seed_quota_waiter("bafy-claim-race-waiter", "claim-race-waiter", 100)
            .await;
        fixture.enqueue_submit().await;
        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let claim_a = jobs::claim_due_jobs(
            fixture.store.db(),
            submit.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let worker_a = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claim_a,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("worker A did not enter the blocked provider Submit");

        let reclaim_at = Utc::now();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET locked_until='{}' WHERE id='{}'",
                (reclaim_at - chrono::Duration::seconds(1)).to_rfc3339(),
                submit.id
            ))
            .await
            .unwrap();
        leases::cancel_lease(fixture.store.db(), "lease-1", reclaim_at)
            .await
            .unwrap();
        let claim_b = jobs::claim_due_jobs(
            fixture.store.db(),
            reclaim_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|claimed| claimed.model.id == submit.id)
        .expect("worker B must reclaim the expired ambiguous Submit");
        assert!(claim_b.reclaimed);
        super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            claim_b,
        )
        .await
        .unwrap();

        for _ in 0..4 {
            let remote =
                remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                    .one(fixture.store.db())
                    .await
                    .unwrap()
                    .unwrap();
            if remote.status == "absent" {
                break;
            }
            let pending = pin_job::Entity::find()
                .filter(pin_job::Column::State.eq("pending"))
                .filter(pin_job::Column::Operation.is_in(["reconcile", "unpin"]))
                .order_by_asc(pin_job::Column::NextAttemptAt)
                .one(fixture.store.db())
                .await
                .unwrap()
                .expect("reclaimed adoption must converge through remote cleanup");
            fixture.run_claim_at(pending.next_attempt_at).await;
        }
        assert_eq!(
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .status,
            "absent"
        );

        blocker.release.notify_one();
        let _stale_result = worker_a.await.unwrap();

        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (remote.status.as_str(), remote.request_id.as_deref()),
            ("absent", None),
            "worker A's stale response must not revive a released remote"
        );
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-claim-race-waiter".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "waiting"
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::TargetId.eq("target-1"))
                .filter(pin_job::Column::State.ne("done"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            0,
            "the stale response must not project target-scoped work"
        );
    }

    #[tokio::test]
    async fn observation_transaction_fence_rejects_reclaimed_claim_before_lifecycle_write() {
        let fixture = fixture([]).await;
        fixture.enqueue_submit().await;
        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let old_claim = jobs::claim_due_jobs(
            fixture.store.db(),
            submit.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let reclaim_at = Utc::now();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET locked_until='{}' WHERE id='{}'; \
                 UPDATE remote_pins SET status='absent', request_id=NULL WHERE provider='noop' \
                    AND cid='bafy-worker'",
                (reclaim_at - chrono::Duration::seconds(1)).to_rfc3339(),
                submit.id,
            ))
            .await
            .unwrap();
        let replacement = jobs::claim_due_jobs(
            fixture.store.db(),
            reclaim_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_ne!(old_claim.model.locked_until, replacement.model.locked_until);

        let result = super::persist_observation_status_phase(
            &fixture.store,
            &old_claim,
            super::RemoteStatusUpdate {
                provider: "noop",
                cid: "bafy-worker",
                request_id: "stale-observation",
                origin: super::RemoteStatusOrigin::Adopt,
                status: RemotePinStatus::Queued,
                error_class: None,
                error_text: None,
                now: Utc::now(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            result,
            super::PersistObservationResult::StaleClaim
        ));
        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (remote.status.as_str(), remote.request_id),
            ("absent", None)
        );
    }

    #[tokio::test]
    async fn quota_no_request_zero_match_recovery_marks_submit_safe_then_releases_once() {
        let fixture = fixture([Script::Find(Ok(Vec::new()))]).await;
        fixture.enqueue_submit().await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE pin_leases SET state='cancelled' WHERE id='lease-1'; \
                 UPDATE pin_lease_targets SET state='released' WHERE id='target-1'; \
                 UPDATE pin_jobs SET submit_phase='recovering' WHERE operation='submit'",
            )
            .await
            .unwrap();

        fixture.run_one_due().await;
        assert_eq!(fixture.provider.finds.load(Ordering::SeqCst), 1);
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("submit"))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "done"
        );
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));

        fixture.run_one_due().await;
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (0, 0));
        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.status, "absent");

        let txn = fixture.store.db().begin().await.unwrap();
        assert_eq!(
            leases::complete_no_request_remote_absence(
                &txn,
                "noop",
                "bafy-worker",
                remote.epoch,
                Utc::now(),
            )
            .await
            .unwrap(),
            leases::NoRequestRemoteCompletion::Stale
        );
        txn.commit().await.unwrap();
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (0, 0));
    }

    #[tokio::test]
    async fn not_found_delete_with_new_shared_target_compensates_without_releasing_quota() {
        let blocker = Arc::new(UnpinBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(Some(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::NotFound,
                "not found",
            )))),
        });
        let fixture = fixture([Script::BlockUnpin(blocker.clone())]).await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='request-delete', status='pinned' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='pinned' WHERE id='target-1'",
            )
            .await
            .unwrap();
        leases::cancel_lease(fixture.store.db(), "lease-1", Utc::now())
            .await
            .unwrap();
        let unpin = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("unpin"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            unpin.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("Unpin did not enter provider DELETE");

        let now = Utc::now();
        let created = now.to_rfc3339();
        let expires = (now + chrono::Duration::hours(1)).to_rfc3339();
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "INSERT INTO pin_leases \
                 (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
                  last_touched_at, expires_at, generation, state) \
                 VALUES ('lease-during-delete', 'object-1', 'copy', 'policy', 'all', 'full', \
                         '{created}', '{created}', '{expires}', 1, 'active'); \
                 INSERT INTO pin_lease_targets \
                 (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
                 VALUES ('target-during-delete', 'lease-during-delete', 'bafy-worker', 100, \
                         'noop', 'waiting', '{created}', '{created}')"
            ))
            .await
            .unwrap();
        blocker.release.notify_one();
        task.await.unwrap().unwrap();

        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 1);
        assert_eq!(remote.status, "reserved");
        assert_eq!(remote.request_id, None);
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .filter(pin_job::Column::TargetId.eq("target-during-delete"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("compensation must force a replacement Submit");
        assert_eq!(submit.state, "pending");
    }

    async fn blocked_delete_with_expired_manual_renewal_converges(
        delete_result: Result<(), ProviderError>,
    ) {
        let blocker = Arc::new(UnpinBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(Some(delete_result)),
        });
        let fixture = fixture([
            Script::BlockUnpin(blocker.clone()),
            Script::Submit(Ok(remote(
                "renewed-manual-request",
                RemotePinStatus::Queued,
            ))),
            Script::Get(Ok(remote(
                "renewed-manual-request",
                RemotePinStatus::Pinned,
            ))),
        ])
        .await;
        let expiry = Utc::now() - chrono::Duration::seconds(1);
        fixture
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_leases SET source='manual', expires_at='{}' WHERE id='lease-1'; \
                 UPDATE remote_pins SET request_id='manual-delete-request', status='pinned' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='pinned' WHERE id='target-1'",
                expiry.to_rfc3339()
            ))
            .await
            .unwrap();
        let expired = leases::expire_due_leases(fixture.store.db(), Utc::now())
            .await
            .unwrap();
        assert_eq!(expired, vec!["lease-1".to_owned()]);
        let expired_lease = pin_lease::Entity::find_by_id("lease-1".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired_lease.state, "expired");
        assert!(expired_lease.generation > 1);
        let old_epoch =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .epoch;
        let unpin = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("unpin"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .expect("expiry must enqueue Unpin");
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            unpin.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let entered = blocker.entered.notified();
        let db = fixture.store.db().clone();
        let coordinator = fixture.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("expired manual Unpin did not enter provider DELETE");

        let renewal = leases::renew_manual_lease(
            fixture.store.db(),
            "object-1",
            "lease-1",
            Utc::now() + chrono::Duration::hours(2),
            Utc::now(),
        )
        .await
        .unwrap();
        assert!(matches!(
            renewal,
            leases::ManualLeaseRenewalOutcome::Reactivated { .. }
        ));
        blocker.release.notify_one();
        task.await.unwrap().unwrap();

        let renewed_lease = pin_lease::Entity::find_by_id("lease-1".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let compensated =
            remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(renewed_lease.state, "active");
        assert!(renewed_lease.generation > expired_lease.generation);
        assert!(compensated.epoch > old_epoch);
        assert_eq!(compensated.status, "reserved");
        assert_eq!(compensated.request_id, None);
        let usage = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));

        fixture.run_one_due().await;
        fixture.run_one_due().await;
        fixture.run_one_due().await;
        let pinned = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pinned.status, "pinned");
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-1".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned"
        );
        assert_eq!(fixture.provider.submits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.gets.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn blocked_delete_success_with_expired_manual_renewal_retains_quota_and_repins() {
        blocked_delete_with_expired_manual_renewal_converges(Ok(())).await;
    }

    #[tokio::test]
    async fn blocked_delete_not_found_with_expired_manual_renewal_retains_quota_and_repins() {
        blocked_delete_with_expired_manual_renewal_converges(Err(provider_error(
            crate::pinning::provider::ProviderErrorClass::NotFound,
            "raw not-found response",
        )))
        .await;
    }

    #[tokio::test]
    async fn unchanged_epoch_no_refs_releases_usage_once_and_reactivated_unpin_has_no_side_effect()
    {
        let fixture = fixture([Script::Unpin(Ok(()))]).await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='release-once', status='pinned' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='pinned' WHERE id='target-1'",
            )
            .await
            .unwrap();
        leases::cancel_lease(fixture.store.db(), "lease-1", Utc::now())
            .await
            .unwrap();
        fixture.run_one_due().await;
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 1);
        let released = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((released.reserved_bytes, released.reserved_pins), (0, 0));
        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let jobs::NewPinJob::Remote(duplicate) =
            jobs::unpin_job("noop", "bafy-worker", remote.epoch, Utc::now())
        else {
            unreachable!()
        };
        jobs::ensure_or_reactivate_unpin_job(fixture.store.db(), duplicate, Utc::now())
            .await
            .unwrap();
        fixture.run_one_due().await;
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 1);
        let still_released = pin_provider_usage::Entity::find_by_id("noop".to_owned())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (still_released.reserved_bytes, still_released.reserved_pins),
            (0, 0)
        );
    }

    #[tokio::test]
    async fn duplicate_unpin_enqueue_is_one_worker_delete() {
        let fixture = fixture([Script::Unpin(Ok(()))]).await;
        fixture
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='duplicate-delete', status='pinned' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='pinned' WHERE id='target-1'",
            )
            .await
            .unwrap();
        leases::cancel_lease(fixture.store.db(), "lease-1", Utc::now())
            .await
            .unwrap();
        let remote = remote_pin::Entity::find_by_id(("noop".to_owned(), "bafy-worker".to_owned()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        for _ in 0..2 {
            jobs::enqueue_job(
                fixture.store.db(),
                jobs::unpin_job("noop", "bafy-worker", remote.epoch, Utc::now()),
            )
            .await
            .unwrap();
        }
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("unpin"))
                .count(fixture.store.db())
                .await
                .unwrap(),
            1
        );
        fixture.run_one_due().await;
        assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 1);
        assert_eq!(
            pin_provider_usage::Entity::find_by_id("noop".to_owned())
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .reserved_pins,
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn structured_transitions_report_durable_outcomes_and_redact_provider_material() {
        const ISOLATED_TRACE_ENV: &str = "IPFS_S3_ISOLATED_WORKER_TRACE_TEST";
        if std::env::var_os(ISOLATED_TRACE_ENV).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "pinning::worker::tests::structured_transitions_report_durable_outcomes_and_redact_provider_material",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(ISOLATED_TRACE_ENV, "1")
                .output()
                .expect("failed to start isolated tracing test process");
            assert!(
                output.status.success(),
                "isolated tracing test failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(capture.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _default_guard = tracing::dispatcher::set_default(&dispatch);
        tracing::callsite::rebuild_interest_cache();

        let submit = fixture([
            Script::Submit(Ok(remote("trace-request", RemotePinStatus::Queued))),
            Script::Get(Err(provider_error(
                crate::pinning::provider::ProviderErrorClass::Transient,
                "Bearer trace-secret raw-provider-body",
            ))),
        ])
        .await;
        submit.enqueue_submit().await;
        submit.run_one_due().await;
        submit.run_one_due().await;

        let future_reconcile = fixture([]).await;
        let future_due = Utc::now() + chrono::Duration::minutes(2);
        future_reconcile
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE remote_pins SET epoch=31, request_id='future-failed', status='failed', \
                 failure_attempts=1, last_failed_request_id='future-failed', \
                 next_retry_at='{}' WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='degraded' WHERE id='target-1'",
                future_due.to_rfc3339()
            ))
            .await
            .unwrap();
        jobs::enqueue_job(
            future_reconcile.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 31, Utc::now()),
        )
        .await
        .unwrap();
        future_reconcile.run_one_due().await;

        let no_request_wait = fixture([]).await;
        no_request_wait.enqueue_submit().await;
        let future_lock = Utc::now() + chrono::Duration::minutes(5);
        no_request_wait
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE pin_jobs SET state='running', submit_phase='recovering', \
                 locked_until='{}' WHERE operation='submit'; \
                 UPDATE pin_leases SET state='cancelled', generation=2 WHERE id='lease-1'; \
                 UPDATE pin_lease_targets SET state='released' WHERE id='target-1'; \
                 UPDATE remote_pins SET epoch=41 WHERE provider='noop' AND cid='bafy-worker'",
                future_lock.to_rfc3339()
            ))
            .await
            .unwrap();
        jobs::enqueue_job(
            no_request_wait.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 41, Utc::now()),
        )
        .await
        .unwrap();
        no_request_wait.run_one_due().await;

        let stale_blocker = Arc::new(GetBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: remote("trace-stale-old", RemotePinStatus::Pinning),
        });
        let stale = fixture([Script::BlockGet(stale_blocker.clone())]).await;
        stale.enqueue_poll("trace-stale-old").await;
        let due = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(stale.store.db())
            .await
            .unwrap()
            .unwrap()
            .next_attempt_at;
        let claimed = jobs::claim_due_jobs(stale.store.db(), due, chrono::Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let entered = stale_blocker.entered.notified();
        let db = stale.store.db().clone();
        let coordinator = stale.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("trace stale Poll did not enter provider GET");
        stale
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET request_id='trace-stale-current' \
                 WHERE provider='noop' AND cid='bafy-worker'",
            )
            .await
            .unwrap();
        stale_blocker.release.notify_one();
        task.await.unwrap().unwrap();

        let stale_resubmit_blocker = Arc::new(UnpinBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(Some(Ok(()))),
        });
        let stale_resubmit = fixture([Script::BlockUnpin(stale_resubmit_blocker.clone())]).await;
        stale_resubmit
            .store
            .db()
            .execute_unprepared(&format!(
                "UPDATE remote_pins SET epoch=51, request_id='trace-stale-resubmit', \
                 status='failed', failure_attempts=1, \
                 last_failed_request_id='trace-stale-resubmit', next_retry_at='{}' \
                 WHERE provider='noop' AND cid='bafy-worker'; \
                 UPDATE pin_lease_targets SET state='degraded' WHERE id='target-1'",
                (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339()
            ))
            .await
            .unwrap();
        jobs::enqueue_job(
            stale_resubmit.store.db(),
            jobs::reconcile_job("noop", "bafy-worker", 51, Utc::now()),
        )
        .await
        .unwrap();
        let claimed = jobs::claim_due_jobs(
            stale_resubmit.store.db(),
            Utc::now(),
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let entered = stale_resubmit_blocker.entered.notified();
        let db = stale_resubmit.store.db().clone();
        let coordinator = stale_resubmit.coordinator.clone();
        let task = tokio::spawn(async move {
            super::execute_claimed_job(
                &Store::new(db),
                &coordinator,
                &Arc::new(Semaphore::new(2)),
                claimed,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered)
            .await
            .expect("trace failed request DELETE did not enter provider");
        stale_resubmit
            .store
            .db()
            .execute_unprepared(
                "UPDATE remote_pins SET epoch=52 \
                 WHERE provider='noop' AND cid='bafy-worker' \
                   AND request_id='trace-stale-resubmit'",
            )
            .await
            .unwrap();
        stale_resubmit_blocker.release.notify_one();
        task.await.unwrap().unwrap();

        let output = capture.text();
        assert!(
            output.contains("old_state=\"pending\" new_state=\"running\""),
            "{output}"
        );
        assert!(
            output.contains("old_state=\"ready\" new_state=\"calling\""),
            "{output}"
        );
        assert!(
            output.contains("old_state=\"reserved\" new_state=\"queued\""),
            "{output}"
        );
        assert!(output.contains("new_state=\"reconcile_wait\""), "{output}");
        assert!(
            output.contains("new_state=\"stale_current_reconcile\""),
            "{output}"
        );
        for field in [
            "provider=noop",
            "cid=bafy-worker",
            "object_id=Some(\"object-1\")",
            "lease_id=Some(\"lease-1\")",
            "target_id=Some(\"target-1\")",
            "job_id=submit:noop:bafy-worker:target-1:g1",
            "remote_request_id=Some(\"trace-request\")",
        ] {
            assert!(
                output.contains(field),
                "missing trace field `{field}`:\n{output}"
            );
        }
        assert!(output.contains("job_id=reconcile:noop:bafy-worker:e31"));
        assert!(output.contains("remote_request_id=Some(\"future-failed\")"));
        assert!(output.contains("remote_request_id=Some(\"trace-stale-old\")"));
        let future_wait = output
            .lines()
            .find(|line| {
                line.contains("job_id=reconcile:noop:bafy-worker:e31")
                    && line.contains("new_state=\"reconcile_wait\"")
            })
            .expect("future failed Reconcile must emit its durable wait transition");
        assert!(future_wait.contains("old_state=\"running\""));
        let no_request_wait = output
            .lines()
            .find(|line| {
                line.contains("job_id=reconcile:noop:bafy-worker:e41")
                    && line.contains("new_state=\"reconcile_wait\"")
            })
            .expect("no-request ambiguity must emit Reconcile wait, not absence");
        assert!(no_request_wait.contains("old_state=\"running\""));
        let stale_transition = output
            .lines()
            .find(|line| {
                line.contains("remote_request_id=Some(\"trace-stale-old\")")
                    && line.contains("new_state=\"stale_current_reconcile\"")
            })
            .expect("stale provider observation must emit current-Reconcile handoff");
        assert!(stale_transition.contains("old_state=\"running\""));
        let retry_transition = output
            .lines()
            .find(|line| {
                line.contains("job_id=poll:noop:bafy-worker:target-1:g1:")
                    && line.contains("new_state=\"retry_scheduled\"")
            })
            .expect("Poll retry must emit its durable retry transition");
        assert!(retry_transition.contains("remote_request_id=Some(\"trace-request\")"));
        let stale_resubmit_transition = output
            .lines()
            .find(|line| {
                line.contains("job_id=reconcile:noop:bafy-worker:e51")
                    && line.contains("new_state=\"stale_current_reconcile\"")
            })
            .expect("stale failed resubmit must not be reported as ready");
        assert!(
            stale_resubmit_transition.contains("remote_request_id=Some(\"trace-stale-resubmit\")")
        );
        assert!(!output.lines().any(|line| {
            line.contains("job_id=reconcile:noop:bafy-worker:e51")
                && line.contains("new_state=\"resubmit_ready\"")
        }));
        assert!(!output.contains("old_state=\"queued\" new_state=\"pinning\""));
        for secret in ["trace-secret", "raw-provider-body", "Bearer"] {
            assert!(
                !output.contains(secret),
                "provider material leaked into tracing: {output}"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn status_projection_transition_survives_follow_up_coordination_failure() {
        const ISOLATED_TRACE_ENV: &str = "IPFS_S3_ISOLATED_STATUS_PHASE_TRACE_TEST";
        if std::env::var_os(ISOLATED_TRACE_ENV).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "pinning::worker::tests::status_projection_transition_survives_follow_up_coordination_failure",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(ISOLATED_TRACE_ENV, "1")
                .output()
                .expect("failed to start isolated status-phase tracing test process");
            assert!(
                output.status.success(),
                "isolated status-phase tracing test failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(capture.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _default_guard = tracing::dispatcher::set_default(&dispatch);
        tracing::callsite::rebuild_interest_cache();

        let mut failed = remote("trace-status-failed", RemotePinStatus::Failed);
        failed.failure_reason = Some("Bearer trace-status-secret raw-status-body".to_owned());
        let mut fixture = fixture([Script::Get(Ok(failed))]).await;
        PinningCoordinator::configure_policy_providers_for_test(
            &mut fixture.coordinator,
            "policy",
            &["noop", "fast"],
        );
        fixture
            .store
            .db()
            .execute_unprepared("UPDATE pin_leases SET provider_mode='one' WHERE id='lease-1'")
            .await
            .unwrap();
        fixture.enqueue_poll("trace-status-failed").await;
        let job = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let gate = Arc::new(super::ObservationAfterStatusCommitGate {
            job_id: job.id.clone(),
            request_id: "trace-status-failed".to_owned(),
            fail_once: std::sync::atomic::AtomicBool::new(true),
            arrived: Notify::new(),
        });
        let gate_key = (gate.job_id.clone(), gate.request_id.clone());
        super::OBSERVATION_AFTER_STATUS_COMMIT
            .lock()
            .await
            .insert(gate_key.clone(), gate);
        let claimed = jobs::claim_due_jobs(
            fixture.store.db(),
            job.next_attempt_at,
            chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let error = super::execute_claimed_job(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            claimed,
        )
        .await
        .unwrap_err();
        super::OBSERVATION_AFTER_STATUS_COMMIT
            .lock()
            .await
            .remove(&gate_key);
        assert_eq!(
            error.to_string(),
            "database error: test coordination failure after durable pin status projection"
        );

        let output = capture.text();
        let status_events = output
            .lines()
            .filter(|line| {
                line.contains(&format!("job_id={}", job.id))
                    && line.contains("phase=\"status_projection\"")
                    && line.contains("outcome=\"committed\"")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            status_events.len(),
            1,
            "durable status transition must have one audit meaning: {output}"
        );
        let status = status_events[0];
        assert!(status.contains("old_state=\"queued\""), "{status}");
        assert!(status.contains("new_state=\"failed\""), "{status}");
        assert!(
            status.contains("remote_request_id=Some(\"trace-status-failed\")"),
            "{status}"
        );
        let follow_up_events = output
            .lines()
            .filter(|line| {
                line.contains(&format!("job_id={}", job.id))
                    && line.contains("phase=\"one_coordination\"")
                    && line.contains("outcome=\"failed\"")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            follow_up_events.len(),
            1,
            "follow-up failure must have one separate audit meaning: {output}"
        );
        let follow_up = follow_up_events[0];
        assert!(follow_up.contains("error_class=\"database\""));
        for secret in ["trace-status-secret", "raw-status-body", "Bearer"] {
            assert!(
                !output.contains(secret),
                "provider material leaked into phase tracing: {output}"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_task_failures_are_logged_with_their_job_identity() {
        const ISOLATED_TRACE_ENV: &str = "IPFS_S3_ISOLATED_WORKER_FAILURE_TRACE_TEST";
        if std::env::var_os(ISOLATED_TRACE_ENV).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "pinning::worker::tests::worker_task_failures_are_logged_with_their_job_identity",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(ISOLATED_TRACE_ENV, "1")
                .output()
                .expect("failed to start isolated tracing test process");
            assert!(
                output.status.success(),
                "isolated tracing test failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(capture.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _default_guard = tracing::dispatcher::set_default(&dispatch);
        tracing::callsite::rebuild_interest_cache();

        // The blocker proves the spawned task actually reached the provider.
        // Its release then lets the blocked Submit panic, which surfaces to
        // `run_worker` as a `JoinError`.
        let blocker = Arc::new(SubmitBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(None),
        });
        let fixture = fixture([Script::BlockSubmit(blocker.clone())]).await;
        fixture.enqueue_submit().await;
        let job = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();

        let entered = blocker.entered.notified();
        let handle = fixture
            .coordinator
            .clone()
            .start(fixture.store.clone(), CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(5), entered)
            .await
            .expect("worker did not enter Submit");

        // `result` is None, so the blocked Submit panics on release.
        blocker.release.notify_one();
        handle.shutdown(std::time::Duration::from_secs(5)).await;

        let text = capture.text();
        let failure_line = text
            .lines()
            .find(|line| line.contains("pinning worker task failed"))
            .unwrap_or_else(|| panic!("expected a worker task-failure log line in:\n{text}"));

        for field in [
            format!("job_id={}", job.id),
            "provider=noop".to_owned(),
            "cid=bafy-worker".to_owned(),
            "lease_id=Some(\"lease-1\")".to_owned(),
            "target_id=Some(\"target-1\")".to_owned(),
        ] {
            assert!(
                failure_line.contains(&field),
                "worker task-failure log must carry `{field}`: {failure_line}"
            );
        }
        assert!(
            !failure_line.contains("no recorded job context"),
            "the failing task must be attributable to its claimed job: {failure_line}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_tick_join_keeps_a_later_panic_bound_to_its_own_job() {
        const ISOLATED_TRACE_ENV: &str = "IPFS_S3_ISOLATED_WORKER_TICK_FAILURE_TRACE_TEST";
        if std::env::var_os(ISOLATED_TRACE_ENV).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "pinning::worker::tests::worker_tick_join_keeps_a_later_panic_bound_to_its_own_job",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(ISOLATED_TRACE_ENV, "1")
                .output()
                .expect("failed to start isolated tracing test process");
            assert!(
                output.status.success(),
                "isolated tracing test failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(capture.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _default_guard = tracing::dispatcher::set_default(&dispatch);
        tracing::callsite::rebuild_interest_cache();

        let first_submit = Arc::new(SubmitBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(None),
        });
        let panic_get = Arc::new(Notify::new());
        let fixture = fixture([
            Script::BlockSubmit(first_submit.clone()),
            Script::PanicGet(panic_get.clone()),
        ])
        .await;
        fixture.enqueue_submit().await;
        let handle = fixture
            .coordinator
            .clone()
            .start(fixture.store.clone(), CancellationToken::new());

        first_submit.entered.notified().await;
        *first_submit.result.lock().await =
            Some(Ok(remote("tick-poll-request", RemotePinStatus::Queued)));
        first_submit.release.notify_one();
        panic_get.notified().await;
        tokio::task::yield_now().await;

        let panic_job = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let text = capture.text();
        let failure_line = text
            .lines()
            .find(|line| {
                line.contains("pinning worker task failed")
                    && !line.contains("during drain")
                    && line.contains(&format!("job_id={}", panic_job.id))
            })
            .unwrap_or_else(|| panic!("expected main/tick worker failure line in:\n{text}"));
        for field in [
            format!("job_id={}", panic_job.id),
            "provider=noop".to_owned(),
            "cid=bafy-worker".to_owned(),
            "lease_id=Some(\"lease-1\")".to_owned(),
            "target_id=Some(\"target-1\")".to_owned(),
        ] {
            assert!(
                failure_line.contains(&field),
                "main/tick join associated the panic with the wrong task: {failure_line}"
            );
        }
        handle.shutdown(std::time::Duration::from_secs(5)).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_job_execution_failures_log_all_identity_fields() {
        const ISOLATED_TRACE_ENV: &str = "IPFS_S3_ISOLATED_WORKER_EXECUTION_FAILURE_TRACE_TEST";
        if std::env::var_os(ISOLATED_TRACE_ENV).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "pinning::worker::tests::worker_job_execution_failures_log_all_identity_fields",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(ISOLATED_TRACE_ENV, "1")
                .output()
                .expect("failed to start isolated tracing test process");
            assert!(
                output.status.success(),
                "isolated tracing test failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(capture.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _default_guard = tracing::dispatcher::set_default(&dispatch);
        tracing::callsite::rebuild_interest_cache();

        let blocker = Arc::new(SubmitBlocker {
            entered: Notify::new(),
            release: Notify::new(),
            result: Mutex::new(None),
        });
        let fixture = fixture([Script::BlockSubmit(blocker.clone())]).await;
        fixture.enqueue_submit().await;
        let job = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        let request_id = "execution-failure-request".to_owned();
        let gate = Arc::new(super::ObservationAfterStatusCommitGate {
            job_id: job.id.clone(),
            request_id: request_id.clone(),
            fail_once: std::sync::atomic::AtomicBool::new(true),
            arrived: Notify::new(),
        });
        let gate_key = (job.id.clone(), request_id.clone());
        super::OBSERVATION_AFTER_STATUS_COMMIT
            .lock()
            .await
            .insert(gate_key.clone(), gate.clone());

        let handle = fixture
            .coordinator
            .clone()
            .start(fixture.store.clone(), CancellationToken::new());
        blocker.entered.notified().await;
        let gate_arrived = gate.arrived.notified();
        let mut failed = remote(&request_id, RemotePinStatus::Failed);
        failed.failure_reason = Some("Bearer raw-provider-body test-provider-token".to_owned());
        *blocker.result.lock().await = Some(Ok(failed));
        blocker.release.notify_one();
        gate_arrived.await;
        tokio::task::yield_now().await;
        handle.shutdown(std::time::Duration::from_secs(5)).await;
        super::OBSERVATION_AFTER_STATUS_COMMIT
            .lock()
            .await
            .remove(&gate_key);

        let text = capture.text();
        let failure_line = text
            .lines()
            .find(|line| {
                line.contains("pinning job execution failed")
                    && line.contains(&format!("job_id={}", job.id))
            })
            .unwrap_or_else(|| panic!("expected job-execution failure line in:\n{text}"));
        for field in [
            format!("job_id={}", job.id),
            "provider=noop".to_owned(),
            "cid=bafy-worker".to_owned(),
            "lease_id=Some(\"lease-1\")".to_owned(),
            "target_id=Some(\"target-1\")".to_owned(),
        ] {
            assert!(
                failure_line.contains(&field),
                "job-execution failure omitted `{field}`: {failure_line}"
            );
        }
        for secret in ["Bearer", "raw-provider-body", "test-provider-token"] {
            assert!(
                !text.contains(secret),
                "worker failure logging must not expose provider material: {text}"
            );
        }
    }
}
