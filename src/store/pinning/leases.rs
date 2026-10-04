use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use chrono::{Duration as ChronoDuration, Utc};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, Condition, ConnectionTrait, DatabaseBackend,
    DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
    sea_query::Expr,
};
use sha2::{Digest, Sha256};

use crate::{
    error::{AppError, AppResult},
    pinning::{
        config::{ProviderLimitMap, ProviderLimits, ProviderMode},
        provider::RemotePinStatus,
    },
    store::{
        entities::{
            object, pin_invocation_route, pin_job, pin_lease, pin_lease_target, pin_provider_route,
            pin_resource_history, remote_pin,
        },
        pinning::{
            jobs::{self, NewPinJob, NoRequestSubmitAmbiguity},
            ledger,
            quota::{self, ConfirmedReleaseOutcome},
        },
    },
};

pub use super::jobs::DateTimeUtc;
pub use super::quota::ReservationOutcome;

#[cfg(test)]
pub(crate) mod test_gates {
    use std::{sync::Arc, sync::LazyLock};

    use tokio::sync::{Barrier, Mutex, Notify};

    pub struct DeleteAfterRefsGate {
        pub provider: &'static str,
        pub cid: &'static str,
        pub arrived: Notify,
        pub resume: Notify,
    }

    pub struct StatusAfterReadGate {
        pub provider: &'static str,
        pub cid: &'static str,
        pub barrier: Arc<Barrier>,
    }

    pub struct RenewalAfterSnapshotGate {
        pub lease_id: &'static str,
        pub arrived: Notify,
        pub resume: Notify,
    }

    pub struct RenewalBeforeOwnerGuardGate {
        pub lease_id: &'static str,
        pub arrived: Notify,
        pub resume: Notify,
    }

    pub struct LifecycleLeaseCasGate {
        pub lease_id: &'static str,
        pub expected_generation: i64,
        pub staged_generation: i64,
        pub arrived: Notify,
        pub resume: Notify,
    }

    pub struct RetryDegradeAfterLeaseSnapshotGate {
        pub lease_id: &'static str,
        pub arrived: Notify,
        pub resume: Notify,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum LifecycleOrderEvent {
        DesiredTargetsRead(String, String),
        OwnerLock(String),
        OwnerGuard(String),
        LeaseCas(String),
        TargetCas(String),
        RemoteWork(String, String),
        LeaseLock(String),
        TargetLock(String),
        RemoteLock(String, String),
        RemoteStatusWrite(String, String),
        RemoteCompensation(String, String),
        RemoteResubmit(String, String),
        TargetProjection(String),
        JobClaimLock(String),
        JobComplete(String),
        WorkerTransactionBoundary,
    }

    pub struct LifecycleOrderRecorder {
        pub owner_ids: std::collections::BTreeSet<String>,
        pub lease_ids: std::collections::BTreeSet<String>,
        pub target_ids: std::collections::BTreeSet<String>,
        pub remote_pairs: std::collections::BTreeSet<(String, String)>,
        pub record_desired_target_reads: bool,
        pub events: Vec<LifecycleOrderEvent>,
    }

    pub static DELETE_AFTER_REFS: LazyLock<Mutex<Option<Arc<DeleteAfterRefsGate>>>> =
        LazyLock::new(|| Mutex::new(None));
    pub static STATUS_AFTER_READ: LazyLock<Mutex<Option<Arc<StatusAfterReadGate>>>> =
        LazyLock::new(|| Mutex::new(None));
    pub static RENEWAL_AFTER_SNAPSHOT: LazyLock<Mutex<Option<Arc<RenewalAfterSnapshotGate>>>> =
        LazyLock::new(|| Mutex::new(None));
    pub static RENEWAL_BEFORE_OWNER_GUARD: LazyLock<
        Mutex<Option<Arc<RenewalBeforeOwnerGuardGate>>>,
    > = LazyLock::new(|| Mutex::new(None));
    pub static EVICTION_BEFORE_LEASE_CAS: LazyLock<Mutex<Option<Arc<LifecycleLeaseCasGate>>>> =
        LazyLock::new(|| Mutex::new(None));
    pub static FAILOVER_BEFORE_LEASE_CAS: LazyLock<Mutex<Option<Arc<LifecycleLeaseCasGate>>>> =
        LazyLock::new(|| Mutex::new(None));
    pub static RETRY_DEGRADE_AFTER_LEASE_SNAPSHOT: LazyLock<
        Mutex<Option<Arc<RetryDegradeAfterLeaseSnapshotGate>>>,
    > = LazyLock::new(|| Mutex::new(None));
    pub static LIFECYCLE_ORDER_RECORDER: LazyLock<Mutex<Option<LifecycleOrderRecorder>>> =
        LazyLock::new(|| Mutex::new(None));
    pub static LIFECYCLE_ORDER_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
}

#[cfg(test)]
async fn pause_retry_degrade_after_lease_snapshot(lease_id: &str) {
    let gate = test_gates::RETRY_DEGRADE_AFTER_LEASE_SNAPSHOT
        .lock()
        .await
        .clone();
    if let Some(gate) = gate.filter(|gate| gate.lease_id == lease_id) {
        gate.arrived.notify_one();
        gate.resume.notified().await;
    }
}

#[cfg(test)]
async fn pause_after_delete_ref_snapshot(provider: &str, cid: &str) {
    let gate = test_gates::DELETE_AFTER_REFS.lock().await.clone();
    if let Some(gate) = gate.filter(|gate| gate.provider == provider && gate.cid == cid) {
        gate.arrived.notify_one();
        gate.resume.notified().await;
    }
}

#[cfg(test)]
async fn pause_after_remote_status_read(provider: &str, cid: &str) {
    let gate = test_gates::STATUS_AFTER_READ.lock().await.clone();
    if let Some(gate) = gate.filter(|gate| gate.provider == provider && gate.cid == cid) {
        gate.barrier.wait().await;
    }
}

#[cfg(test)]
async fn pause_after_renewal_snapshot(lease_id: &str) {
    let gate = test_gates::RENEWAL_AFTER_SNAPSHOT.lock().await.clone();
    if let Some(gate) = gate.filter(|gate| gate.lease_id == lease_id) {
        gate.arrived.notify_one();
        gate.resume.notified().await;
    }
}

#[cfg(test)]
async fn stage_renewal_owner_before_guard<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    owner: &object::Model,
) -> AppResult<()> {
    let gate = test_gates::RENEWAL_BEFORE_OWNER_GUARD.lock().await.clone();
    let Some(gate) = gate.filter(|gate| gate.lease_id == lease_id) else {
        return Ok(());
    };
    gate.arrived.notify_one();
    gate.resume.notified().await;
    let staged = object::Entity::update_many()
        .col_expr(object::Column::IsLatest, Expr::value(false))
        .filter(object::Column::Id.eq(&owner.id))
        .filter(object::Column::IsLatest.eq(true))
        .exec(db)
        .await?;
    if staged.rows_affected != 1 {
        return Err(AppError::Database(
            "test renewal owner supersession staging compare-and-set failed".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
async fn stage_lifecycle_lease_generation<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    gate: &tokio::sync::Mutex<Option<std::sync::Arc<test_gates::LifecycleLeaseCasGate>>>,
) -> AppResult<()> {
    let gate = gate.lock().await.clone();
    let Some(gate) = gate.filter(|gate| gate.lease_id == lease_id) else {
        return Ok(());
    };
    gate.arrived.notify_one();
    gate.resume.notified().await;
    let staged = pin_lease::Entity::update_many()
        .col_expr(
            pin_lease::Column::Generation,
            Expr::value(gate.staged_generation),
        )
        .filter(pin_lease::Column::Id.eq(lease_id))
        .filter(pin_lease::Column::Generation.eq(gate.expected_generation))
        .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
        .exec(db)
        .await?;
    if staged.rows_affected != 1 {
        return Err(AppError::Database(
            "test lifecycle generation staging compare-and-set failed".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
async fn stage_eviction_lease_generation<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
) -> AppResult<()> {
    stage_lifecycle_lease_generation(db, lease_id, &test_gates::EVICTION_BEFORE_LEASE_CAS).await
}

#[cfg(test)]
async fn stage_failover_lease_generation<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
) -> AppResult<()> {
    stage_lifecycle_lease_generation(db, lease_id, &test_gates::FAILOVER_BEFORE_LEASE_CAS).await
}

#[cfg(test)]
async fn record_lease_cas(lease_id: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder
        .as_mut()
        .filter(|recorder| recorder.lease_ids.contains(lease_id))
    {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::LeaseCas(
                lease_id.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_owner_lock(owner_id: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder
        .as_mut()
        .filter(|recorder| recorder.owner_ids.contains(owner_id))
    {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::OwnerLock(
                owner_id.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_owner_guard(owner_id: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder
        .as_mut()
        .filter(|recorder| recorder.owner_ids.contains(owner_id))
    {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::OwnerGuard(
                owner_id.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_desired_targets_read(provider: &str, cid: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder.as_mut().filter(|recorder| {
        recorder.record_desired_target_reads
            && recorder
                .remote_pairs
                .contains(&(provider.to_owned(), cid.to_owned()))
    }) {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::DesiredTargetsRead(
                provider.to_owned(),
                cid.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_target_cas(target_id: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder
        .as_mut()
        .filter(|recorder| recorder.target_ids.contains(target_id))
    {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::TargetCas(
                target_id.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_remote_work(provider: &str, cid: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder.as_mut().filter(|recorder| {
        recorder
            .remote_pairs
            .contains(&(provider.to_owned(), cid.to_owned()))
    }) {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::RemoteWork(
                provider.to_owned(),
                cid.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_lease_lock(lease_id: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder
        .as_mut()
        .filter(|recorder| recorder.lease_ids.contains(lease_id))
    {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::LeaseLock(
                lease_id.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_target_lock(target_id: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder
        .as_mut()
        .filter(|recorder| recorder.target_ids.contains(target_id))
    {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::TargetLock(
                target_id.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_remote_lock(provider: &str, cid: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder.as_mut().filter(|recorder| {
        recorder
            .remote_pairs
            .contains(&(provider.to_owned(), cid.to_owned()))
    }) {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::RemoteLock(
                provider.to_owned(),
                cid.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_remote_status_write(provider: &str, cid: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder.as_mut().filter(|recorder| {
        recorder
            .remote_pairs
            .contains(&(provider.to_owned(), cid.to_owned()))
    }) {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::RemoteStatusWrite(
                provider.to_owned(),
                cid.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_remote_compensation(provider: &str, cid: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder.as_mut().filter(|recorder| {
        recorder
            .remote_pairs
            .contains(&(provider.to_owned(), cid.to_owned()))
    }) {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::RemoteCompensation(
                provider.to_owned(),
                cid.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_remote_resubmit(provider: &str, cid: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder.as_mut().filter(|recorder| {
        recorder
            .remote_pairs
            .contains(&(provider.to_owned(), cid.to_owned()))
    }) {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::RemoteResubmit(
                provider.to_owned(),
                cid.to_owned(),
            ));
    }
}

#[cfg(test)]
async fn record_target_projection(target_id: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder
        .as_mut()
        .filter(|recorder| recorder.target_ids.contains(target_id))
    {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::TargetProjection(
                target_id.to_owned(),
            ));
    }
}

#[cfg(test)]
pub(crate) async fn record_worker_transaction_boundary_for_test(provider: &str, cid: &str) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder.as_mut().filter(|recorder| {
        recorder
            .remote_pairs
            .contains(&(provider.to_owned(), cid.to_owned()))
    }) {
        recorder
            .events
            .push(test_gates::LifecycleOrderEvent::WorkerTransactionBoundary);
    }
}

#[cfg(test)]
pub(crate) async fn record_worker_job_event_for_test(
    provider: &str,
    cid: &str,
    event: test_gates::LifecycleOrderEvent,
) {
    let mut recorder = test_gates::LIFECYCLE_ORDER_RECORDER.lock().await;
    if let Some(recorder) = recorder.as_mut().filter(|recorder| {
        recorder
            .remote_pairs
            .contains(&(provider.to_owned(), cid.to_owned()))
    }) {
        recorder.events.push(event);
    }
}

const LEASE_ACTIVE: &str = "active";
const LEASE_EXPIRED: &str = "expired";
const LEASE_CANCELLED: &str = "cancelled";
const LEASE_EVICTED: &str = "evicted";
const MANUAL_SOURCE: &str = "manual";

const TARGET_WAITING: &str = "waiting";
const TARGET_SUBMITTED: &str = "submitted";
const TARGET_PINNED: &str = "pinned";
const TARGET_DEGRADED: &str = "degraded";
const TARGET_QUOTA_WAITING: &str = "quota_waiting";
const TARGET_QUOTA_BLOCKED: &str = "quota_blocked";
const TARGET_EVICTED: &str = "evicted";
const TARGET_RELEASED: &str = "released";

const REMOTE_RESERVED: &str = "reserved";
const REMOTE_QUEUED: &str = "queued";
const REMOTE_PINNING: &str = "pinning";
const REMOTE_PINNED: &str = "pinned";
const REMOTE_FAILED: &str = "failed";
const REMOTE_ABSENT: &str = "absent";

const JOB_DONE: &str = "done";
const JOB_PENDING: &str = "pending";
const JOB_RUNNING: &str = "running";

/// The outcome of a generation or remote-epoch guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationDecision {
    Current,
    Stale,
    NoLongerNeeded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteDeleteCompletion {
    Released,
    Compensated { submit_job_id: String },
    ReconcileRequired { reconcile_job_id: String },
}

#[derive(Debug, Clone)]
pub struct RemoteStatusUpdate<'a> {
    pub provider: &'a str,
    pub cid: &'a str,
    pub request_id: &'a str,
    pub origin: RemoteStatusOrigin,
    pub status: RemotePinStatus,
    pub error_class: Option<&'a str>,
    pub error_text: Option<&'a str>,
    pub now: DateTimeUtc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteStatusOrigin {
    Adopt,
    ExistingRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffectedLeaseOutcome {
    pub lease_id: String,
    pub target_id: String,
    pub provider_mode: ProviderMode,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteStatusApplyResult {
    Applied {
        affected: Vec<AffectedLeaseOutcome>,
        failure: Option<RemoteFailureProgress>,
        previous_status: String,
        current_status: String,
    },
    StaleRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFailureProgress {
    pub attempts: i32,
    pub next_retry_at: Option<DateTimeUtc>,
    pub newly_counted: bool,
    pub exhausted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetProjection {
    Pinned,
    Submitted { poll_job_id: String },
    Waiting { submit_job_id: String },
    Degraded { reconcile_job_id: String },
    QuotaWaiting,
    QuotaBlocked,
}

/// A target projection that reports the quota decision when an absent remote was reacquired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitsAwareTargetProjection {
    pub projection: TargetProjection,
    pub reservation: Option<ReservationOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoRequestRemoteCompletion {
    Released,
    Wait { next_check_at: DateTimeUtc },
    Stale,
}

pub const MAX_FAILED_REQUEST_ATTEMPTS: i32 = 8;
pub const FAILED_REQUEST_BASE_BACKOFF: Duration = Duration::from_secs(1);
pub const FAILED_REQUEST_MAX_BACKOFF: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailedRemoteRetryDecision {
    Scheduled {
        reconcile_job_id: String,
        at: DateTimeUtc,
    },
    NotNeeded,
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailedRemoteResubmitDecision {
    Prepared {
        new_epoch: i64,
        submit_job_id: String,
    },
    Stale,
    NoAllModeTarget,
    Exhausted,
}

/// The durable state a remote-scoped worker may use to choose its next convergence action.
#[derive(Debug, Clone)]
pub struct RemoteWorkSnapshot {
    pub remote: remote_pin::Model,
    pub desired: Vec<RemoteDesiredTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDesiredTarget {
    pub lease_id: String,
    pub target_id: String,
    pub owner_object_id: String,
    pub generation: i64,
    pub provider_mode: ProviderMode,
}

/// One desired target retired by a provider quota eviction transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaEvictedTarget {
    pub lease_id: String,
    pub target_id: String,
    pub provider: String,
    pub cid: String,
    pub provider_mode: ProviderMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualLeaseRenewalOutcome {
    Kept {
        generation: i64,
    },
    Extended {
        generation: i64,
    },
    Reactivated {
        generation: i64,
        restored_target_ids: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManualLeaseOwnerScope {
    Latest,
    RetainedVersion,
}

#[derive(Debug, thiserror::Error)]
pub enum RenewManualLeaseError {
    #[error("manual lease is not owned by the latest object")]
    NotLatestOwner,
    #[error("manual lease state cannot be renewed")]
    InvalidState,
    #[error("expired manual lease has no recoverable remote reservation")]
    NoRecoverableReservation,
    #[error(transparent)]
    Database(#[from] sea_orm::DbErr),
}

/// Serializes a tag-only `Keep` decision against lifecycle changes without traversing or
/// locking the shared target frontier. The caller must already hold the latest owner lock and
/// must not perform target, remote, quota, or job work after this guard.
pub(crate) async fn guard_manual_lease_snapshot<C: ConnectionTrait>(
    db: &C,
    expected: &pin_lease::Model,
) -> Result<bool, sea_orm::DbErr> {
    let guarded = pin_lease::Entity::update_many()
        .col_expr(
            pin_lease::Column::LastTouchedAt,
            Expr::value(expected.last_touched_at),
        )
        .filter(pin_lease::Column::Id.eq(&expected.id))
        .filter(pin_lease::Column::OwnerObjectId.eq(&expected.owner_object_id))
        .filter(pin_lease::Column::Source.eq(&expected.source))
        .filter(pin_lease::Column::PolicyId.eq(&expected.policy_id))
        .filter(pin_lease::Column::ProviderMode.eq(&expected.provider_mode))
        .filter(pin_lease::Column::ContentMode.eq(&expected.content_mode))
        .filter(pin_lease::Column::CreatedAt.eq(expected.created_at))
        .filter(pin_lease::Column::LastTouchedAt.eq(expected.last_touched_at))
        .filter(pin_lease::Column::ExpiresAt.eq(expected.expires_at))
        .filter(pin_lease::Column::Generation.eq(expected.generation))
        .filter(pin_lease::Column::State.eq(&expected.state))
        .exec(db)
        .await?;
    Ok(guarded.rows_affected == 1)
}

#[derive(Clone)]
struct DesiredTarget {
    target: pin_lease_target::Model,
    lease: pin_lease::Model,
}

#[derive(Clone)]
struct OrderedDesiredLifecycleSnapshot {
    /// Desired targets for the observed remote only. These are the only targets a status update
    /// may project.
    desired: Vec<DesiredTarget>,
    /// Every target of every lease affected by `desired`, including cross-provider and terminal
    /// siblings. Keeping this complete frontier in memory makes availability a snapshot
    /// computation rather than a post-remote sibling query.
    frontier: Vec<DesiredTarget>,
    remote: remote_pin::Model,
}

#[allow(clippy::large_enum_variant)]
enum OrderedDesiredLifecycleSnapshotOutcome {
    Current(OrderedDesiredLifecycleSnapshot),
    Stale,
    MissingRemote,
}

/// Checks whether a target still belongs to an active lease at the expected generation.
pub async fn check_target_generation<C: ConnectionTrait>(
    db: &C,
    target_id: &str,
    expected_generation: i64,
) -> AppResult<GenerationDecision> {
    let Some(target) = pin_lease_target::Entity::find_by_id(target_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(GenerationDecision::Stale);
    };
    let Some(lease) = pin_lease::Entity::find_by_id(target.lease_id.clone())
        .one(db)
        .await?
    else {
        return Ok(GenerationDecision::Stale);
    };
    if lease.generation != expected_generation {
        return Ok(GenerationDecision::Stale);
    }
    if lease.state != LEASE_ACTIVE || !is_desired_target_state(&target.state) {
        return Ok(GenerationDecision::NoLongerNeeded);
    }
    Ok(GenerationDecision::Current)
}

/// Marks a current all-mode target degraded while its Submit is being recovered after provider
/// I/O failed without a durable PSA request identity. The caller must first fence the exact job
/// claim in the same transaction. This function then follows the canonical lease -> target lock
/// order; it never changes the remote identity, quota usage, or retry ownership.
pub async fn mark_all_mode_target_degraded_for_retry(
    db: &DatabaseTransaction,
    job: &pin_job::Model,
    now: DateTimeUtc,
) -> AppResult<()> {
    let (Some(lease_id), Some(target_id), Some(expected_generation)) = (
        job.lease_id.as_deref(),
        job.target_id.as_deref(),
        job.expected_generation,
    ) else {
        return Ok(());
    };
    if job.operation != "submit" || job.expected_remote_epoch.is_some() {
        return Ok(());
    }
    let Some(expected_lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(());
    };
    if expected_lease.state != LEASE_ACTIVE
        || expected_lease.provider_mode != "all"
        || expected_lease.generation != expected_generation
    {
        return Ok(());
    }
    #[cfg(test)]
    pause_retry_degrade_after_lease_snapshot(lease_id).await;

    let lease = match lock_lifecycle_lease(db, &expected_lease, true).await {
        Ok(Some(lease)) => lease,
        Ok(None) => return Ok(()),
        Err(error) if is_sqlite_lifecycle_contention(db, &error) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if lease.state != LEASE_ACTIVE
        || lease.provider_mode != "all"
        || lease.generation != expected_generation
    {
        return Ok(());
    }
    let Some(expected_target) = pin_lease_target::Entity::find_by_id(target_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(());
    };
    if expected_target.lease_id != lease.id
        || expected_target.provider != job.provider
        || expected_target.cid != job.cid
        || !matches!(
            expected_target.state.as_str(),
            TARGET_WAITING | TARGET_SUBMITTED
        )
    {
        return Ok(());
    }
    let target = match lock_lifecycle_target(db, &expected_target, true).await {
        Ok(Some(target)) => target,
        Ok(None) => return Ok(()),
        Err(error) if is_sqlite_lifecycle_contention(db, &error) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if target.lease_id != lease.id
        || target.provider != job.provider
        || target.cid != job.cid
        || !matches!(target.state.as_str(), TARGET_WAITING | TARGET_SUBMITTED)
    {
        return Ok(());
    }

    #[cfg(test)]
    record_target_projection(&target.id).await;

    let updated = pin_lease_target::Entity::update_many()
        .col_expr(
            pin_lease_target::Column::State,
            Expr::value(TARGET_DEGRADED.to_owned()),
        )
        .col_expr(pin_lease_target::Column::LastTouchedAt, Expr::value(now))
        .filter(pin_lease_target::Column::Id.eq(&target.id))
        .filter(pin_lease_target::Column::LeaseId.eq(&target.lease_id))
        .filter(pin_lease_target::Column::Provider.eq(&target.provider))
        .filter(pin_lease_target::Column::Cid.eq(&target.cid))
        .filter(pin_lease_target::Column::LogicalSize.eq(target.logical_size))
        .filter(pin_lease_target::Column::CreatedAt.eq(target.created_at))
        .filter(pin_lease_target::Column::LastTouchedAt.eq(target.last_touched_at))
        .filter(pin_lease_target::Column::State.eq(&target.state))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(stale_lifecycle_error("all-mode retry target projection"));
    }
    Ok(())
}

/// Checks the remote epoch without opening or committing a transaction.
pub async fn check_remote_epoch<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
) -> AppResult<GenerationDecision> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(GenerationDecision::Stale);
    };
    if remote.epoch != expected_remote_epoch {
        return Ok(GenerationDecision::Stale);
    }
    if remote.status == REMOTE_ABSENT {
        return Ok(GenerationDecision::NoLongerNeeded);
    }
    Ok(GenerationDecision::Current)
}

/// Locks one exact remote epoch inside the caller's transaction without changing its value.
pub async fn guard_reconcile_remote_epoch<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
) -> AppResult<bool> {
    Ok(remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::Epoch,
            Expr::value(expected_remote_epoch),
        )
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Cid.eq(cid))
        .filter(remote_pin::Column::Epoch.eq(expected_remote_epoch))
        .exec(db)
        .await?
        .rows_affected
        == 1)
}

/// Reads the remote row and its canonical, ordered active desired set without mutating either.
pub async fn remote_work_snapshot<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<RemoteWorkSnapshot>> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let desired = desired_targets(db, provider, cid)
        .await?
        .into_iter()
        .map(|desired| {
            Ok(RemoteDesiredTarget {
                lease_id: desired.lease.id,
                target_id: desired.target.id,
                owner_object_id: desired.lease.owner_object_id,
                generation: desired.lease.generation,
                provider_mode: provider_mode(&desired.lease.provider_mode)?,
            })
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(Some(RemoteWorkSnapshot { remote, desired }))
}

/// Projects one active target from its shared remote row and creates only canonical durable work.
pub async fn project_target_from_remote<C: ConnectionTrait>(
    db: &C,
    target_id: &str,
    now: DateTimeUtc,
) -> AppResult<TargetProjection> {
    project_target_from_remote_inner(db, target_id, None, now)
        .await?
        .ok_or_else(|| invalid("unscoped target projection unexpectedly failed an epoch guard"))
}

/// Projects one Reconcile target only while the ordered remote lock is at the exact job epoch.
pub async fn project_reconcile_target_from_remote<C: ConnectionTrait>(
    db: &C,
    target_id: &str,
    expected_remote_epoch: i64,
    now: DateTimeUtc,
) -> AppResult<Option<TargetProjection>> {
    project_target_from_remote_inner(db, target_id, Some(expected_remote_epoch), now).await
}

async fn project_target_from_remote_inner<C: ConnectionTrait>(
    db: &C,
    target_id: &str,
    expected_remote_epoch: Option<i64>,
    now: DateTimeUtc,
) -> AppResult<Option<TargetProjection>> {
    let Some(target) = pin_lease_target::Entity::find_by_id(target_id.to_owned())
        .one(db)
        .await?
    else {
        return Err(invalid("target does not exist"));
    };
    match target.state.as_str() {
        TARGET_QUOTA_WAITING => return Ok(Some(TargetProjection::QuotaWaiting)),
        TARGET_QUOTA_BLOCKED => return Ok(Some(TargetProjection::QuotaBlocked)),
        _ => {}
    }
    if !is_desired_target_state(&target.state) {
        return Err(invalid("target is not an active desired target"));
    }
    let snapshot =
        match ordered_desired_lifecycle_snapshot(db, &target.provider, &target.cid).await? {
            OrderedDesiredLifecycleSnapshotOutcome::Current(snapshot) => snapshot,
            OrderedDesiredLifecycleSnapshotOutcome::Stale => {
                return Err(stale_lifecycle_error("target projection"));
            }
            OrderedDesiredLifecycleSnapshotOutcome::MissingRemote => {
                return Err(invalid("target remote pin does not exist"));
            }
        };
    let desired = snapshot
        .desired
        .iter()
        .find(|desired| desired.target.id == target_id)
        .ok_or_else(|| stale_lifecycle_error("target projection"))?;
    if expected_remote_epoch.is_some_and(|expected| snapshot.remote.epoch != expected) {
        return Ok(None);
    }
    Ok(Some(
        project_prelocked_desired_target(
            db,
            desired,
            &snapshot.desired,
            &snapshot.remote,
            ProjectionAccess::Admission,
            now,
        )
        .await?,
    ))
}

/// Projects a target inserted by a caller that already holds the lifecycle and remote frontier.
///
/// Unlike `project_target_from_remote`, this must not reacquire shared lifecycle locks after the
/// caller has reached the remote/usage portion of the canonical lock order. The inserted target
/// belongs to the caller's locked lease; other desired rows are read only to select the canonical
/// shared work owner and are never mutated here.
async fn project_inserted_target_from_prelocked_remote<C: ConnectionTrait>(
    db: &C,
    target_id: &str,
    provider: &str,
    cid: &str,
    now: DateTimeUtc,
) -> AppResult<TargetProjection> {
    let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
        .ok_or_else(|| invalid("target remote pin does not exist"))?;
    let desired = desired_targets(db, provider, cid).await?;
    let inserted = desired
        .iter()
        .find(|desired| desired.target.id == target_id)
        .ok_or_else(|| stale_lifecycle_error("inserted target projection"))?;
    project_prelocked_desired_target(
        db,
        inserted,
        &desired,
        &remote,
        ProjectionAccess::Admission,
        now,
    )
    .await
}

/// Acquires the union of lifecycle rows that a publication can replace or attach to.
///
/// The caller must already own the corresponding latest-object locks. All leases are locked
/// before any targets, and all targets before any remotes, so a multi-provider publication cannot
/// invert the lifecycle ordering used by cancellation, expiry, and deletion.
pub async fn lock_publication_lifecycle_frontier<C: ConnectionTrait>(
    db: &C,
    previous_owner_ids: &[String],
    attachment_pairs: &[(String, String)],
) -> AppResult<()> {
    let mut previous_owner_ids = previous_owner_ids.to_vec();
    previous_owner_ids.sort();
    previous_owner_ids.dedup();

    let mut attachment_pairs = attachment_pairs.to_vec();
    attachment_pairs.sort();
    attachment_pairs.dedup();

    let previous_leases = if previous_owner_ids.is_empty() {
        Vec::new()
    } else {
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.is_in(previous_owner_ids.clone()))
            .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
            .order_by_asc(pin_lease::Column::Id)
            .all(db)
            .await?
    };

    let mut attachment_snapshots = BTreeMap::new();
    let mut lease_by_id: BTreeMap<String, pin_lease::Model> = previous_leases
        .iter()
        .cloned()
        .map(|lease| (lease.id.clone(), lease))
        .collect();
    for (provider, cid) in &attachment_pairs {
        let desired = desired_targets(db, provider, cid).await?;
        for target in &desired {
            lease_by_id
                .entry(target.lease.id.clone())
                .or_insert_with(|| target.lease.clone());
        }
        attachment_snapshots.insert((provider.clone(), cid.clone()), desired);
    }

    let lease_ids: Vec<_> = lease_by_id.keys().cloned().collect();
    let mut target_frontier = if lease_ids.is_empty() {
        Vec::new()
    } else {
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.is_in(lease_ids.clone()))
            .order_by_asc(pin_lease_target::Column::CreatedAt)
            .order_by_asc(pin_lease_target::Column::Id)
            .all(db)
            .await?
    };
    target_frontier.sort_by(compare_target_order);

    let mut remote_pairs: BTreeSet<_> = attachment_pairs.iter().cloned().collect();
    for target in &target_frontier {
        if is_desired_target_state(&target.state) {
            remote_pairs.insert((target.provider.clone(), target.cid.clone()));
        }
    }

    for lease in lease_by_id.values() {
        if lock_lifecycle_lease(db, lease, true).await?.is_none() {
            return Err(stale_lifecycle_error("publication lifecycle lease prelock"));
        }
    }
    for target in &target_frontier {
        if lock_lifecycle_target(db, target, true).await?.is_none() {
            return Err(stale_lifecycle_error(
                "publication lifecycle target prelock",
            ));
        }
    }
    for (provider, cid) in &remote_pairs {
        lock_remote_after_lifecycle(db, provider, cid).await?;
    }

    let current_previous_leases = if previous_owner_ids.is_empty() {
        Vec::new()
    } else {
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.is_in(previous_owner_ids))
            .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
            .order_by_asc(pin_lease::Column::Id)
            .all(db)
            .await?
    };
    if current_previous_leases != previous_leases {
        return Err(stale_lifecycle_error("publication previous-owner prelock"));
    }

    let current_target_frontier = if lease_ids.is_empty() {
        Vec::new()
    } else {
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.is_in(lease_ids))
            .order_by_asc(pin_lease_target::Column::CreatedAt)
            .order_by_asc(pin_lease_target::Column::Id)
            .all(db)
            .await?
    };
    if current_target_frontier != target_frontier {
        return Err(stale_lifecycle_error("publication target frontier prelock"));
    }

    for ((provider, cid), expected) in attachment_snapshots {
        let current = desired_targets(db, &provider, &cid).await?;
        if !same_desired_lifecycle_snapshot(&expected, &current) {
            return Err(stale_lifecycle_error("publication attachment prelock"));
        }
    }
    Ok(())
}

/// Projects a target from a remote row, reacquiring an absent reservation only under explicit
/// caller-supplied provider limits. This function never opens or commits a transaction.
pub async fn project_target_from_remote_with_limits<C: ConnectionTrait>(
    db: &C,
    target_id: &str,
    limits: &ProviderLimitMap,
    now: DateTimeUtc,
) -> AppResult<LimitsAwareTargetProjection> {
    let Some(target) = pin_lease_target::Entity::find_by_id(target_id.to_owned())
        .one(db)
        .await?
    else {
        return Err(invalid("target does not exist"));
    };
    if target.state == TARGET_QUOTA_WAITING {
        return Ok(LimitsAwareTargetProjection {
            projection: TargetProjection::QuotaWaiting,
            reservation: None,
        });
    }
    if target.state == TARGET_QUOTA_BLOCKED {
        return Ok(LimitsAwareTargetProjection {
            projection: TargetProjection::QuotaBlocked,
            reservation: None,
        });
    }
    if !is_desired_target_state(&target.state) {
        return Err(invalid("target is not an active desired target"));
    }
    let snapshot =
        match ordered_desired_lifecycle_snapshot(db, &target.provider, &target.cid).await? {
            OrderedDesiredLifecycleSnapshotOutcome::Current(snapshot) => snapshot,
            OrderedDesiredLifecycleSnapshotOutcome::Stale => {
                return Err(stale_lifecycle_error("limits-aware target projection"));
            }
            OrderedDesiredLifecycleSnapshotOutcome::MissingRemote => {
                return Err(invalid("target remote pin does not exist"));
            }
        };
    let desired = snapshot
        .desired
        .iter()
        .find(|desired| desired.target.id == target_id)
        .ok_or_else(|| stale_lifecycle_error("limits-aware target projection"))?;
    if snapshot.remote.status != REMOTE_ABSENT {
        return Ok(LimitsAwareTargetProjection {
            projection: project_prelocked_desired_target(
                db,
                desired,
                &snapshot.desired,
                &snapshot.remote,
                ProjectionAccess::Admission,
                now,
            )
            .await?,
            reservation: None,
        });
    }

    let reservation = quota::reserve_unique(
        db,
        &target.provider,
        &target.cid,
        target.logical_size,
        limits,
        now,
    )
    .await?;
    let projection = match &reservation {
        ReservationOutcome::Reserved | ReservationOutcome::Reused => {
            let remote = lock_remote_after_lifecycle(db, &target.provider, &target.cid)
                .await?
                .ok_or_else(|| invalid("target remote pin disappeared during reservation"))?;
            project_prelocked_desired_target(
                db,
                desired,
                &snapshot.desired,
                &remote,
                ProjectionAccess::Admission,
                now,
            )
            .await?
        }
        ReservationOutcome::QuotaWaiting { .. } => {
            set_prelocked_target_state(db, &desired.target, TARGET_QUOTA_WAITING).await?;
            TargetProjection::QuotaWaiting
        }
        ReservationOutcome::QuotaBlocked => {
            set_prelocked_target_state(db, &desired.target, TARGET_QUOTA_BLOCKED).await?;
            TargetProjection::QuotaBlocked
        }
    };
    Ok(LimitsAwareTargetProjection {
        projection,
        reservation: Some(reservation),
    })
}

/// Reserves and projects active provider waiters in durable FIFO order.
///
/// The caller must own transaction scope. This function acquires every candidate lease and target
/// before remote/usage work so multiple grants share the normal lifecycle lock order.
pub(crate) async fn wake_quota_waiting_targets<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<Vec<String>> {
    let waiting = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::State.eq(TARGET_QUOTA_WAITING))
        .filter(pin_lease_target::Column::LastTouchedAt.lte(now))
        .order_by_asc(pin_lease_target::Column::CreatedAt)
        .order_by_asc(pin_lease_target::Column::Id)
        .all(db)
        .await?;
    let mut candidates = Vec::with_capacity(waiting.len());
    let mut leases = BTreeMap::new();
    for target in waiting {
        let Some(lease) = pin_lease::Entity::find_by_id(target.lease_id.clone())
            .one(db)
            .await?
        else {
            continue;
        };
        if lease.state == LEASE_ACTIVE {
            leases.entry(lease.id.clone()).or_insert(lease.clone());
            candidates.push((target, lease));
        }
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let pairs = candidates
        .iter()
        .map(|(target, _)| (target.provider.clone(), target.cid.clone()))
        .collect::<BTreeSet<_>>();
    for (provider, cid) in &pairs {
        for desired in desired_targets(db, provider, cid).await? {
            leases
                .entry(desired.lease.id.clone())
                .or_insert(desired.lease);
        }
    }

    let mut target_frontier = Vec::new();
    for lease_id in leases.keys() {
        target_frontier.extend(lease_targets(db, lease_id).await?);
    }
    target_frontier.sort_by(compare_target_order);
    target_frontier.dedup_by(|left, right| left.id == right.id);

    for lease in leases.values() {
        let locked = lock_lifecycle_lease(db, lease, true)
            .await?
            .ok_or_else(|| stale_lifecycle_error("quota waiter lease prelock"))?;
        if locked != *lease {
            return Err(stale_lifecycle_error("quota waiter lease frontier"));
        }
    }
    for target in &target_frontier {
        let locked = lock_lifecycle_target(db, target, true)
            .await?
            .ok_or_else(|| stale_lifecycle_error("quota waiter target prelock"))?;
        if locked != *target {
            return Err(stale_lifecycle_error("quota waiter target frontier"));
        }
    }
    for (provider, cid) in &pairs {
        lock_remote_after_lifecycle(db, provider, cid).await?;
    }

    let limit_map = ProviderLimitMap::from([(provider.to_owned(), limits.clone())]);
    let mut woken = Vec::new();
    for (target, _) in candidates {
        let reservation = quota::reserve_unique_for_waiter(
            db,
            provider,
            &target.cid,
            target.logical_size,
            &limit_map,
            now,
        )
        .await?;
        match reservation {
            ReservationOutcome::QuotaWaiting { .. } => break,
            ReservationOutcome::QuotaBlocked => {
                // This is a stable non-debt budget decision: waiter reservation
                // maps operator debt to waiting, without rereading mutable debt.
                set_prelocked_target_state(db, &target, TARGET_QUOTA_BLOCKED).await?;
            }
            ReservationOutcome::Reserved | ReservationOutcome::Reused => {
                set_prelocked_target_state(db, &target, TARGET_WAITING).await?;
                quota::refresh_remote_max_active_touch(db, provider, &target.cid).await?;
                project_inserted_target_from_prelocked_remote(
                    db,
                    &target.id,
                    provider,
                    &target.cid,
                    now,
                )
                .await?;
                woken.push(target.id);
            }
        }
    }
    Ok(woken)
}

/// Requeues all-mode targets released by quota eviction after a durable cooldown.
///
/// Ordinary cancellation/expiry targets are excluded because their parent lease is not active.
/// The future touch is a durable not-before marker; these retry waiters never drive another
/// eviction, so a CID that just gained headroom cannot be immediately displaced.
pub(crate) async fn requeue_released_all_quota_targets<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    retry_delay: ChronoDuration,
    now: DateTimeUtc,
) -> AppResult<Vec<String>> {
    let evicted = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::State.eq(TARGET_EVICTED))
        .order_by_asc(pin_lease_target::Column::CreatedAt)
        .order_by_asc(pin_lease_target::Column::Id)
        .all(db)
        .await?;
    let mut candidates = Vec::new();
    let mut leases = BTreeMap::new();
    let mut remotes = BTreeMap::new();
    for target in evicted {
        let Some(lease) = pin_lease::Entity::find_by_id(target.lease_id.clone())
            .one(db)
            .await?
        else {
            continue;
        };
        if lease.state != LEASE_ACTIVE || lease.provider_mode != "all" {
            continue;
        }
        let Some(remote) =
            remote_pin::Entity::find_by_id((provider.to_owned(), target.cid.clone()))
                .one(db)
                .await?
        else {
            continue;
        };
        if remote.status != REMOTE_ABSENT {
            continue;
        }
        leases.entry(lease.id.clone()).or_insert(lease.clone());
        remotes
            .entry((remote.provider.clone(), remote.cid.clone()))
            .or_insert(remote);
        candidates.push((target, lease));
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let mut target_frontier = Vec::new();
    for lease_id in leases.keys() {
        target_frontier.extend(lease_targets(db, lease_id).await?);
    }
    target_frontier.sort_by(compare_target_order);
    target_frontier.dedup_by(|left, right| left.id == right.id);
    for lease in leases.values() {
        let locked = lock_lifecycle_lease(db, lease, true)
            .await?
            .ok_or_else(|| stale_lifecycle_error("quota retry lease prelock"))?;
        if locked != *lease {
            return Err(stale_lifecycle_error("quota retry lease frontier"));
        }
    }
    for target in &target_frontier {
        let locked = lock_lifecycle_target(db, target, true)
            .await?
            .ok_or_else(|| stale_lifecycle_error("quota retry target prelock"))?;
        if locked != *target {
            return Err(stale_lifecycle_error("quota retry target frontier"));
        }
    }
    for (provider, cid) in remotes.keys() {
        lock_remote_after_lifecycle(db, provider, cid).await?;
    }

    let mut requeued = Vec::with_capacity(candidates.len());
    for (target, _) in candidates {
        let remote = remotes
            .get(&(target.provider.clone(), target.cid.clone()))
            .expect("every quota retry target has a remote snapshot");
        let retry_at = remote
            .last_touched_at
            .checked_add_signed(retry_delay)
            .ok_or_else(|| invalid("quota retry time overflow"))?
            .max(now)
            .max(
                target
                    .created_at
                    .checked_add_signed(ChronoDuration::milliseconds(1))
                    .ok_or_else(|| invalid("quota retry marker overflow"))?,
            );
        let updated = pin_lease_target::Entity::update_many()
            .col_expr(
                pin_lease_target::Column::State,
                Expr::value(TARGET_QUOTA_WAITING.to_owned()),
            )
            .col_expr(
                pin_lease_target::Column::LastTouchedAt,
                Expr::value(retry_at),
            )
            .filter(pin_lease_target::Column::Id.eq(&target.id))
            .filter(pin_lease_target::Column::LeaseId.eq(&target.lease_id))
            .filter(pin_lease_target::Column::Provider.eq(&target.provider))
            .filter(pin_lease_target::Column::Cid.eq(&target.cid))
            .filter(pin_lease_target::Column::State.eq(TARGET_EVICTED))
            .filter(pin_lease_target::Column::LastTouchedAt.eq(target.last_touched_at))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Err(stale_lifecycle_error("quota retry target"));
        }
        requeued.push(target.id);
    }
    Ok(requeued)
}

/// Applies a provider observation only when the request identity still belongs to this remote.
pub async fn apply_remote_status<C: ConnectionTrait>(
    db: &C,
    update: RemoteStatusUpdate<'_>,
) -> AppResult<RemoteStatusApplyResult> {
    apply_remote_status_inner(db, update, false, None, None).await
}

/// Applies a live worker observation with monotonic status protection for one request identity.
pub async fn apply_worker_remote_status<C: ConnectionTrait>(
    db: &C,
    update: RemoteStatusUpdate<'_>,
) -> AppResult<RemoteStatusApplyResult> {
    apply_remote_status_inner(db, update, true, None, None).await
}

/// A claimed Submit can follow its own queued response even when the allocation
/// route has since retired or changed strategy. This does not grant admission.
pub(crate) async fn apply_claimed_worker_remote_status<C: ConnectionTrait>(
    db: &C,
    update: RemoteStatusUpdate<'_>,
    job: &pin_job::Model,
) -> AppResult<RemoteStatusApplyResult> {
    apply_remote_status_inner(db, update, true, None, Some(job)).await
}

/// Applies a remote-scoped Reconcile observation only at the job's exact remote epoch.
///
/// Submit and Poll are target-scoped and must use [`apply_worker_remote_status`] instead.
pub async fn apply_reconcile_remote_status<C: ConnectionTrait>(
    db: &C,
    expected_remote_epoch: i64,
    update: RemoteStatusUpdate<'_>,
) -> AppResult<RemoteStatusApplyResult> {
    apply_remote_status_inner(db, update, true, Some(expected_remote_epoch), None).await
}

/// Internal worker path: the caller fences the claim in the same transaction.
/// Reconcile can transfer only an existing request's read-only follow-up.
pub(crate) async fn apply_claimed_reconcile_remote_status<C: ConnectionTrait>(
    db: &C,
    update: RemoteStatusUpdate<'_>,
    job: &pin_job::Model,
) -> AppResult<RemoteStatusApplyResult> {
    let Some(epoch) = job.expected_remote_epoch else {
        return Ok(RemoteStatusApplyResult::StaleRequest);
    };
    apply_remote_status_inner(db, update, true, Some(epoch), Some(job)).await
}

async fn apply_remote_status_inner<C: ConnectionTrait>(
    db: &C,
    update: RemoteStatusUpdate<'_>,
    enforce_monotonic_status: bool,
    expected_remote_epoch: Option<i64>,
    observation_job: Option<&pin_job::Model>,
) -> AppResult<RemoteStatusApplyResult> {
    for attempt in 0..REMOTE_STATUS_WRITE_RETRY_LIMIT {
        let snapshot =
            match ordered_desired_lifecycle_snapshot(db, update.provider, update.cid).await {
                Ok(OrderedDesiredLifecycleSnapshotOutcome::Current(snapshot)) => snapshot,
                Ok(OrderedDesiredLifecycleSnapshotOutcome::MissingRemote) => {
                    return Ok(RemoteStatusApplyResult::StaleRequest);
                }
                Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale) => {
                    retry_ordered_lifecycle_or_rollback(
                        db.get_database_backend(),
                        "remote status ordered lifecycle snapshot",
                    )?;
                    remote_status_retry_delay(attempt).await;
                    continue;
                }
                Err(error)
                    if db.get_database_backend() == DatabaseBackend::Sqlite
                        && is_sqlite_contention(&error.to_string()) =>
                {
                    remote_status_retry_delay(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };

        if expected_remote_epoch.is_some_and(|expected| snapshot.remote.epoch != expected) {
            return Ok(RemoteStatusApplyResult::StaleRequest);
        }
        if let Some(job) = observation_job
            && !super::ledger::submission::projection_allowed(db, job, &snapshot.remote).await?
        {
            return Ok(RemoteStatusApplyResult::StaleRequest);
        }

        #[cfg(test)]
        if attempt == 0 {
            pause_after_remote_status_read(update.provider, update.cid).await;
        }

        let (failure, remote) = match apply_remote_status_write(
            db,
            &update,
            &snapshot.remote,
            enforce_monotonic_status,
            snapshot
                .desired
                .iter()
                .any(|desired| desired.lease.provider_mode == "all"),
        )
        .await?
        {
            RemoteStatusWriteOutcome::Applied { failure, remote } => (failure, remote),
            RemoteStatusWriteOutcome::StaleRequest => {
                return Ok(RemoteStatusApplyResult::StaleRequest);
            }
            RemoteStatusWriteOutcome::Retry => {
                retry_ordered_lifecycle_or_rollback(
                    db.get_database_backend(),
                    "remote status compare-and-set",
                )?;
                remote_status_retry_delay(attempt).await;
                continue;
            }
        };

        let historical_poll = if matches!(remote.status.as_str(), REMOTE_QUEUED | REMOTE_PINNING)
            && remote.request_id.as_deref() == Some(update.request_id)
        {
            match observation_job {
                Some(job)
                    if job.operation != "submit" || update.origin == RemoteStatusOrigin::Adopt =>
                {
                    historical_poll_followup_allowed(db, job, &snapshot.desired, &remote).await?
                }
                _ => false,
            }
        } else {
            false
        };

        // The snapshot was locked in lease → target → remote order. Projection deliberately uses
        // only those rows, so it never reacquires a target after the remote status CAS.
        for desired in &snapshot.desired {
            project_prelocked_desired_target(
                db,
                desired,
                &snapshot.desired,
                &remote,
                ProjectionAccess::Observation { historical_poll },
                update.now,
            )
            .await?;
        }

        let projected_target_state = target_state_from_remote(&remote.status)
            .ok_or_else(|| invalid("remote status cannot be projected to a target state"))?;
        let observed_target_ids: BTreeSet<_> = snapshot
            .desired
            .iter()
            .map(|desired| desired.target.id.as_str())
            .collect();
        let availability: BTreeMap<_, _> =
            snapshot
                .frontier
                .iter()
                .fold(BTreeMap::new(), |mut availability, sibling| {
                    let state = if observed_target_ids.contains(sibling.target.id.as_str()) {
                        projected_target_state
                    } else {
                        sibling.target.state.as_str()
                    };
                    availability
                        .entry(sibling.lease.id.clone())
                        .and_modify(|available| *available |= state == TARGET_PINNED)
                        .or_insert(state == TARGET_PINNED);
                    availability
                });

        let mut affected = Vec::with_capacity(snapshot.desired.len());
        for desired in snapshot.desired {
            affected.push(AffectedLeaseOutcome {
                lease_id: desired.lease.id.clone(),
                target_id: desired.target.id,
                provider_mode: provider_mode(&desired.lease.provider_mode)?,
                available: availability
                    .get(&desired.lease.id)
                    .copied()
                    .unwrap_or(false),
            });
        }
        return Ok(RemoteStatusApplyResult::Applied {
            affected,
            failure,
            previous_status: snapshot.remote.status,
            current_status: remote.status,
        });
    }
    Err(AppError::Database(
        "remote status compare-and-set exhausted concurrent retries".to_owned(),
    ))
}

/// A claimed Submit's response can hand off its first read-only Poll to the
/// prelocked desired owner even if its original owner was cancelled. Reconcile
/// still requires an existing Poll proving the request. Neither grants admission
/// or crosses an account/credential/endpoint revision or an archived release.
async fn historical_poll_followup_allowed<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    desired: &[DesiredTarget],
    remote: &remote_pin::Model,
) -> AppResult<bool> {
    if job.provider != remote.provider
        || job.cid != remote.cid
        || remote.request_id.is_none()
        || desired.is_empty()
    {
        return Ok(false);
    }
    match job.operation.as_str() {
        "submit" if jobs::submit_names_original_target(job) => {}
        "reconcile"
            if job.expected_remote_epoch == Some(remote.epoch)
                && job.lease_id.is_none()
                && job.target_id.is_none()
                && job.expected_generation.is_none() => {}
        _ => return Ok(false),
    }
    let captured = pin_invocation_route::Entity::find_by_id(job.id.clone())
        .one(db)
        .await?;
    let ledger_route = ledger::get(db, &job.provider, &job.cid).await?;
    let configured = pin_provider_route::Entity::find_by_id(job.provider.clone())
        .one(db)
        .await?;
    let (Some(captured), Some(ledger_route), Some(configured)) =
        (captured, ledger_route, configured)
    else {
        return Ok(false);
    };
    if ledger_route.route.as_deref() != Some(captured.route.as_str())
        || captured.remote_epoch > remote.epoch
    {
        return Ok(false);
    }
    if job.operation == "reconcile"
        && (captured.remote_epoch != remote.epoch
            || !ledger::has_historical_poll_request(db, remote, &captured.route).await?)
    {
        return Ok(false);
    }
    let (Ok(historical), Ok(current)) = (
        serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(&captured.route),
        serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(
            &configured.snapshot,
        ),
    ) else {
        return Ok(false);
    };
    let mut compatible = historical.clone();
    compatible.api_profile = current.api_profile.clone();
    compatible.strategy = current.strategy.clone();
    compatible.cleanup = current.cleanup;
    if compatible != current {
        return Ok(false);
    }
    if captured.remote_epoch < remote.epoch
        && pin_resource_history::Entity::find()
            .filter(pin_resource_history::Column::Provider.eq(&job.provider))
            .filter(pin_resource_history::Column::Cid.eq(&job.cid))
            .filter(pin_resource_history::Column::Epoch.gte(captured.remote_epoch))
            .filter(pin_resource_history::Column::Epoch.lt(remote.epoch))
            .one(db)
            .await?
            .is_some()
    {
        return Ok(false);
    }
    Ok(true)
}

#[allow(clippy::large_enum_variant)]
enum RemoteStatusWriteOutcome {
    Applied {
        failure: Option<RemoteFailureProgress>,
        remote: remote_pin::Model,
    },
    StaleRequest,
    Retry,
}

const REMOTE_STATUS_WRITE_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleRetryPolicy {
    Retry,
    Rollback,
}

fn lifecycle_retry_policy(backend: DatabaseBackend) -> LifecycleRetryPolicy {
    if backend == DatabaseBackend::Postgres {
        LifecycleRetryPolicy::Rollback
    } else {
        LifecycleRetryPolicy::Retry
    }
}

fn retry_ordered_lifecycle_or_rollback(backend: DatabaseBackend, operation: &str) -> AppResult<()> {
    match lifecycle_retry_policy(backend) {
        LifecycleRetryPolicy::Retry => Ok(()),
        LifecycleRetryPolicy::Rollback => Err(AppError::Database(format!(
            "stale {operation}; PostgreSQL caller transaction must roll back before retry"
        ))),
    }
}

async fn apply_remote_status_write<C: ConnectionTrait>(
    db: &C,
    update: &RemoteStatusUpdate<'_>,
    remote: &remote_pin::Model,
    enforce_monotonic_status: bool,
    count_failed_request: bool,
) -> AppResult<RemoteStatusWriteOutcome> {
    let accepted_request_id = match update.origin {
        RemoteStatusOrigin::ExistingRequest
            if remote.request_id.as_deref() == Some(update.request_id) =>
        {
            remote.request_id.clone()
        }
        RemoteStatusOrigin::ExistingRequest => return Ok(RemoteStatusWriteOutcome::StaleRequest),
        RemoteStatusOrigin::Adopt if remote.request_id.is_none() => {
            Some(update.request_id.to_owned())
        }
        RemoteStatusOrigin::Adopt if remote.request_id.as_deref() == Some(update.request_id) => {
            remote.request_id.clone()
        }
        RemoteStatusOrigin::Adopt => return Ok(RemoteStatusWriteOutcome::StaleRequest),
    };

    if enforce_monotonic_status && !worker_status_transition_allowed(&remote.status, update.status)
    {
        return Ok(RemoteStatusWriteOutcome::StaleRequest);
    }

    let mut attempts = remote
        .failure_attempts
        .clamp(0, MAX_FAILED_REQUEST_ATTEMPTS);
    let mut next_retry_at = remote.next_retry_at;
    let mut last_failed_request_id = remote.last_failed_request_id.clone();
    let mut last_error_class = update.error_class.map(str::to_owned);
    let mut last_error_text = update.error_text.map(str::to_owned);
    let mut failure = None;
    match update.status {
        RemotePinStatus::Failed => {
            if count_failed_request {
                let newly_counted = last_failed_request_id.as_deref() != Some(update.request_id);
                if newly_counted {
                    attempts = attempts.saturating_add(1).min(MAX_FAILED_REQUEST_ATTEMPTS);
                    last_failed_request_id = Some(update.request_id.to_owned());
                    next_retry_at = if attempts >= MAX_FAILED_REQUEST_ATTEMPTS {
                        None
                    } else {
                        Some(update.now + duration_as_chrono(failure_backoff(attempts))?)
                    };
                } else {
                    last_error_class = last_error_class.or(remote.last_error_class.clone());
                    last_error_text = last_error_text.or(remote.last_error_text.clone());
                }
                failure = Some(RemoteFailureProgress {
                    attempts,
                    next_retry_at,
                    newly_counted,
                    exhausted: attempts >= MAX_FAILED_REQUEST_ATTEMPTS,
                });
            }
        }
        RemotePinStatus::Queued | RemotePinStatus::Pinning => {
            next_retry_at = None;
        }
        RemotePinStatus::Pinned => {
            attempts = 0;
            next_retry_at = None;
            last_failed_request_id = None;
            last_error_class = None;
            last_error_text = None;
        }
    }

    let mut persisted = remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::RequestId,
            Expr::value(accepted_request_id.clone()),
        )
        .col_expr(
            remote_pin::Column::Status,
            Expr::value(remote_status_name(update.status).to_owned()),
        )
        .col_expr(remote_pin::Column::FailureAttempts, Expr::value(attempts))
        .col_expr(remote_pin::Column::NextRetryAt, Expr::value(next_retry_at))
        .col_expr(
            remote_pin::Column::LastFailedRequestId,
            Expr::value(last_failed_request_id.clone()),
        )
        .col_expr(remote_pin::Column::LastTouchedAt, Expr::value(update.now))
        .col_expr(
            remote_pin::Column::LastErrorClass,
            Expr::value(last_error_class.clone()),
        )
        .col_expr(
            remote_pin::Column::LastErrorText,
            Expr::value(last_error_text.clone()),
        )
        .filter(remote_pin::Column::Provider.eq(update.provider))
        .filter(remote_pin::Column::Cid.eq(update.cid))
        .filter(remote_pin::Column::Epoch.eq(remote.epoch))
        .filter(remote_pin::Column::Status.eq(&remote.status))
        .filter(remote_pin::Column::FailureAttempts.eq(remote.failure_attempts));
    persisted = match remote.request_id.as_deref() {
        Some(request_id) => persisted.filter(remote_pin::Column::RequestId.eq(request_id)),
        None => persisted.filter(remote_pin::Column::RequestId.is_null()),
    };
    persisted = match remote.last_failed_request_id.as_deref() {
        Some(request_id) => {
            persisted.filter(remote_pin::Column::LastFailedRequestId.eq(request_id))
        }
        None => persisted.filter(remote_pin::Column::LastFailedRequestId.is_null()),
    };
    persisted = match remote.next_retry_at {
        Some(due) => persisted.filter(remote_pin::Column::NextRetryAt.eq(due)),
        None => persisted.filter(remote_pin::Column::NextRetryAt.is_null()),
    };
    persisted = match remote.last_error_class.as_deref() {
        Some(error_class) => persisted.filter(remote_pin::Column::LastErrorClass.eq(error_class)),
        None => persisted.filter(remote_pin::Column::LastErrorClass.is_null()),
    };
    persisted = match remote.last_error_text.as_deref() {
        Some(error_text) => persisted.filter(remote_pin::Column::LastErrorText.eq(error_text)),
        None => persisted.filter(remote_pin::Column::LastErrorText.is_null()),
    };
    #[cfg(test)]
    record_remote_status_write(update.provider, update.cid).await;

    match persisted.exec(db).await {
        Ok(updated) if updated.rows_affected == 1 => {
            let mut updated_remote = remote.clone();
            updated_remote.request_id = accepted_request_id;
            updated_remote.status = remote_status_name(update.status).to_owned();
            updated_remote.failure_attempts = attempts;
            updated_remote.next_retry_at = next_retry_at;
            updated_remote.last_failed_request_id = last_failed_request_id;
            updated_remote.last_touched_at = update.now;
            updated_remote.last_error_class = last_error_class;
            updated_remote.last_error_text = last_error_text;
            Ok(RemoteStatusWriteOutcome::Applied {
                failure,
                remote: updated_remote,
            })
        }
        Ok(_) => Ok(RemoteStatusWriteOutcome::Retry),
        Err(error) if is_sqlite_contention(&error.to_string()) => {
            Ok(RemoteStatusWriteOutcome::Retry)
        }
        Err(error) => Err(error.into()),
    }
}

fn worker_status_transition_allowed(current: &str, observed: RemotePinStatus) -> bool {
    match current {
        REMOTE_RESERVED => true,
        // An absent row has already released local quota. Only the reservation path may
        // reactivate it and account capacity before a new Submit is projected.
        REMOTE_ABSENT => false,
        REMOTE_QUEUED => true,
        REMOTE_PINNING => observed != RemotePinStatus::Queued,
        REMOTE_PINNED => observed == RemotePinStatus::Pinned,
        REMOTE_FAILED => observed == RemotePinStatus::Failed,
        _ => false,
    }
}

fn is_sqlite_contention(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("database is locked") || message.contains("database is busy")
}

async fn remote_status_retry_delay(attempt: usize) {
    let milliseconds = 1_u64.checked_shl(attempt.min(4) as u32).unwrap_or(16);
    tokio::time::sleep(Duration::from_millis(milliseconds)).await;
}

/// Completes a successful or not-found provider DELETE without releasing a reused reservation.
pub async fn complete_remote_delete<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    now: DateTimeUtc,
) -> AppResult<RemoteDeleteCompletion> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Err(invalid("remote pin does not exist"));
    };
    let mut current_refs = desired_targets(db, provider, cid).await?;

    #[cfg(test)]
    pause_after_delete_ref_snapshot(provider, cid).await;

    if remote.epoch == expected_remote_epoch && current_refs.is_empty() {
        let deleted_request_id = remote.request_id.as_deref();
        if let NoRequestSubmitAmbiguity::Wait { next_check_at } =
            jobs::resolve_no_request_submit_ambiguity(db, provider, cid, now).await?
            && let Some(completion) = defer_delete_for_submit_ambiguity(
                db,
                provider,
                cid,
                expected_remote_epoch,
                deleted_request_id,
                next_check_at,
                now,
            )
            .await?
        {
            return Ok(completion);
        }

        current_refs = desired_targets(db, provider, cid).await?;
        let guarded_remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await?
            .ok_or_else(|| invalid("remote disappeared during delete completion"))?;
        if current_refs.is_empty()
            && guarded_remote.epoch == expected_remote_epoch
            && guarded_remote.request_id.as_deref() == deleted_request_id
            && let NoRequestSubmitAmbiguity::Wait { next_check_at } =
                jobs::resolve_no_request_submit_ambiguity(db, provider, cid, now).await?
            && let Some(completion) = defer_delete_for_submit_ambiguity(
                db,
                provider,
                cid,
                expected_remote_epoch,
                deleted_request_id,
                next_check_at,
                now,
            )
            .await?
        {
            return Ok(completion);
        }

        current_refs = desired_targets(db, provider, cid).await?;
        let guarded_remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await?
            .ok_or_else(|| invalid("remote disappeared during delete completion"))?;
        if current_refs.is_empty()
            && guarded_remote.epoch == expected_remote_epoch
            && guarded_remote.request_id.as_deref() == deleted_request_id
        {
            match quota::confirmed_release_for_request(
                db,
                provider,
                cid,
                expected_remote_epoch,
                deleted_request_id,
                now,
            )
            .await?
            {
                ConfirmedReleaseOutcome::Released | ConfirmedReleaseOutcome::AlreadyAbsent => {
                    mark_terminal_targets_released(db, provider, cid).await?;
                    return Ok(RemoteDeleteCompletion::Released);
                }
                ConfirmedReleaseOutcome::Stale => {
                    current_refs = desired_targets(db, provider, cid).await?;
                }
            }
        }
    }

    if current_refs.is_empty() {
        let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await?
            .ok_or_else(|| invalid("remote disappeared during delete completion"))?;
        let reconcile_job_id = ensure_reconcile(db, provider, cid, remote.epoch, now).await?;
        return Ok(RemoteDeleteCompletion::ReconcileRequired { reconcile_job_id });
    }

    // A DELETE with live references is compensation, not release. Refresh the desired snapshot
    // until its ordered lifecycle acquisition is current, then mutate the remote before projecting
    // only the already-locked targets.
    for attempt in 0..REMOTE_STATUS_WRITE_RETRY_LIMIT {
        let snapshot = match ordered_desired_lifecycle_snapshot(db, provider, cid).await? {
            OrderedDesiredLifecycleSnapshotOutcome::Current(snapshot) => snapshot,
            OrderedDesiredLifecycleSnapshotOutcome::MissingRemote => {
                return Err(invalid("remote disappeared during delete completion"));
            }
            OrderedDesiredLifecycleSnapshotOutcome::Stale => {
                retry_ordered_lifecycle_or_rollback(
                    db.get_database_backend(),
                    "remote delete ordered lifecycle snapshot",
                )?;
                remote_status_retry_delay(attempt).await;
                continue;
            }
        };
        if snapshot.desired.is_empty() {
            let reconcile_job_id =
                ensure_reconcile(db, provider, cid, snapshot.remote.epoch, now).await?;
            return Ok(RemoteDeleteCompletion::ReconcileRequired { reconcile_job_id });
        }

        // A DELETE invalidates the persisted request even when a desired reference changed the
        // epoch after the initial zero-reference snapshot. The newer epoch is the compensation
        // guard and remains reserved for the current references.
        let current_epoch = if snapshot.remote.epoch == expected_remote_epoch {
            increment_epoch(snapshot.remote.epoch)?
        } else {
            snapshot.remote.epoch
        };
        #[cfg(test)]
        record_remote_compensation(provider, cid).await;

        let mut compensation = remote_pin::Entity::update_many()
            .col_expr(
                remote_pin::Column::RequestId,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                remote_pin::Column::Status,
                Expr::value(REMOTE_RESERVED.to_owned()),
            )
            .col_expr(remote_pin::Column::Epoch, Expr::value(current_epoch))
            .col_expr(remote_pin::Column::LastTouchedAt, Expr::value(now))
            .filter(remote_pin::Column::Provider.eq(provider))
            .filter(remote_pin::Column::Cid.eq(cid))
            .filter(remote_pin::Column::Epoch.eq(snapshot.remote.epoch))
            .filter(remote_pin::Column::Status.eq(&snapshot.remote.status));
        compensation = match snapshot.remote.request_id.as_deref() {
            Some(request_id) => compensation.filter(remote_pin::Column::RequestId.eq(request_id)),
            None => compensation.filter(remote_pin::Column::RequestId.is_null()),
        };
        if compensation.exec(db).await?.rows_affected != 1 {
            retry_ordered_lifecycle_or_rollback(
                db.get_database_backend(),
                "remote delete compensation compare-and-set",
            )?;
            remote_status_retry_delay(attempt).await;
            continue;
        }

        for desired in &snapshot.desired {
            #[cfg(test)]
            record_target_projection(&desired.target.id).await;

            set_prelocked_target_state(db, &desired.target, TARGET_WAITING).await?;
        }
        let canonical = snapshot
            .desired
            .first()
            .ok_or_else(|| invalid("active delete compensation has no canonical target"))?;
        let NewPinJob::Target(job) = jobs::submit_job(
            provider,
            cid,
            &canonical.lease.id,
            &canonical.target.id,
            canonical.lease.generation,
            now,
        ) else {
            unreachable!("submit constructor is target scoped")
        };
        let submit_job_id = job.id.clone();
        jobs::ensure_or_reactivate_submit_job(db, job, now).await?;
        return Ok(RemoteDeleteCompletion::Compensated { submit_job_id });
    }
    Err(AppError::Database(
        "remote delete compensation exhausted concurrent retries".to_owned(),
    ))
}

async fn defer_delete_for_submit_ambiguity<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    deleted_request_id: Option<&str>,
    next_check_at: DateTimeUtc,
    now: DateTimeUtc,
) -> AppResult<Option<RemoteDeleteCompletion>> {
    if !desired_targets(db, provider, cid).await?.is_empty() {
        return Ok(None);
    }
    let current = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
        .ok_or_else(|| invalid("remote disappeared during delete ambiguity guard"))?;
    if current.epoch != expected_remote_epoch || current.request_id.as_deref() != deleted_request_id
    {
        let reconcile_job_id = ensure_reconcile(db, provider, cid, current.epoch, now).await?;
        return Ok(Some(RemoteDeleteCompletion::ReconcileRequired {
            reconcile_job_id,
        }));
    }

    let next_epoch = increment_epoch(current.epoch)?;
    let mut cleared = remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::RequestId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            remote_pin::Column::Status,
            Expr::value(REMOTE_RESERVED.to_owned()),
        )
        .col_expr(remote_pin::Column::Epoch, Expr::value(next_epoch))
        .col_expr(remote_pin::Column::LastTouchedAt, Expr::value(now))
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Cid.eq(cid))
        .filter(remote_pin::Column::Epoch.eq(expected_remote_epoch))
        .filter(remote_pin::Column::Status.eq(&current.status));
    cleared = match deleted_request_id {
        Some(request_id) => cleared.filter(remote_pin::Column::RequestId.eq(request_id)),
        None => cleared.filter(remote_pin::Column::RequestId.is_null()),
    };
    if cleared.exec(db).await?.rows_affected != 1 {
        if !desired_targets(db, provider, cid).await?.is_empty() {
            return Ok(None);
        }
        let current = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await?
            .ok_or_else(|| invalid("remote disappeared during delete ambiguity retry"))?;
        let reconcile_job_id = ensure_reconcile(db, provider, cid, current.epoch, now).await?;
        return Ok(Some(RemoteDeleteCompletion::ReconcileRequired {
            reconcile_job_id,
        }));
    }

    let reconcile_job_id =
        ensure_reconcile(db, provider, cid, next_epoch, next_check_at.max(now)).await?;
    Ok(Some(RemoteDeleteCompletion::ReconcileRequired {
        reconcile_job_id,
    }))
}

/// Releases only after a current no-request Reconcile resolves Submit ambiguity.
pub async fn complete_no_request_remote_absence<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    now: DateTimeUtc,
) -> AppResult<NoRequestRemoteCompletion> {
    if !guard_reconcile_remote_epoch(db, provider, cid, expected_remote_epoch).await?
        || !matches!(
            check_remote_epoch(db, provider, cid, expected_remote_epoch).await?,
            GenerationDecision::Current
        )
        || !desired_targets(db, provider, cid).await?.is_empty()
    {
        return Ok(NoRequestRemoteCompletion::Stale);
    }
    let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
        .expect("checked by check_remote_epoch");
    if remote.request_id.is_some() {
        return Ok(NoRequestRemoteCompletion::Stale);
    }
    if let NoRequestSubmitAmbiguity::Wait { next_check_at } =
        jobs::resolve_no_request_submit_ambiguity(db, provider, cid, now).await?
    {
        return Ok(NoRequestRemoteCompletion::Wait { next_check_at });
    }

    // Recheck after cancelling safe jobs; a concurrent Submit may have started meanwhile.
    if !matches!(
        check_remote_epoch(db, provider, cid, expected_remote_epoch).await?,
        GenerationDecision::Current
    ) || !desired_targets(db, provider, cid).await?.is_empty()
    {
        return Ok(NoRequestRemoteCompletion::Stale);
    }
    let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
        .expect("checked by check_remote_epoch");
    if remote.request_id.is_some() {
        return Ok(NoRequestRemoteCompletion::Stale);
    }
    if let NoRequestSubmitAmbiguity::Wait { next_check_at } =
        jobs::resolve_no_request_submit_ambiguity(db, provider, cid, now).await?
    {
        return Ok(NoRequestRemoteCompletion::Wait { next_check_at });
    }

    match quota::confirmed_release_for_request(db, provider, cid, expected_remote_epoch, None, now)
        .await?
    {
        ConfirmedReleaseOutcome::Released | ConfirmedReleaseOutcome::AlreadyAbsent => {
            Ok(NoRequestRemoteCompletion::Released)
        }
        ConfirmedReleaseOutcome::Stale => Ok(NoRequestRemoteCompletion::Stale),
    }
}

/// Ensures the one shared all-mode failed-request retry is due at its persisted time.
pub async fn ensure_failed_remote_retry<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    _now: DateTimeUtc,
) -> AppResult<FailedRemoteRetryDecision> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(FailedRemoteRetryDecision::NotNeeded);
    };
    if remote.status != REMOTE_FAILED || !has_all_mode_target(db, provider, cid).await? {
        return Ok(FailedRemoteRetryDecision::NotNeeded);
    }
    if remote.failure_attempts >= MAX_FAILED_REQUEST_ATTEMPTS {
        return Ok(FailedRemoteRetryDecision::Exhausted);
    }
    let Some(at) = remote.next_retry_at else {
        return Ok(FailedRemoteRetryDecision::NotNeeded);
    };
    // An old request may still report failure after its route is retired or changed.
    // Record that observation, but do not schedule a fresh attempt on its behalf.
    match quota::assert_reusable_route(db, provider, cid).await {
        Ok(()) => {}
        Err(AppError::InvalidPinningRequest(_)) => {
            return Ok(FailedRemoteRetryDecision::NotNeeded);
        }
        Err(error) => return Err(error),
    }
    let reconcile_job_id = ensure_reconcile(db, provider, cid, remote.epoch, at).await?;
    Ok(FailedRemoteRetryDecision::Scheduled {
        reconcile_job_id,
        at,
    })
}

/// Explicitly restarts a failed shared retry budget after a user desired-set touch.
pub async fn reset_failed_remote_retry_on_user_touch<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTimeUtc,
) -> AppResult<FailedRemoteRetryDecision> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(FailedRemoteRetryDecision::NotNeeded);
    };
    if remote.status != REMOTE_FAILED {
        return Ok(FailedRemoteRetryDecision::NotNeeded);
    }
    let next_retry_at = now + duration_as_chrono(FAILED_REQUEST_BASE_BACKOFF)?;
    remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::FailureAttempts, Expr::value(0_i32))
        .col_expr(
            remote_pin::Column::NextRetryAt,
            Expr::value(Some(next_retry_at)),
        )
        .col_expr(remote_pin::Column::LastTouchedAt, Expr::value(now))
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Cid.eq(cid))
        .filter(remote_pin::Column::Epoch.eq(remote.epoch))
        .filter(remote_pin::Column::Status.eq(REMOTE_FAILED))
        .exec(db)
        .await?;
    ensure_failed_remote_retry(db, provider, cid, now).await
}

/// Converts one due, current failed request into a fresh canonical Submit without releasing quota.
pub async fn prepare_failed_remote_resubmit<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    expected_request_id: &str,
    now: DateTimeUtc,
) -> AppResult<FailedRemoteResubmitDecision> {
    let snapshot = match ordered_desired_lifecycle_snapshot(db, provider, cid).await? {
        OrderedDesiredLifecycleSnapshotOutcome::Current(snapshot) => snapshot,
        OrderedDesiredLifecycleSnapshotOutcome::Stale
        | OrderedDesiredLifecycleSnapshotOutcome::MissingRemote => {
            return Ok(FailedRemoteResubmitDecision::Stale);
        }
    };
    let remote = &snapshot.remote;
    if remote.epoch != expected_remote_epoch
        || remote.status != REMOTE_FAILED
        || remote.request_id.as_deref() != Some(expected_request_id)
        || remote.next_retry_at.is_none_or(|due| due > now)
    {
        return Ok(FailedRemoteResubmitDecision::Stale);
    }
    if remote.failure_attempts >= MAX_FAILED_REQUEST_ATTEMPTS {
        return Ok(FailedRemoteResubmitDecision::Exhausted);
    }
    if !snapshot
        .desired
        .iter()
        .any(|desired| desired.lease.provider_mode == "all")
    {
        return Ok(FailedRemoteResubmitDecision::NoAllModeTarget);
    }
    // A historical Reconcile may inspect its captured request, but a new
    // Submit needs today's matching allocation route.
    quota::assert_reusable_route(db, provider, cid).await?;

    let new_epoch = increment_epoch(remote.epoch)?;

    #[cfg(test)]
    record_remote_resubmit(provider, cid).await;

    let mut updated = remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::RequestId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            remote_pin::Column::Status,
            Expr::value(REMOTE_RESERVED.to_owned()),
        )
        .col_expr(remote_pin::Column::Epoch, Expr::value(new_epoch))
        .col_expr(
            remote_pin::Column::NextRetryAt,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            remote_pin::Column::LastErrorClass,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            remote_pin::Column::LastErrorText,
            Expr::value(Option::<String>::None),
        )
        .col_expr(remote_pin::Column::LastTouchedAt, Expr::value(now))
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Cid.eq(cid))
        .filter(remote_pin::Column::Epoch.eq(remote.epoch))
        .filter(remote_pin::Column::Status.eq(REMOTE_FAILED))
        .filter(remote_pin::Column::FailureAttempts.eq(remote.failure_attempts));
    updated = match remote.request_id.as_deref() {
        Some(request_id) => updated.filter(remote_pin::Column::RequestId.eq(request_id)),
        None => updated.filter(remote_pin::Column::RequestId.is_null()),
    };
    updated = match remote.last_failed_request_id.as_deref() {
        Some(request_id) => updated.filter(remote_pin::Column::LastFailedRequestId.eq(request_id)),
        None => updated.filter(remote_pin::Column::LastFailedRequestId.is_null()),
    };
    updated = match remote.next_retry_at {
        Some(due) => updated.filter(remote_pin::Column::NextRetryAt.eq(due)),
        None => updated.filter(remote_pin::Column::NextRetryAt.is_null()),
    };
    updated = match remote.last_error_class.as_deref() {
        Some(error_class) => updated.filter(remote_pin::Column::LastErrorClass.eq(error_class)),
        None => updated.filter(remote_pin::Column::LastErrorClass.is_null()),
    };
    updated = match remote.last_error_text.as_deref() {
        Some(error_text) => updated.filter(remote_pin::Column::LastErrorText.eq(error_text)),
        None => updated.filter(remote_pin::Column::LastErrorText.is_null()),
    };
    if updated.exec(db).await?.rows_affected != 1 {
        return Ok(FailedRemoteResubmitDecision::Stale);
    }
    mark_polls_for_request_done(db, provider, cid, expected_request_id, now).await?;
    for desired in snapshot
        .desired
        .iter()
        .filter(|desired| desired.lease.provider_mode == "all")
    {
        #[cfg(test)]
        record_target_projection(&desired.target.id).await;

        set_prelocked_target_state(db, &desired.target, TARGET_WAITING).await?;
    }
    let canonical = snapshot
        .desired
        .iter()
        .find(|desired| desired.lease.provider_mode == "all")
        .ok_or_else(|| invalid("failed remote has no canonical all-mode target"))?;
    let NewPinJob::Target(job) = jobs::submit_job(
        provider,
        cid,
        &canonical.lease.id,
        &canonical.target.id,
        canonical.lease.generation,
        now,
    ) else {
        unreachable!("submit constructor is target scoped")
    };
    let submit_job_id = job.id.clone();
    jobs::ensure_or_reactivate_submit_job(db, job, now).await?;
    Ok(FailedRemoteResubmitDecision::Prepared {
        new_epoch,
        submit_job_id,
    })
}

/// Renews only a manual lease still owned by the supplied immutable latest object.
///
/// The caller must pass its transaction when renewal is coupled with other state changes. This
/// function deliberately never opens, commits, or rolls back an independent transaction.
pub async fn renew_manual_lease<C: ConnectionTrait>(
    db: &C,
    owner_object_id: &str,
    lease_id: &str,
    retain_until: DateTimeUtc,
    now: DateTimeUtc,
) -> Result<ManualLeaseRenewalOutcome, RenewManualLeaseError> {
    renew_manual_lease_for_owner(
        db,
        owner_object_id,
        lease_id,
        retain_until,
        now,
        ManualLeaseOwnerScope::Latest,
    )
    .await
}

/// Renews a manual lease owned by an exact retained version.
///
/// The caller must lock and revalidate the exact public version identity in the same transaction.
pub(crate) async fn renew_retained_version_manual_lease<C: ConnectionTrait>(
    db: &C,
    owner_object_id: &str,
    lease_id: &str,
    retain_until: DateTimeUtc,
    now: DateTimeUtc,
) -> Result<ManualLeaseRenewalOutcome, RenewManualLeaseError> {
    renew_manual_lease_for_owner(
        db,
        owner_object_id,
        lease_id,
        retain_until,
        now,
        ManualLeaseOwnerScope::RetainedVersion,
    )
    .await
}

async fn renew_manual_lease_for_owner<C: ConnectionTrait>(
    db: &C,
    owner_object_id: &str,
    lease_id: &str,
    retain_until: DateTimeUtc,
    now: DateTimeUtc,
    owner_scope: ManualLeaseOwnerScope,
) -> Result<ManualLeaseRenewalOutcome, RenewManualLeaseError> {
    let snapshot = renewal_snapshot(db, owner_object_id, lease_id, owner_scope).await?;

    #[cfg(test)]
    pause_after_renewal_snapshot(lease_id).await;

    let lease = &snapshot.lease;
    if lease.source != MANUAL_SOURCE {
        return Err(RenewManualLeaseError::InvalidState);
    }
    if lease.owner_object_id != owner_object_id
        || (owner_scope == ManualLeaseOwnerScope::Latest && !snapshot.owner.is_latest)
    {
        return Err(RenewManualLeaseError::NotLatestOwner);
    }
    match lease.state.as_str() {
        LEASE_ACTIVE => {
            if retain_until == lease.expires_at {
                #[cfg(test)]
                stage_renewal_owner_before_guard(db, lease_id, &snapshot.owner)
                    .await
                    .map_err(app_to_renewal_error)?;
                guard_renewal_owner(db, &snapshot.owner, owner_scope).await?;
                return Ok(ManualLeaseRenewalOutcome::Kept {
                    generation: lease.generation,
                });
            }
            if retain_until < lease.expires_at {
                return Err(RenewManualLeaseError::InvalidState);
            }
            revalidate_renewal_snapshot(db, &snapshot).await?;
            #[cfg(test)]
            stage_renewal_owner_before_guard(db, lease_id, &snapshot.owner)
                .await
                .map_err(app_to_renewal_error)?;
            guard_renewal_owner(db, &snapshot.owner, owner_scope).await?;
            let generation = advance_active_manual_lease(db, lease, retain_until, now).await?;
            let targets: Vec<_> = snapshot
                .targets
                .iter()
                .filter(|target| is_desired_target_state(&target.state))
                .cloned()
                .collect();
            refresh_renewal_snapshot_targets(db, &snapshot, generation, &targets, now).await?;
            return Ok(ManualLeaseRenewalOutcome::Extended { generation });
        }
        LEASE_EXPIRED => {}
        LEASE_CANCELLED | LEASE_EVICTED => return Err(RenewManualLeaseError::InvalidState),
        _ => return Err(RenewManualLeaseError::InvalidState),
    }

    let recoverable: Vec<_> = snapshot
        .targets
        .iter()
        .filter(|target| target_recoverable_from_snapshot(&snapshot, target))
        .cloned()
        .collect();
    if recoverable.is_empty() {
        return Err(RenewManualLeaseError::NoRecoverableReservation);
    }
    revalidate_renewal_snapshot(db, &snapshot).await?;
    #[cfg(test)]
    stage_renewal_owner_before_guard(db, lease_id, &snapshot.owner)
        .await
        .map_err(app_to_renewal_error)?;
    guard_renewal_owner(db, &snapshot.owner, owner_scope).await?;
    let generation = reactivate_expired_manual_lease(db, lease, retain_until, now).await?;
    let restored_target_ids = restore_snapshot_targets(db, &snapshot, &recoverable, now).await?;
    if restored_target_ids.is_empty() {
        return Err(RenewManualLeaseError::NoRecoverableReservation);
    }
    refresh_renewal_snapshot_targets(db, &snapshot, generation, &recoverable, now).await?;
    Ok(ManualLeaseRenewalOutcome::Reactivated {
        generation,
        restored_target_ids,
    })
}

#[derive(Clone)]
struct RenewalSnapshot {
    owner: object::Model,
    lease: pin_lease::Model,
    /// All rows owned by the renewed lease, including released targets that can be restored.
    targets: Vec<pin_lease_target::Model>,
    /// The complete lease/target lifecycle frontier needed to choose a canonical shared target
    /// for every provider/CID touched by the renewed lease.
    lifecycle_leases: BTreeMap<String, pin_lease::Model>,
    lifecycle_targets: Vec<pin_lease_target::Model>,
    remotes: std::collections::BTreeMap<(String, String), remote_pin::Model>,
    usage: std::collections::BTreeMap<
        String,
        Option<crate::store::entities::pin_provider_usage::Model>,
    >,
    unpin_jobs: Vec<pin_job::Model>,
}

fn renewal_owner_condition(expected: &object::Model) -> Condition {
    Condition::all()
        .add(object::Column::Id.eq(&expected.id))
        .add(object::Column::Bucket.eq(&expected.bucket))
        .add(object::Column::Key.eq(&expected.key))
        .add(object::Column::Cid.eq(&expected.cid))
        .add(object::Column::Size.eq(expected.size))
        .add(object::Column::Etag.eq(&expected.etag))
        .add(object::Column::Encrypted.eq(expected.encrypted))
        .add(object::Column::Multipart.eq(expected.multipart))
        .add(object::Column::IsLatest.eq(expected.is_latest))
}

fn renewal_owner_query(expected: &object::Model) -> sea_orm::Select<object::Entity> {
    object::Entity::find_by_id(expected.id.clone())
        .filter(object::Column::IsLatest.eq(expected.is_latest))
}

fn renewal_owner_lock_query(expected: &object::Model) -> sea_orm::Select<object::Entity> {
    renewal_owner_query(expected).lock_exclusive()
}

/// Acquires the owner before any lifecycle lock. PostgreSQL retains this exact `FOR UPDATE`
/// lock in the caller-owned transaction; SQLite takes its serialized owner guard only at the
/// final success point, before any lease/target/remote mutation.
async fn lock_renewal_owner<C: ConnectionTrait>(
    db: &C,
    expected: &object::Model,
    owner_scope: ManualLeaseOwnerScope,
) -> Result<object::Model, RenewManualLeaseError> {
    #[cfg(test)]
    record_owner_lock(&expected.id).await;

    let owner = if db.get_database_backend() == DatabaseBackend::Postgres {
        renewal_owner_lock_query(expected).one(db).await?
    } else {
        renewal_owner_query(expected).one(db).await?
    };
    if let Some(owner) = owner {
        return if owner == *expected {
            Ok(owner)
        } else {
            Err(stale_renewal_error())
        };
    }

    // This check is still before lifecycle locking, so it cannot invert owner → lease ordering.
    // Later paths must use `guard_renewal_owner` only, never acquire this row again.
    if owner_scope == ManualLeaseOwnerScope::Latest
        && object::Entity::find_by_id(expected.id.clone())
            .one(db)
            .await?
            .is_some_and(|owner| !owner.is_latest)
    {
        Err(RenewManualLeaseError::NotLatestOwner)
    } else {
        Err(stale_renewal_error())
    }
}

/// Performs the portable exact-owner compare-and-set after revalidation and immediately before
/// each successful renewal mutation. Latest-owner callers additionally require `is_latest=true`.
/// It intentionally uses the caller's connection and never opens a transaction or reacquires the
/// owner after lifecycle locks.
async fn guard_renewal_owner<C: ConnectionTrait>(
    db: &C,
    expected: &object::Model,
    owner_scope: ManualLeaseOwnerScope,
) -> Result<(), RenewManualLeaseError> {
    if owner_scope == ManualLeaseOwnerScope::Latest && !expected.is_latest {
        return Err(RenewManualLeaseError::NotLatestOwner);
    }

    #[cfg(test)]
    record_owner_guard(&expected.id).await;

    let guarded = object::Entity::update_many()
        .col_expr(object::Column::IsLatest, Expr::value(expected.is_latest))
        .filter(renewal_owner_condition(expected))
        .exec(db)
        .await?;
    if guarded.rows_affected == 1 {
        Ok(())
    } else {
        Err(RenewManualLeaseError::NotLatestOwner)
    }
}

async fn advance_active_manual_lease<C: ConnectionTrait>(
    db: &C,
    lease: &pin_lease::Model,
    retain_until: DateTimeUtc,
    now: DateTimeUtc,
) -> Result<i64, RenewManualLeaseError> {
    let generation = increment_epoch(lease.generation).map_err(app_to_renewal_error)?;

    #[cfg(test)]
    record_lease_cas(&lease.id).await;

    let updated = pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Generation, Expr::value(generation))
        .col_expr(pin_lease::Column::ExpiresAt, Expr::value(retain_until))
        .col_expr(pin_lease::Column::LastTouchedAt, Expr::value(now))
        .filter(pin_lease::Column::Id.eq(&lease.id))
        .filter(pin_lease::Column::OwnerObjectId.eq(&lease.owner_object_id))
        .filter(pin_lease::Column::Source.eq(&lease.source))
        .filter(pin_lease::Column::Generation.eq(lease.generation))
        .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
        .filter(pin_lease::Column::ExpiresAt.eq(lease.expires_at))
        .filter(pin_lease::Column::LastTouchedAt.eq(lease.last_touched_at))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(stale_renewal_error());
    }
    Ok(generation)
}

async fn reactivate_expired_manual_lease<C: ConnectionTrait>(
    db: &C,
    lease: &pin_lease::Model,
    retain_until: DateTimeUtc,
    now: DateTimeUtc,
) -> Result<i64, RenewManualLeaseError> {
    let generation = increment_epoch(lease.generation).map_err(app_to_renewal_error)?;

    #[cfg(test)]
    record_lease_cas(&lease.id).await;

    let updated = pin_lease::Entity::update_many()
        .col_expr(
            pin_lease::Column::State,
            Expr::value(LEASE_ACTIVE.to_owned()),
        )
        .col_expr(pin_lease::Column::Generation, Expr::value(generation))
        .col_expr(pin_lease::Column::ExpiresAt, Expr::value(retain_until))
        .col_expr(pin_lease::Column::LastTouchedAt, Expr::value(now))
        .filter(pin_lease::Column::Id.eq(&lease.id))
        .filter(pin_lease::Column::OwnerObjectId.eq(&lease.owner_object_id))
        .filter(pin_lease::Column::Source.eq(&lease.source))
        .filter(pin_lease::Column::Generation.eq(lease.generation))
        .filter(pin_lease::Column::State.eq(LEASE_EXPIRED))
        .filter(pin_lease::Column::ExpiresAt.eq(lease.expires_at))
        .filter(pin_lease::Column::LastTouchedAt.eq(lease.last_touched_at))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(stale_renewal_error());
    }
    Ok(generation)
}

async fn renewal_snapshot<C: ConnectionTrait>(
    db: &C,
    owner_object_id: &str,
    lease_id: &str,
    owner_scope: ManualLeaseOwnerScope,
) -> Result<RenewalSnapshot, RenewManualLeaseError> {
    let Some(owner) = object::Entity::find_by_id(owner_object_id.to_owned())
        .one(db)
        .await?
    else {
        return Err(RenewManualLeaseError::NotLatestOwner);
    };
    if owner_scope == ManualLeaseOwnerScope::Latest && !owner.is_latest {
        return Err(RenewManualLeaseError::NotLatestOwner);
    }
    let Some(lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Err(RenewManualLeaseError::NotLatestOwner);
    };
    let own_targets = lease_targets(db, lease_id)
        .await
        .map_err(app_to_renewal_error)?;
    let pairs: BTreeSet<_> = own_targets
        .iter()
        .map(|target| (target.provider.clone(), target.cid.clone()))
        .collect();

    // Discover every shared active reference before acquiring any lifecycle lock. The subsequent
    // frontier includes all rows of each discovered lease so an all-mode sibling on another
    // provider cannot race canonical/availability decisions made for this renewal.
    let mut discovered_leases = BTreeMap::from([(lease.id.clone(), lease.clone())]);
    for (provider, cid) in &pairs {
        for desired in desired_targets(db, provider, cid)
            .await
            .map_err(app_to_renewal_error)?
        {
            discovered_leases.insert(desired.lease.id.clone(), desired.lease);
        }
    }
    let lifecycle_lease_ids: Vec<_> = discovered_leases.keys().cloned().collect();
    let mut discovered_targets = if lifecycle_lease_ids.is_empty() {
        Vec::new()
    } else {
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.is_in(lifecycle_lease_ids))
            .order_by_asc(pin_lease_target::Column::CreatedAt)
            .order_by_asc(pin_lease_target::Column::Id)
            .all(db)
            .await?
    };
    discovered_targets.sort_by(compare_target_order);

    // The owner is always the first lifecycle lock. PostgreSQL keeps this exact FOR UPDATE row
    // lock through the caller-owned transaction; SQLite defers its no-op CAS to the final guard
    // so unrelated writers can still resolve the snapshot before a renewal mutates anything.
    let owner = lock_renewal_owner(db, &owner, owner_scope).await?;
    let mut lifecycle_leases = BTreeMap::new();
    for (id, expected) in &discovered_leases {
        let locked = lock_lifecycle_lease(db, expected, false)
            .await
            .map_err(RenewManualLeaseError::Database)?
            .ok_or_else(stale_renewal_error)?;
        if locked.id != *id {
            return Err(stale_renewal_error());
        }
        lifecycle_leases.insert(id.clone(), locked);
    }
    let mut lifecycle_targets = Vec::with_capacity(discovered_targets.len());
    for expected in &discovered_targets {
        let Some(_lease) = lifecycle_leases.get(&expected.lease_id) else {
            return Err(stale_renewal_error());
        };
        let locked = lock_lifecycle_target(db, expected, false)
            .await
            .map_err(RenewManualLeaseError::Database)?
            .ok_or_else(stale_renewal_error)?;
        lifecycle_targets.push(locked);
    }
    lifecycle_targets.sort_by(compare_target_order);
    let targets: Vec<_> = lifecycle_targets
        .iter()
        .filter(|target| target.lease_id == lease_id)
        .cloned()
        .collect();
    if targets.len() != own_targets.len()
        || !targets
            .iter()
            .zip(&own_targets)
            .all(|(locked, expected)| locked == expected)
    {
        return Err(stale_renewal_error());
    }

    let providers: BTreeSet<_> = targets
        .iter()
        .map(|target| target.provider.clone())
        .collect();
    let mut remotes = std::collections::BTreeMap::new();
    let mut usage = std::collections::BTreeMap::new();
    let mut unpin_jobs = Vec::new();
    for (provider, cid) in &pairs {
        if let Some(remote) = lock_remote_after_lifecycle(db, provider, cid).await? {
            remotes.insert((provider.clone(), cid.clone()), remote);
        }
    }
    // All remotes precede usage and jobs in the renewal lock order. Interleaving an unpin-job
    // lock with the next remote would otherwise invert two concurrent multi-target renewals.
    for (provider, cid) in &pairs {
        let jobs_query = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq(provider))
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("unpin"))
            .filter(pin_job::Column::State.is_in([JOB_PENDING, JOB_RUNNING]))
            .order_by_asc(pin_job::Column::Id);
        let mut jobs = if db.get_database_backend() == DatabaseBackend::Postgres {
            jobs_query.lock_exclusive().all(db).await?
        } else {
            jobs_query.all(db).await?
        };
        unpin_jobs.append(&mut jobs);
    }
    for provider in providers {
        let usage_query =
            crate::store::entities::pin_provider_usage::Entity::find_by_id(provider.clone());
        let row = if db.get_database_backend() == DatabaseBackend::Postgres {
            usage_query.lock_exclusive().one(db).await?
        } else {
            usage_query.one(db).await?
        };
        usage.insert(provider, row);
    }
    Ok(RenewalSnapshot {
        owner,
        lease,
        targets,
        lifecycle_leases,
        lifecycle_targets,
        remotes,
        usage,
        unpin_jobs,
    })
}

async fn revalidate_renewal_snapshot<C: ConnectionTrait>(
    db: &C,
    snapshot: &RenewalSnapshot,
) -> Result<(), RenewManualLeaseError> {
    // The owner was acquired before the lifecycle frontier. Re-reading it here would invert
    // owner → lease → target → remote lock order; `guard_renewal_owner` validates it immediately
    // before every successful mutation instead.
    for expected in snapshot.lifecycle_leases.values() {
        let current = pin_lease::Entity::find_by_id(expected.id.clone())
            .one(db)
            .await?
            .ok_or_else(|| stale_renewal_revalidation_error(db.get_database_backend()))?;
        if current != *expected {
            return Err(stale_renewal_revalidation_error(db.get_database_backend()));
        }
    }
    for expected in &snapshot.lifecycle_targets {
        let current = pin_lease_target::Entity::find_by_id(expected.id.clone())
            .one(db)
            .await?
            .ok_or_else(|| stale_renewal_revalidation_error(db.get_database_backend()))?;
        if current != *expected {
            return Err(stale_renewal_revalidation_error(db.get_database_backend()));
        }
    }
    for ((provider, cid), expected) in &snapshot.remotes {
        let current = remote_pin::Entity::find_by_id((provider.clone(), cid.clone()))
            .one(db)
            .await?
            .ok_or_else(|| stale_renewal_revalidation_error(db.get_database_backend()))?;
        if current != *expected {
            return Err(stale_renewal_revalidation_error(db.get_database_backend()));
        }
    }
    for (provider, expected) in &snapshot.usage {
        if quota::read_usage(db, provider)
            .await
            .map_err(app_to_renewal_error)?
            != *expected
        {
            return Err(stale_renewal_revalidation_error(db.get_database_backend()));
        }
    }
    let pairs: BTreeSet<_> = snapshot.remotes.keys().cloned().collect();
    let mut unpin_jobs = Vec::new();
    for (provider, cid) in pairs {
        let mut jobs = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq(provider))
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("unpin"))
            .filter(pin_job::Column::State.is_in([JOB_PENDING, JOB_RUNNING]))
            .order_by_asc(pin_job::Column::Id)
            .all(db)
            .await?;
        unpin_jobs.append(&mut jobs);
    }
    if unpin_jobs != snapshot.unpin_jobs {
        return Err(stale_renewal_revalidation_error(db.get_database_backend()));
    }
    Ok(())
}

fn target_recoverable_from_snapshot(
    snapshot: &RenewalSnapshot,
    target: &pin_lease_target::Model,
) -> bool {
    let Some(remote) = snapshot
        .remotes
        .get(&(target.provider.clone(), target.cid.clone()))
    else {
        return false;
    };
    let held_usage = snapshot
        .usage
        .get(&target.provider)
        .and_then(Option::as_ref)
        .is_some_and(|usage| usage.reserved_pins >= 1 && usage.reserved_bytes >= remote.cid_size);
    let unpin_pending = snapshot.unpin_jobs.iter().any(|job| {
        job.provider == target.provider
            && job.cid == target.cid
            && matches!(job.state.as_str(), JOB_PENDING | JOB_RUNNING)
    });
    remote.status != REMOTE_ABSENT
        && held_usage
        && (remote.request_id.is_some()
            || is_capacity_holding_status(&remote.status)
            || unpin_pending)
        && target_state_from_remote(&remote.status).is_some()
}

async fn restore_snapshot_targets<C: ConnectionTrait>(
    db: &C,
    snapshot: &RenewalSnapshot,
    targets: &[pin_lease_target::Model],
    now: DateTimeUtc,
) -> Result<Vec<String>, RenewManualLeaseError> {
    let mut restored = Vec::with_capacity(targets.len());
    for target in targets {
        let remote = snapshot
            .remotes
            .get(&(target.provider.clone(), target.cid.clone()))
            .expect("recoverable target has snapshotted remote");
        let state = target_state_from_remote(&remote.status)
            .expect("recoverable target has restorable remote status");
        let last_touched_at = target.last_touched_at.max(now).max(remote.last_touched_at);

        #[cfg(test)]
        record_target_cas(&target.id).await;

        let updated = pin_lease_target::Entity::update_many()
            .col_expr(
                pin_lease_target::Column::State,
                Expr::value(state.to_owned()),
            )
            .col_expr(
                pin_lease_target::Column::LastTouchedAt,
                Expr::value(last_touched_at),
            )
            .filter(pin_lease_target::Column::Id.eq(&target.id))
            .filter(pin_lease_target::Column::LeaseId.eq(&target.lease_id))
            .filter(pin_lease_target::Column::State.eq(&target.state))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Err(stale_renewal_error());
        }
        restored.push(target.id.clone());
    }
    Ok(restored)
}

async fn refresh_renewal_snapshot_targets<C: ConnectionTrait>(
    db: &C,
    snapshot: &RenewalSnapshot,
    generation: i64,
    targets: &[pin_lease_target::Model],
    now: DateTimeUtc,
) -> Result<(), RenewManualLeaseError> {
    let mut ordered_targets = targets.to_vec();
    ordered_targets.sort_by(compare_target_order);
    let pairs: BTreeSet<_> = ordered_targets
        .iter()
        .map(|target| (target.provider.clone(), target.cid.clone()))
        .collect();
    let mut refreshed_targets = BTreeMap::new();
    for target in &ordered_targets {
        let remote = snapshot
            .remotes
            .get(&(target.provider.clone(), target.cid.clone()))
            .expect("renewed target has snapshotted remote");
        let refreshed = renewed_target_after_refresh(target, remote, now)?;

        #[cfg(test)]
        record_target_cas(&target.id).await;

        let updated = pin_lease_target::Entity::update_many()
            .col_expr(
                pin_lease_target::Column::LastTouchedAt,
                Expr::value(refreshed.last_touched_at),
            )
            .filter(pin_lease_target::Column::Id.eq(&target.id))
            .filter(pin_lease_target::Column::State.eq(&refreshed.state))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Err(stale_renewal_error());
        }
        refreshed_targets.insert(target.id.clone(), refreshed);
    }
    let renewed_target_ids: BTreeSet<_> = ordered_targets
        .iter()
        .map(|target| target.id.clone())
        .collect();

    // The complete shared snapshot was locked before the remote rows. Canonical selection and
    // max-touch computation are consequently pure in-memory projections; no post-frontier
    // sibling query is allowed here.
    let mut shared_desired = BTreeMap::new();
    for (provider, cid) in &pairs {
        shared_desired.insert(
            (provider.clone(), cid.clone()),
            renewal_projected_desired_targets(
                snapshot,
                provider,
                cid,
                generation,
                &refreshed_targets,
            )?,
        );
    }
    let mut max_active_touch = BTreeMap::new();
    for ((provider, cid), desired) in &shared_desired {
        let max_touch = desired
            .iter()
            .map(|desired| desired.target.last_touched_at)
            .max()
            .ok_or_else(stale_renewal_error)?;
        max_active_touch.insert((provider.clone(), cid.clone()), max_touch);
    }
    let mut bumped = std::collections::BTreeMap::new();
    for (provider, cid) in &pairs {
        let remote = snapshot
            .remotes
            .get(&(provider.clone(), cid.clone()))
            .ok_or_else(stale_renewal_error)?;
        let epoch = bump_snapshotted_remote_epoch(
            db,
            remote,
            *max_active_touch
                .get(&(provider.clone(), cid.clone()))
                .expect("each renewal pair has a max active touch"),
        )
        .await?;
        bumped.insert(
            (provider.clone(), cid.clone()),
            remote_pin::Model {
                epoch,
                last_touched_at: *max_active_touch
                    .get(&(provider.clone(), cid.clone()))
                    .expect("each renewal pair has a max active touch"),
                ..remote.clone()
            },
        );
    }
    for (provider, cid) in pairs {
        let desired = shared_desired
            .get(&(provider.clone(), cid.clone()))
            .expect("every renewal pair has a desired-target snapshot");
        let remote = reset_prelocked_failed_remote_retry_on_user_touch(
            db,
            bumped
                .get(&(provider.clone(), cid.clone()))
                .expect("each renewal pair was bumped"),
            now,
        )
        .await?;
        for target in desired
            .iter()
            .filter(|target| renewed_target_ids.contains(&target.target.id))
        {
            project_prelocked_desired_target(
                db,
                target,
                desired,
                &remote,
                ProjectionAccess::Admission,
                now,
            )
            .await
            .map_err(app_to_renewal_error)?;
        }
        let has_all_mode_target = desired
            .iter()
            .any(|target| target.lease.provider_mode == "all");
        let reconcile_at = if remote.status == REMOTE_FAILED && has_all_mode_target {
            remote
                .next_retry_at
                .expect("failed renewal retry was reset in the prelocked remote snapshot")
        } else {
            now
        };
        ensure_reconcile(db, &provider, &cid, remote.epoch, reconcile_at)
            .await
            .map_err(app_to_renewal_error)?;
    }
    Ok(())
}

fn renewed_target_after_refresh(
    target: &pin_lease_target::Model,
    remote: &remote_pin::Model,
    now: DateTimeUtc,
) -> Result<pin_lease_target::Model, RenewManualLeaseError> {
    let state = if is_desired_target_state(&target.state) {
        target.state.clone()
    } else {
        target_state_from_remote(&remote.status)
            .ok_or_else(stale_renewal_error)?
            .to_owned()
    };
    Ok(pin_lease_target::Model {
        state,
        last_touched_at: target.last_touched_at.max(now).max(remote.last_touched_at),
        ..target.clone()
    })
}

fn renewal_projected_desired_targets(
    snapshot: &RenewalSnapshot,
    provider: &str,
    cid: &str,
    generation: i64,
    refreshed_targets: &BTreeMap<String, pin_lease_target::Model>,
) -> Result<Vec<DesiredTarget>, RenewManualLeaseError> {
    let mut projected = Vec::new();
    for target in &snapshot.lifecycle_targets {
        if target.provider != provider || target.cid != cid {
            continue;
        }
        let target = refreshed_targets
            .get(&target.id)
            .cloned()
            .unwrap_or_else(|| target.clone());
        let mut lease = snapshot
            .lifecycle_leases
            .get(&target.lease_id)
            .cloned()
            .ok_or_else(stale_renewal_error)?;
        if lease.id == snapshot.lease.id {
            lease.state = LEASE_ACTIVE.to_owned();
            lease.generation = generation;
        }
        if lease.state == LEASE_ACTIVE && is_desired_target_state(&target.state) {
            projected.push(DesiredTarget { target, lease });
        }
    }
    projected.sort_by(|left, right| compare_target_order(&left.target, &right.target));
    if projected.is_empty() {
        return Err(stale_renewal_error());
    }
    Ok(projected)
}

async fn reset_prelocked_failed_remote_retry_on_user_touch<C: ConnectionTrait>(
    db: &C,
    remote: &remote_pin::Model,
    now: DateTimeUtc,
) -> Result<remote_pin::Model, RenewManualLeaseError> {
    if remote.status != REMOTE_FAILED {
        return Ok(remote.clone());
    }
    let next_retry_at =
        now + duration_as_chrono(FAILED_REQUEST_BASE_BACKOFF).map_err(app_to_renewal_error)?;
    let mut updated = remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::FailureAttempts, Expr::value(0_i32))
        .col_expr(
            remote_pin::Column::NextRetryAt,
            Expr::value(Some(next_retry_at)),
        )
        .col_expr(
            remote_pin::Column::LastTouchedAt,
            Expr::value(remote.last_touched_at),
        )
        .filter(remote_pin::Column::Provider.eq(&remote.provider))
        .filter(remote_pin::Column::Cid.eq(&remote.cid))
        .filter(remote_pin::Column::Epoch.eq(remote.epoch))
        .filter(remote_pin::Column::Status.eq(&remote.status))
        .filter(remote_pin::Column::FailureAttempts.eq(remote.failure_attempts));
    updated = match remote.request_id.as_deref() {
        Some(request_id) => updated.filter(remote_pin::Column::RequestId.eq(request_id)),
        None => updated.filter(remote_pin::Column::RequestId.is_null()),
    };
    updated = match remote.last_failed_request_id.as_deref() {
        Some(request_id) => updated.filter(remote_pin::Column::LastFailedRequestId.eq(request_id)),
        None => updated.filter(remote_pin::Column::LastFailedRequestId.is_null()),
    };
    updated = match remote.next_retry_at {
        Some(due) => updated.filter(remote_pin::Column::NextRetryAt.eq(due)),
        None => updated.filter(remote_pin::Column::NextRetryAt.is_null()),
    };
    updated = match remote.last_error_class.as_deref() {
        Some(error_class) => updated.filter(remote_pin::Column::LastErrorClass.eq(error_class)),
        None => updated.filter(remote_pin::Column::LastErrorClass.is_null()),
    };
    updated = match remote.last_error_text.as_deref() {
        Some(error_text) => updated.filter(remote_pin::Column::LastErrorText.eq(error_text)),
        None => updated.filter(remote_pin::Column::LastErrorText.is_null()),
    };
    if updated.exec(db).await?.rows_affected != 1 {
        return Err(stale_renewal_error());
    }
    Ok(remote_pin::Model {
        failure_attempts: 0,
        next_retry_at: Some(next_retry_at),
        ..remote.clone()
    })
}

async fn bump_snapshotted_remote_epoch<C: ConnectionTrait>(
    db: &C,
    remote: &remote_pin::Model,
    last_touched_at: DateTimeUtc,
) -> Result<i64, RenewManualLeaseError> {
    let epoch = increment_epoch(remote.epoch).map_err(app_to_renewal_error)?;

    #[cfg(test)]
    record_remote_work(&remote.provider, &remote.cid).await;

    let mut updated = remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::Epoch, Expr::value(epoch))
        .col_expr(
            remote_pin::Column::LastTouchedAt,
            Expr::value(last_touched_at),
        )
        .filter(remote_pin::Column::Provider.eq(&remote.provider))
        .filter(remote_pin::Column::Cid.eq(&remote.cid))
        .filter(remote_pin::Column::Epoch.eq(remote.epoch))
        .filter(remote_pin::Column::Status.eq(&remote.status))
        .filter(remote_pin::Column::FailureAttempts.eq(remote.failure_attempts));
    updated = match remote.request_id.as_deref() {
        Some(request_id) => updated.filter(remote_pin::Column::RequestId.eq(request_id)),
        None => updated.filter(remote_pin::Column::RequestId.is_null()),
    };
    updated = match remote.last_failed_request_id.as_deref() {
        Some(request_id) => updated.filter(remote_pin::Column::LastFailedRequestId.eq(request_id)),
        None => updated.filter(remote_pin::Column::LastFailedRequestId.is_null()),
    };
    updated = match remote.next_retry_at {
        Some(next_retry_at) => updated.filter(remote_pin::Column::NextRetryAt.eq(next_retry_at)),
        None => updated.filter(remote_pin::Column::NextRetryAt.is_null()),
    };
    updated = match remote.last_error_class.as_deref() {
        Some(error_class) => updated.filter(remote_pin::Column::LastErrorClass.eq(error_class)),
        None => updated.filter(remote_pin::Column::LastErrorClass.is_null()),
    };
    updated = match remote.last_error_text.as_deref() {
        Some(error_text) => updated.filter(remote_pin::Column::LastErrorText.eq(error_text)),
        None => updated.filter(remote_pin::Column::LastErrorText.is_null()),
    };
    if updated.exec(db).await?.rows_affected != 1 {
        return Err(stale_renewal_error());
    }
    Ok(epoch)
}

/// Bumps a shared remote once after a caller has added or touched a desired reference.
pub async fn touch_target_reference<C: ConnectionTrait>(
    db: &C,
    target_id: &str,
    now: DateTimeUtc,
) -> AppResult<GenerationDecision> {
    let Some(target) = pin_lease_target::Entity::find_by_id(target_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(GenerationDecision::Stale);
    };
    if !matches!(
        check_target_generation(
            db,
            target_id,
            current_generation(db, &target.lease_id).await?
        )
        .await?,
        GenerationDecision::Current
    ) {
        return Ok(GenerationDecision::NoLongerNeeded);
    }
    pin_lease_target::Entity::update_many()
        .col_expr(pin_lease_target::Column::LastTouchedAt, Expr::value(now))
        .filter(pin_lease_target::Column::Id.eq(target_id))
        .exec(db)
        .await?;
    match bump_remote_epoch(db, &target.provider, &target.cid, now).await? {
        Some(_) => {
            quota::refresh_remote_max_active_touch(db, &target.provider, &target.cid).await?;
            Ok(GenerationDecision::Current)
        }
        None => Ok(GenerationDecision::Stale),
    }
}

/// Begins an epoch-bound remote Unpin. It never releases quota; completion owns that decision.
pub async fn begin_remote_unpin<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<i64> {
    if !desired_targets(db, provider, cid).await?.is_empty() {
        return Err(invalid("remote pin still has active desired targets"));
    }
    let now = Utc::now();
    let epoch = bump_remote_epoch(db, provider, cid, now)
        .await?
        .ok_or_else(|| invalid("remote pin does not exist"))?;
    let NewPinJob::Remote(job) = jobs::unpin_job(provider, cid, epoch, now) else {
        unreachable!("unpin constructor is remote scoped")
    };
    jobs::enqueue_job(db, NewPinJob::Remote(job)).await?;
    Ok(epoch)
}

/// Cancels one active lease, preserves its rows, and only starts epoch-bound remote cleanup.
pub async fn cancel_lease<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    now: DateTimeUtc,
) -> AppResult<GenerationDecision> {
    let Some(lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(GenerationDecision::Stale);
    };
    if lease.state != LEASE_ACTIVE {
        return Ok(GenerationDecision::NoLongerNeeded);
    }
    end_leases(db, &[lease], LEASE_CANCELLED, TARGET_RELEASED, now).await?;
    Ok(GenerationDecision::Current)
}

/// Ends every active lease owned by one immutable object in one lifecycle transition.
///
/// Publication overwrite/delete callers pass their transaction so all lease generations,
/// targets, remote epochs, and remote-scoped outbox work share the object metadata boundary.
pub async fn end_active_leases_for_object<C: ConnectionTrait>(
    db: &C,
    owner_object_id: &str,
    now: DateTimeUtc,
) -> AppResult<Vec<String>> {
    let leases = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq(owner_object_id))
        .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
        .order_by_asc(pin_lease::Column::Id)
        .all(db)
        .await?;
    let ended = leases.iter().map(|lease| lease.id.clone()).collect();
    end_leases(db, &leases, LEASE_CANCELLED, TARGET_RELEASED, now).await?;
    Ok(ended)
}

/// Expires every due active lease without deleting targets or decrementing quota.
pub async fn expire_due_leases<C: ConnectionTrait>(
    db: &C,
    now: DateTimeUtc,
) -> AppResult<Vec<String>> {
    let leases = pin_lease::Entity::find()
        .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
        .filter(pin_lease::Column::ExpiresAt.lte(now))
        .order_by_asc(pin_lease::Column::ExpiresAt)
        .order_by_asc(pin_lease::Column::Id)
        .all(db)
        .await?;
    let ids = leases.iter().map(|lease| lease.id.clone()).collect();
    end_leases(db, &leases, LEASE_EXPIRED, TARGET_RELEASED, now).await?;
    Ok(ids)
}

/// Evicts a provider/CID desired set and starts unpin/reconcile work without releasing quota.
pub async fn evict_provider_cid<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTimeUtc,
) -> AppResult<GenerationDecision> {
    let evicted = evict_provider_cids(db, &[(provider.to_owned(), cid.to_owned())], now).await?;
    if evicted.is_empty() {
        return Ok(GenerationDecision::NoLongerNeeded);
    }
    Ok(GenerationDecision::Current)
}

/// Evicts a deterministic set of provider/CIDs under one complete lifecycle frontier.
///
/// Every affected lease generation and every selected remote epoch advances exactly once even
/// when one lease contributes more than one selected CID. The caller owns transaction scope.
pub async fn evict_provider_cids<C: ConnectionTrait>(
    db: &C,
    pairs: &[(String, String)],
    now: DateTimeUtc,
) -> AppResult<Vec<QuotaEvictedTarget>> {
    let mut pairs = pairs.to_vec();
    pairs.sort();
    pairs.dedup();

    let mut targets = Vec::new();
    for (provider, cid) in &pairs {
        targets.extend(desired_targets(db, provider, cid).await?);
    }
    targets.sort_by(|left, right| compare_target_order(&left.target, &right.target));
    targets.dedup_by(|left, right| left.target.id == right.target.id);
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let mut leases = BTreeMap::new();
    let mut original_targets = BTreeMap::new();
    for desired in &targets {
        let lease_id = desired.lease.id.clone();
        leases.insert(lease_id.clone(), desired.lease.clone());
    }
    for lease_id in leases.keys() {
        original_targets.insert(lease_id.clone(), lease_targets(db, lease_id).await?);
    }

    // Acquire the complete lifecycle frontier before any selected remote row.
    for (lease_id, lease) in &leases {
        let locked = lock_lifecycle_lease(db, lease, true)
            .await?
            .ok_or_else(|| stale_lifecycle_error("quota eviction lease prelock"))?;
        if locked != *lease || locked.id != *lease_id {
            return Err(stale_lifecycle_error("quota eviction lease frontier"));
        }
    }
    let mut target_frontier = original_targets
        .values()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    target_frontier.sort_by(compare_target_order);
    for target in &target_frontier {
        let locked = lock_lifecycle_target(db, target, true)
            .await?
            .ok_or_else(|| stale_lifecycle_error("quota eviction target prelock"))?;
        if locked != *target {
            return Err(stale_lifecycle_error("quota eviction target frontier"));
        }
    }
    let remote_frontier = target_frontier
        .iter()
        .filter(|target| is_desired_target_state(&target.state))
        .map(|target| (target.provider.clone(), target.cid.clone()))
        .collect::<BTreeSet<_>>();
    for (provider, cid) in &remote_frontier {
        lock_remote_after_lifecycle(db, provider, cid).await?;
    }

    // Phase 1: all lease CAS operations complete in stable ID order.
    for lease in leases.values() {
        #[cfg(test)]
        stage_eviction_lease_generation(db, &lease.id).await?;

        let generation = increment_epoch(lease.generation)?;
        // Quota eviction is a temporary placement decision. Keep both one/all parent leases
        // active so one-mode can fail over and all-mode can retry after confirmed release.
        let state = LEASE_ACTIVE;

        #[cfg(test)]
        record_lease_cas(&lease.id).await;

        let updated = pin_lease::Entity::update_many()
            .col_expr(pin_lease::Column::Generation, Expr::value(generation))
            .col_expr(pin_lease::Column::State, Expr::value(state.to_owned()))
            .col_expr(pin_lease::Column::LastTouchedAt, Expr::value(now))
            .filter(pin_lease::Column::Id.eq(&lease.id))
            .filter(pin_lease::Column::Generation.eq(lease.generation))
            .filter(pin_lease::Column::State.eq(&lease.state))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Err(stale_lifecycle_error("eviction lease"));
        }
    }

    // Phase 2: target transitions use the original snapshot in stable `(created_at, id)` order.
    // The caller-provided transaction must roll back if a target CAS is stale after a lease CAS.
    for desired in &targets {
        let target = &desired.target;

        #[cfg(test)]
        record_target_cas(&target.id).await;

        let updated = pin_lease_target::Entity::update_many()
            .col_expr(
                pin_lease_target::Column::State,
                Expr::value(TARGET_EVICTED.to_owned()),
            )
            .filter(pin_lease_target::Column::Id.eq(&target.id))
            .filter(pin_lease_target::Column::LeaseId.eq(&target.lease_id))
            .filter(pin_lease_target::Column::Provider.eq(&target.provider))
            .filter(pin_lease_target::Column::Cid.eq(&target.cid))
            .filter(pin_lease_target::Column::LogicalSize.eq(target.logical_size))
            .filter(pin_lease_target::Column::CreatedAt.eq(target.created_at))
            .filter(pin_lease_target::Column::State.eq(&target.state))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Err(stale_lifecycle_error("eviction target"));
        }
    }
    // A lease-generation bump must not strand a non-evicted all-mode sibling behind an old
    // target-scoped Submit/Poll generation. Reproject every sibling while its complete frontier
    // remains locked; selected remotes still advance only once below.
    for target in target_frontier.iter().filter(|target| {
        is_desired_target_state(&target.state)
            && !targets.iter().any(|evicted| evicted.target.id == target.id)
    }) {
        project_inserted_target_from_prelocked_remote(
            db,
            &target.id,
            &target.provider,
            &target.cid,
            now,
        )
        .await?;
    }
    for (provider, cid) in &pairs {
        if let Some(epoch) = bump_remote_epoch(db, provider, cid, now).await? {
            schedule_after_desired_change(db, provider, cid, epoch, now).await?;
        }
    }

    targets
        .into_iter()
        .map(|desired| {
            Ok(QuotaEvictedTarget {
                lease_id: desired.lease.id,
                target_id: desired.target.id,
                provider: desired.target.provider,
                cid: desired.target.cid,
                provider_mode: provider_mode(&desired.lease.provider_mode)?,
            })
        })
        .collect()
}

/// Retires one one-mode target in favor of an already-reserved sibling target of the same lease.
/// It does not create rows or reserve quota; publication/failover policy owns feasibility first.
pub async fn failover_target_if_feasible<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    retiring_target_id: &str,
    replacement_target_id: &str,
    now: DateTimeUtc,
) -> AppResult<GenerationDecision> {
    let Some(lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(GenerationDecision::Stale);
    };
    if lease.state != LEASE_ACTIVE || lease.provider_mode != "one" {
        return Ok(GenerationDecision::NoLongerNeeded);
    }
    let Some(retiring) = pin_lease_target::Entity::find_by_id(retiring_target_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(GenerationDecision::Stale);
    };
    let Some(replacement) = pin_lease_target::Entity::find_by_id(replacement_target_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(GenerationDecision::Stale);
    };
    if retiring.lease_id != lease_id
        || replacement.lease_id != lease_id
        || !same_content_cid(&retiring.cid, &replacement.cid)
        || retiring.provider == replacement.provider
        || !is_desired_target_state(&retiring.state)
        || !is_desired_target_state(&replacement.state)
    {
        return Ok(GenerationDecision::NoLongerNeeded);
    }
    let Some(replacement_remote) =
        remote_pin::Entity::find_by_id((replacement.provider.clone(), replacement.cid.clone()))
            .one(db)
            .await?
    else {
        return Ok(GenerationDecision::NoLongerNeeded);
    };
    if replacement_remote.status == REMOTE_ABSENT {
        return Ok(GenerationDecision::NoLongerNeeded);
    }

    #[cfg(test)]
    stage_failover_lease_generation(db, lease_id).await?;

    let generation = increment_epoch(lease.generation)?;

    #[cfg(test)]
    record_lease_cas(&lease.id).await;

    let lease_updated = pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Generation, Expr::value(generation))
        .col_expr(pin_lease::Column::LastTouchedAt, Expr::value(now))
        .filter(pin_lease::Column::Id.eq(lease_id))
        .filter(pin_lease::Column::Generation.eq(lease.generation))
        .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
        .filter(pin_lease::Column::ProviderMode.eq("one"))
        .exec(db)
        .await?;
    if lease_updated.rows_affected != 1 {
        return Err(stale_lifecycle_error("failover lease"));
    }

    let mut targets = [(&retiring, true), (&replacement, false)];
    targets.sort_by(|(left, _), (right, _)| compare_target_order(left, right));
    for (target, retiring_target) in targets {
        #[cfg(test)]
        record_target_cas(&target.id).await;

        let state = if retiring_target {
            TARGET_RELEASED.to_owned()
        } else {
            target.state.clone()
        };
        let updated = pin_lease_target::Entity::update_many()
            .col_expr(pin_lease_target::Column::State, Expr::value(state))
            .filter(pin_lease_target::Column::Id.eq(&target.id))
            .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
            .filter(pin_lease_target::Column::Provider.eq(&target.provider))
            .filter(pin_lease_target::Column::Cid.eq(&target.cid))
            .filter(pin_lease_target::Column::LogicalSize.eq(target.logical_size))
            .filter(pin_lease_target::Column::CreatedAt.eq(target.created_at))
            .filter(pin_lease_target::Column::State.eq(&target.state))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Err(stale_lifecycle_error(if retiring_target {
                "failover retiring target"
            } else {
                "failover replacement target"
            }));
        }
    }
    for (provider, cid) in BTreeSet::from([
        (retiring.provider.clone(), retiring.cid.clone()),
        (replacement.provider.clone(), replacement.cid.clone()),
    ]) {
        if let Some(epoch) = bump_remote_epoch(db, &provider, &cid, now).await? {
            schedule_after_desired_change(db, &provider, &cid, epoch, now).await?;
        }
    }
    Ok(GenerationDecision::Current)
}

/// Creates the next sticky one-mode assignment without retiring the failed record.
///
/// `ordered_providers` must start with `failed_target_id`'s provider and contain only policy
/// candidates that the coordinator currently considers healthy. The caller owns transaction
/// scope; this function follows lease → target → remote → usage ordering and never performs
/// provider I/O.
pub async fn fail_one_target<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    failed_target_id: &str,
    ordered_providers: &[String],
    limits: &ProviderLimitMap,
    now: DateTimeUtc,
) -> AppResult<Option<pin_lease_target::Model>> {
    fail_one_target_at_generation(
        db,
        lease_id,
        failed_target_id,
        ordered_providers,
        limits,
        now,
        None,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TargetJobFailoverOutcome {
    Stale,
    Current,
}

/// Validates one exact target-scoped job and performs failover in the caller's transaction.
pub(crate) async fn fail_one_target_for_job<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    ordered_providers: &[String],
    limits: &ProviderLimitMap,
    now: DateTimeUtc,
) -> AppResult<TargetJobFailoverOutcome> {
    let (Some(lease_id), Some(target_id), Some(expected_generation)) = (
        job.lease_id.as_deref(),
        job.target_id.as_deref(),
        job.expected_generation,
    ) else {
        return Ok(TargetJobFailoverOutcome::Stale);
    };
    if !matches!(job.operation.as_str(), "submit" | "poll") || job.expected_remote_epoch.is_some() {
        return Ok(TargetJobFailoverOutcome::Stale);
    }
    let Some(target) = pin_lease_target::Entity::find_by_id(target_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(TargetJobFailoverOutcome::Stale);
    };
    let Some(lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(TargetJobFailoverOutcome::Stale);
    };
    if target.lease_id != lease.id
        || target.provider != job.provider
        || target.cid != job.cid
        || lease.state != LEASE_ACTIVE
        || lease.provider_mode != "one"
        || lease.generation != expected_generation
        || !is_desired_target_state(&target.state)
    {
        return Ok(TargetJobFailoverOutcome::Stale);
    }
    let mut targets = lease_targets(db, lease_id).await?;
    targets.sort_by(compare_target_order);
    if targets
        .iter()
        .rfind(|candidate| {
            same_content_cid(&candidate.cid, &target.cid)
                && is_desired_target_state(&candidate.state)
        })
        .map(|candidate| candidate.id.as_str())
        != Some(target_id)
    {
        return Ok(TargetJobFailoverOutcome::Stale);
    }

    let replacement = fail_one_target_at_generation(
        db,
        lease_id,
        target_id,
        ordered_providers,
        limits,
        now,
        Some(expected_generation),
    )
    .await?;
    if replacement.is_none() && !jobs::check_target_job_generation(db, job).await? {
        return Ok(TargetJobFailoverOutcome::Stale);
    }
    Ok(TargetJobFailoverOutcome::Current)
}

async fn fail_one_target_at_generation<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    failed_target_id: &str,
    ordered_providers: &[String],
    limits: &ProviderLimitMap,
    now: DateTimeUtc,
    expected_generation: Option<i64>,
) -> AppResult<Option<pin_lease_target::Model>> {
    let Some(lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if lease.state != LEASE_ACTIVE
        || lease.provider_mode != "one"
        || expected_generation.is_some_and(|expected| lease.generation != expected)
    {
        return Ok(None);
    }
    let Some(failed) = pin_lease_target::Entity::find_by_id(failed_target_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if failed.lease_id != lease_id
        || !matches!(
            failed.state.as_str(),
            TARGET_WAITING | TARGET_SUBMITTED | TARGET_PINNED | TARGET_DEGRADED | TARGET_EVICTED
        )
    {
        return Ok(None);
    }
    let Some(failed_index) = ordered_providers
        .iter()
        .position(|provider| provider == &failed.provider)
    else {
        return Ok(None);
    };

    let mut targets = lease_targets(db, lease_id).await?;
    targets.sort_by(compare_target_order);
    let eligible_suffix = ordered_providers[failed_index + 1..]
        .iter()
        .filter(|provider| {
            limits
                .get(*provider)
                .is_some_and(|provider_limits| provider_limits.enabled)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let existing = targets
        .iter()
        .rfind(|target| {
            same_content_cid(&target.cid, &failed.cid)
                && target.id != failed.id
                && is_desired_target_state(&target.state)
                && eligible_suffix.contains(&target.provider)
        })
        .cloned();

    let locked_lease = lock_lifecycle_lease(db, &lease, true)
        .await?
        .ok_or_else(|| stale_lifecycle_error("one failover lease prelock"))?;
    for target in &targets {
        lock_lifecycle_target(db, target, true)
            .await?
            .ok_or_else(|| stale_lifecycle_error("one failover target prelock"))?;
    }
    if locked_lease != lease || lease_targets(db, lease_id).await? != targets {
        return Err(stale_lifecycle_error("one failover frontier"));
    }

    let existing_providers: BTreeSet<_> = targets
        .iter()
        .filter(|target| same_content_cid(&target.cid, &failed.cid))
        .map(|target| target.provider.clone())
        .collect();
    let candidates = if existing.is_some() {
        Vec::new()
    } else {
        ordered_providers[failed_index + 1..]
            .iter()
            .filter(|provider| {
                limits
                    .get(*provider)
                    .is_some_and(|provider_limits| provider_limits.enabled)
                    && !existing_providers.contains(*provider)
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    if existing.is_none() && candidates.is_empty() {
        return Ok(None);
    }

    // Lock the complete remote frontier that immediate pinned convergence could retire before
    // any usage row. This keeps the lifecycle -> remote -> usage order even when a shared
    // fallback is already pinned and convergence occurs inside this caller-owned transaction.
    let mut candidate_cids = BTreeMap::new();
    for provider in &candidates {
        candidate_cids.insert(
            provider.clone(),
            super::ledger::allocation_cid(db, provider, &failed.cid).await?,
        );
    }
    let mut remote_pairs = targets
        .iter()
        .filter(|target| {
            same_content_cid(&target.cid, &failed.cid) && is_desired_target_state(&target.state)
        })
        .map(|target| (target.provider.clone(), target.cid.clone()))
        .chain(
            candidates
                .iter()
                .map(|provider| (provider.clone(), candidate_cids[provider].clone())),
        )
        .collect::<Vec<_>>();
    remote_pairs.sort();
    remote_pairs.dedup();
    let mut candidate_remotes = BTreeMap::new();
    for (provider, cid) in &remote_pairs {
        candidate_remotes.insert(
            (provider.clone(), cid.clone()),
            lock_remote_after_lifecycle(db, provider, cid).await?,
        );
    }
    quota::lock_publication_usage_rows(db, &candidates).await?;

    if let Some(existing) = existing {
        if existing.state == TARGET_PINNED {
            let cleanup =
                converge_one_after_replacement_inner(db, lease_id, &existing.id, now, true).await?;
            for job in cleanup {
                jobs::enqueue_job(db, job).await?;
            }
        }
        return Ok(pin_lease_target::Entity::find_by_id(existing.id)
            .one(db)
            .await?);
    }

    let mut selected = None;
    for provider in candidates {
        let cid = &candidate_cids[&provider];
        if candidate_remotes
            .get(&(provider.clone(), cid.clone()))
            .and_then(Option::as_ref)
            .is_some_and(|remote| remote.status == REMOTE_FAILED)
        {
            continue;
        }
        let reservation =
            quota::reserve_unique(db, &provider, cid, failed.logical_size, limits, now).await?;
        if matches!(
            reservation,
            ReservationOutcome::Reserved | ReservationOutcome::Reused
        ) {
            selected = Some((provider, cid.clone()));
            break;
        }
    }
    let Some((provider, cid)) = selected else {
        return Ok(None);
    };

    let generation = increment_epoch(lease.generation)?;
    #[cfg(test)]
    record_lease_cas(lease_id).await;
    let updated = pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Generation, Expr::value(generation))
        .col_expr(pin_lease::Column::LastTouchedAt, Expr::value(now))
        .filter(pin_lease::Column::Id.eq(lease_id))
        .filter(pin_lease::Column::Generation.eq(lease.generation))
        .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
        .filter(pin_lease::Column::ProviderMode.eq("one"))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(stale_lifecycle_error("one failover lease"));
    }

    let replacement_id = target_id(lease_id, &provider, &cid);
    let replacement_created_at = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(&provider))
        .filter(pin_lease_target::Column::Cid.eq(&cid))
        .order_by_desc(pin_lease_target::Column::CreatedAt)
        .order_by_desc(pin_lease_target::Column::Id)
        .one(db)
        .await?
        .map(|target| target.created_at + ChronoDuration::nanoseconds(1))
        .unwrap_or(now)
        .max(now);
    pin_lease_target::Entity::insert(pin_lease_target::ActiveModel {
        id: Set(replacement_id.clone()),
        lease_id: Set(lease_id.to_owned()),
        cid: Set(cid.clone()),
        logical_size: Set(failed.logical_size),
        provider: Set(provider.clone()),
        state: Set(TARGET_WAITING.to_owned()),
        created_at: Set(replacement_created_at),
        last_touched_at: Set(now),
    })
    .exec(db)
    .await?;
    quota::refresh_remote_max_active_touch(db, &provider, &cid).await?;
    project_inserted_target_from_prelocked_remote(db, &replacement_id, &provider, &cid, now)
        .await?;
    let replacement = pin_lease_target::Entity::find_by_id(replacement_id)
        .one(db)
        .await?;
    if let Some(replacement) = &replacement
        && replacement.state == TARGET_PINNED
    {
        let cleanup =
            converge_one_after_replacement_inner(db, lease_id, &replacement.id, now, true).await?;
        for job in cleanup {
            jobs::enqueue_job(db, job).await?;
        }
    }
    Ok(replacement)
}

/// Retires older one-mode assignments only after the newest sticky replacement is pinned.
/// Cleanup work is returned to the caller so it can be inserted in the same transaction.
pub async fn converge_one_after_replacement<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    winning_target_id: &str,
    now: DateTimeUtc,
) -> AppResult<Vec<NewPinJob>> {
    converge_one_after_replacement_inner(db, lease_id, winning_target_id, now, false).await
}

async fn converge_one_after_replacement_inner<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    winning_target_id: &str,
    now: DateTimeUtc,
    lifecycle_prelocked: bool,
) -> AppResult<Vec<NewPinJob>> {
    let Some(lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(Vec::new());
    };
    if lease.state != LEASE_ACTIVE || lease.provider_mode != "one" {
        return Ok(Vec::new());
    }
    let mut targets = lease_targets(db, lease_id).await?;
    targets.sort_by(compare_target_order);
    let Some(winner) = targets
        .iter()
        .find(|target| target.id == winning_target_id)
        .cloned()
    else {
        return Ok(Vec::new());
    };
    if winner.state != TARGET_PINNED {
        return Ok(Vec::new());
    }
    let current_assignment = targets.iter().rfind(|target| {
        same_content_cid(&target.cid, &winner.cid) && is_desired_target_state(&target.state)
    });
    if current_assignment.map(|target| target.id.as_str()) != Some(winning_target_id) {
        return Ok(Vec::new());
    }
    let retiring = targets
        .iter()
        .filter(|target| {
            same_content_cid(&target.cid, &winner.cid)
                && target.id != winner.id
                && is_desired_target_state(&target.state)
        })
        .cloned()
        .collect::<Vec<_>>();
    if retiring.is_empty() {
        return Ok(Vec::new());
    }

    if !lifecycle_prelocked {
        lock_lifecycle_lease(db, &lease, true)
            .await?
            .ok_or_else(|| stale_lifecycle_error("one convergence lease prelock"))?;
        for target in &targets {
            lock_lifecycle_target(db, target, true)
                .await?
                .ok_or_else(|| stale_lifecycle_error("one convergence target prelock"))?;
        }
        if lease_targets(db, lease_id).await? != targets {
            return Err(stale_lifecycle_error("one convergence frontier"));
        }
    }
    let remote_pairs = retiring
        .iter()
        .map(|target| (target.provider.clone(), target.cid.clone()))
        .collect::<BTreeSet<_>>();
    if !lifecycle_prelocked {
        for (provider, cid) in &remote_pairs {
            lock_remote_after_lifecycle(db, provider, cid).await?;
        }
    }

    let generation = increment_epoch(lease.generation)?;
    #[cfg(test)]
    record_lease_cas(lease_id).await;
    let updated = pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Generation, Expr::value(generation))
        .col_expr(pin_lease::Column::LastTouchedAt, Expr::value(now))
        .filter(pin_lease::Column::Id.eq(lease_id))
        .filter(pin_lease::Column::Generation.eq(lease.generation))
        .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
        .filter(pin_lease::Column::ProviderMode.eq("one"))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(stale_lifecycle_error("one convergence lease"));
    }
    for target in &retiring {
        #[cfg(test)]
        record_target_cas(&target.id).await;
        let updated = pin_lease_target::Entity::update_many()
            .col_expr(
                pin_lease_target::Column::State,
                Expr::value(TARGET_RELEASED.to_owned()),
            )
            .col_expr(pin_lease_target::Column::LastTouchedAt, Expr::value(now))
            .filter(pin_lease_target::Column::Id.eq(&target.id))
            .filter(pin_lease_target::Column::State.eq(&target.state))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Err(stale_lifecycle_error("one convergence retiring target"));
        }
    }

    let mut cleanup = Vec::new();
    for (provider, cid) in remote_pairs {
        let Some(epoch) = bump_remote_epoch(db, &provider, &cid, now).await? else {
            continue;
        };
        let remote = remote_pin::Entity::find_by_id((provider.clone(), cid.clone()))
            .one(db)
            .await?
            .ok_or_else(|| invalid("one convergence remote disappeared"))?;
        let job = if !desired_targets(db, &provider, &cid).await?.is_empty()
            || remote.request_id.is_none()
        {
            jobs::reconcile_job(&provider, &cid, epoch, now)
        } else {
            jobs::unpin_job(&provider, &cid, epoch, now)
        };
        cleanup.push(job);
    }
    Ok(cleanup)
}

async fn desired_targets<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Vec<DesiredTarget>> {
    #[cfg(test)]
    record_desired_targets_read(provider, cid).await;

    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::Cid.eq(cid))
        .filter(pin_lease_target::Column::State.is_in(active_target_states()))
        .order_by_asc(pin_lease_target::Column::CreatedAt)
        .order_by_asc(pin_lease_target::Column::Id)
        .all(db)
        .await?;
    let mut desired = Vec::with_capacity(targets.len());
    for target in targets {
        let Some(lease) = pin_lease::Entity::find_by_id(target.lease_id.clone())
            .one(db)
            .await?
        else {
            continue;
        };
        if lease.state == LEASE_ACTIVE {
            desired.push(DesiredTarget { target, lease });
        }
    }
    Ok(desired)
}

/// Reconstructs current projection outcomes for worker crash recovery without acquiring locks.
/// Mutating consumers must revalidate every row in a new canonical lifecycle transaction.
pub(crate) async fn current_remote_outcomes<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Vec<AffectedLeaseOutcome>> {
    let desired = desired_targets(db, provider, cid).await?;
    if desired.is_empty() {
        return Ok(Vec::new());
    }
    let lease_ids = desired
        .iter()
        .map(|desired| desired.lease.id.clone())
        .collect::<BTreeSet<_>>();
    let all_targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.is_in(lease_ids))
        .all(db)
        .await?;
    let available = all_targets
        .into_iter()
        .fold(BTreeMap::new(), |mut availability, target| {
            availability
                .entry(target.lease_id)
                .and_modify(|available| *available |= target.state == TARGET_PINNED)
                .or_insert(target.state == TARGET_PINNED);
            availability
        });
    desired
        .into_iter()
        .map(|desired| {
            Ok(AffectedLeaseOutcome {
                lease_id: desired.lease.id.clone(),
                target_id: desired.target.id,
                provider_mode: provider_mode(&desired.lease.provider_mode)?,
                available: available.get(&desired.lease.id).copied().unwrap_or(false),
            })
        })
        .collect()
}

/// Acquires the complete lifecycle frontier for one observed shared remote.
///
/// Discovery identifies desired targets for the observed `(provider, cid)` and their lease IDs.
/// Before the remote is read, the frontier then locks every affected lease and every target row of
/// those leases, including cross-provider and terminal siblings. PostgreSQL uses ordered `FOR
/// UPDATE` reads. SQLite uses exact no-op CASes on active lifecycle rows, which serializes the
/// same frontier when the caller owns a transaction; a contention/stale result is retried by the
/// caller's established SQLite path rather than by opening a hidden transaction here.
async fn ordered_desired_lifecycle_snapshot<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<OrderedDesiredLifecycleSnapshotOutcome> {
    let candidates = desired_targets(db, provider, cid).await?;

    let mut candidate_leases = BTreeMap::new();
    for desired in &candidates {
        candidate_leases.insert(desired.lease.id.clone(), desired.lease.clone());
    }
    let lease_ids: Vec<_> = candidate_leases.keys().cloned().collect();
    let mut expected_frontier = if lease_ids.is_empty() {
        Vec::new()
    } else {
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.is_in(lease_ids))
            .order_by_asc(pin_lease_target::Column::CreatedAt)
            .order_by_asc(pin_lease_target::Column::Id)
            .all(db)
            .await?
    };
    expected_frontier.sort_by(compare_target_order);

    let mut locked_leases = BTreeMap::new();
    for (lease_id, expected) in &candidate_leases {
        let lease = match lock_lifecycle_lease(db, expected, true).await {
            Ok(Some(lease)) => lease,
            Ok(None) => return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale),
            Err(error) if is_sqlite_lifecycle_contention(db, &error) => {
                return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale);
            }
            Err(error) => return Err(error.into()),
        };
        if lease.id != *lease_id {
            return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale);
        }
        locked_leases.insert(lease_id.clone(), lease);
    }

    let mut frontier = Vec::with_capacity(expected_frontier.len());
    for expected in &expected_frontier {
        let target = match lock_lifecycle_target(db, expected, true).await {
            Ok(Some(target)) => target,
            Ok(None) => return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale),
            Err(error) if is_sqlite_lifecycle_contention(db, &error) => {
                return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale);
            }
            Err(error) => return Err(error.into()),
        };
        let Some(lease) = locked_leases.get(&target.lease_id) else {
            return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale);
        };
        frontier.push(DesiredTarget {
            target,
            lease: lease.clone(),
        });
    }
    frontier.sort_by(|left, right| compare_target_order(&left.target, &right.target));
    let desired: Vec<_> = frontier
        .iter()
        .filter(|sibling| {
            sibling.target.provider == provider
                && sibling.target.cid == cid
                && is_desired_target_state(&sibling.target.state)
                && sibling.lease.state == LEASE_ACTIVE
        })
        .cloned()
        .collect();

    let remote = match lock_remote_after_lifecycle(db, provider, cid).await {
        Ok(Some(remote)) => remote,
        Ok(None) => return Ok(OrderedDesiredLifecycleSnapshotOutcome::MissingRemote),
        Err(error) if is_sqlite_lifecycle_contention(db, &error) => {
            return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale);
        }
        Err(error) => return Err(error.into()),
    };
    if desired.len() != candidates.len() {
        return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale);
    }
    if !same_desired_lifecycle_snapshot(&candidates, &desired) {
        return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale);
    }
    if remote.provider != provider || remote.cid != cid {
        return Ok(OrderedDesiredLifecycleSnapshotOutcome::Stale);
    }
    Ok(OrderedDesiredLifecycleSnapshotOutcome::Current(
        OrderedDesiredLifecycleSnapshot {
            desired,
            frontier,
            remote,
        },
    ))
}

fn is_sqlite_lifecycle_contention<C: ConnectionTrait>(db: &C, error: &sea_orm::DbErr) -> bool {
    db.get_database_backend() == DatabaseBackend::Sqlite && is_sqlite_contention(&error.to_string())
}

fn lifecycle_lease_query(expected: &pin_lease::Model) -> sea_orm::Select<pin_lease::Entity> {
    pin_lease::Entity::find_by_id(expected.id.clone())
        .filter(pin_lease::Column::OwnerObjectId.eq(&expected.owner_object_id))
        .filter(pin_lease::Column::Source.eq(&expected.source))
        .filter(pin_lease::Column::PolicyId.eq(&expected.policy_id))
        .filter(pin_lease::Column::ProviderMode.eq(&expected.provider_mode))
        .filter(pin_lease::Column::ContentMode.eq(&expected.content_mode))
        .filter(pin_lease::Column::CreatedAt.eq(expected.created_at))
        .filter(pin_lease::Column::LastTouchedAt.eq(expected.last_touched_at))
        .filter(pin_lease::Column::ExpiresAt.eq(expected.expires_at))
        .filter(pin_lease::Column::Generation.eq(expected.generation))
        .filter(pin_lease::Column::State.eq(&expected.state))
}

fn lifecycle_lease_lock_query(expected: &pin_lease::Model) -> sea_orm::Select<pin_lease::Entity> {
    lifecycle_lease_query(expected).lock_exclusive()
}

async fn lock_lifecycle_lease<C: ConnectionTrait>(
    db: &C,
    expected: &pin_lease::Model,
    sqlite_write_lock: bool,
) -> Result<Option<pin_lease::Model>, sea_orm::DbErr> {
    #[cfg(test)]
    record_lease_lock(&expected.id).await;

    if db.get_database_backend() == DatabaseBackend::Postgres {
        return lifecycle_lease_lock_query(expected).one(db).await;
    }
    if sqlite_write_lock && expected.state == LEASE_ACTIVE {
        let updated = pin_lease::Entity::update_many()
            .col_expr(
                pin_lease::Column::Generation,
                Expr::value(expected.generation),
            )
            .filter(pin_lease::Column::Id.eq(&expected.id))
            .filter(pin_lease::Column::OwnerObjectId.eq(&expected.owner_object_id))
            .filter(pin_lease::Column::Source.eq(&expected.source))
            .filter(pin_lease::Column::PolicyId.eq(&expected.policy_id))
            .filter(pin_lease::Column::ProviderMode.eq(&expected.provider_mode))
            .filter(pin_lease::Column::ContentMode.eq(&expected.content_mode))
            .filter(pin_lease::Column::CreatedAt.eq(expected.created_at))
            .filter(pin_lease::Column::LastTouchedAt.eq(expected.last_touched_at))
            .filter(pin_lease::Column::ExpiresAt.eq(expected.expires_at))
            .filter(pin_lease::Column::Generation.eq(expected.generation))
            .filter(pin_lease::Column::State.eq(&expected.state))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Ok(None);
        }
    }
    lifecycle_lease_query(expected).one(db).await
}

fn lifecycle_target_query(
    expected: &pin_lease_target::Model,
) -> sea_orm::Select<pin_lease_target::Entity> {
    pin_lease_target::Entity::find_by_id(expected.id.clone())
        .filter(pin_lease_target::Column::LeaseId.eq(&expected.lease_id))
        .filter(pin_lease_target::Column::Provider.eq(&expected.provider))
        .filter(pin_lease_target::Column::Cid.eq(&expected.cid))
        .filter(pin_lease_target::Column::LogicalSize.eq(expected.logical_size))
        .filter(pin_lease_target::Column::CreatedAt.eq(expected.created_at))
        .filter(pin_lease_target::Column::LastTouchedAt.eq(expected.last_touched_at))
        .filter(pin_lease_target::Column::State.eq(&expected.state))
}

fn lifecycle_target_lock_query(
    expected: &pin_lease_target::Model,
) -> sea_orm::Select<pin_lease_target::Entity> {
    lifecycle_target_query(expected).lock_exclusive()
}

async fn lock_lifecycle_target<C: ConnectionTrait>(
    db: &C,
    expected: &pin_lease_target::Model,
    sqlite_write_lock: bool,
) -> Result<Option<pin_lease_target::Model>, sea_orm::DbErr> {
    #[cfg(test)]
    record_target_lock(&expected.id).await;

    if db.get_database_backend() == DatabaseBackend::Postgres {
        return lifecycle_target_lock_query(expected).one(db).await;
    }
    if sqlite_write_lock {
        let updated = pin_lease_target::Entity::update_many()
            .col_expr(
                pin_lease_target::Column::State,
                Expr::value(expected.state.clone()),
            )
            .filter(pin_lease_target::Column::Id.eq(&expected.id))
            .filter(pin_lease_target::Column::LeaseId.eq(&expected.lease_id))
            .filter(pin_lease_target::Column::Provider.eq(&expected.provider))
            .filter(pin_lease_target::Column::Cid.eq(&expected.cid))
            .filter(pin_lease_target::Column::LogicalSize.eq(expected.logical_size))
            .filter(pin_lease_target::Column::CreatedAt.eq(expected.created_at))
            .filter(pin_lease_target::Column::LastTouchedAt.eq(expected.last_touched_at))
            .filter(pin_lease_target::Column::State.eq(&expected.state))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Ok(None);
        }
    }
    lifecycle_target_query(expected).one(db).await
}

fn lifecycle_remote_query(provider: &str, cid: &str) -> sea_orm::Select<remote_pin::Entity> {
    remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
}

fn lifecycle_remote_lock_query(provider: &str, cid: &str) -> sea_orm::Select<remote_pin::Entity> {
    lifecycle_remote_query(provider, cid).lock_exclusive()
}

#[cfg(test)]
fn render_ordered_lifecycle_prelock_queries(
    owner: &object::Model,
    leases: &[pin_lease::Model],
    targets: &[pin_lease_target::Model],
    provider: &str,
    cid: &str,
) -> Vec<(String, String)> {
    let mut leases = leases.to_vec();
    leases.sort_by(|left, right| left.id.cmp(&right.id));
    let mut targets = targets.to_vec();
    targets.sort_by(compare_target_order);
    let mut rendered = Vec::with_capacity(leases.len() + targets.len() + 2);
    rendered.push((
        format!("owner:{}", owner.id),
        renewal_owner_lock_query(owner)
            .build(DatabaseBackend::Postgres)
            .to_string(),
    ));
    for lease in &leases {
        rendered.push((
            format!("lease:{}", lease.id),
            lifecycle_lease_lock_query(lease)
                .build(DatabaseBackend::Postgres)
                .to_string(),
        ));
    }
    for target in &targets {
        rendered.push((
            format!("target:{}", target.id),
            lifecycle_target_lock_query(target)
                .build(DatabaseBackend::Postgres)
                .to_string(),
        ));
    }
    rendered.push((
        format!("remote:{provider}:{cid}"),
        lifecycle_remote_lock_query(provider, cid)
            .build(DatabaseBackend::Postgres)
            .to_string(),
    ));
    rendered
}

async fn lock_remote_after_lifecycle<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> Result<Option<remote_pin::Model>, sea_orm::DbErr> {
    #[cfg(test)]
    record_remote_lock(provider, cid).await;

    if db.get_database_backend() == DatabaseBackend::Postgres {
        lifecycle_remote_lock_query(provider, cid).one(db).await
    } else {
        lifecycle_remote_query(provider, cid).one(db).await
    }
}

fn same_desired_lifecycle_snapshot(left: &[DesiredTarget], right: &[DesiredTarget]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.lease.id == right.lease.id
                && left.lease.generation == right.lease.generation
                && left.lease.state == right.lease.state
                && left.target.id == right.target.id
                && left.target.lease_id == right.target.lease_id
                && left.target.provider == right.target.provider
                && left.target.cid == right.target.cid
                && left.target.logical_size == right.target.logical_size
                && left.target.created_at == right.target.created_at
                && left.target.last_touched_at == right.target.last_touched_at
                && left.target.state == right.target.state
        })
}

#[derive(Clone, Copy)]
enum ProjectionAccess {
    Admission,
    Observation { historical_poll: bool },
}

async fn project_prelocked_desired_target<C: ConnectionTrait>(
    db: &C,
    desired: &DesiredTarget,
    all_desired: &[DesiredTarget],
    remote: &remote_pin::Model,
    access: ProjectionAccess,
    now: DateTimeUtc,
) -> AppResult<TargetProjection> {
    // Admission may create/attach work; an observation only updates an existing
    // desired target. A retired/changed route cannot authorize new jobs, but it
    // cannot invalidate a captured, already-fenced provider response either.
    let schedule_work = match access {
        ProjectionAccess::Admission => {
            quota::assert_reusable_route(db, &desired.target.provider, &desired.target.cid).await?;
            true
        }
        ProjectionAccess::Observation { .. } if remote.status == REMOTE_PINNED => false,
        ProjectionAccess::Observation { historical_poll } => {
            match quota::assert_reusable_route(db, &desired.target.provider, &desired.target.cid)
                .await
            {
                Ok(()) => true,
                Err(AppError::InvalidPinningRequest(_)) => {
                    historical_poll
                        && matches!(remote.status.as_str(), REMOTE_QUEUED | REMOTE_PINNING)
                }
                Err(error) => return Err(error),
            }
        }
    };
    #[cfg(test)]
    record_target_projection(&desired.target.id).await;

    let canonical = || {
        all_desired
            .first()
            .ok_or_else(|| invalid("remote has no active desired target"))
    };
    match remote.status.as_str() {
        REMOTE_PINNED => {
            set_prelocked_target_state(db, &desired.target, TARGET_PINNED).await?;
            Ok(TargetProjection::Pinned)
        }
        REMOTE_QUEUED | REMOTE_PINNING if remote.request_id.is_some() => {
            set_prelocked_target_state(db, &desired.target, TARGET_SUBMITTED).await?;
            let canonical = canonical()?;
            let request_id = remote.request_id.as_deref().expect("checked above");
            let NewPinJob::Target(job) = jobs::poll_job(
                &desired.target.provider,
                &desired.target.cid,
                &canonical.lease.id,
                &canonical.target.id,
                canonical.lease.generation,
                request_id,
                now + duration_as_chrono(jobs::POLL_INTERVAL)?,
            ) else {
                unreachable!("poll constructor is target scoped")
            };
            let poll_job_id = job.id.clone();
            if schedule_work {
                ensure_prelocked_target_job(db, job, now).await?;
            }
            Ok(TargetProjection::Submitted { poll_job_id })
        }
        REMOTE_RESERVED if remote.request_id.is_none() => {
            set_prelocked_target_state(db, &desired.target, TARGET_WAITING).await?;
            let canonical = canonical()?;
            let NewPinJob::Target(job) = jobs::submit_job(
                &desired.target.provider,
                &desired.target.cid,
                &canonical.lease.id,
                &canonical.target.id,
                canonical.lease.generation,
                now,
            ) else {
                unreachable!("submit constructor is target scoped")
            };
            let submit_job_id = job.id.clone();
            if !schedule_work {
                return Ok(TargetProjection::Waiting { submit_job_id });
            }
            if let Some(blocking) = jobs::blocking_live_submit(
                db,
                &desired.target.provider,
                &desired.target.cid,
                &submit_job_id,
            )
            .await?
            {
                ensure_reconcile(
                    db,
                    &desired.target.provider,
                    &desired.target.cid,
                    remote.epoch,
                    now,
                )
                .await?;
                return Ok(TargetProjection::Waiting {
                    submit_job_id: blocking.id,
                });
            }
            ensure_prelocked_target_job(db, job, now).await?;
            Ok(TargetProjection::Waiting { submit_job_id })
        }
        REMOTE_FAILED => {
            set_prelocked_target_state(db, &desired.target, TARGET_DEGRADED).await?;
            let NewPinJob::Remote(job) = jobs::reconcile_job(
                &desired.target.provider,
                &desired.target.cid,
                remote.epoch,
                remote.next_retry_at.unwrap_or(now),
            ) else {
                unreachable!("reconcile constructor is remote scoped")
            };
            let reconcile_job_id = job.id.clone();
            if schedule_work
                && remote.failure_attempts < MAX_FAILED_REQUEST_ATTEMPTS
                && remote.next_retry_at.is_some()
                && all_desired
                    .iter()
                    .any(|target| target.lease.provider_mode == "all")
            {
                jobs::ensure_or_reactivate_reconcile_job(db, job, now).await?;
            }
            Ok(TargetProjection::Degraded { reconcile_job_id })
        }
        REMOTE_ABSENT => Err(invalid(
            "remote absent projection requires explicit provider limits",
        )),
        _ => Err(invalid("remote request and status are inconsistent")),
    }
}

/// Persists work for a canonical target that was selected from the caller's complete lifecycle
/// snapshot. Unlike the generic job helpers, this intentionally does not re-query canonical
/// siblings after the lease → target → remote frontier has been acquired.
async fn ensure_prelocked_target_job<C: ConnectionTrait>(
    db: &C,
    job: jobs::TargetPinJob,
    now: DateTimeUtc,
) -> AppResult<()> {
    let submit_phase = match job.operation {
        jobs::TargetJobOperation::Submit => Some("ready"),
        jobs::TargetJobOperation::Poll => None,
    };
    let Some(existing) = pin_job::Entity::find_by_id(job.id.clone()).one(db).await? else {
        jobs::insert_new_job(db, NewPinJob::Target(job), now).await?;
        return Ok(());
    };
    match existing.state.as_str() {
        JOB_PENDING | JOB_RUNNING => Ok(()),
        JOB_DONE => {
            // A stable target job ID can survive a released/reallocated remote. Never
            // borrow today's route/epoch for a historical or uncaptured done row.
            if let Some(row) = ledger::get(db, &job.provider, &job.cid).await? {
                let route = row.route.ok_or_else(|| {
                    invalid("prelocked target job has no current invocation identity")
                })?;
                let captured = pin_invocation_route::Entity::find_by_id(existing.id.clone())
                    .one(db)
                    .await?;
                let remote =
                    remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
                        .one(db)
                        .await?;
                if !captured.zip(remote).is_some_and(|(captured, remote)| {
                    captured.route == route && captured.remote_epoch == remote.epoch
                }) {
                    return Err(invalid(
                        "prelocked target job has no current invocation identity",
                    ));
                }
            }
            let updated = pin_job::Entity::update_many()
                .col_expr(pin_job::Column::State, Expr::value(JOB_PENDING.to_owned()))
                .col_expr(pin_job::Column::Attempts, Expr::value(0_i32))
                .col_expr(
                    pin_job::Column::NextAttemptAt,
                    Expr::value(job.next_attempt_at),
                )
                .col_expr(
                    pin_job::Column::LockedUntil,
                    Expr::value(Option::<DateTimeUtc>::None),
                )
                .col_expr(
                    pin_job::Column::SubmitPhase,
                    Expr::value(submit_phase.map(str::to_owned)),
                )
                .col_expr(
                    pin_job::Column::LastError,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
                .filter(pin_job::Column::Id.eq(existing.id))
                .filter(pin_job::Column::State.eq(JOB_DONE))
                .exec(db)
                .await?;
            if updated.rows_affected != 1 {
                return Err(stale_lifecycle_error("prelocked target job reactivation"));
            }
            Ok(())
        }
        _ => Err(invalid("pin job has an invalid persisted state")),
    }
}

async fn set_prelocked_target_state<C: ConnectionTrait>(
    db: &C,
    target: &pin_lease_target::Model,
    state: &str,
) -> AppResult<()> {
    let updated = pin_lease_target::Entity::update_many()
        .col_expr(
            pin_lease_target::Column::State,
            Expr::value(state.to_owned()),
        )
        .filter(pin_lease_target::Column::Id.eq(&target.id))
        .filter(pin_lease_target::Column::LeaseId.eq(&target.lease_id))
        .filter(pin_lease_target::Column::Provider.eq(&target.provider))
        .filter(pin_lease_target::Column::Cid.eq(&target.cid))
        .filter(pin_lease_target::Column::LogicalSize.eq(target.logical_size))
        .filter(pin_lease_target::Column::CreatedAt.eq(target.created_at))
        .filter(pin_lease_target::Column::State.eq(&target.state))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(stale_lifecycle_error("prelocked target projection"));
    }
    Ok(())
}

async fn lease_targets<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
) -> AppResult<Vec<pin_lease_target::Model>> {
    Ok(pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
        .order_by_asc(pin_lease_target::Column::CreatedAt)
        .order_by_asc(pin_lease_target::Column::Id)
        .all(db)
        .await?)
}

async fn has_all_mode_target<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<bool> {
    Ok(desired_targets(db, provider, cid)
        .await?
        .iter()
        .any(|desired| desired.lease.provider_mode == "all"))
}

async fn mark_terminal_targets_released<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<()> {
    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::Cid.eq(cid))
        .filter(pin_lease_target::Column::State.is_in(active_target_states()))
        .all(db)
        .await?;
    for target in targets {
        pin_lease_target::Entity::update_many()
            .col_expr(
                pin_lease_target::Column::State,
                Expr::value(TARGET_RELEASED.to_owned()),
            )
            .filter(pin_lease_target::Column::Id.eq(&target.id))
            .filter(pin_lease_target::Column::State.eq(&target.state))
            .filter(
                pin_lease_target::Column::LeaseId.in_subquery(
                    pin_lease::Entity::find()
                        .select_only()
                        .column(pin_lease::Column::Id)
                        .filter(pin_lease::Column::State.is_in([
                            LEASE_EXPIRED,
                            LEASE_CANCELLED,
                            LEASE_EVICTED,
                        ]))
                        .into_query(),
                ),
            )
            .exec(db)
            .await?;
    }
    Ok(())
}

async fn ensure_reconcile<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    epoch: i64,
    at: DateTimeUtc,
) -> AppResult<String> {
    let NewPinJob::Remote(job) = jobs::reconcile_job(provider, cid, epoch, at) else {
        unreachable!("reconcile constructor is remote scoped")
    };
    let id = job.id.clone();
    jobs::ensure_or_reactivate_reconcile_job(db, job, at).await?;
    Ok(id)
}

async fn mark_polls_for_request_done<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    request_id: &str,
    now: DateTimeUtc,
) -> AppResult<()> {
    let hash = hex::encode(Sha256::digest(request_id.as_bytes()));
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(JOB_DONE.to_owned()))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Provider.eq(provider))
        .filter(pin_job::Column::Cid.eq(cid))
        .filter(pin_job::Column::Operation.eq("poll"))
        .filter(pin_job::Column::Id.like(format!("%:{hash}")))
        .exec(db)
        .await?;
    Ok(())
}

async fn bump_remote_epoch<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTimeUtc,
) -> AppResult<Option<i64>> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let epoch = increment_epoch(remote.epoch)?;

    #[cfg(test)]
    record_remote_work(provider, cid).await;

    let updated = remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::Epoch, Expr::value(epoch))
        .col_expr(remote_pin::Column::LastTouchedAt, Expr::value(now))
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Cid.eq(cid))
        .filter(remote_pin::Column::Epoch.eq(remote.epoch))
        .exec(db)
        .await?;
    Ok((updated.rows_affected == 1).then_some(epoch))
}

async fn end_leases<C: ConnectionTrait>(
    db: &C,
    leases: &[pin_lease::Model],
    state: &str,
    target_state: &str,
    now: DateTimeUtc,
) -> AppResult<()> {
    let mut pairs = BTreeSet::new();
    let mut ordered_leases: Vec<_> = leases
        .iter()
        .filter(|lease| lease.state == LEASE_ACTIVE)
        .collect();
    ordered_leases.sort_by(|left, right| left.id.cmp(&right.id));

    // Complete every lease CAS before taking target rows, so callers acquire lifecycle rows in
    // one global lease → target → remote order.
    for lease in &ordered_leases {
        #[cfg(test)]
        record_lease_cas(&lease.id).await;

        let updated = pin_lease::Entity::update_many()
            .col_expr(pin_lease::Column::State, Expr::value(state.to_owned()))
            .col_expr(
                pin_lease::Column::Generation,
                Expr::value(increment_epoch(lease.generation)?),
            )
            .col_expr(pin_lease::Column::LastTouchedAt, Expr::value(now))
            .filter(pin_lease::Column::Id.eq(&lease.id))
            .filter(pin_lease::Column::Generation.eq(lease.generation))
            .filter(pin_lease::Column::State.eq(LEASE_ACTIVE))
            .exec(db)
            .await?;
        if updated.rows_affected != 1 {
            return Err(AppError::Database(
                "stale lease lifecycle compare-and-set".to_owned(),
            ));
        }
    }

    let mut targets = Vec::new();
    for lease in ordered_leases {
        targets.extend(
            lease_targets(db, &lease.id)
                .await?
                .into_iter()
                .filter(|target| {
                    is_desired_target_state(&target.state)
                        || matches!(
                            target.state.as_str(),
                            TARGET_QUOTA_WAITING | TARGET_QUOTA_BLOCKED
                        )
                }),
        );
    }
    targets.sort_by(compare_target_order);
    for target in targets {
        if is_desired_target_state(&target.state) {
            pairs.insert((target.provider.clone(), target.cid.clone()));
        }

        #[cfg(test)]
        record_target_cas(&target.id).await;

        pin_lease_target::Entity::update_many()
            .col_expr(
                pin_lease_target::Column::State,
                Expr::value(target_state.to_owned()),
            )
            .filter(pin_lease_target::Column::Id.eq(&target.id))
            .exec(db)
            .await?;
    }
    for (provider, cid) in pairs {
        if let Some(epoch) = bump_remote_epoch(db, &provider, &cid, now).await? {
            schedule_after_desired_change(db, &provider, &cid, epoch, now).await?;
        }
    }
    Ok(())
}

async fn schedule_after_desired_change<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    epoch: i64,
    now: DateTimeUtc,
) -> AppResult<()> {
    let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?;
    let Some(remote) = remote else {
        return Ok(());
    };
    if !desired_targets(db, provider, cid).await?.is_empty() {
        if remote.status != REMOTE_ABSENT {
            ensure_reconcile(db, provider, cid, epoch, now).await?;
        }
    } else if remote.status != REMOTE_ABSENT || remote.request_id.is_some() {
        let NewPinJob::Remote(job) = jobs::unpin_job(provider, cid, epoch, now) else {
            unreachable!("unpin constructor is remote scoped")
        };
        jobs::enqueue_job(db, NewPinJob::Remote(job)).await?;
    }
    Ok(())
}

fn compare_target_order(
    left: &pin_lease_target::Model,
    right: &pin_lease_target::Model,
) -> std::cmp::Ordering {
    left.created_at
        .cmp(&right.created_at)
        .then_with(|| left.id.cmp(&right.id))
}

pub(crate) fn target_id(lease_id: &str, provider: &str, cid: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(lease_id.as_bytes());
    digest.update([0]);
    digest.update(provider.as_bytes());
    digest.update([0]);
    digest.update(cid.as_bytes());
    format!("target:{}", hex::encode(digest.finalize()))
}

fn same_content_cid(left: &str, right: &str) -> bool {
    left == right || crate::pinning::provider::cids_equivalent(left, right).unwrap_or(false)
}

fn active_target_states() -> [&'static str; 4] {
    [
        TARGET_WAITING,
        TARGET_SUBMITTED,
        TARGET_PINNED,
        TARGET_DEGRADED,
    ]
}

fn is_desired_target_state(state: &str) -> bool {
    active_target_states().contains(&state)
}

fn is_capacity_holding_status(status: &str) -> bool {
    matches!(
        status,
        REMOTE_RESERVED | REMOTE_QUEUED | REMOTE_PINNING | REMOTE_PINNED | REMOTE_FAILED
    )
}

fn remote_status_name(status: RemotePinStatus) -> &'static str {
    match status {
        RemotePinStatus::Queued => REMOTE_QUEUED,
        RemotePinStatus::Pinning => REMOTE_PINNING,
        RemotePinStatus::Pinned => REMOTE_PINNED,
        RemotePinStatus::Failed => REMOTE_FAILED,
    }
}

fn target_state_from_remote(status: &str) -> Option<&'static str> {
    match status {
        REMOTE_RESERVED => Some(TARGET_WAITING),
        REMOTE_QUEUED | REMOTE_PINNING => Some(TARGET_SUBMITTED),
        REMOTE_PINNED => Some(TARGET_PINNED),
        REMOTE_FAILED => Some(TARGET_DEGRADED),
        _ => None,
    }
}

fn provider_mode(raw: &str) -> AppResult<ProviderMode> {
    match raw {
        "one" => Ok(ProviderMode::One),
        "all" => Ok(ProviderMode::All),
        _ => Err(invalid("lease has invalid provider mode")),
    }
}

fn increment_epoch(value: i64) -> AppResult<i64> {
    value
        .checked_add(1)
        .ok_or_else(|| invalid("generation or remote epoch overflow"))
}

fn failure_backoff(attempts: i32) -> Duration {
    let mut delay = FAILED_REQUEST_BASE_BACKOFF;
    for _ in 1..attempts.max(1) {
        delay = delay
            .checked_mul(2)
            .unwrap_or(FAILED_REQUEST_MAX_BACKOFF)
            .min(FAILED_REQUEST_MAX_BACKOFF);
    }
    delay
}

fn duration_as_chrono(duration: Duration) -> AppResult<ChronoDuration> {
    ChronoDuration::from_std(duration).map_err(|_| invalid("pinning delay exceeds chrono range"))
}

async fn current_generation<C: ConnectionTrait>(db: &C, lease_id: &str) -> AppResult<i64> {
    pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
        .map(|lease| lease.generation)
        .ok_or_else(|| invalid("target lease does not exist"))
}

fn invalid(message: &str) -> AppError {
    AppError::InvalidPinningRequest(message.to_owned())
}

fn stale_lifecycle_error(operation: &str) -> AppError {
    AppError::Database(format!("stale {operation} compare-and-set"))
}

fn app_to_renewal_error(error: AppError) -> RenewManualLeaseError {
    match error {
        AppError::Database(message) => {
            RenewManualLeaseError::Database(sea_orm::DbErr::Custom(message))
        }
        other => RenewManualLeaseError::Database(sea_orm::DbErr::Custom(other.to_string())),
    }
}

fn stale_renewal_error() -> RenewManualLeaseError {
    RenewManualLeaseError::Database(sea_orm::DbErr::Custom(
        "stale manual lease renewal compare-and-set".to_owned(),
    ))
}

fn stale_renewal_revalidation_error(backend: DatabaseBackend) -> RenewManualLeaseError {
    if backend == DatabaseBackend::Postgres {
        RenewManualLeaseError::Database(sea_orm::DbErr::Custom(
            "stale manual lease renewal snapshot; PostgreSQL caller transaction must roll back before retry"
                .to_owned(),
        ))
    } else {
        stale_renewal_error()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{Duration as ChronoDuration, TimeZone};
    use sea_orm::{
        ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait,
        PaginatorTrait, QueryFilter, TransactionTrait,
    };

    use super::test_gates;
    use super::*;
    use crate::{
        pinning::config::{ProviderLimitMap, ProviderLimits},
        store::entities::{pin_job, pin_lease, pin_lease_target, pin_provider_usage, remote_pin},
    };

    fn time(seconds: i64) -> DateTimeUtc {
        Utc.with_ymd_and_hms(2026, 7, 21, 0, 0, 0).single().unwrap()
            + ChronoDuration::seconds(seconds)
    }

    async fn setup() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        db.execute_unprepared(
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest) \
             VALUES ('object-1', 'bucket', 'key', 'QmObject', 100, 'QmObject', TRUE)",
        )
        .await
        .unwrap();
        db
    }

    async fn setup_file_backed(name: &str) -> (tempfile::TempDir, DatabaseConnection) {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join(format!("{name}.sqlite"));
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("PRAGMA journal_mode = WAL")
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        db.execute_unprepared(
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest) \
             VALUES ('object-1', 'bucket', 'key', 'QmObject', 100, 'QmObject', TRUE)",
        )
        .await
        .unwrap();
        (directory, db)
    }

    async fn seed_remote(
        db: &DatabaseConnection,
        provider: &str,
        cid: &str,
        status: &str,
        request_id: Option<&str>,
        epoch: i64,
    ) {
        let request_id = request_id
            .map(|value| format!("'{value}'"))
            .unwrap_or_else(|| "NULL".to_owned());
        db.execute_unprepared(&format!(
            "INSERT INTO remote_pins \
             (provider, cid, request_id, cid_size, status, epoch, failure_attempts, last_touched_at) \
             VALUES ('{provider}', '{cid}', {request_id}, 100, '{status}', {epoch}, 0, '{}')",
            time(0).to_rfc3339(),
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT OR IGNORE INTO pin_provider_usage (provider, reserved_bytes, reserved_pins) \
             VALUES ('{provider}', 100, 1)"
        ))
        .await
        .unwrap();
    }

    async fn seed_absent_remote_without_usage(
        db: &DatabaseConnection,
        provider: &str,
        cid: &str,
        epoch: i64,
    ) {
        db.execute_unprepared(&format!(
            "INSERT INTO remote_pins \
             (provider, cid, request_id, cid_size, status, epoch, failure_attempts, last_touched_at) \
             VALUES ('{provider}', '{cid}', NULL, 100, 'absent', {epoch}, 0, '{}')",
            time(0).to_rfc3339(),
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn worker_observation_cannot_reactivate_quota_released_absent_remote() {
        let db = setup().await;
        seed_absent_remote_without_usage(&db, "pinata", "bafy-absent-observation", 7).await;

        assert_eq!(
            apply_worker_remote_status(
                &db,
                RemoteStatusUpdate {
                    provider: "pinata",
                    cid: "bafy-absent-observation",
                    request_id: "stale-request",
                    origin: RemoteStatusOrigin::Adopt,
                    status: RemotePinStatus::Queued,
                    error_class: None,
                    error_text: None,
                    now: time(1),
                },
            )
            .await
            .unwrap(),
            RemoteStatusApplyResult::StaleRequest
        );
        let remote = remote(&db, "pinata", "bafy-absent-observation").await;
        assert_eq!(
            (remote.status.as_str(), remote.request_id),
            ("absent", None)
        );
        assert!(
            pin_provider_usage::Entity::find_by_id("pinata".to_owned())
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
    }

    fn limits(max_bytes: i64, max_pins: i64) -> ProviderLimitMap {
        ProviderLimitMap::from([(
            "pinata".to_owned(),
            ProviderLimits {
                priority: 1,
                max_bytes,
                max_pins,
                enabled: true,
            },
        )])
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_lease_target(
        db: &DatabaseConnection,
        lease_id: &str,
        source: &str,
        provider_mode: &str,
        lease_state: &str,
        generation: i64,
        target_id: &str,
        provider: &str,
        cid: &str,
        target_state: &str,
        expires_at: DateTimeUtc,
    ) {
        let created = time(0).to_rfc3339();
        db.execute_unprepared(&format!(
            "INSERT INTO pin_leases \
             (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
              last_touched_at, expires_at, generation, state) \
             VALUES ('{lease_id}', 'object-1', '{source}', 'policy', '{provider_mode}', 'full', \
                     '{created}', '{created}', '{}', {generation}, '{lease_state}')",
            expires_at.to_rfc3339(),
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('{target_id}', '{lease_id}', '{cid}', 100, '{provider}', '{target_state}', \
                     '{created}', '{created}')"
        ))
        .await
        .unwrap();
    }

    async fn remote(db: &DatabaseConnection, provider: &str, cid: &str) -> remote_pin::Model {
        remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    async fn target_state(db: &DatabaseConnection, target_id: &str) -> String {
        pin_lease_target::Entity::find_by_id(target_id.to_owned())
            .one(db)
            .await
            .unwrap()
            .unwrap()
            .state
    }

    async fn lease(db: &DatabaseConnection, lease_id: &str) -> pin_lease::Model {
        pin_lease::Entity::find_by_id(lease_id.to_owned())
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    async fn lease_target_rows(
        db: &DatabaseConnection,
        lease_id: &str,
    ) -> Vec<pin_lease_target::Model> {
        lease_targets(db, lease_id).await.unwrap()
    }

    async fn usage(db: &DatabaseConnection, provider: &str) -> (i64, i64) {
        let row = quota::read_usage(db, provider).await.unwrap().unwrap();
        (row.reserved_bytes, row.reserved_pins)
    }

    async fn start_lifecycle_order_recording(
        lease_ids: &[&str],
        target_ids: &[&str],
        remote_pairs: &[(&str, &str)],
    ) {
        *test_gates::LIFECYCLE_ORDER_RECORDER.lock().await =
            Some(test_gates::LifecycleOrderRecorder {
                owner_ids: BTreeSet::new(),
                lease_ids: lease_ids.iter().map(|id| (*id).to_owned()).collect(),
                target_ids: target_ids.iter().map(|id| (*id).to_owned()).collect(),
                remote_pairs: remote_pairs
                    .iter()
                    .map(|(provider, cid)| ((*provider).to_owned(), (*cid).to_owned()))
                    .collect(),
                record_desired_target_reads: false,
                events: Vec::new(),
            });
    }

    async fn start_lifecycle_order_recording_with_desired_target_reads(
        lease_ids: &[&str],
        target_ids: &[&str],
        remote_pairs: &[(&str, &str)],
    ) {
        *test_gates::LIFECYCLE_ORDER_RECORDER.lock().await =
            Some(test_gates::LifecycleOrderRecorder {
                owner_ids: BTreeSet::new(),
                lease_ids: lease_ids.iter().map(|id| (*id).to_owned()).collect(),
                target_ids: target_ids.iter().map(|id| (*id).to_owned()).collect(),
                remote_pairs: remote_pairs
                    .iter()
                    .map(|(provider, cid)| ((*provider).to_owned(), (*cid).to_owned()))
                    .collect(),
                record_desired_target_reads: true,
                events: Vec::new(),
            });
    }

    async fn include_owner_in_lifecycle_order_recording(owner_id: &str) {
        test_gates::LIFECYCLE_ORDER_RECORDER
            .lock()
            .await
            .as_mut()
            .expect("lifecycle order recorder was configured")
            .owner_ids
            .insert(owner_id.to_owned());
    }

    async fn finish_lifecycle_order_recording() -> Vec<test_gates::LifecycleOrderEvent> {
        test_gates::LIFECYCLE_ORDER_RECORDER
            .lock()
            .await
            .take()
            .expect("lifecycle order recorder was configured")
            .events
    }

    async fn apply_status_in_caller_owned_sqlite_transaction_with_retry(
        db: DatabaseConnection,
        update: RemoteStatusUpdate<'static>,
        start: Arc<tokio::sync::Barrier>,
    ) -> RemoteStatusApplyResult {
        for attempt in 0..REMOTE_STATUS_WRITE_RETRY_LIMIT {
            let transaction = db.begin().await.unwrap();
            if attempt == 0 {
                start.wait().await;
            }
            match apply_remote_status(&transaction, update.clone()).await {
                Ok(result) => match transaction.commit().await {
                    Ok(()) => return result,
                    Err(error) if is_sqlite_contention(&error.to_string()) => {}
                    Err(error) => panic!("caller-owned SQLite status commit failed: {error}"),
                },
                Err(error) => {
                    let retryable = is_sqlite_contention(&error.to_string())
                        || error.to_string().contains("exhausted concurrent retries");
                    transaction.rollback().await.unwrap();
                    if !retryable {
                        panic!("caller-owned SQLite status update failed: {error}");
                    }
                }
            }
            remote_status_retry_delay(attempt).await;
        }
        panic!("caller-owned SQLite status update exhausted retries");
    }

    #[test]
    fn generation_guard_exposes_current_stale_and_no_longer_needed() {
        assert_eq!(GenerationDecision::Current, GenerationDecision::Current);
    }

    #[tokio::test]
    async fn target_and_remote_guards_reject_stale_generations_and_epochs() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy", "reserved", None, 4).await;
        seed_lease_target(
            &db,
            "lease-guard",
            "guard",
            "all",
            "active",
            3,
            "target-guard",
            "pinata",
            "bafy",
            "waiting",
            time(100),
        )
        .await;
        assert_eq!(
            check_target_generation(&db, "target-guard", 3)
                .await
                .unwrap(),
            GenerationDecision::Current
        );
        assert_eq!(
            check_remote_epoch(&db, "pinata", "bafy", 4).await.unwrap(),
            GenerationDecision::Current
        );
        db.execute_unprepared("UPDATE pin_leases SET generation = 4 WHERE id = 'lease-guard'")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE remote_pins SET epoch = 5 WHERE provider = 'pinata' AND cid = 'bafy'",
        )
        .await
        .unwrap();
        assert_eq!(
            check_target_generation(&db, "target-guard", 3)
                .await
                .unwrap(),
            GenerationDecision::Stale
        );
        assert_eq!(
            check_remote_epoch(&db, "pinata", "bafy", 4).await.unwrap(),
            GenerationDecision::Stale
        );
    }

    #[tokio::test]
    async fn retry_degradation_cannot_cross_a_manual_renewal_generation() {
        let (_directory, db) = setup_file_backed("retry-degrade-renewal-race").await;
        seed_remote(
            &db,
            "pinata",
            "bafy-retry-degrade-renewal",
            "reserved",
            None,
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-retry-degrade-renewal",
            "manual",
            "all",
            "active",
            1,
            "target-retry-degrade-renewal",
            "pinata",
            "bafy-retry-degrade-renewal",
            "waiting",
            time(100),
        )
        .await;
        jobs::enqueue_job(
            &db,
            jobs::submit_job(
                "pinata",
                "bafy-retry-degrade-renewal",
                "lease-retry-degrade-renewal",
                "target-retry-degrade-renewal",
                1,
                time(0),
            ),
        )
        .await
        .unwrap();
        let job = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let gate = Arc::new(test_gates::RetryDegradeAfterLeaseSnapshotGate {
            lease_id: "lease-retry-degrade-renewal",
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        *test_gates::RETRY_DEGRADE_AFTER_LEASE_SNAPSHOT.lock().await = Some(gate.clone());

        let degradation_db = db.clone();
        let degradation = tokio::spawn(async move {
            let transaction = degradation_db.begin().await.unwrap();
            let result = mark_all_mode_target_degraded_for_retry(&transaction, &job, time(2)).await;
            if result.is_ok() {
                transaction.commit().await.unwrap();
            } else {
                transaction.rollback().await.unwrap();
            }
            result
        });
        gate.arrived.notified().await;
        let renewal = db.begin().await.unwrap();
        assert_eq!(
            renew_manual_lease(
                &renewal,
                "object-1",
                "lease-retry-degrade-renewal",
                time(200),
                time(1),
            )
            .await
            .unwrap(),
            ManualLeaseRenewalOutcome::Extended { generation: 2 }
        );
        renewal.commit().await.unwrap();
        gate.resume.notify_one();
        let degradation_result = degradation.await.unwrap();
        *test_gates::RETRY_DEGRADE_AFTER_LEASE_SNAPSHOT.lock().await = None;
        degradation_result.unwrap();

        assert_eq!(
            lease(&db, "lease-retry-degrade-renewal").await.generation,
            2
        );
        assert_eq!(
            target_state(&db, "target-retry-degrade-renewal").await,
            "waiting",
            "old recovery work must not degrade the renewed generation"
        );
    }

    #[tokio::test]
    async fn one_status_projects_active_shared_targets_and_request_ownership_is_exact() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy", "queued", Some("request-1"), 4).await;
        seed_lease_target(
            &db,
            "lease-one",
            "automatic",
            "one",
            "active",
            1,
            "target-one",
            "pinata",
            "bafy",
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-all",
            "copy",
            "all",
            "active",
            1,
            "target-all",
            "pinata",
            "bafy",
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-terminal",
            "old",
            "all",
            "cancelled",
            1,
            "target-terminal",
            "pinata",
            "bafy",
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-quota",
            "blocked",
            "all",
            "active",
            1,
            "target-quota",
            "pinata",
            "bafy",
            "quota_waiting",
            time(100),
        )
        .await;

        let result = apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy",
                request_id: "request-1",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Pinned,
                error_class: None,
                error_text: None,
                now: time(1),
            },
        )
        .await
        .unwrap();
        let RemoteStatusApplyResult::Applied {
            affected, failure, ..
        } = result
        else {
            panic!("expected accepted provider observation")
        };
        assert_eq!(affected.len(), 2);
        assert!(affected.iter().all(|outcome| outcome.available));
        assert_eq!(failure, None);
        assert_eq!(target_state(&db, "target-one").await, "pinned");
        assert_eq!(target_state(&db, "target-all").await, "pinned");
        assert_eq!(target_state(&db, "target-terminal").await, "waiting");
        assert_eq!(target_state(&db, "target-quota").await, "quota_waiting");

        assert!(matches!(
            apply_remote_status(
                &db,
                RemoteStatusUpdate {
                    provider: "pinata",
                    cid: "bafy",
                    request_id: "old-request",
                    origin: RemoteStatusOrigin::ExistingRequest,
                    status: RemotePinStatus::Pinned,
                    error_class: None,
                    error_text: None,
                    now: time(2),
                },
            )
            .await
            .unwrap(),
            RemoteStatusApplyResult::StaleRequest
        ));
        assert!(matches!(
            apply_remote_status(
                &db,
                RemoteStatusUpdate {
                    provider: "pinata",
                    cid: "bafy",
                    request_id: "replacement",
                    origin: RemoteStatusOrigin::Adopt,
                    status: RemotePinStatus::Pinned,
                    error_class: None,
                    error_text: None,
                    now: time(2),
                },
            )
            .await
            .unwrap(),
            RemoteStatusApplyResult::StaleRequest
        ));
    }

    #[tokio::test]
    async fn failed_requests_are_idempotent_back_off_and_stop_after_eight() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy", "queued", Some("request-1"), 1).await;
        seed_lease_target(
            &db,
            "lease-all",
            "automatic",
            "all",
            "active",
            1,
            "target-all",
            "pinata",
            "bafy",
            "submitted",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-all-copy",
            "copy",
            "all",
            "active",
            1,
            "target-all-copy",
            "pinata",
            "bafy",
            "submitted",
            time(100),
        )
        .await;

        for attempt in 1..=MAX_FAILED_REQUEST_ATTEMPTS {
            let request_id = format!("request-{attempt}");
            let origin = if attempt == 1 {
                RemoteStatusOrigin::ExistingRequest
            } else {
                RemoteStatusOrigin::Adopt
            };
            let failure_at = time(i64::from(attempt) * 10);
            let first = apply_remote_status(
                &db,
                RemoteStatusUpdate {
                    provider: "pinata",
                    cid: "bafy",
                    request_id: &request_id,
                    origin,
                    status: RemotePinStatus::Failed,
                    error_class: Some("transient"),
                    error_text: Some("redacted"),
                    now: failure_at,
                },
            )
            .await
            .unwrap();
            let RemoteStatusApplyResult::Applied {
                affected,
                failure: Some(progress),
                ..
            } = first
            else {
                panic!("failed status must report progress")
            };
            assert_eq!(progress.attempts, attempt);
            assert!(progress.newly_counted);
            assert_eq!(progress.exhausted, attempt == MAX_FAILED_REQUEST_ATTEMPTS);
            assert_eq!(affected.len(), 2);
            let duplicate = apply_remote_status(
                &db,
                RemoteStatusUpdate {
                    provider: "pinata",
                    cid: "bafy",
                    request_id: &request_id,
                    origin: RemoteStatusOrigin::ExistingRequest,
                    status: RemotePinStatus::Failed,
                    error_class: Some("transient"),
                    error_text: Some("redacted"),
                    now: failure_at + ChronoDuration::seconds(1),
                },
            )
            .await
            .unwrap();
            let RemoteStatusApplyResult::Applied {
                affected,
                failure: Some(progress),
                ..
            } = duplicate
            else {
                panic!("duplicate failure must be accepted")
            };
            assert_eq!(affected.len(), 2);
            assert_eq!(progress.attempts, attempt);
            assert!(!progress.newly_counted);
            if attempt < MAX_FAILED_REQUEST_ATTEMPTS {
                let remote_state = remote(&db, "pinata", "bafy").await;
                let due = remote_state.next_retry_at.unwrap_or_else(|| {
                    panic!("attempt {attempt} unexpectedly has no retry due: {remote_state:?}")
                });
                assert!(matches!(
                    ensure_failed_remote_retry(&db, "pinata", "bafy", failure_at)
                        .await
                        .unwrap(),
                    FailedRemoteRetryDecision::Scheduled { at, .. } if at == due
                ));
                assert!(matches!(
                    prepare_failed_remote_resubmit(
                        &db,
                        "pinata",
                        "bafy",
                        remote(&db, "pinata", "bafy").await.epoch,
                        &request_id,
                        due,
                    )
                    .await
                    .unwrap(),
                    FailedRemoteResubmitDecision::Prepared { .. }
                ));
            }
        }
        let exhausted = remote(&db, "pinata", "bafy").await;
        assert_eq!(exhausted.failure_attempts, 8);
        assert_eq!(exhausted.next_retry_at, None);
        assert!(matches!(
            ensure_failed_remote_retry(&db, "pinata", "bafy", time(100))
                .await
                .unwrap(),
            FailedRemoteRetryDecision::Exhausted
        ));
        assert_eq!(target_state(&db, "target-all").await, "degraded");
        let reconciles = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("pinata"))
            .filter(pin_job::Column::Cid.eq("bafy"))
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .all(&db)
            .await
            .unwrap();
        assert!(
            !reconciles.iter().any(|job| {
                job.expected_remote_epoch == Some(exhausted.epoch)
                    && job.state != "done"
                    && job.next_attempt_at <= time(1_000)
            }),
            "attempt eight must leave no current-epoch due Reconcile"
        );
        let stale_reconcile = reconciles
            .iter()
            .find(|job| {
                job.expected_remote_epoch
                    .is_some_and(|epoch| epoch < exhausted.epoch)
                    && job.state != "done"
            })
            .expect("a prior retry row remains durable but stale");
        assert!(
            !jobs::check_remote_job_epoch(&db, stale_reconcile)
                .await
                .unwrap(),
            "prior epoch retry work must be guard-stale rather than spin"
        );

        apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy",
                request_id: "request-8",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Pinned,
                error_class: None,
                error_text: None,
                now: time(100),
            },
        )
        .await
        .unwrap();
        let pinned = remote(&db, "pinata", "bafy").await;
        assert_eq!(pinned.failure_attempts, 0);
        assert_eq!(pinned.next_retry_at, None);
        assert_eq!(pinned.last_failed_request_id, None);
    }

    #[tokio::test]
    async fn pure_one_failed_observation_preserves_shared_all_retry_budget() {
        let db = setup().await;
        let cid = "bafy-pure-one-budget";
        seed_remote(&db, "pinata", cid, "queued", Some("one-request"), 4).await;
        seed_lease_target(
            &db,
            "lease-pure-one-budget",
            "automatic",
            "one",
            "active",
            1,
            "target-pure-one-budget",
            "pinata",
            cid,
            "waiting",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE remote_pins SET failure_attempts=5, \
             last_failed_request_id='legacy-all-request', next_retry_at='{}' \
             WHERE provider='pinata' AND cid='{cid}'",
            time(50).to_rfc3339()
        ))
        .await
        .unwrap();

        let applied = apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid,
                request_id: "one-request",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Failed,
                error_class: Some("remote_failed"),
                error_text: Some("remote pin failed"),
                now: time(2),
            },
        )
        .await
        .unwrap();
        let RemoteStatusApplyResult::Applied {
            affected,
            failure,
            previous_status,
            current_status,
        } = applied
        else {
            panic!("pure One current request must apply")
        };
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0].provider_mode, ProviderMode::One);
        assert!(!affected[0].available);
        assert_eq!(failure, None);
        assert_eq!(previous_status, "queued");
        assert_eq!(current_status, "failed");
        let remote = remote(&db, "pinata", cid).await;
        assert_eq!(remote.status, "failed");
        assert_eq!(remote.failure_attempts, 5);
        assert_eq!(
            remote.last_failed_request_id.as_deref(),
            Some("legacy-all-request")
        );
        assert_eq!(remote.next_retry_at, Some(time(50)));
        assert_eq!(remote.last_error_class.as_deref(), Some("remote_failed"));
        assert_eq!(remote.last_error_text.as_deref(), Some("remote pin failed"));
        assert_eq!(
            target_state(&db, "target-pure-one-budget").await,
            "degraded"
        );
        assert_eq!(
            ensure_failed_remote_retry(&db, "pinata", cid, time(3))
                .await
                .unwrap(),
            FailedRemoteRetryDecision::NotNeeded
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("pinata"))
                .filter(pin_job::Column::Cid.eq(cid))
                .filter(pin_job::Column::Operation.eq("reconcile"))
                .count(&db)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn mixed_one_all_failed_observation_counts_each_request_once() {
        let db = setup().await;
        let cid = "bafy-mixed-budget";
        seed_remote(&db, "pinata", cid, "queued", Some("mixed-request"), 3).await;
        seed_lease_target(
            &db,
            "lease-mixed-one",
            "automatic",
            "one",
            "active",
            1,
            "target-mixed-one",
            "pinata",
            cid,
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-mixed-all",
            "copy",
            "all",
            "active",
            1,
            "target-mixed-all",
            "pinata",
            cid,
            "waiting",
            time(100),
        )
        .await;

        for duplicate in [false, true] {
            let applied = apply_remote_status(
                &db,
                RemoteStatusUpdate {
                    provider: "pinata",
                    cid,
                    request_id: "mixed-request",
                    origin: RemoteStatusOrigin::ExistingRequest,
                    status: RemotePinStatus::Failed,
                    error_class: Some("remote_failed"),
                    error_text: Some("remote pin failed"),
                    now: if duplicate { time(3) } else { time(2) },
                },
            )
            .await
            .unwrap();
            let RemoteStatusApplyResult::Applied {
                affected,
                failure: Some(progress),
                ..
            } = applied
            else {
                panic!("mixed current failed request must report shared failure progress")
            };
            assert_eq!(affected.len(), 2);
            assert!(affected.iter().any(|outcome| {
                outcome.lease_id == "lease-mixed-one" && outcome.provider_mode == ProviderMode::One
            }));
            assert!(affected.iter().any(|outcome| {
                outcome.lease_id == "lease-mixed-all" && outcome.provider_mode == ProviderMode::All
            }));
            assert_eq!(progress.attempts, 1);
            assert_eq!(progress.newly_counted, !duplicate);
        }
        let remote = remote(&db, "pinata", cid).await;
        assert_eq!(remote.failure_attempts, 1);
        assert_eq!(
            remote.last_failed_request_id.as_deref(),
            Some("mixed-request")
        );
        assert!(remote.next_retry_at.is_some());
    }

    #[tokio::test]
    async fn stale_delete_compensates_current_reference_without_releasing_quota() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy", "pinned", Some("delete-request"), 7).await;
        let delete_epoch = begin_remote_unpin(&db, "pinata", "bafy").await.unwrap();
        assert_eq!(delete_epoch, 8);
        seed_lease_target(
            &db,
            "lease-new",
            "automatic",
            "one",
            "active",
            1,
            "target-new",
            "pinata",
            "bafy",
            "waiting",
            time(100),
        )
        .await;
        assert_eq!(
            touch_target_reference(&db, "target-new", time(1))
                .await
                .unwrap(),
            GenerationDecision::Current
        );
        assert_eq!(remote(&db, "pinata", "bafy").await.epoch, 9);
        let completion = complete_remote_delete(&db, "pinata", "bafy", delete_epoch, time(2))
            .await
            .unwrap();
        assert!(matches!(
            completion,
            RemoteDeleteCompletion::Compensated { .. }
        ));
        assert_eq!(usage(&db, "pinata").await, (100, 1));
        let compensated = remote(&db, "pinata", "bafy").await;
        assert_eq!(compensated.status, "reserved");
        assert_eq!(compensated.request_id, None);
        assert_eq!(target_state(&db, "target-new").await, "waiting");
    }

    #[tokio::test]
    async fn stale_delete_release_cas_keeps_a_target_activated_after_the_ref_snapshot_desired() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-delete-race",
            "pinned",
            Some("delete"),
            7,
        )
        .await;
        let gate = Arc::new(test_gates::DeleteAfterRefsGate {
            provider: "pinata",
            cid: "bafy-delete-race",
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        *test_gates::DELETE_AFTER_REFS.lock().await = Some(gate.clone());

        let completion_db = db.clone();
        let completion = tokio::spawn(async move {
            complete_remote_delete(&completion_db, "pinata", "bafy-delete-race", 7, time(2)).await
        });
        gate.arrived.notified().await;

        seed_lease_target(
            &db,
            "lease-added-after-delete-snapshot",
            "automatic",
            "all",
            "active",
            1,
            "target-added-after-delete-snapshot",
            "pinata",
            "bafy-delete-race",
            "waiting",
            time(100),
        )
        .await;
        assert_eq!(
            touch_target_reference(&db, "target-added-after-delete-snapshot", time(1))
                .await
                .unwrap(),
            GenerationDecision::Current
        );
        gate.resume.notify_one();
        *test_gates::DELETE_AFTER_REFS.lock().await = None;

        assert!(matches!(
            completion.await.unwrap().unwrap(),
            RemoteDeleteCompletion::Compensated { .. }
                | RemoteDeleteCompletion::ReconcileRequired { .. }
        ));
        assert_eq!(
            target_state(&db, "target-added-after-delete-snapshot").await,
            "waiting",
            "a release CAS made stale by a new desired ref must never clean up that ref"
        );
        assert_eq!(usage(&db, "pinata").await, (100, 1));
    }

    #[tokio::test]
    async fn concurrent_duplicate_failed_observations_count_one_attempt_once() {
        let (_directory, db) = setup_file_backed("concurrent-failed-observation").await;
        seed_remote(
            &db,
            "pinata",
            "bafy-concurrent-failed",
            "queued",
            Some("request-1"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-concurrent-failed",
            "automatic",
            "all",
            "active",
            1,
            "target-concurrent-failed",
            "pinata",
            "bafy-concurrent-failed",
            "submitted",
            time(100),
        )
        .await;
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        *test_gates::STATUS_AFTER_READ.lock().await =
            Some(Arc::new(test_gates::StatusAfterReadGate {
                provider: "pinata",
                cid: "bafy-concurrent-failed",
                barrier: barrier.clone(),
            }));

        let first_db = db.clone();
        let first = tokio::spawn(async move {
            apply_remote_status(
                &first_db,
                RemoteStatusUpdate {
                    provider: "pinata",
                    cid: "bafy-concurrent-failed",
                    request_id: "request-1",
                    origin: RemoteStatusOrigin::ExistingRequest,
                    status: RemotePinStatus::Failed,
                    error_class: Some("transient"),
                    error_text: Some("redacted"),
                    now: time(1),
                },
            )
            .await
        });
        let second_db = db.clone();
        let second = tokio::spawn(async move {
            apply_remote_status(
                &second_db,
                RemoteStatusUpdate {
                    provider: "pinata",
                    cid: "bafy-concurrent-failed",
                    request_id: "request-1",
                    origin: RemoteStatusOrigin::ExistingRequest,
                    status: RemotePinStatus::Failed,
                    error_class: Some("transient"),
                    error_text: Some("redacted"),
                    now: time(1),
                },
            )
            .await
        });
        barrier.wait().await;
        *test_gates::STATUS_AFTER_READ.lock().await = None;

        let outcomes = [
            first.await.unwrap().unwrap(),
            second.await.unwrap().unwrap(),
        ];
        let progress: Vec<_> = outcomes
            .into_iter()
            .map(|outcome| match outcome {
                RemoteStatusApplyResult::Applied {
                    failure: Some(progress),
                    ..
                } => progress,
                other => panic!("duplicate failure should be accepted, got {other:?}"),
            })
            .collect();
        assert_eq!(
            progress.iter().filter(|entry| entry.newly_counted).count(),
            1
        );
        assert!(progress.iter().all(|entry| entry.attempts == 1));
        let persisted = remote(&db, "pinata", "bafy-concurrent-failed").await;
        assert_eq!(persisted.failure_attempts, 1);
        assert_eq!(
            persisted.last_failed_request_id.as_deref(),
            Some("request-1")
        );
        assert_eq!(
            persisted.next_retry_at,
            Some(time(1) + ChronoDuration::seconds(1))
        );
    }

    #[tokio::test]
    async fn opposite_failover_candidate_orders_converge_on_file_backed_sqlite() {
        async fn run_failover_with_contention_retry(
            db: DatabaseConnection,
            lease_id: &'static str,
            target_id: &'static str,
            providers: Vec<String>,
            limits: ProviderLimitMap,
            mut winner_committed: tokio::sync::watch::Receiver<bool>,
            winner_signal: tokio::sync::watch::Sender<bool>,
        ) -> pin_lease_target::Model {
            for _ in 0..8 {
                let txn = db.begin().await.unwrap();
                match fail_one_target(&txn, lease_id, target_id, &providers, &limits, time(2)).await
                {
                    Ok(Some(replacement)) => match txn.commit().await {
                        Ok(()) => {
                            winner_signal.send_replace(true);
                            return replacement;
                        }
                        Err(error) if is_sqlite_contention(&error.to_string()) => {
                            if !*winner_committed.borrow() {
                                winner_committed
                                    .changed()
                                    .await
                                    .expect("a competing failover must remain observable");
                            }
                        }
                        Err(error) => panic!("unexpected failover commit error: {error}"),
                    },
                    Ok(None) => panic!("opposite failover must have a reservable provider"),
                    Err(error) if is_sqlite_contention(&error.to_string()) => {
                        let _ = txn.rollback().await;
                        if !*winner_committed.borrow() {
                            winner_committed
                                .changed()
                                .await
                                .expect("a competing failover must remain observable");
                        }
                    }
                    Err(error) => panic!("unexpected failover error: {error}"),
                }
            }
            panic!("SQLite failover contention did not converge within bounded retries")
        }

        let (_directory, db) = setup_file_backed("opposite-failover-order").await;
        let cid = "bafy-opposite-order";
        seed_remote(&db, "a0", cid, "failed", Some("failed-a"), 1).await;
        seed_remote(&db, "b0", cid, "failed", Some("failed-b"), 1).await;
        seed_lease_target(
            &db,
            "lease-opposite-a",
            "automatic",
            "one",
            "active",
            1,
            "target-opposite-a",
            "a0",
            cid,
            "degraded",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-opposite-b",
            "copy",
            "one",
            "active",
            1,
            "target-opposite-b",
            "b0",
            cid,
            "degraded",
            time(100),
        )
        .await;
        let provider_limits = ProviderLimitMap::from(["a0", "b0", "p1", "p2"].map(|provider| {
            (
                provider.to_owned(),
                ProviderLimits {
                    priority: 1,
                    max_bytes: 10_000,
                    max_pins: 100,
                    enabled: true,
                },
            )
        }));
        let (winner_signal, winner_committed) = tokio::sync::watch::channel(false);

        let first = run_failover_with_contention_retry(
            db.clone(),
            "lease-opposite-a",
            "target-opposite-a",
            vec!["a0".to_owned(), "p1".to_owned(), "p2".to_owned()],
            provider_limits.clone(),
            winner_committed.clone(),
            winner_signal.clone(),
        );
        let second = run_failover_with_contention_retry(
            db.clone(),
            "lease-opposite-b",
            "target-opposite-b",
            vec!["b0".to_owned(), "p2".to_owned(), "p1".to_owned()],
            provider_limits,
            winner_committed,
            winner_signal,
        );
        let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(first, second)
        })
        .await
        .expect("opposite failovers must converge without a lock cycle");

        assert_eq!(first.provider, "p1");
        assert_eq!(second.provider, "p2");
        assert_eq!(lease(&db, "lease-opposite-a").await.generation, 2);
        assert_eq!(lease(&db, "lease-opposite-b").await.generation, 2);
        assert_eq!(remote(&db, "a0", cid).await.epoch, 1);
        assert_eq!(remote(&db, "b0", cid).await.epoch, 1);
        assert_eq!(remote(&db, "p1", cid).await.epoch, 1);
        assert_eq!(remote(&db, "p2", cid).await.epoch, 1);
        let submits = pin_job::Entity::find()
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("submit"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(submits.len(), 2);
        assert!(submits.iter().all(|job| job.expected_generation == Some(2)));
        assert!(
            submits
                .iter()
                .all(|job| job.expected_remote_epoch.is_none())
        );
    }

    #[tokio::test]
    async fn caller_owned_file_backed_sqlite_all_mode_statuses_serialize_cross_provider_availability()
     {
        let (_directory, db) = setup_file_backed("all-mode-cross-provider-status").await;
        seed_remote(
            &db,
            "pinata",
            "bafy-cross-provider-status",
            "pinned",
            Some("request-pinata"),
            1,
        )
        .await;
        seed_remote(
            &db,
            "filebase",
            "bafy-cross-provider-status",
            "pinned",
            Some("request-filebase"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-cross-provider-status",
            "automatic",
            "all",
            "active",
            1,
            "target-cross-provider-pinata",
            "pinata",
            "bafy-cross-provider-status",
            "pinned",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-cross-provider-filebase', 'lease-cross-provider-status', \
                     'bafy-cross-provider-status', 100, 'filebase', 'pinned', '{}', '{}')",
            time(1).to_rfc3339(),
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();

        let start = Arc::new(tokio::sync::Barrier::new(2));
        let pinata = tokio::spawn(apply_status_in_caller_owned_sqlite_transaction_with_retry(
            db.clone(),
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy-cross-provider-status",
                request_id: "request-pinata",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Failed,
                error_class: Some("transient"),
                error_text: None,
                now: time(2),
            },
            start.clone(),
        ));
        let filebase = tokio::spawn(apply_status_in_caller_owned_sqlite_transaction_with_retry(
            db.clone(),
            RemoteStatusUpdate {
                provider: "filebase",
                cid: "bafy-cross-provider-status",
                request_id: "request-filebase",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Failed,
                error_class: Some("transient"),
                error_text: None,
                now: time(2),
            },
            start,
        ));
        let results = [pinata.await.unwrap(), filebase.await.unwrap()];
        let availability: Vec<_> = results
            .into_iter()
            .map(|result| match result {
                RemoteStatusApplyResult::Applied { affected, .. } => {
                    assert_eq!(affected.len(), 1);
                    affected[0].available
                }
                other => panic!("concurrent all-mode status must apply, got {other:?}"),
            })
            .collect();
        assert_eq!(
            availability.iter().filter(|available| **available).count(),
            1
        );
        assert_eq!(
            availability.iter().filter(|available| !**available).count(),
            1
        );
        assert_eq!(
            target_state(&db, "target-cross-provider-pinata").await,
            TARGET_DEGRADED
        );
        assert_eq!(
            target_state(&db, "target-cross-provider-filebase").await,
            TARGET_DEGRADED
        );
        assert_eq!(
            remote(&db, "pinata", "bafy-cross-provider-status")
                .await
                .status,
            REMOTE_FAILED
        );
        assert_eq!(
            remote(&db, "filebase", "bafy-cross-provider-status")
                .await
                .status,
            REMOTE_FAILED
        );
    }

    #[tokio::test]
    async fn ordinary_projection_rejects_absent_remote_without_provider_limits() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy-absent-projection", "absent", None, 3).await;
        seed_lease_target(
            &db,
            "lease-absent-projection",
            "automatic",
            "all",
            "active",
            1,
            "target-absent-projection",
            "pinata",
            "bafy-absent-projection",
            "waiting",
            time(100),
        )
        .await;

        let error = project_target_from_remote(&db, "target-absent-projection", time(1))
            .await
            .expect_err("absent remotes require explicit provider limits");
        assert!(error.to_string().contains("provider limits"));
        assert_eq!(
            remote(&db, "pinata", "bafy-absent-projection").await.status,
            "absent"
        );
        assert_eq!(usage(&db, "pinata").await, (100, 1));
        assert_eq!(
            target_state(&db, "target-absent-projection").await,
            "waiting"
        );
    }

    #[tokio::test]
    async fn limits_aware_projection_reacquires_absent_capacity_once_and_enqueues_one_submit() {
        let db = setup().await;
        seed_absent_remote_without_usage(&db, "pinata", "bafy-reacquire", 3).await;
        seed_lease_target(
            &db,
            "lease-reacquire",
            "automatic",
            "all",
            "active",
            1,
            "target-reacquire",
            "pinata",
            "bafy-reacquire",
            "waiting",
            time(100),
        )
        .await;

        let result = project_target_from_remote_with_limits(
            &db,
            "target-reacquire",
            &limits(1_000, 10),
            time(1),
        )
        .await
        .unwrap();
        assert!(matches!(
            result.projection,
            TargetProjection::Waiting { .. }
        ));
        assert_eq!(result.reservation, Some(ReservationOutcome::Reserved));
        assert_eq!(usage(&db, "pinata").await, (100, 1));
        assert_eq!(
            remote(&db, "pinata", "bafy-reacquire").await.status,
            "reserved"
        );
        assert_eq!(target_state(&db, "target-reacquire").await, "waiting");
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("pinata"))
                .filter(pin_job::Column::Cid.eq("bafy-reacquire"))
                .filter(pin_job::Column::Operation.eq("submit"))
                .count(&db)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn limits_aware_projection_surfaces_evictions_without_reserving_or_submitting() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy-held", "pinned", Some("held"), 1).await;
        seed_lease_target(
            &db,
            "lease-held",
            "automatic",
            "all",
            "active",
            1,
            "target-held",
            "pinata",
            "bafy-held",
            "pinned",
            time(100),
        )
        .await;
        seed_absent_remote_without_usage(&db, "pinata", "bafy-wait", 4).await;
        seed_lease_target(
            &db,
            "lease-wait",
            "quota-wait",
            "all",
            "active",
            1,
            "target-wait",
            "pinata",
            "bafy-wait",
            "waiting",
            time(100),
        )
        .await;

        let result =
            project_target_from_remote_with_limits(&db, "target-wait", &limits(100, 1), time(1))
                .await
                .unwrap();
        assert_eq!(result.projection, TargetProjection::QuotaWaiting);
        assert!(matches!(
            result.reservation,
            Some(ReservationOutcome::QuotaWaiting { ref evict })
                if evict == &vec![("pinata".to_owned(), "bafy-held".to_owned())]
        ));
        assert_eq!(usage(&db, "pinata").await, (100, 1));
        assert_eq!(remote(&db, "pinata", "bafy-wait").await.status, "absent");
        assert_eq!(target_state(&db, "target-wait").await, "quota_waiting");
        assert!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("pinata"))
                .filter(pin_job::Column::Cid.eq("bafy-wait"))
                .filter(pin_job::Column::Operation.eq("submit"))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn limits_aware_projection_blocks_oversize_absent_target_without_usage_or_submit() {
        let db = setup().await;
        seed_absent_remote_without_usage(&db, "pinata", "bafy-oversize-projection", 6).await;
        seed_lease_target(
            &db,
            "lease-oversize-projection",
            "automatic",
            "all",
            "active",
            1,
            "target-oversize-projection",
            "pinata",
            "bafy-oversize-projection",
            "waiting",
            time(100),
        )
        .await;
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET logical_size = 101 WHERE id = 'target-oversize-projection'",
        )
        .await
        .unwrap();

        let result = project_target_from_remote_with_limits(
            &db,
            "target-oversize-projection",
            &limits(100, 1),
            time(1),
        )
        .await
        .unwrap();
        assert_eq!(result.projection, TargetProjection::QuotaBlocked);
        assert_eq!(result.reservation, Some(ReservationOutcome::QuotaBlocked));
        assert!(quota::read_usage(&db, "pinata").await.unwrap().is_none());
        assert_eq!(
            remote(&db, "pinata", "bafy-oversize-projection")
                .await
                .status,
            "absent"
        );
        assert_eq!(
            target_state(&db, "target-oversize-projection").await,
            "quota_blocked"
        );
        assert!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("pinata"))
                .filter(pin_job::Column::Cid.eq("bafy-oversize-projection"))
                .filter(pin_job::Column::Operation.eq("submit"))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn no_request_absence_waits_for_ambiguous_submit_then_releases_once() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy", "reserved", None, 3).await;
        seed_lease_target(
            &db,
            "lease-old",
            "old",
            "all",
            "cancelled",
            1,
            "target-old",
            "pinata",
            "bafy",
            "released",
            time(1),
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, expected_generation, state, attempts, \
              next_attempt_at, locked_until, submit_phase) \
             VALUES ('submit:pinata:bafy:target-old:g1', 'submit', 'pinata', 'bafy', 'lease-old', \
                     'target-old', 1, 'running', 0, '{}', '{}', 'calling')",
            time(1).to_rfc3339(),
            time(10).to_rfc3339(),
        ))
        .await
        .unwrap();
        assert!(matches!(
            complete_no_request_remote_absence(&db, "pinata", "bafy", 3, time(2))
                .await
                .unwrap(),
            NoRequestRemoteCompletion::Wait { next_check_at } if next_check_at == time(10)
        ));
        assert_eq!(usage(&db, "pinata").await, (100, 1));
        db.execute_unprepared("UPDATE pin_jobs SET state = 'done', locked_until = NULL WHERE id = 'submit:pinata:bafy:target-old:g1'")
            .await
            .unwrap();
        assert_eq!(
            complete_no_request_remote_absence(&db, "pinata", "bafy", 3, time(11))
                .await
                .unwrap(),
            NoRequestRemoteCompletion::Released
        );
        assert_eq!(usage(&db, "pinata").await, (0, 0));
        assert_eq!(remote(&db, "pinata", "bafy").await.status, "absent");
    }

    #[tokio::test]
    async fn confirmed_delete_with_running_submit_clears_deleted_identity_without_release() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-delete-submit-race",
            "pinned",
            Some("deleted-request"),
            3,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-delete-submit-race",
            "automatic",
            "all",
            "cancelled",
            1,
            "target-delete-submit-race",
            "pinata",
            "bafy-delete-submit-race",
            "released",
            time(1),
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, expected_generation, state, attempts, \
              next_attempt_at, locked_until, submit_phase) \
             VALUES ('submit-delete-race', 'submit', 'pinata', 'bafy-delete-submit-race', \
                     'lease-delete-submit-race', 'target-delete-submit-race', 1, 'running', 0, \
                     '{}', '{}', 'calling')",
            time(2).to_rfc3339(),
            time(10).to_rfc3339(),
        ))
        .await
        .unwrap();

        let completion =
            complete_remote_delete(&db, "pinata", "bafy-delete-submit-race", 3, time(3))
                .await
                .unwrap();

        assert!(matches!(
            completion,
            RemoteDeleteCompletion::ReconcileRequired { .. }
        ));
        assert_eq!(usage(&db, "pinata").await, (100, 1));
        let remote = remote(&db, "pinata", "bafy-delete-submit-race").await;
        assert_eq!(remote.request_id, None);
        assert_eq!(remote.status, "reserved");
        assert_eq!(remote.epoch, 4);
        assert!(
            pin_job::Entity::find_by_id("reconcile:pinata:bafy-delete-submit-race:e4".to_owned())
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn confirmed_delete_cancels_only_safe_pending_submit_phases_before_release() {
        for phase in ["ready", "recovery_backoff"] {
            let db = setup().await;
            let cid = format!("bafy-safe-delete-{phase}");
            let job_id = format!("submit-safe-delete-{phase}");
            seed_remote(&db, "pinata", &cid, "pinned", Some("deleted-request"), 3).await;
            seed_lease_target(
                &db,
                &format!("lease-safe-delete-{phase}"),
                "automatic",
                "all",
                "cancelled",
                1,
                &format!("target-safe-delete-{phase}"),
                "pinata",
                &cid,
                "released",
                time(1),
            )
            .await;
            db.execute_unprepared(&format!(
                "INSERT INTO pin_jobs \
                 (id, operation, provider, cid, lease_id, target_id, expected_generation, state, \
                  attempts, next_attempt_at, submit_phase) \
                 VALUES ('{job_id}', 'submit', 'pinata', '{cid}', \
                         'lease-safe-delete-{phase}', 'target-safe-delete-{phase}', 1, 'pending', \
                         0, '{}', '{phase}')",
                time(10).to_rfc3339(),
            ))
            .await
            .unwrap();

            assert_eq!(
                complete_remote_delete(&db, "pinata", &cid, 3, time(2))
                    .await
                    .unwrap(),
                RemoteDeleteCompletion::Released
            );
            assert_eq!(usage(&db, "pinata").await, (0, 0));
            let job = pin_job::Entity::find_by_id(job_id)
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.state, "done");
            assert_eq!(job.submit_phase.as_deref(), Some("ready"));
        }
    }

    #[tokio::test]
    async fn no_request_submit_phase_matrix_releases_only_provably_safe_pending_work() {
        let cases = [
            ("pending", "ready", false),
            ("pending", "recovery_backoff", false),
            ("pending", "calling", true),
            ("pending", "recovering", true),
            ("running", "ready", true),
            ("running", "recovery_backoff", true),
            ("running", "calling", true),
            ("running", "recovering", true),
        ];
        for (state, phase, must_wait) in cases {
            let db = setup().await;
            let cid = format!("bafy-matrix-{state}-{phase}");
            let job_id = format!("submit-matrix-{state}-{phase}");
            seed_remote(&db, "pinata", &cid, "reserved", None, 3).await;
            seed_lease_target(
                &db,
                &format!("lease-matrix-{state}-{phase}"),
                "automatic",
                "all",
                "cancelled",
                1,
                &format!("target-matrix-{state}-{phase}"),
                "pinata",
                &cid,
                "released",
                time(1),
            )
            .await;
            let locked = if state == "running" {
                format!(", locked_until='{}'", time(10).to_rfc3339())
            } else {
                String::new()
            };
            db.execute_unprepared(&format!(
                "INSERT INTO pin_jobs \
                 (id, operation, provider, cid, lease_id, target_id, expected_generation, state, \
                  attempts, next_attempt_at, submit_phase) \
                 VALUES ('{job_id}', 'submit', 'pinata', '{cid}', \
                         'lease-matrix-{state}-{phase}', 'target-matrix-{state}-{phase}', 1, \
                         '{state}', 0, '{}', '{phase}'); \
                 UPDATE pin_jobs SET updated_at=updated_at{locked} WHERE id='{job_id}'",
                time(2).to_rfc3339(),
            ))
            .await
            .unwrap();

            let completion = complete_no_request_remote_absence(&db, "pinata", &cid, 3, time(3))
                .await
                .unwrap();
            if must_wait {
                assert!(matches!(completion, NoRequestRemoteCompletion::Wait { .. }));
                assert_eq!(usage(&db, "pinata").await, (100, 1));
                assert_ne!(remote(&db, "pinata", &cid).await.status, "absent");
            } else {
                assert_eq!(completion, NoRequestRemoteCompletion::Released);
                assert_eq!(usage(&db, "pinata").await, (0, 0));
                assert_eq!(
                    pin_job::Entity::find_by_id(job_id)
                        .one(&db)
                        .await
                        .unwrap()
                        .unwrap()
                        .state,
                    "done"
                );
            }
        }
    }

    #[tokio::test]
    async fn no_request_adoption_retains_usage_and_requires_ordinary_unpin() {
        let db = setup().await;
        let cid = "bafy-adopted-after-ambiguity";
        seed_remote(&db, "pinata", cid, "reserved", None, 3).await;
        seed_lease_target(
            &db,
            "lease-adopted-after-ambiguity",
            "automatic",
            "all",
            "cancelled",
            1,
            "target-adopted-after-ambiguity",
            "pinata",
            cid,
            "released",
            time(1),
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_jobs \
             (id, operation, provider, cid, lease_id, target_id, expected_generation, state, \
              attempts, next_attempt_at, locked_until, submit_phase) \
             VALUES ('submit-adopted', 'submit', 'pinata', '{cid}', \
                     'lease-adopted-after-ambiguity', 'target-adopted-after-ambiguity', 1, \
                     'running', 0, '{}', '{}', 'recovering')",
            time(2).to_rfc3339(),
            time(10).to_rfc3339(),
        ))
        .await
        .unwrap();
        assert!(matches!(
            complete_no_request_remote_absence(&db, "pinata", cid, 3, time(3))
                .await
                .unwrap(),
            NoRequestRemoteCompletion::Wait { .. }
        ));
        db.execute_unprepared(
            "UPDATE remote_pins SET request_id='adopted-request', status='queued' \
             WHERE provider='pinata' AND cid='bafy-adopted-after-ambiguity'; \
             UPDATE pin_jobs SET state='done', locked_until=NULL WHERE id='submit-adopted'",
        )
        .await
        .unwrap();

        assert_eq!(
            complete_no_request_remote_absence(&db, "pinata", cid, 3, time(4))
                .await
                .unwrap(),
            NoRequestRemoteCompletion::Stale
        );
        assert_eq!(usage(&db, "pinata").await, (100, 1));
        assert_eq!(begin_remote_unpin(&db, "pinata", cid).await.unwrap(), 4);
        assert!(
            pin_job::Entity::find_by_id("unpin:pinata:bafy-adopted-after-ambiguity:e4".to_owned())
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(usage(&db, "pinata").await, (100, 1));
    }

    #[tokio::test]
    async fn manual_renewal_keeps_equal_extends_and_reactivates_same_target() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy", "pinned", Some("request-1"), 1).await;
        seed_lease_target(
            &db,
            "lease-manual",
            "manual",
            "all",
            "active",
            1,
            "target-manual",
            "pinata",
            "bafy",
            "pinned",
            time(10),
        )
        .await;
        assert_eq!(
            renew_manual_lease(&db, "object-1", "lease-manual", time(10), time(1))
                .await
                .unwrap(),
            ManualLeaseRenewalOutcome::Kept { generation: 1 }
        );
        let before_shorter_lease = lease(&db, "lease-manual").await;
        let before_shorter_target = target_state(&db, "target-manual").await;
        let before_shorter_remote = remote(&db, "pinata", "bafy").await;
        assert!(matches!(
            renew_manual_lease(&db, "object-1", "lease-manual", time(9), time(1))
                .await
                .unwrap_err(),
            RenewManualLeaseError::InvalidState
        ));
        assert_eq!(lease(&db, "lease-manual").await, before_shorter_lease);
        assert_eq!(
            target_state(&db, "target-manual").await,
            before_shorter_target
        );
        assert_eq!(remote(&db, "pinata", "bafy").await, before_shorter_remote);
        assert_eq!(
            renew_manual_lease(&db, "object-1", "lease-manual", time(20), time(2))
                .await
                .unwrap(),
            ManualLeaseRenewalOutcome::Extended { generation: 2 }
        );
        assert_eq!(remote(&db, "pinata", "bafy").await.epoch, 2);
        db.execute_unprepared("UPDATE pin_leases SET state = 'expired' WHERE id = 'lease-manual'")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state = 'released' WHERE id = 'target-manual'",
        )
        .await
        .unwrap();
        let renewed = renew_manual_lease(&db, "object-1", "lease-manual", time(30), time(3))
            .await
            .unwrap();
        assert!(matches!(
            renewed,
            ManualLeaseRenewalOutcome::Reactivated { ref restored_target_ids, generation: 3 }
                if restored_target_ids == &["target-manual".to_owned()]
        ));
        assert_eq!(target_state(&db, "target-manual").await, "pinned");
        assert_eq!(usage(&db, "pinata").await, (100, 1));

        db.execute_unprepared("UPDATE pin_leases SET state = 'expired' WHERE id = 'lease-manual'")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state = 'released' WHERE id = 'target-manual'",
        )
        .await
        .unwrap();
        db.execute_unprepared("UPDATE remote_pins SET status = 'absent', request_id = NULL WHERE provider = 'pinata' AND cid = 'bafy'")
            .await
            .unwrap();
        assert!(matches!(
            renew_manual_lease(&db, "object-1", "lease-manual", time(40), time(4))
                .await
                .unwrap_err(),
            RenewManualLeaseError::NoRecoverableReservation
        ));
        db.execute_unprepared(
            "UPDATE pin_leases SET state = 'cancelled' WHERE id = 'lease-manual'",
        )
        .await
        .unwrap();
        assert!(matches!(
            renew_manual_lease(&db, "object-1", "lease-manual", time(40), time(4))
                .await
                .unwrap_err(),
            RenewManualLeaseError::InvalidState
        ));
    }

    #[tokio::test]
    async fn only_generation_advancing_renewals_or_explicit_touch_restart_stopped_budget() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-renew-budget",
            "failed",
            Some("stopped-active"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-renew-budget",
            "manual",
            "all",
            "active",
            1,
            "target-renew-budget",
            "pinata",
            "bafy-renew-budget",
            "degraded",
            time(10),
        )
        .await;
        db.execute_unprepared(
            "UPDATE remote_pins SET failure_attempts=8, \
             last_failed_request_id='stopped-active', next_retry_at=NULL \
             WHERE provider='pinata' AND cid='bafy-renew-budget'",
        )
        .await
        .unwrap();

        assert_eq!(
            renew_manual_lease(&db, "object-1", "lease-renew-budget", time(10), time(1),)
                .await
                .unwrap(),
            ManualLeaseRenewalOutcome::Kept { generation: 1 }
        );
        let equal = remote(&db, "pinata", "bafy-renew-budget").await;
        assert_eq!(equal.epoch, 1);
        assert_eq!(equal.failure_attempts, 8);
        assert_eq!(equal.next_retry_at, None);
        assert_eq!(
            ensure_failed_remote_retry(&db, "pinata", "bafy-renew-budget", time(1))
                .await
                .unwrap(),
            FailedRemoteRetryDecision::Exhausted
        );
        assert_eq!(
            remote(&db, "pinata", "bafy-renew-budget")
                .await
                .failure_attempts,
            8,
            "ordinary due scan must not restart a stopped budget"
        );

        assert_eq!(
            renew_manual_lease(&db, "object-1", "lease-renew-budget", time(20), time(2),)
                .await
                .unwrap(),
            ManualLeaseRenewalOutcome::Extended { generation: 2 }
        );
        let extended = remote(&db, "pinata", "bafy-renew-budget").await;
        assert_eq!(extended.epoch, 2);
        assert_eq!(extended.failure_attempts, 0);
        assert!(extended.next_retry_at.is_some());
        let extended_reconcile = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("pinata"))
            .filter(pin_job::Column::Cid.eq("bafy-renew-budget"))
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(2))
            .one(&db)
            .await
            .unwrap()
            .expect("active extension must create one current-epoch retry owner");
        assert_eq!(extended_reconcile.state, "pending");

        db.execute_unprepared(
            "UPDATE pin_leases SET state='expired' WHERE id='lease-renew-budget'; \
             UPDATE pin_lease_targets SET state='released' WHERE id='target-renew-budget'; \
             UPDATE remote_pins SET request_id='stopped-expired', status='failed', \
             failure_attempts=8, last_failed_request_id='stopped-expired', next_retry_at=NULL \
             WHERE provider='pinata' AND cid='bafy-renew-budget'",
        )
        .await
        .unwrap();
        let reactivated =
            renew_manual_lease(&db, "object-1", "lease-renew-budget", time(30), time(21))
                .await
                .unwrap();
        assert!(matches!(
            reactivated,
            ManualLeaseRenewalOutcome::Reactivated { ref restored_target_ids, generation: 3 }
                if restored_target_ids == &["target-renew-budget".to_owned()]
        ));
        let reactivated_remote = remote(&db, "pinata", "bafy-renew-budget").await;
        assert_eq!(reactivated_remote.epoch, 3);
        assert_eq!(reactivated_remote.failure_attempts, 0);
        assert!(reactivated_remote.next_retry_at.is_some());
        assert_eq!(target_state(&db, "target-renew-budget").await, "degraded");
        let reactivated_reconcile = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("pinata"))
            .filter(pin_job::Column::Cid.eq("bafy-renew-budget"))
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(3))
            .one(&db)
            .await
            .unwrap()
            .expect("expired reactivation must create one current-epoch retry owner");
        assert_eq!(reactivated_reconcile.state, "pending");

        db.execute_unprepared(
            "UPDATE remote_pins SET failure_attempts=8, next_retry_at=NULL \
             WHERE provider='pinata' AND cid='bafy-renew-budget'",
        )
        .await
        .unwrap();
        assert!(matches!(
            reset_failed_remote_retry_on_user_touch(&db, "pinata", "bafy-renew-budget", time(22),)
                .await
                .unwrap(),
            FailedRemoteRetryDecision::Scheduled { .. }
        ));
        let explicitly_reset = remote(&db, "pinata", "bafy-renew-budget").await;
        assert_eq!(explicitly_reset.failure_attempts, 0);
        assert!(explicitly_reset.next_retry_at.is_some());
    }

    #[tokio::test]
    async fn renewal_owner_supersession_before_equal_kept_is_rejected() {
        let _renewal_owner_gate_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-owner-equal",
            "pinned",
            Some("request-owner-equal"),
            3,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-owner-equal",
            "manual",
            "all",
            "active",
            4,
            "target-owner-equal",
            "pinata",
            "bafy-owner-equal",
            "pinned",
            time(10),
        )
        .await;
        let original_lease = lease(&db, "lease-owner-equal").await;
        let gate = Arc::new(test_gates::RenewalBeforeOwnerGuardGate {
            lease_id: "lease-owner-equal",
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        *test_gates::RENEWAL_BEFORE_OWNER_GUARD.lock().await = Some(gate.clone());

        let renewal_db = db.clone();
        let renewal = tokio::spawn(async move {
            let transaction = renewal_db.begin().await.unwrap();
            let result = renew_manual_lease(
                &transaction,
                "object-1",
                "lease-owner-equal",
                time(10),
                time(1),
            )
            .await;
            match result {
                Ok(outcome) => {
                    transaction.commit().await.unwrap();
                    Ok(outcome)
                }
                Err(error) => {
                    transaction.rollback().await.unwrap();
                    Err(error)
                }
            }
        });
        gate.arrived.notified().await;
        gate.resume.notify_one();
        *test_gates::RENEWAL_BEFORE_OWNER_GUARD.lock().await = None;

        assert!(matches!(
            renewal.await.unwrap().unwrap_err(),
            RenewManualLeaseError::NotLatestOwner
        ));
        assert_eq!(lease(&db, "lease-owner-equal").await, original_lease);
        assert!(
            object::Entity::find_by_id("object-1".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .is_latest
        );
    }

    #[tokio::test]
    async fn renewal_owner_supersession_after_revalidation_rolls_back_active_extension() {
        let _renewal_owner_gate_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-owner-extension",
            "pinned",
            Some("request-owner-extension"),
            3,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-owner-extension",
            "manual",
            "all",
            "active",
            4,
            "target-owner-extension",
            "pinata",
            "bafy-owner-extension",
            "pinned",
            time(10),
        )
        .await;
        let original_lease = lease(&db, "lease-owner-extension").await;
        let original_targets = lease_target_rows(&db, "lease-owner-extension").await;
        let original_remote = remote(&db, "pinata", "bafy-owner-extension").await;
        let original_usage = usage(&db, "pinata").await;
        let original_jobs = pin_job::Entity::find().all(&db).await.unwrap();
        let gate = Arc::new(test_gates::RenewalBeforeOwnerGuardGate {
            lease_id: "lease-owner-extension",
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        *test_gates::RENEWAL_BEFORE_OWNER_GUARD.lock().await = Some(gate.clone());

        let renewal_db = db.clone();
        let renewal = tokio::spawn(async move {
            let transaction = renewal_db.begin().await.unwrap();
            let result = renew_manual_lease(
                &transaction,
                "object-1",
                "lease-owner-extension",
                time(20),
                time(1),
            )
            .await;
            match result {
                Ok(outcome) => {
                    transaction.commit().await.unwrap();
                    Ok(outcome)
                }
                Err(error) => {
                    transaction.rollback().await.unwrap();
                    Err(error)
                }
            }
        });
        gate.arrived.notified().await;
        gate.resume.notify_one();
        *test_gates::RENEWAL_BEFORE_OWNER_GUARD.lock().await = None;

        assert!(matches!(
            renewal.await.unwrap().unwrap_err(),
            RenewManualLeaseError::NotLatestOwner
        ));
        assert_eq!(lease(&db, "lease-owner-extension").await, original_lease);
        assert_eq!(
            lease_target_rows(&db, "lease-owner-extension").await,
            original_targets
        );
        assert_eq!(
            remote(&db, "pinata", "bafy-owner-extension").await,
            original_remote
        );
        assert_eq!(usage(&db, "pinata").await, original_usage);
        assert_eq!(
            pin_job::Entity::find().all(&db).await.unwrap(),
            original_jobs
        );
        assert!(
            object::Entity::find_by_id("object-1".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .is_latest
        );
    }

    #[tokio::test]
    async fn manual_renewal_rejects_wrong_or_nonlatest_owner_without_mutation() {
        let db = setup().await;
        db.execute_unprepared(
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest) \
             VALUES ('object-2', 'bucket', 'key-2', 'QmObject2', 100, 'QmObject2', TRUE)",
        )
        .await
        .unwrap();
        seed_remote(
            &db,
            "pinata",
            "bafy-owner",
            "pinned",
            Some("request-owner"),
            3,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-owner",
            "manual",
            "all",
            "active",
            4,
            "target-owner",
            "pinata",
            "bafy-owner",
            "pinned",
            time(10),
        )
        .await;
        let original_lease = lease(&db, "lease-owner").await;
        let original_target = target_state(&db, "target-owner").await;
        let original_remote = remote(&db, "pinata", "bafy-owner").await;
        assert!(matches!(
            renew_manual_lease(&db, "object-2", "lease-owner", time(20), time(1))
                .await
                .unwrap_err(),
            RenewManualLeaseError::NotLatestOwner
        ));
        assert_eq!(lease(&db, "lease-owner").await, original_lease);
        assert_eq!(target_state(&db, "target-owner").await, original_target);
        assert_eq!(remote(&db, "pinata", "bafy-owner").await, original_remote);

        db.execute_unprepared("UPDATE objects SET is_latest = FALSE WHERE id = 'object-1'")
            .await
            .unwrap();
        assert!(matches!(
            renew_manual_lease(&db, "object-1", "lease-owner", time(20), time(1))
                .await
                .unwrap_err(),
            RenewManualLeaseError::NotLatestOwner
        ));
        assert_eq!(lease(&db, "lease-owner").await, original_lease);
        assert_eq!(target_state(&db, "target-owner").await, original_target);
        assert_eq!(remote(&db, "pinata", "bafy-owner").await, original_remote);
    }

    #[tokio::test]
    async fn expired_decompressed_manual_renewal_restores_only_original_recoverable_rows() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-held",
            "pinned",
            Some("request-held"),
            4,
        )
        .await;
        seed_remote(&db, "filebase", "bafy-absent", "absent", None, 9).await;
        seed_lease_target(
            &db,
            "lease-decompressed",
            "manual",
            "all",
            "expired",
            7,
            "target-held",
            "pinata",
            "bafy-held",
            "released",
            time(1),
        )
        .await;
        db.execute_unprepared(
            "UPDATE pin_leases SET content_mode = 'decompressed' WHERE id = 'lease-decompressed'",
        )
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-absent', 'lease-decompressed', 'bafy-absent', 100, 'filebase', \
                     'released', '{}', '{}')",
            time(0).to_rfc3339(),
            time(0).to_rfc3339(),
        ))
        .await
        .unwrap();
        let original_targets = lease_target_rows(&db, "lease-decompressed").await;
        let original_ids_and_providers: Vec<_> = original_targets
            .iter()
            .map(|target| {
                (
                    target.id.clone(),
                    target.provider.clone(),
                    target.cid.clone(),
                )
            })
            .collect();
        assert_eq!(
            lease(&db, "lease-decompressed").await.content_mode,
            "decompressed"
        );

        let outcome = renew_manual_lease(&db, "object-1", "lease-decompressed", time(30), time(2))
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            ManualLeaseRenewalOutcome::Reactivated { ref restored_target_ids, generation: 8 }
                if restored_target_ids == &["target-held".to_owned()]
        ));
        let restored_targets = lease_target_rows(&db, "lease-decompressed").await;
        assert_eq!(
            restored_targets.len(),
            2,
            "renewal must not insert a target"
        );
        assert_eq!(
            restored_targets
                .iter()
                .map(|target| {
                    (
                        target.id.clone(),
                        target.provider.clone(),
                        target.cid.clone(),
                    )
                })
                .collect::<Vec<_>>(),
            original_ids_and_providers
        );
        assert_eq!(
            lease(&db, "lease-decompressed").await.content_mode,
            "decompressed"
        );
        assert_eq!(target_state(&db, "target-held").await, "pinned");
        assert_eq!(target_state(&db, "target-absent").await, "released");
        assert_eq!(remote(&db, "pinata", "bafy-held").await.epoch, 5);
        assert_eq!(remote(&db, "filebase", "bafy-absent").await.epoch, 9);
        assert_eq!(usage(&db, "pinata").await, (100, 1));

        db.execute_unprepared(
            "UPDATE pin_leases SET state = 'expired' WHERE id = 'lease-decompressed'",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state = 'released' WHERE lease_id = 'lease-decompressed'",
        )
        .await
        .unwrap();
        db.execute_unprepared("UPDATE remote_pins SET status = 'absent', request_id = NULL WHERE provider = 'pinata' AND cid = 'bafy-held'")
            .await
            .unwrap();
        let before_released_lease = lease(&db, "lease-decompressed").await;
        let before_released_targets = lease_target_rows(&db, "lease-decompressed").await;
        let before_released_held = remote(&db, "pinata", "bafy-held").await;
        let before_released_absent = remote(&db, "filebase", "bafy-absent").await;
        assert!(matches!(
            renew_manual_lease(&db, "object-1", "lease-decompressed", time(40), time(3),)
                .await
                .unwrap_err(),
            RenewManualLeaseError::NoRecoverableReservation
        ));
        assert_eq!(
            lease(&db, "lease-decompressed").await,
            before_released_lease
        );
        assert_eq!(
            lease_target_rows(&db, "lease-decompressed").await,
            before_released_targets
        );
        assert_eq!(
            remote(&db, "pinata", "bafy-held").await,
            before_released_held
        );
        assert_eq!(
            remote(&db, "filebase", "bafy-absent").await,
            before_released_absent
        );

        db.execute_unprepared(
            "UPDATE pin_leases SET state = 'evicted' WHERE id = 'lease-decompressed'",
        )
        .await
        .unwrap();
        let before_evicted = lease(&db, "lease-decompressed").await;
        assert!(matches!(
            renew_manual_lease(&db, "object-1", "lease-decompressed", time(40), time(3),)
                .await
                .unwrap_err(),
            RenewManualLeaseError::InvalidState
        ));
        assert_eq!(lease(&db, "lease-decompressed").await, before_evicted);
    }

    #[tokio::test]
    async fn failover_bumps_lease_and_both_remote_epochs_once_without_stale_mutation() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-failover",
            "pinned",
            Some("old-request"),
            2,
        )
        .await;
        seed_remote(&db, "filebase", "bafy-failover", "reserved", None, 5).await;
        seed_lease_target(
            &db,
            "lease-failover",
            "one-policy",
            "one",
            "active",
            3,
            "target-old",
            "pinata",
            "bafy-failover",
            "pinned",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-new', 'lease-failover', 'bafy-failover', 100, 'filebase', 'waiting', \
                     '{}', '{}')",
            time(1).to_rfc3339(),
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();
        assert_eq!(
            failover_target_if_feasible(
                &db,
                "lease-failover",
                "target-old",
                "target-new",
                time(2),
            )
            .await
            .unwrap(),
            GenerationDecision::Current
        );
        assert_eq!(lease(&db, "lease-failover").await.generation, 4);
        assert_eq!(remote(&db, "pinata", "bafy-failover").await.epoch, 3);
        assert_eq!(remote(&db, "filebase", "bafy-failover").await.epoch, 6);
        let after_lease = lease(&db, "lease-failover").await;
        let after_old = remote(&db, "pinata", "bafy-failover").await;
        let after_new = remote(&db, "filebase", "bafy-failover").await;
        assert_eq!(
            failover_target_if_feasible(
                &db,
                "lease-failover",
                "target-old",
                "target-new",
                time(3),
            )
            .await
            .unwrap(),
            GenerationDecision::NoLongerNeeded
        );
        assert_eq!(lease(&db, "lease-failover").await, after_lease);
        assert_eq!(remote(&db, "pinata", "bafy-failover").await, after_old);
        assert_eq!(remote(&db, "filebase", "bafy-failover").await, after_new);
    }

    #[tokio::test]
    async fn one_failover_is_sticky_until_replacement_pins_then_returns_exact_old_cleanup() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-sticky-failover",
            "failed",
            Some("failed-primary"),
            2,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-sticky-failover",
            "automatic",
            "one",
            "active",
            3,
            "target-sticky-primary",
            "pinata",
            "bafy-sticky-failover",
            "degraded",
            time(100),
        )
        .await;
        let limits = ProviderLimitMap::from([
            (
                "pinata".to_owned(),
                ProviderLimits {
                    priority: 1,
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            ),
            (
                "filebase".to_owned(),
                ProviderLimits {
                    priority: 2,
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            ),
        ]);
        let providers = vec!["pinata".to_owned(), "filebase".to_owned()];

        let replacement = fail_one_target(
            &db,
            "lease-sticky-failover",
            "target-sticky-primary",
            &providers,
            &limits,
            time(1),
        )
        .await
        .unwrap()
        .expect("terminal primary must select the next reservable provider");
        assert_eq!(replacement.provider, "filebase");
        assert_eq!(replacement.state, "waiting");
        assert_eq!(target_state(&db, "target-sticky-primary").await, "degraded");
        assert_eq!(lease(&db, "lease-sticky-failover").await.generation, 4);
        assert_eq!(remote(&db, "pinata", "bafy-sticky-failover").await.epoch, 2);
        assert_eq!(
            remote(&db, "filebase", "bafy-sticky-failover").await.epoch,
            1
        );

        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .filter(pin_job::Column::Provider.eq("filebase"))
            .one(&db)
            .await
            .unwrap()
            .expect("replacement Submit must be durable");
        assert_eq!(submit.lease_id.as_deref(), Some("lease-sticky-failover"));
        assert_eq!(submit.target_id.as_deref(), Some(replacement.id.as_str()));
        assert_eq!(submit.expected_generation, Some(4));
        assert_eq!(submit.expected_remote_epoch, None);

        let duplicate = fail_one_target(
            &db,
            "lease-sticky-failover",
            "target-sticky-primary",
            &providers,
            &limits,
            time(2),
        )
        .await
        .unwrap()
        .expect("duplicate observation must retain the sticky replacement");
        assert_eq!(duplicate.id, replacement.id);
        assert_eq!(lease(&db, "lease-sticky-failover").await.generation, 4);
        assert_eq!(
            remote(&db, "filebase", "bafy-sticky-failover").await.epoch,
            1
        );

        db.execute_unprepared(
            "UPDATE remote_pins SET request_id = 'replacement-pinned', status = 'pinned' \
             WHERE provider = 'filebase' AND cid = 'bafy-sticky-failover'; \
             UPDATE pin_lease_targets SET state = 'pinned' WHERE id = 'target-sticky-primary' OR provider = 'filebase'",
        )
        .await
        .unwrap();
        let cleanup =
            converge_one_after_replacement(&db, "lease-sticky-failover", &replacement.id, time(3))
                .await
                .unwrap();

        assert_eq!(cleanup.len(), 1);
        let NewPinJob::Remote(cleanup) = &cleanup[0] else {
            panic!("old cleanup must be remote scoped")
        };
        assert_eq!(cleanup.operation, jobs::RemoteJobOperation::Unpin);
        assert_eq!(cleanup.provider, "pinata");
        assert_eq!(cleanup.cid, "bafy-sticky-failover");
        assert_eq!(cleanup.expected_remote_epoch, 3);
        assert_eq!(target_state(&db, "target-sticky-primary").await, "released");
        assert_eq!(lease(&db, "lease-sticky-failover").await.generation, 5);
        assert_eq!(remote(&db, "pinata", "bafy-sticky-failover").await.epoch, 3);
        assert_eq!(
            remote(&db, "filebase", "bafy-sticky-failover").await.epoch,
            1
        );
    }

    #[tokio::test]
    async fn consecutive_one_failures_advance_p1_to_p2_to_p3_without_failback() {
        let db = setup().await;
        let cid = "bafy-consecutive-failover";
        seed_remote(&db, "p1", cid, "failed", Some("failed-p1"), 1).await;
        seed_lease_target(
            &db,
            "lease-consecutive",
            "automatic",
            "one",
            "active",
            1,
            "target-p1",
            "p1",
            cid,
            "degraded",
            time(100),
        )
        .await;
        let provider_limits = ProviderLimitMap::from(["p1", "p2", "p3"].map(|provider| {
            (
                provider.to_owned(),
                ProviderLimits {
                    priority: match provider {
                        "p1" => 1,
                        "p2" => 2,
                        _ => 3,
                    },
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            )
        }));
        let providers = ["p1".to_owned(), "p2".to_owned(), "p3".to_owned()];

        let p2 = fail_one_target(
            &db,
            "lease-consecutive",
            "target-p1",
            &providers,
            &provider_limits,
            time(1),
        )
        .await
        .unwrap()
        .expect("p1 failure must select p2");
        assert_eq!(p2.provider, "p2");
        assert_eq!(lease(&db, "lease-consecutive").await.generation, 2);
        assert_eq!(remote(&db, "p2", cid).await.epoch, 1);
        db.execute_unprepared(&format!(
            "UPDATE remote_pins SET request_id='failed-p2', status='failed' \
             WHERE provider='p2' AND cid='{cid}'; \
             UPDATE pin_lease_targets SET state='degraded' WHERE id='{}'",
            p2.id
        ))
        .await
        .unwrap();

        let p3 = fail_one_target(
            &db,
            "lease-consecutive",
            &p2.id,
            &providers,
            &provider_limits,
            time(2),
        )
        .await
        .unwrap()
        .expect("p2 failure must advance strictly to p3");
        assert_eq!(p3.provider, "p3", "p2 failure must never fail back to p1");
        assert_eq!(lease(&db, "lease-consecutive").await.generation, 3);
        assert_eq!(remote(&db, "p1", cid).await.epoch, 1);
        assert_eq!(remote(&db, "p2", cid).await.epoch, 1);
        assert_eq!(remote(&db, "p3", cid).await.epoch, 1);
        let submits = pin_job::Entity::find()
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("submit"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(submits.len(), 2);
        assert!(submits.iter().any(|job| {
            job.provider == "p2"
                && job.target_id.as_deref() == Some(p2.id.as_str())
                && job.expected_generation == Some(2)
                && job.expected_remote_epoch.is_none()
        }));
        assert!(submits.iter().any(|job| {
            job.provider == "p3"
                && job.target_id.as_deref() == Some(p3.id.as_str())
                && job.expected_generation == Some(3)
                && job.expected_remote_epoch.is_none()
        }));

        db.execute_unprepared(&format!(
            "UPDATE remote_pins SET request_id='pinned-p3', status='pinned' \
             WHERE provider='p3' AND cid='{cid}'; \
             UPDATE pin_lease_targets SET state='pinned' WHERE id='{}'",
            p3.id
        ))
        .await
        .unwrap();
        let cleanup = converge_one_after_replacement(&db, "lease-consecutive", &p3.id, time(3))
            .await
            .unwrap();
        assert_eq!(cleanup.len(), 2);
        for cleanup_job in cleanup {
            let NewPinJob::Remote(cleanup) = &cleanup_job else {
                panic!("retired providers require remote-scoped cleanup")
            };
            assert_eq!(cleanup.operation, jobs::RemoteJobOperation::Unpin);
            assert_eq!(cleanup.expected_remote_epoch, 2);
            assert!(matches!(cleanup.provider.as_str(), "p1" | "p2"));
            jobs::enqueue_job(&db, cleanup_job).await.unwrap();
        }
        assert_eq!(lease(&db, "lease-consecutive").await.generation, 4);
        assert_eq!(target_state(&db, "target-p1").await, "released");
        assert_eq!(target_state(&db, &p2.id).await, "released");
        assert_eq!(target_state(&db, &p3.id).await, "pinned");
        assert_eq!(remote(&db, "p1", cid).await.epoch, 2);
        assert_eq!(remote(&db, "p2", cid).await.epoch, 2);
        assert_eq!(remote(&db, "p3", cid).await.epoch, 1);
    }

    #[tokio::test]
    async fn already_pinned_replacement_atomically_retires_ambiguous_old_assignment() {
        let db = setup().await;
        let cid = "bafy-pinned-replacement";
        seed_remote(&db, "p1", cid, "reserved", None, 2).await;
        seed_remote(&db, "p2", cid, "pinned", Some("shared-p2"), 7).await;
        seed_lease_target(
            &db,
            "lease-shared-p2",
            "copy",
            "all",
            "active",
            9,
            "target-shared-p2",
            "p2",
            cid,
            "pinned",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-pinned-replacement",
            "automatic",
            "one",
            "active",
            4,
            "target-ambiguous-p1",
            "p1",
            cid,
            "waiting",
            time(100),
        )
        .await;
        jobs::enqueue_job(
            &db,
            jobs::submit_job(
                "p1",
                cid,
                "lease-pinned-replacement",
                "target-ambiguous-p1",
                4,
                time(1),
            ),
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "UPDATE pin_jobs SET state='running', submit_phase='calling', locked_until='2026-07-21T01:00:00Z' \
             WHERE provider='p1' AND cid='bafy-pinned-replacement' AND operation='submit'",
        )
        .await
        .unwrap();
        let provider_limits = ProviderLimitMap::from(["p1", "p2"].map(|provider| {
            (
                provider.to_owned(),
                ProviderLimits {
                    priority: if provider == "p1" { 1 } else { 2 },
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            )
        }));

        let transaction = db.begin().await.unwrap();
        let replacement = fail_one_target(
            &transaction,
            "lease-pinned-replacement",
            "target-ambiguous-p1",
            &["p1".to_owned(), "p2".to_owned()],
            &provider_limits,
            time(2),
        )
        .await
        .unwrap()
        .expect("pinned shared fallback must be reused");
        assert_eq!(replacement.provider, "p2");
        assert_eq!(replacement.state, "pinned");
        assert_eq!(
            pin_lease::Entity::find_by_id("lease-shared-p2".to_owned())
                .one(&transaction)
                .await
                .unwrap()
                .unwrap()
                .generation,
            9,
            "read-only shared projection must not mutate an existing all-mode lease"
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-shared-p2".to_owned())
                .one(&transaction)
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned"
        );
        assert_eq!(
            pin_lease::Entity::find_by_id("lease-pinned-replacement".to_owned())
                .one(&transaction)
                .await
                .unwrap()
                .unwrap()
                .generation,
            6,
            "failover and immediate convergence each advance generation once"
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-ambiguous-p1".to_owned())
                .one(&transaction)
                .await
                .unwrap()
                .unwrap()
                .state,
            "released"
        );
        assert_eq!(
            remote_pin::Entity::find_by_id(("p1".to_owned(), cid.to_owned()))
                .one(&transaction)
                .await
                .unwrap()
                .unwrap()
                .epoch,
            3
        );
        assert_eq!(
            remote_pin::Entity::find_by_id(("p2".to_owned(), cid.to_owned()))
                .one(&transaction)
                .await
                .unwrap()
                .unwrap()
                .epoch,
            8
        );
        let cleanup = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("p1"))
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(3))
            .one(&transaction)
            .await
            .unwrap()
            .expect("ambiguous old Submit must schedule current remote Reconcile");
        assert!(cleanup.lease_id.is_none());
        assert!(cleanup.target_id.is_none());
        assert!(cleanup.expected_generation.is_none());
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("p2"))
                .filter(pin_job::Column::Operation.eq("submit"))
                .count(&transaction)
                .await
                .unwrap(),
            0,
            "already pinned fallback must not create a second Submit"
        );
        transaction.commit().await.unwrap();
    }

    #[tokio::test]
    async fn idempotent_failover_call_converges_an_existing_pinned_suffix_assignment() {
        let db = setup().await;
        let cid = "bafy-existing-pinned-replacement";
        seed_remote(&db, "p1", cid, "failed", Some("old-request"), 2).await;
        seed_remote(&db, "p2", cid, "pinned", Some("new-request"), 7).await;
        seed_lease_target(
            &db,
            "lease-existing-pinned",
            "automatic",
            "one",
            "active",
            4,
            "target-existing-old",
            "p1",
            cid,
            "degraded",
            time(100),
        )
        .await;
        let replacement_id = target_id("lease-existing-pinned", "p2", cid);
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('{replacement_id}', 'lease-existing-pinned', '{cid}', 100, 'p2', 'pinned', \
                     '{}', '{}')",
            time(1).to_rfc3339(),
            time(1).to_rfc3339()
        ))
        .await
        .unwrap();
        let provider_limits = ProviderLimitMap::from([
            (
                "p1".to_owned(),
                ProviderLimits {
                    priority: 1,
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            ),
            (
                "p2".to_owned(),
                ProviderLimits {
                    priority: 2,
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            ),
        ]);

        let existing = fail_one_target(
            &db,
            "lease-existing-pinned",
            "target-existing-old",
            &["p1".to_owned(), "p2".to_owned()],
            &provider_limits,
            time(2),
        )
        .await
        .unwrap()
        .expect("existing strict-suffix assignment must be reused");
        assert_eq!(existing.id, replacement_id);
        assert_eq!(existing.state, "pinned");
        assert_eq!(lease(&db, "lease-existing-pinned").await.generation, 5);
        assert_eq!(target_state(&db, "target-existing-old").await, "released");
        assert_eq!(target_state(&db, &replacement_id).await, "pinned");
        assert_eq!(remote(&db, "p1", cid).await.epoch, 3);
        assert_eq!(remote(&db, "p2", cid).await.epoch, 7);
        let cleanup = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("p1"))
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("unpin"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(3))
            .one(&db)
            .await
            .unwrap()
            .expect("existing pinned assignment must retire old remote atomically");
        assert!(cleanup.lease_id.is_none());
        assert!(cleanup.target_id.is_none());
        assert!(cleanup.expected_generation.is_none());
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("p2"))
                .filter(pin_job::Column::Operation.eq("submit"))
                .count(&db)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn one_mode_eviction_preserves_active_lease_for_exact_next_provider_failover() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-one-evicted",
            "pinned",
            Some("evicted-primary"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-one-evicted",
            "automatic",
            "one",
            "active",
            1,
            "target-one-evicted",
            "pinata",
            "bafy-one-evicted",
            "pinned",
            time(100),
        )
        .await;

        assert_eq!(
            evict_provider_cid(&db, "pinata", "bafy-one-evicted", time(1))
                .await
                .unwrap(),
            GenerationDecision::Current
        );
        assert_eq!(
            (
                lease(&db, "lease-one-evicted").await.state,
                lease(&db, "lease-one-evicted").await.generation,
            ),
            ("active".to_owned(), 2)
        );
        assert_eq!(target_state(&db, "target-one-evicted").await, "evicted");
        assert_eq!(remote(&db, "pinata", "bafy-one-evicted").await.epoch, 2);
        let old_cleanup = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("pinata"))
            .filter(pin_job::Column::Cid.eq("bafy-one-evicted"))
            .filter(pin_job::Column::Operation.eq("unpin"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(2))
            .one(&db)
            .await
            .unwrap()
            .expect("evicted old provider must retain remote-scoped cleanup");
        assert!(old_cleanup.lease_id.is_none());
        assert!(old_cleanup.target_id.is_none());
        assert!(old_cleanup.expected_generation.is_none());

        let limits = ProviderLimitMap::from([
            (
                "pinata".to_owned(),
                ProviderLimits {
                    priority: 1,
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            ),
            (
                "filebase".to_owned(),
                ProviderLimits {
                    priority: 2,
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            ),
        ]);
        let replacement = fail_one_target(
            &db,
            "lease-one-evicted",
            "target-one-evicted",
            &["pinata".to_owned(), "filebase".to_owned()],
            &limits,
            time(2),
        )
        .await
        .unwrap()
        .expect("one-mode eviction must select the next provider");
        assert_eq!(replacement.provider, "filebase");
        assert_eq!(replacement.state, "waiting");
        assert_eq!(lease(&db, "lease-one-evicted").await.generation, 3);
        assert_eq!(remote(&db, "pinata", "bafy-one-evicted").await.epoch, 2);
        assert_eq!(remote(&db, "filebase", "bafy-one-evicted").await.epoch, 1);
        let replacement_submit = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("filebase"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            replacement_submit.lease_id.as_deref(),
            Some("lease-one-evicted")
        );
        assert_eq!(
            replacement_submit.target_id.as_deref(),
            Some(replacement.id.as_str())
        );
        assert_eq!(replacement_submit.expected_generation, Some(3));
        assert_eq!(replacement_submit.expected_remote_epoch, None);
    }

    #[tokio::test]
    async fn one_failover_skips_quota_blocked_fallback_and_selects_next_reservable_provider() {
        let db = setup().await;
        let cid = "bafy-quota-skip";
        seed_remote(&db, "p1", cid, "failed", Some("failed-p1"), 1).await;
        seed_lease_target(
            &db,
            "lease-quota-skip",
            "automatic",
            "one",
            "active",
            1,
            "target-quota-p1",
            "p1",
            cid,
            "degraded",
            time(100),
        )
        .await;
        let provider_limits = ProviderLimitMap::from([
            (
                "p1".to_owned(),
                ProviderLimits {
                    priority: 1,
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            ),
            (
                "p2".to_owned(),
                ProviderLimits {
                    priority: 2,
                    max_bytes: 1_000,
                    max_pins: 0,
                    enabled: true,
                },
            ),
            (
                "p3".to_owned(),
                ProviderLimits {
                    priority: 3,
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            ),
        ]);

        let replacement = fail_one_target(
            &db,
            "lease-quota-skip",
            "target-quota-p1",
            &["p1".to_owned(), "p2".to_owned(), "p3".to_owned()],
            &provider_limits,
            time(1),
        )
        .await
        .unwrap()
        .expect("p3 must be selected after p2 cannot reserve quota");
        assert_eq!(replacement.provider, "p3");
        assert_eq!(replacement.state, "waiting");
        assert_eq!(lease(&db, "lease-quota-skip").await.generation, 2);
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq("lease-quota-skip"))
                .filter(pin_lease_target::Column::Provider.eq("p2"))
                .count(&db)
                .await
                .unwrap(),
            0
        );
        assert_eq!(remote(&db, "p1", cid).await.epoch, 1);
        assert_eq!(remote(&db, "p3", cid).await.epoch, 1);
        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("p3"))
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(submit.lease_id.as_deref(), Some("lease-quota-skip"));
        assert_eq!(submit.target_id.as_deref(), Some(replacement.id.as_str()));
        assert_eq!(submit.expected_generation, Some(2));
        assert_eq!(submit.expected_remote_epoch, None);
    }

    #[tokio::test]
    async fn all_mode_quota_eviction_preserves_pinned_sibling_availability() {
        let db = setup().await;
        let cid = "bafy-all-eviction";
        seed_remote(&db, "p1", cid, "pinned", Some("pinned-p1"), 1).await;
        seed_remote(&db, "p2", cid, "pinned", Some("pinned-p2"), 1).await;
        seed_lease_target(
            &db,
            "lease-all-eviction",
            "automatic",
            "all",
            "active",
            1,
            "target-all-p1",
            "p1",
            cid,
            "pinned",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-all-p2', 'lease-all-eviction', '{cid}', 100, 'p2', 'pinned', '{}', '{}')",
            time(1).to_rfc3339(),
            time(1).to_rfc3339()
        ))
        .await
        .unwrap();

        assert_eq!(
            evict_provider_cid(&db, "p1", cid, time(2)).await.unwrap(),
            GenerationDecision::Current
        );
        let lease = lease(&db, "lease-all-eviction").await;
        assert_eq!(lease.state, "active");
        assert_eq!(lease.generation, 2);
        assert_eq!(target_state(&db, "target-all-p1").await, "evicted");
        assert_eq!(target_state(&db, "target-all-p2").await, "pinned");
        assert_eq!(remote(&db, "p1", cid).await.epoch, 2);
        assert_eq!(remote(&db, "p2", cid).await.epoch, 1);
        let cleanup = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("p1"))
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::Operation.eq("unpin"))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(2))
            .one(&db)
            .await
            .unwrap()
            .expect("evicted all-mode target requires exact remote cleanup");
        assert!(cleanup.lease_id.is_none());
        assert!(cleanup.target_id.is_none());
        assert!(cleanup.expected_generation.is_none());

        let projected = apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "p2",
                cid,
                request_id: "pinned-p2",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Pinned,
                error_class: None,
                error_text: None,
                now: time(3),
            },
        )
        .await
        .unwrap();
        let RemoteStatusApplyResult::Applied { affected, .. } = projected else {
            panic!("current pinned sibling projection must apply")
        };
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0].lease_id, "lease-all-eviction");
        assert!(affected[0].available);
    }

    #[tokio::test]
    async fn adopt_null_queued_projection_uses_one_canonical_poll_and_excludes_noncurrent_targets()
    {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy-queued", "reserved", None, 6).await;
        seed_lease_target(
            &db,
            "lease-queue-a",
            "queue-a",
            "all",
            "active",
            2,
            "target-a",
            "pinata",
            "bafy-queued",
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-queue-b",
            "queue-b",
            "all",
            "active",
            2,
            "target-b",
            "pinata",
            "bafy-queued",
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-queue-terminal",
            "queue-terminal",
            "all",
            "cancelled",
            2,
            "target-terminal-queued",
            "pinata",
            "bafy-queued",
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-queue-quota",
            "queue-quota",
            "all",
            "active",
            2,
            "target-quota-queued",
            "pinata",
            "bafy-queued",
            "quota_waiting",
            time(100),
        )
        .await;

        let result = apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy-queued",
                request_id: "adopted-request",
                origin: RemoteStatusOrigin::Adopt,
                status: RemotePinStatus::Queued,
                error_class: None,
                error_text: None,
                now: time(5),
            },
        )
        .await
        .unwrap();
        let RemoteStatusApplyResult::Applied { affected, .. } = result else {
            panic!("null request should be adopted")
        };
        assert_eq!(affected.len(), 2);
        assert_eq!(
            remote(&db, "pinata", "bafy-queued")
                .await
                .request_id
                .as_deref(),
            Some("adopted-request")
        );
        assert_eq!(target_state(&db, "target-a").await, "submitted");
        assert_eq!(target_state(&db, "target-b").await, "submitted");
        assert_eq!(target_state(&db, "target-terminal-queued").await, "waiting");
        assert_eq!(
            target_state(&db, "target-quota-queued").await,
            "quota_waiting"
        );
        let polls = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("pinata"))
            .filter(pin_job::Column::Cid.eq("bafy-queued"))
            .filter(pin_job::Column::Operation.eq("poll"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(polls.len(), 1);
        assert_eq!(polls[0].target_id.as_deref(), Some("target-a"));
        assert_eq!(polls[0].expected_generation, Some(2));
        assert_eq!(polls[0].next_attempt_at, time(10));
    }

    #[tokio::test]
    async fn stale_end_lease_compare_and_set_does_not_schedule_or_mutate_targets() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy-cas", "pinned", Some("request-cas"), 1).await;
        seed_lease_target(
            &db,
            "lease-cas",
            "cas",
            "all",
            "active",
            1,
            "target-cas",
            "pinata",
            "bafy-cas",
            "pinned",
            time(100),
        )
        .await;
        let stale = lease(&db, "lease-cas").await;
        db.execute_unprepared("UPDATE pin_leases SET generation = 2 WHERE id = 'lease-cas'")
            .await
            .unwrap();
        assert!(matches!(
            end_leases(&db, &[stale], LEASE_EXPIRED, TARGET_RELEASED, time(1))
                .await
                .unwrap_err(),
            AppError::Database(_)
        ));
        assert_eq!(lease(&db, "lease-cas").await.generation, 2);
        assert_eq!(target_state(&db, "target-cas").await, "pinned");
        assert_eq!(remote(&db, "pinata", "bafy-cas").await.epoch, 1);
        assert!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("pinata"))
                .filter(pin_job::Column::Cid.eq("bafy-cas"))
                .all(&db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn stale_manual_renewal_compare_and_sets_return_database_errors_before_remote_work() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-active-cas",
            "pinned",
            Some("request-active"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-active-cas",
            "manual",
            "all",
            "active",
            1,
            "target-active-cas",
            "pinata",
            "bafy-active-cas",
            "pinned",
            time(10),
        )
        .await;
        let active_stale = lease(&db, "lease-active-cas").await;
        db.execute_unprepared("UPDATE pin_leases SET generation = 2 WHERE id = 'lease-active-cas'")
            .await
            .unwrap();
        assert!(matches!(
            advance_active_manual_lease(&db, &active_stale, time(20), time(1))
                .await
                .unwrap_err(),
            RenewManualLeaseError::Database(_)
        ));
        assert_eq!(lease(&db, "lease-active-cas").await.generation, 2);
        assert_eq!(remote(&db, "pinata", "bafy-active-cas").await.epoch, 1);

        seed_remote(
            &db,
            "filebase",
            "bafy-expired-cas",
            "pinned",
            Some("request-expired"),
            3,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-expired-cas",
            "manual-expired",
            "all",
            "expired",
            3,
            "target-expired-cas",
            "filebase",
            "bafy-expired-cas",
            "released",
            time(1),
        )
        .await;
        let expired_stale = lease(&db, "lease-expired-cas").await;
        db.execute_unprepared(
            "UPDATE pin_leases SET generation = 4 WHERE id = 'lease-expired-cas'",
        )
        .await
        .unwrap();
        assert!(matches!(
            reactivate_expired_manual_lease(&db, &expired_stale, time(20), time(1))
                .await
                .unwrap_err(),
            RenewManualLeaseError::Database(_)
        ));
        let expired_after = lease(&db, "lease-expired-cas").await;
        assert_eq!(
            (expired_after.state.as_str(), expired_after.generation),
            ("expired", 4)
        );
        assert_eq!(remote(&db, "filebase", "bafy-expired-cas").await.epoch, 3);
    }

    #[tokio::test]
    async fn caller_transaction_rolls_back_expired_renewal_when_confirmed_release_wins_after_snapshot()
     {
        let (_directory, db) = setup_file_backed("renewal-release-race").await;
        seed_remote(
            &db,
            "pinata",
            "bafy-renewal-release-race",
            "pinned",
            Some("request-race"),
            4,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-renewal-release-race",
            "manual",
            "all",
            "expired",
            7,
            "target-renewal-release-race",
            "pinata",
            "bafy-renewal-release-race",
            "released",
            time(1),
        )
        .await;
        let original_targets = lease_target_rows(&db, "lease-renewal-release-race").await;
        let gate = Arc::new(test_gates::RenewalAfterSnapshotGate {
            lease_id: "lease-renewal-release-race",
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        *test_gates::RENEWAL_AFTER_SNAPSHOT.lock().await = Some(gate.clone());

        let renewal_db = db.clone();
        let renewal = tokio::spawn(async move {
            let transaction = renewal_db.begin().await.unwrap();
            let result = renew_manual_lease(
                &transaction,
                "object-1",
                "lease-renewal-release-race",
                time(30),
                time(2),
            )
            .await;
            match result {
                Ok(outcome) => {
                    transaction.commit().await.unwrap();
                    Ok(outcome)
                }
                Err(error) => {
                    transaction.rollback().await.unwrap();
                    Err(error)
                }
            }
        });
        gate.arrived.notified().await;
        assert_eq!(
            quota::confirmed_release(&db, "pinata", "bafy-renewal-release-race", 4, time(3))
                .await
                .unwrap(),
            ConfirmedReleaseOutcome::Released
        );
        gate.resume.notify_one();
        *test_gates::RENEWAL_AFTER_SNAPSHOT.lock().await = None;

        assert!(matches!(
            renewal.await.unwrap().unwrap_err(),
            RenewManualLeaseError::Database(_)
        ));
        let final_lease = lease(&db, "lease-renewal-release-race").await;
        let final_targets = lease_target_rows(&db, "lease-renewal-release-race").await;
        assert_eq!(final_lease.state, "expired");
        assert_eq!(final_lease.generation, 7);
        assert_eq!(final_targets, original_targets);
        assert_eq!(
            target_state(&db, "target-renewal-release-race").await,
            "released"
        );
        assert_eq!(
            remote(&db, "pinata", "bafy-renewal-release-race")
                .await
                .status,
            "absent"
        );
        assert_eq!(usage(&db, "pinata").await, (0, 0));
    }

    #[tokio::test]
    async fn cancellation_expiry_and_eviction_bump_each_affected_remote_once() {
        let db = setup().await;
        seed_remote(&db, "pinata", "bafy-cancel", "pinned", Some("r1"), 1).await;
        seed_lease_target(
            &db,
            "lease-cancel",
            "automatic",
            "all",
            "active",
            1,
            "target-cancel",
            "pinata",
            "bafy-cancel",
            "pinned",
            time(100),
        )
        .await;
        assert_eq!(
            cancel_lease(&db, "lease-cancel", time(1)).await.unwrap(),
            GenerationDecision::Current
        );
        assert_eq!(remote(&db, "pinata", "bafy-cancel").await.epoch, 2);

        seed_remote(&db, "pinata", "bafy-expire", "pinned", Some("r2"), 1).await;
        seed_lease_target(
            &db,
            "lease-expire",
            "expire",
            "all",
            "active",
            1,
            "target-expire",
            "pinata",
            "bafy-expire",
            "pinned",
            time(1),
        )
        .await;
        assert_eq!(
            expire_due_leases(&db, time(2)).await.unwrap(),
            vec!["lease-expire"]
        );
        assert_eq!(remote(&db, "pinata", "bafy-expire").await.epoch, 2);

        seed_remote(&db, "pinata", "bafy-evict", "pinned", Some("r3"), 1).await;
        seed_lease_target(
            &db,
            "lease-evict",
            "evict",
            "all",
            "active",
            1,
            "target-evict",
            "pinata",
            "bafy-evict",
            "pinned",
            time(100),
        )
        .await;
        assert_eq!(
            evict_provider_cid(&db, "pinata", "bafy-evict", time(2))
                .await
                .unwrap(),
            GenerationDecision::Current
        );
        assert_eq!(remote(&db, "pinata", "bafy-evict").await.epoch, 2);
    }

    #[tokio::test]
    async fn eviction_stale_lease_cas_rolls_back_all_target_remote_and_job_mutations() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-evict-stale-a",
            "pinned",
            Some("request-a"),
            3,
        )
        .await;
        seed_remote(
            &db,
            "filebase",
            "bafy-evict-stale-b",
            "pinned",
            Some("request-b"),
            7,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-evict-stale",
            "evict-stale",
            "all",
            "active",
            11,
            "target-evict-stale-a",
            "pinata",
            "bafy-evict-stale-a",
            "pinned",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-evict-stale-b', 'lease-evict-stale', 'bafy-evict-stale-b', 100, \
                     'filebase', 'pinned', '{}', '{}')",
            time(1).to_rfc3339(),
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();
        let before_lease = lease(&db, "lease-evict-stale").await;
        let before_targets = lease_target_rows(&db, "lease-evict-stale").await;
        let before_remote_a = remote(&db, "pinata", "bafy-evict-stale-a").await;
        let before_remote_b = remote(&db, "filebase", "bafy-evict-stale-b").await;
        let before_jobs = pin_job::Entity::find().all(&db).await.unwrap();
        let gate = Arc::new(test_gates::LifecycleLeaseCasGate {
            lease_id: "lease-evict-stale",
            expected_generation: 11,
            staged_generation: 12,
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        *test_gates::EVICTION_BEFORE_LEASE_CAS.lock().await = Some(gate.clone());

        let eviction_db = db.clone();
        let eviction = tokio::spawn(async move {
            let transaction = eviction_db.begin().await.unwrap();
            let result =
                evict_provider_cid(&transaction, "pinata", "bafy-evict-stale-a", time(2)).await;
            transaction.rollback().await.unwrap();
            result
        });
        gate.arrived.notified().await;
        gate.resume.notify_one();
        *test_gates::EVICTION_BEFORE_LEASE_CAS.lock().await = None;

        assert!(matches!(
            eviction.await.unwrap(),
            Err(AppError::Database(message)) if message.contains("stale")
        ));
        assert_eq!(lease(&db, "lease-evict-stale").await, before_lease);
        assert_eq!(
            lease_target_rows(&db, "lease-evict-stale").await,
            before_targets
        );
        assert_eq!(
            remote(&db, "pinata", "bafy-evict-stale-a").await,
            before_remote_a
        );
        assert_eq!(
            remote(&db, "filebase", "bafy-evict-stale-b").await,
            before_remote_b
        );
        assert_eq!(pin_job::Entity::find().all(&db).await.unwrap(), before_jobs);
    }

    #[tokio::test]
    async fn failover_stale_lease_cas_returns_before_target_release_or_remote_work() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-failover-stale",
            "pinned",
            Some("old-request"),
            2,
        )
        .await;
        seed_remote(&db, "filebase", "bafy-failover-stale", "reserved", None, 5).await;
        seed_lease_target(
            &db,
            "lease-failover-stale",
            "failover-stale",
            "one",
            "active",
            4,
            "target-failover-stale-old",
            "pinata",
            "bafy-failover-stale",
            "pinned",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-failover-stale-new', 'lease-failover-stale', 'bafy-failover-stale', \
                     100, 'filebase', 'waiting', '{}', '{}')",
            time(1).to_rfc3339(),
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();
        let before_lease = lease(&db, "lease-failover-stale").await;
        let before_targets = lease_target_rows(&db, "lease-failover-stale").await;
        let before_old_remote = remote(&db, "pinata", "bafy-failover-stale").await;
        let before_new_remote = remote(&db, "filebase", "bafy-failover-stale").await;
        let before_jobs = pin_job::Entity::find().all(&db).await.unwrap();
        let gate = Arc::new(test_gates::LifecycleLeaseCasGate {
            lease_id: "lease-failover-stale",
            expected_generation: 4,
            staged_generation: 5,
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        *test_gates::FAILOVER_BEFORE_LEASE_CAS.lock().await = Some(gate.clone());

        let failover_db = db.clone();
        let failover = tokio::spawn(async move {
            let transaction = failover_db.begin().await.unwrap();
            let result = failover_target_if_feasible(
                &transaction,
                "lease-failover-stale",
                "target-failover-stale-old",
                "target-failover-stale-new",
                time(2),
            )
            .await;
            transaction.rollback().await.unwrap();
            result
        });
        gate.arrived.notified().await;
        gate.resume.notify_one();
        *test_gates::FAILOVER_BEFORE_LEASE_CAS.lock().await = None;

        assert!(matches!(
            failover.await.unwrap(),
            Err(AppError::Database(message)) if message.contains("stale")
        ));
        assert_eq!(lease(&db, "lease-failover-stale").await, before_lease);
        assert_eq!(
            lease_target_rows(&db, "lease-failover-stale").await,
            before_targets
        );
        assert_eq!(
            remote(&db, "pinata", "bafy-failover-stale").await,
            before_old_remote
        );
        assert_eq!(
            remote(&db, "filebase", "bafy-failover-stale").await,
            before_new_remote
        );
        assert_eq!(pin_job::Entity::find().all(&db).await.unwrap(), before_jobs);
    }

    #[tokio::test]
    async fn eviction_orders_lease_target_and_remote_lifecycle_cas_events() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-eviction-order",
            "pinned",
            Some("request-order"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-eviction-order-z",
            "eviction-order-z",
            "all",
            "active",
            1,
            "target-eviction-order-a",
            "pinata",
            "bafy-eviction-order",
            "pinned",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-eviction-order-a",
            "eviction-order-a",
            "all",
            "active",
            1,
            "target-eviction-order-z",
            "pinata",
            "bafy-eviction-order",
            "pinned",
            time(100),
        )
        .await;
        start_lifecycle_order_recording(
            &["lease-eviction-order-a", "lease-eviction-order-z"],
            &["target-eviction-order-a", "target-eviction-order-z"],
            &[("pinata", "bafy-eviction-order")],
        )
        .await;

        evict_provider_cid(&db, "pinata", "bafy-eviction-order", time(1))
            .await
            .unwrap();

        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::LeaseLock("lease-eviction-order-a".to_owned()),
                test_gates::LifecycleOrderEvent::LeaseLock("lease-eviction-order-z".to_owned()),
                test_gates::LifecycleOrderEvent::TargetLock("target-eviction-order-a".to_owned()),
                test_gates::LifecycleOrderEvent::TargetLock("target-eviction-order-z".to_owned()),
                test_gates::LifecycleOrderEvent::RemoteLock(
                    "pinata".to_owned(),
                    "bafy-eviction-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::LeaseCas("lease-eviction-order-a".to_owned()),
                test_gates::LifecycleOrderEvent::LeaseCas("lease-eviction-order-z".to_owned()),
                test_gates::LifecycleOrderEvent::TargetCas("target-eviction-order-a".to_owned()),
                test_gates::LifecycleOrderEvent::TargetCas("target-eviction-order-z".to_owned()),
                test_gates::LifecycleOrderEvent::RemoteWork(
                    "pinata".to_owned(),
                    "bafy-eviction-order".to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn failover_orders_lease_then_created_target_order_then_remote_pairs() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-failover-order",
            "pinned",
            Some("old-request"),
            2,
        )
        .await;
        seed_remote(&db, "filebase", "bafy-failover-order", "reserved", None, 5).await;
        seed_lease_target(
            &db,
            "lease-failover-order",
            "failover-order",
            "one",
            "active",
            3,
            "target-failover-order-retiring",
            "pinata",
            "bafy-failover-order",
            "pinned",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-failover-order-replacement', 'lease-failover-order', \
                     'bafy-failover-order', 100, 'filebase', 'waiting', '{}', '{}')",
            time(1).to_rfc3339(),
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();
        start_lifecycle_order_recording(
            &["lease-failover-order"],
            &[
                "target-failover-order-retiring",
                "target-failover-order-replacement",
            ],
            &[
                ("filebase", "bafy-failover-order"),
                ("pinata", "bafy-failover-order"),
            ],
        )
        .await;

        failover_target_if_feasible(
            &db,
            "lease-failover-order",
            "target-failover-order-retiring",
            "target-failover-order-replacement",
            time(2),
        )
        .await
        .unwrap();

        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::LeaseCas("lease-failover-order".to_owned()),
                test_gates::LifecycleOrderEvent::TargetCas(
                    "target-failover-order-retiring".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetCas(
                    "target-failover-order-replacement".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteWork(
                    "filebase".to_owned(),
                    "bafy-failover-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteWork(
                    "pinata".to_owned(),
                    "bafy-failover-order".to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn pinned_one_failover_converges_without_reacquiring_lifecycle_after_remote_locks() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        let cid = "bafy-pinned-failover-order";
        seed_remote(&db, "p1", cid, "failed", Some("old-request"), 2).await;
        seed_remote(&db, "p2", cid, "pinned", Some("shared-request"), 7).await;
        seed_lease_target(
            &db,
            "lease-pinned-failover-order",
            "failover-order-new",
            "one",
            "active",
            4,
            "target-pinned-failover-old",
            "p1",
            cid,
            "degraded",
            time(100),
        )
        .await;
        let replacement_id = target_id("lease-pinned-failover-order", "p2", cid);
        start_lifecycle_order_recording(
            &["lease-pinned-failover-order"],
            &["target-pinned-failover-old", replacement_id.as_str()],
            &[("p1", cid), ("p2", cid)],
        )
        .await;

        fail_one_target(
            &db,
            "lease-pinned-failover-order",
            "target-pinned-failover-old",
            &["p1".to_owned(), "p2".to_owned()],
            &ProviderLimitMap::from([
                (
                    "p1".to_owned(),
                    ProviderLimits {
                        priority: 1,
                        max_bytes: 1_000,
                        max_pins: 10,
                        enabled: true,
                    },
                ),
                (
                    "p2".to_owned(),
                    ProviderLimits {
                        priority: 2,
                        max_bytes: 1_000,
                        max_pins: 10,
                        enabled: true,
                    },
                ),
            ]),
            time(2),
        )
        .await
        .unwrap();

        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::LeaseLock(
                    "lease-pinned-failover-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetLock(
                    "target-pinned-failover-old".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteLock("p1".to_owned(), cid.to_owned()),
                test_gates::LifecycleOrderEvent::RemoteLock("p2".to_owned(), cid.to_owned()),
                test_gates::LifecycleOrderEvent::LeaseCas("lease-pinned-failover-order".to_owned(),),
                test_gates::LifecycleOrderEvent::TargetProjection(replacement_id),
                test_gates::LifecycleOrderEvent::LeaseCas("lease-pinned-failover-order".to_owned(),),
                test_gates::LifecycleOrderEvent::TargetCas("target-pinned-failover-old".to_owned(),),
                test_gates::LifecycleOrderEvent::RemoteWork("p1".to_owned(), cid.to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn reversed_provider_priority_locks_shared_remote_frontier_lexically() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        let cid = "bafy-reversed-lock-order";
        seed_remote(&db, "z0", cid, "failed", Some("failed-z0"), 1).await;
        seed_lease_target(
            &db,
            "lease-reversed-lock-order",
            "automatic",
            "one",
            "active",
            1,
            "target-reversed-lock-order",
            "z0",
            cid,
            "degraded",
            time(100),
        )
        .await;
        let provider_limits = ProviderLimitMap::from(["z0", "p2", "p1"].map(|provider| {
            (
                provider.to_owned(),
                ProviderLimits {
                    priority: 1,
                    max_bytes: 1_000,
                    max_pins: 10,
                    enabled: true,
                },
            )
        }));
        start_lifecycle_order_recording(
            &["lease-reversed-lock-order"],
            &["target-reversed-lock-order"],
            &[("z0", cid), ("p2", cid), ("p1", cid)],
        )
        .await;

        let replacement = fail_one_target(
            &db,
            "lease-reversed-lock-order",
            "target-reversed-lock-order",
            &["z0".to_owned(), "p2".to_owned(), "p1".to_owned()],
            &provider_limits,
            time(2),
        )
        .await
        .unwrap()
        .unwrap();
        let events = finish_lifecycle_order_recording().await;
        let remote_locks = events
            .iter()
            .filter_map(|event| match event {
                test_gates::LifecycleOrderEvent::RemoteLock(provider, cid) => {
                    Some((provider.clone(), cid.clone()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(replacement.provider, "p2");
        assert_eq!(
            remote_locks,
            vec![
                ("p1".to_owned(), cid.to_owned()),
                ("p2".to_owned(), cid.to_owned()),
                ("z0".to_owned(), cid.to_owned()),
            ],
            "policy preference may be reversed, but the portable/PostgreSQL remote lock frontier must remain lexical"
        );
        let first_remote = events
            .iter()
            .position(|event| matches!(event, test_gates::LifecycleOrderEvent::RemoteLock(_, _)))
            .unwrap();
        assert!(!events[first_remote + 1..].iter().any(|event| matches!(
            event,
            test_gates::LifecycleOrderEvent::LeaseLock(_)
                | test_gates::LifecycleOrderEvent::TargetLock(_)
        )));
    }

    #[tokio::test]
    async fn end_leases_normalizes_reversed_input_to_lease_target_remote_order() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-end-order-a",
            "pinned",
            Some("request-a"),
            1,
        )
        .await;
        seed_remote(
            &db,
            "filebase",
            "bafy-end-order-z",
            "pinned",
            Some("request-z"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-end-order-a",
            "end-order-a",
            "all",
            "active",
            1,
            "target-end-order-a",
            "pinata",
            "bafy-end-order-a",
            "pinned",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-end-order-z",
            "end-order-z",
            "all",
            "active",
            1,
            "target-end-order-z",
            "filebase",
            "bafy-end-order-z",
            "pinned",
            time(100),
        )
        .await;
        let lease_z = lease(&db, "lease-end-order-z").await;
        let lease_a = lease(&db, "lease-end-order-a").await;
        start_lifecycle_order_recording(
            &["lease-end-order-a", "lease-end-order-z"],
            &["target-end-order-a", "target-end-order-z"],
            &[
                ("filebase", "bafy-end-order-z"),
                ("pinata", "bafy-end-order-a"),
            ],
        )
        .await;

        end_leases(
            &db,
            &[lease_z, lease_a],
            LEASE_CANCELLED,
            TARGET_RELEASED,
            time(1),
        )
        .await
        .unwrap();

        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::LeaseCas("lease-end-order-a".to_owned()),
                test_gates::LifecycleOrderEvent::LeaseCas("lease-end-order-z".to_owned()),
                test_gates::LifecycleOrderEvent::TargetCas("target-end-order-a".to_owned()),
                test_gates::LifecycleOrderEvent::TargetCas("target-end-order-z".to_owned()),
                test_gates::LifecycleOrderEvent::RemoteWork(
                    "filebase".to_owned(),
                    "bafy-end-order-z".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteWork(
                    "pinata".to_owned(),
                    "bafy-end-order-a".to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn manual_renewal_orders_owner_then_lease_target_and_remote_work() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        db.execute_unprepared(
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest) VALUES ('object-renewal-order', 'bucket', 'renewal-order-key', 'bafy-renewal-order', 100, 'bafy-renewal-order', TRUE)",
        )
        .await
        .unwrap();
        seed_remote(
            &db,
            "pinata",
            "bafy-renewal-order",
            "pinned",
            Some("request-order"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-renewal-order",
            "manual",
            "all",
            "active",
            1,
            "target-renewal-order",
            "pinata",
            "bafy-renewal-order",
            "pinned",
            time(10),
        )
        .await;
        let updated = db
            .execute_unprepared(
                "UPDATE pin_leases SET owner_object_id = 'object-renewal-order' WHERE id = 'lease-renewal-order'",
            )
            .await
            .unwrap();
        assert_eq!(updated.rows_affected(), 1);
        start_lifecycle_order_recording(
            &["lease-renewal-order"],
            &["target-renewal-order"],
            &[("pinata", "bafy-renewal-order")],
        )
        .await;
        include_owner_in_lifecycle_order_recording("object-renewal-order").await;

        renew_manual_lease(
            &db,
            "object-renewal-order",
            "lease-renewal-order",
            time(20),
            time(1),
        )
        .await
        .unwrap();

        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::OwnerLock("object-renewal-order".to_owned()),
                test_gates::LifecycleOrderEvent::LeaseLock("lease-renewal-order".to_owned()),
                test_gates::LifecycleOrderEvent::TargetLock("target-renewal-order".to_owned(),),
                test_gates::LifecycleOrderEvent::RemoteLock(
                    "pinata".to_owned(),
                    "bafy-renewal-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::OwnerGuard("object-renewal-order".to_owned()),
                test_gates::LifecycleOrderEvent::LeaseCas("lease-renewal-order".to_owned()),
                test_gates::LifecycleOrderEvent::TargetCas("target-renewal-order".to_owned()),
                test_gates::LifecycleOrderEvent::RemoteWork(
                    "pinata".to_owned(),
                    "bafy-renewal-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetProjection(
                    "target-renewal-order".to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn apply_remote_status_orders_prelocks_remote_write_and_projections() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-status-order",
            "queued",
            Some("request-status-order"),
            4,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-z-status-order",
            "status-z",
            "all",
            "active",
            1,
            "target-z-created-first",
            "pinata",
            "bafy-status-order",
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-a-status-order",
            "status-a",
            "all",
            "active",
            1,
            "target-a-created-later",
            "pinata",
            "bafy-status-order",
            "waiting",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}', last_touched_at = '{}' \
             WHERE id = 'target-z-created-first'",
            time(1).to_rfc3339(),
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}', last_touched_at = '{}' \
             WHERE id = 'target-a-created-later'",
            time(2).to_rfc3339(),
            time(2).to_rfc3339(),
        ))
        .await
        .unwrap();
        start_lifecycle_order_recording(
            &["lease-a-status-order", "lease-z-status-order"],
            &["target-z-created-first", "target-a-created-later"],
            &[("pinata", "bafy-status-order")],
        )
        .await;

        apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy-status-order",
                request_id: "request-status-order",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Pinned,
                error_class: None,
                error_text: None,
                now: time(3),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::LeaseLock("lease-a-status-order".to_owned()),
                test_gates::LifecycleOrderEvent::LeaseLock("lease-z-status-order".to_owned()),
                test_gates::LifecycleOrderEvent::TargetLock("target-z-created-first".to_owned(),),
                test_gates::LifecycleOrderEvent::TargetLock("target-a-created-later".to_owned(),),
                test_gates::LifecycleOrderEvent::RemoteLock(
                    "pinata".to_owned(),
                    "bafy-status-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteStatusWrite(
                    "pinata".to_owned(),
                    "bafy-status-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetProjection(
                    "target-z-created-first".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetProjection(
                    "target-a-created-later".to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn complete_remote_delete_compensation_orders_prelocks_remote_and_targets() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-delete-order",
            "pinned",
            Some("delete-order"),
            6,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-z-delete-order",
            "delete-z",
            "all",
            "active",
            1,
            "target-z-delete-first",
            "pinata",
            "bafy-delete-order",
            "pinned",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-a-delete-order",
            "delete-a",
            "all",
            "active",
            1,
            "target-a-delete-later",
            "pinata",
            "bafy-delete-order",
            "pinned",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}' WHERE id = 'target-z-delete-first'",
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}' WHERE id = 'target-a-delete-later'",
            time(2).to_rfc3339(),
        ))
        .await
        .unwrap();
        start_lifecycle_order_recording(
            &["lease-a-delete-order", "lease-z-delete-order"],
            &["target-z-delete-first", "target-a-delete-later"],
            &[("pinata", "bafy-delete-order")],
        )
        .await;

        assert!(matches!(
            complete_remote_delete(&db, "pinata", "bafy-delete-order", 6, time(3))
                .await
                .unwrap(),
            RemoteDeleteCompletion::Compensated { .. }
        ));
        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::LeaseLock("lease-a-delete-order".to_owned()),
                test_gates::LifecycleOrderEvent::LeaseLock("lease-z-delete-order".to_owned()),
                test_gates::LifecycleOrderEvent::TargetLock("target-z-delete-first".to_owned(),),
                test_gates::LifecycleOrderEvent::TargetLock("target-a-delete-later".to_owned(),),
                test_gates::LifecycleOrderEvent::RemoteLock(
                    "pinata".to_owned(),
                    "bafy-delete-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteCompensation(
                    "pinata".to_owned(),
                    "bafy-delete-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetProjection(
                    "target-z-delete-first".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetProjection(
                    "target-a-delete-later".to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn postgres_ordered_lifecycle_prelock_queries_render_for_update_in_global_order() {
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-prelock-sql",
            "pinned",
            Some("request"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-z-prelock-sql",
            "prelock-z",
            "all",
            "active",
            1,
            "target-z-created-first",
            "pinata",
            "bafy-prelock-sql",
            "pinned",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-a-prelock-sql",
            "prelock-a",
            "all",
            "active",
            1,
            "target-a-created-later",
            "pinata",
            "bafy-prelock-sql",
            "pinned",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}' WHERE id = 'target-z-created-first'",
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}' WHERE id = 'target-a-created-later'",
            time(2).to_rfc3339(),
        ))
        .await
        .unwrap();
        let targets = vec![
            pin_lease_target::Entity::find_by_id("target-a-created-later".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            pin_lease_target::Entity::find_by_id("target-z-created-first".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
        ];
        let rendered = render_ordered_lifecycle_prelock_queries(
            &object::Entity::find_by_id("object-1".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            &[
                lease(&db, "lease-z-prelock-sql").await,
                lease(&db, "lease-a-prelock-sql").await,
            ],
            &targets,
            "pinata",
            "bafy-prelock-sql",
        );
        assert_eq!(
            rendered
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "owner:object-1",
                "lease:lease-a-prelock-sql",
                "lease:lease-z-prelock-sql",
                "target:target-z-created-first",
                "target:target-a-created-later",
                "remote:pinata:bafy-prelock-sql",
            ]
        );
        assert!(
            rendered
                .iter()
                .all(|(_, query)| query.contains("FOR UPDATE"))
        );
        assert!(rendered[0].1.contains("\"objects\""));
        assert!(rendered[1].1.contains("\"pin_leases\""));
        assert!(rendered[3].1.contains("\"pin_lease_targets\""));
        assert!(rendered[5].1.contains("\"remote_pins\""));
    }

    #[tokio::test]
    async fn status_availability_excludes_the_affected_remote_but_keeps_other_pinned_targets() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-availability-affected",
            "pinned",
            Some("availability-request"),
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-availability",
            "automatic",
            "all",
            "active",
            1,
            "target-availability-affected",
            "pinata",
            "bafy-availability-affected",
            "pinned",
            time(100),
        )
        .await;

        let pinned = apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy-availability-affected",
                request_id: "availability-request",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Pinned,
                error_class: None,
                error_text: None,
                now: time(1),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            pinned,
            RemoteStatusApplyResult::Applied { ref affected, .. } if affected.len() == 1 && affected[0].available
        ));

        let failed = apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy-availability-affected",
                request_id: "availability-request",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Failed,
                error_class: Some("transient"),
                error_text: None,
                now: time(2),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            failed,
            RemoteStatusApplyResult::Applied { ref affected, .. } if affected.len() == 1 && !affected[0].available
        ));

        let queued = apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy-availability-affected",
                request_id: "availability-request",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Queued,
                error_class: None,
                error_text: None,
                now: time(3),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            queued,
            RemoteStatusApplyResult::Applied { ref affected, .. } if affected.len() == 1 && !affected[0].available
        ));

        apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy-availability-affected",
                request_id: "availability-request",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Pinned,
                error_class: None,
                error_text: None,
                now: time(4),
            },
        )
        .await
        .unwrap();
        seed_remote(
            &db,
            "filebase",
            "bafy-availability-unaffected",
            "pinned",
            Some("unaffected-request"),
            1,
        )
        .await;
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-availability-unaffected', 'lease-availability', \
                     'bafy-availability-unaffected', 100, 'filebase', 'pinned', '{}', '{}')",
            time(1).to_rfc3339(),
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();

        start_lifecycle_order_recording(
            &["lease-availability"],
            &[
                "target-availability-affected",
                "target-availability-unaffected",
            ],
            &[("pinata", "bafy-availability-affected")],
        )
        .await;

        let failed_with_other_pin = apply_remote_status(
            &db,
            RemoteStatusUpdate {
                provider: "pinata",
                cid: "bafy-availability-affected",
                request_id: "availability-request",
                origin: RemoteStatusOrigin::ExistingRequest,
                status: RemotePinStatus::Failed,
                error_class: Some("transient"),
                error_text: None,
                now: time(5),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            failed_with_other_pin,
            RemoteStatusApplyResult::Applied { ref affected, .. } if affected.len() == 1 && affected[0].available
        ));
        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::LeaseLock("lease-availability".to_owned()),
                test_gates::LifecycleOrderEvent::TargetLock(
                    "target-availability-affected".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetLock(
                    "target-availability-unaffected".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteLock(
                    "pinata".to_owned(),
                    "bafy-availability-affected".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteStatusWrite(
                    "pinata".to_owned(),
                    "bafy-availability-affected".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetProjection(
                    "target-availability-affected".to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn renewal_projects_only_its_locked_targets_but_uses_shared_oldest_canonical_target() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        db.execute_unprepared(
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest) \
             VALUES ('object-shared-renewal-order', 'bucket', 'shared-renewal-order-key', \
                     'bafy-shared-renewal-owner', 100, 'bafy-shared-renewal-owner', TRUE)",
        )
        .await
        .unwrap();
        seed_remote(&db, "pinata", "bafy-shared-renewal", "reserved", None, 1).await;
        seed_lease_target(
            &db,
            "lease-renewed-shared",
            "manual",
            "all",
            "active",
            1,
            "target-renewed-shared",
            "pinata",
            "bafy-shared-renewal",
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-unrelated-shared",
            "automatic",
            "all",
            "active",
            1,
            "target-unrelated-shared",
            "pinata",
            "bafy-shared-renewal",
            "waiting",
            time(100),
        )
        .await;
        let updated = db
            .execute_unprepared(
                "UPDATE pin_leases SET owner_object_id = 'object-shared-renewal-order' \
                 WHERE id = 'lease-renewed-shared'",
            )
            .await
            .unwrap();
        assert_eq!(updated.rows_affected(), 1);
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}', last_touched_at = '{}' \
             WHERE id = 'target-unrelated-shared'",
            time(1).to_rfc3339(),
            time(5).to_rfc3339(),
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}' WHERE id = 'target-renewed-shared'",
            time(2).to_rfc3339(),
        ))
        .await
        .unwrap();
        let unrelated_before =
            pin_lease_target::Entity::find_by_id("target-unrelated-shared".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        start_lifecycle_order_recording_with_desired_target_reads(
            &["lease-renewed-shared", "lease-unrelated-shared"],
            &["target-renewed-shared", "target-unrelated-shared"],
            &[("pinata", "bafy-shared-renewal")],
        )
        .await;
        include_owner_in_lifecycle_order_recording("object-shared-renewal-order").await;

        assert_eq!(
            renew_manual_lease(
                &db,
                "object-shared-renewal-order",
                "lease-renewed-shared",
                time(200),
                time(3)
            )
            .await
            .unwrap(),
            ManualLeaseRenewalOutcome::Extended { generation: 2 }
        );

        let events = finish_lifecycle_order_recording().await;
        let first_lifecycle_lock = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    test_gates::LifecycleOrderEvent::LeaseLock(_)
                        | test_gates::LifecycleOrderEvent::TargetLock(_)
                        | test_gates::LifecycleOrderEvent::RemoteLock(_, _)
                )
            })
            .expect("renewal must prelock its complete lifecycle snapshot");
        assert!(
            events
                .iter()
                .skip(first_lifecycle_lock)
                .all(|event| !matches!(
                    event,
                    test_gates::LifecycleOrderEvent::DesiredTargetsRead(_, _)
                )),
            "canonical selection must not query shared siblings after the lifecycle frontier"
        );
        assert_eq!(
            events,
            vec![
                test_gates::LifecycleOrderEvent::DesiredTargetsRead(
                    "pinata".to_owned(),
                    "bafy-shared-renewal".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::OwnerLock(
                    "object-shared-renewal-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::LeaseLock("lease-renewed-shared".to_owned(),),
                test_gates::LifecycleOrderEvent::LeaseLock("lease-unrelated-shared".to_owned(),),
                test_gates::LifecycleOrderEvent::TargetLock("target-unrelated-shared".to_owned(),),
                test_gates::LifecycleOrderEvent::TargetLock("target-renewed-shared".to_owned(),),
                test_gates::LifecycleOrderEvent::RemoteLock(
                    "pinata".to_owned(),
                    "bafy-shared-renewal".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::OwnerGuard(
                    "object-shared-renewal-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::LeaseCas("lease-renewed-shared".to_owned()),
                test_gates::LifecycleOrderEvent::TargetCas("target-renewed-shared".to_owned()),
                test_gates::LifecycleOrderEvent::RemoteWork(
                    "pinata".to_owned(),
                    "bafy-shared-renewal".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetProjection(
                    "target-renewed-shared".to_owned(),
                ),
            ]
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-unrelated-shared".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            unrelated_before
        );
        assert_eq!(
            remote(&db, "pinata", "bafy-shared-renewal")
                .await
                .last_touched_at,
            time(5)
        );
        let NewPinJob::Target(canonical) = jobs::submit_job(
            "pinata",
            "bafy-shared-renewal",
            "lease-unrelated-shared",
            "target-unrelated-shared",
            1,
            time(3),
        ) else {
            unreachable!("submit constructor is target scoped")
        };
        assert!(
            pin_job::Entity::find_by_id(canonical.id)
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn failed_resubmit_orders_prelocks_remote_prepare_and_target_projections() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-resubmit-order",
            "failed",
            Some("failed-request"),
            7,
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE remote_pins SET failure_attempts = 2, next_retry_at = '{}' \
             WHERE provider = 'pinata' AND cid = 'bafy-resubmit-order'",
            time(0).to_rfc3339(),
        ))
        .await
        .unwrap();
        seed_lease_target(
            &db,
            "lease-z-resubmit-order",
            "resubmit-z",
            "all",
            "active",
            1,
            "target-z-resubmit-first",
            "pinata",
            "bafy-resubmit-order",
            "degraded",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-a-resubmit-order",
            "resubmit-a",
            "all",
            "active",
            1,
            "target-a-resubmit-later",
            "pinata",
            "bafy-resubmit-order",
            "degraded",
            time(100),
        )
        .await;
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}' WHERE id = 'target-z-resubmit-first'",
            time(1).to_rfc3339(),
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "UPDATE pin_lease_targets SET created_at = '{}' WHERE id = 'target-a-resubmit-later'",
            time(2).to_rfc3339(),
        ))
        .await
        .unwrap();
        start_lifecycle_order_recording(
            &["lease-a-resubmit-order", "lease-z-resubmit-order"],
            &["target-z-resubmit-first", "target-a-resubmit-later"],
            &[("pinata", "bafy-resubmit-order")],
        )
        .await;

        assert!(matches!(
            prepare_failed_remote_resubmit(
                &db,
                "pinata",
                "bafy-resubmit-order",
                7,
                "failed-request",
                time(1),
            )
            .await
            .unwrap(),
            FailedRemoteResubmitDecision::Prepared { .. }
        ));
        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::LeaseLock("lease-a-resubmit-order".to_owned()),
                test_gates::LifecycleOrderEvent::LeaseLock("lease-z-resubmit-order".to_owned()),
                test_gates::LifecycleOrderEvent::TargetLock("target-z-resubmit-first".to_owned(),),
                test_gates::LifecycleOrderEvent::TargetLock("target-a-resubmit-later".to_owned(),),
                test_gates::LifecycleOrderEvent::RemoteLock(
                    "pinata".to_owned(),
                    "bafy-resubmit-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteResubmit(
                    "pinata".to_owned(),
                    "bafy-resubmit-order".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetProjection(
                    "target-z-resubmit-first".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetProjection(
                    "target-a-resubmit-later".to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn publication_prelocks_union_of_leases_then_targets_then_remotes() {
        let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let db = setup().await;
        seed_remote(
            &db,
            "pinata",
            "bafy-publication-frontier-z",
            "reserved",
            None,
            1,
        )
        .await;
        seed_remote(
            &db,
            "filebase",
            "bafy-publication-frontier-a",
            "reserved",
            None,
            1,
        )
        .await;
        seed_lease_target(
            &db,
            "lease-publication-frontier-z",
            "automatic",
            "all",
            "active",
            1,
            "target-publication-frontier-z",
            "pinata",
            "bafy-publication-frontier-z",
            "waiting",
            time(100),
        )
        .await;
        seed_lease_target(
            &db,
            "lease-publication-frontier-a",
            "manual",
            "all",
            "active",
            1,
            "target-publication-frontier-a",
            "filebase",
            "bafy-publication-frontier-a",
            "waiting",
            time(100),
        )
        .await;
        start_lifecycle_order_recording(
            &[
                "lease-publication-frontier-a",
                "lease-publication-frontier-z",
            ],
            &[
                "target-publication-frontier-a",
                "target-publication-frontier-z",
            ],
            &[
                ("filebase", "bafy-publication-frontier-a"),
                ("pinata", "bafy-publication-frontier-z"),
            ],
        )
        .await;

        lock_publication_lifecycle_frontier(
            &db,
            &["object-1".to_owned()],
            &[
                (
                    "pinata".to_owned(),
                    "bafy-publication-frontier-z".to_owned(),
                ),
                (
                    "filebase".to_owned(),
                    "bafy-publication-frontier-a".to_owned(),
                ),
            ],
        )
        .await
        .unwrap();

        assert_eq!(
            finish_lifecycle_order_recording().await,
            vec![
                test_gates::LifecycleOrderEvent::LeaseLock(
                    "lease-publication-frontier-a".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::LeaseLock(
                    "lease-publication-frontier-z".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetLock(
                    "target-publication-frontier-a".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::TargetLock(
                    "target-publication-frontier-z".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteLock(
                    "filebase".to_owned(),
                    "bafy-publication-frontier-a".to_owned(),
                ),
                test_gates::LifecycleOrderEvent::RemoteLock(
                    "pinata".to_owned(),
                    "bafy-publication-frontier-z".to_owned(),
                ),
            ]
        );
    }

    #[test]
    fn postgres_prelock_staleness_requires_rollback_but_sqlite_can_retry() {
        assert_eq!(
            lifecycle_retry_policy(DatabaseBackend::Postgres),
            LifecycleRetryPolicy::Rollback
        );
        assert_eq!(
            lifecycle_retry_policy(DatabaseBackend::Sqlite),
            LifecycleRetryPolicy::Retry
        );
    }
}
