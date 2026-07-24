use std::{cmp, collections::BTreeSet};

use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, TryInsertResult, sea_query::Expr,
};

use crate::{
    error::{AppError, AppResult},
    pinning::{
        config::{ProviderLimitMap, ProviderLimits},
        quota::{
            CapacityDecision, EvictionCandidate, ProviderUsage, reservation_decision,
            select_eviction_candidates,
        },
    },
    store::{
        entities::{pin_lease, pin_lease_target, pin_provider_usage, remote_pin},
        pinning::leases,
    },
};

pub type DateTimeUtc = DateTime<Utc>;

const REMOTE_STATUS_RESERVED: &str = "reserved";
const REMOTE_STATUS_QUEUED: &str = "queued";
const REMOTE_STATUS_PINNING: &str = "pinning";
const REMOTE_STATUS_PINNED: &str = "pinned";
const REMOTE_STATUS_FAILED: &str = "failed";
const REMOTE_STATUS_ABSENT: &str = "absent";
const ACTIVE_LEASE_STATE: &str = "active";
const ACTIVE_TARGET_STATES: [&str; 4] = ["waiting", "submitted", "pinned", "degraded"];
const TARGET_QUOTA_WAITING: &str = "quota_waiting";
const SQLITE_RETRY_LIMIT: usize = 8;

#[cfg(test)]
#[derive(Clone)]
enum EvictionDrift {
    CancelWaiter {
        selected_cid: &'static str,
        target_id: &'static str,
        lease_id: &'static str,
    },
    TouchCandidate {
        selected_cid: &'static str,
        target_id: &'static str,
        lease_id: &'static str,
        touched_at: DateTimeUtc,
    },
    ReleaseHeadroom {
        selected_cid: &'static str,
        cid: &'static str,
        target_id: &'static str,
        lease_id: &'static str,
        cid_size: i64,
    },
}

#[cfg(test)]
static EVICTION_DRIFT: std::sync::LazyLock<tokio::sync::Mutex<Option<EvictionDrift>>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(None));

#[cfg(test)]
static EVICTION_DRIFT_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

#[cfg(test)]
async fn stage_eviction_drift<C: ConnectionTrait>(
    db: &C,
    selected: &[(String, String)],
) -> AppResult<()> {
    let selected_cids = selected
        .iter()
        .map(|(_, cid)| cid.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut gate = EVICTION_DRIFT.lock().await;
    let matches = gate.as_ref().is_some_and(|drift| {
        let selected_cid = match drift {
            EvictionDrift::CancelWaiter { selected_cid, .. }
            | EvictionDrift::TouchCandidate { selected_cid, .. }
            | EvictionDrift::ReleaseHeadroom { selected_cid, .. } => *selected_cid,
        };
        selected_cids.contains(selected_cid)
    });
    let Some(drift) = matches.then(|| gate.take()).flatten() else {
        return Ok(());
    };
    drop(gate);
    match drift {
        EvictionDrift::CancelWaiter {
            target_id,
            lease_id,
            ..
        } => {
            pin_lease_target::Entity::update_many()
                .col_expr(
                    pin_lease_target::Column::State,
                    Expr::value("released".to_owned()),
                )
                .filter(pin_lease_target::Column::Id.eq(target_id))
                .exec(db)
                .await?;
            pin_lease::Entity::update_many()
                .col_expr(
                    pin_lease::Column::State,
                    Expr::value("cancelled".to_owned()),
                )
                .filter(pin_lease::Column::Id.eq(lease_id))
                .exec(db)
                .await?;
        }
        EvictionDrift::TouchCandidate {
            target_id,
            lease_id,
            touched_at,
            ..
        } => {
            let target = pin_lease_target::Entity::find_by_id(target_id.to_owned())
                .one(db)
                .await?
                .expect("test drift target must exist");
            pin_lease_target::Entity::update_many()
                .col_expr(
                    pin_lease_target::Column::LastTouchedAt,
                    Expr::value(touched_at),
                )
                .filter(pin_lease_target::Column::Id.eq(target_id))
                .exec(db)
                .await?;
            pin_lease::Entity::update_many()
                .col_expr(pin_lease::Column::LastTouchedAt, Expr::value(touched_at))
                .filter(pin_lease::Column::Id.eq(lease_id))
                .exec(db)
                .await?;
            remote_pin::Entity::update_many()
                .col_expr(remote_pin::Column::LastTouchedAt, Expr::value(touched_at))
                .filter(remote_pin::Column::Provider.eq(target.provider))
                .filter(remote_pin::Column::Cid.eq(target.cid))
                .exec(db)
                .await?;
        }
        EvictionDrift::ReleaseHeadroom {
            cid,
            target_id,
            lease_id,
            cid_size,
            ..
        } => {
            let usage = read_usage(db, "pinata")
                .await?
                .expect("test usage row must exist");
            remote_pin::Entity::update_many()
                .col_expr(
                    remote_pin::Column::Status,
                    Expr::value(REMOTE_STATUS_ABSENT.to_owned()),
                )
                .col_expr(
                    remote_pin::Column::RequestId,
                    Expr::value(Option::<String>::None),
                )
                .filter(remote_pin::Column::Provider.eq("pinata"))
                .filter(remote_pin::Column::Cid.eq(cid))
                .exec(db)
                .await?;
            pin_lease_target::Entity::update_many()
                .col_expr(
                    pin_lease_target::Column::State,
                    Expr::value("released".to_owned()),
                )
                .filter(pin_lease_target::Column::Id.eq(target_id))
                .exec(db)
                .await?;
            pin_lease::Entity::update_many()
                .col_expr(
                    pin_lease::Column::State,
                    Expr::value("cancelled".to_owned()),
                )
                .filter(pin_lease::Column::Id.eq(lease_id))
                .exec(db)
                .await?;
            pin_provider_usage::Entity::update_many()
                .col_expr(
                    pin_provider_usage::Column::ReservedBytes,
                    Expr::value(usage.reserved_bytes - cid_size),
                )
                .col_expr(
                    pin_provider_usage::Column::ReservedPins,
                    Expr::value(usage.reserved_pins - 1),
                )
                .filter(pin_provider_usage::Column::Provider.eq("pinata"))
                .exec(db)
                .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod order_events {
    use std::cell::RefCell;

    std::thread_local! {
        static EVENTS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
    }

    pub fn clear() {
        EVENTS.with(|events| events.borrow_mut().clear());
    }

    pub fn record(event: &'static str) {
        EVENTS.with(|events| events.borrow_mut().push(event));
    }

    pub fn take() -> Vec<&'static str> {
        EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
    }
}

#[cfg(test)]
fn record_order_event(event: &'static str) {
    order_events::record(event);
}

#[cfg(not(test))]
fn record_order_event(_: &'static str) {}

/// Result of reserving capacity for one unique `(provider, cid)` remote pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReservationOutcome {
    Reused,
    Reserved,
    QuotaWaiting { evict: Vec<(String, String)> },
    QuotaBlocked,
}

/// Result of a guarded, already-confirmed release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmedReleaseOutcome {
    Released,
    AlreadyAbsent,
    Stale,
}

/// Returns the usage row when it already exists, without mutating accounting.
pub async fn read_usage<C: ConnectionTrait>(
    db: &C,
    provider: &str,
) -> AppResult<Option<pin_provider_usage::Model>> {
    Ok(pin_provider_usage::Entity::find_by_id(provider.to_owned())
        .one(db)
        .await?)
}

/// Ensures the provider has a zero-initialized usage row and returns its current values.
///
/// This never opens a transaction. Callers that combine it with remote/target state changes must
/// pass their own SeaORM transaction so every mutation commits or rolls back together.
pub async fn ensure_usage_row<C: ConnectionTrait>(
    db: &C,
    provider: &str,
) -> AppResult<pin_provider_usage::Model> {
    record_order_event("usage_ensure");
    let inserted = pin_provider_usage::Entity::insert(pin_provider_usage::ActiveModel {
        provider: Set(provider.to_owned()),
        reserved_bytes: Set(0),
        reserved_pins: Set(0),
        observed_bytes: Set(None),
        observed_pins: Set(None),
        observed_at: Set(None),
    })
    .on_conflict_do_nothing()
    .exec(db)
    .await?;
    let usage = pin_provider_usage::Entity::find_by_id(provider.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| {
            AppError::Internal(format!(
                "provider usage row disappeared after ensure: {provider}; inserted={inserted:?}"
            ))
        })?;
    Ok(usage)
}

/// Ensures and locks every possible publication provider's accounting row in lexical order.
///
/// Publication calls this only after its complete lifecycle remote frontier is locked and before
/// the first reservation. The rows remain owned by the caller's transaction, preventing provider
/// priority order from inverting the global remote → usage ordering used by renewal.
pub(crate) async fn lock_publication_usage_rows<C: ConnectionTrait>(
    db: &C,
    providers: &[String],
) -> AppResult<()> {
    for provider in ordered_unique_providers(providers) {
        ensure_usage_row(db, &provider).await?;
        if db.get_database_backend() == DatabaseBackend::Postgres {
            pin_provider_usage::Entity::find_by_id(provider)
                .lock_exclusive()
                .one(db)
                .await?
                .ok_or_else(|| {
                    AppError::Internal(
                        "provider usage row disappeared during publication prelock".to_owned(),
                    )
                })?;
        }
    }
    Ok(())
}

fn ordered_unique_providers(providers: &[String]) -> Vec<String> {
    let mut providers = providers.to_vec();
    providers.sort();
    providers.dedup();
    providers
}

/// Reserves capacity for a unique remote pin or returns a durable quota outcome.
///
/// The caller owns transaction scope. In particular, callers creating a lease target must call
/// this and insert/touch that target in the same transaction, then call
/// [`refresh_remote_max_active_touch`]. A capacity-holding reuse is one new desired reference and
/// therefore advances the remote epoch exactly once per invocation.
pub async fn reserve_unique<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    cid_size: i64,
    limits: &ProviderLimitMap,
    now: DateTimeUtc,
) -> AppResult<ReservationOutcome> {
    for attempt in 0..SQLITE_RETRY_LIMIT {
        match reserve_unique_attempt(db, provider, cid, cid_size, limits, now).await {
            Ok(ReserveAttempt::Outcome(outcome)) => return Ok(outcome),
            Ok(ReserveAttempt::Retry) => retry_delay(attempt).await,
            Err(AppError::Database(message)) if is_sqlite_contention(&message) => {
                retry_delay(attempt).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(AppError::Database(
        "quota reservation exhausted retrying a concurrent SQLite write".to_owned(),
    ))
}

/// Recomputes and persists a remote pin's newest active desired-target touch.
///
/// It is intentionally separate from reservation because a caller normally inserts or renews the
/// target in the same outer transaction after reserving capacity.
pub async fn refresh_remote_max_active_touch<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<DateTimeUtc>> {
    let Some(last_active_touch) = newest_active_target_touch(db, provider, cid).await? else {
        return Ok(None);
    };
    remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::LastTouchedAt,
            Expr::value(last_active_touch),
        )
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Cid.eq(cid))
        .exec(db)
        .await?;
    Ok(Some(last_active_touch))
}

/// Marks a capacity-holding remote absent and decrements its unique reservation once.
///
/// Preconditions: the caller has already established, in the same caller-owned transaction, that
/// `expected_remote_epoch` is current and that no active desired target still references this
/// `(provider, cid)`. This primitive guards the epoch and capacity state but deliberately does not
/// implement cancellation, expiry, eviction, or provider confirmation.
pub async fn confirmed_release<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    now: DateTimeUtc,
) -> AppResult<ConfirmedReleaseOutcome> {
    confirmed_release_inner(
        db,
        provider,
        cid,
        expected_remote_epoch,
        RequestIdentityGuard::Any,
        now,
    )
    .await
}

pub(crate) async fn confirmed_release_for_request<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    expected_request_id: Option<&str>,
    now: DateTimeUtc,
) -> AppResult<ConfirmedReleaseOutcome> {
    confirmed_release_inner(
        db,
        provider,
        cid,
        expected_remote_epoch,
        RequestIdentityGuard::Exact(expected_request_id),
        now,
    )
    .await
}

enum RequestIdentityGuard<'a> {
    Any,
    Exact(Option<&'a str>),
}

async fn confirmed_release_inner<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    request_guard: RequestIdentityGuard<'_>,
    now: DateTimeUtc,
) -> AppResult<ConfirmedReleaseOutcome> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(ConfirmedReleaseOutcome::Stale);
    };
    if remote.epoch != expected_remote_epoch {
        return Ok(ConfirmedReleaseOutcome::Stale);
    }
    if let RequestIdentityGuard::Exact(expected) = request_guard
        && remote.request_id.as_deref() != expected
    {
        return Ok(ConfirmedReleaseOutcome::Stale);
    }
    if remote.status == REMOTE_STATUS_ABSENT {
        return Ok(ConfirmedReleaseOutcome::AlreadyAbsent);
    }
    if !is_capacity_holding_status(&remote.status) {
        return Err(invalid_quota(
            "remote pin is not in a releasable capacity state",
        ));
    }
    if remote.cid_size < 0 {
        return Err(invalid_quota("remote CID size cannot be negative"));
    }

    let usage = read_usage(db, provider)
        .await?
        .ok_or_else(|| AppError::Internal(format!("missing provider usage row for {provider}")))?;
    let next_bytes = usage
        .reserved_bytes
        .checked_sub(remote.cid_size)
        .ok_or_else(|| invalid_quota("reserved byte accounting underflow"))?;
    let next_pins = usage
        .reserved_pins
        .checked_sub(1)
        .ok_or_else(|| invalid_quota("reserved pin accounting underflow"))?;
    if next_bytes < 0 || next_pins < 0 {
        return Err(invalid_quota("reserved provider usage cannot be negative"));
    }

    let mut released = remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::Status,
            Expr::value(REMOTE_STATUS_ABSENT.to_owned()),
        )
        .col_expr(
            remote_pin::Column::RequestId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(remote_pin::Column::FailureAttempts, Expr::value(0_i32))
        .col_expr(
            remote_pin::Column::NextRetryAt,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            remote_pin::Column::LastFailedRequestId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(remote_pin::Column::LastTouchedAt, Expr::value(now))
        .col_expr(
            remote_pin::Column::LastErrorClass,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            remote_pin::Column::LastErrorText,
            Expr::value(Option::<String>::None),
        )
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Cid.eq(cid))
        .filter(remote_pin::Column::Epoch.eq(expected_remote_epoch))
        .filter(remote_pin::Column::Status.is_in(capacity_holding_statuses()));
    if let RequestIdentityGuard::Exact(expected) = request_guard {
        released = match expected {
            Some(request_id) => released.filter(remote_pin::Column::RequestId.eq(request_id)),
            None => released.filter(remote_pin::Column::RequestId.is_null()),
        };
    }
    let released = released.exec(db).await?;
    if released.rows_affected != 1 {
        return Ok(ConfirmedReleaseOutcome::Stale);
    }
    let updated_usage = pin_provider_usage::Entity::update_many()
        .col_expr(
            pin_provider_usage::Column::ReservedBytes,
            Expr::value(next_bytes),
        )
        .col_expr(
            pin_provider_usage::Column::ReservedPins,
            Expr::value(next_pins),
        )
        .filter(pin_provider_usage::Column::Provider.eq(provider))
        .filter(pin_provider_usage::Column::ReservedBytes.eq(usage.reserved_bytes))
        .filter(pin_provider_usage::Column::ReservedPins.eq(usage.reserved_pins))
        .exec(db)
        .await?;
    if updated_usage.rows_affected != 1 {
        return Err(AppError::Internal(
            "provider usage changed during a confirmed release; caller transaction is required"
                .to_owned(),
        ));
    }
    Ok(ConfirmedReleaseOutcome::Released)
}

/// Reserves and projects provider waiters in FIFO order without exceeding local limits.
///
/// Callers couple this to a confirmed release in their own transaction. A periodic worker scan
/// invokes the same API to recover a crash after release but before wake.
pub async fn wake_provider_waiters<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<Vec<String>> {
    leases::wake_quota_waiting_targets(db, provider, limits, now).await
}

/// Marks enough oldest reservations to satisfy the oldest active FIFO waiter after confirmation.
pub(crate) async fn evict_for_provider_waiter(
    db: &DatabaseTransaction,
    provider: &str,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<Vec<leases::QuotaEvictedTarget>> {
    evict_for_required_headroom(db, provider, EvictionRequest::Waiter, limits, now).await
}

/// Applies one bounded provider-quota response eviction while protecting the errored CID.
///
/// The caller must fence the exact running Submit claim as the first operation in `db`. This
/// function then validates the target generation and reserved request-less remote under the same
/// transaction before any unrelated reservation is marked for eviction.
#[derive(Clone, Copy)]
pub(crate) struct ProviderQuotaWork<'a> {
    pub cid: &'a str,
    pub lease_id: &'a str,
    pub target_id: &'a str,
    pub expected_generation: i64,
}

pub(crate) async fn evict_for_provider_quota(
    db: &DatabaseTransaction,
    provider: &str,
    work: ProviderQuotaWork<'_>,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<Vec<leases::QuotaEvictedTarget>> {
    evict_for_required_headroom(
        db,
        provider,
        EvictionRequest::ProviderQuota(work),
        limits,
        now,
    )
    .await
}

enum ReserveAttempt {
    Outcome(ReservationOutcome),
    Retry,
}

/// Builds the existing-row acquisition query used by PostgreSQL transactions.
///
/// The lock only spans the subsequent usage/CAS work when `db` is a caller-owned transaction.
/// SQLite deliberately uses its ordinary-read branch in [`acquire_existing_remote`].
fn existing_remote_lock_query(provider: &str, cid: &str) -> sea_orm::Select<remote_pin::Entity> {
    remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned())).lock_exclusive()
}

/// Acquires an existing remote pin before any existing-CID usage mutation.
///
/// PostgreSQL uses `FOR UPDATE` so the absent-row reservation path has one durable lock order:
/// remote pin first, then provider usage. New CIDs have no row to lock; their separate path grants
/// usage then attempts the insert, and on conflict undoes usage without reading or locking remote.
async fn acquire_existing_remote<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<remote_pin::Model>> {
    record_order_event("remote_acquire");
    if db.get_database_backend() == DatabaseBackend::Postgres {
        Ok(existing_remote_lock_query(provider, cid).one(db).await?)
    } else {
        Ok(
            remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
                .one(db)
                .await?,
        )
    }
}

async fn reserve_unique_attempt<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    cid_size: i64,
    limits: &ProviderLimitMap,
    now: DateTimeUtc,
) -> AppResult<ReserveAttempt> {
    let provider_limits = provider_limits(limits, provider)?;
    if cid_size < 0 {
        return Err(invalid_quota("CID size cannot be negative"));
    }
    if cid_size > provider_limits.max_bytes {
        return Ok(ReserveAttempt::Outcome(ReservationOutcome::QuotaBlocked));
    }

    let existing = acquire_existing_remote(db, provider, cid).await?;
    match existing {
        Some(remote) if is_capacity_holding_status(&remote.status) => {
            reuse_capacity_holding_remote(db, provider, cid, remote, now).await
        }
        Some(remote) if remote.status == REMOTE_STATUS_ABSENT => {
            reacquire_absent_remote(db, provider, cid, cid_size, provider_limits, remote, now).await
        }
        Some(_) => Err(invalid_quota("remote pin has an unknown status")),
        None => reserve_new_remote(db, provider, cid, cid_size, provider_limits, now).await,
    }
}

async fn reuse_capacity_holding_remote<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    remote: remote_pin::Model,
    now: DateTimeUtc,
) -> AppResult<ReserveAttempt> {
    let epoch = next_epoch(remote.epoch)?;
    let last_touched_at = newest_active_target_touch(db, provider, cid)
        .await?
        .map(|touch| cmp::max(touch, now))
        .unwrap_or_else(|| cmp::max(remote.last_touched_at, now));
    record_order_event("remote_reuse_cas");
    let updated = remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::Epoch, Expr::value(epoch))
        .col_expr(
            remote_pin::Column::LastTouchedAt,
            Expr::value(last_touched_at),
        )
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Cid.eq(cid))
        .filter(remote_pin::Column::Epoch.eq(remote.epoch))
        .filter(remote_pin::Column::Status.eq(remote.status))
        .exec(db)
        .await?;
    Ok(if updated.rows_affected == 1 {
        ReserveAttempt::Outcome(ReservationOutcome::Reused)
    } else {
        ReserveAttempt::Retry
    })
}

async fn reserve_new_remote<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    cid_size: i64,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<ReserveAttempt> {
    match reserve_capacity(db, provider, cid_size, limits).await? {
        CapacityReservation::Reserved(grant) => {
            let inserted = match remote_pin::Entity::insert(reserved_remote_model(
                provider, cid, cid_size, 1, now,
            ))
            .on_conflict_do_nothing()
            .exec(db)
            .await
            {
                Ok(inserted) => inserted,
                Err(error) => {
                    undo_capacity_grant(db, provider, grant).await?;
                    return Err(error.into());
                }
            };
            if matches!(inserted, TryInsertResult::Inserted(_)) {
                return Ok(ReserveAttempt::Outcome(ReservationOutcome::Reserved));
            }
            undo_capacity_grant(db, provider, grant).await?;
            Ok(ReserveAttempt::Retry)
        }
        CapacityReservation::Waiting(usage) => {
            let evict = eviction_candidates(db, provider, cid_size, usage, limits).await?;
            Ok(ReserveAttempt::Outcome(ReservationOutcome::QuotaWaiting {
                evict,
            }))
        }
        CapacityReservation::Retry => Ok(ReserveAttempt::Retry),
    }
}

async fn reacquire_absent_remote<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    cid_size: i64,
    limits: &ProviderLimits,
    remote: remote_pin::Model,
    now: DateTimeUtc,
) -> AppResult<ReserveAttempt> {
    let epoch = next_epoch(remote.epoch)?;
    match reserve_capacity(db, provider, cid_size, limits).await? {
        CapacityReservation::Reserved(grant) => {
            record_order_event("remote_absent_cas");
            let activated = match remote_pin::Entity::update_many()
                .col_expr(remote_pin::Column::CidSize, Expr::value(cid_size))
                .col_expr(
                    remote_pin::Column::Status,
                    Expr::value(REMOTE_STATUS_RESERVED.to_owned()),
                )
                .col_expr(remote_pin::Column::Epoch, Expr::value(epoch))
                .col_expr(
                    remote_pin::Column::RequestId,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(remote_pin::Column::FailureAttempts, Expr::value(0_i32))
                .col_expr(
                    remote_pin::Column::NextRetryAt,
                    Expr::value(Option::<DateTimeUtc>::None),
                )
                .col_expr(
                    remote_pin::Column::LastFailedRequestId,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(remote_pin::Column::LastTouchedAt, Expr::value(now))
                .col_expr(
                    remote_pin::Column::LastErrorClass,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(
                    remote_pin::Column::LastErrorText,
                    Expr::value(Option::<String>::None),
                )
                .filter(remote_pin::Column::Provider.eq(provider))
                .filter(remote_pin::Column::Cid.eq(cid))
                .filter(remote_pin::Column::Status.eq(REMOTE_STATUS_ABSENT))
                .filter(remote_pin::Column::Epoch.eq(remote.epoch))
                .exec(db)
                .await
            {
                Ok(activated) => activated,
                Err(error) => {
                    undo_capacity_grant(db, provider, grant).await?;
                    return Err(error.into());
                }
            };
            if activated.rows_affected == 1 {
                Ok(ReserveAttempt::Outcome(ReservationOutcome::Reserved))
            } else {
                undo_capacity_grant(db, provider, grant).await?;
                Ok(ReserveAttempt::Retry)
            }
        }
        CapacityReservation::Waiting(usage) => {
            let evict = eviction_candidates(db, provider, cid_size, usage, limits).await?;
            Ok(ReserveAttempt::Outcome(ReservationOutcome::QuotaWaiting {
                evict,
            }))
        }
        CapacityReservation::Retry => Ok(ReserveAttempt::Retry),
    }
}

enum CapacityReservation {
    Reserved(CapacityGrant),
    Waiting(pin_provider_usage::Model),
    Retry,
}

#[derive(Debug, Clone, Copy)]
struct CapacityGrant {
    cid_size: i64,
}

async fn reserve_capacity<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid_size: i64,
    limits: &ProviderLimits,
) -> AppResult<CapacityReservation> {
    let usage = ensure_usage_row(db, provider).await?;
    match reservation_decision(
        ProviderUsage {
            reserved_bytes: usage.reserved_bytes,
            reserved_pins: usage.reserved_pins,
        },
        cid_size,
        limits,
    )? {
        CapacityDecision::Block => Err(invalid_quota("unexpected blocked capacity reservation")),
        CapacityDecision::Wait => Ok(CapacityReservation::Waiting(usage)),
        CapacityDecision::Reserve => {
            let next_bytes = usage
                .reserved_bytes
                .checked_add(cid_size)
                .ok_or_else(|| invalid_quota("reserved byte accounting overflow"))?;
            let next_pins = usage
                .reserved_pins
                .checked_add(1)
                .ok_or_else(|| invalid_quota("reserved pin accounting overflow"))?;
            record_order_event("usage_update");
            let updated = pin_provider_usage::Entity::update_many()
                .col_expr(
                    pin_provider_usage::Column::ReservedBytes,
                    Expr::value(next_bytes),
                )
                .col_expr(
                    pin_provider_usage::Column::ReservedPins,
                    Expr::value(next_pins),
                )
                .filter(pin_provider_usage::Column::Provider.eq(provider))
                .filter(pin_provider_usage::Column::ReservedBytes.eq(usage.reserved_bytes))
                .filter(pin_provider_usage::Column::ReservedPins.eq(usage.reserved_pins))
                .exec(db)
                .await?;
            Ok(if updated.rows_affected == 1 {
                CapacityReservation::Reserved(CapacityGrant { cid_size })
            } else {
                CapacityReservation::Retry
            })
        }
    }
}

/// Removes a capacity increment that could not be paired with a remote-row mutation.
///
/// A failed cleanup is an error, never a retry: retrying would let a later observation treat the
/// unpaired counter increment as a real reservation.
async fn undo_capacity_grant<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    grant: CapacityGrant,
) -> AppResult<()> {
    for attempt in 0..SQLITE_RETRY_LIMIT {
        let usage = match read_usage(db, provider).await {
            Ok(Some(usage)) => usage,
            Ok(None) => {
                return Err(AppError::Internal(format!(
                    "missing provider usage row for {provider}"
                )));
            }
            Err(AppError::Database(message)) if is_sqlite_contention(&message) => {
                retry_delay(attempt).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let next_bytes = usage
            .reserved_bytes
            .checked_sub(grant.cid_size)
            .ok_or_else(|| invalid_quota("reserved byte accounting underflow"))?;
        let next_pins = usage
            .reserved_pins
            .checked_sub(1)
            .ok_or_else(|| invalid_quota("reserved pin accounting underflow"))?;
        if next_bytes < 0 || next_pins < 0 {
            return Err(invalid_quota("reserved provider usage cannot be negative"));
        }
        let updated = match pin_provider_usage::Entity::update_many()
            .col_expr(
                pin_provider_usage::Column::ReservedBytes,
                Expr::value(next_bytes),
            )
            .col_expr(
                pin_provider_usage::Column::ReservedPins,
                Expr::value(next_pins),
            )
            .filter(pin_provider_usage::Column::Provider.eq(provider))
            .filter(pin_provider_usage::Column::ReservedBytes.eq(usage.reserved_bytes))
            .filter(pin_provider_usage::Column::ReservedPins.eq(usage.reserved_pins))
            .exec(db)
            .await
        {
            Ok(updated) => updated,
            Err(error) if is_sqlite_contention(&error.to_string()) => {
                retry_delay(attempt).await;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        if updated.rows_affected == 1 {
            return Ok(());
        }
        retry_delay(attempt).await;
    }
    Err(AppError::Internal(
        "could not undo an unpaired provider capacity grant".to_owned(),
    ))
}

async fn eviction_candidates<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid_size: i64,
    usage: pin_provider_usage::Model,
    limits: &ProviderLimits,
) -> AppResult<Vec<(String, String)>> {
    let remotes = remote_pin::Entity::find()
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Status.is_in(capacity_holding_statuses()))
        .all(db)
        .await?;
    let mut candidates = Vec::with_capacity(remotes.len());
    for remote in remotes {
        let Some(last_active_touch) = newest_active_target_touch(db, provider, &remote.cid).await?
        else {
            continue;
        };
        candidates.push(EvictionCandidate {
            provider: provider.to_owned(),
            cid: remote.cid,
            cid_size: remote.cid_size,
            last_active_touch,
        });
    }
    select_eviction_candidates(
        ProviderUsage {
            reserved_bytes: usage.reserved_bytes,
            reserved_pins: usage.reserved_pins,
        },
        cid_size,
        limits,
        candidates,
    )
}

#[derive(Clone, Copy)]
enum EvictionRequest<'a> {
    Waiter,
    ProviderQuota(ProviderQuotaWork<'a>),
}

struct EvictionSnapshot {
    selected: Vec<(String, String)>,
    potential_pairs: Vec<(String, String)>,
    waiter_owner_ids: Vec<String>,
}

/// Evicts only after locking and revalidating every lifecycle row that can affect selection.
///
/// The caller owns the transaction. The first snapshot discovers the complete potential
/// frontier; lifecycle rows then lock before their remotes, and provider usage locks after every
/// remote. A second snapshot under those locks is the sole source of the destructive selection.
async fn evict_for_required_headroom(
    db: &DatabaseTransaction,
    provider: &str,
    request: EvictionRequest<'_>,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<Vec<leases::QuotaEvictedTarget>> {
    let initial = eviction_snapshot(db, provider, request, limits, now).await?;
    if initial.selected.is_empty() {
        return Ok(Vec::new());
    }

    leases::lock_publication_lifecycle_frontier(
        db,
        &initial.waiter_owner_ids,
        &initial.potential_pairs,
    )
    .await?;
    lock_eviction_usage_row(db, provider).await?;

    #[cfg(test)]
    stage_eviction_drift(db, &initial.selected).await?;

    let revalidated = eviction_snapshot(db, provider, request, limits, now).await?;
    if revalidated.selected.is_empty() {
        return Ok(Vec::new());
    }
    if revalidated.potential_pairs != initial.potential_pairs
        || revalidated.waiter_owner_ids != initial.waiter_owner_ids
    {
        // A row outside the locked frontier appeared or disappeared. Commit no destructive work;
        // the durable waiter scan or the next exact provider response will build a fresh frontier.
        return Ok(Vec::new());
    }
    leases::evict_provider_cids(db, &revalidated.selected, now).await
}

/// Builds a deterministic eviction decision without mutating state.
///
/// The first call determines which complete lifecycle frontier must lock. The second runs after
/// that frontier and its provider usage row are locked, so it observes the only snapshot allowed
/// to drive eviction.
async fn eviction_snapshot<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    request: EvictionRequest<'_>,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<EvictionSnapshot> {
    let (cid_size, excluded_cid, waiter_owner_ids) = match request {
        EvictionRequest::Waiter => {
            let waiting = active_quota_waiters(db, provider, now).await?;
            let Some((target, _)) = waiting.first() else {
                return Ok(EvictionSnapshot {
                    selected: Vec::new(),
                    potential_pairs: Vec::new(),
                    waiter_owner_ids: Vec::new(),
                });
            };
            if target.last_touched_at > target.created_at {
                // A previously evicted all-mode target may reuse genuine headroom after its
                // cooldown, but it never evicts the CID that just replaced it.
                return Ok(EvictionSnapshot {
                    selected: Vec::new(),
                    potential_pairs: Vec::new(),
                    waiter_owner_ids: Vec::new(),
                });
            }
            (
                target.logical_size,
                None,
                waiting
                    .into_iter()
                    .map(|(_, lease)| lease.owner_object_id)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            )
        }
        EvictionRequest::ProviderQuota(work) => {
            let Some(cid_size) = current_provider_quota_work_size(db, provider, work).await? else {
                return Ok(EvictionSnapshot {
                    selected: Vec::new(),
                    potential_pairs: Vec::new(),
                    waiter_owner_ids: Vec::new(),
                });
            };
            (cid_size, Some(work.cid), Vec::new())
        }
    };
    if cid_size < 0 {
        return Err(invalid_quota("CID size cannot be negative"));
    }
    if cid_size > limits.max_bytes {
        return Ok(EvictionSnapshot {
            selected: Vec::new(),
            potential_pairs: Vec::new(),
            waiter_owner_ids: Vec::new(),
        });
    }
    let Some(usage) = read_usage(db, provider).await? else {
        return Ok(EvictionSnapshot {
            selected: Vec::new(),
            potential_pairs: Vec::new(),
            waiter_owner_ids: Vec::new(),
        });
    };
    let mut projected = ProviderUsage {
        reserved_bytes: usage.reserved_bytes,
        reserved_pins: usage.reserved_pins,
    };
    let remotes = remote_pin::Entity::find()
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Status.is_in(capacity_holding_statuses()))
        .order_by_asc(remote_pin::Column::Cid)
        .all(db)
        .await?;
    let potential_pairs = remotes
        .iter()
        .map(|remote| (remote.provider.clone(), remote.cid.clone()))
        .collect();
    let mut candidates = Vec::with_capacity(remotes.len());
    for remote in remotes {
        match newest_active_target_touch(db, provider, &remote.cid).await? {
            Some(last_active_touch) if excluded_cid != Some(remote.cid.as_str()) => {
                candidates.push(EvictionCandidate {
                    provider: provider.to_owned(),
                    cid: remote.cid,
                    cid_size: remote.cid_size,
                    last_active_touch,
                });
            }
            Some(_) => {}
            None => {
                projected.reserved_bytes = projected
                    .reserved_bytes
                    .checked_sub(remote.cid_size)
                    .ok_or_else(|| invalid_quota("projected byte accounting underflow"))?;
                projected.reserved_pins = projected
                    .reserved_pins
                    .checked_sub(1)
                    .ok_or_else(|| invalid_quota("projected pin accounting underflow"))?;
                if projected.reserved_bytes < 0 || projected.reserved_pins < 0 {
                    return Err(invalid_quota("projected provider usage cannot be negative"));
                }
            }
        }
    }
    Ok(EvictionSnapshot {
        selected: select_eviction_candidates(projected, cid_size, limits, candidates)?,
        potential_pairs,
        waiter_owner_ids,
    })
}

/// Revalidates that a provider quota response belongs to live capacity-acquisition work.
async fn current_provider_quota_work_size<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    work: ProviderQuotaWork<'_>,
) -> AppResult<Option<i64>> {
    let Some(target) = pin_lease_target::Entity::find_by_id(work.target_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if target.lease_id != work.lease_id
        || target.provider != provider
        || target.cid != work.cid
        || !ACTIVE_TARGET_STATES.contains(&target.state.as_str())
    {
        return Ok(None);
    }
    let Some(lease) = pin_lease::Entity::find_by_id(work.lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if lease.state != ACTIVE_LEASE_STATE || lease.generation != work.expected_generation {
        return Ok(None);
    }
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), work.cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if remote.status != REMOTE_STATUS_RESERVED || remote.request_id.is_some() {
        return Ok(None);
    }
    Ok(Some(remote.cid_size))
}

/// Returns the exact FIFO waiters whose owning lease remains active.
async fn active_quota_waiters<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    now: DateTimeUtc,
) -> AppResult<Vec<(pin_lease_target::Model, pin_lease::Model)>> {
    let waiting = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::State.eq(TARGET_QUOTA_WAITING))
        .filter(pin_lease_target::Column::LastTouchedAt.lte(now))
        .order_by_asc(pin_lease_target::Column::CreatedAt)
        .order_by_asc(pin_lease_target::Column::Id)
        .all(db)
        .await?;
    let mut active = Vec::with_capacity(waiting.len());
    for target in waiting {
        let Some(lease) = pin_lease::Entity::find_by_id(target.lease_id.clone())
            .one(db)
            .await?
        else {
            continue;
        };
        if lease.state == ACTIVE_LEASE_STATE {
            active.push((target, lease));
        }
    }
    Ok(active)
}

/// Locks provider accounting after the lifecycle remote frontier.
async fn lock_eviction_usage_row<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<()> {
    let usage = read_usage(db, provider).await?.ok_or_else(|| {
        AppError::Internal(format!(
            "provider usage row disappeared during eviction prelock: {provider}"
        ))
    })?;
    if db.get_database_backend() == DatabaseBackend::Postgres {
        pin_provider_usage::Entity::find_by_id(provider.to_owned())
            .lock_exclusive()
            .one(db)
            .await?
            .ok_or_else(|| {
                AppError::Internal(format!(
                    "provider usage row disappeared during eviction prelock: {provider}"
                ))
            })?;
        return Ok(());
    }

    // SQLite has no row-level `FOR UPDATE`; this exact no-op CAS takes its serialized write
    // guard while preserving the same lifecycle → remote → usage order as PostgreSQL.
    let locked = pin_provider_usage::Entity::update_many()
        .col_expr(
            pin_provider_usage::Column::ReservedBytes,
            Expr::value(usage.reserved_bytes),
        )
        .col_expr(
            pin_provider_usage::Column::ReservedPins,
            Expr::value(usage.reserved_pins),
        )
        .filter(pin_provider_usage::Column::Provider.eq(provider))
        .filter(pin_provider_usage::Column::ReservedBytes.eq(usage.reserved_bytes))
        .filter(pin_provider_usage::Column::ReservedPins.eq(usage.reserved_pins))
        .exec(db)
        .await?;
    if locked.rows_affected != 1 {
        return Err(AppError::Internal(
            "provider usage changed during eviction prelock".to_owned(),
        ));
    }
    Ok(())
}

async fn newest_active_target_touch<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<DateTimeUtc>> {
    Ok(pin_lease_target::Entity::find()
        .inner_join(pin_lease::Entity)
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::Cid.eq(cid))
        .filter(pin_lease_target::Column::State.is_in(ACTIVE_TARGET_STATES))
        .filter(pin_lease::Column::State.eq(ACTIVE_LEASE_STATE))
        .order_by_desc(pin_lease_target::Column::LastTouchedAt)
        .order_by_asc(pin_lease_target::Column::Id)
        .one(db)
        .await?
        .map(|target| target.last_touched_at))
}

fn reserved_remote_model(
    provider: &str,
    cid: &str,
    cid_size: i64,
    epoch: i64,
    now: DateTimeUtc,
) -> remote_pin::ActiveModel {
    remote_pin::ActiveModel {
        provider: Set(provider.to_owned()),
        cid: Set(cid.to_owned()),
        request_id: Set(None),
        cid_size: Set(cid_size),
        status: Set(REMOTE_STATUS_RESERVED.to_owned()),
        epoch: Set(epoch),
        failure_attempts: Set(0),
        next_retry_at: Set(None),
        last_failed_request_id: Set(None),
        last_touched_at: Set(now),
        last_error_class: Set(None),
        last_error_text: Set(None),
    }
}

fn provider_limits<'a>(
    limits: &'a ProviderLimitMap,
    provider: &str,
) -> AppResult<&'a ProviderLimits> {
    limits
        .get(provider)
        .ok_or_else(|| invalid_quota("provider does not have validated limits"))
}

fn next_epoch(epoch: i64) -> AppResult<i64> {
    epoch
        .checked_add(1)
        .ok_or_else(|| invalid_quota("remote epoch overflow"))
}

fn is_capacity_holding_status(status: &str) -> bool {
    capacity_holding_statuses().contains(&status)
}

fn capacity_holding_statuses() -> [&'static str; 5] {
    [
        REMOTE_STATUS_RESERVED,
        REMOTE_STATUS_QUEUED,
        REMOTE_STATUS_PINNING,
        REMOTE_STATUS_PINNED,
        REMOTE_STATUS_FAILED,
    ]
}

fn is_sqlite_contention(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("database is locked") || message.contains("database is busy")
}

async fn retry_delay(attempt: usize) {
    let milliseconds = 1_u64.checked_shl(attempt.min(4) as u32).unwrap_or(16);
    tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
}

fn invalid_quota(message: &str) -> AppError {
    AppError::InvalidPinningRequest(message.to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{Duration, TimeZone, Utc};
    use sea_orm::{
        ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend,
        DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryTrait, TransactionTrait,
        sea_query::Expr,
    };
    use tokio::sync::Barrier;

    use super::{
        CapacityReservation, ConfirmedReleaseOutcome, EVICTION_DRIFT, EVICTION_DRIFT_TEST_LOCK,
        EvictionDrift, ProviderQuotaWork, ReservationOutcome, ReserveAttempt, confirmed_release,
        evict_for_provider_quota, evict_for_provider_waiter, existing_remote_lock_query,
        order_events, ordered_unique_providers, read_usage, refresh_remote_max_active_touch,
        reserve_capacity, reserve_new_remote, reserve_unique, reserve_unique_attempt,
        undo_capacity_grant, wake_provider_waiters,
    };
    use crate::{
        pinning::{
            config::{ProviderLimitMap, ProviderLimits},
            quota::{CapacityDecision, ProviderUsage, reservation_decision},
        },
        store::{
            entities::{pin_job, pin_lease_target, remote_pin},
            pinning::leases::{self, RemoteDeleteCompletion},
        },
    };

    fn time(seconds: i64) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 21, 0, 0, 0).single().unwrap() + Duration::seconds(seconds)
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

    #[test]
    fn publication_usage_prelock_normalizes_provider_order() {
        assert_eq!(
            ordered_unique_providers(&[
                "pinata".to_owned(),
                "filebase".to_owned(),
                "pinata".to_owned(),
            ]),
            vec!["filebase".to_owned(), "pinata".to_owned()]
        );
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
            "INSERT INTO objects (id, bucket, key, cid, size, etag) \
             VALUES ('object-1', 'bucket', 'key', 'QmObject', 7, 'QmObject')",
        )
        .await
        .unwrap();
        db
    }

    async fn usage(db: &DatabaseConnection) -> (i64, i64) {
        let usage = read_usage(db, "pinata").await.unwrap().unwrap();
        (usage.reserved_bytes, usage.reserved_pins)
    }

    async fn seed_active_target(
        db: &DatabaseConnection,
        cid: &str,
        target_id: &str,
        touched_at: chrono::DateTime<Utc>,
    ) {
        let timestamp = touched_at.to_rfc3339();
        let lease_id = format!("lease-{target_id}");
        db.execute_unprepared(&format!(
            "INSERT INTO pin_leases \
             (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
              last_touched_at, expires_at, generation, state) \
             VALUES ('{lease_id}', 'object-1', 'source-{target_id}', 'policy', 'all', 'object', \
                     '{timestamp}', '{timestamp}', '{timestamp}', 1, 'active')"
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('{target_id}', '{lease_id}', '{cid}', 1, 'pinata', 'waiting', \
                     '{timestamp}', '{timestamp}')"
        ))
        .await
        .unwrap();
    }

    async fn seed_quota_waiter(
        db: &DatabaseConnection,
        cid: &str,
        target_id: &str,
        logical_size: i64,
        created_at: chrono::DateTime<Utc>,
    ) {
        let timestamp = created_at.to_rfc3339();
        let expires_at = (created_at + Duration::hours(1)).to_rfc3339();
        let lease_id = format!("lease-{target_id}");
        db.execute_unprepared(&format!(
            "INSERT INTO pin_leases \
             (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
              last_touched_at, expires_at, generation, state) \
             VALUES ('{lease_id}', 'object-1', 'source-{target_id}', 'policy', 'all', 'object', \
                     '{timestamp}', '{timestamp}', '{expires_at}', 1, 'active'); \
             INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('{target_id}', '{lease_id}', '{cid}', {logical_size}, 'pinata', \
                     'quota_waiting', '{timestamp}', '{timestamp}')"
        ))
        .await
        .unwrap();
    }

    #[test]
    fn pure_decisions_reject_negative_and_overflowing_accounting() {
        let limit = ProviderLimits {
            priority: 1,
            max_bytes: i64::MAX,
            max_pins: i64::MAX,
            enabled: true,
        };

        assert!(
            reservation_decision(
                ProviderUsage {
                    reserved_bytes: -1,
                    reserved_pins: 0,
                },
                1,
                &limit,
            )
            .is_err()
        );
        assert!(
            reservation_decision(
                ProviderUsage {
                    reserved_bytes: i64::MAX,
                    reserved_pins: 0,
                },
                1,
                &limit,
            )
            .is_err()
        );
        assert_eq!(
            reservation_decision(
                ProviderUsage {
                    reserved_bytes: 0,
                    reserved_pins: 0,
                },
                i64::MAX,
                &limit,
            )
            .unwrap(),
            CapacityDecision::Reserve
        );
    }

    #[test]
    fn postgres_existing_remote_acquisition_renders_for_update() {
        let sql = existing_remote_lock_query("pinata", "bafy-lock")
            .build(DatabaseBackend::Postgres)
            .to_string();

        assert!(sql.starts_with("SELECT \"remote_pins\".\"provider\""));
        assert!(sql.contains("FROM \"remote_pins\""));
        assert!(sql.contains("\"remote_pins\".\"provider\" = 'pinata'"));
        assert!(sql.contains("\"remote_pins\".\"cid\" = 'bafy-lock'"));
        assert!(sql.ends_with("FOR UPDATE"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn existing_absent_acquires_remote_before_usage_then_cas() {
        let db = setup().await;
        let limits = limits(1_000, 10);
        reserve_unique(&db, "pinata", "bafy-order-absent", 10, &limits, time(1))
            .await
            .unwrap();
        confirmed_release(&db, "pinata", "bafy-order-absent", 1, time(2))
            .await
            .unwrap();
        order_events::clear();

        assert!(matches!(
            reserve_unique_attempt(&db, "pinata", "bafy-order-absent", 10, &limits, time(3),)
                .await
                .unwrap(),
            ReserveAttempt::Outcome(ReservationOutcome::Reserved)
        ));
        assert_eq!(
            order_events::take(),
            vec![
                "remote_acquire",
                "usage_ensure",
                "usage_update",
                "remote_absent_cas",
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capacity_holding_existing_acquires_remote_before_remote_only_cas() {
        let db = setup().await;
        let limits = limits(1_000, 10);
        reserve_unique(&db, "pinata", "bafy-order-held", 10, &limits, time(1))
            .await
            .unwrap();
        order_events::clear();

        assert!(matches!(
            reserve_unique_attempt(&db, "pinata", "bafy-order-held", 10, &limits, time(2),)
                .await
                .unwrap(),
            ReserveAttempt::Outcome(ReservationOutcome::Reused)
        ));
        assert_eq!(
            order_events::take(),
            vec!["remote_acquire", "remote_reuse_cas"]
        );
    }

    #[tokio::test]
    async fn shared_sequential_reuse_bumps_epoch_once_without_a_second_charge() {
        let db = setup().await;
        let limits = limits(1_000, 10);

        assert_eq!(
            reserve_unique(&db, "pinata", "bafy-shared", 100, &limits, time(1))
                .await
                .unwrap(),
            ReservationOutcome::Reserved
        );
        let initial =
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-shared".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert_eq!((initial.status.as_str(), initial.epoch), ("reserved", 1));
        assert_eq!(initial.request_id, None);
        assert_eq!(initial.failure_attempts, 0);
        assert_eq!(initial.next_retry_at, None);
        assert_eq!(initial.last_failed_request_id, None);
        assert_eq!(initial.last_error_class, None);
        assert_eq!(initial.last_error_text, None);
        assert_eq!(
            reserve_unique(&db, "pinata", "bafy-shared", 100, &limits, time(2))
                .await
                .unwrap(),
            ReservationOutcome::Reused
        );

        assert_eq!(usage(&db).await, (100, 1));
        let remote =
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-shared".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(remote.epoch, 2);
        assert_eq!(remote.status, "reserved");
        assert_eq!(remote.last_touched_at, time(2));
    }

    #[tokio::test]
    async fn every_capacity_holding_status_reuses_without_a_second_charge() {
        let db = setup().await;
        let limits = limits(1_000, 10);
        reserve_unique(&db, "pinata", "bafy-capacity", 100, &limits, time(1))
            .await
            .unwrap();

        for status in ["reserved", "queued", "pinning", "pinned", "failed"] {
            remote_pin::Entity::update_many()
                .col_expr(remote_pin::Column::Status, Expr::value(status.to_owned()))
                .filter(remote_pin::Column::Provider.eq("pinata"))
                .filter(remote_pin::Column::Cid.eq("bafy-capacity"))
                .exec(&db)
                .await
                .unwrap();
            assert_eq!(
                reserve_unique(&db, "pinata", "bafy-capacity", 100, &limits, time(2))
                    .await
                    .unwrap(),
                ReservationOutcome::Reused,
                "{status} must retain unique capacity"
            );
        }

        assert_eq!(usage(&db).await, (100, 1));
        assert_eq!(
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-capacity".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .epoch,
            6
        );
    }

    #[tokio::test]
    async fn absent_remote_reacquires_once_and_clears_stale_identity() {
        let db = setup().await;
        let limits = limits(1_000, 10);
        reserve_unique(&db, "pinata", "bafy-absent", 100, &limits, time(1))
            .await
            .unwrap();

        assert_eq!(
            confirmed_release(&db, "pinata", "bafy-absent", 1, time(2))
                .await
                .unwrap(),
            ConfirmedReleaseOutcome::Released
        );
        db.execute_unprepared(
            "UPDATE remote_pins SET request_id = 'old-request', failure_attempts = 2, \
             next_retry_at = '2026-07-21T00:00:09+00:00', \
             last_failed_request_id = 'old-request', last_error_class = 'transient', \
             last_error_text = 'old error' WHERE provider = 'pinata' AND cid = 'bafy-absent'",
        )
        .await
        .unwrap();
        assert_eq!(
            reserve_unique(&db, "pinata", "bafy-absent", 11, &limits, time(3))
                .await
                .unwrap(),
            ReservationOutcome::Reserved
        );

        assert_eq!(usage(&db).await, (11, 1));
        let remote =
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-absent".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            (remote.status.as_str(), remote.epoch, remote.cid_size),
            ("reserved", 2, 11)
        );
        assert_eq!(remote.request_id, None);
        assert_eq!(remote.failure_attempts, 0);
        assert_eq!(remote.next_retry_at, None);
        assert_eq!(remote.last_failed_request_id, None);
        assert_eq!(remote.last_error_class, None);
        assert_eq!(remote.last_error_text, None);
    }

    #[tokio::test]
    async fn oversize_is_blocked_without_creating_a_usage_or_remote_row() {
        let db = setup().await;
        let limits = limits(10, 10);

        assert!(matches!(
            reserve_unique(&db, "pinata", "bafy-negative", -1, &limits, time(1)).await,
            Err(crate::error::AppError::InvalidPinningRequest(_))
        ));
        assert_eq!(
            reserve_unique(&db, "pinata", "bafy-oversize", 11, &limits, time(1))
                .await
                .unwrap(),
            ReservationOutcome::QuotaBlocked
        );
        assert!(read_usage(&db, "pinata").await.unwrap().is_none());
        assert!(
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-oversize".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn insufficient_capacity_returns_oldest_active_touch_without_mutation() {
        let db = setup().await;
        let limits = limits(100, 3);
        reserve_unique(&db, "pinata", "bafy-old", 40, &limits, time(20))
            .await
            .unwrap();
        reserve_unique(&db, "pinata", "bafy-shared", 40, &limits, time(20))
            .await
            .unwrap();
        seed_active_target(&db, "bafy-old", "target-old", time(1)).await;
        seed_active_target(&db, "bafy-shared", "target-shared-old", time(2)).await;
        seed_active_target(&db, "bafy-shared", "target-shared-new", time(10)).await;
        assert_eq!(
            refresh_remote_max_active_touch(&db, "pinata", "bafy-old")
                .await
                .unwrap(),
            Some(time(1))
        );
        assert_eq!(
            refresh_remote_max_active_touch(&db, "pinata", "bafy-shared")
                .await
                .unwrap(),
            Some(time(10))
        );

        assert_eq!(
            reserve_unique(&db, "pinata", "bafy-incoming", 30, &limits, time(30))
                .await
                .unwrap(),
            ReservationOutcome::QuotaWaiting {
                evict: vec![("pinata".to_owned(), "bafy-old".to_owned())]
            }
        );
        assert_eq!(usage(&db).await, (80, 2));
        assert!(
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-incoming".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn eviction_marks_oldest_then_confirmed_release_wakes_and_projects_fifo_waiter() {
        let db = setup().await;
        let limits = limits(100, 2);
        reserve_unique(&db, "pinata", "bafy-old", 40, &limits, time(1))
            .await
            .unwrap();
        reserve_unique(&db, "pinata", "bafy-new", 40, &limits, time(2))
            .await
            .unwrap();
        seed_active_target(&db, "bafy-old", "target-old", time(1)).await;
        seed_active_target(&db, "bafy-new", "target-new", time(2)).await;
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state='pinned'; \
             UPDATE remote_pins SET status='pinned', request_id='request-old' \
             WHERE provider='pinata' AND cid='bafy-old'; \
             UPDATE remote_pins SET status='pinned', request_id='request-new' \
             WHERE provider='pinata' AND cid='bafy-new'",
        )
        .await
        .unwrap();
        refresh_remote_max_active_touch(&db, "pinata", "bafy-old")
            .await
            .unwrap();
        refresh_remote_max_active_touch(&db, "pinata", "bafy-new")
            .await
            .unwrap();
        seed_quota_waiter(&db, "bafy-incoming", "target-incoming", 40, time(3)).await;

        let txn = db.begin().await.unwrap();
        let evicted = evict_for_provider_waiter(&txn, "pinata", &limits["pinata"], time(4))
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].cid, "bafy-old");
        assert_eq!(usage(&db).await, (80, 2));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-old".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .state,
            "evicted"
        );
        let cleanup = pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq("pinata"))
            .filter(pin_job::Column::Cid.eq("bafy-old"))
            .filter(pin_job::Column::Operation.eq("unpin"))
            .one(&db)
            .await
            .unwrap()
            .expect("oldest eviction must enqueue one exact-epoch Unpin");

        let txn = db.begin().await.unwrap();
        assert_eq!(
            leases::complete_remote_delete(
                &txn,
                "pinata",
                "bafy-old",
                cleanup.expected_remote_epoch.unwrap(),
                time(5),
            )
            .await
            .unwrap(),
            RemoteDeleteCompletion::Released
        );
        let woken = wake_provider_waiters(&txn, "pinata", &limits["pinata"], time(5))
            .await
            .unwrap();
        let retried = leases::requeue_released_all_quota_targets(
            &txn,
            "pinata",
            Duration::seconds(10),
            time(5),
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        assert_eq!(woken, vec!["target-incoming".to_owned()]);
        assert_eq!(retried, vec!["target-old".to_owned()]);
        assert_eq!(usage(&db).await, (80, 2));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-incoming".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .state,
            "waiting"
        );
        let submit = pin_job::Entity::find()
            .filter(pin_job::Column::TargetId.eq("target-incoming"))
            .filter(pin_job::Column::Operation.eq("submit"))
            .one(&db)
            .await
            .unwrap()
            .expect("woken waiter must pass through canonical remote projection");
        assert_eq!(submit.state, "pending");

        assert!(
            wake_provider_waiters(&db, "pinata", &limits["pinata"], time(14))
                .await
                .unwrap()
                .is_empty(),
            "the evicted all-mode target must respect one worker interval"
        );
        let txn = db.begin().await.unwrap();
        let evicted = evict_for_provider_waiter(&txn, "pinata", &limits["pinata"], time(16))
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert!(
            evicted.is_empty(),
            "an all-mode retry may wait for headroom but must not evict the new winner"
        );
        assert_eq!(usage(&db).await, (80, 2));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-old".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .state,
            "quota_waiting"
        );
    }

    #[tokio::test]
    async fn eviction_marks_enough_oldest_cids_once_and_projects_pending_releases() {
        let db = setup().await;
        let limits = limits(100, 3);
        for (cid, target, touched) in [
            ("bafy-a", "target-a", time(1)),
            ("bafy-b", "target-b", time(2)),
            ("bafy-c", "target-c", time(3)),
        ] {
            reserve_unique(&db, "pinata", cid, 30, &limits, touched)
                .await
                .unwrap();
            seed_active_target(&db, cid, target, touched).await;
        }
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state='pinned' \
              WHERE id IN ('target-a', 'target-b', 'target-c'); \
              UPDATE remote_pins SET status='pinned', request_id='request-' || cid \
              WHERE cid IN ('bafy-a', 'bafy-b', 'bafy-c')",
        )
        .await
        .unwrap();
        for cid in ["bafy-a", "bafy-b", "bafy-c"] {
            refresh_remote_max_active_touch(&db, "pinata", cid)
                .await
                .unwrap();
        }
        seed_quota_waiter(
            &db,
            "bafy-incoming-large",
            "target-incoming-large",
            70,
            time(4),
        )
        .await;

        let txn = db.begin().await.unwrap();
        let evicted = evict_for_provider_waiter(&txn, "pinata", &limits["pinata"], time(5))
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(
            evicted
                .iter()
                .map(|target| target.cid.as_str())
                .collect::<Vec<_>>(),
            vec!["bafy-a", "bafy-b"]
        );
        assert_eq!(usage(&db).await, (90, 3));
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Provider.eq("pinata"))
                .filter(pin_job::Column::Operation.eq("unpin"))
                .count(&db)
                .await
                .unwrap(),
            2
        );

        let txn = db.begin().await.unwrap();
        assert!(
            evict_for_provider_waiter(&txn, "pinata", &limits["pinata"], time(6))
                .await
                .unwrap()
                .is_empty(),
            "already pending confirmed releases must prevent over-eviction"
        );
        txn.commit().await.unwrap();
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-c".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned"
        );
        assert_eq!(usage(&db).await, (90, 3));
    }

    #[tokio::test]
    async fn automatic_and_manual_lease_sources_are_both_oldest_evictable_reservations() {
        let db = setup().await;
        let limits = limits(100, 3);
        for (cid, target, touched) in [
            ("bafy-automatic", "target-automatic", time(1)),
            ("bafy-manual", "target-manual", time(2)),
        ] {
            reserve_unique(&db, "pinata", cid, 30, &limits, touched)
                .await
                .unwrap();
            seed_active_target(&db, cid, target, touched).await;
        }
        db.execute_unprepared(
            "UPDATE pin_leases SET source='automatic' WHERE id='lease-target-automatic'; \
             UPDATE pin_leases SET source='manual' WHERE id='lease-target-manual'; \
             UPDATE pin_lease_targets SET state='pinned'; \
             UPDATE remote_pins SET status='pinned', request_id='request-' || cid",
        )
        .await
        .unwrap();
        for cid in ["bafy-automatic", "bafy-manual"] {
            refresh_remote_max_active_touch(&db, "pinata", cid)
                .await
                .unwrap();
        }
        seed_quota_waiter(
            &db,
            "bafy-needs-both-sources",
            "target-needs-both-sources",
            90,
            time(3),
        )
        .await;

        let txn = db.begin().await.unwrap();
        let evicted = evict_for_provider_waiter(&txn, "pinata", &limits["pinata"], time(4))
            .await
            .unwrap();
        txn.commit().await.unwrap();

        assert_eq!(
            evicted
                .iter()
                .map(|target| target.cid.as_str())
                .collect::<Vec<_>>(),
            vec!["bafy-automatic", "bafy-manual"]
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::State.eq("evicted"))
                .count(&db)
                .await
                .unwrap(),
            2
        );
        assert_eq!(usage(&db).await, (60, 2));
    }

    #[tokio::test]
    async fn eviction_insufficient_candidates_leave_every_lifecycle_row_unchanged() {
        let db = setup().await;
        let limits = limits(100, 2);
        reserve_unique(&db, "pinata", "bafy-quota-current", 80, &limits, time(1))
            .await
            .unwrap();
        reserve_unique(&db, "pinata", "bafy-too-small", 10, &limits, time(2))
            .await
            .unwrap();
        seed_active_target(&db, "bafy-quota-current", "target-quota-current", time(2)).await;
        seed_active_target(&db, "bafy-too-small", "target-too-small", time(1)).await;
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state='pinned' \
             WHERE id IN ('target-headroom-old', 'target-headroom-release'); \
             UPDATE remote_pins SET status='pinned', request_id='request-' || cid \
             WHERE cid IN ('bafy-headroom-old', 'bafy-headroom-release')",
        )
        .await
        .unwrap();
        for cid in ["bafy-quota-current", "bafy-too-small"] {
            refresh_remote_max_active_touch(&db, "pinata", cid)
                .await
                .unwrap();
        }
        let before_target = pin_lease_target::Entity::find_by_id("target-too-small".to_owned())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let before_remote =
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-too-small".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();

        let txn = db.begin().await.unwrap();
        let evicted = evict_for_provider_quota(
            &txn,
            "pinata",
            ProviderQuotaWork {
                cid: "bafy-quota-current",
                lease_id: "lease-target-quota-current",
                target_id: "target-quota-current",
                expected_generation: 1,
            },
            &limits["pinata"],
            time(3),
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        assert!(evicted.is_empty());
        assert_eq!(usage(&db).await, (90, 2));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-too-small".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            before_target
        );
        assert_eq!(
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-too-small".to_owned(),))
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            before_remote
        );
        assert_eq!(pin_job::Entity::find().count(&db).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn waiter_cancelled_after_selection_prevents_stale_destructive_eviction() {
        let _gate = EVICTION_DRIFT_TEST_LOCK.lock().await;
        let db = setup().await;
        let limits = limits(100, 2);
        reserve_unique(&db, "pinata", "bafy-cancel-candidate", 80, &limits, time(1))
            .await
            .unwrap();
        seed_active_target(
            &db,
            "bafy-cancel-candidate",
            "target-cancel-candidate",
            time(1),
        )
        .await;
        seed_quota_waiter(
            &db,
            "bafy-cancelled-waiter",
            "target-cancelled-waiter",
            80,
            time(2),
        )
        .await;
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state='pinned' \
             WHERE id='target-cancel-candidate'; \
             UPDATE remote_pins SET status='pinned', request_id='request-cancel-candidate' \
             WHERE provider='pinata' AND cid='bafy-cancel-candidate'",
        )
        .await
        .unwrap();
        *EVICTION_DRIFT.lock().await = Some(EvictionDrift::CancelWaiter {
            selected_cid: "bafy-cancel-candidate",
            target_id: "target-cancelled-waiter",
            lease_id: "lease-target-cancelled-waiter",
        });

        let txn = db.begin().await.unwrap();
        let evicted = evict_for_provider_waiter(&txn, "pinata", &limits["pinata"], time(3))
            .await
            .unwrap();
        txn.commit().await.unwrap();

        assert!(evicted.is_empty());
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-cancel-candidate".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned"
        );
    }

    #[tokio::test]
    async fn candidate_touch_after_selection_recomputes_the_oldest_locked_prefix() {
        let _gate = EVICTION_DRIFT_TEST_LOCK.lock().await;
        let db = setup().await;
        let limits = limits(100, 3);
        for (cid, target, touched) in [
            ("bafy-touch-old", "target-touch-old", time(1)),
            ("bafy-touch-next", "target-touch-next", time(2)),
        ] {
            reserve_unique(&db, "pinata", cid, 40, &limits, touched)
                .await
                .unwrap();
            seed_active_target(&db, cid, target, touched).await;
        }
        seed_quota_waiter(&db, "bafy-touch-waiter", "target-touch-waiter", 40, time(3)).await;
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state='pinned' \
             WHERE id IN ('target-touch-old', 'target-touch-next'); \
             UPDATE remote_pins SET status='pinned', request_id='request-' || cid \
             WHERE cid IN ('bafy-touch-old', 'bafy-touch-next')",
        )
        .await
        .unwrap();
        *EVICTION_DRIFT.lock().await = Some(EvictionDrift::TouchCandidate {
            selected_cid: "bafy-touch-old",
            target_id: "target-touch-old",
            lease_id: "lease-target-touch-old",
            touched_at: time(10),
        });

        let txn = db.begin().await.unwrap();
        let evicted = evict_for_provider_waiter(&txn, "pinata", &limits["pinata"], time(11))
            .await
            .unwrap();
        txn.commit().await.unwrap();

        assert_eq!(
            evicted
                .iter()
                .map(|target| target.cid.as_str())
                .collect::<Vec<_>>(),
            vec!["bafy-touch-next"]
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-touch-old".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned"
        );
    }

    #[tokio::test]
    async fn concurrent_headroom_release_after_selection_avoids_unneeded_eviction() {
        let _gate = EVICTION_DRIFT_TEST_LOCK.lock().await;
        let db = setup().await;
        let limits = limits(100, 3);
        for (cid, target, touched) in [
            ("bafy-headroom-old", "target-headroom-old", time(1)),
            ("bafy-headroom-release", "target-headroom-release", time(2)),
        ] {
            reserve_unique(&db, "pinata", cid, 40, &limits, touched)
                .await
                .unwrap();
            seed_active_target(&db, cid, target, touched).await;
        }
        seed_quota_waiter(
            &db,
            "bafy-headroom-waiter",
            "target-headroom-waiter",
            40,
            time(3),
        )
        .await;
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state='pinned' \
             WHERE id IN ('target-headroom-old', 'target-headroom-release'); \
             UPDATE remote_pins SET status='pinned', request_id='request-' || cid \
             WHERE cid IN ('bafy-headroom-old', 'bafy-headroom-release')",
        )
        .await
        .unwrap();
        *EVICTION_DRIFT.lock().await = Some(EvictionDrift::ReleaseHeadroom {
            selected_cid: "bafy-headroom-old",
            cid: "bafy-headroom-release",
            target_id: "target-headroom-release",
            lease_id: "lease-target-headroom-release",
            cid_size: 40,
        });

        let txn = db.begin().await.unwrap();
        let evicted = evict_for_provider_waiter(&txn, "pinata", &limits["pinata"], time(4))
            .await
            .unwrap();
        txn.commit().await.unwrap();

        assert!(evicted.is_empty());
        assert_eq!(usage(&db).await, (40, 1));
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-headroom-old".to_owned())
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .state,
            "pinned"
        );
    }

    #[tokio::test]
    async fn confirmed_release_is_epoch_guarded_and_idempotent() {
        let db = setup().await;
        let limits = limits(100, 1);
        reserve_unique(&db, "pinata", "bafy-release", 10, &limits, time(1))
            .await
            .unwrap();

        assert_eq!(
            confirmed_release(&db, "pinata", "bafy-release", 2, time(2))
                .await
                .unwrap(),
            ConfirmedReleaseOutcome::Stale
        );
        assert_eq!(usage(&db).await, (10, 1));
        assert_eq!(
            confirmed_release(&db, "pinata", "bafy-release", 1, time(2))
                .await
                .unwrap(),
            ConfirmedReleaseOutcome::Released
        );
        assert_eq!(usage(&db).await, (0, 0));
        assert_eq!(
            confirmed_release(&db, "pinata", "bafy-release", 1, time(3))
                .await
                .unwrap(),
            ConfirmedReleaseOutcome::AlreadyAbsent
        );
        assert_eq!(usage(&db).await, (0, 0));
    }

    #[tokio::test]
    async fn concurrent_file_backed_sqlite_reservation_charges_one_shared_cid_once() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("quota-reservation.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        let db = Database::connect(options).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        let limits = limits(1_000, 10);

        let (first, second) = tokio::join!(
            reserve_unique(&db, "pinata", "bafy-concurrent", 100, &limits, time(1)),
            reserve_unique(&db, "pinata", "bafy-concurrent", 100, &limits, time(1)),
        );
        assert!(matches!(
            first.unwrap(),
            ReservationOutcome::Reserved | ReservationOutcome::Reused
        ));
        assert!(matches!(
            second.unwrap(),
            ReservationOutcome::Reserved | ReservationOutcome::Reused
        ));
        assert_eq!(usage(&db).await, (100, 1));
        assert_eq!(
            remote_pin::Entity::find()
                .filter(remote_pin::Column::Provider.eq("pinata"))
                .filter(remote_pin::Column::Cid.eq("bafy-concurrent"))
                .count(&db)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_different_cids_near_capacity_leave_no_unpaired_remote_row() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("quota-near-capacity.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        let db = Database::connect(options).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        let limits = limits(100, 2);

        let (first, second) = tokio::join!(
            reserve_unique(&db, "pinata", "bafy-near-a", 60, &limits, time(1)),
            reserve_unique(&db, "pinata", "bafy-near-b", 60, &limits, time(1)),
        );
        let outcomes = [first.unwrap(), second.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ReservationOutcome::Reserved))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ReservationOutcome::QuotaWaiting { .. }))
                .count(),
            1
        );
        assert_eq!(usage(&db).await, (60, 1));
        assert_eq!(remote_pin::Entity::find().count(&db).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn concurrent_fifo_wakes_never_overcommit_released_headroom() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("quota-concurrent-wake.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        let db = Database::connect(options).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared(
            "INSERT INTO buckets (name) VALUES ('bucket'); \
             INSERT INTO objects (id, bucket, key, cid, size, etag) \
             VALUES ('object-1', 'bucket', 'key', 'QmObject', 7, 'QmObject')",
        )
        .await
        .unwrap();
        seed_quota_waiter(&db, "bafy-wake-a", "wake-a", 60, time(1)).await;
        seed_quota_waiter(&db, "bafy-wake-b", "wake-b", 60, time(2)).await;
        let provider_limits = limits(100, 2)["pinata"].clone();
        let barrier = Arc::new(Barrier::new(2));

        let wake = |db: DatabaseConnection, barrier: Arc<Barrier>| {
            let provider_limits = provider_limits.clone();
            async move {
                let txn = db.begin().await.unwrap();
                barrier.wait().await;
                match wake_provider_waiters(&txn, "pinata", &provider_limits, time(3)).await {
                    Ok(woken) => {
                        txn.commit().await.unwrap();
                        Ok(woken)
                    }
                    Err(error) => {
                        txn.rollback().await.unwrap();
                        Err(error)
                    }
                }
            }
        };
        let (first, second) =
            tokio::join!(wake(db.clone(), barrier.clone()), wake(db.clone(), barrier));
        assert!(first.is_ok() || second.is_ok());

        assert_eq!(usage(&db).await, (60, 1));
        let targets = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::Provider.eq("pinata"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(
            targets
                .iter()
                .filter(|target| target.state == "waiting")
                .count(),
            1
        );
        assert_eq!(
            targets
                .iter()
                .filter(|target| target.state == "quota_waiting")
                .count(),
            1
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("submit"))
                .count(&db)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_confirmed_releases_and_fifo_wakes_never_overcommit() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory
            .path()
            .join("quota-concurrent-release-wake.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        let db = Database::connect(options).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared(
            "INSERT INTO buckets (name) VALUES ('bucket'); \
             INSERT INTO objects (id, bucket, key, cid, size, etag) \
             VALUES ('object-1', 'bucket', 'key', 'QmObject', 7, 'QmObject')",
        )
        .await
        .unwrap();
        let provider_limits = limits(100, 3)["pinata"].clone();
        let provider_limit_map = limits(100, 3);
        for (cid, target, touched) in [
            ("bafy-release-a", "target-release-a", time(1)),
            ("bafy-release-b", "target-release-b", time(2)),
        ] {
            reserve_unique(&db, "pinata", cid, 40, &provider_limit_map, touched)
                .await
                .unwrap();
            seed_active_target(&db, cid, target, touched).await;
        }
        db.execute_unprepared(
            "UPDATE pin_leases SET state='cancelled' WHERE id IN \
                 ('lease-target-release-a', 'lease-target-release-b'); \
             UPDATE pin_lease_targets SET state='evicted' WHERE id IN \
                 ('target-release-a', 'target-release-b'); \
             UPDATE remote_pins SET status='pinned', request_id='request-' || cid \
                 WHERE cid IN ('bafy-release-a', 'bafy-release-b')",
        )
        .await
        .unwrap();
        seed_quota_waiter(&db, "bafy-release-wake-a", "release-wake-a", 60, time(3)).await;
        seed_quota_waiter(&db, "bafy-release-wake-b", "release-wake-b", 60, time(4)).await;
        let barrier = Arc::new(Barrier::new(2));

        let release_and_wake =
            |db: DatabaseConnection, cid: &'static str, barrier: Arc<Barrier>| {
                let provider_limits = provider_limits.clone();
                async move {
                    for attempt in 0..8 {
                        let txn = db.begin().await.unwrap();
                        if attempt == 0 {
                            barrier.wait().await;
                        }
                        let result = async {
                            assert_eq!(
                                leases::complete_remote_delete(&txn, "pinata", cid, 1, time(5),)
                                    .await?,
                                RemoteDeleteCompletion::Released
                            );
                            let woken =
                                wake_provider_waiters(&txn, "pinata", &provider_limits, time(5))
                                    .await?;
                            Ok::<_, crate::error::AppError>(woken)
                        }
                        .await;
                        match result {
                            Ok(woken) => {
                                txn.commit().await.unwrap();
                                return woken;
                            }
                            Err(error) if super::is_sqlite_contention(&error.to_string()) => {
                                txn.rollback().await.unwrap();
                                super::retry_delay(attempt).await;
                            }
                            Err(error) => {
                                txn.rollback().await.unwrap();
                                panic!("release+wake failed: {error}");
                            }
                        }
                    }
                    panic!("release+wake exhausted SQLite retries")
                }
            };
        let (first, second) = tokio::join!(
            release_and_wake(db.clone(), "bafy-release-a", barrier.clone()),
            release_and_wake(db.clone(), "bafy-release-b", barrier),
        );

        assert_eq!(first.len() + second.len(), 1);
        assert_eq!(usage(&db).await, (60, 1));
        let targets = pin_lease_target::Entity::find()
            .filter(
                pin_lease_target::Column::Id
                    .is_in(["release-wake-a".to_owned(), "release-wake-b".to_owned()]),
            )
            .all(&db)
            .await
            .unwrap();
        assert_eq!(
            targets
                .iter()
                .filter(|target| target.state == "waiting")
                .count(),
            1
        );
        assert_eq!(
            targets
                .iter()
                .filter(|target| target.state == "quota_waiting")
                .count(),
            1
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("submit"))
                .count(&db)
                .await
                .unwrap(),
            1
        );

        let txn = db.begin().await.unwrap();
        assert_eq!(
            leases::complete_remote_delete(&txn, "pinata", "bafy-release-a", 1, time(6),)
                .await
                .unwrap(),
            RemoteDeleteCompletion::Released
        );
        assert!(
            wake_provider_waiters(&txn, "pinata", &provider_limits, time(6))
                .await
                .unwrap()
                .is_empty()
        );
        txn.commit().await.unwrap();
        assert_eq!(usage(&db).await, (60, 1));
    }

    #[tokio::test]
    async fn retry_cleanup_restores_a_tentative_capacity_grant_before_retrying() {
        let db = setup().await;
        let limits = limits(100, 10);
        let CapacityReservation::Reserved(grant) =
            reserve_capacity(&db, "pinata", 60, &limits["pinata"])
                .await
                .unwrap()
        else {
            panic!("fixture capacity grant must succeed");
        };
        assert_eq!(usage(&db).await, (60, 1));

        undo_capacity_grant(&db, "pinata", grant).await.unwrap();

        assert_eq!(usage(&db).await, (0, 0));
    }

    #[tokio::test]
    async fn insert_conflict_after_capacity_grant_undoes_before_returning_retry() {
        let db = setup().await;
        let limits = limits(100, 10);
        reserve_unique(&db, "pinata", "bafy-conflict", 10, &limits, time(1))
            .await
            .unwrap();
        confirmed_release(&db, "pinata", "bafy-conflict", 1, time(2))
            .await
            .unwrap();
        let before =
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-conflict".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();

        assert!(matches!(
            reserve_new_remote(
                &db,
                "pinata",
                "bafy-conflict",
                60,
                &limits["pinata"],
                time(3)
            )
            .await
            .unwrap(),
            ReserveAttempt::Retry
        ));

        assert_eq!(usage(&db).await, (0, 0));
        assert_eq!(
            remote_pin::Entity::find_by_id(("pinata".to_owned(), "bafy-conflict".to_owned()))
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            before
        );
    }
}
