use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Duration,
};

use super::management::{
    ManagementError,
    actions::job_authority::safe as management_error,
    records::{self, DecodeWork, FrozenRows, RowView, SharedDecodeBudget},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sea_orm::sea_query::{Condition, Expr, Func};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, TransactionTrait, TryInsertResult,
};
use sha2::{Digest, Sha256};

use crate::{
    error::{AppError, AppResult},
    store::entities::{pin_job, pin_lease, pin_lease_target, remote_pin},
};

pub type DateTimeUtc = DateTime<Utc>;

const STATE_PENDING: &str = "pending";
const STATE_RUNNING: &str = "running";
const STATE_DONE: &str = "done";
const REMOTE_STATUS_RESERVED: &str = "reserved";
const REMOTE_STATUS_QUEUED: &str = "queued";
const REMOTE_STATUS_PINNING: &str = "pinning";
const LEASE_STATE_ACTIVE: &str = "active";
const SUBMIT_PHASE_READY: &str = "ready";
const SUBMIT_PHASE_CALLING: &str = "calling";
const SUBMIT_PHASE_RECOVERING: &str = "recovering";
const SUBMIT_PHASE_RECOVERY_BACKOFF: &str = "recovery_backoff";
pub(crate) const MAX_SUBMIT_RECOVERY_ATTEMPTS: i32 = 8;
const MIN_SUBMIT_RECOVERY_BACKOFF: Duration = Duration::from_secs(1);
const MAX_SUBMIT_RECOVERY_BACKOFF: Duration = Duration::from_secs(300);
const MAX_SQLITE_CLAIM_RETRIES: usize = 4;
const PARKED_RECONCILE_REASON: &str = "submit requires operator attention";
pub(crate) const RPC_RECOVERY_PARK_REASON: &str = "RPC side effects require operator attention";

/// Minimal Stage 1 execution history. Secrets and arbitrary provider text never enter it.
pub mod history {
    use sea_orm::entity::prelude::*;
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
    #[sea_orm(table_name = "pin_submit_history")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub job_id: String,
        /// Captured outbound correlation. NULL means a pre-migration invocation
        /// whose provider metadata used the structured job ID.
        pub correlation: Option<String>,
        pub api: String,
        pub strategy: String,
        pub effect: String,
        pub state: String,
        pub first_error: Option<String>,
        pub last_error: Option<String>,
        pub submit_calls: i32,
        pub recovery_queries: i32,
        pub started_at: DateTimeUtc,
    }
    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}
    impl ActiveModelBehavior for ActiveModel {}
}

pub async fn submission_history<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> AppResult<Option<history::Model>> {
    Ok(history::Entity::find_by_id(id.to_owned()).one(db).await?)
}

/// Read-only proof for an explicit management Retry of the exact parked source.
/// A NULL claim alone never proves a call did not create a resource.
#[cfg(test)]
pub(crate) async fn management_retryable_park<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    route: &crate::pinning::identity::ProviderRouteSnapshot,
) -> AppResult<bool> {
    let work = SharedDecodeBudget::default();
    management_retryable_park_at_allocation(db, job, route, None, DecodeWork::shared(&work)).await
}

fn management_decode<T: serde::de::DeserializeOwned>(
    raw: &str,
    work: DecodeWork<'_>,
) -> AppResult<Option<T>> {
    match work.with(|work| work.typed(raw)) {
        Ok(value) => Ok(Some(value)),
        Err(ManagementError::Budget) => Err(management_error(ManagementError::Budget)),
        Err(_) => Ok(None),
    }
}

async fn management_retryable_park_at_allocation<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    route: &crate::pinning::identity::ProviderRouteSnapshot,
    allocation_epoch: Option<i64>,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    use crate::store::entities::{pin_invocation_route, remote_pin_ledger};
    if job.operation != "submit"
        || job.state != STATE_RUNNING
        || job.submit_phase.as_deref() != Some(SUBMIT_PHASE_RECOVERING)
        || job.locked_until.is_some()
        || !check_target_job_generation_with_work(db, job, work)
            .await
            .map_err(management_error)?
    {
        return Ok(false);
    }
    let Some(owner) = canonical_desired_target_with_work(db, &job.provider, &job.cid, work)
        .await
        .map_err(management_error)?
    else {
        return Ok(false);
    };
    let now = crate::store::database_clock::database_now(db).await?;
    let Some(lease) = records::current_model::<pin_lease::Entity, _>(db, &[&owner.lease_id], work)
        .await
        .map_err(management_error)?
    else {
        return Ok(false);
    };
    if job.target_id.as_deref() != Some(&owner.id) || lease.expires_at <= now {
        return Ok(false);
    }
    let Some(history) = records::current_model::<history::Entity, _>(db, &[&job.id], work)
        .await
        .map_err(management_error)?
    else {
        return Ok(false);
    };
    let Some((api, strategy)) = crate::pinning::worker::invocation_route_for_snapshot(route) else {
        return Ok(false);
    };
    if history.effect != "not_created"
        || !matches!(history.state.as_str(), "blocked" | "needs_attention")
        || history.api != api
        || history.strategy != strategy
        || history.submit_calls <= 0
        || history.submit_calls >= MAX_SUBMIT_RECOVERY_ATTEMPTS
    {
        return Ok(false);
    }
    let Some(invocation) =
        records::current_model::<pin_invocation_route::Entity, _>(db, &[&job.id], work)
            .await
            .map_err(management_error)?
    else {
        return Ok(false);
    };
    let Some(remote) =
        records::current_model::<remote_pin::Entity, _>(db, &[&job.provider, &job.cid], work)
            .await
            .map_err(management_error)?
    else {
        return Ok(false);
    };
    let Some(ledger) = records::current_model::<remote_pin_ledger::Entity, _>(
        db,
        &[&job.provider, &job.cid],
        work,
    )
    .await
    .map_err(management_error)?
    else {
        return Ok(false);
    };
    let original_route = management_decode::<crate::pinning::identity::ProviderRouteSnapshot>(
        &invocation.route,
        work,
    )?;
    let ledger_route = match ledger.route.as_deref() {
        Some(raw) => {
            management_decode::<crate::pinning::identity::ProviderRouteSnapshot>(raw, work)?
        }
        None => None,
    };
    if remote.status != REMOTE_STATUS_RESERVED
        || remote.request_id.is_some()
        || remote.epoch != allocation_epoch.unwrap_or(invocation.remote_epoch)
        || ledger.effect != "not_created"
        || ledger_route.as_ref() != Some(route)
        || original_route.as_ref() != Some(route)
    {
        return Ok(false);
    }
    work.copy(&(&job.provider, &job.cid))
        .map_err(management_error)?;
    work.with(|work| work.nodes(2)).map_err(management_error)?;
    let released=db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),"SELECT EXISTS(SELECT 1 FROM pin_resource_history WHERE provider=$1 AND cid=$2 AND epoch >= $3) AS present",[job.provider.clone().into(),job.cid.clone().into(),invocation.remote_epoch.into()])).await?.ok_or_else(||invalid_job("release history result is missing"))?;
    if released.try_get::<bool>("", "present")? {
        return Ok(false);
    }
    if let Some(receipt) = super::ledger::submission::latest_rpc_with_work(db, &job.id, work)
        .await
        .map_err(management_error)?
    {
        let resources = management_decode::<Vec<super::ledger::submission::ResourceEvidence>>(
            &receipt.resources,
            work,
        )?;
        let receipt_route = management_decode::<crate::pinning::identity::ProviderRouteSnapshot>(
            &receipt.route,
            work,
        )?;
        if receipt.needs_attention
            || receipt.outcome != "not_submitted"
            || receipt.effect != "not_created"
            || receipt.submit_call != history.submit_calls
            || receipt.remote_epoch != invocation.remote_epoch
            || receipt.observed_at.is_none()
            || resources.is_none_or(|resources| !resources.is_empty())
            || receipt.expected_cid != job.cid
            || receipt.provider != job.provider
            || receipt_route.as_ref() != Some(route)
        {
            return Ok(false);
        }
    } else if super::ledger::submission::is_rpc(route) {
        return Ok(false);
    }
    work.copy(&(&job.provider, &job.cid, &job.id))
        .map_err(management_error)?;
    work.with(|work| work.nodes(2)).map_err(management_error)?;
    let others=db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),"SELECT EXISTS(SELECT 1 FROM pin_jobs WHERE provider=$1 AND cid=$2 AND id<>$3 AND state IN ('pending','running') AND (operation IN ('submit','unpin') OR locked_until IS NOT NULL)) AS present",[job.provider.clone().into(),job.cid.clone().into(),job.id.clone().into()])).await?.ok_or_else(||invalid_job("competing job result is missing"))?;
    if others.try_get::<bool>("", "present")? {
        return Ok(false);
    }
    Ok(true)
}

/// Caller holds the native management fence and publishes its receipt in this
/// transaction. Preserve the old invocation/effect/count and existing reservation.
pub(crate) async fn resume_management_submit_in_transaction(
    tx: &sea_orm::DatabaseTransaction,
    job: &pin_job::Model,
    route: &crate::pinning::identity::ProviderRouteSnapshot,
    now: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    resume_management_submit(tx, job, route, now, None, work).await
}

/// A newly allocated logical intent may reuse the reservation without replacing
/// its canonical Submit source. The native fence must prove that exact reuse.
pub(crate) async fn resume_management_submit_after_allocation(
    tx: &sea_orm::DatabaseTransaction,
    job: &pin_job::Model,
    route: &crate::pinning::identity::ProviderRouteSnapshot,
    before: FrozenRows<'_>,
    attached: &pin_lease_target::Model,
    now: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    let Some(current) = lock_submit_receipt_with_work(tx, &job.id, work).await? else {
        return Ok(false);
    };
    if current != *job {
        return Ok(false);
    }
    super::quota::lock_allocation_admission_with_work(tx, &job.provider, work).await?;
    if !management_park_allocation_continuity(tx, job, before, attached, work).await? {
        return Ok(false);
    }
    let remote =
        records::current_model::<remote_pin::Entity, _>(tx, &[&job.provider, &job.cid], work)
            .await
            .map_err(management_error)?
            .ok_or_else(|| stale_claim_error(&job.id))?;
    resume_management_submit(tx, job, route, now, Some(remote.epoch), work).await
}

async fn management_park_allocation_continuity(
    tx: &sea_orm::DatabaseTransaction,
    job: &pin_job::Model,
    before: FrozenRows<'_>,
    attached: &pin_lease_target::Model,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    use super::management::native::{self, value};
    let invalid = |_| invalid_job("management allocation continuity is invalid");
    before.validate_current().map_err(invalid)?;
    if attached.provider != job.provider
        || attached.cid != job.cid
        || attached.state != "waiting"
        || before.row("pin_lease_targets", &[&attached.id]).is_some()
        || before.row("pin_leases", &[&attached.lease_id]).is_some()
    {
        return Ok(false);
    }
    if records::current_model::<pin_lease_target::Entity, _>(tx, &[&attached.id], work)
        .await
        .map_err(management_error)?
        .as_ref()
        != Some(attached)
    {
        return Ok(false);
    }
    let Some(lease) =
        records::current_model::<pin_lease::Entity, _>(tx, &[&attached.lease_id], work)
            .await
            .map_err(management_error)?
    else {
        return Ok(false);
    };
    if lease.source != lease.id
        || !lease.id.starts_with("management:")
        || lease.state != "active"
        || lease.generation != 1
        || attached.id != super::leases::target_id(&lease.id, &job.provider, &job.cid)
        || attached.created_at != attached.last_touched_at
    {
        return Ok(false);
    }
    let current_rows =
        super::management::actions::job_authority::park_scope::capture(tx, job, before, work)
            .await
            .map_err(management_error)?;
    let after = FrozenRows::Frame(&current_rows);
    if before.scope() != after.scope() || before.identity() != after.identity() {
        return Ok(false);
    }
    let Some(old) = before.row("remote_pins", &[&job.provider, &job.cid]) else {
        return Ok(false);
    };
    let Some(current) = after.row("remote_pins", &[&job.provider, &job.cid]) else {
        return Ok(false);
    };
    let epoch = value(old, "epoch").and_then(|epoch| epoch.parse::<i64>().ok());
    let invocation = before.row("pin_invocation_routes", &[&job.id]);
    if epoch.is_none()
        || invocation
            .and_then(|row| value(row, "remote_epoch"))
            .and_then(|epoch| epoch.parse::<i64>().ok())
            != epoch
        || epoch.and_then(|epoch| epoch.checked_add(1))
            != value(current, "epoch").and_then(|epoch| epoch.parse::<i64>().ok())
    {
        return Ok(false);
    }
    let targets = before
        .table("pin_lease_targets")
        .ok_or_else(|| invalid_job("target fence is missing"))?;
    let count = targets
        .rows
        .iter()
        .filter(|row| {
            value(*row, "provider") == Some(&job.provider) && value(*row, "cid") == Some(&job.cid)
        })
        .count();
    work.with(|work| work.nodes(2 + count * 2))
        .map_err(management_error)?;
    let old_targets = targets
        .rows
        .iter()
        .filter(|row| {
            value(row, "provider") == Some(&job.provider) && value(row, "cid") == Some(&job.cid)
        })
        .collect::<Vec<_>>();
    let lease_ids = old_targets
        .iter()
        .filter_map(|row| value(row, "lease_id"))
        .collect::<std::collections::BTreeSet<_>>();
    let mut touch = None;
    for target in &old_targets {
        if value(target, "state").is_some_and(is_desired_target_state)
            && value(
                before
                    .row(
                        "pin_leases",
                        &[value(target, "lease_id")
                            .ok_or_else(|| invalid_job("target owner is missing"))?],
                    )
                    .ok_or_else(|| invalid_job("lease fence is missing"))?,
                "state",
            ) == Some(LEASE_STATE_ACTIVE)
        {
            let time = native::parse_time(
                value(target, "last_touched_at")
                    .ok_or_else(|| invalid_job("target touch is missing"))?,
            )
            .map_err(invalid)?;
            touch = Some(touch.map_or(time, |old: DateTimeUtc| old.max(time)));
        }
    }
    let expected_touch = touch
        .unwrap_or(
            native::parse_time(
                value(old, "last_touched_at")
                    .ok_or_else(|| invalid_job("remote touch is missing"))?,
            )
            .map_err(invalid)?,
        )
        .max(attached.created_at);
    if native::parse_time(
        value(current, "last_touched_at").ok_or_else(|| invalid_job("remote touch is missing"))?,
    )
    .map_err(invalid)?
        != expected_touch
    {
        return Ok(false);
    }
    if !old
        .iter()
        .filter(|(column, _)| column.as_str() != "epoch" && column.as_str() != "last_touched_at")
        .eq(current.iter().filter(|(column, _)| {
            column.as_str() != "epoch" && column.as_str() != "last_touched_at"
        }))
    {
        return Ok(false);
    }
    work.with(|work| {
        work.nodes(
            1 + before
                .table("pin_jobs")
                .ok_or(ManagementError::Schema)?
                .rows
                .iter()
                .filter(|row| {
                    value(*row, "provider") == Some(&job.provider)
                        && value(*row, "cid") == Some(&job.cid)
                })
                .count(),
        )
    })
    .map_err(management_error)?;
    let job_ids = before
        .table("pin_jobs")
        .ok_or_else(|| invalid_job("job fence is missing"))?
        .rows
        .iter()
        .filter(|row| {
            value(row, "provider") == Some(&job.provider) && value(row, "cid") == Some(&job.cid)
        })
        .filter_map(|row| value(row, "id"))
        .collect::<std::collections::BTreeSet<_>>();
    for name in [
        "pin_lease_targets",
        "pin_leases",
        "pin_lease_requirements",
        "pin_jobs",
        "pin_submit_history",
        "pin_invocation_routes",
        "pin_submit_observations",
        "pin_replica_observations",
        "pin_resource_history",
        "remote_pin_ledger",
        "pin_provider_usage",
    ] {
        let table = before
            .table(name)
            .ok_or_else(|| invalid_job("allocation fence is missing"))?;
        let relevant = |row: RowView<'_>| match name {
            "pin_leases" => value(row, "id").is_some_and(|id| lease_ids.contains(id)),
            "pin_lease_requirements" => {
                value(row, "lease_id").is_some_and(|id| lease_ids.contains(id))
            }
            "pin_submit_history" | "pin_invocation_routes" => {
                value(row, "job_id").is_some_and(|id| job_ids.contains(id))
            }
            "pin_submit_observations" | "pin_replica_observations" => {
                value(row, "provider") == Some(&job.provider)
                    && value(row, "expected_cid") == Some(&job.cid)
            }
            "pin_provider_usage" => value(row, "provider") == Some(&job.provider),
            _ => {
                value(row, "provider") == Some(&job.provider)
                    && value(row, "cid") == Some(&job.cid)
                    && !(name == "pin_lease_targets" && value(row, "id") == Some(&attached.id))
            }
        };
        let current = after
            .table(name)
            .ok_or_else(|| invalid_job("allocation fence is missing"))?;
        if !table
            .rows
            .iter()
            .filter(|row| relevant(*row))
            .eq(current.rows.iter().filter(|row| relevant(*row)))
        {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn resume_management_submit(
    tx: &sea_orm::DatabaseTransaction,
    job: &pin_job::Model,
    route: &crate::pinning::identity::ProviderRouteSnapshot,
    now: DateTimeUtc,
    allocation_epoch: Option<i64>,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    let Some(current) = lock_submit_receipt_with_work(tx, &job.id, work).await? else {
        return Ok(false);
    };
    if current != *job
        || !management_retryable_park_at_allocation(tx, &current, route, allocation_epoch, work)
            .await?
    {
        return Ok(false);
    }
    super::quota::lock_allocation_admission_with_work(tx, &job.provider, work).await?;
    let changed = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(
            pin_job::Column::SubmitPhase,
            Expr::value(SUBMIT_PHASE_READY),
        )
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(now))
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .filter(pin_job::Column::Id.eq(&job.id))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::SubmitPhase.eq(SUBMIT_PHASE_RECOVERING))
        .filter(pin_job::Column::LockedUntil.is_null())
        .exec(tx)
        .await?;
    if changed.rows_affected != 1 {
        return Err(stale_claim_error(&job.id));
    }
    let changed = history::Entity::update_many()
        .col_expr(history::Column::State, Expr::value("active"))
        .filter(history::Column::JobId.eq(&job.id))
        .filter(history::Column::Effect.eq("not_created"))
        .filter(history::Column::State.is_in(["blocked", "needs_attention"]))
        .exec(tx)
        .await?;
    if changed.rows_affected != 1 {
        return Err(stale_claim_error(&job.id));
    }
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(now))
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .filter(pin_job::Column::Provider.eq(&job.provider))
        .filter(pin_job::Column::Cid.eq(&job.cid))
        .filter(pin_job::Column::Operation.eq("reconcile"))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.is_null())
        .filter(pin_job::Column::LastError.eq(PARKED_RECONCILE_REASON))
        .exec(tx)
        .await?;
    Ok(true)
}

/// Explicit operator repair hook, never called by ordinary scheduling or TOML
/// reload. Only a definitively rejected call on the same historical route can
/// resume. Unknown effects require reconciliation, not this hook.
pub async fn resume_rejected_submit_after_repair<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    id: &str,
    api: &str,
    strategy: &str,
    now: DateTimeUtc,
) -> AppResult<bool> {
    let txn = db.begin().await?;
    if super::ledger::submission::latest_rpc(&txn, id)
        .await?
        .is_some_and(|row| row.needs_attention)
    {
        txn.rollback().await?;
        return Ok(false);
    }
    let updated = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(id))
        .filter(pin_job::Column::Operation.eq("submit"))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.is_null())
        .exec(&txn)
        .await?;
    if updated.rows_affected != 1 {
        txn.rollback().await?;
        return Ok(false);
    }
    let Some(history) = submission_history(&txn, id).await? else {
        txn.rollback().await?;
        return Ok(false);
    };
    if history.state != "blocked"
        || history.effect != "not_created"
        || history.api != api
        || history.strategy != strategy
        || history.submit_calls >= MAX_SUBMIT_RECOVERY_ATTEMPTS
    {
        txn.rollback().await?;
        return Ok(false);
    }
    history::Entity::update_many()
        .col_expr(history::Column::State, Expr::value("active"))
        .col_expr(history::Column::StartedAt, Expr::value(now))
        .filter(history::Column::JobId.eq(id))
        .exec(&txn)
        .await?;
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(
            pin_job::Column::SubmitPhase,
            Expr::value(SUBMIT_PHASE_READY),
        )
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(id))
        .exec(&txn)
        .await?;
    // Only explicit repair wakes matching parked reconciliation. Routine scans
    // must not reactivate this manual isolation barrier.
    let job = pin_job::Entity::find_by_id(id.to_owned())
        .one(&txn)
        .await?
        .ok_or_else(|| invalid_job("repaired Submit disappeared"))?;
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(now))
        .filter(pin_job::Column::Provider.eq(job.provider))
        .filter(pin_job::Column::Cid.eq(job.cid))
        .filter(pin_job::Column::Operation.eq("reconcile"))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.is_null())
        .filter(pin_job::Column::LastError.eq(PARKED_RECONCILE_REASON))
        .exec(&txn)
        .await?;
    txn.commit().await?;
    Ok(true)
}

pub async fn record_submit_invocation<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    api: &str,
    strategy: &str,
    now: DateTimeUtc,
) -> AppResult<()> {
    let work = SharedDecodeBudget::default();
    record_submit_invocation_with_work(db, claimed, api, strategy, now, DecodeWork::shared(&work))
        .await
        .map(|_| ())
}
pub(crate) async fn record_submit_invocation_with_work<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    api: &str,
    strategy: &str,
    now: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<Option<history::Model>> {
    super::management::admission::lock_sqlite_writer_in_caller_transaction(db).await?;
    if !super::ledger::submit_dispatch_open_with_work(db, &claimed.model, work).await? {
        return Err(invalid_job(
            "retired domain cannot start another invocation",
        ));
    }
    if !fence_job_claim(db, &claimed.model.id, claimed_lock(&claimed.model)?).await? {
        return Err(stale_claim_error(&claimed.model.id));
    }
    if super::ledger::submission::latest_rpc(db, &claimed.model.id)
        .await?
        .is_some_and(|row| row.outcome != "not_submitted")
    {
        return Err(invalid_job(
            "RPC invocation has unreconciled effects; repeated POST is unsafe",
        ));
    }
    // Preserve the previous Submit effect for the lifetime fence. A retry's
    // `not_created` evidence is lost as soon as history becomes `unknown`.
    if super::ledger::submission::is_rpc_api(api)
        && let Some(row) =
            super::ledger::get(db, &claimed.model.provider, &claimed.model.cid).await?
        && !super::ledger::submission::held_evidence_known(db, &row).await?
    {
        return Err(invalid_job(
            "historical RPC resource lacks fenced submission evidence",
        ));
    }
    super::ledger::advance_submit_invocation_epoch(db, claimed, api, strategy).await?;
    super::quota::lock_allocation_admission_with_work(db, &claimed.model.provider, work).await?;
    let old = submission_history(db, &claimed.model.id).await?;
    if let Some(old) = &old {
        if old.api != api
            || old.strategy != strategy
            || old.submit_calls >= MAX_SUBMIT_RECOVERY_ATTEMPTS
        {
            return Err(invalid_job(
                "historical submit route changed or budget exhausted",
            ));
        }
        history::Entity::update_many()
            .col_expr(
                history::Column::SubmitCalls,
                Expr::col(history::Column::SubmitCalls).add(1),
            )
            .col_expr(history::Column::Effect, Expr::value("unknown"))
            .col_expr(history::Column::State, Expr::value("active"))
            .filter(history::Column::JobId.eq(&claimed.model.id))
            .exec(db)
            .await?;
    } else {
        history::Entity::insert(history::ActiveModel {
            job_id: Set(claimed.model.id.clone()),
            correlation: Set(None),
            api: Set(api.to_owned()),
            strategy: Set(strategy.to_owned()),
            effect: Set("unknown".into()),
            state: Set("active".into()),
            first_error: Set(None),
            last_error: Set(None),
            submit_calls: Set(1),
            recovery_queries: Set(0),
            started_at: Set(now),
        })
        .exec(db)
        .await?;
    }
    super::ledger::mark_effect(db, &claimed.model.provider, &claimed.model.cid, "unknown").await?;
    super::ledger::capture_invocation(
        db,
        &claimed.model.id,
        &claimed.model.provider,
        &claimed.model.cid,
    )
    .await?;
    Ok(old)
}

pub async fn record_submit_error<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    effect: &str,
    safe_error: &str,
) -> AppResult<()> {
    if !fence_job_claim(db, &claimed.model.id, claimed_lock(&claimed.model)?).await? {
        return Err(stale_claim_error(&claimed.model.id));
    }
    history::Entity::update_many()
        .col_expr(history::Column::Effect, Expr::value(effect))
        .col_expr(
            history::Column::FirstError,
            Func::coalesce([
                Expr::col(history::Column::FirstError).into(),
                Expr::value(safe_error),
            ])
            .into(),
        )
        .col_expr(history::Column::LastError, Expr::value(safe_error))
        .filter(history::Column::JobId.eq(&claimed.model.id))
        .exec(db)
        .await?;
    super::ledger::mark_effect(db, &claimed.model.provider, &claimed.model.cid, effect).await?;
    super::ledger::record_error(db, &claimed.model.provider, &claimed.model.cid, safe_error)
        .await?;
    Ok(())
}

/// Preserve unknown work as unclaimable by both generations of workers.
pub async fn park_identity_job<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
) -> AppResult<()> {
    if !fence_job_claim(db, &claimed.model.id, claimed_lock(&claimed.model)?).await? {
        return Err(stale_claim_error(&claimed.model.id));
    }
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_RUNNING))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value("historical identity unavailable; needs_attention"),
        )
        .filter(pin_job::Column::Id.eq(&claimed.model.id))
        .exec(db)
        .await?;
    super::ledger::record_error(
        db,
        &claimed.model.provider,
        &claimed.model.cid,
        "historical identity unavailable; needs_attention",
    )
    .await?;
    Ok(())
}

/// Non-done/calling barrier retains references and reservations. NULL lock is
/// unclaimable even by legacy workers; the detailed terminal state lives in history.
#[cfg(test)]
struct ParkTestGate {
    job_id: String,
    fenced: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[cfg(test)]
static PARK_TEST_GATE: std::sync::LazyLock<
    tokio::sync::Mutex<Option<std::sync::Arc<ParkTestGate>>>,
> = std::sync::LazyLock::new(|| tokio::sync::Mutex::new(None));

pub async fn park_submit<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    state: &str,
    safe_error: &str,
    now: DateTimeUtc,
) -> AppResult<()> {
    let txn = db.begin().await?;
    match park_submit_in_transaction(&txn, claimed, state, safe_error, now).await {
        Ok(()) => {
            txn.commit().await?;
            Ok(())
        }
        Err(error) => {
            txn.rollback().await?;
            Err(error)
        }
    }
}

async fn park_submit_in_transaction(
    db: &sea_orm::DatabaseTransaction,
    claimed: &ClaimedPinJob,
    state: &str,
    safe_error: &str,
    now: DateTimeUtc,
) -> AppResult<()> {
    if !fence_job_claim(db, &claimed.model.id, claimed_lock(&claimed.model)?).await? {
        return Err(stale_claim_error(&claimed.model.id));
    }
    #[cfg(test)]
    {
        let gate = PARK_TEST_GATE.lock().await.clone();
        if let Some(gate) = gate.filter(|gate| gate.job_id == claimed.model.id) {
            gate.fenced.notify_one();
            gate.resume.notified().await;
        }
    }
    if submission_history(db, &claimed.model.id).await?.is_none() {
        history::Entity::insert(history::ActiveModel {
            job_id: Set(claimed.model.id.clone()),
            correlation: Set(None),
            api: Set("unknown".into()),
            strategy: Set("unknown".into()),
            effect: Set("unknown".into()),
            state: Set(state.into()),
            first_error: Set(Some(safe_error.into())),
            last_error: Set(Some(safe_error.into())),
            submit_calls: Set(0),
            recovery_queries: Set(0),
            started_at: Set(now),
        })
        .exec(db)
        .await?;
    }
    history::Entity::update_many()
        .col_expr(history::Column::State, Expr::value(state))
        .col_expr(
            history::Column::LastError,
            Func::coalesce([
                Expr::col(history::Column::LastError).into(),
                Expr::value(safe_error),
            ])
            .into(),
        )
        .filter(history::Column::JobId.eq(&claimed.model.id))
        .exec(db)
        .await?;
    let updated = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_RUNNING))
        .col_expr(
            pin_job::Column::SubmitPhase,
            Expr::value(SUBMIT_PHASE_RECOVERING),
        )
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(pin_job::Column::LastError, Expr::value(safe_error))
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(&claimed.model.id))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(claimed_lock(&claimed.model)?))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(stale_claim_error(&claimed.model.id));
    }
    // A late receipt may have committed after recovery read `in_flight` but
    // before this exact claim parked. Serialize both orders on the job first.
    if state == "needs_attention" && safe_error == RPC_RECOVERY_PARK_REASON {
        super::ledger::submission::wake_matching_parked(db, &claimed.model.id, now).await?;
    }
    Ok(())
}

/// Receipt appends must lock even a *different* current claim before touching
/// history/remote/evidence. A failed old-claim predicate does not lock a PG row.
pub(crate) async fn lock_submit_receipt<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> AppResult<Option<pin_job::Model>> {
    lock_submit_receipt_rows(db, id).await?;
    Ok(pin_job::Entity::find_by_id(id.to_owned()).one(db).await?)
}

async fn lock_submit_receipt_with_work<C: ConnectionTrait>(
    db: &C,
    id: &str,
    work: DecodeWork<'_>,
) -> AppResult<Option<pin_job::Model>> {
    work.copy(&(id, id)).map_err(management_error)?;
    lock_submit_receipt_rows(db, id).await?;
    records::current_model::<pin_job::Entity, _>(db, &[id], work)
        .await
        .map_err(management_error)
}

async fn lock_submit_receipt_rows<C: ConnectionTrait>(db: &C, id: &str) -> AppResult<()> {
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::col(pin_job::Column::LockedUntil).into(),
        )
        .filter(pin_job::Column::Id.eq(id))
        .exec(db)
        .await?;
    history::Entity::update_many()
        .col_expr(
            history::Column::State,
            Expr::col(history::Column::State).into(),
        )
        .filter(history::Column::JobId.eq(id))
        .exec(db)
        .await?;
    Ok(())
}

/// Only the exact RPC attention park can become due local recovery. Never
/// mutate another caller's live claim or change the captured invocation.
pub(crate) async fn wake_rpc_receipt<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    submit_call: i32,
    now: DateTimeUtc,
) -> AppResult<bool> {
    let changed = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(now))
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(&job.id))
        .filter(pin_job::Column::Operation.eq("submit"))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::SubmitPhase.eq(SUBMIT_PHASE_RECOVERING))
        .filter(pin_job::Column::LockedUntil.is_null())
        .filter(pin_job::Column::LastError.eq(RPC_RECOVERY_PARK_REASON))
        .filter(pin_job::Column::Provider.eq(&job.provider))
        .filter(pin_job::Column::Cid.eq(&job.cid))
        .filter(pin_job::Column::LeaseId.eq(job.lease_id.clone()))
        .filter(pin_job::Column::TargetId.eq(job.target_id.clone()))
        .filter(pin_job::Column::ExpectedGeneration.eq(job.expected_generation))
        .exec(db)
        .await?;
    if changed.rows_affected != 1 {
        return Ok(false);
    }
    let changed = history::Entity::update_many()
        .col_expr(history::Column::State, Expr::value("active"))
        .filter(history::Column::JobId.eq(&job.id))
        .filter(history::Column::SubmitCalls.eq(submit_call))
        .filter(history::Column::State.eq("needs_attention"))
        .exec(db)
        .await?;
    if changed.rows_affected != 1 {
        return Err(stale_claim_error(&job.id)); // caller transaction rolls back the job CAS too
    }
    Ok(true)
}

pub async fn begin_recovery_query<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
) -> AppResult<Option<history::Model>> {
    if !fence_job_claim(db, &claimed.model.id, claimed_lock(&claimed.model)?).await? {
        return Err(stale_claim_error(&claimed.model.id));
    }
    let history = submission_history(db, &claimed.model.id).await?;
    match history {
        Some(history)
            if history.api != "unknown"
                && history.state == "active"
                && history.recovery_queries < MAX_SUBMIT_RECOVERY_ATTEMPTS
                && now.signed_duration_since(history.started_at) < ChronoDuration::hours(1) =>
        {
            history::Entity::update_many()
                .col_expr(
                    history::Column::RecoveryQueries,
                    Expr::col(history::Column::RecoveryQueries).add(1),
                )
                .filter(history::Column::JobId.eq(&claimed.model.id))
                .exec(db)
                .await?;
            Ok(Some(history))
        }
        _ => {
            park_submit(
                db,
                claimed,
                "needs_attention",
                "historical route unknown or recovery budget exhausted",
                now,
            )
            .await?;
            Ok(None)
        }
    }
}

/// Lock and recheck the parked Submit rows shared with explicit repair. Call
/// before acquiring remote/Reconcile locks, and retain this transaction through
/// the Reconcile park decision. IDs are ordered to serialize multi-row scopes.
pub async fn fence_parked_submits(
    db: &sea_orm::DatabaseTransaction,
    provider: &str,
    cid: &str,
) -> AppResult<bool> {
    let candidates = pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(provider))
        .filter(pin_job::Column::Cid.eq(cid))
        .filter(pin_job::Column::Operation.eq("submit"))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.is_null())
        .order_by_asc(pin_job::Column::Id)
        .all(db)
        .await?;
    let mut parked = false;
    for candidate in candidates {
        // The UPDATE predicate is rechecked after any wait on repair's row
        // lock. A stale SELECT must never authorize a later Reconcile park.
        let fenced = pin_job::Entity::update_many()
            .col_expr(
                pin_job::Column::LockedUntil,
                Expr::col(pin_job::Column::LockedUntil).into(),
            )
            .filter(pin_job::Column::Id.eq(candidate.id))
            .filter(pin_job::Column::Operation.eq("submit"))
            .filter(pin_job::Column::State.eq(STATE_RUNNING))
            .filter(pin_job::Column::LockedUntil.is_null())
            .exec(db)
            .await?;
        parked |= fenced.rows_affected == 1;
    }
    Ok(parked)
}

/// Keep remote responsibility without periodically scheduling work which cannot
/// make progress until an operator resolves a parked Submit.
/// The caller must first hold `fence_parked_submits` in this same transaction,
/// before taking remote/Reconcile locks, so repair cannot miss this park.
pub async fn park_reconcile_for_submit_attention(
    db: &sea_orm::DatabaseTransaction,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
) -> AppResult<()> {
    if claimed.model.operation != "reconcile" {
        return Err(invalid_job("only Reconcile may wait for Submit attention"));
    }
    let updated = pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(PARKED_RECONCILE_REASON),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(&claimed.model.id))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(claimed_lock(&claimed.model)?))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(stale_claim_error(&claimed.model.id));
    }
    Ok(())
}

#[cfg(test)]
type ClaimUpdateRecorder = std::sync::Arc<std::sync::Mutex<Vec<String>>>;

#[cfg(test)]
static CLAIM_UPDATE_RECORDER: std::sync::LazyLock<std::sync::Mutex<Option<ClaimUpdateRecorder>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(None));

#[cfg(test)]
fn record_claim_update(job_id: &str) {
    let recorder = CLAIM_UPDATE_RECORDER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if let Some(recorder) = recorder {
        recorder
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(job_id.to_owned());
    }
}

/// The durable cadence used for the first and ordinary successful Poll continuation.
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The four persisted job operations. Their database representation is always lowercase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinJobOperation {
    Submit,
    Poll,
    Unpin,
    Reconcile,
}

impl PinJobOperation {
    fn persisted(self) -> &'static str {
        match self {
            Self::Submit => "submit",
            Self::Poll => "poll",
            Self::Unpin => "unpin",
            Self::Reconcile => "reconcile",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetJobOperation {
    Submit,
    Poll,
}

impl TargetJobOperation {
    fn operation(self) -> PinJobOperation {
        match self {
            Self::Submit => PinJobOperation::Submit,
            Self::Poll => PinJobOperation::Poll,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteJobOperation {
    Unpin,
    Reconcile,
}

impl RemoteJobOperation {
    fn operation(self) -> PinJobOperation {
        match self {
            Self::Unpin => PinJobOperation::Unpin,
            Self::Reconcile => PinJobOperation::Reconcile,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitPhase {
    Ready,
    Calling,
    Recovering,
    RecoveryBackoff,
}

impl SubmitPhase {
    fn persisted(self) -> &'static str {
        match self {
            Self::Ready => SUBMIT_PHASE_READY,
            Self::Calling => SUBMIT_PHASE_CALLING,
            Self::Recovering => SUBMIT_PHASE_RECOVERING,
            Self::RecoveryBackoff => SUBMIT_PHASE_RECOVERY_BACKOFF,
        }
    }
}

/// A target-scoped Submit or Poll. It cannot represent remote-scoped nullable fields.
#[derive(Debug, Clone)]
pub struct TargetPinJob {
    pub id: String,
    pub operation: TargetJobOperation,
    pub provider: String,
    pub cid: String,
    pub lease_id: String,
    pub target_id: String,
    pub expected_generation: i64,
    pub next_attempt_at: DateTimeUtc,
}

/// A remote-scoped Unpin or Reconcile. It cannot represent target-scoped nullable fields.
#[derive(Debug, Clone)]
pub struct RemotePinJob {
    pub id: String,
    pub operation: RemoteJobOperation,
    pub provider: String,
    pub cid: String,
    pub expected_remote_epoch: i64,
    pub next_attempt_at: DateTimeUtc,
}

#[derive(Debug, Clone)]
pub enum NewPinJob {
    Target(TargetPinJob),
    Remote(RemotePinJob),
}

#[derive(Debug, Clone)]
pub struct ClaimedPinJob {
    pub model: pin_job::Model,
    pub reclaimed: bool,
    pub previous_state: String,
    pub object_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsureJobOutcome {
    Inserted,
    Pending,
    Running,
    Reactivated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitCallDecision {
    ReadyToCall,
    NoLongerDesired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitRecoveryDecision {
    RetryScheduled { next_attempt_at: DateTimeUtc },
    NoLongerDesired { reconcile_job_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoRequestSubmitAmbiguity {
    Clear {
        cancelled_never_started: Vec<String>,
    },
    Wait {
        next_check_at: DateTimeUtc,
    },
}

pub fn submit_job(
    provider: &str,
    cid: &str,
    lease_id: &str,
    target_id: &str,
    generation: i64,
    at: DateTimeUtc,
) -> NewPinJob {
    NewPinJob::Target(TargetPinJob {
        id: stable_submit_id(provider, cid, target_id, generation),
        operation: TargetJobOperation::Submit,
        provider: provider.to_owned(),
        cid: cid.to_owned(),
        lease_id: lease_id.to_owned(),
        target_id: target_id.to_owned(),
        expected_generation: generation,
        next_attempt_at: at,
    })
}

pub fn poll_job(
    provider: &str,
    cid: &str,
    lease_id: &str,
    target_id: &str,
    generation: i64,
    request_id: &str,
    at: DateTimeUtc,
) -> NewPinJob {
    NewPinJob::Target(TargetPinJob {
        id: stable_poll_id(provider, cid, target_id, generation, request_id),
        operation: TargetJobOperation::Poll,
        provider: provider.to_owned(),
        cid: cid.to_owned(),
        lease_id: lease_id.to_owned(),
        target_id: target_id.to_owned(),
        expected_generation: generation,
        next_attempt_at: at,
    })
}

pub fn unpin_job(provider: &str, cid: &str, remote_epoch: i64, at: DateTimeUtc) -> NewPinJob {
    NewPinJob::Remote(RemotePinJob {
        id: stable_unpin_id(provider, cid, remote_epoch),
        operation: RemoteJobOperation::Unpin,
        provider: provider.to_owned(),
        cid: cid.to_owned(),
        expected_remote_epoch: remote_epoch,
        next_attempt_at: at,
    })
}

pub fn reconcile_job(provider: &str, cid: &str, remote_epoch: i64, at: DateTimeUtc) -> NewPinJob {
    NewPinJob::Remote(RemotePinJob {
        id: stable_reconcile_id(provider, cid, remote_epoch),
        operation: RemoteJobOperation::Reconcile,
        provider: provider.to_owned(),
        cid: cid.to_owned(),
        expected_remote_epoch: remote_epoch,
        next_attempt_at: at,
    })
}

/// Enqueues typed work once. A duplicate stable ID never changes existing durable state.
pub async fn enqueue_job<C: ConnectionTrait>(db: &C, job: NewPinJob) -> AppResult<()> {
    insert_new_job(db, job, Utc::now()).await.map(|_| ())
}

/// Ensures the canonical reserved/no-request owner has exactly one ready Submit job.
pub async fn ensure_or_reactivate_submit_job<C: ConnectionTrait>(
    db: &C,
    job: TargetPinJob,
    now: DateTimeUtc,
) -> AppResult<EnsureJobOutcome> {
    let work = SharedDecodeBudget::default();
    ensure_submit_with_work(db, job, now, DecodeWork::shared(&work)).await
}
pub(crate) async fn ensure_submit_with_work<C: ConnectionTrait>(
    db: &C,
    mut job: TargetPinJob,
    now: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<EnsureJobOutcome> {
    if job.operation != TargetJobOperation::Submit {
        return Err(invalid_job("submit ensure requires a Submit job"));
    }
    validate_submit_owner_with_work(db, &job, work).await?;
    super::quota::lock_allocation_admission_with_work(db, &job.provider, work).await?;
    if let Some((_, state)) =
        blocking_live_submit_with_work(db, &job.provider, &job.cid, &job.id, work).await?
    {
        return match state.as_str() {
            STATE_PENDING => Ok(EnsureJobOutcome::Pending),
            STATE_RUNNING => Ok(EnsureJobOutcome::Running),
            _ => Err(invalid_job("blocking Submit job is not live")),
        };
    }
    job.next_attempt_at = now;
    ensure_target_job_with_work(db, job, now, true, work).await
}

pub(crate) async fn blocking_live_submit_with_work<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    id: &str,
    work: DecodeWork<'_>,
) -> AppResult<Option<(String, String)>> {
    for _ in 0..2 {
        work.copy(&(provider, cid, id)).map_err(management_error)?;
    }
    let (rows,bytes)=records::bounded_projection(db,"FROM (SELECT id,state FROM pin_jobs WHERE provider=$1 AND cid=$2 AND id<>$3 AND operation='submit' AND state IN ('pending','running') ORDER BY id LIMIT 1) AS blocker WHERE TRUE",&[("id","id"),("state","state")],"id",vec![provider.into(),cid.into(),id.into()],(1,super::management::native::MAX_BYTES),work).await.map_err(management_error)?;
    work.index_cells(rows.len() * 2, bytes)
        .map_err(management_error)?;
    rows.first()
        .map(|row| Ok((row.try_get("", "id")?, row.try_get("", "state")?)))
        .transpose()
}

/// Ensures the canonical queued/pinning owner has one bounded-due Poll job.
pub async fn ensure_or_reactivate_poll_job<C: ConnectionTrait>(
    db: &C,
    job: TargetPinJob,
    now: DateTimeUtc,
) -> AppResult<EnsureJobOutcome> {
    if job.operation != TargetJobOperation::Poll {
        return Err(invalid_job("poll ensure requires a Poll job"));
    }
    let earliest = now + ChronoDuration::seconds(1);
    let latest = now + ChronoDuration::seconds(30);
    if job.next_attempt_at < earliest || job.next_attempt_at > latest {
        return Err(invalid_job(
            "poll due time must be between one and thirty seconds",
        ));
    }
    validate_poll_owner(db, &job).await?;
    ensure_target_job(db, job, now, false).await
}

/// Ensures one remote-scoped Reconcile at the current epoch.
pub async fn ensure_or_reactivate_reconcile_job<C: ConnectionTrait>(
    db: &C,
    job: RemotePinJob,
    now: DateTimeUtc,
) -> AppResult<EnsureJobOutcome> {
    let work = SharedDecodeBudget::default();
    ensure_reconcile_with_work(db, job, now, DecodeWork::shared(&work)).await
}
pub(crate) async fn ensure_reconcile_with_work<C: ConnectionTrait>(
    db: &C,
    job: RemotePinJob,
    now: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<EnsureJobOutcome> {
    if job.operation != RemoteJobOperation::Reconcile {
        return Err(invalid_job("reconcile ensure requires a Reconcile job"));
    }
    let remote =
        records::current_model::<remote_pin::Entity, _>(db, &[&job.provider, &job.cid], work)
            .await
            .map_err(management_error)?
            .ok_or_else(|| invalid_job("remote job does not name the current epoch"))?;
    if remote.epoch != job.expected_remote_epoch {
        return Err(invalid_job("remote job does not name the current epoch"));
    }
    let original =
        super::legacy_migration::original_job_provider_with_work(db, &job.id, &job.provider, work)
            .await
            .map_err(management_error)?;
    if job.id != stable_reconcile_id(&original, &job.cid, job.expected_remote_epoch) {
        return Err(invalid_job(
            "Reconcile job ID does not match its remote epoch",
        ));
    }

    work.copy(&job.id).map_err(management_error)?;
    let job_id = job.id.clone();
    let Some(existing) = records::current_model::<pin_job::Entity, _>(db, &[&job_id], work)
        .await
        .map_err(management_error)?
    else {
        return if insert_new_job_with_work(db, NewPinJob::Remote(job), now, work).await? {
            Ok(EnsureJobOutcome::Inserted)
        } else {
            let persisted = records::current_model::<pin_job::Entity, _>(db, &[&job_id], work)
                .await
                .map_err(management_error)?
                .ok_or_else(|| stale_claim_error(&job_id))?;
            match persisted.state.as_str() {
                STATE_PENDING => Ok(EnsureJobOutcome::Pending),
                STATE_RUNNING => Ok(EnsureJobOutcome::Running),
                _ => Err(stale_claim_error(&job_id)),
            }
        };
    };

    match existing.state.as_str() {
        STATE_PENDING => {
            if job.next_attempt_at < existing.next_attempt_at {
                pin_job::Entity::update_many()
                    .col_expr(
                        pin_job::Column::NextAttemptAt,
                        Expr::value(job.next_attempt_at),
                    )
                    .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
                    .filter(pin_job::Column::Id.eq(job.id))
                    .filter(pin_job::Column::State.eq(STATE_PENDING))
                    .exec(db)
                    .await?;
            }
            Ok(EnsureJobOutcome::Pending)
        }
        STATE_RUNNING => Ok(EnsureJobOutcome::Running),
        STATE_DONE => {
            reactivate_job(db, &existing.id, job.next_attempt_at, now, None).await?;
            Ok(EnsureJobOutcome::Reactivated)
        }
        _ => Err(invalid_job("pin job has an invalid persisted state")),
    }
}

/// Ensures one remote-scoped Unpin at the current epoch, including compensation reactivation.
pub async fn ensure_or_reactivate_unpin_job<C: ConnectionTrait>(
    db: &C,
    job: RemotePinJob,
    now: DateTimeUtc,
) -> AppResult<EnsureJobOutcome> {
    if job.operation != RemoteJobOperation::Unpin {
        return Err(invalid_job("unpin ensure requires an Unpin job"));
    }
    validate_remote_epoch(db, &job).await?;

    let job_id = job.id.clone();
    let next_attempt_at = job.next_attempt_at;
    let Some(existing) = pin_job::Entity::find_by_id(job_id.clone()).one(db).await? else {
        return if insert_new_job(db, NewPinJob::Remote(job), now).await? {
            Ok(EnsureJobOutcome::Inserted)
        } else {
            remote_job_outcome_after_conflict(db, &job_id, next_attempt_at, now).await
        };
    };

    match existing.state.as_str() {
        STATE_PENDING => Ok(EnsureJobOutcome::Pending),
        STATE_RUNNING => Ok(EnsureJobOutcome::Running),
        STATE_DONE => {
            reactivate_job(db, &existing.id, job.next_attempt_at, now, None).await?;
            Ok(EnsureJobOutcome::Reactivated)
        }
        _ => Err(invalid_job("pin job has an invalid persisted state")),
    }
}

/// Reserve SQLite's writer before the claim transaction establishes a read snapshot.
async fn lock_sqlite_claim_writer<C: ConnectionTrait>(db: &C) -> Result<(), sea_orm::DbErr> {
    if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
        db.execute(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "UPDATE main.pin_jobs SET id=id WHERE 1=0",
        ))
        .await?;
    }
    Ok(())
}

/// Claims due unlocked work. Expired Submit locks are reclaimed in `recovering` before return.
pub async fn claim_due_jobs<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    now: DateTimeUtc,
    lock_for: ChronoDuration,
    limit: u64,
) -> AppResult<Vec<ClaimedPinJob>> {
    let locked_until = now + lock_for;
    for attempt in 0..=MAX_SQLITE_CLAIM_RETRIES {
        let result = db
            .transaction(|txn| {
                Box::pin(async move {
                    lock_sqlite_claim_writer(txn).await?;
                    let candidates = pin_job::Entity::find()
                        .filter(due_claim_condition(now))
                        .order_by_asc(pin_job::Column::NextAttemptAt)
                        .order_by_asc(pin_job::Column::Id)
                        .limit(limit)
                        .all(txn)
                        .await?;
                    let mut claimed = Vec::with_capacity(candidates.len());

                    for candidate in candidates {
                        if let Some(job) =
                            claim_candidate(txn, candidate, now, locked_until).await?
                        {
                            claimed.push(job);
                        }
                    }
                    Ok::<_, sea_orm::DbErr>(claimed)
                })
            })
            .await;
        match result {
            Ok(claimed) => return Ok(claimed),
            Err(error)
                if is_sqlite_contention(&error.to_string())
                    && attempt < MAX_SQLITE_CLAIM_RETRIES =>
            {
                sqlite_claim_retry_delay(attempt).await;
            }
            Err(error) => return Err(AppError::Database(error.to_string())),
        }
    }
    unreachable!("SQLite claim retry loop always returns or errors")
}

/// Receipt-owned scope, never a caller-supplied list of queue UUIDs. There is no
/// global expiry/quota scan here; execution uses the normal claim protocol.
pub(crate) async fn claim_due_management_jobs(
    db: &sea_orm::DatabaseConnection,
    scope: &super::management::admission::ManagementJobScope,
    now: DateTimeUtc,
    lock_for: ChronoDuration,
    limit: u64,
) -> AppResult<Vec<ClaimedPinJob>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let tx = db.begin().await?;
    let work = super::management::records::SharedDecodeBudget::default();
    scope.revalidate_with_work(&tx, work.clone()).await?;
    use sea_orm::QueryTrait;
    let context = DecodeWork::shared(&work);
    context.copy(&[now; 6]).map_err(management_error)?;
    // Numbered parameters can be reused by the metadata/body guards on both backends.
    let query = pin_job::Entity::find()
        .select_only()
        .column(pin_job::Column::Id)
        .filter(due_claim_condition(now))
        .filter(scope.candidates_with_work(context)?)
        .order_by_asc(pin_job::Column::Id)
        .build(sea_orm::DatabaseBackend::Postgres);
    let (candidates, candidate_bytes) = records::bounded_projection(
        &tx,
        &format!("FROM ({}) AS candidates WHERE TRUE", query.sql),
        &[("id", "id")],
        "id",
        query.values.map(|values| values.0).unwrap_or_default(),
        (
            super::management::native::MAX_ROWS,
            super::management::native::MAX_BYTES,
        ),
        context,
    )
    .await
    .map_err(management_error)?;
    context
        .index_cells(candidates.len(), candidate_bytes)
        .map_err(management_error)?;
    context
        .with(|work| work.nodes(1))
        .map_err(management_error)?;
    let mut claimed = Vec::new();
    for candidate in candidates {
        if claimed.len() as u64 >= limit {
            break;
        }
        let id = candidate.try_get::<String>("", "id")?;
        let candidate = records::current_model::<pin_job::Entity, _>(&tx, &[&id], context)
            .await
            .map_err(management_error)?
            .ok_or_else(|| invalid_job("management candidate disappeared"))?;
        if !scope
            .contains_with_work(&tx, &candidate, work.clone())
            .await?
        {
            continue;
        }
        if let Some(job) =
            claim_candidate_with_work(&tx, candidate, now, now + lock_for, work.clone()).await?
        {
            if !scope
                .contains_with_work(&tx, &job.model, work.clone())
                .await?
            {
                return Err(invalid_job(
                    "management claim changed its original applied scope",
                ));
            }
            context
                .with(|work| work.nodes(1))
                .map_err(management_error)?;
            claimed.push(job);
        }
    }
    tx.commit().await?;
    Ok(claimed)
}

pub(crate) struct WorkerClaimSchedule<'a> {
    pub in_flight_by_provider: &'a BTreeMap<String, usize>,
    pub last_served_ticket_by_provider: &'a BTreeMap<String, u64>,
    pub configured_priorities: &'a BTreeMap<String, u32>,
    pub active_job_ids: &'a BTreeSet<String>,
}

/// Claims due work using provider occupancy and recent-service tickets for fair scheduling.
///
/// SQLite reserves its writer before candidate reads. The selected rows are then claimed in globally stable
/// job-id order so concurrent PostgreSQL claimants cannot acquire overlapping row locks in
/// opposite provider-local orders. Successful claims are returned in their original fair order.
pub async fn claim_due_jobs_fair<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    now: DateTimeUtc,
    lock_for: ChronoDuration,
    limit: u64,
    in_flight_by_provider: &BTreeMap<String, usize>,
    last_served_ticket_by_provider: &BTreeMap<String, u64>,
    configured_priorities: &BTreeMap<String, u32>,
) -> AppResult<Vec<ClaimedPinJob>> {
    claim_due_worker_jobs_fair(
        db,
        now,
        lock_for,
        limit,
        WorkerClaimSchedule {
            in_flight_by_provider,
            last_served_ticket_by_provider,
            configured_priorities,
            active_job_ids: &BTreeSet::new(),
        },
    )
    .await
}

/// Excludes this worker's live tasks before candidate limits and claim writes.
pub(crate) async fn claim_due_worker_jobs_fair<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    now: DateTimeUtc,
    lock_for: ChronoDuration,
    limit: u64,
    schedule: WorkerClaimSchedule<'_>,
) -> AppResult<Vec<ClaimedPinJob>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let locked_until = now + lock_for;
    for attempt in 0..=MAX_SQLITE_CLAIM_RETRIES {
        let in_flight_by_provider = schedule.in_flight_by_provider.clone();
        let last_served_ticket_by_provider = schedule.last_served_ticket_by_provider.clone();
        let configured_priorities = schedule.configured_priorities.clone();
        let active_job_ids = schedule.active_job_ids.clone();
        let result = db
            .transaction(|txn| {
                Box::pin(async move {
                    lock_sqlite_claim_writer(txn).await?;
                    let mut due = Condition::all().add(due_claim_condition(now));
                    if !active_job_ids.is_empty() {
                        due = due.add(pin_job::Column::Id.is_not_in(active_job_ids));
                    }
                    let due_providers: Vec<String> = pin_job::Entity::find()
                        .select_only()
                        .column(pin_job::Column::Provider)
                        .filter(due.clone())
                        .group_by(pin_job::Column::Provider)
                        .into_tuple()
                        .all(txn)
                        .await?;
                    let capacity = usize::try_from(limit).unwrap_or(usize::MAX);
                    let mut candidates_by_provider = BTreeMap::new();
                    for provider in due_providers {
                        let candidates = pin_job::Entity::find()
                            .filter(due.clone())
                            .filter(pin_job::Column::Provider.eq(provider.clone()))
                            .order_by_asc(pin_job::Column::NextAttemptAt)
                            .order_by_asc(pin_job::Column::Id)
                            .limit(limit)
                            .all(txn)
                            .await?;
                        if !candidates.is_empty() {
                            candidates_by_provider.insert(provider, VecDeque::from(candidates));
                        }
                    }

                    let fair_plan = select_fair_candidates(
                        candidates_by_provider,
                        capacity,
                        &in_flight_by_provider,
                        &last_served_ticket_by_provider,
                        &configured_priorities,
                    );
                    let mut update_plan = fair_plan.clone();
                    update_plan.sort_by(|left, right| left.candidate.id.cmp(&right.candidate.id));

                    let mut claimed = Vec::with_capacity(update_plan.len());
                    for ranked in update_plan {
                        if let Some(job) =
                            claim_candidate(txn, ranked.candidate, now, locked_until).await?
                        {
                            claimed.push((ranked.fair_rank, job));
                        }
                    }
                    claimed.sort_by_key(|(fair_rank, _)| *fair_rank);
                    Ok::<_, sea_orm::DbErr>(
                        claimed.into_iter().map(|(_, job)| job).collect::<Vec<_>>(),
                    )
                })
            })
            .await;
        match result {
            Ok(claimed) => return Ok(claimed),
            Err(error)
                if is_sqlite_contention(&error.to_string())
                    && attempt < MAX_SQLITE_CLAIM_RETRIES =>
            {
                sqlite_claim_retry_delay(attempt).await;
            }
            Err(error) => return Err(AppError::Database(error.to_string())),
        }
    }
    unreachable!("SQLite claim retry loop always returns or errors")
}

#[derive(Clone)]
struct RankedClaimCandidate {
    fair_rank: usize,
    candidate: pin_job::Model,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EffectiveServiceTicket {
    Existing(u64),
    Selected(usize),
}

fn select_fair_candidates(
    mut candidates_by_provider: BTreeMap<String, VecDeque<pin_job::Model>>,
    capacity: usize,
    in_flight_by_provider: &BTreeMap<String, usize>,
    last_served_ticket_by_provider: &BTreeMap<String, u64>,
    configured_priorities: &BTreeMap<String, u32>,
) -> Vec<RankedClaimCandidate> {
    let mut selected_by_provider = BTreeMap::<String, usize>::new();
    let mut effective_service = last_served_ticket_by_provider
        .iter()
        .map(|(provider, ticket)| (provider.clone(), EffectiveServiceTicket::Existing(*ticket)))
        .collect::<BTreeMap<_, _>>();
    let mut selected = Vec::with_capacity(capacity.min(candidates_by_provider.len()));

    while selected.len() < capacity && !candidates_by_provider.is_empty() {
        let Some(provider) = candidates_by_provider
            .keys()
            .min_by(|left, right| {
                effective_provider_occupancy(left, in_flight_by_provider, &selected_by_provider)
                    .cmp(&effective_provider_occupancy(
                        right,
                        in_flight_by_provider,
                        &selected_by_provider,
                    ))
                    .then_with(|| {
                        effective_service
                            .get(*left)
                            .copied()
                            .unwrap_or(EffectiveServiceTicket::Existing(0))
                            .cmp(
                                &effective_service
                                    .get(*right)
                                    .copied()
                                    .unwrap_or(EffectiveServiceTicket::Existing(0)),
                            )
                    })
                    .then_with(|| {
                        configured_priorities
                            .get(*left)
                            .copied()
                            .unwrap_or(u32::MAX)
                            .cmp(
                                &configured_priorities
                                    .get(*right)
                                    .copied()
                                    .unwrap_or(u32::MAX),
                            )
                    })
                    .then_with(|| left.cmp(right))
            })
            .cloned()
        else {
            break;
        };
        let (candidate, queue_is_empty) = match candidates_by_provider.get_mut(&provider) {
            Some(queue) => (queue.pop_front(), queue.is_empty()),
            None => (None, true),
        };
        if queue_is_empty {
            candidates_by_provider.remove(&provider);
        }
        let Some(candidate) = candidate else {
            continue;
        };
        let fair_rank = selected.len();
        *selected_by_provider.entry(provider.clone()).or_default() += 1;
        effective_service.insert(provider, EffectiveServiceTicket::Selected(fair_rank));
        selected.push(RankedClaimCandidate {
            fair_rank,
            candidate,
        });
    }
    selected
}

fn effective_provider_occupancy(
    provider: &str,
    in_flight_by_provider: &BTreeMap<String, usize>,
    selected_by_provider: &BTreeMap<String, usize>,
) -> usize {
    in_flight_by_provider
        .get(provider)
        .copied()
        .unwrap_or_default()
        .saturating_add(
            selected_by_provider
                .get(provider)
                .copied()
                .unwrap_or_default(),
        )
}

async fn claim_candidate<C: ConnectionTrait>(
    db: &C,
    candidate: pin_job::Model,
    now: DateTimeUtc,
    locked_until: DateTimeUtc,
) -> Result<Option<ClaimedPinJob>, sea_orm::DbErr> {
    claim_candidate_with_work(
        db,
        candidate,
        now,
        locked_until,
        super::management::records::SharedDecodeBudget::default(),
    )
    .await
}
async fn claim_candidate_with_work<C: ConnectionTrait>(
    db: &C,
    candidate: pin_job::Model,
    now: DateTimeUtc,
    locked_until: DateTimeUtc,
    work: super::management::records::SharedDecodeBudget,
) -> Result<Option<ClaimedPinJob>, sea_orm::DbErr> {
    let context = DecodeWork::shared(&work);
    let budget_error =
        |_| sea_orm::DbErr::Custom("claim data could not be read within its budget".into());
    let reclaimed = candidate.state == STATE_RUNNING;
    context.copy(&candidate.state).map_err(budget_error)?;
    let previous_state = candidate.state.clone();
    let object_id = if let Some(lease_id) = candidate.lease_id.as_deref() {
        records::current_model::<pin_lease::Entity, _>(db, &[lease_id], context)
            .await
            .map_err(budget_error)?
            .map(|lease| lease.owner_object_id)
    } else {
        None
    };
    #[cfg(test)]
    record_claim_update(&candidate.id);
    let mut update = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_RUNNING))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Some(locked_until)),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(candidate.id.clone()))
        .filter(due_claim_condition(now));
    if reclaimed && candidate.operation == PinJobOperation::Submit.persisted() {
        update = update.col_expr(
            pin_job::Column::SubmitPhase,
            Expr::value(Some(SubmitPhase::Recovering.persisted())),
        );
    }
    if update.exec(db).await?.rows_affected != 1 {
        return Ok(None);
    }
    let model = records::current_model::<pin_job::Entity, _>(db, &[&candidate.id], context)
        .await
        .map_err(budget_error)?
        .ok_or_else(|| sea_orm::DbErr::RecordNotFound("claimed pin job disappeared".to_owned()))?;
    let claimed = ClaimedPinJob {
        model,
        reclaimed,
        previous_state,
        object_id,
    };
    match super::management::actions::job_authority::check_before_call_with_work(
        db,
        &claimed.model,
        work,
    )
    .await
    {
        Ok(_) => Ok(Some(claimed)),
        Err(
            super::management::ManagementError::Confirmation
            | super::management::ManagementError::InvalidEvidence
            | super::management::ManagementError::Unauthorized
            | super::management::ManagementError::Changed
            | super::management::ManagementError::Expired,
        ) => {
            // Isolate one invalid/orphaned management row. Do not poison fair
            // scheduling of unrelated ordinary jobs or allow a None fallback.
            finish_nonprojecting_job(db, &claimed, true, now)
                .await
                .map_err(|_| {
                    sea_orm::DbErr::Custom("management claim could not be isolated".into())
                })?;
            Ok(None)
        }
        Err(_) => Err(sea_orm::DbErr::Custom(
            "management claim authority could not be checked".into(),
        )),
    }
}

/// Extends one exact running claim without changing its operation phase.
///
/// A missing row means the caller no longer owns the claim and must not begin a provider call.
pub async fn renew_job_claim<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    expected_locked_until: DateTimeUtc,
    new_locked_until: DateTimeUtc,
    now: DateTimeUtc,
) -> AppResult<Option<DateTimeUtc>> {
    if new_locked_until <= expected_locked_until {
        return Err(invalid_job("renewed job lock must advance"));
    }
    let work = super::management::records::SharedDecodeBudget::default();
    let Some(job) =
        records::current_model::<pin_job::Entity, _>(db, &[job_id], DecodeWork::shared(&work))
            .await
            .map_err(management_error)?
    else {
        return Ok(None);
    };
    // LedgerOnly may finish gathering an already-started receipt. It does not
    // grant another call: the worker checks the operation-specific permit there.
    super::management::actions::job_authority::check_before_call_with_work(db, &job, work)
        .await
        .map_err(management_error)?;
    let updated = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::LockedUntil, Expr::value(new_locked_until))
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(job_id))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(expected_locked_until))
        .exec(db)
        .await?;
    Ok((updated.rows_affected == 1).then_some(new_locked_until))
}

/// Fences one exact running claim without advancing or releasing it.
///
/// This is intentionally a no-op write rather than an unlocked read. When it is the first
/// operation in a caller-owned transaction, later lifecycle mutations in that transaction are
/// ordered after the durable job claim and cannot accept a response from an older claim window.
pub async fn fence_job_claim<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    expected_locked_until: DateTimeUtc,
) -> AppResult<bool> {
    let fenced = pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::col(pin_job::Column::LockedUntil).into(),
        )
        .filter(pin_job::Column::Id.eq(job_id))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(expected_locked_until))
        .exec(db)
        .await?;
    Ok(fenced.rows_affected == 1)
}

/// Reschedules a successfully observed Poll without creating work or increasing attempts.
pub async fn reschedule_poll_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    expected_locked_until: DateTimeUtc,
    now: DateTimeUtc,
    requested_interval: Duration,
) -> AppResult<DateTimeUtc> {
    let interval = requested_interval.clamp(Duration::from_secs(1), Duration::from_secs(30));
    let next_attempt_at = now + duration_as_chrono(interval)?;
    let result = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(next_attempt_at))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(job_id))
        .filter(pin_job::Column::Operation.eq(PinJobOperation::Poll.persisted()))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(expected_locked_until))
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(stale_claim_error(job_id));
    }
    Ok(next_attempt_at)
}

/// Reschedules a claimed Reconcile in place at its caller-selected durable time.
pub async fn reschedule_reconcile_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    expected_locked_until: DateTimeUtc,
    next_attempt_at: DateTimeUtc,
) -> AppResult<()> {
    let result = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(next_attempt_at))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(Utc::now()))
        .filter(pin_job::Column::Id.eq(job_id))
        .filter(pin_job::Column::Operation.eq(PinJobOperation::Reconcile.persisted()))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(expected_locked_until))
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(stale_claim_error(job_id));
    }
    Ok(())
}

/// Returns whether target-scoped work still names an active desired target at its generation.
pub async fn check_target_job_generation<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
) -> AppResult<bool> {
    if !matches!(job.operation.as_str(), "submit" | "poll") {
        return Ok(false);
    }
    let (Some(lease_id), Some(target_id), Some(generation)) = (
        job.lease_id.as_deref(),
        job.target_id.as_deref(),
        job.expected_generation,
    ) else {
        return Ok(false);
    };
    if job.expected_remote_epoch.is_some() {
        return Ok(false);
    }
    let Some(target) = pin_lease_target::Entity::find_by_id(target_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    let Some(lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    Ok(target.lease_id == lease_id
        && target.provider == job.provider
        && target.cid == job.cid
        && is_desired_target_state(&target.state)
        && lease.state == LEASE_STATE_ACTIVE
        && lease.generation == generation)
}

pub(crate) async fn check_target_job_generation_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    work: DecodeWork<'_>,
) -> super::management::Result<bool> {
    if !matches!(job.operation.as_str(), "submit" | "poll") || job.expected_remote_epoch.is_some() {
        return Ok(false);
    }
    let (Some(lease_id), Some(target_id), Some(generation)) =
        (&job.lease_id, &job.target_id, job.expected_generation)
    else {
        return Ok(false);
    };
    let Some(target) =
        records::current_model::<pin_lease_target::Entity, _>(db, &[target_id], work).await?
    else {
        return Ok(false);
    };
    let Some(lease) = records::current_model::<pin_lease::Entity, _>(db, &[lease_id], work).await?
    else {
        return Ok(false);
    };
    Ok(target.lease_id == *lease_id
        && target.provider == job.provider
        && target.cid == job.cid
        && is_desired_target_state(&target.state)
        && lease.state == LEASE_STATE_ACTIVE
        && lease.generation == generation)
}

/// A Poll may follow reference-only remote epoch increments, but it must still
/// name the original request and the current canonical target/generation.
pub(crate) async fn poll_continuity_owner_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    request_id: &str,
    work: DecodeWork<'_>,
) -> super::management::Result<bool> {
    if job.operation != "poll" || !check_target_job_generation_with_work(db, job, work).await? {
        return Ok(false);
    }
    Ok(
        poll_names_request_with_work(db, job, request_id, work).await?
            && canonical_desired_target_with_work(db, &job.provider, &job.cid, work)
                .await?
                .is_some_and(|target| Some(target.id.as_str()) == job.target_id.as_deref()),
    )
}

/// Durable request evidence only, not authority to execute the historical owner.
pub(crate) async fn poll_names_request<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    request_id: &str,
) -> AppResult<bool> {
    let (Some(_), Some(target_id), Some(generation)) = (
        job.lease_id.as_deref(),
        job.target_id.as_deref(),
        job.expected_generation,
    ) else {
        return Ok(false);
    };
    let provider =
        super::legacy_migration::original_job_provider(db, &job.id, &job.provider).await?;
    Ok(poll_names_request_at_provider(
        job, request_id, &provider, target_id, generation,
    ))
}

pub(crate) async fn poll_names_request_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    request_id: &str,
    work: super::management::records::DecodeWork<'_>,
) -> super::management::Result<bool> {
    let (Some(_), Some(target), Some(generation)) = (
        job.lease_id.as_deref(),
        job.target_id.as_deref(),
        job.expected_generation,
    ) else {
        return Ok(false);
    };
    let provider =
        super::legacy_migration::original_job_provider_with_work(db, &job.id, &job.provider, work)
            .await?;
    const HASH_SHAPE: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    work.copy(&(request_id, HASH_SHAPE))?;
    work.copy(&("poll:", &provider, &job.cid, target, generation, HASH_SHAPE))?;
    Ok(poll_names_request_at_provider(
        job, request_id, &provider, target, generation,
    ))
}

fn poll_names_request_at_provider(
    job: &pin_job::Model,
    request_id: &str,
    provider: &str,
    target_id: &str,
    generation: i64,
) -> bool {
    job.operation == "poll"
        && job.expected_remote_epoch.is_none()
        && job.id == stable_poll_id(provider, &job.cid, target_id, generation, request_id)
}

/// The already-claimed Submit's immutable target/generation remains evidence
/// of its invocation even after that target's lease is cancelled.
pub(crate) fn submit_names_original_target(job: &pin_job::Model) -> bool {
    let (Some(_), Some(target_id), Some(generation)) = (
        job.lease_id.as_deref(),
        job.target_id.as_deref(),
        job.expected_generation,
    ) else {
        return false;
    };
    job.operation == "submit"
        && job.expected_remote_epoch.is_none()
        && job.id == stable_submit_id(&job.provider, &job.cid, target_id, generation)
}

/// Returns whether remote-scoped work still names the current remote epoch.
/// Unpin additionally requires that no active desired target remains.
pub async fn check_remote_job_epoch<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
) -> AppResult<bool> {
    if !matches!(job.operation.as_str(), "unpin" | "reconcile")
        || job.lease_id.is_some()
        || job.target_id.is_some()
        || job.expected_generation.is_some()
    {
        return Ok(false);
    }
    let Some(expected_epoch) = job.expected_remote_epoch else {
        return Ok(false);
    };
    let Some(remote) = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    if remote.epoch != expected_epoch {
        return Ok(false);
    }
    if job.operation == PinJobOperation::Unpin.persisted() {
        return Ok(canonical_desired_target(db, &job.provider, &job.cid)
            .await?
            .is_none());
    }
    Ok(true)
}

/// Moves one current ready/recovery-backoff Submit into `calling` before provider HTTP.
pub async fn prepare_submit_call<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
) -> AppResult<SubmitCallDecision> {
    let work = SharedDecodeBudget::default();
    prepare_submit_call_with_work(db, claimed, now, DecodeWork::shared(&work)).await
}
pub(crate) async fn prepare_submit_call_with_work<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<SubmitCallDecision> {
    super::management::admission::lock_sqlite_writer_in_caller_transaction(db).await?;
    let job = &claimed.model;
    let locked_until = claimed_lock(job)?;
    if job.operation != PinJobOperation::Submit.persisted() {
        return Err(invalid_job("submit preparation requires a claimed Submit"));
    }

    let canonical = canonical_desired_target(db, &job.provider, &job.cid).await?;
    let current = check_target_job_generation(db, job).await?
        && canonical.as_ref().map(|target| target.id.as_str()) == job.target_id.as_deref()
        && remote_is_reserved_without_request(db, &job.provider, &job.cid).await?;
    if !current {
        complete_job(db, &job.id, locked_until, now).await?;
        ensure_current_reconcile(db, &job.provider, &job.cid, now).await?;
        return Ok(SubmitCallDecision::NoLongerDesired);
    }
    if !matches!(
        job.submit_phase.as_deref(),
        Some(SUBMIT_PHASE_READY | SUBMIT_PHASE_RECOVERY_BACKOFF)
    ) {
        return Err(invalid_job(
            "submit phase is not eligible to call the provider",
        ));
    }
    super::quota::lock_allocation_admission_with_work(db, &job.provider, work).await?;
    if db.get_database_backend() == DatabaseBackend::Postgres {
        // This is the new-dispatch tail, not the historical receipt append.
        // A job-first receipt may be waiting for our earlier remote lock.
        db.query_one(sea_orm::Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT id FROM pin_jobs WHERE id=$1 FOR UPDATE NOWAIT",
            [job.id.clone().into()],
        ))
        .await?;
    }

    let result = pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::SubmitPhase,
            Expr::value(Some(SubmitPhase::Calling.persisted())),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(job.id.clone()))
        .filter(pin_job::Column::Operation.eq(PinJobOperation::Submit.persisted()))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(locked_until))
        .filter(
            Condition::any()
                .add(pin_job::Column::SubmitPhase.eq(SUBMIT_PHASE_READY))
                .add(pin_job::Column::SubmitPhase.eq(SUBMIT_PHASE_RECOVERY_BACKOFF)),
        )
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(stale_claim_error(&job.id));
    }
    Ok(SubmitCallDecision::ReadyToCall)
}

/// Durably records that a provider Submit may have been accepted before any recovery lookup.
/// The exact claim lock remains held so the same execution can find and adopt immediately.
pub async fn mark_submit_recovering_after_call<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
    redacted_error: &str,
) -> AppResult<()> {
    let job = &claimed.model;
    let locked_until = claimed_lock(job)?;
    validate_claimed_submit(job)?;
    let result = pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::SubmitPhase,
            Expr::value(Some(SubmitPhase::Recovering.persisted())),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Some(redacted_error.to_owned())),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(job.id.clone()))
        .filter(pin_job::Column::Operation.eq(PinJobOperation::Submit.persisted()))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(locked_until))
        .filter(pin_job::Column::SubmitPhase.eq(SUBMIT_PHASE_CALLING))
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(stale_claim_error(&job.id));
    }
    Ok(())
}

/// Records a conclusive zero-result Submit recovery lookup without issuing another POST.
pub async fn record_submit_recovery_no_match<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
    backoff: Duration,
) -> AppResult<SubmitRecoveryDecision> {
    let job = &claimed.model;
    let locked_until = claimed_lock(job)?;
    validate_claimed_submit(job)?;
    if super::ledger::submission::latest_rpc(db, &job.id)
        .await?
        .is_some_and(|row| row.outcome != "not_submitted" || row.needs_attention)
    {
        return Err(invalid_job(
            "expected CID absence cannot erase RPC side effects",
        ));
    }
    if canonical_desired_target(db, &job.provider, &job.cid)
        .await?
        .is_none()
    {
        complete_job(db, &job.id, locked_until, now).await?;
        let reconcile_job_id = ensure_current_reconcile(db, &job.provider, &job.cid, now)
            .await?
            .ok_or_else(|| invalid_job("remote pin is missing during Submit recovery"))?;
        return Ok(SubmitRecoveryDecision::NoLongerDesired { reconcile_job_id });
    }

    let next_attempt_at = now + duration_as_chrono(backoff)?;
    let result = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(
            pin_job::Column::SubmitPhase,
            Expr::value(Some(SubmitPhase::RecoveryBackoff.persisted())),
        )
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(next_attempt_at))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(job.id.clone()))
        .filter(pin_job::Column::Operation.eq(PinJobOperation::Submit.persisted()))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(locked_until))
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(stale_claim_error(&job.id));
    }
    Ok(SubmitRecoveryDecision::RetryScheduled { next_attempt_at })
}

/// Keeps ambiguous Submit recovery durable, but stops automatic work at the budget.
pub async fn retry_submit_recovery<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
    backoff: Duration,
    redacted_error: &str,
) -> AppResult<()> {
    let job = &claimed.model;
    let locked_until = claimed_lock(job)?;
    validate_claimed_submit(job)?;
    let attempts = job.attempts.clamp(0, MAX_SUBMIT_RECOVERY_ATTEMPTS);
    let attempts = if attempts < MAX_SUBMIT_RECOVERY_ATTEMPTS {
        attempts + 1
    } else {
        attempts
    };
    if attempts >= MAX_SUBMIT_RECOVERY_ATTEMPTS {
        return park_submit(db, claimed, "needs_attention", redacted_error, now).await;
    }
    let cadence = if attempts == MAX_SUBMIT_RECOVERY_ATTEMPTS {
        MAX_SUBMIT_RECOVERY_BACKOFF
    } else {
        clamped_submit_recovery_backoff(backoff)
    };
    let next_attempt_at = checked_next_attempt(now, cadence)?;
    let result = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(pin_job::Column::Attempts, Expr::value(attempts))
        .col_expr(
            pin_job::Column::SubmitPhase,
            Expr::value(Some(SubmitPhase::Recovering.persisted())),
        )
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(next_attempt_at))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Some(redacted_error.to_owned())),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(job.id.clone()))
        .filter(pin_job::Column::Operation.eq(PinJobOperation::Submit.persisted()))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(locked_until))
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(stale_claim_error(&job.id));
    }
    Ok(())
}

/// Resolves only Submit rows that are provably never started; provider ambiguity always waits.
pub async fn resolve_no_request_submit_ambiguity<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTimeUtc,
) -> AppResult<NoRequestSubmitAmbiguity> {
    let safe_rows = pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(provider))
        .filter(pin_job::Column::Cid.eq(cid))
        .filter(pin_job::Column::Operation.eq(PinJobOperation::Submit.persisted()))
        .filter(pin_job::Column::State.eq(STATE_PENDING))
        .filter(
            Condition::any()
                .add(pin_job::Column::SubmitPhase.eq(SUBMIT_PHASE_READY))
                .add(pin_job::Column::SubmitPhase.eq(SUBMIT_PHASE_RECOVERY_BACKOFF)),
        )
        .order_by_asc(pin_job::Column::Id)
        .all(db)
        .await?;
    let mut cancelled_never_started = Vec::with_capacity(safe_rows.len());
    for row in safe_rows {
        let result = pin_job::Entity::update_many()
            .col_expr(pin_job::Column::State, Expr::value(STATE_DONE))
            .col_expr(
                pin_job::Column::SubmitPhase,
                Expr::value(Some(SubmitPhase::Ready.persisted())),
            )
            .col_expr(
                pin_job::Column::LockedUntil,
                Expr::value(Option::<DateTimeUtc>::None),
            )
            .col_expr(
                pin_job::Column::LastError,
                Expr::value(Option::<String>::None),
            )
            .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
            .filter(pin_job::Column::Id.eq(row.id.clone()))
            .filter(pin_job::Column::State.eq(STATE_PENDING))
            .filter(
                Condition::any()
                    .add(pin_job::Column::SubmitPhase.eq(SUBMIT_PHASE_READY))
                    .add(pin_job::Column::SubmitPhase.eq(SUBMIT_PHASE_RECOVERY_BACKOFF)),
            )
            .exec(db)
            .await?;
        if result.rows_affected == 1 {
            cancelled_never_started.push(row.id);
        }
    }

    let unresolved = pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(provider))
        .filter(pin_job::Column::Cid.eq(cid))
        .filter(pin_job::Column::Operation.eq(PinJobOperation::Submit.persisted()))
        .filter(pin_job::Column::State.ne(STATE_DONE))
        .order_by_asc(pin_job::Column::NextAttemptAt)
        .all(db)
        .await?;
    let next_check_at = unresolved
        .iter()
        .map(|row| {
            if row.state == STATE_RUNNING {
                row.locked_until.unwrap_or(row.next_attempt_at)
            } else {
                row.next_attempt_at
            }
        })
        .min();
    match next_check_at {
        Some(next_check_at) => Ok(NoRequestSubmitAmbiguity::Wait { next_check_at }),
        None => Ok(NoRequestSubmitAmbiguity::Clear {
            cancelled_never_started,
        }),
    }
}

/// Retries ordinary (non-Submit) work with a capped exponential delay and an exact claim lock.
#[allow(clippy::too_many_arguments)]
pub async fn retry_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    expected_locked_until: DateTimeUtc,
    now: DateTimeUtc,
    base_backoff: Duration,
    max_backoff: Duration,
    max_attempts: u32,
    redacted_error: &str,
) -> AppResult<DateTimeUtc> {
    let job = pin_job::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| stale_claim_error(job_id))?;
    if job.operation == PinJobOperation::Submit.persisted() {
        return Err(invalid_job(
            "Submit recovery must use retry_submit_recovery",
        ));
    }
    let attempt_cap = i32::try_from(max_attempts).unwrap_or(i32::MAX).max(0);
    let attempts = job.attempts.clamp(0, attempt_cap);
    let attempts = if attempts < attempt_cap {
        attempts + 1
    } else {
        attempts
    };
    let next_attempt_at =
        checked_next_attempt(now, bounded_backoff(base_backoff, max_backoff, attempts))?;
    let result = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(pin_job::Column::Attempts, Expr::value(attempts))
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(next_attempt_at))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Some(redacted_error.to_owned())),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(job_id))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(expected_locked_until))
        .filter(pin_job::Column::Operation.ne(PinJobOperation::Submit.persisted()))
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(stale_claim_error(job_id));
    }
    super::ledger::record_error(db, &job.provider, &job.cid, redacted_error).await?;
    Ok(next_attempt_at)
}

/// Marks exactly one claimed job done and clears its lock.
/// A Submit without a persisted remote request is normalized back to the safe `ready` phase.
pub async fn complete_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    expected_locked_until: DateTimeUtc,
    now: DateTimeUtc,
) -> AppResult<()> {
    if !fence_job_claim(db, job_id, expected_locked_until).await? {
        return Err(stale_claim_error(job_id));
    }
    let job = pin_job::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| stale_claim_error(job_id))?;
    let submit_phase = if job.operation == PinJobOperation::Submit.persisted() {
        let remote = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
            .one(db)
            .await?;
        if remote.and_then(|row| row.request_id).is_none() {
            Some(SubmitPhase::Ready.persisted())
        } else {
            history::Entity::update_many()
                .col_expr(history::Column::Effect, Expr::value("created"))
                .col_expr(history::Column::State, Expr::value("settled"))
                .filter(history::Column::JobId.eq(&job.id))
                .exec(db)
                .await?;
            job.submit_phase.as_deref()
        }
    } else {
        None
    };
    let result = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_DONE))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(pin_job::Column::SubmitPhase, Expr::value(submit_phase))
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(job_id))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(expected_locked_until))
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(stale_claim_error(job_id));
    }
    Ok(())
}

/// Finalizes only the observation's exact claim, not Submit history, a remote
/// lifetime, quota or a successor job. Unknown responsibility stays isolated.
pub(crate) async fn finish_nonprojecting_job<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    attention: bool,
    now: DateTimeUtc,
) -> AppResult<()> {
    let until = claimed_lock(&claimed.model)?;
    if !fence_job_claim(db, &claimed.model.id, until).await? {
        return Ok(());
    }
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::State,
            Expr::value(if attention { STATE_RUNNING } else { STATE_DONE }),
        )
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTimeUtc>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(if attention {
                Some("management observation needs_attention")
            } else {
                None
            }),
        )
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(&claimed.model.id))
        .filter(pin_job::Column::State.eq(STATE_RUNNING))
        .filter(pin_job::Column::LockedUntil.eq(until))
        .exec(db)
        .await?;
    Ok(())
}

fn new_job_active_model(job: NewPinJob, now: DateTimeUtc) -> pin_job::ActiveModel {
    match job {
        NewPinJob::Target(job) => {
            let operation = job.operation.operation();
            pin_job::ActiveModel {
                id: Set(job.id),
                operation: Set(operation.persisted().to_owned()),
                provider: Set(job.provider),
                cid: Set(job.cid),
                lease_id: Set(Some(job.lease_id)),
                target_id: Set(Some(job.target_id)),
                expected_generation: Set(Some(job.expected_generation)),
                expected_remote_epoch: Set(None),
                state: Set(STATE_PENDING.to_owned()),
                attempts: Set(0),
                next_attempt_at: Set(job.next_attempt_at),
                locked_until: Set(None),
                submit_phase: Set((operation == PinJobOperation::Submit)
                    .then(|| SubmitPhase::Ready.persisted().to_owned())),
                last_error: Set(None),
                created_at: Set(now),
                updated_at: Set(now),
            }
        }
        NewPinJob::Remote(job) => {
            let operation = job.operation.operation();
            pin_job::ActiveModel {
                id: Set(job.id),
                operation: Set(operation.persisted().to_owned()),
                provider: Set(job.provider),
                cid: Set(job.cid),
                lease_id: Set(None),
                target_id: Set(None),
                expected_generation: Set(None),
                expected_remote_epoch: Set(Some(job.expected_remote_epoch)),
                state: Set(STATE_PENDING.to_owned()),
                attempts: Set(0),
                next_attempt_at: Set(job.next_attempt_at),
                locked_until: Set(None),
                submit_phase: Set(None),
                last_error: Set(None),
                created_at: Set(now),
                updated_at: Set(now),
            }
        }
    }
}

pub(crate) async fn insert_new_job<C: ConnectionTrait>(
    db: &C,
    job: NewPinJob,
    now: DateTimeUtc,
) -> AppResult<bool> {
    let work = SharedDecodeBudget::default();
    insert_new_job_with_work(db, job, now, DecodeWork::shared(&work)).await
}
pub(crate) async fn insert_new_job_with_work<C: ConnectionTrait>(
    db: &C,
    job: NewPinJob,
    now: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    let operation = match &job {
        NewPinJob::Target(job) => job.operation.operation(),
        NewPinJob::Remote(job) => job.operation.operation(),
    };
    work.copy(&(
        operation.persisted(),
        STATE_PENDING,
        (operation == PinJobOperation::Submit).then_some(SUBMIT_PHASE_READY),
    ))
    .map_err(management_error)?;
    let model = new_job_active_model(job, now);
    let next_attempt_at = *model.next_attempt_at.as_ref();
    work.copy(&(
        model.id.as_ref(),
        model.provider.as_ref(),
        model.cid.as_ref(),
    ))
    .map_err(management_error)?;
    let job_id = model.id.as_ref().to_owned();
    let provider = model.provider.as_ref().to_owned();
    let cid = model.cid.as_ref().to_owned();
    if model.operation.as_ref() == PinJobOperation::Submit.persisted() {
        super::quota::lock_allocation_admission_with_work(db, &provider, work).await?;
    }
    let result = pin_job::Entity::insert(model)
        .on_conflict_do_nothing()
        .exec(db)
        .await?;
    let inserted = matches!(result, TryInsertResult::Inserted(_));
    if inserted {
        super::ledger::capture_invocation_with_work(db, &job_id, &provider, &cid, work).await?;
        validate_inserted_job_times(db, &job_id, now, next_attempt_at, work).await?;
    }
    Ok(inserted)
}

/// Compare against the actual insert inputs before publishing an invocation or receipt.
/// Historical readers use the frozen facts, not a new wall-clock observation.
pub(crate) async fn validate_inserted_job_times<C: ConnectionTrait>(
    db: &C,
    id: &str,
    created: DateTimeUtc,
    next: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<()> {
    work.copy(id).map_err(management_error)?;
    work.index_cells(1, 8).map_err(management_error)?;
    let row=db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT COUNT(*) AS n FROM pin_jobs WHERE id=$1 AND created_at=$2 AND updated_at=$2 AND next_attempt_at=$3",
        [id.into(),created.into(),next.into()])).await?.ok_or_else(||management_error(ManagementError::Confirmation))?;
    if row.try_get::<i64>("", "n")? != 1 {
        return Err(management_error(ManagementError::Confirmation));
    }
    Ok(())
}

async fn ensure_target_job<C: ConnectionTrait>(
    db: &C,
    job: TargetPinJob,
    now: DateTimeUtc,
    submit: bool,
) -> AppResult<EnsureJobOutcome> {
    let work = SharedDecodeBudget::default();
    ensure_target_job_with_work(db, job, now, submit, DecodeWork::shared(&work)).await
}
async fn ensure_target_job_with_work<C: ConnectionTrait>(
    db: &C,
    job: TargetPinJob,
    now: DateTimeUtc,
    submit: bool,
    work: DecodeWork<'_>,
) -> AppResult<EnsureJobOutcome> {
    let Some(existing) = records::current_model::<pin_job::Entity, _>(db, &[&job.id], work)
        .await
        .map_err(management_error)?
    else {
        work.copy(&job.id).map_err(management_error)?;
        let id = job.id.clone();
        return if insert_new_job_with_work(db, NewPinJob::Target(job), now, work).await? {
            Ok(EnsureJobOutcome::Inserted)
        } else {
            target_outcome_after_conflict(db, &id, work).await
        };
    };
    match existing.state.as_str() {
        STATE_PENDING => Ok(EnsureJobOutcome::Pending),
        STATE_RUNNING => Ok(EnsureJobOutcome::Running),
        STATE_DONE => {
            let phase = submit.then_some(SubmitPhase::Ready.persisted());
            reactivate_job(db, &existing.id, job.next_attempt_at, now, phase).await?;
            Ok(EnsureJobOutcome::Reactivated)
        }
        _ => Err(invalid_job("pin job has an invalid persisted state")),
    }
}

async fn target_outcome_after_conflict<C: ConnectionTrait>(
    db: &C,
    id: &str,
    work: DecodeWork<'_>,
) -> AppResult<EnsureJobOutcome> {
    let persisted = records::current_model::<pin_job::Entity, _>(db, &[id], work)
        .await
        .map_err(management_error)?
        .ok_or_else(|| stale_claim_error(id))?;
    match persisted.state.as_str() {
        STATE_PENDING => Ok(EnsureJobOutcome::Pending),
        STATE_RUNNING => Ok(EnsureJobOutcome::Running),
        STATE_DONE => Err(stale_claim_error(id)),
        _ => Err(invalid_job("pin job has an invalid persisted state")),
    }
}

async fn remote_job_outcome_after_conflict<C: ConnectionTrait>(
    db: &C,
    id: &str,
    next_attempt_at: DateTimeUtc,
    now: DateTimeUtc,
) -> AppResult<EnsureJobOutcome> {
    let existing = pin_job::Entity::find_by_id(id.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| stale_claim_error(id))?;
    match existing.state.as_str() {
        STATE_PENDING => Ok(EnsureJobOutcome::Pending),
        STATE_RUNNING => Ok(EnsureJobOutcome::Running),
        STATE_DONE => {
            reactivate_job(db, &existing.id, next_attempt_at, now, None).await?;
            Ok(EnsureJobOutcome::Reactivated)
        }
        _ => Err(invalid_job("pin job has an invalid persisted state")),
    }
}

async fn reactivate_job<C: ConnectionTrait>(
    db: &C,
    id: &str,
    next_attempt_at: DateTimeUtc,
    now: DateTimeUtc,
    submit_phase: Option<&str>,
) -> AppResult<()> {
    let result = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value(STATE_PENDING))
        .col_expr(pin_job::Column::Attempts, Expr::value(0))
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(next_attempt_at))
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
        .filter(pin_job::Column::Id.eq(id))
        .filter(pin_job::Column::State.eq(STATE_DONE))
        .exec(db)
        .await?;
    if result.rows_affected != 1 {
        return Err(stale_claim_error(id));
    }
    Ok(())
}

async fn validate_submit_owner_with_work<C: ConnectionTrait>(
    db: &C,
    job: &TargetPinJob,
    work: DecodeWork<'_>,
) -> AppResult<()> {
    work.copy(&job.id).map_err(management_error)?;
    if job.id
        != stable_submit_id(
            &job.provider,
            &job.cid,
            &job.target_id,
            job.expected_generation,
        )
    {
        return Err(invalid_job("Submit job ID does not match its scope"));
    }
    let canonical = canonical_desired_target_with_work(db, &job.provider, &job.cid, work)
        .await
        .map_err(management_error)?;
    work.copy(&(
        &job.lease_id,
        job.expected_generation,
        &job.provider,
        &job.cid,
    ))
    .map_err(management_error)?;
    let current=db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),"SELECT COUNT(*) AS n FROM pin_leases l WHERE l.id=$1 AND l.generation=$2 AND l.state='active' AND EXISTS (SELECT 1 FROM remote_pins r WHERE r.provider=$3 AND r.cid=$4 AND r.status='reserved' AND r.request_id IS NULL)",[job.lease_id.clone().into(),job.expected_generation.into(),job.provider.clone().into(),job.cid.clone().into()])).await?.ok_or_else(||invalid_job("Submit owner unavailable"))?.try_get::<i64>("","n")?;
    if canonical
        .as_ref()
        .is_none_or(|target| target.id != job.target_id || target.lease_id != job.lease_id)
        || current != 1
    {
        return Err(invalid_job(
            "Submit job is not the canonical reserved target",
        ));
    }
    Ok(())
}

async fn validate_poll_owner<C: ConnectionTrait>(db: &C, job: &TargetPinJob) -> AppResult<()> {
    if !check_target_values_current(db, job).await?
        || canonical_desired_target(db, &job.provider, &job.cid)
            .await?
            .as_ref()
            .map(|target| target.id.as_str())
            != Some(job.target_id.as_str())
    {
        return Err(invalid_job("Poll job is not the canonical desired target"));
    }
    let remote = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(db)
        .await?;
    let Some(remote) = remote else {
        return Err(invalid_job(
            "Poll job does not own a queued or pinning request",
        ));
    };
    let Some(request_id) = remote.request_id.as_deref() else {
        return Err(invalid_job(
            "Poll job does not own a queued or pinning request",
        ));
    };
    if !(matches!(
        remote.status.as_str(),
        REMOTE_STATUS_QUEUED | REMOTE_STATUS_PINNING
    ) || (remote.status == "pinned"
        && crate::store::entities::pin_replica_observation::receipt::retained_cluster(
            db,
            &job.provider,
            &job.cid,
        )
        .await?))
    {
        return Err(invalid_job(
            "Poll job does not own a queued or pinning request",
        ));
    }
    let original =
        super::legacy_migration::original_job_provider(db, &job.id, &job.provider).await?;
    if job.id
        != stable_poll_id(
            &original,
            &job.cid,
            &job.target_id,
            job.expected_generation,
            request_id,
        )
    {
        return Err(invalid_job(
            "Poll job ID does not match the current request",
        ));
    }
    Ok(())
}

async fn validate_remote_epoch<C: ConnectionTrait>(db: &C, job: &RemotePinJob) -> AppResult<()> {
    let remote = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(db)
        .await?;
    if remote.is_none_or(|remote| remote.epoch != job.expected_remote_epoch) {
        return Err(invalid_job("remote job does not name the current epoch"));
    }
    let original =
        super::legacy_migration::original_job_provider(db, &job.id, &job.provider).await?;
    let expected_id = match job.operation {
        RemoteJobOperation::Unpin => {
            format!(
                "unpin:{}:{}:e{}",
                original, job.cid, job.expected_remote_epoch
            )
        }
        RemoteJobOperation::Reconcile => {
            stable_reconcile_id(&original, &job.cid, job.expected_remote_epoch)
        }
    };
    if job.id != expected_id {
        return Err(invalid_job(
            "Reconcile job ID does not match its remote epoch",
        ));
    }
    Ok(())
}

async fn check_target_values_current<C: ConnectionTrait>(
    db: &C,
    job: &TargetPinJob,
) -> AppResult<bool> {
    let Some(target) = pin_lease_target::Entity::find_by_id(job.target_id.clone())
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    let Some(lease) = pin_lease::Entity::find_by_id(job.lease_id.clone())
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    Ok(target.lease_id == job.lease_id
        && target.provider == job.provider
        && target.cid == job.cid
        && is_desired_target_state(&target.state)
        && lease.state == LEASE_STATE_ACTIVE
        && lease.generation == job.expected_generation)
}

async fn remote_is_reserved_without_request<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<bool> {
    Ok(
        remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await?
            .is_some_and(|remote| {
                remote.status == REMOTE_STATUS_RESERVED && remote.request_id.is_none()
            }),
    )
}

async fn canonical_desired_target<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<pin_lease_target::Model>> {
    Ok(pin_lease_target::Entity::find()
        .inner_join(pin_lease::Entity)
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::Cid.eq(cid))
        .filter(pin_lease::Column::State.eq(LEASE_STATE_ACTIVE))
        .filter(pin_lease_target::Column::State.is_in([
            "waiting",
            "submitted",
            "pinned",
            "degraded",
        ]))
        .order_by_asc(pin_lease_target::Column::CreatedAt)
        .order_by_asc(pin_lease_target::Column::Id)
        .one(db)
        .await?)
}

pub(crate) async fn canonical_desired_target_with_work<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    work: DecodeWork<'_>,
) -> super::management::Result<Option<pin_lease_target::Model>> {
    for _ in 0..2 {
        work.copy(&(provider, cid))?;
    }
    let mut targets=records::current_models::<pin_lease_target::Entity,_>(db,"FROM (SELECT pin_lease_targets.* FROM pin_lease_targets INNER JOIN pin_leases ON pin_lease_targets.lease_id=pin_leases.id WHERE pin_lease_targets.provider=$1 AND pin_lease_targets.cid=$2 AND pin_leases.state='active' AND pin_lease_targets.state IN ('waiting','submitted','pinned','degraded') ORDER BY pin_lease_targets.created_at,pin_lease_targets.id LIMIT 1) AS pin_lease_targets WHERE TRUE","created_at,id",vec![provider.into(),cid.into()],1,work).await?;
    Ok(targets.pop())
}

async fn ensure_current_reconcile<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTimeUtc,
) -> AppResult<Option<String>> {
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let NewPinJob::Remote(job) = reconcile_job(provider, cid, remote.epoch, now) else {
        unreachable!("reconcile_job is always remote-scoped")
    };
    let id = job.id.clone();
    ensure_or_reactivate_reconcile_job(db, job, now).await?;
    Ok(Some(id))
}

fn due_claim_condition(now: DateTimeUtc) -> Condition {
    Condition::any()
        .add(
            Condition::all()
                .add(pin_job::Column::State.eq(STATE_PENDING))
                .add(pin_job::Column::NextAttemptAt.lte(now))
                .add(pin_job::Column::LockedUntil.is_null()),
        )
        .add(
            Condition::all()
                .add(pin_job::Column::State.eq(STATE_RUNNING))
                .add(pin_job::Column::LockedUntil.lt(now)),
        )
}

fn is_desired_target_state(state: &str) -> bool {
    matches!(state, "waiting" | "submitted" | "pinned" | "degraded")
}

fn claimed_lock(job: &pin_job::Model) -> AppResult<DateTimeUtc> {
    if job.state != STATE_RUNNING {
        return Err(stale_claim_error(&job.id));
    }
    job.locked_until.ok_or_else(|| stale_claim_error(&job.id))
}

fn validate_claimed_submit(job: &pin_job::Model) -> AppResult<()> {
    if job.operation != PinJobOperation::Submit.persisted() {
        return Err(invalid_job("operation is not Submit"));
    }
    claimed_lock(job).map(|_| ())
}

fn duration_as_chrono(duration: Duration) -> AppResult<ChronoDuration> {
    ChronoDuration::from_std(duration)
        .map_err(|_| AppError::Internal("job delay exceeds chrono range".to_owned()))
}

fn checked_next_attempt(now: DateTimeUtc, delay: Duration) -> AppResult<DateTimeUtc> {
    now.checked_add_signed(duration_as_chrono(delay)?)
        .ok_or_else(|| AppError::Internal("job retry timestamp overflow".to_owned()))
}

pub(crate) fn stable_submit_id(
    provider: &str,
    cid: &str,
    target_id: &str,
    generation: i64,
) -> String {
    format!("submit:{provider}:{cid}:{target_id}:g{generation}")
}

pub(crate) fn stable_poll_id(
    provider: &str,
    cid: &str,
    target_id: &str,
    generation: i64,
    request_id: &str,
) -> String {
    let request_hash = hex::encode(Sha256::digest(request_id.as_bytes()));
    format!("poll:{provider}:{cid}:{target_id}:g{generation}:{request_hash}")
}

pub(crate) fn stable_unpin_id(provider: &str, cid: &str, epoch: i64) -> String {
    format!("unpin:{provider}:{cid}:e{epoch}")
}
pub(crate) fn stable_reconcile_id(provider: &str, cid: &str, remote_epoch: i64) -> String {
    format!("reconcile:{provider}:{cid}:e{remote_epoch}")
}

fn clamped_submit_recovery_backoff(backoff: Duration) -> Duration {
    backoff.clamp(MIN_SUBMIT_RECOVERY_BACKOFF, MAX_SUBMIT_RECOVERY_BACKOFF)
}

fn is_sqlite_contention(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("database is locked") || error.contains("database is busy")
}

async fn sqlite_claim_retry_delay(attempt: usize) {
    let milliseconds = 1_u64.checked_shl(attempt.min(4) as u32).unwrap_or(16);
    tokio::time::sleep(Duration::from_millis(milliseconds)).await;
}

fn bounded_backoff(base: Duration, max: Duration, attempts: i32) -> Duration {
    let cap = base.min(max);
    let mut delay = cap;
    for _ in 1..attempts.max(1) {
        delay = delay.checked_mul(2).unwrap_or(max).min(max);
    }
    delay
}

fn invalid_job(message: &str) -> AppError {
    AppError::InvalidPinningRequest(message.to_owned())
}

fn stale_claim_error(job_id: &str) -> AppError {
    AppError::Internal(format!("stale pin job claim: {job_id}"))
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use sea_orm::{
        ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait,
        PaginatorTrait, QueryFilter,
    };
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::store::entities::pin_job;

    #[tokio::test]
    async fn stage1_review_park_fence_and_history_are_atomic_during_real_takeover() {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("park-race.db")
                .display()
                .to_string()
                .replace('\\', "/")
        );
        let db = Database::connect(&url).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket'); INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('object-1','bucket','key','cid',1,'cid')").await.unwrap();
        seed_target_and_remote(
            &db,
            "lease",
            "target",
            "park-race",
            "cid",
            1,
            "waiting",
            "reserved",
            None,
            1,
            time(0),
        )
        .await;
        enqueue_job(
            &db,
            submit_job("park-race", "cid", "lease", "target", 1, time(0)),
        )
        .await
        .unwrap();
        let claim = claim_due_jobs(&db, time(1), Duration::seconds(30), 1)
            .await
            .unwrap()
            .remove(0);
        let id = claim.model.id.clone();
        record_submit_invocation(&db, &claim, "psa", "cid", time(1))
            .await
            .unwrap();
        let gate = std::sync::Arc::new(ParkTestGate {
            job_id: id.clone(),
            fenced: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        *PARK_TEST_GATE.lock().await = Some(gate.clone());
        let old_db = db.clone();
        let old = tokio::spawn(async move {
            park_submit(
                &old_db,
                &claim,
                "needs_attention",
                "old-owner-error",
                time(2),
            )
            .await
        });
        gate.fenced.notified().await;
        let new_db = Database::connect(&url).await.unwrap();
        let new_id = id.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut successor = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            let claims = claim_due_jobs(&new_db, time(60), Duration::seconds(30), 1).await?;
            if !claims.is_empty() {
                history::Entity::update_many()
                    .col_expr(history::Column::State, Expr::value("active"))
                    .col_expr(
                        history::Column::LastError,
                        Expr::value("new-owner-evidence"),
                    )
                    .filter(history::Column::JobId.eq(new_id))
                    .exec(&new_db)
                    .await?;
            }
            Ok::<_, AppError>(claims.len())
        });
        started_rx.await.unwrap();
        let early =
            tokio::time::timeout(std::time::Duration::from_millis(250), &mut successor).await;
        gate.resume.notify_one();
        let old_result = old.await.unwrap();
        let count = match early {
            Ok(result) => result.unwrap(),
            Err(_) => successor.await.unwrap(),
        };
        let count = match count {
            Ok(count) => count,
            Err(AppError::Database(message)) if message.contains("database is locked") => {
                // SQLite cannot upgrade a deferred reader while the parker owns
                // the write transaction. Retry only after that transaction ends.
                claim_due_jobs(&db, time(60), Duration::seconds(30), 1)
                    .await
                    .unwrap()
                    .len()
            }
            Err(error) => panic!("unexpected takeover failure: {error}"),
        };
        *PARK_TEST_GATE.lock().await = None;
        let persisted = submission_history(&db, &id).await.unwrap().unwrap();
        if count == 1 {
            assert!(old_result.is_err());
            assert_eq!(
                persisted.state, "active",
                "a stale parker must not mutate successor history"
            );
            assert_eq!(persisted.last_error.as_deref(), Some("new-owner-evidence"));
        } else {
            old_result.unwrap();
            assert_eq!(persisted.state, "needs_attention");
        }
    }

    #[tokio::test]
    async fn stage1_review_park_cas_failure_rolls_back_history_with_job() {
        let db = setup().await;
        seed_target_and_remote(
            &db,
            "lease",
            "target",
            "pinata",
            "cid",
            1,
            "waiting",
            "reserved",
            None,
            1,
            time(0),
        )
        .await;
        enqueue_job(
            &db,
            submit_job("pinata", "cid", "lease", "target", 1, time(0)),
        )
        .await
        .unwrap();
        let claim = claim_due_jobs(&db, time(1), Duration::seconds(30), 1)
            .await
            .unwrap()
            .remove(0);
        record_submit_invocation(&db, &claim, "pinata_v3", "cid", time(1))
            .await
            .unwrap();
        let before = submission_history(&db, &claim.model.id).await.unwrap();
        db.execute_unprepared("CREATE TRIGGER reject_park_cas BEFORE UPDATE OF locked_until ON pin_jobs WHEN OLD.locked_until IS NOT NULL AND NEW.locked_until IS NULL BEGIN SELECT RAISE(IGNORE); END").await.unwrap();
        assert!(
            park_submit(&db, &claim, "needs_attention", "must-rollback", time(2))
                .await
                .is_err()
        );
        assert_eq!(
            submission_history(&db, &claim.model.id).await.unwrap(),
            before
        );
        assert_eq!(
            job(&db, &claim.model.id).await.locked_until,
            claim.model.locked_until
        );
    }

    fn time(seconds: i64) -> DateTimeUtc {
        Utc.with_ymd_and_hms(2026, 7, 21, 0, 0, 0).single().unwrap() + Duration::seconds(seconds)
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

    #[allow(clippy::too_many_arguments)]
    async fn seed_target_and_remote(
        db: &DatabaseConnection,
        lease_id: &str,
        target_id: &str,
        provider: &str,
        cid: &str,
        generation: i64,
        target_state: &str,
        remote_status: &str,
        request_id: Option<&str>,
        epoch: i64,
        created_at: DateTimeUtc,
    ) {
        let created_at = created_at.to_rfc3339();
        db.execute_unprepared(&format!(
            "INSERT INTO pin_leases \
             (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
              last_touched_at, expires_at, generation, state) \
             VALUES ('{lease_id}', 'object-1', 'test', 'policy', 'all', 'full', \
                     '{created_at}', '{created_at}', '{created_at}', {generation}, 'active')"
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('{target_id}', '{lease_id}', '{cid}', 1, '{provider}', '{target_state}', \
                     '{created_at}', '{created_at}')"
        ))
        .await
        .unwrap();
        let request_id = request_id
            .map(|value| format!("'{value}'"))
            .unwrap_or_else(|| "NULL".to_owned());
        db.execute_unprepared(&format!(
            "INSERT INTO remote_pins \
             (provider, cid, request_id, cid_size, status, epoch, failure_attempts, last_touched_at) \
             VALUES ('{provider}', '{cid}', {request_id}, 1, '{remote_status}', {epoch}, 0, \
                     '{created_at}')"
        ))
        .await
        .unwrap();
    }

    async fn job(db: &DatabaseConnection, id: &str) -> pin_job::Model {
        pin_job::Entity::find_by_id(id.to_owned())
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn stage1_recovery_budget_parks_unclaimable_without_releasing_responsibility() {
        let db = setup().await;
        seed_target_and_remote(
            &db,
            "lease",
            "target",
            "pinata",
            "cid",
            1,
            "waiting",
            "reserved",
            None,
            1,
            time(0),
        )
        .await;
        enqueue_job(
            &db,
            submit_job("pinata", "cid", "lease", "target", 1, time(0)),
        )
        .await
        .unwrap();
        let id = "submit:pinata:cid:target:g1";
        db.execute_unprepared("UPDATE pin_jobs SET submit_phase='recovering', attempts=7")
            .await
            .unwrap();
        let claimed = claim_due_jobs(&db, time(1), Duration::seconds(30), 1)
            .await
            .unwrap()
            .remove(0);
        retry_submit_recovery(
            &db,
            &claimed,
            time(1),
            std::time::Duration::from_secs(1),
            "protocol error",
        )
        .await
        .unwrap();
        let row = job(&db, id).await;
        assert_eq!(row.state, "running");
        assert_eq!(
            row.locked_until, None,
            "parked claims must not be acquired by old workers"
        );
        assert!(
            claim_due_jobs(&db, time(10000), Duration::seconds(30), 1)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            resolve_no_request_submit_ambiguity(&db, "pinata", "cid", time(10000))
                .await
                .unwrap(),
            NoRequestSubmitAmbiguity::Wait { .. }
        ));
    }

    #[test]
    fn typed_constructors_keep_scopes_phases_and_stable_ids_exact() {
        let now = time(0);
        let NewPinJob::Target(submit) = submit_job("pinata", "bafy", "lease", "target", 7, now)
        else {
            panic!("submit must be target-scoped")
        };
        assert_eq!(submit.id, "submit:pinata:bafy:target:g7");
        assert_eq!(submit.operation, TargetJobOperation::Submit);
        assert_eq!(submit.lease_id, "lease");
        assert_eq!(submit.expected_generation, 7);

        let hash = hex::encode(Sha256::digest(b"request-1"));
        let NewPinJob::Target(poll) =
            poll_job("pinata", "bafy", "lease", "target", 7, "request-1", now)
        else {
            panic!("poll must be target-scoped")
        };
        assert_eq!(poll.id, format!("poll:pinata:bafy:target:g7:{hash}"));
        assert_eq!(poll.operation, TargetJobOperation::Poll);

        let NewPinJob::Remote(unpin) = unpin_job("pinata", "bafy", 9, now) else {
            panic!("unpin must be remote-scoped")
        };
        assert_eq!(unpin.id, "unpin:pinata:bafy:e9");
        assert_eq!(unpin.operation, RemoteJobOperation::Unpin);
        assert_eq!(unpin.expected_remote_epoch, 9);

        let NewPinJob::Remote(reconcile) = reconcile_job("pinata", "bafy", 9, now) else {
            panic!("reconcile must be remote-scoped")
        };
        assert_eq!(reconcile.id, "reconcile:pinata:bafy:e9");
        assert_eq!(reconcile.operation, RemoteJobOperation::Reconcile);
        assert_eq!(POLL_INTERVAL, std::time::Duration::from_secs(5));
    }

    #[tokio::test]
    async fn enqueue_deduplicates_the_stable_id() {
        let db = setup().await;
        let now = time(0);
        let first = submit_job("pinata", "bafy", "lease", "target", 1, now);
        let second = submit_job(
            "pinata",
            "bafy",
            "lease",
            "target",
            1,
            now + Duration::seconds(1),
        );

        enqueue_job(&db, first).await.unwrap();
        enqueue_job(&db, second).await.unwrap();

        assert_eq!(pin_job::Entity::find().count(&db).await.unwrap(), 1);
        let persisted = job(&db, "submit:pinata:bafy:target:g1").await;
        assert_eq!(persisted.operation, "submit");
        assert_eq!(persisted.submit_phase.as_deref(), Some("ready"));
        assert_eq!(persisted.expected_remote_epoch, None);
    }

    #[tokio::test]
    async fn claims_are_exclusive_and_expired_submit_claims_become_recovering() {
        let db = setup().await;
        let now = time(0);
        enqueue_job(&db, submit_job("pinata", "bafy", "lease", "target", 1, now))
            .await
            .unwrap();

        let fresh = claim_due_jobs(&db, now, Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(!fresh.reclaimed);
        assert_eq!(fresh.model.submit_phase.as_deref(), Some("ready"));
        assert!(
            claim_due_jobs(&db, now, Duration::seconds(30), 1)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            claim_due_jobs(&db, now + Duration::seconds(29), Duration::seconds(30), 1)
                .await
                .unwrap()
                .is_empty()
        );

        let reclaimed = claim_due_jobs(&db, now + Duration::seconds(31), Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(reclaimed.reclaimed);
        assert_eq!(reclaimed.model.submit_phase.as_deref(), Some("recovering"));
    }

    #[tokio::test]
    async fn fair_claim_rounds_cover_distinct_providers_before_provider_backlog() {
        let db = setup().await;
        let now = time(0);
        enqueue_job(&db, unpin_job("slow", "bafy-slow-1", 1, now))
            .await
            .unwrap();
        enqueue_job(
            &db,
            unpin_job("slow", "bafy-slow-2", 1, now + Duration::milliseconds(1)),
        )
        .await
        .unwrap();
        enqueue_job(
            &db,
            unpin_job("fast", "bafy-fast", 1, now + Duration::milliseconds(2)),
        )
        .await
        .unwrap();
        enqueue_job(
            &db,
            unpin_job("third", "bafy-third", 1, now + Duration::milliseconds(3)),
        )
        .await
        .unwrap();
        let priorities = BTreeMap::from([
            ("slow".to_owned(), 1),
            ("fast".to_owned(), 2),
            ("third".to_owned(), 3),
        ]);

        let first = claim_due_jobs_fair(
            &db,
            now + Duration::seconds(1),
            Duration::seconds(30),
            2,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &priorities,
        )
        .await
        .unwrap();
        assert_eq!(
            first
                .iter()
                .map(|job| job.model.provider.as_str())
                .collect::<Vec<_>>(),
            vec!["slow", "fast"]
        );

        let occupied = BTreeMap::from([("slow".to_owned(), 1), ("fast".to_owned(), 1)]);
        let service = BTreeMap::from([("slow".to_owned(), 1_u64), ("fast".to_owned(), 2_u64)]);
        let refill = claim_due_jobs_fair(
            &db,
            now + Duration::seconds(1),
            Duration::seconds(30),
            1,
            &occupied,
            &service,
            &priorities,
        )
        .await
        .unwrap();
        assert_eq!(refill.len(), 1);
        assert_eq!(refill[0].model.provider, "third");
    }

    #[tokio::test]
    async fn fair_claim_excludes_locally_active_expired_jobs_without_starving_the_next_candidate() {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("fair-active.db")
                .display()
                .to_string()
                .replace('\\', "/")
        );
        let db = Database::connect(&url).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        let other_worker = Database::connect(&url).await.unwrap();
        enqueue_job(&db, unpin_job("provider", "active", 1, time(0)))
            .await
            .unwrap();
        let active = claim_due_jobs_fair(
            &db,
            time(1),
            Duration::seconds(30),
            1,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        enqueue_job(&db, unpin_job("provider", "next", 1, time(2)))
            .await
            .unwrap();
        let active_job_ids = BTreeSet::from([active.model.id.clone()]);
        let occupancy = BTreeMap::from([("provider".to_owned(), 1)]);
        let refill = claim_due_worker_jobs_fair(
            &db,
            time(60),
            Duration::seconds(30),
            1,
            WorkerClaimSchedule {
                in_flight_by_provider: &occupancy,
                last_served_ticket_by_provider: &BTreeMap::new(),
                configured_priorities: &BTreeMap::new(),
                active_job_ids: &active_job_ids,
            },
        )
        .await
        .unwrap();
        assert_eq!(refill.len(), 1);
        assert_eq!(refill[0].model.cid, "next");
        assert!(!refill[0].reclaimed);
        assert_eq!(job(&db, &active.model.id).await, active.model);
        complete_job(
            &db,
            &refill[0].model.id,
            refill[0].model.locked_until.unwrap(),
            time(60),
        )
        .await
        .unwrap();

        // Local activity is not a durable lock against another worker's recovery.
        let reclaimed = claim_due_jobs_fair(
            &other_worker,
            time(60),
            Duration::seconds(30),
            1,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(reclaimed.model.id, active.model.id);
        assert!(reclaimed.reclaimed);
        assert_ne!(reclaimed.model.locked_until, active.model.locked_until);
        assert!(
            complete_job(
                &db,
                &active.model.id,
                active.model.locked_until.unwrap(),
                time(60),
            )
            .await
            .is_err()
        );
        assert_eq!(job(&db, &active.model.id).await, reclaimed.model);
    }

    #[tokio::test]
    async fn opposite_fair_orders_use_one_global_job_id_update_order() {
        let first_db = setup().await;
        let second_db = setup().await;
        let now = time(0);
        for db in [&first_db, &second_db] {
            enqueue_job(db, unpin_job("a-lock-order", "lock-order-a", 1, now))
                .await
                .unwrap();
            enqueue_job(db, unpin_job("b-lock-order", "lock-order-b", 1, now))
                .await
                .unwrap();
        }
        let priorities = BTreeMap::from([
            ("a-lock-order".to_owned(), 1),
            ("b-lock-order".to_owned(), 1),
        ]);

        let first_log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        *CLAIM_UPDATE_RECORDER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(first_log.clone());
        let first = claim_due_jobs_fair(
            &first_db,
            now,
            Duration::seconds(30),
            2,
            &BTreeMap::new(),
            &BTreeMap::from([
                ("a-lock-order".to_owned(), 0),
                ("b-lock-order".to_owned(), 9),
            ]),
            &priorities,
        )
        .await
        .unwrap();
        *CLAIM_UPDATE_RECORDER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;

        let second_log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        *CLAIM_UPDATE_RECORDER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(second_log.clone());
        let second = claim_due_jobs_fair(
            &second_db,
            now,
            Duration::seconds(30),
            2,
            &BTreeMap::new(),
            &BTreeMap::from([
                ("a-lock-order".to_owned(), 9),
                ("b-lock-order".to_owned(), 0),
            ]),
            &priorities,
        )
        .await
        .unwrap();
        *CLAIM_UPDATE_RECORDER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;

        let first_fair = first
            .iter()
            .map(|job| job.model.provider.as_str())
            .collect::<Vec<_>>();
        let second_fair = second
            .iter()
            .map(|job| job.model.provider.as_str())
            .collect::<Vec<_>>();
        assert_eq!(first_fair, vec!["a-lock-order", "b-lock-order"]);
        assert_eq!(second_fair, vec!["b-lock-order", "a-lock-order"]);

        let only_test_jobs = |log: &std::sync::Mutex<Vec<String>>| {
            log.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|id| id.contains("lock-order"))
                .cloned()
                .collect::<Vec<_>>()
        };
        let first_updates = only_test_jobs(&first_log);
        let second_updates = only_test_jobs(&second_log);
        assert_eq!(first_updates, second_updates);
        assert!(first_updates.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[tokio::test]
    async fn poll_and_reconcile_ensure_reactivate_and_reschedule_in_place() {
        let db = setup().await;
        let now = time(0);
        seed_target_and_remote(
            &db,
            "lease-1",
            "target-1",
            "pinata",
            "bafy",
            3,
            "submitted",
            "queued",
            Some("request-1"),
            4,
            now,
        )
        .await;
        let NewPinJob::Target(poll) = poll_job(
            "pinata",
            "bafy",
            "lease-1",
            "target-1",
            3,
            "request-1",
            now + Duration::seconds(5),
        ) else {
            panic!("expected poll job")
        };
        let poll_id = poll.id.clone();
        assert_eq!(
            ensure_or_reactivate_poll_job(&db, poll, now).await.unwrap(),
            EnsureJobOutcome::Inserted
        );
        db.execute_unprepared(&format!(
            "UPDATE pin_jobs SET attempts = 2 WHERE id = '{poll_id}'"
        ))
        .await
        .unwrap();
        let claimed = claim_due_jobs(&db, now + Duration::seconds(5), Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let next = reschedule_poll_job(
            &db,
            &poll_id,
            claimed.model.locked_until.unwrap(),
            now + Duration::seconds(5),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(next, now + Duration::seconds(6));
        let rescheduled = job(&db, &poll_id).await;
        assert_eq!(rescheduled.state, "pending");
        assert_eq!(rescheduled.attempts, 2);
        assert_eq!(rescheduled.locked_until, None);
        assert_eq!(pin_job::Entity::find().count(&db).await.unwrap(), 1);
        let completed_poll =
            claim_due_jobs(&db, now + Duration::seconds(6), Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
        complete_job(
            &db,
            &poll_id,
            completed_poll.model.locked_until.unwrap(),
            now + Duration::seconds(6),
        )
        .await
        .unwrap();
        let NewPinJob::Target(reactivated_poll) = poll_job(
            "pinata",
            "bafy",
            "lease-1",
            "target-1",
            3,
            "request-1",
            now + Duration::seconds(7),
        ) else {
            panic!("expected poll job")
        };
        assert_eq!(
            ensure_or_reactivate_poll_job(&db, reactivated_poll, now + Duration::seconds(6))
                .await
                .unwrap(),
            EnsureJobOutcome::Reactivated
        );
        assert_eq!(job(&db, &poll_id).await.attempts, 0);
        let completed_poll =
            claim_due_jobs(&db, now + Duration::seconds(7), Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
        complete_job(
            &db,
            &poll_id,
            completed_poll.model.locked_until.unwrap(),
            now + Duration::seconds(7),
        )
        .await
        .unwrap();

        let recl = reconcile_job("pinata", "bafy", 4, now + Duration::seconds(20));
        assert_eq!(
            ensure_or_reactivate_reconcile_job(
                &db,
                match recl.clone() {
                    NewPinJob::Remote(job) => job,
                    NewPinJob::Target(_) => panic!("expected reconcile job"),
                },
                now,
            )
            .await
            .unwrap(),
            EnsureJobOutcome::Inserted
        );
        let NewPinJob::Remote(recl) = recl else {
            panic!("expected reconcile job")
        };
        assert_eq!(
            ensure_or_reactivate_reconcile_job(
                &db,
                RemotePinJob {
                    next_attempt_at: now + Duration::seconds(30),
                    ..recl.clone()
                },
                now,
            )
            .await
            .unwrap(),
            EnsureJobOutcome::Pending
        );
        assert_eq!(
            job(&db, &recl.id).await.next_attempt_at,
            recl.next_attempt_at
        );
        let reconcile_claim = claim_due_jobs(&db, recl.next_attempt_at, Duration::seconds(30), 2)
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.model.id == recl.id)
            .unwrap();
        reschedule_reconcile_job(
            &db,
            &recl.id,
            reconcile_claim.model.locked_until.unwrap(),
            now + Duration::seconds(40),
        )
        .await
        .unwrap();
        assert_eq!(
            job(&db, &recl.id).await.next_attempt_at,
            now + Duration::seconds(40)
        );
        let completed_reconcile =
            claim_due_jobs(&db, now + Duration::seconds(40), Duration::seconds(30), 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
        complete_job(
            &db,
            &recl.id,
            completed_reconcile.model.locked_until.unwrap(),
            now + Duration::seconds(40),
        )
        .await
        .unwrap();
        assert_eq!(
            ensure_or_reactivate_reconcile_job(
                &db,
                RemotePinJob {
                    next_attempt_at: now + Duration::seconds(50),
                    ..recl
                },
                now + Duration::seconds(40),
            )
            .await
            .unwrap(),
            EnsureJobOutcome::Reactivated
        );
    }

    #[tokio::test]
    async fn submit_prepare_and_recovery_preserve_one_stable_owner() {
        let db = setup().await;
        let now = time(0);
        seed_target_and_remote(
            &db, "lease-1", "target-1", "pinata", "bafy", 3, "waiting", "reserved", None, 4, now,
        )
        .await;
        let NewPinJob::Target(submit) = submit_job(
            "pinata",
            "bafy",
            "lease-1",
            "target-1",
            3,
            now + Duration::seconds(90),
        ) else {
            panic!("expected submit job")
        };
        let submit_id = submit.id.clone();
        assert_eq!(
            ensure_or_reactivate_submit_job(&db, submit, now)
                .await
                .unwrap(),
            EnsureJobOutcome::Inserted
        );
        assert_eq!(job(&db, &submit_id).await.next_attempt_at, now);
        let initial = claim_due_jobs(&db, now, Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            prepare_submit_call(&db, &initial, now).await.unwrap(),
            SubmitCallDecision::ReadyToCall
        );
        assert_eq!(
            job(&db, &submit_id).await.submit_phase.as_deref(),
            Some("calling")
        );
        retry_submit_recovery(
            &db,
            &initial,
            now,
            std::time::Duration::from_secs(2),
            "transient find failure",
        )
        .await
        .unwrap();
        let recovering = claim_due_jobs(&db, now + Duration::seconds(2), Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(recovering.model.submit_phase.as_deref(), Some("recovering"));
        assert_eq!(
            record_submit_recovery_no_match(
                &db,
                &recovering,
                now + Duration::seconds(2),
                std::time::Duration::from_secs(3),
            )
            .await
            .unwrap(),
            SubmitRecoveryDecision::RetryScheduled {
                next_attempt_at: now + Duration::seconds(5)
            }
        );
        let backed_off = job(&db, &submit_id).await;
        assert_eq!(backed_off.attempts, 1);
        assert_eq!(backed_off.submit_phase.as_deref(), Some("recovery_backoff"));
        let resumed = claim_due_jobs(&db, now + Duration::seconds(5), Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(!resumed.reclaimed);
        assert_eq!(
            prepare_submit_call(&db, &resumed, now + Duration::seconds(5))
                .await
                .unwrap(),
            SubmitCallDecision::ReadyToCall
        );
    }

    #[tokio::test]
    async fn ensures_reject_forged_ids_and_poll_ids_for_replaced_requests() {
        let db = setup().await;
        let now = time(0);
        seed_target_and_remote(
            &db,
            "lease-1",
            "target-1",
            "pinata",
            "bafy",
            3,
            "submitted",
            "queued",
            Some("request-1"),
            4,
            now,
        )
        .await;
        let NewPinJob::Target(old_poll) = poll_job(
            "pinata",
            "bafy",
            "lease-1",
            "target-1",
            3,
            "request-0",
            now + Duration::seconds(5),
        ) else {
            panic!("expected poll job")
        };
        db.execute_unprepared(
            "UPDATE remote_pins SET request_id = 'request-2' WHERE provider = 'pinata' AND cid = 'bafy'",
        )
        .await
        .unwrap();
        assert!(
            ensure_or_reactivate_poll_job(&db, old_poll, now)
                .await
                .is_err()
        );

        db.execute_unprepared(
            "UPDATE remote_pins SET request_id = NULL, status = 'reserved' \
             WHERE provider = 'pinata' AND cid = 'bafy'",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state = 'waiting' WHERE id = 'target-1'",
        )
        .await
        .unwrap();
        let NewPinJob::Target(mut forged_submit) =
            submit_job("pinata", "bafy", "lease-1", "target-1", 3, now)
        else {
            panic!("expected submit job")
        };
        forged_submit.id = "submit:forged".to_owned();
        assert!(
            ensure_or_reactivate_submit_job(&db, forged_submit, now)
                .await
                .is_err()
        );

        let NewPinJob::Remote(mut forged_reconcile) = reconcile_job("pinata", "bafy", 4, now)
        else {
            panic!("expected reconcile job")
        };
        forged_reconcile.id = "reconcile:forged".to_owned();
        assert!(
            ensure_or_reactivate_reconcile_job(&db, forged_reconcile, now)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn submit_recovery_backoff_is_clamped_and_attempts_remain_capped() {
        let db = setup().await;
        let now = time(0);
        seed_target_and_remote(
            &db, "lease-1", "target-1", "pinata", "bafy", 3, "waiting", "reserved", None, 4, now,
        )
        .await;
        let NewPinJob::Target(submit) = submit_job("pinata", "bafy", "lease-1", "target-1", 3, now)
        else {
            panic!("expected submit job")
        };
        let submit_id = submit.id.clone();
        ensure_or_reactivate_submit_job(&db, submit, now)
            .await
            .unwrap();
        let initial = claim_due_jobs(&db, now, Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        retry_submit_recovery(
            &db,
            &initial,
            now,
            std::time::Duration::ZERO,
            "transient find failure",
        )
        .await
        .unwrap();
        let minimum = job(&db, &submit_id).await;
        assert_eq!(minimum.next_attempt_at, now + Duration::seconds(1));
        assert_eq!(minimum.attempts, 1);
        assert_eq!(minimum.submit_phase.as_deref(), Some("recovering"));

        db.execute_unprepared(&format!(
            "UPDATE pin_jobs SET attempts = 8 WHERE id = '{submit_id}'"
        ))
        .await
        .unwrap();
        let capped = claim_due_jobs(&db, now + Duration::seconds(1), Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        retry_submit_recovery(
            &db,
            &capped,
            now + Duration::seconds(1),
            std::time::Duration::from_secs(999),
            "transient find failure",
        )
        .await
        .unwrap();
        let maximum = job(&db, &submit_id).await;
        assert_eq!(maximum.state, "running");
        assert_eq!(maximum.locked_until, None);
        assert_eq!(maximum.attempts, 8);
        assert_eq!(maximum.submit_phase.as_deref(), Some("recovering"));
    }

    #[tokio::test]
    async fn submit_recovery_at_attempt_cap_parks_without_claimable_lock() {
        let db = setup().await;
        let now = time(0);
        seed_target_and_remote(
            &db, "lease-1", "target-1", "pinata", "bafy", 3, "waiting", "reserved", None, 4, now,
        )
        .await;
        let NewPinJob::Target(submit) = submit_job("pinata", "bafy", "lease-1", "target-1", 3, now)
        else {
            panic!("expected submit job")
        };
        let submit_id = submit.id.clone();
        ensure_or_reactivate_submit_job(&db, submit, now)
            .await
            .unwrap();
        db.execute_unprepared(&format!(
            "UPDATE pin_jobs SET attempts = 8 WHERE id = '{submit_id}'"
        ))
        .await
        .unwrap();
        let claimed = claim_due_jobs(&db, now, Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        retry_submit_recovery(
            &db,
            &claimed,
            now,
            std::time::Duration::from_secs(1),
            "transient find failure",
        )
        .await
        .unwrap();

        let persisted = job(&db, &submit_id).await;
        assert_eq!(persisted.locked_until, None);
        assert_eq!(persisted.state, "running");
        assert_eq!(persisted.submit_phase.as_deref(), Some("recovering"));
        assert_eq!(persisted.attempts, 8);
    }

    #[tokio::test]
    async fn concurrent_file_backed_sqlite_claims_have_one_live_owner() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("pin-job-claims.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(4);
        let db = Database::connect(options).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();

        let now = time(0);
        let NewPinJob::Remote(job_to_enqueue) = reconcile_job("pinata", "bafy", 1, now) else {
            panic!("expected reconcile job")
        };
        let job_id = job_to_enqueue.id.clone();
        enqueue_job(&db, NewPinJob::Remote(job_to_enqueue))
            .await
            .unwrap();

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first_db = db.clone();
        let first_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            claim_due_jobs(&first_db, now, Duration::seconds(30), 1).await
        });
        let second_db = db.clone();
        let second = tokio::spawn(async move {
            barrier.wait().await;
            claim_due_jobs(&second_db, now, Duration::seconds(30), 1).await
        });

        let (first, second) = tokio::join!(first, second);
        let claims = [first.unwrap().unwrap(), second.unwrap().unwrap()];
        assert_eq!(claims.iter().map(Vec::len).sum::<usize>(), 1);
        let persisted = job(&db, &job_id).await;
        assert_eq!(persisted.state, "running");
        assert!(persisted.locked_until > Some(now));
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::State.eq("running"))
                .filter(pin_job::Column::LockedUntil.gt(now))
                .count(&db)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_fair_claimants_make_progress_without_duplicate_jobs() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("fair-pin-job-claims.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(4);
        let db = Database::connect(options).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();

        let now = time(0);
        for provider in ["fair-a", "fair-b", "fair-c"] {
            for suffix in ["1", "2"] {
                enqueue_job(
                    &db,
                    unpin_job(provider, &format!("concurrent-{provider}-{suffix}"), 1, now),
                )
                .await
                .unwrap();
            }
        }
        let priorities = BTreeMap::from([
            ("fair-a".to_owned(), 1),
            ("fair-b".to_owned(), 2),
            ("fair-c".to_owned(), 3),
        ]);
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));

        let first_db = db.clone();
        let first_barrier = barrier.clone();
        let first_priorities = priorities.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            claim_due_jobs_fair(
                &first_db,
                now,
                Duration::seconds(30),
                3,
                &BTreeMap::new(),
                &BTreeMap::from([
                    ("fair-a".to_owned(), 0),
                    ("fair-b".to_owned(), 10),
                    ("fair-c".to_owned(), 20),
                ]),
                &first_priorities,
            )
            .await
        });
        let second_db = db.clone();
        let second = tokio::spawn(async move {
            barrier.wait().await;
            claim_due_jobs_fair(
                &second_db,
                now,
                Duration::seconds(30),
                3,
                &BTreeMap::new(),
                &BTreeMap::from([
                    ("fair-a".to_owned(), 20),
                    ("fair-b".to_owned(), 10),
                    ("fair-c".to_owned(), 0),
                ]),
                &priorities,
            )
            .await
        });

        let (first, second) = tokio::join!(first, second);
        let claimed = first
            .unwrap()
            .unwrap()
            .into_iter()
            .chain(second.unwrap().unwrap())
            .collect::<Vec<_>>();
        let unique = claimed
            .iter()
            .map(|job| job.model.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(claimed.len(), 6, "both claimants must make total progress");
        assert_eq!(unique.len(), claimed.len(), "a job was claimed twice");
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::State.eq(STATE_RUNNING))
                .count(&db)
                .await
                .unwrap(),
            6
        );
    }

    #[tokio::test]
    async fn remote_epoch_guard_blocks_unpin_with_desired_work_and_retry_is_bounded() {
        let db = setup().await;
        let now = time(0);
        seed_target_and_remote(
            &db, "lease-1", "target-1", "pinata", "bafy", 3, "waiting", "reserved", None, 4, now,
        )
        .await;
        let NewPinJob::Remote(unpin) = unpin_job("pinata", "bafy", 4, now) else {
            panic!("expected unpin job")
        };
        let unpin_id = unpin.id.clone();
        enqueue_job(&db, NewPinJob::Remote(unpin)).await.unwrap();
        assert!(
            !check_remote_job_epoch(&db, &job(&db, &unpin_id).await)
                .await
                .unwrap()
        );
        db.execute_unprepared(
            "UPDATE pin_lease_targets SET state = 'released' WHERE id = 'target-1'",
        )
        .await
        .unwrap();
        assert!(
            check_remote_job_epoch(&db, &job(&db, &unpin_id).await)
                .await
                .unwrap()
        );
        let claimed = claim_due_jobs(&db, now, Duration::seconds(30), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let next = retry_job(
            &db,
            &unpin_id,
            claimed.model.locked_until.unwrap(),
            now,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(2),
            2,
            "retryable provider error",
        )
        .await
        .unwrap();
        assert_eq!(next, now + Duration::seconds(1));
        let retried = job(&db, &unpin_id).await;
        assert_eq!((retried.state.as_str(), retried.attempts), ("pending", 1));
        assert_eq!(retried.locked_until, None);
    }

    #[tokio::test]
    async fn target_generation_guard_rejects_stale_work() {
        let db = setup().await;
        let now = time(0);
        seed_target_and_remote(
            &db, "lease-1", "target-1", "pinata", "bafy", 3, "waiting", "reserved", None, 1, now,
        )
        .await;
        enqueue_job(
            &db,
            submit_job("pinata", "bafy", "lease-1", "target-1", 2, now),
        )
        .await
        .unwrap();
        let stale = job(&db, "submit:pinata:bafy:target-1:g2").await;
        assert!(!check_target_job_generation(&db, &stale).await.unwrap());

        enqueue_job(
            &db,
            submit_job("pinata", "bafy", "lease-1", "target-1", 3, now),
        )
        .await
        .unwrap();
        let current = job(&db, "submit:pinata:bafy:target-1:g3").await;
        assert!(check_target_job_generation(&db, &current).await.unwrap());
    }

    #[tokio::test]
    async fn ambiguity_only_clears_never_started_submit_and_waits_for_running_work() {
        let db = setup().await;
        let now = time(0);
        let safe = submit_job("pinata", "bafy", "lease", "target", 1, now);
        enqueue_job(&db, safe).await.unwrap();

        assert_eq!(
            resolve_no_request_submit_ambiguity(&db, "pinata", "bafy", now)
                .await
                .unwrap(),
            NoRequestSubmitAmbiguity::Clear {
                cancelled_never_started: vec!["submit:pinata:bafy:target:g1".to_owned()]
            }
        );
        assert_eq!(job(&db, "submit:pinata:bafy:target:g1").await.state, "done");

        enqueue_job(&db, submit_job("pinata", "bafy", "lease", "target", 2, now))
            .await
            .unwrap();
        let running = claim_due_jobs(&db, now, Duration::seconds(30), 1)
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.model.id.ends_with(":g2"))
            .unwrap();
        assert_eq!(
            resolve_no_request_submit_ambiguity(&db, "pinata", "bafy", now)
                .await
                .unwrap(),
            NoRequestSubmitAmbiguity::Wait {
                next_check_at: running.model.locked_until.unwrap()
            }
        );
        assert_eq!(job(&db, &running.model.id).await.state, "running");
    }
}
