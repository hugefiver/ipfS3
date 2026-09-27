//! Durable identity/ownership evidence layered on the existing remote epoch and
//! capacity row. No network operations or independently invented resource fences.
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, QueryFilter,
    QuerySelect, Set, sea_query::OnConflict,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::{AppError, AppResult},
    pinning::{
        identity::{CleanupMode, ProviderIdentity, ProviderRouteSnapshot},
        provider::{RemotePinStatus, RemoteRef},
    },
    store::entities::{pin_job, pin_provider_route, remote_pin_ledger},
};

pub use crate::pinning::identity::Ownership;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerEffect {
    Reserved,
    Unknown,
    NotCreated,
    Confirmed,
    Retained,
    CleanupPending,
    Absent,
}

impl LedgerEffect {
    fn from_persisted(raw: &str) -> Self {
        match raw {
            "reserved" => Self::Reserved,
            "not_created" => Self::NotCreated,
            "confirmed" => Self::Confirmed,
            "retained" => Self::Retained,
            "cleanup_pending" => Self::CleanupPending,
            "absent" => Self::Absent,
            _ => Self::Unknown,
        }
    }
}

pub async fn register_route<C: ConnectionTrait>(
    db: &C,
    key: &str,
    identity: &ProviderIdentity,
) -> AppResult<()> {
    let snapshot = serde_json::to_string(&identity.route_snapshot())
        .map_err(|_| AppError::Internal("invalid provider snapshot".into()))?;
    pin_provider_route::Entity::insert(pin_provider_route::ActiveModel {
        provider: Set(key.into()),
        snapshot: Set(snapshot),
        display_name: Set(identity.display_name.clone()),
        retired: Set(identity.retired),
    })
    .on_conflict(
        OnConflict::column(pin_provider_route::Column::Provider)
            .update_columns([
                pin_provider_route::Column::Snapshot,
                pin_provider_route::Column::DisplayName,
                pin_provider_route::Column::Retired,
            ])
            .to_owned(),
    )
    .exec(db)
    .await?;
    Ok(())
}

/// Publication callers acquire usage first, then lock every selected registered route
/// until commit. The short I/O preflight uses the same comparison without a lock;
/// only the transaction check is an allocation proof.
pub(crate) async fn verify_selected_routes<C: ConnectionTrait>(
    db: &C,
    expected: &BTreeMap<String, ProviderRouteSnapshot>,
    lock: bool,
) -> AppResult<()> {
    for (key, snapshot) in expected {
        let query = pin_provider_route::Entity::find_by_id(key.clone());
        let route = if lock && db.get_database_backend() == DatabaseBackend::Postgres {
            query.lock_exclusive().one(db).await?
        } else {
            query.one(db).await?
        };
        let route = route.ok_or_else(|| {
            AppError::InvalidPinningRequest("captured provider route is unavailable".into())
        })?;
        let current: ProviderRouteSnapshot =
            serde_json::from_str(&route.snapshot).map_err(|_| {
                AppError::InvalidPinningRequest("registered provider route is invalid".into())
            })?;
        if route.retired || current != *snapshot {
            return Err(AppError::InvalidPinningRequest(
                "captured provider route no longer matches registration".into(),
            ));
        }
    }
    Ok(())
}

pub async fn get<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<remote_pin_ledger::Model>> {
    Ok(
        remote_pin_ledger::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await?,
    )
}

/// Called in the existing quota reservation transaction, before a Submit can exist.
pub async fn capture_reservation<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<()> {
    let Some(route) = pin_provider_route::Entity::find_by_id(provider.to_owned())
        .one(db)
        .await?
    else {
        // Low-level callers without a configured runtime retain the legacy API.
        // Production registers routes before any request is accepted.
        return Ok(());
    };
    if route.retired {
        return Err(AppError::Internal(
            "provider retired before allocation".into(),
        ));
    }
    remote_pin_ledger::Entity::insert(remote_pin_ledger::ActiveModel {
        provider: Set(provider.into()),
        cid: Set(cid.into()),
        route: Set(Some(route.snapshot)),
        ownership: Set("unknown".into()),
        effect: Set("reserved".into()),
        ..Default::default()
    })
    .on_conflict_do_nothing()
    .exec(db)
    .await?;
    Ok(())
}

/// Called only after the original remote CAS has acquired a confirmed-absent
/// resource. Its previous lifetime was archived by confirmed_release.
pub async fn capture_reallocation<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<()> {
    let Some(row) = get(db, provider, cid).await? else {
        return capture_reservation(db, provider, cid).await;
    };
    if row.effect != "absent" {
        return Err(AppError::Internal(
            "absent resource lacks confirmed release evidence".into(),
        ));
    }
    let route = pin_provider_route::Entity::find_by_id(provider.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| AppError::Internal("provider route unavailable for reallocation".into()))?;
    if route.retired {
        return Err(AppError::Internal(
            "retired provider cannot allocate a new resource".into(),
        ));
    }
    let mut active: remote_pin_ledger::ActiveModel = row.into();
    active.route = Set(Some(route.snapshot));
    active.ownership = Set("unknown".into());
    active.effect = Set("reserved".into());
    active.first_observed_at = Set(None);
    active.last_observed_at = Set(None);
    active.remote_pinned_at = Set(None);
    active.gateway_verified_at = Set(None);
    active.content_verified_at = Set(None);
    active.first_error = Set(None);
    active.last_error = Set(None);
    active.update(db).await?;
    Ok(())
}

pub async fn archive_confirmed_release<C: ConnectionTrait>(
    db: &C,
    remote: &crate::store::entities::remote_pin::Model,
) -> AppResult<()> {
    use crate::store::entities::pin_resource_history;
    if let Some(row) = get(db, &remote.provider, &remote.cid).await? {
        let evidence = serde_json::json!({ "ledger": row, "request_id": remote.request_id, "status_before_release": remote.status, "confirmed_absent": true }).to_string();
        pin_resource_history::Entity::insert(pin_resource_history::ActiveModel {
            provider: Set(remote.provider.clone()),
            cid: Set(remote.cid.clone()),
            epoch: Set(remote.epoch),
            ledger: Set(evidence),
        })
        .on_conflict_do_nothing()
        .exec(db)
        .await?;
    }
    Ok(())
}

pub async fn capture_invocation<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    provider: &str,
    cid: &str,
) -> AppResult<()> {
    use crate::store::entities::{pin_invocation_route, remote_pin};
    let Some(route) = get(db, provider, cid).await?.and_then(|row| row.route) else {
        return Ok(());
    };
    let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?
        .ok_or_else(|| AppError::Internal("invocation remote missing".into()))?;
    pin_invocation_route::Entity::insert(pin_invocation_route::ActiveModel {
        job_id: Set(job_id.into()),
        route: Set(route.clone()),
        remote_epoch: Set(remote.epoch),
    })
    .on_conflict_do_nothing()
    .exec(db)
    .await?;
    let captured = pin_invocation_route::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| AppError::Internal("invocation route missing".into()))?;
    if captured.route != route || captured.remote_epoch != remote.epoch {
        return Err(AppError::Internal(
            "invocation identity cannot change".into(),
        ));
    }
    Ok(())
}

/// A Submit's captured epoch is a reference CAS, not a resource lifetime ID.
/// Only the original desired target may follow same-lifetime reference attaches;
/// an archived confirmed release separates lifetimes even when the route is equal.
/// Called inside the claimed Submit's prepare/invocation transaction, after its
/// job fence and before the previous not-created effect is overwritten.
pub(crate) async fn advance_submit_invocation_epoch<C: ConnectionTrait>(
    db: &C,
    claimed: &super::jobs::ClaimedPinJob,
    api: &str,
    strategy: &str,
) -> AppResult<()> {
    use crate::store::entities::{pin_invocation_route, pin_resource_history, remote_pin};
    let job = &claimed.model;
    if job.operation != "submit" {
        return Err(AppError::Internal(
            "only Submit may advance its invocation epoch".into(),
        ));
    }
    let ledger = get(db, &job.provider, &job.cid).await?;
    // Preserve the low-level pre-registry store API; production always registers
    // provider routes, so an absent snapshot there is not a safe fallback.
    if ledger.as_ref().and_then(|row| row.route.as_ref()).is_none()
        && legacy_unregistered_store(db, &job.provider).await?
    {
        return Ok(());
    }
    let ledger =
        ledger.ok_or_else(|| AppError::Internal("Submit resource route missing".into()))?;
    let captured = pin_invocation_route::Entity::find_by_id(job.id.clone())
        .one(db)
        .await?
        .ok_or_else(|| AppError::Internal("Submit invocation route missing".into()))?;
    // The job claim is already fenced; lock remote before inspecting history so
    // a concurrent confirmed release cannot cross the archive boundary unseen.
    let query = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()));
    let remote = if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    }
    .ok_or_else(|| AppError::Internal("Submit remote missing".into()))?;
    if ledger.route.as_deref() != Some(captured.route.as_str())
        || serde_json::from_str::<ProviderRouteSnapshot>(&captured.route).is_err()
    {
        return Err(AppError::Internal(
            "invocation identity cannot change".into(),
        ));
    }
    if captured.remote_epoch == remote.epoch {
        return Ok(());
    }
    let history = super::jobs::submission_history(db, &job.id).await?;
    let prior_call_is_safe = match history {
        // A fresh ready claim has not yet issued HTTP. Missing history on a
        // recovery/repair claim is not evidence that its previous call was safe.
        None => {
            !claimed.reclaimed
                && claimed.previous_state == "pending"
                && job.submit_phase.as_deref() == Some("ready")
                && job.attempts == 0
        }
        Some(history) => {
            history.effect == "not_created"
                && history.state == "active"
                && history.submit_calls < 8
                && history.api == api
                && history.strategy == strategy
        }
    };
    let released = pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.eq(&job.provider))
        .filter(pin_resource_history::Column::Cid.eq(&job.cid))
        .filter(pin_resource_history::Column::Epoch.gte(captured.remote_epoch))
        .filter(pin_resource_history::Column::Epoch.lt(remote.epoch))
        .one(db)
        .await?
        .is_some();
    if captured.remote_epoch > remote.epoch
        || remote.status != "reserved"
        || remote.request_id.is_some()
        || released
        || !prior_call_is_safe
        || !super::jobs::check_target_job_generation(db, job).await?
    {
        return Err(AppError::Internal(
            "invocation identity cannot change".into(),
        ));
    }
    let updated = pin_invocation_route::Entity::update_many()
        .col_expr(
            pin_invocation_route::Column::RemoteEpoch,
            sea_orm::sea_query::Expr::value(remote.epoch),
        )
        .filter(pin_invocation_route::Column::JobId.eq(&job.id))
        .filter(pin_invocation_route::Column::Route.eq(&captured.route))
        .filter(pin_invocation_route::Column::RemoteEpoch.eq(captured.remote_epoch))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(AppError::Internal(
            "invocation identity cannot change".into(),
        ));
    }
    Ok(())
}

pub fn decode_route(row: &remote_pin_ledger::Model) -> Option<ProviderRouteSnapshot> {
    row.route
        .as_deref()
        .and_then(|route| serde_json::from_str(route).ok())
}

pub async fn route_matches<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    identity: Option<&ProviderIdentity>,
) -> AppResult<bool> {
    let Some(row) = get(db, provider, cid).await? else {
        return legacy_unregistered_store(db, provider).await;
    };
    Ok(decode_route(&row)
        .zip(identity)
        .is_some_and(|(route, identity)| route_compatible(route, identity)))
}

fn route_compatible(mut route: ProviderRouteSnapshot, identity: &ProviderIdentity) -> bool {
    // Historical API/strategy/cleanup are executed from the snapshot, never
    // replaced by current TOML. Account/credential/endpoint changes isolate.
    route.api_profile.clone_from(&identity.api_profile);
    route.strategy.clone_from(&identity.strategy);
    route.cleanup = identity.cleanup;
    identity.validate_route_snapshot(&route).is_ok()
}

/// Every IO-capable non-Submit job uses the route captured on creation. Legacy
/// jobs without a capture cannot borrow a later lifetime's allocation route.
pub(crate) async fn job_route_matches<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    identity: Option<&ProviderIdentity>,
) -> AppResult<bool> {
    use crate::store::entities::{pin_invocation_route, pin_resource_history, remote_pin};
    let row = get(db, &job.provider, &job.cid).await?;
    let Some(row) = row else {
        return legacy_unregistered_store(db, &job.provider).await;
    };
    let Some(captured) = pin_invocation_route::Entity::find_by_id(job.id.clone())
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    let remote = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(db)
        .await?;
    let route = serde_json::from_str::<ProviderRouteSnapshot>(&captured.route).ok();
    let Some(remote) = remote else {
        return Ok(false);
    };
    if job
        .expected_remote_epoch
        .is_some_and(|epoch| epoch != captured.remote_epoch)
        || route.as_ref() != decode_route(&row).as_ref()
        || !route
            .zip(identity)
            .is_some_and(|(route, identity)| route_compatible(route, identity))
    {
        return Ok(false);
    }
    if remote.epoch == captured.remote_epoch {
        return Ok(true);
    }
    // Poll IDs are intentionally stable across same-lifetime reference attaches.
    // Never recapture a pending/running Poll from the new allocation: its original
    // route and epoch remain the immutable lower bound for confirmed releases.
    if job.operation != "poll"
        || captured.remote_epoch > remote.epoch
        || job.expected_remote_epoch.is_some()
        || !matches!(remote.status.as_str(), "queued" | "pinning")
    {
        return Ok(false);
    }
    let Some(request_id) = remote.request_id.as_deref() else {
        return Ok(false);
    };
    if !super::jobs::poll_continuity_owner(db, job, request_id).await? {
        return Ok(false);
    }
    let released = pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.eq(&job.provider))
        .filter(pin_resource_history::Column::Cid.eq(&job.cid))
        .filter(pin_resource_history::Column::Epoch.gte(captured.remote_epoch))
        .filter(pin_resource_history::Column::Epoch.lt(remote.epoch))
        .one(db)
        .await?
        .is_some();
    if released {
        return Ok(false);
    }
    // The worker checks this on the claimed job and again immediately before
    // GET; its exact claim fence and request-id CAS still guard provider I/O
    // and the subsequent observation. A concurrent new attach may advance
    // the epoch again without invalidating this lifetime.
    Ok(true)
}

pub(crate) async fn job_route<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
) -> AppResult<Option<ProviderRouteSnapshot>> {
    use crate::store::entities::pin_invocation_route;
    Ok(pin_invocation_route::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await?
        .and_then(|captured| serde_json::from_str(&captured.route).ok()))
}

/// A cancelled owner's Poll can prove the request's origin, but cannot itself
/// run for another target. Called with the current desired lifecycle prelocked;
/// read historical evidence without acquiring any old lease/target/job locks.
pub(crate) async fn has_historical_poll_request<C: ConnectionTrait>(
    db: &C,
    remote: &crate::store::entities::remote_pin::Model,
    route: &str,
) -> AppResult<bool> {
    use crate::store::entities::{pin_invocation_route, pin_resource_history};
    let Some(request_id) = remote.request_id.as_deref() else {
        return Ok(false);
    };
    let polls = pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(&remote.provider))
        .filter(pin_job::Column::Cid.eq(&remote.cid))
        .filter(pin_job::Column::Operation.eq("poll"))
        .all(db)
        .await?;
    for poll in polls {
        if !super::jobs::poll_names_request(&poll, request_id) {
            continue;
        }
        let Some(captured) = pin_invocation_route::Entity::find_by_id(poll.id)
            .one(db)
            .await?
        else {
            continue;
        };
        if captured.route != route || captured.remote_epoch > remote.epoch {
            continue;
        }
        if pin_resource_history::Entity::find()
            .filter(pin_resource_history::Column::Provider.eq(&remote.provider))
            .filter(pin_resource_history::Column::Cid.eq(&remote.cid))
            .filter(pin_resource_history::Column::Epoch.gte(captured.remote_epoch))
            .filter(pin_resource_history::Column::Epoch.lte(remote.epoch))
            .one(db)
            .await?
            .is_none()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn mark_effect<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    effect: &str,
) -> AppResult<()> {
    if let Some(row) = get(db, provider, cid).await? {
        if matches!(
            row.effect.as_str(),
            "confirmed" | "retained" | "cleanup_pending"
        ) && matches!(effect, "unknown" | "not_created" | "reserved")
        {
            return Ok(());
        }
        let mut active: remote_pin_ledger::ActiveModel = row.into();
        active.effect = Set(effect.into());
        active.update(db).await?;
    }
    Ok(())
}

/// This is evidence of a response to our own submit/correlation, not permission
/// inferred from a CID search. External adoption must use ExternalExisting.
pub async fn observe<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    status: RemotePinStatus,
    ownership: Ownership,
    now: DateTime<Utc>,
) -> AppResult<()> {
    observe_inner(db, provider, cid, status, ownership, now, None).await
}

/// Called only for a live, claimed provider response after the ordered desired
/// lifecycle snapshot has projected the status in this same transaction.
/// `has_desired_refs` comes from that prelocked snapshot: never acquire target
/// locks after the remote status write just to confirm the ledger.
pub(crate) async fn observe_claimed<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    request_id: &str,
    has_desired_refs: bool,
    status: RemotePinStatus,
    ownership: Ownership,
    now: DateTime<Utc>,
) -> AppResult<()> {
    observe_inner(
        db,
        &job.provider,
        &job.cid,
        status,
        ownership,
        now,
        Some((job, request_id, has_desired_refs)),
    )
    .await
}

async fn observe_inner<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    status: RemotePinStatus,
    ownership: Ownership,
    now: DateTime<Utc>,
    claim: Option<(&pin_job::Model, &str, bool)>,
) -> AppResult<()> {
    if let Some(row) = get(db, provider, cid).await? {
        let confirmed_retained = if status == RemotePinStatus::Pinned && row.effect == "retained" {
            match claim {
                Some((job, request_id, true)) => {
                    retained_confirmation_matches(db, &row, job, request_id).await?
                }
                _ => false,
            }
        } else {
            false
        };
        let mut active: remote_pin_ledger::ActiveModel = row.clone().into();
        if row.first_observed_at.is_none() {
            active.first_observed_at = Set(Some(now));
        }
        active.last_observed_at = Set(Some(now));
        if status == RemotePinStatus::Pinned && row.remote_pinned_at.is_none() {
            active.remote_pinned_at = Set(Some(now));
        }
        if row.ownership == "unknown" && row.route.is_some() {
            active.ownership = Set(match ownership {
                Ownership::ApplicationCreated => "application_created",
                Ownership::ExternalExisting => "external_existing",
                Ownership::Unknown => "unknown",
            }
            .into());
        }
        // A queued/failed response confirms a request identifier, not a pin.
        // Preserve historical confirmation and a retained lifetime on later
        // degraded observations; never free unknown/retained capacity here.
        if status == RemotePinStatus::Pinned
            && (confirmed_retained
                || !matches!(row.effect.as_str(), "retained" | "cleanup_pending"))
        {
            active.effect = Set("confirmed".into());
        } else if row.effect == "reserved" && status != RemotePinStatus::Pinned {
            active.effect = Set("unknown".into());
        }
        active.update(db).await?;
    }
    Ok(())
}

async fn retained_confirmation_matches<C: ConnectionTrait>(
    db: &C,
    row: &remote_pin_ledger::Model,
    job: &pin_job::Model,
    request_id: &str,
) -> AppResult<bool> {
    use crate::store::entities::{pin_invocation_route, pin_resource_history, remote_pin};
    if !matches!(job.operation.as_str(), "submit" | "poll")
        || job.provider != row.provider
        || job.cid != row.cid
    {
        return Ok(false);
    }
    let captured = pin_invocation_route::Entity::find_by_id(job.id.clone())
        .one(db)
        .await?;
    let remote = remote_pin::Entity::find_by_id((row.provider.clone(), row.cid.clone()))
        .one(db)
        .await?;
    let configured = pin_provider_route::Entity::find_by_id(row.provider.clone())
        .one(db)
        .await?;
    let (Some(captured), Some(remote), Some(configured)) = (captured, remote, configured) else {
        return Ok(false);
    };
    let (Ok(mut route), Ok(current)) = (
        serde_json::from_str::<ProviderRouteSnapshot>(&captured.route),
        serde_json::from_str::<ProviderRouteSnapshot>(&configured.snapshot),
    ) else {
        return Ok(false);
    };
    route.api_profile.clone_from(&current.api_profile);
    route.strategy.clone_from(&current.strategy);
    route.cleanup = current.cleanup;
    if row.route.as_deref() != Some(captured.route.as_str())
        || route != current
        || captured.remote_epoch > remote.epoch
        || job
            .expected_remote_epoch
            .is_some_and(|epoch| epoch != remote.epoch)
        || remote.status != "pinned"
        || remote.request_id.as_deref() != Some(request_id)
    {
        return Ok(false);
    }
    // A Poll's original epoch can precede reference-only attaches, but cannot
    // cross a confirmed release into another resource lifetime.
    let released = pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.eq(&row.provider))
        .filter(pin_resource_history::Column::Cid.eq(&row.cid))
        .filter(pin_resource_history::Column::Epoch.gte(captured.remote_epoch))
        .filter(pin_resource_history::Column::Epoch.lt(remote.epoch))
        .one(db)
        .await?
        .is_some();
    Ok(!released)
}

pub async fn cleanup_allowed<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<bool> {
    let Some(row) = get(db, provider, cid).await? else {
        return legacy_unregistered_store(db, provider).await;
    };
    Ok(row.ownership == "application_created"
        && decode_route(&row).is_some_and(|route| route.cleanup == CleanupMode::Managed))
}

// Retain the pre-registry low-level store API used by legacy embedders. The
// gateway always registers identities before publication: once initialized,
// even a missing ledger row for a legacy name is unknown and fails closed.
async fn legacy_unregistered_store<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<bool> {
    Ok(!provider.starts_with("domain:")
        && pin_provider_route::Entity::find().one(db).await?.is_none())
}

#[derive(Debug, Serialize)]
pub struct ResourceStatus {
    pub cid: String,
    pub route: Option<ProviderRouteSnapshot>,
    pub display_name: Option<String>,
    pub remote_ref: Option<RemoteRef>,
    pub active_references: usize,
    pub remote_status: String,
    pub ownership: Ownership,
    pub effect: LedgerEffect,
    pub epoch: i64,
    pub first_observed_at: Option<DateTime<Utc>>,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub remote_pinned_at: Option<DateTime<Utc>>,
    pub gateway_verified_at: Option<DateTime<Utc>>,
    pub content_verified_at: Option<DateTime<Utc>>,
    pub first_error: Option<String>,
    pub last_error: Option<String>,
}

/// Read-only bounded status for doctor/RPC integration. Historical confirmation
/// remains separate from the latest status; remote pinned is not verification.
pub async fn status<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<ResourceStatus>> {
    let Some(snapshot) = super::leases::remote_work_snapshot(db, provider, cid).await? else {
        return Ok(None);
    };
    let remote = snapshot.remote;
    let ledger = get(db, provider, cid).await?;
    let configured = pin_provider_route::Entity::find_by_id(provider.to_owned())
        .one(db)
        .await?;
    let route = ledger.as_ref().and_then(decode_route);
    let ownership = ledger
        .as_ref()
        .map(|r| Ownership::from_persisted(&r.ownership))
        .unwrap_or(Ownership::Unknown);
    let remote_ref = route
        .clone()
        .zip(remote.request_id.clone())
        .map(|(route, opaque_id)| RemoteRef {
            resource_type: route.resource_type(),
            cid: remote.cid.clone(),
            opaque_id,
            route,
            ownership,
        });
    Ok(Some(ResourceStatus {
        cid: remote.cid,
        route,
        remote_ref,
        active_references: snapshot.desired.len(),
        display_name: configured.map(|r| r.display_name),
        remote_status: remote.status,
        ownership,
        effect: ledger
            .as_ref()
            .map(|r| LedgerEffect::from_persisted(&r.effect))
            .unwrap_or(LedgerEffect::Unknown),
        epoch: remote.epoch,
        first_observed_at: ledger.as_ref().and_then(|r| r.first_observed_at),
        last_observed_at: ledger.as_ref().and_then(|r| r.last_observed_at),
        remote_pinned_at: ledger.as_ref().and_then(|r| r.remote_pinned_at),
        gateway_verified_at: ledger.as_ref().and_then(|r| r.gateway_verified_at),
        content_verified_at: ledger.as_ref().and_then(|r| r.content_verified_at),
        first_error: ledger.as_ref().and_then(|r| r.first_error.clone()),
        last_error: ledger.and_then(|r| r.last_error).or(remote.last_error_text),
    }))
}

#[derive(Debug, Serialize)]
pub struct LeaseStatus {
    pub owner_object_id: String,
    pub local_metadata_published: bool,
    pub lease_state: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub any_provider_available: bool,
    pub all_targets_pinned: bool,
    pub targets: usize,
    pub pinned_targets: usize,
}

/// Availability is not the all-provider completion contract, and an expired
/// intent does not become active merely because the retained resource is pinned.
pub async fn lease_status<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
) -> AppResult<Option<LeaseStatus>> {
    use crate::store::entities::{object, pin_lease, pin_lease_target, remote_pin};
    let Some(lease) = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
        .all(db)
        .await?;
    let mut pinned = 0;
    let mut available_cids = std::collections::BTreeSet::new();
    for target in &targets {
        if target.state != "pinned" {
            continue;
        }
        let remote = remote_pin::Entity::find_by_id((target.provider.clone(), target.cid.clone()))
            .one(db)
            .await?;
        if !remote.is_some_and(|remote| remote.status == "pinned") {
            continue;
        }
        let evidence = get(db, &target.provider, &target.cid).await?;
        let confirmed = if let Some(evidence) = evidence {
            decode_route(&evidence).is_some() && evidence.effect == "confirmed"
        } else {
            legacy_unregistered_store(db, &target.provider).await?
        };
        if confirmed {
            pinned += 1;
            available_cids.insert(&target.cid);
        }
    }
    let cids: std::collections::BTreeSet<_> = targets.iter().map(|t| &t.cid).collect();
    let active = lease.state == "active";
    let local_metadata_published = object::Entity::find_by_id(lease.owner_object_id.clone())
        .one(db)
        .await?
        .is_some();
    Ok(Some(LeaseStatus {
        owner_object_id: lease.owner_object_id,
        local_metadata_published,
        lease_state: lease.state,
        created_at: lease.created_at,
        expires_at: lease.expires_at,
        any_provider_available: active && !cids.is_empty() && cids == available_cids,
        all_targets_pinned: active && !targets.is_empty() && targets.len() == pinned,
        targets: targets.len(),
        pinned_targets: pinned,
    }))
}

pub async fn record_error<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    evidence: &str,
) -> AppResult<()> {
    use sea_orm::sea_query::{Expr, Func};
    remote_pin_ledger::Entity::update_many()
        .col_expr(
            remote_pin_ledger::Column::FirstError,
            Func::coalesce([
                Expr::col(remote_pin_ledger::Column::FirstError).into(),
                Expr::value(evidence),
            ])
            .into(),
        )
        .col_expr(remote_pin_ledger::Column::LastError, Expr::value(evidence))
        .filter(remote_pin_ledger::Column::Provider.eq(provider))
        .filter(remote_pin_ledger::Column::Cid.eq(cid))
        .exec(db)
        .await?;
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct InvocationSnapshot {
    pub job_id: String,
    pub correlation: String,
    pub operation: String,
    pub cid: String,
    pub route: Option<ProviderRouteSnapshot>,
    pub actual_api: Option<String>,
    pub actual_strategy: Option<String>,
    pub effect: String,
    pub state: String,
    pub submit_calls: i32,
    pub recovery_queries: i32,
    pub started_at: Option<DateTime<Utc>>,
    pub first_error: Option<String>,
    pub last_error: Option<String>,
}

/// Joins Stage 1's actual invocation evidence with the immutable allocation
/// identity. A missing historical route stays None; current config is not used.
pub async fn invocation_snapshot<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
) -> AppResult<Option<InvocationSnapshot>> {
    use crate::store::entities::{pin_invocation_route, pin_job};
    let Some(job) = pin_job::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let history = super::jobs::submission_history(db, job_id).await?;
    let captured = pin_invocation_route::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await?;
    let route = captured.and_then(|captured| serde_json::from_str(&captured.route).ok());
    Ok(Some(InvocationSnapshot {
        job_id: job.id.clone(),
        correlation: job.id,
        operation: job.operation,
        cid: job.cid,
        route,
        actual_api: history.as_ref().map(|h| h.api.clone()),
        actual_strategy: history.as_ref().map(|h| h.strategy.clone()),
        effect: history
            .as_ref()
            .map(|h| h.effect.clone())
            .unwrap_or_else(|| "unknown".into()),
        state: history
            .as_ref()
            .map(|h| h.state.clone())
            .unwrap_or(job.state),
        submit_calls: history.as_ref().map_or(0, |h| h.submit_calls),
        recovery_queries: history.as_ref().map_or(0, |h| h.recovery_queries),
        started_at: history.as_ref().map(|h| h.started_at),
        first_error: history.as_ref().and_then(|h| h.first_error.clone()),
        last_error: history.and_then(|h| h.last_error).or(job.last_error),
    }))
}
