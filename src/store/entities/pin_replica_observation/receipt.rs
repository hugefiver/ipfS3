//! Historical replica facts. Deserializing these does not create adapter proof.
//! Retirement closes allocation, not compatible reads or original-lifetime settlement.
use super::super::pin_lease_requirement::contract::read_route_compatible;
use super::{ActiveModel, Column, Entity};
use crate::{
    error::{AppError, AppResult},
    pinning::{
        identity::{ProviderRouteSnapshot, RemoteResourceType},
        provider::PinObservation,
    },
    store::{
        entities::{pin_invocation_route, pin_provider_route, remote_pin},
        pinning::{
            jobs::{self, ClaimedPinJob},
            ledger,
        },
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

pub(crate) struct ReplicaReceipt {
    pub projectable: bool,
    pub complete: bool,
}

pub async fn is_cluster<C: ConnectionTrait>(db: &C, job_id: &str) -> AppResult<bool> {
    Ok(ledger::job_route(db, job_id)
        .await?
        .is_some_and(|route| route.resource_type() == RemoteResourceType::ClusterPin))
}

pub async fn retained_cluster<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<bool> {
    Ok(ledger::get(db, provider, cid)
        .await?
        .as_ref()
        .and_then(ledger::decode_route)
        .is_some_and(|route| route.resource_type() == RemoteResourceType::ClusterPin))
}

pub(crate) async fn append<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    observation: &PinObservation,
    request: &str,
    phase: &str,
    now: DateTime<Utc>,
) -> AppResult<ReplicaReceipt> {
    let job = &claimed.model;
    let Some(capture) = pin_invocation_route::Entity::find_by_id(job.id.clone())
        .one(db)
        .await?
    else {
        return Ok(ReplicaReceipt {
            projectable: false,
            complete: false,
        });
    };
    let route: ProviderRouteSnapshot = serde_json::from_str(&capture.route)
        .map_err(|_| AppError::Internal("invalid replica invocation route".into()))?;
    if route.resource_type() != RemoteResourceType::ClusterPin {
        return Ok(ReplicaReceipt {
            projectable: false,
            complete: false,
        });
    }
    let until = job
        .locked_until
        .ok_or_else(|| AppError::Internal("replica claim missing".into()))?;
    let live = jobs::fence_job_claim(db, &job.id, until).await?;
    let remote = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(db)
        .await?;
    let registered_query = pin_provider_route::Entity::find_by_id(job.provider.clone());
    let registered = if db.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        use sea_orm::QuerySelect;
        registered_query.lock_shared().one(db).await?
    } else {
        registered_query.one(db).await?
    };
    let registered_matches = registered
        .and_then(|row| serde_json::from_str::<ProviderRouteSnapshot>(&row.snapshot).ok())
        .is_some_and(|current| read_route_compatible(&route, &current));
    let projectable = live
        && registered_matches
        && jobs::check_target_job_generation(db, job).await?
        && remote
            .as_ref()
            .is_some_and(|remote| remote.epoch >= capture.remote_epoch)
        && (phase != "poll"
            || remote.as_ref().and_then(|row| row.request_id.as_deref()) == Some(request))
        && ledger::submission::projection_allowed(
            db,
            job,
            remote
                .as_ref()
                .ok_or_else(|| AppError::Internal("replica remote missing".into()))?,
        )
        .await?
        && unreleased(db, &job.provider, &job.cid, capture.remote_epoch).await?;
    let outcome = if phase == "poll" {
        super::validation::outcome(observation, &route, &job.cid, request, now)
    } else {
        "Unknown"
    };
    Entity::insert(ActiveModel {
        sequence: Default::default(),
        job_id: Set(job.id.clone()),
        provider: Set(job.provider.clone()),
        expected_cid: Set(job.cid.clone()),
        route: Set(capture.route),
        remote_epoch: Set(capture.remote_epoch),
        claim_until: Set(until),
        lease_id: Set(job.lease_id.clone()),
        target_id: Set(job.target_id.clone()),
        expected_generation: Set(job.expected_generation),
        phase: Set(phase.into()),
        resources: Set(serde_json::to_string(&observation.resources)
            .map_err(|_| AppError::Internal("replica evidence serialization".into()))?),
        outcome: Set(outcome.into()),
        safe_error: Set(observation
            .result
            .as_ref()
            .err()
            .map(|error| error.safe_evidence("replica_poll"))),
        projectable: Set(projectable),
        observed_at: Set(now),
    })
    .exec(db)
    .await?;
    Ok(ReplicaReceipt {
        projectable,
        complete: outcome == "Complete",
    })
}

async fn unreleased<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    epoch: i64,
) -> AppResult<bool> {
    Ok(
        !crate::store::pinning::legacy_migration::release_history(db, provider, cid)
            .await?
            .iter()
            .any(|row| row.epoch >= epoch),
    )
}

pub async fn fresh_complete<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    route: &ProviderRouteSnapshot,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let operational =
        crate::store::pinning::legacy_migration::resource_key(db, provider, cid).await?;
    let provider = operational.as_str();
    let registered = pin_provider_route::Entity::find_by_id(provider.to_owned())
        .one(db)
        .await?;
    if !registered
        .and_then(|row| serde_json::from_str::<ProviderRouteSnapshot>(&row.snapshot).ok())
        .is_some_and(|current| read_route_compatible(route, &current))
    {
        return Ok(false);
    }
    let keys = crate::store::pinning::legacy_migration::historical_keys(
        db,
        "pin_replica_observations",
        provider,
        Some(cid),
    )
    .await?;
    let rows = Entity::find()
        .filter(Column::Provider.is_in(keys))
        .filter(Column::ExpectedCid.eq(cid))
        .filter(Column::Projectable.eq(true))
        .order_by_desc(Column::Sequence)
        .all(db)
        .await?;
    let mut row = None;
    for candidate in rows {
        let pk = [("sequence".into(), candidate.sequence.to_string())].into();
        let mapped = crate::store::pinning::legacy_migration::resolve_legacy_binding(
            db,
            "pin_replica_observations",
            &pk,
            &candidate.provider,
        )
        .await?;
        if candidate.provider == provider || mapped.is_some_and(|m| m.operational_key == provider) {
            row = Some(candidate);
            break;
        }
    }
    let Some(row) = row else { return Ok(false) };
    let Ok(recorded) = serde_json::from_str::<ProviderRouteSnapshot>(&row.route) else {
        return Ok(false);
    };
    if row.phase != "poll"
        || !matches!(row.outcome.as_str(), "Complete" | "Insufficient")
        || !read_route_compatible(route, &recorded)
        || !unreleased(db, provider, cid, row.remote_epoch).await?
    {
        return Ok(false);
    }
    let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(db)
        .await?;
    let Some(remote) = remote.filter(|remote| {
        remote.epoch >= row.remote_epoch && matches!(remote.status.as_str(), "pinned" | "pinning")
    }) else {
        return Ok(false);
    };
    let Ok(resources) =
        serde_json::from_str::<Vec<crate::pinning::provider::ObservedResource>>(&row.resources)
    else {
        return Ok(false);
    };
    let [resource] = resources.as_slice() else {
        return Ok(false);
    };
    Ok(
        remote.request_id.as_deref() == Some(resource.request_id.as_str())
        && super::validation::evidence_valid(resource, route, cid, now)
        // A later lease can demand stricter freshness, never revive facts after
        // the captured reader's own max-age expired by deserializing history.
        && super::validation::evidence_valid(resource, &recorded, cid, now)
        && resource.replica_evidence.as_ref().is_some_and(|e|
            e.pinned_peers.len() >= super::validation::birth_threshold(resource, route).unwrap_or(usize::MAX)),
    )
}
