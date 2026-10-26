//! RPC submission evidence is independent of terminal job and lease lifetimes.
use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, Iterable, QueryFilter, QueryOrder,
    Statement, TransactionTrait,
    sea_query::{Expr, Iden},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use sea_orm::Set;

use super::{
    super::jobs::{self, ClaimedPinJob},
    submission_entity as evidence,
};
use crate::{
    error::{AppError, AppResult},
    pinning::{
        identity::{CanonicalResourceKey, Ownership, ProviderRouteSnapshot, RemoteResourceType},
        provider::{
            ObservedResource, ObservedResourceStatus, ProviderError, RemotePinStatus, SubmitEffect,
            SubmitObservation, canonical_resource_cid, cids_equivalent,
        },
    },
    store::entities::{
        pin_invocation_route, pin_job, pin_lease, pin_lease_target, pin_provider_route,
        pin_resource_history, remote_pin, remote_pin_ledger,
    },
};

#[path = "submission/bounded.rs"]
mod bounded;
pub(crate) use bounded::{latest_rpc, latest_rpc_with_work, rejected_non_rpc_call_with_work};

#[cfg(test)]
#[path = "submission_context_tests.rs"]
mod context_tests;

#[cfg(test)]
#[path = "submission/late_tests.rs"]
mod late_tests;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEvidence {
    pub resource: ObservedResource,
    pub key: Option<CanonicalResourceKey>,
}

pub(crate) fn resource_evidence_for_route(
    route: &ProviderRouteSnapshot,
    resource: ObservedResource,
) -> ResourceEvidence {
    let key = canonical_resource_cid(&resource.cid)
        .ok()
        .map(|cid| CanonicalResourceKey {
            backend: route.backend.clone(),
            scope: route.scope.clone(),
            resource_type: resource.resource_type,
            resource_id: cid,
        });
    ResourceEvidence { resource, key }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObservationProtocol {
    RpcReceipt,
    NonRpcObservation,
}

pub(crate) fn observation_protocol(
    route: &ProviderRouteSnapshot,
) -> AppResult<ObservationProtocol> {
    crate::pinning::worker::invocation_route_for_snapshot(route).ok_or_else(|| {
        AppError::InvalidPinningRequest("unsupported historical observation route".into())
    })?;
    Ok(if is_rpc(route) {
        ObservationProtocol::RpcReceipt
    } else {
        ObservationProtocol::NonRpcObservation
    })
}

pub fn is_rpc(route: &ProviderRouteSnapshot) -> bool {
    matches!(
        route.resource_type(),
        RemoteResourceType::RpcPin | RemoteResourceType::ClusterPin
    )
}

pub fn is_rpc_api(api: &str) -> bool {
    matches!(
        api,
        "rpc" | "kubo" | "filebase-rpc" | "cluster-proxy" | "cluster-rest"
    )
}

pub fn typed_error(error: &ProviderError, effect: SubmitEffect) -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(&error.safe_evidence("submit")).expect("safe evidence is JSON");
    value["effect"] = match effect {
        SubmitEffect::NotSubmitted => "not_created",
        SubmitEffect::Observed => "observed",
        SubmitEffect::Unknown => "unknown",
    }
    .into();
    value.to_string()
}

pub async fn latest<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
) -> AppResult<Option<evidence::Model>> {
    let relation = fact_relation(db).await?;
    db.query_one(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!(
            "SELECT * FROM {} WHERE job_id=$1 ORDER BY submit_call DESC LIMIT 1",
            relation.table
        ),
        [job_id.into()],
    ))
    .await?
    .map(|row| evidence::Model::from_query_result(&row, "").map_err(Into::into))
    .transpose()
}

/// Must share the exact claim/invocation transaction before provider I/O.
pub async fn begin<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTime<Utc>,
) -> AppResult<Option<String>> {
    let Some(captured) = pin_invocation_route::Entity::find_by_id(claimed.model.id.clone())
        .one(db)
        .await?
    else {
        return Ok(None); // legacy low-level callers have no configured registry
    };
    let route: ProviderRouteSnapshot = serde_json::from_str(&captured.route)
        .map_err(|_| AppError::Internal("invalid captured invocation route".into()))?;
    if !is_rpc(&route) {
        return Ok(None);
    }
    let relation = fact_relation(db).await?;
    if db.query_one(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!("SELECT id FROM {} WHERE provider=$1 AND expected_cid=$2 AND needs_attention=$3 LIMIT 1", relation.table),
        [claimed.model.provider.clone().into(), claimed.model.cid.clone().into(), true.into()],
    )).await?
        .is_some()
    {
        return Err(AppError::InvalidPinningRequest(
            "RPC resource has an unresolved invocation; repeated POST is unsafe".into(),
        ));
    }
    let history = jobs::submission_history(db, &claimed.model.id)
        .await?
        .ok_or_else(|| AppError::Internal("RPC invocation has no history".into()))?;
    let claim_until = claimed
        .model
        .locked_until
        .ok_or_else(|| AppError::Internal("RPC invocation has no claim".into()))?;
    let id = format!("{}:call:{}", claimed.model.id, history.submit_calls);
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!("INSERT INTO {} (id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,effect,resources,outcome,safe_error,needs_attention,started_at,observed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'unknown','[]','in_flight',NULL,$9,$10,NULL)", relation.table),
        vec![id.clone().into(), claimed.model.id.clone().into(), history.submit_calls.into(), claimed.model.provider.clone().into(), claimed.model.cid.clone().into(), captured.route.into(), captured.remote_epoch.into(), claim_until.into(), true.into(), now.into()],
    )).await?;
    Ok(Some(id))
}

/// No quota admission here: evidence for already dispatched effects cannot be
/// rejected because capacity is full. The original logical reservation remains.
/// A stale claim/lifetime may append its own evidence, but cannot project status.
/// Atomic with the caller's transaction; a nested savepoint is not a durable
/// receipt commit. Worker completion uses the Store-owned fact transaction.
pub async fn record<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    id: &str,
    observation: &SubmitObservation,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let txn = db.begin().await?;
    let result = record_in_transaction(&txn, claimed, id, observation, now).await;
    match result {
        Ok(can_project) => {
            txn.commit().await?;
            Ok(can_project)
        }
        Err(error) => {
            txn.rollback().await?;
            Err(error)
        }
    }
}

/// An immutable invocation identity captured before dispatch. It conveys no
/// current business authority and cannot be constructed from a provider response.
#[derive(Clone)]
pub(crate) struct InvocationFact {
    row: evidence::Model,
    relation: FactRelation,
}

#[cfg(test)]
tokio::task_local! {
    static RPC_AUTHORITY_SQL_FAILURE: ();
}

#[derive(Clone)]
struct FactRelation {
    table: String,
    postgres_identity: Option<(String, i64)>,
}

async fn fact_relation<C: ConnectionTrait>(db: &C) -> AppResult<FactRelation> {
    if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
        return Ok(FactRelation {
            table: "\"main\".\"pin_submit_observations\"".into(),
            postgres_identity: None,
        });
    }
    // Resolve only a permanent application relation, in the configured schema
    // path. A connection-local TEMP copy must never capture or receive facts.
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT n.nspname::text AS schema_name, c.oid::bigint AS relation_oid \
         FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace \
         WHERE c.relname='pin_submit_observations' AND c.relpersistence='p' \
         AND c.relkind IN ('r','p') AND n.nspname=ANY(pg_catalog.current_schemas(false)) \
         ORDER BY pg_catalog.array_position(pg_catalog.current_schemas(false),n.nspname) LIMIT 1",
        ))
        .await?
        .ok_or_else(|| AppError::Internal("persistent RPC receipt relation missing".into()))?;
    let schema: String = row.try_get("", "schema_name")?;
    let oid: i64 = row.try_get("", "relation_oid")?;
    Ok(FactRelation {
        table: format!(
            "\"{}\".\"pin_submit_observations\"",
            schema.replace('"', "\"\"")
        ),
        postgres_identity: Some((schema, oid)),
    })
}

pub(crate) async fn capture_fact<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    id: &str,
) -> AppResult<InvocationFact> {
    let claim = claimed
        .model
        .locked_until
        .ok_or_else(|| AppError::Internal("RPC claim missing".into()))?;
    let relation = fact_relation(db).await?;
    // Compare timestamps in the database's native representation, including PG
    // microsecond precision. Never substitute a later worker's claim.
    let row = db.query_one(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!("SELECT * FROM {} WHERE id=$1 AND job_id=$2 AND provider=$3 AND expected_cid=$4 AND claim_until=$5", relation.table),
        [id.into(), claimed.model.id.clone().into(), claimed.model.provider.clone().into(), claimed.model.cid.clone().into(), claim.into()],
    )).await?.ok_or_else(|| AppError::Internal("RPC observation invocation identity mismatch".into()))?;
    let row = evidence::Model::from_query_result(&row, "")?;
    let route: ProviderRouteSnapshot = serde_json::from_str(&row.route)
        .map_err(|_| AppError::Internal("RPC observation historical route invalid".into()))?;
    if row.id != format!("{}:call:{}", row.job_id, row.submit_call) || !is_rpc(&route) {
        return Err(AppError::Internal(
            "RPC observation invocation identity mismatch".into(),
        ));
    }
    Ok(InvocationFact { row, relation })
}

/// Owns and commits the outermost transaction. No runtime catalog, management
/// admission, lifecycle, release-context read or wake is part of this fact write.
pub(crate) async fn complete_fact(
    store: &crate::store::Store,
    invocation: &InvocationFact,
    observation: &SubmitObservation,
    now: DateTime<Utc>,
) -> AppResult<evidence::Model> {
    let tx = store.db().begin().await?;
    let row = write_fact(&tx, invocation, observation, now).await?;
    tx.commit().await?;
    Ok(row)
}

async fn record_in_transaction<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    id: &str,
    observation: &SubmitObservation,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let invocation = capture_fact(db, claimed, id).await?;
    let row = write_fact(db, &invocation, observation, now).await?;
    let can_project = project_receipt(db, claimed, &row, now, None, true).await?;
    if matches!(row.outcome.as_str(), "matched" | "accepted") {
        if can_project {
            db.execute(Statement::from_sql_and_values(
                db.get_database_backend(),
                format!(
                    "UPDATE {} SET needs_attention=$1 WHERE id=$2",
                    invocation.relation.table
                ),
                [false.into(), id.into()],
            ))
            .await?;
        } else {
            wake_matching_parked(db, &claimed.model.id, now).await?;
        }
    }
    Ok(can_project)
}

async fn write_fact<C: ConnectionTrait>(
    db: &C,
    invocation: &InvocationFact,
    observation: &SubmitObservation,
    now: DateTime<Utc>,
) -> AppResult<evidence::Model> {
    let row = &invocation.row;
    let route: ProviderRouteSnapshot = serde_json::from_str(&row.route)
        .map_err(|_| AppError::Internal("RPC observation historical route invalid".into()))?;
    let resources: Vec<_> = observation
        .resources
        .iter()
        .cloned()
        .map(|mut resource| {
            // No RPC state supplied by Stage 5 is ApplicationCreated proof.
            if matches!(
                resource.resource_type,
                RemoteResourceType::RpcPin | RemoteResourceType::ClusterPin
            ) || resource.ownership == Ownership::ApplicationCreated
            {
                resource.ownership = Ownership::Unknown;
            }
            resource_evidence_for_route(&route, resource)
        })
        .collect();
    let keys: std::collections::BTreeSet<_> =
        resources.iter().filter_map(|r| r.key.as_ref()).collect();
    let matches_expected = resources.iter().all(|r| {
        r.resource.resource_type == route.resource_type()
            && cids_equivalent(&r.resource.cid, &row.expected_cid).unwrap_or(false)
    });
    let cluster = route.resource_type() == RemoteResourceType::ClusterPin;
    let successful = observation.result.as_ref().is_ok_and(|remote| {
        !remote.request_id.is_empty()
            && resources
                .iter()
                .all(|r| r.resource.request_id == remote.request_id)
            && (remote.status == RemotePinStatus::Pinned
                || (cluster && remote.status == RemotePinStatus::Pinning))
            && cids_equivalent(&remote.cid, &row.expected_cid).unwrap_or(false)
    }) && observation.effect == SubmitEffect::Observed
        && keys.len() == 1
        && matches_expected
        && resources.iter().all(|r| {
            r.key.is_some()
                && if cluster {
                    matches!(
                        r.resource.status,
                        ObservedResourceStatus::PinAccepted
                            | ObservedResourceStatus::ReplicasVerified
                    )
                } else {
                    r.resource.status == ObservedResourceStatus::RecursiveVerified
                }
        });
    let safe_not_submitted = observation.effect == SubmitEffect::NotSubmitted
        && resources.is_empty()
        && observation.result.is_err();
    let mismatch = !matches_expected || keys.len() > 1;
    let outcome = if successful {
        if cluster { "accepted" } else { "matched" }
    } else if safe_not_submitted {
        "not_submitted"
    } else if mismatch {
        "cid_mismatch"
    } else {
        "unknown"
    };
    let effect = if safe_not_submitted {
        "not_created"
    } else if observation.effect == SubmitEffect::Observed {
        "observed"
    } else {
        "unknown"
    };
    // Class-derived error evidence is diagnostic only. Override its effect with
    // the typed write evidence (HTTP 400/403 after dispatch remains unknown).
    let effective = if observation.effect == SubmitEffect::NotSubmitted && !safe_not_submitted {
        SubmitEffect::Unknown
    } else {
        observation.effect
    };
    let safe_error = observation
        .result
        .as_ref()
        .err()
        .map(|error| typed_error(error, effective));
    let json = serde_json::to_string(&resources)
        .map_err(|_| AppError::Internal("RPC evidence serialization failed".into()))?;
    let mut sql = format!(
        "UPDATE {} SET effect=$1, resources=$2, outcome=$3, safe_error=$4, needs_attention=$5, observed_at=$6 WHERE id=$7 AND job_id=$8 AND submit_call=$9 AND provider=$10 AND expected_cid=$11 AND route=$12 AND remote_epoch=$13 AND claim_until=$14 AND started_at=$15 AND observed_at IS NULL",
        invocation.relation.table
    );
    let mut values = vec![
        effect.into(),
        json.clone().into(),
        outcome.into(),
        safe_error.clone().into(),
        (!safe_not_submitted).into(),
        now.into(),
        row.id.clone().into(),
        row.job_id.clone().into(),
        row.submit_call.into(),
        row.provider.clone().into(),
        row.expected_cid.clone().into(),
        row.route.clone().into(),
        row.remote_epoch.into(),
        row.claim_until.into(),
        row.started_at.into(),
    ];
    if let Some((schema, oid)) = &invocation.relation.postgres_identity {
        sql.push_str(" AND EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid::bigint=$16 AND n.nspname::text=$17 AND c.relname='pin_submit_observations' AND c.relpersistence='p')");
        values.push((*oid).into());
        values.push(schema.clone().into());
    }
    let written = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            values,
        ))
        .await?;
    if written.rows_affected() != 1 {
        return Err(AppError::Internal(
            "RPC invocation observation already recorded or identity changed".into(),
        ));
    }
    let mut completed = row.clone();
    completed.effect = effect.into();
    completed.resources = json;
    completed.outcome = outcome.into();
    completed.safe_error = safe_error;
    completed.needs_attention = !safe_not_submitted;
    completed.observed_at = Some(now);
    Ok(completed)
}

pub(crate) async fn project_runtime_receipt(
    tx: &crate::store::RuntimeTransaction,
    claimed: &ClaimedPinJob,
    row: &evidence::Model,
    now: DateTime<Utc>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    project_receipt(
        tx.db(),
        claimed,
        row,
        now,
        Some((tx.catalog(work), work)),
        true,
    )
    .await
}

/// The worker retains this complete ordered frontier for the ensuing status
/// projection. Do not reacquire a partial lease frontier after its remote lock.
pub(crate) async fn project_runtime_observation_receipt(
    tx: &crate::store::RuntimeTransaction,
    claimed: &ClaimedPinJob,
    row: &evidence::Model,
    now: DateTime<Utc>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
    _frontier: &crate::store::pinning::leases::LockedWorkerObservation<'_>,
) -> AppResult<bool> {
    project_receipt(
        tx.db(),
        claimed,
        row,
        now,
        Some((tx.catalog(work), work)),
        false,
    )
    .await
}

async fn project_receipt<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    row: &evidence::Model,
    now: DateTime<Utc>,
    runtime: Option<(
        crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
        crate::store::pinning::management::records::DecodeWork<'_>,
    )>,
    lock_lifecycle: bool,
) -> AppResult<bool> {
    let claim = claimed
        .model
        .locked_until
        .ok_or_else(|| AppError::Internal("RPC claim missing".into()))?;
    let desired = if lock_lifecycle {
        lock_receipt_lifecycle(db, &claimed.model, now).await?
    } else if claimed.model.operation == "submit"
        && jobs::check_target_job_generation(db, &claimed.model).await?
    {
        // The complete frontier already holds every desired lease and target.
        // Read the original TTL without acquiring a new lock after the remote.
        match claimed.model.lease_id.as_ref() {
            Some(id) => pin_lease::Entity::find_by_id(id.clone())
                .one(db)
                .await?
                .is_some_and(|lease| lease.expires_at > now),
            None => false,
        }
    } else {
        false
    };
    let live = jobs::fence_job_claim(db, &claimed.model.id, claim).await?;
    let current_job = jobs::lock_submit_receipt(db, &claimed.model.id).await?;
    let remote = lock_receipt_remote(db, &claimed.model.provider, &claimed.model.cid).await?;
    let matched_context = match (current_job.as_ref(), remote.as_ref()) {
        (Some(job), Some(remote)) => {
            receipt_context_core(
                db,
                row,
                job,
                remote,
                matching_request(row).as_deref(),
                runtime,
            )
            .await?
        }
        _ => false,
    };
    // Current authority can deny projection, never erase the invocation fact.
    let managed_projection = async {
        #[cfg(test)]
        if RPC_AUTHORITY_SQL_FAILURE.try_with(|()| ()).is_ok() {
            db.execute_unprepared("SELECT * FROM rpc_receipt_missing_authority_relation")
                .await?;
        }
        match runtime {
        Some((catalog, work)) => {
            crate::store::pinning::management::actions::job_authority::projection_allowed_catalog(
                catalog,
                &claimed.model,
                work.budget().clone(),
            )
            .await
        }
        None => {
            crate::store::pinning::management::actions::job_authority::projection_allowed(
                db,
                &claimed.model,
            )
            .await
        }
        }
    }
    .await?;
    let can_project = desired && live && matched_context && managed_projection;
    Ok(can_project)
}

/// Lock the original lease → target before the job/remote receipt tail. Failed
/// lifecycle fences deny projection without discarding an already-dispatched fact.
pub(crate) async fn lock_receipt_lifecycle<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let (Some(lease), Some(target), Some(generation)) = (
        job.lease_id.as_deref(),
        job.target_id.as_deref(),
        job.expected_generation,
    ) else {
        return Ok(false);
    };
    if job.operation != "submit" || job.expected_remote_epoch.is_some() {
        return Ok(false);
    }
    let locked_lease = pin_lease::Entity::update_many()
        .col_expr(
            pin_lease::Column::Generation,
            Expr::col(pin_lease::Column::Generation).into(),
        )
        .filter(pin_lease::Column::Id.eq(lease))
        .filter(pin_lease::Column::State.eq("active"))
        .filter(pin_lease::Column::Generation.eq(generation))
        .filter(pin_lease::Column::ExpiresAt.gt(now))
        .exec(db)
        .await?;
    if locked_lease.rows_affected != 1 {
        return Ok(false);
    }
    let target = pin_lease_target::Entity::update_many()
        .col_expr(
            pin_lease_target::Column::State,
            Expr::col(pin_lease_target::Column::State).into(),
        )
        .filter(pin_lease_target::Column::Id.eq(target))
        .filter(pin_lease_target::Column::LeaseId.eq(lease))
        .filter(pin_lease_target::Column::Provider.eq(&job.provider))
        .filter(pin_lease_target::Column::Cid.eq(&job.cid))
        .filter(pin_lease_target::Column::State.is_in([
            "waiting",
            "submitted",
            "pinned",
            "degraded",
        ]))
        .exec(db)
        .await?;
    Ok(target.rows_affected == 1)
}

async fn lock_receipt_remote<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<remote_pin::Model>> {
    remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::Epoch,
            Expr::col(remote_pin::Column::Epoch).into(),
        )
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Cid.eq(cid))
        .exec(db)
        .await?;
    Ok(
        remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await?,
    )
}

/// Revalidate complete roots/request and their historical canonical scope;
/// `matched` alone is not an authorization to replay a malformed receipt.
fn matching_request(row: &evidence::Model) -> Option<String> {
    if !matches!(row.outcome.as_str(), "matched" | "accepted")
        || row.effect != "observed"
        || row.observed_at.is_none()
    {
        return None;
    }
    let route: ProviderRouteSnapshot = serde_json::from_str(&row.route).ok()?;
    let family = route.resource_type();
    if (family == RemoteResourceType::ClusterPin) != (row.outcome == "accepted") {
        return None;
    }
    let resources: Vec<ResourceEvidence> = serde_json::from_str(&row.resources).ok()?;
    let request = &resources.first()?.resource.request_id;
    if request.is_empty() {
        return None;
    }
    let expected = canonical_resource_cid(&row.expected_cid).ok()?;
    let key = CanonicalResourceKey {
        backend: route.backend,
        scope: route.scope,
        resource_type: family,
        resource_id: expected,
    };
    resources
        .iter()
        .all(|resource| {
            resource.key.as_ref() == Some(&key)
                && resource.resource.resource_type == family
                && if family == RemoteResourceType::ClusterPin {
                    matches!(
                        resource.resource.status,
                        ObservedResourceStatus::PinAccepted
                            | ObservedResourceStatus::ReplicasVerified
                    )
                } else {
                    resource.resource.status == ObservedResourceStatus::RecursiveVerified
                }
                && resource.resource.request_id == *request
                && cids_equivalent(&resource.resource.cid, &row.expected_cid).unwrap_or(false)
        })
        .then(|| request.clone())
}

async fn receipt_context_core<C: ConnectionTrait>(
    db: &C,
    row: &evidence::Model,
    job: &crate::store::entities::pin_job::Model,
    remote: &remote_pin::Model,
    request: Option<&str>,
    runtime: Option<(
        crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
        crate::store::pinning::management::records::DecodeWork<'_>,
    )>,
) -> AppResult<bool> {
    let pk = [("id".into(), row.id.clone())].into();
    let operational = match runtime {
        Some((catalog, work)) => crate::store::pinning::legacy_migration::resolve_with_catalog(
            "pin_submit_observations",
            &pk,
            &row.provider,
            &work,
            catalog,
        )
        .await
        .map_err(crate::store::pinning::management::admission::work_error)?
        .map_or_else(|| row.provider.clone(), |binding| binding.operational_key),
        None => {
            crate::store::pinning::legacy_migration::captured_provider(
                db,
                "pin_submit_observations",
                &pk,
                &row.provider,
            )
            .await?
        }
    };
    if job.id != row.job_id
        || job.operation != "submit"
        || job.provider != operational
        || job.cid != row.expected_cid
        || remote.provider != operational
        || remote.cid != row.expected_cid
        || remote.epoch < row.remote_epoch
        || remote.status == "absent"
        || request.is_some_and(|request| {
            remote
                .request_id
                .as_deref()
                .is_some_and(|existing| existing != request)
        })
        || !jobs::check_target_job_generation(db, job).await?
    {
        return Ok(false);
    }
    let Some(captured) = pin_invocation_route::Entity::find_by_id(row.job_id.clone())
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    if captured.route != row.route || captured.remote_epoch != row.remote_epoch {
        return Ok(false);
    }
    let route: ProviderRouteSnapshot = serde_json::from_str(&row.route)
        .map_err(|_| AppError::Internal("invalid RPC replay route".into()))?;
    let Some(history) = jobs::submission_history(db, &row.job_id).await? else {
        return Ok(false);
    };
    if history.submit_calls != row.submit_call
        || (history.api != route.api_profile && history.api != "rpc")
        || history.strategy != route.strategy
        // NULL retains the documented original-job-ID correlation fallback.
        // A present-but-empty correlation is not valid historical evidence.
        || history
            .correlation
            .as_ref()
            .is_some_and(|value| value.is_empty())
    {
        return Ok(false);
    }
    let registered =
        crate::store::entities::pin_provider_route::Entity::find_by_id(operational.clone())
            .one(db)
            .await?;
    if !registered.is_some_and(|registered| {
        !registered.retired && registered_invocation_matches(&row.route, &registered.snapshot)
    }) || match runtime {
        Some((catalog, work)) => {
            super::get_catalog(catalog, &row.provider, &row.expected_cid, work).await?
        }
        None => super::get(db, &row.provider, &row.expected_cid).await?,
    }
    .is_none_or(|ledger| ledger.route.as_deref() != Some(row.route.as_str()))
    {
        return Ok(false);
    }
    let releases = match runtime {
        Some((catalog, work)) => crate::store::pinning::legacy_migration::release_history_catalog(
            catalog,
            &operational,
            &row.expected_cid,
            work,
        )
        .await
        .map_err(crate::store::pinning::management::admission::work_error)?,
        None => {
            crate::store::pinning::legacy_migration::release_history(
                db,
                &operational,
                &row.expected_cid,
            )
            .await?
        }
    };
    Ok(!releases
        .iter()
        .any(|history| history.epoch >= row.remote_epoch))
}

/// Both receipt-arrival and parking call this inside their existing transaction.
/// No remote mutation occurs here: only a new scheduler claim may project/settle.
pub(crate) async fn wake_matching_parked<C: ConnectionTrait>(
    db: &C,
    id: &str,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    wake_parked_core(db, id, now, None).await
}

pub(crate) async fn wake_runtime_parked(
    tx: &crate::store::RuntimeTransaction,
    id: &str,
    now: DateTime<Utc>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    wake_parked_core(tx.db(), id, now, Some((tx.catalog(work), work))).await
}

async fn wake_parked_core<C: ConnectionTrait>(
    db: &C,
    id: &str,
    now: DateTime<Utc>,
    runtime: Option<(
        crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
        crate::store::pinning::management::records::DecodeWork<'_>,
    )>,
) -> AppResult<bool> {
    // Unlocked lookup discovers the lifecycle frontier, never authorization.
    let Some(snapshot) = pin_job::Entity::find_by_id(id.to_owned()).one(db).await? else {
        return Ok(false);
    };
    // The generic park caller already owns the job lock. Only the fresh runtime
    // wake takes lifecycle writer locks here, in lease → target → job order.
    if runtime.is_some() && !lock_receipt_lifecycle(db, &snapshot, now).await? {
        return Ok(false);
    }
    let Some(job) = jobs::lock_submit_receipt(db, id).await? else {
        return Ok(false);
    };
    if job.lease_id != snapshot.lease_id
        || job.target_id != snapshot.target_id
        || job.expected_generation != snapshot.expected_generation
        || job.provider != snapshot.provider
        || job.cid != snapshot.cid
    {
        return Ok(false);
    }
    let Some(remote) = lock_receipt_remote(db, &job.provider, &job.cid).await? else {
        return Ok(false);
    };
    let Some(row) = latest(db, id).await? else {
        return Ok(false);
    };
    let Some(request) = matching_request(&row) else {
        return Ok(false);
    };
    if job.state != "running"
        || job.locked_until.is_some()
        || job.submit_phase.as_deref() != Some("recovering")
        || job.last_error.as_deref() != Some(jobs::RPC_RECOVERY_PARK_REASON)
        || jobs::submission_history(db, id)
            .await?
            .is_none_or(|history| history.state != "needs_attention")
        || !receipt_context_core(db, &row, &job, &remote, Some(&request), runtime).await?
    {
        return Ok(false);
    }
    let authority = match runtime {
        Some((catalog, work)) => {
            crate::store::pinning::management::actions::job_authority::projection_allowed_catalog(
                catalog,
                &job,
                work.budget().clone(),
            )
            .await?
        }
        None => {
            crate::store::pinning::management::actions::job_authority::projection_allowed(db, &job)
                .await?
        }
    };
    if !authority {
        return Ok(false);
    }
    jobs::wake_rpc_receipt(db, &job, row.submit_call, now).await
}

/// Primary-key keyset paging bounds the rows examined, not merely the number
/// of matches returned. Cursor wrap revisits transiently unavailable receipts;
/// no restart depends on an in-memory notification or a persisted cursor.
pub(crate) async fn parked_receipt_candidates<C: ConnectionTrait>(
    db: &C,
    after: &str,
    limit: u64,
) -> AppResult<(Option<String>, Vec<pin_job::Model>)> {
    let relation = fact_relation(db).await?;
    let jobs_table = if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
        "\"main\".\"pin_jobs\""
    } else {
        "\"pin_jobs\""
    };
    let rows = db.query_all(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!("SELECT id, job_id, outcome, observed_at, needs_attention FROM {} WHERE id > $1 ORDER BY id LIMIT $2", relation.table),
        [after.into(), i64::try_from(limit).unwrap_or(i64::MAX).into()],
    )).await?;
    let mut cursor = None;
    let mut candidates = Vec::new();
    for row in rows {
        cursor = Some(row.try_get::<String>("", "id")?);
        if !row.try_get::<bool>("", "needs_attention")?
            || row
                .try_get::<Option<DateTime<Utc>>>("", "observed_at")?
                .is_none()
            || !matches!(
                row.try_get::<String>("", "outcome")?.as_str(),
                "matched" | "accepted"
            )
        {
            continue;
        }
        if let Some(job) = db.query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("SELECT * FROM {jobs_table} WHERE id=$1 AND operation='submit' AND state='running' AND locked_until IS NULL AND submit_phase='recovering' AND last_error=$2"),
            [row.try_get::<String>("", "job_id")?.into(), jobs::RPC_RECOVERY_PARK_REASON.into()],
        )).await? {
            candidates.push(pin_job::Model::from_query_result(&job, "")?);
        }
    }
    Ok((cursor, candidates))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionBarrier {
    Clear,
    Temporary,
    Operator,
}

/// No locking reads: admission is also called after lifecycle/remote prelocks.
/// A single statement gives SQLite and PG READ COMMITTED one coherent view,
/// without a reverse job lock or depending on the caller's isolation level.
#[derive(Default)]
struct AdmissionSnapshot {
    evidence: Vec<evidence::Model>,
    jobs: BTreeMap<String, pin_job::Model>,
    histories: BTreeMap<String, jobs::history::Model>,
    remotes: BTreeMap<String, remote_pin::Model>,
    captures: BTreeMap<String, pin_invocation_route::Model>,
    ledgers: BTreeMap<String, remote_pin_ledger::Model>,
    registered: Option<pin_provider_route::Model>,
    archives: Vec<pin_resource_history::Model>,
    targets: BTreeMap<String, pin_lease_target::Model>,
    leases: BTreeMap<String, pin_lease::Model>,
}

struct AdmissionTable {
    kind: &'static str,
    name: String,
    columns: BTreeSet<String>,
    filter: String,
}

fn admission_table<E: EntityTrait + Default>(kind: &'static str, filter: &str) -> AdmissionTable {
    AdmissionTable {
        kind,
        name: E::default().table_name().to_owned(),
        columns: E::Column::iter()
            .map(|column| Iden::to_string(&column))
            .collect(),
        filter: filter.into(),
    }
}

impl AdmissionSnapshot {
    async fn read<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<Self> {
        Self::read_with_catalog(
            provider,
            crate::store::pinning::legacy_migration::ReceiptCatalog::full(db),
            &(),
        )
        .await
    }

    async fn read_with_catalog<W: crate::store::pinning::legacy_migration::ReceiptWork>(
        provider: &str,
        catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
        work: &W,
    ) -> AppResult<Self> {
        let mut keys = match catalog {
            crate::store::pinning::legacy_migration::ReceiptCatalog::Full(_) => {
                crate::store::pinning::legacy_migration::admission_historical_keys(
                    &catalog, provider, work,
                )
                .await
            }
            _ => {
                crate::store::pinning::legacy_migration::admission_historical_keys_with_catalog(
                    provider, work, catalog,
                )
                .await
            }
        }
        .map_err(W::into_app)?;
        keys.retain(|key| key != provider);
        keys.sort();
        keys.dedup();
        keys.insert(0, provider.into());
        let slots = (1..=keys.len())
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(",");
        let scope = format!("\"provider\" IN ({slots})");
        let evidence_jobs =
            format!("SELECT \"job_id\" FROM \"pin_submit_observations\" WHERE {scope}");
        let invocation_scope = format!("\"job_id\" IN ({evidence_jobs})");
        let job_scope = format!("{scope} AND \"id\" IN ({evidence_jobs})");
        let remote_scope = format!(
            "{scope} AND \"cid\" IN (SELECT \"expected_cid\" FROM \"pin_submit_observations\" WHERE {scope})"
        );
        let target_scope = format!(
            "{scope} AND \"id\" IN (SELECT \"target_id\" FROM \"pin_jobs\" WHERE {job_scope})"
        );
        let lease_scope = format!(
            "\"id\" IN (SELECT \"lease_id\" FROM \"pin_lease_targets\" WHERE {scope}) \
             AND \"id\" IN (SELECT \"lease_id\" FROM \"pin_jobs\" WHERE {job_scope} \
             UNION SELECT \"lease_id\" FROM \"pin_lease_targets\" WHERE {target_scope})"
        );
        let tables = [
            admission_table::<evidence::Entity>("evidence", &scope),
            admission_table::<pin_job::Entity>("job", &job_scope),
            admission_table::<jobs::history::Entity>("history", &invocation_scope),
            admission_table::<remote_pin::Entity>("remote", &remote_scope),
            admission_table::<pin_invocation_route::Entity>("capture", &invocation_scope),
            admission_table::<remote_pin_ledger::Entity>("ledger", &scope),
            admission_table::<pin_provider_route::Entity>("registered", "\"provider\" = $1"),
            admission_table::<pin_resource_history::Entity>("archive", &scope),
            admission_table::<pin_lease_target::Entity>("target", &target_scope),
            admission_table::<pin_lease::Entity>("lease", &lease_scope),
        ];
        let mut columns = BTreeMap::new();
        for table in &tables {
            for column in &table.columns {
                columns.entry(column.clone()).or_insert(&table.name);
            }
        }
        // A zero-row typed seed prevents PG resolving early NULL-only UNION
        // columns as text (e.g. timestamps/booleans/counters appearing later).
        // Native columns also preserve exact chrono and boolean decoding; no
        // lossy JSON timestamp conversion or fabricated model fields.
        let seed = columns
            .iter()
            .map(|(column, table)| {
                format!("(SELECT \"{column}\" FROM \"{table}\" WHERE 1=0) AS \"{column}\"")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("SELECT 'empty' AS \"__admission_kind\", {seed} WHERE 1=0");
        for table in &tables {
            let fields = columns
                .keys()
                .map(|column| {
                    if table.columns.contains(column) {
                        format!("\"{column}\"")
                    } else {
                        format!("NULL AS \"{column}\"")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(
                " UNION ALL SELECT '{}' AS \"__admission_kind\", {fields} FROM \"{}\" WHERE {}",
                table.kind, table.name, table.filter
            ));
        }
        let mut snapshot = Self::default();
        for row in catalog
            .query_all(Statement::from_sql_and_values(
                catalog.get_database_backend(),
                sql,
                keys.into_iter()
                    .map(sea_orm::Value::from)
                    .collect::<Vec<_>>(),
            ))
            .await?
        {
            match row.try_get::<String>("", "__admission_kind")?.as_str() {
                "evidence" => {
                    let mut model = evidence::Model::from_query_result(&row, "")?;
                    if model.provider != provider {
                        let pk = [("id".into(), model.id.clone())].into();
                        let Some(binding) =
                            crate::store::pinning::legacy_migration::resolve_with_catalog(
                                "pin_submit_observations",
                                &pk,
                                &model.provider,
                                work,
                                catalog,
                            )
                            .await
                            .map_err(W::into_app)?
                        else {
                            continue;
                        };
                        if binding.operational_key != provider {
                            continue;
                        }
                        model.provider = provider.into(); // ephemeral admission projection only
                    }
                    snapshot.evidence.push(model);
                }
                "job" => {
                    let model = pin_job::Model::from_query_result(&row, "")?;
                    snapshot.jobs.insert(model.id.clone(), model);
                }
                "history" => {
                    let model = jobs::history::Model::from_query_result(&row, "")?;
                    snapshot.histories.insert(model.job_id.clone(), model);
                }
                "remote" => {
                    let model = remote_pin::Model::from_query_result(&row, "")?;
                    snapshot.remotes.insert(model.cid.clone(), model);
                }
                "capture" => {
                    let model = pin_invocation_route::Model::from_query_result(&row, "")?;
                    snapshot.captures.insert(model.job_id.clone(), model);
                }
                "ledger" => {
                    let model = remote_pin_ledger::Model::from_query_result(&row, "")?;
                    snapshot.ledgers.insert(model.cid.clone(), model);
                }
                "registered" => {
                    snapshot.registered =
                        Some(pin_provider_route::Model::from_query_result(&row, "")?)
                }
                "archive" => {
                    let mut model = pin_resource_history::Model::from_query_result(&row, "")?;
                    if model.provider != provider {
                        let pk = [
                            ("provider".into(), model.provider.clone()),
                            ("cid".into(), model.cid.clone()),
                            ("epoch".into(), model.epoch.to_string()),
                        ]
                        .into();
                        let Some(binding) =
                            crate::store::pinning::legacy_migration::resolve_with_catalog(
                                "pin_resource_history",
                                &pk,
                                &model.provider,
                                work,
                                catalog,
                            )
                            .await
                            .map_err(W::into_app)?
                        else {
                            continue;
                        };
                        if binding.operational_key != provider {
                            continue;
                        }
                        model.provider = provider.into();
                    }
                    snapshot.archives.push(model);
                }
                "target" => {
                    let model = pin_lease_target::Model::from_query_result(&row, "")?;
                    snapshot.targets.insert(model.id.clone(), model);
                }
                "lease" => {
                    let model = pin_lease::Model::from_query_result(&row, "")?;
                    snapshot.leases.insert(model.id.clone(), model);
                }
                _ => return Err(AppError::Internal("invalid admission snapshot row".into())),
            }
        }
        Ok(snapshot)
    }

    fn released(&self, row: &evidence::Model) -> bool {
        self.archives
            .iter()
            .any(|archive| archive.cid == row.expected_cid && archive.epoch >= row.remote_epoch)
    }

    fn context_valid(
        &self,
        row: &evidence::Model,
        job: &pin_job::Model,
        remote: &remote_pin::Model,
        request: Option<&str>,
    ) -> AppResult<bool> {
        let (Some(target), Some(lease), Some(generation), Some(capture), Some(history)) = (
            job.target_id.as_ref().and_then(|id| self.targets.get(id)),
            job.lease_id.as_ref().and_then(|id| self.leases.get(id)),
            job.expected_generation,
            self.captures.get(&row.job_id),
            self.histories.get(&row.job_id),
        ) else {
            return Ok(false);
        };
        let route: ProviderRouteSnapshot = serde_json::from_str(&row.route)
            .map_err(|_| AppError::Internal("invalid RPC admission route".into()))?;
        Ok(job.id == row.job_id
            && job.operation == "submit"
            && job.provider == row.provider
            && job.cid == row.expected_cid
            && remote.provider == row.provider
            && remote.cid == row.expected_cid
            && remote.epoch >= row.remote_epoch
            && remote.status != "absent"
            && !request.is_some_and(|request| {
                remote
                    .request_id
                    .as_deref()
                    .is_some_and(|existing| existing != request)
            })
            && job.expected_remote_epoch.is_none()
            && Some(target.lease_id.as_str()) == job.lease_id.as_deref()
            && target.provider == job.provider
            && target.cid == job.cid
            && matches!(
                target.state.as_str(),
                "waiting" | "submitted" | "pinned" | "degraded"
            )
            && lease.state == "active"
            && lease.generation == generation
            && capture.route == row.route
            && capture.remote_epoch == row.remote_epoch
            && history.submit_calls == row.submit_call
            && (history.api == route.api_profile || history.api == "rpc")
            && history.strategy == route.strategy
            && !history
                .correlation
                .as_ref()
                .is_some_and(|value| value.is_empty())
            && self
                .registered
                .as_ref()
                .is_some_and(|registered| !registered.retired && registered.snapshot == row.route)
            && self
                .ledgers
                .get(&row.expected_cid)
                .is_some_and(|ledger| ledger.route.as_deref() == Some(row.route.as_str()))
            && !self.released(row))
    }

    fn temporary(&self, row: &evidence::Model, now: DateTime<Utc>) -> AppResult<bool> {
        let (Some(job), Some(remote), Some(history)) = (
            self.jobs.get(&row.job_id),
            self.remotes.get(&row.expected_cid),
            self.histories.get(&row.job_id),
        ) else {
            return Ok(false);
        };
        if row.outcome == "in_flight" && row.observed_at.is_none() {
            return Ok(job.state == "running"
                && job.locked_until.is_some_and(|until| until > now)
                && history.state == "active"
                && self.context_valid(row, job, remote, None)?);
        }
        if let Some(request) = matching_request(row) {
            let runnable = (job.state == "pending"
                && job.submit_phase.as_deref() == Some("recovering")
                && history.state == "active")
                || (job.state == "running"
                    && job.locked_until.is_some()
                    && history.state == "active")
                || (job.state == "running"
                    && job.locked_until.is_none()
                    && job.submit_phase.as_deref() == Some("recovering")
                    && job.last_error.as_deref() == Some(jobs::RPC_RECOVERY_PARK_REASON)
                    && history.state == "needs_attention");
            return Ok(runnable && self.context_valid(row, job, remote, Some(&request))?);
        }
        Ok(false)
    }

    fn held_known(&self, ledger: &remote_pin_ledger::Model) -> bool {
        if ledger.effect == "reserved" {
            return super::decode_route(ledger).is_some_and(|route| is_rpc(&route));
        }
        let rows = self
            .evidence
            .iter()
            .filter(|row| row.expected_cid == ledger.cid)
            .collect::<Vec<_>>();
        if ledger.effect == "absent" {
            return !rows.is_empty()
                && rows.iter().all(|row| {
                    !row.needs_attention
                        && row.outcome == "not_submitted"
                        && ledger.route.as_deref() == Some(row.route.as_str())
                });
        }
        rows.into_iter().any(|row| {
            !row.needs_attention
                && matches!(
                    row.outcome.as_str(),
                    "matched" | "accepted" | "not_submitted"
                )
                && ledger.route.as_deref() == Some(row.route.as_str())
                && (ledger.effect != "not_created" || row.outcome == "not_submitted")
                && !self.released(row)
        })
    }
}

/// A reclaimed matching invocation may settle its temporary claim-loss debt
/// only after the status projection passed the same original lifetime fence.
pub async fn settle_matching<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
) -> AppResult<()> {
    settle_matching_core(db, job, None).await
}

pub(crate) async fn settle_runtime_matching(
    tx: &crate::store::RuntimeTransaction,
    job: &crate::store::entities::pin_job::Model,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<()> {
    if tx.db().get_database_backend() != sea_orm::DatabaseBackend::Sqlite {
        return settle_matching(tx.db(), job).await;
    }
    settle_matching_core(tx.db(), job, Some((tx.catalog(work), work))).await
}

async fn settle_matching_core<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    runtime: Option<(
        crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
        crate::store::pinning::management::records::DecodeWork<'_>,
    )>,
) -> AppResult<()> {
    if job.operation != "submit" {
        return Ok(());
    }
    let Some(row) = latest(db, &job.id).await? else {
        return Ok(());
    };
    if !matches!(row.outcome.as_str(), "matched" | "accepted") {
        return Ok(());
    }
    let Some(remote) = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(db)
        .await?
    else {
        return Ok(());
    };
    if projection_core(db, job, &remote, runtime).await? {
        evidence::Entity::update_many()
            .col_expr(evidence::Column::NeedsAttention, Expr::value(false))
            .filter(evidence::Column::Id.eq(row.id))
            .exec(db)
            .await?;
    }
    Ok(())
}

pub async fn has_debt<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<bool> {
    Ok(admission_barrier(db, provider).await? != AdmissionBarrier::Clear)
}

pub async fn admission_barrier<C: ConnectionTrait>(
    db: &C,
    provider: &str,
) -> AppResult<AdmissionBarrier> {
    let now = Utc::now();
    let snapshot = AdmissionSnapshot::read(db, provider).await?;
    admission_barrier_from_snapshot(snapshot, now)
}

pub(in crate::store::pinning) async fn admission_barrier_with_catalog(
    provider: &str,
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<AdmissionBarrier> {
    let now = Utc::now();
    let snapshot = AdmissionSnapshot::read_with_catalog(provider, catalog, &work).await?;
    admission_barrier_from_snapshot(snapshot, now)
}

fn admission_barrier_from_snapshot(
    snapshot: AdmissionSnapshot,
    now: DateTime<Utc>,
) -> AppResult<AdmissionBarrier> {
    let mut temporary = false;
    let mut temporary_cids = std::collections::BTreeSet::new();
    for row in snapshot.evidence.iter().filter(|row| row.needs_attention) {
        if !snapshot.temporary(row, now)? {
            return Ok(AdmissionBarrier::Operator);
        }
        temporary = true;
        temporary_cids.insert((row.expected_cid.clone(), row.route.clone()));
    }
    // Older RPC holds without typed historical evidence are not made safe by
    // today's registration. Recognizing that ambiguity only denies admission;
    // it never rekeys old resources or assigns a new route/ownership.
    let current_rpc = snapshot
        .registered
        .as_ref()
        .and_then(|row| serde_json::from_str::<ProviderRouteSnapshot>(&row.snapshot).ok())
        .is_some_and(|route| is_rpc(&route));
    for row in snapshot.ledgers.values() {
        if (super::decode_route(row).is_some_and(|route| is_rpc(&route))
            || (current_rpc && super::decode_route(row).is_none()))
            && !snapshot.held_known(row)
        {
            if row
                .route
                .as_ref()
                .is_some_and(|route| temporary_cids.contains(&(row.cid.clone(), route.clone())))
            {
                continue;
            }
            return Ok(AdmissionBarrier::Operator);
        }
    }
    Ok(if temporary {
        AdmissionBarrier::Temporary
    } else {
        AdmissionBarrier::Clear
    })
}

pub async fn held_evidence_known<C: ConnectionTrait>(
    db: &C,
    ledger: &crate::store::entities::remote_pin_ledger::Model,
) -> AppResult<bool> {
    held_evidence_core(db, ledger, None).await
}

pub(in crate::store::pinning) async fn held_evidence_known_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    ledger: &crate::store::entities::remote_pin_ledger::Model,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    held_evidence_core(&catalog, ledger, Some((catalog, work))).await
}

async fn held_evidence_core<C: ConnectionTrait>(
    db: &C,
    ledger: &crate::store::entities::remote_pin_ledger::Model,
    runtime: Option<(
        crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
        crate::store::pinning::management::records::DecodeWork<'_>,
    )>,
) -> AppResult<bool> {
    if ledger.effect == "reserved" {
        return Ok(super::decode_route(ledger).is_some_and(|route| is_rpc(&route)));
    }
    let rows = match runtime {
        Some((catalog, work)) => observations_catalog(catalog, &ledger.provider, work).await?,
        None => observations(db, &ledger.provider).await?,
    }
    .into_iter()
    .filter(|row| row.expected_cid == ledger.cid)
    .collect::<Vec<_>>();
    if ledger.effect == "absent" {
        return Ok(!rows.is_empty()
            && rows.iter().all(|row| {
                !row.needs_attention
                    && row.outcome == "not_submitted"
                    && ledger.route.as_deref() == Some(row.route.as_str())
            }));
    }
    for row in rows {
        if row.needs_attention
            || !matches!(
                row.outcome.as_str(),
                "matched" | "accepted" | "not_submitted"
            )
            || ledger.route.as_deref() != Some(row.route.as_str())
        {
            continue;
        }
        if ledger.effect == "not_created" && row.outcome != "not_submitted" {
            continue;
        }
        let released = match runtime {
            Some((catalog, work)) => {
                crate::store::pinning::legacy_migration::release_history_catalog(
                    catalog,
                    &ledger.provider,
                    &ledger.cid,
                    work,
                )
                .await
                .map_err(crate::store::pinning::management::admission::work_error)?
            }
            None => {
                crate::store::pinning::legacy_migration::release_history(
                    db,
                    &ledger.provider,
                    &ledger.cid,
                )
                .await?
            }
        };
        if !released
            .iter()
            .any(|history| history.epoch >= row.remote_epoch)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn matching_pin_held<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<bool> {
    let rows = observations(db, provider).await?.into_iter().filter(|row| {
        row.expected_cid == cid && matches!(row.outcome.as_str(), "matched" | "accepted")
    });
    for row in rows {
        if !crate::store::pinning::legacy_migration::release_history(db, provider, cid)
            .await?
            .iter()
            .any(|history| history.epoch >= row.remote_epoch)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(in crate::store::pinning) async fn matching_pin_held_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    provider: &str,
    cid: &str,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    let rows = observations_catalog(catalog, provider, work).await?;
    for row in rows.into_iter().filter(|row| {
        row.expected_cid == cid && matches!(row.outcome.as_str(), "matched" | "accepted")
    }) {
        if !crate::store::pinning::legacy_migration::release_history_catalog(
            catalog, provider, cid, work,
        )
        .await
        .map_err(crate::store::pinning::management::admission::work_error)?
        .iter()
        .any(|history| history.epoch >= row.remote_epoch)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Last pre-dispatch fence: a fresh claim alone cannot authorize an archived
/// resource lifetime or today's different endpoint/account/strategy.
pub async fn dispatch_allowed<C: ConnectionTrait>(db: &C, id: &str) -> AppResult<bool> {
    let session = crate::store::pinning::management::records::SharedDecodeBudget::default();
    dispatch_allowed_catalog(
        crate::store::pinning::legacy_migration::ReceiptCatalog::full(db),
        id,
        crate::store::pinning::management::records::DecodeWork::shared(&session),
    )
    .await
}

async fn dispatch_allowed_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    id: &str,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    let db = &catalog;
    let Some(row) = evidence::Entity::find_by_id(id.to_owned()).one(db).await? else {
        return Ok(false);
    };
    let pk = [("id".into(), row.id.clone())].into();
    let migrated = if matches!(
        catalog,
        crate::store::pinning::legacy_migration::ReceiptCatalog::Full(_)
    ) {
        crate::store::pinning::legacy_migration::resolve_legacy_binding(
            db,
            "pin_submit_observations",
            &pk,
            &row.provider,
        )
        .await?
    } else {
        crate::store::pinning::legacy_migration::resolve_with_catalog(
            "pin_submit_observations",
            &pk,
            &row.provider,
            &work,
            catalog,
        )
        .await
        .map_err(crate::store::pinning::management::admission::work_error)?
    };
    if migrated.is_some() {
        return Ok(false);
    }
    if row.outcome != "in_flight" || row.observed_at.is_some() {
        return Ok(false);
    }
    let original: ProviderRouteSnapshot = serde_json::from_str(&row.route).map_err(|_| {
        AppError::InvalidPinningRequest(
            "retirement attribution needs attention; dispatch denied".into(),
        )
    })?;
    let closed = match catalog {
        crate::store::pinning::legacy_migration::ReceiptCatalog::Full(_) => {
            super::super::management::admission::registration_closed(db, &original).await?
        }
        _ => {
            super::super::management::admission::registration_closed_catalog(
                catalog, &original, work,
            )
            .await?
        }
    };
    if closed {
        return Ok(false);
    }
    let Some(captured) = pin_invocation_route::Entity::find_by_id(row.job_id.clone())
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    if captured.route != row.route || captured.remote_epoch != row.remote_epoch {
        return Ok(false);
    }
    let Some(remote) =
        remote_pin::Entity::find_by_id((row.provider.clone(), row.expected_cid.clone()))
            .one(db)
            .await?
    else {
        return Ok(false);
    };
    if remote.epoch < row.remote_epoch || remote.status == "absent" {
        return Ok(false);
    }
    let Some(registered) =
        crate::store::entities::pin_provider_route::Entity::find_by_id(row.provider.clone())
            .one(db)
            .await?
    else {
        return Ok(false);
    };
    if registered.retired || !registered_invocation_matches(&row.route, &registered.snapshot) {
        return Ok(false);
    }
    let same_route = super::get(db, &row.provider, &row.expected_cid)
        .await?
        .is_some_and(|ledger| ledger.route.as_deref() == Some(row.route.as_str()));
    if !same_route {
        return Ok(false);
    }
    Ok(pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.eq(row.provider))
        .filter(pin_resource_history::Column::Cid.eq(row.expected_cid))
        .filter(pin_resource_history::Column::Epoch.gte(row.remote_epoch))
        .one(db)
        .await?
        .is_none())
}

pub(crate) async fn dispatch_allowed_store(
    store: &crate::store::Store,
    id: &str,
) -> AppResult<bool> {
    if store.db().get_database_backend() != sea_orm::DatabaseBackend::Sqlite {
        return dispatch_allowed(store.db(), id).await;
    }
    let session = crate::store::pinning::management::records::SharedDecodeBudget::default();
    let work = crate::store::pinning::management::records::DecodeWork::shared(&session);
    let tx = store.begin_runtime(work).await?;
    let result = dispatch_allowed_catalog(tx.catalog(work), id, work).await;
    tx.rollback().await?;
    result
}

fn registered_invocation_matches(original: &str, current: &str) -> bool {
    if original == current {
        return true;
    }
    match (
        serde_json::from_str::<ProviderRouteSnapshot>(original),
        serde_json::from_str::<ProviderRouteSnapshot>(current),
    ) {
        (Ok(original), Ok(current)) => original.resource_type() == RemoteResourceType::ClusterPin
            && crate::store::entities::pin_lease_requirement::contract::invocation_route_compatible(
                &original, &current,
            ),
        _ => false,
    }
}

/// Called inside the prelocked lifecycle transaction. Same-lifetime reference
/// attachments may advance epoch; an archived release or route change may not.
pub async fn projection_allowed<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    remote: &remote_pin::Model,
) -> AppResult<bool> {
    projection_core(db, job, remote, None).await
}

pub(in crate::store::pinning) async fn projection_allowed_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    job: &crate::store::entities::pin_job::Model,
    remote: &remote_pin::Model,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    if matches!(
        catalog,
        crate::store::pinning::legacy_migration::ReceiptCatalog::Full(_)
    ) {
        return projection_allowed(&catalog, job, remote).await;
    }
    projection_core(&catalog, job, remote, Some((catalog, work))).await
}

async fn projection_core<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    remote: &remote_pin::Model,
    runtime: Option<(
        crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
        crate::store::pinning::management::records::DecodeWork<'_>,
    )>,
) -> AppResult<bool> {
    let Some(captured) = pin_invocation_route::Entity::find_by_id(job.id.clone())
        .one(db)
        .await?
    else {
        return Ok(!match runtime {
            Some((catalog, work)) => {
                super::get_catalog(catalog, &job.provider, &job.cid, work).await?
            }
            None => super::get(db, &job.provider, &job.cid).await?,
        }
        .and_then(|row| super::decode_route(&row))
        .is_some_and(|route| is_rpc(&route)));
    };
    let route: ProviderRouteSnapshot = serde_json::from_str(&captured.route)
        .map_err(|_| AppError::Internal("invalid historical projection route".into()))?;
    if !is_rpc(&route) {
        return Ok(true);
    }
    if remote.epoch < captured.remote_epoch
        || remote.provider != job.provider
        || remote.cid != job.cid
    {
        return Ok(false);
    }
    let same_route = match runtime {
        Some((catalog, work)) => super::get_catalog(catalog, &job.provider, &job.cid, work).await?,
        None => super::get(db, &job.provider, &job.cid).await?,
    }
    .and_then(|row| super::decode_route(&row))
    .is_some_and(|current| current == route);
    if !same_route {
        return Ok(false);
    }
    let releases = match runtime {
        Some((catalog, work)) => crate::store::pinning::legacy_migration::release_history_catalog(
            catalog,
            &job.provider,
            &job.cid,
            work,
        )
        .await
        .map_err(crate::store::pinning::management::admission::work_error)?,
        None => {
            crate::store::pinning::legacy_migration::release_history(db, &job.provider, &job.cid)
                .await?
        }
    };
    if releases
        .iter()
        .any(|history| history.epoch >= captured.remote_epoch)
    {
        return Ok(false);
    }
    if job.operation == "submit" {
        let Some(row) = latest(db, &job.id).await? else {
            return Ok(false);
        };
        let Some(request) = matching_request(&row) else {
            return Ok(false);
        };
        return receipt_context_core(db, &row, job, remote, Some(&request), runtime).await;
    }
    Ok(true)
}

/// Query the immutable original route and every actual resource, even after job deletion.
pub async fn observations<C: ConnectionTrait>(
    db: &C,
    provider: &str,
) -> AppResult<Vec<evidence::Model>> {
    let keys = crate::store::pinning::legacy_migration::historical_keys(
        db,
        "pin_submit_observations",
        provider,
        None,
    )
    .await?;
    let rows = evidence::Entity::find()
        .filter(evidence::Column::Provider.is_in(keys))
        .order_by_asc(evidence::Column::StartedAt)
        .all(db)
        .await?;
    let mut result = Vec::new();
    for row in rows {
        let pk = [("id".into(), row.id.clone())].into();
        let mapped = crate::store::pinning::legacy_migration::resolve_legacy_binding(
            db,
            "pin_submit_observations",
            &pk,
            &row.provider,
        )
        .await?;
        if row.provider == provider || mapped.is_some_and(|m| m.operational_key == provider) {
            result.push(row);
        }
    }
    Ok(result)
}

async fn observations_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    provider: &str,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<Vec<evidence::Model>> {
    if matches!(
        catalog,
        crate::store::pinning::legacy_migration::ReceiptCatalog::Full(_)
    ) {
        return observations(&catalog, provider).await;
    }
    let keys = crate::store::pinning::legacy_migration::historical_keys_with_catalog(
        "pin_submit_observations",
        provider,
        None,
        work,
        catalog,
    )
    .await
    .map_err(crate::store::pinning::management::admission::work_error)?;
    let rows = evidence::Entity::find()
        .filter(evidence::Column::Provider.is_in(keys))
        .order_by_asc(evidence::Column::StartedAt)
        .all(&catalog)
        .await?;
    let mut result = Vec::new();
    for row in rows {
        let pk = [("id".into(), row.id.clone())].into();
        let mapped = crate::store::pinning::legacy_migration::resolve_with_catalog(
            "pin_submit_observations",
            &pk,
            &row.provider,
            &work,
            catalog,
        )
        .await
        .map_err(crate::store::pinning::management::admission::work_error)?;
        if row.provider == provider || mapped.is_some_and(|m| m.operational_key == provider) {
            result.push(row);
        }
    }
    Ok(result)
}

/// Lookup by the recorded historical scope, not current provider registration.
/// Each invocation remains visible; only its canonical resource identity is shared.
pub async fn resource_observations<C: ConnectionTrait>(
    db: &C,
    key: &CanonicalResourceKey,
) -> AppResult<Vec<(evidence::Model, ResourceEvidence)>> {
    let rows = evidence::Entity::find()
        .order_by_asc(evidence::Column::StartedAt)
        .all(db)
        .await?;
    let mut found = Vec::new();
    for row in rows {
        let pk = [("id".into(), row.id.clone())].into();
        let binding = crate::store::pinning::legacy_migration::resolve_legacy_binding(
            db,
            "pin_submit_observations",
            &pk,
            &row.provider,
        )
        .await?;
        let resources: Vec<ResourceEvidence> = serde_json::from_str(&row.resources)
            .map_err(|_| AppError::Internal("invalid durable RPC resources".into()))?;
        for resource in resources {
            let declared = binding.as_ref().is_some_and(|binding| {
                resource.key.is_some()
                    && binding.declaration_route.backend == key.backend
                    && binding.declaration_route.scope == key.scope
                    && binding.declaration_route.resource_type() == key.resource_type
                    && resource.resource.resource_type == key.resource_type
                    && canonical_resource_cid(&resource.resource.cid)
                        .ok()
                        .as_deref()
                        == Some(key.resource_id.as_str())
            });
            if resource.key.as_ref() == Some(key) || declared {
                found.push((row.clone(), resource));
            }
        }
    }
    Ok(found)
}
