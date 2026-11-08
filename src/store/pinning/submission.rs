//! RPC submission evidence is independent of terminal job and lease lifetimes.
use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, Iterable, QueryFilter, QueryOrder,
    QuerySelect, Statement, TransactionTrait,
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
            ObservedResource, ObservedResourceStatus, PinObservation, ProviderError, RemotePin,
            RemotePinStatus, SubmitEffect, SubmitObservation, canonical_resource_cid,
            cids_equivalent,
        },
    },
    store::entities::{
        pin_invocation_route, pin_job, pin_lease, pin_lease_target, pin_provider_route,
        pin_replica_observation, pin_resource_history, remote_pin, remote_pin_ledger,
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

fn complete_resource_status(
    route: &ProviderRouteSnapshot,
    expected_cid: &str,
    resource: &ObservedResource,
) -> bool {
    if route.resource_type() == RemoteResourceType::ClusterPin {
        return matches!(
            resource.status,
            ObservedResourceStatus::PinAccepted | ObservedResourceStatus::ReplicasVerified
        );
    }
    resource.status == ObservedResourceStatus::RecursiveVerified
        || resource.status == ObservedResourceStatus::PinAccepted
            && crate::pinning::ipfs_rpc::canonical_cid_request_id(route, expected_cid)
                .ok()
                .flatten()
                .as_deref()
                == Some(resource.request_id.as_str())
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
    let can_project = project_receipt(db, claimed, &row, now, None, true, None).await?;
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
            r.key.is_some() && complete_resource_status(&route, &row.expected_cid, &r.resource)
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
        None,
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
        None,
    )
    .await
}

pub(crate) async fn project_runtime_recovery_receipt(
    tx: &crate::store::RuntimeTransaction,
    claimed: &ClaimedPinJob,
    row: &evidence::Model,
    recovery_sequence: i64,
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
        Some(recovery_sequence),
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
    recovery_sequence: Option<i64>,
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
            let request = if let Some(sequence) = recovery_sequence {
                verified_rpc_recovery(
                    db,
                    row,
                    job,
                    runtime
                        .map(|(_, work)| work.budget().clone())
                        .unwrap_or_default(),
                )
                .await?
                .filter(|recovery| recovery.sequence == sequence)
                .and_then(|recovery| recovery.observation.result.ok())
                .map(|remote| remote.request_id)
            } else {
                matching_request(row)
            };
            recovery_sequence.is_none_or(|_| remote.epoch == row.remote_epoch && request.is_some())
                && receipt_context_core(db, row, job, remote, request.as_deref(), runtime).await?
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
pub(crate) fn unknown_recovery_request(row: &evidence::Model) -> Option<String> {
    if row.outcome != "unknown"
        || row.observed_at.is_none()
        || row.id != format!("{}:call:{}", row.job_id, row.submit_call)
    {
        return None;
    }
    let route: ProviderRouteSnapshot = serde_json::from_str(&row.route).ok()?;
    let request = crate::pinning::ipfs_rpc::canonical_cid_request_id(&route, &row.expected_cid)
        .ok()
        .flatten()?;
    let resources: Vec<ResourceEvidence> = serde_json::from_str(&row.resources).ok()?;
    if resources.is_empty() {
        return (row.effect == "unknown").then_some(request);
    }
    let [resource] = resources.as_slice() else {
        return None;
    };
    let expected = canonical_resource_cid(&row.expected_cid).ok()?;
    let key = CanonicalResourceKey {
        backend: route.backend,
        scope: route.scope,
        resource_type: RemoteResourceType::RpcPin,
        resource_id: expected,
    };
    (row.effect == "observed"
        && resource.key.as_ref() == Some(&key)
        && resource.resource.resource_type == RemoteResourceType::RpcPin
        && resource.resource.status == ObservedResourceStatus::PinAccepted
        && resource.resource.ownership == Ownership::Unknown
        && resource.resource.replica_evidence.is_none()
        && resource.resource.request_id == request
        && cids_equivalent(&resource.resource.cid, &row.expected_cid).ok() == Some(true))
    .then_some(request)
}

fn terminal_read_request(row: &evidence::Model) -> Option<String> {
    if let Some(request) = unknown_recovery_request(row) {
        return Some(request);
    }
    let route: ProviderRouteSnapshot = serde_json::from_str(&row.route).ok()?;
    let canonical = crate::pinning::ipfs_rpc::canonical_cid_request_id(&route, &row.expected_cid)
        .ok()
        .flatten()?;
    if row.id != format!("{}:call:{}", row.job_id, row.submit_call) {
        return None;
    }
    matching_request(row).filter(|request| request == &canonical)
}

pub(crate) struct VerifiedRpcRecovery {
    pub sequence: i64,
    pub observation: PinObservation,
}

fn recovery_resource(
    row: &evidence::Model,
    fact: &pin_replica_observation::Model,
    request: &str,
) -> Option<ObservedResource> {
    let observed_at = row.observed_at.as_ref()?;
    if fact.provider != row.provider
        || fact.expected_cid != row.expected_cid
        || fact.route != row.route
        || fact.remote_epoch != row.remote_epoch
        || fact.projectable
        || fact.outcome != "ReportedPinned"
        || fact.safe_error.is_some()
        || &fact.observed_at < observed_at
    {
        return None;
    }
    let resources: Vec<ObservedResource> = serde_json::from_str(&fact.resources).ok()?;
    let [resource] = resources.as_slice() else {
        return None;
    };
    (resource.resource_type == RemoteResourceType::RpcPin
        && resource.status == ObservedResourceStatus::RecursiveVerified
        && resource.ownership == Ownership::Unknown
        && resource.replica_evidence.is_none()
        && resource.request_id == request
        && cids_equivalent(&resource.cid, &row.expected_cid).ok() == Some(true))
    .then(|| resource.clone())
}

/// Consume only an actual positive read fact bound to this original unknown
/// invocation. Automatic reads name the Submit job; explicit management reads
/// retain their own Reconcile job and must carry matching confirmed authority.
pub(crate) async fn verified_rpc_recovery<C: ConnectionTrait>(
    db: &C,
    row: &evidence::Model,
    job: &pin_job::Model,
    work: crate::store::pinning::management::records::SharedDecodeBudget,
) -> AppResult<Option<VerifiedRpcRecovery>> {
    let request = if job.state == "error" {
        terminal_read_request(row)
    } else {
        unknown_recovery_request(row)
    };
    let Some(request) = request else {
        return Ok(None);
    };
    let facts = pin_replica_observation::Entity::find()
        .filter(pin_replica_observation::Column::Provider.eq(&row.provider))
        .filter(pin_replica_observation::Column::ExpectedCid.eq(&row.expected_cid))
        .filter(pin_replica_observation::Column::Route.eq(&row.route))
        .filter(pin_replica_observation::Column::RemoteEpoch.eq(row.remote_epoch))
        .filter(pin_replica_observation::Column::Projectable.eq(false))
        .order_by_desc(pin_replica_observation::Column::Sequence)
        .limit(u64::try_from(jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS).unwrap_or(8))
        .all(db)
        .await?;
    for fact in facts {
        if row
            .observed_at
            .as_ref()
            .is_some_and(|observed_at| &fact.observed_at < observed_at)
        {
            continue;
        }
        let Some(resource) = recovery_resource(row, &fact, &request) else {
            return Ok(None);
        };
        let origin_matches = if fact.job_id == row.job_id {
            fact.phase == "submit"
                && fact.lease_id == job.lease_id
                && fact.target_id == job.target_id
                && fact.expected_generation == job.expected_generation
        } else if fact.phase == "reconcile" {
            let Some(management_job) = pin_job::Entity::find_by_id(&fact.job_id).one(db).await?
            else {
                continue;
            };
            let authority =
                crate::store::pinning::management::actions::job_authority::lookup_with_work(
                    db,
                    &management_job,
                    work.clone(),
                )
                .await
                .map_err(crate::store::pinning::management::actions::job_authority::safe)?;
            authority.is_some_and(|authority| {
                authority.directive
                    == crate::store::pinning::management::records::JobDirective::ReadOnlyObserve
                    && authority.provider == row.provider
                    && authority.cid == row.expected_cid
                    && authority.remote_epoch == row.remote_epoch
                    && serde_json::from_str::<ProviderRouteSnapshot>(&row.route)
                        .ok()
                        .as_ref()
                        == Some(&authority.route)
                    && authority.lease_id == job.lease_id
                    && authority.lease_generation == job.expected_generation
                    && authority
                        .resource_id
                        .as_deref()
                        .is_none_or(|id| id == request)
            })
        } else {
            false
        };
        if origin_matches {
            return Ok(Some(VerifiedRpcRecovery {
                sequence: fact.sequence,
                observation: PinObservation {
                    result: Ok(RemotePin {
                        request_id: request.clone(),
                        cid: resource.cid.clone(),
                        status: RemotePinStatus::Pinned,
                        raw_status: "recursive".into(),
                        failure_reason: None,
                    }),
                    resources: vec![resource],
                },
            }));
        }
    }
    Ok(None)
}

pub(crate) async fn recovery_read_allowed<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    receipt_id: &str,
) -> AppResult<bool> {
    let Some(row) = latest(db, &job.id).await? else {
        return Ok(false);
    };
    let Some(request) = unknown_recovery_request(&row) else {
        return Ok(false);
    };
    let Some(remote) = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    Ok(row.id == receipt_id
        && remote.epoch == row.remote_epoch
        && receipt_context_core(db, &row, job, &remote, Some(&request), None).await?)
}

pub(crate) fn matching_request(row: &evidence::Model) -> Option<String> {
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
        backend: route.backend.clone(),
        scope: route.scope.clone(),
        resource_type: family,
        resource_id: expected,
    };
    resources
        .iter()
        .all(|resource| {
            resource.key.as_ref() == Some(&key)
                && resource.resource.resource_type == family
                && complete_resource_status(&route, &row.expected_cid, &resource.resource)
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
    wake_parked_core(db, id, now, None, false).await
}

pub(crate) async fn wake_runtime_parked(
    tx: &crate::store::RuntimeTransaction,
    id: &str,
    now: DateTime<Utc>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    wake_parked_core(tx.db(), id, now, Some((tx.catalog(work), work)), true).await
}

pub(in crate::store::pinning) async fn wake_runtime_matching_parked(
    tx: &crate::store::RuntimeTransaction,
    id: &str,
    now: DateTime<Utc>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    // Parking already owns the job fence; preserve its job-first protocol.
    wake_parked_core(tx.db(), id, now, Some((tx.catalog(work), work)), false).await
}

async fn wake_parked_core<C: ConnectionTrait>(
    db: &C,
    id: &str,
    now: DateTime<Utc>,
    runtime: Option<(
        crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
        crate::store::pinning::management::records::DecodeWork<'_>,
    )>,
    lifecycle_first: bool,
) -> AppResult<bool> {
    // Unlocked lookup discovers the lifecycle frontier, never authorization.
    let Some(snapshot) = pin_job::Entity::find_by_id(id.to_owned()).one(db).await? else {
        return Ok(false);
    };
    // The generic park caller already owns the job lock. Only the fresh runtime
    // wake takes lifecycle writer locks here, in lease → target → job order.
    if lifecycle_first && !lock_receipt_lifecycle(db, &snapshot, now).await? {
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
    let Some(history) = jobs::submission_history(db, id).await? else {
        return Ok(false);
    };
    if job.state == "running"
        && job.locked_until.is_none()
        && (history.recovery_queries >= jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS
            || now.signed_duration_since(history.started_at) >= chrono::Duration::hours(1))
    {
        jobs::terminate_parked_submit(db, &job, "submit recovery budget exhausted", now).await?;
        return Ok(false);
    }
    let matching = matching_request(&row);
    let unknown = unknown_recovery_request(&row);
    let verified = if unknown.is_some() {
        verified_rpc_recovery(
            db,
            &row,
            &job,
            runtime
                .map(|(_, work)| work.budget().clone())
                .unwrap_or_default(),
        )
        .await?
    } else {
        None
    };
    let request = matching.as_ref().or(unknown.as_ref());
    let automatic_unknown = unknown.is_some() && verified.is_none();
    if job.state != "running"
        || job.locked_until.is_some()
        || job.submit_phase.as_deref() != Some("recovering")
        || history.state != "needs_attention"
        || request.is_none()
        || matching.is_some() && job.last_error.as_deref() != Some(jobs::RPC_RECOVERY_PARK_REASON)
        || automatic_unknown
            && (job.last_error.as_deref() != Some(jobs::RPC_RECOVERY_PARK_REASON)
                || history.recovery_queries >= jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS
                || now.signed_duration_since(history.started_at) >= chrono::Duration::hours(1))
        || unknown.is_some() && remote.epoch != row.remote_epoch
        || !receipt_context_core(
            db,
            &row,
            &job,
            &remote,
            request.map(String::as_str),
            runtime,
        )
        .await?
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
    jobs::wake_rpc_parked(
        db,
        &job,
        row.submit_call,
        verified.is_none().then_some(jobs::RPC_RECOVERY_PARK_REASON),
        now,
    )
    .await
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
        format!("SELECT e.id, e.job_id, e.outcome, e.observed_at, e.needs_attention FROM {} AS e WHERE e.id > $1 AND EXISTS (SELECT 1 FROM {jobs_table} AS j WHERE j.id=e.job_id AND j.operation='submit' AND j.state='running' AND j.locked_until IS NULL AND j.submit_phase='recovering') ORDER BY e.id LIMIT $2", relation.table),
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
                "matched" | "accepted" | "unknown"
            )
        {
            continue;
        }
        if let Some(job) = db.query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("SELECT * FROM {jobs_table} WHERE id=$1 AND operation='submit' AND state='running' AND locked_until IS NULL AND submit_phase='recovering'"),
            [row.try_get::<String>("", "job_id")?.into()],
        )).await? {
            candidates.push(pin_job::Model::from_query_result(&job, "")?);
        }
    }
    Ok((cursor, candidates))
}

pub(crate) async fn missing_receipt_park_candidates<C: ConnectionTrait>(
    db: &C,
    after: &str,
    limit: u64,
) -> AppResult<(Option<String>, Vec<pin_job::Model>)> {
    let rows = pin_job::Entity::find()
        .filter(pin_job::Column::Id.gt(after))
        .filter(pin_job::Column::Operation.eq("submit"))
        .filter(pin_job::Column::State.eq("running"))
        .filter(pin_job::Column::LockedUntil.is_null())
        .filter(pin_job::Column::SubmitPhase.eq("recovering"))
        .filter(pin_job::Column::LastError.eq(jobs::RPC_MISSING_RECEIPT_PARK_REASON))
        .order_by_asc(pin_job::Column::Id)
        .limit(limit)
        .all(db)
        .await?;
    let cursor = rows.last().map(|job| job.id.clone());
    Ok((cursor, rows))
}

pub(crate) async fn parked_receipt_scope_candidates<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Vec<pin_job::Model>> {
    let rows = evidence::Entity::find()
        .filter(evidence::Column::Provider.eq(provider))
        .filter(evidence::Column::ExpectedCid.eq(cid))
        .filter(evidence::Column::Outcome.is_in(["unknown", "matched", "accepted"]))
        .filter(evidence::Column::NeedsAttention.eq(true))
        .order_by_desc(evidence::Column::SubmitCall)
        .limit(u64::try_from(jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS).unwrap_or(8))
        .all(db)
        .await?;
    let mut candidates = Vec::new();
    for row in rows {
        if let Some(job) = pin_job::Entity::find_by_id(row.job_id).one(db).await?
            && job.operation == "submit"
            && (job.state == "error"
                || (job.state == "running" && job.submit_phase.as_deref() == Some("recovering")))
            && job.locked_until.is_none()
        {
            candidates.push(job);
        }
    }
    Ok(candidates)
}

pub(crate) async fn reset_runtime_missing_receipt(
    tx: &crate::store::RuntimeTransaction,
    snapshot: &pin_job::Model,
    now: DateTime<Utc>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    crate::store::pinning::management::admission::lock_sqlite_writer_in_caller_transaction(tx.db())
        .await?;
    crate::store::pinning::quota::lock_runtime_allocation_admission(tx, &snapshot.provider, work)
        .await?;
    if tx.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        tx.query_one(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            "SELECT id FROM pin_jobs WHERE id=$1 FOR UPDATE NOWAIT",
            [snapshot.id.clone().into()],
        ))
        .await?;
    }
    let Some(job) = jobs::lock_submit_receipt(tx.db(), &snapshot.id).await? else {
        return Ok(false);
    };
    let exact_claim = snapshot.locked_until.is_some() && job.locked_until == snapshot.locked_until;
    let exact_park = snapshot.locked_until.is_none()
        && job.locked_until.is_none()
        && job.last_error.as_deref() == Some(jobs::RPC_MISSING_RECEIPT_PARK_REASON);
    if !(exact_claim || exact_park)
        || job.operation != "submit"
        || job.state != "running"
        || job.submit_phase.as_deref() != Some("recovering")
        || !jobs::check_target_job_generation(tx.db(), &job).await?
        || !job
            .last_error
            .as_deref()
            .is_none_or(|error| error == jobs::RPC_MISSING_RECEIPT_PARK_REASON)
    {
        return Ok(false);
    }
    let Some(capture) = pin_invocation_route::Entity::find_by_id(&job.id)
        .one(tx.db())
        .await?
    else {
        return Ok(false);
    };
    let Ok(route) = serde_json::from_str::<ProviderRouteSnapshot>(&capture.route) else {
        return Ok(false);
    };
    if crate::pinning::ipfs_rpc::canonical_cid_request_id(&route, &job.cid)
        .ok()
        .flatten()
        .is_none()
    {
        return Ok(false);
    }
    let Some(remote) = lock_receipt_remote(tx.db(), &job.provider, &job.cid).await? else {
        return Ok(false);
    };
    let Some(ledger) =
        remote_pin_ledger::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
            .one(tx.db())
            .await?
    else {
        return Ok(false);
    };
    if capture.remote_epoch != remote.epoch
        || remote.status != "reserved"
        || remote.request_id.is_some()
        || remote.failure_attempts != 0
        || remote.next_retry_at.is_some()
        || remote.last_failed_request_id.is_some()
        || remote.last_error_class.is_some()
        || remote.last_error_text.is_some()
        || ledger.route.as_deref() != Some(capture.route.as_str())
        || ledger.ownership != "unknown"
        || ledger.effect != "reserved"
        || ledger.first_observed_at.is_some()
        || ledger.last_observed_at.is_some()
        || ledger.remote_pinned_at.is_some()
        || ledger.gateway_verified_at.is_some()
        || ledger.content_verified_at.is_some()
        || ledger.first_error.is_some()
        || ledger.last_error.is_some()
    {
        return Ok(false);
    }
    let history = jobs::submission_history(tx.db(), &job.id).await?;
    if history.as_ref().is_some_and(|history| {
        history.correlation.is_some()
            || history.api != "unknown"
            || history.strategy != "unknown"
            || history.effect != "unknown"
            || history.state != "needs_attention"
            || history.submit_calls != 0
            || history.recovery_queries != 0
            || history.first_error.as_deref() != Some(jobs::RPC_MISSING_RECEIPT_PARK_REASON)
            || history.last_error.as_deref() != Some(jobs::RPC_MISSING_RECEIPT_PARK_REASON)
    }) {
        return Ok(false);
    }
    if evidence::Entity::find()
        .filter(evidence::Column::JobId.eq(&job.id))
        .one(tx.db())
        .await?
        .is_some()
        || pin_replica_observation::Entity::find()
            .filter(pin_replica_observation::Column::Provider.eq(&job.provider))
            .filter(pin_replica_observation::Column::ExpectedCid.eq(&job.cid))
            .filter(pin_replica_observation::Column::RemoteEpoch.eq(remote.epoch))
            .one(tx.db())
            .await?
            .is_some()
    {
        return Ok(false);
    }
    let aliases = observations_catalog(tx.catalog(work), &job.provider, work).await?;
    if aliases
        .iter()
        .any(|row| row.expected_cid == job.cid && row.remote_epoch == remote.epoch)
    {
        return Ok(false);
    }
    if crate::store::pinning::legacy_migration::release_history_catalog(
        tx.catalog(work),
        &job.provider,
        &job.cid,
        work,
    )
    .await
    .map_err(crate::store::pinning::management::admission::work_error)?
    .iter()
    .any(|release| release.epoch >= remote.epoch)
    {
        return Ok(false);
    }
    let competing = pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(&job.provider))
        .filter(pin_job::Column::Cid.eq(&job.cid))
        .filter(pin_job::Column::Id.ne(&job.id))
        .filter(pin_job::Column::Operation.is_in(["submit", "poll", "unpin"]))
        .filter(pin_job::Column::State.is_in(["pending", "running"]))
        .one(tx.db())
        .await?;
    if competing.is_some() {
        return Ok(false);
    }
    if history.is_some() {
        let deleted = jobs::history::Entity::delete_many()
            .filter(jobs::history::Column::JobId.eq(&job.id))
            .filter(jobs::history::Column::SubmitCalls.eq(0))
            .filter(jobs::history::Column::RecoveryQueries.eq(0))
            .filter(jobs::history::Column::Effect.eq("unknown"))
            .filter(jobs::history::Column::State.eq("needs_attention"))
            .filter(jobs::history::Column::FirstError.eq(jobs::RPC_MISSING_RECEIPT_PARK_REASON))
            .filter(jobs::history::Column::LastError.eq(jobs::RPC_MISSING_RECEIPT_PARK_REASON))
            .exec(tx.db())
            .await?;
        if deleted.rows_affected != 1 {
            return Err(AppError::Internal(
                "zero-call RPC history changed during recovery".into(),
            ));
        }
    }
    let mut update = pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value("pending"))
        .col_expr(pin_job::Column::SubmitPhase, Expr::value("ready"))
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(now))
        .col_expr(pin_job::Column::UpdatedAt, Expr::value(now))
        .filter(pin_job::Column::Id.eq(&job.id))
        .filter(pin_job::Column::Operation.eq("submit"))
        .filter(pin_job::Column::State.eq("running"))
        .filter(pin_job::Column::SubmitPhase.eq("recovering"))
        .filter(pin_job::Column::LeaseId.eq(job.lease_id.clone()))
        .filter(pin_job::Column::TargetId.eq(job.target_id.clone()))
        .filter(pin_job::Column::ExpectedGeneration.eq(job.expected_generation));
    update = match snapshot.locked_until {
        Some(claim) => update.filter(pin_job::Column::LockedUntil.eq(claim)),
        None => update
            .filter(pin_job::Column::LockedUntil.is_null())
            .filter(pin_job::Column::LastError.eq(jobs::RPC_MISSING_RECEIPT_PARK_REASON)),
    };
    let changed = update.exec(tx.db()).await?;
    if changed.rows_affected != 1 {
        return Err(AppError::Internal(
            "zero-call RPC job changed during recovery".into(),
        ));
    }
    Ok(true)
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
    recoveries: Vec<pin_replica_observation::Model>,
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
        let recovery_scope = format!(
            "{scope} AND EXISTS (SELECT 1 FROM \"pin_submit_observations\" AS evidence \
             WHERE evidence.\"provider\"=\"pin_replica_observations\".\"provider\" \
             AND evidence.\"expected_cid\"=\"pin_replica_observations\".\"expected_cid\" \
             AND evidence.\"route\"=\"pin_replica_observations\".\"route\" \
             AND evidence.\"remote_epoch\"=\"pin_replica_observations\".\"remote_epoch\") \
             AND (\"pin_replica_observations\".\"projectable\"=TRUE \
             OR NOT EXISTS (SELECT 1 FROM \"pin_replica_observations\" AS newer \
             WHERE newer.\"provider\"=\"pin_replica_observations\".\"provider\" \
             AND newer.\"expected_cid\"=\"pin_replica_observations\".\"expected_cid\" \
             AND newer.\"route\"=\"pin_replica_observations\".\"route\" \
             AND newer.\"remote_epoch\"=\"pin_replica_observations\".\"remote_epoch\" \
             AND newer.\"projectable\"=FALSE \
             AND newer.\"sequence\">\"pin_replica_observations\".\"sequence\"))"
        );
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
            admission_table::<pin_replica_observation::Entity>("recovery", &recovery_scope),
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
                "recovery" => {
                    let mut model = pin_replica_observation::Model::from_query_result(&row, "")?;
                    if model.provider != provider {
                        let pk = [("sequence".into(), model.sequence.to_string())].into();
                        let Some(binding) =
                            crate::store::pinning::legacy_migration::resolve_with_catalog(
                                "pin_replica_observations",
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
                    snapshot.recoveries.push(model);
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
        if let Some(request) = unknown_recovery_request(row) {
            let claimed =
                job.state == "running" && job.locked_until.is_some() && history.state == "active";
            let due = job.state == "pending"
                && job.submit_phase.as_deref() == Some("recovering")
                && history.state == "active";
            let bounded_park = job.state == "running"
                && job.locked_until.is_none()
                && job.submit_phase.as_deref() == Some("recovering")
                && job.last_error.as_deref() == Some(jobs::RPC_RECOVERY_PARK_REASON)
                && history.state == "needs_attention"
                && history.recovery_queries < jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS
                && now.signed_duration_since(history.started_at) < chrono::Duration::hours(1);
            return Ok(remote.epoch == row.remote_epoch
                && (claimed || due || bounded_park)
                && self.context_valid(row, job, remote, Some(&request))?);
        }
        Ok(false)
    }

    fn recovered_known(&self, row: &evidence::Model, ledger: &remote_pin_ledger::Model) -> bool {
        let request = if self
            .jobs
            .get(&row.job_id)
            .is_some_and(|job| job.state == "error")
        {
            terminal_read_request(row)
        } else {
            unknown_recovery_request(row)
        };
        let Some(request) = request else {
            return false;
        };
        let (Some(job), Some(history), Some(remote), Some(capture)) = (
            self.jobs.get(&row.job_id),
            self.histories.get(&row.job_id),
            self.remotes.get(&row.expected_cid),
            self.captures.get(&row.job_id),
        ) else {
            return false;
        };
        let Some(recovery) = self
            .recoveries
            .iter()
            .filter(|fact| {
                fact.job_id == row.job_id
                    && fact.provider == row.provider
                    && fact.expected_cid == row.expected_cid
                    && fact.route == row.route
                    && fact.remote_epoch == row.remote_epoch
                    && fact.phase == "submit_recovery"
                    && fact.projectable
            })
            .max_by_key(|fact| fact.sequence)
        else {
            return false;
        };
        let Some(source_sequence) = recovery
            .outcome
            .strip_prefix("VerifiedPinned:")
            .and_then(|sequence| sequence.parse::<i64>().ok())
            .filter(|sequence| *sequence < recovery.sequence)
        else {
            return false;
        };
        if self.recoveries.iter().any(|fact| {
            !fact.projectable
                && fact.provider == recovery.provider
                && fact.expected_cid == recovery.expected_cid
                && fact.route == recovery.route
                && fact.remote_epoch == recovery.remote_epoch
                && fact.sequence > source_sequence
        }) {
            return false;
        }
        let Ok(resources) = serde_json::from_str::<Vec<ObservedResource>>(&recovery.resources)
        else {
            return false;
        };
        let [resource] = resources.as_slice() else {
            return false;
        };
        let Some(observed_at) = row.observed_at.as_ref() else {
            return false;
        };
        !row.needs_attention
            && recovery.projectable
            && recovery.phase == "submit_recovery"
            && recovery.safe_error.is_none()
            && recovery.provider == row.provider
            && recovery.expected_cid == row.expected_cid
            && recovery.route == row.route
            && recovery.remote_epoch == row.remote_epoch
            && recovery.lease_id == job.lease_id
            && recovery.target_id == job.target_id
            && recovery.expected_generation == job.expected_generation
            && &recovery.observed_at >= observed_at
            && resource.resource_type == RemoteResourceType::RpcPin
            && resource.status == ObservedResourceStatus::RecursiveVerified
            && resource.ownership == Ownership::Unknown
            && resource.replica_evidence.is_none()
            && resource.request_id == request
            && cids_equivalent(&resource.cid, &row.expected_cid).ok() == Some(true)
            && history.submit_calls == row.submit_call
            && history.state == "settled"
            && history.effect == "created"
            && capture.route == row.route
            && capture.remote_epoch == row.remote_epoch
            && remote.provider == row.provider
            && remote.cid == row.expected_cid
            && remote.epoch == row.remote_epoch
            && remote.status == "pinned"
            && remote.request_id.as_deref() == Some(request.as_str())
            && ledger.effect == "confirmed"
            && ledger.route.as_deref() == Some(row.route.as_str())
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
            !self.released(row)
                && (self.recovered_known(row, ledger)
                    || !row.needs_attention
                        && (row.outcome == "not_submitted"
                            || !self
                                .jobs
                                .get(&row.job_id)
                                .is_some_and(|job| job.state == "error"))
                        && matches!(
                            row.outcome.as_str(),
                            "matched" | "accepted" | "not_submitted"
                        )
                        && ledger.route.as_deref() == Some(row.route.as_str())
                        && (ledger.effect != "not_created" || row.outcome == "not_submitted"))
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
    if projection_core(db, job, &remote, runtime, None).await? {
        evidence::Entity::update_many()
            .col_expr(evidence::Column::NeedsAttention, Expr::value(false))
            .filter(evidence::Column::Id.eq(row.id))
            .exec(db)
            .await?;
    }
    Ok(())
}

/// A confirmed Reconcile may close an invocation's debt without waking its
/// terminal Submit or granting target projection, Submit, release, or cleanup.
pub(crate) async fn settle_terminal_read(
    tx: &crate::store::RuntimeTransaction,
    read_job: &pin_job::Model,
    source_id: &str,
    now: DateTime<Utc>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    use crate::store::pinning::management::actions::job_authority::{self, JobPermit};
    let Some(claim_until) = read_job.locked_until else {
        return Ok(false);
    };
    if read_job.operation != "reconcile"
        || !jobs::fence_job_claim(tx.db(), &read_job.id, claim_until).await?
    {
        return Ok(false);
    }
    job_authority::fence_runtime_authority(tx, read_job, work.budget().clone()).await?;
    let permit =
        job_authority::check_before_call_catalog(tx.catalog(work), read_job, work.budget().clone())
            .await
            .map_err(job_authority::safe)?;
    let Some(JobPermit::ReadOnly(authority)) = permit else {
        return Ok(false);
    };
    let Some(job) = jobs::lock_submit_receipt(tx.db(), source_id).await? else {
        return Ok(false);
    };
    if job.state != "error" || job.operation != "submit" || job.locked_until.is_some() {
        return Ok(false);
    }
    let Some(row) = latest(tx.db(), &job.id).await? else {
        return Ok(false);
    };
    let Some(request) = terminal_read_request(&row) else {
        return Ok(false);
    };
    let Some(capture) = pin_invocation_route::Entity::find_by_id(&job.id)
        .one(tx.db())
        .await?
    else {
        return Ok(false);
    };
    if !row.needs_attention
        || row.job_id != job.id
        || row.provider != job.provider
        || row.expected_cid != job.cid
        || job.expected_remote_epoch.is_some()
        || job.lease_id.is_none()
        || job.target_id.is_none()
        || job.expected_generation.is_none()
        || authority.provider != row.provider
        || authority.cid != row.expected_cid
        || authority.remote_epoch != row.remote_epoch
        || serde_json::from_str::<ProviderRouteSnapshot>(&row.route)
            .ok()
            .as_ref()
            != Some(&authority.route)
        || capture.route != row.route
        || capture.remote_epoch != row.remote_epoch
    {
        return Ok(false);
    }
    let Some(remote) = lock_receipt_remote(tx.db(), &job.provider, &job.cid).await? else {
        return Ok(false);
    };
    let Some(ledger) =
        remote_pin_ledger::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
            .one(tx.db())
            .await?
    else {
        return Ok(false);
    };
    if remote.epoch != row.remote_epoch
        || remote.status == "absent"
        || remote.request_id.as_ref().is_some_and(|id| id != &request)
        || ledger.route.as_deref() != Some(row.route.as_str())
        || crate::store::pinning::legacy_migration::release_history_catalog(
            tx.catalog(work),
            &job.provider,
            &job.cid,
            work,
        )
        .await
        .map_err(job_authority::safe)?
        .iter()
        .any(|release| release.epoch >= row.remote_epoch)
    {
        return Ok(false);
    }
    let Some(recovery) = verified_rpc_recovery(tx.db(), &row, &job, work.budget().clone()).await?
    else {
        return Ok(false);
    };
    let Some(fact) = pin_replica_observation::Entity::find_by_id(recovery.sequence)
        .one(tx.db())
        .await?
    else {
        return Ok(false);
    };
    if fact.job_id != read_job.id || fact.claim_until != claim_until {
        return Ok(false);
    }
    let history = jobs::history::Entity::update_many()
        .col_expr(jobs::history::Column::State, Expr::value("settled"))
        .col_expr(jobs::history::Column::Effect, Expr::value("created"))
        .filter(jobs::history::Column::JobId.eq(&job.id))
        .filter(jobs::history::Column::SubmitCalls.eq(row.submit_call))
        .exec(tx.db())
        .await?;
    if history.rows_affected != 1 {
        return Ok(false);
    }
    let remote_changed = remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::Status, Expr::value("pinned"))
        .col_expr(remote_pin::Column::RequestId, Expr::value(request))
        .filter(remote_pin::Column::Provider.eq(&job.provider))
        .filter(remote_pin::Column::Cid.eq(&job.cid))
        .filter(remote_pin::Column::Epoch.eq(row.remote_epoch))
        .exec(tx.db())
        .await?;
    let ledger_changed = remote_pin_ledger::Entity::update_many()
        .col_expr(remote_pin_ledger::Column::Effect, Expr::value("confirmed"))
        .col_expr(
            remote_pin_ledger::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(remote_pin_ledger::Column::LastObservedAt, Expr::value(now))
        .col_expr(remote_pin_ledger::Column::RemotePinnedAt, Expr::value(now))
        .filter(remote_pin_ledger::Column::Provider.eq(&job.provider))
        .filter(remote_pin_ledger::Column::Cid.eq(&job.cid))
        .filter(remote_pin_ledger::Column::Route.eq(&row.route))
        .exec(tx.db())
        .await?;
    if remote_changed.rows_affected != 1 || ledger_changed.rows_affected != 1 {
        return Err(AppError::Database(
            "terminal recovery resource compare-and-set failed".into(),
        ));
    }
    settle_recovery_fact(tx, &job, &row, recovery, claim_until, now).await?;
    Ok(true)
}

pub(crate) async fn settle_runtime_recovery(
    tx: &crate::store::RuntimeTransaction,
    job: &pin_job::Model,
    recovery_sequence: i64,
    now: DateTime<Utc>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<()> {
    let Some(row) = latest(tx.db(), &job.id).await? else {
        return Ok(());
    };
    let Some(recovery) = verified_rpc_recovery(tx.db(), &row, job, work.budget().clone())
        .await?
        .filter(|recovery| recovery.sequence == recovery_sequence)
    else {
        return Ok(());
    };
    let claim_until = job
        .locked_until
        .ok_or_else(|| AppError::Internal("RPC recovery claim missing".into()))?;
    settle_recovery_fact(tx, job, &row, recovery, claim_until, now).await
}

async fn settle_recovery_fact(
    tx: &crate::store::RuntimeTransaction,
    job: &pin_job::Model,
    row: &evidence::Model,
    recovery: VerifiedRpcRecovery,
    claim_until: DateTime<Utc>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let settled = evidence::Entity::update_many()
        .col_expr(evidence::Column::NeedsAttention, Expr::value(false))
        .filter(evidence::Column::Id.eq(&row.id))
        .filter(evidence::Column::JobId.eq(&job.id))
        .filter(evidence::Column::SubmitCall.eq(row.submit_call))
        .filter(evidence::Column::Route.eq(&row.route))
        .filter(evidence::Column::RemoteEpoch.eq(row.remote_epoch))
        .filter(evidence::Column::Outcome.eq(&row.outcome))
        .filter(evidence::Column::Effect.eq(&row.effect))
        .filter(evidence::Column::NeedsAttention.eq(true))
        .exec(tx.db())
        .await?;
    if settled.rows_affected != 1 {
        return Err(AppError::Internal(
            "RPC recovery receipt identity changed".into(),
        ));
    }
    let existing = pin_replica_observation::Entity::find()
        .filter(pin_replica_observation::Column::JobId.eq(&job.id))
        .filter(pin_replica_observation::Column::Phase.eq("submit_recovery"))
        .filter(pin_replica_observation::Column::Route.eq(&row.route))
        .filter(pin_replica_observation::Column::RemoteEpoch.eq(row.remote_epoch))
        .filter(pin_replica_observation::Column::Projectable.eq(true))
        .one(tx.db())
        .await?;
    if existing.is_some() {
        return Err(AppError::Internal(
            "RPC recovery closure already exists".into(),
        ));
    }
    pin_replica_observation::Entity::insert(pin_replica_observation::ActiveModel {
        sequence: Default::default(),
        job_id: sea_orm::Set(job.id.clone()),
        provider: sea_orm::Set(job.provider.clone()),
        expected_cid: sea_orm::Set(job.cid.clone()),
        route: sea_orm::Set(row.route.clone()),
        remote_epoch: sea_orm::Set(row.remote_epoch),
        claim_until: sea_orm::Set(claim_until),
        lease_id: sea_orm::Set(job.lease_id.clone()),
        target_id: sea_orm::Set(job.target_id.clone()),
        expected_generation: sea_orm::Set(job.expected_generation),
        phase: sea_orm::Set("submit_recovery".into()),
        resources: sea_orm::Set(
            serde_json::to_string(&recovery.observation.resources).map_err(|_| {
                AppError::Internal("RPC recovery closure serialization failed".into())
            })?,
        ),
        outcome: sea_orm::Set(format!("VerifiedPinned:{}", recovery.sequence)),
        safe_error: sea_orm::Set(None),
        projectable: sea_orm::Set(true),
        observed_at: sea_orm::Set(now),
    })
    .exec(tx.db())
    .await?;
    Ok(())
}

pub(crate) async fn has_resource_debt<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<bool> {
    if evidence::Entity::find()
        .filter(evidence::Column::Provider.eq(provider))
        .filter(evidence::Column::ExpectedCid.eq(cid))
        .filter(evidence::Column::NeedsAttention.eq(true))
        .one(db)
        .await?
        .is_some()
    {
        return Ok(true);
    }
    if let Some(ledger) =
        remote_pin_ledger::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(db)
            .await?
        && (matches!(ledger.effect.as_str(), "unknown" | "reported_unknown")
            || super::decode_route(&ledger).is_some_and(|route| is_rpc(&route))
                && !held_evidence_known(db, &ledger).await?)
    {
        return Ok(true);
    }
    let jobs = pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(provider))
        .filter(pin_job::Column::Cid.eq(cid))
        .filter(pin_job::Column::Operation.eq("submit"))
        .all(db)
        .await?;
    for job in jobs {
        if jobs::submission_history(db, &job.id)
            .await?
            .is_some_and(|history| history.submit_calls > 0 && history.effect == "unknown")
        {
            return Ok(true);
        }
    }
    Ok(false)
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
    let snapshot = match runtime {
        Some((catalog, work)) => {
            AdmissionSnapshot::read_with_catalog(&ledger.provider, catalog, &work).await?
        }
        None => AdmissionSnapshot::read(db, &ledger.provider).await?,
    };
    if snapshot.held_known(ledger) {
        return Ok(true);
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
        if row.outcome != "not_submitted"
            && snapshot
                .jobs
                .get(&row.job_id)
                .is_some_and(|job| job.state == "error")
        {
            continue;
        }
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
    if let Some(ledger) = super::get(db, provider, cid).await?
        && held_evidence_known(db, &ledger).await?
    {
        return Ok(true);
    }
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
    if let Some(ledger) = super::get_catalog(catalog, provider, cid, work).await?
        && held_evidence_known_catalog(catalog, &ledger, work).await?
    {
        return Ok(true);
    }
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
    let same_route = super::get_catalog(catalog, &row.provider, &row.expected_cid, work)
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
    projection_core(db, job, remote, None, None).await
}

pub(crate) async fn projection_allowed_catalog(
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
    projection_core(&catalog, job, remote, Some((catalog, work)), None).await
}

pub(crate) async fn recovery_projection_allowed_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    job: &pin_job::Model,
    remote: &remote_pin::Model,
    recovery_sequence: i64,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<bool> {
    projection_core(
        &catalog,
        job,
        remote,
        Some((catalog, work)),
        Some(recovery_sequence),
    )
    .await
}

async fn projection_core<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    remote: &remote_pin::Model,
    runtime: Option<(
        crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
        crate::store::pinning::management::records::DecodeWork<'_>,
    )>,
    recovery_sequence: Option<i64>,
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
        let request = if let Some(sequence) = recovery_sequence {
            verified_rpc_recovery(
                db,
                &row,
                job,
                runtime
                    .map(|(_, work)| work.budget().clone())
                    .unwrap_or_default(),
            )
            .await?
            .filter(|recovery| recovery.sequence == sequence)
            .and_then(|recovery| recovery.observation.result.ok())
            .map(|remote| remote.request_id)
        } else {
            matching_request(&row)
        };
        let Some(request) = request else {
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
