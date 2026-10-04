//! RPC submission evidence is independent of terminal job and lease lifetimes.
use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, Iterable, QueryFilter, QueryOrder,
    Set, Statement, TransactionTrait,
    sea_query::{Expr, Iden},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEvidence {
    pub resource: ObservedResource,
    pub key: Option<CanonicalResourceKey>,
}

pub fn is_rpc(route: &ProviderRouteSnapshot) -> bool {
    route.resource_type() == RemoteResourceType::RpcPin
}

pub fn is_rpc_api(api: &str) -> bool {
    matches!(api, "rpc" | "kubo" | "filebase-rpc")
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
    Ok(evidence::Entity::find()
        .filter(evidence::Column::JobId.eq(job_id))
        .order_by_desc(evidence::Column::SubmitCall)
        .one(db)
        .await?)
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
    if evidence::Entity::find()
        .filter(evidence::Column::Provider.eq(&claimed.model.provider))
        .filter(evidence::Column::ExpectedCid.eq(&claimed.model.cid))
        .filter(evidence::Column::NeedsAttention.eq(true))
        .one(db)
        .await?
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
    evidence::Entity::insert(evidence::ActiveModel {
        id: Set(id.clone()),
        job_id: Set(claimed.model.id.clone()),
        submit_call: Set(history.submit_calls),
        provider: Set(claimed.model.provider.clone()),
        expected_cid: Set(claimed.model.cid.clone()),
        route: Set(captured.route),
        remote_epoch: Set(captured.remote_epoch),
        claim_until: Set(claim_until),
        effect: Set("unknown".into()),
        resources: Set("[]".into()),
        outcome: Set("in_flight".into()),
        safe_error: Set(None),
        needs_attention: Set(true),
        started_at: Set(now),
        observed_at: Set(None),
    })
    .exec(db)
    .await?;
    Ok(Some(id))
}

/// No quota admission here: evidence for already dispatched effects cannot be
/// rejected because capacity is full. The original logical reservation remains.
/// A stale claim/lifetime may append its own evidence, but cannot project status.
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

async fn record_in_transaction<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    id: &str,
    observation: &SubmitObservation,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let claim = claimed
        .model
        .locked_until
        .ok_or_else(|| AppError::Internal("RPC claim missing".into()))?;
    // Compare the claim in the database's native timestamp representation.
    // PG persists microseconds while a renewed caller token can have nanoseconds.
    // This is the same exact fence used by every other worker mutation, not a
    // time tolerance; stale receipts still lock unconditionally below to append.
    let live = jobs::fence_job_claim(db, &claimed.model.id, claim).await?;
    let current_job = jobs::lock_submit_receipt(db, &claimed.model.id).await?;
    let remote = lock_receipt_remote(db, &claimed.model.provider, &claimed.model.cid).await?;
    let row = evidence::Entity::find_by_id(id.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| AppError::Internal("RPC invocation footprint missing".into()))?;
    if row.job_id != claimed.model.id
        || row.provider != claimed.model.provider
        || row.expected_cid != claimed.model.cid
    {
        return Err(AppError::Internal(
            "RPC observation invocation identity mismatch".into(),
        ));
    }
    let route: ProviderRouteSnapshot = serde_json::from_str(&row.route)
        .map_err(|_| AppError::Internal("RPC observation historical route invalid".into()))?;
    let resources: Vec<_> = observation
        .resources
        .iter()
        .cloned()
        .map(|mut resource| {
            // No RPC state supplied by Stage 5 is ApplicationCreated proof.
            if resource.resource_type == RemoteResourceType::RpcPin
                || resource.ownership == Ownership::ApplicationCreated
            {
                resource.ownership = Ownership::Unknown;
            }
            let key = canonical_resource_cid(&resource.cid)
                .ok()
                .map(|cid| CanonicalResourceKey {
                    backend: route.backend.clone(),
                    scope: route.scope.clone(),
                    resource_type: resource.resource_type,
                    resource_id: cid,
                });
            ResourceEvidence { resource, key }
        })
        .collect();
    let keys: std::collections::BTreeSet<_> =
        resources.iter().filter_map(|r| r.key.as_ref()).collect();
    let matches_expected = resources.iter().all(|r| {
        r.resource.resource_type == RemoteResourceType::RpcPin
            && cids_equivalent(&r.resource.cid, &row.expected_cid).unwrap_or(false)
    });
    let successful = observation.result.as_ref().is_ok_and(|remote| {
        !remote.request_id.is_empty()
            && resources
                .iter()
                .all(|r| r.resource.request_id == remote.request_id)
            && remote.status == RemotePinStatus::Pinned
            && cids_equivalent(&remote.cid, &row.expected_cid).unwrap_or(false)
    }) && observation.effect == SubmitEffect::Observed
        && keys.len() == 1
        && matches_expected
        && resources.iter().all(|r| {
            r.key.is_some() && r.resource.status == ObservedResourceStatus::RecursiveVerified
        });
    let safe_not_submitted = observation.effect == SubmitEffect::NotSubmitted
        && resources.is_empty()
        && observation.result.is_err();
    let mismatch = !matches_expected || keys.len() > 1;
    let outcome = if successful {
        "matched"
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
    let captured = pin_invocation_route::Entity::find_by_id(claimed.model.id.clone())
        .one(db)
        .await?;
    let current_lifetime = captured
        .zip(remote.as_ref())
        .is_some_and(|(capture, remote)| {
            capture.route == row.route
                && capture.remote_epoch == row.remote_epoch
                && remote.epoch >= row.remote_epoch
        });
    let released = pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.eq(&row.provider))
        .filter(pin_resource_history::Column::Cid.eq(&row.expected_cid))
        .filter(pin_resource_history::Column::Epoch.gte(row.remote_epoch))
        .one(db)
        .await?
        .is_some();
    let matched_context = if successful {
        match (current_job.as_ref(), remote.as_ref()) {
            (Some(job), Some(remote)) => {
                receipt_context_valid(
                    db,
                    &row,
                    job,
                    remote,
                    resources.first().map(|r| r.resource.request_id.as_str()),
                )
                .await?
            }
            _ => false,
        }
    } else {
        true
    };
    let can_project = live && current_lifetime && !released && matched_context;
    let written = evidence::Entity::update_many()
        .col_expr(evidence::Column::Effect, Expr::value(effect))
        .col_expr(evidence::Column::Resources, Expr::value(json))
        .col_expr(evidence::Column::Outcome, Expr::value(outcome))
        .col_expr(evidence::Column::SafeError, Expr::value(safe_error))
        .col_expr(
            evidence::Column::NeedsAttention,
            Expr::value(!safe_not_submitted && (!successful || !can_project)),
        )
        .col_expr(evidence::Column::ObservedAt, Expr::value(Some(now)))
        .filter(evidence::Column::Id.eq(id))
        .filter(evidence::Column::ObservedAt.is_null())
        .exec(db)
        .await?;
    if written.rows_affected != 1 {
        return Err(AppError::Internal(
            "RPC invocation observation already recorded".into(),
        ));
    }
    if successful && !can_project {
        wake_matching_parked(db, &claimed.model.id, now).await?;
    }
    Ok(can_project)
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
    if row.outcome != "matched" || row.effect != "observed" || row.observed_at.is_none() {
        return None;
    }
    let route: ProviderRouteSnapshot = serde_json::from_str(&row.route).ok()?;
    let resources: Vec<ResourceEvidence> = serde_json::from_str(&row.resources).ok()?;
    let request = &resources.first()?.resource.request_id;
    if request.is_empty() {
        return None;
    }
    let expected = canonical_resource_cid(&row.expected_cid).ok()?;
    let key = CanonicalResourceKey {
        backend: route.backend,
        scope: route.scope,
        resource_type: RemoteResourceType::RpcPin,
        resource_id: expected,
    };
    resources
        .iter()
        .all(|resource| {
            resource.key.as_ref() == Some(&key)
                && resource.resource.resource_type == RemoteResourceType::RpcPin
                && resource.resource.status == ObservedResourceStatus::RecursiveVerified
                && resource.resource.request_id == *request
                && cids_equivalent(&resource.resource.cid, &row.expected_cid).unwrap_or(false)
        })
        .then(|| request.clone())
}

async fn receipt_context_valid<C: ConnectionTrait>(
    db: &C,
    row: &evidence::Model,
    job: &crate::store::entities::pin_job::Model,
    remote: &remote_pin::Model,
    request: Option<&str>,
) -> AppResult<bool> {
    if job.id != row.job_id
        || job.operation != "submit"
        || job.provider != row.provider
        || job.cid != row.expected_cid
        || remote.provider != row.provider
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
        crate::store::entities::pin_provider_route::Entity::find_by_id(row.provider.clone())
            .one(db)
            .await?;
    if !registered.is_some_and(|registered| !registered.retired && registered.snapshot == row.route)
        || !super::get(db, &row.provider, &row.expected_cid)
            .await?
            .is_some_and(|ledger| ledger.route.as_deref() == Some(row.route.as_str()))
    {
        return Ok(false);
    }
    Ok(pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.eq(&row.provider))
        .filter(pin_resource_history::Column::Cid.eq(&row.expected_cid))
        .filter(pin_resource_history::Column::Epoch.gte(row.remote_epoch))
        .one(db)
        .await?
        .is_none())
}

/// Both receipt-arrival and parking call this inside their existing transaction.
/// No remote mutation occurs here: only a new scheduler claim may project/settle.
pub(crate) async fn wake_matching_parked<C: ConnectionTrait>(
    db: &C,
    id: &str,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let Some(job) = jobs::lock_submit_receipt(db, id).await? else {
        return Ok(false);
    };
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
        || !jobs::submission_history(db, id)
            .await?
            .is_some_and(|history| history.state == "needs_attention")
        || !receipt_context_valid(db, &row, &job, &remote, Some(&request)).await?
    {
        return Ok(false);
    }
    jobs::wake_rpc_receipt(db, &job, row.submit_call, now).await
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
    filter: &'static str,
}

fn admission_table<E: EntityTrait + Default>(
    kind: &'static str,
    filter: &'static str,
) -> AdmissionTable {
    AdmissionTable {
        kind,
        name: E::default().table_name().to_owned(),
        columns: E::Column::iter()
            .map(|column| Iden::to_string(&column))
            .collect(),
        filter,
    }
}

impl AdmissionSnapshot {
    async fn read<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<Self> {
        let scope = "\"provider\" = $1";
        let invocation_scope = "\"job_id\" IN (SELECT \"job_id\" FROM \"pin_submit_observations\" WHERE \"provider\" = $1)";
        let tables = [
            admission_table::<evidence::Entity>("evidence", scope),
            admission_table::<pin_job::Entity>("job", scope),
            admission_table::<jobs::history::Entity>("history", invocation_scope),
            admission_table::<remote_pin::Entity>("remote", scope),
            admission_table::<pin_invocation_route::Entity>("capture", invocation_scope),
            admission_table::<remote_pin_ledger::Entity>("ledger", scope),
            admission_table::<pin_provider_route::Entity>("registered", scope),
            admission_table::<pin_resource_history::Entity>("archive", scope),
            admission_table::<pin_lease_target::Entity>("target", scope),
            admission_table::<pin_lease::Entity>(
                "lease",
                "\"id\" IN (SELECT \"lease_id\" FROM \"pin_lease_targets\" WHERE \"provider\" = $1)",
            ),
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
        for row in db
            .query_all(Statement::from_sql_and_values(
                db.get_database_backend(),
                sql,
                [provider.into()],
            ))
            .await?
        {
            match row.try_get::<String>("", "__admission_kind")?.as_str() {
                "evidence" => snapshot
                    .evidence
                    .push(evidence::Model::from_query_result(&row, "")?),
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
                "archive" => snapshot
                    .archives
                    .push(pin_resource_history::Model::from_query_result(&row, "")?),
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
                && matches!(row.outcome.as_str(), "matched" | "not_submitted")
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
    if job.operation != "submit" {
        return Ok(());
    }
    let Some(row) = latest(db, &job.id).await? else {
        return Ok(());
    };
    if row.outcome != "matched" {
        return Ok(());
    }
    let Some(remote) = remote_pin::Entity::find_by_id((job.provider.clone(), job.cid.clone()))
        .one(db)
        .await?
    else {
        return Ok(());
    };
    if projection_allowed(db, job, &remote).await? {
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
    if ledger.effect == "reserved" {
        return Ok(super::decode_route(ledger).is_some_and(|route| is_rpc(&route)));
    }
    let rows = evidence::Entity::find()
        .filter(evidence::Column::Provider.eq(&ledger.provider))
        .filter(evidence::Column::ExpectedCid.eq(&ledger.cid))
        .all(db)
        .await?;
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
            || !matches!(row.outcome.as_str(), "matched" | "not_submitted")
            || ledger.route.as_deref() != Some(row.route.as_str())
        {
            continue;
        }
        if ledger.effect == "not_created" && row.outcome != "not_submitted" {
            continue;
        }
        if pin_resource_history::Entity::find()
            .filter(pin_resource_history::Column::Provider.eq(&ledger.provider))
            .filter(pin_resource_history::Column::Cid.eq(&ledger.cid))
            .filter(pin_resource_history::Column::Epoch.gte(row.remote_epoch))
            .one(db)
            .await?
            .is_none()
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
    let rows = evidence::Entity::find()
        .filter(evidence::Column::Provider.eq(provider))
        .filter(evidence::Column::ExpectedCid.eq(cid))
        .filter(evidence::Column::Outcome.eq("matched"))
        .all(db)
        .await?;
    for row in rows {
        if pin_resource_history::Entity::find()
            .filter(pin_resource_history::Column::Provider.eq(provider))
            .filter(pin_resource_history::Column::Cid.eq(cid))
            .filter(pin_resource_history::Column::Epoch.gte(row.remote_epoch))
            .one(db)
            .await?
            .is_none()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Last pre-dispatch fence: a fresh claim alone cannot authorize an archived
/// resource lifetime or today's different endpoint/account/strategy.
pub async fn dispatch_allowed<C: ConnectionTrait>(db: &C, id: &str) -> AppResult<bool> {
    let Some(row) = evidence::Entity::find_by_id(id.to_owned()).one(db).await? else {
        return Ok(false);
    };
    if row.outcome != "in_flight" || row.observed_at.is_some() {
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
    if registered.retired || registered.snapshot != row.route {
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

/// Called inside the prelocked lifecycle transaction. Same-lifetime reference
/// attachments may advance epoch; an archived release or route change may not.
pub async fn projection_allowed<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    remote: &remote_pin::Model,
) -> AppResult<bool> {
    let Some(captured) = pin_invocation_route::Entity::find_by_id(job.id.clone())
        .one(db)
        .await?
    else {
        return Ok(!super::get(db, &job.provider, &job.cid)
            .await?
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
    let same_route = super::get(db, &job.provider, &job.cid)
        .await?
        .and_then(|row| super::decode_route(&row))
        .is_some_and(|current| current == route);
    if !same_route {
        return Ok(false);
    }
    if pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.eq(&job.provider))
        .filter(pin_resource_history::Column::Cid.eq(&job.cid))
        .filter(pin_resource_history::Column::Epoch.gte(captured.remote_epoch))
        .one(db)
        .await?
        .is_some()
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
        return receipt_context_valid(db, &row, job, remote, Some(&request)).await;
    }
    Ok(true)
}

/// Query the immutable original route and every actual resource, even after job deletion.
pub async fn observations<C: ConnectionTrait>(
    db: &C,
    provider: &str,
) -> AppResult<Vec<evidence::Model>> {
    Ok(evidence::Entity::find()
        .filter(evidence::Column::Provider.eq(provider))
        .order_by_asc(evidence::Column::StartedAt)
        .all(db)
        .await?)
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
        let resources: Vec<ResourceEvidence> = serde_json::from_str(&row.resources)
            .map_err(|_| AppError::Internal("invalid durable RPC resources".into()))?;
        for resource in resources {
            if resource.key.as_ref() == Some(key) {
                found.push((row.clone(), resource));
            }
        }
    }
    Ok(found)
}
