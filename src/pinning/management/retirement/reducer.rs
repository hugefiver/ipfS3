use super::{
    spool::{Run, exec, fact, get, one, page_keys},
    *,
};
use crate::store::pinning::management::native::value;
use sea_orm::{ConnectionTrait, TransactionTrait};
mod batch;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceIdentity {
    Known(String),
    Unresolved(i64),
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryTotals {
    pub leases: usize,
    pub targets: usize,
    pub active_targets: usize,
    pub current_resources: usize,
    pub historical_resources: usize,
    pub roots: usize,
    pub logical_bytes: u64,
    pub unique_cids: usize,
    pub unique_resources: usize,
    pub accounted_bytes: u64,
    pub retained: usize,
    pub unknown: usize,
    pub inflight: usize,
    pub cleanup_pending: usize,
    pub attribution_complete: bool,
    pub routes_available: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Routes,
    LedgerRoutes,
    HistoryRoutes,
    InvocationRoutes,
    SubmissionRoutes,
    ReplicaRoutes,
    Leases,
    Targets,
    Jobs,
    RemotePins,
    Ledgers,
    History,
    Roots,
    Submissions,
    Replicas,
}
const PHASES: &[Phase] = &[
    Phase::Routes,
    Phase::LedgerRoutes,
    Phase::HistoryRoutes,
    Phase::InvocationRoutes,
    Phase::SubmissionRoutes,
    Phase::ReplicaRoutes,
    Phase::Leases,
    Phase::Targets,
    Phase::Jobs,
    Phase::RemotePins,
    Phase::Ledgers,
    Phase::History,
    Phase::Roots,
    Phase::Submissions,
    Phase::Replicas,
];
impl Phase {
    fn table(self) -> &'static str {
        match self {
            Self::Routes => "pin_provider_routes",
            Self::LedgerRoutes | Self::Ledgers => "remote_pin_ledger",
            Self::HistoryRoutes | Self::History => "pin_resource_history",
            Self::InvocationRoutes => "pin_invocation_routes",
            Self::SubmissionRoutes | Self::Submissions => "pin_submit_observations",
            Self::ReplicaRoutes | Self::Replicas => "pin_replica_observations",
            Self::Leases => "pin_leases",
            Self::Targets => "pin_lease_targets",
            Self::Jobs => "pin_jobs",
            Self::RemotePins => "remote_pins",
            Self::Roots => "zip_root_references",
        }
    }
    fn discovers_route(self) -> bool {
        matches!(
            self,
            Self::Routes
                | Self::LedgerRoutes
                | Self::HistoryRoutes
                | Self::InvocationRoutes
                | Self::SubmissionRoutes
                | Self::ReplicaRoutes
        )
    }
}

fn required<'a>(row: &'a NativeRow, name: &str) -> Result<&'a str> {
    value(row, name).ok_or(ManagementError::InvalidEvidence)
}
fn same_domain(a: &RouteIdentity, b: &RouteIdentity) -> bool {
    a.backend == b.backend && a.scope == b.scope && a.resource_type == b.resource_type
}
fn domain(route: &RouteIdentity) -> Result<String> {
    encode(&(&route.backend, &route.scope, route.resource_type))
}
fn canonical(cid: &str) -> Result<String> {
    crate::pinning::provider::canonical_resource_cid(cid)
        .map_err(|_| ManagementError::InvalidEvidence)
}
struct ProjectionContext<'a, 'run> {
    checkpoint: &'a SourceCheckpoint,
    configuration: &'a ConfigurationBinding,
    run: &'a Run<'run>,
    allocations: &'a std::collections::BTreeMap<String, (Option<RouteSnapshot>, Option<String>)>,
    writes: Option<&'a batch::Writes>,
}

impl InventorySpool {
    pub(super) async fn lookup<T: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
        id: &str,
    ) -> Result<Option<T>> {
        self.lookup_in(&self.db, kind, id).await
    }
    pub(super) async fn lookup_in<T: serde::de::DeserializeOwned>(
        &self,
        db: &impl ConnectionTrait,
        kind: &str,
        id: &str,
    ) -> Result<Option<T>> {
        one(
            db,
            "SELECT payload FROM facts WHERE kind=$1 AND identity=$2",
            vec![kind.into(), id.into()],
        )
        .await?
        .map(|r| decode(&get::<String>(&r, "payload")?))
        .transpose()
    }
    fn allocated(context: &ProjectionContext<'_, '_>, provider: &str) -> bool {
        context
            .allocations
            .get(provider)
            .is_some_and(|(route, _)| route.is_some())
    }

    /// Relations and reducer cursors are committed together. A restart never
    /// recounts a successful prefix and a failed page cannot publish EOF.
    pub async fn reduce(
        &self,
        config: &ConfigurationBinding,
        limits: &RunLimits,
    ) -> Result<Progress> {
        super::cache_guard::check(&self.db).await?;
        let mut run = super::spool::task_run(&self.db, limits).await?;
        run.check()?;
        let checkpoint = self.checkpoint().await?;
        let configuration_tx = self
            .db
            .begin()
            .await
            .map_err(|_| ManagementError::Database)?;
        fact(&configuration_tx, "configuration", "inventory", config).await?;
        super::cache_guard::accept(&configuration_tx).await?;
        configuration_tx
            .commit()
            .await
            .map_err(|_| ManagementError::Database)?;
        let mut committed = false;
        loop {
            let state = one(
                &self.db,
                "SELECT phase,reducer_table,reducer_after,rows,bytes FROM state WHERE id=1",
                vec![],
            )
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
            let phase: i64 = get(&state, "phase")?;
            if phase == 0 {
                return Err(ManagementError::InvalidEvidence);
            }
            if phase == 2 {
                if !self.reduced_here.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(ManagementError::InvalidEvidence);
                }
                return Ok(Progress::Reduced);
            }
            run.task(
                get::<i64>(&state, "rows")? as u64,
                get::<i64>(&state, "bytes")? as u64,
            )?;
            let index = get::<i64>(&state, "reducer_table")? as usize;
            let after: String = get(&state, "reducer_after")?;
            let Some(&phase) = PHASES.get(index) else {
                let Some(totals) = self.compute_totals(&mut run, committed).await? else {
                    return Ok(Progress::Pending);
                };
                let tx = self
                    .db
                    .begin()
                    .await
                    .map_err(|_| ManagementError::Database)?;
                fact(&tx, "totals", "inventory", &totals).await?;
                fact(&tx, "configuration", "inventory", config).await?;
                exec(&tx, "UPDATE state SET phase=2 WHERE id=1", vec![]).await?;
                run.check()?;
                super::cache_guard::accept(&tx).await?;
                tx.commit().await.map_err(|_| ManagementError::Database)?;
                self.reduced_here
                    .store(true, std::sync::atomic::Ordering::Release);
                return Ok(Progress::Reduced);
            };
            if !run.page(0, 0)? {
                return Ok(Progress::Pending);
            }
            let table = phase.table();
            let page_started = std::time::Instant::now();
            let keys = page_keys(&self.db,
                "SELECT row_key AS identity FROM members WHERE table_name=$1 AND row_key>$2 ORDER BY row_key LIMIT 64",
                vec![table.into(), after.clone().into()],
            ).await?;
            if !keys.is_empty() {
                let mut projected = self.projection_page(&self.db, &keys).await?;
                let read_rows = projected.len() as u64;
                let bytes = projected
                    .iter()
                    .filter_map(|(_, row)| row.as_ref().ok())
                    .flat_map(|row| row.values())
                    .flatten()
                    .map(|v| v.len() as u64)
                    .sum();
                let mut allocations = std::collections::BTreeMap::new();
                let mut route_bytes = 0usize;
                let mut take = projected.len();
                if !phase.discovers_route() {
                    for (index, (_, row)) in projected.iter().enumerate() {
                        if let Ok(row) = row
                            && let Some(provider) = value(row, "provider")
                            && !allocations.contains_key(provider)
                        {
                            let size=one(&self.db,"SELECT length(CAST(payload AS BLOB)) AS bytes FROM facts WHERE kind='route' AND identity=$1",vec![provider.into()]).await?.map(|row|get::<i64>(&row,"bytes")).transpose()?.unwrap_or(0);
                            let size = usize::try_from(size)
                                .map_err(|_| ManagementError::InvalidEvidence)?;
                            if route_bytes > 0 && size > 1024 * 1024 - route_bytes.min(1024 * 1024)
                            {
                                take = index;
                                break;
                            }
                            route_bytes = route_bytes
                                .checked_add(size)
                                .ok_or(ManagementError::Budget)?;
                            let route = self.lookup::<RouteSnapshot>("route", provider).await?;
                            let attribution = one(
                                &self.db,
                                "SELECT domain FROM allocation_attribution WHERE allocation=$1",
                                vec![provider.into()],
                            )
                            .await?
                            .map(|row| get::<Option<String>>(&row, "domain"))
                            .transpose()?
                            .flatten();
                            allocations.insert(provider.to_owned(), (route, attribution));
                        }
                    }
                }
                projected.truncate(take);
                super::spool::budget::charge(&self.db, &run, read_rows, bytes).await?;
                let tx = self
                    .db
                    .begin()
                    .await
                    .map_err(|_| ManagementError::Database)?;
                let result = async {
                    let writes=batch::Writes::default();
                    let mut last = after.clone();
                    for (key, row) in projected {
                        run.check()?;
                        let identity = encode(&key)?;
                        match row {
                            Ok(row) => self.reduce_row(
                                &tx, phase, &key, &row,
                                &ProjectionContext { checkpoint: &checkpoint, configuration: config, run: &run, allocations:&allocations, writes:matches!(phase,Phase::Leases|Phase::Targets).then_some(&writes) },
                            ).await?,
                            Err(ManagementError::Budget) => {
                                run.check()?;
                                fact(&tx, "unknown", &identity, &key).await?;
                            }
                            _ => return Err(ManagementError::InvalidEvidence),
                        }
                        last = identity;
                        let pending={let mut writes=writes.lock().map_err(|_|ManagementError::InvalidEvidence)?;writes.full().then(||std::mem::take(&mut *writes))};
                        if let Some(pending)=pending {pending.flush(&tx,&run).await?;}
                    }
                    writes.into_inner().map_err(|_|ManagementError::InvalidEvidence)?.flush(&tx,&run).await?;
                    let changed = tx.execute(sea_orm::Statement::from_sql_and_values(
                        sea_orm::DatabaseBackend::Sqlite,
                        "UPDATE state SET reducer_after=$1 WHERE id=1 AND phase=1 AND reducer_table=$2 AND reducer_after=$3",
                        vec![last.into(), (index as i64).into(), after.into()],
                    )).await.map_err(|_| ManagementError::Database)?;
                    if changed.rows_affected() != 1 { return Err(ManagementError::Changed); }
                    run.check()
                }.await;
                if let Err(error) = result {
                    tx.rollback().await.map_err(|_| ManagementError::Database)?;
                    if error == ManagementError::Budget
                        && committed
                        && run.remaining().is_zero()
                        && !limits.cancellation.is_cancelled()
                    {
                        return Ok(Progress::Pending);
                    }
                    return Err(error);
                }
                super::cache_guard::accept(&tx).await?;
                tx.commit().await.map_err(|_| ManagementError::Database)?;
                super::spool::budget::timed(&self.db, "reducer", page_started.elapsed()).await?;
                committed = true;
            } else {
                if one(
                    &self.db,
                    "SELECT table_name FROM eof WHERE table_name=$1",
                    vec![table.into()],
                )
                .await?
                .is_none()
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                let tx = self
                    .db
                    .begin()
                    .await
                    .map_err(|_| ManagementError::Database)?;
                exec(
                    &tx,
                    "UPDATE state SET reducer_table=$1,reducer_after='' WHERE id=1",
                    vec![((index + 1) as i64).into()],
                )
                .await?;
                super::cache_guard::accept(&tx).await?;
                tx.commit().await.map_err(|_| ManagementError::Database)?;
            }
        }
    }

    async fn reduce_row(
        &self,
        tx: &impl ConnectionTrait,
        phase: Phase,
        key: &NativeKey,
        row: &NativeRow,
        context: &ProjectionContext<'_, '_>,
    ) -> Result<()> {
        context.run.check()?;
        let cp = context.checkpoint;
        let config = context.configuration;
        let unknown = || encode(key);
        if phase.discovers_route() {
            let route = match phase {
                Phase::Routes => value(row, "snapshot")
                    .map(decode::<crate::pinning::identity::ProviderRouteSnapshot>),
                Phase::HistoryRoutes => value(row, "ledger").map(|s| {
                    let v: serde_json::Value = decode(s)?;
                    let raw = v
                        .get("ledger")
                        .and_then(|l| l.get("route"))
                        .and_then(|v| v.as_str())
                        .ok_or(ManagementError::InvalidEvidence)?;
                    decode(raw)
                }),
                _ => value(row, "route").map(decode),
            }
            .transpose();
            match route {
                Ok(Some(original)) => {
                    let identity = RouteIdentity::from_original(original.clone());
                    if phase == Phase::Routes {
                        fact(tx, "known_allocation", required(row, "provider")?, &true).await?;
                    }
                    if same_domain(&identity, &cp.route)
                        || identity.provider_id == cp.route.provider_id
                    {
                        let allocation = if let Some(provider) = value(row, "provider") {
                            provider.to_owned()
                        } else if let Some(job) = value(row, "job_id") {
                            let jobkey = NativeKey {
                                table: "pin_jobs".into(),
                                values: vec![job.into()],
                            };
                            let job = self
                                .projection_row_in(tx, &jobkey)
                                .await?
                                .ok_or(ManagementError::InvalidEvidence)?;
                            required(&job, "provider")?.to_owned()
                        } else {
                            return fact(tx, "unknown", &unknown()?, key).await;
                        };
                        let available = config.routes.contains(&original);
                        let domain = domain(&identity)?;
                        if let Some(attribution) = one(
                            tx,
                            "SELECT domain FROM allocation_attribution WHERE allocation=$1",
                            vec![allocation.clone().into()],
                        )
                        .await?
                        {
                            if get::<Option<String>>(&attribution, "domain")?
                                .is_some_and(|known| known != domain)
                            {
                                exec(tx,"UPDATE allocation_attribution SET domain=NULL WHERE allocation=$1",vec![allocation.clone().into()]).await?;
                            }
                        } else {
                            exec(
                                tx,
                                "INSERT INTO allocation_attribution VALUES($1,$2)",
                                vec![allocation.clone().into(), domain.clone().into()],
                            )
                            .await?;
                        }
                        let route = RouteSnapshot {
                            identity,
                            original: Some(original.clone()),
                            available,
                            allocation_allowed: false,
                            cleanup: match original.cleanup {
                                crate::pinning::identity::CleanupMode::Managed => {
                                    CleanupMode::Managed
                                }
                                _ => CleanupMode::Retain,
                            },
                            exclusive_management: false,
                        };
                        // Historical revisions share allocation keys. Preserve
                        // all routes, not just the current configured alias.
                        fact(tx, "route_revision", &encode(&route.identity)?, &route).await?;
                        if self
                            .lookup_in::<RouteSnapshot>(tx, "route", &allocation)
                            .await?
                            .is_none()
                        {
                            fact(tx, "route", &allocation, &route).await?;
                        }
                        if phase == Phase::Routes {
                            fact(tx, "registered_route", &allocation, &route).await?;
                            fact(tx, "registered_identity", &encode(&route.identity)?, &route)
                                .await?;
                            if self
                                .lookup_in::<NativeKey>(
                                    tx,
                                    "registered_anchor",
                                    &encode(&route.identity)?,
                                )
                                .await?
                                .is_none()
                            {
                                fact(tx, "registered_anchor", &encode(&route.identity)?, key)
                                    .await?;
                            }
                        }
                    }
                }
                _ => fact(tx, "unknown", &unknown()?, key).await?,
            }
        } else if phase == Phase::Leases {
            context
                .fact(tx, "lease_all", required(row, "id")?, row)
                .await?;
            let time = |column| {
                value(row, column)
                    .and_then(|raw| crate::store::pinning::management::native::parse_time(raw).ok())
            };
            if !matches!(
                value(row, "state"),
                Some("active" | "expired" | "cancelled")
            ) || value(row, "generation")
                .and_then(|n| n.parse::<i64>().ok())
                .is_none_or(|n| n <= 0)
                || time("created_at")
                    .zip(time("expires_at"))
                    .is_none_or(|(start, end)| end <= start)
            {
                context
                    .fact(tx, "lease_invalid", required(row, "id")?, &true)
                    .await?;
            }
        } else if phase == Phase::Targets {
            context
                .fact(tx, "target_lease", required(row, "lease_id")?, &true)
                .await?;
            let allocation = required(row, "provider")?;
            if Self::allocated(context, allocation) {
                let lease_id = required(row, "lease_id")?;
                let lease = self
                    .lookup_in::<NativeRow>(tx, "lease_all", lease_id)
                    .await?;
                if self
                    .lookup_in::<bool>(tx, "lease_invalid", lease_id)
                    .await?
                    .is_some()
                {
                    let key = NativeKey {
                        table: "pin_leases".into(),
                        values: vec![lease_id.into()],
                    };
                    context.fact(tx, "unknown", &encode(&key)?, &key).await?;
                }
                let active = lease.as_ref().is_some_and(|lease| {
                    value(lease, "state") == Some("active")
                        && value(lease, "expires_at")
                            .and_then(|v| {
                                crate::store::pinning::management::native::parse_time(v).ok()
                            })
                            .is_some_and(|expiry| expiry > cp.observed_at)
                });
                if let Some(lease) = lease {
                    context.fact(tx, "lease", lease_id, &lease).await?
                } else {
                    context.fact(tx, "unknown", &unknown()?, key).await?
                }
                let Ok(cid) = canonical(required(row, "cid")?) else {
                    return context.fact(tx, "unknown", &unknown()?, key).await;
                };
                let bytes = value(row, "logical_size").and_then(|v| v.parse::<u64>().ok());
                if bytes.is_none() {
                    context.fact(tx, "unknown", &unknown()?, key).await?
                }
                let referenced =
                    active && !matches!(value(row, "state"), Some("released" | "evicted"));
                let edge = vec![
                    required(row, "id")?.into(),
                    lease_id.into(),
                    cid.clone().into(),
                    allocation.into(),
                    bytes.unwrap_or(0).to_string().into(),
                    i64::from(active).into(),
                    i64::from(referenced).into(),
                ];
                if let Some(writes) = context.writes {
                    writes
                        .lock()
                        .map_err(|_| ManagementError::InvalidEvidence)?
                        .edge(edge);
                } else {
                    exec(
                        tx,
                        "INSERT INTO target_edges VALUES($1,$2,$3,$4,$5,$6,$7)",
                        edge,
                    )
                    .await?;
                }
                self.count_relation(
                    tx,
                    key,
                    allocation,
                    &cid,
                    [referenced, false, false],
                    context,
                )
                .await?;
                context
                    .fact(tx, "target", required(row, "id")?, row)
                    .await?;
                if let Some(writes) = context.writes {
                    writes
                        .lock()
                        .map_err(|_| ManagementError::InvalidEvidence)?
                        .cid(&cid);
                } else {
                    add_cid(tx, &cid).await?;
                }
            } else if self
                .lookup_in::<bool>(tx, "known_allocation", allocation)
                .await?
                .is_none()
            {
                fact(tx, "unknown", &unknown()?, key).await?;
            }
        } else if phase == Phase::Jobs {
            let allocation = required(row, "provider")?;
            if Self::allocated(context, allocation) {
                if !matches!(value(row, "state"), Some("pending" | "running" | "done"))
                    || !matches!(
                        value(row, "operation"),
                        Some("submit" | "poll" | "unpin" | "reconcile")
                    )
                {
                    fact(tx, "unknown", &unknown()?, key).await?;
                }
                let Ok(cid) = canonical(required(row, "cid")?) else {
                    return fact(tx, "unknown", &unknown()?, key).await;
                };
                let inflight = matches!(
                    value(row, "state"),
                    Some("running" | "claimed" | "submitting")
                );
                let cleanup =
                    value(row, "operation") == Some("unpin") && value(row, "state") != Some("done");
                exec(
                    tx,
                    "INSERT INTO jobs VALUES($1,$2,$3,$4,$5)",
                    vec![
                        required(row, "id")?.into(),
                        allocation.into(),
                        cid.clone().into(),
                        i64::from(inflight).into(),
                        i64::from(cleanup).into(),
                    ],
                )
                .await?;
                self.count_relation(
                    tx,
                    key,
                    allocation,
                    &cid,
                    [false, inflight, cleanup],
                    context,
                )
                .await?;
                fact(tx, "job", required(row, "id")?, row).await?;
                if value(row, "state") != Some("done")
                    && !self
                        .contains_in(
                            tx,
                            &NativeKey {
                                table: "remote_pins".into(),
                                values: vec![allocation.into(), required(row, "cid")?.into()],
                            },
                        )
                        .await?
                {
                    fact(tx, "unknown", &unknown()?, key).await?;
                }
            } else if self
                .lookup_in::<bool>(tx, "known_allocation", allocation)
                .await?
                .is_none()
            {
                fact(tx, "unknown", &unknown()?, key).await?;
            }
        } else if phase == Phase::RemotePins {
            if Self::allocated(context, required(row, "provider")?) {
                fact(
                    tx,
                    "remote_pin",
                    &encode(&(required(row, "provider")?, required(row, "cid")?))?,
                    row,
                )
                .await?;
                if !self
                    .contains_in(
                        tx,
                        &NativeKey {
                            table: "remote_pin_ledger".into(),
                            values: key.values.clone(),
                        },
                    )
                    .await?
                {
                    fact(tx, "unknown", &unknown()?, key).await?;
                }
            } else if self
                .lookup_in::<bool>(tx, "known_allocation", required(row, "provider")?)
                .await?
                .is_none()
            {
                fact(tx, "unknown", &unknown()?, key).await?;
            }
        } else if matches!(phase, Phase::Ledgers | Phase::History) {
            let allocation = required(row, "provider")?;
            if !Self::allocated(context, allocation) {
                return Ok(());
            }
            let result = self.remote(tx, row, phase == Phase::History).await;
            match result {
                Ok(remote) => {
                    fact(
                        tx,
                        if phase == Phase::Ledgers {
                            "current"
                        } else {
                            "history"
                        },
                        &encode(key)?,
                        &remote,
                    )
                    .await?;
                    quote(tx, &remote).await?;
                }
                Err(_) => fact(tx, "unknown", &unknown()?, key).await?,
            }
        } else if phase == Phase::Roots {
            // Roots cannot be attributed to a provider from their CID. Preserve
            // their complete keys and force explicit ownership proofs for release.
            if super::super::root_references::logically_active(row) {
                fact(tx, "root", &encode(key)?, key).await?;
            } else if value(row, "logical_released_at")
                .and_then(|raw| crate::store::pinning::management::native::parse_time(raw).ok())
                .is_none_or(|released| released > cp.observed_at)
            {
                fact(tx, "root", &encode(key)?, key).await?;
                fact(tx, "unknown", &unknown()?, key).await?;
            }
        } else if let Some(provider) = value(row, "provider") {
            if Self::allocated(context, provider) {
                match super::observations::reduce(
                    tx,
                    key,
                    row,
                    phase == Phase::Replicas,
                    context.run,
                )
                .await
                {
                    Ok(()) => {}
                    Err(ManagementError::InvalidEvidence) => {
                        fact(tx, "unknown", &unknown()?, key).await?
                    }
                    Err(error) => return Err(error),
                }
            } else if self
                .lookup_in::<bool>(tx, "known_allocation", provider)
                .await?
                .is_none()
            {
                fact(tx, "unknown", &unknown()?, key).await?;
            }
        }
        Ok(())
    }

    async fn remote(
        &self,
        tx: &impl ConnectionTrait,
        row: &NativeRow,
        historical: bool,
    ) -> Result<RemoteSnapshot> {
        let (ledger, resource_id) = if historical {
            let envelope: serde_json::Value = decode(required(row, "ledger")?)?;
            let model: crate::store::entities::remote_pin_ledger::Model = serde_json::from_value(
                envelope
                    .get("ledger")
                    .cloned()
                    .ok_or(ManagementError::InvalidEvidence)?,
            )
            .map_err(|_| ManagementError::InvalidEvidence)?;
            if model.provider != required(row, "provider")? || model.cid != required(row, "cid")? {
                return Err(ManagementError::InvalidEvidence);
            }
            (
                LedgerFields {
                    cid: model.cid,
                    route: model.route,
                    state: model.effect,
                    ownership: model.ownership,
                    epoch: required(row, "epoch")?
                        .parse()
                        .map_err(|_| ManagementError::InvalidEvidence)?,
                    bytes: None,
                },
                envelope
                    .get("request_id")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
            )
        } else {
            let provider = required(row, "provider")?.to_owned();
            let cid = required(row, "cid")?.to_owned();
            let remote = self
                .lookup_in::<NativeRow>(tx, "remote_pin", &encode(&(&provider, &cid))?)
                .await?;
            let remote = remote.ok_or(ManagementError::InvalidEvidence)?;
            let id = value(&remote, "request_id").map(str::to_owned);
            let bytes = if value(&remote, "status") == Some("absent") {
                None
            } else {
                value(&remote, "cid_size").and_then(|s| s.parse().ok())
            };
            (
                LedgerFields {
                    cid,
                    route: value(row, "route").map(str::to_owned),
                    state: required(row, "effect")?.into(),
                    ownership: value(row, "ownership").unwrap_or("unknown").into(),
                    epoch: required(&remote, "epoch")?
                        .parse()
                        .map_err(|_| ManagementError::InvalidEvidence)?,
                    bytes,
                },
                id,
            )
        };
        let route = RouteIdentity::from_original(decode(
            ledger
                .route
                .as_deref()
                .ok_or(ManagementError::InvalidEvidence)?,
        )?);
        let cid = canonical(&ledger.cid)?;
        let resource_key = encode(&route.remote_key(&cid))?;
        let counts = one(
            tx,
            "SELECT refs,inflight,cleanup FROM resource_counts WHERE resource_key=$1",
            vec![resource_key.clone().into()],
        )
        .await?;
        let (references, inflight, cleanup_jobs) = if let Some(counts) = counts {
            (
                u64::try_from(get::<i64>(&counts, "refs")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?,
                u64::try_from(get::<i64>(&counts, "inflight")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?,
                u64::try_from(get::<i64>(&counts, "cleanup")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?,
            )
        } else {
            (0, 0, 0)
        };
        let effect = if inflight > 0 {
            SideEffect::InFlight
        } else {
            match ledger.state.as_str() {
                "not_created" | "absent" => SideEffect::Rejected,
                "confirmed" | "retained" | "cleanup_pending" if resource_id.is_some() => {
                    SideEffect::Present
                }
                _ => SideEffect::Unknown,
            }
        };
        let ownership = match ledger.ownership.as_str() {
            "application_created" if route.resource_type != ResourceType::RpcPin => {
                Ownership::ApplicationCreated
            }
            "external_existing" => Ownership::ExternalExisting,
            _ => Ownership::Unknown,
        };
        Ok(RemoteSnapshot {
            key: route.remote_key(&cid),
            route,
            resource_id,
            epoch: ledger.epoch,
            effect,
            ownership,
            application_references: (!historical).then_some(references),
            inflight_operations: if historical {
                None
            } else {
                Some(inflight.try_into().map_err(|_| ManagementError::Budget)?)
            },
            retained: ledger.state == "retained",
            cleanup_pending: ledger.state == "cleanup_pending" || cleanup_jobs > 0,
            accounted_bytes: ledger.bytes,
            confirmations: vec![],
            confirmed_replicas: None,
            observation_current: None,
            replica_started_at: None,
        })
    }
    async fn count_relation(
        &self,
        tx: &impl ConnectionTrait,
        key: &NativeKey,
        allocation: &str,
        cid: &str,
        flags: [bool; 3],
        context: &ProjectionContext<'_, '_>,
    ) -> Result<()> {
        if !flags.into_iter().any(|flag| flag) {
            return Ok(());
        }
        let (route, attribution) = context
            .allocations
            .get(allocation)
            .ok_or(ManagementError::InvalidEvidence)?;
        let Some(known) = attribution else {
            return fact(tx, "unknown", &encode(key)?, key).await;
        };
        let route = route.as_ref().ok_or(ManagementError::InvalidEvidence)?;
        if domain(&route.identity)? != *known {
            return Err(ManagementError::InvalidEvidence);
        }
        let resource = encode(&route.identity.remote_key(cid))?;
        if let Some(writes) = context.writes {
            return writes
                .lock()
                .map_err(|_| ManagementError::InvalidEvidence)?
                .count(resource, flags);
        }
        let old = one(
            tx,
            "SELECT refs,inflight,cleanup FROM resource_counts WHERE resource_key=$1",
            vec![resource.clone().into()],
        )
        .await?;
        let mut counts = [0i64; 3];
        for (index, column) in ["refs", "inflight", "cleanup"].into_iter().enumerate() {
            let previous = old
                .as_ref()
                .map(|row| get::<i64>(row, column))
                .transpose()?
                .unwrap_or(0);
            if previous < 0 {
                return Err(ManagementError::InvalidEvidence);
            }
            counts[index] = previous
                .checked_add(i64::from(flags[index]))
                .ok_or(ManagementError::Budget)?;
        }
        let sql = if old.is_some() {
            "UPDATE resource_counts SET refs=$2,inflight=$3,cleanup=$4 WHERE resource_key=$1"
        } else {
            "INSERT INTO resource_counts VALUES($1,$2,$3,$4)"
        };
        exec(
            tx,
            sql,
            vec![
                resource.into(),
                counts[0].into(),
                counts[1].into(),
                counts[2].into(),
            ],
        )
        .await
    }
}
struct LedgerFields {
    cid: String,
    route: Option<String>,
    state: String,
    ownership: String,
    epoch: i64,
    bytes: Option<u64>,
}
pub(super) async fn add_cid(tx: &impl ConnectionTrait, cid: &str) -> Result<()> {
    exec(
        tx,
        "INSERT INTO cids VALUES($1) ON CONFLICT(cid) DO NOTHING",
        vec![cid.into()],
    )
    .await
}
pub(super) async fn quote(tx: &impl ConnectionTrait, remote: &RemoteSnapshot) -> Result<()> {
    let identity = encode(&(
        &remote.key,
        remote
            .resource_id
            .as_ref()
            .map_or(ResourceIdentity::Unresolved(remote.epoch), |id| {
                ResourceIdentity::Known(id.clone())
            }),
    ))?;
    let mut bytes = remote.accounted_bytes.unwrap_or(0);
    let mut unknown = remote.has_unknown_responsibility();
    let mut inflight =
        remote.effect == SideEffect::InFlight || remote.inflight_operations.is_some_and(|n| n > 0);
    let mut cleanup = remote.cleanup_pending;
    let mut retained = remote.retained;
    if let Some(row) = one(
        tx,
        "SELECT * FROM quotes WHERE identity=$1",
        vec![identity.clone().into()],
    )
    .await?
    {
        bytes = bytes.max(
            get::<String>(&row, "bytes")?
                .parse()
                .map_err(|_| ManagementError::InvalidEvidence)?,
        );
        unknown |= get::<i64>(&row, "unknown")? != 0;
        inflight |= get::<i64>(&row, "inflight")? != 0;
        cleanup |= get::<i64>(&row, "cleanup")? != 0;
        retained |= get::<i64>(&row, "retained")? != 0;
        exec(tx,"UPDATE quotes SET bytes=$2,unknown=$3,inflight=$4,cleanup=$5,retained=$6 WHERE identity=$1",vec![identity.into(),bytes.to_string().into(),i64::from(unknown).into(),i64::from(inflight).into(),i64::from(cleanup).into(),i64::from(retained).into()]).await?;
    } else {
        exec(
            tx,
            "INSERT INTO quotes VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
            vec![
                identity.into(),
                encode(&remote.key)?.into(),
                remote.key.cid.clone().into(),
                bytes.to_string().into(),
                i64::from(unknown).into(),
                i64::from(inflight).into(),
                i64::from(cleanup).into(),
                i64::from(retained).into(),
            ],
        )
        .await?;
    }
    add_cid(tx, &remote.key.cid).await
}
