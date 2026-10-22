//! Resumable confirmation staging. Only the target archive supplies accepted cursors.
use super::{
    spool::{Run, exec, get, one},
    *,
};
use crate::pinning::management::DryRunPlan;
use crate::store::pinning::management::{
    native,
    records::{self, LocalAuthorization, segmented::progress},
    transaction::{ConfirmationEngine, ConfirmationTransaction, TransactionFailure},
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmationPhase {
    #[default]
    Validating,
    Canonical,
    Comparing,
    Archiving,
    Ready,
    Confirmed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ConfirmationEffects {
    pub database_commits: u64,
    pub commit_unknown: bool,
}
pub struct ConfirmationAdvance {
    pub phase: ConfirmationPhase,
    pub effects: ConfirmationEffects,
    pub progress: Result<ConfirmationProgress>,
}
pub enum ConfirmationProgress {
    Incomplete,
    Ready(Box<ReadyConfirmation>),
    Confirmed(Box<records::ConfirmedPlan>),
}

/// Non-importable publication prerequisite. The final transaction checks its
/// immutable DB root; neither its local cache nor a JSON phase grants authority.
pub struct ReadyConfirmation {
    pub(crate) task: std::sync::Arc<task_budget::TaskBudget>,
    pub(crate) plan: DryRunPlan,
    pub(crate) archive_id: String,
    pub(crate) header: String,
    pub(crate) position: progress::Position,
    pub(crate) origin: SourceCheckpoint,
    pub(crate) request: PlanRequest,
    pub(crate) snapshot: Snapshot,
    pub(crate) principal: String,
}
impl ReadyConfirmation {
    pub fn plan(&self) -> &DryRunPlan {
        &self.plan
    }
    pub(crate) async fn check(&self, tx: &sea_orm::DatabaseTransaction) -> Result<()> {
        self.origin.fence(tx).await?;
        if progress::load(tx, &self.archive_id, &self.header, true)
            .await?
            .as_ref()
            != Some(&self.position)
            || !self.position.sealed
        {
            return Err(ManagementError::Changed);
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Stage {
    Source,
    Wire,
    Reduce,
    Compare,
    Union,
    UnionCompare,
    Archive,
    Ready,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    stage: Stage,
    source: source::attest::Cursor,
    provider: usize,
    comparison: comparison::Cursor,
    canonical: Option<CanonicalPlan>,
    metered_before: Option<TaskCoordinates>,
    metered_after: Option<TaskCoordinates>,
    wire_after: Option<(String, Vec<u8>)>,
    wire_count: i64,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskCoordinates {
    rows: i64,
    bytes: i64,
    pages: i64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalPlan {
    items: Vec<PlanEntry>,
    summary: Summary,
}
#[derive(Serialize)]
struct Fixed<'a> {
    format: &'static str,
    principal: &'a str,
    request: &'a PlanRequest,
    local_digest: String,
    local_observed_at: i64,
    local_database_now: Option<chrono::DateTime<chrono::Utc>>,
    configuration: &'a ConfigurationBinding,
    origin: SourceCheckpoint,
    reviewed_digest: String,
    summary: &'a Summary,
    inventories: Vec<&'a RetirementInventoryRef>,
}

struct ReviewedPlan<'a> {
    request: &'a PlanRequest,
    snapshot: &'a Snapshot,
    plan: &'a DryRunPlan,
}

struct ProgressArchive<'a> {
    db: &'a DatabaseConnection,
    id: &'a str,
    header: &'a str,
}

struct ConfirmationUpdate<'a> {
    position: &'a mut progress::Position,
    state: &'a mut State,
    effects: &'a mut ConfirmationEffects,
    phase: &'a mut ConfirmationPhase,
}

impl PreparedPlanning {
    pub async fn advance_confirmation(
        &self,
        db: &DatabaseConnection,
        config: &crate::pinning::management::local_config::ManagementConfig,
        auth: &LocalAuthorization,
        request: &PlanRequest,
        snapshot: &Snapshot,
        limits: &RunLimits,
    ) -> ConfirmationAdvance {
        advance(db, config, auth, request, snapshot, &self.plan, limits).await
    }
}

pub(crate) async fn advance(
    db: &DatabaseConnection,
    config: &crate::pinning::management::local_config::ManagementConfig,
    auth: &LocalAuthorization,
    request: &PlanRequest,
    snapshot: &Snapshot,
    plan: &DryRunPlan,
    limits: &RunLimits,
) -> ConfirmationAdvance {
    let mut effects = ConfirmationEffects::default();
    let mut phase = ConfirmationPhase::Validating;
    let progress = advance_inner(
        db,
        config,
        auth,
        ReviewedPlan {
            request,
            snapshot,
            plan,
        },
        limits,
        &mut effects,
        &mut phase,
    )
    .await;
    let progress = match progress {
        Err(ManagementError::Budget)
            if !effects.commit_unknown
                && !limits.cancellation.is_cancelled()
                && Run::new(limits).remaining().is_zero() =>
        {
            Ok(ConfirmationProgress::Incomplete)
        }
        progress => progress,
    };
    ConfirmationAdvance {
        phase,
        effects,
        progress,
    }
}

async fn advance_inner(
    db: &DatabaseConnection,
    config: &crate::pinning::management::local_config::ManagementConfig,
    auth: &LocalAuthorization,
    review: ReviewedPlan<'_>,
    limits: &RunLimits,
    effects: &mut ConfirmationEffects,
    phase: &mut ConfirmationPhase,
) -> Result<ConfirmationProgress> {
    let mut engine = ConfirmationEngine::new(db);
    let ReviewedPlan {
        request,
        snapshot,
        plan,
    } = review;
    let primary = plan
        .retirement_sources
        .first()
        .ok_or(ManagementError::Confirmation)?;
    let origin = primary.checkpoint().await?;
    if !origin.complete()
        || origin.is_status()
        || snapshot
            .database_now
            .is_none_or(|now| now > origin.observed_at)
        || origin.database_scope != auth.database_scope()
        || origin.database_identity != auth.database_identity()
    {
        return Err(ManagementError::Changed);
    }
    let configuration = snapshot
        .configuration
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let task = primary
        .task
        .as_ref()
        .ok_or(ManagementError::InvalidEvidence)?;
    let current_configuration = ConfigurationBinding {
        routes: config
            .pinning
            .providers
            .iter()
            .map(|p| p.route_snapshot())
            .collect(),
        policies: config.pinning.policies.clone(),
        limits: config
            .pinning
            .provider_limits
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        zip_rules: Some(config.zip_rules.clone()),
        source_node: config.source_node().map(str::to_owned),
    };
    if configuration != &current_configuration {
        return Err(ManagementError::Changed);
    }
    let header = encode(&Fixed {
        format: "S75-confirm-prefix-1",
        principal: auth.principal(),
        request,
        local_digest: binding_digest(snapshot)?,
        local_observed_at: snapshot.observed_at,
        local_database_now: snapshot.database_now,
        configuration,
        origin: origin.rewind(),
        reviewed_digest: binding_digest(&plan.items())?,
        summary: plan.summary(),
        inventories: plan
            .retirement_sources
            .iter()
            .map(|s| s.reference())
            .collect(),
    })?;
    if header.len() > native::meter::PAGE_BYTES {
        return Err(ManagementError::Budget);
    }
    let id = format!("checkpoint:{}", primary.reference.archive_id);
    let mut position = match progress::load(db, &id, &header, false).await? {
        Some(position) => position,
        None => {
            validate_local(
                &mut engine,
                config,
                request,
                snapshot,
                &origin,
                task,
                limits,
            )
            .await?;
            let state = State {
                stage: Stage::Source,
                source: source::attest::Cursor::new(origin.clone()),
                provider: 0,
                comparison: Default::default(),
                canonical: None,
                metered_before: None,
                metered_after: None,
                wire_after: None,
                wire_count: 0,
            };
            let cursor = encode(&state)?;
            let mut tx = engine
                .begin(Run::new(limits).remaining(), limits.cancellation.clone())
                .await
                .map_err(TransactionFailure::management_error)?;
            let id_copy = id.clone();
            let header_copy = header.clone();
            let origin_copy = origin.clone();
            let cursor_copy = cursor.clone();
            let (created, wrote) = tx
                .run(move |tx, _| {
                    Box::pin(async move {
                        native::scope::writer_boundary(tx).await?;
                        origin_copy.check(tx).await?;
                        let now = crate::store::database_clock::database_now(tx)
                            .await
                            .map_err(|_| ManagementError::Database)?;
                        if origin_copy.observed_at > now {
                            return Err(ManagementError::InvalidEvidence);
                        }
                        if let Some(existing) =
                            progress::load(tx, &id_copy, &header_copy, true).await?
                        {
                            return Ok((existing, false));
                        }
                        let created = progress::create(
                            tx,
                            &id_copy,
                            &header_copy,
                            &origin_copy.database_scope,
                            &origin_copy.database_identity,
                            &cursor_copy,
                        )
                        .await?;
                        Ok((created, true))
                    })
                })
                .await
                .map_err(|error| staging_failure(error, effects))?;
            if wrote {
                commit(
                    &ProgressArchive {
                        db,
                        id: &id,
                        header: &header,
                    },
                    tx,
                    None,
                    &created,
                    &[],
                    effects,
                )
                .await?;
            } else {
                tx.rollback()
                    .await
                    .map_err(TransactionFailure::management_error)?;
            }
            created
        }
    };
    let mut state: State = decode(&position.cursor)?;
    reconcile(primary, &state).await?;
    cache_guard::install(
        &primary.db,
        &[
            "parts",
            "members",
            "wire_keys",
            "eof",
            "facts",
            "quotes",
            "target_edges",
            "jobs",
            "cids",
            "totals_progress",
        ],
    )
    .await?;
    loop {
        cache_guard::check(&primary.db).await?;
        let caps = task.caps(primary, limits).await?;
        let mut run = spool::task_run(&primary.db, &caps).await?;
        if state.stage == Stage::Ready {
            *phase = ConfirmationPhase::Ready;
            if !position.sealed {
                return Err(ManagementError::InvalidEvidence);
            }
            let canonical = state.canonical.ok_or(ManagementError::InvalidEvidence)?;
            let plan = DryRunPlan::from_canonical(
                request.plan_id.clone(),
                canonical.items,
                canonical.summary,
            );
            return Ok(ConfirmationProgress::Ready(Box::new(ReadyConfirmation {
                task: task.clone(),
                plan,
                archive_id: id,
                header,
                position,
                origin,
                request: request.clone(),
                snapshot: snapshot.clone(),
                principal: auth.principal().into(),
            })));
        }
        if state.stage != Stage::Source && state.stage != Stage::Wire {
            break;
        }
        if !run.page(0, 0)? {
            return Ok(ConfirmationProgress::Incomplete);
        }
        if state.stage == Stage::Wire {
            state.metered_before = Some(coordinates(primary).await?);
            spool::budget::charge(&primary.db, &run, 512, 2 * 1024 * 1024).await?;
            let (after, count, payload, done) =
                source::attest::wire(primary, state.wire_after.as_ref()).await?;
            spool::budget::copy_work(&primary.db, &run, payload.len()).await?;
            state.wire_count = state
                .wire_count
                .checked_add(count)
                .ok_or(ManagementError::Budget)?;
            state.wire_after = after;
            if done {
                if state.wire_count != state.source.position {
                    return Err(ManagementError::Changed);
                }
                state.stage = Stage::Reduce;
            }
            canonical::save(
                &mut engine,
                &ProgressArchive {
                    db,
                    id: &id,
                    header: &header,
                },
                ConfirmationUpdate {
                    position: &mut position,
                    state: &mut state,
                    effects,
                    phase,
                },
                &payload,
                primary,
                limits,
            )
            .await?;
            continue;
        }
        let before = state.clone();
        spool::budget::charge(
            &primary.db,
            &run,
            1,
            (header.len() + encode(&state)?.len()) as u64,
        )
        .await?;
        let local = source::attest::local(primary, &state.source, &run).await?;
        // The original task pays before any target-DB page attempt. Its pending
        // coordinates survive both response loss and interrupted reconciliation.
        let metered_before = coordinates(primary).await?;
        let prepaid_rows = 512u64;
        let prepaid_bytes = local
            .bytes
            .checked_add(16 * 1024 * 1024)
            .ok_or(ManagementError::Budget)?;
        let pending = encode(&(
            position.cursor.clone(),
            metered_before.clone(),
            prepaid_rows,
            prepaid_bytes,
        ))?;
        let local_tx = primary
            .db
            .begin()
            .await
            .map_err(|_| ManagementError::Database)?;
        spool::budget::charge(&local_tx, &run, prepaid_rows, prepaid_bytes).await?;
        exec(
            &local_tx,
            "UPDATE task_budget SET confirmation_pending=$1 WHERE id=1",
            vec![pending.into()],
        )
        .await?;
        local_tx
            .commit()
            .await
            .map_err(|_| ManagementError::Database)?;
        let mut tx = engine
            .begin(run.remaining(), limits.cancellation.clone())
            .await
            .map_err(TransactionFailure::management_error)?;
        let id_copy = id.clone();
        let header_copy = header.clone();
        let expected = position.clone();
        let cancel = limits.cancellation.clone();
        let (next, payload, next_state) = tx
            .run(move |tx, _| {
                Box::pin(async move {
                    native::scope::writer_boundary(tx).await?;
                    if progress::load(tx, &id_copy, &header_copy, true)
                        .await?
                        .as_ref()
                        != Some(&expected)
                    {
                        return Err(ManagementError::Changed);
                    }
                    before.source.source.check(tx).await?;
                    let local_bytes = local.bytes;
                    let accepted =
                        source::attest::compare(tx, before.source.clone(), local, cancel).await?;
                    let payload = serde_json::to_vec(&accepted)
                        .map_err(|_| ManagementError::InvalidEvidence)?;
                    let mut next_state = before;
                    next_state.source = accepted.after;
                    if accepted.before.source.complete() {
                        next_state.stage = Stage::Wire;
                    }
                    next_state.metered_before = Some(metered_before.clone());
                    next_state.metered_after = Some(TaskCoordinates {
                        rows: metered_before.rows + accepted.members.len() as i64 + 1,
                        bytes: metered_before.bytes
                            + i64::try_from(local_bytes + payload.len() as u64 * 2)
                                .map_err(|_| ManagementError::Budget)?,
                        pages: metered_before.pages + 1,
                    });
                    let next = progress::append(
                        tx,
                        &id_copy,
                        &header_copy,
                        &expected,
                        &encode(&next_state)?,
                        &payload,
                        false,
                    )
                    .await?;
                    next_state.source.source.check(tx).await?;
                    Ok((next, payload, next_state))
                })
            })
            .await
            .map_err(|error| staging_failure(error, effects))?;
        commit(
            &ProgressArchive {
                db,
                id: &id,
                header: &header,
            },
            tx,
            Some(&position),
            &next,
            &payload,
            effects,
        )
        .await?;
        position = next;
        state = next_state;
        reconcile(primary, &state).await?;
    }
    let mut canonical = Canonical::open(plan, limits).await?;
    let result = canonical
        .advance(
            &mut engine,
            &ProgressArchive {
                db,
                id: &id,
                header: &header,
            },
            auth,
            (review, configuration),
            limits,
            ConfirmationUpdate {
                position: &mut position,
                state: &mut state,
                effects,
                phase,
            },
        )
        .await;
    let closed = canonical.close().await;
    match (result, closed) {
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(error),
        (Ok(progress), Ok(())) => Ok(progress),
    }
}

async fn coordinates(spool: &InventorySpool) -> Result<TaskCoordinates> {
    let row = one(
        &spool.db,
        "SELECT rows_used,bytes_used,pages_used FROM task_budget WHERE id=1",
        vec![],
    )
    .await?
    .ok_or(ManagementError::InvalidEvidence)?;
    Ok(TaskCoordinates {
        rows: get(&row, "rows_used")?,
        bytes: get(&row, "bytes_used")?,
        pages: get(&row, "pages_used")?,
    })
}
async fn reconcile(spool: &InventorySpool, state: &State) -> Result<()> {
    reconcile_coordinates(
        spool,
        state.metered_before.as_ref(),
        state.metered_after.as_ref(),
    )
    .await
}
async fn reconcile_coordinates(
    spool: &InventorySpool,
    metered_before: Option<&TaskCoordinates>,
    metered_after: Option<&TaskCoordinates>,
) -> Result<()> {
    let row = one(
        &spool.db,
        "SELECT confirmation_pending FROM task_budget WHERE id=1",
        vec![],
    )
    .await?
    .ok_or(ManagementError::InvalidEvidence)?;
    let Some(pending) = get::<Option<String>>(&row, "confirmation_pending")? else {
        return Ok(());
    };
    let (_, before, rows, bytes): (String, TaskCoordinates, u64, u64) = decode(&pending)?;
    let tx = spool
        .db
        .begin()
        .await
        .map_err(|_| ManagementError::Database)?;
    if metered_before == Some(&before) {
        let after = metered_after.ok_or(ManagementError::InvalidEvidence)?;
        if after.pages != before.pages.checked_add(1).ok_or(ManagementError::Budget)? {
            return Err(ManagementError::InvalidEvidence);
        }
        let used_rows = after
            .rows
            .checked_sub(before.rows)
            .ok_or(ManagementError::InvalidEvidence)?;
        let used_bytes = after
            .bytes
            .checked_sub(before.bytes)
            .ok_or(ManagementError::InvalidEvidence)?;
        if used_rows < 0 || used_bytes < 0 || used_rows > rows as i64 || used_bytes > bytes as i64 {
            return Err(ManagementError::InvalidEvidence);
        }
        exec(&tx,"UPDATE task_budget SET rows_used=rows_used-$1,bytes_used=bytes_used-$2,confirmation_pending=NULL WHERE id=1 AND confirmation_pending=$3",
            vec![(rows as i64-used_rows).into(),(bytes as i64-used_bytes).into(),pending.into()]).await?;
    } else {
        // An unaccepted attempt keeps its prepaid failure work. It is never
        // erased because a new process has no measurement of its last read.
        exec(&tx,"UPDATE task_budget SET confirmation_pending=NULL WHERE id=1 AND confirmation_pending=$1",vec![pending.into()]).await?;
    }
    tx.commit().await.map_err(|_| ManagementError::Database)
}

async fn commit(
    archive: &ProgressArchive<'_>,
    tx: ConfirmationTransaction<'_, '_>,
    before: Option<&progress::Position>,
    after: &progress::Position,
    payload: &[u8],
    effects: &mut ConfirmationEffects,
) -> Result<()> {
    let ProgressArchive { db, id, header } = *archive;
    match tx.commit().await {
        Ok(()) => {
            effects.database_commits += 1;
            Ok(())
        }
        Err(failure) => {
            let resolved = if let Some(before) = before {
                progress::committed(db, id, header, before, after, payload).await
            } else {
                progress::load(db, id, header, false)
                    .await
                    .map(|row| row.as_ref() == Some(after))
            };
            match resolved {
                Ok(true) => {
                    effects.database_commits += 1;
                    Ok(())
                }
                Ok(false) => {
                    effects.commit_unknown |= !failure.rollback_confirmed;
                    Err(failure.management_error())
                }
                Err(_) => {
                    effects.commit_unknown = true;
                    Err(ManagementError::Database)
                }
            }
        }
    }
}

fn staging_failure(
    error: TransactionFailure,
    effects: &mut ConfirmationEffects,
) -> ManagementError {
    effects.commit_unknown |= !error.rollback_confirmed;
    error.management_error()
}

fn binding_digest(value: &impl Serialize) -> Result<String> {
    use sha2::{Digest, Sha256};
    struct Hash(Sha256);
    impl std::io::Write for Hash {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut hash = Hash(Sha256::new());
    hash.0.update(b"S75-confirm-binding-1\0");
    serde_json::to_writer(&mut hash, value).map_err(|_| ManagementError::InvalidEvidence)?;
    Ok(hex::encode(hash.0.finalize()))
}

mod canonical;
use canonical::Canonical;
pub(super) async fn reconcile_retained(
    workspace: &mut Workspace,
    spools: &[InventorySpool],
) -> Result<()> {
    let Some(primary) = spools.first() else {
        return Ok(());
    };
    for entry in spools
        .iter()
        .map(|spool| {
            canonical::name(spool.reference()).map(|name| (name, Some(spool.reference()), false))
        })
        .chain(std::iter::once(
            canonical::union_name(primary.reference()).map(|name| (name, None, true)),
        ))
    {
        let (name, reference, union) = entry?;
        let path = workspace.named(&name)?;
        if !path.try_exists().map_err(|_| ManagementError::Database)? {
            continue;
        }
        drop(workspace.open_named(&name)?);
        let db = spool::connect_file(&path).await?;
        let result = async {
            if let Some(expected) = reference {
                let row = one(&db, "SELECT reference FROM state WHERE id=1", vec![])
                    .await?
                    .ok_or(ManagementError::InvalidEvidence)?;
                if decode::<RetirementInventoryRef>(&get::<String>(&row, "reference")?)?
                    != *expected
                {
                    return Err(ManagementError::InvalidEvidence);
                }
            }
            db.execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "ATTACH DATABASE $1 AS original",
                [primary
                    .path
                    .to_str()
                    .ok_or(ManagementError::InvalidEvidence)?
                    .into()],
            ))
            .await
            .map_err(|_| ManagementError::Database)?;
            task_budget::TaskBudget::forward_cache(&db, union).await
        }
        .await;
        db.close().await.map_err(|_| ManagementError::Database)?;
        result?;
    }
    Ok(())
}
#[cfg(test)]
mod tests;

async fn validate_local(
    engine: &mut ConfirmationEngine<'_>,
    config: &crate::pinning::management::local_config::ManagementConfig,
    request: &PlanRequest,
    snapshot: &Snapshot,
    origin: &SourceCheckpoint,
    task: &task_budget::TaskBudget,
    limits: &RunLimits,
) -> Result<()> {
    let (accounting, budget) = task.begin_output(limits).await?;
    let budget = tokio::sync::Mutex::new(budget);
    let work = records::SharedDecodeBudget::default();
    let result = async {
        let mut tx = engine
            .begin(Run::new(limits).remaining(), limits.cancellation.clone())
            .await
            .map_err(TransactionFailure::management_error)?;
        tx.run_with(
            (config, request, snapshot, origin, &budget, work),
            |tx, _, (config, request, snapshot, origin, budget, work)| {
                Box::pin(async move {
                    origin.check(tx).await?;
                    let mut budget = budget.lock().await;
                    let scope =
                        crate::pinning::management::db_snapshot::PlanningScope::capture_current(
                            tx,
                            &config.pinning,
                            request,
                            work,
                            &mut budget,
                        )
                        .await
                        .map_err(snapshot_error)?;
                    let now = snapshot
                        .database_now
                        .ok_or(ManagementError::InvalidEvidence)?;
                    let fresh = scope
                        .planning_snapshot(config, request, now, work)
                        .map_err(snapshot_error)?;
                    if encode(&fresh)? != encode(snapshot)? {
                        return Err(ManagementError::Changed);
                    }
                    origin.check(tx).await
                })
            },
        )
        .await
        .map_err(TransactionFailure::management_error)?;
        tx.rollback()
            .await
            .map_err(TransactionFailure::management_error)
    }
    .await;
    let metered = accounting.finish(budget.into_inner().work).await;
    match (result, metered) {
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}
fn snapshot_error(
    error: crate::pinning::management::db_snapshot::SnapshotError,
) -> ManagementError {
    if error == crate::pinning::management::db_snapshot::SnapshotError::LimitExceeded {
        ManagementError::Budget
    } else {
        ManagementError::Changed
    }
}
