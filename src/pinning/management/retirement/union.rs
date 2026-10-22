use super::{
    spool::{exec, get, one},
    *,
};
use sea_orm::{DatabaseConnection, TransactionTrait};
use std::path::Path;

#[cfg(test)]
mod recovery_tests;
mod work;

async fn guarded_exec(
    db: &DatabaseConnection,
    sql: &str,
    values: Vec<sea_orm::Value>,
) -> Result<()> {
    let tx = db.begin().await.map_err(|_| ManagementError::Database)?;
    super::cache_guard::check(&tx).await?;
    exec(&tx, sql, values).await?;
    super::cache_guard::accept(&tx).await?;
    tx.commit().await.map_err(|_| ManagementError::Database)
}

/// A separate disk workspace unions inventory and local quotes. No global
/// HashMap, per-provider subtotal addition, or duplicate epoch reservation.
pub struct PlanUnion {
    pub(super) db: DatabaseConnection,
}
impl PlanUnion {
    pub(crate) fn retain(&self) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            db: self.db.clone(),
        })
    }
    pub(super) fn budget_connection(&self) -> DatabaseConnection {
        self.db.clone()
    }
    pub(super) async fn start_task(&self, limits: &RunLimits) -> Result<()> {
        super::spool::task_run(&self.db, limits).await.map(|_| ())
    }
    pub(super) async fn task_work(&self) -> Result<TaskWork> {
        let work = work::Usage::read(&self.db).await?;
        let pages = one(
            &self.db,
            "SELECT pages_used FROM task_budget WHERE id=1",
            vec![],
        )
        .await?
        .map(|row| get::<i64>(&row, "pages_used"))
        .transpose()?
        .unwrap_or(0);
        Ok(TaskWork {
            rows: work.rows,
            bytes: work.bytes,
            pages: u64::try_from(pages).map_err(|_| ManagementError::InvalidEvidence)?,
        })
    }
    pub(super) async fn resume_task(path: &Path) -> Result<Self> {
        let db = super::spool::connect_file(path).await?;
        let result = async {
            db.execute_unprepared("PRAGMA cache_size=-2048; PRAGMA temp_store=FILE;")
                .await
                .map_err(|_| ManagementError::Database)?;
            work::Usage::read(&db).await?;
            Ok(())
        }
        .await;
        super::spool::close_failed_open(&db, result).await?;
        Ok(Self { db })
    }
    pub async fn close(&self) -> Result<()> {
        self.db
            .clone()
            .close()
            .await
            .map_err(|_| ManagementError::Database)
    }
    /// Uses the same protected-file and protected-parent contract as InventorySpool.
    pub async fn create(path: &Path, reservation: std::fs::File) -> Result<Self> {
        if reservation
            .metadata()
            .map_err(|_| ManagementError::Database)?
            .len()
            != 0
        {
            return Err(ManagementError::InvalidEvidence);
        }
        drop(reservation);
        let db = super::spool::connect_file(path).await?;
        let initialized = async {
        db.execute_unprepared("PRAGMA cache_size=-2048; PRAGMA temp_store=FILE;")
            .await
            .map_err(|_| ManagementError::Database)?;
        for sql in [
            super::spool::budget::SCHEMA,
            "CREATE TABLE work(id INTEGER PRIMARY KEY CHECK(id=1),rows BIGINT NOT NULL CHECK(rows>=0),bytes BIGINT NOT NULL CHECK(bytes>=0))",
            "INSERT INTO work VALUES(1,0,0)",
            "CREATE TABLE quotes(identity TEXT PRIMARY KEY,remote_key TEXT NOT NULL,cid TEXT NOT NULL,bytes TEXT NOT NULL,unknown INTEGER NOT NULL,inflight INTEGER NOT NULL,cleanup INTEGER NOT NULL,retained INTEGER NOT NULL)",
            "CREATE INDEX quote_remote ON quotes(remote_key)",
            "CREATE TABLE cids(cid TEXT PRIMARY KEY)",
            "CREATE TABLE transfers(identity TEXT PRIMARY KEY)",
            "CREATE TABLE unknowns(identity TEXT PRIMARY KEY)",
            "CREATE TABLE binding(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL)",
            "CREATE TABLE merged(inventory TEXT PRIMARY KEY,kind TEXT NOT NULL,after_key TEXT NOT NULL,complete INTEGER NOT NULL)",
            "CREATE TABLE output(id INTEGER PRIMARY KEY CHECK(id=1),base TEXT NOT NULL,summary TEXT NOT NULL,stage INTEGER NOT NULL,after_key TEXT NOT NULL)",
        ] {
            db.execute_unprepared(sql)
                .await
                .map_err(|_| ManagementError::Database)?;
        }
        Ok(())
        }.await;
        super::spool::close_failed_open(&db, initialized).await?;
        Ok(Self { db })
    }
    pub(crate) async fn bind(&self, entries: &[PlanEntry]) -> Result<()> {
        let payload = encode(&entries)?;
        if let Some(row) = one(&self.db, "SELECT payload FROM binding WHERE id=1", vec![]).await? {
            if get::<String>(&row, "payload")? != payload {
                return Err(ManagementError::Changed);
            }
        } else {
            guarded_exec(
                &self.db,
                "INSERT INTO binding VALUES(1,$1)",
                vec![payload.into()],
            )
            .await?;
        }
        Ok(())
    }
    pub async fn merge_inventory(
        &self,
        review: &RetirementReview<'_>,
        limits: &RunLimits,
    ) -> Result<Progress> {
        super::cache_guard::check(&self.db).await?;
        let id = encode(review.reference())?;
        let mut run = super::spool::task_run(&self.db, limits).await?;
        if one(
            &self.db,
            "SELECT inventory FROM merged WHERE inventory=$1",
            vec![id.clone().into()],
        )
        .await?
        .is_none()
        {
            guarded_exec(
                &self.db,
                "INSERT INTO merged VALUES($1,'current','',0)",
                vec![id.clone().into()],
            )
            .await?;
        }
        loop {
            let state = one(
                &self.db,
                "SELECT * FROM merged WHERE inventory=$1",
                vec![id.clone().into()],
            )
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
            if get::<i64>(&state, "complete")? != 0 {
                return Ok(Progress::Reduced);
            }
            let used = work::Usage::read(&self.db).await?;
            if !run.page(used.rows, used.bytes)? {
                return Ok(Progress::Pending);
            }
            let kind: String = get(&state, "kind")?;
            let after: String = get(&state, "after_key")?;
            let rows = work::facts(&self.db, &review.spool.db, &kind, &after, &run).await?;
            let last = rows
                .last()
                .map(|row| get::<String>(row, "identity"))
                .transpose()?;
            let tx = self
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            let outcome = async {
            for row in &rows {
                let payload: String = get(row, "payload")?;
                match kind.as_str() {
                    "current" | "history" => {
                        let remote: RemoteSnapshot = decode(&payload)?;
                        super::reducer::quote(&tx, &remote).await?;
                    }
                    "target" => {
                        let target: NativeRow = decode(&payload)?;
                        let cid = crate::pinning::provider::canonical_resource_cid(
                            crate::store::pinning::management::native::value(&target, "cid")
                                .ok_or(ManagementError::InvalidEvidence)?,
                        )
                        .map_err(|_| ManagementError::InvalidEvidence)?;
                        super::reducer::add_cid(&tx, &cid).await?;
                    }
                    "unknown" => {
                        let identity: String = get(row, "identity")?;
                        if one(
                            &tx,
                            "SELECT identity FROM unknowns WHERE identity=$1",
                            vec![identity.clone().into()],
                        )
                        .await?
                        .is_none()
                        {
                            exec(
                                &tx,
                                "INSERT INTO unknowns VALUES($1)",
                                vec![identity.into()],
                            )
                            .await?;
                        }
                    }
                    _ => return Err(ManagementError::InvalidEvidence),
                }
            }
            if let Some(last) = last {
                work::cas(
                    &tx,
                    "UPDATE merged SET after_key=$2 WHERE inventory=$1 AND kind=$3 AND after_key=$4 AND complete=0",
                    vec![id.clone().into(), last.into(), kind.clone().into(), after.clone().into()],
                )
                .await?;
            } else if let Some(next) = match kind.as_str() {
                "current" => Some("history"),
                "history" => Some("target"),
                "target" => Some("unknown"),
                _ => None,
            } {
                work::cas(
                    &tx,
                    "UPDATE merged SET kind=$2,after_key='' WHERE inventory=$1 AND kind=$3 AND after_key=$4 AND complete=0",
                    vec![id.clone().into(), next.into(), kind.clone().into(), after.clone().into()],
                )
                .await?;
            } else {
                work::cas(
                    &tx,
                    "UPDATE merged SET complete=1 WHERE inventory=$1 AND kind=$2 AND after_key=$3 AND complete=0",
                    vec![id.clone().into(), kind.into(), after.into()],
                )
                .await?;
            }
            run.check()?;
            Ok(())
            }.await;
            work::commit(tx, outcome).await?;
        }
    }
    pub(crate) async fn merge_local(
        &self,
        entries: &[PlanEntry],
        snapshot: &Snapshot,
    ) -> Result<()> {
        super::cache_guard::check(&self.db).await?;
        let tx = self
            .db
            .begin()
            .await
            .map_err(|_| ManagementError::Database)?;
        for entry in entries {
            let Some(Capture::Object(c)) = &entry.capture else {
                continue;
            };
            super::reducer::add_cid(&tx, &c.object.identity.cid).await?;
            for remote in c
                .remote
                .iter()
                .chain(c.destination_remote.iter())
                .chain(&c.historical_remotes)
            {
                super::reducer::quote(&tx, remote).await?;
            }
            let cid = c
                .target
                .as_ref()
                .map_or(c.object.identity.cid.as_str(), |t| &t.cid);
            let routes = c
                .requirement_routes
                .iter()
                .chain(std::iter::once(&c.route))
                .chain(c.destination_route.iter());
            for route in routes {
                let key = route.identity.remote_key(cid);
                for remote in snapshot
                    .remotes
                    .iter()
                    .chain(&snapshot.historical_remotes)
                    .filter(|r| r.key == key)
                {
                    super::reducer::quote(&tx, remote).await?;
                }
            }
            if snapshot.applied.iter().any(|r| r.binding == *entry) {
                continue;
            }
            if let Some(PlannedStep::Object(step)) = &entry.step {
                if step.source == RemoteDirective::SubmitWithFence {
                    transfer(&tx, &c.route.identity.remote_key(cid)).await?;
                }
                if step.destination == Some(RemoteDirective::SubmitWithFence) {
                    transfer(
                        &tx,
                        &c.destination_route
                            .as_ref()
                            .ok_or(ManagementError::InvalidEvidence)?
                            .identity
                            .remote_key(cid),
                    )
                    .await?;
                }
                if let Action::Create(intent) | Action::Reapply(intent) | Action::Backfill(intent) =
                    &entry.request.action
                {
                    for route in &c.requirement_routes {
                        if !route.available
                            || !route.allocation_allowed
                            || !c.object.allowed_routes.contains(&route.identity)
                        {
                            continue;
                        }
                        let key = route.identity.remote_key(cid);
                        let remote = snapshot.remotes.iter().find(|r| r.key == key);
                        let historical = remote
                            .and_then(|r| snapshot.routes.iter().find(|s| s.identity == r.route));
                        let now = snapshot
                            .database_now
                            .or_else(|| chrono::DateTime::from_timestamp(snapshot.observed_at, 0))
                            .ok_or(ManagementError::InvalidEvidence)?;
                        if super::super::semantics::directive(
                            c,
                            route,
                            remote,
                            historical,
                            &intent.requirements,
                            now,
                        ) == Ok(RemoteDirective::SubmitWithFence)
                        {
                            transfer(&tx, &key).await?;
                        }
                    }
                }
            }
        }
        super::cache_guard::accept(&tx).await?;
        tx.commit().await.map_err(|_| ManagementError::Database)
    }
    /// Resume the output reducer without resetting its cursor or subtotal.
    pub async fn finish(&self, base: Summary, limits: &RunLimits) -> Result<Option<Summary>> {
        super::cache_guard::check(&self.db).await?;
        let base_json = encode(&base)?;
        if one(&self.db, "SELECT id FROM output WHERE id=1", vec![])
            .await?
            .is_none()
        {
            guarded_exec(
                &self.db,
                "INSERT INTO output VALUES(1,$1,$1,0,'')",
                vec![base_json.clone().into()],
            )
            .await?;
        }
        let mut run = super::spool::task_run(&self.db, limits).await?;
        loop {
            let state = one(&self.db, "SELECT * FROM output WHERE id=1", vec![])
                .await?
                .ok_or(ManagementError::InvalidEvidence)?;
            if get::<String>(&state, "base")? != base_json {
                return Err(ManagementError::Changed);
            }
            let mut summary: Summary = decode(&get::<String>(&state, "summary")?)?;
            let stage: i64 = get(&state, "stage")?;
            let after: String = get(&state, "after_key")?;
            if stage == 4 {
                summary.physical_release_bytes = 0;
                return Ok(Some(summary));
            }
            let used = work::Usage::read(&self.db).await?;
            if !run.page(used.rows, used.bytes)? {
                return Ok(None);
            }
            let row = work::output_row(&self.db, stage, &after, &run).await?;
            let tx = self
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            let outcome = async {
            if let Some(row) = row {
                let id: String = get(&row, "identity")?;
                match stage {
                    0 => {
                        summary.unique_resources += 1;
                        summary.accounted_remote_bytes = summary
                            .accounted_remote_bytes
                            .checked_add(
                                get::<String>(&row, "bytes")?
                                    .parse()
                                    .map_err(|_| ManagementError::InvalidEvidence)?,
                            )
                            .ok_or(ManagementError::Budget)?;
                        summary.unknown_resources += usize::from(get::<i64>(&row, "unknown")? != 0);
                        summary.inflight_resources +=
                            usize::from(get::<i64>(&row, "inflight")? != 0);
                        summary.cleanup_pending_resources +=
                            usize::from(get::<i64>(&row, "cleanup")? != 0);
                    }
                    1 => summary.unique_cids += 1,
                    2 => {
                        summary.possible_new_transfers += 1;
                        if one(
                            &tx,
                            "SELECT identity FROM quotes WHERE remote_key=$1 LIMIT 1",
                            vec![id.clone().into()],
                        )
                        .await?
                        .is_none()
                        {
                            summary.unique_resources += 1;
                        }
                    }
                    _ => summary.unknown_resources += 1,
                }
                work::cas(
                    &tx,
                    "UPDATE output SET after_key=$1,summary=$2 WHERE id=1 AND stage=$3 AND after_key=$4",
                    vec![id.into(), encode(&summary)?.into(), stage.into(), after.clone().into()],
                )
                .await?;
            } else {
                work::cas(
                    &tx,
                    "UPDATE output SET stage=stage+1,after_key='' WHERE id=1 AND stage=$1 AND after_key=$2",
                    vec![stage.into(), after.into()],
                )
                .await?;
            }
            run.check()?;
            Ok(())
            }.await;
            work::commit(tx, outcome).await?;
        }
    }
}
use sea_orm::ConnectionTrait;
async fn transfer(tx: &impl ConnectionTrait, key: &RemoteKey) -> Result<()> {
    let id = encode(key)?;
    if one(
        tx,
        "SELECT identity FROM transfers WHERE identity=$1",
        vec![id.clone().into()],
    )
    .await?
    .is_none()
    {
        exec(tx, "INSERT INTO transfers VALUES($1)", vec![id.into()]).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn union_output_cannot_ignore_the_task_row_quota() {
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let union = PlanUnion::create(&path, file).await.unwrap();
        let cid = cid::Cid::new_v1(
            0x55,
            cid::multihash::Multihash::<64>::wrap(0x12, &[0; 32]).unwrap(),
        )
        .to_string();
        exec(&union.db, "INSERT INTO cids VALUES($1)", vec![cid.into()])
            .await
            .unwrap();
        let mut limits = RunLimits::default();
        limits.total_rows = 0;
        let result = union.finish(Summary::default(), &limits).await;
        union.close().await.unwrap();
        workspace.cleanup().unwrap();
        assert!(matches!(result, Err(ManagementError::Budget)));
    }
}
