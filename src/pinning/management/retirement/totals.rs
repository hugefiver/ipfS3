use super::{
    spool::{Run, get, one, page_keys},
    *,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement, TransactionTrait};

const STAGES: &[&str] = &[
    "lease_all",
    "lease",
    "target_edges",
    "current",
    "history",
    "root",
    "unknown",
    "route_revision",
    "quotes",
    "cids",
];

impl InventorySpool {
    pub(super) async fn compute_totals(
        &self,
        run: &mut Run<'_>,
        mut committed: bool,
    ) -> Result<Option<InventoryTotals>> {
        loop {
            let state = one(&self.db, "SELECT * FROM totals_progress WHERE id=1", vec![])
                .await?
                .ok_or(ManagementError::InvalidEvidence)?;
            let stage = get::<i64>(&state, "stage")? as usize;
            let after: String = get(&state, "after_key")?;
            let mut totals: InventoryTotals = decode(&get::<String>(&state, "payload")?)?;
            let Some(kind) = STAGES.get(stage) else {
                return Ok(Some(totals));
            };
            if !run.page(0, 0)? {
                return Ok(None);
            }
            let (query, values) = match *kind {
                "target_edges" => (
                    "SELECT target_id AS identity FROM target_edges WHERE target_id>$1 ORDER BY target_id LIMIT 64",
                    vec![after.clone().into()],
                ),
                "quotes" => (
                    "SELECT identity FROM quotes WHERE identity>$1 ORDER BY identity LIMIT 64",
                    vec![after.clone().into()],
                ),
                "cids" => (
                    "SELECT cid AS identity FROM cids WHERE cid>$1 ORDER BY cid LIMIT 64",
                    vec![after.clone().into()],
                ),
                "lease_all" => (
                    "SELECT identity FROM facts f WHERE kind='lease_all' AND identity>$1 AND NOT EXISTS (SELECT 1 FROM facts t WHERE t.kind='target_lease' AND t.identity=f.identity) ORDER BY identity LIMIT 64",
                    vec![after.clone().into()],
                ),
                _ => (
                    "SELECT identity FROM facts WHERE kind=$1 AND identity>$2 ORDER BY identity LIMIT 64",
                    vec![(*kind).into(), after.clone().into()],
                ),
            };
            let page_started = std::time::Instant::now();
            let keys = page_keys(&self.db, query, values).await?;
            super::spool::budget::charge(
                &self.db,
                run,
                keys.len() as u64,
                keys.iter().map(|key| key.len() as u64).sum(),
            )
            .await?;
            let tx = self
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            let result = async {
                if *kind == "target_edges" && !keys.is_empty() {
                    let slots=(1..=keys.len()).map(|i|format!("${i}")).collect::<Vec<_>>().join(",");
                    let rows=tx.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                        format!("SELECT CASE WHEN length(logical_bytes)<=32 THEN logical_bytes END AS logical_bytes,active FROM target_edges WHERE target_id IN ({slots})"),keys.iter().cloned().map(sea_orm::Value::from)
                    )).await.map_err(|_|ManagementError::Database)?;
                    if rows.len()!=keys.len() {return Err(ManagementError::InvalidEvidence);}
                    for row in rows {
                        totals.targets+=1;
                        totals.active_targets+=usize::from(get::<i64>(&row,"active")?!=0);
                        totals.logical_bytes=totals.logical_bytes.checked_add(get::<String>(&row,"logical_bytes")?.parse().map_err(|_|ManagementError::InvalidEvidence)?).ok_or(ManagementError::Budget)?;
                    }
                }
                for id in &keys {
                    run.check()?;
                    if *kind!="target_edges" {self.total_row(&tx, kind, id, &mut totals).await?;}
                }
                let (next_stage, next_after) = if let Some(last) = keys.last() {
                    (stage, last.as_str())
                } else { (stage + 1, "") };
                let updated = tx.execute(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "UPDATE totals_progress SET stage=$1,after_key=$2,payload=$3 WHERE id=1 AND stage=$4 AND after_key=$5",
                    vec![(next_stage as i64).into(), next_after.into(), encode(&totals)?.into(), (stage as i64).into(), after.into()],
                )).await.map_err(|_| ManagementError::Database)?;
                if updated.rows_affected() != 1 { return Err(ManagementError::Changed); }
                run.check()
            }.await;
            if let Err(error) = result {
                tx.rollback().await.map_err(|_| ManagementError::Database)?;
                // page() distinguishes cancellation/task exhaustion from an
                // elapsed slice without consuming another page after expiry.
                if error == ManagementError::Budget
                    && committed
                    && run.remaining().is_zero()
                    && !run.page(0, 0)?
                {
                    return Ok(None);
                }
                return Err(error);
            }
            super::cache_guard::accept(&tx).await?;
            tx.commit().await.map_err(|_| ManagementError::Database)?;
            super::spool::budget::timed(&self.db, "totals", page_started.elapsed()).await?;
            committed = true;
        }
    }

    async fn total_row(
        &self,
        tx: &DatabaseTransaction,
        kind: &str,
        id: &str,
        totals: &mut InventoryTotals,
    ) -> Result<()> {
        match kind {
            "lease_all" => {
                if self
                    .lookup_in::<bool>(tx, "target_lease", id)
                    .await?
                    .is_none()
                {
                    let lease = self
                        .lookup_in::<NativeRow>(tx, "lease_all", id)
                        .await?
                        .ok_or(ManagementError::InvalidEvidence)?;
                    super::spool::fact(tx, "lease", id, &lease).await?;
                    totals.attribution_complete = false;
                }
            }
            "lease" => totals.leases += 1,
            "target_edges" => {
                let row = one(
                    tx,
                    "SELECT logical_bytes,active FROM target_edges WHERE target_id=$1",
                    vec![id.into()],
                )
                .await?
                .ok_or(ManagementError::InvalidEvidence)?;
                totals.targets += 1;
                totals.active_targets += usize::from(get::<i64>(&row, "active")? != 0);
                totals.logical_bytes = totals
                    .logical_bytes
                    .checked_add(
                        get::<String>(&row, "logical_bytes")?
                            .parse()
                            .map_err(|_| ManagementError::InvalidEvidence)?,
                    )
                    .ok_or(ManagementError::Budget)?;
            }
            "current" => totals.current_resources += 1,
            "history" => totals.historical_resources += 1,
            "root" => {
                totals.roots += 1;
                totals.attribution_complete = false;
            }
            "unknown" => {
                totals.unknown += 1;
                totals.attribution_complete = false;
            }
            "route_revision" => {
                let route = self
                    .lookup_in::<RouteSnapshot>(tx, kind, id)
                    .await?
                    .ok_or(ManagementError::InvalidEvidence)?;
                totals.routes_available &= route.available;
            }
            "quotes" => {
                let row = one(
                    tx,
                    "SELECT bytes,unknown,inflight,cleanup,retained FROM quotes WHERE identity=$1",
                    vec![id.into()],
                )
                .await?
                .ok_or(ManagementError::InvalidEvidence)?;
                totals.unique_resources += 1;
                totals.accounted_bytes = totals
                    .accounted_bytes
                    .checked_add(
                        get::<String>(&row, "bytes")?
                            .parse()
                            .map_err(|_| ManagementError::InvalidEvidence)?,
                    )
                    .ok_or(ManagementError::Budget)?;
                totals.unknown += usize::from(get::<i64>(&row, "unknown")? != 0);
                totals.inflight += usize::from(get::<i64>(&row, "inflight")? != 0);
                totals.cleanup_pending += usize::from(get::<i64>(&row, "cleanup")? != 0);
                totals.retained += usize::from(get::<i64>(&row, "retained")? != 0);
            }
            "cids" => totals.unique_cids += 1,
            _ => return Err(ManagementError::InvalidEvidence),
        }
        Ok(())
    }
}
