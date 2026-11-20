use super::{
    Action, AppResult, Candidate, Claim, OCCUPIED,
    candidates::{DELETE, STALE, SUBMIT},
    stmt,
};
use crate::pinning::config::ProviderLimitMap;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, QueryResult, Value};
use std::collections::{BTreeMap, BTreeSet};

fn bind_row<const N: usize>(values: &mut Vec<Value>, row: [Value; N]) -> String {
    let start = values.len() + 1;
    values.extend(row);
    let binds = (start..=values.len())
        .map(|n| format!("${n}"))
        .collect::<Vec<_>>();
    format!("({})", binds.join(","))
}

fn row_lock(tx: &DatabaseTransaction) -> &'static str {
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        " FOR UPDATE OF p SKIP LOCKED"
    } else {
        ""
    }
}

const FIELDS: &str =
    "p.provider,p.cid,COALESCE(p.source_cid,p.cid) AS source_cid,p.handle,p.attempt";

pub(super) fn new_claim(row: &QueryResult, action: Action) -> AppResult<Claim> {
    Ok(Claim {
        provider: row.try_get("", "provider")?,
        cid: row.try_get("", "cid")?,
        source_cid: row.try_get("", "source_cid")?,
        action,
        handle: row.try_get("", "handle")?,
        eligible_at_micros: row.try_get("", "eligible_at")?,
        attempt: row.try_get::<i32>("", "attempt")? + 1,
        token: uuid::Uuid::new_v4().to_string(),
    })
}

pub(super) async fn expire_submits<'a>(
    tx: &DatabaseTransaction,
    stale: impl Iterator<Item = &'a Candidate>,
    now: i64,
) -> AppResult<()> {
    let mut values = vec![now.into()];
    let keys = stale
        .map(|c| {
            bind_row(
                &mut values,
                [c.provider.as_str().into(), c.cid.as_str().into()],
            )
        })
        .collect::<Vec<_>>();
    if !keys.is_empty() {
        let sql = format!("UPDATE pin_resources SET status='unknown',claim_until=NULL WHERE (provider,cid) IN
            (SELECT provider,cid FROM pin_resources p WHERE (provider,cid) IN (VALUES {}) AND {STALE}{})",keys.join(","),row_lock(tx));
        tx.execute(stmt(tx, &sql, values)).await?;
    }
    Ok(())
}

pub(super) async fn due_deletes(
    tx: &DatabaseTransaction,
    budgets: &BTreeMap<&str, usize>,
    now: i64,
) -> AppResult<Vec<QueryResult>> {
    let mut values = vec![now.into()];
    let mut scans = Vec::new();
    for (provider, budget) in budgets {
        values.push((*provider).into());
        let n = values.len();
        values.push((*budget as i64).into());
        let cap = values.len();
        scans.push(format!("SELECT * FROM (SELECT {FIELDS},next_at,CASE WHEN claim_token IS NOT NULL AND claim_until>next_at THEN claim_until ELSE next_at END AS eligible_at FROM pin_resources p WHERE provider=${n} AND {DELETE}
            ORDER BY next_at,cid LIMIT ${cap}{}) AS deletes_{n}", row_lock(tx)));
    }
    let sql = format!(
        "{} ORDER BY next_at,provider,cid",
        scans.join(" UNION ALL ")
    );
    Ok(tx.query_all(stmt(tx, &sql, values)).await?)
}

pub(super) async fn capacity<'a>(
    tx: &DatabaseTransaction,
    providers: BTreeSet<&'a str>,
    limits: &ProviderLimitMap,
    blocked: &mut BTreeSet<String>,
    now: i64,
) -> AppResult<BTreeMap<&'a str, (i64, i64)>> {
    let mut room = providers
        .into_iter()
        .map(|p| (p, (limits[p].max_pins, limits[p].max_bytes)))
        .collect::<BTreeMap<_, _>>();
    if tx.get_database_backend() == DatabaseBackend::Postgres && !room.is_empty() {
        let mut values = Vec::new();
        let rows = room
            .keys()
            .map(|&p| bind_row(&mut values, [p.into()]))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT provider,pg_try_advisory_xact_lock(1844053111,hashtext(provider)) AS locked FROM (VALUES {rows}) AS routes(provider)"
        );
        for row in tx.query_all(stmt(tx, &sql, values)).await? {
            if !row.try_get::<bool>("", "locked")? {
                let provider: String = row.try_get("", "provider")?;
                room.remove(provider.as_str());
                blocked.insert(provider);
            }
        }
    }
    // READ COMMITTED must take a fresh statement snapshot after acquiring the provider locks.
    if !room.is_empty() {
        let mut values = Vec::new();
        let routes = room
            .keys()
            .map(|&p| bind_row(&mut values, [p.into()]))
            .collect::<Vec<_>>()
            .join(",");
        let sql=format!("SELECT provider,COUNT(*) AS pins,CAST(SUM(logical_size) AS BIGINT) AS bytes FROM pin_resources
            WHERE provider IN (VALUES {routes}) AND status!='cleaned' AND ({OCCUPIED}) GROUP BY provider");
        for row in tx.query_all(stmt(tx, &sql, values)).await? {
            let provider: String = row.try_get("", "provider")?;
            let (pins, bytes) = room.get_mut(provider.as_str()).unwrap();
            *pins -= row.try_get::<i64>("", "pins")?;
            *bytes -= row.try_get::<i64>("", "bytes")?;
        }
    }
    // At the exact byte limit, only a due zero-byte object can make another page useful.
    let mut values = vec![now.into()];
    let full = room
        .iter()
        .filter(|(_, (pins, bytes))| *pins > 0 && *bytes == 0)
        .map(|(&p, _)| bind_row(&mut values, [p.into()]))
        .collect::<Vec<_>>();
    if !full.is_empty() {
        let sql = format!("WITH full_routes(provider) AS (VALUES {}) SELECT f.provider FROM full_routes f
            WHERE NOT EXISTS (SELECT 1 FROM pin_resources p WHERE p.provider=f.provider AND logical_size=0 AND {SUBMIT})", full.join(","));
        for row in tx.query_all(stmt(tx, &sql, values)).await? {
            let provider: String = row.try_get("", "provider")?;
            room.remove(provider.as_str());
            blocked.insert(provider);
        }
    }
    room.retain(|&p, (pins, bytes)| {
        let usable = *pins > 0 && *bytes >= 0;
        if !usable {
            blocked.insert(p.to_owned());
        }
        usable
    });
    Ok(room)
}

pub(super) async fn waiting<'a>(
    tx: &DatabaseTransaction,
    candidates: impl Iterator<Item = (usize, &'a Candidate)>,
    now: i64,
) -> AppResult<BTreeMap<usize, QueryResult>> {
    let mut values = vec![now.into()];
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    for (i, c) in candidates {
        if seen.insert((&c.provider, &c.cid)) {
            rows.push(bind_row(
                &mut values,
                [
                    c.provider.as_str().into(),
                    c.cid.as_str().into(),
                    (i as i32).into(),
                ],
            ));
        }
    }
    if rows.is_empty() {
        return Ok(BTreeMap::new());
    }
    let sql=format!("WITH wanted(provider,cid,ord) AS (VALUES {}) SELECT {FIELDS},p.next_at AS eligible_at,p.logical_size,CAST(w.ord AS INTEGER) AS ord
        FROM wanted w JOIN pin_resources p ON p.provider=w.provider AND p.cid=w.cid WHERE {SUBMIT}{}",rows.join(","),row_lock(tx));
    tx.query_all(stmt(tx, &sql, values))
        .await?
        .into_iter()
        .map(|row| Ok((row.try_get::<i32>("", "ord")? as usize, row)))
        .collect()
}

pub(super) async fn write_claims(
    tx: &DatabaseTransaction,
    claims: &[Claim],
    until: i64,
) -> AppResult<()> {
    if !claims.is_empty() {
        let mut values = vec![until.into()];
        let rows = claims
            .iter()
            .map(|c| {
                bind_row(
                    &mut values,
                    [
                        c.provider.as_str().into(),
                        c.cid.as_str().into(),
                        c.token.as_str().into(),
                    ],
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("WITH chosen(provider,cid,token) AS (VALUES {rows}) UPDATE pin_resources AS p
            SET status='pending',claim_token=chosen.token,claim_until=$1,attempt=attempt+1 FROM chosen
            WHERE p.provider=chosen.provider AND p.cid=chosen.cid");
        tx.execute(stmt(tx, &sql, values)).await?;
    }
    Ok(())
}
