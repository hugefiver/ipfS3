use super::{AppResult, stmt};
use crate::pinning::config::ProviderLimits;
use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, DatabaseConnection, Value};

// Three bind parameters per claim keep even a full page below SQLite's 999-bind limit.
pub(crate) const MAX_CANDIDATES: usize = 128;
pub(super) const STALE: &str = "action='submit' AND status='pending' AND claim_until<=$1";
pub(super) const DELETE: &str = "action='delete' AND status IN ('pending','failed') AND next_at<=$1 AND (claim_token IS NULL OR claim_until<=$1)";
pub(super) const SUBMIT: &str = "action='submit' AND status='waiting' AND claim_token IS NULL AND next_at<=$1
    AND EXISTS (SELECT 1 FROM pin_refs r WHERE r.provider=p.provider AND r.cid=p.cid AND r.expires_at>$1)";

pub(super) const STALE_SUBMIT: i32 = 0;
pub(super) const DUE_DELETE: i32 = 1;
pub(super) const WAITING_SUBMIT: i32 = 2;

#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub provider: String,
    pub cid: String,
    pub(super) kind: i32,
    due: i64,
}

pub(crate) async fn candidate_page(
    db: &DatabaseConnection,
    provider: &str,
    limits: &ProviderLimits,
    after: Option<&Candidate>,
    limit: usize,
    now: DateTime<Utc>,
) -> AppResult<Vec<Candidate>> {
    let mut values = vec![
        now.timestamp_micros().into(),
        provider.into(),
        (limit.min(MAX_CANDIDATES) as i64).into(),
    ];
    let mut scans = Vec::new();
    for (kind, predicate, due) in [
        (STALE_SUBMIT, STALE, "claim_until"),
        (DUE_DELETE, DELETE, "next_at"),
        (WAITING_SUBMIT, SUBMIT, "next_at"),
    ] {
        if after.is_some_and(|cursor| kind < cursor.kind)
            || (kind == WAITING_SUBMIT && !limits.enabled)
        {
            continue;
        }
        let mut filter = predicate.to_owned();
        if kind == WAITING_SUBMIT {
            values.push(limits.max_bytes.into());
            filter.push_str(&format!(" AND logical_size<=${}", values.len()));
        }
        if let Some(cursor) = after.filter(|cursor| cursor.kind == kind) {
            let n = values.len();
            values.extend([Value::from(cursor.due), cursor.cid.as_str().into()]);
            filter.push_str(&format!(" AND ({due},cid)>(${},${})", n + 1, n + 2));
        }
        scans.push(format!(
            "SELECT * FROM (SELECT cid,{kind} AS kind,{due} AS due FROM pin_resources p
            WHERE provider=$2 AND {filter} ORDER BY {due},cid LIMIT $3) AS page_{kind}"
        ));
    }
    if scans.is_empty() {
        return Ok(Vec::new());
    }
    let sql = format!(
        "{} ORDER BY kind,due,cid LIMIT $3",
        scans.join(" UNION ALL ")
    );
    db.query_all(stmt(db, &sql, values))
        .await?
        .into_iter()
        .map(|row| {
            Ok(Candidate {
                provider: provider.to_owned(),
                cid: row.try_get("", "cid")?,
                kind: row.try_get("", "kind")?,
                due: row.try_get("", "due")?,
            })
        })
        .collect()
}
