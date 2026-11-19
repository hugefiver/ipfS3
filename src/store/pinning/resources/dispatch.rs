use super::{
    Action, AppResult, Candidate, Claim, MAX_CANDIDATES, candidate_page,
    candidates::{STALE_SUBMIT, WAITING_SUBMIT},
    dispatch_sql::*,
    sqlite_writer,
};
use crate::pinning::config::{ProviderLimitMap, ProviderLimits};
use chrono::{DateTime, Utc};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, IsolationLevel, TransactionTrait,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(crate) struct BatchClaims {
    pub claims: Vec<Claim>,
    /// Indices to evict from the advisory cache, including locked or outdated hints.
    pub consumed: Vec<usize>,
    /// Submit admission cannot progress for these routes until capacity or contention changes.
    pub blocked: BTreeSet<String>,
}

pub(crate) async fn claim_batch(
    db: &DatabaseConnection,
    candidates: &[Candidate],
    limits: &ProviderLimitMap,
    budgets: &BTreeMap<String, usize>,
    free_slots: usize,
    now: DateTime<Utc>,
    claim_until: DateTime<Utc>,
) -> AppResult<BatchClaims> {
    let candidates = &candidates[..candidates.len().min(MAX_CANDIDATES)];
    let mut batch = BatchClaims::default();
    let mut available = BTreeMap::new();
    for c in candidates {
        let budget = budgets
            .get(&c.provider)
            .copied()
            .unwrap_or(0)
            .min(free_slots);
        if budget > 0 {
            let n = available.entry(c.provider.as_str()).or_insert(0);
            *n = budget.min(*n + 1);
        }
    }
    if available.is_empty() {
        return Ok(batch);
    }
    let now = now.timestamp_micros();
    let isolation = (db.get_database_backend() == DatabaseBackend::Postgres)
        .then_some(IsolationLevel::ReadCommitted);
    let tx = db.begin_with_config(isolation, None).await?;
    sqlite_writer(&tx).await?;
    let ready = |c: &Candidate| available.contains_key(c.provider.as_str());
    // A lost POST keeps its token so the sender's late ACK can still settle it.
    expire_submits(
        &tx,
        candidates
            .iter()
            .filter(|c| c.kind == STALE_SUBMIT && ready(c)),
        now,
    )
    .await?;
    let mut deletes = BTreeMap::<String, BTreeSet<String>>::new();
    // A removal may have become due since the submit page was read.
    for row in due_deletes(&tx, &available, now)
        .await?
        .into_iter()
        .take(free_slots)
    {
        let claim = new_claim(&row, Action::Delete)?;
        *available.get_mut(claim.provider.as_str()).unwrap() -= 1;
        deletes
            .entry(claim.provider.clone())
            .or_default()
            .insert(claim.cid.clone());
        batch.claims.push(claim);
    }
    let submit_routes = candidates
        .iter()
        .filter(|c| {
            c.kind == WAITING_SUBMIT
                && available.get(c.provider.as_str()).is_some_and(|n| *n > 0)
                && limits[&c.provider].enabled
                && batch.claims.len() < free_slots
        })
        .map(|c| c.provider.as_str())
        .collect();
    let mut room = capacity(&tx, submit_routes, limits, &mut batch.blocked, now).await?;
    let mut rows = waiting(
        &tx,
        candidates
            .iter()
            .enumerate()
            .filter(|(_, c)| c.kind == WAITING_SUBMIT && room.contains_key(c.provider.as_str())),
        now,
    )
    .await?;
    for (i, c) in candidates.iter().enumerate() {
        let Some(budget) = available.get_mut(c.provider.as_str()) else {
            continue;
        };
        if c.kind == STALE_SUBMIT
            || deletes
                .get(&c.provider)
                .is_some_and(|cids| cids.contains(&c.cid))
        {
            batch.consumed.push(i);
            continue;
        }
        if *budget == 0 || batch.claims.len() == free_slots {
            continue;
        }
        batch.consumed.push(i);
        let Some(row) = rows.remove(&i) else { continue };
        let (pins, bytes) = room.get_mut(c.provider.as_str()).unwrap();
        if *pins == 0 {
            batch.blocked.insert(c.provider.clone());
            continue;
        }
        let size = row.try_get::<i64>("", "logical_size")?;
        if size > *bytes {
            continue;
        }
        *bytes -= size;
        *pins -= 1;
        *budget -= 1;
        batch.claims.push(new_claim(&row, Action::Submit)?);
    }
    write_claims(&tx, &batch.claims, claim_until.timestamp_micros()).await?;
    tx.commit().await?;
    Ok(batch)
}

pub async fn claim(
    db: &DatabaseConnection,
    provider: &str,
    limits: &ProviderLimits,
    now: DateTime<Utc>,
    claim_until: DateTime<Utc>,
) -> AppResult<Option<Claim>> {
    let routes = BTreeMap::from([(provider.to_owned(), limits.clone())]);
    let budgets = BTreeMap::from([(provider.to_owned(), 1)]);
    let mut cursor = None;
    loop {
        let mut page =
            candidate_page(db, provider, limits, cursor.as_ref(), MAX_CANDIDATES, now).await?;
        let mut batch = claim_batch(db, &page, &routes, &budgets, 1, now, claim_until).await?;
        if !batch.claims.is_empty()
            || page.len() < MAX_CANDIDATES
            || batch.blocked.contains(provider)
        {
            return Ok(batch.claims.pop());
        }
        cursor = page.pop();
    }
}
