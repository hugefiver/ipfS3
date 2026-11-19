use super::super::legacy_migration::ReceiptCatalog;
use super::*;
use std::collections::BTreeMap;

pub(crate) struct EvictionSnapshot {
    demand: QuotaWaiterCandidate,
    demand_remote: Option<remote_pin::Model>,
    #[cfg(test)]
    selected: Vec<(String, String)>,
    victims: BTreeMap<String, (remote_pin::Model, DateTimeUtc)>,
    frontier: leases::EvictionFrontier,
    usage: ProviderUsage,
    pending: ProviderUsage,
}

#[derive(Clone, Copy)]
pub(crate) struct ProviderQuotaWork<'a> {
    pub cid: &'a str,
    pub lease_id: &'a str,
    pub target_id: &'a str,
    pub expected_generation: i64,
}

pub(crate) async fn discover_waiter_eviction<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<Option<EvictionSnapshot>> {
    let session = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&session);
    let Some(demand) =
        leases::find_quota_waiter_for_eviction(provider, now, ReceiptCatalog::full(db), work)
            .await?
    else {
        return Ok(None);
    };
    if demand.target.last_touched_at > demand.target.created_at {
        // A cooled-down all-mode retry may reuse space but must not displace its replacement.
        return Ok(None);
    }
    let remote = remote_pin::Entity::find_by_id((provider.to_owned(), demand.target.cid.clone()))
        .one(db)
        .await?;
    if remote
        .as_ref()
        .is_some_and(|r| is_capacity_holding_status(&r.status))
    {
        return Ok(None);
    }
    let size = demand.target.logical_size;
    discover(db, demand, remote, size, limits).await
}

pub(crate) async fn discover_provider_quota_eviction<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    work: ProviderQuotaWork<'_>,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<Option<EvictionSnapshot>> {
    let Some(target) = pin_lease_target::Entity::find_by_id(work.target_id)
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if target.lease_id != work.lease_id
        || target.provider != provider
        || target.cid != work.cid
        || !ACTIVE_TARGET_STATES.contains(&target.state.as_str())
    {
        return Ok(None);
    }
    let Some(lease) = pin_lease::Entity::find_by_id(work.lease_id).one(db).await? else {
        return Ok(None);
    };
    if lease.generation != work.expected_generation
        || lease.state != ACTIVE_LEASE_STATE
        || lease.expires_at <= now
    {
        return Ok(None);
    }
    let Some(remote) = remote_pin::Entity::find_by_id((provider.to_owned(), work.cid.to_owned()))
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if remote.status != REMOTE_STATUS_RESERVED || remote.request_id.is_some() {
        return Ok(None);
    }
    let size = remote.cid_size;
    discover(
        db,
        QuotaWaiterCandidate { lease, target },
        Some(remote),
        size,
        limits,
    )
    .await
}

async fn discover<C: ConnectionTrait>(
    db: &C,
    demand: QuotaWaiterCandidate,
    demand_remote: Option<remote_pin::Model>,
    size: i64,
    limits: &ProviderLimits,
) -> AppResult<Option<EvictionSnapshot>> {
    let provider = &demand.target.provider;
    let Some(usage) = pin_provider_usage::Entity::find_by_id(provider)
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let usage = ProviderUsage {
        reserved_bytes: usage.reserved_bytes,
        reserved_pins: usage.reserved_pins,
    };
    if reservation_decision(usage, size, limits)? != CapacityDecision::Wait {
        return Ok(None);
    }
    let barriers = super::super::ledger::submission::admission_barriers_with_catalog(
        provider,
        ReceiptCatalog::full(db),
        &(),
        true,
    )
    .await?;
    let remotes = remote_pin::Entity::find()
        .filter(remote_pin::Column::Provider.eq(provider))
        .filter(remote_pin::Column::Status.is_in(capacity_holding_statuses()))
        .all(db)
        .await?;
    let touches: BTreeMap<String, DateTimeUtc> = pin_lease_target::Entity::find()
        .inner_join(pin_lease::Entity)
        .select_only()
        .column(pin_lease_target::Column::Cid)
        .column_as(pin_lease_target::Column::LastTouchedAt.max(), "newest")
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .filter(pin_lease_target::Column::State.is_in(ACTIVE_TARGET_STATES))
        .filter(pin_lease::Column::State.eq(ACTIVE_LEASE_STATE))
        .group_by(pin_lease_target::Column::Cid)
        .into_tuple()
        .all(db)
        .await?
        .into_iter()
        .collect();
    let mut victims = BTreeMap::new();
    let mut candidates = Vec::new();
    let mut pending = ProviderUsage {
        reserved_bytes: 0,
        reserved_pins: 0,
    };
    let mut reclaimable = pending;
    for remote in remotes {
        let has_debt = barriers.contains_key(&remote.cid);
        match touches.get(&remote.cid).copied() {
            Some(touch) if !has_debt && remote.cid != demand.target.cid => {
                candidates.push(EvictionCandidate {
                    provider: provider.clone(),
                    cid: remote.cid.clone(),
                    cid_size: remote.cid_size,
                    last_active_touch: touch,
                });
                victims.insert(remote.cid.clone(), (remote, touch));
            }
            Some(_) => (),
            None => {
                add_pending(&mut pending, remote.cid_size)?;
                if !has_debt {
                    add_pending(&mut reclaimable, remote.cid_size)?;
                }
            }
        }
    }
    let projected = ProviderUsage {
        reserved_bytes: usage
            .reserved_bytes
            .checked_sub(reclaimable.reserved_bytes)
            .ok_or_else(|| invalid_quota("projected byte accounting underflow"))?,
        reserved_pins: usage.reserved_pins - reclaimable.reserved_pins,
    };
    let selected = select_eviction_candidates(projected, size, limits, candidates)?;
    if selected.is_empty() {
        return Ok(None);
    }
    let victims = selected
        .iter()
        .map(|(_, cid)| {
            (
                cid.clone(),
                victims.remove(cid).expect("selected from candidates"),
            )
        })
        .collect();
    let frontier = leases::eviction_frontier(db, &selected).await?;
    Ok(Some(EvictionSnapshot {
        demand,
        demand_remote,
        #[cfg(test)]
        selected,
        victims,
        frontier,
        usage,
        pending,
    }))
}

/// A capacity-holding CID without a desired reference is already on its way out.
/// Compare this headroom after locking usage: another eviction need not change usage yet.
async fn pending_headroom<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<ProviderUsage> {
    let totals = db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT COUNT(*) AS pins, CAST(COALESCE(SUM(r.cid_size),0) AS BIGINT) AS bytes \
         FROM remote_pins r WHERE r.provider=$1 AND r.status IN ('reserved','queued','pinning','pinned','failed') \
         AND NOT EXISTS (SELECT 1 FROM pin_lease_targets t JOIN pin_leases l ON l.id=t.lease_id \
         WHERE t.provider=r.provider AND t.cid=r.cid AND l.state='active' \
         AND t.state IN ('waiting','submitted','pinned','degraded'))", [provider.into()])).await?.expect("ungrouped aggregate returns one row");
    Ok(ProviderUsage {
        reserved_bytes: totals.try_get("", "bytes")?,
        reserved_pins: totals.try_get("", "pins")?,
    })
}

pub(crate) async fn apply_eviction(
    db: &DatabaseTransaction,
    snapshot: &EvictionSnapshot,
    now: DateTimeUtc,
    runtime: Option<&crate::store::RuntimeTransaction>,
) -> AppResult<Vec<leases::QuotaEvictedTarget>> {
    #[cfg(test)]
    stage_eviction_drift(db, &snapshot.selected).await?;
    if snapshot.demand.lease.expires_at <= now
        || !snapshot.frontier.lock(db, &snapshot.demand).await?
    {
        return Ok(Vec::new());
    }
    let demand = &snapshot.demand.target;
    let provider = &demand.provider;
    let remote = remote_pin::Entity::find_by_id((provider.clone(), demand.cid.clone()))
        .one(db)
        .await?;
    if remote != snapshot.demand_remote {
        return Ok(Vec::new());
    }
    let mut usage = pin_provider_usage::Entity::find_by_id(provider);
    if db.get_database_backend() == DatabaseBackend::Postgres {
        usage = usage.lock_exclusive();
    }
    if !usage.one(db).await?.is_some_and(|usage| {
        usage.reserved_pins == snapshot.usage.reserved_pins
            && usage.reserved_bytes == snapshot.usage.reserved_bytes
    }) {
        return Ok(Vec::new());
    }
    let session = SharedDecodeBudget::default();
    let work = DecodeWork::shared(&session);
    let catalog = runtime.map_or_else(|| ReceiptCatalog::full(db), |tx| tx.catalog(work));
    let mut cids = snapshot
        .victims
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    if demand.state == TARGET_QUOTA_WAITING {
        cids.push(&demand.cid);
    }
    for page in cids.chunks(64) {
        if !super::super::ledger::submission::resource_admission_barriers_with_catalog(
            provider, page, catalog, &work, true,
        )
        .await?
        .is_empty()
        {
            return Ok(Vec::new());
        }
    }
    for (cid, (remote, touched)) in &snapshot.victims {
        if remote_pin::Entity::find_by_id((provider.clone(), cid.clone()))
            .one(db)
            .await?
            .as_ref()
            != Some(remote)
            || newest_active_target_touch(db, provider, cid).await? != Some(*touched)
        {
            return Ok(Vec::new());
        }
    }
    if pending_headroom(db, provider).await? != snapshot.pending {
        return Ok(Vec::new());
    }
    snapshot.frontier.apply(db, now, runtime).await
}

fn add_pending(pending: &mut ProviderUsage, size: i64) -> AppResult<()> {
    pending.reserved_bytes = pending
        .reserved_bytes
        .checked_add(size)
        .ok_or_else(|| invalid_quota("pending byte accounting overflow"))?;
    pending.reserved_pins += 1;
    Ok(())
}
