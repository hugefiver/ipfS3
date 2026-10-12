use super::{invalid, ledger};
use crate::store::pinning::publication::{self as publication, PinTargetSpec};
use crate::{
    error::AppResult,
    pinning::{
        config::ProviderLimitMap,
        identity::ProviderRouteSnapshot,
        zip_policy::{CapturedZipIntent, ZipIntentSelection},
    },
};
use chrono::{DateTime, Utc};
use sea_orm::ConnectionTrait;
use std::collections::{BTreeMap, BTreeSet};

type FrozenAllocation = (
    ProviderLimitMap,
    BTreeMap<String, ProviderRouteSnapshot>,
    BTreeSet<String>,
);

pub(super) fn frozen_allocation(
    capture: &CapturedZipIntent,
    selected: &ZipIntentSelection,
) -> AppResult<FrozenAllocation> {
    let mut limits = ProviderLimitMap::new();
    let mut routes = BTreeMap::new();
    let mut enabled = BTreeSet::new();
    for lease in &selected.leases {
        for provider in &lease.providers {
            let limit = capture
                .provider_limits(&provider.route.provider_id)
                .ok_or_else(invalid)?
                .clone();
            if limit.enabled != provider.enabled
                || !lease.intent.providers.contains(&provider.config_name)
                || limits
                    .insert(provider.config_name.clone(), limit.clone())
                    .is_some_and(|old| old != limit)
                || routes
                    .insert(provider.config_name.clone(), provider.route.clone())
                    .is_some_and(|old| old != provider.route)
            {
                return Err(invalid());
            }
            if provider.enabled {
                enabled.insert(provider.config_name.clone());
            }
        }
        if lease
            .intent
            .providers
            .iter()
            .any(|p| !limits.contains_key(p))
        {
            return Err(invalid());
        }
    }
    Ok((limits, routes, enabled))
}

pub(super) async fn attachment_pairs<C: ConnectionTrait>(
    db: &C,
    selected: &ZipIntentSelection,
    limits: &ProviderLimitMap,
) -> AppResult<Vec<(String, String)>> {
    let mut pairs = BTreeSet::new();
    for lease in &selected.leases {
        for provider in publication::ordered_enabled_providers(&lease.intent, limits)? {
            for target in &lease.targets {
                pairs.insert((
                    provider.clone(),
                    ledger::allocation_cid(db, &provider, &target.cid).await?,
                ));
            }
        }
    }
    Ok(pairs.into_iter().collect())
}

pub(super) fn verify_candidate(
    candidate: &ZipIntentSelection,
    exact: &ZipIntentSelection,
) -> AppResult<()> {
    let shape = |s: &ZipIntentSelection| {
        s.leases
            .iter()
            .map(|l| {
                (
                    l.owner.object_id.clone(),
                    l.intent.clone(),
                    l.providers.clone(),
                    l.targets
                        .iter()
                        .map(|t| (t.object_id.clone(), t.cid.clone(), t.logical_size))
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    };
    if shape(candidate) != shape(exact) {
        return Err(invalid());
    }
    Ok(())
}

pub(super) async fn create_leases<C: ConnectionTrait>(
    db: &C,
    selected: &ZipIntentSelection,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut affected = BTreeSet::new();
    let mut failed = BTreeSet::new();
    for lease in &selected.leases {
        let targets = lease
            .targets
            .iter()
            .map(|t| (t.cid.clone(), t.logical_size))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .map(|(cid, logical_size)| PinTargetSpec { cid, logical_size })
            .collect::<Vec<_>>();
        if targets.is_empty() {
            continue;
        }
        let id = publication::lease_id(&lease.owner.object_id, lease.intent.source);
        publication::insert_lease(
            db,
            &id,
            &lease.owner.object_id,
            &lease.intent,
            &targets,
            now,
        )
        .await?;
        let mut assigned = BTreeMap::new();
        for target in targets {
            publication::assign_target_providers(
                db,
                &id,
                &lease.intent,
                &target,
                limits,
                now,
                &mut assigned,
                &mut affected,
                &mut failed,
            )
            .await?;
        }
    }
    for (provider, cid) in failed {
        publication::leases::reset_failed_remote_retry_on_user_touch(db, &provider, &cid, now)
            .await?;
        publication::ensure_failed_attachment_reconcile(db, &provider, &cid, now).await?;
    }
    for (provider, cid) in affected {
        publication::project_desired_remote_targets(db, &provider, &cid, now).await?;
    }
    Ok(())
}
