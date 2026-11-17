use super::invalid;
use crate::{
    error::AppResult,
    pinning::{
        config::ProviderLimitMap,
        identity::ProviderRouteSnapshot,
        zip_policy::{CapturedZipIntent, ZipIntentSelection},
    },
};
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

pub(super) type SelectionShape = Vec<(
    String,
    crate::pinning::policy::LeaseIntent,
    Vec<(String, bool)>,
    Vec<(String, String, i64)>,
)>;

pub(super) fn selection_shape(selected: &ZipIntentSelection) -> SelectionShape {
    selected
        .leases
        .iter()
        .map(|lease| {
            (
                lease.owner.object_id.clone(),
                lease.intent.clone(),
                lease
                    .providers
                    .iter()
                    .map(|provider| (provider.config_name.clone(), provider.enabled))
                    .collect(),
                lease
                    .targets
                    .iter()
                    .map(|target| {
                        (
                            target.object_id.clone(),
                            target.cid.clone(),
                            target.logical_size,
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

pub(super) fn verify_candidate(
    candidate: &SelectionShape,
    exact: &ZipIntentSelection,
) -> AppResult<()> {
    if candidate.len() != exact.leases.len()
        || !candidate.iter().zip(&exact.leases).all(
            |((owner, intent, providers, targets), lease)| {
                owner == &lease.owner.object_id
                    && intent == &lease.intent
                    && providers.len() == lease.providers.len()
                    && providers
                        .iter()
                        .zip(&lease.providers)
                        .all(|((name, enabled), provider)| {
                            name == &provider.config_name && *enabled == provider.enabled
                        })
                    && targets.len() == lease.targets.len()
                    && targets
                        .iter()
                        .zip(&lease.targets)
                        .all(|((owner, cid, size), target)| {
                            owner == &target.object_id
                                && cid == &target.cid
                                && *size == target.logical_size
                        })
            },
        )
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
pub(in crate::store::pinning::publication) async fn attachment_pairs<
    R: super::super::reservations::Reservations,
>(
    reservations: &R,
    selected: &ZipIntentSelection,
    limits: &ProviderLimitMap,
    maintenance: &mut crate::store::zip::publication_lease::PublicationLease<'_>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<Vec<(String, String)>> {
    let mut pairs = BTreeSet::new();
    for lease in &selected.leases {
        for provider in super::super::ordered_enabled_providers(&lease.intent, limits)? {
            for target in &lease.targets {
                maintenance.checkpoint(reservations.db()).await?;
                let cid = match reservations.runtime() {
                    Some(_) => {
                        super::ledger::allocation_cid_with_work(
                            &reservations.catalog(work),
                            &provider,
                            &target.cid,
                            work,
                        )
                        .await?
                    }
                    None => {
                        super::ledger::allocation_cid(reservations.db(), &provider, &target.cid)
                            .await?
                    }
                };
                pairs.insert((provider.clone(), cid));
            }
        }
    }
    Ok(pairs.into_iter().collect())
}
