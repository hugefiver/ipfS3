use super::{allocation::frozen_allocation, invalid, sql};
use crate::{
    error::AppResult,
    pinning::{
        config::ProviderLimitMap,
        identity::{ProviderRouteSnapshot, RemoteResourceType},
        zip_policy::{CapturedZipIntent, ZipIntentSelection},
    },
    store::{
        entities::{remote_pin, remote_pin_ledger},
        pinning::{
            management::{admission::work_error, records::DecodeWork},
            publication,
        },
    },
};
use sea_orm::ConnectionTrait;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct AllocationKeys {
    pub limits: ProviderLimitMap,
    pub routes: BTreeMap<String, ProviderRouteSnapshot>,
    pub enabled: BTreeSet<String>,
    pub canonical: BTreeMap<(String, String), String>,
    pub pairs: Vec<(String, String)>,
    _resident: crate::store::pinning::management::native::meter::ResidentGuard,
}

impl AllocationKeys {
    pub async fn load<C: ConnectionTrait>(
        db: &C,
        capture: &CapturedZipIntent,
        selected: &ZipIntentSelection,
        current_limits: Option<&ProviderLimitMap>,
        work: DecodeWork<'_>,
        mut maintenance: Option<&mut super::PublicationLease<'_>>,
    ) -> AppResult<Self> {
        let (mut limits, routes, enabled) = frozen_allocation(capture, selected)?;
        if let Some(current) = current_limits {
            for (name, frozen) in &mut limits {
                let current = current.get(name).ok_or_else(invalid)?;
                if frozen.enabled && !current.enabled {
                    return Err(invalid());
                }
                frozen.max_bytes = current.max_bytes;
                frozen.max_pins = current.max_pins;
            }
        }
        let mut raw = BTreeSet::new();
        for lease in &selected.leases {
            let providers = publication::ordered_enabled_providers(&lease.intent, &limits)?;
            for provider in providers {
                for target in &lease.targets {
                    work.copy(&(&provider, &target.cid)).map_err(work_error)?;
                    raw.insert((provider.clone(), target.cid.clone()));
                }
            }
        }
        let mut canonical = BTreeMap::new();
        let mut expected: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (provider, cid) in raw {
            let rpc = matches!(
                routes[&provider].resource_type(),
                RemoteResourceType::RpcPin | RemoteResourceType::ClusterPin
            );
            let key = if rpc {
                crate::pinning::provider::canonical_resource_cid(&cid).map_err(|_| invalid())?
            } else {
                cid.clone()
            };
            if rpc {
                expected
                    .entry(provider.clone())
                    .or_default()
                    .insert(key.clone());
            }
            canonical.insert((provider, cid), key);
        }
        for (provider, expected) in expected {
            let mut resident = Vec::new();
            let remotes = sql::read::<remote_pin::Entity, _>(
                db,
                "provider=$1",
                vec![provider.clone().into()],
                "cid",
                work,
                &mut resident,
                maintenance.as_deref_mut(),
            )
            .await?;
            let ledgers = sql::read::<remote_pin_ledger::Entity, _>(
                db,
                "provider=$1",
                vec![provider.into()],
                "cid",
                work,
                &mut resident,
                maintenance.as_deref_mut(),
            )
            .await?;
            for old in remotes
                .iter()
                .map(|r| &r.cid)
                .chain(ledgers.iter().map(|r| &r.cid))
            {
                if let Ok(key) = crate::pinning::provider::canonical_resource_cid(old)
                    && old != &key
                    && expected.contains(&key)
                {
                    return Err(invalid());
                }
            }
        }
        let pairs = canonical
            .iter()
            .map(|((provider, _), cid)| (provider.clone(), cid.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let bytes = canonical
            .iter()
            .try_fold(0usize, |total, ((provider, raw), cid)| {
                total
                    .checked_add(provider.len())
                    .and_then(|n| n.checked_add(raw.len()))
                    .and_then(|n| n.checked_add(cid.len()))
                    .and_then(|n| n.checked_add(192))
                    .ok_or(crate::error::AppError::ManagementBudget)
            })?
            .checked_add(
                routes
                    .len()
                    .checked_mul(2048)
                    .ok_or(crate::error::AppError::ManagementBudget)?,
            )
            .and_then(|n| n.checked_add(limits.len().checked_mul(256)?))
            .ok_or(crate::error::AppError::ManagementBudget)?;
        let resident = sql::retain_copy(work, bytes)?;
        Ok(Self {
            limits,
            routes,
            enabled,
            canonical,
            pairs,
            _resident: resident,
        })
    }

    pub async fn verify_aliases<C: ConnectionTrait>(
        &self,
        db: &C,
        work: DecodeWork<'_>,
        mut maintenance: Option<&mut super::PublicationLease<'_>>,
    ) -> AppResult<()> {
        let mut expected: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for ((provider, _), cid) in &self.canonical {
            if matches!(
                self.routes[provider].resource_type(),
                RemoteResourceType::RpcPin | RemoteResourceType::ClusterPin
            ) {
                expected
                    .entry(provider.clone())
                    .or_default()
                    .insert(cid.clone());
            }
        }
        for (provider, expected) in expected {
            let mut resident = Vec::new();
            let remotes = sql::read::<remote_pin::Entity, _>(
                db,
                "provider=$1",
                vec![provider.clone().into()],
                "cid",
                work,
                &mut resident,
                maintenance.as_deref_mut(),
            )
            .await?;
            let ledgers = sql::read::<remote_pin_ledger::Entity, _>(
                db,
                "provider=$1",
                vec![provider.into()],
                "cid",
                work,
                &mut resident,
                maintenance.as_deref_mut(),
            )
            .await?;
            for old in remotes
                .iter()
                .map(|row| &row.cid)
                .chain(ledgers.iter().map(|row| &row.cid))
            {
                if let Ok(canonical) = crate::pinning::provider::canonical_resource_cid(old)
                    && old != &canonical
                    && expected.contains(&canonical)
                {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }
}
