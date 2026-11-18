use super::{allocation::frozen_allocation, invalid, sql};
use crate::{
    error::AppResult,
    pinning::{
        config::ProviderLimitMap,
        identity::{ProviderRouteSnapshot, RemoteResourceType},
        zip_policy::{CapturedZipIntent, ZipIntentSelection},
    },
    store::pinning::{
        management::{admission::work_error, records::DecodeWork},
        publication,
    },
};
use sea_orm::{ConnectionTrait, Statement};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct AllocationKeys {
    pub limits: ProviderLimitMap,
    pub routes: BTreeMap<String, ProviderRouteSnapshot>,
    pub enabled: BTreeSet<String>,
    pub canonical: BTreeMap<(String, String), String>,
    pub pairs: Vec<(String, String)>,
}

async fn reject_historical_aliases<C: ConnectionTrait>(
    db: &C,
    expected: &BTreeMap<String, BTreeSet<String>>,
    work: DecodeWork<'_>,
    mut maintenance: Option<&mut super::PublicationLease<'_>>,
) -> AppResult<()> {
    for (provider, cids) in expected {
        for chunk in cids.iter().collect::<Vec<_>>().chunks(sql::PARAMS - 1) {
            if let Some(lease) = maintenance.as_deref_mut() {
                lease.checkpoint(db).await?;
            }
            sql::charge(
                work,
                crate::store::pinning::management::native::meter::WorkMeter {
                    sql: 1,
                    ..Default::default()
                },
            )?;
            let mut values = vec![provider.as_str().into()];
            values.extend(chunk.iter().map(|cid| cid.as_str().into()));
            let row = db
                .query_one(Statement::from_sql_and_values(
                    db.get_database_backend(),
                    format!(
                        "SELECT 1 AS present FROM pin_cid_comparisons comparison \
                         WHERE comparison.canonical_cid IN ({}) \
                         AND comparison.cid<>comparison.canonical_cid \
                         AND (EXISTS(SELECT 1 FROM remote_pins remote \
                             WHERE remote.provider=$1 AND remote.cid=comparison.cid) \
                         OR EXISTS(SELECT 1 FROM remote_pin_ledger ledger \
                             WHERE ledger.provider=$1 AND ledger.cid=comparison.cid)) LIMIT 1",
                        sql::slots(chunk.len(), 1)
                    ),
                    values,
                ))
                .await?;
            if row.is_some() {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

impl AllocationKeys {
    pub async fn load<C: ConnectionTrait>(
        db: &C,
        capture: &CapturedZipIntent,
        selected: &ZipIntentSelection,
        current_limits: Option<&ProviderLimitMap>,
        work: DecodeWork<'_>,
        maintenance: Option<&mut super::PublicationLease<'_>>,
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
        reject_historical_aliases(db, &expected, work, maintenance).await?;
        let pairs = canonical
            .iter()
            .map(|((provider, _), cid)| (provider.clone(), cid.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(Self {
            limits,
            routes,
            enabled,
            canonical,
            pairs,
        })
    }

    pub async fn verify_aliases<C: ConnectionTrait>(
        &self,
        db: &C,
        work: DecodeWork<'_>,
        maintenance: Option<&mut super::PublicationLease<'_>>,
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
        reject_historical_aliases(db, &expected, work, maintenance).await
    }
}
