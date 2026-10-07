//! Birth intent, independent of future policy/configuration and selected targets.
use crate::{
    error::{AppError, AppResult},
    pinning::{identity::ProviderRouteSnapshot, provider::canonical_resource_cid},
    store::entities::{pin_lease_target, pin_provider_route, remote_pin},
};
use chrono::{DateTime, Utc};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BirthContract {
    pub provider_mode: String,
    pub content_mode: String,
    pub content_cids: BTreeSet<String>,
    pub candidate_order: Vec<String>,
    /// Includes blocked/quota-waiting/disabled candidates; None is unknown.
    pub candidates: BTreeMap<String, Option<ProviderRouteSnapshot>>,
}

#[allow(clippy::too_many_arguments)]
pub async fn insert<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    owner: &str,
    provider_mode: &str,
    content_mode: &str,
    cids: impl Iterator<Item = String>,
    candidates: &[String],
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut routes = BTreeMap::new();
    for provider in candidates {
        let route = pin_provider_route::Entity::find_by_id(provider.clone())
            .one(db)
            .await?
            .and_then(|row| serde_json::from_str(&row.snapshot).ok());
        routes.insert(provider.clone(), route);
    }
    let contract = BirthContract {
        provider_mode: provider_mode.into(),
        content_mode: content_mode.into(),
        content_cids: cids.collect(),
        candidate_order: candidates.to_vec(),
        candidates: routes,
    };
    super::Entity::insert(super::ActiveModel {
        lease_id: Set(lease_id.into()),
        owner_object_id: Set(owner.into()),
        contract: Set(serde_json::to_string(&contract)
            .map_err(|_| AppError::Internal("birth contract serialization".into()))?),
        created_at: Set(now),
    })
    .exec(db)
    .await?;
    Ok(())
}

pub async fn get<C: ConnectionTrait>(db: &C, lease_id: &str) -> AppResult<Option<BirthContract>> {
    super::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?
        .map(|row| {
            serde_json::from_str(&row.contract)
                .map_err(|_| AppError::Internal("invalid birth contract".into()))
        })
        .transpose()
}

pub async fn frozen_candidates<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
) -> AppResult<Option<Vec<String>>> {
    Ok(get(db, lease_id).await?.map(|birth| birth.candidate_order))
}

pub fn read_route_compatible(
    original: &ProviderRouteSnapshot,
    current: &ProviderRouteSnapshot,
) -> bool {
    if original.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin
        && (original.api_profile != current.api_profile
            || original.strategy != current.strategy
            || original.confirmation.is_none()
            || current.confirmation.is_none())
    {
        return false;
    }
    let mut comparable = original.clone();
    comparable.api_profile = current.api_profile.clone();
    comparable.strategy = current.strategy.clone();
    comparable.cleanup = current.cleanup;
    // A changed min/max-age never substitutes for the birth requirement. The
    // historical read uses original.confirmation and its original adapter.
    comparable.confirmation = current.confirmation.clone();
    comparable == *current
}

/// A captured Cluster invocation may execute its original threshold after a
/// min/age change, but may not borrow any changed write route or permission.
pub(crate) fn invocation_route_compatible(
    original: &ProviderRouteSnapshot,
    current: &ProviderRouteSnapshot,
) -> bool {
    let mut comparable = original.clone();
    if original.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin {
        if original.confirmation.is_none() || current.confirmation.is_none() {
            return false;
        }
        comparable.confirmation = current.confirmation.clone();
    }
    comparable == *current
}

pub async fn target_confirmed<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let ledger = crate::store::pinning::ledger::get(db, provider, cid).await?;
    let Some(route) = ledger
        .as_ref()
        .and_then(crate::store::pinning::ledger::decode_route)
    else {
        return Ok(false);
    };
    let Some(birth) = get(db, lease_id).await? else {
        // Old Cluster leases have no original confirmation contract.
        return Ok(route.resource_type()
            != crate::pinning::identity::RemoteResourceType::ClusterPin
            && ledger.is_some_and(|row| row.effect == "confirmed"));
    };
    let Some(Some(original)) = birth.candidates.get(provider) else {
        return Ok(false);
    };
    let same_route =
        if route.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin {
            read_route_compatible(original, &route)
        } else {
            original == &route
        };
    if !same_route
        || !birth.content_cids.iter().any(|object_cid| {
            canonical_resource_cid(object_cid).ok() == canonical_resource_cid(cid).ok()
        })
    {
        return Ok(false);
    }
    if route.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin {
        return super::super::pin_replica_observation::receipt::fresh_complete(
            db, provider, cid, original, now,
        )
        .await;
    }
    Ok(ledger.is_some_and(|row| row.effect == "confirmed"))
}

pub async fn completion<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    now: DateTime<Utc>,
) -> AppResult<Option<(bool, bool, usize)>> {
    let Some(birth) = get(db, lease_id).await? else {
        return Ok(None);
    };
    let mut any = !birth.content_cids.is_empty() && !birth.candidates.is_empty();
    let mut all = any;
    let mut pinned = 0;
    for object_cid in &birth.content_cids {
        let mut available = false;
        for (provider, route) in &birth.candidates {
            let cid = if route.as_ref().is_some_and(|route| {
                matches!(
                    route.resource_type(),
                    crate::pinning::identity::RemoteResourceType::ClusterPin
                        | crate::pinning::identity::RemoteResourceType::RpcPin
                )
            }) {
                canonical_resource_cid(object_cid).unwrap_or_else(|_| object_cid.clone())
            } else {
                object_cid.clone()
            };
            let remote = remote_pin::Entity::find_by_id((provider.clone(), cid.clone()))
                .one(db)
                .await?;
            let attached = pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .filter(pin_lease_target::Column::Provider.eq(provider))
                .filter(pin_lease_target::Column::Cid.eq(&cid))
                .filter(pin_lease_target::Column::State.eq("pinned"))
                .one(db)
                .await?
                .is_some();
            let cluster = route.as_ref().is_some_and(|route| {
                route.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin
            });
            let confirmed = attached
                && remote.is_some_and(|row| {
                    row.status == "pinned" || (cluster && row.status == "pinning")
                })
                && target_confirmed(db, lease_id, provider, &cid, now).await?;
            available |= confirmed;
            all &= confirmed;
            pinned += usize::from(confirmed);
        }
        any &= available;
    }
    // One succeeds per content CID; All retains the complete birth candidate set.
    let complete = if birth.provider_mode == "one" {
        any
    } else {
        all
    };
    Ok(Some((any, complete, pinned)))
}
