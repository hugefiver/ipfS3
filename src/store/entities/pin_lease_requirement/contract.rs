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
    insert_inner(db, lease_id, owner, provider_mode, content_mode, cids, candidates, now, None).await
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_management<C: ConnectionTrait>(
    db:&C,lease_id:&str,owner:&str,provider_mode:&str,content_mode:&str,
    cids:impl Iterator<Item=String>,candidates:&[String],now:DateTime<Utc>,
    requirements:&crate::pinning::management::Requirements,
)->AppResult<()> {
    insert_inner(db,lease_id,owner,provider_mode,content_mode,cids,candidates,now,Some(requirements)).await
}

#[allow(clippy::too_many_arguments)]
async fn insert_inner<C:ConnectionTrait>(
    db:&C,lease_id:&str,owner:&str,provider_mode:&str,content_mode:&str,
    cids:impl Iterator<Item=String>,candidates:&[String],now:DateTime<Utc>,
    requirements:Option<&crate::pinning::management::Requirements>,
)->AppResult<()> {
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
    let encoded=if let Some(requirements)=requirements {
        let mut encoded=serde_json::to_value(&contract).map_err(|_|AppError::Internal("birth contract serialization".into()))?;
        encoded.as_object_mut().ok_or_else(||AppError::Internal("birth contract serialization".into()))?
            .insert("management_requirements".into(),serde_json::to_value(requirements).map_err(|_|AppError::Internal("birth contract serialization".into()))?);
        serde_json::to_string(&encoded)
    } else {serde_json::to_string(&contract)}.map_err(|_|AppError::Internal("birth contract serialization".into()))?;
    super::Entity::insert(super::ActiveModel {
        lease_id: Set(lease_id.into()),
        owner_object_id: Set(owner.into()),
        contract: Set(encoded),
        created_at: Set(now),
    })
    .exec(db)
    .await?;
    Ok(())
}

pub async fn management_requirements<C:ConnectionTrait>(db:&C,lease_id:&str)->AppResult<Option<crate::pinning::management::Requirements>> {
    let Some(row)=super::Entity::find_by_id(lease_id.to_owned()).one(db).await? else{return Ok(None)};
    let encoded:serde_json::Value=serde_json::from_str(&row.contract).map_err(|_|AppError::Internal("invalid birth contract".into()))?;
    encoded.get("management_requirements").cloned().map(serde_json::from_value).transpose()
        .map_err(|_|AppError::Internal("invalid management birth requirements".into()))
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
    let Some(birth) = get(db, lease_id).await? else {
        return Ok(None);
    };
    let mut common=None;
    for cid in &birth.content_cids {
        let candidates=frozen_candidates_for_cid(db,lease_id,cid).await?.ok_or_else(||AppError::Internal("birth contract disappeared".into()))?;
        if common.as_ref().is_some_and(|previous|*previous!=candidates) {
            return Err(AppError::InvalidPinningRequest("CID-specific management candidates require an exact CID".into()));
        }
        common=Some(candidates);
    }
    Ok(common.or_else(||Some(vec![])))
}

pub async fn frozen_candidates_for_cid<C:ConnectionTrait>(db:&C,lease_id:&str,cid:&str)->AppResult<Option<Vec<String>>> {
    let Some(birth)=get(db,lease_id).await? else{return Ok(None)};
    let resolved=crate::store::pinning::management::actions::points::candidates_for_cid(db,lease_id,cid,&birth)
        .await.map_err(|_|AppError::InvalidPinningRequest("invalid exact management point".into()))?;
    Ok(Some(resolved.into_iter().map(|(_,allocation,_)|allocation).collect()))
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
    let birth=get(db,lease_id).await?;
    let resolved=if let Some(birth)=&birth {
        crate::store::pinning::management::actions::points::candidates_for_cid(db,lease_id,cid,birth)
            .await.map_err(|_|AppError::InvalidPinningRequest("invalid exact management point".into()))?
    } else {vec![]};
    let selected=resolved.iter().filter(|(slot,allocation,_)|slot==provider||allocation==provider).collect::<Vec<_>>();
    if selected.len()>1 {return Ok(false);}
    let allocation=selected.first().map_or(provider,|(_,allocation,_)|allocation.as_str());
    if crate::store::pinning::legacy_migration::resource_binding(db, allocation, cid)
        .await?
        .is_some_and(|binding| binding.historical_route.is_none())
    {
        return Ok(false);
    }
    let ledger = crate::store::pinning::ledger::get(db, allocation, cid).await?;
    let Some(route) = ledger
        .as_ref()
        .and_then(crate::store::pinning::ledger::decode_route)
    else {
        return Ok(false);
    };
    let Some(birth) = birth else {
        // Old Cluster leases have no original confirmation contract.
        return Ok(route.resource_type()
            != crate::pinning::identity::RemoteResourceType::ClusterPin
            && ledger.is_some_and(|row| row.effect == "confirmed"));
    };
    let Some((slot,_,point))=selected.first().copied() else{return Ok(false)};
    let Some(original) = birth.candidates.get(slot).and_then(Option::as_ref) else {
        return Ok(false);
    };
    let same_route = if let Some(point)=point {
        route==point.destination_route
            || (point.destination_allocation==point.source_allocation&&route==point.source_resource_route)
    } else {
        if route.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin {
            read_route_compatible(original, &route)
        } else {
            original == &route
        }};
    if !same_route
        || !birth.content_cids.iter().any(|object_cid| {
            canonical_resource_cid(object_cid).ok() == canonical_resource_cid(cid).ok()
        })
    {
        return Ok(false);
    }
    let full=management_requirements(db,lease_id).await?;
    // The logical slot retains its birth proof even for an older contract with
    // no additive management requirements. Pinned PSA facts are not peer proof.
    if !destination_can_prove_birth(original,&route){return Ok(false);}
    if let Some(requirements)=&full {
        use crate::pinning::management::ConfirmationLevel;
        let Some(row)=&ledger else{return Ok(false)};
        if requirements.levels.iter().any(|level|match level {
            ConfirmationLevel::Accepted=>false,
            ConfirmationLevel::RemoteSubmitted=>row.first_observed_at.is_none(),
            ConfirmationLevel::RemotePinned=>row.remote_pinned_at.is_none()&&row.effect!="confirmed",
            ConfirmationLevel::GatewayVerified=>row.gateway_verified_at.is_none(),
            ConfirmationLevel::ContentVerified=>row.content_verified_at.is_none(),
            ConfirmationLevel::ReplicationComplete=>route.resource_type()!=crate::pinning::identity::RemoteResourceType::ClusterPin,
        }) {return Ok(false);}
        if route.resource_type()!=crate::pinning::identity::RemoteResourceType::ClusterPin && requirements.min_replicas>1 {return Ok(false);}
    }
    if route.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin {
        let mut required=route.clone();
        // Evaluate destination observations using at least the original slot's
        // Cluster min/age, plus the complete explicit management requirements.
        let Some(confirmation)=replica_requirement(original,&route,full.as_ref()) else{return Ok(false)};
        required.confirmation=Some(confirmation);
        return super::super::pin_replica_observation::receipt::fresh_complete(
            db, allocation, cid, &required, now,
        )
        .await;
    }
    Ok(ledger.is_some_and(|row| row.effect == "confirmed"))
}

fn destination_can_prove_birth(original:&ProviderRouteSnapshot,destination:&ProviderRouteSnapshot)->bool {
    original.resource_type()!=crate::pinning::identity::RemoteResourceType::ClusterPin
        || original.confirmation.is_some()&&destination.resource_type()==crate::pinning::identity::RemoteResourceType::ClusterPin&&destination.confirmation.is_some()
}
fn replica_requirement(original:&ProviderRouteSnapshot,destination:&ProviderRouteSnapshot,full:Option<&crate::pinning::management::Requirements>)->Option<crate::pinning::identity::ConfirmationRequirement> {
    use crate::pinning::identity::ConfirmationRequirement::ClusterReplicas;
    if !destination_can_prove_birth(original,destination){return None;}
    let ClusterReplicas {mut min_replicas,mut max_age_secs}=destination.confirmation.clone()?;
    if let Some(ClusterReplicas {min_replicas:minimum,max_age_secs:age})=original.confirmation.as_ref() {min_replicas=min_replicas.max(*minimum);max_age_secs=max_age_secs.min(*age);}
    if let Some(full)=full {min_replicas=min_replicas.max(full.min_replicas);if let Some(age)=full.max_age_secs {max_age_secs=max_age_secs.min(age);}}
    Some(ClusterReplicas {min_replicas,max_age_secs})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn historical_cluster_slot_requires_peer_proof_without_management_additive_fields() {
        use crate::pinning::identity::{CleanupMode,ConfirmationRequirement::ClusterReplicas};
        let source=ProviderRouteSnapshot {provider_id:"source".into(),backend:"cluster".into(),scope:"original-pinset".into(),storage_domain:"old".into(),credential_revision:1,endpoint_revision:1,secret_ref:None,api_profile:"cluster-rest".into(),strategy:"rest-pin".into(),cleanup:CleanupMode::Retain,confirmation:Some(ClusterReplicas {min_replicas:3,max_age_secs:10})};
        let mut destination=source.clone();destination.provider_id="destination".into();destination.confirmation=Some(ClusterReplicas {min_replicas:1,max_age_secs:60});
        assert!(replica_requirement(&source,&destination,None)==Some(ClusterReplicas {min_replicas:3,max_age_secs:10}));
        destination.api_profile="noop".into();destination.confirmation=None;
        assert!(!destination_can_prove_birth(&source,&destination));assert!(replica_requirement(&source,&destination,None).is_none());
        // Returning to the original logical slot does not remove its threshold.
        assert!(replica_requirement(&source,&source,None)==source.confirmation);
    }
}

pub async fn completion<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    now: DateTime<Utc>,
) -> AppResult<Option<(bool, bool, usize)>> {
    let Some(birth) = get(db, lease_id).await? else {
        return Ok(None);
    };
    let lease=super::super::pin_lease::Entity::find_by_id(lease_id.to_owned()).one(db).await?;
    if lease.is_none_or(|lease|lease.state!="active"||lease.expires_at<=now){return Ok(Some((false,false,0)));}
    let mut any = !birth.content_cids.is_empty() && !birth.candidates.is_empty();
    let mut all = any;
    let mut pinned = 0;
    for object_cid in &birth.content_cids {
        let mut available = false;
        let resolved=crate::store::pinning::management::actions::points::candidates_for_cid(db,lease_id,object_cid,&birth)
            .await.map_err(|_|AppError::InvalidPinningRequest("invalid exact management point".into()))?;
        for (provider, operational, point) in &resolved {
            let route=point.as_ref().map(|point|&point.destination_route).or_else(||birth.candidates.get(provider).and_then(Option::as_ref));
            let cid = if route.is_some_and(|route| {
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
            let remote = remote_pin::Entity::find_by_id((operational.clone(), cid.clone()))
                .one(db)
                .await?;
            let attached = pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .filter(pin_lease_target::Column::Provider.eq(operational))
                .filter(pin_lease_target::Column::Cid.eq(&cid))
                .filter(pin_lease_target::Column::State.eq("pinned"))
                .one(db)
                .await?
                .is_some();
            let cluster = route.is_some_and(|route| {
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
