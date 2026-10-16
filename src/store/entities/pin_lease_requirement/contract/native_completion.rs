//! Native completion without management suppliers. Participation is checked
//! before and after the read, never cached as an execution permission.
use super::*;
use crate::store::pinning::legacy_migration as legacy;
use sea_orm::{ColumnTrait, DatabaseBackend, QueryFilter, Statement};

pub(super) async fn management_participates<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    work: SharedDecodeBudget,
) -> management::Result<bool> {
    let context = DecodeWork::shared(&work);
    if management::records::current_model::<super::super::super::pin_lease::Entity, _>(
        db,
        &[lease_id],
        context,
    )
    .await?
    .is_some_and(|lease| lease.source.starts_with("management:"))
    {
        return Ok(true);
    }
    context.with(|work| work.nodes(1))?;
    let sql = match db.get_database_backend() {
        DatabaseBackend::Sqlite => {
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='pin_management_records') AS present"
        }
        DatabaseBackend::Postgres => {
            "SELECT to_regclass('pin_management_records') IS NOT NULL AS present"
        }
        _ => return Err(ManagementError::Schema),
    };
    let present = db
        .query_one(Statement::from_string(db.get_database_backend(), sql))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Database)?
        .try_get::<bool>("", "present")
        .map_err(|_| ManagementError::Schema)?;
    if !present {
        return Ok(false);
    }
    management::records::lease_has_selectors(db, lease_id, context).await
}

async fn candidates<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    birth: &BirthContract,
) -> AppResult<Vec<(String, String)>> {
    let key = [("lease_id".into(), lease_id.into())].into();
    let mut seen = std::collections::BTreeSet::new();
    let mut result = Vec::new();
    for slot in &birth.candidate_order {
        let allocation = legacy::resolve_legacy_binding(db, "pin_lease_requirements", &key, slot)
            .await?
            .map_or_else(|| slot.clone(), |binding| binding.operational_key);
        if !seen.insert(allocation.clone()) {
            return Err(AppError::InvalidPinningRequest(
                "duplicate native candidate allocation".into(),
            ));
        }
        result.push((slot.clone(), allocation));
    }
    Ok(result)
}

pub(super) async fn target_confirmed<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
) -> AppResult<bool> {
    let birth = get(db, lease_id).await?;
    let resolved = if let Some(birth) = &birth {
        candidates(db, lease_id, birth).await?
    } else {
        vec![]
    };
    let selected = resolved
        .iter()
        .filter(|(slot, allocation)| slot == provider || allocation == provider)
        .collect::<Vec<_>>();
    if selected.len() > 1 {
        return Ok(false);
    }
    let allocation = selected
        .first()
        .map_or(provider, |(_, allocation)| allocation.as_str());
    if legacy::resource_binding(db, allocation, cid)
        .await?
        .is_some_and(|binding| binding.historical_route.is_none())
    {
        return Ok(false);
    }
    let ledger = crate::store::pinning::ledger::get(db, allocation, cid).await?;
    let Some(route) = ledger
        .as_ref()
        .and_then(|row| row.route.as_deref())
        .and_then(|raw| serde_json::from_str::<ProviderRouteSnapshot>(raw).ok())
    else {
        return Ok(false);
    };
    let Some(birth) = birth else {
        return Ok(route.resource_type()
            != crate::pinning::identity::RemoteResourceType::ClusterPin
            && ledger.is_some_and(|row| row.effect == "confirmed"));
    };
    let Some((slot, _)) = selected.first().copied() else {
        return Ok(false);
    };
    let Some(original) = birth.candidates.get(slot).and_then(Option::as_ref) else {
        return Ok(false);
    };
    let compatible =
        if route.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin {
            read_route_compatible(original, &route)
        } else {
            *original == route
        };
    if !compatible
        || !birth.content_cids.iter().any(|object_cid| {
            canonical_resource_cid(object_cid).ok() == canonical_resource_cid(cid).ok()
        })
    {
        return Ok(false);
    }
    let full = management_requirements(db, lease_id).await?;
    if !destination_can_prove_birth(original, &route) {
        return Ok(false);
    }
    if let Some(requirements) = &full {
        use crate::pinning::management::ConfirmationLevel;
        let Some(row) = &ledger else { return Ok(false) };
        if requirements.levels.iter().any(|level| match level {
            ConfirmationLevel::Accepted => false,
            ConfirmationLevel::RemoteSubmitted => row.first_observed_at.is_none(),
            ConfirmationLevel::RemotePinned => {
                row.remote_pinned_at.is_none() && row.effect != "confirmed"
            }
            ConfirmationLevel::GatewayVerified => row.gateway_verified_at.is_none(),
            ConfirmationLevel::ContentVerified => row.content_verified_at.is_none(),
            ConfirmationLevel::ReplicationComplete => {
                route.resource_type() != crate::pinning::identity::RemoteResourceType::ClusterPin
            }
        }) {
            return Ok(false);
        }
        if route.resource_type() != crate::pinning::identity::RemoteResourceType::ClusterPin
            && requirements.min_replicas > 1
        {
            return Ok(false);
        }
    }
    if route.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin {
        let mut required = route.clone();
        let Some(confirmation) = replica_requirement(original, &route, full.as_ref()) else {
            return Ok(false);
        };
        required.confirmation = Some(confirmation);
        return super::super::super::pin_replica_observation::receipt::fresh_complete(
            db, allocation, cid, &required, now,
        )
        .await;
    }
    Ok(ledger.is_some_and(|row| row.effect == "confirmed"))
}

pub(super) async fn completion<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    now: DateTime<Utc>,
) -> AppResult<Option<(bool, bool, usize)>> {
    let Some(birth) = get(db, lease_id).await? else {
        return Ok(None);
    };
    let lease = super::super::super::pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await?;
    if lease.is_none_or(|lease| lease.state != "active" || lease.expires_at <= now) {
        return Ok(Some((false, false, 0)));
    }
    let mut any = !birth.content_cids.is_empty() && !birth.candidates.is_empty();
    let mut all = any;
    let mut pinned = 0;
    for object_cid in &birth.content_cids {
        let mut available = false;
        let resolved = candidates(db, lease_id, &birth).await?;
        for (slot, allocation) in &resolved {
            let route = birth.candidates.get(slot).and_then(Option::as_ref);
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
            let remote = remote_pin::Entity::find_by_id((allocation.clone(), cid.clone()))
                .one(db)
                .await?;
            let attached = pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .filter(pin_lease_target::Column::Provider.eq(allocation))
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
                    row.status == "pinned" || cluster && row.status == "pinning"
                })
                && target_confirmed(db, lease_id, slot, &cid, now).await?;
            available |= confirmed;
            all &= confirmed;
            pinned += usize::from(confirmed);
        }
        any &= available;
    }
    Ok(Some((
        any,
        if birth.provider_mode == "one" {
            any
        } else {
            all
        },
        pinned,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn native_decompressed_content_reports_incomplete_without_management_authority() {
        let fixture =
            crate::store::pinning::management::actions::tests::fixture("native-content-completion")
                .await;
        let owner = match &fixture.binding.request.items[0].subject {
            crate::pinning::management::Subject::Object { identity, .. } => &identity.object_id,
            _ => unreachable!(),
        };
        let now = crate::store::database_clock::database_now(&fixture.db)
            .await
            .unwrap();
        let provider = &fixture.config.pinning.providers[0];
        fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,generation,state,created_at,last_touched_at,expires_at) VALUES($1,$2,'automatic',$3,'all','decompressed',1,'active',$4,$4,$5)",["native-content".into(),owner.clone().into(),fixture.config.pinning.policies[0].identity.clone().into(),now.into(),(now+chrono::Duration::hours(1)).into()])).await.unwrap();
        let cids = (0u64..5000).map(|index| {
            let mut digest = [0u8; 32];
            digest[..8].copy_from_slice(&index.to_le_bytes());
            cid::Cid::new_v1(
                0x55,
                cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap(),
            )
            .to_string()
        });
        insert(
            &fixture.db,
            "native-content",
            owner,
            "all",
            "decompressed",
            cids,
            std::slice::from_ref(&provider.name),
            now,
        )
        .await
        .unwrap();
        assert_eq!(
            super::super::completion(&fixture.db, "native-content", now)
                .await
                .unwrap(),
            Some((false, false, 0))
        );
    }
}
