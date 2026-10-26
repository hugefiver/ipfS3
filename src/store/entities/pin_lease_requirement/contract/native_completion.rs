//! Native completion without management suppliers. Participation is checked
//! before and after the read, never cached as an execution permission.
use super::*;
use crate::store::pinning::legacy_migration as legacy;
#[cfg(test)]
use sea_orm::Statement;

#[path = "native_completion/batch.rs"]
mod batch;
#[cfg(test)]
#[path = "native_completion/tests/behavior.rs"]
mod behavior;

pub(super) async fn management_participates<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    work: SharedDecodeBudget,
) -> management::Result<bool> {
    let context = DecodeWork::shared(&work);
    if management::admission::authority::lease_participates(db, lease_id, context).await? {
        return Ok(true);
    }
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
    Ok(false)
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
    target_confirmed_for_birth(db, lease_id, provider, cid, now, birth.as_ref(), &resolved).await
}

async fn target_confirmed_for_birth<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
    birth: Option<&BirthContract>,
    resolved: &[(String, String)],
) -> AppResult<bool> {
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
        let Some(confirmation) = replica_requirement(original, original, full.as_ref()) else {
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
    let resolved = candidates(db, lease_id, &birth).await?;
    let mut combinations = batch::combinations(&birth, &resolved);
    let mut current_cid = None;
    let mut available = false;
    loop {
        let combinations = combinations
            .by_ref()
            .take(batch::PAGE_SIZE)
            .collect::<Vec<_>>();
        if combinations.is_empty() {
            break;
        }
        let page = batch::Page::load(db, lease_id, &combinations).await?;
        for combination in &combinations {
            if current_cid != Some(combination.object_cid) {
                if current_cid.is_some() {
                    any &= available;
                }
                current_cid = Some(combination.object_cid);
                available = false;
            }
            let confirmed = page.eligible(combination)
                && target_confirmed_for_birth(
                    db,
                    lease_id,
                    combination.slot,
                    &combination.query_cid,
                    now,
                    Some(&birth),
                    &resolved,
                )
                .await?;
            available |= confirmed;
            all &= confirmed;
            pinned += usize::from(confirmed);
        }
    }
    if !birth.content_cids.is_empty() {
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
        let mut fixture =
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
        let queries = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = queries.clone();
        fixture.db.set_metric_callback(move |_| {
            observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
        let started = std::time::Instant::now();
        assert_eq!(
            super::super::completion(&fixture.db, "native-content", now)
                .await
                .unwrap(),
            Some((false, false, 0))
        );
        let queries = queries.load(std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "native 5000 CIDs: {queries} SQL statements, {:?}",
            started.elapsed()
        );
        assert!(
            queries <= 2 * 5000_usize.div_ceil(256) + 64,
            "empty confirmation reads must scale with bounded pages, got {queries}"
        );
    }
}
