//! Management completion shares its caller's row and receipt work.
use super::*;
use crate::pinning::provider::ObservedResource;
use crate::store::pinning::{
    legacy_migration as legacy,
    management::{
        ManagementError, Result,
        records::{self, DecodeWork},
    },
};

fn parsed<T: serde::de::DeserializeOwned>(raw: &str, work: DecodeWork<'_>) -> Result<Option<T>> {
    match work.with(|work| work.typed(raw)) {
        Ok(value) => Ok(Some(value)),
        Err(ManagementError::Budget) => Err(ManagementError::Budget),
        Err(_) => Ok(None),
    }
}

pub(crate) async fn fresh_complete_catalog(
    catalog: legacy::ReceiptCatalog<'_>,
    provider: &str,
    cid: &str,
    route: &ProviderRouteSnapshot,
    now: DateTime<Utc>,
    work: DecodeWork<'_>,
) -> Result<bool> {
    let db = &catalog;
    let provider = legacy::resource_key_catalog(catalog, provider, cid, &work).await?;
    let Some(registered) =
        records::current_model::<pin_provider_route::Entity, _>(db, &[&provider], work).await?
    else {
        return Ok(false);
    };
    let Some(current) = parsed::<ProviderRouteSnapshot>(&registered.snapshot, work)? else {
        return Ok(false);
    };
    work.copy(&(route, &current))?;
    if !read_route_compatible(route, &current) {
        return Ok(false);
    }
    let keys = legacy::historical_keys_catalog(
        catalog,
        "pin_replica_observations",
        &provider,
        Some(cid),
        &work,
    )
    .await?;
    if keys.is_empty() {
        return Ok(false);
    }
    work.copy(&keys)?;
    work.copy(&(cid, cid))?;
    work.with(|work| work.nodes(1 + keys.len()))?;
    let parameters = (1..=keys.len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(",");
    let parameter = keys.len() + 1;
    let mut values = keys
        .into_iter()
        .map(Into::into)
        .collect::<Vec<sea_orm::Value>>();
    values.push(cid.into());
    let (candidates,bytes)=records::bounded_projection(db,&format!("FROM pin_replica_observations WHERE provider IN ({parameters}) AND expected_cid=${parameter} AND projectable=TRUE"),&[("sequence","sequence"),("provider","provider")],"sequence DESC",values,(crate::store::pinning::management::native::MAX_ROWS,crate::store::pinning::management::native::MAX_BYTES),work).await?;
    work.index_cells(candidates.len() * 2, bytes)?;
    let mut selected = None;
    for candidate in candidates {
        let sequence: String = candidate
            .try_get("", "sequence")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        let original: String = candidate
            .try_get("", "provider")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        work.copy(&[("sequence", &sequence)])?;
        let key = [("sequence".into(), sequence.clone())].into();
        let mapped = legacy::resolve_with_catalog(
            "pin_replica_observations",
            &key,
            &original,
            &work,
            catalog,
        )
        .await?;
        if original == provider || mapped.is_some_and(|binding| binding.operational_key == provider)
        {
            let row = records::current_model::<Entity, _>(db, &[&sequence], work)
                .await?
                .ok_or(ManagementError::Changed)?;
            if row.provider != original || !row.projectable || row.expected_cid != cid {
                return Err(ManagementError::Changed);
            }
            selected = Some(row);
            break;
        }
    }
    let Some(row) = selected else {
        return Ok(false);
    };
    let Some(recorded) = parsed::<ProviderRouteSnapshot>(&row.route, work)? else {
        return Ok(false);
    };
    work.copy(&(route, &recorded))?;
    if row.phase != "poll"
        || !matches!(row.outcome.as_str(), "Complete" | "Insufficient")
        || !read_route_compatible(route, &recorded)
        || legacy::release_history_current_catalog(catalog, &provider, cid, &work)
            .await?
            .iter()
            .any(|release| release.epoch >= row.remote_epoch)
    {
        return Ok(false);
    }
    let Some(remote) = records::current_model::<remote_pin::Entity, _>(db, &[&provider, cid], work)
        .await?
        .filter(|remote| {
            remote.epoch >= row.remote_epoch
                && matches!(remote.status.as_str(), "pinned" | "pinning")
        })
    else {
        return Ok(false);
    };
    let Some(resources) = parsed::<Vec<ObservedResource>>(&row.resources, work)? else {
        return Ok(false);
    };
    let [resource] = resources.as_slice() else {
        return Ok(false);
    };
    Ok(
        remote.request_id.as_deref() == Some(resource.request_id.as_str())
            && super::super::validation::evidence_valid(resource, route, cid, now)
            && super::super::validation::evidence_valid(resource, &recorded, cid, now)
            && resource.replica_evidence.as_ref().is_some_and(|evidence| {
                evidence.pinned_peers.len()
                    >= super::super::validation::birth_threshold(resource, route)
                        .unwrap_or(usize::MAX)
            }),
    )
}
