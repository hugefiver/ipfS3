//! Point projections. Original models/payloads remain the historical evidence.
use super::{LegacyBinding, historical_keys, resolve_legacy_binding};
use crate::{error::AppResult, store::entities::pin_resource_history};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use std::collections::{BTreeMap, BTreeSet};

pub async fn resource_binding<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<LegacyBinding>> {
    let Some(binding) = identity_binding(db, provider, cid).await? else {
        return Ok(None);
    };
    if super::resource_lifetime::fresh(db, &binding, cid)
        .await?
        .is_some()
    {
        return Ok(None);
    }
    Ok(Some(binding))
}

async fn identity_binding<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<LegacyBinding>> {
    if super::receipt::active(db).await?.is_empty() {
        return Ok(None);
    }
    for table in ["remote_pin_ledger", "remote_pins"] {
        for original in historical_keys(db, table, provider, Some(cid)).await? {
            let key = [
                ("provider".into(), original.clone()),
                ("cid".into(), cid.into()),
            ]
            .into();
            if let Some(binding) = resolve_legacy_binding(db, table, &key, &original).await? {
                return Ok(Some(binding));
            }
        }
    }
    Ok(None)
}

pub async fn resource_key<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<String> {
    Ok(identity_binding(db, provider, cid)
        .await?
        .map_or_else(|| provider.into(), |b| b.operational_key))
}

pub async fn captured_provider<C: ConnectionTrait>(
    db: &C,
    table: &str,
    key: &BTreeMap<String, String>,
    provider: &str,
) -> AppResult<String> {
    Ok(resolve_legacy_binding(db, table, key, provider)
        .await?
        .map_or_else(|| provider.into(), |b| b.operational_key))
}

pub async fn original_job_provider<C: ConnectionTrait>(
    db: &C,
    id: &str,
    provider: &str,
) -> AppResult<String> {
    if super::receipt::active(db).await?.is_empty() {
        return Ok(provider.into());
    }
    let pk = [("id".into(), id.into())].into();
    for original in historical_keys(db, "pin_jobs", provider, None).await? {
        if resolve_legacy_binding(db, "pin_jobs", &pk, &original)
            .await?
            .is_some()
        {
            return Ok(original);
        }
    }
    Ok(provider.into())
}

pub async fn job_io_allowed<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
) -> AppResult<bool> {
    if super::receipt::active(db).await?.is_empty() {
        return Ok(true);
    }
    if matches!(job.operation.as_str(), "submit" | "unpin")
        && super::receipt::job_was_present(db, job).await?
    {
        return Ok(false);
    }
    // A new allocation cannot resurrect an original job ID or recapture its
    // original call. Check the point-scoped job receipt before the resource mask.
    let pk = [("id".into(), job.id.clone())].into();
    for original in historical_keys(db, "pin_jobs", &job.provider, None).await? {
        if let Some(binding) = resolve_legacy_binding(db, "pin_jobs", &pk, &original).await?
            && (binding.source_retired || !matches!(job.operation.as_str(), "poll" | "reconcile"))
        {
            return Ok(false);
        }
    }
    if let Some(binding) = identity_binding(db, &job.provider, &job.cid).await?
        && let Some(fresh) = super::resource_lifetime::fresh(db, &binding, &job.cid).await?
    {
        return super::resource_lifetime::capture_is_fresh(db, &job.id, &fresh).await;
    }
    let resource = resource_binding(db, &job.provider, &job.cid).await?;
    if resource.as_ref().is_some_and(|binding| {
        binding.source_retired
            || binding.historical_route.is_none()
            || matches!(job.operation.as_str(), "submit" | "unpin")
    }) {
        return Ok(false);
    }
    if let Some(binding) = resource {
        let registered =
            crate::store::entities::pin_provider_route::Entity::find_by_id(job.provider.clone())
                .one(db)
                .await?;
        let original = binding.historical_route.as_ref();
        let current = registered.as_ref().and_then(|r| {
            serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(&r.snapshot)
                .ok()
        });
        let compatible = original.zip(current.as_ref()).is_some_and(|(old,current)| {
            old.api_profile == current.api_profile && old.strategy == current.strategy
                && crate::store::entities::pin_lease_requirement::contract::read_route_compatible(old,current)
        });
        if !compatible || registered.is_none_or(|r| r.retired) {
            return Ok(false);
        }
    }
    let pk = [("id".into(), job.id.clone())].into();
    for original in historical_keys(db, "pin_jobs", &job.provider, None).await? {
        if let Some(binding) = resolve_legacy_binding(db, "pin_jobs", &pk, &original).await? {
            return Ok(
                !binding.source_retired && matches!(job.operation.as_str(), "poll" | "reconcile")
            );
        }
    }
    Ok(true)
}

pub async fn release_history<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Vec<pin_resource_history::Model>> {
    if super::receipt::active(db).await?.is_empty() {
        return Ok(pin_resource_history::Entity::find()
            .filter(pin_resource_history::Column::Provider.eq(provider))
            .filter(pin_resource_history::Column::Cid.eq(cid))
            .all(db)
            .await?);
    }
    let operational = resource_key(db, provider, cid).await?;
    let keys = historical_keys(db, "pin_resource_history", &operational, Some(cid)).await?;
    let rows = pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.is_in(keys))
        .filter(pin_resource_history::Column::Cid.eq(cid))
        .all(db)
        .await?;
    let mut result = vec![];
    let mut seen = BTreeSet::new();
    for row in rows {
        let pk = [
            ("provider".into(), row.provider.clone()),
            ("cid".into(), row.cid.clone()),
            ("epoch".into(), row.epoch.to_string()),
        ]
        .into();
        let mapped = resolve_legacy_binding(db, "pin_resource_history", &pk, &row.provider).await?;
        if (row.provider == operational || mapped.is_some_and(|m| m.operational_key == operational))
            && seen.insert((row.provider.clone(), row.cid.clone(), row.epoch))
        {
            result.push(row);
        }
    }
    Ok(result)
}
