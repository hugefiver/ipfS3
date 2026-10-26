//! Point projections. Original models/payloads remain the historical evidence.
use super::receipt::ReceiptCatalog;
use super::receipt::{historical_keys_with_work, resolve_with_work, work::Work};
use super::{LegacyBinding, resolve_legacy_binding};
use crate::store::pinning::management::{
    self,
    records::{DecodeWork, locator::AuthorityView},
};
use crate::{error::AppResult, store::entities::pin_resource_history};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use std::collections::{BTreeMap, BTreeSet};

pub async fn resource_binding<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Option<LegacyBinding>> {
    resource_binding_current_catalog(ReceiptCatalog::full(db), provider, cid).await
}

pub(crate) async fn resource_binding_current_catalog(
    catalog: ReceiptCatalog<'_>,
    provider: &str,
    cid: &str,
) -> AppResult<Option<LegacyBinding>> {
    let Some(binding) = identity_binding_catalog(catalog, provider, cid, &()).await? else {
        return Ok(None);
    };
    if super::resource_lifetime::fresh(&catalog, &binding, cid)
        .await?
        .is_some()
    {
        return Ok(None);
    }
    Ok(Some(binding))
}
#[cfg(test)]
pub(crate) async fn resource_binding_with_work<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    work: DecodeWork<'_>,
) -> management::Result<Option<LegacyBinding>> {
    resource_binding_catalog(ReceiptCatalog::full(db), provider, cid, work).await
}

pub(crate) async fn resource_binding_catalog(
    catalog: ReceiptCatalog<'_>,
    provider: &str,
    cid: &str,
    work: DecodeWork<'_>,
) -> management::Result<Option<LegacyBinding>> {
    let Some(binding) = identity_binding_catalog(catalog, provider, cid, &work).await? else {
        return Ok(None);
    };
    if super::resource_lifetime::fresh_with_work(&catalog, &binding, cid, &work)
        .await?
        .is_some()
    {
        return Ok(None);
    }
    Ok(Some(binding))
}

async fn identity_binding_catalog<W: Work>(
    catalog: ReceiptCatalog<'_>,
    provider: &str,
    cid: &str,
    work: &W,
) -> Result<Option<LegacyBinding>, W::Error> {
    let db = &catalog;
    let versions = catalog.classify().await.map_err(W::application)?;
    if versions.last().map(String::as_str) == Some(management::schema::VERSION) {
        let session = management::records::SharedDecodeBudget::default();
        let context = work.decode().unwrap_or(DecodeWork::shared(&session));
        let candidates = catalog
            .load(&resource_receipt_selectors(provider, cid), context)
            .await
            .map_err(|error| W::application(management::admission::work_error(error)))?;
        return identity_binding_from_catalog(provider, cid, &candidates, work, context, catalog)
            .await;
    }
    for table in ["remote_pin_ledger", "remote_pins"] {
        for original in historical_keys_with_work(db, table, provider, Some(cid), work).await? {
            work.copy(&[("provider", original.as_str()), ("cid", cid)])?;
            let key = [
                ("provider".into(), original.clone()),
                ("cid".into(), cid.into()),
            ]
            .into();
            if let Some(binding) = resolve_with_work(db, table, &key, &original, work).await? {
                return Ok(Some(binding));
            }
        }
    }
    Ok(None)
}

pub(crate) fn resource_receipt_selectors(
    provider: &str,
    cid: &str,
) -> [management::records::locator::AuthoritySelector; 4] {
    use management::records::locator::AuthoritySelector;
    let resource = |table: &str| {
        AuthoritySelector::LegacyResource(table.into(), provider.into(), Some(cid.into()))
    };
    let point = |table: &str| {
        AuthoritySelector::LegacyPoint(
            table.into(),
            vec![provider.into(), cid.into()],
            provider.into(),
        )
    };
    [
        resource("remote_pin_ledger"),
        point("remote_pin_ledger"),
        resource("remote_pins"),
        point("remote_pins"),
    ]
}

pub(crate) async fn resource_receipt_candidates_with_work<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    work: DecodeWork<'_>,
) -> management::Result<Option<AuthorityView>> {
    let versions = super::admission_schema::receipt_schema(db)
        .await
        .map_err(<DecodeWork<'_> as Work>::application)?;
    if versions.last().map(String::as_str) != Some(management::schema::VERSION) {
        return Ok(None);
    }
    management::admission::authority::load(db, &resource_receipt_selectors(provider, cid), work)
        .await
        .map(Some)
}

pub(in crate::store::pinning) async fn identity_binding_from_catalog<W: Work>(
    provider: &str,
    cid: &str,
    candidates: &AuthorityView,
    work: &W,
    context: DecodeWork<'_>,
    catalog: ReceiptCatalog<'_>,
) -> Result<Option<LegacyBinding>, W::Error> {
    if candidates.originals.is_empty() {
        return Ok(None);
    }
    for table in ["remote_pin_ledger", "remote_pins"] {
        for original in super::receipt::historical_keys_from_catalog(
            table,
            provider,
            Some(cid),
            candidates,
            work,
            context,
            catalog,
        )
        .await?
        {
            work.copy(&[("provider", original.as_str()), ("cid", cid)])?;
            let key = [
                ("provider".into(), original.clone()),
                ("cid".into(), cid.into()),
            ]
            .into();
            if let Some(binding) =
                super::receipt::resolve_with_catalog(table, &key, &original, work, catalog).await?
            {
                return Ok(Some(binding));
            }
        }
    }
    Ok(None)
}

pub(in crate::store::pinning) async fn resource_binding_from_catalog(
    provider: &str,
    cid: &str,
    candidates: &AuthorityView,
    work: DecodeWork<'_>,
    catalog: ReceiptCatalog<'_>,
) -> management::Result<Option<LegacyBinding>> {
    let Some(binding) =
        identity_binding_from_catalog(provider, cid, candidates, &work, work, catalog).await?
    else {
        return Ok(None);
    };
    if super::resource_lifetime::fresh_with_work(&catalog, &binding, cid, &work)
        .await?
        .is_some()
    {
        return Ok(None);
    }
    Ok(Some(binding))
}

pub async fn resource_key<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<String> {
    resource_key_with_work(db, provider, cid, &()).await
}

async fn resource_key_with_work<C: ConnectionTrait, W: Work>(
    db: &C,
    provider: &str,
    cid: &str,
    work: &W,
) -> Result<String, W::Error> {
    resource_key_catalog(ReceiptCatalog::full(db), provider, cid, work).await
}

pub(crate) async fn resource_key_catalog<W: Work>(
    catalog: ReceiptCatalog<'_>,
    provider: &str,
    cid: &str,
    work: &W,
) -> Result<String, W::Error> {
    if let Some(binding) = identity_binding_catalog(catalog, provider, cid, work).await? {
        return Ok(binding.operational_key);
    }
    work.copy(provider)?;
    Ok(provider.into())
}
pub(crate) async fn resource_key_budgeted<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    work: DecodeWork<'_>,
) -> management::Result<String> {
    resource_key_with_work(db, provider, cid, &work).await
}

pub(in crate::store::pinning) async fn resource_key_from_loaded(
    provider: &str,
    cid: &str,
    candidates: &AuthorityView,
    work: DecodeWork<'_>,
    catalog: ReceiptCatalog<'_>,
) -> management::Result<String> {
    let binding =
        identity_binding_from_catalog(provider, cid, candidates, &work, work, catalog).await?;
    if let Some(binding) = binding {
        return Ok(binding.operational_key);
    }
    work.copy(provider)?;
    Ok(provider.to_owned())
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
    original_job_provider_core(db, id, provider, &()).await
}
pub(crate) async fn original_job_provider_with_work<C: ConnectionTrait>(
    db: &C,
    id: &str,
    provider: &str,
    work: DecodeWork<'_>,
) -> management::Result<String> {
    original_job_provider_core(db, id, provider, &work).await
}

pub(in crate::store::pinning) async fn original_job_provider_catalog(
    catalog: ReceiptCatalog<'_>,
    id: &str,
    provider: &str,
    work: DecodeWork<'_>,
) -> management::Result<String> {
    if matches!(catalog, ReceiptCatalog::Full(_)) {
        return original_job_provider_with_work(&catalog, id, provider, work).await;
    }
    work.copy(&[("id", id)])?;
    let selector =
        management::records::locator::AuthoritySelector::LegacyJob(provider.into(), id.into());
    let candidates = catalog.load(&[selector], work).await?;
    let pk = [("id".into(), id.into())].into();
    for original in
        super::receipt::historical_job_keys_from_catalog(provider, id, &candidates, work, catalog)
            .await?
    {
        if super::receipt::resolve_with_catalog("pin_jobs", &pk, &original, &work, catalog)
            .await?
            .is_some()
        {
            return Ok(original);
        }
    }
    work.copy(provider)?;
    Ok(provider.into())
}
async fn original_job_provider_core<C: ConnectionTrait, W: Work>(
    db: &C,
    id: &str,
    provider: &str,
    work: &W,
) -> Result<String, W::Error> {
    work.copy(&[("id", id)])?;
    let pk = [("id".into(), id.into())].into();
    for original in super::historical_job_keys(db, provider, id, work).await? {
        if resolve_with_work(db, "pin_jobs", &pk, &original, work)
            .await?
            .is_some()
        {
            return Ok(original);
        }
    }
    work.copy(provider)?;
    Ok(provider.into())
}

pub async fn job_io_allowed<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
) -> AppResult<bool> {
    let session = management::records::SharedDecodeBudget::default();
    job_io_allowed_with_work(db, job, DecodeWork::shared(&session))
        .await
        .map_err(management::admission::work_error)
}

pub(crate) async fn job_io_allowed_with_work<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    work: DecodeWork<'_>,
) -> management::Result<bool> {
    job_io_allowed_catalog(ReceiptCatalog::full(db), job, work).await
}

pub(crate) async fn job_io_allowed_catalog(
    catalog: ReceiptCatalog<'_>,
    job: &crate::store::entities::pin_job::Model,
    work: DecodeWork<'_>,
) -> management::Result<bool> {
    work.copy(&(&job.id, &job.operation, &job.provider, &job.cid))?;
    let candidates = catalog.load(&job_receipt_selectors(job), work).await?;
    job_io_allowed_from_catalog(job, &candidates, work, catalog).await
}

#[cfg(test)]
pub(crate) async fn job_io_allowed_from_loaded<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    candidates: &AuthorityView,
    work: DecodeWork<'_>,
) -> management::Result<bool> {
    job_io_allowed_from_catalog(job, candidates, work, ReceiptCatalog::full(db)).await
}

pub(in crate::store::pinning) async fn job_io_allowed_from_catalog(
    job: &crate::store::entities::pin_job::Model,
    candidates: &AuthorityView,
    work: DecodeWork<'_>,
    catalog: ReceiptCatalog<'_>,
) -> management::Result<bool> {
    // Only the complete six-root union proves ordinary IO has no receipt restriction.
    if candidates.originals.is_empty() {
        return Ok(true);
    }
    if matches!(job.operation.as_str(), "submit" | "unpin")
        && super::receipt::job_was_present_from_loaded(job, candidates, work, catalog).await?
    {
        return Ok(false);
    }
    // A new allocation cannot resurrect an original job ID or recapture its
    // original call. Check the point-scoped job receipt before the resource mask.
    let pk = [("id".into(), job.id.clone())].into();
    for original in super::receipt::historical_job_keys_from_catalog(
        &job.provider,
        &job.id,
        candidates,
        work,
        catalog,
    )
    .await?
    {
        if let Some(binding) =
            super::receipt::resolve_with_catalog("pin_jobs", &pk, &original, &work, catalog).await?
            && (binding.source_retired || !matches!(job.operation.as_str(), "poll" | "reconcile"))
        {
            return Ok(false);
        }
    }
    if let Some(binding) =
        identity_binding_from_catalog(&job.provider, &job.cid, candidates, &work, work, catalog)
            .await?
        && let Some(fresh) =
            super::resource_lifetime::fresh_with_work(&catalog, &binding, &job.cid, &work).await?
    {
        return super::resource_lifetime::capture_is_fresh_with_work(
            &catalog, &job.id, &fresh, work,
        )
        .await;
    }
    let resource =
        resource_binding_from_catalog(&job.provider, &job.cid, candidates, work, catalog).await?;
    if resource.as_ref().is_some_and(|binding| {
        binding.source_retired
            || binding.historical_route.is_none()
            || matches!(job.operation.as_str(), "submit" | "unpin")
    }) {
        return Ok(false);
    }
    if let Some(binding) = resource {
        let registered = management::records::current_model::<
            crate::store::entities::pin_provider_route::Entity,
            _,
        >(&catalog, &[&job.provider], work)
        .await?;
        let original = binding.historical_route.as_ref();
        let current = registered
            .as_ref()
            .map(|r| {
                work.parse::<crate::pinning::identity::ProviderRouteSnapshot>(
                    &r.snapshot,
                    "invalid registered route",
                )
            })
            .transpose()?;
        let compatible = original.zip(current.as_ref()).is_some_and(|(old,current)| {
            old.api_profile == current.api_profile && old.strategy == current.strategy
                && crate::store::entities::pin_lease_requirement::contract::read_route_compatible(old,current)
        });
        if !compatible || registered.is_none_or(|r| r.retired) {
            return Ok(false);
        }
    }
    let pk = [("id".into(), job.id.clone())].into();
    for original in super::receipt::historical_job_keys_from_catalog(
        &job.provider,
        &job.id,
        candidates,
        work,
        catalog,
    )
    .await?
    {
        if let Some(binding) =
            super::receipt::resolve_with_catalog("pin_jobs", &pk, &original, &work, catalog).await?
        {
            return Ok(
                !binding.source_retired && matches!(job.operation.as_str(), "poll" | "reconcile")
            );
        }
    }
    Ok(true)
}

pub(crate) fn job_receipt_selectors(
    job: &crate::store::entities::pin_job::Model,
) -> [management::records::locator::AuthoritySelector; 6] {
    use management::records::locator::AuthoritySelector;
    [
        AuthoritySelector::LegacyJob(job.provider.clone(), job.id.clone()),
        AuthoritySelector::LegacyPoint(
            "pin_jobs".into(),
            vec![job.id.clone()],
            job.provider.clone(),
        ),
        AuthoritySelector::LegacyPoint(
            "remote_pin_ledger".into(),
            vec![job.provider.clone(), job.cid.clone()],
            job.provider.clone(),
        ),
        AuthoritySelector::LegacyPoint(
            "remote_pins".into(),
            vec![job.provider.clone(), job.cid.clone()],
            job.provider.clone(),
        ),
        AuthoritySelector::LegacyResource(
            "remote_pin_ledger".into(),
            job.provider.clone(),
            Some(job.cid.clone()),
        ),
        AuthoritySelector::LegacyResource(
            "remote_pins".into(),
            job.provider.clone(),
            Some(job.cid.clone()),
        ),
    ]
}

pub async fn release_history<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
) -> AppResult<Vec<pin_resource_history::Model>> {
    release_history_core(db, provider, cid, &()).await
}
pub(crate) async fn release_history_with_work<'a, C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    view: impl Into<DecodeWork<'a>>,
) -> management::Result<Vec<pin_resource_history::Model>> {
    release_history_core(db, provider, cid, &view.into()).await
}
async fn release_history_core<C: ConnectionTrait, W: Work>(
    db: &C,
    provider: &str,
    cid: &str,
    work: &W,
) -> Result<Vec<pin_resource_history::Model>, W::Error> {
    release_history_current_catalog(ReceiptCatalog::full(db), provider, cid, work).await
}

pub(crate) async fn release_history_current_catalog<W: Work>(
    catalog: ReceiptCatalog<'_>,
    provider: &str,
    cid: &str,
    work: &W,
) -> Result<Vec<pin_resource_history::Model>, W::Error> {
    let db = &catalog;
    let operational = resource_key_catalog(catalog, provider, cid, work).await?;
    let keys = super::receipt::historical_keys_catalog(
        catalog,
        "pin_resource_history",
        &operational,
        Some(cid),
        work,
    )
    .await?;
    work.copy(cid)?;
    let rows = pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.is_in(keys))
        .filter(pin_resource_history::Column::Cid.eq(cid))
        .all(db)
        .await
        .map_err(|error| W::application(error.into()))?;
    let mut result = vec![];
    let mut seen = BTreeSet::new();
    for row in rows {
        work.copy(&[
            ("provider", row.provider.as_str()),
            ("cid", row.cid.as_str()),
        ])?;
        work.copy(&("epoch", row.epoch))?;
        let pk = [
            ("provider".into(), row.provider.clone()),
            ("cid".into(), row.cid.clone()),
            ("epoch".into(), row.epoch.to_string()),
        ]
        .into();
        let mapped = super::receipt::resolve_with_catalog(
            "pin_resource_history",
            &pk,
            &row.provider,
            work,
            catalog,
        )
        .await?;
        if row.provider == operational || mapped.is_some_and(|m| m.operational_key == operational) {
            work.copy(&(&row.provider, &row.cid, row.epoch))?;
            if !seen.insert((row.provider.clone(), row.cid.clone(), row.epoch)) {
                continue;
            }
            result.push(row);
        }
    }
    Ok(result)
}

pub(in crate::store::pinning) async fn release_history_catalog(
    catalog: ReceiptCatalog<'_>,
    provider: &str,
    cid: &str,
    work: DecodeWork<'_>,
) -> management::Result<Vec<pin_resource_history::Model>> {
    if matches!(catalog, ReceiptCatalog::Full(_)) {
        return release_history_with_work(&catalog, provider, cid, work).await;
    }
    let resource = catalog
        .load(&resource_receipt_selectors(provider, cid), work)
        .await?;
    let operational = resource_key_from_loaded(provider, cid, &resource, work, catalog).await?;
    let selectors = [
        management::records::locator::AuthoritySelector::LegacyResource(
            "pin_resource_history".into(),
            operational.clone(),
            Some(cid.into()),
        ),
    ];
    let candidates = catalog.load(&selectors, work).await?;
    let keys = super::receipt::historical_keys_from_catalog(
        "pin_resource_history",
        &operational,
        Some(cid),
        &candidates,
        &work,
        work,
        catalog,
    )
    .await?;
    work.copy(cid)?;
    let rows = pin_resource_history::Entity::find()
        .filter(pin_resource_history::Column::Provider.is_in(keys))
        .filter(pin_resource_history::Column::Cid.eq(cid))
        .all(&catalog)
        .await
        .map_err(|error| <DecodeWork<'_> as Work>::application(error.into()))?;
    let mut result = vec![];
    let mut seen = BTreeSet::new();
    for row in rows {
        work.copy(&[
            ("provider", row.provider.as_str()),
            ("cid", row.cid.as_str()),
        ])?;
        work.copy(&("epoch", row.epoch))?;
        let pk = [
            ("provider".into(), row.provider.clone()),
            ("cid".into(), row.cid.clone()),
            ("epoch".into(), row.epoch.to_string()),
        ]
        .into();
        let mapped = super::receipt::resolve_with_catalog(
            "pin_resource_history",
            &pk,
            &row.provider,
            &work,
            catalog,
        )
        .await?;
        if row.provider == operational || mapped.is_some_and(|m| m.operational_key == operational) {
            work.copy(&(&row.provider, &row.cid, row.epoch))?;
            if seen.insert((row.provider.clone(), row.cid.clone(), row.epoch)) {
                result.push(row);
            }
        }
    }
    Ok(result)
}
