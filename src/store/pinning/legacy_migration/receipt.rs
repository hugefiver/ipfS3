use super::{
    Binding, Confirmation, Receipt, SCHEMA,
    model::{field, index_rows, key},
    refused,
};
use crate::{
    error::AppResult,
    pinning::identity::ProviderRouteSnapshot,
    store::entities::{pin_legacy_migration as operation, pin_legacy_migration_item as item},
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, Set};
use std::collections::{BTreeMap, BTreeSet};
pub(super) mod work;
use work::Work;
mod indexed;
pub(crate) use indexed::admission_historical_keys;
mod maintenance;
use crate::store::pinning::management::records::locator::AuthoritySelector;

pub(super) async fn lookup<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> AppResult<Option<operation::Model>> {
    let row = operation::Entity::find_by_id(id.to_owned()).one(db).await?;
    if let Some(row) = &row {
        complete(db, row).await?;
    }
    Ok(row)
}
pub(super) async fn reverse<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> AppResult<Option<operation::Model>> {
    let row = operation::Entity::find()
        .filter(operation::Column::Reverses.eq(id))
        .one(db)
        .await?;
    if let Some(row) = &row {
        complete(db, row).await?;
    }
    Ok(row)
}
async fn complete<C: ConnectionTrait>(db: &C, row: &operation::Model) -> AppResult<()> {
    complete_with_work(db, row, &()).await
}
async fn complete_with_work<C: ConnectionTrait, W: Work>(
    db: &C,
    row: &operation::Model,
    work: &W,
) -> Result<(), W::Error> {
    work.copy(&row.id)?;
    let count = item::Entity::find()
        .filter(item::Column::OperationId.eq(&row.id))
        .count(db)
        .await
        .map_err(|error| W::application(error.into()))?;
    if count != row.item_count as u64 {
        return Err(W::application(refused("incomplete database receipt")));
    }
    Ok(())
}

async fn budget<C: ConnectionTrait>(db: &C, additional: usize) -> AppResult<()> {
    let bytes = if db.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        "octet_length(confirmation)+octet_length(declaration)"
    } else {
        "length(CAST(confirmation AS BLOB))+length(CAST(declaration AS BLOB))"
    };
    let sql = format!("SELECT COALESCE(SUM({bytes}),0) AS bytes FROM pin_legacy_migrations");
    let bytes: i64 = db
        .query_one(sea_orm::Statement::from_string(
            db.get_database_backend(),
            sql,
        ))
        .await?
        .ok_or_else(|| refused("receipt budget unavailable"))?
        .try_get("", "bytes")?;
    let total = usize::try_from(bytes)
        .ok()
        .and_then(|n| n.checked_add(additional));
    if total.is_none_or(|n| n > super::MAX_BYTES) {
        return Err(refused("complete receipt frontier exceeds byte budget"));
    }
    Ok(())
}
pub(super) fn matches(row: &operation::Model, confirmation: &Confirmation) -> AppResult<()> {
    let original: Confirmation =
        serde_json::from_str(&row.confirmation).map_err(|_| refused("invalid database receipt"))?;
    if &original != confirmation
        || row.id != confirmation.operation_id
        || row.database_scope != confirmation.database_scope
    {
        return Err(refused(
            "client confirmation does not match committed DB receipt",
        ));
    }
    Ok(())
}
pub(super) fn rollback_matches(
    row: &operation::Model,
    confirmation: &Confirmation,
) -> AppResult<()> {
    let original: Confirmation = serde_json::from_str(&row.confirmation)
        .map_err(|_| refused("invalid DB receipt confirmation"))?;
    if confirmation
        .rollback_of
        .as_deref()
        .is_none_or(|id| id != row.id)
    {
        return matches(row, confirmation);
    }
    if original.operation_id != confirmation.operation_id
        || original.database_scope != confirmation.database_scope
        || original.database_identity != confirmation.database_identity
        || original.schema != confirmation.schema
        || original.receipt_frontier != confirmation.receipt_frontier
        || original.before != confirmation.before
        || original.after != confirmation.after
        || original.declaration.namespaces != confirmation.declaration.namespaces
    {
        return Err(refused(
            "rollback proposal does not match original DB receipt",
        ));
    }
    Ok(())
}
pub(super) fn public(row: &operation::Model) -> Receipt {
    Receipt {
        id: row.id.clone(),
        direction: row.direction.clone(),
        reverses: row.reverses.clone(),
        db_clock: row.db_clock.clone(),
        database_scope: row.database_scope.clone(),
        evidence: "committed_database_receipt".into(),
    }
}

pub(super) async fn append(
    db: &sea_orm::DatabaseTransaction,
    confirmation: &Confirmation,
    reverses: Option<&str>,
) -> AppResult<Receipt> {
    let id = if reverses.is_some() {
        uuid::Uuid::new_v4().to_string()
    } else {
        confirmation.operation_id.clone()
    };
    let clock = crate::store::database_clock::database_now(db)
        .await?
        .to_rfc3339();
    let json = |v: &Confirmation| {
        serde_json::to_string(v).map_err(|_| refused("receipt serialization failed"))
    };
    let decl = serde_json::to_string(&confirmation.declaration)
        .map_err(|_| refused("declaration serialization failed"))?;
    let item_count = confirmation
        .before
        .iter()
        .map(|i| i.rows.len().max(1))
        .sum::<usize>();
    let model = operation::Model {
        id: id.clone(),
        direction: if reverses.is_some() {
            "rollback"
        } else {
            "apply"
        }
        .into(),
        reverses: reverses.map(str::to_owned),
        database_scope: confirmation.database_scope.clone(),
        db_clock: clock,
        schema_version: SCHEMA.into(),
        declaration: decl,
        confirmation: json(confirmation)?,
        item_count: i32::try_from(item_count)
            .map_err(|_| refused("receipt item budget exceeded"))?,
    };
    budget(db, model.confirmation.len() + model.declaration.len()).await?;
    let result = public(&model);
    use sea_orm::IntoActiveModel;
    operation::Entity::insert(model.into_active_model())
        .exec(db)
        .await?;
    use crate::store::pinning::management::{admission, native, records};
    let mut sources = BTreeSet::from([native::NativeKey {
        table: "pin_legacy_migrations".into(),
        values: vec![id.clone()],
    }]);
    let mut bindings_by_row: BTreeMap<(&str, &BTreeMap<String, String>), Vec<&Binding>> =
        BTreeMap::new();
    for binding in confirmation
        .declaration
        .namespaces
        .iter()
        .flat_map(|n| &n.bindings)
    {
        bindings_by_row
            .entry((&binding.table, &binding.key))
            .or_default()
            .push(binding);
    }
    let mut ordinal = 0;
    for image in &confirmation.before {
        let after = confirmation
            .after
            .iter()
            .find(|i| i.table == image.table)
            .ok_or_else(|| refused("receipt post-image incomplete"))?;
        let after_rows = index_rows(after);
        // Empty tables have a sentinel item: their absence is explicit evidence.
        let original_rows = image
            .rows
            .iter()
            .map(Some)
            .chain(image.rows.is_empty().then_some(None));
        for row in original_rows {
            let row_key = row.map(|r| key(image, r)).unwrap_or_default();
            let bindings = bindings_by_row
                .get(&(image.table.as_str(), &row_key))
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let before_image =
                serde_json::to_string(&row).map_err(|_| refused("receipt image invalid"))?;
            let mut after_key = row_key.clone();
            if image.table != "pin_provider_routes"
                && matches!(
                    image.table.as_str(),
                    "remote_pins" | "remote_pin_ledger" | "pin_provider_usage"
                )
                && let Some(provider) = after_key.get("provider").cloned()
                && let Some(ns) = confirmation
                    .declaration
                    .namespaces
                    .iter()
                    .find(|n| n.source == provider)
            {
                after_key.insert("provider".into(), ns.target.clone());
            }
            let after_row = after_rows.get(&after_key).copied();
            let after_image =
                serde_json::to_string(&after_row).map_err(|_| refused("receipt image invalid"))?;
            let (before_image, after_image) = if reverses.is_some() {
                (after_image, before_image)
            } else {
                (before_image, after_image)
            };
            item::Entity::insert(item::ActiveModel {
                operation_id: Set(id.clone()),
                ordinal: Set(ordinal),
                table_kind: Set(image.table.clone()),
                original_key: Set(
                    serde_json::to_string(&row_key).map_err(|_| refused("receipt key invalid"))?
                ),
                before_image: Set(before_image),
                after_image: Set(after_image),
                bindings: Set(serde_json::to_string(&bindings)
                    .map_err(|_| refused("receipt bindings invalid"))?),
            })
            .exec(db)
            .await?;
            sources.insert(native::NativeKey {
                table: "pin_legacy_migration_items".into(),
                values: vec![id.clone(), ordinal.to_string()],
            });
            ordinal += 1;
        }
    }
    let (before, after) = if reverses.is_some() {
        (&confirmation.after, &confirmation.before)
    } else {
        (&confirmation.before, &confirmation.after)
    };
    if let Some(after) = after
        .iter()
        .find(|image| image.table == "pin_provider_routes")
    {
        let before = before
            .iter()
            .find(|image| image.table == after.table)
            .ok_or_else(|| refused("route before image missing"))?;
        for row in after.rows.iter().filter(|row| !before.rows.contains(row)) {
            sources.insert(native::NativeKey {
                table: "pin_provider_routes".into(),
                values: vec![
                    field(row, "provider")
                        .ok_or_else(|| refused("route key missing"))?
                        .into(),
                ],
            });
        }
    }
    if super::management_closure::enabled(&confirmation.schema) {
        let session = records::SharedDecodeBudget::default();
        admission::authority::rebuild_sources(db, &sources, records::DecodeWork::shared(&session))
            .await
            .map_err(admission::work_error)?;
    }
    Ok(result)
}

#[derive(Clone, Debug)]
pub struct LegacyBinding {
    pub operational_key: String,
    pub historical_route: Option<ProviderRouteSnapshot>,
    pub declaration_route: ProviderRouteSnapshot,
    pub receipt_id: String,
    pub source_retired: bool,
    pub provenance: &'static str,
    pub historical_epoch: Option<i64>,
    pub closed_at_migration: bool,
    pub release_epochs_at_migration: Vec<i64>,
}

/// Only an original table/PK/provider item in a committed, unreversed receipt resolves.
pub async fn resolve_legacy_binding<C: ConnectionTrait>(
    db: &C,
    table: &str,
    original_key: &BTreeMap<String, String>,
    provider: &str,
) -> AppResult<Option<LegacyBinding>> {
    resolve_with_work(db, table, original_key, provider, &()).await
}

pub(crate) async fn resolve_legacy_binding_with_work<'a, C: ConnectionTrait>(
    db: &C,
    table: &str,
    original_key: &BTreeMap<String, String>,
    provider: &str,
    work: impl Into<crate::store::pinning::management::records::DecodeWork<'a>>,
) -> crate::store::pinning::management::Result<Option<LegacyBinding>> {
    resolve_with_work(db, table, original_key, provider, &work.into()).await
}

pub(super) async fn resolve_with_work<C: ConnectionTrait, W: Work>(
    db: &C,
    table: &str,
    original_key: &BTreeMap<String, String>,
    provider: &str,
    work: &W,
) -> Result<Option<LegacyBinding>, W::Error> {
    let mut found = None;
    work.copy(original_key)?;
    let encoded = serde_json::to_string(original_key)
        .map_err(|_| W::application(refused("invalid original row identity")))?;
    let layout = crate::store::pinning::management::native::current_catalog()
        .iter()
        .find(|layout| layout.table == table)
        .ok_or_else(|| W::application(refused("original table unavailable")))?;
    let values = layout
        .primary_key
        .iter()
        .map(|column| {
            original_key
                .get(column)
                .cloned()
                .ok_or_else(|| W::application(refused("original key incomplete")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.len() != original_key.len() {
        return Err(W::application(refused("original key invalid")));
    }
    for op in indexed::active_for(
        db,
        AuthoritySelector::LegacyPoint(table.into(), values, provider.into()),
        work,
    )
    .await?
    {
        work.copy(&(&op.id, table, &encoded))?;
        let rows = item::Entity::find()
            .filter(item::Column::OperationId.eq(&op.id))
            .filter(item::Column::TableKind.eq(table))
            .filter(item::Column::OriginalKey.eq(&encoded))
            .all(db)
            .await
            .map_err(|error| W::application(error.into()))?;
        for row in rows {
            let bindings: Vec<Binding> = work.parse(&row.bindings, "invalid DB receipt binding")?;
            let Some(binding) = bindings.iter().find(|b| b.provider == provider) else {
                continue;
            };
            let decl: super::Declaration =
                work.parse(&op.declaration, "invalid DB receipt declaration")?;
            let ns = decl
                .namespaces
                .iter()
                .find(|n| n.source == provider)
                .ok_or_else(|| W::application(refused("receipt namespace missing")))?;
            if !ns.bindings.contains(binding) {
                return Err(W::application(refused(
                    "item not covered by database declaration",
                )));
            }
            let confirmation: Confirmation =
                work.parse(&op.confirmation, "invalid DB receipt images")?;
            let source_retired = confirmation
                .before
                .iter()
                .filter(|i| i.table == "pin_provider_routes")
                .flat_map(|i| &i.rows)
                .any(|r| {
                    field(r, "provider") == Some(provider)
                        && matches!(field(r, "retired"), Some("1" | "true"))
                });
            let cid = original_key.get("cid");
            let remote = cid.and_then(|cid| {
                confirmation
                    .after
                    .iter()
                    .filter(|i| i.table == "remote_pins")
                    .flat_map(|i| &i.rows)
                    .find(|r| {
                        field(r, "provider") == Some(ns.target.as_str())
                            && field(r, "cid") == Some(cid.as_str())
                    })
            });
            let ledger = cid.and_then(|cid| {
                confirmation
                    .after
                    .iter()
                    .filter(|i| i.table == "remote_pin_ledger")
                    .flat_map(|i| &i.rows)
                    .find(|r| {
                        field(r, "provider") == Some(ns.target.as_str())
                            && field(r, "cid") == Some(cid.as_str())
                    })
            });
            let historical_epoch = if let Some(epoch) = remote.and_then(|r| field(r, "epoch")) {
                work.copy(epoch)?;
                epoch.parse::<i64>().ok().filter(|epoch| *epoch >= 0)
            } else {
                None
            };
            let closed_at_migration = remote.is_some_and(|r| field(r, "status") == Some("absent"))
                && ledger.is_none_or(|r| field(r, "effect") == Some("absent"));
            let mut release_epochs_at_migration = Vec::new();
            for epoch in confirmation
                .after
                .iter()
                .filter(|i| i.table == "pin_resource_history")
                .flat_map(|i| &i.rows)
                .filter(|r| {
                    field(r, "cid") == cid.map(String::as_str)
                        && field(r, "provider").is_some_and(|p| p == ns.source || p == ns.target)
                })
                .filter_map(|r| field(r, "epoch"))
            {
                work.copy(epoch)?;
                let Ok(epoch) = epoch.parse::<i64>() else {
                    continue;
                };
                work.copy(&epoch)?;
                release_epochs_at_migration.push(epoch);
            }
            work.copy(&(
                &ns.target,
                &binding.historical_route,
                &ns.target_route,
                &op.id,
            ))?;
            let value = LegacyBinding {
                operational_key: ns.target.clone(),
                historical_route: binding.historical_route.clone(),
                declaration_route: ns.target_route.clone(),
                receipt_id: op.id.clone(),
                source_retired,
                provenance: "migration_declaration_not_remote_evidence",
                historical_epoch,
                closed_at_migration,
                release_epochs_at_migration,
            };
            if found.is_some() {
                return Err(W::application(refused("ambiguous active receipt mapping")));
            }
            found = Some(value);
        }
    }
    Ok(found)
}

pub async fn source_is_migrated<C: ConnectionTrait>(db: &C, provider: &str) -> AppResult<bool> {
    for op in
        indexed::active_for(db, AuthoritySelector::LegacyNamespace(provider.into()), &()).await?
    {
        let decl: super::Declaration = serde_json::from_str(&op.declaration)
            .map_err(|_| refused("invalid DB receipt declaration"))?;
        if decl
            .namespaces
            .iter()
            .any(|n| n.source == provider && n.source != n.target)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) async fn source_is_migrated_with_work<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> crate::store::pinning::management::Result<bool> {
    for op in indexed::active_for(
        db,
        AuthoritySelector::LegacyNamespace(provider.into()),
        &work,
    )
    .await?
    {
        let decl: super::Declaration =
            work.parse(&op.declaration, "invalid DB receipt declaration")?;
        if decl
            .namespaces
            .iter()
            .any(|n| n.source == provider && n.source != n.target)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn source_has_receipt<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    versions: &[String],
) -> AppResult<bool> {
    if !super::management_closure::enabled(versions) {
        return maintenance::source_has_receipt(db, provider, versions).await;
    }
    for op in
        indexed::active_for(db, AuthoritySelector::LegacyNamespace(provider.into()), &()).await?
    {
        let declaration: super::Declaration = serde_json::from_str(&op.declaration)
            .map_err(|_| refused("invalid DB receipt declaration"))?;
        if declaration
            .namespaces
            .iter()
            .any(|ns| ns.source == provider)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn job_was_present_from_loaded<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    candidates: &crate::store::pinning::management::records::locator::AuthorityView,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> crate::store::pinning::management::Result<bool> {
    for operation in indexed::active_for_loaded(
        db,
        &AuthoritySelector::LegacyJob(job.provider.clone(), job.id.clone()),
        candidates,
        &work,
        work,
    )
    .await?
    {
        let confirmation: Confirmation =
            work.parse(&operation.confirmation, "invalid DB receipt images")?;
        for ns in &confirmation.declaration.namespaces {
            if ns.target != job.provider
                || !ns.bindings.iter().any(|b| {
                    matches!(b.table.as_str(), "remote_pins" | "remote_pin_ledger")
                        && b.key.get("cid") == Some(&job.cid)
                })
            {
                continue;
            }
            if confirmation
                .before
                .iter()
                .filter(|i| i.table == "pin_jobs")
                .flat_map(|i| &i.rows)
                .any(|r| {
                    field(r, "id") == Some(job.id.as_str())
                        && field(r, "cid") == Some(job.cid.as_str())
                        && field(r, "provider").is_some_and(|p| p == ns.source || p == ns.target)
                })
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Query candidates only. Callers retaining immutable facts must subsequently
/// check the original PK with resolve_legacy_binding before projecting identity.
pub async fn historical_keys<C: ConnectionTrait>(
    db: &C,
    table: &str,
    provider: &str,
    cid: Option<&str>,
) -> AppResult<Vec<String>> {
    historical_keys_with_work(db, table, provider, cid, &()).await
}
pub(super) async fn historical_keys_with_work<C: ConnectionTrait, W: Work>(
    db: &C,
    table: &str,
    provider: &str,
    cid: Option<&str>,
    work: &W,
) -> Result<Vec<String>, W::Error> {
    work.copy(provider)?;
    let active = indexed::active_for(
        db,
        AuthoritySelector::LegacyResource(table.into(), provider.into(), cid.map(str::to_owned)),
        work,
    )
    .await?;
    resource_keys_from_active(table, provider, cid, active, work)
}

pub(super) async fn historical_keys_from_loaded<C: ConnectionTrait, W: Work>(
    db: &C,
    table: &str,
    provider: &str,
    cid: Option<&str>,
    candidates: &crate::store::pinning::management::records::locator::AuthorityView,
    work: &W,
    context: crate::store::pinning::management::records::DecodeWork<'_>,
) -> Result<Vec<String>, W::Error> {
    work.copy(provider)?;
    let active = indexed::active_for_loaded(
        db,
        &AuthoritySelector::LegacyResource(table.into(), provider.into(), cid.map(str::to_owned)),
        candidates,
        work,
        context,
    )
    .await?;
    resource_keys_from_active(table, provider, cid, active, work)
}

fn resource_keys_from_active<W: Work>(
    table: &str,
    provider: &str,
    cid: Option<&str>,
    active: Vec<operation::Model>,
    work: &W,
) -> Result<Vec<String>, W::Error> {
    let mut result: BTreeSet<String> = [provider.into()].into();
    for op in active {
        let decl: super::Declaration =
            work.parse(&op.declaration, "invalid DB receipt declaration")?;
        for ns in &decl.namespaces {
            if ns.target == provider
                && ns.bindings.iter().any(|b| {
                    b.table == table
                        && cid.is_none_or(|cid| b.key.get("cid").is_none_or(|c| c == cid))
                })
            {
                work.copy(&ns.source)?;
                result.insert(ns.source.clone());
            }
        }
    }
    Ok(result.into_iter().collect())
}

pub(crate) async fn historical_job_keys<C: ConnectionTrait, W: Work>(
    db: &C,
    provider: &str,
    id: &str,
    work: &W,
) -> Result<Vec<String>, W::Error> {
    work.copy(&(provider, id))?;
    let active = indexed::active_for(
        db,
        AuthoritySelector::LegacyJob(provider.into(), id.into()),
        work,
    )
    .await?;
    job_keys_from_active(provider, active, work)
}

pub(crate) async fn historical_job_keys_from_loaded<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    id: &str,
    candidates: &crate::store::pinning::management::records::locator::AuthorityView,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> crate::store::pinning::management::Result<Vec<String>> {
    work.copy(&(provider, id))?;
    let active = indexed::active_for_loaded(
        db,
        &AuthoritySelector::LegacyJob(provider.into(), id.into()),
        candidates,
        &work,
        work,
    )
    .await?;
    job_keys_from_active(provider, active, &work)
}

fn job_keys_from_active<W: Work>(
    provider: &str,
    active: Vec<operation::Model>,
    work: &W,
) -> Result<Vec<String>, W::Error> {
    let mut result = BTreeSet::from([provider.to_owned()]);
    for op in active {
        let declaration: super::Declaration =
            work.parse(&op.declaration, "invalid DB receipt declaration")?;
        for ns in declaration.namespaces {
            if ns.target == provider {
                work.copy(&ns.source)?;
                result.insert(ns.source);
            }
        }
    }
    Ok(result.into_iter().collect())
}
