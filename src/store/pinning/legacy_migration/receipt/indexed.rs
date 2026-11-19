//! Current receipt reads use exact locators; validated historical prefixes are read-only.
use super::*;
use crate::store::pinning::management::{admission, native, records};
use records::locator::{AuthoritySelector, AuthorityView};
#[path = "historical.rs"]
mod historical;

#[cfg(test)]
pub(crate) async fn admission_historical_keys<C: ConnectionTrait, W: Work>(
    db: &C,
    provider: &str,
    work: &W,
) -> Result<Vec<String>, W::Error> {
    admission_historical_keys_with_catalog(provider, work, ReceiptCatalog::full(db)).await
}

async fn admission_historical_keys_with_catalog<W: Work>(
    provider: &str,
    work: &W,
    catalog: ReceiptCatalog<'_>,
) -> Result<Vec<String>, W::Error> {
    let selectors = [
        AuthoritySelector::LegacyResource("pin_submit_observations".into(), provider.into(), None),
        AuthoritySelector::LegacyResource("pin_resource_history".into(), provider.into(), None),
    ];
    let session = records::SharedDecodeBudget::default();
    let context = work
        .decode()
        .unwrap_or(records::DecodeWork::shared(&session));
    let mut candidates: Option<AuthorityView> = None;
    let mut keys = Vec::new();
    for (table, selector) in ["pin_submit_observations", "pin_resource_history"]
        .into_iter()
        .zip(&selectors)
    {
        work.copy(provider)?;
        let versions = catalog.classify().await.map_err(W::application)?;
        let active = if crate::store::migrations::history::has_current_pinning_layout(&versions) {
            let view = match &mut candidates {
                Some(view) => view,
                slot @ None => slot.insert(
                    catalog
                        .load(&selectors, context)
                        .await
                        .map_err(|e| W::application(admission::work_error(e)))?,
                ),
            };
            active_for_loaded_catalog(selector, view, work, context, catalog).await?
        } else {
            historical::active_with_work(&catalog, &versions, work).await?
        };
        keys.extend(resource_keys_from_active(
            table, provider, None, active, work,
        )?);
    }
    Ok(keys)
}

const ADMISSION_RESOURCE_TABLES: [&str; 3] = [
    "pin_submit_observations",
    "remote_pin_ledger",
    "pin_resource_history",
];

pub(in crate::store::pinning) async fn admission_historical_resources_with_catalog<W: Work>(
    provider: &str,
    cids: Option<&[&str]>,
    work: &W,
    catalog: ReceiptCatalog<'_>,
) -> Result<Vec<(String, String, Option<String>)>, W::Error> {
    if cids.is_some_and(|cids| cids.is_empty()) {
        return Ok(Vec::new());
    }
    let versions = catalog.classify().await.map_err(W::application)?;
    if !crate::store::migrations::history::has_current_pinning_layout(&versions) {
        return admission_historical_keys_with_catalog(provider, work, catalog)
            .await
            .map(|keys| {
                keys.into_iter()
                    .filter(|key| key != provider)
                    .flat_map(|key| {
                        [
                            "pin_submit_observations",
                            "remote_pin_ledger",
                            "pin_resource_history",
                            "remote_pins",
                            "pin_replica_observations",
                            "pin_jobs",
                            "pin_lease_targets",
                        ]
                        .map(|table| (table.into(), key.clone(), None))
                    })
                    .collect()
            });
    }

    let cids = cids.map(|cids| cids.iter().copied().collect::<BTreeSet<_>>());
    let requested = match &cids {
        Some(cids) => cids.iter().copied().map(Some).collect::<Vec<_>>(),
        None => vec![None],
    };
    let mut ids = BTreeSet::new();
    let tables: &[&str] = if cids.is_some() {
        &[
            "pin_submit_observations",
            "remote_pin_ledger",
            "pin_resource_history",
            "remote_pins",
            "pin_replica_observations",
            "pin_jobs",
            "pin_lease_targets",
        ]
    } else {
        &ADMISSION_RESOURCE_TABLES
    };
    for batch in requested.chunks(128) {
        let selectors = batch
            .iter()
            .flat_map(|cid| {
                tables
                    .iter()
                    .map(move |table| admission_selector::<W>(table, provider, *cid))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for source in admission_locator_values(selectors, false, work, catalog).await? {
            let key = records::locator::key_from_source(&source)
                .map_err(|error| W::application(admission::work_error(error)))?;
            let id = match (key.table.as_str(), key.values.as_slice()) {
                ("pin_legacy_migrations", [id]) | ("pin_legacy_migration_items", [id, ..]) => id,
                _ => return Err(W::application(refused("invalid admission locator root"))),
            };
            work.copy(id)?;
            ids.insert(id.clone());
        }
    }

    let session = records::SharedDecodeBudget::default();
    let context = work
        .decode()
        .unwrap_or(records::DecodeWork::shared(&session));
    let mut active = Vec::new();
    let mut ids = ids.into_iter().peekable();
    while ids.peek().is_some() {
        active.extend(
            active_for_ids(ids.by_ref().take(128).collect(), work, context, catalog).await?,
        );
    }
    let mut resources = BTreeSet::new();
    for operation in active {
        let declaration: crate::store::pinning::legacy_migration::Declaration =
            work.parse(&operation.declaration, "invalid DB receipt declaration")?;
        let confirmation: crate::store::pinning::legacy_migration::Confirmation =
            work.parse(&operation.confirmation, "invalid DB receipt confirmation")?;
        for namespace in declaration
            .namespaces
            .into_iter()
            .filter(|namespace| namespace.target == provider)
        {
            for binding in namespace
                .bindings
                .into_iter()
                .filter(|binding| binding.provider == namespace.source)
            {
                let cid = binding.key.get("cid").map(String::as_str).or_else(|| {
                    confirmation
                        .before
                        .iter()
                        .filter(|image| image.table == binding.table)
                        .flat_map(|image| &image.rows)
                        .find(|row| {
                            binding.key.iter().all(|(column, value)| {
                                native::value(*row, column) == Some(value.as_str())
                            })
                        })
                        .and_then(|row| {
                            native::value(row, "cid").or_else(|| native::value(row, "expected_cid"))
                        })
                });
                if let Some(cid) = cid
                    && cids
                        .as_ref()
                        .is_none_or(|requested| requested.contains(cid))
                {
                    work.copy(&(binding.table.as_str(), namespace.source.as_str(), cid))?;
                    resources.insert((binding.table, namespace.source.clone(), Some(cid.into())));
                }
            }
        }
    }
    Ok(resources.into_iter().collect())
}

/// Query names for report discovery; the caller still checks observation attribution.
pub(in crate::store::pinning) async fn admission_report_providers_with_catalog<W: Work>(
    provider: &str,
    work: &W,
    catalog: ReceiptCatalog<'_>,
) -> Result<Vec<String>, W::Error> {
    let table = "pin_submit_observations";
    work.copy(provider)?;
    let versions = catalog.classify().await.map_err(W::application)?;
    if !crate::store::migrations::history::has_current_pinning_layout(&versions) {
        let active = historical::active_with_work(&catalog, &versions, work).await?;
        return resource_keys_from_active(table, provider, None, active, work);
    }

    let selector = admission_selector::<W>(table, provider, None)?;
    let mut names = BTreeSet::from([provider.to_owned()]);
    for key in admission_locator_values(vec![selector], true, work, catalog).await? {
        names.insert(work.parse(&key, "invalid admission namespace selector")?);
    }
    Ok(names.into_iter().collect())
}

fn admission_selector<W: Work>(
    table: &str,
    provider: &str,
    cid: Option<&str>,
) -> Result<String, W::Error> {
    serde_json::to_string(&(table, provider, cid))
        .map_err(|_| W::application(refused("invalid admission selector")))
}

async fn admission_locator_values<W: Work>(
    selectors: Vec<String>,
    namespace_names: bool,
    work: &W,
    catalog: ReceiptCatalog<'_>,
) -> Result<Vec<String>, W::Error> {
    work.cells(selectors.len(), selectors.iter().map(String::len).sum())?;
    let slots = (1..=selectors.len())
        .map(|slot| format!("${slot}"))
        .collect::<Vec<_>>()
        .join(",");
    let (value, joins) = if namespace_names {
        (
            "name.selector_key",
            "LEFT JOIN pin_management_locator name ON name.source_key=requested.source_key \
             AND name.selector_kind='legacy_namespace'",
        )
    } else {
        ("requested.source_key", "")
    };
    // A null result marks incomplete membership in the same snapshot as the lookup.
    let rows = catalog
        .query_all(sea_orm::Statement::from_sql_and_values(
            catalog.get_database_backend(),
            format!(
                "SELECT NULL AS locator_value WHERE EXISTS ( \
                     SELECT 1 FROM pin_management_pending_scope \
                     WHERE (selector_kind='unknown' AND selector_key='*') \
                     OR (selector_kind='legacy_resource' AND selector_key IN ({slots})) \
                 ) UNION \
                 SELECT CASE WHEN control.source_generation>=0 AND control.locator_generation>=0 \
                     AND control.source_generation=control.sealed_source \
                     AND control.locator_generation=control.sealed_locator \
                     THEN {value} END AS locator_value \
                 FROM pin_management_locator requested \
                 LEFT JOIN pin_management_source_control control ON control.source_key=requested.source_key \
                 {joins} WHERE requested.selector_kind='legacy_resource' \
                 AND requested.selector_key IN ({slots})"
            ),
            selectors.into_iter().map(sea_orm::Value::from),
        ))
        .await
        .map_err(|error| W::application(error.into()))?;
    let values = rows
        .into_iter()
        .map(|row| row.try_get::<Option<String>>("", "locator_value"))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| W::application(error.into()))?;
    work.cells(values.len(), values.iter().flatten().map(String::len).sum())?;
    values
        .into_iter()
        .map(|value| {
            value.ok_or_else(|| {
                W::application(admission::work_error(
                    crate::store::pinning::management::ManagementError::Changed,
                ))
            })
        })
        .collect()
}

pub(super) async fn active_for<C: ConnectionTrait, W: Work>(
    db: &C,
    selector: AuthoritySelector,
    work: &W,
) -> Result<Vec<operation::Model>, W::Error> {
    active_for_catalog(selector, work, ReceiptCatalog::full(db)).await
}

pub(super) async fn active_for_catalog<W: Work>(
    selector: AuthoritySelector,
    work: &W,
    catalog: ReceiptCatalog<'_>,
) -> Result<Vec<operation::Model>, W::Error> {
    let versions = catalog.classify().await.map_err(W::application)?;
    if !crate::store::migrations::history::has_current_pinning_layout(&versions) {
        return historical::active_with_work(&catalog, &versions, work).await;
    }
    let session = records::SharedDecodeBudget::default();
    let context = work
        .decode()
        .unwrap_or(records::DecodeWork::shared(&session));
    let candidates = catalog
        .load(std::slice::from_ref(&selector), context)
        .await
        .map_err(|e| W::application(admission::work_error(e)))?;
    match catalog {
        ReceiptCatalog::Full(_) => {
            active_for_loaded(&catalog, &selector, &candidates, work, context).await
        }
        _ => active_for_loaded_catalog(&selector, &candidates, work, context, catalog).await,
    }
}

pub(super) async fn active_for_loaded<C: ConnectionTrait, W: Work>(
    db: &C,
    selector: &AuthoritySelector,
    candidates: &AuthorityView,
    work: &W,
    context: records::DecodeWork<'_>,
) -> Result<Vec<operation::Model>, W::Error> {
    active_for_loaded_catalog(
        selector,
        candidates,
        work,
        context,
        ReceiptCatalog::full(db),
    )
    .await
}

pub(in crate::store::pinning) async fn historical_resource_keys_from_loaded_catalog<W: Work>(
    resources: &[(String, String, String)],
    candidates: &AuthorityView,
    work: &W,
    context: records::DecodeWork<'_>,
    catalog: ReceiptCatalog<'_>,
) -> Result<
    (
        BTreeMap<(String, String, String), Vec<String>>,
        BTreeMap<(String, String, String), LegacyBinding>,
    ),
    W::Error,
> {
    let mut ids = BTreeSet::new();
    for (table, provider, cid) in resources {
        ids.extend(operation_ids(
            candidates,
            &AuthoritySelector::LegacyResource(table.clone(), provider.clone(), Some(cid.clone())),
            work,
        )?);
    }
    let (active, members) = load_active_for_ids(ids, work, context, catalog).await?;
    let mut requested = BTreeMap::<(String, String), BTreeSet<String>>::new();
    let mut result = BTreeMap::<(String, String, String), BTreeSet<String>>::new();
    let mut bindings = BTreeMap::new();
    for (table, provider, cid) in resources {
        requested
            .entry((table.clone(), provider.clone()))
            .or_default()
            .insert(cid.clone());
        result.insert(
            (table.clone(), provider.clone(), cid.clone()),
            BTreeSet::from([provider.clone()]),
        );
    }
    let mut metadata = BTreeMap::new();
    for op in &active {
        let declaration: crate::store::pinning::legacy_migration::Declaration =
            work.parse(&op.declaration, "invalid DB receipt declaration")?;
        let confirmation: crate::store::pinning::legacy_migration::Confirmation =
            work.parse(&op.confirmation, "invalid DB receipt images")?;
        for namespace in &declaration.namespaces {
            for binding in &namespace.bindings {
                let domain = (binding.table.clone(), namespace.target.clone());
                let Some(cids) = requested.get(&domain) else {
                    continue;
                };
                if let Some(cid) = binding.key.get("cid") {
                    if cids.contains(cid) {
                        work.copy(&namespace.source)?;
                        result
                            .get_mut(&(domain.0, domain.1, cid.clone()))
                            .ok_or_else(|| W::application(refused("requested resource missing")))?
                            .insert(namespace.source.clone());
                    }
                } else {
                    for cid in cids {
                        work.copy(&namespace.source)?;
                        result
                            .get_mut(&(domain.0.clone(), domain.1.clone(), cid.clone()))
                            .ok_or_else(|| W::application(refused("requested resource missing")))?
                            .insert(namespace.source.clone());
                    }
                }
            }
        }
        metadata.insert(op.id.clone(), (op, declaration, confirmation));
    }
    if let Some(members) = &members {
        for (key, row) in &members.originals {
            if key.table != "pin_legacy_migration_items" {
                continue;
            }
            let value = |column| {
                native::value(row, column)
                    .ok_or_else(|| W::application(refused("incomplete migration item")))
            };
            let Some((op, declaration, confirmation)) = metadata.get(value("operation_id")?) else {
                continue;
            };
            let table = value("table_kind")?;
            let original_key: BTreeMap<String, String> =
                work.parse(value("original_key")?, "invalid original key")?;
            let Some(provider) = original_key.get("provider") else {
                continue;
            };
            let Some(cid) = original_key.get("cid") else {
                continue;
            };
            let item_bindings: Vec<crate::store::pinning::legacy_migration::Binding> =
                work.parse(value("bindings")?, "invalid DB receipt binding")?;
            let Some(item_binding) = item_bindings
                .iter()
                .find(|binding| &binding.provider == provider)
            else {
                continue;
            };
            let namespace = declaration
                .namespaces
                .iter()
                .find(|namespace| &namespace.source == provider)
                .ok_or_else(|| W::application(refused("receipt namespace missing")))?;
            if !namespace.bindings.contains(item_binding) {
                return Err(W::application(refused(
                    "item not covered by database declaration",
                )));
            }
            let resource = (table.to_owned(), namespace.target.clone(), cid.clone());
            if !result.contains_key(&resource) {
                continue;
            }
            let source_retired = confirmation
                .before
                .iter()
                .filter(|image| image.table == "pin_provider_routes")
                .flat_map(|image| &image.rows)
                .any(|row| {
                    field(row, "provider") == Some(provider.as_str())
                        && matches!(field(row, "retired"), Some("1" | "true"))
                });
            let remote = confirmation
                .after
                .iter()
                .filter(|image| image.table == "remote_pins")
                .flat_map(|image| &image.rows)
                .find(|row| {
                    field(row, "provider") == Some(namespace.target.as_str())
                        && field(row, "cid") == Some(cid.as_str())
                });
            let ledger = confirmation
                .after
                .iter()
                .filter(|image| image.table == "remote_pin_ledger")
                .flat_map(|image| &image.rows)
                .find(|row| {
                    field(row, "provider") == Some(namespace.target.as_str())
                        && field(row, "cid") == Some(cid.as_str())
                });
            let historical_epoch = remote
                .and_then(|row| field(row, "epoch"))
                .and_then(|epoch| epoch.parse::<i64>().ok())
                .filter(|epoch| *epoch >= 0);
            let closed_at_migration = remote
                .is_some_and(|row| field(row, "status") == Some("absent"))
                && ledger.is_none_or(|row| field(row, "effect") == Some("absent"));
            let release_epochs_at_migration = confirmation
                .after
                .iter()
                .filter(|image| image.table == "pin_resource_history")
                .flat_map(|image| &image.rows)
                .filter(|row| {
                    field(row, "cid") == Some(cid.as_str())
                        && field(row, "provider").is_some_and(|value| {
                            value == namespace.source || value == namespace.target
                        })
                })
                .filter_map(|row| field(row, "epoch")?.parse::<i64>().ok())
                .collect();
            let binding = LegacyBinding {
                operational_key: namespace.target.clone(),
                historical_route: item_binding.historical_route.clone(),
                declaration_route: namespace.target_route.clone(),
                receipt_id: op.id.clone(),
                source_retired,
                provenance: "migration_declaration_not_remote_evidence",
                historical_epoch,
                closed_at_migration,
                release_epochs_at_migration,
            };
            if bindings.insert(resource, binding).is_some() {
                return Err(W::application(refused("ambiguous active receipt mapping")));
            }
        }
    }
    Ok((
        result
            .into_iter()
            .map(|(resource, providers)| (resource, providers.into_iter().collect()))
            .collect(),
        bindings,
    ))
}

pub(super) async fn active_for_loaded_catalog<W: Work>(
    selector: &AuthoritySelector,
    candidates: &AuthorityView,
    work: &W,
    context: records::DecodeWork<'_>,
    catalog: ReceiptCatalog<'_>,
) -> Result<Vec<operation::Model>, W::Error> {
    let ids = operation_ids(candidates, selector, work)?;
    active_for_ids(ids, work, context, catalog).await
}

async fn active_for_ids<W: Work>(
    ids: BTreeSet<String>,
    work: &W,
    context: records::DecodeWork<'_>,
    catalog: ReceiptCatalog<'_>,
) -> Result<Vec<operation::Model>, W::Error> {
    Ok(load_active_for_ids(ids, work, context, catalog).await?.0)
}

async fn load_active_for_ids<W: Work>(
    ids: BTreeSet<String>,
    work: &W,
    context: records::DecodeWork<'_>,
    catalog: ReceiptCatalog<'_>,
) -> Result<(Vec<operation::Model>, Option<AuthorityView>), W::Error> {
    if ids.is_empty() {
        return Ok((Vec::new(), None));
    }
    let roots = ids
        .iter()
        .map(|id| AuthoritySelector::LegacyOperation(id.clone()))
        .collect::<Vec<_>>();
    let members = catalog
        .load(&roots, context)
        .await
        .map_err(|e| W::application(admission::work_error(e)))?;
    let mut active = Vec::new();
    for id in ids {
        let mut original = None;
        let mut reversed = false;
        let root = AuthoritySelector::LegacyOperation(id.clone());
        for key in source_closure(&members, &root, work)? {
            if key.table != "pin_legacy_migrations" {
                continue;
            }
            let row = &members.originals[key];
            work.copy(row)?;
            let text = |column| {
                native::value(row, column)
                    .map(str::to_owned)
                    .ok_or_else(|| W::application(refused("incomplete operation original")))
            };
            let op = operation::Model {
                id: text("id")?,
                direction: text("direction")?,
                reverses: native::value(row, "reverses").map(str::to_owned),
                database_scope: text("database_scope")?,
                db_clock: text("db_clock")?,
                schema_version: text("schema_version")?,
                declaration: text("declaration")?,
                confirmation: text("confirmation")?,
                item_count: text("item_count")?
                    .parse()
                    .map_err(|_| W::application(refused("invalid item count")))?,
            };
            complete_with_work(&catalog, &op, work).await?;
            if op.id == id {
                if original.replace(op).is_some() {
                    return Err(W::application(refused("duplicate operation")));
                }
            } else if op.reverses.as_deref() == Some(id.as_str()) && op.direction == "rollback" {
                if reversed {
                    return Err(W::application(refused("ambiguous reversal")));
                }
                reversed = true;
            }
        }
        let op = original.ok_or_else(|| W::application(refused("operation original missing")))?;
        if !matches!(op.direction.as_str(), "apply" | "rollback") {
            return Err(W::application(refused("invalid operation direction")));
        }
        if op.direction == "apply" && !reversed {
            active.push(op);
        }
    }
    Ok((active, Some(members)))
}

fn membership_sources<'a, W: Work>(
    candidates: &'a AuthorityView,
    selector: &AuthoritySelector,
    work: &W,
) -> Result<&'a [native::NativeKey], W::Error> {
    for membership in &candidates.proof.memberships {
        work.cells(1, 0)?;
        if &membership.selector == selector {
            return Ok(&membership.sources);
        }
    }
    Err(W::application(refused("authority membership missing")))
}

// Only the requested membership closure supplies candidates from a union view.
fn operation_ids<W: Work>(
    candidates: &AuthorityView,
    selector: &AuthoritySelector,
    work: &W,
) -> Result<BTreeSet<String>, W::Error> {
    let mut ids = BTreeSet::new();
    for key in source_closure(candidates, selector, work)? {
        let column = match key.table.as_str() {
            "pin_legacy_migrations" => Some("id"),
            "pin_legacy_migration_items" => Some("operation_id"),
            _ => None,
        };
        if let Some(column) = column {
            let id = native::value(&candidates.originals[key], column)
                .ok_or_else(|| W::application(refused("missing operation identity")))?;
            work.copy(id)?;
            ids.insert(id.to_owned());
        }
    }
    Ok(ids)
}

fn source_closure<'a, W: Work>(
    candidates: &'a AuthorityView,
    selector: &AuthoritySelector,
    work: &W,
) -> Result<BTreeSet<&'a native::NativeKey>, W::Error> {
    let sources = membership_sources(candidates, selector, work)?;
    work.cells(sources.len(), 0)?;
    let mut pending = sources.iter().collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    let mut visited_memberships = BTreeSet::from([selector]);
    while let Some(key) = pending.pop() {
        work.cells(1, 0)?;
        if !visited.insert(key) {
            continue;
        }
        work.cells(2, 0)?;
        candidates
            .originals
            .get(key)
            .ok_or_else(|| W::application(refused("authority original missing")))?;
        let classification = candidates
            .classifications
            .get(key)
            .ok_or_else(|| W::application(refused("authority classification missing")))?;
        work.cells(classification.dependencies.len(), 0)?;
        pending.extend(classification.dependencies.keys());
        for lookup in &classification.membership_lookups {
            work.cells(1, 0)?;
            if !visited_memberships.insert(lookup) {
                continue;
            }
            let sources = membership_sources(candidates, lookup, work)?;
            work.cells(sources.len(), 0)?;
            pending.extend(sources);
        }
    }
    Ok(visited)
}

#[cfg(test)]
#[path = "indexed_tests.rs"]
mod tests;
