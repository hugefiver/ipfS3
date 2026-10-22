//! Current receipt reads use exact locators; validated historical prefixes are read-only.
use super::*;
use crate::store::pinning::management::{admission, native, records};
use records::locator::{AuthoritySelector, AuthorityView};
#[path = "historical.rs"]
mod historical;

pub(crate) async fn admission_historical_keys<C: ConnectionTrait, W: Work>(
    db: &C,
    provider: &str,
    work: &W,
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
        let versions = super::super::admission_schema::receipt_schema(db)
            .await
            .map_err(W::application)?;
        let active = if versions.last().map(String::as_str)
            == Some(crate::store::pinning::management::schema::VERSION)
        {
            let view = match &mut candidates {
                Some(view) => view,
                slot @ None => slot.insert(
                    admission::authority::load(db, &selectors, context)
                        .await
                        .map_err(|e| W::application(admission::work_error(e)))?,
                ),
            };
            active_for_loaded(db, selector, view, work, context).await?
        } else {
            historical::active_with_work(db, &versions, work).await?
        };
        keys.extend(resource_keys_from_active(
            table, provider, None, active, work,
        )?);
    }
    Ok(keys)
}

pub(super) async fn active_for<C: ConnectionTrait, W: Work>(
    db: &C,
    selector: AuthoritySelector,
    work: &W,
) -> Result<Vec<operation::Model>, W::Error> {
    let versions = super::super::admission_schema::receipt_schema(db)
        .await
        .map_err(W::application)?;
    if versions.last().map(String::as_str)
        != Some(crate::store::pinning::management::schema::VERSION)
    {
        return historical::active_with_work(db, &versions, work).await;
    }
    let session = records::SharedDecodeBudget::default();
    let context = work
        .decode()
        .unwrap_or(records::DecodeWork::shared(&session));
    let candidates = admission::authority::load(db, std::slice::from_ref(&selector), context)
        .await
        .map_err(|e| W::application(admission::work_error(e)))?;
    active_for_loaded(db, &selector, &candidates, work, context).await
}

pub(super) async fn active_for_loaded<C: ConnectionTrait, W: Work>(
    db: &C,
    selector: &AuthoritySelector,
    candidates: &AuthorityView,
    work: &W,
    context: records::DecodeWork<'_>,
) -> Result<Vec<operation::Model>, W::Error> {
    let ids = operation_ids(candidates, selector, work)?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    // Candidates supply identities only; this read establishes fresh originals.
    let roots = ids
        .iter()
        .map(|id| AuthoritySelector::LegacyOperation(id.clone()))
        .collect::<Vec<_>>();
    let members = admission::authority::load(db, &roots, context)
        .await
        .map_err(|e| W::application(admission::work_error(e)))?;
    let mut active = Vec::new();
    for id in ids {
        let mut original = None;
        let mut reversed = false;
        let root = AuthoritySelector::LegacyOperation(id.clone());
        // Overlapping roots retain independent projection and completeness checks.
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
            complete_with_work(db, &op, work).await?;
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
    Ok(active)
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
            if ids.insert(id.to_owned()) && ids.len() > 128 {
                return Err(W::application(refused("too many operation roots")));
            }
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
