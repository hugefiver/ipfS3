//! Stable typed coordinates, not searches inside management's opaque frames.
use super::{
    Binding, Namespace, TableImage,
    model::{field, key},
    refused,
};
use crate::{
    error::AppResult,
    store::pinning::management::{
        actions::provider_points::{Locator, ProviderPoint, VerifiedRecord},
        native::NativeKey,
    },
};

fn token(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn native_key(key: &NativeKey) -> AppResult<String> {
    Ok(format!(
        "{}/{}",
        token(&key.table),
        token(
            &serde_json::to_string(&key.values)
                .map_err(|_| refused("typed management coordinates cannot be encoded"))?
        )
    ))
}

pub(super) fn pointer(locator: &Locator) -> AppResult<String> {
    Ok(match locator {
        Locator::SnapshotRoute {
            container,
            item,
            field,
        } => format!("/{container}/@item/{}/@route/{}", token(item), token(field)),
        Locator::NativeColumn {
            container,
            key,
            column,
        } => format!(
            "/{container}/@native/{}/{}",
            native_key(key)?,
            token(column)
        ),
        Locator::BirthCandidate {
            container,
            key,
            slot,
        } => format!(
            "/{container}/@native/{}/@candidate/{}",
            native_key(key)?,
            token(slot)
        ),
        Locator::Job { job_id } => format!("/result/@job/{}", token(job_id)),
        Locator::Migration {
            contract,
            slot,
            destination,
        } => format!(
            "/result/@migration/{}/{}/{}",
            native_key(contract)?,
            token(slot),
            if *destination {
                "destination"
            } else {
                "source"
            }
        ),
    })
}

pub(super) fn bindings(
    view: &VerifiedRecord,
    record: &TableImage,
    original: &super::NativeRow,
    ns: &Namespace,
) -> AppResult<Vec<Binding>> {
    let mut result = Vec::<Binding>::new();
    for point in view.points() {
        if point.allocation != ns.source {
            return Err(refused("typed management point differs from namespace"));
        }
        let binding = Binding {
            table: record.table.clone(),
            key: key(record, original),
            provider: ns.source.clone(),
            pointer: Some(pointer(&point.locator)?),
            historical_route: point.original_route.clone(),
        };
        if let Some(existing) = result
            .iter()
            .find(|existing| existing.pointer == binding.pointer)
        {
            if existing != &binding {
                return Err(refused("conflicting typed management point"));
            }
        } else {
            result.push(binding);
        }
    }
    Ok(result)
}

/// Supply the full original route only to a matching live operational point.
/// Historical frame rows alone cannot override today's exact row/epoch binding.
pub(super) fn enrich(
    bindings: &mut [Binding],
    images: &[TableImage],
    views: &[VerifiedRecord],
    ns: &Namespace,
) -> AppResult<()> {
    for binding in bindings
        .iter_mut()
        .filter(|binding| binding.pointer.is_none() && binding.historical_route.is_none())
    {
        let Some(table) = images.iter().find(|table| table.table == binding.table) else {
            continue;
        };
        let Some(row) = table.rows.iter().find(|row| {
            key(table, row) == binding.key && field(row, "provider") == Some(&ns.source)
        }) else {
            continue;
        };
        for view in views {
            for point in view.points() {
                let route = match &point.locator {
                    Locator::Job { job_id }
                        if table.table == "pin_jobs" && field(row, "id") == Some(job_id) =>
                    {
                        point.original_route.as_ref()
                    }
                    Locator::Job { .. }
                        if point.resource_key.as_ref().is_some_and(|original| {
                            original.table == table.table
                                && table
                                    .primary_key
                                    .iter()
                                    .map(|column| field(row, column))
                                    .eq(original.values.iter().map(|value| Some(value.as_str())))
                        }) && epoch_matches(row, point) =>
                    {
                        point.original_route.as_ref()
                    }
                    Locator::Job { .. }
                        if table.table == "pin_lease_targets"
                            && point.job_authority.as_ref().is_some_and(|authority| {
                                field(row, "id") == authority.target_id.as_deref()
                                    && field(row, "lease_id") == authority.lease_id.as_deref()
                                    && field(row, "expected_generation")
                                        .and_then(|value| value.parse::<i64>().ok())
                                        == authority.lease_generation
                                    && field(row, "cid") == Some(authority.cid.as_str())
                            }) =>
                    {
                        point.original_route.as_ref()
                    }
                    Locator::NativeColumn {
                        key: original,
                        column: "provider",
                        ..
                    } if original.table == table.table
                        && table
                            .primary_key
                            .iter()
                            .map(|column| field(row, column))
                            .eq(original.values.iter().map(|value| Some(value.as_str())))
                        && epoch_matches(row, point) =>
                    {
                        point.original_route.as_ref()
                    }
                    Locator::Migration { destination, .. }
                        if table.table == "pin_lease_targets" =>
                    {
                        let Some(migration) = &point.migration_point else {
                            return Err(refused("verified migration point missing"));
                        };
                        let target = if *destination {
                            &migration.destination_target_id
                        } else {
                            &migration.source_target_id
                        };
                        if field(row, "id") != Some(target)
                            || field(row, "lease_id") != Some(&migration.lease_id)
                            || field(row, "cid")
                                .and_then(|cid| {
                                    crate::pinning::provider::canonical_resource_cid(cid).ok()
                                })
                                .as_deref()
                                != Some(&migration.cid)
                        {
                            continue;
                        }
                        Some(if *destination {
                            &migration.destination_route
                        } else {
                            &migration.source_route
                        })
                    }
                    _ => None,
                };
                if let Some(route) = route {
                    if binding
                        .historical_route
                        .as_ref()
                        .is_some_and(|existing| existing != route)
                    {
                        return Err(refused("conflicting original operational management route"));
                    }
                    binding.historical_route = Some(route.clone());
                }
            }
        }
    }
    Ok(())
}

fn epoch_matches(row: &super::NativeRow, point: &ProviderPoint) -> bool {
    point.epoch.is_none_or(|epoch| {
        field(row, "epoch")
            .or_else(|| field(row, "remote_epoch"))
            .and_then(|value| value.parse::<i64>().ok())
            == Some(epoch)
    })
}
