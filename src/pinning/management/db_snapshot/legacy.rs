//! Exact R26 point projections over indexed, complete operation originals.
use super::*;
use crate::store::pinning::{
    legacy_migration::{Binding, Confirmation, Declaration, LegacyBinding},
    management::native::{NativeFrame, NativeRow, value},
};
use std::collections::{BTreeMap, BTreeSet};

fn required<'a>(row: &'a NativeRow, column: &str) -> Result<&'a str> {
    value(row, column).ok_or(SnapshotError::InvalidEvidence)
}

pub(super) fn resolve(
    frame: &NativeFrame,
    table: &str,
    key: &BTreeMap<String, String>,
    provider: &str,
    work: &SharedDecodeBudget,
) -> Result<Option<LegacyBinding>> {
    let operations = &frame
        .table("pin_legacy_migrations")
        .ok_or(SnapshotError::InvalidEvidence)?
        .rows;
    let items = &frame
        .table("pin_legacy_migration_items")
        .ok_or(SnapshotError::InvalidEvidence)?
        .rows;
    let reversed = operations
        .iter()
        .filter_map(|row| value(row, "reverses"))
        .collect::<BTreeSet<_>>();
    let mut found = None;
    for op in operations.iter().filter(|row| {
        value(row, "direction") == Some("apply")
            && value(row, "id").is_some_and(|id| !reversed.contains(id))
    }) {
        let id = required(op, "id")?;
        let members = items
            .iter()
            .filter(|row| value(row, "operation_id") == Some(id))
            .collect::<Vec<_>>();
        if read::field::<i32>(op, "item_count")? < 0
            || members.len() != read::field::<i32>(op, "item_count")? as usize
        {
            return Err(SnapshotError::InvalidEvidence);
        }
        for item in members
            .iter()
            .filter(|row| value(row, "table_kind") == Some(table))
        {
            let item_key =
                decode::<BTreeMap<String, String>>(required(item, "original_key")?, work)?;
            if item_key.as_ref() != key {
                continue;
            }
            let ordinal = required(item, "ordinal")?;
            let bindings = decode::<Vec<Binding>>(
                frame
                    .original_value("pin_legacy_migration_items", &[id, ordinal], "bindings")
                    .ok_or(SnapshotError::InvalidEvidence)?,
                work,
            )?;
            let matching = bindings
                .iter()
                .filter(|binding| {
                    binding.provider == provider && binding.table == table && &binding.key == key
                })
                .collect::<Vec<_>>();
            if matching.is_empty() {
                continue;
            }
            let declaration = decode::<Declaration>(
                frame
                    .original_value("pin_legacy_migrations", &[id], "declaration")
                    .ok_or(SnapshotError::InvalidEvidence)?,
                work,
            )?;
            let confirmation = decode::<Confirmation>(
                frame
                    .original_value("pin_legacy_migrations", &[id], "confirmation")
                    .ok_or(SnapshotError::InvalidEvidence)?,
                work,
            )?;
            if confirmation.operation_id != id
                || declaration.database_scope != required(op, "database_scope")?
                || confirmation.database_scope != declaration.database_scope
                || confirmation.declaration != *declaration
            {
                return Err(SnapshotError::InvalidEvidence);
            }
            for binding in matching {
                let namespaces = declaration
                    .namespaces
                    .iter()
                    .filter(|namespace| {
                        namespace.source == provider && namespace.bindings.contains(binding)
                    })
                    .collect::<Vec<_>>();
                let [namespace] = namespaces.as_slice() else {
                    return Err(SnapshotError::InvalidEvidence);
                };
                let source_retired = confirmation
                    .before
                    .iter()
                    .filter(|image| image.table == "pin_provider_routes")
                    .flat_map(|image| &image.rows)
                    .any(|row| {
                        value(row, "provider") == Some(provider)
                            && matches!(value(row, "retired"), Some("1" | "true"))
                    });
                let cid = key.get("cid").map(String::as_str);
                let remote = confirmation
                    .after
                    .iter()
                    .filter(|image| image.table == "remote_pins")
                    .flat_map(|image| &image.rows)
                    .find(|row| {
                        value(row, "provider") == Some(&namespace.target)
                            && value(row, "cid") == cid
                    });
                let ledger = confirmation
                    .after
                    .iter()
                    .filter(|image| image.table == "remote_pin_ledger")
                    .flat_map(|image| &image.rows)
                    .find(|row| {
                        value(row, "provider") == Some(&namespace.target)
                            && value(row, "cid") == cid
                    });
                let historical_epoch = remote
                    .and_then(|row| value(row, "epoch"))
                    .map(str::parse::<i64>)
                    .transpose()
                    .map_err(|_| SnapshotError::InvalidEvidence)?;
                let release_epochs_at_migration = confirmation
                    .after
                    .iter()
                    .filter(|image| image.table == "pin_resource_history")
                    .flat_map(|image| &image.rows)
                    .filter(|row| {
                        value(row, "cid") == cid
                            && value(row, "provider").is_some_and(|provider| {
                                provider == namespace.source || provider == namespace.target
                            })
                    })
                    .map(|row| read::field(row, "epoch"))
                    .collect::<Result<Vec<i64>>>()?;
                if found.is_some() {
                    return Err(SnapshotError::InvalidEvidence);
                }
                found = Some(LegacyBinding {
                    operational_key: namespace.target.clone(),
                    historical_route: binding.historical_route.clone(),
                    declaration_route: namespace.target_route.clone(),
                    receipt_id: id.into(),
                    source_retired,
                    provenance: "migration_declaration_not_remote_evidence",
                    historical_epoch,
                    closed_at_migration: remote
                        .is_some_and(|row| value(row, "status") == Some("absent"))
                        && ledger.is_none_or(|row| value(row, "effect") == Some("absent")),
                    release_epochs_at_migration,
                });
            }
        }
    }
    Ok(found)
}

pub(super) fn resource(
    frame: &NativeFrame,
    provider: &str,
    cid: &str,
    work: &SharedDecodeBudget,
) -> Result<Option<LegacyBinding>> {
    let mut found = None;
    for table in ["remote_pin_ledger", "remote_pins"] {
        for item in &frame
            .table("pin_legacy_migration_items")
            .ok_or(SnapshotError::InvalidEvidence)?
            .rows
        {
            if value(item, "table_kind") != Some(table) {
                continue;
            }
            let key = decode::<BTreeMap<String, String>>(required(item, "original_key")?, work)?;
            if key.get("cid").map(String::as_str) != Some(cid) {
                continue;
            }
            let Some(original) = key.get("provider") else {
                continue;
            };
            if let Some(binding) = resolve(frame, table, &key, original, work)? {
                if original != provider && binding.operational_key != provider {
                    continue;
                }
                if found.as_ref().is_some_and(|prior: &LegacyBinding| {
                    prior.receipt_id != binding.receipt_id
                        || prior.operational_key != binding.operational_key
                        || prior.historical_route != binding.historical_route
                }) {
                    return Err(SnapshotError::InvalidEvidence);
                }
                found = Some(binding);
            }
        }
    }
    Ok(found)
}
