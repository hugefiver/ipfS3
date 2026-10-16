use super::*;
use facts::{Dependency, Key, Selector};

pub(super) fn reservations(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
) -> Result<Vec<Dependency>> {
    let Some(Capture::Object(capture)) = &action.receipt.binding.capture else {
        return Ok(vec![]);
    };
    let frame = binding.frozen_rows()?;
    let mut dependencies = Vec::new();
    for target in frame
        .table("pin_lease_targets")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
    {
        if crate::pinning::provider::canonical_resource_cid(cell(target, "cid")?)
            .ok()
            .as_deref()
            != Some(&capture.object.identity.cid)
        {
            continue;
        }
        let provider = cell(target, "provider")?;
        let cid = cell(target, "cid")?;
        let lease = cell(target, "lease_id")?;
        let Some(remote) = frame.row("remote_pins", &[provider, cid]) else {
            continue;
        };
        for row in frame
            .table("pin_management_records")
            .ok_or(ManagementError::Schema)?
            .rows
            .iter()
        {
            if native::value(row, "kind") != Some("action")
                || native::value(row, "state") != Some("applied")
            {
                continue;
            }
            let ids = binding.with_decode_work(|work| {
                work.typed_shared::<Vec<String>>(cell(row, "intent_ids")?)
            })?;
            if !ids.iter().any(|id| id == lease) {
                continue;
            }
            let key = native::NativeKey {
                table: "pin_management_records".into(),
                values: ["database_scope", "plan_id", "item_id", "kind"]
                    .iter()
                    .map(|column| cell(row, column).map(str::to_owned))
                    .collect::<Result<_>>()?,
            };
            let trusted =
                records::private_action_from_rows_with_work(frame, &key, binding.decode_work())?
                    .ok_or(ManagementError::Confirmation)?;
            if !matches!(
                trusted.action_view().receipt.binding.request.action,
                Action::Create(_) | Action::Backfill(_) | Action::Reapply(_)
            ) || !trusted
                .action_view()
                .targets
                .iter()
                .any(|id| Some(id.as_str()) == native::value(target, "id"))
            {
                continue;
            }
            let identity = trusted.facts_identity()?;
            let selector = Selector::Reservation(
                cell(target, "id")?.into(),
                provider.into(),
                cid.into(),
                epoch(&remote)?,
            );
            binding.copy(&(&key, &identity, &selector))?;
            dependencies.push(Dependency(
                "reservation_birth".into(),
                Key(key.table, key.values),
                identity,
                selector,
            ));
        }
    }
    Ok(dependencies)
}

/// A frozen legacy mapping verifies its receipt and exact item, not only the
/// declaration's alias. The native inventory supplies all original raw cells.
pub(super) fn legacy(
    binding: records::BindingView<'_>,
    point: &records::MigrationPoint,
) -> Result<Option<(Dependency, String)>> {
    legacy_target(
        binding,
        &point.source_target_id,
        &point.source_allocation,
        &point.source_resource_route,
    )
}
pub(in crate::store::pinning::management::actions) fn destination_legacy(
    binding: records::BindingView<'_>,
    point: &records::MigrationPoint,
) -> Result<Option<(Dependency, String)>> {
    legacy_target(
        binding,
        &point.destination_target_id,
        &point.destination_allocation,
        &point.destination_route,
    )
}
fn legacy_target(
    binding: records::BindingView<'_>,
    target_id: &str,
    provider: &str,
    route: &crate::pinning::identity::ProviderRouteSnapshot,
) -> Result<Option<(Dependency, String)>> {
    use crate::store::pinning::legacy_migration::{Binding, Confirmation, Declaration};
    let frame = binding.frozen_rows()?;
    binding.copy(&[("id", target_id)])?;
    let expected = std::collections::BTreeMap::from([("id".to_owned(), target_id.to_owned())]);
    let mut found = None;
    for row in frame
        .table("pin_legacy_migrations")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
    {
        if native::value(row, "direction") != Some("apply") {
            continue;
        }
        let id = cell(row, "id")?;
        if frame
            .table("pin_legacy_migrations")
            .ok_or(ManagementError::Schema)?
            .rows
            .iter()
            .any(|r| native::value(r, "reverses") == Some(id))
        {
            continue;
        }
        let raw = frame
            .original_value("pin_legacy_migrations", &[id], "confirmation")
            .ok_or(ManagementError::Confirmation)?;
        let confirmation: Confirmation = binding.decode(raw)?;
        let declaration: Declaration = binding.decode(
            frame
                .original_value("pin_legacy_migrations", &[id], "declaration")
                .ok_or(ManagementError::Confirmation)?,
        )?;
        crate::store::pinning::legacy_migration::validate_declaration(&declaration)
            .map_err(|_| ManagementError::Confirmation)?;
        if confirmation.operation_id != id
            || native::value(row, "database_scope") != Some(&confirmation.database_scope)
            || confirmation.declaration != declaration
            || declaration.database_scope != confirmation.database_scope
            || native::value(row, "db_clock") != Some(&confirmation.db_clock)
        {
            return Err(ManagementError::Confirmation);
        }
        let items = frame
            .table("pin_legacy_migration_items")
            .ok_or(ManagementError::Schema)?
            .rows
            .iter()
            .filter(|r| native::value(r, "operation_id") == Some(id))
            .collect::<Vec<_>>();
        let count = cell(row, "item_count")?
            .parse::<usize>()
            .map_err(|_| ManagementError::Confirmation)?;
        if count != items.len()
            || count
                != confirmation
                    .before
                    .iter()
                    .map(|table| table.rows.len().max(1))
                    .sum::<usize>()
        {
            return Err(ManagementError::Confirmation);
        }
        for namespace in &declaration.namespaces {
            if namespace.source != provider {
                continue;
            }
            let bindings = namespace
                .bindings
                .iter()
                .filter(|b| {
                    b.table == "pin_lease_targets"
                        && b.key == expected
                        && b.provider == provider
                        && b.historical_route.as_ref() == Some(route)
                })
                .collect::<Vec<_>>();
            if bindings.is_empty() {
                continue;
            }
            if bindings.len() != 1 || found.is_some() {
                return Err(ManagementError::Confirmation);
            }
            let mut matched = 0;
            for item in &items {
                if native::value(item, "table_kind") != Some("pin_lease_targets") {
                    continue;
                }
                let key: std::collections::BTreeMap<String, String> =
                    binding.decode(cell(item, "original_key")?)?;
                if key != expected {
                    continue;
                }
                matched += 1;
                let item_bindings: Vec<Binding> = binding.decode(
                    frame
                        .original_value(
                            "pin_legacy_migration_items",
                            &[id, cell(item, "ordinal")?],
                            "bindings",
                        )
                        .unwrap_or(cell(item, "bindings")?),
                )?;
                if !item_bindings.contains(bindings[0]) {
                    return Err(ManagementError::Confirmation);
                }
                for (column, tables) in [
                    ("before_image", &confirmation.before),
                    ("after_image", &confirmation.after),
                ] {
                    let raw = frame
                        .original_value(
                            "pin_legacy_migration_items",
                            &[id, cell(item, "ordinal")?],
                            column,
                        )
                        .ok_or(ManagementError::Confirmation)?;
                    let actual: Option<native::NativeRow> = binding.decode(raw)?;
                    let expected = tables
                        .iter()
                        .find(|t| t.table == "pin_lease_targets")
                        .and_then(|t| {
                            t.rows
                                .iter()
                                .find(|r| native::value(r, "id") == Some(target_id))
                        });
                    if actual.as_ref() != expected {
                        return Err(ManagementError::Confirmation);
                    }
                }
            }
            if matched != 1 {
                return Err(ManagementError::Confirmation);
            }
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(b"stage7/legacy-confirmation/1\0");
            h.update(raw.as_bytes());
            binding.copy(&(id, &expected, &namespace.target))?;
            binding.copy(&(target_id, provider))?;
            let dependency = Dependency(
                "legacy_movement".into(),
                Key("pin_legacy_migrations".into(), vec![id.into()]),
                hex::encode(h.finalize()),
                Selector::Legacy(
                    "pin_lease_targets".into(),
                    vec![target_id.into()],
                    provider.into(),
                ),
            );
            binding.copy(&dependency)?;
            found = Some((dependency, namespace.target.clone()));
        }
    }
    Ok(found)
}
