use super::*;
use crate::pinning::management::{RetirementSelection, SelectionEvidence};
use capture::key;
use facts::{Dependency, ItemFacts, Key, Outcome, RowRef, Selector};
use rows::RowSet;

fn add(keys: &mut BTreeSet<Key>, key: Key, binding: records::BindingView<'_>) -> Result<()> {
    if !keys.contains(&key) {
        binding.copy(&key)?;
        keys.insert(key);
    }
    Ok(())
}
pub(super) fn keys(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    before: &impl RowSet,
    after: &impl RowSet,
    keys: &mut BTreeSet<Key>,
) -> Result<()> {
    let frame = binding.frozen_rows()?;
    for id in &action.receipt.intent_ids {
        for table in ["pin_leases", "pin_lease_requirements"] {
            add(keys, key(table, id), binding)?;
        }
    }
    for id in &action.targets {
        add(keys, key("pin_lease_targets", id), binding)?;
    }
    let mut pairs = BTreeSet::new();
    for (k, row) in before.iter().chain(after.iter()) {
        if k.table() == "pin_lease_targets"
            && (action
                .receipt
                .intent_ids
                .iter()
                .any(|id| native::value(row, "lease_id") == Some(id))
                || action.targets.iter().any(|id| id == k.value(0)))
        {
            add(keys, k.copy_out(binding)?, binding)?;
            binding.copy(&(cell(row, "provider")?, cell(row, "cid")?))?;
            pairs.insert((
                cell(row, "provider")?.to_owned(),
                cell(row, "cid")?.to_owned(),
            ));
        }
    }
    if let Some(Capture::Object(capture)) = &action.receipt.binding.capture {
        for identity in [&capture.owner_identity, &capture.object.identity] {
            add(keys, key("objects", &identity.object_id), binding)?;
            add(
                keys,
                key("object_versions", &identity.private_version_id),
                binding,
            )?;
        }
        add(
            keys,
            key(
                "pin_extension_decisions",
                &capture.object.identity.private_version_id,
            ),
            binding,
        )?;
        if let Some(target) = &capture.target {
            for table in ["pin_leases", "pin_lease_requirements"] {
                add(keys, key(table, &target.lease.id), binding)?;
            }
            add(keys, key("pin_lease_targets", &target.id), binding)?;
            pairs.insert((
                cell(&target.native_tuple, "provider")?.into(),
                cell(&target.native_tuple, "cid")?.into(),
            ));
        }
        if matches!(
            action.receipt.binding.request.action,
            Action::Create(_) | Action::Backfill(_) | Action::Reapply(_)
        ) {
            let summary = capture
                .object
                .read_evidence
                .as_ref()
                .ok_or(ManagementError::Confirmation)?;
            binding.with_decode_work(|work| {
                accounting::trusted_proof_with_work(frame, summary, work).map(|_| ())
            })?;
            add(keys, attestation(binding, capture)?, binding)?;
            if let SelectionEvidence::TrustedManifest {
                batch_id,
                entry_path,
                ..
            } = &capture.object.evidence
            {
                add(keys, key("zip_batches", batch_id), binding)?;
                let table = frame
                    .table("zip_manifest_entries")
                    .ok_or(ManagementError::Schema)?;
                let found = table
                    .rows
                    .iter()
                    .find(|row| {
                        native::value(row, "batch_id") == Some(batch_id)
                            && native::value(row, "entry_path")
                                .or_else(|| native::value(row, "path"))
                                == Some(entry_path)
                    })
                    .ok_or(ManagementError::Confirmation)?;
                add(
                    keys,
                    Key(
                        table.table.clone(),
                        table
                            .primary_key
                            .iter()
                            .map(|column| cell(found, column).map(str::to_owned))
                            .collect::<Result<_>>()?,
                    ),
                    binding,
                )?;
            }
        }
        for remote in &capture.historical_remotes {
            for (k, _) in before.iter() {
                if k.table() == "pin_resource_history"
                    && k.value(2).parse::<i64>().ok() == Some(remote.epoch)
                    && crate::pinning::provider::canonical_resource_cid(k.value(1))
                        .ok()
                        .as_deref()
                        == Some(&remote.key.cid)
                {
                    add(keys, k.copy_out(binding)?, binding)?;
                }
            }
        }
    }
    // Relevant shared targets and jobs are found from the complete execution
    // membership, never from caller-supplied authority or birth lists.
    for (k, row) in before.iter().chain(after.iter()) {
        let pair = native::value(row, "provider").zip(native::value(row, "cid"));
        if pair.is_some_and(|(provider, cid)| pairs.iter().any(|p| p.0 == provider && p.1 == cid))
            && matches!(
                k.table(),
                "pin_lease_targets"
                    | "pin_jobs"
                    | "remote_pins"
                    | "remote_pin_ledger"
                    | "pin_resource_history"
            )
        {
            add(keys, k.copy_out(binding)?, binding)?;
            if k.table() == "pin_lease_targets" {
                let lease = cell(row, "lease_id")?;
                add(keys, key("pin_leases", lease), binding)?;
                add(keys, key("pin_lease_requirements", lease), binding)?;
            }
            if k.table() == "pin_jobs" {
                add(keys, key("pin_invocation_routes", k.value(0)), binding)?;
                add(keys, key("pin_submit_history", k.value(0)), binding)?;
                if let Some(lease) = native::value(row, "lease_id") {
                    add(keys, key("pin_leases", lease), binding)?;
                    add(keys, key("pin_lease_requirements", lease), binding)?;
                }
            }
        }
    }
    for (provider, cid) in pairs {
        add(keys, key("pin_provider_usage", &provider), binding)?;
        for table in ["remote_pins", "remote_pin_ledger"] {
            add(
                keys,
                Key(table.into(), vec![provider.clone(), cid.clone()]),
                binding,
            )?;
        }
    }
    for job in &action.jobs {
        for table in ["pin_jobs", "pin_invocation_routes", "pin_submit_history"] {
            add(keys, key(table, &job.job_id), binding)?;
        }
    }
    for root in &action.root_releases {
        add(keys, Key(root.table.clone(), root.values.clone()), binding)?;
        add(
            keys,
            Key("zip_root_builds".into(), root.values[..3].to_vec()),
            binding,
        )?;
        add(keys, key("zip_batches", &root.values[0]), binding)?;
    }
    if matches!(
        action.receipt.binding.request.action,
        Action::Retire | Action::RetireWith { .. }
    ) {
        let retirement = retirement_key(binding, &action.receipt.binding)?;
        for route in super::super::retirement::route_keys(binding, &action.receipt.binding)? {
            add(keys, key("pin_provider_routes", &route), binding)?;
        }
        add(keys, retirement, binding)?;
    }
    Ok(())
}

fn attestation(
    binding: records::BindingView<'_>,
    capture: &crate::pinning::management::ObjectCapture,
) -> Result<Key> {
    let frame = binding.frozen_rows()?;
    let summary = capture
        .object
        .read_evidence
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let table = frame
        .table("pin_management_records")
        .ok_or(ManagementError::Schema)?;
    for row in table.rows.iter() {
        if native::value(row, "kind") != Some("evidence_preparation")
            || native::value(row, "item_id") != Some(&summary.identity.private_version_id)
        {
            continue;
        }
        let values = table
            .primary_key
            .iter()
            .map(|column| cell(row, column).map(str::to_owned))
            .collect::<Result<Vec<_>>>()?;
        let refs = values.iter().map(String::as_str).collect::<Vec<_>>();
        let encoded = frame
            .original_value(table.table, &refs, "result")
            .ok_or(ManagementError::Confirmation)?;
        let proof = binding.with_decode_work(|work| {
            crate::pinning::management::evidence::from_persisted_with_work(encoded, None, work)
        });
        if proof
            .as_ref()
            .is_ok_and(|proof| summary.matches_attestation(proof))
        {
            return Ok(Key(table.table.clone(), values));
        }
        if matches!(proof, Err(ManagementError::Budget)) {
            return Err(ManagementError::Budget);
        }
    }
    Err(ManagementError::Confirmation)
}

fn reference(f: &ItemFacts, key: &Key, after: bool) -> Result<RowRef> {
    if after && let Some(i) = f.2.iter().position(|r| &r.0 == key) {
        return Ok(RowRef::Output(i));
    }
    f.1.iter()
        .position(|r| &r.0 == key)
        .map(RowRef::Read)
        .ok_or(ManagementError::Confirmation)
}
pub(super) fn outcome(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    f: &ItemFacts,
    before: &impl RowSet,
    after: &impl RowSet,
    renewal: Option<crate::store::pinning::leases::ManualLeaseRenewalOutcome>,
) -> Result<Outcome> {
    let r = |table: &str, id: &str, after| reference(f, &key(table, id), after);
    let selected = match &action.receipt.binding.capture {
        Some(Capture::Object(capture)) => capture.target.as_ref(),
        _ => None,
    };
    match &action.receipt.binding.request.action {
        Action::Create(_) | Action::Backfill(_) | Action::Reapply(_) => {
            let Some(Capture::Object(capture)) = &action.receipt.binding.capture else {
                return Err(ManagementError::Confirmation);
            };
            let [lease] = action.receipt.intent_ids.as_slice() else {
                return Err(ManagementError::Confirmation);
            };
            let decision = key(
                "pin_extension_decisions",
                &capture.object.identity.private_version_id,
            );
            let frame = binding.frozen_rows()?;
            let decision = if frame.row(&decision.0, &[&decision.1[0]]).is_some() {
                Some(reference(f, &decision, false)?)
            } else {
                None
            };
            let mut origin = Vec::new();
            if let SelectionEvidence::TrustedManifest { batch_id, .. } = &capture.object.evidence {
                origin.push(r("zip_batches", batch_id, false)?);
                origin.push(RowRef::Read(
                    f.1.iter()
                        .position(|r| r.0.0 == "zip_manifest_entries")
                        .ok_or(ManagementError::Confirmation)?,
                ));
            }
            Ok(Outcome::Allocate(
                r("pin_leases", lease, true)?,
                r("pin_lease_requirements", lease, true)?,
                reference(f, &attestation(binding, capture)?, false)?,
                decision,
                origin,
            ))
        }
        Action::Renew { .. } => {
            let id = &selected.ok_or(ManagementError::Confirmation)?.lease.id;
            let mut desired = before
                .iter()
                .filter(|(k, row)| {
                    k.table() == "pin_lease_targets"
                        && native::value(row, "lease_id") == Some(id)
                        && super::desired(row)
                })
                .map(|(k, _)| {
                    f.1.iter()
                        .position(|read| k.matches(&read.0))
                        .map(RowRef::Read)
                        .ok_or(ManagementError::Confirmation)
                })
                .collect::<Result<Vec<_>>>()?;
            desired.sort_by_key(|r| match r {
                RowRef::Read(i) => f.1[*i].0.1.clone(),
                _ => vec![],
            });
            let requirement = before
                .contains_key(&key("pin_lease_requirements", id))
                .then(|| r("pin_lease_requirements", id, false))
                .transpose()?;
            let extended = matches!(
                renewal,
                Some(crate::store::pinning::leases::ManualLeaseRenewalOutcome::Extended { .. })
            );
            Ok(Outcome::Renew(
                r("pin_leases", id, false)?,
                r("pin_leases", id, true)?,
                requirement,
                desired,
                extended,
            ))
        }
        Action::Retry | Action::Reconcile => {
            let target = selected.ok_or(ManagementError::Confirmation)?;
            let provider = cell(&target.native_tuple, "provider")?;
            let cid = cell(&target.native_tuple, "cid")?;
            let Some(Capture::Object(capture)) = &action.receipt.binding.capture else {
                return Err(ManagementError::Confirmation);
            };
            let mut expected = BTreeSet::new();
            expected.insert(Key("remote_pins".into(), vec![provider.into(), cid.into()]));
            for remote in &capture.historical_remotes {
                expected.insert(historical(binding, remote)?);
            }
            let resources = expected
                .iter()
                .map(|key| {
                    Ok((
                        reference(f, key, false)?,
                        if key.0 == "remote_pins" {
                            Some(reference(
                                f,
                                &Key("remote_pin_ledger".into(), key.1.clone()),
                                false,
                            )?)
                        } else {
                            None
                        },
                    ))
                })
                .collect::<Result<_>>()?;
            Ok(Outcome::Recover(
                r("pin_lease_targets", &target.id, false)?,
                r("pin_leases", &target.lease.id, false)?,
                resources,
            ))
        }
        Action::Migrate { .. } => {
            let target = selected.ok_or(ManagementError::Confirmation)?;
            let destination = action
                .migration_points
                .first()
                .map(|p| r("pin_lease_targets", &p.destination_target_id, true))
                .transpose()?;
            Ok(Outcome::Migrate(
                r("pin_lease_targets", &target.id, false)?,
                r("pin_leases", &target.lease.id, false)?,
                r("pin_lease_requirements", &target.lease.id, false)?,
                destination,
            ))
        }
        Action::Cancel => {
            let id = &selected.ok_or(ManagementError::Confirmation)?.lease.id;
            Ok(Outcome::Cancel(
                r("pin_leases", id, false)?,
                r("pin_leases", id, true)?,
            ))
        }
        Action::Retire | Action::RetireWith { .. } => {
            let key = retirement_key(binding, &action.receipt.binding)?;
            let roots = action
                .root_releases
                .iter()
                .map(|k| reference(f, &Key(k.table.clone(), k.values.clone()), true))
                .collect::<Result<_>>()?;
            if !after.contains_key(&key) {
                return Err(ManagementError::Confirmation);
            }
            Ok(Outcome::Retire(reference(f, &key, true)?, roots))
        }
    }
}

pub(super) fn retirement_key(binding: records::BindingView<'_>, entry: &PlanEntry) -> Result<Key> {
    let frame = binding.frozen_rows()?;
    let crate::pinning::management::Subject::Provider { route } = &entry.request.subject else {
        return Err(ManagementError::Confirmation);
    };
    let mut domains = frame
        .table("pin_provider_routes")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
        .filter_map(|row| native::value(row, "snapshot").map(|raw| (row, raw)));
    let mut selected = None;
    for (row, raw) in &mut domains {
        if binding.decode_route(raw)?.as_ref().eq(route
            .original
            .as_ref()
            .ok_or(ManagementError::Confirmation)?)
        {
            if selected.is_some() {
                return Err(ManagementError::Confirmation);
            }
            selected = Some(key("pin_management_retirements", cell(row, "provider")?));
        }
    }
    selected.ok_or(ManagementError::Confirmation)
}
fn historical(
    binding: records::BindingView<'_>,
    remote: &crate::pinning::management::RemoteSnapshot,
) -> Result<Key> {
    let frame = binding.frozen_rows()?;
    let table = frame
        .table("pin_resource_history")
        .ok_or(ManagementError::Schema)?;
    let mut selected = None;
    for row in table.rows.iter() {
        if native::value(row, "epoch").and_then(|v| v.parse::<i64>().ok()) != Some(remote.epoch)
            || crate::pinning::provider::canonical_resource_cid(cell(row, "cid")?)
                .ok()
                .as_deref()
                != Some(&remote.key.cid)
        {
            continue;
        }
        let refs = [
            cell(row, "provider")?,
            cell(row, "cid")?,
            cell(row, "epoch")?,
        ];
        let encoded = frame
            .original_value("pin_resource_history", &refs, "ledger")
            .ok_or(ManagementError::Confirmation)?;
        let envelope: serde_json::Value = binding.decode(encoded)?;
        let route = envelope
            .get("ledger")
            .and_then(|v| v.get("route"))
            .and_then(serde_json::Value::as_str)
            .map(|raw| binding.decode_route(raw))
            .transpose()?;
        if route.as_deref() != remote.route.original.as_ref()
            || envelope
                .get("request_id")
                .and_then(serde_json::Value::as_str)
                != remote.resource_id.as_deref()
        {
            continue;
        }
        if selected.is_some() {
            return Err(ManagementError::Confirmation);
        }
        binding.copy(&refs)?;
        selected = Some(Key(
            "pin_resource_history".into(),
            refs.into_iter().map(str::to_owned).collect(),
        ));
    }
    selected.ok_or(ManagementError::Confirmation)
}

pub(super) fn dependencies(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
) -> Result<Vec<Dependency>> {
    let mut dependencies = Vec::new();
    let frame = binding.frozen_rows()?;
    let mut add = |role: &str, key: &native::NativeKey, selector: Selector| -> Result<()> {
        let trusted =
            records::private_action_from_rows_with_work(frame, key, binding.decode_work())?
                .ok_or(ManagementError::Confirmation)?;
        let identity = trusted.facts_identity()?;
        binding.copy(&(role, &key.table, &key.values, &identity, &selector))?;
        dependencies.push(Dependency(
            role.into(),
            Key(key.table.clone(), key.values.clone()),
            identity,
            selector,
        ));
        Ok(())
    };
    for point in &action.migration_points {
        if let Some(key) = &point.predecessor {
            add(
                "migration_predecessor",
                key,
                Selector::Migration(
                    point.lease_id.clone(),
                    point.cid.clone(),
                    point.slot.clone(),
                ),
            )?;
        }
    }
    if let Action::RetireWith {
        selection: RetirementSelection::Migrate { targets },
    } = &action.receipt.binding.request.action
    {
        for selected in targets {
            let key = native::NativeKey {
                table: "pin_management_records".into(),
                values: vec![
                    binding.database_scope.clone(),
                    selected.plan_id.clone(),
                    selected.item_id.clone(),
                    "action".into(),
                ],
            };
            let trusted =
                records::private_action_from_rows_with_work(frame, &key, binding.decode_work())?
                    .ok_or(ManagementError::Confirmation)?;
            let matches = trusted
                .action_view()
                .migration_points
                .iter()
                .filter(|p| {
                    p.lease_id == selected.lease_id
                        && p.cid == selected.cid
                        && p.source_target_id == selected.source_target_id
                        && selected.destination.original.as_ref() == Some(&p.destination_route)
                })
                .collect::<Vec<_>>();
            let [point] = matches.as_slice() else {
                return Err(ManagementError::Confirmation);
            };
            add(
                "migration_predecessor",
                &key,
                Selector::Migration(
                    point.lease_id.clone(),
                    point.cid.clone(),
                    point.slot.clone(),
                ),
            )?;
        }
    }
    // Exact original Submit references come from indexed job IDs, not by parsing
    // unrelated result bodies. Ordinary native births have no management grant.
    for job in &action.jobs {
        if job.directive != records::JobDirective::Submit
            || frame.row("pin_jobs", &[&job.job_id]).is_none()
        {
            continue;
        }
        if matches!(
            action.receipt.binding.request.action,
            Action::RetireWith {
                selection: RetirementSelection::Migrate { .. }
            }
        ) {
            let (key, id) = retirement::original_job(binding, action, job)?;
            add("original_submit", &key, Selector::Submit([id]))?;
            continue;
        }
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
            let ids: Vec<String> = binding.decode(cell(row, "job_ids")?)?;
            if !ids.contains(&job.job_id) {
                continue;
            }
            let key = native::NativeKey {
                table: "pin_management_records".into(),
                values: ["database_scope", "plan_id", "item_id", "kind"]
                    .iter()
                    .map(|c| cell(row, c).map(str::to_owned))
                    .collect::<Result<_>>()?,
            };
            add(
                "original_submit",
                &key,
                Selector::Submit([job.job_id.clone()]),
            )?;
        }
    }
    dependencies.extend(super::dependencies::reservations(binding, action)?);
    for point in &action.migration_points {
        if let Some((dependency, _)) = super::dependencies::legacy(binding, point)? {
            dependencies.push(dependency);
        }
    }
    if let Action::RetireWith {
        selection: RetirementSelection::Migrate { targets },
    } = &action.receipt.binding.request.action
    {
        for selected in targets {
            let key = native::NativeKey {
                table: "pin_management_records".into(),
                values: vec![
                    binding.database_scope.clone(),
                    selected.plan_id.clone(),
                    selected.item_id.clone(),
                    "action".into(),
                ],
            };
            let trusted =
                records::private_action_from_rows_with_work(frame, &key, binding.decode_work())?
                    .ok_or(ManagementError::Confirmation)?;
            let point = trusted
                .action_view()
                .migration_points
                .iter()
                .find(|p| {
                    p.lease_id == selected.lease_id
                        && p.cid == selected.cid
                        && p.source_target_id == selected.source_target_id
                        && selected.destination.original.as_ref() == Some(&p.destination_route)
                })
                .ok_or(ManagementError::Confirmation)?;
            if let Some((dependency, _)) = super::dependencies::legacy(binding, point)? {
                dependencies.push(dependency);
            }
            if let Some((dependency, _)) = super::dependencies::destination_legacy(binding, point)?
            {
                dependencies.push(dependency);
            }
        }
    }
    dependencies.sort();
    dependencies.dedup();
    Ok(dependencies)
}
