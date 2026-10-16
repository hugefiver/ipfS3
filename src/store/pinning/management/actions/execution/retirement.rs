//! Retirement selections are reproduced against their own frozen registry.
use super::*;
use crate::pinning::management::{RetirementSelection, Subject};

pub(super) fn validate(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
) -> Result<()> {
    let Action::RetireWith {
        selection: RetirementSelection::Migrate { targets },
    } = &action.receipt.binding.request.action
    else {
        return Ok(());
    };
    let Subject::Provider { .. } = &action.receipt.binding.request.subject else {
        return Err(ManagementError::Confirmation);
    };
    let before = binding.frozen_rows()?;
    let retirement = closure::retirement_key(binding, &action.receipt.binding)?;
    let domain = &retirement.1[0];
    let mut active = BTreeSet::new();
    for target in before
        .table("pin_lease_targets")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
    {
        if target.cell("provider") != Some(domain) {
            continue;
        }
        let lease = before
            .row("pin_leases", &[cell(target, "lease_id")?])
            .ok_or(ManagementError::Confirmation)?;
        if lease.cell("state") == Some("active") {
            binding.copy(&cell(target, "id")?)?;
            active.insert(cell(target, "id")?);
        }
    }
    binding.with_decode_work(|work| work.nodes(2 + targets.len()))?;
    let mut selected = BTreeSet::new();
    let mut destinations = BTreeSet::new();
    for selection in targets {
        if !selected.insert(selection.source_target_id.as_str())
            || !active.contains(selection.source_target_id.as_str())
        {
            return Err(ManagementError::Confirmation);
        }
        binding.copy(&(
            binding.database_scope,
            &selection.plan_id,
            &selection.item_id,
            "action",
        ))?;
        let key = native::NativeKey {
            table: "pin_management_records".into(),
            values: vec![
                binding.database_scope.clone(),
                selection.plan_id.clone(),
                selection.item_id.clone(),
                "action".into(),
            ],
        };
        let trusted =
            records::private_action_from_rows_with_work(before, &key, binding.decode_work())?
                .ok_or(ManagementError::Confirmation)?;
        let mut matching = trusted
            .action_view()
            .migration_points
            .iter()
            .filter(|point| {
                point.lease_id == selection.lease_id
                    && point.source_target_id == selection.source_target_id
                    && point.cid == selection.cid
                    && selection.destination.original.as_ref() == Some(&point.destination_route)
            });
        let point = matching.next().ok_or(ManagementError::Confirmation)?;
        if matching.next().is_some() {
            return Err(ManagementError::Confirmation);
        }
        trusted.validate_point(point)?;
        let source = dependencies::legacy(binding, point)?.map(|(_, allocation)| allocation);
        if source.as_deref().unwrap_or(&point.source_allocation) != domain {
            return Err(ManagementError::Confirmation);
        }
        let source_target = before
            .row("pin_lease_targets", &[&point.source_target_id])
            .ok_or(ManagementError::Confirmation)?;
        if source_target.cell("lease_id") != Some(&point.lease_id)
            || crate::pinning::provider::canonical_resource_cid(cell(source_target, "cid")?)
                .ok()
                .as_deref()
                != Some(&point.cid)
        {
            return Err(ManagementError::Confirmation);
        }
        let tip = points::frozen_tip(binding, &point.lease_id, &point.cid, &point.slot)?
            .ok_or(ManagementError::Confirmation)?;
        if tip.0 != key || tip.1 != *point {
            return Err(ManagementError::Confirmation);
        }
        points::validate_frozen_current(binding, point)?;
        binding.copy(&point.destination_target_id)?;
        destinations.insert(point.destination_target_id.clone());
    }
    if selected != active
        || !destinations.iter().eq(action.targets.iter())
        || !action.migration_points.is_empty()
        || !action.receipt.intent_ids.is_empty()
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}

pub(in crate::store::pinning::management::actions) fn validate_job(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    authority: &records::JobAuthority,
) -> Result<()> {
    original_job(binding, action, authority).map(|_| ())
}

pub(super) fn original_job(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    authority: &records::JobAuthority,
) -> Result<(native::NativeKey, String)> {
    let Action::RetireWith {
        selection: RetirementSelection::Migrate { targets },
    } = &action.receipt.binding.request.action
    else {
        return Err(ManagementError::Confirmation);
    };
    if authority.directive != records::JobDirective::Submit {
        return Err(ManagementError::Confirmation);
    }
    let before = binding.frozen_rows()?;
    let lease = authority
        .lease_id
        .as_deref()
        .ok_or(ManagementError::Confirmation)?;
    let target = before
        .row(
            "pin_lease_targets",
            &[authority
                .target_id
                .as_deref()
                .ok_or(ManagementError::Confirmation)?],
        )
        .ok_or(ManagementError::Confirmation)?;
    if !super::desired(&target)
        || target.cell("lease_id") != Some(lease)
        || target.cell("provider") != Some(&authority.provider)
        || target.cell("cid") != Some(&authority.cid)
        || authority.resource_key.table != "remote_pins"
        || authority
            .resource_key
            .values
            .iter()
            .map(String::as_str)
            .ne([authority.provider.as_str(), authority.cid.as_str()])
        || !action.targets.iter().any(|id| {
            before
                .row("pin_lease_targets", &[id])
                .is_some_and(|destination| {
                    destination.cell("provider") == target.cell("provider")
                        && destination.cell("cid") == target.cell("cid")
                        && targets.iter().any(|selection| {
                            destination.cell("lease_id") == Some(&selection.lease_id)
                                && selection.cid
                                    == authority
                                        .source
                                        .as_ref()
                                        .map(|source| source.cid.as_str())
                                        .unwrap_or("")
                                && selection.destination.original.as_ref() == Some(&authority.route)
                        })
                })
        })
    {
        return Err(ManagementError::Confirmation);
    }
    for trusted in records::frozen_actions_for_lease(before, lease, binding.decode_work())? {
        for seed in &trusted.action_view().jobs {
            if seed.directive != records::JobDirective::Submit {
                continue;
            }
            binding.copy(&(seed, &authority.job_id))?;
            let mut candidate = seed.clone();
            candidate.job_id.clone_from(&authority.job_id);
            if candidate != *authority {
                continue;
            }
            trusted.validate_job(seed)?;
            if seed.job_id != authority.job_id && !poll_from_rows(binding, seed, authority)? {
                continue;
            }
            binding.copy(&(trusted.record_key(), &seed.job_id))?;
            return Ok((trusted.record_key().clone(), seed.job_id.clone()));
        }
    }
    Err(ManagementError::Confirmation)
}

fn poll_from_rows(
    binding: records::BindingView<'_>,
    seed: &records::JobAuthority,
    authority: &records::JobAuthority,
) -> Result<bool> {
    use crate::store::pinning::jobs;
    let rows = binding.frozen_rows()?;
    let poll = rows
        .row("pin_jobs", &[&authority.job_id])
        .ok_or(ManagementError::Confirmation)?;
    let original = rows
        .row("pin_jobs", &[&seed.job_id])
        .ok_or(ManagementError::Confirmation)?;
    if cell(original, "operation")? != "submit" || cell(poll, "operation")? != "poll" {
        return Ok(false);
    }
    if original.cell("provider") != Some(&seed.provider)
        || original.cell("cid") != Some(&seed.cid)
        || original.cell("lease_id") != seed.lease_id.as_deref()
        || original.cell("target_id") != seed.target_id.as_deref()
        || original
            .cell("expected_generation")
            .and_then(|s| s.parse().ok())
            != seed.lease_generation
    {
        return Ok(false);
    }
    let history = rows
        .row("pin_submit_history", &[&seed.job_id])
        .ok_or(ManagementError::Confirmation)?;
    let Some((api, strategy)) = crate::pinning::worker::invocation_route_for_snapshot(&seed.route)
    else {
        return Ok(false);
    };
    if cell(history, "submit_calls")?
        .parse::<i32>()
        .ok()
        .is_none_or(|calls| calls <= 0)
        || cell(history, "api")? != api
        || cell(history, "strategy")? != strategy
    {
        return Ok(false);
    }
    let remote = rows
        .row("remote_pins", &[&seed.provider, &seed.cid])
        .ok_or(ManagementError::Confirmation)?;
    let current_epoch = cell(remote, "epoch")?
        .parse::<i64>()
        .map_err(|_| ManagementError::Confirmation)?;
    let mut prior = seed.remote_epoch;
    for id in [&seed.job_id, &authority.job_id] {
        let invocation = rows
            .row("pin_invocation_routes", &[id])
            .ok_or(ManagementError::Confirmation)?;
        let epoch = cell(invocation, "remote_epoch")?
            .parse::<i64>()
            .map_err(|_| ManagementError::Confirmation)?;
        if binding.decode_route(cell(invocation, "route")?)?.as_ref() != &seed.route
            || epoch < prior
            || epoch > current_epoch
        {
            return Ok(false);
        }
        prior = epoch;
    }
    let ledger = rows
        .row("remote_pin_ledger", &[&seed.provider, &seed.cid])
        .ok_or(ManagementError::Confirmation)?;
    if binding.decode_route(cell(ledger, "route")?)?.as_ref() != &seed.route {
        return Ok(false);
    }
    for history in rows
        .table("pin_resource_history")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
    {
        if history.cell("provider") == Some(&seed.provider)
            && history.cell("cid") == Some(&seed.cid)
        {
            let epoch = cell(history, "epoch")?
                .parse::<i64>()
                .map_err(|_| ManagementError::Confirmation)?;
            if epoch >= seed.remote_epoch && epoch <= current_epoch {
                return Ok(false);
            }
        }
    }
    let Some(request) = remote.cell("request_id") else {
        return Ok(false);
    };
    let (Some(lease), Some(target), Some(generation)) =
        (&seed.lease_id, &seed.target_id, seed.lease_generation)
    else {
        return Ok(false);
    };
    binding.copy(&(
        &seed.provider,
        &seed.cid,
        lease,
        target,
        request,
        &authority.job_id,
    ))?;
    binding.with_decode_work(|work| {
        work.nodes(20)?;
        work.bytes(authority.job_id.len() + 64)
    })?;
    let jobs::NewPinJob::Target(expected) = jobs::poll_job(
        &seed.provider,
        &seed.cid,
        lease,
        target,
        generation,
        request,
        seed.started_at.ok_or(ManagementError::Confirmation)?,
    ) else {
        return Ok(false);
    };
    Ok(expected.id == authority.job_id)
}
