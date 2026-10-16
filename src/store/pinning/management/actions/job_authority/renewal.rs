//! Exact renewal outcomes and entry-local readonly observation births.
use super::super::execution::{EntryOutcome, Resource, cell, desired, epoch};
use super::*;
use crate::pinning::management::Action;

pub(crate) fn validate_outcome(
    binding: records::BindingView<'_>,
    entry: &PlanEntry,
    outcome: &EntryOutcome,
    resources: &[(&Resource, &Resource)],
    births: &[&records::RowHandle],
) -> Result<()> {
    let Action::Renew { expires_at } = entry.request.action else {
        return if matches!(outcome, EntryOutcome::Ordinary) {
            Ok(())
        } else {
            Err(ManagementError::Confirmation)
        };
    };
    let Some(Capture::Object(capture)) = &entry.capture else {
        return Err(ManagementError::Confirmation);
    };
    let target = capture
        .target
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let frame = binding.frozen_rows()?;
    let original = frame
        .row("pin_leases", &[&target.lease.id])
        .ok_or(ManagementError::Confirmation)?;
    let expiry = DateTime::from_timestamp(expires_at, 0).ok_or(ManagementError::Confirmation)?;
    let old_expiry = native::parse_time(cell(original, "expires_at")?)?;
    let (old, new, contract, targets, extended) = match outcome {
        EntryOutcome::Extended {
            old_lease,
            new_lease,
            contract,
            desired,
        } => (*old_lease, *new_lease, contract, desired, true),
        EntryOutcome::Kept {
            lease,
            contract,
            desired,
        } => (*lease, *lease, contract, desired, false),
        EntryOutcome::Ordinary => return Err(ManagementError::Confirmation),
    };
    if records::RowView::Handle(old) != original
        || target
            .lease
            .native_tuple
            .as_ref()
            .is_none_or(|row| original != *row)
        || native::value(original, "owner_object_id") != Some(&capture.owner_identity.object_id)
        || native::value(original, "state") != Some("active")
        || contract.as_deref()
            != frame
                .row("pin_lease_requirements", &[&target.lease.id])
                .and_then(|row| native::value(row, "contract"))
        || [
            "id",
            "owner_object_id",
            "source",
            "policy_id",
            "provider_mode",
            "content_mode",
            "state",
            "created_at",
        ]
        .iter()
        .any(|column| native::value(old, column) != native::value(new, column))
    {
        return Err(ManagementError::Confirmation);
    }
    let generation = cell(original, "generation")?
        .parse::<i64>()
        .map_err(|_| ManagementError::Confirmation)?;
    if target.lease.generation != generation
        || extended != (expiry > old_expiry)
        || cell(new, "generation")?.parse::<i64>().ok()
            != if extended {
                generation.checked_add(1)
            } else {
                Some(generation)
            }
        || native::parse_time(cell(new, "expires_at")?)?
            != if extended { expiry } else { old_expiry }
    {
        return Err(ManagementError::Confirmation);
    }
    let mut expected = frame
        .table("pin_lease_targets")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
        .filter(|row| native::value(row, "lease_id") == Some(&target.lease.id) && desired(row))
        .collect::<Vec<_>>();
    expected.sort_by(|a, b| native::value(a, "id").cmp(&native::value(b, "id")));
    if !targets
        .iter()
        .map(|row| records::RowView::Handle(row))
        .eq(expected)
    {
        return Err(ManagementError::Confirmation);
    }
    let pairs = targets
        .iter()
        .map(|row| Ok((cell(row, "provider")?, cell(row, "cid")?)))
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    for (provider, cid) in pairs {
        let (pre, post) = resources
            .iter()
            .find(|(pre, _)| pre.key.values == [provider, cid])
            .copied()
            .ok_or(ManagementError::Confirmation)?;
        if native::value(pre.remote, "request_id") != native::value(post.remote, "request_id")
            || native::value(pre.ledger, "route") != native::value(post.ledger, "route")
            || if extended {
                epoch(&pre.remote)?.checked_add(1) != Some(epoch(&post.remote)?)
            } else {
                pre != post
            }
        {
            return Err(ManagementError::Confirmation);
        }
    }
    if !extended && !births.is_empty() {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn validate_observation(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    authority: &records::JobAuthority,
    outcome: &EntryOutcome,
    pre: &Resource,
    post: &Resource,
    job: &records::RowHandle,
    invocation: &records::RowHandle,
) -> Result<()> {
    let EntryOutcome::Extended {
        old_lease,
        new_lease,
        contract,
        desired,
    } = outcome
    else {
        return Err(ManagementError::Confirmation);
    };
    let Some(Capture::Object(capture)) = &action.receipt.binding.capture else {
        return Err(ManagementError::Confirmation);
    };
    if !binding
        .confirmation
        .bindings
        .contains(&action.receipt.binding)
        || !action.receipt.job_ids.contains(&authority.job_id)
        || authority.directive != records::JobDirective::ReadOnlyObserve
        || authority.target_id.is_some()
        || authority.resource_key != *post.key
        || authority.provider != cell(post.remote, "provider")?
        || authority.cid != cell(post.remote, "cid")?
        || !desired.iter().any(|target| {
            native::value(target, "provider") == Some(&authority.provider)
                && native::value(target, "cid") == Some(&authority.cid)
        })
        || authority.remote_epoch != epoch(&post.remote)?
        || epoch(&pre.remote)?.checked_add(1) != Some(authority.remote_epoch)
        || authority.resource_id.as_deref() != native::value(post.remote, "request_id")
        || native::value(pre.remote, "request_id") != native::value(post.remote, "request_id")
        || binding
            .decode_route(cell(post.ledger, "route")?)
            .ok()
            .as_deref()
            != Some(&authority.route)
        || native::value(pre.ledger, "route") != native::value(post.ledger, "route")
        || !capture
            .requirement_routes
            .iter()
            .chain(&capture.historical_routes)
            .chain([&capture.route])
            .chain(capture.remote_route.iter())
            .any(|route| route.original.as_ref() == Some(&authority.route))
        || authority.owner.as_ref() != Some(&capture.owner_identity)
        || authority.source.as_ref() != Some(&capture.object.identity)
        || authority.lease_id.as_deref() != native::value(new_lease, "id")
        || !action
            .receipt
            .intent_ids
            .contains(&cell(new_lease, "id")?.to_owned())
        || authority.lease_generation != cell(new_lease, "generation")?.parse().ok()
        || authority.started_at != Some(native::parse_time(cell(old_lease, "created_at")?)?)
        || authority.expires_at != Some(native::parse_time(cell(new_lease, "expires_at")?)?)
        || authority.lease_source.as_deref() != native::value(old_lease, "source")
        || authority.policy_id.as_deref() != native::value(old_lease, "policy_id")
        || authority.provider_mode.as_deref() != native::value(old_lease, "provider_mode")
        || authority.content_mode.as_deref() != native::value(old_lease, "content_mode")
        || authority.lease_contract.as_deref() != *contract
        || native::value(job, "id") != Some(&authority.job_id)
        || native::value(job, "operation") != Some("reconcile")
        || native::value(job, "provider") != Some(&authority.provider)
        || native::value(job, "cid") != Some(&authority.cid)
        || native::value(job, "lease_id").is_some()
        || native::value(job, "target_id").is_some()
        || native::value(job, "expected_generation").is_some()
        || native::value(job, "expected_remote_epoch").and_then(|value| value.parse().ok())
            != Some(authority.remote_epoch)
        || native::value(invocation, "job_id") != Some(&authority.job_id)
        || native::value(invocation, "remote_epoch").and_then(|value| value.parse().ok())
            != Some(authority.remote_epoch)
        || binding
            .decode_route(cell(invocation, "route")?)
            .ok()
            .as_deref()
            != Some(&authority.route)
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}
