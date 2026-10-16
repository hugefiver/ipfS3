//! Native resource transition predicates for one item, including unchanged pairs.
use super::validate::{Boundary, Step};
use super::*;
use crate::pinning::management::{PlannedStep, RemoteDirective};

pub(super) fn outputs(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    op: &OperationRead,
) -> Result<()> {
    let request = &action.receipt.binding.request.action;
    let allocating = matches!(
        request,
        Action::Create(_) | Action::Backfill(_) | Action::Reapply(_)
    );
    let selected = match &action.receipt.binding.capture {
        Some(Capture::Object(capture)) => capture.target.as_ref(),
        _ => None,
    };
    for output in &op.facts.2 {
        let key = &output.0;
        let old = op.before(key)?;
        let new = op.after(key)?.ok_or(ManagementError::Confirmation)?;
        let allowed: &[&str] = match key.0.as_str() {
            "pin_leases" => {
                if allocating && old.is_none() && action.receipt.intent_ids.contains(&key.1[0]) {
                    continue;
                }
                if selected.is_none_or(|t| t.lease.id != key.1[0]) {
                    return Err(ManagementError::Confirmation);
                }
                match request {
                    Action::Renew { .. } => &["generation", "expires_at", "last_touched_at"],
                    Action::Cancel => &["generation", "state", "last_touched_at"],
                    _ => return Err(ManagementError::Confirmation),
                }
            }
            "pin_lease_requirements" => {
                if !allocating || old.is_some() || !action.receipt.intent_ids.contains(&key.1[0]) {
                    return Err(ManagementError::Confirmation);
                }
                continue;
            }
            "pin_lease_targets" => {
                if !action.targets.contains(&key.1[0]) {
                    let target = selected.ok_or(ManagementError::Confirmation)?;
                    if !matches!(request, Action::Retry)
                        || native::value(new, "provider")
                            != native::value(&target.native_tuple, "provider")
                        || native::value(new, "cid") != native::value(&target.native_tuple, "cid")
                        || native::value(new, "state") != Some("waiting")
                    {
                        return Err(ManagementError::Confirmation);
                    }
                    let lease = op
                        .before(&capture::key("pin_leases", cell(new, "lease_id")?))?
                        .ok_or(ManagementError::Confirmation)?;
                    if native::value(lease, "provider_mode") != Some("all")
                        || native::value(lease, "state") != Some("active")
                    {
                        return Err(ManagementError::Confirmation);
                    }
                }
                if old.is_none() && (allocating || matches!(request, Action::Migrate { .. })) {
                    continue;
                }
                if !matches!(
                    request,
                    Action::Retry | Action::Cancel | Action::Renew { .. }
                ) {
                    return Err(ManagementError::Confirmation);
                }
                &["state", "last_touched_at"]
            }
            "remote_pins" | "remote_pin_ledger" => {
                let allocated = op
                    .facts
                    .2
                    .iter()
                    .filter(|o| o.0.0 == "pin_lease_targets")
                    .any(|target| {
                        op.after(&target.0).ok().flatten().is_some_and(|t| {
                            native::value(t, "provider") == Some(&key.1[0])
                                && native::value(t, "cid") == Some(&key.1[1])
                                && desired(t)
                        })
                    });
                if old.is_none() {
                    if !allocated || !(allocating || matches!(request, Action::Migrate { .. })) {
                        return Err(ManagementError::Confirmation);
                    }
                    continue;
                }
                // Existing resource lifetimes and their exact transition columns
                // are checked by validate for every complete remote/ledger pair.
                continue;
            }
            "pin_jobs" => {
                if old.is_none() {
                    let operation = cell(new, "operation")?;
                    let valid = match request {
                        Action::Create(_)
                        | Action::Backfill(_)
                        | Action::Reapply(_)
                        | Action::Retry
                        | Action::Migrate { .. } => matches!(operation, "submit" | "reconcile"),
                        Action::Renew { .. } | Action::Reconcile => operation == "reconcile",
                        Action::Cancel => matches!(operation, "reconcile" | "unpin"),
                        Action::RetireWith {
                            selection:
                                crate::pinning::management::RetirementSelection::ControlledCleanup {
                                    ..
                                },
                        } => operation == "unpin",
                        _ => false,
                    };
                    if !valid
                        || operation == "submit"
                            && native::value(new, "lease_id").is_none_or(|id| {
                                !action.receipt.intent_ids.iter().any(|intent| intent == id)
                            })
                    {
                        return Err(ManagementError::Confirmation);
                    }
                    continue;
                }
                if !(allocating || matches!(request, Action::Retry)) {
                    return Err(ManagementError::Confirmation);
                }
                &[
                    "state",
                    "submit_phase",
                    "next_attempt_at",
                    "updated_at",
                    "last_error",
                    "attempts",
                    "locked_until",
                ]
            }
            "pin_invocation_routes" => {
                if old.is_none() {
                    continue;
                }
                if !matches!(request, Action::Retry) {
                    return Err(ManagementError::Confirmation);
                }
                &["remote_epoch"]
            }
            "pin_submit_history" => {
                if old.is_none() || !(allocating || matches!(request, Action::Retry)) {
                    return Err(ManagementError::Confirmation);
                }
                &["state"]
            }
            "pin_resource_history" => {
                if old.is_some() || !matches!(request, Action::Retry) {
                    return Err(ManagementError::Confirmation);
                }
                continue;
            }
            "pin_provider_usage" => {
                usage(action, op, key, new)?;
                if old.is_none() {
                    continue;
                }
                &["reserved_bytes", "reserved_pins"]
            }
            "pin_provider_routes" => {
                if !matches!(request, Action::Retire | Action::RetireWith { .. })
                    || key
                        != &capture::key(
                            "pin_provider_routes",
                            &super::closure::retirement_key(binding, &action.receipt.binding)?.1[0],
                        )
                    || !matches!(native::value(new, "retired"), Some("1" | "true"))
                {
                    return Err(ManagementError::Confirmation);
                }
                &["retired"]
            }
            "pin_management_retirements" => {
                if old.is_some()
                    || key != &super::closure::retirement_key(binding, &action.receipt.binding)?
                {
                    return Err(ManagementError::Confirmation);
                }
                continue;
            }
            "zip_root_references" => {
                if !action
                    .root_releases
                    .iter()
                    .any(|r| r.table == key.0 && r.values == key.1)
                {
                    return Err(ManagementError::Confirmation);
                }
                &["logical_released_at"]
            }
            _ => return Err(ManagementError::Confirmation),
        };
        same_except(old.ok_or(ManagementError::Confirmation)?, new, allowed)?;
    }
    Ok(())
}

fn usage(
    action: &records::CommittedAction,
    op: &OperationRead,
    key: &facts::Key,
    new: &records::RowHandle,
) -> Result<()> {
    if !matches!(
        action.receipt.binding.request.action,
        Action::Create(_) | Action::Backfill(_) | Action::Reapply(_) | Action::Migrate { .. }
    ) {
        return Err(ManagementError::Confirmation);
    }
    let old = op.before(key)?;
    let mut bytes = 0i64;
    let mut pins = 0i64;
    for output in &op.facts.2 {
        if output.0.0 == "remote_pins"
            && output.0.1[0] == key.1[0]
            && op
                .before(&output.0)?
                .is_none_or(|r| native::value(r, "status") == Some("absent"))
        {
            let remote = op.after(&output.0)?.ok_or(ManagementError::Confirmation)?;
            bytes = bytes
                .checked_add(
                    cell(remote, "cid_size")?
                        .parse()
                        .map_err(|_| ManagementError::Confirmation)?,
                )
                .ok_or(ManagementError::Confirmation)?;
            pins += 1;
        }
    }
    for (column, delta) in [("reserved_bytes", bytes), ("reserved_pins", pins)] {
        let prior = old
            .map(|r| {
                cell(r, column)
                    .and_then(|s| s.parse::<i64>().map_err(|_| ManagementError::Confirmation))
            })
            .transpose()?
            .unwrap_or(0);
        if prior.checked_add(delta) != cell(new, column)?.parse::<i64>().ok() {
            return Err(ManagementError::Confirmation);
        }
    }
    Ok(())
}

pub(super) fn validate(
    binding: records::BindingView<'_>,
    frame: records::FrozenRows<'_>,
    entry: &PlanEntry,
    step: &Step,
    boundary: &Boundary,
) -> Result<()> {
    let before = &boundary.before;
    let after = &boundary.after;
    match &entry.request.action {
        Action::Create(_) | Action::Backfill(_) | Action::Reapply(_) => {
            let granted = step.destinations.iter().any(|target| {
                desired(target)
                    && pair(target, before)
                    && step.intents.iter().any(|lease| {
                        native::value(lease, "id") == native::value(target, "lease_id")
                    })
            });
            if granted {
                allocation(frame, before, after)
            } else {
                unchanged(before, after)
            }
        }
        Action::Reconcile | Action::Retire | Action::RetireWith { .. } => unchanged(before, after),
        Action::Renew { .. } => {
            let EntryOutcome::Extended {
                desired: targets, ..
            } = &step.outcome
            else {
                return unchanged(before, after);
            };
            if !targets.iter().any(|target| pair(target, before)) {
                return unchanged(before, after);
            }
            advance(before, after)?;
            let mut allowed = vec!["epoch", "last_touched_at"];
            if native::value(before.remote, "status") == Some("failed") {
                if native::value(after.remote, "failure_attempts") != Some("0")
                    || native::value(after.remote, "next_retry_at").is_none()
                {
                    return Err(ManagementError::Confirmation);
                }
                allowed.extend(["failure_attempts", "next_retry_at"]);
            }
            same_except(&before.remote, after.remote, &allowed)?;
            if before.ledger != after.ledger {
                return Err(ManagementError::Confirmation);
            }
            Ok(())
        }
        Action::Cancel => {
            let Some(Capture::Object(capture)) = &entry.capture else {
                return Err(ManagementError::Confirmation);
            };
            let target = capture
                .target
                .as_ref()
                .ok_or(ManagementError::Confirmation)?;
            let old = frame
                .row("pin_leases", &[&target.lease.id])
                .ok_or(ManagementError::Confirmation)?;
            let closes = native::value(old, "state") == Some("active")
                && frame
                    .table("pin_lease_targets")
                    .ok_or(ManagementError::Schema)?
                    .rows
                    .iter()
                    .any(|target| {
                        native::value(target, "lease_id") == native::value(old, "id")
                            && desired(&target)
                            && pair(&target, before)
                    });
            if !closes {
                return unchanged(before, after);
            }
            let ended = step
                .intents
                .iter()
                .find(|lease| native::value(lease, "id") == native::value(old, "id"))
                .ok_or(ManagementError::Confirmation)?;
            if native::value(ended, "state") != Some("cancelled")
                || cell(old, "generation")?
                    .parse::<i64>()
                    .ok()
                    .and_then(|generation| generation.checked_add(1))
                    != cell(ended, "generation")?.parse().ok()
            {
                return Err(ManagementError::Confirmation);
            }
            same_except(&old, ended, &["state", "generation", "last_touched_at"])?;
            advance(before, after)?;
            same_except(&before.remote, after.remote, &["epoch", "last_touched_at"])?;
            if before.ledger != after.ledger {
                return Err(ManagementError::Confirmation);
            }
            Ok(())
        }
        Action::Migrate { destination } => {
            let Some(Capture::Object(capture)) = &entry.capture else {
                return Err(ManagementError::Confirmation);
            };
            let source = capture
                .target
                .as_ref()
                .ok_or(ManagementError::Confirmation)?;
            let mut allocated = false;
            for target in &step.destinations {
                if cell(target, "id")? != source.id
                    && desired(target)
                    && pair(target, before)
                    && frame
                        .row("pin_lease_targets", &[cell(target, "id")?])
                        .is_none()
                {
                    allocated = true;
                }
            }
            let route = frame
                .row("pin_provider_routes", &[&before.key.values[0]])
                .and_then(|row| native::value(row, "snapshot"))
                .map(|encoded| binding.decode_route(encoded))
                .transpose()?;
            if allocated && route.as_deref() == destination.original.as_ref() {
                allocation(frame, before, after)
            } else {
                unchanged(before, after)
            }
        }
        Action::Retry => {
            let (Some(Capture::Object(capture)), Some(PlannedStep::Object(plan))) =
                (&entry.capture, &entry.step)
            else {
                return Err(ManagementError::Confirmation);
            };
            let target = capture
                .target
                .as_ref()
                .ok_or(ManagementError::Confirmation)?;
            let resubmit = plan.source == RemoteDirective::SubmitWithFence
                && pair(&target.native_tuple, before)
                && native::value(before.remote, "status") == Some("failed")
                && native::value(before.remote, "request_id").is_some();
            if !resubmit {
                return unchanged(before, after);
            }
            advance(before, after)?;
            if native::value(after.remote, "status") != Some("reserved")
                || [
                    "request_id",
                    "next_retry_at",
                    "last_error_class",
                    "last_error_text",
                ]
                .iter()
                .any(|column| native::value(after.remote, column).is_some())
            {
                return Err(ManagementError::Confirmation);
            }
            same_except(
                &before.remote,
                after.remote,
                &[
                    "epoch",
                    "last_touched_at",
                    "status",
                    "request_id",
                    "next_retry_at",
                    "last_error_class",
                    "last_error_text",
                ],
            )?;
            if before.ledger != after.ledger {
                return Err(ManagementError::Confirmation);
            }
            Ok(())
        }
    }
}

fn pair(target: &impl native::RawCells, resource: &Resource) -> bool {
    native::value(target, "provider") == resource.key.values.first().map(String::as_str)
        && native::value(target, "cid") == resource.key.values.get(1).map(String::as_str)
}
fn unchanged(before: &Resource, after: &Resource) -> Result<()> {
    if before != after {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}
fn advance(before: &Resource, after: &Resource) -> Result<()> {
    if epoch(&before.remote)?.checked_add(1) != Some(epoch(&after.remote)?) {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}
fn same_except(
    before: &impl native::RawCells,
    after: &records::RowHandle,
    allowed: &[&str],
) -> Result<()> {
    if after.iter().any(|(column, value)| {
        !allowed.contains(&column.as_str()) && before.raw_cell(column) != Some(value)
    }) {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}
fn allocation(frame: records::FrozenRows<'_>, before: &Resource, after: &Resource) -> Result<()> {
    advance(before, after)?;
    if native::value(before.remote, "status") == Some("absent") {
        if native::value(before.ledger, "effect") != Some("absent")
            || native::value(after.remote, "status") != Some("reserved")
            || native::value(after.remote, "failure_attempts") != Some("0")
            || [
                "request_id",
                "next_retry_at",
                "last_failed_request_id",
                "last_error_class",
                "last_error_text",
            ]
            .iter()
            .any(|column| native::value(after.remote, column).is_some())
        {
            return Err(ManagementError::Confirmation);
        }
        same_except(
            &before.remote,
            after.remote,
            &[
                "epoch",
                "last_touched_at",
                "status",
                "request_id",
                "failure_attempts",
                "next_retry_at",
                "last_failed_request_id",
                "last_error_class",
                "last_error_text",
            ],
        )?;
        let registered = frame
            .row("pin_provider_routes", &[&before.key.values[0]])
            .ok_or(ManagementError::Confirmation)?;
        if native::value(after.ledger, "route") != native::value(registered, "snapshot")
            || native::value(after.ledger, "ownership") != Some("unknown")
            || native::value(after.ledger, "effect") != Some("reserved")
            || [
                "first_observed_at",
                "last_observed_at",
                "remote_pinned_at",
                "gateway_verified_at",
                "content_verified_at",
                "first_error",
                "last_error",
            ]
            .iter()
            .any(|column| native::value(after.ledger, column).is_some())
        {
            return Err(ManagementError::Confirmation);
        }
    } else {
        same_except(&before.remote, after.remote, &["epoch", "last_touched_at"])?;
        same_except(&before.ledger, after.ledger, &["effect"])?;
        if native::value(before.ledger, "effect") != native::value(after.ledger, "effect")
            && !(native::value(before.remote, "status") == Some("pinned")
                && native::value(before.ledger, "effect") == Some("retained")
                && native::value(before.ledger, "remote_pinned_at").is_some()
                && native::value(after.ledger, "effect") == Some("confirmed"))
        {
            return Err(ManagementError::Confirmation);
        }
    }
    Ok(())
}
