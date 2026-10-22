use super::*;
use capture::key;
use facts::{Key, RowRef};
use rows::{LatestRows, RowSet};

pub(super) struct Boundary<'a> {
    pub before: Resource<'a>,
    pub after: Resource<'a>,
}
pub(super) struct Step<'a> {
    pub intents: Vec<&'a records::RowHandle>,
    pub destinations: Vec<&'a records::RowHandle>,
    pub resources: Vec<Boundary<'a>>,
    pub births: Vec<(&'a records::RowHandle, &'a records::RowHandle)>,
    pub outcome: EntryOutcome<'a>,
}

fn view<'a>(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    op: &'a OperationRead,
) -> Result<Step<'a>> {
    let resources = op
        .facts
        .1
        .iter()
        .filter(|read| read.0.0 == "remote_pins")
        .count();
    binding.with_decode_work(|work| {
        work.nodes(
            5 + action.receipt.intent_ids.len()
                + action.targets.len()
                + resources
                + op.facts.3.len() * 2,
        )
    })?;
    let mut intents = Vec::new();
    for id in &action.receipt.intent_ids {
        intents.push(
            op.after(&key("pin_leases", id))?
                .ok_or(ManagementError::Confirmation)?,
        );
    }
    let mut destinations = Vec::new();
    for id in &action.targets {
        destinations.push(
            op.after(&key("pin_lease_targets", id))?
                .ok_or(ManagementError::Confirmation)?,
        );
    }
    let mut resources = Vec::new();
    for read in &op.facts.1 {
        if read.0.0 == "remote_pins" {
            let ledger = &op
                .facts
                .1
                .iter()
                .find(|candidate| candidate.0.0 == "remote_pin_ledger" && candidate.0.1 == read.0.1)
                .ok_or(ManagementError::Confirmation)?
                .0;
            if let (Some(pre), Some(pre_ledger), Some(post), Some(post_ledger)) = (
                op.before(&read.0)?,
                op.before(ledger)?,
                op.after(&read.0)?,
                op.after(ledger)?,
            ) {
                binding.copy(&(&read.0.0, &read.0.1))?;
                binding.with_decode_work(|work| work.nodes(1))?;
                let k = std::sync::Arc::new(native::NativeKey {
                    table: read.0.0.clone(),
                    values: read.0.1.clone(),
                });
                resources.push(Boundary {
                    before: Resource {
                        key: k.clone(),
                        remote: pre,
                        ledger: pre_ledger,
                    },
                    after: Resource {
                        key: k,
                        remote: post,
                        ledger: post_ledger,
                    },
                });
            }
        }
    }
    let mut births = Vec::new();
    for Birth(j, i) in &op.facts.3 {
        births.push((op.row(RowRef::Output(*j))?, op.row(RowRef::Output(*i))?));
    }
    let outcome = if let Outcome::Renew(old, new, contract, targets, extended) = op.outcome() {
        let lease = op.row(*new)?;
        let contract = contract
            .map(|r| op.row(r).and_then(|row| cell(row, "contract")))
            .transpose()?;
        binding.with_decode_work(|work| work.nodes(1 + targets.len()))?;
        let desired = targets.iter().map(|r| op.row(*r)).collect::<Result<_>>()?;
        if *extended {
            EntryOutcome::Extended {
                old_lease: op.row(*old)?,
                new_lease: lease,
                contract,
                desired,
            }
        } else {
            EntryOutcome::Kept {
                lease,
                contract,
                desired,
            }
        }
    } else {
        EntryOutcome::Ordinary
    };
    Ok(Step {
        intents,
        destinations,
        resources,
        births,
        outcome,
    })
}

pub(super) fn action(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    op: &OperationRead,
) -> Result<()> {
    let f = &op.facts;
    let entry = &action.receipt.binding;
    if binding.confirmation.bindings.get(f.0) != Some(entry)
        || action.state == records::ActionState::Stopped
        || action.targets.windows(2).any(|p| p[0] >= p[1])
        || action
            .receipt
            .intent_ids
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != action.receipt.intent_ids.len()
        || action.jobs.windows(2).any(|p| p[0].job_id >= p[1].job_id)
        || !action
            .jobs
            .iter()
            .map(|j| &j.job_id)
            .eq(&action.receipt.job_ids)
        || f.6.windows(2).any(|p| p[0] >= p[1])
        || f.6 != closure::dependencies(binding, action)?
    {
        return Err(ManagementError::Confirmation);
    }
    let step = view(binding, action, op)?;
    super::effects::outputs(binding, action, op)?;
    if let Some(Capture::Object(capture)) = &entry.capture {
        for lease in &step.intents {
            if native::value(lease, "owner_object_id") != Some(&capture.owner_identity.object_id) {
                return Err(ManagementError::Confirmation);
            }
        }
    }
    retirement::validate(binding, action)?;
    let mut expected_births = Vec::new();
    for (slot, output) in f.2.iter().enumerate() {
        if output.0.0 == "pin_jobs" && op.before(&output.0)?.is_none() && output.2.is_some() {
            let invocation =
                f.2.iter()
                    .position(|o| o.0 == key("pin_invocation_routes", &output.0.1[0]))
                    .ok_or(ManagementError::Confirmation)?;
            expected_births.push(Birth(slot, invocation));
        }
    }
    expected_births.sort_by(|a, b| f.2[a.0].0.1.cmp(&f.2[b.0].0.1));
    if f.3 != expected_births || f.4.len() != action.jobs.len() {
        return Err(ManagementError::Confirmation);
    }
    for (ordinal, (link, authority)) in f.4.iter().zip(&action.jobs).enumerate() {
        if link.0 != ordinal
            || op.key(link.1)? != &key("pin_jobs", &authority.job_id)
            || op.key(link.2)? != &key("pin_invocation_routes", &authority.job_id)
        {
            return Err(ManagementError::Confirmation);
        }
        let row = op.row(link.1)?;
        let invocation = op.row(link.2)?;
        validate_job_rows(binding, action, authority, row, invocation)?;
        if let Some(id) = &authority.lease_id {
            let lease = op
                .after(&key("pin_leases", id))?
                .ok_or(ManagementError::Confirmation)?;
            if native::value(lease, "source") != authority.lease_source.as_deref()
                || native::value(lease, "generation").and_then(|v| v.parse::<i64>().ok())
                    != authority.lease_generation
                || native::value(lease, "owner_object_id")
                    != authority.owner.as_ref().map(|o| o.object_id.as_str())
                || Some(native::parse_time(cell(lease, "created_at")?)?) != authority.started_at
                || Some(native::parse_time(cell(lease, "expires_at")?)?) != authority.expires_at
                || native::value(lease, "policy_id") != authority.policy_id.as_deref()
                || native::value(lease, "provider_mode") != authority.provider_mode.as_deref()
                || native::value(lease, "content_mode") != authority.content_mode.as_deref()
                || op
                    .after(&key("pin_lease_requirements", id))?
                    .and_then(|r| native::value(r, "contract"))
                    != authority.lease_contract.as_deref()
            {
                return Err(ManagementError::Confirmation);
            }
        }
    }
    for (job, invocation) in &step.births {
        birth(binding, action, op, job, invocation)?;
        if native::value(job, "id") != native::value(invocation, "job_id") {
            return Err(ManagementError::Confirmation);
        }
        let operation = cell(job, "operation")?;
        if !matches!(operation, "submit" | "poll" | "reconcile" | "unpin") {
            return Err(ManagementError::Confirmation);
        }
        if matches!(operation, "reconcile" | "unpin")
            && (native::value(job, "lease_id").is_some()
                || native::value(job, "target_id").is_some()
                || native::value(job, "expected_generation").is_some()
                || native::value(job, "expected_remote_epoch")
                    != native::value(invocation, "remote_epoch"))
        {
            return Err(ManagementError::Confirmation);
        }
    }
    let frame = binding.frozen_rows()?;
    for boundary in &step.resources {
        super::effects::validate(binding, frame, entry, &step, boundary)?;
    }
    job_authority::renewal::validate_outcome(
        binding,
        entry,
        &step.outcome,
        &step
            .resources
            .iter()
            .map(|b| (&b.before, &b.after))
            .collect::<Vec<_>>(),
        &step.births.iter().map(|b| b.0).collect::<Vec<_>>(),
    )?;
    match (&entry.request.action, op.outcome()) {
        (
            Action::Create(intent) | Action::Backfill(intent) | Action::Reapply(intent),
            Outcome::Allocate(lease, requirement, attestation, decision, origin),
        ) => {
            let lease = op.row(*lease)?;
            let id = cell(lease, "id")?;
            if action.receipt.intent_ids != [id]
                || !id.starts_with("management:")
                || native::value(lease, "source") != Some(id)
                || native::value(lease, "generation") != Some("1")
                || native::value(lease, "state") != Some("active")
                || cell(op.row(*requirement)?, "lease_id")? != id
            {
                return Err(ManagementError::Confirmation);
            }
            let Some(Capture::Object(capture)) = &entry.capture else {
                return Err(ManagementError::Confirmation);
            };
            if native::value(lease, "owner_object_id") != Some(&capture.owner_identity.object_id)
                || native::value(lease, "policy_id") != Some(&intent.requirements.policy_revision)
                || native::parse_time(cell(lease, "created_at")?)?.timestamp() != intent.started_at
                || native::parse_time(cell(lease, "expires_at")?)?.timestamp() != intent.expires_at
            {
                return Err(ManagementError::Confirmation);
            }
            if op.key(*attestation)?.0 != "pin_management_records"
                || native::value(op.row(*attestation)?, "kind") != Some("evidence_preparation")
            {
                return Err(ManagementError::Confirmation);
            }
            let decision_key = key(
                "pin_extension_decisions",
                &capture.object.identity.private_version_id,
            );
            if op.before(&decision_key)?.is_some() != decision.is_some() {
                return Err(ManagementError::Confirmation);
            }
            if let Some(r) = decision
                && (op.key(*r)? != &decision_key
                    || op.row(*r)?
                        != op
                            .before(&decision_key)?
                            .ok_or(ManagementError::Confirmation)?)
            {
                return Err(ManagementError::Confirmation);
            }
            if matches!(
                capture.object.evidence,
                crate::pinning::management::SelectionEvidence::TrustedManifest { .. }
            ) != (origin.len() == 2)
            {
                return Err(ManagementError::Confirmation);
            }
            if action.decision.as_ref().is_none_or(|d| {
                d.id != id
                    || d.owner != capture.owner_identity
                    || d.intent != *intent
                    || Some(&d.configuration) != binding.confirmation.configuration.as_ref()
            }) {
                return Err(ManagementError::Confirmation);
            }
            allocation::validate(
                binding,
                action,
                op,
                intent,
                capture,
                lease,
                op.row(*requirement)?,
            )?;
        }
        (Action::Renew { .. }, Outcome::Renew(..)) => {
            if action.receipt.job_ids
                != step
                    .births
                    .iter()
                    .map(|b| cell(b.0, "id").map(str::to_owned))
                    .collect::<Result<Vec<_>>>()?
            {
                return Err(ManagementError::Confirmation);
            }
        }
        (Action::Retry | Action::Reconcile, Outcome::Recover(target, lease, resources)) => {
            let Some(Capture::Object(capture)) = &entry.capture else {
                return Err(ManagementError::Confirmation);
            };
            let selected = capture
                .target
                .as_ref()
                .ok_or(ManagementError::Confirmation)?;
            if action.receipt.intent_ids != [selected.lease.id.clone()]
                || action.targets != [selected.id.clone()]
                || op.row(*target)? != &selected.native_tuple
                || native::value(op.row(*lease)?, "id") != Some(&selected.lease.id)
                || resources.is_empty()
            {
                return Err(ManagementError::Confirmation);
            }
            if matches!(entry.request.action, Action::Reconcile)
                && action
                    .jobs
                    .iter()
                    .any(|j| j.directive != records::JobDirective::ReadOnlyObserve)
            {
                return Err(ManagementError::Confirmation);
            }
        }
        (Action::Migrate { .. }, Outcome::Migrate(..)) => {
            if action.migration_points.len() > 1 {
                return Err(ManagementError::Confirmation);
            }
            let Some(Capture::Object(capture)) = &entry.capture else {
                return Err(ManagementError::Confirmation);
            };
            let target = capture
                .target
                .as_ref()
                .ok_or(ManagementError::Confirmation)?;
            let mut targets = vec![target.id.clone()];
            for p in &action.migration_points {
                targets.push(p.destination_target_id.clone());
            }
            targets.sort();
            targets.dedup();
            if action.targets != targets || action.receipt.intent_ids != [target.lease.id.clone()] {
                return Err(ManagementError::Confirmation);
            }
        }
        (Action::Cancel, Outcome::Cancel(old, new)) => {
            if native::value(op.row(*old)?, "id") != native::value(op.row(*new)?, "id")
                || native::value(op.row(*new)?, "state") != Some("cancelled")
            {
                return Err(ManagementError::Confirmation);
            }
        }
        (Action::Retire | Action::RetireWith { .. }, Outcome::Retire(retirement, roots)) => {
            if !action.receipt.intent_ids.is_empty()
                || !action.migration_points.is_empty()
                || op.key(*retirement)?.0 != "pin_management_retirements"
                || roots.len() != action.root_releases.len()
            {
                return Err(ManagementError::Confirmation);
            }
            if !matches!(
                entry.request.action,
                Action::RetireWith {
                    selection: crate::pinning::management::RetirementSelection::Migrate { .. }
                }
            ) && !action.targets.is_empty()
            {
                return Err(ManagementError::Confirmation);
            }
        }
        _ => return Err(ManagementError::Confirmation),
    }
    for authority in &action.jobs {
        job(binding, action, op, authority)?;
    }
    Ok(())
}

fn birth(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    op: &OperationRead,
    j: &records::RowHandle,
    i: &records::RowHandle,
) -> Result<()> {
    let operation = cell(j, "operation")?;
    let created = native::parse_time(cell(j, "created_at")?)?;
    let next = native::parse_time(cell(j, "next_attempt_at")?)?;
    if native::value(j, "state") != Some("pending")
        || native::value(j, "attempts") != Some("0")
        || native::value(j, "locked_until").is_some()
        || native::value(j, "last_error").is_some()
        || native::value(j, "submit_phase")
            != if operation == "submit" {
                Some("ready")
            } else {
                None
            }
        || native::parse_time(cell(j, "updated_at")?)? != created
    {
        return Err(ManagementError::Confirmation);
    }
    let provider = cell(j, "provider")?;
    let cid = cell(j, "cid")?;
    if next != created {
        let remote = op
            .facts
            .1
            .iter()
            .find(|read| {
                read.0.0 == "remote_pins" && read.0.1.iter().map(String::as_str).eq([provider, cid])
            })
            .ok_or(ManagementError::Confirmation)?;
        let remote = op.after(&remote.0)?.ok_or(ManagementError::Confirmation)?;
        let valid = match operation {
            "poll" => {
                next == created
                    + chrono::Duration::from_std(crate::store::pinning::jobs::POLL_INTERVAL)
                        .map_err(|_| ManagementError::Confirmation)?
            }
            "reconcile" => {
                native::value(remote, "status") == Some("failed")
                    && native::value(remote, "next_retry_at")
                        .map(native::parse_time)
                        .transpose()?
                        == Some(next)
            }
            _ => false,
        };
        if !valid {
            return Err(ManagementError::Confirmation);
        }
    }
    let route = binding.decode_route(cell(i, "route")?)?;
    match cell(j, "operation")? {
        "submit" | "poll" => {
            use crate::store::pinning::jobs;
            let lease = op
                .after(&key("pin_leases", cell(j, "lease_id")?))?
                .ok_or(ManagementError::Confirmation)?;
            let target = op
                .after(&key("pin_lease_targets", cell(j, "target_id")?))?
                .ok_or(ManagementError::Confirmation)?;
            if native::value(j, "expected_remote_epoch").is_some()
                || native::value(j, "expected_generation") != native::value(lease, "generation")
                || native::value(target, "lease_id") != native::value(lease, "id")
                || native::value(target, "provider") != Some(provider)
                || native::value(target, "cid") != Some(cid)
            {
                return Err(ManagementError::Confirmation);
            }
            let generation = cell(lease, "generation")?
                .parse::<i64>()
                .map_err(|_| ManagementError::Confirmation)?;
            let target_id = cell(target, "id")?;
            binding.with_decode_work(|work| {
                work.nodes(1)?;
                work.bytes(provider.len() + cid.len() + target_id.len() + "submit:::g".len() + 20)
            })?;
            let expected = if operation == "submit" {
                jobs::stable_submit_id(provider, cid, target_id, generation)
            } else {
                let remote = op
                    .facts
                    .1
                    .iter()
                    .find(|read| {
                        read.0.0 == "remote_pins"
                            && read.0.1.iter().map(String::as_str).eq([provider, cid])
                    })
                    .ok_or(ManagementError::Confirmation)?;
                let remote = op.after(&remote.0)?.ok_or(ManagementError::Confirmation)?;
                let request = cell(remote, "request_id")?;
                binding.with_decode_work(|work| {
                    work.nodes(1)?;
                    work.bytes(65)
                })?;
                jobs::stable_poll_id(provider, cid, target_id, generation, request)
            };
            if cell(j, "id")? != expected {
                return Err(ManagementError::Confirmation);
            }
        }
        "reconcile" | "unpin" => {
            let id = cell(j, "id")?;
            let custom = action.jobs.iter().any(|authority| {
                authority.job_id == id
                    && matches!(
                        (operation, authority.directive),
                        ("reconcile", records::JobDirective::ReadOnlyObserve)
                            | ("unpin", records::JobDirective::ExistingCleanup)
                    )
            }) && id
                .strip_prefix(if operation == "reconcile" {
                    "management-observe:"
                } else {
                    "management-cleanup:"
                })
                .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok());
            if !custom {
                let epoch = cell(i, "remote_epoch")?
                    .parse::<i64>()
                    .map_err(|_| ManagementError::Confirmation)?;
                binding.with_decode_work(|work| {
                    work.nodes(1)?;
                    work.bytes(provider.len() + cid.len() + "reconcile:::e".len() + 20)
                })?;
                let expected = if operation == "reconcile" {
                    crate::store::pinning::jobs::stable_reconcile_id(provider, cid, epoch)
                } else {
                    crate::store::pinning::jobs::stable_unpin_id(provider, cid, epoch)
                };
                if id != expected {
                    return Err(ManagementError::Confirmation);
                }
            }
        }
        _ => return Err(ManagementError::Confirmation),
    }
    let cancellation = if matches!(action.receipt.binding.request.action, Action::Cancel)
        && let Some(Capture::Object(capture)) = &action.receipt.binding.capture
        && let Some(target) = &capture.target
    {
        let rows = binding.frozen_rows()?;
        rows.table("pin_lease_targets")
            .ok_or(ManagementError::Schema)?
            .rows
            .iter()
            .any(|target_row| {
                native::value(target_row, "lease_id") == Some(&target.lease.id)
                    && native::value(target_row, "provider") == Some(provider)
                    && native::value(target_row, "cid") == Some(cid)
            })
            && rows
                .row("remote_pin_ledger", &[provider, cid])
                .and_then(|ledger| ledger.cell("route"))
                .map(|encoded| binding.decode_route(encoded))
                .transpose()?
                .as_ref()
                == Some(&route)
    } else {
        false
    };
    let valid = match &action.receipt.binding.capture {
        Some(Capture::Object(capture)) => {
            crate::pinning::provider::canonical_resource_cid(cid)
                .ok()
                .as_deref()
                == Some(&capture.object.identity.cid)
                && (cancellation
                    || capture
                        .requirement_routes
                        .iter()
                        .chain(&capture.historical_routes)
                        .chain([&capture.route])
                        .chain(capture.destination_route.iter())
                        .chain(capture.remote_route.iter())
                        .chain(capture.destination_remote_route.iter())
                        .any(|r| r.original.as_ref() == Some(route.as_ref())))
        }
        Some(Capture::Provider(capture)) => {
            cell(j, "operation")? == "unpin"
                && capture.mutation_scope.remotes.iter().any(|r| {
                    r.epoch.to_string() == cell(i, "remote_epoch").unwrap_or("")
                        && r.route.original.as_ref() == Some(route.as_ref())
                        && crate::pinning::provider::canonical_resource_cid(cid)
                            .ok()
                            .as_deref()
                            == Some(&r.key.cid)
                })
        }
        None => false,
    };
    if !valid {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}

pub(super) fn closure(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    op: &OperationRead,
    cursor: &Cursor,
) -> Result<()> {
    let before = LatestRows::before(binding, cursor)?;
    let after = before.after(op);
    let mut expected = BTreeSet::new();
    closure::keys(binding, action, &before, &after, &mut expected)?;
    if op
        .facts
        .2
        .iter()
        .any(|output| !expected.contains(&output.0))
    {
        return Err(ManagementError::Confirmation);
    }
    if !expected.iter().eq(op.facts.1.iter().map(|r| &r.0)) {
        return Err(ManagementError::Confirmation);
    }
    let renewal = if let Outcome::Renew(_, _, _, _, extended) = op.outcome() {
        Some(if *extended {
            crate::store::pinning::leases::ManualLeaseRenewalOutcome::Extended { generation: 0 }
        } else {
            crate::store::pinning::leases::ManualLeaseRenewalOutcome::Kept { generation: 0 }
        })
    } else {
        None
    };
    if closure::outcome(binding, action, &op.facts, &before, &after, renewal)? != op.facts.5 {
        return Err(ManagementError::Confirmation);
    }
    // The full target predicate includes manifest members not mentioned by facts.
    let mut selected = Vec::new();
    for (key, row) in after
        .iter()
        .filter(|(k, _)| k.table() == "pin_lease_targets")
    {
        if action
            .receipt
            .intent_ids
            .iter()
            .any(|id| native::value(row, "lease_id") == Some(id))
        {
            binding.copy(&key.value(0))?;
            selected.push(key.value(0));
        }
    }
    selected.sort_unstable();
    if matches!(
        action.receipt.binding.request.action,
        Action::Create(_)
            | Action::Backfill(_)
            | Action::Reapply(_)
            | Action::Renew { .. }
            | Action::Cancel
    ) && selected != action.targets
    {
        return Err(ManagementError::Confirmation);
    }
    eligibility(action, op, &before, &after)?;
    action_check(binding, action, op)
}
fn action_check(
    binding: records::BindingView<'_>,
    a: &records::CommittedAction,
    o: &OperationRead,
) -> Result<()> {
    action(binding, a, o)
}

fn eligibility(
    action: &records::CommittedAction,
    op: &OperationRead,
    before: &impl RowSet,
    after: &impl RowSet,
) -> Result<()> {
    let mut expected = BTreeSet::new();
    let readonly = op.facts.3.iter().any(|b| {
        op.row(RowRef::Output(b.0))
            .is_ok_and(|j| native::value(j, "operation") == Some("reconcile"))
    });
    for (k, j) in after.iter().filter(|(k, _)| k.table() == "pin_jobs") {
        let born = !before
            .iter()
            .any(|(old, _)| old.table() == "pin_jobs" && old.value(0) == k.value(0));
        let operation = cell(j, "operation")?;
        if !matches!(
            cell(j, "state")?,
            "pending" | "retry" | "running" | "blocked"
        ) {
            continue;
        }
        if matches!(action.receipt.binding.request.action, Action::Cancel) && !born {
            continue;
        }
        let lease = native::value(j, "lease_id")
            .is_some_and(|id| action.receipt.intent_ids.iter().any(|i| i == id));
        let target =
            native::value(j, "target_id").is_some_and(|id| action.targets.iter().any(|i| i == id));
        let shared = after
            .iter()
            .filter(|(k, _)| k.table() == "pin_lease_targets")
            .any(|(_, t)| {
                native::value(t, "id").is_some_and(|id| action.targets.iter().any(|i| i == id))
                    && native::value(t, "provider") == native::value(j, "provider")
                    && native::value(t, "cid") == native::value(j, "cid")
            });
        let remote = matches!(action.receipt.binding.request.action, Action::Cancel)
            && before.iter().any(|(k, t)| {
                k.table() == "pin_lease_targets"
                    && native::value(t, "lease_id")
                        .is_some_and(|id| action.receipt.intent_ids.iter().any(|i| i == id))
                    && native::value(t, "provider") == native::value(j, "provider")
                    && native::value(t, "cid") == native::value(j, "cid")
            });
        if matches!(operation, "submit" | "poll") {
            let Some(id) = native::value(j, "lease_id") else {
                continue;
            };
            let Some(l) = after.get(&key("pin_leases", id)) else {
                continue;
            };
            if native::value(j, "expected_generation") != native::value(l, "generation")
                || native::value(l, "state") != Some("active")
            {
                continue;
            }
        }
        let observation = born && operation == "reconcile";
        if matches!(action.receipt.binding.request.action, Action::Renew { .. })
            && !(observation && shared)
        {
            continue;
        }
        let explicit = observation;
        let cleanup = born
            && operation == "unpin"
            && matches!(
                action.receipt.binding.request.action,
                Action::RetireWith {
                    selection: crate::pinning::management::RetirementSelection::ControlledCleanup { .. }
                }
            );
        if explicit
            || cleanup
            || lease
            || target
            || remote
            || shared && matches!(operation, "submit" | "poll")
            || observation && shared
        {
            let born_intent = lease
                && born
                && matches!(
                    action.receipt.binding.request.action,
                    Action::Create(_) | Action::Backfill(_) | Action::Reapply(_)
                );
            if matches!(operation, "submit" | "poll")
                && (matches!(
                    action.receipt.binding.request.action,
                    Action::Cancel | Action::Reconcile
                ) || !born_intent && readonly)
            {
                continue;
            }
            expected.insert(k.value(0));
        }
    }
    if !expected
        .iter()
        .copied()
        .eq(action.receipt.job_ids.iter().map(String::as_str))
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}

fn validate_job_rows(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    a: &records::JobAuthority,
    j: &records::RowHandle,
    i: &records::RowHandle,
) -> Result<()> {
    let operation = cell(j, "operation")?;
    let invocation_epoch = cell(i, "remote_epoch")?
        .parse::<i64>()
        .map_err(|_| ManagementError::Confirmation)?;
    let retained_poll = operation == "poll"
        && a.directive == records::JobDirective::Submit
        && matches!(
            action.receipt.binding.request.action,
            Action::RetireWith {
                selection: crate::pinning::management::RetirementSelection::Migrate { .. }
            }
        );
    if native::value(j, "id") != Some(&a.job_id)
        || native::value(i, "job_id") != Some(&a.job_id)
        || native::value(j, "provider") != Some(&a.provider)
        || native::value(j, "cid") != Some(&a.cid)
        || native::value(j, "target_id") != a.target_id.as_deref()
        || native::value(j, "lease_id").is_some_and(|id| Some(id) != a.lease_id.as_deref())
        || native::value(j, "expected_generation")
            .is_some_and(|v| v.parse::<i64>().ok() != a.lease_generation)
        || native::value(j, "expected_remote_epoch")
            .is_some_and(|v| v.parse::<i64>().ok() != Some(a.remote_epoch))
        || invocation_epoch != a.remote_epoch
            && (!retained_poll || invocation_epoch < a.remote_epoch)
        || binding.decode_route(cell(i, "route")?)?.as_ref() != &a.route
        || !matches!(
            (a.directive, operation),
            (records::JobDirective::Submit, "submit" | "poll")
                | (records::JobDirective::ReadOnlyObserve, "reconcile")
                | (records::JobDirective::ExistingCleanup, "unpin")
        )
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}
pub(super) fn job(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    op: &OperationRead,
    authority: &records::JobAuthority,
) -> Result<()> {
    if !action.jobs.contains(authority) {
        return Err(ManagementError::Confirmation);
    }
    let job_key = key("pin_jobs", &authority.job_id);
    if authority.directive == records::JobDirective::Submit
        && op
            .facts
            .1
            .iter()
            .any(|read| read.0 == job_key && matches!(read.1, facts::Input::Output(..)))
    {
        let original = binding.frozen_rows()?;
        binding
            .with_decode_work(|work| work.nodes(original.all_rows().count() + op.facts.1.len()))?;
        let mut rows = original.all_rows().collect::<Vec<_>>();
        for read in &op.facts.1 {
            let table = native::current_catalog()
                .iter()
                .find(|table| table.table == read.0.0)
                .ok_or(ManagementError::Schema)?;
            rows.retain(|(old_table, row)| {
                old_table.table != table.table
                    || !table
                        .primary_key
                        .iter()
                        .zip(&read.0.1)
                        .all(|(column, value)| native::value(*row, column) == Some(value))
            });
            if let Some(row) = op.before(&read.0)? {
                rows.push((table, records::RowView::Handle(row)));
            }
        }
        let boundary = records::ExecutionRows {
            scope: binding.database_scope,
            identity: binding.database_identity,
            rows,
        };
        return job_authority::validate_authority_at_boundary(
            binding,
            action,
            authority,
            records::FrozenRows::Execution(&boundary),
        );
    }
    if matches!(action.receipt.binding.request.action, Action::Renew { .. }) {
        let step = view(binding, action, op)?;
        let boundary = step
            .resources
            .iter()
            .find(|b| b.after.key.as_ref() == &authority.resource_key)
            .ok_or(ManagementError::Confirmation)?;
        let birth = step
            .births
            .iter()
            .find(|b| native::value(b.0, "id") == Some(&authority.job_id))
            .ok_or(ManagementError::Confirmation)?;
        return job_authority::renewal::validate_observation(
            binding,
            action,
            authority,
            &step.outcome,
            &boundary.before,
            &boundary.after,
            birth.0,
            birth.1,
        );
    }
    if matches!(
        action.receipt.binding.request.action,
        Action::Migrate { .. }
    ) && authority.directive == records::JobDirective::ReadOnlyObserve
    {
        let k = Key(
            authority.resource_key.table.clone(),
            authority.resource_key.values.clone(),
        );
        if let (Some(pre), Some(post)) = (op.before(&k)?, op.after(&k)?) {
            let frame = binding.frozen_rows()?;
            let original = frame
                .row(&k.0, &k.1.iter().map(String::as_str).collect::<Vec<_>>())
                .ok_or(ManagementError::Confirmation)?;
            if authority.remote_epoch != epoch(&original)? {
                if epoch(pre)?.checked_add(1) != Some(authority.remote_epoch)
                    || epoch(post)? != authority.remote_epoch
                    || native::value(post, "request_id") != authority.resource_id.as_deref()
                {
                    return Err(ManagementError::Confirmation);
                }
                binding.copy(authority)?;
                let mut frozen = authority.clone();
                frozen.remote_epoch = epoch(&original)?;
                return job_authority::validate_frozen_authority(binding, action, &frozen);
            }
        }
    }
    job_authority::validate_frozen_authority(binding, action, authority)
}
