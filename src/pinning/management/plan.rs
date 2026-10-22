use std::collections::{BTreeMap, BTreeSet};

use super::{capture::capture, model::*, semantics::evaluate, summary::summarize, validate::*};

#[derive(Clone)]
pub struct DryRunPlan {
    plan_id: String,
    items: Vec<PlanEntry>,
    summary: Summary,
    pub(crate) retirement_sources: Vec<std::sync::Arc<super::retirement::InventorySpool>>,
    pub(crate) retirement_union: Option<std::sync::Arc<super::retirement::PlanUnion>>,
}

impl PartialEq for DryRunPlan {
    fn eq(&self, other: &Self) -> bool {
        self.plan_id == other.plan_id && self.items == other.items && self.summary == other.summary
    }
}
impl Eq for DryRunPlan {}

impl DryRunPlan {
    pub(crate) fn from_canonical(plan_id: String, items: Vec<PlanEntry>, summary: Summary) -> Self {
        Self {
            plan_id,
            items,
            summary,
            retirement_sources: Vec::new(),
            retirement_union: None,
        }
    }
    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }
    pub fn items(&self) -> &[PlanEntry] {
        &self.items
    }
    pub fn summary(&self) -> &Summary {
        &self.summary
    }
    /// Capture the exact reviewed plan. The parent records who confirmed it.
    pub fn confirmation(&self, candidate_item_ids: Vec<String>) -> Confirmation {
        Confirmation { candidate_item_ids }
    }
}

/// Provider and mixed plans consume sealed retirement evidence independently of
/// the bounded local Snapshot. `None` is resumable work, never an executable plan.
/// Confirmation requires the shared task retained by `PreparedPlanning`; standalone
/// inventories remain usable for review without acquiring confirmation authority.
pub async fn dry_run_retirement(
    request: &PlanRequest,
    local: &Snapshot,
    inventories: &[super::retirement::RetirementReview<'_>],
    mutations: &[(String, super::retirement::RetirementMutationScope)],
    union: &super::retirement::PlanUnion,
    limits: &super::retirement::RunLimits,
) -> Result<Option<DryRunPlan>, SafeError> {
    use crate::store::pinning::management::ManagementError;
    let map = |error| match error {
        ManagementError::Budget => SafeError::LimitExceeded,
        ManagementError::Changed => SafeError::SnapshotChanged,
        _ => SafeError::UnknownEvidence,
    };
    validate_request(request)?;
    validate_snapshot(local)?;
    let work: crate::store::pinning::management::records::SharedDecodeBudget = Default::default();
    let points = planning_points_with_work(local, work.clone())?;
    let view = SnapshotView {
        work: Some(&work),
        ..SnapshotView::from(local)
    };
    let mut items = Vec::with_capacity(request.items.len());
    let mut budget = Budget::default();
    for item in &request.items {
        let id = IdempotencyId(format!(
            "management-v1:{}:{}",
            request.plan_id, item.item_id
        ));
        let entry = if let Some(applied) = local.applied.iter().find(|a| a.binding.id == id) {
            if applied.binding.request != *item {
                return Err(SafeError::IdempotencyConflict);
            }
            applied.binding.clone()
        } else if let Subject::Provider { route } = &item.subject {
            let review = inventories
                .iter()
                .find(|r| &r.checkpoint().route == route)
                .ok_or(SafeError::IncompleteInventory)?;
            let mutation = mutations
                .iter()
                .find(|(id, _)| id == &item.item_id)
                .map(|(_, m)| m.clone())
                .unwrap_or_default();
            let captured = review.capture(item, mutation).await.map_err(map)?;
            if matches!(
                item.action,
                Action::RetireWith {
                    selection: RetirementSelection::ReleaseRootReferences { .. }
                }
            ) && super::root_references::selected(item, &view)?
                != captured.mutation_scope.root_references
            {
                return Err(SafeError::UnknownEvidence);
            }
            super::capture::validate_retirement_selection(item, &captured, &view, &points)?;
            PlanEntry {
                id,
                request: item.clone(),
                status: PlanStatus::Ready,
                step: Some(PlannedStep::Retire(review.step())),
                capture: Some(Capture::Provider(Box::new(captured))),
            }
        } else {
            build_entry(&request.plan_id, item, &view, &points)
        };
        budget.entry(&entry)?;
        items.push(entry);
    }
    items.sort_by(|a, b| a.request.item_id.cmp(&b.request.item_id));
    check_overlaps(&items)?;
    union.bind(&items).await.map_err(map)?;
    for review in inventories {
        if union.merge_inventory(review, limits).await.map_err(map)?
            == super::retirement::Progress::Pending
        {
            return Ok(None);
        }
    }
    let local_entries = items
        .iter()
        .filter(|entry| !matches!(entry.request.subject, Subject::Provider { .. }))
        .cloned()
        .collect::<Vec<_>>();
    let mut base = summarize(&local_entries, &view)?;
    base.items = items.len();
    base.unique_cids = 0;
    base.unique_resources = 0;
    base.possible_new_transfers = 0;
    base.accounted_remote_bytes = 0;
    base.unknown_resources = 0;
    base.inflight_resources = 0;
    base.cleanup_pending_resources = 0;
    for entry in &items {
        if let Some(Capture::Provider(c)) = &entry.capture {
            base.logical_targets += c.totals.targets;
            base.logical_intent_bytes = base
                .logical_intent_bytes
                .checked_add(c.totals.logical_bytes)
                .ok_or(SafeError::LimitExceeded)?;
            if local.applied.iter().any(|a| a.binding == *entry) {
                base.already_applied += 1;
            }
        }
    }
    union.merge_local(&items, local).await.map_err(map)?;
    let Some(summary) = union.finish(base, limits).await.map_err(map)? else {
        return Ok(None);
    };
    Ok(Some(DryRunPlan {
        plan_id: request.plan_id.clone(),
        items,
        summary,
        retirement_sources: inventories
            .iter()
            .map(|review| std::sync::Arc::new(review.retain_source()))
            .collect(),
        retirement_union: Some(union.retain()),
    }))
}

fn build_entry(
    plan_id: &str,
    request: &ItemRequest,
    snapshot: &SnapshotView<'_>,
    points: &[crate::store::pinning::management::records::MigrationPoint],
) -> PlanEntry {
    let mut entry = PlanEntry {
        id: IdempotencyId(format!("management-v1:{plan_id}:{}", request.item_id)),
        request: request.clone(),
        status: PlanStatus::Ready,
        capture: None,
        step: None,
    };
    match capture(request, snapshot, points) {
        Ok(captured) => {
            match super::capture::slot_executions(request, &captured, snapshot, points).and_then(
                |slots| {
                    evaluate(
                        request,
                        &captured,
                        &slots,
                        snapshot.database_now.unwrap_or_else(|| {
                            chrono::DateTime::from_timestamp(snapshot.observed_at, 0)
                                .expect("validated database clock")
                        }),
                    )
                },
            ) {
                Ok(mut step) => {
                    if let PlannedStep::Retire(retirement) = &mut step {
                        // Unprojectable native PKs still own responsibilities;
                        // lack of an epoch is not an empty retirement inventory.
                        retirement.unknown += snapshot.unknown_resources.len();
                        retirement.needs_attention |= !snapshot.unknown_resources.is_empty();
                    }
                    entry.step = Some(step);
                    if matches!(&captured, Capture::Object(capture)
                        if capture.object.evidence == SelectionEvidence::LegacyZipCandidate)
                    {
                        entry.status = PlanStatus::Candidate;
                    }
                }
                Err(error) => entry.status = PlanStatus::Blocked(error),
            }
            entry.capture = Some(captured);
        }
        Err(error) => entry.status = PlanStatus::Blocked(error),
    }
    entry
}

/// Rebuilds a frozen provider/mixed review from its original bounded local
/// closure and disk inventories. No current DB clock, configuration or status
/// is consulted, and the returned plan is not an execution capability.
pub struct FrozenRetirementPlan<'a> {
    pub request: &'a PlanRequest,
    pub local: &'a Snapshot,
    pub mutations: &'a [(String, super::retirement::RetirementMutationScope)],
    pub entries: &'a [PlanEntry],
    pub summary: Summary,
}
pub async fn restore_retirement_review(
    frozen: FrozenRetirementPlan<'_>,
    inventories: &[super::retirement::RetirementReview<'_>],
    union: &super::retirement::PlanUnion,
    limits: &super::retirement::RunLimits,
) -> Result<Option<DryRunPlan>, SafeError> {
    let Some(plan) = dry_run_retirement(
        frozen.request,
        frozen.local,
        inventories,
        frozen.mutations,
        union,
        limits,
    )
    .await?
    else {
        return Ok(None);
    };
    if plan.items != frozen.entries || plan.summary != frozen.summary {
        return Err(SafeError::ConfirmationMismatch);
    }
    Ok(Some(plan))
}

fn check_overlaps(entries: &[PlanEntry]) -> Result<(), SafeError> {
    let mut subjects = BTreeSet::new();
    let mut leases = BTreeMap::new();
    let retired: BTreeSet<_> = entries
        .iter()
        .filter_map(|entry| match &entry.request.subject {
            Subject::Provider { route } => Some(&route.provider_id),
            _ => None,
        })
        .collect();
    let retired_domains: BTreeSet<_> = entries
        .iter()
        .filter_map(|entry| match &entry.request.subject {
            Subject::Provider { route } => {
                Some((&route.backend, &route.scope, route.resource_type))
            }
            _ => None,
        })
        .collect();
    let mut retiring = BTreeSet::new();
    let mut released_roots = BTreeSet::new();
    for entry in entries {
        if let Action::RetireWith {
            selection: RetirementSelection::ReleaseRootReferences { references },
        } = &entry.request.action
        {
            for key in references {
                if !released_roots.insert(key) {
                    return Err(SafeError::DuplicateIdentity);
                }
            }
        }
        match &entry.request.subject {
            Subject::Provider { route } => {
                if !retiring.insert(&route.provider_id) {
                    return Err(SafeError::DuplicateIdentity);
                }
            }
            Subject::Object { identity, target } => {
                let key = match target {
                    TargetSelection::New(route) => (
                        0,
                        identity.private_version_id.clone(),
                        Some(route.remote_key(&identity.cid)),
                    ),
                    TargetSelection::Existing { target_id, .. } => (1, target_id.clone(), None),
                };
                if !subjects.insert(key) {
                    return Err(SafeError::DuplicateIdentity);
                }
            }
        }
        if let Some(Capture::Object(capture)) = &entry.capture {
            if retired.contains(&capture.route.identity.provider_id)
                || std::iter::once(&capture.route)
                    .chain(capture.destination_route.iter())
                    .any(|route| {
                        retired_domains.contains(&(
                            &route.identity.backend,
                            &route.identity.scope,
                            route.identity.resource_type,
                        ))
                    })
                || capture
                    .destination_route
                    .as_ref()
                    .is_some_and(|route| retired.contains(&route.identity.provider_id))
            {
                return Err(SafeError::InvalidActionScope);
            }
            if let Some(target) = &capture.target {
                let mutation =
                    matches!(entry.request.action, Action::Cancel | Action::Renew { .. });
                if leases
                    .insert(&target.lease.id, mutation)
                    .is_some_and(|previous| previous || mutation)
                {
                    return Err(SafeError::InvalidActionScope);
                }
            }
        }
    }
    Ok(())
}

/// Read-only planning. Selection is exact; no prefix scan, tag replay, or IO.
pub fn dry_run(request: &PlanRequest, snapshot: &Snapshot) -> Result<DryRunPlan, SafeError> {
    dry_run_with_work(request, snapshot, Default::default())
}
pub fn dry_run_with_work(
    request: &PlanRequest,
    snapshot: &Snapshot,
    work: crate::store::pinning::management::records::SharedDecodeBudget,
) -> Result<DryRunPlan, SafeError> {
    let points = planning_points_with_work(snapshot, work.clone())?;
    dry_run_view_with_work(request, snapshot.into(), &points, work)
}

pub(crate) fn planning_points_with_work(
    snapshot: &Snapshot,
    work: crate::store::pinning::management::records::SharedDecodeBudget,
) -> Result<Vec<crate::store::pinning::management::records::MigrationPoint>, SafeError> {
    let Some(frame) = &snapshot.native_frame else {
        return Ok(vec![]);
    };
    // A public imported frame carries no private applied authority. File
    // projections are supplied separately and compared with the stored plan.
    if !frame.has_private_material() {
        return Ok(vec![]);
    }
    crate::store::pinning::management::actions::points::planning_points_with_work(frame, work)
        .map_err(|error| {
            if error == crate::store::pinning::management::ManagementError::Budget {
                SafeError::LimitExceeded
            } else {
                SafeError::UnknownEvidence
            }
        })
}

pub(crate) fn dry_run_view_with_work(
    request: &PlanRequest,
    snapshot: SnapshotView<'_>,
    points: &[crate::store::pinning::management::records::MigrationPoint],
    work: crate::store::pinning::management::records::SharedDecodeBudget,
) -> Result<DryRunPlan, SafeError> {
    let snapshot = SnapshotView {
        work: Some(&work),
        ..snapshot
    };
    validate_planning_inputs(request, snapshot, points, &work)?;
    let mut items = Vec::with_capacity(request.items.len());
    let mut output_budget = Budget::default();
    for request_item in &request.items {
        let id = format!("management-v1:{}:{}", request.plan_id, request_item.item_id);
        if let Some(receipt) = snapshot
            .applied
            .iter()
            .find(|receipt| receipt.binding.id.as_str() == id)
        {
            if receipt.binding.request != *request_item {
                return Err(SafeError::IdempotencyConflict);
            }
            items.push(receipt.binding.clone());
        } else {
            items.push(build_entry(
                &request.plan_id,
                request_item,
                &snapshot,
                points,
            ));
        }
        output_budget.entry(items.last().ok_or(SafeError::InvalidInput)?)?;
    }
    items.sort_by(|a, b| a.request.item_id.cmp(&b.request.item_id));
    check_overlaps(&items)?;
    let summary = summarize(&items, &snapshot)?;
    work.lock()
        .map_err(|_| SafeError::InvalidInput)?
        .copy(&request.plan_id)
        .map_err(|_| SafeError::LimitExceeded)?;
    Ok(DryRunPlan {
        plan_id: request.plan_id.clone(),
        items,
        summary,
        retirement_sources: Vec::new(),
        retirement_union: None,
    })
}

fn validate_planning_inputs(
    request: &PlanRequest,
    snapshot: SnapshotView<'_>,
    points: &[crate::store::pinning::management::records::MigrationPoint],
    work: &crate::store::pinning::management::records::SharedDecodeBudget,
) -> Result<(), SafeError> {
    validate_request(request)?;
    super::validate_snapshot_view_with_work(
        snapshot,
        &mut *work.lock().map_err(|_| SafeError::InvalidInput)?,
    )?;
    if points.len() > MAX_RECORDS || (!points.is_empty() && snapshot.rows.is_none()) {
        return Err(SafeError::LimitExceeded);
    }
    for (index, point) in points.iter().enumerate() {
        if points[..index].iter().any(|prior| {
            (&prior.lease_id, &prior.cid, &prior.slot) == (&point.lease_id, &point.cid, &point.slot)
        }) || point.table != "pin_lease_requirements"
            || point.primary_key.table != point.table
            || point.primary_key.values.len() != 1
            || point.primary_key.values.first() != Some(&point.lease_id)
            || point.capture_position.strip_prefix("candidates/") != Some(point.slot.as_str())
            || point.expires_at <= point.started_at
        {
            return Err(SafeError::InvalidInput);
        }
        validate_route_identity(&RouteIdentity::from_original(point.source_route.clone()))?;
        validate_route_identity(&RouteIdentity::from_original(
            point.destination_route.clone(),
        ))?;
    }
    Ok(())
}

/// Binding validation alone grants no permission and does not check current state.
pub(crate) fn validate_confirmation<'a>(
    plan: &DryRunPlan,
    confirmation: &'a Confirmation,
) -> Result<&'a [String], SafeError> {
    validate_acks(plan, &confirmation.candidate_item_ids)
}
pub(crate) fn validate_acks<'a>(
    plan: &DryRunPlan,
    acks: &'a [String],
) -> Result<&'a [String], SafeError> {
    validate_entry_acks(&plan.items, acks)
}
fn validate_entry_acks<'a>(
    entries: &[PlanEntry],
    acks: &'a [String],
) -> Result<&'a [String], SafeError> {
    if acks.len() > MAX_ITEMS {
        return Err(SafeError::ConfirmationMismatch);
    }
    for (index, id) in acks.iter().enumerate() {
        label(id)?;
        if acks[..index].contains(id)
            || !entries
                .iter()
                .any(|entry| entry.request.item_id == *id && entry.status == PlanStatus::Candidate)
        {
            return Err(SafeError::ConfirmationMismatch);
        }
    }
    Ok(acks)
}

/// All-or-nothing preflight against a newly supplied authoritative snapshot.
/// This is not an execution permit: parent must recheck under durable CAS/claim.
pub fn revalidate(
    plan: &DryRunPlan,
    confirmation: &Confirmation,
    current: &Snapshot,
) -> Result<Vec<CheckedItem>, SafeError> {
    validate_snapshot(current)?;
    let acknowledged = validate_confirmation(plan, confirmation)?;
    revalidate_entries(
        &plan.plan_id,
        &plan.items,
        acknowledged,
        current,
        Default::default(),
    )
}

pub(crate) fn revalidate_review(
    binding: crate::store::pinning::management::records::BindingView<'_>,
    current: &Snapshot,
) -> Result<Vec<CheckedItem>, SafeError> {
    super::validate_snapshot_view_with_work(
        current.into(),
        &mut *binding
            .decode_work()
            .budget()
            .lock()
            .map_err(|_| SafeError::InvalidInput)?,
    )?;
    let original = binding
        .frozen_rows()
        .map_err(|_| SafeError::SnapshotChanged)?;
    let fresh = current
        .native_frame
        .as_ref()
        .ok_or(SafeError::SnapshotChanged)?;
    if original.scope() != fresh.database_scope
        || original.identity() != fresh.database_identity
        || !original
            .all_rows()
            .map(|(table, row)| (&table.table, row))
            .eq(
                crate::store::pinning::management::records::FrozenRows::Frame(fresh)
                    .all_rows()
                    .map(|(table, row)| (&table.table, row)),
            )
        || binding.confirmation.configuration != &current.configuration
    {
        return Err(SafeError::SnapshotChanged);
    }
    let acknowledged = validate_entry_acks(
        binding.confirmation.bindings,
        binding.confirmation.candidate_item_ids,
    )?;
    revalidate_entries(
        binding.confirmation.plan_id,
        binding.confirmation.bindings,
        acknowledged,
        current,
        binding.decode_work().budget().clone(),
    )
}

fn revalidate_entries(
    plan_id: &str,
    entries: &[PlanEntry],
    acknowledged: &[String],
    current: &Snapshot,
    work: crate::store::pinning::management::records::SharedDecodeBudget,
) -> Result<Vec<CheckedItem>, SafeError> {
    let mut checked = Vec::with_capacity(entries.len());
    let points = planning_points_with_work(current, work.clone())?;
    let view = SnapshotView {
        work: Some(&work),
        ..current.into()
    };
    for entry in entries {
        if let Some(receipt) = current
            .applied
            .iter()
            .find(|receipt| receipt.binding.id == entry.id)
        {
            if receipt.binding != *entry {
                return Err(SafeError::IdempotencyConflict);
            }
            checked.push(CheckedItem {
                id: entry.id.clone(),
                outcome: RevalidationOutcome::AlreadyApplied,
            });
            continue;
        }
        if matches!(entry.status, PlanStatus::Blocked(_)) {
            return Err(SafeError::PlanBlocked);
        }
        if entry.status == PlanStatus::Candidate && !acknowledged.contains(&entry.request.item_id) {
            return Err(SafeError::CandidateConfirmationRequired);
        }
        let fresh = if let Some(Capture::Provider(capture)) = &entry.capture {
            let frame = current
                .native_frame
                .as_ref()
                .ok_or(SafeError::SnapshotChanged)?;
            crate::store::pinning::management::records::validate_retirement_entry(
                &work,
                &frame.database_scope,
                &frame.database_identity,
                plan_id,
                entry,
            )
            .map_err(|_| SafeError::IncompleteInventory)?;
            super::capture::validate_retirement_selection(&entry.request, capture, &view, &points)?;
            entry.clone()
        } else {
            build_entry(plan_id, &entry.request, &view, &points)
        };
        if fresh != *entry {
            return Err(SafeError::SnapshotChanged);
        }
        let step = fresh.step.ok_or(SafeError::PlanBlocked)?;
        checked.push(CheckedItem {
            id: entry.id.clone(),
            outcome: RevalidationOutcome::Checked(step),
        });
    }
    Ok(checked)
}
