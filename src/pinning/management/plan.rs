use std::collections::{BTreeMap, BTreeSet};

use super::{capture::capture, model::*, semantics::evaluate, summary::summarize, validate::*};

#[derive(Clone, PartialEq, Eq)]
pub struct DryRunPlan {
    plan_id: String,
    items: Vec<PlanEntry>,
    summary: Summary,
    native_frame: Option<NativeFrame>,
    configuration: Option<ConfigurationBinding>,
}

impl DryRunPlan {
    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }
    pub fn items(&self) -> &[PlanEntry] {
        &self.items
    }
    pub fn summary(&self) -> &Summary {
        &self.summary
    }
    pub fn native_frame(&self) -> Option<&NativeFrame> { self.native_frame.as_ref() }

    /// Capture the exact reviewed plan. The parent records who confirmed it.
    pub fn confirmation(&self, candidate_item_ids: Vec<String>) -> Confirmation {
        Confirmation {
            plan_id: self.plan_id.clone(),
            bindings: self.items.clone(),
            candidate_item_ids,
            native_frame: self.native_frame.clone(),
            configuration: self.configuration.clone(),
        }
    }
}

fn build_entry(plan_id: &str, request: &ItemRequest, snapshot: &Snapshot, points:&[crate::store::pinning::management::records::MigrationPoint]) -> PlanEntry {
    let mut entry = PlanEntry {
        id: IdempotencyId(format!("management-v1:{plan_id}:{}", request.item_id)),
        request: request.clone(),
        status: PlanStatus::Ready,
        capture: None,
        step: None,
    };
    match capture(request, snapshot,points) {
        Ok(captured) => {
            match evaluate(request, &captured, snapshot.database_now.unwrap_or_else(||chrono::DateTime::from_timestamp(snapshot.observed_at,0).expect("validated database clock"))) {
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
    let mut retiring = BTreeSet::new();
    for entry in entries {
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
    let points=planning_points(snapshot)?;
    dry_run_with_points(request,snapshot,&points)
}

pub(crate) fn planning_points(snapshot:&Snapshot)->Result<Vec<crate::store::pinning::management::records::MigrationPoint>,SafeError> {
    let Some(frame)=&snapshot.native_frame else{return Ok(vec![])};
    // A public imported frame carries no private applied authority. File
    // projections are supplied separately and compared with the stored plan.
    if !frame.has_private_material() {return Ok(vec![]);}
    crate::store::pinning::management::actions::points::planning_points(frame).map_err(|_|SafeError::UnknownEvidence)
}

pub(crate) fn dry_run_with_points(request:&PlanRequest,snapshot:&Snapshot,points:&[crate::store::pinning::management::records::MigrationPoint])->Result<DryRunPlan,SafeError> {
    validate_request(request)?;
    validate_snapshot(snapshot)?;
    if points.len()>MAX_RECORDS||(!points.is_empty()&&snapshot.native_frame.is_none()) {return Err(SafeError::LimitExceeded);}
    let mut coordinates=BTreeSet::new();
    for point in points {
        if !coordinates.insert((&point.lease_id,&point.cid,&point.slot))||point.table!="pin_lease_requirements"||point.primary_key.table!=point.table||point.primary_key.values!=vec![point.lease_id.clone()]
            || point.capture_position!=format!("candidates/{}",point.slot)||point.expires_at<=point.started_at
        {return Err(SafeError::InvalidInput);}
        validate_route_identity(&RouteIdentity::from_original(point.source_route.clone()))?;
        validate_route_identity(&RouteIdentity::from_original(point.destination_route.clone()))?;
    }
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
            items.push(build_entry(&request.plan_id, request_item, snapshot,points));
        }
        output_budget.entry(items.last().ok_or(SafeError::InvalidInput)?)?;
    }
    items.sort_by(|a, b| a.request.item_id.cmp(&b.request.item_id));
    check_overlaps(&items)?;
    let summary = summarize(&items, snapshot)?;
    Ok(DryRunPlan {
        plan_id: request.plan_id.clone(),
        items,
        summary,
        native_frame: snapshot.native_frame.clone(),
        configuration: snapshot.configuration.clone(),
    })
}

/// Binding validation alone grants no permission and does not check current state.
pub(crate) fn validate_confirmation<'a>(
    plan: &DryRunPlan,
    confirmation: &'a Confirmation,
) -> Result<BTreeSet<&'a str>, SafeError> {
    if confirmation.plan_id != plan.plan_id
        || confirmation.native_frame != plan.native_frame
        || confirmation.configuration != plan.configuration
        || confirmation.bindings != plan.items
        || confirmation.candidate_item_ids.len() > MAX_ITEMS
    {
        return Err(SafeError::ConfirmationMismatch);
    }
    let mut acknowledged = BTreeSet::new();
    for id in &confirmation.candidate_item_ids {
        label(id)?;
        if !acknowledged.insert(id.as_str())
            || !plan
                .items
                .iter()
                .any(|entry| entry.request.item_id == *id && entry.status == PlanStatus::Candidate)
        {
            return Err(SafeError::ConfirmationMismatch);
        }
    }
    Ok(acknowledged)
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
    if plan.native_frame != current.native_frame || plan.configuration != current.configuration { return Err(SafeError::SnapshotChanged); }
    let mut checked = Vec::with_capacity(plan.items.len());
    let points=planning_points(current)?;
    for entry in &plan.items {
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
        if entry.status == PlanStatus::Candidate
            && !acknowledged.contains(entry.request.item_id.as_str())
        {
            return Err(SafeError::CandidateConfirmationRequired);
        }
        let fresh = build_entry(&plan.plan_id, &entry.request, current,&points);
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
