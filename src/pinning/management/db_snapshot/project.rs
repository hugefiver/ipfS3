use super::super::{
    CleanupMode, ConfirmationLevel, Ownership, ProviderMode, SideEffect, Subject, TargetSelection,
};
use super::*;
use crate::{
    pinning::{identity, provider::SubmitEffect},
    store::{entities::remote_pin_ledger, pinning::ledger::submission::ResourceEvidence},
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
struct HistoricalEnvelope {
    ledger: remote_pin_ledger::Model,
    request_id: Option<String>,
    confirmed_absent: bool,
}

fn ownership(
    value: &str,
    route: Option<&RouteIdentity>,
    gaps: &mut BTreeSet<EvidenceGap>,
) -> Ownership {
    match value {
        "external_existing" => Ownership::ExternalExisting,
        "application_created"
            if route.is_some_and(|route| route.resource_type != ResourceType::RpcPin) =>
        {
            Ownership::ApplicationCreated
        }
        "application_created" => {
            gaps.insert(EvidenceGap::OwnershipProofUnavailable);
            Ownership::Unknown
        }
        _ => Ownership::Unknown,
    }
}

fn ledger_state(value: &str) -> LedgerState {
    match value {
        "reserved" => LedgerState::Reserved,
        "not_created" => LedgerState::NotCreated,
        "confirmed" => LedgerState::Confirmed,
        "retained" => LedgerState::Retained,
        "cleanup_pending" => LedgerState::CleanupPending,
        "absent" => LedgerState::Absent,
        _ => LedgerState::Unknown,
    }
}

fn effect(ledger: LedgerState, inflight: bool) -> SideEffect {
    if inflight {
        return SideEffect::InFlight;
    }
    match ledger {
        LedgerState::NotCreated => SideEffect::Rejected,
        // An absent invocation row is not proof that no old dispatch happened.
        LedgerState::Reserved => SideEffect::Unknown,
        LedgerState::Confirmed | LedgerState::Retained | LedgerState::CleanupPending => {
            SideEffect::Present
        }
        _ => SideEffect::Unknown,
    }
}

fn state(value: &str) -> StoredTargetState {
    match value {
        "waiting" => StoredTargetState::Waiting,
        "submitted" => StoredTargetState::Submitted,
        "pinned" => StoredTargetState::Pinned,
        "degraded" => StoredTargetState::Degraded,
        "quota_waiting" => StoredTargetState::QuotaWaiting,
        "quota_blocked" => StoredTargetState::QuotaBlocked,
        "evicted" => StoredTargetState::Evicted,
        "released" => StoredTargetState::Released,
        _ => StoredTargetState::Unknown,
    }
}

fn route_evidence(
    raw: &ProviderRouteSnapshot,
    config: &ValidatedPinningConfig,
    registered: bool,
    retired: bool,
) -> RouteEvidence {
    // Exact historical administrator revisions incl. secret reference must match.
    // This is local adapter/config availability, NOT live account capability.
    let configured = config
        .providers
        .iter()
        .find(|provider| provider.route_snapshot() == *raw);
    let available = super::super::local_config::historical_route_available(raw, config);
    RouteEvidence {
        identity: route(raw),
        available,
        allocation_allowed: available
            && registered
            && !retired
            && configured
                .is_some_and(|provider| !provider.identity.retired && provider.limits.enabled),
        cleanup: match raw.cleanup {
            identity::CleanupMode::Retain => CleanupMode::Retain,
            identity::CleanupMode::Managed => CleanupMode::Managed,
        },
        exclusive_management: None,
    }
}

pub(super) fn assemble(
    observed_at: i64,
    objects: Vec<ObjectEvidence>,
    rows: read::Rows,
    config: &ValidatedPinningConfig,
    request: &PlanRequest,
    work: &SharedDecodeBudget,
) -> Result<DatabaseSnapshot> {
    let mut gaps = BTreeSet::new();
    if !objects.is_empty() {
        gaps.extend([
            EvidenceGap::StoredByteSafetyUnavailable,
            EvidenceGap::ReadabilityNotProbed,
        ]);
        if objects
            .iter()
            .any(|object| object.control_revision.is_none())
        {
            gaps.insert(EvidenceGap::DecisionRevisionUnavailable);
        }
    }
    let mut selected_routes = Vec::new();
    let mut allocations = BTreeSet::new();
    let mut provider_inventory = false;
    for item in &request.items {
        match &item.subject {
            Subject::Object { identity, target } => match target {
                TargetSelection::New(route) => selected_routes.push(route.clone()),
                TargetSelection::Existing { target_id, source } => {
                    let target = rows
                        .targets
                        .iter()
                        .find(|target| target.id == *target_id)
                        .ok_or(SnapshotError::MissingTarget)?;
                    let lease = rows
                        .leases
                        .iter()
                        .find(|lease| lease.id == target.lease_id)
                        .ok_or(SnapshotError::InvalidEvidence)?;
                    if lease.owner_object_id != identity.object_id
                        || source.as_ref().is_some_and(|source| {
                            canonical(&target.cid).ok().as_deref() != Some(&source.cid)
                        })
                    {
                        return Err(SnapshotError::MissingTarget);
                    }
                    allocations.insert(target.provider.clone());
                }
            },
            Subject::Provider { route } => {
                provider_inventory = true;
                selected_routes.push(route.clone());
            }
        }
        if let Action::Migrate { destination } = &item.action {
            selected_routes.push(destination.as_ref().clone());
        }
    }
    let selected_ids: BTreeSet<_> = request
        .items
        .iter()
        .flat_map(|item| match &item.action {
            Action::Create(intent) | Action::Reapply(intent) | Action::Backfill(intent) => {
                intent.requirements.provider_ids.clone()
            }
            _ => vec![],
        })
        .chain(
            selected_routes
                .iter()
                .map(|route| route.provider_id.clone()),
        )
        .collect();
    let selected_domain = |raw: &ProviderRouteSnapshot| {
        selected_ids.contains(&raw.provider_id)
            || selected_routes.iter().any(|selected| {
                selected.backend == raw.backend
                    && selected.scope == raw.scope
                    && selected.resource_type == resource_type(raw.resource_type())
            })
    };
    for provider in &config.providers {
        if selected_domain(&provider.identity.route_snapshot()) {
            allocations.insert(provider.identity.allocation_key());
        }
    }
    let mut registered = BTreeMap::new();
    for row in &rows.routes {
        let raw = decode::<ProviderRouteSnapshot>(&row.snapshot, work)?;
        if selected_domain(&raw) {
            allocations.insert(row.provider.clone());
        }
        registered.insert(row.provider.clone(), (raw, row.retired));
    }
    let mut ledger_routes = BTreeMap::new();
    for row in &rows.ledgers {
        let raw = row
            .route
            .as_deref()
            .and_then(|json| decode::<ProviderRouteSnapshot>(json, work).ok());
        if raw.as_deref().is_some_and(&selected_domain) {
            allocations.insert(row.provider.clone());
        }
        ledger_routes.insert((row.provider.clone(), row.cid.clone()), raw);
    }
    let mut historical = Vec::new();
    for row in &rows.history {
        let Ok(envelope) = decode::<HistoricalEnvelope>(&row.ledger, work) else {
            gaps.insert(EvidenceGap::HistoricalRouteUnavailable);
            continue;
        };
        if envelope.ledger.provider != row.provider
            || envelope.ledger.cid != row.cid
            || row.epoch < 0
        {
            gaps.insert(EvidenceGap::HistoricalRouteUnavailable);
            continue;
        }
        let raw = envelope
            .ledger
            .route
            .as_deref()
            .and_then(|json| decode::<ProviderRouteSnapshot>(json, work).ok());
        if raw.as_deref().is_some_and(&selected_domain) {
            allocations.insert(row.provider.clone());
        }
        historical.push((row, envelope, raw));
    }
    let mut invocation_routes = BTreeMap::new();
    for row in &rows.invocations {
        let raw = decode::<ProviderRouteSnapshot>(&row.route, work)?;
        if selected_domain(&raw)
            && let Some(job) = rows.jobs.iter().find(|job| job.id == row.job_id)
        {
            allocations.insert(job.provider.clone());
        }
        invocation_routes.insert(row.job_id.clone(), (raw, row.remote_epoch));
    }
    let mut submission_routes = BTreeMap::new();
    for row in &rows.submissions {
        let raw = decode::<ProviderRouteSnapshot>(&row.route, work)?;
        if selected_domain(&raw) {
            allocations.insert(row.provider.clone());
        }
        submission_routes.insert(row.id.clone(), raw);
    }
    let object_ids: BTreeSet<_> = objects
        .iter()
        .map(|object| object.identity.object_id.clone())
        .collect();
    // Keep all sibling responsibilities of each explicitly selected object.
    for target in &rows.targets {
        if rows
            .leases
            .iter()
            .any(|lease| lease.id == target.lease_id && object_ids.contains(&lease.owner_object_id))
        {
            allocations.insert(target.provider.clone());
        }
    }
    let mut routes: BTreeMap<RouteIdentity, RouteEvidence> = BTreeMap::new();
    for (allocation, (raw, retired)) in &registered {
        if allocations.contains(allocation) {
            let evidence = route_evidence(raw, config, true, *retired);
            routes.insert(evidence.identity.clone(), evidence);
        }
    }
    // Historical availability never silently inherits the newest route revision.
    for raw in ledger_routes
        .iter()
        .filter(|((allocation, _), _)| allocations.contains(allocation))
        .filter_map(|(_, route)| route.as_ref())
        .chain(
            historical
                .iter()
                .filter(|(row, _, _)| allocations.contains(&row.provider))
                .filter_map(|(_, _, route)| route.as_ref()),
        )
        .chain(
            rows.jobs
                .iter()
                .filter(|job| allocations.contains(&job.provider))
                .filter_map(|job| invocation_routes.get(&job.id).map(|(route, _)| route)),
        )
        .chain(
            rows.submissions
                .iter()
                .filter(|row| allocations.contains(&row.provider))
                .filter_map(|row| submission_routes.get(&row.id)),
        )
    {
        let evidence = route_evidence(raw, config, false, true);
        if !evidence.available {
            gaps.insert(EvidenceGap::HistoricalRouteUnavailable);
        }
        routes
            .entry(evidence.identity.clone())
            .and_modify(|current| {
                // Redacted secret-reference disagreements cannot be hidden by an
                // identical public route projection. Never publish a false positive.
                current.available &= evidence.available;
                current.allocation_allowed &= evidence.available;
            })
            .or_insert(evidence);
    }
    let mut targets = Vec::new();
    let mut lease_ids = BTreeSet::new();
    for target in &rows.targets {
        if !allocations.contains(&target.provider) {
            continue;
        }
        lease_ids.insert(target.lease_id.clone());
        let state = state(&target.state);
        if state == StoredTargetState::Unknown {
            gaps.insert(EvidenceGap::PersistedStateUnsupported);
        }
        targets.push(TargetEvidence {
            id: target.id.clone(),
            lease_id: target.lease_id.clone(),
            allocation: target.provider.clone(),
            cid: canonical(&target.cid)?,
            original_cid: target.cid.clone(),
            logical_bytes: number(target.logical_size)?,
            state,
            registered_route: registered.get(&target.provider).map(|(raw, _)| route(raw)),
            created_at: target.created_at.timestamp(),
            last_touched_at: target.last_touched_at.timestamp(),
        });
    }
    let mut leases = Vec::new();
    for lease in &rows.leases {
        let has_targets = rows
            .targets
            .iter()
            .any(|target| target.lease_id == lease.id);
        if !(lease_ids.contains(&lease.id)
            || object_ids.contains(&lease.owner_object_id)
            || provider_inventory && !has_targets)
        {
            continue;
        }
        if !has_targets {
            gaps.insert(EvidenceGap::UnattributedLease);
        }
        let state = match lease.state.as_str() {
            "active" => StoredLeaseState::Active,
            "expired" => StoredLeaseState::Expired,
            "cancelled" => StoredLeaseState::Cancelled,
            "evicted" => StoredLeaseState::Evicted,
            _ => StoredLeaseState::Unknown,
        };
        if state == StoredLeaseState::Unknown {
            gaps.insert(EvidenceGap::PersistedStateUnsupported);
        }
        if lease.generation <= 0 || lease.expires_at <= lease.created_at {
            return Err(SnapshotError::InvalidEvidence);
        }
        leases.push(LeaseEvidence {
            id: lease.id.clone(),
            object_id: lease.owner_object_id.clone(),
            generation: lease.generation,
            started_at: lease.created_at.timestamp(),
            expires_at: lease.expires_at.timestamp(),
            state,
            policy_id: lease.policy_id.clone(),
            provider_mode: match lease.provider_mode.as_str() {
                "one" => Some(ProviderMode::One),
                "all" => Some(ProviderMode::All),
                _ => None,
            },
            has_targets,
        });
    }
    let mut jobs = Vec::new();
    for job in &rows.jobs {
        if !allocations.contains(&job.provider) {
            continue;
        }
        let invocation = invocation_routes.get(&job.id);
        let history = rows.submit_history.iter().find(|row| row.job_id == job.id);
        let state = match job.state.as_str() {
            "pending" => JobState::Pending,
            "running" => JobState::Running,
            "done" => JobState::Done,
            _ => JobState::Unknown,
        };
        let operation = match job.operation.as_str() {
            "submit" => Operation::Submit,
            "poll" => Operation::Poll,
            "unpin" => Operation::Unpin,
            "reconcile" => Operation::Reconcile,
            _ => Operation::Unknown,
        };
        if state == JobState::Unknown || operation == Operation::Unknown {
            gaps.insert(EvidenceGap::PersistedStateUnsupported);
        }
        if history.is_some() && invocation.is_none() {
            gaps.insert(EvidenceGap::HistoricalRouteUnavailable);
        }
        jobs.push(JobEvidence {
            id: job.id.clone(),
            allocation: job.provider.clone(),
            cid: canonical(&job.cid)?,
            target_id: job.target_id.clone(),
            lease_id: job.lease_id.clone(),
            expected_generation: job.expected_generation,
            expected_remote_epoch: job.expected_remote_epoch,
            state,
            operation,
            claim_until: job.locked_until.map(|time| time.timestamp()),
            historical_route: invocation.map(|(raw, _)| route(raw)),
            invocation_epoch: invocation.map(|(_, epoch)| *epoch),
            submitted_calls: history.map(|row| row.submit_calls),
            submission_effect: match history.map(|row| row.effect.as_str()) {
                Some("not_created") => SideEffect::Rejected,
                Some("unknown") => SideEffect::Unknown,
                _ => SideEffect::Unknown,
            },
        });
    }
    let mut submissions = Vec::new();
    for row in &rows.submissions {
        if !allocations.contains(&row.provider) {
            continue;
        }
        let raw = submission_routes
            .get(&row.id)
            .ok_or(SnapshotError::InvalidEvidence)?;
        let route = route(raw);
        let resources = decode::<Vec<ResourceEvidence>>(&row.resources, work)?;
        DecodeWork::shared(work)
            .with(|work| work.nodes(1 + resources.len()))
            .map_err(|_| SnapshotError::LimitExceeded)?;
        let mut projected = Vec::new();
        for evidence in resources.iter() {
            let resource = &evidence.resource;
            let cid = canonical(&resource.cid)?;
            if evidence.key.as_ref().is_some_and(|key| {
                key.backend != raw.backend
                    || key.scope != raw.scope
                    || key.resource_type != resource.resource_type
                    || key.resource_id != cid
            }) {
                return Err(SnapshotError::InvalidEvidence);
            }
            DecodeWork::shared(work)
                .copy(&(&resource.cid, &resource.request_id))
                .map_err(|_| SnapshotError::LimitExceeded)?;
            projected.push(SubmittedResource {
                resource_type: resource_type(resource.resource_type),
                cid,
                original_cid: resource.cid.clone(),
                resource_id: resource.request_id.clone(),
                ownership: ownership(
                    match resource.ownership {
                        identity::Ownership::ApplicationCreated => "application_created",
                        identity::Ownership::ExternalExisting => "external_existing",
                        identity::Ownership::Unknown => "unknown",
                    },
                    Some(&route),
                    &mut gaps,
                ),
                status: resource.status,
            });
        }
        gaps.insert(EvidenceGap::SubmissionProjectionUnavailable);
        submissions.push(SubmissionEvidence {
            id: row.id.clone(),
            job_id: row.job_id.clone(),
            allocation: row.provider.clone(),
            expected_cid: canonical(&row.expected_cid)?,
            route,
            remote_epoch: row.remote_epoch,
            claim_until: row.claim_until.timestamp(),
            effect: match row.effect.as_str() {
                "not_created" | "not_submitted" => SubmitEffect::NotSubmitted,
                "observed" => SubmitEffect::Observed,
                _ => SubmitEffect::Unknown,
            },
            inflight: row.outcome == "in_flight",
            needs_attention: row.needs_attention,
            resources: projected,
        });
    }
    let references = |allocation: &str, cid: &str| -> u64 {
        targets
            .iter()
            .filter(|target| {
                target.allocation == allocation
                    && target.cid == cid
                    && !matches!(
                        target.state,
                        StoredTargetState::Released | StoredTargetState::Evicted
                    )
                    && leases.iter().any(|lease| {
                        lease.id == target.lease_id
                            && lease.state == StoredLeaseState::Active
                            && lease.expires_at > observed_at
                    })
            })
            .count() as u64
    };
    let mut remotes = Vec::new();
    let pairs: BTreeSet<_> = rows
        .remotes
        .iter()
        .map(|row| (row.provider.clone(), row.cid.clone()))
        .chain(
            rows.ledgers
                .iter()
                .map(|row| (row.provider.clone(), row.cid.clone())),
        )
        .collect();
    for (allocation, original_cid) in pairs {
        let cid = canonical(&original_cid).unwrap_or_else(|_| original_cid.clone());
        let remote = rows
            .remotes
            .iter()
            .find(|row| row.provider == allocation && row.cid == original_cid);
        let ledger = rows
            .ledgers
            .iter()
            .find(|row| row.provider == allocation && row.cid == original_cid);
        let route = ledger_routes
            .get(&(allocation.clone(), original_cid.clone()))
            .and_then(|route| route.as_ref())
            .map(|raw| super::route(raw));
        if route.is_none() {
            gaps.insert(EvidenceGap::HistoricalRouteUnavailable);
        }
        if remote.is_none() {
            gaps.insert(EvidenceGap::OrphanLedgerEpochUnavailable);
        }
        let related: Vec<_> = jobs
            .iter()
            .filter(|job| job.allocation == allocation && job.cid == cid)
            .collect();
        let inflight = related
            .iter()
            // Only the store's complete parked-source proof excludes a Running job.
            .filter(|job| job.state == JobState::Running && !rows.retryable_parks.contains(&job.id))
            .count() as u32;
        let state = ledger.map_or(LedgerState::Unknown, |row| ledger_state(&row.effect));
        let unsettled = submissions.iter().any(|row| {
            row.allocation == allocation && row.expected_cid == cid && row.needs_attention
        });
        remotes.push(RemoteEvidence {
            allocation: allocation.clone(),
            cid: cid.clone(),
            original_cid,
            route: route.clone(),
            resource_id: remote.and_then(|row| row.request_id.clone()),
            epoch: remote.map(|row| row.epoch),
            effect: if inflight > 0 {
                SideEffect::InFlight
            } else if unsettled {
                SideEffect::Unknown
            } else {
                effect(state, false)
            },
            ledger_state: state,
            ownership: ownership(
                ledger.map_or("unknown", |row| row.ownership.as_str()),
                route.as_ref(),
                &mut gaps,
            ),
            // The target/job projection is scoped. An out-of-scope alias still
            // has a real resource row, but that projection cannot prove zero.
            application_references: allocations
                .contains(&allocation)
                .then(|| references(&allocation, &cid)),
            inflight_jobs: allocations.contains(&allocation).then_some(inflight),
            retained: state == LedgerState::Retained,
            cleanup_pending: state == LedgerState::CleanupPending
                || related
                    .iter()
                    .any(|job| job.operation == Operation::Unpin && job.state != JobState::Done),
            accounted_bytes: remote
                .filter(|row| row.status != "absent")
                .map(|row| number(row.cid_size))
                .transpose()?,
            confirmations: confirmations(ledger),
            observation_current: false,
            confirmed_absent: state == LedgerState::Absent,
        });
    }
    let mut historical_remotes = Vec::new();
    for (row, envelope, raw) in historical {
        let cid = canonical(&row.cid).unwrap_or_else(|_| row.cid.clone());
        let route = raw.as_ref().map(|raw| super::route(raw));
        if route.is_none() {
            gaps.insert(EvidenceGap::HistoricalRouteUnavailable);
        }
        // History envelope has NO reservation size/ref frontier/inflight count.
        // Preserve absence explicitly; do not turn released history into permission
        // to POST, or borrow today's remote size and scope for an older epoch.
        gaps.insert(EvidenceGap::HistoricalAccountingUnavailable);
        let state = ledger_state(&envelope.ledger.effect);
        DecodeWork::shared(work)
            .copy(&envelope.request_id)
            .map_err(|_| SnapshotError::LimitExceeded)?;
        historical_remotes.push(RemoteEvidence {
            allocation: row.provider.clone(),
            cid,
            original_cid: row.cid.clone(),
            route: route.clone(),
            resource_id: envelope.request_id.clone(),
            epoch: Some(row.epoch),
            effect: if envelope.confirmed_absent {
                SideEffect::Unknown
            } else {
                effect(state, false)
            },
            ledger_state: state,
            ownership: ownership(&envelope.ledger.ownership, route.as_ref(), &mut gaps),
            application_references: None,
            inflight_jobs: None,
            retained: state == LedgerState::Retained,
            cleanup_pending: state == LedgerState::CleanupPending,
            accounted_bytes: None,
            confirmations: confirmations(Some(&envelope.ledger)),
            observation_current: false,
            confirmed_absent: envelope.confirmed_absent,
        });
    }
    let mut canonical_keys = BTreeSet::new();
    for remote in &remotes {
        if let Some(route) = &remote.route
            && !canonical_keys.insert((route.remote_key(&remote.cid), remote.epoch))
        {
            gaps.insert(EvidenceGap::CanonicalAliasCollision);
        }
    }
    let mut usage = Vec::new();
    for row in &rows.usage {
        if allocations.contains(&row.provider) {
            usage.push(UsageEvidence {
                allocation: row.provider.clone(),
                reserved_bytes: number(row.reserved_bytes)?,
                reserved_pins: number(row.reserved_pins)?,
            });
        }
    }
    targets.sort_by(|a, b| a.id.cmp(&b.id));
    leases.sort_by(|a, b| a.id.cmp(&b.id));
    jobs.sort_by(|a, b| a.id.cmp(&b.id));
    submissions.sort_by(|a, b| a.id.cmp(&b.id));
    usage.sort_by(|a, b| a.allocation.cmp(&b.allocation));
    historical_remotes
        .sort_by(|a, b| (&a.allocation, &a.cid, a.epoch).cmp(&(&b.allocation, &b.cid, b.epoch)));
    Ok(DatabaseSnapshot {
        observed_at,
        objects,
        routes: routes.into_values().collect(),
        leases,
        targets,
        remotes,
        historical_remotes,
        jobs,
        submissions,
        usage,
        applied: vec![],
        needs_attention: gaps.into_iter().collect(),
        planning: None,
        scope: None,
    })
}

fn confirmations(row: Option<&remote_pin_ledger::Model>) -> Vec<ConfirmationLevel> {
    let Some(row) = row else {
        return vec![];
    };
    [
        (row.remote_pinned_at, ConfirmationLevel::RemotePinned),
        (row.gateway_verified_at, ConfirmationLevel::GatewayVerified),
        (row.content_verified_at, ConfirmationLevel::ContentVerified),
    ]
    .into_iter()
    .filter_map(|(at, level)| at.map(|_| level))
    .collect()
}
