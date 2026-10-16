use super::model::*;

fn active(target: &TargetSnapshot, now: chrono::DateTime<chrono::Utc>) -> Result<(), SafeError> {
    let expiry = if let Some(tuple) = &target.lease.native_tuple {
        crate::store::pinning::management::native::parse_time(
            crate::store::pinning::management::native::value(tuple, "expires_at")
                .ok_or(SafeError::UnknownEvidence)?,
        )
        .map_err(|_| SafeError::UnknownEvidence)?
    } else {
        chrono::DateTime::from_timestamp(target.lease.expires_at, 0)
            .ok_or(SafeError::InvalidInput)?
    };
    if target.lease.state != LeaseState::Active || expiry <= now {
        return Err(SafeError::LeaseNotActive);
    }
    Ok(())
}

fn available(route: Option<&RouteSnapshot>) -> Result<(), SafeError> {
    if !route.is_some_and(|route| route.available) {
        return Err(SafeError::HistoricalRouteUnavailable);
    }
    Ok(())
}

pub(crate) fn directive(
    capture: &ObjectCapture,
    route: &RouteSnapshot,
    remote: Option<&RemoteSnapshot>,
    historical: Option<&RouteSnapshot>,
    requirements: &Requirements,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<RemoteDirective, SafeError> {
    let key = route.identity.remote_key(&capture.object.identity.cid);
    let history: Vec<_> = capture
        .historical_remotes
        .iter()
        .filter(|remote| remote.key == key)
        .collect();
    if history.iter().any(|remote| {
        remote.effect == SideEffect::InFlight || remote.inflight_operations.is_some_and(|n| n > 0)
    }) {
        return Ok(RemoteDirective::WaitForInflight);
    }
    let unresolved: Vec<_> = history
        .iter()
        .filter(|remote| {
            remote.effect == SideEffect::Unknown
                || remote.cleanup_pending
                || remote.inflight_operations.is_none()
        })
        .collect();
    if !unresolved.is_empty() {
        for remote in unresolved {
            available(
                capture
                    .historical_routes
                    .iter()
                    .find(|route| route.identity == remote.route),
            )?;
        }
        return Ok(RemoteDirective::ReconcileHistorical);
    }
    // An older known resource also forbids a fresh POST until it is observed.
    if remote.is_none_or(|remote| {
        matches!(
            remote.effect,
            SideEffect::NotSubmitted | SideEffect::Rejected
        )
    }) && let Some(existing) = history
        .iter()
        .find(|remote| remote.effect == SideEffect::Present)
    {
        available(
            capture
                .historical_routes
                .iter()
                .find(|route| route.identity == existing.route),
        )?;
        return Ok(RemoteDirective::ObserveExisting);
    }
    if let Some(remote) = remote {
        if remote.inflight_operations.is_some_and(|n| n > 0)
            || remote.effect == SideEffect::InFlight
        {
            return Ok(RemoteDirective::WaitForInflight);
        }
        if remote.effect == SideEffect::Unknown || remote.cleanup_pending {
            available(historical)?;
            return Ok(RemoteDirective::ReconcileHistorical);
        }
        if remote.effect == SideEffect::Present {
            if remote.observation_current == Some(true)
                && requirements.max_age_secs.is_none_or(|age| {
                    remote.replica_started_at.is_some_and(|started| {
                        started <= now
                            && now.signed_duration_since(started)
                                < chrono::Duration::seconds(i64::from(age))
                    })
                })
                && requirements
                    .levels
                    .iter()
                    .all(|level| remote.confirmations.contains(level))
                && remote
                    .confirmed_replicas
                    .is_some_and(|n| n >= requirements.min_replicas)
            {
                return Ok(RemoteDirective::ReuseConfirmed);
            }
            available(historical)?;
            return Ok(RemoteDirective::ObserveExisting);
        }
    }
    available(Some(route))?;
    if !route.allocation_allowed {
        return Err(SafeError::AllocationStopped);
    }
    Ok(RemoteDirective::SubmitWithFence)
}

fn object_step(
    request: &ItemRequest,
    capture: &ObjectCapture,
    slots: &[super::capture::SlotExecution<'_>],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<ObjectStep, SafeError> {
    let object = &capture.object;
    if object.readable != Some(true) {
        return Err(SafeError::UnreadableObject);
    }
    if object
        .read_evidence
        .as_ref()
        .is_some_and(|proof| proof.stored_bytes.is_none())
    {
        return Err(SafeError::StoredByteProofUnavailable);
    }
    let source_allowed = object.allowed_routes.contains(&capture.route.identity)
        || matches!(request.action, Action::Migrate { .. })
            && capture.route.identity.original.is_some()
            && object
                .read_evidence
                .as_ref()
                .is_some_and(|proof| proof.stored_bytes.is_some())
            && object.allowed_routes.iter().any(|route| {
                route.storage_domain == capture.route.identity.storage_domain
                    && route.remote_key(&object.identity.cid)
                        == capture.route.identity.remote_key(&object.identity.cid)
            });
    if object.permitted != Some(true) || !source_allowed {
        return Err(SafeError::SafetyProhibition);
    }
    // Preserve the complete frozen One/All candidate set. The allocator must not
    // silently pick a file-only upload alternative for a directory's CID/CAR proof.
    if object
        .read_evidence
        .as_ref()
        .is_some_and(|proof| proof.proof.is_directory())
        && capture
            .requirement_routes
            .iter()
            .any(|route| !matches!(route.identity.strategy.as_str(), "cid" | "car"))
    {
        return Err(SafeError::SafetyProhibition);
    }
    if !capture.ledger_domain.complete {
        return Err(SafeError::IncompleteInventory);
    }
    let mut step = ObjectStep {
        source: RemoteDirective::NoWrite,
        destination: None,
        preserve_source_responsibility: true,
    };
    match &request.action {
        Action::Create(intent) | Action::Reapply(intent) | Action::Backfill(intent) => {
            let resources = capture
                .requirement_routes
                .iter()
                .map(|route| route.identity.remote_key(&object.identity.cid))
                .collect::<std::collections::BTreeSet<_>>();
            if resources.len() != capture.requirement_routes.len() {
                return Err(SafeError::InvalidActionScope);
            }
            let eligible = |route: &RouteSnapshot| {
                route.available
                    && route.allocation_allowed
                    && object.allowed_routes.contains(&route.identity)
            };
            if capture.requirement_routes.len() != intent.requirements.provider_ids.len()
                || match intent.requirements.mode {
                    ProviderMode::All => capture
                        .requirement_routes
                        .iter()
                        .any(|route| !eligible(route)),
                    ProviderMode::One => !capture.requirement_routes.iter().any(eligible),
                }
            {
                return Err(SafeError::AllocationStopped);
            }
            if capture.target.is_some() {
                return Err(SafeError::InvalidActionScope);
            }
            if object.decision == Some(DecisionState::Skipped)
                && !matches!(request.action, Action::Reapply(_))
            {
                return Err(SafeError::RequiresExplicitReapply);
            }
            if intent.started_at > now.timestamp() || intent.expires_at <= now.timestamp() {
                return Err(SafeError::IntentExpired);
            }
            if !intent
                .requirements
                .provider_ids
                .contains(&capture.route.identity.provider_id)
            {
                return Err(SafeError::InvalidActionScope);
            }
            if !capture.route.allocation_allowed {
                return Err(SafeError::AllocationStopped);
            }
            step.source = directive(
                capture,
                &capture.route,
                capture.remote.as_ref(),
                capture.remote_route.as_ref(),
                &intent.requirements,
                now,
            )?;
        }
        Action::Retire | Action::RetireWith { .. } => return Err(SafeError::InvalidActionScope),
        action => {
            let target = capture
                .target
                .as_ref()
                .ok_or(SafeError::InvalidActionScope)?;
            match action {
                Action::Cancel => {} // No cleanup, quota release, unpin, or block removal.
                Action::Renew { expires_at } => {
                    active(target, now)?;
                    if *expires_at < target.lease.expires_at {
                        return Err(SafeError::InvalidInput);
                    }
                }
                Action::Retry => {
                    active(target, now)?;
                    if target.state == TargetState::Blocked {
                        return Err(SafeError::SafetyProhibition);
                    }
                    if target.state == TargetState::Confirmed
                        && capture.remote.as_ref().is_none_or(|remote| {
                            matches!(
                                remote.effect,
                                SideEffect::NotSubmitted | SideEffect::Rejected
                            )
                        })
                    {
                        return Err(SafeError::InconsistentSnapshot);
                    }
                    step.source = directive(
                        capture,
                        &capture.route,
                        capture.remote.as_ref(),
                        capture.remote_route.as_ref(),
                        target
                            .requirements
                            .as_ref()
                            .ok_or(SafeError::UnknownEvidence)?,
                        now,
                    )?;
                }
                Action::Reconcile => {
                    available(if capture.remote.is_some() {
                        capture.remote_route.as_ref()
                    } else {
                        Some(&capture.route)
                    })?;
                    for remote in &capture.historical_remotes {
                        available(
                            capture
                                .historical_routes
                                .iter()
                                .find(|route| route.identity == remote.route),
                        )?;
                    }
                    step.source = RemoteDirective::ReconcileHistorical;
                }
                Action::Migrate { destination } => {
                    active(target, now)?;
                    if !object.allowed_routes.contains(destination) {
                        return Err(SafeError::SafetyProhibition);
                    }
                    let destination_route = capture
                        .destination_route
                        .as_ref()
                        .ok_or(SafeError::MissingRoute)?;
                    if !capture
                        .destination_ledger_domain
                        .as_ref()
                        .is_some_and(|domain| domain.complete)
                    {
                        return Err(SafeError::IncompleteInventory);
                    }
                    if destination.as_ref() == &capture.route.identity
                        || (destination.storage_domain == capture.route.identity.storage_domain
                            && destination.remote_key(&object.identity.cid)
                                != capture.route.identity.remote_key(&object.identity.cid))
                    {
                        return Err(SafeError::InvalidActionScope);
                    }
                    if capture.route.identity.resource_type == ResourceType::ClusterPin
                        && (destination.resource_type != ResourceType::ClusterPin
                            || destination
                                .original
                                .as_ref()
                                .is_none_or(|route| route.confirmation.is_none()))
                    {
                        return Err(SafeError::SafetyProhibition);
                    }
                    if capture.route.identity.resource_type == ResourceType::ClusterPin {
                        let requirements = target
                            .requirements
                            .as_ref()
                            .ok_or(SafeError::UnknownEvidence)?;
                        let Some(
                            crate::pinning::identity::ConfirmationRequirement::ClusterReplicas {
                                min_replicas,
                                max_age_secs,
                            },
                        ) = destination
                            .original
                            .as_ref()
                            .and_then(|route| route.confirmation.as_ref())
                        else {
                            return Err(SafeError::SafetyProhibition);
                        };
                        if *min_replicas<requirements.min_replicas || requirements.max_age_secs.is_some_and(|age|*max_age_secs>age)
                            || capture.route.identity.original.as_ref().and_then(|route|route.confirmation.as_ref()).is_some_and(|source|match source {
                                crate::pinning::identity::ConfirmationRequirement::ClusterReplicas {min_replicas:source_min,max_age_secs:source_age}=>min_replicas<source_min||max_age_secs>source_age,
                            }) {return Err(SafeError::SafetyProhibition);}
                    }
                    if !target.frozen_candidates.is_empty() {
                        let selected = slots
                            .iter()
                            .filter(|slot| {
                                slot.selected_target
                                    && slot.route.as_ref() == Some(&capture.route.identity)
                            })
                            .collect::<Vec<_>>();
                        if selected.len() != 1
                            || target.frozen_candidates.iter().any(|candidate| {
                                candidate.slot != selected[0].slot
                                    && candidate.route.as_ref().is_some_and(|route| {
                                        route.remote_key(&object.identity.cid)
                                            == destination.remote_key(&object.identity.cid)
                                    })
                            })
                            || slots.iter().any(|slot| {
                                slot.slot != selected[0].slot
                                    && slot.route.as_ref().is_some_and(|route| {
                                        route.remote_key(&object.identity.cid)
                                            == destination.remote_key(&object.identity.cid)
                                    })
                            })
                        {
                            return Err(SafeError::InvalidActionScope);
                        }
                    }
                    // The source bytes are read through the independently proved
                    // local route. Historical remote calls still require their
                    // exact invocation route; destination execution is separate.
                    let source_key = capture.route.identity.remote_key(&object.identity.cid);
                    let unresolved_history = capture.historical_remotes.iter().any(|remote| {
                        remote.key == source_key
                            && (matches!(remote.effect, SideEffect::Unknown | SideEffect::InFlight)
                                || remote.inflight_operations.is_some_and(|n| n > 0)
                                || remote.cleanup_pending)
                    });
                    if unresolved_history {
                        step.source = directive(
                            capture,
                            &capture.route,
                            capture.remote.as_ref(),
                            capture.remote_route.as_ref(),
                            target
                                .requirements
                                .as_ref()
                                .ok_or(SafeError::UnknownEvidence)?,
                            now,
                        )?;
                        step.destination = Some(RemoteDirective::NoWrite);
                        return Ok(step);
                    }
                    for remote in &capture.historical_remotes {
                        available(
                            capture
                                .historical_routes
                                .iter()
                                .find(|route| route.identity == remote.route),
                        )?;
                    }
                    if let Some(remote) = &capture.remote {
                        if remote.effect == SideEffect::Unknown
                            || remote.cleanup_pending
                            || remote.inflight_operations.is_some_and(|n| n > 0)
                            || remote.effect == SideEffect::InFlight
                        {
                            step.source = directive(
                                capture,
                                &capture.route,
                                Some(remote),
                                capture.remote_route.as_ref(),
                                target
                                    .requirements
                                    .as_ref()
                                    .ok_or(SafeError::UnknownEvidence)?,
                                now,
                            )?;
                            step.destination = Some(RemoteDirective::NoWrite);
                            return Ok(step);
                        }
                        // Preserve the resource's actual old management protocol, even when confirmed.
                        available(capture.remote_route.as_ref())?;
                    }
                    if !destination_route.allocation_allowed {
                        return Err(SafeError::AllocationStopped);
                    }
                    step.destination = Some(directive(
                        capture,
                        destination_route,
                        capture.destination_remote.as_ref(),
                        capture.destination_remote_route.as_ref(),
                        target
                            .requirements
                            .as_ref()
                            .ok_or(SafeError::UnknownEvidence)?,
                        now,
                    )?);
                }
                _ => return Err(SafeError::InvalidActionScope),
            }
        }
    }
    Ok(step)
}

fn retirement(capture: &RetirementCapture) -> Result<RetirementStep, SafeError> {
    if !capture.inventory.complete {
        return Err(SafeError::IncompleteInventory);
    }
    for target in &capture.targets {
        if !capture.inventory.leases.contains(&target.lease) {
            return Err(SafeError::IncompleteInventory);
        }
    }
    let resources: Vec<_> = capture
        .remotes
        .iter()
        .chain(&capture.historical_remotes)
        .collect();
    Ok(RetirementStep {
        stop_new_allocations: true,
        needs_attention: !capture.inventory.attribution_complete
            || capture.routes.iter().any(|route| !route.available)
            || resources.iter().any(|remote| {
                remote.has_unknown_responsibility()
                    || !capture
                        .routes
                        .iter()
                        .any(|route| route.identity == remote.route && route.available)
            }),
        leases: capture.inventory.leases.len(),
        retained: resources.iter().filter(|remote| remote.retained).count(),
        unknown: resources
            .iter()
            .filter(|remote| remote.has_unknown_responsibility())
            .count(),
        inflight: resources
            .iter()
            .filter(|remote| {
                remote.effect == SideEffect::InFlight
                    || remote.inflight_operations.is_some_and(|n| n > 0)
            })
            .count(),
        cleanup_pending: resources
            .iter()
            .filter(|remote| remote.cleanup_pending)
            .count(),
        root_references: capture.inventory.root_reference_ids.len(),
    })
}

pub(crate) fn evaluate(
    request: &ItemRequest,
    capture: &Capture,
    slots: &[super::capture::SlotExecution<'_>],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<PlannedStep, SafeError> {
    match capture {
        Capture::Object(capture) => {
            object_step(request, capture, slots, now).map(PlannedStep::Object)
        }
        Capture::Provider(capture)
            if matches!(request.action, Action::Retire | Action::RetireWith { .. }) =>
        {
            if let Action::RetireWith { selection } = &request.action {
                validate_disposition(capture, selection)?;
            }
            retirement(capture).map(PlannedStep::Retire)
        }
        _ => Err(SafeError::InvalidActionScope),
    }
}

fn validate_disposition(
    capture: &RetirementCapture,
    selection: &RetirementSelection,
) -> Result<(), SafeError> {
    match selection {
        RetirementSelection::Retain => Ok(()),
        RetirementSelection::ReleaseRootReferences { references } => {
            if references.len() != capture.root_references.len()
                || references.iter().any(|key| {
                    !capture
                        .root_references
                        .iter()
                        .any(|proof| proof.key == *key)
                })
            {
                return Err(SafeError::UnknownEvidence);
            }
            Ok(())
        }
        RetirementSelection::Migrate { targets } => {
            if !capture.inventory.attribution_complete
                || !capture.inventory.root_reference_ids.is_empty()
                || targets.is_empty()
            {
                return Err(SafeError::IncompleteInventory);
            }
            for selected in targets {
                if !capture.targets.iter().any(|target| {
                    target.id == selected.source_target_id
                        && target.lease.id == selected.lease_id
                        && target.cid == selected.cid
                }) {
                    return Err(SafeError::InvalidActionScope);
                }
            }
            Ok(())
        }
        RetirementSelection::ControlledCleanup { resources } => {
            if !capture.inventory.attribution_complete
                || !capture.inventory.root_reference_ids.is_empty()
                || !capture.historical_remotes.is_empty()
                || resources.len() != capture.remotes.len()
            {
                return Err(SafeError::IncompleteInventory);
            }
            for remote in &capture.remotes {
                if remote.ownership != Ownership::ApplicationCreated
                    || remote.application_references != Some(0)
                    || remote.inflight_operations != Some(0)
                    || remote.has_unknown_responsibility()
                    || remote.retained
                    || remote.cleanup_pending
                    || remote.effect != SideEffect::Present
                    || matches!(
                        remote.route.resource_type,
                        ResourceType::RpcPin | ResourceType::ClusterPin
                    )
                    || !capture.routes.iter().any(|route| {
                        route.identity == remote.route
                            && route.available
                            && route.cleanup == CleanupMode::Managed
                    })
                    || !resources.iter().any(|selected| {
                        selected.key == remote.key
                            && selected.route == remote.route
                            && selected.epoch == remote.epoch
                            && Some(&selected.resource_id) == remote.resource_id.as_ref()
                    })
                {
                    return Err(SafeError::SafetyProhibition);
                }
            }
            Ok(())
        }
    }
}
