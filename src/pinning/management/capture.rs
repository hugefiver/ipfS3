use super::model::*;

fn route(snapshot: &Snapshot, identity: &RouteIdentity) -> Result<RouteSnapshot, SafeError> {
    snapshot
        .routes
        .iter()
        .find(|route| route.identity == *identity)
        .cloned()
        .ok_or(SafeError::MissingRoute)
}

fn domain(snapshot: &Snapshot, route: &RouteIdentity) -> Result<LedgerDomainSnapshot, SafeError> {
    snapshot
        .ledger_domains
        .iter()
        .find(|domain| {
            domain.backend == route.backend
                && domain.scope == route.scope
                && domain.resource_type == route.resource_type
        })
        .cloned()
        .ok_or(SafeError::IncompleteInventory)
}

fn remote(snapshot: &Snapshot, route: &RouteIdentity, cid: &str) -> Option<RemoteSnapshot> {
    let key = route.remote_key(cid);
    snapshot
        .remotes
        .iter()
        .find(|remote| remote.key == key)
        .cloned()
}

fn remote_route(snapshot: &Snapshot, remote: &Option<RemoteSnapshot>) -> Option<RouteSnapshot> {
    remote
        .as_ref()
        .and_then(|remote| route(snapshot, &remote.route).ok())
}

pub(crate) fn requirement_routes(request: &ItemRequest, target: Option<&TargetSnapshot>, snapshot: &Snapshot) -> Result<Vec<RouteSnapshot>, SafeError> {
    let requirements = match &request.action {
        Action::Create(intent) | Action::Reapply(intent) | Action::Backfill(intent) => Some(&intent.requirements),
        _ => target.and_then(|target| target.requirements.as_ref()),
    };
    let mut routes = Vec::new();
    if let Some(requirements) = requirements {
        for provider in &requirements.provider_ids {
            let candidates = if let Some(target) = target && !target.frozen_candidates.is_empty() {
                let frozen = target.frozen_candidates.iter().filter_map(|candidate| candidate.route.as_ref())
                    .filter(|route| route.provider_id == *provider).collect::<Vec<_>>();
                if frozen.len() != 1 { return Err(SafeError::UnknownEvidence); }
                snapshot.routes.iter().filter(|route| route.identity == *frozen[0]).collect::<Vec<_>>()
            } else {
                let configured = snapshot.configuration.as_ref().and_then(|config| config.routes.iter().find(|route| route.provider_id == *provider));
                let selected = match &request.subject {
                    Subject::Object { target: TargetSelection::New(route), .. } => Some(route),
                    Subject::Object { .. } => target.and_then(|target| target.route.as_ref()),
                    Subject::Provider { .. } => None,
                }.filter(|route| route.provider_id == *provider);
                snapshot.routes.iter().filter(|route| route.identity.provider_id == *provider
                    && configured.map_or_else(|| selected.is_none_or(|selected| route.identity == *selected),
                        |configured| route.original.as_ref() == Some(configured))).collect::<Vec<_>>()
            };
            if candidates.len() != 1 { return Err(SafeError::MissingRoute); }
            routes.push(candidates[0].clone());
        }
    }
    Ok(routes)
}

pub(crate) fn capture(request: &ItemRequest, snapshot: &Snapshot, points:&[crate::store::pinning::management::records::MigrationPoint]) -> Result<Capture, SafeError> {
    match &request.subject {
        Subject::Object { identity, target } => {
            let owner_identity=identity.clone();
            let mut object = snapshot
                .objects
                .iter()
                .find(|object| object.identity == *identity)
                .cloned()
                .ok_or(SafeError::MissingExactObject)?;
            let (target, identity) = match target {
                TargetSelection::New(route) => (None, route.clone()),
                TargetSelection::Existing { target_id, source } => {
                    let mut target = snapshot
                        .targets
                        .iter()
                        .find(|target| target.id == *target_id)
                        .cloned()
                        .ok_or(SafeError::MissingTarget)?;
                    if let Some(frame)=&snapshot.native_frame {crate::store::pinning::management::actions::points::project_target(frame,&mut target,points).map_err(|_|SafeError::UnknownEvidence)?;}
                    if let Some(contract)=&target.frozen_contract
                        && let Some(encoded)=crate::store::pinning::management::native::value(contract,"contract") {
                        let encoded:serde_json::Value=serde_json::from_str(encoded).map_err(|_|SafeError::UnknownEvidence)?;
                        if let Some(full)=encoded.get("management_requirements") {
                            target.requirements=Some(serde_json::from_value(full.clone()).map_err(|_|SafeError::UnknownEvidence)?);
                        }
                    }
                    if target.lease.object_id != identity.object_id {
                        return Err(SafeError::InconsistentSnapshot);
                    }
                    if let Some(source) = source {
                        if source.cid!=target.cid {return Err(SafeError::InconsistentSnapshot);}
                        object=snapshot.objects.iter().find(|object|object.identity==*source).cloned().ok_or(SafeError::MissingExactObject)?;
                    } else if target.cid!=object.identity.cid {
                        return Err(SafeError::UnknownEvidence);
                    }
                    let route = target.route.clone().ok_or(SafeError::UnknownEvidence)?;
                    (Some(target), route)
                }
            };
            let route = route(snapshot, &identity)?;
            let ledger_domain = domain(snapshot, &identity)?;
            let target_cid = target.as_ref().map_or(object.identity.cid.as_str(), |t| t.cid.as_str());
            if let Some(frame)=&snapshot.native_frame {
                let scope=super::decision_scope::DecisionScope::for_item(request,snapshot);
                for key in &snapshot.unknown_resources {
                    if !scope.proves_disjoint(frame,key) {return Err(SafeError::UnknownEvidence);}
                }
            }
            let remote = remote(snapshot, &identity, target_cid);
            let remote_route = remote_route(snapshot, &remote);
            let (
                destination_route,
                destination_ledger_domain,
                destination_remote,
                destination_remote_route,
            ) = if let Action::Migrate { destination } = &request.action {
                let destination_route = self::route(snapshot, destination)?;
                let destination_domain = domain(snapshot, destination)?;
                let destination_remote = self::remote(snapshot, destination, target_cid);
                let historical = self::remote_route(snapshot, &destination_remote);
                (
                    Some(destination_route),
                    Some(destination_domain),
                    destination_remote,
                    historical,
                )
            } else {
                (None, None, None, None)
            };
            let source_key = identity.remote_key(target_cid);
            let destination_key = destination_route
                .as_ref()
                .map(|route| route.identity.remote_key(target_cid));
            let mut historical_remotes: Vec<_> = snapshot
                .historical_remotes
                .iter()
                .filter(|remote| {
                    remote.key == source_key || destination_key.as_ref() == Some(&remote.key)
                })
                .cloned()
                .collect();
            historical_remotes.sort_by(|a, b| (&a.key, a.epoch).cmp(&(&b.key, b.epoch)));
            let mut historical_routes: Vec<_> = snapshot
                .routes
                .iter()
                .filter(|route| {
                    historical_remotes
                        .iter()
                        .any(|remote| remote.route == route.identity)
                })
                .cloned()
                .collect();
            historical_routes.sort_by(|a, b| a.identity.cmp(&b.identity));
            let requirement_routes=requirement_routes(request,target.as_ref(),snapshot)?;
            Ok(Capture::Object(Box::new(ObjectCapture {
                owner_identity,
                object,
                target,
                route,
                ledger_domain,
                remote,
                remote_route,
                destination_route,
                destination_ledger_domain,
                destination_remote,
                destination_remote_route,
                historical_remotes,
                historical_routes,
                requirement_routes,
            })))
        }
        Subject::Provider { route: identity } => {
            route(snapshot, identity)?;
            let mut inventory = snapshot
                .inventories
                .iter()
                .find(|inventory| inventory.provider_id == identity.provider_id)
                .cloned()
                .ok_or(SafeError::IncompleteInventory)?;
            inventory.leases.sort_by(|a, b| a.id.cmp(&b.id));
            inventory.root_reference_ids.sort();
            let owns_domain = |remote: &RemoteSnapshot| {
                snapshot.routes.iter().any(|route| {
                    route.identity.provider_id == identity.provider_id
                        && route.identity.remote_key(&remote.key.cid) == remote.key
                })
            };
            let mut remotes: Vec<_> = snapshot
                .remotes
                .iter()
                .filter(|remote| {
                    remote.route.provider_id == identity.provider_id || owns_domain(remote)
                })
                .cloned()
                .collect();
            let mut historical_remotes: Vec<_> = snapshot
                .historical_remotes
                .iter()
                .filter(|remote| {
                    remote.route.provider_id == identity.provider_id || owns_domain(remote)
                })
                .cloned()
                .collect();
            let mut projected=snapshot.targets.clone();
            if let Some(frame)=&snapshot.native_frame {
                for target in &mut projected {crate::store::pinning::management::actions::points::project_target(frame,target,points).map_err(|_|SafeError::UnknownEvidence)?;}
                // Only exact committed points can resolve the old projection's
                // missing destination route. Other orphan/private/unknown debt
                // remains incomplete; no receipt means no attribution upgrade.
                let point_resolved=snapshot.targets.iter().zip(&projected).any(|(before,after)|before.route.is_none()&&after.route.is_some());
                if point_resolved && projected.iter().all(|target|target.route.is_some())
                    && inventory.leases.iter().all(|lease|projected.iter().any(|target|target.lease.id==lease.id))
                    && inventory.root_reference_ids.is_empty() && snapshot.unknown_resources.is_empty()
                    && frame.table("remote_pin_ledger").is_some_and(|table|table.rows.iter().all(|row|crate::store::pinning::management::native::value(row,"route").and_then(|route|serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(route).ok()).is_some()))
                {inventory.attribution_complete=true;}
            }
            let mut targets: Vec<_> = projected
                .iter()
                .filter(|target| target.route.as_ref().is_none_or(|route|route.provider_id == identity.provider_id) || target.frozen_candidates.iter().any(|candidate|candidate.route.as_ref().is_some_and(|route|route.provider_id==identity.provider_id)))
                .cloned()
                .collect();
            let mut routes: Vec<_> = snapshot
                .routes
                .iter()
                .filter(|route| {
                    route.identity.provider_id == identity.provider_id
                        || remotes
                            .iter()
                            .chain(&historical_remotes)
                            .any(|remote| remote.route == route.identity)
                })
                .cloned()
                .collect();
            routes.sort_by(|a, b| a.identity.cmp(&b.identity));
            targets.sort_by(|a, b| a.id.cmp(&b.id));
            remotes.sort_by(|a, b| a.key.cmp(&b.key));
            historical_remotes.sort_by(|a, b| (&a.key, a.epoch).cmp(&(&b.key, b.epoch)));
            let capture=RetirementCapture {
                inventory,
                routes,
                targets,
                remotes,
                historical_remotes,
            };
            validate_retirement_selection(request,&capture,snapshot,points)?;
            Ok(Capture::Provider(Box::new(capture)))
        }
    }
}

fn validate_retirement_selection(request:&ItemRequest,capture:&RetirementCapture,snapshot:&Snapshot,points:&[crate::store::pinning::management::records::MigrationPoint])->Result<(),SafeError> {
    let Action::RetireWith {selection:RetirementSelection::Migrate {targets}}=&request.action else{return Ok(())};
    let Subject::Provider {route}=&request.subject else{return Err(SafeError::InvalidActionScope)};
    let frame=snapshot.native_frame.as_ref().ok_or(SafeError::UnknownEvidence)?;
    let domain=frame.table("pin_provider_routes").ok_or(SafeError::UnknownEvidence)?.rows.iter().find(|row|crate::store::pinning::management::native::value(row,"snapshot").and_then(|value|serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(value).ok()).as_ref()==route.original.as_ref()).and_then(|row|crate::store::pinning::management::native::value(row,"provider")).ok_or(SafeError::UnknownEvidence)?;
    let active=capture.targets.iter().filter(|target|crate::store::pinning::management::native::value(&target.native_tuple,"provider")==Some(domain)&&target.lease.state==LeaseState::Active).collect::<Vec<_>>();
    if active.len()!=targets.len() {return Err(SafeError::IncompleteInventory);}
    for target in targets {
        if !active.iter().any(|entry|entry.id==target.source_target_id&&entry.lease.id==target.lease_id&&entry.cid==target.cid)
            || !frame.table("pin_management_records").is_some_and(|table|table.rows.iter().any(|row|crate::store::pinning::management::native::value(row,"plan_id")==Some(&target.plan_id)&&crate::store::pinning::management::native::value(row,"item_id")==Some(&target.item_id)&&crate::store::pinning::management::native::value(row,"kind")==Some("action")&&crate::store::pinning::management::native::value(row,"state")==Some("applied")))
            || !points.iter().any(|point|point.lease_id==target.lease_id&&point.source_target_id==target.source_target_id&&point.cid==target.cid&&point.source_allocation==domain&&target.destination.original.as_ref()==Some(&point.destination_route))
        {return Err(SafeError::ConfirmationMismatch);}
    }
    Ok(())
}
