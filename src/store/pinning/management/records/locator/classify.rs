//! Closed source decoders. Unsupported or malformed originals cannot seal.
use super::*;
use crate::pinning::{
    identity::ProviderRouteSnapshot,
    management::{
        Capture, ItemRequest, PlanEntry, RouteSnapshot, Subject, TargetSelection, TargetSnapshot,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DependencyKind {
    Archive,
    Predecessor,
    LegacyOperation,
    Reversal,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SourceClassification {
    pub selectors: BTreeSet<AuthoritySelector>,
    pub dependencies: BTreeMap<NativeKey, DependencyKind>,
    pub membership_lookups: BTreeSet<AuthoritySelector>,
    /// Original plan order, not current SQL row order or caller order.
    pub ordered_plan_members: Vec<NativeKey>,
    /// All original operational names, including aliases and historical R26 names.
    pub operational_keys: BTreeMap<String, BTreeSet<String>>,
    pub item_position: Option<usize>,
    pub same_plan_predecessors: BTreeSet<usize>,
}

pub(super) fn action_key(scope: &str, plan: &str, item: &str) -> NativeKey {
    NativeKey {
        table: crate::store::pinning::management::schema::RECORDS.into(),
        values: vec![scope.into(), plan.into(), item.into(), "action".into()],
    }
}
pub(super) fn route(
    result: &mut SourceClassification,
    route: &ProviderRouteSnapshot,
    operational: Option<&str>,
) -> Result<()> {
    let domain = crate::store::pinning::management::admission::allocation_domain(route)
        .map_err(|_| ManagementError::InvalidEvidence)?;
    route_selectors(route, &mut result.selectors)?;
    let keys = result.operational_keys.entry(domain.clone()).or_default();
    keys.insert(domain);
    keys.insert(route.provider_id.clone());
    if let Some(alias) = route.provider_id.strip_prefix("legacy:") {
        keys.insert(alias.into());
    }
    if let Some(operational) = operational {
        keys.insert(operational.into());
    }
    Ok(())
}
pub(super) fn request(result: &mut SourceClassification, request: &ItemRequest) -> Result<()> {
    match &request.subject {
        Subject::Object { identity, target } => {
            result.selectors.insert(AuthoritySelector::ExactVersion(
                identity.private_version_id.clone(),
            ));
            match target {
                TargetSelection::Existing { target_id, source } => {
                    result
                        .selectors
                        .insert(AuthoritySelector::Target(target_id.clone()));
                    if let Some(source) = source {
                        result.selectors.insert(AuthoritySelector::ExactVersion(
                            source.private_version_id.clone(),
                        ));
                    }
                }
                TargetSelection::New(identity) => {
                    route(
                        result,
                        identity
                            .original
                            .as_ref()
                            .ok_or(ManagementError::InvalidEvidence)?,
                        None,
                    )?;
                }
            }
        }
        Subject::Provider { route: identity } => {
            route(
                result,
                identity
                    .original
                    .as_ref()
                    .ok_or(ManagementError::InvalidEvidence)?,
                None,
            )?;
        }
    }
    if let crate::pinning::management::Action::Migrate { destination } = &request.action {
        route(
            result,
            destination
                .original
                .as_ref()
                .ok_or(ManagementError::InvalidEvidence)?,
            None,
        )?;
    }
    Ok(())
}

pub(super) fn captured_route(
    result: &mut SourceClassification,
    snapshot: &RouteSnapshot,
) -> Result<()> {
    let original = snapshot
        .original
        .as_ref()
        .or(snapshot.identity.original.as_ref())
        .ok_or(ManagementError::InvalidEvidence)?;
    if snapshot.identity
        != crate::pinning::management::RouteIdentity::from_original(original.clone())
    {
        return Err(ManagementError::InvalidEvidence);
    }
    route(result, original, None)
}
pub(super) fn captured_target(
    result: &mut SourceClassification,
    target: &TargetSnapshot,
) -> Result<()> {
    result
        .selectors
        .insert(AuthoritySelector::Target(target.id.clone()));
    result
        .selectors
        .insert(AuthoritySelector::Lease(target.lease.id.clone()));
    if let Some(identity) = &target.route {
        let original = identity
            .original
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?;
        route(
            result,
            original,
            native::value(&target.native_tuple, "provider"),
        )?;
    }
    for candidate in &target.frozen_candidates {
        let identity = candidate
            .route
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?;
        route(
            result,
            identity
                .original
                .as_ref()
                .ok_or(ManagementError::InvalidEvidence)?,
            Some(&candidate.allocation),
        )?;
        if let Some(operation) = &candidate.receipt_id {
            result.dependencies.insert(
                NativeKey {
                    table: "pin_legacy_migrations".into(),
                    values: vec![operation.clone()],
                },
                DependencyKind::LegacyOperation,
            );
        }
    }
    Ok(())
}
fn entry(result: &mut SourceClassification, entry: &PlanEntry) -> Result<()> {
    request(result, &entry.request)?;
    match &entry.capture {
        Some(Capture::Object(capture)) => {
            result.selectors.insert(AuthoritySelector::ExactVersion(
                capture.owner_identity.private_version_id.clone(),
            ));
            captured_route(result, &capture.route)?;
            for snapshot in [
                &capture.remote_route,
                &capture.destination_route,
                &capture.destination_remote_route,
            ]
            .into_iter()
            .flatten()
            .chain(&capture.historical_routes)
            .chain(&capture.requirement_routes)
            {
                captured_route(result, snapshot)?;
            }
            if let Some(target) = &capture.target {
                captured_target(result, target)?;
            }
        }
        Some(Capture::Provider(capture)) => {
            for snapshot in &capture.mutation_scope.routes {
                captured_route(result, snapshot)?;
            }
            for target in &capture.mutation_scope.targets {
                captured_target(result, target)?;
            }
        }
        None => {}
    }
    Ok(())
}

pub(super) fn classify_source(key: &NativeKey, row: &NativeRow) -> Result<SourceClassification> {
    let mut result = SourceClassification {
        selectors: basic_selectors(key, row)?,
        ..Default::default()
    };
    narrow::extend(key, row, &mut result.selectors)?;
    match key.table.as_str() {
        "pin_provider_routes" => {
            let snapshot: ProviderRouteSnapshot = serde_json::from_str(required(row, "snapshot")?)
                .map_err(|_| ManagementError::InvalidEvidence)?;
            route(&mut result, &snapshot, Some(required(row, "provider")?))?;
        }
        "pin_management_records" => {
            let scope = required(row, "database_scope")?;
            let plan = required(row, "plan_id")?;
            let kind = required(row, "kind")?;
            if kind == "database_scope" {
                return Ok(result);
            }
            result
                .membership_lookups
                .insert(AuthoritySelector::RecordMembers(scope.into(), plan.into()));
            if kind == "action" {
                classify_archive(key, row, &mut result, None)?;
                if required(row, "state")? == "applied" {
                    let decoded = results::decode_facts(
                        required(row, "result")?
                            .strip_prefix("S7R1\n")
                            .ok_or(ManagementError::InvalidEvidence)?,
                        &mut DecodeBudget::default(),
                    )?;
                    let results::DecodedResult::Facts(action, facts, _) = decoded;
                    if action.receipt.binding.request.item_id != required(row, "item_id")? {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    entry(&mut result, &action.receipt.binding)?;
                    for job in &action.jobs {
                        route(&mut result, &job.route, Some(&job.provider))?;
                    }
                    for point in &action.migration_points {
                        route(
                            &mut result,
                            &point.source_route,
                            Some(&point.source_allocation),
                        )?;
                        route(
                            &mut result,
                            &point.source_resource_route,
                            Some(&point.source_allocation),
                        )?;
                        route(
                            &mut result,
                            &point.destination_route,
                            Some(&point.destination_allocation),
                        )?;
                        if let Some(predecessor) = &point.predecessor {
                            result
                                .dependencies
                                .insert(predecessor.clone(), DependencyKind::Predecessor);
                        }
                    }
                    use crate::store::pinning::management::actions::execution::facts;
                    result.item_position = Some(facts.0);
                    for facts::Read(_, input) in &facts.1 {
                        if let facts::Input::Output(position, _, _) = input {
                            if *position >= facts.0 {
                                return Err(ManagementError::InvalidEvidence);
                            }
                            result.same_plan_predecessors.insert(*position);
                        }
                    }
                    for facts::Dependency(_, facts::Key(table, values), _, selector) in &facts.6 {
                        let dependency = NativeKey {
                            table: table.clone(),
                            values: values.clone(),
                        };
                        result
                            .dependencies
                            .insert(dependency, DependencyKind::Predecessor);
                        match selector {
                            facts::Selector::Submit([job]) => {
                                result
                                    .membership_lookups
                                    .insert(AuthoritySelector::Job(job.clone()));
                            }
                            facts::Selector::Migration(lease, cid, slot) => {
                                result
                                    .membership_lookups
                                    .insert(AuthoritySelector::MigrationSlot(
                                        lease.clone(),
                                        cid.clone(),
                                        slot.clone(),
                                    ));
                            }
                            facts::Selector::Legacy(table, key, provider) => {
                                result
                                    .membership_lookups
                                    .insert(AuthoritySelector::LegacyPoint(
                                        table.clone(),
                                        key.clone(),
                                        provider.clone(),
                                    ));
                            }
                            facts::Selector::Reservation(_, _, _, _) => {}
                        }
                    }
                }
            }
        }
        "pin_management_retirements" => {
            result.dependencies.insert(
                action_key(
                    required(row, "database_scope")?,
                    required(row, "plan_id")?,
                    required(row, "item_id")?,
                ),
                DependencyKind::Archive,
            );
        }
        "pin_legacy_migrations" => {
            let declaration: crate::store::pinning::legacy_migration::Declaration =
                serde_json::from_str(required(row, "declaration")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            let confirmation: crate::store::pinning::legacy_migration::Confirmation =
                serde_json::from_str(required(row, "confirmation")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            if declaration.database_scope != required(row, "database_scope")? {
                return Err(ManagementError::InvalidEvidence);
            }
            if confirmation.version != 1
                || declaration.version != 1
                || confirmation.database_scope != declaration.database_scope
                || confirmation.declaration != declaration
                || required(row, "schema_version")?
                    != crate::store::pinning::legacy_migration::SCHEMA
            {
                return Err(ManagementError::InvalidEvidence);
            }
            match (required(row, "direction")?, native::value(row, "reverses")) {
                ("apply", None) if confirmation.operation_id == required(row, "id")? => {}
                ("rollback", Some(id)) if confirmation.operation_id == id => {}
                _ => return Err(ManagementError::InvalidEvidence),
            }
            if confirmation
                .before
                .iter()
                .map(|t| t.rows.len().max(1))
                .sum::<usize>()
                != required(row, "item_count")?
                    .parse::<usize>()
                    .map_err(|_| ManagementError::InvalidEvidence)?
            {
                return Err(ManagementError::InvalidEvidence);
            }
            result
                .membership_lookups
                .insert(AuthoritySelector::LegacyOperation(
                    required(row, "id")?.into(),
                ));
            if let Some(reverses) = native::value(row, "reverses") {
                result.dependencies.insert(
                    NativeKey {
                        table: key.table.clone(),
                        values: vec![reverses.into()],
                    },
                    DependencyKind::Reversal,
                );
            }
            for namespace in declaration.namespaces {
                if namespace.backend != namespace.target_route.backend
                    || namespace.scope != namespace.target_route.scope
                    || namespace.resource_type != namespace.target_route.resource_type()
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                route(
                    &mut result,
                    &namespace.target_route,
                    Some(&namespace.target),
                )?;
                let domain = crate::store::pinning::management::admission::allocation_domain(
                    &namespace.target_route,
                )
                .map_err(|_| ManagementError::InvalidEvidence)?;
                result
                    .operational_keys
                    .entry(domain)
                    .or_default()
                    .insert(namespace.source);
                for binding in namespace.bindings {
                    if let Some(original) = binding.historical_route {
                        route(&mut result, &original, Some(&binding.provider))?;
                    }
                }
            }
        }
        "pin_legacy_migration_items" => {
            let operation = required(row, "operation_id")?;
            result.dependencies.insert(
                NativeKey {
                    table: "pin_legacy_migrations".into(),
                    values: vec![operation.into()],
                },
                DependencyKind::LegacyOperation,
            );
            result
                .membership_lookups
                .insert(AuthoritySelector::LegacyOperation(operation.into()));
            let bindings: Vec<crate::store::pinning::legacy_migration::Binding> =
                serde_json::from_str(required(row, "bindings")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            for binding in bindings {
                if let Some(original) = binding.historical_route {
                    route(&mut result, &original, Some(&binding.provider))?;
                }
            }
        }
        _ => return Err(ManagementError::InvalidEvidence),
    }
    for dependency in result.dependencies.keys() {
        result
            .selectors
            .insert(AuthoritySelector::Dependency(dependency.clone()));
    }
    // Route-name discovery is separate from permission/domain authority. A
    // point read must not load every action ever recorded in the same domain.
    if matches!(
        key.table.as_str(),
        "pin_provider_routes" | "pin_legacy_migrations"
    ) {
        for domain in result.operational_keys.keys() {
            result
                .selectors
                .insert(AuthoritySelector::RouteDomain(domain.clone()));
        }
    }
    Ok(result)
}

pub(super) fn classify_archive(
    key: &NativeKey,
    row: &NativeRow,
    result: &mut SourceClassification,
    historical: Option<&crate::store::initialization::history::legacy::Review>,
) -> Result<()> {
    let mut work = DecodeBudget::default();
    let binding: archive::StoredBinding = work.typed(
        required(row, "binding")?
            .strip_prefix(archive::BINDING)
            .ok_or(ManagementError::InvalidEvidence)?,
    )?;
    let (anchor, binding_hash, confirmed) = match binding {
        archive::StoredBinding::Draft(key, hash) => (
            NativeKey {
                table: key.0,
                values: key.1,
            },
            hash,
            false,
        ),
        archive::StoredBinding::Confirmed(key, hash, _, _) => (
            NativeKey {
                table: key.0,
                values: key.1,
            },
            hash,
            true,
        ),
    };
    if required(row, "state")? == "applied" && !confirmed {
        return Err(ManagementError::InvalidEvidence);
    }
    if anchor.table != key.table
        || anchor.values.len() != 4
        || anchor.values[..2] != key.values[..2]
        || anchor.values[3] != "action"
    {
        return Err(ManagementError::InvalidEvidence);
    }
    if anchor != *key {
        result
            .dependencies
            .insert(anchor.clone(), DependencyKind::Archive);
    }
    let raw = required(row, "native_before")?;
    if let Some(body) = raw.strip_prefix(archive::LOCATOR) {
        let (pointer, hash, manifest): (archive::Key, String, String) = work.typed(body)?;
        if pointer.0 != anchor.table
            || pointer.1 != anchor.values
            || anchor == *key
            || hash != binding_hash
            || manifest.len() != 64
        {
            return Err(ManagementError::InvalidEvidence);
        }
        return Ok(());
    }
    if anchor != *key {
        return Err(ManagementError::InvalidEvidence);
    }
    let dependencies = archive::dependencies(std::sync::Arc::new(row.clone()), &mut work)?;
    for dependency in dependencies {
        if dependency != *key {
            result
                .dependencies
                .insert(dependency, DependencyKind::Archive);
        }
    }
    let sections = archive::Sections::parse(raw, &mut work)?;
    let head: archive::Head = work.typed(&raw[sections.head])?;
    let core_raw = &raw[sections.core];
    if binding_hash != head.5
        || archive::identity(b"stage7/core/1\0", core_raw.as_bytes()) != head.5
    {
        return Err(ManagementError::InvalidEvidence);
    }
    if let Some(review) = historical {
        if review.request.plan_id != required(row, "plan_id")? {
            return Err(ManagementError::InvalidEvidence);
        }
        for item in &review.entries {
            result.ordered_plan_members.push(action_key(
                required(row, "database_scope")?,
                &review.request.plan_id,
                &item.request.item_id,
            ));
            upgrade::entry(result, item)?;
        }
        return Ok(());
    }
    let core: archive::Core = work.typed(core_raw)?;
    if core.7 != head.6 {
        return Err(ManagementError::InvalidEvidence);
    }
    crate::pinning::management::validate_request(&core.0)
        .map_err(|_| ManagementError::InvalidEvidence)?;
    if core.0.plan_id != required(row, "plan_id")? || core.0.items.len() != core.1.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    for item in core.1.iter() {
        if !core.0.items.contains(&item.request) {
            return Err(ManagementError::InvalidEvidence);
        }
        result.ordered_plan_members.push(action_key(
            required(row, "database_scope")?,
            &core.0.plan_id,
            &item.request.item_id,
        ));
        entry(result, item)?;
    }
    if result
        .ordered_plan_members
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}
