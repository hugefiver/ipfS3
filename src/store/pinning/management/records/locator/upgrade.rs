//! Historical Provider captures are locator data, never live execution grants.
use super::*;
use crate::store::initialization::{history::legacy, read_context};

pub(super) async fn classify_for_write(
    tx: &DatabaseTransaction,
    key: &NativeKey,
    row: &NativeRow,
    budget: &mut Budget,
) -> Result<SourceClassification> {
    if read_context::active(tx).await? {
        if key.table == "pin_management_records" && native::value(row, "kind") == Some("action") {
            let encoding = legacy::encoding(tx, key, budget).await?;
            if encoding == Some(true) {
                let review = legacy::read(tx, key, budget).await?;
                return action(key, row, &review);
            }
            if encoding.is_none() {
                let (scope, identity) = native::scope::binding(tx).await?;
                let local = required(row, "database_scope")? == scope
                    && required(row, "native_identity")? == identity
                    && required(row, "principal")? == "local_configuration";
                let before = required(row, "native_before")?;
                if !local && !before.starts_with("S7F1\n") && !before.starts_with("S7L1\n") {
                    // Conversion retains this opaque foreign original, but it
                    // has no local archive membership or execution selectors.
                    return Ok(SourceClassification::default());
                }
            }
        } else if key.table == "pin_management_retirements" {
            let action_key = NativeKey {
                table: "pin_management_records".into(),
                values: ["database_scope", "plan_id", "item_id", "record_kind"]
                    .map(|column| required(row, column).map(str::to_owned))
                    .into_iter()
                    .collect::<Result<_>>()?,
            };
            if legacy::encoding(tx, &action_key, budget).await? == Some(true) {
                let review = legacy::retirement(tx, row, budget).await?;
                let recorded = review
                    .recorded_result(&action_key)?
                    .ok_or(ManagementError::InvalidEvidence)?;
                let crate::pinning::management::Subject::Provider { route } =
                    &recorded.receipt.binding.request.subject
                else {
                    return Err(ManagementError::InvalidEvidence);
                };
                let domain = canonical_domain(
                    route
                        .original
                        .as_ref()
                        .ok_or(ManagementError::InvalidEvidence)?,
                )?;
                let mut result = SourceClassification::default();
                result.selectors.extend([
                    AuthoritySelector::RecordMembers(
                        required(row, "database_scope")?.into(),
                        required(row, "plan_id")?.into(),
                    ),
                    AuthoritySelector::Domain(domain.clone()),
                    AuthoritySelector::RetirementDomain(domain),
                    AuthoritySelector::RetirementDomain(required(row, "allocation_domain")?.into()),
                    AuthoritySelector::Dependency(action_key.clone()),
                ]);
                result
                    .dependencies
                    .insert(action_key, DependencyKind::Archive);
                return Ok(result);
            }
        }
    }
    let mut result = classify::classify_source(key, row)?;
    narrow::bind_retirement_domain(tx, key, row, &mut result, budget).await?;
    Ok(result)
}

fn action(
    key: &NativeKey,
    row: &NativeRow,
    review: &legacy::Review,
) -> Result<SourceClassification> {
    let mut result = SourceClassification::default();
    let members = AuthoritySelector::RecordMembers(
        required(row, "database_scope")?.into(),
        required(row, "plan_id")?.into(),
    );
    result.selectors.insert(members.clone());
    result.membership_lookups.insert(members);
    classify::classify_archive(key, row, &mut result, Some(review))?;
    if let Some(recorded) = review.recorded_result(key)? {
        entry(&mut result, &recorded.receipt.binding)?;
        result.item_position = review
            .entries
            .iter()
            .position(|entry| entry.request.item_id == key.values[2]);
        for id in &recorded.receipt.intent_ids {
            result
                .selectors
                .insert(AuthoritySelector::Lease(id.clone()));
        }
        for id in &recorded.receipt.job_ids {
            result.selectors.insert(AuthoritySelector::Job(id.clone()));
        }
        for id in &recorded.targets {
            result
                .selectors
                .insert(AuthoritySelector::Target(id.clone()));
        }
        for job in &recorded.jobs {
            result
                .selectors
                .insert(AuthoritySelector::Job(job.job_id.clone()));
            if let Some(id) = &job.lease_id {
                result
                    .selectors
                    .insert(AuthoritySelector::Lease(id.clone()));
            }
            if let Some(id) = &job.target_id {
                result
                    .selectors
                    .insert(AuthoritySelector::Target(id.clone()));
            }
            for object in job.owner.iter().chain(job.source.iter()) {
                result.selectors.insert(AuthoritySelector::ExactVersion(
                    object.private_version_id.clone(),
                ));
            }
            classify::route(&mut result, &job.route, Some(&job.provider))?;
        }
        for point in &recorded.migration_points {
            result.selectors.extend([
                AuthoritySelector::Lease(point.lease_id.clone()),
                AuthoritySelector::MigrationSlot(
                    point.lease_id.clone(),
                    point.cid.clone(),
                    point.slot.clone(),
                ),
                AuthoritySelector::Target(point.source_target_id.clone()),
                AuthoritySelector::Target(point.destination_target_id.clone()),
            ]);
            classify::route(
                &mut result,
                &point.source_route,
                Some(&point.source_allocation),
            )?;
            classify::route(
                &mut result,
                &point.source_resource_route,
                Some(&point.source_allocation),
            )?;
            classify::route(
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
    }
    for dependency in result.dependencies.keys() {
        if !review.source_keys().any(|source| source == dependency)
            && dependency.table == "pin_management_records"
        {
            return Err(ManagementError::InvalidEvidence);
        }
        result
            .selectors
            .insert(AuthoritySelector::Dependency(dependency.clone()));
    }
    Ok(result)
}

pub(super) fn entry(result: &mut SourceClassification, entry: &legacy::PlanEntry) -> Result<()> {
    classify::request(result, &entry.request)?;
    match &entry.capture {
        Some(legacy::Capture::Object(capture)) => {
            result.selectors.insert(AuthoritySelector::ExactVersion(
                capture.owner_identity.private_version_id.clone(),
            ));
            classify::captured_route(result, &capture.route)?;
            for route in [
                &capture.remote_route,
                &capture.destination_route,
                &capture.destination_remote_route,
            ]
            .into_iter()
            .flatten()
            .chain(&capture.historical_routes)
            .chain(&capture.requirement_routes)
            {
                classify::captured_route(result, route)?;
            }
            if let Some(target) = &capture.target {
                classify::captured_target(result, target)?;
            }
        }
        Some(legacy::Capture::Provider(capture)) => {
            for route in &capture.routes {
                classify::captured_route(result, route)?;
            }
            for target in &capture.targets {
                classify::captured_target(result, target)?;
            }
            for remote in capture.remotes.iter().chain(&capture.historical_remotes) {
                classify::route(
                    result,
                    remote
                        .route
                        .original
                        .as_ref()
                        .ok_or(ManagementError::InvalidEvidence)?,
                    None,
                )?;
            }
            for lease in &capture.inventory.leases {
                result
                    .selectors
                    .insert(AuthoritySelector::Lease(lease.id.clone()));
            }
        }
        None => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pinning::{
        identity::{CleanupMode, ProviderRouteSnapshot},
        management as model,
    };

    #[test]
    fn historical_provider_capture_preserves_exact_domains_targets_and_leases() {
        let route = ProviderRouteSnapshot {
            provider_id: "legacy:old-provider".into(),
            backend: "noop".into(),
            scope: "old-account".into(),
            storage_domain: "remote".into(),
            credential_revision: 1,
            endpoint_revision: 1,
            secret_ref: None,
            api_profile: "noop".into(),
            strategy: "cid".into(),
            cleanup: CleanupMode::Retain,
            confirmation: None,
        };
        let lease = model::LeaseSnapshot {
            id: "historical-lease".into(),
            object_id: "historical-owner".into(),
            generation: 3,
            started_at: 1,
            expires_at: 2,
            state: model::LeaseState::Active,
            native_tuple: None,
        };
        let identity = model::RouteIdentity::from_original(route.clone());
        let mut historical = legacy::PlanEntry {
            id: model::IdempotencyId("historical-entry".into()),
            request: model::ItemRequest {
                item_id: "provider".into(),
                action: model::Action::Retire,
                subject: model::Subject::Provider {
                    route: identity.clone(),
                },
            },
            status: model::PlanStatus::Ready,
            step: None,
            capture: Some(legacy::Capture::Provider(Box::new(
                legacy::RetirementCapture {
                    inventory: model::ProviderInventory {
                        provider_id: route.provider_id.clone(),
                        frontier: vec![],
                        complete: true,
                        attribution_complete: false,
                        allocation_stopped: false,
                        leases: vec![lease.clone()],
                        root_reference_ids: vec![],
                    },
                    routes: vec![],
                    targets: vec![model::TargetSnapshot {
                        id: "historical-target".into(),
                        native_tuple: BTreeMap::new(),
                        frozen_contract: None,
                        lease,
                        cid: "cid".into(),
                        logical_bytes: 1,
                        route: Some(identity),
                        frozen_candidates: vec![],
                        requirements: None,
                        state: model::TargetState::Pending,
                    }],
                    remotes: vec![],
                    historical_remotes: vec![],
                    root_references: vec![],
                },
            ))),
        };
        let mut classified = SourceClassification::default();
        entry(&mut classified, &historical).unwrap();
        let domain = canonical_domain(&route).unwrap();
        for expected in [
            AuthoritySelector::Domain(domain.clone()),
            AuthoritySelector::Route(route.provider_id.clone()),
            AuthoritySelector::Lease("historical-lease".into()),
            AuthoritySelector::Target("historical-target".into()),
        ] {
            assert!(classified.selectors.contains(&expected));
        }
        assert!(classified.operational_keys[&domain].contains("old-provider"));
        assert!(
            !classified
                .selectors
                .contains(&AuthoritySelector::RetirementDomain(domain))
        );

        let Some(legacy::Capture::Provider(capture)) = historical.capture.as_mut() else {
            unreachable!()
        };
        capture.targets[0]
            .route
            .as_mut()
            .unwrap()
            .original
            .as_mut()
            .unwrap()
            .scope
            .clear();
        assert_eq!(
            entry(&mut SourceClassification::default(), &historical),
            Err(ManagementError::InvalidEvidence)
        );
    }
}
