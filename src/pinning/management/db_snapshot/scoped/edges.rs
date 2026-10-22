use super::*;
use crate::store::entities::pin_lease_requirement::contract::BirthContract;

fn required<'a>(row: &'a NativeRow, column: &str) -> Result<&'a str> {
    value(row, column).ok_or(SnapshotError::InvalidEvidence)
}

pub(super) fn expand(
    pending: &mut Pending,
    evidence: &PlanningScope,
    request: &PlanRequest,
    work: &SharedDecodeBudget,
) -> Result<()> {
    for (key, row) in &evidence.originals {
        if !pending.processed.insert(key.clone()) {
            continue;
        }
        match key.table.as_str() {
            "pin_provider_routes" => {
                let provider = required(row, "provider")?;
                let route = decode::<ProviderRouteSnapshot>(required(row, "snapshot")?, work)?;
                for cid in relevant_cids(evidence, provider, provider) {
                    pending.route(&route, &cid)?;
                }
            }
            "remote_pin_ledger" => {
                if let Some(raw) = value(row, "route") {
                    pending.route(
                        decode::<ProviderRouteSnapshot>(raw, work)?.as_ref(),
                        required(row, "cid")?,
                    )?;
                }
            }
            "pin_resource_history" => {
                #[derive(serde::Deserialize)]
                struct Envelope {
                    ledger: crate::store::entities::remote_pin_ledger::Model,
                }
                let envelope = decode::<Envelope>(required(row, "ledger")?, work)?;
                if let Some(raw) = &envelope.ledger.route {
                    pending.route(
                        decode::<ProviderRouteSnapshot>(raw, work)?.as_ref(),
                        required(row, "cid")?,
                    )?;
                }
            }
            "pin_invocation_routes" => {
                let id = required(row, "job_id")?;
                let job = evidence
                    .originals
                    .get(&NativeKey {
                        table: "pin_jobs".into(),
                        values: vec![id.into()],
                    })
                    .ok_or(SnapshotError::InvalidEvidence)?;
                pending.route(
                    decode::<ProviderRouteSnapshot>(required(row, "route")?, work)?.as_ref(),
                    required(job, "cid")?,
                )?;
            }
            "pin_submit_observations" | "pin_replica_observations" => {
                pending.route(
                    decode::<ProviderRouteSnapshot>(required(row, "route")?, work)?.as_ref(),
                    required(row, "expected_cid")?,
                )?;
            }
            "pin_leases" => {
                let selected = evidence.memberships.iter().any(|(selector, members)| {
                    matches!(selector, Selector::OwnerLeases { .. }) && members.contains(key)
                });
                pending.lease(
                    required(row, "id")?,
                    selected || pending.full_leases.contains(required(row, "id")?),
                );
            }
            "pin_lease_targets" => {
                let id = required(row, "id")?;
                let lease = required(row, "lease_id")?;
                let full=request.items.iter().any(|item|matches!(&item.subject,Subject::Object {target:TargetSelection::Existing {target_id,..},..} if target_id==id));
                pending.lease(lease, full);
                pending.family(required(row, "provider")?, required(row, "cid")?);
                pending
                    .authority
                    .insert(AuthoritySelector::Target(id.into()));
            }
            "pin_jobs" => {
                let id = required(row, "id")?;
                pending
                    .business
                    .insert(Selector::JobAttempts { job_id: id.into() });
                pending.exact("pin_invocation_routes", vec![id.into()]);
                pending.authority.insert(AuthoritySelector::Job(id.into()));
                if let Some(lease) = value(row, "lease_id") {
                    pending.lease(lease, false);
                }
                if let Some(target) = value(row, "target_id") {
                    pending.exact("pin_lease_targets", vec![target.into()]);
                }
            }
            "zip_manifest_entries" => {
                pending.exact("zip_batches", vec![required(row, "batch_id")?.into()]);
            }
            "pin_management_records" => {
                pending.business.insert(Selector::RecordMembers {
                    database_scope: required(row, "database_scope")?.into(),
                    plan_id: required(row, "plan_id")?.into(),
                });
            }
            "pin_management_retirements" => {
                pending.authority.insert(AuthoritySelector::RecordMembers(
                    required(row, "database_scope")?.into(),
                    required(row, "plan_id")?.into(),
                ));
            }
            "pin_legacy_migrations" => {
                pending.authority.insert(AuthoritySelector::LegacyOperation(
                    required(row, "id")?.into(),
                ));
                if let Some(id) = value(row, "reverses") {
                    pending
                        .authority
                        .insert(AuthoritySelector::LegacyOperation(id.into()));
                }
                let declaration = decode::<crate::store::pinning::legacy_migration::Declaration>(
                    required(row, "declaration")?,
                    work,
                )?;
                for namespace in &declaration.namespaces {
                    for cid in relevant_cids(evidence, &namespace.source, &namespace.target) {
                        pending.family(&namespace.source, &cid);
                        pending.family(&namespace.target, &cid);
                        pending.route(&namespace.target_route, &cid)?;
                    }
                }
            }
            "pin_legacy_migration_items" => {
                pending.authority.insert(AuthoritySelector::LegacyOperation(
                    required(row, "operation_id")?.into(),
                ));
            }
            _ => {}
        }
        if let Some(provider) = value(row, "provider") {
            pending.authority.insert(AuthoritySelector::LegacyPoint(
                key.table.clone(),
                key.values.clone(),
                provider.into(),
            ));
        }
    }
    // Requirements of a shared lease are complete, but only its relevant CIDs
    // propagate. Do not recursively absorb unrelated objects owned by that lease.
    for (key, row) in &evidence.originals {
        if key.table != "pin_lease_requirements" {
            continue;
        }
        let lease = required(row, "lease_id")?;
        let birth = decode::<BirthContract>(required(row, "contract")?, work)?;
        let cids = evidence
            .originals
            .iter()
            .filter(|(key, target)| {
                key.table == "pin_lease_targets" && value(target, "lease_id") == Some(lease)
            })
            .map(|(_, target)| required(target, "cid").map(str::to_owned))
            .collect::<Result<BTreeSet<_>>>()?;
        for cid in cids {
            for slot in &birth.candidate_order {
                pending.authority.insert(AuthoritySelector::MigrationSlot(
                    lease.into(),
                    canonical(&cid)?,
                    slot.clone(),
                ));
                pending.authority.insert(AuthoritySelector::LegacyPoint(
                    key.table.clone(),
                    key.values.clone(),
                    slot.clone(),
                ));
                if let Some(Some(route)) = birth.candidates.get(slot) {
                    pending.route(route, &cid)?;
                }
                pending.family(slot, &cid);
            }
        }
    }
    for (key, row) in &evidence.originals {
        if key.table != "pin_provider_routes" {
            continue;
        }
        let route = decode::<ProviderRouteSnapshot>(required(row, "snapshot")?, work)?;
        for (backend, scope, kind, cid) in pending.resources.clone() {
            if (backend.as_str(), scope.as_str(), kind)
                == (
                    route.backend.as_str(),
                    route.scope.as_str(),
                    route.resource_type(),
                )
            {
                pending.family(required(row, "provider")?, &cid);
            }
        }
    }
    Ok(())
}

fn relevant_cids(evidence: &PlanningScope, source: &str, target: &str) -> BTreeSet<String> {
    evidence
        .memberships
        .keys()
        .filter_map(|selector| match selector {
            Selector::ResourceFamily { provider, cid }
                if provider == source || provider == target =>
            {
                Some(cid.clone())
            }
            _ => None,
        })
        .collect()
}

/// Authority is restored by the record layer, not inferred from locator JSON.
/// Follow only action-relevant logical slots and their exact predecessor edges.
pub(super) fn trusted(
    pending: &mut Pending,
    evidence: &PlanningScope,
    frame: &NativeFrame,
    work: &SharedDecodeBudget,
) -> Result<()> {
    for action in
        storage::records::frame_actions_with_work(frame, work.clone()).map_err(map_error)?
    {
        for point in &action.action_view().migration_points {
            let related = evidence.originals.iter().any(|(key, row)| {
                key.table == "pin_lease_targets"
                    && value(row, "lease_id") == Some(&point.lease_id)
                    && value(row, "cid")
                        .and_then(|cid| canonical(cid).ok())
                        .as_ref()
                        == Some(&point.cid)
            });
            if !related {
                continue;
            }
            action.validate_point(point).map_err(map_error)?;
            pending.lease(&point.lease_id, false);
            pending.exact("pin_lease_targets", vec![point.source_target_id.clone()]);
            pending.exact(
                "pin_lease_targets",
                vec![point.destination_target_id.clone()],
            );
            pending.family(&point.source_allocation, &point.cid);
            pending.family(&point.destination_allocation, &point.cid);
            pending.route(&point.source_route, &point.cid)?;
            pending.route(&point.source_resource_route, &point.cid)?;
            pending.route(&point.destination_route, &point.cid)?;
            pending.authority.insert(AuthoritySelector::MigrationSlot(
                point.lease_id.clone(),
                point.cid.clone(),
                point.slot.clone(),
            ));
            if let Some(predecessor) = &point.predecessor {
                pending
                    .business
                    .insert(Selector::Exact(predecessor.clone()));
                if predecessor.table != "pin_management_records" || predecessor.values.len() != 4 {
                    return Err(SnapshotError::InvalidEvidence);
                }
                pending.authority.insert(AuthoritySelector::RecordMembers(
                    predecessor.values[0].clone(),
                    predecessor.values[1].clone(),
                ));
            }
        }
    }
    Ok(())
}
