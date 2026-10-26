//! Exact logical-slot resolver. Never a provider-wide alias and never a rewrite
//! of the original frozen contract, invocation or resource-history JSON.
use super::*;
use crate::{
    pinning::provider::canonical_resource_cid,
    store::entities::{pin_lease, pin_lease_requirement, pin_lease_target, remote_pin},
};
use sea_orm::{ConnectionTrait, DatabaseTransaction};

pub async fn resolve_slot<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    cid: &str,
    slot: &str,
) -> Result<Option<records::MigrationPoint>> {
    Ok(resolved_slot(db, lease_id, cid, slot)
        .await?
        .map(|resolved| resolved.point))
}

#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedSlot {
    pub point: records::MigrationPoint,
    pub record_key: native::NativeKey,
    pub allocation: String,
    pub resource_key: native::NativeKey,
    pub execution_route: crate::pinning::identity::ProviderRouteSnapshot,
}

pub async fn resolved_slot<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    cid: &str,
    slot: &str,
) -> Result<Option<ResolvedSlot>> {
    resolved_slot_with_work(
        db,
        lease_id,
        cid,
        slot,
        records::SharedDecodeBudget::default(),
    )
    .await
}
pub(crate) async fn resolved_slot_with_work<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    cid: &str,
    slot: &str,
    work: records::SharedDecodeBudget,
) -> Result<Option<ResolvedSlot>> {
    records::DecodeWork::shared(&work).copy(cid)?;
    let cid = canonical_resource_cid(cid).map_err(|_| ManagementError::InvalidEvidence)?;
    let actions = crate::store::pinning::management::admission::authority::actions_for_lease(
        db,
        lease_id,
        work.clone(),
    )
    .await?;
    resolved_from_actions(
        db,
        lease_id,
        &cid,
        slot,
        &actions,
        records::DecodeWork::shared(&work),
    )
    .await
}

async fn resolved_from_actions<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    cid: &str,
    slot: &str,
    actions: &[records::TrustedAppliedAction],
    work: records::DecodeWork<'_>,
) -> Result<Option<ResolvedSlot>> {
    work.with(|work| work.nodes(1))?;
    let mut nodes = std::collections::BTreeMap::new();
    for trusted in actions {
        let action = trusted.action_view();
        for point in &action.migration_points {
            if point.lease_id != lease_id || point.cid != cid || point.slot != slot {
                continue;
            }
            trusted.validate_point(point)?;
            work.with(|work| work.nodes(1))?;
            if nodes.insert(trusted.record_key(), point).is_some() {
                return Err(ManagementError::InvalidEvidence);
            }
        }
    }
    if nodes.is_empty() {
        return Ok(None);
    }
    work.with(|work| work.nodes(3 + nodes.len() * 2))?;
    let parents = nodes
        .values()
        .filter_map(|point| point.predecessor.as_ref())
        .collect::<std::collections::BTreeSet<_>>();
    if parents.len()
        != nodes
            .values()
            .filter(|point| point.predecessor.is_some())
            .count()
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let tips = nodes
        .keys()
        .filter(|key| !parents.contains(**key))
        .copied()
        .collect::<Vec<_>>();
    if tips.len() != 1 {
        return Err(ManagementError::InvalidEvidence);
    }
    let key = tips[0];
    let mut next = Some(key);
    let mut visited = std::collections::BTreeSet::new();
    while let Some(current) = next {
        if !visited.insert(current) || visited.len() > 32 {
            return Err(ManagementError::Budget);
        }
        let point = nodes.get(current).ok_or(ManagementError::InvalidEvidence)?;
        next = point.predecessor.as_ref();
    }
    if visited.len() != nodes.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    let point = *nodes.get(key).ok_or(ManagementError::InvalidEvidence)?;
    let binding = actions
        .first()
        .ok_or(ManagementError::InvalidEvidence)?
        .binding();
    let (allocation, execution_route) = validate_current(db, point, binding).await?;
    binding.copy(&(
        point,
        key,
        &point.destination_resource_key.table,
        &allocation,
        &point.destination_resource_key.values[1],
    ))?;
    let resource_key = native::NativeKey {
        table: point.destination_resource_key.table.clone(),
        values: vec![
            allocation.clone(),
            point.destination_resource_key.values[1].clone(),
        ],
    };
    Ok(Some(ResolvedSlot {
        point: point.clone(),
        record_key: key.clone(),
        allocation,
        resource_key,
        execution_route,
    }))
}

/// CID-specific ordered slots. `frozen_candidates`' old lease-only call cannot
/// collapse per-CID migrations into a global provider replacement.
pub async fn candidates_for_cid<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    cid: &str,
    birth: &pin_lease_requirement::contract::BirthContract,
) -> Result<Vec<(String, String, Option<records::MigrationPoint>)>> {
    candidates_for_cid_with_work(
        db,
        lease_id,
        cid,
        birth,
        records::SharedDecodeBudget::default(),
    )
    .await
}
pub(crate) async fn candidates_for_cid_with_work<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    cid: &str,
    birth: &pin_lease_requirement::contract::BirthContract,
    work: records::SharedDecodeBudget,
) -> Result<Vec<(String, String, Option<records::MigrationPoint>)>> {
    candidates_for_cid_catalog(
        crate::store::pinning::legacy_migration::ReceiptCatalog::full(db),
        lease_id,
        cid,
        birth,
        work,
    )
    .await
}

pub(crate) async fn candidates_for_cid_catalog(
    catalog: crate::store::pinning::legacy_migration::ReceiptCatalog<'_>,
    lease_id: &str,
    cid: &str,
    birth: &pin_lease_requirement::contract::BirthContract,
    work: records::SharedDecodeBudget,
) -> Result<Vec<(String, String, Option<records::MigrationPoint>)>> {
    let db = &catalog;
    if birth.candidate_order.is_empty() {
        return Ok(vec![]);
    }
    let context = records::DecodeWork::shared(&work);
    context.copy(cid)?;
    let cid = canonical_resource_cid(cid).map_err(|_| ManagementError::InvalidEvidence)?;
    let actions =
        crate::store::pinning::management::admission::authority::actions_for_lease_catalog(
            catalog,
            lease_id,
            work.clone(),
        )
        .await?;
    context.copy(&("lease_id", lease_id))?;
    let key = [("lease_id".into(), lease_id.to_owned())].into();
    let mut result = Vec::new();
    let mut allocations = std::collections::BTreeSet::new();
    for slot in &birth.candidate_order {
        let resolved = resolved_from_actions(db, lease_id, &cid, slot, &actions, context).await?;
        context.copy(slot)?;
        if let Some(point) = &resolved {
            context.copy(&point.allocation)?;
        }
        let allocation = if let Some(point) = &resolved {
            point.allocation.clone()
        } else if let Some(binding) = crate::store::pinning::legacy_migration::resolve_with_catalog(
            "pin_lease_requirements",
            &key,
            slot,
            &context,
            catalog,
        )
        .await?
        {
            binding.operational_key
        } else {
            context.copy(slot)?;
            slot.clone()
        };
        context.copy(&allocation)?;
        if !allocations.insert(allocation.clone()) {
            return Err(ManagementError::InvalidEvidence);
        }
        result.push((
            slot.clone(),
            allocation,
            resolved.map(|resolved| resolved.point),
        ));
    }
    Ok(result)
}

pub(crate) fn planning_points_with_work(
    frame: &native::NativeFrame,
    work: records::SharedDecodeBudget,
) -> Result<Vec<records::MigrationPoint>> {
    work.lock()
        .map_err(|_| ManagementError::InvalidEvidence)?
        .nodes(1)?;
    let mut points = Vec::new();
    for trusted in records::frame_actions_with_work(frame, work.clone())? {
        for point in &trusted.action_view().migration_points {
            trusted.validate_point(point)?;
            work.lock()
                .map_err(|_| ManagementError::InvalidEvidence)?
                .copy(&(trusted.record_key(), point))?;
            points.push((trusted.record_key().clone(), point.clone()));
        }
    }
    let mut points = latest_points(
        points,
        &mut *work.lock().map_err(|_| ManagementError::InvalidEvidence)?,
    )?;
    for point in &mut points {
        work.lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .copy(&[("id", &point.destination_target_id)])?;
        let key = [("id".into(), point.destination_target_id.clone())].into();
        point.planning_projection = movement_in_rows_with_work(
            frame.into(),
            "pin_lease_targets",
            &key,
            &point.destination_allocation,
            &point.destination_route,
            &mut *work.lock().map_err(|_| ManagementError::InvalidEvidence)?,
        )?;
    }
    Ok(points)
}

pub(crate) fn project_target_rows(
    frame: records::FrozenRows<'_>,
    private: bool,
    target: &mut crate::pinning::management::TargetSnapshot,
    points: &[records::MigrationPoint],
    work: &records::SharedDecodeBudget,
) -> Result<()> {
    for point in points {
        if point.lease_id != target.lease.id || point.cid != target.cid {
            continue;
        }
        let contract_matches = if private {
            frame.original_row_matches(
                "pin_lease_requirements",
                &[&point.lease_id],
                &point.original_contract,
            )
        } else {
            frame.projected_row_matches(
                "pin_lease_requirements",
                &[&point.lease_id],
                &point.original_contract,
            )
        };
        if !contract_matches {
            return Err(ManagementError::Changed);
        }
        let destination = frame
            .row("pin_lease_targets", &[&point.destination_target_id])
            .ok_or(ManagementError::Changed)?;
        let key = [("id".into(), point.destination_target_id.clone())].into();
        let movement = if private {
            let original = movement_in_rows_with_work(
                frame,
                "pin_lease_targets",
                &key,
                &point.destination_allocation,
                &point.destination_route,
                &mut *work.lock().map_err(|_| ManagementError::InvalidEvidence)?,
            )?;
            if original != point.planning_projection {
                return Err(ManagementError::Changed);
            }
            original
        } else {
            point.planning_projection.clone()
        };
        if let Some(projection) = &movement
            && (projection.legacy_record.table != "pin_legacy_migrations"
                || projection.legacy_record.values.len() != 1
                || frame
                    .row(
                        "pin_legacy_migrations",
                        &[&projection.legacy_record.values[0]],
                    )
                    .is_none())
        {
            return Err(ManagementError::InvalidEvidence);
        }
        let allocation = movement.as_ref().map_or_else(
            || point.destination_allocation.clone(),
            |projection| projection.allocation.clone(),
        );
        let receipt = movement
            .as_ref()
            .map(|projection| &projection.legacy_record.values[0]);
        work.lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .copy(&receipt)?;
        let receipt = receipt.cloned();
        let execution_route = movement.map_or_else(
            || point.destination_route.clone(),
            |projection| projection.execution_route,
        );
        if native::value(destination, "lease_id") != Some(&point.lease_id)
            || native::value(destination, "provider") != Some(&allocation)
            || native::value(destination, "cid")
                .and_then(|cid| canonical_resource_cid(cid).ok())
                .as_deref()
                != Some(&point.cid)
        {
            return Err(ManagementError::Changed);
        }
        let candidate = target
            .frozen_candidates
            .iter_mut()
            .find(|candidate| candidate.slot == point.slot)
            .ok_or(ManagementError::InvalidEvidence)?;
        candidate.allocation = allocation;
        candidate.receipt_id = receipt;
        if target.id == point.destination_target_id {
            target.route = Some(crate::pinning::management::RouteIdentity::from_original(
                execution_route,
            ));
            if target.requirements.is_none() {
                fn present<'de, D: serde::Deserializer<'de>>(
                    d: D,
                ) -> std::result::Result<Option<crate::pinning::management::Requirements>, D::Error>
                {
                    <crate::pinning::management::Requirements as serde::Deserialize>::deserialize(d)
                        .map(Some)
                }
                #[derive(serde::Deserialize)]
                struct Envelope {
                    #[serde(default, deserialize_with = "present")]
                    management_requirements: Option<crate::pinning::management::Requirements>,
                }
                let raw = native::value(&point.original_contract, "contract")
                    .ok_or(ManagementError::InvalidEvidence)?;
                let birth = work
                    .lock()
                    .map_err(|_| ManagementError::InvalidEvidence)?
                    .typed_shared::<pin_lease_requirement::contract::BirthContract>(raw)?;
                let encoded = work
                    .lock()
                    .map_err(|_| ManagementError::InvalidEvidence)?
                    .typed_shared::<Envelope>(raw)?;
                if let Some(full) = &encoded.management_requirements {
                    work.lock()
                        .map_err(|_| ManagementError::InvalidEvidence)?
                        .copy(full)?;
                    target.requirements = Some(full.clone());
                } else {
                    use crate::pinning::management::{
                        ConfirmationLevel, ProviderMode, Requirements,
                    };
                    if !matches!(birth.provider_mode.as_str(), "one" | "all") {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    let candidates = &target.frozen_candidates;
                    let provider_ids = candidates
                        .iter()
                        .filter_map(|candidate| {
                            candidate
                                .route
                                .as_ref()
                                .map(|route| route.provider_id.clone())
                        })
                        .collect::<Vec<_>>();
                    let proof=candidates.iter().find(|candidate|candidate.slot==point.slot)
                            .and_then(|candidate|candidate.route.as_ref()).and_then(|route|route.original.as_ref())
                            .and_then(|original|match original.confirmation {
                                Some(crate::pinning::identity::ConfirmationRequirement::ClusterReplicas {min_replicas,max_age_secs}) if min_replicas>0&&max_age_secs>0=>Some((min_replicas,Some(max_age_secs),ConfirmationLevel::ReplicationComplete)),
                                Some(_)=>None,
                                None if original.resource_type()!=crate::pinning::identity::RemoteResourceType::ClusterPin=>Some((1,None,ConfirmationLevel::RemotePinned)),
                                None=>None,
                            });
                    if !(birth.provider_mode == "all"
                        && candidates.iter().any(|candidate| candidate.route.is_none()))
                        && provider_ids
                            .iter()
                            .collect::<std::collections::BTreeSet<_>>()
                            .len()
                            == provider_ids.len()
                        && let Some((min_replicas, max_age_secs, level)) = proof
                    {
                        target.requirements = Some(Requirements {
                            mode: if birth.provider_mode == "all" {
                                ProviderMode::All
                            } else {
                                ProviderMode::One
                            },
                            provider_ids,
                            levels: vec![level],
                            min_replicas,
                            max_age_secs,
                            policy_revision: native::value(
                                target
                                    .lease
                                    .native_tuple
                                    .as_ref()
                                    .ok_or(ManagementError::InvalidEvidence)?,
                                "policy_id",
                            )
                            .ok_or(ManagementError::InvalidEvidence)?
                            .into(),
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn validate_birth<'a>(
    tx: &DatabaseTransaction,
    binding: impl Into<records::BindingView<'a>>,
    action: &records::CommittedAction,
) -> Result<()> {
    let binding = binding.into();
    let mut coordinates = std::collections::BTreeSet::new();
    for point in &action.migration_points {
        if !coordinates.insert((&point.lease_id, &point.cid, &point.slot)) {
            return Err(ManagementError::InvalidEvidence);
        }
        validate_original(binding, action, point)?;
        validate_current(tx, point, binding).await?;
    }
    Ok(())
}

pub(crate) fn validate_original<'a>(
    binding: impl Into<records::BindingView<'a>>,
    action: &records::CommittedAction,
    point: &records::MigrationPoint,
) -> Result<()> {
    let binding = binding.into();
    let mut context = records::ValidationContext::with_work(binding.decode_work().budget().clone());
    context.with_record(record_key(binding, action), |context| {
        validate_original_with_context(binding, action, point, context)
    })
}
pub(crate) fn validate_original_with_context<'a>(
    binding: impl Into<records::BindingView<'a>>,
    action: &records::CommittedAction,
    point: &records::MigrationPoint,
    context: &mut records::ValidationContext,
) -> Result<()> {
    let binding = binding.into();
    context.with_predecessor(|context| validate_original_point(binding, action, point, context))
}
fn validate_original_point(
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    point: &records::MigrationPoint,
    context: &mut records::ValidationContext,
) -> Result<()> {
    context.point()?;
    if point.planning_projection.is_some() {
        return Err(ManagementError::Confirmation);
    }
    use crate::pinning::management::{Action, Capture};
    let entry = &action.receipt.binding;
    let (Action::Migrate { destination }, Some(Capture::Object(capture))) =
        (&entry.request.action, &entry.capture)
    else {
        return Err(ManagementError::Confirmation);
    };
    let target = capture
        .target
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let before = binding.frozen_rows()?;
    if point.source_resource_key.table != "remote_pins"
        || point.source_resource_key.values.len() != 2
        || point.source_resource_key.values[0] != point.source_allocation
        || point.destination_resource_key.table != "remote_pins"
        || point.destination_resource_key.values.len() != 2
        || point.destination_resource_key.values[0] != point.destination_allocation
    {
        return Err(ManagementError::Confirmation);
    }
    let remote = before
        .row(
            "remote_pins",
            &[
                &point.source_resource_key.values[0],
                &point.source_resource_key.values[1],
            ],
        )
        .ok_or(ManagementError::InvalidEvidence)?;
    if point.table != "pin_lease_requirements"
        || point.primary_key.table != point.table
        || point.primary_key.values.len() != 1
        || point.primary_key.values[0] != target.lease.id
        || point.capture_position.strip_prefix("candidates/") != Some(point.slot.as_str())
        || !before
            .row("pin_lease_requirements", &[&target.lease.id])
            .is_some_and(|row| row == point.original_contract)
        || point.lease_id != target.lease.id
        || point.source_target_id != target.id
        || point.cid != target.cid
        || native::value(&target.native_tuple, "provider") != Some(&point.source_allocation)
        || native::value(remote, "epoch").and_then(|epoch| epoch.parse::<i64>().ok())
            != Some(point.source_epoch)
        || native::value(remote, "request_id") != point.source_resource_id.as_deref()
        || capture.route.original.as_ref() != Some(&point.source_route)
        || destination.original.as_ref() != Some(&point.destination_route)
        || capture
            .remote_route
            .as_ref()
            .and_then(|route| route.original.as_ref())
            != Some(&point.source_resource_route)
        || before
            .row(
                "remote_pin_ledger",
                &[
                    &point.source_resource_key.values[0],
                    &point.source_resource_key.values[1],
                ],
            )
            .and_then(|row| native::value(row, "route"))
            .map(|encoded| binding.decode_route(encoded))
            .transpose()?
            .as_deref()
            != Some(&point.source_resource_route)
        || !before
            .row("pin_leases", &[&point.lease_id])
            .is_some_and(|row| row == point.source_lease)
        || point.source_generation != target.lease.generation
        || native::value(&point.source_lease, "generation").and_then(|value| value.parse().ok())
            != Some(point.source_generation)
        || native::value(&point.source_lease, "created_at")
            .and_then(|value| native::parse_time(value).ok())
            != Some(point.started_at)
        || native::value(&point.source_lease, "expires_at")
            .and_then(|value| native::parse_time(value).ok())
            != Some(point.expires_at)
        || native::value(&target.native_tuple, "cid") != Some(&point.source_resource_key.values[1])
        || canonical_resource_cid(&point.destination_resource_key.values[1])
            .ok()
            .as_deref()
            != Some(&point.cid)
        || !action.targets.contains(&point.destination_target_id)
        || !action.receipt.intent_ids.contains(&point.lease_id)
    {
        return Err(ManagementError::Confirmation);
    }
    let contract: pin_lease_requirement::contract::BirthContract = binding.decode(
        native::value(&point.original_contract, "contract")
            .ok_or(ManagementError::InvalidEvidence)?,
    )?;
    if let Some(key) = &point.predecessor {
        let trusted = records::private_action_from_rows(before, key, context)?
            .ok_or(ManagementError::Confirmation)?;
        let old_action = trusted.action_view();
        if key != trusted.record_key() {
            return Err(ManagementError::Confirmation);
        }
        let previous = old_action
            .migration_points
            .iter()
            .filter(|old| {
                old.lease_id == point.lease_id && old.cid == point.cid && old.slot == point.slot
            })
            .collect::<Vec<_>>();
        if previous.len() != 1 {
            return Err(ManagementError::Confirmation);
        }
        let previous = previous[0];
        binding.copy(&[("id", &previous.destination_target_id)])?;
        let original_key = [("id".into(), previous.destination_target_id.clone())].into();
        let movement = binding.with_decode_work(|work| {
            movement_in_rows_with_work(
                before,
                "pin_lease_targets",
                &original_key,
                &previous.destination_allocation,
                &previous.destination_route,
                work,
            )
        })?;
        let allocation = movement
            .as_ref()
            .map_or(&previous.destination_allocation, |projection| {
                &projection.allocation
            });
        let route = movement
            .as_ref()
            .map_or(&previous.destination_route, |projection| {
                &projection.execution_route
            });
        if previous.original_contract != point.original_contract
            || previous.destination_target_id != point.source_target_id
            || allocation != &point.source_allocation
            || route != &point.source_route
            || previous.started_at != point.started_at
            || previous.expires_at > point.expires_at
            || previous.source_generation > point.source_generation
            || previous.destination_epoch > point.source_epoch
            || [
                "id",
                "owner_object_id",
                "source",
                "policy_id",
                "provider_mode",
                "content_mode",
                "created_at",
            ]
            .iter()
            .any(|column| {
                native::value(&previous.source_lease, column)
                    != native::value(&point.source_lease, column)
            })
        {
            return Err(ManagementError::Confirmation);
        }
    } else if contract
        .candidates
        .get(&point.slot)
        .and_then(Option::as_ref)
        != Some(&point.source_route)
    {
        return Err(ManagementError::Confirmation);
    }
    // Explicit route movement never weakens the Cluster birth threshold. The
    // consumer evaluates the original full contract against destination facts.
    if matches!(
        point.source_route.resource_type(),
        crate::pinning::identity::RemoteResourceType::ClusterPin
    ) && point.source_route.confirmation.is_none()
    {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}

pub(crate) async fn validate_current<C: ConnectionTrait>(
    db: &C,
    point: &records::MigrationPoint,
    binding: records::BindingView<'_>,
) -> Result<(String, crate::pinning::identity::ProviderRouteSnapshot)> {
    let work = binding.decode_work();
    binding.copy(&[("id", &point.destination_target_id)])?;
    let contract =
        records::current_model::<pin_lease_requirement::Entity, _>(db, &[&point.lease_id], work)
            .await?
            .ok_or(ManagementError::Changed)?;
    if native::value(&point.original_contract, "contract") != Some(contract.contract.as_str()) {
        return Err(ManagementError::Changed);
    }
    let lease = records::current_model::<pin_lease::Entity, _>(db, &[&point.lease_id], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    let target = records::current_model::<pin_lease_target::Entity, _>(
        db,
        &[&point.destination_target_id],
        work,
    )
    .await?
    .ok_or(ManagementError::Changed)?;
    let key = [("id".into(), point.destination_target_id.clone())].into();
    let (allocation, execution_route) = if let Some(binding) =
        crate::store::pinning::legacy_migration::resolve_legacy_binding_with_work(
            db,
            "pin_lease_targets",
            &key,
            &point.destination_allocation,
            binding,
        )
        .await?
    {
        if binding.historical_route.as_ref() != Some(&point.destination_route) {
            return Err(ManagementError::Changed);
        }
        (binding.operational_key, binding.declaration_route)
    } else {
        binding.copy(&(&point.destination_allocation, &point.destination_route))?;
        (
            point.destination_allocation.clone(),
            point.destination_route.clone(),
        )
    };
    work.copy(&(&target.cid, &target.cid))?;
    if lease.created_at != point.started_at
        || lease.expires_at < point.expires_at
        || lease.generation < point.source_generation
        || lease.source
            != native::value(&point.source_lease, "source")
                .ok_or(ManagementError::InvalidEvidence)?
        || lease.owner_object_id
            != native::value(&point.source_lease, "owner_object_id")
                .ok_or(ManagementError::InvalidEvidence)?
        || target.lease_id != lease.id
        || target.provider != allocation
        || canonical_resource_cid(&target.cid).ok().as_deref() != Some(&point.cid)
    {
        return Err(ManagementError::Changed);
    }
    if target.cid != point.destination_resource_key.values[1] {
        return Err(ManagementError::Changed);
    }
    let remote =
        records::current_model::<remote_pin::Entity, _>(db, &[&target.provider, &target.cid], work)
            .await?
            .ok_or(ManagementError::Changed)?;
    if remote.epoch < point.destination_epoch {
        return Err(ManagementError::Changed);
    }
    let released = crate::store::pinning::legacy_migration::release_history_with_work(
        db,
        &remote.provider,
        &remote.cid,
        binding,
    )
    .await?;
    if released
        .iter()
        .any(|row| row.epoch >= point.destination_epoch && row.epoch < remote.epoch)
    {
        return Err(ManagementError::Changed);
    }
    if point.destination_resource_id.is_some() && point.destination_resource_id != remote.request_id
    {
        return Err(ManagementError::Changed);
    }
    Ok((allocation, execution_route))
}

pub(crate) fn frozen_tip(
    binding: records::BindingView<'_>,
    lease: &str,
    cid: &str,
    slot: &str,
) -> Result<Option<(native::NativeKey, records::MigrationPoint)>> {
    let actions =
        records::frozen_actions_for_lease(binding.frozen_rows()?, lease, binding.decode_work())?;
    let mut nodes = std::collections::BTreeMap::new();
    for trusted in &actions {
        for point in &trusted.action_view().migration_points {
            if point.lease_id != lease || point.cid != cid || point.slot != slot {
                continue;
            }
            trusted.validate_point(point)?;
            binding.with_decode_work(|work| work.nodes(1))?;
            if nodes.insert(trusted.record_key(), point).is_some() {
                return Err(ManagementError::Confirmation);
            }
        }
    }
    if nodes.is_empty() {
        return Ok(None);
    }
    binding.with_decode_work(|work| work.nodes(3 + nodes.len() * 2))?;
    let parents = nodes
        .values()
        .filter_map(|point| point.predecessor.as_ref())
        .collect::<std::collections::BTreeSet<_>>();
    if parents.len()
        != nodes
            .values()
            .filter(|point| point.predecessor.is_some())
            .count()
    {
        return Err(ManagementError::Confirmation);
    }
    let mut tips = nodes.keys().filter(|key| !parents.contains(**key));
    let key = *tips.next().ok_or(ManagementError::Confirmation)?;
    if tips.next().is_some() {
        return Err(ManagementError::Confirmation);
    }
    let mut next = Some(key);
    let mut visited = std::collections::BTreeSet::new();
    while let Some(key) = next {
        if !visited.insert(key) || visited.len() > 32 {
            return Err(ManagementError::Budget);
        }
        next = nodes
            .get(key)
            .ok_or(ManagementError::Confirmation)?
            .predecessor
            .as_ref();
    }
    if visited.len() != nodes.len() {
        return Err(ManagementError::Confirmation);
    }
    let point = *nodes.get(key).ok_or(ManagementError::Confirmation)?;
    binding.copy(&(key, point))?;
    Ok(Some((key.clone(), point.clone())))
}

pub(crate) fn validate_frozen_current(
    binding: records::BindingView<'_>,
    point: &records::MigrationPoint,
) -> Result<()> {
    let rows = binding.frozen_rows()?;
    let lease = rows
        .row("pin_leases", &[&point.lease_id])
        .ok_or(ManagementError::Confirmation)?;
    let target = rows
        .row("pin_lease_targets", &[&point.destination_target_id])
        .ok_or(ManagementError::Confirmation)?;
    if !rows
        .row("pin_lease_requirements", &[&point.lease_id])
        .is_some_and(|row| row == point.original_contract)
        || native::parse_time(super::execution::cell(lease, "created_at")?)? != point.started_at
        || native::parse_time(super::execution::cell(lease, "expires_at")?)? < point.expires_at
        || lease
            .cell("generation")
            .and_then(|value| value.parse::<i64>().ok())
            .is_none_or(|generation| generation < point.source_generation)
        || [
            "owner_object_id",
            "source",
            "policy_id",
            "provider_mode",
            "content_mode",
        ]
        .iter()
        .any(|column| lease.cell(column) != native::value(&point.source_lease, column))
        || target.cell("lease_id") != Some(&point.lease_id)
        || target.cell("cid")
            != point
                .destination_resource_key
                .values
                .get(1)
                .map(String::as_str)
    {
        return Err(ManagementError::Confirmation);
    }
    let movement = super::execution::destination_legacy(binding, point)?;
    let provider = movement
        .as_ref()
        .map_or(point.destination_allocation.as_str(), |(_, allocation)| {
            allocation.as_str()
        });
    if target.cell("provider") != Some(provider) {
        return Err(ManagementError::Confirmation);
    }
    let remote = rows
        .row(
            "remote_pins",
            &[provider, &point.destination_resource_key.values[1]],
        )
        .ok_or(ManagementError::Confirmation)?;
    let epoch = remote
        .cell("epoch")
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(ManagementError::Confirmation)?;
    if epoch < point.destination_epoch
        || point.destination_resource_id.is_some()
            && remote.cell("request_id") != point.destination_resource_id.as_deref()
    {
        return Err(ManagementError::Confirmation);
    }
    for row in rows
        .table("pin_resource_history")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
    {
        if row.cell("provider") == Some(provider)
            && row.cell("cid")
                == point
                    .destination_resource_key
                    .values
                    .get(1)
                    .map(String::as_str)
            && row
                .cell("epoch")
                .and_then(|value| value.parse::<i64>().ok())
                .is_some_and(|released| released >= point.destination_epoch && released < epoch)
        {
            return Err(ManagementError::Confirmation);
        }
    }
    Ok(())
}

fn record_key<'a>(
    binding: impl Into<records::BindingView<'a>>,
    action: &records::CommittedAction,
) -> native::NativeKey {
    let binding = binding.into();
    native::NativeKey {
        table: super::super::schema::RECORDS.into(),
        values: vec![
            binding.database_scope.clone(),
            binding.request.plan_id.clone(),
            action.receipt.binding.request.item_id.clone(),
            "action".into(),
        ],
    }
}

fn latest_points(
    points: Vec<(native::NativeKey, records::MigrationPoint)>,
    work: &mut records::DecodeBudget,
) -> Result<Vec<records::MigrationPoint>> {
    work.nodes(2)?;
    let mut groups = std::collections::BTreeMap::<_, std::collections::BTreeMap<_, _>>::new();
    for (record, point) in points {
        work.copy(&(&point.lease_id, &point.cid, &point.slot))?;
        work.nodes(3)?;
        let key = (
            point.lease_id.clone(),
            point.cid.clone(),
            point.slot.clone(),
        );
        if groups
            .entry(key)
            .or_default()
            .insert(record, point)
            .is_some()
        {
            return Err(ManagementError::InvalidEvidence);
        }
    }
    let mut output = Vec::new();
    for mut nodes in groups.into_values() {
        work.nodes(3)?;
        for point in nodes.values() {
            if let Some(key) = &point.predecessor {
                work.copy(key)?;
                work.nodes(1)?;
            }
        }
        let parents = nodes
            .values()
            .filter_map(|point| point.predecessor.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if parents.len()
            != nodes
                .values()
                .filter(|point| point.predecessor.is_some())
                .count()
        {
            return Err(ManagementError::InvalidEvidence);
        }
        for key in nodes.keys().filter(|key| !parents.contains(*key)) {
            work.copy(key)?;
            work.nodes(1)?;
        }
        let tips = nodes
            .keys()
            .filter(|key| !parents.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        if tips.len() != 1 {
            return Err(ManagementError::InvalidEvidence);
        }
        work.copy(&tips[0])?;
        let tip = tips[0].clone();
        work.copy(&tip)?;
        let mut next = Some(tip.clone());
        let mut seen = std::collections::BTreeSet::new();
        while let Some(key) = next {
            work.copy(&key)?;
            work.nodes(1)?;
            if !seen.insert(key.clone()) || seen.len() > 32 {
                return Err(ManagementError::Budget);
            }
            let predecessor = &nodes
                .get(&key)
                .ok_or(ManagementError::InvalidEvidence)?
                .predecessor;
            work.copy(predecessor)?;
            next = predecessor.clone();
        }
        if seen.len() != nodes.len() {
            return Err(ManagementError::InvalidEvidence);
        }
        output.push(nodes.remove(&tip).ok_or(ManagementError::InvalidEvidence)?);
    }
    Ok(output)
}

/// Pure counterpart of the legacy exact table/PK resolver, for frozen native
/// planning frames only. Declared namespace movement is never remote evidence.
pub(crate) fn movement_in_rows_with_work(
    frame: records::FrozenRows<'_>,
    table: &str,
    key: &std::collections::BTreeMap<String, String>,
    provider: &str,
    route: &crate::pinning::identity::ProviderRouteSnapshot,
    work: &mut records::DecodeBudget,
) -> Result<Option<records::NamespacePlanningProjection>> {
    let mut found = None;
    let operations = frame
        .table("pin_legacy_migrations")
        .ok_or(ManagementError::Schema)?;
    for row in operations
        .rows
        .iter()
        .filter(|row| native::value(row, "direction") == Some("apply"))
    {
        let id = native::value(row, "id").ok_or(ManagementError::InvalidEvidence)?;
        if operations
            .rows
            .iter()
            .any(|reverse| native::value(reverse, "reverses") == Some(id))
        {
            continue;
        }
        let encoded = frame
            .original_value("pin_legacy_migrations", &[id], "confirmation")
            .ok_or(ManagementError::InvalidEvidence)?;
        let confirmation: crate::store::pinning::legacy_migration::Confirmation =
            work.typed(encoded)?;
        // Legacy administrative scope/path labels are not Stage7's physical
        // identity. Their complete original receipt is already part of this
        // physical native frame; never reinterpret those labels as new grants.
        if confirmation.operation_id != id
            || native::value(row, "database_scope") != Some(&confirmation.database_scope)
            || confirmation.declaration.database_scope != confirmation.database_scope
        {
            return Err(ManagementError::Unauthorized);
        }
        for namespace in &confirmation.declaration.namespaces {
            if namespace.source != provider
                || !namespace.bindings.iter().any(|binding| {
                    binding.table == table
                        && binding.key == *key
                        && binding.provider == provider
                        && binding.historical_route.as_ref() == Some(route)
                })
            {
                continue;
            }
            if found.is_some() {
                return Err(ManagementError::InvalidEvidence);
            }
            {
                #[derive(serde::Serialize)]
                struct Key<'a> {
                    table: &'a str,
                    values: [&'a str; 1],
                }
                #[derive(serde::Serialize)]
                struct Projection<'a> {
                    legacy_record: Key<'a>,
                    allocation: &'a str,
                    execution_route: &'a crate::pinning::identity::ProviderRouteSnapshot,
                }
                work.copy(&Projection {
                    legacy_record: Key {
                        table: "pin_legacy_migrations",
                        values: [id],
                    },
                    allocation: &namespace.target,
                    execution_route: &namespace.target_route,
                })?;
            }
            found = Some(records::NamespacePlanningProjection {
                legacy_record: native::NativeKey {
                    table: "pin_legacy_migrations".into(),
                    values: vec![id.into()],
                },
                allocation: namespace.target.clone(),
                execution_route: namespace.target_route.clone(),
            });
        }
    }
    Ok(found)
}

pub async fn target_route<C: ConnectionTrait>(
    db: &C,
    target: &pin_lease_target::Model,
) -> Result<Option<crate::pinning::identity::ProviderRouteSnapshot>> {
    let Some(birth) = pin_lease_requirement::contract::get(db, &target.lease_id)
        .await
        .map_err(database)?
    else {
        return Ok(None);
    };
    let mut found = None;
    for slot in &birth.candidate_order {
        if let Some(resolved) = resolved_slot(db, &target.lease_id, &target.cid, slot).await?
            && resolved.point.destination_target_id == target.id
            && resolved.allocation == target.provider
        {
            if found.is_some() {
                return Err(ManagementError::InvalidEvidence);
            }
            found = Some(resolved.execution_route);
        }
    }
    Ok(found)
}

pub async fn failover_candidates<C: ConnectionTrait>(
    db: &C,
    lease: &str,
    cid: &str,
    failed: &str,
) -> Result<Option<Vec<String>>> {
    let Some(birth) = pin_lease_requirement::contract::get(db, lease)
        .await
        .map_err(database)?
    else {
        return Ok(None);
    };
    let candidates = candidates_for_cid(db, lease, cid, &birth)
        .await?
        .into_iter()
        .map(|(_, allocation, _)| allocation)
        .collect::<Vec<_>>();
    let start = candidates
        .iter()
        .position(|candidate| candidate == failed)
        .ok_or(ManagementError::Changed)?;
    Ok(Some(candidates[start..].to_vec()))
}

pub(crate) async fn failover_candidates_runtime(
    tx: &crate::store::RuntimeTransaction,
    lease: &str,
    cid: &str,
    failed: &str,
    work: records::DecodeWork<'_>,
) -> crate::error::AppResult<Option<Vec<String>>> {
    let Some(birth) = pin_lease_requirement::contract::get_with_work(tx.db(), lease, work)
        .await
        .map_err(crate::store::pinning::management::admission::work_error)?
    else {
        return Ok(None);
    };
    let candidates =
        candidates_for_cid_catalog(tx.catalog(work), lease, cid, &birth, work.budget().clone())
            .await
            .map_err(crate::store::pinning::management::admission::work_error)?
            .into_iter()
            .map(|(_, allocation, _)| allocation)
            .collect::<Vec<_>>();
    let start = candidates
        .iter()
        .position(|candidate| candidate == failed)
        .ok_or_else(|| {
            crate::store::pinning::management::admission::work_error(ManagementError::Changed)
        })?;
    Ok(Some(candidates[start..].to_vec()))
}
