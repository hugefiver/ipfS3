use super::*;
use crate::{
    pinning::management::RemoteDirective,
    store::{
        entities::{pin_lease, pin_lease_target, remote_pin},
        pinning::{ledger, publication::PinTargetSpec},
    },
};
use sea_orm::DatabaseTransaction;

pub(super) async fn execute(
    tx: &DatabaseTransaction,
    config: &ManagementConfig,
    entry: &crate::pinning::management::PlanEntry,
    before: records::FrozenRows<'_>,
    result: &mut records::CommittedAction,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let (
        Some(crate::pinning::management::Capture::Object(capture)),
        Some(crate::pinning::management::PlannedStep::Object(step)),
        crate::pinning::management::Action::Migrate { destination },
    ) = (&entry.capture, &entry.step, &entry.request.action)
    else {
        return Err(ManagementError::Confirmation);
    };
    let target = capture
        .target
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let source = records::current_model::<pin_lease_target::Entity, _>(tx, &[&target.id], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    let lease = records::current_model::<pin_lease::Entity, _>(tx, &[&target.lease.id], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    work.copy(&(&lease.id, &source.id))?;
    result.receipt.intent_ids.push(lease.id.clone());
    result.targets.push(source.id.clone());
    if step.destination == Some(RemoteDirective::NoWrite) {
        recovery::observe_capture(tx, capture, before, result, work).await?;
        result.state = records::ActionState::NeedsAttention;
        return Ok(());
    }
    let original = capture
        .route
        .original
        .as_ref()
        .ok_or(ManagementError::InvalidEvidence)?;
    let destination_route = destination
        .original
        .as_ref()
        .ok_or(ManagementError::InvalidEvidence)?;
    let provider = config
        .pinning
        .providers
        .iter()
        .find(|provider| provider.identity.provider_id == destination.provider_id)
        .ok_or(ManagementError::Changed)?;
    work.copy(destination_route)?;
    if provider.route_snapshot() != *destination_route {
        return Err(ManagementError::Changed);
    }
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    let bytes = intents::stored_bytes(capture, now)?;
    let proof = work
        .with(|work| {
            accounting::current_proof_from_source(
                before,
                &config.source_endpoint,
                config.source_node(),
                &capture.object.identity,
                now,
                work,
            )
        })?
        .ok_or(ManagementError::InvalidEvidence)?;
    if capture.object.read_evidence.as_ref()
        != Some(&crate::pinning::management::ReadEvidence::from(
            proof.as_ref(),
        ))
    {
        return Err(ManagementError::Changed);
    }
    let count = accounting::count(
        capture
            .object
            .read_evidence
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?,
    )?;
    if proof.proof.is_directory() && !matches!(destination_route.strategy.as_str(), "cid" | "car") {
        return Err(ManagementError::Confirmation);
    }
    accounting::check_rows_with_work(
        before,
        &provider.name,
        &source.cid,
        count,
        work.budget().clone(),
    )?;
    if proof.proof.is_directory() {
        accounting::check_rows_with_work(
            before,
            &source.provider,
            &source.cid,
            count,
            work.budget().clone(),
        )?;
    }
    intents::check_accounting(tx, &provider.name, &source.cid, bytes, work).await?;
    let source_remote =
        records::current_model::<remote_pin::Entity, _>(tx, &[&source.provider, &source.cid], work)
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
    let contract = work.copy_out(
        before
            .row("pin_lease_requirements", &[&lease.id])
            .ok_or(ManagementError::InvalidEvidence)?,
    )?;
    let birth: crate::store::entities::pin_lease_requirement::contract::BirthContract =
        work.with(|work| {
            work.typed(
                native::value(&contract, "contract").ok_or(ManagementError::InvalidEvidence)?,
            )
        })?;
    work.with(|work| work.nodes(1 + target.frozen_candidates.len()))?;
    let slots = target
        .frozen_candidates
        .iter()
        .filter(|candidate| candidate.allocation == source.provider)
        .collect::<Vec<_>>();
    if slots.len() != 1 {
        return Err(ManagementError::Confirmation);
    }
    let slot = &slots[0].slot;
    let predecessor =
        points::resolved_slot_with_work(tx, &lease.id, &source.cid, slot, work.budget().clone())
            .await?;
    if let Some(previous) = &predecessor {
        if previous.allocation != source.provider
            || previous.point.destination_target_id != source.id
            || previous.execution_route != *original
        {
            return Err(ManagementError::Confirmation);
        }
    } else if birth.candidates.get(slot).and_then(Option::as_ref) != Some(original) {
        return Err(ManagementError::Confirmation);
    }
    let same_domain =
        destination.remote_key(&target.cid) == capture.route.identity.remote_key(&target.cid);
    let attached = if same_domain {
        if provider.name != source.provider {
            return Err(ManagementError::Confirmation);
        }
        work.copy(&(
            &source.id,
            &source.lease_id,
            &source.provider,
            &source.cid,
            &source.state,
        ))?;
        source.clone()
    } else {
        work.copy(&source.cid)?;
        crate::store::pinning::publication::management::attach_destination_with_work(
            tx,
            &lease.id,
            &provider.name,
            &PinTargetSpec {
                cid: source.cid.clone(),
                logical_size: i64::try_from(bytes).map_err(|_| ManagementError::Budget)?,
            },
            &config.pinning.provider_limits,
            now,
            work,
        )
        .await
        .map_err(database)?
    };
    let remote = records::current_model::<remote_pin::Entity, _>(
        tx,
        &[&attached.provider, &attached.cid],
        work,
    )
    .await?
    .ok_or(ManagementError::Changed)?;
    // Reject a migration of two logical One/All slots into one resource. Native
    // quota dedup still applies across distinct owners, never inside a contract.
    for candidate in &target.frozen_candidates {
        if candidate.slot != *slot && candidate.allocation == attached.provider {
            return Err(ManagementError::Confirmation);
        }
    }
    work.copy(&(
        &attached.id,
        &lease.id,
        slot,
        &lease.id,
        &target.cid,
        original,
        &capture.remote_route,
        &attached.provider,
        destination_route,
        &attached.id,
    ))?;
    result.targets.push(attached.id.clone());
    result.migration_points.push(records::MigrationPoint {
        table: "pin_lease_requirements".into(),
        primary_key: native::NativeKey {
            table: "pin_lease_requirements".into(),
            values: vec![lease.id.clone()],
        },
        capture_position: format!("candidates/{slot}"),
        original_contract: contract,
        slot: slot.clone(),
        lease_id: lease.id.clone(),
        cid: target.cid.clone(),
        source_target_id: source.id,
        source_allocation: source.provider,
        source_route: original.clone(),
        source_resource_route: capture
            .remote_route
            .as_ref()
            .and_then(|route| route.original.clone())
            .ok_or(ManagementError::InvalidEvidence)?,
        source_epoch: source_remote.epoch,
        source_resource_id: source_remote.request_id,
        source_resource_key: native::NativeKey {
            table: "remote_pins".into(),
            values: vec![source_remote.provider, source_remote.cid],
        },
        source_generation: lease.generation,
        source_lease: work.copy_out(
            before
                .row("pin_leases", &[&lease.id])
                .ok_or(ManagementError::InvalidEvidence)?,
        )?,
        predecessor: predecessor.map(|previous| previous.record_key),
        destination_allocation: attached.provider.clone(),
        destination_route: destination_route.clone(),
        destination_target_id: attached.id.clone(),
        destination_epoch: remote.epoch,
        started_at: lease.created_at,
        expires_at: lease.expires_at,
        destination_resource_key: native::NativeKey {
            table: "remote_pins".into(),
            values: vec![remote.provider, remote.cid],
        },
        destination_resource_id: remote.request_id,
        planning_projection: None,
    });
    if same_domain {
        // Changing the execution route of the original logical slot does not
        // recapture/rewrite resource history or reserve/release physical quota.
        recovery::observe_capture(tx, capture, before, result, work).await?;
    } else {
        let ledger = ledger::get_with_work(tx, &attached.provider, &attached.cid, work)
            .await
            .map_err(database)?
            .ok_or(ManagementError::InvalidEvidence)?;
        if work.with(|work| {
            work.typed::<crate::pinning::identity::ProviderRouteSnapshot>(
                ledger
                    .route
                    .as_deref()
                    .ok_or(ManagementError::InvalidEvidence)?,
            )
        })? != *destination_route
        {
            return Err(ManagementError::Changed);
        }
        recovery::schedule_target(tx, capture, &attached, before, result, work).await?;
    }
    Ok(())
}
