use super::*;
use crate::{
    pinning::management::{
        Action, PlanEntry, RetirementCapture, RetirementSelection, RouteIdentity,
    },
    store::{
        entities::{pin_provider_route, remote_pin},
        pinning::{
            jobs::{self, NewPinJob},
            ledger,
        },
    },
};
use sea_orm::{DatabaseTransaction, Statement};

pub(super) async fn execute(
    tx: &DatabaseTransaction,
    auth: &records::LocalAuthorization,
    binding: records::BindingView<'_>,
    entry: &PlanEntry,
    capture: &RetirementCapture,
    route: &RouteIdentity,
    result: &mut records::CommittedAction,
) -> Result<()> {
    let frame = binding.frozen_rows()?;
    let mut registered = Vec::new();
    for row in frame
        .table("pin_provider_routes")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
    {
        let captured = binding.decode_route(
            row.cell("snapshot")
                .ok_or(ManagementError::InvalidEvidence)?,
        )?;
        if Some(captured.as_ref()) == route.original.as_ref() {
            binding.with_decode_work(|work| work.nodes(1))?;
            registered.push(row);
        }
    }
    if registered.len() != 1 {
        return Err(ManagementError::Confirmation);
    }
    let domain =
        native::value(registered[0], "provider").ok_or(ManagementError::InvalidEvidence)?;
    let selection = match &entry.request.action {
        Action::Retire => &crate::pinning::management::RetirementSelection::Retain,
        Action::RetireWith { selection } => selection,
        _ => return Err(ManagementError::Confirmation),
    };
    let disposition = match selection {
        RetirementSelection::Retain => records::Disposition::Retain,
        RetirementSelection::ReleaseRootReferences { .. } => {
            roots::release(tx, binding, result).await?;
            records::Disposition::Retain
        }
        RetirementSelection::Migrate { targets } => {
            let active = capture
                .targets
                .iter()
                .filter(|target| {
                    native::value(&target.native_tuple, "provider") == Some(domain)
                        && target.lease.state == crate::pinning::management::LeaseState::Active
                })
                .collect::<Vec<_>>();
            if active.len() != targets.len() {
                return Err(ManagementError::Confirmation);
            }
            binding.with_decode_work(|work| work.nodes(1 + targets.len()))?;
            let mut selected_keys = Vec::with_capacity(targets.len());
            for selected in targets {
                binding.copy(&(
                    super::super::schema::RECORDS,
                    [
                        binding.database_scope,
                        selected.plan_id.as_str(),
                        selected.item_id.as_str(),
                        "action",
                    ],
                ))?;
                selected_keys.push(native::NativeKey {
                    table: super::super::schema::RECORDS.into(),
                    values: vec![
                        binding.database_scope.into(),
                        selected.plan_id.clone(),
                        selected.item_id.clone(),
                        "action".into(),
                    ],
                });
            }
            let selected_actions = records::actions_at_keys_with_work(
                tx,
                &selected_keys,
                binding.decode_work().budget().clone(),
            )
            .await?;
            for (selected, record_key) in targets.iter().zip(&selected_keys) {
                let trusted = selected_actions
                    .iter()
                    .find(|trusted| trusted.record_key() == record_key)
                    .ok_or(ManagementError::Confirmation)?;
                let old = trusted.binding();
                let action = trusted.action_view();
                if old.database_scope != binding.database_scope
                    || old.database_identity != binding.database_identity
                    || old.principal != binding.principal
                {
                    return Err(ManagementError::Unauthorized);
                }
                let point = action
                    .migration_points
                    .iter()
                    .find(|point| {
                        point.lease_id == selected.lease_id
                            && point.source_target_id == selected.source_target_id
                            && point.cid == selected.cid
                            && selected.destination.original.as_ref()
                                == Some(&point.destination_route)
                    })
                    .ok_or(ManagementError::Confirmation)?;
                trusted.validate_point(point)?;
                binding.copy(&[("id", &point.source_target_id)])?;
                let key = [("id".into(), point.source_target_id.clone())].into();
                let source = if let Some(legacy) =
                    crate::store::pinning::legacy_migration::resolve_legacy_binding_with_work(
                        tx,
                        "pin_lease_targets",
                        &key,
                        &point.source_allocation,
                        binding,
                    )
                    .await?
                {
                    if legacy.historical_route.as_ref() != Some(&point.source_resource_route) {
                        return Err(ManagementError::Confirmation);
                    }
                    legacy.operational_key
                } else {
                    binding.copy(&point.source_allocation)?;
                    point.source_allocation.clone()
                };
                if source != domain
                    || !active
                        .iter()
                        .any(|target| target.id == point.source_target_id)
                {
                    return Err(ManagementError::Confirmation);
                }
                let resolved = points::resolved_slot_with_work(
                    tx,
                    &point.lease_id,
                    &point.cid,
                    &point.slot,
                    binding.decode_work().budget().clone(),
                )
                .await?
                .ok_or(ManagementError::Confirmation)?;
                if &resolved.point != point || &resolved.record_key != record_key {
                    return Err(ManagementError::Confirmation);
                }
                binding.copy(&point.destination_target_id)?;
                result.targets.push(point.destination_target_id.clone());
            }
            records::Disposition::Migrate
        }
        RetirementSelection::ControlledCleanup { resources } => {
            let now = crate::store::database_clock::database_now(tx)
                .await
                .map_err(|_| ManagementError::Database)?;
            for selected in resources {
                let source = frame
                    .table("remote_pins")
                    .ok_or(ManagementError::Schema)?
                    .rows
                    .iter()
                    .filter(|row| {
                        native::value(row, "provider") == Some(domain)
                            && native::value(row, "cid")
                                .and_then(|cid| {
                                    crate::pinning::provider::canonical_resource_cid(cid).ok()
                                })
                                .as_deref()
                                == Some(&selected.key.cid)
                    })
                    .collect::<Vec<_>>();
                if source.len() != 1 {
                    return Err(ManagementError::Confirmation);
                }
                let cid =
                    native::value(source[0], "cid").ok_or(ManagementError::InvalidEvidence)?;
                let remote = records::current_model::<remote_pin::Entity, _>(
                    tx,
                    &[domain, cid],
                    binding.decode_work(),
                )
                .await?
                .ok_or(ManagementError::Changed)?;
                if remote.epoch != selected.epoch
                    || remote.request_id.as_deref() != Some(&selected.resource_id)
                    || !ledger::cleanup_allowed_with_work(tx, domain, cid, binding.decode_work())
                        .await
                        .map_err(database)?
                {
                    return Err(ManagementError::Changed);
                }
                binding.copy(&(domain, cid))?;
                let refs=tx.query_one(Statement::from_sql_and_values(tx.get_database_backend(),"SELECT COUNT(*) AS n FROM pin_lease_targets WHERE provider=$1 AND cid=$2 AND state IN ('waiting','submitted','pinned','degraded')",[domain.into(),cid.into()])).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Database)?.try_get::<i64>("","n").map_err(|_|ManagementError::Database)?;
                if refs != 0 {
                    return Err(ManagementError::Changed);
                }
                binding.copy(&(domain, cid, selected))?;
                let NewPinJob::Remote(mut job) = jobs::unpin_job(domain, cid, remote.epoch, now)
                else {
                    unreachable!()
                };
                job.id = format!("management-cleanup:{}", uuid::Uuid::new_v4());
                let id = job.id.clone();
                jobs::insert_new_job_with_work(
                    tx,
                    NewPinJob::Remote(job),
                    now,
                    binding.decode_work(),
                )
                .await
                .map_err(database)?;
                result.jobs.push(records::JobAuthority {
                    job_id: id,
                    directive: records::JobDirective::ExistingCleanup,
                    provider: domain.into(),
                    cid: cid.into(),
                    remote_epoch: remote.epoch,
                    route: selected
                        .route
                        .original
                        .clone()
                        .ok_or(ManagementError::Confirmation)?,
                    resource_key: native::NativeKey {
                        table: "remote_pins".into(),
                        values: vec![domain.into(), cid.into()],
                    },
                    resource_id: remote.request_id,
                    lease_id: None,
                    lease_generation: None,
                    target_id: None,
                    owner: None,
                    source: None,
                    lease_source: None,
                    started_at: None,
                    expires_at: None,
                    policy_id: None,
                    provider_mode: None,
                    content_mode: None,
                    lease_contract: None,
                });
            }
            records::Disposition::ControlledCleanup
        }
    };
    // Stable domain barrier and actual route stop, not just an advisory record.
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_provider_routes SET retired=$1 WHERE provider=$2 AND snapshot=$3",
        [
            true.into(),
            domain.into(),
            native::value(registered[0], "snapshot")
                .ok_or(ManagementError::InvalidEvidence)?
                .into(),
        ],
    ))
    .await
    .map_err(|_| ManagementError::Database)?;
    let record = records::write_retirement_marker(
        tx,
        auth,
        binding,
        &entry.request.item_id,
        domain,
        disposition,
        vec![],
    )
    .await?;
    result.state = if record.state == records::RetirementState::NeedsAttention {
        records::ActionState::NeedsAttention
    } else if result.jobs.is_empty() {
        records::ActionState::Accepted
    } else {
        records::ActionState::Queued
    };
    let current = records::current_model::<pin_provider_route::Entity, _>(
        tx,
        &[domain],
        binding.decode_work(),
    )
    .await?
    .ok_or(ManagementError::Changed)?;
    if !current.retired {
        return Err(ManagementError::Changed);
    }
    Ok(())
}
