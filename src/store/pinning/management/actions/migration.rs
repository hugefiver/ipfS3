use super::*;
use crate::{pinning::management::{ObjectCapture,ObjectStep,RemoteDirective,RouteIdentity},store::{entities::{pin_lease,pin_lease_target,remote_pin},pinning::{ledger,publication::{management::attach_destination_in_transaction,PinTargetSpec}}}};
use sea_orm::{DatabaseTransaction,EntityTrait};

pub(super) async fn execute(tx:&DatabaseTransaction,config:&ManagementConfig,capture:&ObjectCapture,step:&ObjectStep,destination:&RouteIdentity,before:&native::NativeFrame,result:&mut records::CommittedAction)->Result<()> {
    let target=capture.target.as_ref().ok_or(ManagementError::Confirmation)?;
    let source=pin_lease_target::Entity::find_by_id(target.id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    let lease=pin_lease::Entity::find_by_id(target.lease.id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    result.receipt.intent_ids.push(lease.id.clone());
    result.targets.push(source.id.clone());
    if step.destination==Some(RemoteDirective::NoWrite) {
        recovery::observe_capture(tx,capture,before,result).await?;
        result.state=records::ActionState::NeedsAttention;
        return Ok(());
    }
    let original=capture.route.original.as_ref().ok_or(ManagementError::InvalidEvidence)?;
    let destination_route=destination.original.as_ref().ok_or(ManagementError::InvalidEvidence)?;
    let provider=config.pinning.providers.iter().find(|provider|provider.route_snapshot()==*destination_route).ok_or(ManagementError::Changed)?;
    let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
    let bytes=intents::stored_bytes(capture,now)?;
    let proof=accounting::current_proof(before,config,&capture.object.identity,now)?.ok_or(ManagementError::InvalidEvidence)?;
    if capture.object.read_evidence.as_ref()!=Some(&crate::pinning::management::ReadEvidence::from(&proof)){return Err(ManagementError::Changed);}
    let count=accounting::count(capture.object.read_evidence.as_ref().ok_or(ManagementError::InvalidEvidence)?)?;
    if proof.proof.is_directory()&&!matches!(destination_route.strategy.as_str(),"cid"|"car"){return Err(ManagementError::Confirmation);}
    accounting::check_frame(before,&provider.name,&source.cid,count)?;
    if proof.proof.is_directory(){accounting::check_frame(before,&source.provider,&source.cid,count)?;}
    intents::check_accounting(tx,&provider.name,&source.cid,bytes).await?;
    let source_remote=remote_pin::Entity::find_by_id((source.provider.clone(),source.cid.clone())).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
    let contract=before.row("pin_lease_requirements",&[&lease.id]).cloned().ok_or(ManagementError::InvalidEvidence)?;
    let birth:crate::store::entities::pin_lease_requirement::contract::BirthContract=serde_json::from_str(native::value(&contract,"contract").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?;
    let slots=target.frozen_candidates.iter().filter(|candidate|candidate.allocation==source.provider).collect::<Vec<_>>();
    if slots.len()!=1 {return Err(ManagementError::Confirmation);}
    let slot=&slots[0].slot;
    let predecessor=points::resolved_slot(tx,&lease.id,&source.cid,slot).await?;
    if let Some(previous)=&predecessor {
        if previous.allocation!=source.provider||previous.point.destination_target_id!=source.id||previous.execution_route!=*original {return Err(ManagementError::Confirmation);}
    }else if birth.candidates.get(slot).and_then(Option::as_ref)!=Some(original) {return Err(ManagementError::Confirmation);}
    let same_domain=destination.remote_key(&target.cid)==capture.route.identity.remote_key(&target.cid);
    let attached=if same_domain {
        if provider.name!=source.provider {return Err(ManagementError::Confirmation);}
        source.clone()
    } else {
        attach_destination_in_transaction(tx,&lease.id,&provider.name,&PinTargetSpec {cid:source.cid.clone(),logical_size:i64::try_from(bytes).map_err(|_|ManagementError::Budget)?},&config.pinning.provider_limits,now).await.map_err(database)?
    };
    let remote=remote_pin::Entity::find_by_id((attached.provider.clone(),attached.cid.clone())).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    // Reject a migration of two logical One/All slots into one resource. Native
    // quota dedup still applies across distinct owners, never inside a contract.
    for candidate in &target.frozen_candidates {
        if candidate.slot!=*slot && candidate.allocation==attached.provider {return Err(ManagementError::Confirmation);}
    }
    result.targets.push(attached.id.clone());
    result.migration_points.push(records::MigrationPoint {
        table:"pin_lease_requirements".into(),primary_key:native::NativeKey {table:"pin_lease_requirements".into(),values:vec![lease.id.clone()]},
        capture_position:format!("candidates/{slot}"),original_contract:contract,slot:slot.clone(),lease_id:lease.id.clone(),cid:target.cid.clone(),source_target_id:source.id,source_allocation:source.provider,
        source_route:original.clone(),source_resource_route:capture.remote_route.as_ref().and_then(|route|route.original.clone()).ok_or(ManagementError::InvalidEvidence)?,source_epoch:source_remote.epoch,source_resource_id:source_remote.request_id,
        source_resource_key:native::NativeKey {table:"remote_pins".into(),values:vec![source_remote.provider,source_remote.cid]},source_generation:lease.generation,source_lease:before.row("pin_leases",&[&lease.id]).cloned().ok_or(ManagementError::InvalidEvidence)?,predecessor:predecessor.map(|previous|previous.record_key),
        destination_allocation:attached.provider.clone(),destination_route:destination_route.clone(),destination_target_id:attached.id.clone(),destination_epoch:remote.epoch,started_at:lease.created_at,expires_at:lease.expires_at,
        destination_resource_key:native::NativeKey {table:"remote_pins".into(),values:vec![remote.provider,remote.cid]},destination_resource_id:remote.request_id,planning_projection:None,
    });
    if same_domain {
        // Changing the execution route of the original logical slot does not
        // recapture/rewrite resource history or reserve/release physical quota.
        recovery::observe_capture(tx,capture,before,result).await?;
    } else {
        let ledger=ledger::get(tx,&attached.provider,&attached.cid).await.map_err(database)?.ok_or(ManagementError::InvalidEvidence)?;
        if ledger::decode_route(&ledger).as_ref()!=Some(destination_route) {return Err(ManagementError::Changed);}
        recovery::schedule_target(tx,capture,&attached,before,result).await?;
    }
    Ok(())
}
