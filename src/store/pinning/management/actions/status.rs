//! Current status is evidence, not a renewed confirmation or lease capability.
use super::*;
use crate::{pinning::{identity::RemoteResourceType,management::{Action,Capture}},store::entities::{pin_job,pin_lease,pin_lease_requirement,pin_lease_target,remote_pin}};
use sea_orm::{ConnectionTrait,DatabaseTransaction,EntityTrait};
mod observations;

pub(super) async fn fence_business_expiry(tx:&DatabaseTransaction,items:&[records::CommittedAction])->Result<()> {
    let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
    for item in items {
        let action=&item.receipt.binding.request.action;
        if matches!(action,Action::Cancel|Action::Reconcile|Action::Retire|Action::RetireWith {..}) {continue;}
        // An unknown/inflight Retry/Migrate can only create readonly directives.
        if matches!(action,Action::Retry|Action::Migrate {..})&&!item.jobs.is_empty()&&item.jobs.iter().all(|job|job.directive==records::JobDirective::ReadOnlyObserve)&&item.migration_points.is_empty(){continue;}
        if let Some(Capture::Object(capture))=&item.receipt.binding.capture {
            if let Some(proof)=&capture.object.read_evidence&&proof.proof==crate::pinning::management::evidence::ReadProof::CompleteLocalDirectoryDagBlocksV1
                &&(proof.observed_at>now||proof.expires_at<=now){return Err(ManagementError::Expired);}
            if let Some(target)=&capture.target {
                let tuple=target.lease.native_tuple.as_ref().ok_or(ManagementError::InvalidEvidence)?;
                if native::parse_time(native::value(tuple,"expires_at").ok_or(ManagementError::InvalidEvidence)?)?<=now {return Err(ManagementError::Expired);}
            }
        }
        for id in &item.receipt.intent_ids {
            let lease=pin_lease::Entity::find_by_id(id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
            if lease.state!="active"||lease.expires_at<=now {return Err(ManagementError::Expired);}
        }
    }
    Ok(())
}

pub async fn lookup<C:ConnectionTrait>(db:&C,auth:&records::LocalAuthorization,binding:&records::ConfirmedBinding,item_id:&str)->Result<Option<records::CommittedAction>> {
    let Some(mut action)=records::lookup_committed(db,auth,binding,item_id).await? else{return Ok(None)};
    action.state=current_state(db,&action).await?;Ok(Some(action))
}

pub(super) async fn current_state<C:ConnectionTrait>(db:&C,action:&records::CommittedAction)->Result<records::ActionState> {
    use records::ActionState::*;
    if matches!(action.receipt.binding.request.action,Action::Cancel) {return Ok(Accepted);}
    if matches!(action.receipt.binding.request.action,Action::Retire|Action::RetireWith {..}) {return Ok(action.state);}
    if action.state==NeedsAttention||observations::needs_attention(db,action).await? {return Ok(NeedsAttention);}
    let now=crate::store::database_clock::database_now(db).await.map_err(|_|ManagementError::Database)?;
    let mut complete=!action.receipt.intent_ids.is_empty();let mut replica_required=false;
    for id in &action.receipt.intent_ids {
        let lease=pin_lease::Entity::find_by_id(id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
        if lease.state!="active"||lease.expires_at<=now {return Ok(NeedsAttention);}
        let birth=pin_lease_requirement::contract::get(db,id).await.map_err(database)?.ok_or(ManagementError::InvalidEvidence)?;
        let requirements=pin_lease_requirement::contract::management_requirements(db,id).await.map_err(database)?;
        replica_required|=requirements.as_ref().is_some_and(|requirements|requirements.levels.contains(&crate::pinning::management::ConfirmationLevel::ReplicationComplete))||!birth.candidates.is_empty()&&birth.candidates.values().all(|route|route.as_ref().is_some_and(|route|route.resource_type()==RemoteResourceType::ClusterPin));
        let Some((any,all,_))=pin_lease_requirement::contract::completion(db,id,now).await.map_err(database)? else{return Err(ManagementError::InvalidEvidence)};
        complete&=if birth.provider_mode=="all"{all}else{any};
    }
    // The completion consumer verifies every frozen slot/CID, actual target,
    // epoch/route ledger evidence and original Cluster threshold/freshness.
    if matches!(action.receipt.binding.request.action,Action::Reconcile){return Ok(ReadOnly);}
    if complete {return Ok(if replica_required{ReplicationComplete}else{RemoteComplete});}
    if !action.jobs.is_empty()&&action.jobs.iter().all(|job|job.directive==records::JobDirective::ReadOnlyObserve){return Ok(ReadOnly);}
    for authority in &action.jobs {
        if let Some(job)=pin_job::Entity::find_by_id(authority.job_id.clone()).one(db).await.map_err(|_|ManagementError::Database)? {
            if matches!(job.state.as_str(),"pending"|"retry"|"running"){return Ok(Queued);}
        }
    }
    for id in &action.targets {
        let target=pin_lease_target::Entity::find_by_id(id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
        if matches!(target.state.as_str(),"quota_waiting"|"quota_blocked"|"failed"){return Ok(NeedsAttention);}
        if let Some(remote)=remote_pin::Entity::find_by_id((target.provider,target.cid)).one(db).await.map_err(|_|ManagementError::Database)?&&matches!(remote.status.as_str(),"queued"|"pinning"){return Ok(Waiting);}
    }
    Ok(Accepted)
}
