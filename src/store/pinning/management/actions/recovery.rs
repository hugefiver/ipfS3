use super::*;
use crate::{pinning::management::{Action, ObjectCapture, ObjectStep, RemoteDirective, SideEffect}, store::{entities::{pin_job, pin_lease, pin_lease_target, remote_pin}, pinning::{jobs::{self, NewPinJob}, ledger, leases}}};
use sea_orm::{DatabaseTransaction, EntityTrait};

pub(super) async fn execute(tx:&DatabaseTransaction,capture:&ObjectCapture,step:&ObjectStep,action:&Action,before:&native::NativeFrame,result:&mut records::CommittedAction)->Result<()> {
    let target=capture.target.as_ref().ok_or(ManagementError::Confirmation)?;
    result.receipt.intent_ids.push(target.lease.id.clone());
    result.targets.push(target.id.clone());
    if matches!(action,Action::Reconcile) || matches!(step.source,RemoteDirective::ReconcileHistorical|RemoteDirective::ObserveExisting) {
        observe_capture(tx,capture,before,result).await?;
    } else if step.source==RemoteDirective::WaitForInflight {
        result.state=records::ActionState::Waiting;
        // A durable readonly observation can wait for the original in-flight
        // invocation; it can never become its replacement Submit/Unpin.
        observe_capture(tx,capture,before,result).await?;
    } else if step.source==RemoteDirective::ReuseConfirmed {
        let model=pin_lease_target::Entity::find_by_id(target.id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
        let remote=remote_pin::Entity::find_by_id((model.provider.clone(),model.cid.clone())).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
        let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
        if remote.request_id.is_none()||!matches!(remote.status.as_str(),"pinned"|"pinning"){return Err(ManagementError::Changed);}
        if !crate::store::entities::pin_lease_requirement::contract::target_confirmed(tx,&target.lease.id,&model.provider,&model.cid,now).await.map_err(database)?
            ||leases::project_reconcile_target_from_remote(tx,&model.id,remote.epoch,now).await.map_err(database)?.is_none(){return Err(ManagementError::Changed);}
        result.state=records::ActionState::RemoteComplete;
    } else if step.source==RemoteDirective::SubmitWithFence {
        let model=pin_lease_target::Entity::find_by_id(target.id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
        if let Some(remote)=remote_pin::Entity::find_by_id((model.provider.clone(),model.cid.clone())).one(tx).await.map_err(|_|ManagementError::Database)?
            && remote.status=="failed" && let Some(request)=&remote.request_id {
            let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
            let outcome=leases::prepare_failed_remote_resubmit(tx,&model.provider,&model.cid,remote.epoch,request,now).await.map_err(database)?;
            if !matches!(outcome,leases::FailedRemoteResubmitDecision::Prepared {..}) {return Err(ManagementError::Changed);}
        } else {schedule_target(tx,capture,&model,before,result).await?;}
    } else {return Err(ManagementError::Confirmation);}
    Ok(())
}

pub(super) async fn schedule_target(tx:&DatabaseTransaction,capture:&ObjectCapture,target:&pin_lease_target::Model,before:&native::NativeFrame,result:&mut records::CommittedAction)->Result<()> {
    if matches!(target.state.as_str(),"quota_waiting"|"quota_blocked") {result.state=records::ActionState::NeedsAttention;return Ok(());}
    let remote=remote_pin::Entity::find_by_id((target.provider.clone(),target.cid.clone())).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    let row=ledger::get(tx,&target.provider,&target.cid).await.map_err(database)?.ok_or(ManagementError::InvalidEvidence)?;
    let route=ledger::decode_route(&row).ok_or(ManagementError::InvalidEvidence)?;
    let captured=capture.remote.as_ref().filter(|remote|remote.route.original.as_ref()==Some(&route))
        .or_else(||capture.destination_remote.as_ref().filter(|remote|remote.route.original.as_ref()==Some(&route)));
    let old=before.row("remote_pins",&[&target.provider,&target.cid]);
    let history=before.table("pin_resource_history").ok_or(ManagementError::Schema)?.rows.iter().any(|row|native::value(row,"provider")==Some(&target.provider)&&native::value(row,"cid")==Some(&target.cid));
    let safe_new=old.is_none()&&!history;
    let safe_rejected=captured.is_some_and(|remote|matches!(remote.effect,SideEffect::NotSubmitted|SideEffect::Rejected)&&remote.inflight_operations==Some(0)&&!remote.retained&&!remote.cleanup_pending);
    let desired=pin_lease::Entity::find_by_id(target.lease_id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
    if desired.state!="active"||desired.expires_at<=now {return Err(ManagementError::Expired);}
    if row.effect=="confirmed" && crate::store::entities::pin_lease_requirement::contract::target_confirmed(tx,&desired.id,&target.provider,&target.cid,now).await.map_err(database)? {
        if leases::project_reconcile_target_from_remote(tx,&target.id,remote.epoch,now).await.map_err(database)?.is_none(){return Err(ManagementError::Changed);}
        result.state=records::ActionState::RemoteComplete;
    } else if (safe_new||safe_rejected)&&remote.status=="reserved"&&remote.request_id.is_none() {
        let NewPinJob::Target(mut job)=jobs::submit_job(&target.provider,&target.cid,&desired.id,&target.id,desired.generation,now) else{unreachable!()};
        // A management job receives its own exact durable authority, never a
        // reused confirmation UUID permitting a whole provider queue.
        job.id=format!("management-submit:{}",uuid::Uuid::new_v4());
        let id=job.id.clone();
        jobs::ensure_or_reactivate_submit_job(tx,job,now).await.map_err(database)?;
        if pin_job::Entity::find_by_id(id).one(tx).await.map_err(|_|ManagementError::Database)?.is_some() {result.state=records::ActionState::Queued;}
        else {result.state=records::ActionState::Waiting;}
    } else {
        observe(tx,&target.provider,&target.cid,remote.epoch,&route,&desired.id,capture,result,native::NativeKey {table:"remote_pins".into(),values:vec![remote.provider.clone(),remote.cid.clone()]},remote.request_id.clone()).await?;
    }
    Ok(())
}

pub(super) async fn observe_capture(tx:&DatabaseTransaction,capture:&ObjectCapture,before:&native::NativeFrame,result:&mut records::CommittedAction)->Result<()> {
    let target=capture.target.as_ref().ok_or(ManagementError::Confirmation)?;
    let provider=native::value(&target.native_tuple,"provider").ok_or(ManagementError::InvalidEvidence)?;
    let cid=native::value(&target.native_tuple,"cid").ok_or(ManagementError::InvalidEvidence)?;
    let remote=remote_pin::Entity::find_by_id((provider.to_owned(),cid.to_owned())).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    let route=capture.remote_route.as_ref().and_then(|route|route.original.as_ref()).or(capture.route.original.as_ref()).ok_or(ManagementError::InvalidEvidence)?;
    observe(tx,provider,cid,remote.epoch,route,&target.lease.id,capture,result,native::NativeKey {table:"remote_pins".into(),values:vec![remote.provider.clone(),remote.cid.clone()]},remote.request_id.clone()).await?;
    for historical in &capture.historical_remotes {
        let original=historical.route.original.as_ref().ok_or(ManagementError::InvalidEvidence)?;
        let key=historical_key(before,historical)?;
        // Historical observation is fenced to its captured epoch and immutable
        // route even if the current epoch has since changed. C consumes it as
        // readonly debt evidence, not current-resource mutation authority.
        observe(tx,&key.values[0],&key.values[1],historical.epoch,original,&target.lease.id,capture,result,key.clone(),historical.resource_id.clone()).await?;
    }
    Ok(())
}

fn historical_key(frame:&native::NativeFrame,remote:&crate::pinning::management::RemoteSnapshot)->Result<native::NativeKey> {
    let mut found=None;
    for key in frame.frontier.iter().filter(|key|key.table=="pin_resource_history"&&key.values.len()==3) {
        if key.values[2].parse::<i64>().ok()!=Some(remote.epoch)||crate::pinning::provider::canonical_resource_cid(&key.values[1]).ok().as_deref()!=Some(&remote.key.cid) {continue;}
        let encoded=frame.original_value("pin_resource_history",&[&key.values[0],&key.values[1],&key.values[2]],"ledger").ok_or(ManagementError::InvalidEvidence)?;
        let envelope:serde_json::Value=serde_json::from_str(encoded).map_err(|_|ManagementError::InvalidEvidence)?;
        let original=envelope.get("ledger").and_then(|ledger|ledger.get("route")).and_then(serde_json::Value::as_str).and_then(|route|serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(route).ok());
        if original.as_ref()!=remote.route.original.as_ref()||envelope.get("request_id").and_then(serde_json::Value::as_str)!=remote.resource_id.as_deref() {continue;}
        if found.is_some() {return Err(ManagementError::InvalidEvidence);}found=Some(key.clone());
    }
    found.ok_or(ManagementError::InvalidEvidence)
}

#[allow(clippy::too_many_arguments)]
async fn observe(tx:&DatabaseTransaction,provider:&str,cid:&str,epoch:i64,route:&crate::pinning::identity::ProviderRouteSnapshot,lease:&str,capture:&ObjectCapture,result:&mut records::CommittedAction,resource_key:native::NativeKey,resource_id:Option<String>)->Result<()> {
    if result.jobs.iter().any(|job|job.directive==records::JobDirective::ReadOnlyObserve&&job.provider==provider&&job.cid==cid&&job.remote_epoch==epoch&&job.route==*route) {return Ok(());}
    let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
    let NewPinJob::Remote(mut job)=jobs::reconcile_job(provider,cid,epoch,now) else{unreachable!()};
    job.id=format!("management-observe:{}",uuid::Uuid::new_v4());
    let id=job.id.clone();
    job_authority::enqueue_observation(tx,&id,provider,cid,epoch,route,now).await?;
    let lease=pin_lease::Entity::find_by_id(lease.to_owned()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    let contract=crate::store::entities::pin_lease_requirement::Entity::find_by_id(lease.id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.map(|row|row.contract);
    result.jobs.push(records::JobAuthority {job_id:id,directive:records::JobDirective::ReadOnlyObserve,provider:provider.into(),cid:cid.into(),remote_epoch:epoch,route:route.clone(),resource_key,resource_id,lease_id:Some(lease.id),lease_generation:Some(lease.generation),target_id:None,owner:Some(capture.owner_identity.clone()),source:Some(capture.object.identity.clone()),lease_source:Some(lease.source),started_at:Some(lease.created_at),expires_at:Some(lease.expires_at),policy_id:Some(lease.policy_id),provider_mode:Some(lease.provider_mode),content_mode:Some(lease.content_mode),lease_contract:contract});
    result.state=records::ActionState::Queued;
    Ok(())
}
