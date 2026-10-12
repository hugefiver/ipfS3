//! A live successor is derived from native invocation/submission/desired-target
//! evidence. The stored seed, its route and original epoch are never rewritten.
use super::*;
use crate::store::pinning::jobs;

/// A receipt candidate is not a parent grant. Require the actual original
/// Submit plus both immutable invocation locators and the native request-bound
/// Poll. Live lease/resource/release fences remain in `current_resource`.
pub(super) async fn original_submit_for_poll<C:ConnectionTrait>(db:&C,poll:&pin_job::Model,seed:&records::JobAuthority,invocation:&pin_invocation_route::Model)->Result<bool> {
    let Some(original)=pin_job::Entity::find_by_id(seed.job_id.clone()).one(db).await.map_err(|_|ManagementError::Database)? else{return Ok(false)};
    if original.operation!="submit"{return Ok(false);}
    validate_job_resolved(db,&original,seed).await?;
    let mut successor=seed.clone();successor.job_id=poll.id.clone();
    let current=pin_job::Entity::find_by_id(poll.id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Confirmation)?;
    validate_job_resolved(db,&current,&successor).await?;
    let source=pin_invocation_route::Entity::find_by_id(original.id).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Confirmation)?;
    if source.remote_epoch<seed.remote_epoch||source.remote_epoch>invocation.remote_epoch
        ||serde_json::from_str::<ProviderRouteSnapshot>(&source.route).ok().as_ref()!=Some(&seed.route){return Err(ManagementError::Confirmation);}
    let provider=operational_resource(db,seed).await?;
    let Some(remote)=remote_pin::Entity::find_by_id((provider,seed.cid.clone())).one(db).await.map_err(|_|ManagementError::Database)? else{return Ok(false)};
    let Some(request)=remote.request_id.as_deref() else{return Ok(false)};
    jobs::poll_names_request(db,poll,request).await.map_err(database)
}

pub(super) async fn current_resource<C:ConnectionTrait>(db:&C,job:&pin_job::Model,authority:&records::JobAuthority)->Result<bool> {
    let provider=operational_resource(db,authority).await?;
    let Some(remote)=remote_pin::Entity::find_by_id((provider.clone(),authority.cid.clone())).one(db).await.map_err(|_|ManagementError::Database)? else{return Ok(false)};
    let Some(invocation)=pin_invocation_route::Entity::find_by_id(job.id.clone()).one(db).await.map_err(|_|ManagementError::Database)? else{return Ok(false)};
    let row=ledger::get(db,&provider,&authority.cid).await.map_err(database)?;
    if remote.epoch<authority.remote_epoch||invocation.remote_epoch<authority.remote_epoch||invocation.remote_epoch>remote.epoch
        ||row.as_ref().and_then(ledger::decode_route).as_ref()!=Some(&authority.route)
        ||serde_json::from_str::<ProviderRouteSnapshot>(&invocation.route).ok().as_ref()!=Some(&authority.route)
        ||authority.resource_id.is_some()&&authority.resource_id!=remote.request_id{return Ok(false);}
    if authority.directive!=records::JobDirective::Submit{return Ok(remote.epoch==authority.remote_epoch&&invocation.remote_epoch==authority.remote_epoch);}
    let releases=crate::store::pinning::legacy_migration::release_history(db,&provider,&authority.cid).await.map_err(database)?;
    if releases.iter().any(|release|release.epoch>=authority.remote_epoch&&release.epoch<=remote.epoch){return Ok(false);}
    if !jobs::check_target_job_generation(db,job).await.map_err(database)?{return Ok(false);}
    if job.operation=="poll" {
        // This exact durable Poll ID binds the original request, target and
        // generation; current namespace/epoch alone is not continuity evidence.
        let Some(request)=remote.request_id.as_deref() else{return Ok(false)};
        return jobs::poll_continuity_owner(db,job,request).await.map_err(database);
    }
    let current=pin_job::Entity::find_by_id(job.id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    let history=jobs::submission_history(db,&job.id).await.map_err(database)?;
    if history.as_ref().is_some_and(|history|history.api!=authority.route.api_profile||history.strategy!=authority.route.strategy||history.state!="active"||history.submit_calls<=0){return Ok(false);}
    if matches!(current.submit_phase.as_deref(),Some("recovering"|"recovery_backoff")){return Ok(false);}
    if remote.epoch==authority.remote_epoch&&invocation.remote_epoch==authority.remote_epoch{return Ok(true);}
    if remote.status!="reserved"||remote.request_id.is_some(){return Ok(false);}
    // Before the existing native epoch CAS, only an exact fresh ready source or
    // its durable not-created submission proof may proceed. No bare missing
    // invocation/history is interpreted as evidence that a POST never occurred.
    if invocation.remote_epoch!=remote.epoch {
        return Ok(match history {
            None=>current.submit_phase.as_deref()==Some("ready")&&current.attempts==0&&matches!(current.state.as_str(),"pending"|"running")&&authority.resource_id.is_none()&&row.is_some_and(|row|matches!(row.effect.as_str(),"reserved"|"not_created")),
            Some(history)=>history.effect=="not_created"&&history.submit_calls<8,
        });
    }
    // The existing claimed prepare transaction has persisted the successor
    // invocation and exact calling/history footprint before provider IO.
    Ok(history.is_some_and(|history|matches!(history.effect.as_str(),"unknown"|"not_created")&&current.submit_phase.as_deref()==Some("calling")&&current.state=="running"&&current.locked_until.is_some()))
}
