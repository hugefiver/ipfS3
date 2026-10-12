//! Worker-facing typed authority: exact applied action, job/source, immutable
//! route, lease generation and remote epoch. No queue-wide confirmation permit.
use super::*;
use crate::{error::{AppError,AppResult},pinning::{identity::ProviderRouteSnapshot,management::{Capture,PlanEntry}},store::{entities::{object_version,pin_invocation_route,pin_job,pin_lease,remote_pin},pinning::ledger}};
use chrono::{DateTime,Utc};
use sea_orm::{ColumnTrait,ConnectionTrait,DatabaseTransaction,EntityTrait,QueryFilter,Set};
mod successor;

#[derive(Clone,PartialEq,Eq)]
pub enum JobPermit {
    Submit(records::JobAuthority),
    ReadOnly(records::JobAuthority),
    ExistingCleanup(records::JobAuthority),
    /// Late facts may enter the existing ledger, never project/resurrect a lease.
    LedgerOnly(records::JobAuthority),
}

/// Caller invokes this under its normal claim/lifecycle/remote transaction at
/// job birth/claim/heartbeat/before IO. None preserves an ordinary job's existing
/// authority; it does not mean a missing management binding is authorized.
pub async fn check_before_call<C:ConnectionTrait>(db:&C,job:&pin_job::Model)->AppResult<Option<JobPermit>> {
    let Some(authority)=lookup(db,job).await.map_err(safe)? else{return Ok(None)};
    if authority.directive==records::JobDirective::ReadOnlyObserve {
        // Historical/late observation is allowed as readonly ledger evidence,
        // never current lease projection, resubmit or compensation authority.
        return Ok(Some(JobPermit::ReadOnly(authority)));
    }
    let now=crate::store::database_clock::database_now(db).await?;
    if !successor::current_resource(db,job,&authority).await.map_err(safe)? || !current_lease(db,&authority,now).await.map_err(safe)? {
        return Ok(Some(JobPermit::LedgerOnly(authority)));
    }
    let permit=match authority.directive {
        records::JobDirective::Submit=> {
            if !matches!(job.operation.as_str(),"submit"|"poll") {return Err(safe(ManagementError::Confirmation));}
            JobPermit::Submit(authority)
        },
        records::JobDirective::ReadOnlyObserve=> {
            if job.operation!="reconcile" {return Err(safe(ManagementError::Confirmation));}
            JobPermit::ReadOnly(authority)
        },
        records::JobDirective::ExistingCleanup=> {
            if job.operation!="unpin" || !ledger::cleanup_allowed(db,&job.provider,&job.cid).await? {
                return Ok(Some(JobPermit::LedgerOnly(authority)));
            }
            JobPermit::ExistingCleanup(authority)
        },
    };
    Ok(Some(permit))
}

/// Call on result publication too. Management readonly/expired/stale work may
/// append facts with the existing protocol, but may not update a lease or turn
/// a readonly reconcile into a Submit/Unpin.
pub async fn projection_allowed<C:ConnectionTrait>(db:&C,job:&pin_job::Model)->AppResult<bool> {
    Ok(match check_before_call(db,job).await? {
        None=>true,
        Some(JobPermit::Submit(_)|JobPermit::ExistingCleanup(_))=>true,
        Some(JobPermit::ReadOnly(_)|JobPermit::LedgerOnly(_))=>false,
    })
}

pub async fn cleanup_settlement_allowed<C:ConnectionTrait>(db:&C,job:&pin_job::Model)->AppResult<bool> {
    Ok(matches!(check_before_call(db,job).await?,Some(JobPermit::ExistingCleanup(_)))&&job.operation=="unpin")
}

pub async fn lookup<C:ConnectionTrait>(db:&C,job:&pin_job::Model)->Result<Option<records::JobAuthority>> {
    let mut found=None;
    let actions=records::actions_for_job(db,job).await?;
    for (binding,action) in &actions {
        for authority in &action.jobs {
            if authority.job_id!=job.id {continue;}
            validate_stored(binding,action,authority)?;
            validate_job_resolved(db,job,authority).await?;
            let invocation=pin_invocation_route::Entity::find_by_id(job.id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Confirmation)?;
            if invocation.remote_epoch<authority.remote_epoch||serde_json::from_str::<ProviderRouteSnapshot>(&invocation.route).ok().as_ref()!=Some(&authority.route) {return Err(ManagementError::Confirmation);}
            meet(&mut found,authority.clone())?;
        }
    }
    // Poll successors must carry the exact original target/source/generation
    // and invocation. There is no global alias or job-ID-prefix admission.
    if job.operation=="poll" {
        for (binding,action) in &actions {
            for original in &action.jobs {
                if original.job_id==job.id || original.directive!=records::JobDirective::Submit
                    || original.lease_id!=job.lease_id || original.lease_generation!=job.expected_generation || original.target_id!=job.target_id
                    || original.cid!=job.cid
                {continue;}
                validate_stored(binding,action,original)?;
                let invocation=pin_invocation_route::Entity::find_by_id(job.id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Confirmation)?;
                if invocation.remote_epoch<original.remote_epoch || serde_json::from_str::<ProviderRouteSnapshot>(&invocation.route).ok().as_ref()!=Some(&original.route) {return Err(ManagementError::Confirmation);}
                if !successor::original_submit_for_poll(db,job,original,&invocation).await?{continue;}
                let mut successor=original.clone();successor.job_id=job.id.clone();
                // Stable seed equality is preserved for C's execution scope.
                // Invocation/reference progression is a derived live permit,
                // not a rewrite of the immutable applied authority.
                validate_job_resolved(db,job,&successor).await?;
                meet(&mut found,successor)?;
            }
        }
    }
    // An orphaned management lease/job is fail-closed, not an ordinary source.
    if found.is_none() && let Some(lease_id)=&job.lease_id {
        let lease=pin_lease::Entity::find_by_id(lease_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?;
        if lease.is_some_and(|lease|lease.source.starts_with("management:")) {return Err(ManagementError::Confirmation);}
    }
    // A name may deny an orphan; it never grants authority. Remote-only readonly
    // jobs have no lease ID and must not fall through to ordinary reconciliation.
    if found.is_none()&&job.id.starts_with("management-"){return Err(ManagementError::Confirmation);}
    Ok(found)
}

pub(super) async fn birth(tx:&DatabaseTransaction,job:&pin_job::Model,entry:&PlanEntry,directive:records::JobDirective,before:&native::NativeFrame,intents:&[String])->Result<records::JobAuthority> {
    let capture=match &entry.capture {Some(Capture::Object(capture))=>capture,_=>return Err(ManagementError::Confirmation)};
    let invocation=pin_invocation_route::Entity::find_by_id(job.id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
    let route=serde_json::from_str(&invocation.route).map_err(|_|ManagementError::InvalidEvidence)?;
    let remote=remote_pin::Entity::find_by_id((job.provider.clone(),job.cid.clone())).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
    if directive==records::JobDirective::ExistingCleanup {
        return Ok(records::JobAuthority {job_id:job.id.clone(),directive,provider:job.provider.clone(),cid:job.cid.clone(),remote_epoch:invocation.remote_epoch,route,resource_key:native::NativeKey {table:"remote_pins".into(),values:vec![job.provider.clone(),job.cid.clone()]},resource_id:remote.request_id,lease_id:None,lease_generation:None,target_id:None,owner:None,source:None,lease_source:None,started_at:None,expires_at:None,policy_id:None,provider_mode:None,content_mode:None,lease_contract:None});
    }
    if before.row("pin_jobs",&[&job.id]).is_some()&&let Some(authority)=lookup(tx,job).await? {
        if authority.directive!=directive{return Err(ManagementError::Confirmation);}return Ok(authority);
    }
    let lease_id=job.lease_id.as_ref().or_else(||(directive==records::JobDirective::ReadOnlyObserve&&intents.len()==1).then(||&intents[0])).or_else(||capture.target.as_ref().map(|target|&target.lease.id)).ok_or(ManagementError::Confirmation)?;
    let lease=pin_lease::Entity::find_by_id(lease_id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    let (owner,source)=if lease.owner_object_id==capture.owner_identity.object_id {(capture.owner_identity.clone(),capture.object.identity.clone())}
        else {
            if before.row("pin_jobs",&[&job.id]).is_none(){return Err(ManagementError::Confirmation);}
            let versions=object_version::Entity::find().filter(object_version::Column::ObjectId.eq(&lease.owner_object_id)).all(tx).await.map_err(|_|ManagementError::Database)?;
            if versions.len()!=1{return Err(ManagementError::InvalidEvidence);}
            let version=&versions[0];let object=crate::store::entities::object::Entity::find_by_id(lease.owner_object_id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
            let cid=crate::pinning::provider::canonical_resource_cid(&object.cid).map_err(|_|ManagementError::InvalidEvidence)?;
            if crate::pinning::provider::canonical_resource_cid(&job.cid).ok().as_ref()!=Some(&cid){return Err(ManagementError::InvalidEvidence);}
            let owner=crate::pinning::management::ExactObject {bucket:version.bucket.clone(),key:version.key.clone(),public_version:version.version_id.clone().map_or(crate::pinning::management::PublicVersionId::Null,crate::pinning::management::PublicVersionId::Version),private_version_id:version.id.clone(),object_id:lease.owner_object_id.clone(),cid};
            (owner.clone(),owner)
        };
    let contract=crate::store::entities::pin_lease_requirement::Entity::find_by_id(lease.id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.map(|row|row.contract);
    Ok(records::JobAuthority {job_id:job.id.clone(),directive,provider:job.provider.clone(),cid:job.cid.clone(),remote_epoch:invocation.remote_epoch,route,
        resource_key:native::NativeKey {table:"remote_pins".into(),values:vec![job.provider.clone(),job.cid.clone()]},resource_id:remote.request_id,
        lease_id:Some(lease.id),lease_generation:Some(lease.generation),target_id:job.target_id.clone(),owner:Some(owner),source:Some(source),lease_source:Some(lease.source),started_at:Some(lease.created_at),expires_at:Some(lease.expires_at),policy_id:Some(lease.policy_id),provider_mode:Some(lease.provider_mode),content_mode:Some(lease.content_mode),lease_contract:contract})
}

pub(super) async fn enqueue_observation(tx:&DatabaseTransaction,id:&str,provider:&str,cid:&str,epoch:i64,route:&ProviderRouteSnapshot,now:DateTime<Utc>)->Result<()> {
    pin_job::Entity::insert(pin_job::ActiveModel {id:Set(id.into()),operation:Set("reconcile".into()),provider:Set(provider.into()),cid:Set(cid.into()),lease_id:Set(None),target_id:Set(None),expected_generation:Set(None),expected_remote_epoch:Set(Some(epoch)),state:Set("pending".into()),attempts:Set(0),next_attempt_at:Set(now),locked_until:Set(None),submit_phase:Set(None),last_error:Set(None),created_at:Set(now),updated_at:Set(now)}).exec(tx).await.map_err(|_|ManagementError::Database)?;
    pin_invocation_route::Entity::insert(pin_invocation_route::ActiveModel {job_id:Set(id.into()),route:Set(serde_json::to_string(route).map_err(|_|ManagementError::InvalidEvidence)?),remote_epoch:Set(epoch)}).exec(tx).await.map_err(|_|ManagementError::Database)?;
    Ok(())
}

pub(crate) async fn validate_birth(tx:&DatabaseTransaction,binding:&records::ConfirmedBinding,action:&records::CommittedAction)->Result<()> {
    for authority in &action.jobs {
        validate_stored(binding,action,authority)?;
        let job=pin_job::Entity::find_by_id(authority.job_id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
        validate_job_resolved(tx,&job,authority).await?;
        let invocation=pin_invocation_route::Entity::find_by_id(authority.job_id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
        if invocation.remote_epoch<authority.remote_epoch || serde_json::from_str::<ProviderRouteSnapshot>(&invocation.route).ok().as_ref()!=Some(&authority.route) {return Err(ManagementError::InvalidEvidence);}
        if invocation.remote_epoch!=authority.remote_epoch&&!successor::current_resource(tx,&job,authority).await? {return Err(ManagementError::Confirmation);}
        if let Some(id)=&authority.lease_id {
            let lease=pin_lease::Entity::find_by_id(id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
            if authority.lease_generation!=Some(lease.generation)||authority.started_at!=Some(lease.created_at)||authority.expires_at!=Some(lease.expires_at)||authority.lease_source.as_deref()!=Some(&lease.source)||authority.owner.as_ref().is_none_or(|owner|owner.object_id!=lease.owner_object_id){return Err(ManagementError::Confirmation);}
            let contract=crate::store::entities::pin_lease_requirement::Entity::find_by_id(id.clone()).one(tx).await.map_err(|_|ManagementError::Database)?.map(|row|row.contract);
            if authority.policy_id.as_deref()!=Some(&lease.policy_id)||authority.provider_mode.as_deref()!=Some(&lease.provider_mode)||authority.content_mode.as_deref()!=Some(&lease.content_mode)||authority.lease_contract!=contract {return Err(ManagementError::Confirmation);}
        }
    }
    Ok(())
}

fn validate_job(job:&pin_job::Model,authority:&records::JobAuthority)->Result<()> {
    let operation=match authority.directive {records::JobDirective::Submit=>matches!(job.operation.as_str(),"submit"|"poll"),records::JobDirective::ReadOnlyObserve=>job.operation=="reconcile",records::JobDirective::ExistingCleanup=>job.operation=="unpin"};
    if !operation||job.provider!=authority.provider||job.cid!=authority.cid
        || job.expected_remote_epoch.is_some_and(|epoch|epoch!=authority.remote_epoch)
        || job.lease_id.as_ref().is_some_and(|id|Some(id)!=authority.lease_id.as_ref())
        || job.expected_generation.is_some_and(|generation|Some(generation)!=authority.lease_generation)
        || job.target_id!=authority.target_id
    {return Err(ManagementError::Confirmation);}
    Ok(())
}

pub(crate) fn validate_stored(binding:&records::ConfirmedBinding,action:&records::CommittedAction,authority:&records::JobAuthority)->Result<()> {
    if !binding.confirmation.bindings.contains(&action.receipt.binding)||!action.receipt.job_ids.contains(&authority.job_id) {return Err(ManagementError::Confirmation);}
    if !matches!(authority.resource_key.table.as_str(),"remote_pins"|"pin_resource_history")||authority.resource_key.values.first()!=Some(&authority.provider)||authority.resource_key.values.get(1)!=Some(&authority.cid)
        ||(authority.resource_key.table=="pin_resource_history"&&(authority.directive!=records::JobDirective::ReadOnlyObserve||authority.resource_key.values.get(2).and_then(|epoch|epoch.parse::<i64>().ok())!=Some(authority.remote_epoch))) {return Err(ManagementError::Confirmation);}
    match &action.receipt.binding.capture {
        Some(Capture::Object(capture))=> {
            let cancellation=matches!(action.receipt.binding.request.action,crate::pinning::management::Action::Cancel);
            let full_cancel_scope=cancellation&&cancel_resource_in_frame(binding,capture,authority)?;
            if authority.directive==records::JobDirective::ExistingCleanup {
                if !full_cancel_scope||authority.lease_id.is_some(){return Err(ManagementError::Confirmation);}
                return validate_resource_proof(binding,authority);
            }
            let shared=authority.owner.as_ref()!=Some(&capture.owner_identity)||authority.lease_id.as_ref().is_none_or(|id|!action.receipt.intent_ids.contains(id));
            if shared {validate_shared_source(binding,action,authority)?;return validate_resource_proof(binding,authority);}
            if authority.owner.as_ref()!=Some(&capture.owner_identity)||authority.source.as_ref()!=Some(&capture.object.identity)
                || authority.lease_id.as_ref().is_none_or(|id|!action.receipt.intent_ids.contains(id))
                || !full_cancel_scope&&!capture.requirement_routes.iter().chain(&capture.historical_routes).chain([&capture.route]).chain(capture.destination_route.iter()).chain(capture.remote_route.iter()).chain(capture.destination_remote_route.iter())
                    .any(|route|route.original.as_ref()==Some(&authority.route))
                || !full_cancel_scope&&crate::pinning::provider::canonical_resource_cid(&authority.cid).ok().as_deref()!=Some(&capture.object.identity.cid)
            {return Err(ManagementError::Confirmation);}
            if authority.directive==records::JobDirective::ReadOnlyObserve {validate_resource_proof(binding,authority)?;}
        },
        Some(Capture::Provider(capture))=> {
            if authority.directive!=records::JobDirective::ExistingCleanup
                || !capture.remotes.iter().any(|remote|remote.epoch==authority.remote_epoch&&crate::pinning::provider::canonical_resource_cid(&authority.cid).ok().as_deref()==Some(&remote.key.cid)&&remote.route.original.as_ref()==Some(&authority.route)&&remote.resource_id==authority.resource_id)
            {return Err(ManagementError::Confirmation);}
        },
        None=>return Err(ManagementError::Confirmation),
    }
    Ok(())
}

async fn current_lease<C:ConnectionTrait>(db:&C,authority:&records::JobAuthority,now:DateTime<Utc>)->Result<bool> {
    let Some(id)=&authority.lease_id else{return Ok(authority.directive==records::JobDirective::ExistingCleanup)};
    let lease=pin_lease::Entity::find_by_id(id.clone()).one(db).await.map_err(|_|ManagementError::Database)?;
    let Some(lease)=lease else{return Ok(false)};
    let contract=crate::store::entities::pin_lease_requirement::Entity::find_by_id(id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.map(|row|row.contract);
    if authority.policy_id.as_deref()!=Some(&lease.policy_id)||authority.provider_mode.as_deref()!=Some(&lease.provider_mode)||authority.content_mode.as_deref()!=Some(&lease.content_mode)||authority.lease_contract!=contract {return Ok(false);}
    let Some(owner)=&authority.owner else{return Ok(false)};
    if !identity_present(db,owner).await?||authority.source.as_ref().is_none_or(|source|source.cid!=crate::pinning::provider::canonical_resource_cid(&authority.cid).unwrap_or_default()){return Ok(false);}
    if let Some(source)=&authority.source&& !identity_present(db,source).await? {return Ok(false);}
    let exact=object_version::Entity::find_by_id(owner.private_version_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?;
    let Some(exact)=exact else{return Ok(false)};
    Ok(exact.object_id.as_deref()==Some(owner.object_id.as_str()) && exact.bucket==owner.bucket && exact.key==owner.key
        && exact.version_id.as_deref()==match &owner.public_version {crate::pinning::management::PublicVersionId::Null=>None,crate::pinning::management::PublicVersionId::Version(version)=>Some(version.as_str())}
        && lease.owner_object_id==owner.object_id && authority.lease_source.as_deref()==Some(&lease.source)
        && authority.lease_generation==Some(lease.generation) && authority.started_at==Some(lease.created_at)
        && authority.expires_at==Some(lease.expires_at) && lease.state=="active" && now<lease.expires_at)
}

async fn identity_present<C:ConnectionTrait>(db:&C,identity:&crate::pinning::management::ExactObject)->Result<bool> {
    let version=object_version::Entity::find_by_id(identity.private_version_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?;
    let object=crate::store::entities::object::Entity::find_by_id(identity.object_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?;
    Ok(version.is_some_and(|version|version.object_id.as_deref()==Some(identity.object_id.as_str())&&version.bucket==identity.bucket&&version.key==identity.key&&version.version_id.as_deref()==match &identity.public_version {crate::pinning::management::PublicVersionId::Null=>None,crate::pinning::management::PublicVersionId::Version(version)=>Some(version.as_str())})&&object.is_some_and(|object|object.bucket==identity.bucket&&object.key==identity.key&&crate::pinning::provider::canonical_resource_cid(&object.cid).ok().as_ref()==Some(&identity.cid)))
}

fn safe(_:ManagementError)->AppError {AppError::InvalidPinningRequest("management job authority is missing, stale, or invalid".into())}

fn meet(found:&mut Option<records::JobAuthority>,candidate:records::JobAuthority)->Result<()> {
    if found.as_ref().is_some_and(|original|*original!=candidate){return Err(ManagementError::InvalidEvidence);}
    *found=Some(candidate);Ok(())
}
async fn validate_job_resolved<C:ConnectionTrait>(db:&C,job:&pin_job::Model,authority:&records::JobAuthority)->Result<()> {
    let mut operational=authority.clone();
    if job.provider!=authority.provider {
        let key=[("id".into(),job.id.clone())].into();
        let binding=crate::store::pinning::legacy_migration::resolve_legacy_binding(db,"pin_jobs",&key,&authority.provider).await.map_err(database)?.ok_or(ManagementError::Confirmation)?;
        if binding.operational_key!=job.provider||binding.historical_route.as_ref()!=Some(&authority.route){return Err(ManagementError::Confirmation);}
        operational.provider=job.provider.clone();
    }
    validate_job(job,&operational)
}
async fn operational_resource<C:ConnectionTrait>(db:&C,authority:&records::JobAuthority)->Result<String> {
    let mut key=std::collections::BTreeMap::from([("provider".into(),authority.provider.clone()),("cid".into(),authority.cid.clone())]);
    if authority.resource_key.table=="pin_resource_history"{key.insert("epoch".into(),authority.remote_epoch.to_string());}
    if let Some(binding)=crate::store::pinning::legacy_migration::resolve_legacy_binding(db,&authority.resource_key.table,&key,&authority.provider).await.map_err(database)? {
        if binding.historical_route.as_ref()!=Some(&authority.route){return Err(ManagementError::Confirmation);}return Ok(binding.operational_key);
    }
    Ok(authority.provider.clone())
}
fn validate_shared_source(binding:&records::ConfirmedBinding,action:&records::CommittedAction,authority:&records::JobAuthority)->Result<()> {
    if authority.directive!=records::JobDirective::Submit||!matches!(action.receipt.binding.request.action,crate::pinning::management::Action::Create(_)|crate::pinning::management::Action::Backfill(_)|crate::pinning::management::Action::Reapply(_)){return Err(ManagementError::Confirmation);}
    let before=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    let Some(Capture::Object(capture))=&action.receipt.binding.capture else{return Err(ManagementError::Confirmation)};
    if crate::pinning::provider::canonical_resource_cid(&authority.cid).ok().as_deref()!=Some(&capture.object.identity.cid){return Err(ManagementError::Confirmation);}
    let job=before.row("pin_jobs",&[&authority.job_id]).ok_or(ManagementError::Confirmation)?;
    let lease_id=authority.lease_id.as_ref().ok_or(ManagementError::Confirmation)?;let owner=authority.owner.as_ref().ok_or(ManagementError::Confirmation)?;
    let lease=before.row("pin_leases",&[lease_id]).ok_or(ManagementError::Confirmation)?;
    let version=before.row("object_versions",&[&owner.private_version_id]).ok_or(ManagementError::Confirmation)?;
    let object=before.row("objects",&[&owner.object_id]).ok_or(ManagementError::Confirmation)?;
    let invocation=before.row("pin_invocation_routes",&[&authority.job_id]).ok_or(ManagementError::Confirmation)?;
    if native::value(job,"provider")!=Some(&authority.provider)||native::value(job,"cid")!=Some(&authority.cid)||native::value(job,"lease_id")!=Some(lease_id)
        ||native::value(job,"expected_generation").and_then(|value|value.parse().ok())!=authority.lease_generation
        ||native::value(job,"target_id")!=authority.target_id.as_deref()
        ||native::value(lease,"owner_object_id")!=Some(&owner.object_id)||native::value(lease,"source")!=authority.lease_source.as_deref()
        ||native::value(lease,"created_at").and_then(|time|native::parse_time(time).ok())!=authority.started_at
        ||native::value(lease,"expires_at").and_then(|time|native::parse_time(time).ok())!=authority.expires_at
        ||native::value(lease,"policy_id")!=authority.policy_id.as_deref()||native::value(lease,"provider_mode")!=authority.provider_mode.as_deref()||native::value(lease,"content_mode")!=authority.content_mode.as_deref()
        ||before.row("pin_lease_requirements",&[lease_id]).and_then(|row|native::value(row,"contract"))!=authority.lease_contract.as_deref()
        ||native::value(version,"object_id")!=Some(&owner.object_id)||native::value(version,"bucket")!=Some(&owner.bucket)||native::value(version,"key")!=Some(&owner.key)
        ||native::value(version,"version_id")!=match &owner.public_version {crate::pinning::management::PublicVersionId::Null=>None,crate::pinning::management::PublicVersionId::Version(version)=>Some(version.as_str())}
        ||native::value(object,"cid").and_then(|cid|crate::pinning::provider::canonical_resource_cid(cid).ok()).as_ref()!=Some(&owner.cid)||!shared_source_in_frame(before,capture,authority)? {return Err(ManagementError::Confirmation);}
    if native::value(invocation,"remote_epoch").and_then(|value|value.parse().ok())!=Some(authority.remote_epoch)||native::value(invocation,"route").and_then(|encoded|serde_json::from_str::<ProviderRouteSnapshot>(encoded).ok()).as_ref()!=Some(&authority.route){return Err(ManagementError::Confirmation);}
    Ok(())
}
fn shared_source_in_frame(before:&native::NativeFrame,capture:&crate::pinning::management::ObjectCapture,authority:&records::JobAuthority)->Result<bool> {
    let owner=authority.owner.as_ref().ok_or(ManagementError::Confirmation)?;
    let source=authority.source.as_ref().ok_or(ManagementError::Confirmation)?;
    if source==owner{return Ok(true);}
    if owner!=&capture.owner_identity||source!=&capture.object.identity{return Ok(false);}
    let version=before.row("object_versions",&[&source.private_version_id]).ok_or(ManagementError::Confirmation)?;
    let object=before.row("objects",&[&source.object_id]).ok_or(ManagementError::Confirmation)?;
    let target=before.row("pin_lease_targets",&[authority.target_id.as_deref().ok_or(ManagementError::Confirmation)?]).ok_or(ManagementError::Confirmation)?;
    Ok(native::value(version,"object_id")==Some(&source.object_id)&&native::value(version,"bucket")==Some(&source.bucket)&&native::value(version,"key")==Some(&source.key)
        &&native::value(version,"version_id")==match &source.public_version {crate::pinning::management::PublicVersionId::Null=>None,crate::pinning::management::PublicVersionId::Version(version)=>Some(version.as_str())}
        &&native::value(object,"cid").and_then(|cid|crate::pinning::provider::canonical_resource_cid(cid).ok()).as_ref()==Some(&source.cid)
        &&native::value(target,"lease_id")==authority.lease_id.as_deref()&&native::value(target,"provider")==Some(&authority.provider)&&native::value(target,"cid")==Some(&authority.cid))
}
fn validate_resource_proof(binding:&records::ConfirmedBinding,authority:&records::JobAuthority)->Result<()> {
    let frame=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    if authority.resource_key.table=="remote_pins" {
        let remote=frame.row("remote_pins",&[&authority.provider,&authority.cid]).ok_or(ManagementError::Confirmation)?;
        let ledger=frame.row("remote_pin_ledger",&[&authority.provider,&authority.cid]).ok_or(ManagementError::Confirmation)?;
        let original_epoch=native::value(remote,"epoch").and_then(|value|value.parse::<i64>().ok()).ok_or(ManagementError::Confirmation)?;
        let mut closures=0usize;
        for entry in &binding.confirmation.bindings {if matches!(entry.request.action,crate::pinning::management::Action::Cancel)&&let Some(Capture::Object(capture))=&entry.capture&&cancel_resource_in_frame(binding,capture,authority)? {closures+=1;}}
        let cancellation=closures>0&&matches!(authority.directive,records::JobDirective::ReadOnlyObserve|records::JobDirective::ExistingCleanup);
        let creations=binding.confirmation.bindings.iter().filter(|entry|matches!(entry.request.action,crate::pinning::management::Action::Create(_)|crate::pinning::management::Action::Backfill(_)|crate::pinning::management::Action::Reapply(_))&&matches!(&entry.capture,Some(Capture::Object(capture)) if crate::pinning::provider::canonical_resource_cid(&authority.cid).ok().as_deref()==Some(&capture.object.identity.cid)&&capture.requirement_routes.iter().chain(capture.remote_route.iter()).any(|route|route.original.as_ref()==Some(&authority.route)))).count();
        let observation=authority.directive==records::JobDirective::ReadOnlyObserve&&creations>0;
        let maximum_delta=if observation{closures+creations}else{closures};
        let epoch_matches=authority.remote_epoch==original_epoch||(cancellation||observation)&&authority.remote_epoch>=original_epoch&&original_epoch.checked_add(maximum_delta as i64).is_some_and(|maximum|authority.remote_epoch<=maximum);
        if !epoch_matches||native::value(remote,"request_id")!=authority.resource_id.as_deref()
            ||native::value(ledger,"route").and_then(|encoded|serde_json::from_str::<ProviderRouteSnapshot>(encoded).ok()).as_ref()!=Some(&authority.route) {return Err(ManagementError::Confirmation);}
    }else{
        let epoch=authority.remote_epoch.to_string();let row=frame.original_row("pin_resource_history",&[&authority.provider,&authority.cid,&epoch]).ok_or(ManagementError::Confirmation)?;
        let envelope:HistoryEnvelope=serde_json::from_str(native::value(&row,"ledger").ok_or(ManagementError::Confirmation)?).map_err(|_|ManagementError::InvalidEvidence)?;
        if envelope.request_id!=authority.resource_id||serde_json::from_str::<ProviderRouteSnapshot>(&envelope.ledger.route).ok().as_ref()!=Some(&authority.route){return Err(ManagementError::Confirmation);}
    }
    Ok(())
}

fn cancel_resource_in_frame(binding:&records::ConfirmedBinding,capture:&crate::pinning::management::ObjectCapture,authority:&records::JobAuthority)->Result<bool> {
    let Some(target)=&capture.target else{return Ok(false)};
    let frame=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    let lease=frame.row("pin_leases",&[&target.lease.id]).ok_or(ManagementError::Confirmation)?;
    if native::value(lease,"owner_object_id")!=Some(&capture.owner_identity.object_id){return Err(ManagementError::Confirmation);}
    let attached=frame.table("pin_lease_targets").ok_or(ManagementError::Schema)?.rows.iter().any(|row|native::value(row,"lease_id")==Some(&target.lease.id)&&native::value(row,"provider")==Some(&authority.provider)&&native::value(row,"cid")==Some(&authority.cid));
    let route=frame.row("remote_pin_ledger",&[&authority.provider,&authority.cid]).and_then(|row|native::value(row,"route")).and_then(|encoded|serde_json::from_str::<ProviderRouteSnapshot>(encoded).ok());
    Ok(attached&&route.as_ref()==Some(&authority.route))
}
#[derive(serde::Deserialize)]
struct HistoryEnvelope {request_id:Option<String>,ledger:HistoryRoute}
#[derive(serde::Deserialize)]
struct HistoryRoute {route:String}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn reference_epoch_and_poll_successor_preserve_source_but_release_ends_permission() {
        use sea_orm::{ActiveModelTrait,IntoActiveModel,TransactionTrait};
        let fixture=super::super::tests::fixture("epoch-poll").await;
        let applied=super::super::apply(&fixture.db,&fixture.config,&fixture.auth,&fixture.binding).await.unwrap();
        let original=applied.items[0].jobs.iter().find(|job|job.directive==records::JobDirective::Submit).unwrap().clone();
        let tx=fixture.db.begin().await.unwrap();
        let remote=remote_pin::Entity::find_by_id((original.provider.clone(),original.cid.clone())).one(&tx).await.unwrap().unwrap();
        let mut remote=remote.into_active_model();remote.epoch=Set(original.remote_epoch+1);remote.update(&tx).await.unwrap();
        let now=crate::store::database_clock::database_now(&tx).await.unwrap();
        let submit=pin_job::Entity::find_by_id(original.job_id.clone()).one(&tx).await.unwrap().unwrap();
        let mut active=submit.clone().into_active_model();active.state=Set("running".into());active.locked_until=Set(Some(now+chrono::Duration::minutes(1)));active.submit_phase=Set(Some("ready".into()));let model=active.update(&tx).await.unwrap();
        let claimed=crate::store::pinning::jobs::ClaimedPinJob {model,reclaimed:false,previous_state:"pending".into(),object_id:original.owner.as_ref().map(|owner|owner.object_id.clone())};
        assert!(matches!(check_before_call(&tx,&claimed.model).await.unwrap(),Some(JobPermit::Submit(_))));
        let mut recovering=claimed.model.clone().into_active_model();recovering.submit_phase=Set(Some("recovering".into()));recovering.update(&tx).await.unwrap();
        assert!(matches!(check_before_call(&tx,&claimed.model).await.unwrap(),Some(JobPermit::LedgerOnly(_))),"a missing history on recovery is not a zero-POST proof");
        let mut ready=claimed.model.clone().into_active_model();ready.submit_phase=Set(Some("ready".into()));ready.update(&tx).await.unwrap();
        assert!(matches!(crate::store::pinning::jobs::prepare_submit_call(&tx,&claimed,now).await.unwrap(),crate::store::pinning::jobs::SubmitCallDecision::ReadyToCall));
        crate::store::pinning::jobs::record_submit_invocation(&tx,&claimed,&original.route.api_profile,&original.route.strategy,now).await.unwrap();
        assert!(lookup(&tx,&claimed.model).await.unwrap().as_ref()==Some(&original));
        assert!(matches!(check_before_call(&tx,&claimed.model).await.unwrap(),Some(JobPermit::Submit(_))));
        let history=crate::store::pinning::jobs::submission_history(&tx,&original.job_id).await.unwrap().unwrap();let mut blocked=history.clone().into_active_model();blocked.state=Set("needs_attention".into());blocked.update(&tx).await.unwrap();
        assert!(matches!(check_before_call(&tx,&claimed.model).await.unwrap(),Some(JobPermit::LedgerOnly(_))));
        let mut active=history.into_active_model();active.state=Set("active".into());active.update(&tx).await.unwrap();
        let remote=remote_pin::Entity::find_by_id((original.provider.clone(),original.cid.clone())).one(&tx).await.unwrap().unwrap();let mut active=remote.into_active_model();active.request_id=Set(Some("exact-poll-successor".into()));active.status=Set("queued".into());active.update(&tx).await.unwrap();
        let poll=crate::store::pinning::jobs::poll_job(&original.provider,&original.cid,original.lease_id.as_deref().unwrap(),original.target_id.as_deref().unwrap(),original.lease_generation.unwrap(),"exact-poll-successor",now);
        let crate::store::pinning::jobs::NewPinJob::Target(target)=&poll else{unreachable!()};
        let id=target.id.clone();crate::store::pinning::jobs::insert_new_job(&tx,poll,now).await.unwrap();
        let job=pin_job::Entity::find_by_id(id).one(&tx).await.unwrap().unwrap();
        let successor=lookup(&tx,&job).await.unwrap().unwrap();
        assert!(successor.owner==original.owner&&successor.source==original.source&&successor.lease_source==original.lease_source&&successor.lease_contract==original.lease_contract);
        assert_eq!(successor.remote_epoch,original.remote_epoch);
        let mut stable_seed=original.clone();stable_seed.job_id=job.id.clone();assert!(stable_seed==successor,"C's strict seed membership must not be widened");
        assert!(matches!(check_before_call(&tx,&job).await.unwrap(),Some(JobPermit::Submit(_))));
        let submit=pin_job::Entity::find_by_id(original.job_id.clone()).one(&tx).await.unwrap().unwrap();
        // This is an old Submit observation, not a new POST to the now-queued resource.
        assert!(matches!(check_before_call(&tx,&submit).await.unwrap(),Some(JobPermit::LedgerOnly(_))));
        crate::store::entities::pin_resource_history::Entity::insert(crate::store::entities::pin_resource_history::ActiveModel {provider:Set(original.provider.clone()),cid:Set(original.cid.clone()),epoch:Set(original.remote_epoch),ledger:Set(serde_json::json!({"confirmed_absent":true,"request_id":original.resource_id,"ledger":{"route":serde_json::to_string(&original.route).unwrap()}}).to_string())}).exec(&tx).await.unwrap();
        assert!(matches!(check_before_call(&tx,&submit).await.unwrap(),Some(JobPermit::LedgerOnly(_))));
        assert!(lookup(&tx,&job).await.unwrap().as_ref()==Some(&stable_seed));
        assert!(matches!(check_before_call(&tx,&job).await.unwrap(),Some(JobPermit::LedgerOnly(_))));
        tx.rollback().await.unwrap();
    }
    #[tokio::test]
    async fn compatible_shared_authority_meets_and_conflicting_generation_epoch_route_fail_closed() {
        let fixture=super::super::tests::fixture("compatible-meet").await;
        let applied=super::super::apply(&fixture.db,&fixture.config,&fixture.auth,&fixture.binding).await.unwrap();
        let authority=applied.items[0].jobs[0].clone();let mut found=None;
        meet(&mut found,authority.clone()).unwrap();meet(&mut found,authority.clone()).unwrap();assert!(found.as_ref()==Some(&authority));
        for changed in [0,1,2,3] {
            let mut conflicting=authority.clone();
            match changed {0=>conflicting.lease_generation=conflicting.lease_generation.map(|generation|generation+1),1=>conflicting.remote_epoch+=1,2=>conflicting.route.scope="different-account".into(),_=>conflicting.lease_source=Some("different-original-source".into())}
            assert!(meet(&mut found,conflicting).is_err());assert!(found.as_ref()==Some(&authority));
        }
    }

    #[tokio::test]
    async fn shared_native_submit_indexes_only_its_request_bound_poll_and_preserves_all_seed_fields() {
        use sea_orm::{ActiveModelTrait,IntoActiveModel,TransactionTrait};
        let fixture=super::super::tests::fixture_with_automatic("shared-poll-index",true).await;
        let applied=super::super::apply(&fixture.db,&fixture.config,&fixture.auth,&fixture.binding).await.unwrap();
        let action=&applied.items[0];
        let seed=action.jobs.iter().find(|authority|authority.directive==records::JobDirective::Submit&&authority.lease_id.as_ref().is_some_and(|lease|!action.receipt.intent_ids.contains(lease))).unwrap().clone();
        let tx=fixture.db.begin().await.unwrap();let now=crate::store::database_clock::database_now(&tx).await.unwrap();
        let source=pin_job::Entity::find_by_id(seed.job_id.clone()).one(&tx).await.unwrap().unwrap();
        let mut running=source.clone().into_active_model();running.state=Set("running".into());running.locked_until=Set(Some(now+chrono::Duration::minutes(1)));running.submit_phase=Set(Some("ready".into()));let source=running.update(&tx).await.unwrap();
        let claimed=crate::store::pinning::jobs::ClaimedPinJob {model:source.clone(),reclaimed:false,previous_state:"pending".into(),object_id:seed.owner.as_ref().map(|owner|owner.object_id.clone())};
        assert!(matches!(crate::store::pinning::jobs::prepare_submit_call(&tx,&claimed,now).await.unwrap(),crate::store::pinning::jobs::SubmitCallDecision::ReadyToCall));
        crate::store::pinning::jobs::record_submit_invocation(&tx,&claimed,&seed.route.api_profile,&seed.route.strategy,now).await.unwrap();
        // Native invocation is real; the queued request below is a fence fixture,
        // not a POST/PIN proof. C's controlled PSA worker regression covers IO.
        let remote=remote_pin::Entity::find_by_id((seed.provider.clone(),seed.cid.clone())).one(&tx).await.unwrap().unwrap();
        let mut queued=remote.into_active_model();queued.request_id=Set(Some("shared-original-request".into()));queued.status=Set("queued".into());queued.update(&tx).await.unwrap();
        let new_poll=crate::store::pinning::jobs::poll_job(&seed.provider,&seed.cid,seed.lease_id.as_deref().unwrap(),seed.target_id.as_deref().unwrap(),seed.lease_generation.unwrap(),"shared-original-request",now);
        let crate::store::pinning::jobs::NewPinJob::Target(target)=&new_poll else{unreachable!()};let poll_id=target.id.clone();
        crate::store::pinning::jobs::insert_new_job(&tx,new_poll,now).await.unwrap();
        let poll=pin_job::Entity::find_by_id(poll_id).one(&tx).await.unwrap().unwrap();
        assert!(!action.receipt.job_ids.contains(&poll.id));
        assert!(!action.receipt.intent_ids.contains(seed.lease_id.as_ref().unwrap()));
        let mut expected=seed.clone();expected.job_id=poll.id.clone();
        assert!(lookup(&tx,&poll).await.unwrap().as_ref()==Some(&expected));
        assert!(matches!(check_before_call(&tx,&poll).await.unwrap(),Some(JobPermit::Submit(authority)) if authority==expected));
        // An unrelated opaque record cannot become a candidate by sharing only
        // the physical database. It remains present for the full native fence.
        tx.execute_unprepared("INSERT INTO pin_management_records(database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) SELECT database_scope,'poll-unrelated','foreign','action','local_configuration',native_identity,'not-a-binding','not-before','not-a-result','[]','[]','applied',created_at,'2099-01-01T00:00:00Z',created_at FROM pin_management_records WHERE kind='database_scope'").await.unwrap();
        assert!(lookup(&tx,&poll).await.unwrap().as_ref()==Some(&expected));
        let mut wrong_generation=poll.clone();wrong_generation.expected_generation=Some(seed.lease_generation.unwrap()+1);
        assert!(matches!(lookup(&tx,&wrong_generation).await,Ok(None)|Err(ManagementError::Confirmation|ManagementError::InvalidEvidence)));
        let invocation=pin_invocation_route::Entity::find_by_id(poll.id.clone()).one(&tx).await.unwrap().unwrap();
        let mut wrong_route=seed.route.clone();wrong_route.scope="foreign-account".into();let mut changed=invocation.clone().into_active_model();changed.route=Set(serde_json::to_string(&wrong_route).unwrap());changed.update(&tx).await.unwrap();
        assert!(lookup(&tx,&poll).await.is_err());let mut restored=invocation.into_active_model();restored.route=Set(serde_json::to_string(&seed.route).unwrap());restored.update(&tx).await.unwrap();
        let remote=remote_pin::Entity::find_by_id((seed.provider.clone(),seed.cid.clone())).one(&tx).await.unwrap().unwrap();
        let mut changed=remote.clone().into_active_model();changed.request_id=Set(Some("replacement-request".into()));changed.update(&tx).await.unwrap();
        assert!(matches!(lookup(&tx,&poll).await,Ok(None)|Err(ManagementError::Confirmation|ManagementError::InvalidEvidence)));let mut restored=remote.into_active_model();restored.request_id=Set(Some("shared-original-request".into()));restored.update(&tx).await.unwrap();
        let lease=pin_lease::Entity::find_by_id(seed.lease_id.clone().unwrap()).one(&tx).await.unwrap().unwrap();
        let mut changed=lease.clone().into_active_model();changed.source=Set("changed-native-source".into());changed.update(&tx).await.unwrap();
        assert!(lookup(&tx,&poll).await.unwrap().as_ref()==Some(&expected),"historical seed is immutable, not a new source grant");
        assert!(matches!(check_before_call(&tx,&poll).await.unwrap(),Some(JobPermit::LedgerOnly(authority)) if authority==expected));let original_source=lease.source.clone();let mut restored=lease.into_active_model();restored.source=Set(original_source);restored.update(&tx).await.unwrap();
        let mut changed=source.clone().into_active_model();changed.lease_id=Set(Some(action.receipt.intent_ids[0].clone()));changed.update(&tx).await.unwrap();
        assert!(matches!(lookup(&tx,&poll).await,Ok(None)|Err(ManagementError::Confirmation|ManagementError::InvalidEvidence)));let original_lease=source.lease_id.clone();let mut restored=source.into_active_model();restored.lease_id=Set(original_lease);restored.update(&tx).await.unwrap();
        pin_job::Entity::delete_by_id(seed.job_id.clone()).exec(&tx).await.unwrap();
        assert!(matches!(lookup(&tx,&poll).await,Ok(None)|Err(ManagementError::Confirmation|ManagementError::InvalidEvidence)));
        tx.rollback().await.unwrap();
    }
}
