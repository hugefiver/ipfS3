use super::*;
use crate::{pinning::management::{Action,PlanEntry,RetirementCapture,RetirementSelection,RouteIdentity},store::{entities::{pin_lease_target,pin_provider_route,remote_pin},pinning::{jobs::{self,NewPinJob},ledger}}};
use sea_orm::{ColumnTrait,DatabaseTransaction,EntityTrait,QueryFilter,Statement};

pub(super) async fn execute(tx:&DatabaseTransaction,auth:&records::LocalAuthorization,binding:&records::ConfirmedBinding,entry:&PlanEntry,capture:&RetirementCapture,route:&RouteIdentity,result:&mut records::CommittedAction)->Result<()> {
    let frame=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    let registered=frame.table("pin_provider_routes").ok_or(ManagementError::Schema)?.rows.iter().filter(|row|native::value(row,"snapshot").and_then(|value|serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(value).ok()).as_ref()==route.original.as_ref()).collect::<Vec<_>>();
    if registered.len()!=1 {return Err(ManagementError::Confirmation);}
    let domain=native::value(registered[0],"provider").ok_or(ManagementError::InvalidEvidence)?;
    let selection=match &entry.request.action {Action::Retire=>&crate::pinning::management::RetirementSelection::Retain,Action::RetireWith {selection}=>selection,_=>return Err(ManagementError::Confirmation)};
    let disposition=match selection {
        RetirementSelection::Retain=>records::Disposition::Retain,
        RetirementSelection::Migrate {targets}=> {
            let active=capture.targets.iter().filter(|target|native::value(&target.native_tuple,"provider")==Some(domain)&&target.lease.state==crate::pinning::management::LeaseState::Active).collect::<Vec<_>>();
            if active.len()!=targets.len() {return Err(ManagementError::Confirmation);}
            for selected in targets {
                let key=native::NativeKey {table:super::super::schema::RECORDS.into(),values:vec![binding.database_scope.clone(),selected.plan_id.clone(),selected.item_id.clone(),"action".into()]};
                let (old,action)=records::action_at_key(tx,&key).await?.ok_or(ManagementError::Confirmation)?;
                if old.database_scope!=binding.database_scope||old.database_identity!=binding.database_identity||old.principal!=binding.principal {return Err(ManagementError::Unauthorized);}
                let point=action.migration_points.iter().find(|point|point.lease_id==selected.lease_id&&point.source_target_id==selected.source_target_id&&point.cid==selected.cid&&selected.destination.original.as_ref()==Some(&point.destination_route)).ok_or(ManagementError::Confirmation)?;
                let key=[("id".into(),point.source_target_id.clone())].into();
                let source=if let Some(legacy)=crate::store::pinning::legacy_migration::resolve_legacy_binding(tx,"pin_lease_targets",&key,&point.source_allocation).await.map_err(database)? {
                    if legacy.historical_route.as_ref()!=Some(&point.source_resource_route){return Err(ManagementError::Confirmation);}legacy.operational_key
                }else{point.source_allocation.clone()};
                if source!=domain || !active.iter().any(|target|target.id==point.source_target_id) {return Err(ManagementError::Confirmation);}
                let resolved=points::resolve_slot(tx,&point.lease_id,&point.cid,&point.slot).await?.ok_or(ManagementError::Confirmation)?;
                if resolved!=*point {return Err(ManagementError::Confirmation);}
                result.targets.push(point.destination_target_id.clone());
            }
            records::Disposition::Migrate
        },
        RetirementSelection::ControlledCleanup {resources}=> {
            let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
            for selected in resources {
                let source=frame.table("remote_pins").ok_or(ManagementError::Schema)?.rows.iter().filter(|row|native::value(row,"provider")==Some(domain)&&native::value(row,"cid").and_then(|cid|crate::pinning::provider::canonical_resource_cid(cid).ok()).as_deref()==Some(&selected.key.cid)).collect::<Vec<_>>();
                if source.len()!=1 {return Err(ManagementError::Confirmation);}
                let cid=native::value(source[0],"cid").ok_or(ManagementError::InvalidEvidence)?;
                let remote=remote_pin::Entity::find_by_id((domain.to_owned(),cid.to_owned())).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
                if remote.epoch!=selected.epoch||remote.request_id.as_deref()!=Some(&selected.resource_id)||!ledger::cleanup_allowed(tx,domain,cid).await.map_err(database)? {return Err(ManagementError::Changed);}
                let refs=pin_lease_target::Entity::find().filter(pin_lease_target::Column::Provider.eq(domain)).filter(pin_lease_target::Column::Cid.eq(cid)).filter(pin_lease_target::Column::State.is_in(["waiting","submitted","pinned","degraded"])).all(tx).await.map_err(|_|ManagementError::Database)?;
                if !refs.is_empty() {return Err(ManagementError::Changed);}
                let NewPinJob::Remote(mut job)=jobs::unpin_job(domain,cid,remote.epoch,now) else{unreachable!()};
                job.id=format!("management-cleanup:{}",uuid::Uuid::new_v4());let id=job.id.clone();
                jobs::insert_new_job(tx,NewPinJob::Remote(job),now).await.map_err(database)?;
                result.jobs.push(records::JobAuthority {job_id:id,directive:records::JobDirective::ExistingCleanup,provider:domain.into(),cid:cid.into(),remote_epoch:remote.epoch,route:selected.route.original.clone().ok_or(ManagementError::Confirmation)?,resource_key:native::NativeKey {table:"remote_pins".into(),values:vec![domain.into(),cid.into()]},resource_id:remote.request_id,lease_id:None,lease_generation:None,target_id:None,owner:None,source:None,lease_source:None,started_at:None,expires_at:None,policy_id:None,provider_mode:None,content_mode:None,lease_contract:None});
            }
            records::Disposition::ControlledCleanup
        },
    };
    // Stable domain barrier and actual route stop, not just an advisory record.
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_provider_routes SET retired=$1 WHERE provider=$2 AND snapshot=$3",[true.into(),domain.into(),native::value(registered[0],"snapshot").ok_or(ManagementError::InvalidEvidence)?.into()])).await.map_err(|_|ManagementError::Database)?;
    let record=records::write_retirement_in_transaction(tx,auth,binding,&entry.request.item_id,domain,disposition,vec![]).await?;
    result.state=if record.state==records::RetirementState::NeedsAttention {records::ActionState::NeedsAttention}else if result.jobs.is_empty() {records::ActionState::Accepted}else{records::ActionState::Queued};
    let current=pin_provider_route::Entity::find_by_id(domain.to_owned()).one(tx).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    if !current.retired {return Err(ManagementError::Changed);}
    Ok(())
}
