use super::*;
use crate::pinning::management::{Action, Capture, PlanEntry, PlannedStep, Subject};
use sea_orm::{DatabaseTransaction, EntityTrait};

pub(super) async fn execute(
    tx:&DatabaseTransaction,config:&ManagementConfig,auth:&records::LocalAuthorization,
    binding:&records::ConfirmedBinding,entry:&PlanEntry,before:&native::NativeFrame,
)->Result<records::CommittedAction> {
    let mut result=records::CommittedAction {receipt:records::ActionReceipt {binding:entry.clone(),intent_ids:vec![],job_ids:vec![]},state:records::ActionState::Accepted,targets:vec![],jobs:vec![],migration_points:vec![],decision:None};
    match (&entry.capture,&entry.step,&entry.request.subject) {
        (Some(Capture::Object(capture)),Some(PlannedStep::Object(step)),Subject::Object {..})=> {
            match &entry.request.action {
                Action::Create(intent)|Action::Reapply(intent)|Action::Backfill(intent)=>intents::create(tx,config,binding,capture,intent,before,&mut result).await?,
                Action::Renew {expires_at}=>intents::renew(tx,config,capture,*expires_at,&mut result).await?,
                Action::Cancel=>intents::cancel(tx,capture,&mut result).await?,
                Action::Retry|Action::Reconcile=>recovery::execute(tx,capture,step,&entry.request.action,before,&mut result).await?,
                Action::Migrate {destination}=>migration::execute(tx,config,capture,step,destination,before,&mut result).await?,
                _=>return Err(ManagementError::Confirmation),
            }
        },
        (Some(Capture::Provider(capture)),Some(PlannedStep::Retire(_)),Subject::Provider {route})=>{
            retirement::execute(tx,auth,binding,entry,capture,route,&mut result).await?;
        },
        _=>return Err(ManagementError::Confirmation),
    }
    let models=crate::store::entities::pin_lease_target::Entity::find().all(tx).await.map_err(|_|ManagementError::Database)?;
    // A shared/reactivated job retains its actual source, not this referencing
    // lease's owner. Every additional receipt carries the same compatible proof.
    let after=crate::store::entities::pin_job::Entity::find().all(tx).await.map_err(|_|ManagementError::Database)?;
    for job in after {
        if !matches!(job.state.as_str(),"pending"|"retry"|"running"|"blocked")||result.jobs.iter().any(|authority|authority.job_id==job.id) {continue;}
        // Renewal leaves old claims/history intact. An old generation is not
        // re-stamped with the renewed lease's generation by job birth.
        if matches!(job.operation.as_str(),"submit"|"poll")&&!crate::store::pinning::jobs::check_target_job_generation(tx,&job).await.map_err(database)? {continue;}
        let lease_matches=job.lease_id.as_ref().is_some_and(|id|result.receipt.intent_ids.contains(id));
        let target_matches=job.target_id.as_ref().is_some_and(|id|result.targets.contains(id));
        let shared_matches=models.iter().any(|target|result.targets.contains(&target.id)&&target.provider==job.provider&&target.cid==job.cid);
        let remote_matches=matches!(entry.request.action,Action::Cancel) && before.table("pin_lease_targets").is_some_and(|table|table.rows.iter().any(|target|native::value(target,"lease_id").is_some_and(|id|result.receipt.intent_ids.iter().any(|intent|intent==id))&&native::value(target,"provider")==Some(&job.provider)&&native::value(target,"cid")==Some(&job.cid)));
        let born_observation=shared_matches&&job.operation=="reconcile"&&before.row("pin_jobs",&[&job.id]).is_none();
        if lease_matches||target_matches||remote_matches||shared_matches&&matches!(job.operation.as_str(),"submit"|"poll")||born_observation {
            let directive=match job.operation.as_str() {"submit"|"poll"=>records::JobDirective::Submit,"reconcile"=>records::JobDirective::ReadOnlyObserve,"unpin"=>records::JobDirective::ExistingCleanup,_=>return Err(ManagementError::InvalidEvidence)};
            let born_intent_work=lease_matches&&before.row("pin_jobs",&[&job.id]).is_none()&&matches!(entry.request.action,Action::Create(_)|Action::Backfill(_)|Action::Reapply(_));
            if directive==records::JobDirective::Submit&&(matches!(entry.request.action,Action::Cancel|Action::Reconcile)||!born_intent_work&&result.jobs.iter().any(|job|job.directive==records::JobDirective::ReadOnlyObserve)){continue;}
            result.jobs.push(job_authority::birth(tx,&job,entry,directive,before,&result.receipt.intent_ids).await?);
        }
    }
    result.jobs.sort_by(|left,right|left.job_id.cmp(&right.job_id));
    result.receipt.job_ids=result.jobs.iter().map(|job|job.job_id.clone()).collect();
    result.targets.sort();result.targets.dedup();
    Ok(result)
}
