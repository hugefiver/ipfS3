use super::*;
use crate::pinning::{management::{Action,Capture},provider::canonical_resource_cid};

/// No charge is recalculated. Directory sharing requires a persisted compatible
/// management birth/migration receipt in this same unreleased resource lifetime.
/// Historical ordinary file reservations retain their existing numeric check;
/// a proved directory basis can never be borrowed by FileCleanEof or vice versa.
pub(crate) fn check_frame(frame:&native::NativeFrame,provider:&str,cid:&str,expected:Count)->Result<()> {
    let cid=canonical_resource_cid(cid).map_err(|_|ManagementError::InvalidEvidence)?;
    let remotes=frame.table("remote_pins").ok_or(ManagementError::Schema)?.rows.iter().filter(|row|native::value(row,"provider")==Some(provider)&&native::value(row,"cid").and_then(|value|canonical_resource_cid(value).ok()).as_deref()==Some(&cid)).collect::<Vec<_>>();
    if remotes.is_empty(){return Ok(());}if remotes.len()!=1{return Err(ManagementError::InvalidEvidence);}
    let remote=remotes[0];let epoch=native::value(remote,"epoch").and_then(|value|value.parse::<i64>().ok()).ok_or(ManagementError::InvalidEvidence)?;
    if native::value(remote,"cid_size").and_then(|value|value.parse::<u64>().ok())!=Some(expected.bytes){return Err(ManagementError::InvalidEvidence);}
    let targets=frame.table("pin_lease_targets").ok_or(ManagementError::Schema)?.rows.iter().filter(|row|native::value(row,"provider")==Some(provider)&&native::value(row,"cid").and_then(|value|canonical_resource_cid(value).ok()).as_deref()==Some(&cid)).collect::<Vec<_>>();
    let actions=records::frame_actions(frame)?;let mut proved=false;
    for target in &targets {
        let id=native::value(target,"id").ok_or(ManagementError::InvalidEvidence)?;let lease=native::value(target,"lease_id").ok_or(ManagementError::InvalidEvidence)?;
        let mut target_proved=false;
        for (binding,action) in &actions {
            if !action.targets.iter().any(|target|target==id)||!action.receipt.intent_ids.iter().any(|intent|intent==lease){continue;}
            let Some(Capture::Object(capture))=&action.receipt.binding.capture else{continue};
            if capture.object.identity.cid!=cid{continue;}
            let Some(summary)=&capture.object.read_evidence else{continue};
            let count=count(summary)?;
            let before=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::InvalidEvidence)?;
            trusted_proof(before,summary)?;
            let mut original_provider=provider.to_owned();
            let origin=match &action.receipt.binding.request.action {
                Action::Create(_)|Action::Backfill(_)|Action::Reapply(_)=> {
                    let decision=action.decision.as_ref().ok_or(ManagementError::InvalidEvidence)?;
                    if decision.id!=lease||decision.owner!=capture.owner_identity||decision.stored_bytes!=count.bytes{continue;}
                    let mut origin=None;
                    for job in &action.jobs {
                        if !(job.target_id.as_deref()==Some(id)||job.directive==records::JobDirective::ReadOnlyObserve&&job.lease_id.as_deref()==Some(lease))||canonical_resource_cid(&job.cid).ok().as_deref()!=Some(&cid){continue;}
                        super::super::job_authority::validate_stored(binding,action,job)?;
                        if !same_allocation(frame,id,provider,&job.provider,&job.cid,&job.route)?{continue;}
                        original_provider=job.provider.clone();origin=Some(job.remote_epoch);break;
                    }
                    if origin.is_some(){origin
                    }else{
                        // A compatible already-confirmed resource may need no
                        // new job. Its original captured physical lifetime is
                        // retained in the full trusted before image, not guessed
                        // from current counters or a caller's applied claims.
                        before.table("remote_pins").ok_or(ManagementError::Schema)?.rows.iter().find(|row|native::value(row,"provider")==Some(provider)&&native::value(row,"cid").and_then(|cid|canonical_resource_cid(cid).ok()).as_deref()==Some(&cid)).and_then(|row|native::value(row,"epoch").and_then(|epoch|epoch.parse().ok()))
                    }
                },
                Action::Migrate {..}=> {
                    let mut origin=None;
                    for point in &action.migration_points {
                        if point.destination_target_id!=id||point.cid!=cid{continue;}
                        super::super::points::validate_original(binding,action,point)?;
                        if !same_allocation(frame,id,provider,&point.destination_allocation,&point.destination_resource_key.values[1],&point.destination_route)?{continue;}
                        original_provider=point.destination_allocation.clone();origin=Some(point.destination_epoch);break;
                    }origin
                },
                _=>None,
            };
            let Some(origin)=origin else{continue};
            if origin>epoch||released(frame,provider,&original_provider,&cid,origin,epoch)?{continue;}
            if count!=expected{return Err(ManagementError::InvalidEvidence);}
            if native::value(target,"logical_size").and_then(|value|value.parse::<u64>().ok())!=Some(count.bytes){return Err(ManagementError::InvalidEvidence);}
            target_proved=true;proved=true;
        }
        if expected.basis==Basis::DirectoryDagBlocksV1&&!target_proved{return Err(ManagementError::InvalidEvidence);}
    }
    if expected.basis==Basis::DirectoryDagBlocksV1&&!proved{return Err(ManagementError::InvalidEvidence);}Ok(())
}

fn same_allocation(frame:&native::NativeFrame,target:&str,current:&str,original:&str,cid:&str,route:&crate::pinning::identity::ProviderRouteSnapshot)->Result<bool> {
    if current==original{return Ok(true);}
    let target=[("id".into(),target.to_owned())].into();let resource=[("provider".into(),original.to_owned()),("cid".into(),cid.to_owned())].into();
    let target=super::super::points::movement_in_frame(frame,"pin_lease_targets",&target,original,route)?;
    let resource=super::super::points::movement_in_frame(frame,"remote_pins",&resource,original,route)?;
    Ok(target.is_some_and(|point|point.allocation==current)&&resource.is_some_and(|point|point.allocation==current))
}

fn released(frame:&native::NativeFrame,provider:&str,original:&str,cid:&str,first:i64,last:i64)->Result<bool> {
    for row in &frame.table("pin_resource_history").ok_or(ManagementError::Schema)?.rows {
        if ![Some(provider),Some(original)].contains(&native::value(row,"provider"))||native::value(row,"cid").and_then(|value|canonical_resource_cid(value).ok()).as_deref()!=Some(cid){continue;}
        let epoch=native::value(row,"epoch").and_then(|value|value.parse::<i64>().ok()).ok_or(ManagementError::InvalidEvidence)?;
        if epoch<first||epoch>last{continue;}
        // Same boundary as the existing native release_history consumer. A saved
        // lifetime is not a grant to reuse its basis for a replacement resource.
        return Ok(true);
    }
    Ok(false)
}
