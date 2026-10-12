//! Typed read-only inspection of complete original DB protocol rows.
use super::*;
pub(crate) enum RecordPayload {
    Identity,
    Evidence {attestation:crate::pinning::management::evidence::ReadAttestation,original_object:native::NativeRow,original_version:native::NativeRow},
    Plan {binding:ConfirmedBinding,snapshot:crate::pinning::management::Snapshot,action:Option<CommittedAction>,applied:bool},
    Retirement {binding:ConfirmedBinding,inventory:NativeFrame},
}
pub(crate) fn inspect_original_row(table:&str,row:&native::NativeRow)->Result<RecordPayload> {
    if table==super::super::schema::RETIREMENTS {
        let binding=decode_confirmed_binding(native::value(row,"binding").ok_or(ManagementError::InvalidEvidence)?)?;
        let (mut inventory,private):(NativeFrame,Vec<(native::NativeKey,native::NativeRow)>)=serde_json::from_str(&codec::unpack(native::value(row,"inventory").ok_or(ManagementError::InvalidEvidence)?)?).map_err(|_|ManagementError::InvalidEvidence)?;
        inventory.restore_private(private)?;
        if binding.confirmation.native_frame.as_ref()!=Some(&inventory)||native::value(row,"record_kind")!=Some("action")||native::value(row,"plan_id")!=Some(&binding.request.plan_id) {return Err(ManagementError::Confirmation);}
        let entry=binding.confirmation.bindings.iter().find(|entry|Some(entry.request.item_id.as_str())==native::value(row,"item_id")).ok_or(ManagementError::Confirmation)?;
        let disposition=retirement::confirmed_disposition(&entry.request.action)?;
        let expected=match disposition {Disposition::Retain=>"retain",Disposition::Migrate=>"migrate",Disposition::ControlledCleanup=>"controlled_cleanup"};
        if native::value(row,"disposition")!=Some(expected){return Err(ManagementError::Confirmation);}
        return Ok(RecordPayload::Retirement {binding,inventory});
    }
    if table!=super::super::schema::RECORDS{return Err(ManagementError::InvalidEvidence);}
    match native::value(row,"kind") {
        Some("database_scope")=>if native::value(row,"state")==Some("identity"){Ok(RecordPayload::Identity)}else{Err(ManagementError::InvalidEvidence)},
        Some("evidence_preparation")=>{
            let (attestation,_endpoint):(crate::pinning::management::evidence::ReadAttestation,String)=serde_json::from_str(native::value(row,"result").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?;
            let (original_object,original_version):(native::NativeRow,native::NativeRow)=serde_json::from_str(native::value(row,"native_before").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?;
            if native::value(row,"state")!=Some("prepared")||native::value(row,"completed_at").is_none()||native::value(row,"item_id")!=Some(&attestation.identity.private_version_id)
                ||native::value(&original_object,"id")!=Some(&attestation.identity.object_id)||native::value(&original_version,"id")!=Some(&attestation.identity.private_version_id)
                ||native::value(&original_version,"object_id")!=Some(&attestation.identity.object_id){return Err(ManagementError::Confirmation);}
            Ok(RecordPayload::Evidence {attestation,original_object,original_version})
        },
        Some("action")=>{
            let before=native::value(row,"native_before").ok_or(ManagementError::InvalidEvidence)?;
            let (draft,snapshot)=confirmation::original_proposal(before)?;
            let encoded=native::value(row,"binding").ok_or(ManagementError::InvalidEvidence)?;
            let binding=if encoded==before {draft}else{let binding=decode_confirmed_binding(encoded)?;confirmation::verify_original(before,&binding)?;binding};
            if native::value(row,"plan_id")!=Some(&binding.request.plan_id)||!binding.request.items.iter().any(|item|Some(item.item_id.as_str())==native::value(row,"item_id")){return Err(ManagementError::Confirmation);}
            let applied=native::value(row,"state")==Some("applied");
            let action=if applied {action_from_original_row(&binding.database_scope,&binding.database_identity,row)?.map(|(_,action)|action)}else{None};
            if !matches!(native::value(row,"state"),Some("prepared"|"applied"|"stopped"|"needs_attention")){return Err(ManagementError::InvalidEvidence);}
            Ok(RecordPayload::Plan {binding,snapshot,action,applied})
        },
        _=>Err(ManagementError::InvalidEvidence),
    }
}
