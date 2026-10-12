use super::*;
use crate::pinning::management::{evidence,ExactObject};
use chrono::{DateTime,Utc};

fn originals(frame:&native::NativeFrame,endpoint:Option<&str>)->Result<Vec<ReadAttestation>> {
    let table=frame.table(super::super::super::schema::RECORDS).ok_or(ManagementError::Schema)?;
    let mut proofs=Vec::new();
    for row in table.rows.iter().filter(|row|native::value(row,"kind")==Some("evidence_preparation")) {
        if native::value(row,"database_scope")!=Some(&frame.database_scope)||native::value(row,"native_identity")!=Some(&frame.database_identity)||native::value(row,"principal")!=Some("local_configuration")||native::value(row,"state")!=Some("prepared")||native::value(row,"completed_at").is_none(){continue;}
        let key=table.primary_key.iter().map(|column|native::value(row,column).ok_or(ManagementError::InvalidEvidence)).collect::<Result<Vec<_>>>()?;
        let encoded=frame.original_value(&table.table,&key,"result").ok_or(ManagementError::InvalidEvidence)?;
        // Historical basis uses its original private endpoint, not current TOML.
        // Both paths still use the producer's one strict attestation decoder.
        let original_endpoint=if endpoint.is_none(){Some(serde_json::from_str::<(serde::de::IgnoredAny,String)>(encoded).map_err(|_|ManagementError::InvalidEvidence)?.1)}else{None};
        let endpoint=endpoint.or(original_endpoint.as_deref()).ok_or(ManagementError::InvalidEvidence)?;
        let Ok(proof)=evidence::from_persisted(encoded,endpoint) else{continue};
        if native::value(row,"item_id")!=Some(&proof.identity.private_version_id)||native::parse_time(native::value(row,"expires_at").ok_or(ManagementError::InvalidEvidence)?)?!=proof.expires_at{continue;}
        let binding=frame.original_value(&table.table,&key,"binding").ok_or(ManagementError::InvalidEvidence)?;
        if !profile_matches(binding,&proof,endpoint){continue;}
        let created=native::parse_time(native::value(row,"created_at").ok_or(ManagementError::InvalidEvidence)?)?;
        let completed=native::parse_time(native::value(row,"completed_at").ok_or(ManagementError::InvalidEvidence)?)?;
        if created<proof.observed_at||created>=proof.expires_at||completed<created||completed>=proof.expires_at{continue;}
        let before=frame.original_value(&table.table,&key,"native_before").ok_or(ManagementError::InvalidEvidence)?;
        let (object,version):(native::NativeRow,native::NativeRow)=serde_json::from_str(before).map_err(|_|ManagementError::InvalidEvidence)?;
        if frame.original_row("objects",&[&proof.identity.object_id]).as_ref()!=Some(&object)||frame.original_row("object_versions",&[&proof.identity.private_version_id]).as_ref()!=Some(&version)
            ||frame.row("objects",&[&proof.identity.object_id])!=Some(&proof.object_tuple)||frame.row("object_versions",&[&proof.identity.private_version_id])!=Some(&proof.version_tuple){continue;}
        proofs.push(proof);
    }
    Ok(proofs)
}

fn profile_matches(binding:&str,proof:&ReadAttestation,endpoint:&str)->bool {
    let ttl=if proof.proof==evidence::ReadProof::CompleteLocalDirectoryDagBlocksV1 {
        type Profile=(ExactObject,String,u64,bool,i64,evidence::ReadProof,crate::kubo::DirectoryProofBudgets,String,(u64,u32));
        let Ok((identity,node,maximum,directory,ttl,kind,budgets,original_endpoint,idle))=serde_json::from_str::<Profile>(binding) else{return false};
        let Some(receipt)=&proof.directory_receipt else{return false};
        if identity!=proof.identity||node!=proof.source.node_identity||!directory||kind!=proof.proof||maximum!=budgets.max_payload_bytes()||budgets!=receipt.budgets()||original_endpoint!=endpoint||idle.1>=1_000_000_000
            ||u64::try_from(std::time::Duration::new(idle.0,idle.1).as_millis()).ok()!=Some(proof.source.idle_timeout_millis){return false;}ttl
    }else {
        let Ok((identity,node,maximum,directory,ttl))=serde_json::from_str::<(ExactObject,String,u64,bool,i64)>(binding) else{return false};
        if identity!=proof.identity||node!=proof.source.node_identity||directory!=proof.proof.is_directory()||proof.stored_bytes.is_some_and(|bytes|bytes>maximum){return false;}ttl
    };
    ttl>0&&ttl<=3_600_000_000_000&&proof.expires_at.signed_duration_since(proof.observed_at).num_nanoseconds().is_some_and(|duration|duration>0&&duration<=ttl)
}

/// An archived counted directory summary is not its own receipt. The exact
/// persisted evidence row and all original private tuples must still match.
pub(crate) fn trusted_proof(frame:&native::NativeFrame,summary:&ReadEvidence)->Result<ReadAttestation> {
    let mut found=None;
    for proof in originals(frame,None)? {
        if ReadEvidence::from(&proof)!=*summary{continue;}
        if found.as_ref().is_some_and(|old:&ReadAttestation|old!=&proof){return Err(ManagementError::InvalidEvidence);}
        found=Some(proof);
    }
    found.ok_or(ManagementError::InvalidEvidence)
}

/// Fresh source/config/DB-clock selection over the already complete native frame.
/// A new independently verified V1 RID may supersede a residency-only summary;
/// the original size-less RID and bytes remain untouched. Counted kind/byte
/// conflicts for the same root fail closed, including a different exact owner.
pub(crate) fn current_proof(frame:&native::NativeFrame,config:&ManagementConfig,identity:&ExactObject,now:DateTime<Utc>)->Result<Option<ReadAttestation>> {
    let mut found:Option<ReadAttestation>=None;let mut counted=None;
    for proof in originals(frame,Some(&config.source_endpoint))? {
        if proof.identity.cid!=identity.cid||config.source_node()!=Some(proof.source.node_identity.as_str())||proof.observed_at>now||now>=proof.expires_at{continue;}
        if proof.proof.has_stored_byte_count() {
            let value=count(&ReadEvidence::from(&proof))?;
            if counted.is_some_and(|old|old!=value){return Err(ManagementError::InvalidEvidence);}counted=Some(value);
        }
        if proof.identity!=*identity{continue;}
        if found.as_ref().is_none_or(|old|(!old.proof.has_stored_byte_count()&&proof.proof.has_stored_byte_count())||(old.proof==proof.proof&&proof.observed_at>old.observed_at)){found=Some(proof);}
    }
    Ok(found)
}
