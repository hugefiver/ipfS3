//! Metadata candidate lookup followed by complete original binding verification.
//! Ordinary worker jobs do not decode unrelated multi-MiB protocol records.
use super::*;
use sea_orm::{DatabaseBackend,Value};

fn membership(backend:DatabaseBackend,column:&str,parameter:usize)->String {
    if backend==DatabaseBackend::Postgres {format!("{column}::jsonb ? ${parameter}")}
    else {format!("EXISTS (SELECT 1 FROM json_each({column}) WHERE value=${parameter})")}
}

pub(crate) async fn actions_for_job<C:ConnectionTrait>(db:&C,job:&crate::store::entities::pin_job::Model)->Result<Vec<(ConfirmedBinding,CommittedAction)>> {
    let backend=db.get_database_backend();
    let mut predicate=membership(backend,"job_ids",1);let mut values=vec![job.id.clone().into()];
    let mut candidate_rows=0;let mut candidate_bytes=0;
    if job.operation=="poll"&&let Some(lease)=&job.lease_id {
        predicate=format!("({predicate} OR {})",membership(backend,"intent_ids",2));values.push(lease.clone().into());
        let originals=poll_submit_ids(db,job).await?;
        if !originals.is_empty() {
            candidate_rows=originals.len();
            let encoded=serde_json::to_string(&originals).map_err(|_|ManagementError::InvalidEvidence)?;
            candidate_bytes=encoded.len();if candidate_bytes>native::MAX_BYTES{return Err(ManagementError::Budget);}
            let original_membership=if backend==DatabaseBackend::Postgres {
                "job_ids::jsonb ?| ARRAY(SELECT jsonb_array_elements_text($3::jsonb))"
            }else {
                "EXISTS (SELECT 1 FROM json_each(job_ids) AS recorded JOIN json_each($3) AS original ON recorded.value=original.value)"
            };
            predicate=format!("({predicate} OR {original_membership})");values.push(encoded.into());
        }
    }
    select(db,&predicate,values,candidate_rows,candidate_bytes).await
}

/// Native target/generation continuity locates original Submit IDs only. The
/// selected receipts still supply all authority; this query grants none.
async fn poll_submit_ids<C:ConnectionTrait>(db:&C,job:&crate::store::entities::pin_job::Model)->Result<Vec<String>> {
    use crate::store::{entities::remote_pin,pinning::jobs};
    use sea_orm::EntityTrait;
    let (Some(lease),Some(target),Some(generation))=(&job.lease_id,&job.target_id,job.expected_generation) else{return Ok(vec![])};
    if job.expected_remote_epoch.is_some(){return Ok(vec![]);}
    let Some(remote)=remote_pin::Entity::find_by_id((job.provider.clone(),job.cid.clone())).one(db).await.map_err(|_|ManagementError::Database)? else{return Ok(vec![])};
    let Some(request)=remote.request_id.as_deref() else{return Ok(vec![])};
    if !jobs::poll_names_request(db,job,request).await.map_err(|_|ManagementError::Database)?{return Ok(vec![]);}
    // Both rows are persisted native work. Operational provider equality is a
    // locator, not an alias: original full routes/rekey points are checked later.
    let rows=db.query_all(Statement::from_sql_and_values(db.get_database_backend(),format!(
        "SELECT original.id FROM pin_jobs AS original JOIN pin_jobs AS poll ON poll.id=$1 WHERE poll.operation='poll' AND poll.lease_id=$2 AND poll.target_id=$3 AND poll.expected_generation=$4 AND poll.provider=$5 AND poll.cid=$6 AND poll.expected_remote_epoch IS NULL AND original.operation='submit' AND original.lease_id=poll.lease_id AND original.target_id=poll.target_id AND original.expected_generation=poll.expected_generation AND original.provider=poll.provider AND original.cid=poll.cid AND original.expected_remote_epoch IS NULL ORDER BY original.id LIMIT {}",native::MAX_ROWS+1),
        vec![job.id.clone().into(),lease.clone().into(),target.clone().into(),generation.into(),job.provider.clone().into(),job.cid.clone().into()]))
        .await.map_err(|_|ManagementError::Database)?;
    if rows.len()>native::MAX_ROWS{return Err(ManagementError::Budget);}
    let mut bytes=0usize;let mut ids=Vec::with_capacity(rows.len());
    for row in rows {
        let id=row.try_get::<String>("","id").map_err(|_|ManagementError::InvalidEvidence)?;
        bytes=bytes.checked_add(id.len()).ok_or(ManagementError::Budget)?;
        if bytes>native::MAX_BYTES{return Err(ManagementError::Budget);}ids.push(id);
    }
    Ok(ids)
}
pub(crate) async fn actions_for_lease<C:ConnectionTrait>(db:&C,lease:&str)->Result<Vec<(ConfirmedBinding,CommittedAction)>> {
    select(db,&membership(db.get_database_backend(),"intent_ids",1),vec![lease.into()],0,0).await
}
pub(crate) async fn action_at_key<C:ConnectionTrait>(db:&C,key:&native::NativeKey)->Result<Option<(ConfirmedBinding,CommittedAction)>> {
    if key.table!=super::super::schema::RECORDS||key.values.len()!=4||key.values[3]!="action" {return Err(ManagementError::InvalidEvidence);}
    let mut actions=select(db,"database_scope=$1 AND plan_id=$2 AND item_id=$3",key.values[..3].iter().cloned().map(Into::into).collect(),0,0).await?;
    if actions.len()>1{return Err(ManagementError::InvalidEvidence);}Ok(actions.pop())
}
async fn select<C:ConnectionTrait>(db:&C,predicate:&str,values:Vec<Value>,candidate_rows:usize,candidate_bytes:usize)->Result<Vec<(ConfirmedBinding,CommittedAction)>> {
    let remaining=native::MAX_ROWS.checked_sub(candidate_rows).ok_or(ManagementError::Budget)?;
    let rows=db.query_all(Statement::from_sql_and_values(db.get_database_backend(),format!("SELECT database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,completed_at FROM pin_management_records WHERE kind='action' AND state='applied' AND completed_at IS NOT NULL AND ({predicate}) ORDER BY plan_id,item_id LIMIT {}",remaining+1),values)).await.map_err(|_|ManagementError::Database)?;
    if rows.len()>remaining{return Err(ManagementError::Budget);}
    if rows.is_empty(){return Ok(vec![]);}
    let identity=native::database_identity(db).await?;
    if identity.starts_with("diagnostic:"){return Err(ManagementError::Unauthorized);}
    let scope=db.query_one(Statement::from_string(db.get_database_backend(),"SELECT database_scope FROM pin_management_records WHERE kind='database_scope'"))
        .await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Schema)?.try_get::<String>("","database_scope").map_err(|_|ManagementError::Schema)?;
    let mut output=Vec::new();let mut bytes=candidate_bytes;
    for row in rows {
        let mut original=native::NativeRow::new();
        for column in ["database_scope","plan_id","item_id","kind","principal","native_identity","binding","native_before","result","intent_ids","job_ids","state"] {
            let value=row.try_get::<Option<String>>("",column).map_err(|_|ManagementError::InvalidEvidence)?;
            bytes=bytes.checked_add(value.as_ref().map_or(0,String::len)).ok_or(ManagementError::Budget)?;
            if bytes>native::MAX_BYTES {return Err(ManagementError::Budget);}original.insert(column.into(),value);
        }
        original.insert("completed_at".into(),row.try_get::<Option<DateTime<Utc>>>("","completed_at").map_err(|_|ManagementError::InvalidEvidence)?.map(|time|time.to_rfc3339()));
        if let Some((binding,action))=action_from_original_row(&scope,&identity,&original)? {
            for point in &action.migration_points {crate::store::pinning::management::actions::points::validate_original(&binding,&action,point)?;}
            output.push((binding,action));
        }
    }
    Ok(output)
}

/// An exact original row, not a string found somewhere in an opaque frame.
pub(crate) fn action_from_original_row(scope:&str,identity:&str,row:&native::NativeRow)->Result<Option<(ConfirmedBinding,CommittedAction)>> {
    if native::value(row,"kind")!=Some("action")||native::value(row,"state")!=Some("applied")||native::value(row,"completed_at").is_none(){return Ok(None);}
    if native::value(row,"database_scope")!=Some(scope)||native::value(row,"native_identity")!=Some(identity)||native::value(row,"principal")!=Some("local_configuration"){return Err(ManagementError::Unauthorized);}
    let binding=decode_confirmed_binding(native::value(row,"binding").ok_or(ManagementError::InvalidEvidence)?)?;
    confirmation::verify_original(native::value(row,"native_before").ok_or(ManagementError::InvalidEvidence)?,&binding)?;
    let (receipt,action)=results::decode_result(native::value(row,"result").ok_or(ManagementError::InvalidEvidence)?)?;
    if binding.database_scope!=scope||binding.database_identity!=identity||binding.principal!="local_configuration"
        ||native::value(row,"plan_id")!=Some(&binding.request.plan_id)||native::value(row,"item_id")!=Some(&receipt.binding.request.item_id)
        ||!binding.confirmation.bindings.contains(&receipt.binding)
        ||serde_json::from_str::<Vec<String>>(native::value(row,"intent_ids").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?!=receipt.intent_ids
        ||serde_json::from_str::<Vec<String>>(native::value(row,"job_ids").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?!=receipt.job_ids {return Err(ManagementError::Confirmation);}
    if let Some(action)=&action {
        if action.state==ActionState::Stopped||action.jobs.iter().map(|job|job.job_id.clone()).collect::<Vec<_>>()!=receipt.job_ids
            ||action.jobs.iter().map(|job|&job.job_id).collect::<std::collections::BTreeSet<_>>().len()!=action.jobs.len()
            ||action.migration_points.iter().map(|point|(&point.lease_id,&point.cid,&point.slot)).collect::<std::collections::BTreeSet<_>>().len()!=action.migration_points.len(){return Err(ManagementError::Confirmation);}
        for job in &action.jobs {crate::store::pinning::management::actions::job_authority::validate_stored(&binding,action,job)?;}
    }
    Ok(action.map(|action|(binding,action)))
}
