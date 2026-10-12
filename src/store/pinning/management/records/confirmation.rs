//! Explicit DB staging is separate from zero-write dry-run. Wire is a locator
//! and reviewed public proposal, never the source of original private evidence.
use super::*;
use crate::pinning::management::{self as management, DryRunPlan, Snapshot, PlanStatus, wire::ImportedPlan, local_config::ManagementConfig};
use sea_orm::{DatabaseConnection, DatabaseBackend, IsolationLevel, TransactionTrait};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanLocator {pub plan_id:String}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proposal {binding:ConfirmedBinding,snapshot:Snapshot}

pub(super) fn public_equal<T:Serialize+?Sized>(left:&T,right:&T)->Result<bool> {
    Ok(serde_json::to_vec(left).map_err(|_|ManagementError::InvalidEvidence)?==serde_json::to_vec(right).map_err(|_|ManagementError::InvalidEvidence)?)
}
fn bounded(input:&str)->Result<()> {management::wire::check_structure(input.as_bytes()).map_err(|_|ManagementError::Budget)}
fn encode(proposal:&Proposal)->Result<String> {
    let frame=proposal.binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    let encoded=serde_json::to_string(&(proposal,frame.private_material())).map_err(|_|ManagementError::InvalidEvidence)?;
    bounded(&encoded)?;codec::pack(&encoded)
}
fn decode(input:&str)->Result<Proposal> {
    let input=codec::unpack(input)?;
    bounded(&input)?;
    let (mut proposal,private):(Proposal,Vec<(native::NativeKey,native::NativeRow)>)=serde_json::from_str(&input).map_err(|_|ManagementError::InvalidEvidence)?;
    proposal.binding.confirmation.native_frame.as_mut().ok_or(ManagementError::Confirmation)?.restore_private(private.clone())?;
    proposal.snapshot.native_frame.as_mut().ok_or(ManagementError::Confirmation)?.restore_private(private)?;
    management::validate_snapshot(&proposal.snapshot).map_err(|_|ManagementError::InvalidEvidence)?;
    Ok(proposal)
}
pub(super) fn original_proposal(before:&str)->Result<(ConfirmedBinding,Snapshot)> {let proposal=decode(before)?;Ok((proposal.binding,proposal.snapshot))}
pub(super) async fn lock(tx:&DatabaseTransaction)->Result<()> {
    if tx.get_database_backend()==DatabaseBackend::Sqlite {
        tx.execute_unprepared("UPDATE pin_management_records SET database_scope=database_scope WHERE kind='database_scope'").await.map_err(|_|ManagementError::Database)?;
    }else{
        // This protects only foundation records. B/C still own business locks.
        tx.execute_unprepared("LOCK TABLE pin_management_records,pin_management_retirements IN SHARE ROW EXCLUSIVE MODE NOWAIT").await.map_err(|_|ManagementError::Database)?;
    }
    Ok(())
}

pub async fn stage_confirmation(db:&DatabaseConnection,config:&ManagementConfig,auth:&LocalAuthorization,request:&PlanRequest,snapshot:&Snapshot,plan:&DryRunPlan,ttl:chrono::Duration)->Result<PlanLocator> {
    auth.check(db).await?;
    if ttl<=chrono::Duration::zero() || ttl>chrono::Duration::minutes(15) || management::dry_run(request,snapshot).map_err(|_|ManagementError::Confirmation)?!=*plan || plan.items().iter().any(|item|matches!(item.status,PlanStatus::Blocked(_))) {return Err(ManagementError::Confirmation);}
    let tx=db.begin_with_config(Some(IsolationLevel::RepeatableRead),None).await.map_err(|_|ManagementError::Database)?;
    lock(&tx).await?;
    let current=management::db_snapshot::collect_in_transaction(&tx,config,request).await.map_err(|_|ManagementError::Changed)?.planning_snapshot().map_err(|_|ManagementError::Changed)?;
    if management::dry_run(request,&current).map_err(|_|ManagementError::Changed)?!=*plan {return Err(ManagementError::Changed);}
    let now=crate::store::database_clock::database_now(&tx).await.map_err(|_|ManagementError::Database)?;
    let binding=ConfirmedBinding {request:request.clone(),confirmation:plan.confirmation(vec![]),principal:auth.principal.clone(),database_scope:auth.database_scope.clone(),database_identity:auth.database_identity.clone(),confirmed_at:now,expires_at:now.checked_add_signed(ttl).ok_or(ManagementError::Expired)?};
    let frame=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    if frame.database_identity!=auth.database_identity || frame.database_scope!=auth.database_scope {return Err(ManagementError::Unauthorized);}
    let before=encode(&Proposal {binding:binding.clone(),snapshot:snapshot.clone()})?;
    // Immutable original proposal is retained in native_before, even after the
    // prepared row becomes confirmed/applied. Same IDs cannot recapture a body.
    for item in &request.items {
        insert_in_transaction(&tx,auth,RecordWrite {plan_id:&request.plan_id,item_id:&item.item_id,kind:RecordKind::Action,binding:&before,native_before:&before,result:None,intent_ids:&[],job_ids:&[],state:RecordState::Prepared,expires_at:binding.expires_at}).await?;
    }
    native::capture(&tx).await?; // entire post-record inventory, no truncated write
    tx.commit().await.map_err(|_|ManagementError::Database)?;
    Ok(PlanLocator {plan_id:request.plan_id.clone()})
}

pub(super) async fn rows<C:ConnectionTrait>(db:&C,auth:&LocalAuthorization,plan_id:&str)->Result<Vec<native::NativeRow>> {
    let actual=db.query_all(Statement::from_sql_and_values(db.get_database_backend(),format!("SELECT database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at FROM pin_management_records WHERE database_scope=$1 AND plan_id=$2 AND kind='action' ORDER BY item_id LIMIT {}",management::MAX_ITEMS+1),[auth.database_scope.clone().into(),plan_id.into()])).await.map_err(|_|ManagementError::Database)?;
    if actual.len()>management::MAX_ITEMS {return Err(ManagementError::Budget);}
    let mut rows=Vec::new();
    for row in actual {
        let mut original=native::NativeRow::new();
        for column in ["database_scope","plan_id","item_id","kind","principal","native_identity","binding","native_before","result","intent_ids","job_ids","state"] {
            original.insert(column.into(),row.try_get::<Option<String>>("",column).map_err(|_|ManagementError::InvalidEvidence)?);
        }
        for column in ["created_at","expires_at","completed_at"] {original.insert(column.into(),row.try_get::<Option<DateTime<Utc>>>("",column).map_err(|_|ManagementError::InvalidEvidence)?.map(|time|time.to_rfc3339()));}
        if native::value(&original,"principal")!=Some(&auth.principal)||native::value(&original,"native_identity")!=Some(&auth.database_identity) {return Err(ManagementError::Unauthorized);}
        rows.push(original);
    }
    Ok(rows)
}
fn exact_keys(rows:&[native::NativeRow],binding:&ConfirmedBinding)->Result<Vec<native::NativeKey>> {
    if rows.len()!=binding.request.items.len() {return Err(ManagementError::Confirmation);}
    let mut keys=Vec::new();
    for row in rows {
        let item=native::value(row,"item_id").ok_or(ManagementError::InvalidEvidence)?;
        if !binding.request.items.iter().any(|request|request.item_id==item) || !matches!(native::value(row,"state"),Some("prepared"|"applied")) {return Err(ManagementError::Confirmation);}
        let expires=native::value(row,"expires_at").ok_or(ManagementError::InvalidEvidence)?;
        if native::parse_time(expires)?!=binding.expires_at {return Err(ManagementError::Confirmation);}
        if native::value(row,"state")==Some("prepared") && (native::value(row,"result").is_some() || native::value(row,"completed_at").is_some() || native::value(row,"intent_ids")!=Some("[]") || native::value(row,"job_ids")!=Some("[]")) {return Err(ManagementError::Confirmation);}
        keys.push(native::NativeKey {table:super::super::schema::RECORDS.into(),values:vec![binding.database_scope.clone(),binding.request.plan_id.clone(),item.into(),"action".into()]});
    }
    Ok(keys)
}

pub async fn confirm_imported(db:&DatabaseConnection,config:&ManagementConfig,auth:&LocalAuthorization,imported:&ImportedPlan)->Result<ConfirmedBinding> {
    auth.check(db).await?;
    if !imported.untrusted_applied_claims().is_empty() {return Err(ManagementError::Confirmation);}
    let reviewed=imported.confirmation().ok_or(ManagementError::Confirmation)?;
    let tx=db.begin_with_config(Some(IsolationLevel::RepeatableRead),None).await.map_err(|_|ManagementError::Database)?;
    lock(&tx).await?;
    let rows=rows(&tx,auth,&imported.request().plan_id).await?;
    let first=rows.first().ok_or(ManagementError::Confirmation)?;
    let before=native::value(first,"native_before").ok_or(ManagementError::InvalidEvidence)?;
    let proposal=decode(before)?;
    let (original,mut expected)=super::imported::validate_reviewed_original(&proposal.binding,&proposal.snapshot,imported)?;
    let keys=exact_keys(&rows,&proposal.binding)?;
    if rows.iter().any(|row|native::value(row,"native_before")!=Some(before)) {return Err(ManagementError::Confirmation);}
    let now=crate::store::database_clock::database_now(&tx).await.map_err(|_|ManagementError::Database)?;
    if now<proposal.binding.confirmed_at || now>=proposal.binding.expires_at {return Err(ManagementError::Expired);}
    let mut binding=proposal.binding.clone();
    expected.native_frame=proposal.binding.confirmation.native_frame.clone();
    binding.confirmation=expected;
    let first_binding=native::value(first,"binding").ok_or(ManagementError::InvalidEvidence)?;
    let already_confirmed=first_binding!=before;
    if already_confirmed {
        let stored=decode_confirmed_binding(first_binding)?;
        if stored.request!=binding.request || !public_equal(&stored.confirmation,reviewed)? || stored.confirmed_at<proposal.binding.confirmed_at || stored.confirmed_at>now || stored.expires_at!=binding.expires_at || rows.iter().any(|row|native::value(row,"binding")!=Some(first_binding)) {return Err(ManagementError::Confirmation);}
        binding=stored;
        stored_binding(&tx,auth,&binding).await?;
    }else{binding.confirmed_at=now;}
    let encoded=encode_confirmed_binding(&binding)?;
    if !already_confirmed {
        if rows.iter().any(|row|native::value(row,"binding")!=Some(before) || native::value(row,"state")!=Some("prepared")) {return Err(ManagementError::Confirmation);}
        let mut fresh=management::db_snapshot::collect_in_transaction(&tx,config,&binding.request).await.map_err(|_|ManagementError::Changed)?.planning_snapshot().map_err(|_|ManagementError::Changed)?;
        let frame=fresh.native_frame.as_ref().ok_or(ManagementError::Changed)?.without_records(&keys);
        for domain in &mut fresh.ledger_domains {domain.frontier=frame.frontier.clone();}
        for inventory in &mut fresh.inventories {inventory.frontier=frame.frontier.clone();}
        fresh.native_frame=Some(frame);
        management::revalidate(&original,&binding.confirmation,&fresh).map_err(|_|ManagementError::Changed)?;
        for key in &keys {
            let changed=tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET binding=$1 WHERE database_scope=$2 AND plan_id=$3 AND item_id=$4 AND kind='action' AND state='prepared' AND binding=$5",[encoded.clone().into(),auth.database_scope.clone().into(),binding.request.plan_id.clone().into(),key.values[2].clone().into(),before.into()])).await.map_err(|_|ManagementError::Database)?;
            if changed.rows_affected()!=1 {return Err(ManagementError::Confirmation);}
        }
    }
    native::capture(&tx).await?;
    tx.commit().await.map_err(|_|ManagementError::Database)?;
    Ok(binding)
}

pub(super) async fn stored_binding<C:ConnectionTrait>(db:&C,auth:&LocalAuthorization,binding:&ConfirmedBinding)->Result<Vec<native::NativeKey>> {
    let rows=rows(db,auth,&binding.request.plan_id).await?;
    let keys=exact_keys(&rows,binding)?;
    for row in &rows {
        if decode_confirmed_binding(native::value(row,"binding").ok_or(ManagementError::InvalidEvidence)?)?!=*binding {return Err(ManagementError::Confirmation);}
        let before=native::value(row,"native_before").ok_or(ManagementError::InvalidEvidence)?;
        verify_original(before,binding)?;
        let proposal=decode(before)?;
        let created=native::parse_time(native::value(row,"created_at").ok_or(ManagementError::InvalidEvidence)?)?;
        if created<proposal.binding.confirmed_at||created>=proposal.binding.expires_at{return Err(ManagementError::Confirmation);}
        if let Some(completed)=native::value(row,"completed_at") {let completed=native::parse_time(completed)?;if completed<binding.confirmed_at||completed>=binding.expires_at{return Err(ManagementError::Confirmation);}}
    }
    Ok(keys)
}

pub(super) fn verify_original(before:&str,binding:&ConfirmedBinding)->Result<()> {
    let proposal=decode(before)?;let mut original=proposal.binding;
    // The locally staged original already contains the full frozen plan. Do not
    // recursively re-plan every older record while a worker claim is expiring.
    management::validate_request(&original.request).map_err(|_|ManagementError::InvalidEvidence)?;
    if original.confirmation.native_frame!=proposal.snapshot.native_frame||original.confirmation.bindings.len()!=original.request.items.len() {return Err(ManagementError::Confirmation);}
    let acknowledged=binding.confirmation.candidate_item_ids.iter().collect::<std::collections::BTreeSet<_>>();
    if acknowledged.len()!=binding.confirmation.candidate_item_ids.len()
        ||acknowledged.iter().any(|id|!original.confirmation.bindings.iter().any(|entry|entry.request.item_id==**id&&entry.status==PlanStatus::Candidate))
        ||original.confirmation.bindings.iter().zip(&original.request.items).any(|(entry,request)|entry.request!=*request||matches!(entry.status,PlanStatus::Blocked(_))||entry.status==PlanStatus::Candidate&&!acknowledged.contains(&entry.request.item_id)) {return Err(ManagementError::Confirmation);}
    if binding.confirmed_at<original.confirmed_at||binding.confirmed_at>=original.expires_at{return Err(ManagementError::Confirmation);}
    original.confirmed_at=binding.confirmed_at;original.confirmation.candidate_item_ids=binding.confirmation.candidate_item_ids.clone();
    if original!=*binding{return Err(ManagementError::Confirmation);}Ok(())
}

pub(super) async fn stored_encoding<C:ConnectionTrait>(db:&C,auth:&LocalAuthorization,binding:&ConfirmedBinding)->Result<String> {
    let rows=rows(db,auth,&binding.request.plan_id).await?;
    let encoded=native::value(rows.first().ok_or(ManagementError::Confirmation)?,"binding").ok_or(ManagementError::InvalidEvidence)?;
    if decode_confirmed_binding(encoded)?!=*binding||rows.iter().any(|row|native::value(row,"binding")!=Some(encoded)) {return Err(ManagementError::Confirmation);}
    Ok(encoded.into())
}

/// Not a business lock or apply permission. B must call this under its own
/// publication/barrier protocol and independently revalidate current semantics.
pub async fn recheck_confirmed_frame<C:ConnectionTrait>(db:&C,auth:&LocalAuthorization,binding:&ConfirmedBinding)->Result<()> {
    binding.check(db,auth).await?;
    let keys=stored_binding(db,auth,binding).await?;
    let current=native::capture(db).await?.without_records(&keys);
    if binding.confirmation.native_frame.as_ref()!=Some(&current) {return Err(ManagementError::Changed);}
    Ok(())
}

/// Fresh pure semantics and exact original fences in B's transaction. The
/// caller, not this function, owns publication locks/CAS and retirement admission.
pub async fn revalidate_confirmed_in_transaction(tx:&DatabaseTransaction,config:&ManagementConfig,auth:&LocalAuthorization,binding:&ConfirmedBinding)->Result<Vec<management::CheckedItem>> {
    binding.check(tx,auth).await?;
    let rows=rows(tx,auth,&binding.request.plan_id).await?;
    let keys=stored_binding(tx,auth,binding).await?;
    let proposal=decode(native::value(rows.first().ok_or(ManagementError::Confirmation)?,"native_before").ok_or(ManagementError::InvalidEvidence)?)?;
    let plan=management::dry_run(&binding.request,&proposal.snapshot).map_err(|_|ManagementError::InvalidEvidence)?;
    let mut current=management::db_snapshot::collect_in_transaction(tx,config,&binding.request).await.map_err(|_|ManagementError::Changed)?.planning_snapshot().map_err(|_|ManagementError::Changed)?;
    let frame=current.native_frame.as_ref().ok_or(ManagementError::Changed)?.without_records(&keys);
    for domain in &mut current.ledger_domains {domain.frontier=frame.frontier.clone();}
    for inventory in &mut current.inventories {inventory.frontier=frame.frontier.clone();}
    current.native_frame=Some(frame);
    management::revalidate(&plan,&binding.confirmation,&current).map_err(|_|ManagementError::Changed)
}

/// Expired plans can still report a durable applied result. The original binding
/// is restored internally without granting publication or a renewed deadline.
pub async fn lookup_applied_from_imported<C:ConnectionTrait>(db:&C,auth:&LocalAuthorization,imported:&ImportedPlan,item_id:&str)->Result<Option<ActionReceipt>> {
    auth.check(db).await?;
    if !imported.untrusted_applied_claims().is_empty() {return Err(ManagementError::Confirmation);}
    let rows=rows(db,auth,&imported.request().plan_id).await?;
    let Some(row)=rows.iter().find(|row|native::value(row,"item_id")==Some(item_id)) else{return Ok(None)};
    if native::value(row,"state")!=Some("applied") {return Ok(None);}
    let stored=super::imported::load_from_rows(db,auth,imported,&rows).await?;
    lookup_applied(db,auth,&stored,item_id).await
}
