use super::*;
use crate::pinning::management::{Capture, PlannedStep, Subject};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all="snake_case")]
pub enum RetirementState {Retired, NeedsAttention}
impl RetirementState {fn sql(self)->&'static str {match self {Self::Retired=>"retired",Self::NeedsAttention=>"needs_attention"}}}

/// Records a disposition beside B's already-written exact action receipt in the
/// same transaction. This is not admission enforcement or a remote cleanup call.
pub async fn write_retirement_in_transaction(
    tx:&DatabaseTransaction,auth:&LocalAuthorization,confirmation:&ConfirmedBinding,
    item_id:&str,allocation_domain:&str,disposition:Disposition,unresolved:Vec<String>,
)->Result<RetirementRecord> {
    confirmation.check(tx,auth).await?;
    let entry=confirmation.confirmation.bindings.iter().find(|entry|entry.request.item_id==item_id).ok_or(ManagementError::Confirmation)?;
    let (Some(Capture::Provider(capture)),Some(PlannedStep::Retire(step)),Subject::Provider {route})=(&entry.capture,&entry.step,&entry.request.subject) else{return Err(ManagementError::Confirmation)};
    if confirmed_disposition(&entry.request.action)?!=disposition {return Err(ManagementError::Confirmation);}
    let inventory=confirmation.confirmation.native_frame.clone().ok_or(ManagementError::Confirmation)?;
    let original=route.original.as_ref().ok_or(ManagementError::InvalidEvidence)?;
    let registered=inventory.table("pin_provider_routes").ok_or(ManagementError::InvalidEvidence)?.rows.iter().any(|row|native::value(row,"provider")==Some(allocation_domain) && native::value(row,"snapshot").and_then(|value|serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(value).ok()).as_ref()==Some(original));
    if !registered || !capture.inventory.complete || !step.stop_new_allocations || unresolved.len()>native::MAX_ROWS || unresolved.iter().any(|id|id.is_empty() || id.len()>256 || id.chars().any(char::is_control)) {return Err(ManagementError::InvalidEvidence);}
    let mut unresolved=unresolved;
    for remote in capture.remotes.iter().chain(&capture.historical_remotes).filter(|remote|remote.has_unknown_responsibility()) {
        // Exact captured coordinates, not an invented UUID/hash/count proof.
        let locator=serde_json::to_string(&(&remote.key,remote.epoch,&remote.resource_id)).map_err(|_|ManagementError::InvalidEvidence)?;
        unresolved.push(locator);
    }
    unresolved.sort();unresolved.dedup();
    if unresolved.len()>native::MAX_ROWS {return Err(ManagementError::Budget);}
    let state=if step.needs_attention || step.unknown>0 || step.inflight>0 || step.cleanup_pending>0 || !unresolved.is_empty() {RetirementState::NeedsAttention}else{RetirementState::Retired};
    let created_at=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
    let record=RetirementRecord {allocation_domain:allocation_domain.into(),database_scope:auth.database_scope.clone(),plan_id:confirmation.request.plan_id.clone(),item_id:item_id.into(),principal:auth.principal.clone(),native_identity:auth.database_identity.clone(),binding:confirmation.clone(),inventory,disposition,unresolved,state,created_at};
    let binding=encode_confirmed_binding(confirmation)?;
    let inventory=codec::pack(&serde_json::to_string(&(&record.inventory,record.inventory.private_material())).map_err(|_|ManagementError::InvalidEvidence)?)?;
    let unresolved=serde_json::to_string(&record.unresolved).map_err(|_|ManagementError::InvalidEvidence)?;
    let disposition=match disposition {Disposition::Retain=>"retain",Disposition::Migrate=>"migrate",Disposition::ControlledCleanup=>"controlled_cleanup"};
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"INSERT INTO pin_management_retirements (allocation_domain,database_scope,plan_id,item_id,principal,native_identity,binding,inventory,disposition,unresolved,state,created_at,record_kind) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'action')",[record.allocation_domain.clone().into(),record.database_scope.clone().into(),record.plan_id.clone().into(),record.item_id.clone().into(),record.principal.clone().into(),record.native_identity.clone().into(),binding.into(),inventory.into(),disposition.into(),unresolved.into(),state.sql().into(),created_at.into()])).await.map_err(|_|ManagementError::Database)?;
    native::capture(tx).await?;
    Ok(record)
}

pub async fn read_retirement<C:ConnectionTrait>(db:&C,auth:&LocalAuthorization,allocation_domain:&str)->Result<Option<RetirementRecord>> {
    auth.check(db).await?;
    native::capture(db).await?;
    let row=db.query_one(Statement::from_sql_and_values(db.get_database_backend(),"SELECT database_scope,plan_id,item_id,principal,native_identity,binding,inventory,disposition,unresolved,state,created_at,record_kind FROM pin_management_retirements WHERE allocation_domain=$1",[allocation_domain.into()])).await.map_err(|_|ManagementError::Database)?;
    let Some(row)=row else{return Ok(None)};
    let text=|column:&str|row.try_get::<String>("",column).map_err(|_|ManagementError::InvalidEvidence);
    if text("database_scope")?!=auth.database_scope || text("principal")?!=auth.principal || text("native_identity")?!=auth.database_identity || text("record_kind")?!="action" {return Err(ManagementError::Unauthorized);}
    let binding=decode_confirmed_binding(&text("binding")?)?;
    let plan_id=text("plan_id")?;let item_id=text("item_id")?;
    if binding.request.plan_id!=plan_id || lookup_applied(db,auth,&binding,&item_id).await?.is_none() {return Err(ManagementError::InvalidEvidence);}
    let (mut inventory,private):(NativeFrame,Vec<(native::NativeKey,native::NativeRow)>)=serde_json::from_str(&codec::unpack(&text("inventory")?)?).map_err(|_|ManagementError::InvalidEvidence)?;
    inventory.restore_private(private)?;
    if binding.confirmation.native_frame.as_ref()!=Some(&inventory) {return Err(ManagementError::InvalidEvidence);}
    let disposition=match text("disposition")?.as_str() {"retain"=>Disposition::Retain,"migrate"=>Disposition::Migrate,"controlled_cleanup"=>Disposition::ControlledCleanup,_=>return Err(ManagementError::InvalidEvidence)};
    let state=match text("state")?.as_str() {"retired"=>RetirementState::Retired,"needs_attention"=>RetirementState::NeedsAttention,_=>return Err(ManagementError::InvalidEvidence)};
    let unresolved:Vec<String>=serde_json::from_str(&text("unresolved")?).map_err(|_|ManagementError::InvalidEvidence)?;
    let entry=binding.confirmation.bindings.iter().find(|entry|entry.request.item_id==item_id).ok_or(ManagementError::InvalidEvidence)?;
    if confirmed_disposition(&entry.request.action)?!=disposition {return Err(ManagementError::InvalidEvidence);}
    let (Some(Capture::Provider(capture)),Some(PlannedStep::Retire(step)))=(&entry.capture,&entry.step) else{return Err(ManagementError::InvalidEvidence)};
    let unknown=capture.remotes.iter().chain(&capture.historical_remotes).any(|remote|remote.has_unknown_responsibility());
    if state==RetirementState::Retired && (unknown || !capture.inventory.attribution_complete || step.needs_attention || step.unknown>0 || step.inflight>0 || step.cleanup_pending>0 || !unresolved.is_empty()) {return Err(ManagementError::InvalidEvidence);}
    Ok(Some(RetirementRecord {allocation_domain:allocation_domain.into(),database_scope:auth.database_scope.clone(),plan_id,item_id,principal:auth.principal.clone(),native_identity:auth.database_identity.clone(),binding,inventory,disposition,unresolved,state,created_at:row.try_get("","created_at").map_err(|_|ManagementError::InvalidEvidence)?}))
}

pub(super) fn confirmed_disposition(action:&crate::pinning::management::Action)->Result<Disposition> {
    use crate::pinning::management::{Action,RetirementSelection};
    match action {
        Action::Retire|Action::RetireWith {selection:RetirementSelection::Retain}=>Ok(Disposition::Retain),
        Action::RetireWith {selection:RetirementSelection::Migrate {..}}=>Ok(Disposition::Migrate),
        Action::RetireWith {selection:RetirementSelection::ControlledCleanup {..}}=>Ok(Disposition::ControlledCleanup),
        _=>Err(ManagementError::Confirmation),
    }
}
