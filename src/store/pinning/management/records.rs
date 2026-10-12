//! Durable typed record seams for B/C. No business action is implemented here.
use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, DatabaseTransaction, Statement};
use serde::{Deserialize, Serialize};
use super::{ManagementError, Result, native::{self, NativeFrame}};
use crate::pinning::management::{Confirmation, PlanRequest};
mod confirmation;
mod imported;
mod retirement;
mod results;
mod codec;
mod index;
mod inspection;
#[cfg(test)]
pub(crate) mod fault_injection;
pub use results::{ActionState, JobDirective, JobAuthority, MigrationPoint, NamespacePlanningProjection, OperatorDecision, CommittedAction, lookup_committed, write_committed_action_in_transaction};
pub(crate) use results::update_committed_state_in_transaction;
pub(crate) use results::frame_actions;
pub(crate) use results::decode_result as decode_action_result;
pub(crate) use index::{actions_for_job,actions_for_lease,action_at_key,action_from_original_row};
pub(crate) use inspection::{inspect_original_row,RecordPayload};
pub use confirmation::{PlanLocator, stage_confirmation, confirm_imported, recheck_confirmed_frame, revalidate_confirmed_in_transaction, lookup_applied_from_imported};
pub use imported::load_confirmed_from_imported;
pub use retirement::{RetirementState, read_retirement, write_retirement_in_transaction};

/// Constructed only after local Config validation and native DB identity checks.
#[derive(Clone)]
pub struct LocalAuthorization {
    principal: String,
    database_scope: String,
    database_identity: String,
}
impl LocalAuthorization {
    pub fn principal(&self) -> &str { &self.principal }
    pub fn database_scope(&self) -> &str { &self.database_scope }
    pub fn database_identity(&self) -> &str { &self.database_identity }
    pub async fn check<C: ConnectionTrait>(&self, db: &C) -> Result<()> {
        if self.database_identity.starts_with("diagnostic:") {return Err(ManagementError::Unauthorized);}
        if native::database_identity(db).await? != self.database_identity { return Err(ManagementError::Unauthorized); }
        let row = db.query_one(Statement::from_string(db.get_database_backend(),
            "SELECT database_scope FROM pin_management_records WHERE kind='database_scope'"))
            .await.map_err(|_| ManagementError::Database)?.ok_or(ManagementError::Schema)?;
        if row.try_get::<String>("","database_scope").map_err(|_| ManagementError::Schema)? != self.database_scope { return Err(ManagementError::Unauthorized); }
        Ok(())
    }
}

pub async fn authorize_local<C: ConnectionTrait>(config: &crate::config::Config, db: &C) -> Result<LocalAuthorization> {
    config.validate_kubo().map_err(|_| ManagementError::Unauthorized)?;
    if config.auth.credentials.is_empty() || config.auth.credentials.iter().any(|c| c.access_key.is_empty() || c.secret_key.is_empty()) { return Err(ManagementError::Unauthorized); }
    super::schema::validate(db).await?;
    let frame = native::capture(db).await?;
    if frame.database_identity.starts_with("diagnostic:") {return Err(ManagementError::Unauthorized);}
    Ok(LocalAuthorization { principal: "local_configuration".into(), database_scope: frame.database_scope, database_identity: frame.database_identity })
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfirmedBinding {
    pub request: PlanRequest,
    pub confirmation: Confirmation,
    pub principal: String,
    pub database_scope: String,
    pub database_identity: String,
    pub confirmed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl ConfirmedBinding {
    pub async fn check<C: ConnectionTrait>(&self, db: &C, authorization: &LocalAuthorization) -> Result<()> {
        authorization.check(db).await?;
        if self.principal != authorization.principal || self.database_scope != authorization.database_scope || self.database_identity != authorization.database_identity || self.confirmation.plan_id != self.request.plan_id { return Err(ManagementError::Confirmation); }
        let Some(frame) = &self.confirmation.native_frame else { return Err(ManagementError::Confirmation); };
        if frame.database_identity != self.database_identity || frame.database_scope != self.database_scope { return Err(ManagementError::Confirmation); }
        let now = crate::store::database_clock::database_now(db).await.map_err(|_| ManagementError::Database)?;
        if now < self.confirmed_at || now >= self.expires_at || self.expires_at <= self.confirmed_at { return Err(ManagementError::Expired); }
        confirmation::stored_binding(db,authorization,self).await?;
        Ok(())
    }
}

/// Convenience for an in-process plan. File workflows use the explicit stage
/// and confirm_imported functions; neither an ID nor wire data grants authority.
pub async fn confirm(db:&sea_orm::DatabaseConnection,config:&crate::pinning::management::local_config::ManagementConfig,authorization:&LocalAuthorization,request:&PlanRequest,snapshot:&crate::pinning::management::Snapshot,plan:&crate::pinning::management::DryRunPlan,candidate_item_ids:Vec<String>,ttl:chrono::Duration)->Result<ConfirmedBinding> {
    let reviewed=plan.confirmation(candidate_item_ids.clone());
    let acknowledged=crate::pinning::management::plan::validate_confirmation(plan,&reviewed).map_err(|_|ManagementError::Confirmation)?;
    if plan.items().iter().any(|entry|matches!(entry.status,crate::pinning::management::PlanStatus::Blocked(_)) || entry.status==crate::pinning::management::PlanStatus::Candidate&&!acknowledged.contains(entry.request.item_id.as_str())) {return Err(ManagementError::Confirmation);}
    stage_confirmation(db,config,authorization,request,snapshot,plan,ttl).await?;
    let confirmation=reviewed;
    let bytes=crate::pinning::management::wire::encode_plan(request,snapshot,plan,Some(&confirmation)).map_err(|_|ManagementError::Confirmation)?;
    let imported=crate::pinning::management::wire::decode_plan(&bytes).map_err(|_|ManagementError::Confirmation)?;
    confirm_imported(db,config,authorization,&imported).await
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReceipt {
    pub binding: crate::pinning::management::PlanEntry,
    pub intent_ids: Vec<String>,
    pub job_ids: Vec<String>,
}

/// Database record encoding only. The private material is NEVER an export/file
/// format; public wire encoding deliberately excludes it and grants no authority.
pub(crate) fn encode_confirmed_binding(binding:&ConfirmedBinding)->Result<String> {
    let frame=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    frame.validate()?;
    let encoded=serde_json::to_string(&(binding,frame.private_material())).map_err(|_|ManagementError::InvalidEvidence)?;
    crate::pinning::management::wire::check_structure(encoded.as_bytes()).map_err(|_|ManagementError::Budget)?;
    codec::pack(&encoded)
}
fn decode_confirmed_binding(input:&str)->Result<ConfirmedBinding> {
    let input=codec::unpack(input)?;
    crate::pinning::management::wire::check_structure(input.as_bytes()).map_err(|_|ManagementError::Budget)?;
    let (mut binding,private):(ConfirmedBinding,Vec<(native::NativeKey,native::NativeRow)>)=serde_json::from_str(&input).map_err(|_|ManagementError::InvalidEvidence)?;
    binding.confirmation.native_frame.as_mut().ok_or(ManagementError::Confirmation)?.restore_private(private)?;
    Ok(binding)
}

pub async fn write_action_receipt_in_transaction(tx:&DatabaseTransaction,auth:&LocalAuthorization,confirmation:&ConfirmedBinding,receipt:&ActionReceipt,native_before:&NativeFrame)->Result<()> {
    confirmation.check(tx,auth).await?;
    if confirmation.confirmation.native_frame.as_ref()!=Some(native_before) || !confirmation.confirmation.bindings.iter().any(|entry|*entry==receipt.binding) {return Err(ManagementError::Confirmation);}
    validate_result_ids(tx,receipt).await?;
    let result=serde_json::to_string(receipt).map_err(|_|ManagementError::InvalidEvidence)?;
    write_result(tx, auth, confirmation, receipt, &result).await
}

async fn write_result(tx:&DatabaseTransaction,auth:&LocalAuthorization,confirmation:&ConfirmedBinding,receipt:&ActionReceipt,result:&str)->Result<()> {
    // B owns native locking/recheck and the preceding business mutation. This
    // seam records its result in that SAME transaction, not an apply operation.
    let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
    let encoded=confirmation::stored_encoding(tx,auth,confirmation).await?;
    #[cfg(test)]
    fault_injection::before_write(tx,auth,confirmation,receipt).await?;
    let changed=tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "UPDATE pin_management_records SET result=$1,intent_ids=$2,job_ids=$3,state='applied',completed_at=$4 WHERE database_scope=$5 AND plan_id=$6 AND item_id=$7 AND kind='action' AND state='prepared' AND result IS NULL AND binding=$8",
        [result.to_owned().into(),serde_json::to_string(&receipt.intent_ids).map_err(|_|ManagementError::InvalidEvidence)?.into(),serde_json::to_string(&receipt.job_ids).map_err(|_|ManagementError::InvalidEvidence)?.into(),now.into(),auth.database_scope.clone().into(),confirmation.request.plan_id.clone().into(),receipt.binding.request.item_id.clone().into(),encoded.into()])).await.map_err(|_|ManagementError::Database)?;
    if changed.rows_affected()!=1 {return Err(ManagementError::Confirmation);}
    native::capture(tx).await?;
    Ok(())
}

async fn validate_result_ids<C:ConnectionTrait>(db:&C,receipt:&ActionReceipt)->Result<()> {
    use crate::pinning::management::{Action,Capture,Subject};
    let frame=native::capture(db).await?;
    if receipt.intent_ids.iter().collect::<std::collections::BTreeSet<_>>().len()!=receipt.intent_ids.len() || receipt.job_ids.iter().collect::<std::collections::BTreeSet<_>>().len()!=receipt.job_ids.len() {return Err(ManagementError::InvalidEvidence);}
    match &receipt.binding.capture {
        Some(Capture::Object(capture))=>{
            if matches!(receipt.binding.request.action,Action::Create(_)|Action::Reapply(_)|Action::Backfill(_)) && receipt.intent_ids.is_empty() {return Err(ManagementError::InvalidEvidence);}
            for id in &receipt.intent_ids {
                let lease=frame.row("pin_leases",&[id]).ok_or(ManagementError::InvalidEvidence)?;
                if native::value(lease,"owner_object_id")!=Some(&capture.owner_identity.object_id) {return Err(ManagementError::InvalidEvidence);}
            }
            for id in &receipt.job_ids {
                let job=frame.row("pin_jobs",&[id]).ok_or(ManagementError::InvalidEvidence)?;
                 if native::value(job,"cid").and_then(|cid|crate::pinning::provider::canonical_resource_cid(cid).ok()).as_deref()!=Some(&capture.object.identity.cid) {return Err(ManagementError::InvalidEvidence);}
                if let Some(owner)=native::value(job,"lease_id") {
                    let lease=frame.row("pin_leases",&[owner]).ok_or(ManagementError::InvalidEvidence)?;
                    if native::value(lease,"owner_object_id")!=Some(&capture.owner_identity.object_id) {return Err(ManagementError::InvalidEvidence);}
                }
            }
        },
        Some(Capture::Provider(_))=>{
            if !receipt.intent_ids.is_empty() || (!receipt.job_ids.is_empty()&&!matches!(&receipt.binding.request.action,Action::RetireWith {selection:crate::pinning::management::RetirementSelection::ControlledCleanup {..}})) {return Err(ManagementError::InvalidEvidence);}
            let Subject::Provider {route}= &receipt.binding.request.subject else{return Err(ManagementError::Confirmation)};
            let present=frame.table(super::schema::RETIREMENTS).ok_or(ManagementError::Schema)?.rows.iter().any(|row|native::value(row,"plan_id")==Some(receipt.binding.id.as_str().split(':').nth(1).unwrap_or("")) && native::value(row,"item_id")==Some(&receipt.binding.request.item_id) && native::value(row,"native_identity")==Some(&frame.database_identity) && native::value(row,"database_scope")==Some(&frame.database_scope) && frame.table("pin_provider_routes").is_some_and(|table|table.rows.iter().any(|registered|native::value(registered,"provider")==native::value(row,"allocation_domain") && native::value(registered,"snapshot").and_then(|value|serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(value).ok()).as_ref()==route.original.as_ref())));
            if !present {return Err(ManagementError::InvalidEvidence);}
        },
        None=>return Err(ManagementError::InvalidEvidence),
    }
    Ok(())
}

pub async fn lookup_applied<C:ConnectionTrait>(db:&C,authorization:&LocalAuthorization,confirmation:&ConfirmedBinding,item_id:&str)->Result<Option<ActionReceipt>> {
    authorization.check(db).await?;
    if confirmation.principal!=authorization.principal || confirmation.database_scope!=authorization.database_scope || confirmation.database_identity!=authorization.database_identity {return Err(ManagementError::Unauthorized);}
    let row=db.query_one(Statement::from_sql_and_values(db.get_database_backend(),"SELECT principal,native_identity,binding,result,intent_ids,job_ids FROM pin_management_records WHERE database_scope=$1 AND plan_id=$2 AND item_id=$3 AND kind='action' AND state='applied' AND completed_at IS NOT NULL",[authorization.database_scope.clone().into(),confirmation.request.plan_id.clone().into(),item_id.into()])).await.map_err(|_|ManagementError::Database)?;
    let Some(row)=row else{return Ok(None)};
    let binding:String=row.try_get("","binding").map_err(|_|ManagementError::InvalidEvidence)?;
    let stored=decode_confirmed_binding(&binding)?;
    if stored!=*confirmation || row.try_get::<String>("","principal").map_err(|_|ManagementError::InvalidEvidence)?!=authorization.principal || row.try_get::<String>("","native_identity").map_err(|_|ManagementError::InvalidEvidence)?!=authorization.database_identity {return Err(ManagementError::Confirmation);}
    confirmation::stored_binding(db,authorization,confirmation).await?;
    let (receipt,_)=results::decode_result(&row.try_get::<String>("","result").map_err(|_|ManagementError::InvalidEvidence)?)?;
    if serde_json::from_str::<Vec<String>>(&row.try_get::<String>("","intent_ids").map_err(|_|ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?!=receipt.intent_ids || serde_json::from_str::<Vec<String>>(&row.try_get::<String>("","job_ids").map_err(|_|ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?!=receipt.job_ids {return Err(ManagementError::InvalidEvidence);}
    if !confirmation.confirmation.bindings.iter().any(|entry|entry.request.item_id==item_id&&*entry==receipt.binding) {return Err(ManagementError::Confirmation);}
    Ok(Some(receipt))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all="snake_case")]
pub enum RecordKind { EvidencePreparation, Action }
impl RecordKind { fn sql(self) -> &'static str { match self { Self::EvidencePreparation => "evidence_preparation", Self::Action => "action" } } }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all="snake_case")]
pub enum RecordState { Prepared, Applied, Stopped, NeedsAttention }
impl RecordState { fn sql(self) -> &'static str { match self { Self::Prepared => "prepared",Self::Applied => "applied",Self::Stopped => "stopped",Self::NeedsAttention => "needs_attention" } } }

/// Complete binding and original before/result are retained per item, in the
/// caller's business transaction. A record ID alone never proves completion.
pub(crate) struct RecordWrite<'a> {
    pub plan_id: &'a str,
    pub item_id: &'a str,
    pub kind: RecordKind,
    pub binding: &'a str,
    pub native_before: &'a str,
    pub result: Option<&'a str>,
    pub intent_ids: &'a [String],
    pub job_ids: &'a [String],
    pub state: RecordState,
    pub expires_at: DateTime<Utc>,
}

pub(crate) async fn insert_in_transaction(tx: &DatabaseTransaction, auth: &LocalAuthorization, record: RecordWrite<'_>) -> Result<()> {
    auth.check(tx).await?;
    if record.plan_id.is_empty() || record.item_id.is_empty() || record.binding.len().checked_add(record.native_before.len()).and_then(|n| n.checked_add(record.result.map_or(0,str::len))).is_none_or(|n| n > 16*1024*1024) { return Err(ManagementError::Budget); }
    let now = crate::store::database_clock::database_now(tx).await.map_err(|_| ManagementError::Database)?;
    if now >= record.expires_at { return Err(ManagementError::Expired); }
    let intent_ids = serde_json::to_string(record.intent_ids).map_err(|_| ManagementError::InvalidEvidence)?;
    let job_ids = serde_json::to_string(record.job_ids).map_err(|_| ManagementError::InvalidEvidence)?;
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "INSERT INTO pin_management_records (database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [auth.database_scope.clone().into(),record.plan_id.into(),record.item_id.into(),record.kind.sql().into(),auth.principal.clone().into(),auth.database_identity.clone().into(),record.binding.into(),record.native_before.into(),record.result.map(str::to_owned).into(),intent_ids.into(),job_ids.into(),record.state.sql().into(),now.into(),record.expires_at.into(),record.result.map(|_| now).into()]))
        .await.map_err(|_| ManagementError::Database)?;
    Ok(())
}

/// Exact native evidence guard; B must invoke this only after its lock/barrier
/// protocol and before publication. This does not acquire B's locks itself.
pub async fn recheck_frame<C: ConnectionTrait>(db: &C, original: &NativeFrame) -> Result<()> {
    if native::capture(db).await? != *original { return Err(ManagementError::Changed); }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all="snake_case")]
pub enum Disposition { Retain, Migrate, ControlledCleanup }

/// Persistent domain barrier shape; enforcement belongs to wave C.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementRecord {
    pub allocation_domain: String,
    pub database_scope: String,
    pub plan_id: String,
    pub item_id: String,
    pub principal: String,
    pub native_identity: String,
    pub binding: ConfirmedBinding,
    pub inventory: NativeFrame,
    pub disposition: Disposition,
    pub unresolved: Vec<String>,
    pub state: RetirementState,
    pub created_at: DateTime<Utc>,
}
