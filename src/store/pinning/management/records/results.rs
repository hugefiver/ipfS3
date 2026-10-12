//! Strict business result envelope. The original foundation receipt remains
//! readable, but cannot masquerade as a management worker/point authority.
use super::*;
use crate::pinning::{identity::ProviderRouteSnapshot, management::{ExactObject, Intent}};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all="snake_case")]
pub enum ActionState { Stopped, Accepted, Queued, Waiting, ReadOnly, RemoteComplete, ReplicationComplete, NeedsAttention }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all="snake_case")]
pub enum JobDirective { Submit, ReadOnlyObserve, ExistingCleanup }

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobAuthority {
    pub job_id: String,
    pub directive: JobDirective,
    pub provider: String,
    pub cid: String,
    pub remote_epoch: i64,
    pub route: ProviderRouteSnapshot,
    pub resource_key: native::NativeKey,
    pub resource_id: Option<String>,
    pub lease_id: Option<String>,
    pub lease_generation: Option<i64>,
    pub target_id: Option<String>,
    pub owner: Option<ExactObject>,
    pub source: Option<ExactObject>,
    pub lease_source: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub policy_id:Option<String>,pub provider_mode:Option<String>,pub content_mode:Option<String>,pub lease_contract:Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationPoint {
    pub table: String,
    pub primary_key: native::NativeKey,
    pub capture_position: String,
    pub original_contract: native::NativeRow,
    pub slot: String,
    pub lease_id: String,
    pub cid: String,
    pub source_target_id: String,
    pub source_allocation: String,
    pub source_route: ProviderRouteSnapshot,
    pub source_resource_route:ProviderRouteSnapshot,
    pub source_epoch: i64,
    pub source_resource_id: Option<String>,
    pub source_resource_key: native::NativeKey,
    pub source_generation:i64,
    pub source_lease:native::NativeRow,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub predecessor:Option<native::NativeKey>,
    pub destination_allocation: String,
    pub destination_route: ProviderRouteSnapshot,
    pub destination_target_id: String,
    pub destination_epoch: i64,
    pub destination_resource_key:native::NativeKey,
    pub destination_resource_id:Option<String>,
    /// Destination satisfaction must still meet the original logical slot's
    /// complete threshold and freshness. No historical JSON is rewritten.
    pub started_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    /// Archived proposal data only. Formal stored points must keep this None.
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub planning_projection:Option<NamespacePlanningProjection>,
}

#[derive(Clone,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespacePlanningProjection {pub legacy_record:native::NativeKey,pub allocation:String,pub execution_route:ProviderRouteSnapshot}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDecision {
    pub id: String,
    pub owner: ExactObject,
    pub intent: Intent,
    pub stored_bytes: u64,
    pub configuration: crate::pinning::management::ConfigurationBinding,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedAction {
    pub receipt: ActionReceipt,
    pub state: ActionState,
    pub targets: Vec<String>,
    pub jobs: Vec<JobAuthority>,
    pub migration_points: Vec<MigrationPoint>,
    pub decision: Option<OperatorDecision>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredResult { Business(CommittedAction), Foundation(ActionReceipt) }

pub(crate) fn decode_result(input: &str) -> Result<(ActionReceipt, Option<CommittedAction>)> {
    let input=codec::unpack(input)?;
    crate::pinning::management::wire::check_structure(input.as_bytes()).map_err(|_|ManagementError::Budget)?;
    match serde_json::from_str(&input).map_err(|_|ManagementError::InvalidEvidence)? {
        StoredResult::Business(result) => Ok((result.receipt.clone(), Some(result))),
        StoredResult::Foundation(receipt) => Ok((receipt, None)),
    }
}

pub async fn write_committed_action_in_transaction(
    tx: &DatabaseTransaction, auth: &LocalAuthorization, binding: &ConfirmedBinding,
    action: &CommittedAction, before: &NativeFrame,
) -> Result<()> {
    binding.check(tx, auth).await?;
    if binding.confirmation.native_frame.as_ref()!=Some(before)
        || !binding.confirmation.bindings.contains(&action.receipt.binding)
        || action.jobs.iter().map(|job|job.job_id.clone()).collect::<Vec<_>>() != action.receipt.job_ids
    { return Err(ManagementError::Confirmation); }
    crate::store::pinning::management::actions::job_authority::validate_birth(tx, binding, action).await?;
    // Foundation's same-owner assertion stays unchanged. Only a fully verified
    // formal shared authority can reference another owner's existing job.
    let mut ordinary=action.receipt.clone();ordinary.job_ids.clear();
    for authority in &action.jobs {
        let same_capture=match &action.receipt.binding.capture {Some(crate::pinning::management::Capture::Object(capture))=>authority.owner.as_ref().is_none_or(|owner|owner.object_id==capture.owner_identity.object_id)&&crate::pinning::provider::canonical_resource_cid(&authority.cid).ok().as_deref()==Some(&capture.object.identity.cid),_=>true};
        if same_capture {ordinary.job_ids.push(authority.job_id.clone());}
    }
    validate_result_ids(tx,&ordinary).await?;
    crate::store::pinning::management::actions::points::validate_birth(tx, binding, action).await?;
    let result = serde_json::to_string(action).map_err(|_|ManagementError::InvalidEvidence)?;
    crate::pinning::management::wire::check_structure(result.as_bytes()).map_err(|_|ManagementError::Budget)?;
    write_result(tx, auth, binding, &action.receipt, &codec::pack(&result)?).await
}

pub(crate) async fn update_committed_state_in_transaction(tx:&DatabaseTransaction,auth:&LocalAuthorization,binding:&ConfirmedBinding,action:&CommittedAction,state:ActionState)->Result<()> {
    if state==action.state{return Ok(());}
    let original=codec::pack(&serde_json::to_string(action).map_err(|_|ManagementError::InvalidEvidence)?)?;
    let mut updated=action.clone();updated.state=state;
    let encoded=codec::pack(&serde_json::to_string(&updated).map_err(|_|ManagementError::InvalidEvidence)?)?;
    let changed=tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET result=$1 WHERE database_scope=$2 AND plan_id=$3 AND item_id=$4 AND kind='action' AND state='applied' AND result=$5",[encoded.into(),auth.database_scope.clone().into(),binding.request.plan_id.clone().into(),action.receipt.binding.request.item_id.clone().into(),original.into()])).await.map_err(|_|ManagementError::Database)?;
    if changed.rows_affected()!=1{return Err(ManagementError::Confirmation);}Ok(())
}

/// Static trust/binding validation only. Completion and worker consumers perform
/// their own current target/lease/epoch fences; a historical receipt is not live.
pub async fn lookup_committed<C: ConnectionTrait>(
    db: &C, auth: &LocalAuthorization, binding: &ConfirmedBinding, item: &str,
) -> Result<Option<CommittedAction>> {
    let Some(receipt) = lookup_applied(db, auth, binding, item).await? else { return Ok(None); };
    let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT result FROM pin_management_records WHERE database_scope=$1 AND plan_id=$2 AND item_id=$3 AND kind='action' AND state='applied'",
        [auth.database_scope.clone().into(), binding.request.plan_id.clone().into(), item.into()])).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
    let (_, action) = decode_result(&row.try_get::<String>("", "result").map_err(|_|ManagementError::InvalidEvidence)?)?;
    let action = action.ok_or(ManagementError::InvalidEvidence)?;
    if action.receipt != receipt { return Err(ManagementError::InvalidEvidence); }
    for job in &action.jobs {crate::store::pinning::management::actions::job_authority::validate_stored(binding,&action,job)?;}
    for point in &action.migration_points {crate::store::pinning::management::actions::points::validate_original(binding,&action,point)?;}
    Ok(Some(action))
}

/// Pure extraction from a complete native frame. Imported public frames lack
/// this original private DB material and cannot manufacture durable authority.
pub(crate) fn frame_actions(frame:&NativeFrame)->Result<Vec<(ConfirmedBinding,CommittedAction)>> {
    let rows=&frame.table(super::super::schema::RECORDS).ok_or(ManagementError::Schema)?.rows;
    let mut actions=Vec::new();
    for row in rows.iter().filter(|row|native::value(row,"kind")==Some("action")&&native::value(row,"state")==Some("applied")&&native::value(row,"completed_at").is_some()) {
        let scope=native::value(row,"database_scope").ok_or(ManagementError::InvalidEvidence)?;
        let plan=native::value(row,"plan_id").ok_or(ManagementError::InvalidEvidence)?;
        let item=native::value(row,"item_id").ok_or(ManagementError::InvalidEvidence)?;
        if scope!=frame.database_scope||native::value(row,"native_identity")!=Some(&frame.database_identity)||native::value(row,"principal")!=Some("local_configuration") {return Err(ManagementError::Unauthorized);}
        let key=[scope,plan,item,"action"];
        let binding=decode_confirmed_binding(frame.original_value(super::super::schema::RECORDS,&key,"binding").ok_or(ManagementError::InvalidEvidence)?)?;
        confirmation::verify_original(frame.original_value(super::super::schema::RECORDS,&key,"native_before").ok_or(ManagementError::InvalidEvidence)?,&binding)?;
        let (receipt,result)=decode_result(frame.original_value(super::super::schema::RECORDS,&key,"result").ok_or(ManagementError::InvalidEvidence)?)?;
        if binding.request.plan_id!=plan||binding.database_scope!=scope||binding.database_identity!=frame.database_identity||binding.principal!="local_configuration"
            ||receipt.binding.request.item_id!=item||!binding.confirmation.bindings.contains(&receipt.binding)
            ||serde_json::from_str::<Vec<String>>(native::value(row,"intent_ids").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?!=receipt.intent_ids
            ||serde_json::from_str::<Vec<String>>(native::value(row,"job_ids").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?!=receipt.job_ids {return Err(ManagementError::Confirmation);}
        if let Some(result)=result {actions.push((binding,result));}
    }
    Ok(actions)
}
