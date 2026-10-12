//! R26 read-only typed service. Original record PK/full row and declared domains
//! are mandatory. No opaque string search or namespace-wide execution grant.
use super::*;
use crate::pinning::{identity::{ProviderRouteSnapshot,RemoteResourceType},management::{Capture,RouteSnapshot}};
use sea_orm::{ConnectionTrait,Statement};

#[derive(Clone,PartialEq,Eq)]
pub struct Domain {pub allocation:String,pub backend:String,pub scope:String,pub resource_type:RemoteResourceType}
pub struct NamespaceChange {pub source:Domain,pub destination:Domain,pub destination_route:ProviderRouteSnapshot}
#[derive(Clone,Copy,PartialEq,Eq)]
pub enum RecordClass {Identity,Evidence,Staged,AppliedFoundation,Applied,Retirement}
#[derive(Clone,PartialEq,Eq)]
pub enum Locator {
    SnapshotRoute {container:&'static str,item:String,field:String},
    NativeColumn {container:&'static str,key:native::NativeKey,column:&'static str},
    BirthCandidate {container:&'static str,key:native::NativeKey,slot:String},
    Job {job_id:String},
    Migration {contract:native::NativeKey,slot:String,destination:bool},
}
#[derive(Clone,PartialEq,Eq)]
pub enum Evidence {Historical,Staged,ExecutableJob(records::JobDirective),ExecutableSlot,Unresolved}
#[derive(Clone,PartialEq,Eq)]
pub struct ProviderPoint {
    pub locator:Locator,pub allocation:String,pub original_route:Option<ProviderRouteSnapshot>,pub evidence:Evidence,
    pub lease_id:Option<String>,pub lease_generation:Option<i64>,pub cid:Option<String>,pub resource_key:Option<native::NativeKey>,pub epoch:Option<i64>,pub resource_id:Option<String>,
    pub job_authority:Option<records::JobAuthority>,pub migration_point:Option<records::MigrationPoint>,
}
/// No Deserialize/public constructor: caller data cannot create a verified view.
pub struct VerifiedRecord {
    key:native::NativeKey,scope:String,identity:String,class:RecordClass,points:Vec<ProviderPoint>,origins:Vec<(native::NativeKey,native::NativeRow)>,
}
impl VerifiedRecord {
    pub fn record_key(&self)->&native::NativeKey{&self.key}
    pub fn database_scope(&self)->&str{&self.scope}
    pub fn database_identity(&self)->&str{&self.identity}
    pub fn class(&self)->RecordClass{self.class}
    pub fn points(&self)->&[ProviderPoint]{&self.points}
    /// Exact structured original images for the R26-owned typed adapter. These
    /// remain private DB data; they are not CLI serialization or executable work.
    pub fn native_origins(&self)->&[(native::NativeKey,native::NativeRow)]{&self.origins}
}

pub async fn inspect<C:ConnectionTrait>(db:&C,key:&native::NativeKey,original:&native::NativeRow,namespace:&NamespaceChange)->Result<VerifiedRecord> {
    validate_namespace(namespace)?;
    let expected=original_record(db,key).await?.ok_or(ManagementError::InvalidEvidence)?;
    if &expected!=original{return Err(ManagementError::Changed);}
    let identity=native::database_identity(db).await?;
    if identity.starts_with("diagnostic:"){return Err(ManagementError::Unauthorized);}
    let scope=db.query_one(Statement::from_string(db.get_database_backend(),"SELECT database_scope FROM pin_management_records WHERE kind='database_scope'"))
        .await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Schema)?.try_get::<String>("","database_scope").map_err(|_|ManagementError::Schema)?;
    if native::value(original,"database_scope")!=Some(&scope){return Err(ManagementError::Unauthorized);}
    if native::value(original,"kind")!=Some("database_scope")&&(native::value(original,"native_identity")!=Some(&identity)||native::value(original,"principal")!=Some("local_configuration")){return Err(ManagementError::Unauthorized);}
    let mut view=VerifiedRecord {key:key.clone(),scope,identity,class:RecordClass::Identity,points:vec![],origins:vec![]};
    match records::inspect_original_row(&key.table,original)? {
        records::RecordPayload::Identity=>{},
        records::RecordPayload::Evidence {attestation,original_object,original_version}=>{
            view.class=RecordClass::Evidence;
            view.origins.push((native::NativeKey {table:"objects".into(),values:vec![attestation.identity.object_id]},original_object));
            view.origins.push((native::NativeKey {table:"object_versions".into(),values:vec![attestation.identity.private_version_id]},original_version));
        },
        records::RecordPayload::Plan {binding,snapshot,action,applied}=>{
            if binding.database_scope!=view.scope||binding.database_identity!=view.identity{return Err(ManagementError::Unauthorized);}
            view.class=if !applied{RecordClass::Staged}else if action.is_some(){RecordClass::Applied}else{RecordClass::AppliedFoundation};
            let frame=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::InvalidEvidence)?;
            native_points(&mut view,frame,"binding",namespace)?;
            if let Some(before)=snapshot.native_frame.as_ref(){native_points(&mut view,before,"native_before",namespace)?;}
            snapshot_points(&mut view,&binding,namespace)?;
            if let Some(action)=action {
                for job in &action.jobs {
                    job_authority::validate_stored(&binding,&action,job)?;
                    if job.provider!=namespace.source.allocation{continue;}
                    route_facts(&job.route,namespace)?;
                    use sea_orm::EntityTrait;
                    let current=crate::store::entities::pin_job::Entity::find_by_id(job.job_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?;
                    let executable=if let Some(current)=current&&matches!(current.state.as_str(),"pending"|"running"|"retry") {matches!(job_authority::check_before_call(db,&current).await,Ok(Some(job_authority::JobPermit::Submit(_)|job_authority::JobPermit::ReadOnly(_)|job_authority::JobPermit::ExistingCleanup(_))))}else{false};
                    view.points.push(ProviderPoint {locator:Locator::Job {job_id:job.job_id.clone()},allocation:job.provider.clone(),original_route:Some(job.route.clone()),evidence:if executable{Evidence::ExecutableJob(job.directive)}else{Evidence::Historical},lease_id:job.lease_id.clone(),lease_generation:job.lease_generation,cid:Some(crate::pinning::provider::canonical_resource_cid(&job.cid).map_err(|_|ManagementError::InvalidEvidence)?),resource_key:Some(job.resource_key.clone()),epoch:Some(job.remote_epoch),resource_id:job.resource_id.clone(),job_authority:Some(job.clone()),migration_point:None});
                }
                for point in &action.migration_points {
                    points::validate_original(&binding,&action,point)?;
                    for destination in [false,true] {
                        let (allocation,route,resource_key,epoch,resource_id)=if destination {(&point.destination_allocation,&point.destination_route,&point.destination_resource_key,point.destination_epoch,&point.destination_resource_id)}else{(&point.source_allocation,&point.source_resource_route,&point.source_resource_key,point.source_epoch,&point.source_resource_id)};
                        if allocation!=&namespace.source.allocation{continue;}route_facts(route,namespace)?;
                        let live=points::resolved_slot(db,&point.lease_id,&point.cid,&point.slot).await?.is_some_and(|resolved|resolved.record_key==*key);
                        use sea_orm::EntityTrait;
                        let lease=crate::store::entities::pin_lease::Entity::find_by_id(point.lease_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?;
                        let now=crate::store::database_clock::database_now(db).await.map_err(|_|ManagementError::Database)?;
                        let live=live&&lease.is_some_and(|lease|lease.state=="active"&&lease.expires_at>now);
                        view.points.push(ProviderPoint {locator:Locator::Migration {contract:point.primary_key.clone(),slot:point.slot.clone(),destination},allocation:allocation.clone(),original_route:Some(route.clone()),evidence:if live{Evidence::ExecutableSlot}else{Evidence::Historical},lease_id:Some(point.lease_id.clone()),lease_generation:Some(point.source_generation),cid:Some(point.cid.clone()),resource_key:Some(resource_key.clone()),epoch:Some(epoch),resource_id:resource_id.clone(),job_authority:None,migration_point:Some(point.clone())});
                    }
                }
            }
        },
        records::RecordPayload::Retirement {binding,inventory}=>{
            if binding.database_scope!=view.scope||binding.database_identity!=view.identity{return Err(ManagementError::Unauthorized);}
            let action_key=native::NativeKey {table:super::super::schema::RECORDS.into(),values:vec![view.scope.clone(),binding.request.plan_id.clone(),native::value(original,"item_id").ok_or(ManagementError::InvalidEvidence)?.into(),"action".into()]};
            let row=original_record(db,&action_key).await?.ok_or(ManagementError::Confirmation)?;
            let records::RecordPayload::Plan {binding:stored,applied:true,..}=records::inspect_original_row(&action_key.table,&row)? else{return Err(ManagementError::Confirmation)};
            if stored!=binding{return Err(ManagementError::Confirmation);}
            view.class=RecordClass::Retirement;native_points(&mut view,&inventory,"inventory",namespace)?;snapshot_points(&mut view,&binding,namespace)?;
        },
    }
    let mut origins=Vec::new();for origin in view.origins {if !origins.contains(&origin){origins.push(origin);}}view.origins=origins;
    if view.points.len()>native::MAX_ROWS||view.origins.len()>native::MAX_ROWS{return Err(ManagementError::Budget);}Ok(view)
}

pub async fn original_record<C:ConnectionTrait>(db:&C,key:&native::NativeKey)->Result<Option<native::NativeRow>> {
    if !matches!(key.table.as_str(),super::super::schema::RECORDS|super::super::schema::RETIREMENTS){return Err(ManagementError::InvalidEvidence);}
    let table=native::registry().into_iter().find(|table|table.table==key.table).ok_or(ManagementError::Schema)?;
    if table.primary_key.len()!=key.values.len(){return Err(ManagementError::InvalidEvidence);}
    let fields=table.columns.iter().map(|column|format!("CAST(\"{column}\" AS TEXT) AS \"{column}\"")).collect::<Vec<_>>().join(",");
    let predicate=table.primary_key.iter().enumerate().map(|(index,column)|format!("\"{column}\"=${}",index+1)).collect::<Vec<_>>().join(" AND ");
    let row=db.query_one(Statement::from_sql_and_values(db.get_database_backend(),format!("SELECT {fields} FROM \"{}\" WHERE {predicate}",table.table),key.values.iter().cloned().map(Into::into).collect::<Vec<sea_orm::Value>>())).await.map_err(|_|ManagementError::Database)?;
    let Some(row)=row else{return Ok(None)};let mut original=native::NativeRow::new();let mut bytes=0usize;
    for column in table.columns {let value=row.try_get::<Option<String>>("",&column).map_err(|_|ManagementError::InvalidEvidence)?;bytes=bytes.checked_add(value.as_ref().map_or(0,String::len)).ok_or(ManagementError::Budget)?;if bytes>native::MAX_BYTES{return Err(ManagementError::Budget);}original.insert(column,value);}
    Ok(Some(original))
}

fn validate_namespace(namespace:&NamespaceChange)->Result<()> {
    let destination=&namespace.destination;let route=&namespace.destination_route;
    // R26 v1's source is an exact historical SQL coordinate, not a newly
    // configured provider identifier. Preserve its nonempty/1024-byte bound.
    let source=&namespace.source.allocation;
    if source.is_empty()||source.len()>1024
        ||[&namespace.source.backend,&namespace.source.scope,&destination.backend,&destination.scope].iter().any(|value|value.is_empty()||value.len()>255||value.chars().any(char::is_control))
        ||destination.backend!=route.backend||destination.scope!=route.scope||destination.resource_type!=route.resource_type()
        ||namespace.source.backend!=destination.backend||namespace.source.resource_type!=destination.resource_type||namespace.source.scope!=destination.scope&&!namespace.source.scope.starts_with("legacy:") {return Err(ManagementError::InvalidEvidence);}
    let destination_valid=if destination.allocation.starts_with("domain:") {
        destination.allocation==format!("domain:{}",serde_json::to_string(&(&route.backend,&route.scope,route.resource_type())).map_err(|_|ManagementError::InvalidEvidence)?)
    }else {
        !destination.allocation.is_empty()&&destination.allocation.len()<=255&&!destination.allocation.chars().any(char::is_control)
    };
    if !destination_valid {return Err(ManagementError::InvalidEvidence);}Ok(())
}
fn route_facts(route:&ProviderRouteSnapshot,namespace:&NamespaceChange)->Result<()> {
    if route.backend!=namespace.source.backend||route.resource_type()!=namespace.source.resource_type||route.scope!=namespace.source.scope&&!route.scope.starts_with("legacy:"){return Err(ManagementError::InvalidEvidence);}Ok(())
}
fn add_route(view:&mut VerifiedRecord,locator:Locator,allocation:String,route:Option<ProviderRouteSnapshot>,namespace:&NamespaceChange)->Result<()> {
    if allocation!=namespace.source.allocation{return Ok(());}if let Some(route)=&route{route_facts(route,namespace)?;}
    let evidence=if route.is_none(){Evidence::Unresolved}else if view.class==RecordClass::Staged{Evidence::Staged}else{Evidence::Historical};
    view.points.push(ProviderPoint {locator,allocation,original_route:route,evidence,lease_id:None,lease_generation:None,cid:None,resource_key:None,epoch:None,resource_id:None,job_authority:None,migration_point:None});Ok(())
}

fn snapshot_points(view:&mut VerifiedRecord,binding:&records::ConfirmedBinding,namespace:&NamespaceChange)->Result<()> {
    let frame=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::InvalidEvidence)?;
    for entry in &binding.confirmation.bindings {
        let item=entry.request.item_id.clone();let mut routes=Vec::<(String,&RouteSnapshot)>::new();
        match &entry.capture {
            Some(Capture::Object(capture))=>{
                routes.push(("source".into(),&capture.route));
                if let Some(route)=&capture.remote_route{routes.push(("remote_source".into(),route));}
                if let Some(route)=&capture.destination_route{routes.push(("destination".into(),route));}
                if let Some(route)=&capture.destination_remote_route{routes.push(("remote_destination".into(),route));}
                for (index,route) in capture.requirement_routes.iter().enumerate(){routes.push((format!("requirements/{index}"),route));}
                for (index,route) in capture.historical_routes.iter().enumerate(){routes.push((format!("history/{index}"),route));}
            },
            Some(Capture::Provider(capture))=>for (index,route) in capture.routes.iter().enumerate(){routes.push((format!("provider/{index}"),route));},
            None=>return Err(ManagementError::InvalidEvidence),
        }
        for (field,route) in routes {
            let allocations=frame.table("pin_provider_routes").ok_or(ManagementError::Schema)?.rows.iter().filter(|row|native::value(row,"snapshot").and_then(|encoded|serde_json::from_str::<ProviderRouteSnapshot>(encoded).ok()).as_ref()==route.original.as_ref()).filter_map(|row|native::value(row,"provider")).collect::<Vec<_>>();
            for allocation in allocations {add_route(view,Locator::SnapshotRoute {container:"binding",item:item.clone(),field:field.clone()},allocation.into(),route.original.clone(),namespace)?;}
        }
    }Ok(())
}

fn native_points(view:&mut VerifiedRecord,frame:&native::NativeFrame,container:&'static str,namespace:&NamespaceChange)->Result<()> {
    frame.validate()?;
    for table in &frame.tables {
        for projected in &table.rows {
            let values=table.primary_key.iter().map(|column|native::value(projected,column).map(str::to_owned).ok_or(ManagementError::InvalidEvidence)).collect::<Result<Vec<_>>>()?;
            let refs=values.iter().map(String::as_str).collect::<Vec<_>>();
            let row=frame.original_row(&table.table,&refs).ok_or(ManagementError::InvalidEvidence)?;
            let key=native::NativeKey {table:table.table.clone(),values};
            if matches!(table.table.as_str(),"pin_management_records"|"pin_management_retirements"){continue;}
            // Only known provider columns/captured route types, never substring
            // searching opaque framework material or issuing permits from it.
            if native::value(&row,"provider")==Some(&namespace.source.allocation) {
                let route=match table.table.as_str() {
                    "pin_provider_routes"=>native::value(&row,"snapshot").map(decode_route).transpose()?,
                    "remote_pin_ledger"|"pin_submit_observations"|"pin_replica_observations"=>native::value(&row,"route").map(decode_route).transpose()?,
                    "pin_jobs"=>native::value(&row,"id").and_then(|id|frame.row("pin_invocation_routes",&[id])).and_then(|row|native::value(row,"route")).map(decode_route).transpose()?,
                    "remote_pins"=>native::value(&row,"cid").and_then(|cid|frame.row("remote_pin_ledger",&[&namespace.source.allocation,cid])).and_then(|row|native::value(row,"route")).map(decode_route).transpose()?,
                    "pin_resource_history"=>{
                        let envelope:ResourceHistory=serde_json::from_str(native::value(&row,"ledger").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?;
                        envelope.ledger.route.as_deref().map(decode_route).transpose()?
                    },
                    _=>None,
                };
                add_route(view,Locator::NativeColumn {container,key:key.clone(),column:"provider"},namespace.source.allocation.clone(),route,namespace)?;
                let point=view.points.last_mut().ok_or(ManagementError::InvalidEvidence)?;
                point.lease_id=native::value(&row,"lease_id").map(str::to_owned);point.lease_generation=native::value(&row,"expected_generation").and_then(|value|value.parse().ok());
                point.cid=native::value(&row,"cid").map(crate::pinning::provider::canonical_resource_cid).transpose().map_err(|_|ManagementError::InvalidEvidence)?;
                if matches!(table.table.as_str(),"remote_pins"|"remote_pin_ledger"|"pin_resource_history"){point.resource_key=Some(key.clone());}
                point.epoch=native::value(&row,"epoch").or_else(||native::value(&row,"remote_epoch")).and_then(|value|value.parse().ok());point.resource_id=native::value(&row,"request_id").map(str::to_owned);
                view.origins.push((key.clone(),row.clone()));
            }
            if table.table=="pin_lease_requirements" {
                let contract:crate::store::entities::pin_lease_requirement::contract::BirthContract=serde_json::from_str(native::value(&row,"contract").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?;
                for (slot,route) in contract.candidates {if slot!=namespace.source.allocation{continue;}add_route(view,Locator::BirthCandidate {container,key:key.clone(),slot:slot.clone()},slot,route,namespace)?;view.origins.push((key.clone(),row.clone()));}
            }
            if table.table=="pin_invocation_routes"&&let Some(id)=native::value(&row,"job_id")&&frame.row("pin_jobs",&[id]).is_some_and(|job|native::value(job,"provider")==Some(&namespace.source.allocation)) {
                add_route(view,Locator::NativeColumn {container,key:key.clone(),column:"route"},namespace.source.allocation.clone(),native::value(&row,"route").map(decode_route).transpose()?,namespace)?;view.origins.push((key,row));
            }
        }
    }Ok(())
}
fn decode_route(encoded:&str)->Result<ProviderRouteSnapshot>{serde_json::from_str(encoded).map_err(|_|ManagementError::InvalidEvidence)}
#[derive(serde::Deserialize)]
struct ResourceHistory {ledger:HistoricalLedger}
#[derive(serde::Deserialize)]
struct HistoricalLedger {route:Option<String>}
