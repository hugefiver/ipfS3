//! Native bindings drive planning; diagnostic projections are never revisions.
use super::*;
use super::super::*;
use crate::store::pinning::management::native::{NativeFrame, NativeRow, value};
use crate::pinning::{identity::ConfirmationRequirement, policy::PublicationContext, tags::ObjectTag};
use sea_orm::DatabaseTransaction;
use std::collections::BTreeSet;
use crate::store::pinning::management::actions::accounting;
#[path = "native_plan/contracts.rs"]
mod contracts;
#[path = "native_plan/replicas.rs"]
mod replicas;
#[path = "native_plan/resources.rs"]
mod resources;

fn table<'a>(frame:&'a NativeFrame,name:&str)->Result<&'a [NativeRow]> { frame.table(name).map(|t| t.rows.as_slice()).ok_or(SnapshotError::InvalidEvidence) }
fn text(row:&NativeRow,column:&str)->Result<String>{value(row,column).map(str::to_owned).ok_or(SnapshotError::InvalidEvidence)}
fn route(raw:&ProviderRouteSnapshot)->RouteIdentity {
    let mut identity=super::route(raw);identity.original=Some(raw.clone());identity
}

fn original_routes(frame:&NativeFrame,config:&ValidatedPinningConfig)->Result<Vec<RouteSnapshot>> {
    let mut result=Vec::new();
    for row in table(frame,"pin_provider_routes")? {
        let Some(json)=value(row,"snapshot") else{continue};
        let Ok(raw)=decode::<ProviderRouteSnapshot>(json) else{continue};
        if super::super::validate::validate_route_identity(&route(&raw)).is_err(){continue;}
        let configured=config.providers.iter().find(|p|p.route_snapshot()==raw);
        let available=configured.is_some() && !raw.provider_id.starts_with("legacy:");
        let retired=matches!(value(row,"retired"),Some("1"|"true"));
        let domain=text(row,"provider")?;
        let barrier=table(frame,"pin_management_retirements")?.iter().any(|r|value(r,"allocation_domain")==Some(&domain));
        result.push(RouteSnapshot {identity:route(&raw),original:Some(raw.clone()),available,allocation_allowed:available && !retired && !barrier && configured.is_some_and(|p|p.limits.enabled&&!p.identity.retired),cleanup:match raw.cleanup {crate::pinning::identity::CleanupMode::Retain=>CleanupMode::Retain,crate::pinning::identity::CleanupMode::Managed=>CleanupMode::Managed},exclusive_management:false});
    }
    // Historical invocation routes are independent of current registration.
    for (name,column) in [("remote_pin_ledger","route"),("pin_invocation_routes","route"),("pin_submit_observations","route"),("pin_replica_observations","route")] {
        for row in table(frame,name)? {
            let Some(json)=value(row,column) else{continue};
            let Ok(raw)=decode::<ProviderRouteSnapshot>(json) else{continue};
            if super::super::validate::validate_route_identity(&route(&raw)).is_err(){continue;}
            if result.iter().any(|r|r.original.as_ref()==Some(&raw)){continue;}
            let available=config.providers.iter().any(|p|p.route_snapshot()==raw) && !raw.provider_id.starts_with("legacy:");
            result.push(RouteSnapshot {identity:route(&raw),original:Some(raw.clone()),available,allocation_allowed:false,cleanup:match raw.cleanup {crate::pinning::identity::CleanupMode::Retain=>CleanupMode::Retain,crate::pinning::identity::CleanupMode::Managed=>CleanupMode::Managed},exclusive_management:false});
        }
    }
    for row in table(frame,"pin_lease_requirements")? {
        let birth:crate::store::entities::pin_lease_requirement::contract::BirthContract=decode(value(row,"contract").ok_or(SnapshotError::InvalidEvidence)?)?;
        for raw in birth.candidates.values().flatten() {
            if result.iter().any(|r|r.identity==route(raw)){continue;}
            let available=config.providers.iter().any(|p|p.route_snapshot()==*raw)&&!raw.provider_id.starts_with("legacy:");
            result.push(RouteSnapshot {identity:route(raw),original:Some(raw.clone()),available,allocation_allowed:false,cleanup:match raw.cleanup {crate::pinning::identity::CleanupMode::Retain=>CleanupMode::Retain,crate::pinning::identity::CleanupMode::Managed=>CleanupMode::Managed},exclusive_management:false});
        }
    }
    for key in frame.frontier.iter().filter(|key|key.table=="pin_resource_history") {
        let Some(raw)=super::super::decision_scope::resource_route(frame,key) else{continue};
        if result.iter().any(|r|r.original.as_ref()==Some(&raw)){continue;}
        let available=config.providers.iter().any(|p|p.route_snapshot()==raw)&&!raw.provider_id.starts_with("legacy:");
        result.push(RouteSnapshot {identity:route(&raw),original:Some(raw.clone()),available,allocation_allowed:false,cleanup:match raw.cleanup {crate::pinning::identity::CleanupMode::Retain=>CleanupMode::Retain,crate::pinning::identity::CleanupMode::Managed=>CleanupMode::Managed},exclusive_management:false});
    }
    result.sort_by(|a,b|a.identity.cmp(&b.identity)); Ok(result)
}

async fn objects(tx:&DatabaseTransaction,diagnostic:&DatabaseSnapshot,frame:&NativeFrame,config:Option<&local_config::ManagementConfig>,request:&PlanRequest)->Result<Vec<ObjectSnapshot>> {
    let now=crate::store::database_clock::database_now(tx).await.map_err(|_|SnapshotError::DatabaseUnavailable)?;
    let mut result=Vec::new();
    for evidence in &diagnostic.objects {
        frame.row("objects",&[&evidence.identity.object_id]).ok_or(SnapshotError::MissingExactObject)?;
        frame.row("object_versions",&[&evidence.identity.private_version_id]).ok_or(SnapshotError::MissingExactObject)?;
        let mut readable=None;let mut read_evidence=None;let mut conflicting_proof=false;
        if let Some(config)=config {
            match accounting::current_proof(frame,config,&evidence.identity,now) {
                Ok(Some(proof))=>{readable=Some(true);read_evidence=Some(ReadEvidence::from(&proof));},
                Ok(None)=>{},
                Err(_)=>{readable=Some(true);conflicting_proof=true;},
            }
        }
        let (mut permitted,allowed_routes)=safety(evidence,read_evidence.as_ref(),config)?;
        if conflicting_proof{permitted=Some(false);}
        if let Some(config)=config {
            for item in &request.items {
                if !matches!(&item.subject,Subject::Object {identity,..} if *identity==evidence.identity) {continue;}
                if let Action::Create(intent)|Action::Reapply(intent)|Action::Backfill(intent)=&item.action {
                    let policy=config.pinning.policies.iter().find(|p|(p.bucket=="*"||p.bucket==evidence.identity.bucket)&&evidence.identity.key.starts_with(&p.prefix));
                    let expected=policy.map(|p|p.providers.iter().filter_map(|name|config.pinning.providers.iter().find(|provider|provider.name==*name).map(|provider|provider.identity.provider_id.clone())).collect::<Vec<_>>());
                    if policy.is_none_or(|p|intent.expires_at.checked_sub(intent.started_at).is_none_or(|duration|duration>p.max_duration.as_seconds() as i64)|| (p.provider_mode==crate::pinning::config::ProviderMode::All)!=(intent.requirements.mode==ProviderMode::All)) || expected.as_ref()!=Some(&intent.requirements.provider_ids) {permitted=Some(false);}
                }
                if let Some(proof)=read_evidence.as_ref().filter(|proof|proof.proof.has_stored_byte_count()) {
                    let count=accounting::count(proof).map_err(|_|SnapshotError::InvalidEvidence)?;
                    let providers:Vec<_>=match &item.action {
                        Action::Create(intent)|Action::Reapply(intent)|Action::Backfill(intent)=>intent.requirements.provider_ids.iter().filter_map(|id|config.pinning.providers.iter().find(|provider|provider.identity.provider_id==*id)).collect(),
                        Action::Migrate {destination}=>config.pinning.providers.iter().filter(|provider|destination.original.as_ref()==Some(&provider.route_snapshot())).collect(),
                        _=>vec![],
                    };
                    if providers.iter().any(|provider|accounting::check_frame(frame,&provider.name,&evidence.identity.cid,count).is_err()){permitted=Some(false);}
                    if count.basis==accounting::Basis::DirectoryDagBlocksV1&&matches!(item.action,Action::Renew {..}|Action::Retry|Action::Migrate {..}) {
                        if let Subject::Object {target:TargetSelection::Existing {target_id,..},..}=&item.subject {
                            let target=frame.row("pin_lease_targets",&[target_id]).ok_or(SnapshotError::InvalidEvidence)?;
                            let provider=value(target,"provider").ok_or(SnapshotError::InvalidEvidence)?;
                            if accounting::check_frame(frame,provider,&evidence.identity.cid,count).is_err(){permitted=Some(false);}
                        }
                    }
                }
            }
        }
        let selection=if let Some(binding)=evidence.manifest_bindings.first() {
            let batch=frame.row("zip_batches",&[&binding.batch_id]).ok_or(SnapshotError::InvalidEvidence)?;
            let entry=frame.row("zip_manifest_entries",&[&binding.batch_id,&binding.entry_path]).ok_or(SnapshotError::InvalidEvidence)?;
            if value(entry,"version_row_id")!=Some(&evidence.identity.private_version_id) || value(entry,"error_code").is_some() || value(batch,"state")!=Some("published") {return Err(SnapshotError::InvalidEvidence);}
            SelectionEvidence::TrustedManifest {batch_id:binding.batch_id.clone(),entry_path:binding.entry_path.clone(),batch_tuple:batch.clone(),entry_tuple:entry.clone()}
        } else {evidence.selection.clone()};
        result.push(ObjectSnapshot {identity:evidence.identity.clone(),logical_bytes:evidence.logical_bytes,readable,read_evidence,permitted,allowed_routes,control_revision:evidence.control_revision.clone(),config_revision:evidence.config_revision.clone(),decision:evidence.decision,evidence:selection});
    }
    Ok(result)
}

fn safety(evidence:&ObjectEvidence,proof:Option<&ReadEvidence>,config:Option<&local_config::ManagementConfig>)->Result<(Option<bool>,Vec<RouteIdentity>)> {
    let Some(config)=config else{return Ok((None,vec![]))};
    // An explicit management intent is evaluated independently; old object tags
    // are never replayed as a grant. Existing PinPolicyEvaluator validates max TTL,
    // decompression/content mode and policy provider eligibility.
    let tags=[ObjectTag::new("ipfs-s3:pin","true")];
    let context=PublicationContext {bucket:&evidence.identity.bucket,key:&evidence.identity.key,tags:&tags,is_decompress_zip:!evidence.manifest_bindings.is_empty()};
    let policy=match config.policy.evaluate_publication(context) {Ok(policy)=>policy,Err(_)=>return Ok((Some(false),vec![]))};
    let providers:BTreeSet<_>=policy.leases.iter().flat_map(|l|l.providers.iter()).collect();
    if !evidence.manifest_bindings.is_empty() {
        let output=crate::pinning::zip_policy::ZipPublishedOutput {bucket:evidence.identity.bucket.clone(),key:evidence.identity.key.clone(),version_id:evidence.identity.private_version_id.clone(),cid:evidence.identity.cid.clone()};
        let plan=config.zip_rules.plan(crate::pinning::zip_policy::ZipTargets {source:false,extracted:true},None,&[output]).map_err(|_|SnapshotError::InvalidEvidence)?;
        if plan.outputs.iter().any(|out|out.warning==Some(crate::pinning::zip_policy::ZipPlanWarning::Denied)) || plan.outputs.is_empty() {return Ok((Some(false),vec![]));}
    }
    let mut allowed=Vec::new();
    for provider in &config.pinning.providers {
        if !providers.contains(&provider.name) || provider.identity.provider_id.starts_with("legacy:") {continue;}
        let raw=provider.route_snapshot();
        // Stored-byte upload needs an actual proof; metadata plaintext size is
        // never substituted. CID/CAR preserve the original encrypted stored CID.
        let byte_safe=proof.is_some_and(|proof|proof.stored_bytes.is_some()&&proof.proof.has_stored_byte_count()&&(!proof.proof.is_directory()||matches!(raw.strategy.as_str(),"cid"|"car"))) && (!evidence.encrypted || matches!(raw.strategy.as_str(),"cid"|"car"|"upload"));
        if byte_safe && matches!(raw.strategy.as_str(),"cid"|"car"|"upload") {allowed.push(route(&raw));}
    }
    Ok((if proof.is_some_and(|proof|proof.stored_bytes.is_some()){Some(!allowed.is_empty())}else{None},allowed))
}

pub(super) async fn project(tx:&DatabaseTransaction,observed_at:i64,diagnostic:&DatabaseSnapshot,frame:NativeFrame,config:&ValidatedPinningConfig,management_config:Option<&local_config::ManagementConfig>,request:&PlanRequest)->Result<Snapshot> {
    let routes=original_routes(&frame,config)?;
    let objects=objects(tx,diagnostic,&frame,management_config,request).await?;
    let mut targets=Vec::new();
    for evidence in &diagnostic.targets {
        let lease=diagnostic.leases.iter().find(|l|l.id==evidence.lease_id).ok_or(SnapshotError::InvalidEvidence)?;
        // Registration is not a historical execution contract. A missing route
        // remains an owned unknown target, not an error for the entire plan.
        let contract=frame.row("pin_lease_requirements",&[&lease.id]).cloned();
        let (registered,requirements,frozen_candidates)=contracts::target(tx,&frame,evidence,lease,contract.as_ref()).await?;
        targets.push(TargetSnapshot {id:evidence.id.clone(),native_tuple:frame.row("pin_lease_targets",&[&evidence.id]).ok_or(SnapshotError::InvalidEvidence)?.clone(),frozen_contract:contract,lease:LeaseSnapshot {id:lease.id.clone(),object_id:lease.object_id.clone(),generation:lease.generation,started_at:lease.started_at,expires_at:lease.expires_at,native_tuple:frame.row("pin_leases",&[&lease.id]).cloned(),state:match lease.state{StoredLeaseState::Active=>LeaseState::Active,StoredLeaseState::Expired=>LeaseState::Expired,_=>LeaseState::Cancelled}},cid:evidence.cid.clone(),logical_bytes:evidence.logical_bytes,route:registered,frozen_candidates,requirements,state:match evidence.state{StoredTargetState::Pinned=>TargetState::Confirmed,StoredTargetState::Waiting|StoredTargetState::Submitted=>TargetState::Pending,StoredTargetState::Degraded=>TargetState::MissingRequirements,_=>TargetState::Blocked}});
    }
    let configuration=Some(ConfigurationBinding {routes:config.providers.iter().map(|p|p.route_snapshot()).collect(),policies:config.policies.clone(),limits:config.provider_limits.iter().map(|(k,v)|(k.clone(),v.clone())).collect(),zip_rules:management_config.map(|c|c.zip_rules.clone()),source_node:management_config.and_then(|c|c.source_node().map(str::to_owned))});
    let mut snapshot=Snapshot {observed_at,objects,routes,targets,configuration,..Snapshot::default()};
    let (mut remotes,historical_remotes,unknown_resources)=resources::project(tx,&frame,diagnostic,request,&snapshot).await?;
    let now=crate::store::database_clock::database_now(tx).await.map_err(|_|SnapshotError::DatabaseUnavailable)?;
    for remote in &mut remotes { replicas::project(tx,&frame,remote,now).await?; }
    let mut ledger_domains=Vec::new();let mut domains=BTreeSet::new();let mut inventories=Vec::new();
    let unattributed=!unknown_resources.is_empty() || diagnostic.leases.iter().any(|l|!l.has_targets) || snapshot.targets.iter().any(|target|target.route.is_none()) || diagnostic.remotes.iter().chain(&diagnostic.historical_remotes).any(|r|r.route.is_none()||r.epoch.is_none())
        || table(&frame,"remote_pin_ledger")?.iter().any(|row|value(row,"provider").zip(value(row,"cid")).is_none_or(|(provider,cid)|frame.original_value("remote_pin_ledger",&[provider,cid],"route").is_none()))
        || table(&frame,"pin_leases")?.iter().any(|lease|!table(&frame,"pin_lease_targets").is_ok_and(|targets|targets.iter().any(|target|value(target,"lease_id")==value(lease,"id"))));
    for route in &snapshot.routes {
        if domains.insert((route.identity.backend.clone(),route.identity.scope.clone(),route.identity.resource_type)) {
            ledger_domains.push(LedgerDomainSnapshot {backend:route.identity.backend.clone(),scope:route.identity.scope.clone(),resource_type:route.identity.resource_type,frontier:frame.frontier.clone(),complete:true});
        }
        if inventories.iter().any(|i:&ProviderInventory|i.provider_id==route.identity.provider_id){continue;}
        let leases=table(&frame,"pin_leases")?.iter().map(|row|Ok(LeaseSnapshot {id:text(row,"id")?,object_id:text(row,"owner_object_id")?,generation:text(row,"generation")?.parse().map_err(|_|SnapshotError::InvalidEvidence)?,started_at:crate::store::pinning::management::native::parse_time(value(row,"created_at").ok_or(SnapshotError::InvalidEvidence)?).map_err(|_|SnapshotError::InvalidEvidence)?.timestamp(),expires_at:crate::store::pinning::management::native::parse_time(value(row,"expires_at").ok_or(SnapshotError::InvalidEvidence)?).map_err(|_|SnapshotError::InvalidEvidence)?.timestamp(),native_tuple:Some(row.clone()),state:match value(row,"state"){Some("active")=>LeaseState::Active,Some("expired")=>LeaseState::Expired,Some("cancelled"|"evicted")=>LeaseState::Cancelled,_=>LeaseState::Unknown}})).collect::<Result<Vec<_>>>()?;
        let root_reference_ids=frame.frontier.iter().filter(|key|key.table=="zip_root_references").cloned().collect::<Vec<_>>();
        inventories.push(ProviderInventory {provider_id:route.identity.provider_id.clone(),frontier:frame.frontier.clone(),complete:true,attribution_complete:!unattributed&&root_reference_ids.is_empty(),allocation_stopped:!route.allocation_allowed,leases,root_reference_ids});
    }
    snapshot.database_now=Some(crate::store::database_clock::database_now(tx).await.map_err(|_|SnapshotError::DatabaseUnavailable)?);
    snapshot.remotes=remotes;snapshot.historical_remotes=historical_remotes;snapshot.unknown_resources=unknown_resources;
    snapshot.ledger_domains=ledger_domains;snapshot.inventories=inventories;snapshot.native_frame=Some(frame);
    Ok(snapshot)
}
