//! Exact logical-slot resolver. Never a provider-wide alias and never a rewrite
//! of the original frozen contract, invocation or resource-history JSON.
use super::*;
use crate::{pinning::provider::canonical_resource_cid,store::entities::{pin_lease,pin_lease_target,remote_pin,pin_lease_requirement}};
use sea_orm::{ConnectionTrait,DatabaseTransaction,EntityTrait};

pub async fn resolve_slot<C:ConnectionTrait>(db:&C,lease_id:&str,cid:&str,slot:&str)->Result<Option<records::MigrationPoint>> {
    Ok(resolved_slot(db,lease_id,cid,slot).await?.map(|resolved|resolved.point))
}

#[derive(Clone,PartialEq,Eq)]
pub struct ResolvedSlot {pub point:records::MigrationPoint,pub record_key:native::NativeKey,pub allocation:String,pub resource_key:native::NativeKey,pub execution_route:crate::pinning::identity::ProviderRouteSnapshot}

pub async fn resolved_slot<C:ConnectionTrait>(db:&C,lease_id:&str,cid:&str,slot:&str)->Result<Option<ResolvedSlot>> {
    let cid=canonical_resource_cid(cid).map_err(|_|ManagementError::InvalidEvidence)?;
    let mut nodes=std::collections::BTreeMap::new();
    for (binding,action) in records::actions_for_lease(db,lease_id).await? {
        for point in &action.migration_points {
            if point.lease_id!=lease_id || point.cid!=cid || point.slot!=slot {continue;}
            validate_original(&binding,&action,point)?;
            let key=record_key(&binding,&action);
            if nodes.insert(key,point.clone()).is_some(){return Err(ManagementError::InvalidEvidence);}
        }
    }
    if nodes.is_empty(){return Ok(None);}
    let parents=nodes.values().filter_map(|point|point.predecessor.clone()).collect::<std::collections::BTreeSet<_>>();
    if parents.len()!=nodes.values().filter(|point|point.predecessor.is_some()).count() {return Err(ManagementError::InvalidEvidence);}
    let tips=nodes.keys().filter(|key|!parents.contains(*key)).cloned().collect::<Vec<_>>();
    if tips.len()!=1{return Err(ManagementError::InvalidEvidence);}
    let key=tips[0].clone();let mut next=Some(key.clone());let mut visited=std::collections::BTreeSet::new();
    while let Some(current)=next {
        if !visited.insert(current.clone())||visited.len()>32{return Err(ManagementError::Budget);}
        let point=nodes.get(&current).ok_or(ManagementError::InvalidEvidence)?;next=point.predecessor.clone();
    }
    if visited.len()!=nodes.len(){return Err(ManagementError::InvalidEvidence);}
    let point=nodes.remove(&key).ok_or(ManagementError::InvalidEvidence)?;
    let (allocation,execution_route)=validate_current(db,&point).await?;
    let resource_key=native::NativeKey {table:point.destination_resource_key.table.clone(),values:vec![allocation.clone(),point.destination_resource_key.values[1].clone()]};
    Ok(Some(ResolvedSlot {point,record_key:key,allocation,resource_key,execution_route}))
}

/// CID-specific ordered slots. `frozen_candidates`' old lease-only call cannot
/// collapse per-CID migrations into a global provider replacement.
pub async fn candidates_for_cid<C:ConnectionTrait>(db:&C,lease_id:&str,cid:&str,birth:&pin_lease_requirement::contract::BirthContract)->Result<Vec<(String,String,Option<records::MigrationPoint>)>> {
    let key=[("lease_id".into(),lease_id.to_owned())].into();
    let mut result=Vec::new();let mut allocations=std::collections::BTreeSet::new();
    for slot in &birth.candidate_order {
        let resolved=resolved_slot(db,lease_id,cid,slot).await?;
        let allocation=if let Some(point)=&resolved {point.allocation.clone()}else {
            crate::store::pinning::legacy_migration::captured_provider(db,"pin_lease_requirements",&key,slot).await.map_err(database)?
        };
        if !allocations.insert(allocation.clone()) {return Err(ManagementError::InvalidEvidence);}
        result.push((slot.clone(),allocation,resolved.map(|resolved|resolved.point)));
    }
    Ok(result)
}

pub(crate) fn planning_points(frame:&native::NativeFrame)->Result<Vec<records::MigrationPoint>> {
    let mut points=Vec::new();
    for (binding,action) in records::frame_actions(frame)? {
        for point in &action.migration_points {validate_original(&binding,&action,point)?;points.push((record_key(&binding,&action),point.clone()));}
    }
    let mut points=latest_points(points)?;
    for point in &mut points {
        let key=[("id".into(),point.destination_target_id.clone())].into();
        point.planning_projection=movement_in_frame(frame,"pin_lease_targets",&key,&point.destination_allocation,&point.destination_route)?;
    }
    Ok(points)
}

pub(crate) fn project_target(frame:&native::NativeFrame,target:&mut crate::pinning::management::TargetSnapshot,points:&[records::MigrationPoint])->Result<()> {
        for point in points {
            if point.lease_id!=target.lease.id||point.cid!=target.cid {continue;}
            if frame.row("pin_lease_requirements",&[&point.lease_id])!=Some(&point.original_contract) {return Err(ManagementError::Changed);}
            let destination=frame.row("pin_lease_targets",&[&point.destination_target_id]).ok_or(ManagementError::Changed)?;
            let key=[("id".into(),point.destination_target_id.clone())].into();
            let movement=if frame.has_private_material() {
                let original=movement_in_frame(frame,"pin_lease_targets",&key,&point.destination_allocation,&point.destination_route)?;
                if original!=point.planning_projection{return Err(ManagementError::Changed);}original
            }else{point.planning_projection.clone()};
            if let Some(projection)=&movement {
                if projection.legacy_record.table!="pin_legacy_migrations"||projection.legacy_record.values.len()!=1||frame.row("pin_legacy_migrations",&[&projection.legacy_record.values[0]]).is_none(){return Err(ManagementError::InvalidEvidence);}
            }
            let allocation=movement.as_ref().map_or_else(||point.destination_allocation.clone(),|projection|projection.allocation.clone());
            let execution_route=movement.map_or_else(||point.destination_route.clone(),|projection|projection.execution_route);
            if native::value(destination,"lease_id")!=Some(&point.lease_id)||native::value(destination,"provider")!=Some(&allocation)
                ||native::value(destination,"cid").and_then(|cid|canonical_resource_cid(cid).ok()).as_deref()!=Some(&point.cid) {return Err(ManagementError::Changed);}
            let candidate=target.frozen_candidates.iter_mut().find(|candidate|candidate.slot==point.slot).ok_or(ManagementError::InvalidEvidence)?;
            candidate.allocation=allocation;
            candidate.receipt_id=Some(format!("management-point:{}",point.source_target_id));
            if target.id==point.destination_target_id {
                target.route=Some(crate::pinning::management::RouteIdentity::from_original(execution_route));
                if target.requirements.is_none() {
                    let birth:pin_lease_requirement::contract::BirthContract=serde_json::from_str(native::value(&point.original_contract,"contract").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?;
                    let encoded:serde_json::Value=serde_json::from_str(native::value(&point.original_contract,"contract").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?;
                    if let Some(full)=encoded.get("management_requirements") {
                        target.requirements=Some(serde_json::from_value(full.clone()).map_err(|_|ManagementError::InvalidEvidence)?);
                    } else {
                        use crate::pinning::management::{Requirements,ProviderMode,ConfirmationLevel};
                        let birth_route=birth.candidates.get(&point.slot).and_then(Option::as_ref).ok_or(ManagementError::InvalidEvidence)?;
                        let (min_replicas,max_age_secs,level)=match birth_route.confirmation {
                            Some(crate::pinning::identity::ConfirmationRequirement::ClusterReplicas {min_replicas,max_age_secs})=>(min_replicas,Some(max_age_secs),ConfirmationLevel::ReplicationComplete),
                            None=>(1,None,ConfirmationLevel::RemotePinned),
                        };
                        target.requirements=Some(Requirements {mode:if birth.provider_mode=="all"{ProviderMode::All}else{ProviderMode::One},provider_ids:birth.candidate_order.iter().map(|slot|birth.candidates.get(slot).and_then(Option::as_ref).map(|route|route.provider_id.clone()).ok_or(ManagementError::InvalidEvidence)).collect::<Result<Vec<_>>>()?,levels:vec![level],min_replicas,max_age_secs,policy_revision:native::value(target.lease.native_tuple.as_ref().ok_or(ManagementError::InvalidEvidence)?,"policy_id").ok_or(ManagementError::InvalidEvidence)?.into()});
                    }
                }
            }
    }
    Ok(())
}

pub(crate) async fn validate_birth(tx:&DatabaseTransaction,binding:&records::ConfirmedBinding,action:&records::CommittedAction)->Result<()> {
    let mut coordinates=std::collections::BTreeSet::new();
    for point in &action.migration_points {
        if !coordinates.insert((&point.lease_id,&point.cid,&point.slot)) {return Err(ManagementError::InvalidEvidence);}
        validate_original(binding,action,point)?;
            validate_current(tx,point).await?;
    }
    Ok(())
}

pub(crate) fn validate_original(binding:&records::ConfirmedBinding,action:&records::CommittedAction,point:&records::MigrationPoint)->Result<()> {
    validate_chain_original(binding,action,point,0)
}
fn validate_chain_original(binding:&records::ConfirmedBinding,action:&records::CommittedAction,point:&records::MigrationPoint,depth:usize)->Result<()> {
    if depth>=32{return Err(ManagementError::Budget);}
    if point.planning_projection.is_some(){return Err(ManagementError::Confirmation);}
    use crate::pinning::management::{Action,Capture};
    let entry=&action.receipt.binding;
    let (Action::Migrate {destination},Some(Capture::Object(capture)))=(&entry.request.action,&entry.capture) else{return Err(ManagementError::Confirmation)};
    let target=capture.target.as_ref().ok_or(ManagementError::Confirmation)?;
    let before=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    if point.source_resource_key.table!="remote_pins"||point.source_resource_key.values.len()!=2||point.source_resource_key.values[0]!=point.source_allocation||point.destination_resource_key.table!="remote_pins"||point.destination_resource_key.values.len()!=2||point.destination_resource_key.values[0]!=point.destination_allocation{return Err(ManagementError::Confirmation);}
    let remote=before.row("remote_pins",&[&point.source_resource_key.values[0],&point.source_resource_key.values[1]]).ok_or(ManagementError::InvalidEvidence)?;
    if point.table!="pin_lease_requirements" || point.primary_key!= (native::NativeKey {table:point.table.clone(),values:vec![target.lease.id.clone()]})
        || point.capture_position!=format!("candidates/{}",point.slot)
        || before.row("pin_lease_requirements",&[&target.lease.id])!=Some(&point.original_contract)
        || point.lease_id!=target.lease.id || point.source_target_id!=target.id || point.cid!=target.cid
        || native::value(&target.native_tuple,"provider")!=Some(&point.source_allocation)
        || native::value(remote,"epoch").and_then(|epoch|epoch.parse::<i64>().ok())!=Some(point.source_epoch)
        || native::value(remote,"request_id")!=point.source_resource_id.as_deref()
        || capture.route.original.as_ref()!=Some(&point.source_route) || destination.original.as_ref()!=Some(&point.destination_route)
        ||capture.remote_route.as_ref().and_then(|route|route.original.as_ref())!=Some(&point.source_resource_route)
        ||before.row("remote_pin_ledger",&[&point.source_resource_key.values[0],&point.source_resource_key.values[1]]).and_then(|row|native::value(row,"route")).and_then(|encoded|serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(encoded).ok()).as_ref()!=Some(&point.source_resource_route)
        ||before.row("pin_leases",&[&point.lease_id])!=Some(&point.source_lease)||point.source_generation!=target.lease.generation
        ||native::value(&point.source_lease,"generation").and_then(|value|value.parse().ok())!=Some(point.source_generation)
        ||native::value(&point.source_lease,"created_at").and_then(|value|native::parse_time(value).ok())!=Some(point.started_at)
        ||native::value(&point.source_lease,"expires_at").and_then(|value|native::parse_time(value).ok())!=Some(point.expires_at)
        ||native::value(&target.native_tuple,"cid")!=Some(&point.source_resource_key.values[1])
        ||canonical_resource_cid(&point.destination_resource_key.values[1]).ok().as_deref()!=Some(&point.cid)
        || !action.targets.contains(&point.destination_target_id) || !action.receipt.intent_ids.contains(&point.lease_id)
    {return Err(ManagementError::Confirmation);}
    let contract:pin_lease_requirement::contract::BirthContract=serde_json::from_str(native::value(&point.original_contract,"contract").ok_or(ManagementError::InvalidEvidence)?).map_err(|_|ManagementError::InvalidEvidence)?;
    if let Some(key)=&point.predecessor {
        let values=key.values.iter().map(String::as_str).collect::<Vec<_>>();
        let row=before.original_row(&key.table,&values).ok_or(ManagementError::Confirmation)?;
        let (old_binding,old_action)=records::action_from_original_row(&binding.database_scope,&binding.database_identity,&row)?.ok_or(ManagementError::Confirmation)?;
        if key!=&record_key(&old_binding,&old_action){return Err(ManagementError::Confirmation);}
        let previous=old_action.migration_points.iter().filter(|old|old.lease_id==point.lease_id&&old.cid==point.cid&&old.slot==point.slot).collect::<Vec<_>>();
        if previous.len()!=1{return Err(ManagementError::Confirmation);}let previous=previous[0];
        validate_chain_original(&old_binding,&old_action,previous,depth+1)?;
        let original_key=[("id".into(),previous.destination_target_id.clone())].into();
        let movement=movement_in_frame(before,"pin_lease_targets",&original_key,&previous.destination_allocation,&previous.destination_route)?;
        let allocation=movement.as_ref().map_or_else(||previous.destination_allocation.clone(),|projection|projection.allocation.clone());
        let route=movement.map_or_else(||previous.destination_route.clone(),|projection|projection.execution_route);
        if previous.original_contract!=point.original_contract||previous.destination_target_id!=point.source_target_id||allocation!=point.source_allocation||route!=point.source_route
            ||previous.started_at!=point.started_at||previous.expires_at>point.expires_at||previous.source_generation>point.source_generation||previous.destination_epoch>point.source_epoch
            ||["id","owner_object_id","source","policy_id","provider_mode","content_mode","created_at"].iter().any(|column|native::value(&previous.source_lease,column)!=native::value(&point.source_lease,column)){return Err(ManagementError::Confirmation);}
    }else if contract.candidates.get(&point.slot).and_then(Option::as_ref)!=Some(&point.source_route) {return Err(ManagementError::Confirmation);}
    // Explicit route movement never weakens the Cluster birth threshold. The
    // consumer evaluates the original full contract against destination facts.
    if matches!(point.source_route.resource_type(),crate::pinning::identity::RemoteResourceType::ClusterPin) && point.source_route.confirmation.is_none() {return Err(ManagementError::InvalidEvidence);}
    Ok(())
}

async fn validate_current<C:ConnectionTrait>(db:&C,point:&records::MigrationPoint)->Result<(String,crate::pinning::identity::ProviderRouteSnapshot)> {
    let contract=pin_lease_requirement::Entity::find_by_id(point.lease_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    if native::value(&point.original_contract,"contract")!=Some(contract.contract.as_str()) {return Err(ManagementError::Changed);}
    let lease=pin_lease::Entity::find_by_id(point.lease_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    let target=pin_lease_target::Entity::find_by_id(point.destination_target_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    let key=[("id".into(),point.destination_target_id.clone())].into();
    let (allocation,execution_route)=if let Some(binding)=crate::store::pinning::legacy_migration::resolve_legacy_binding(db,"pin_lease_targets",&key,&point.destination_allocation).await.map_err(database)? {
        if binding.historical_route.as_ref()!=Some(&point.destination_route){return Err(ManagementError::Changed);}(binding.operational_key,binding.declaration_route)
    }else{(point.destination_allocation.clone(),point.destination_route.clone())};
    if lease.created_at!=point.started_at || lease.expires_at<point.expires_at || lease.generation<point.source_generation||lease.source!=native::value(&point.source_lease,"source").ok_or(ManagementError::InvalidEvidence)?||lease.owner_object_id!=native::value(&point.source_lease,"owner_object_id").ok_or(ManagementError::InvalidEvidence)?||target.lease_id!=lease.id || target.provider!=allocation
        || canonical_resource_cid(&target.cid).ok().as_deref()!=Some(&point.cid) {return Err(ManagementError::Changed);}
    if target.cid!=point.destination_resource_key.values[1]{return Err(ManagementError::Changed);}
    let remote=remote_pin::Entity::find_by_id((target.provider,target.cid)).one(db).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
    if remote.epoch<point.destination_epoch {return Err(ManagementError::Changed);}
    let released=crate::store::pinning::legacy_migration::release_history(db,&remote.provider,&remote.cid).await.map_err(database)?;
    if released.iter().any(|row|row.epoch>=point.destination_epoch&&row.epoch<remote.epoch) {return Err(ManagementError::Changed);}
    if point.destination_resource_id.is_some()&&point.destination_resource_id!=remote.request_id{return Err(ManagementError::Changed);}
    Ok((allocation,execution_route))
}

fn record_key(binding:&records::ConfirmedBinding,action:&records::CommittedAction)->native::NativeKey {
    native::NativeKey {table:super::super::schema::RECORDS.into(),values:vec![binding.database_scope.clone(),binding.request.plan_id.clone(),action.receipt.binding.request.item_id.clone(),"action".into()]}
}

fn latest_points(points:Vec<(native::NativeKey,records::MigrationPoint)>)->Result<Vec<records::MigrationPoint>> {
    let mut groups=std::collections::BTreeMap::<_,std::collections::BTreeMap<_,_>>::new();
    for (record,point) in points {let key=(point.lease_id.clone(),point.cid.clone(),point.slot.clone());if groups.entry(key).or_default().insert(record,point).is_some(){return Err(ManagementError::InvalidEvidence);}}
    let mut output=Vec::new();
    for mut nodes in groups.into_values() {
        let parents=nodes.values().filter_map(|point|point.predecessor.clone()).collect::<std::collections::BTreeSet<_>>();
        if parents.len()!=nodes.values().filter(|point|point.predecessor.is_some()).count(){return Err(ManagementError::InvalidEvidence);}
        let tips=nodes.keys().filter(|key|!parents.contains(*key)).cloned().collect::<Vec<_>>();
        if tips.len()!=1{return Err(ManagementError::InvalidEvidence);}let tip=tips[0].clone();let mut next=Some(tip.clone());let mut seen=std::collections::BTreeSet::new();
        while let Some(key)=next {if !seen.insert(key.clone())||seen.len()>32{return Err(ManagementError::Budget);}next=nodes.get(&key).ok_or(ManagementError::InvalidEvidence)?.predecessor.clone();}
        if seen.len()!=nodes.len(){return Err(ManagementError::InvalidEvidence);}output.push(nodes.remove(&tip).ok_or(ManagementError::InvalidEvidence)?);
    }Ok(output)
}

/// Pure counterpart of the legacy exact table/PK resolver, for frozen native
/// planning frames only. Declared namespace movement is never remote evidence.
pub(crate) fn movement_in_frame(frame:&native::NativeFrame,table:&str,key:&std::collections::BTreeMap<String,String>,provider:&str,route:&crate::pinning::identity::ProviderRouteSnapshot)->Result<Option<records::NamespacePlanningProjection>> {
    let mut found=None;let operations=frame.table("pin_legacy_migrations").ok_or(ManagementError::Schema)?;
    for row in operations.rows.iter().filter(|row|native::value(row,"direction")==Some("apply")) {
        let id=native::value(row,"id").ok_or(ManagementError::InvalidEvidence)?;
        if operations.rows.iter().any(|reverse|native::value(reverse,"reverses")==Some(id)){continue;}
        let encoded=frame.original_value("pin_legacy_migrations",&[id],"confirmation").ok_or(ManagementError::InvalidEvidence)?;
        let confirmation:crate::store::pinning::legacy_migration::Confirmation=serde_json::from_str(encoded).map_err(|_|ManagementError::InvalidEvidence)?;
        // Legacy administrative scope/path labels are not Stage7's physical
        // identity. Their complete original receipt is already part of this
        // physical native frame; never reinterpret those labels as new grants.
        if confirmation.operation_id!=id||native::value(row,"database_scope")!=Some(&confirmation.database_scope)||confirmation.declaration.database_scope!=confirmation.database_scope {return Err(ManagementError::Unauthorized);}
        for namespace in &confirmation.declaration.namespaces {
            if namespace.source!=provider||!namespace.bindings.iter().any(|binding|binding.table==table&&binding.key==*key&&binding.provider==provider&&binding.historical_route.as_ref()==Some(route)){continue;}
            if found.is_some(){return Err(ManagementError::InvalidEvidence);}found=Some(records::NamespacePlanningProjection {legacy_record:native::NativeKey {table:"pin_legacy_migrations".into(),values:vec![id.into()]},allocation:namespace.target.clone(),execution_route:namespace.target_route.clone()});
        }
    }
    Ok(found)
}

pub async fn target_route<C:ConnectionTrait>(db:&C,target:&pin_lease_target::Model)->Result<Option<crate::pinning::identity::ProviderRouteSnapshot>> {
    let Some(birth)=pin_lease_requirement::contract::get(db,&target.lease_id).await.map_err(database)? else{return Ok(None)};
    let mut found=None;
    for slot in &birth.candidate_order {
        if let Some(resolved)=resolved_slot(db,&target.lease_id,&target.cid,slot).await?&&resolved.point.destination_target_id==target.id&&resolved.allocation==target.provider {
            if found.is_some(){return Err(ManagementError::InvalidEvidence);}found=Some(resolved.execution_route);
        }
    }
    Ok(found)
}

pub async fn failover_candidates<C:ConnectionTrait>(db:&C,lease:&str,cid:&str,failed:&str)->Result<Option<Vec<String>>> {
    let Some(birth)=pin_lease_requirement::contract::get(db,lease).await.map_err(database)? else{return Ok(None)};
    let candidates=candidates_for_cid(db,lease,cid,&birth).await?.into_iter().map(|(_,allocation,_)|allocation).collect::<Vec<_>>();
    let start=candidates.iter().position(|candidate|candidate==failed).ok_or(ManagementError::Changed)?;
    Ok(Some(candidates[start..].to_vec()))
}
