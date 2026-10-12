//! Readonly is an operation class, not proof that an observation succeeded.
use super::*;
use crate::store::{entities::{pin_invocation_route,pin_resource_history,pin_submit_observation,remote_pin_ledger},pinning::{ledger,legacy_migration}};
use sea_orm::{ColumnTrait,QueryFilter};

pub(super) async fn needs_attention<C:ConnectionTrait>(db:&C,action:&records::CommittedAction)->Result<bool> {
    for authority in action.jobs.iter().filter(|authority|authority.directive==records::JobDirective::ReadOnlyObserve) {
        if pin_job::Entity::find_by_id(authority.job_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?.is_none(){return Ok(true);}
        let invocation=pin_invocation_route::Entity::find_by_id(authority.job_id.clone()).one(db).await.map_err(|_|ManagementError::Database)?;
        if invocation.is_none_or(|invocation|invocation.remote_epoch!=authority.remote_epoch||serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(&invocation.route).ok().as_ref()!=Some(&authority.route)){return Ok(true);}
        let mut key=std::collections::BTreeMap::from([("provider".into(),authority.provider.clone()),("cid".into(),authority.cid.clone())]);
        if authority.resource_key.table=="pin_resource_history"{key.insert("epoch".into(),authority.remote_epoch.to_string());}
        let provider=if let Some(binding)=legacy_migration::resolve_legacy_binding(db,&authority.resource_key.table,&key,&authority.provider).await.map_err(database)? {
            if binding.historical_route.as_ref()!=Some(&authority.route){return Ok(true);}binding.operational_key
        }else{authority.provider.clone()};
        let row=if authority.resource_key.table=="pin_resource_history" {
            let Some(history)=pin_resource_history::Entity::find_by_id((provider.clone(),authority.cid.clone(),authority.remote_epoch)).one(db).await.map_err(|_|ManagementError::Database)? else{return Ok(true)};
            let envelope:HistoricalEnvelope=serde_json::from_str(&history.ledger).map_err(|_|ManagementError::InvalidEvidence)?;
            if envelope.request_id!=authority.resource_id{return Ok(true);}Some(envelope.ledger)
        }else{
            let Some(remote)=remote_pin::Entity::find_by_id((provider.clone(),authority.cid.clone())).one(db).await.map_err(|_|ManagementError::Database)? else{return Ok(true)};
            if remote.epoch!=authority.remote_epoch||remote.request_id!=authority.resource_id{return Ok(true);}
            ledger::get(db,&provider,&authority.cid).await.map_err(database)?
        };
        let Some(row)=row else{return Ok(true)};
        if ledger::decode_route(&row).as_ref()!=Some(&authority.route)||matches!(row.effect.as_str(),"unknown"|"reported_unknown")||row.last_error.is_some(){return Ok(true);}
        let providers=legacy_migration::historical_keys(db,"pin_submit_observations",&provider,Some(&authority.cid)).await.map_err(database)?;
        let unresolved=pin_submit_observation::Entity::find().filter(pin_submit_observation::Column::Provider.is_in(providers)).filter(pin_submit_observation::Column::ExpectedCid.eq(&authority.cid)).filter(pin_submit_observation::Column::RemoteEpoch.eq(authority.remote_epoch)).filter(pin_submit_observation::Column::NeedsAttention.eq(true)).all(db).await.map_err(|_|ManagementError::Database)?;
        if unresolved.iter().any(|observation|serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(&observation.route).ok().as_ref()==Some(&authority.route)){return Ok(true);}
    }
    Ok(false)
}
#[derive(serde::Deserialize)]
struct HistoricalEnvelope {ledger:remote_pin_ledger::Model,request_id:Option<String>}
