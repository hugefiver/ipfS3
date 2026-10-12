//! Formal Stage 7 Store apply. Provider IO is deliberately outside this module.
use super::{ManagementError, Result, native, records};
use crate::pinning::management::{local_config::ManagementConfig, RevalidationOutcome};
use sea_orm::{AccessMode, ConnectionTrait, DatabaseBackend, DatabaseConnection, IsolationLevel, TransactionTrait};

mod executor;
mod fence;
mod intents;
pub(crate) mod accounting;
mod recovery;
mod migration;
mod retirement;
pub mod status;
pub mod provider_points;
#[cfg(test)]
mod tests;
pub mod points;
pub mod job_authority;

#[derive(Clone, PartialEq, Eq)]
pub struct ApplyResult {
    pub replayed: bool,
    pub items: Vec<records::CommittedAction>,
}

/// A new write transaction per invocation; never reuse a dry-run RR snapshot.
/// Commit-response failure is resolved by the exact durable receipts first.
pub async fn apply(
    db: &DatabaseConnection, config: &ManagementConfig,
    auth: &records::LocalAuthorization, binding: &records::ConfirmedBinding,
) -> Result<ApplyResult> {
    let tx = if db.get_database_backend()==DatabaseBackend::Postgres {
        db.begin_with_config(Some(IsolationLevel::ReadCommitted),Some(AccessMode::ReadWrite)).await
    } else { db.begin().await }.map_err(|_|ManagementError::Database)?;
    fence::write_intent(&tx).await?;
    // TTL/current config only govern new business. A committed exact result is
    // still replayable after expiry or a subsequent legitimate state transition.
    if let Some(items)=replay(&tx,auth,binding).await? {
        tx.rollback().await.map_err(|_|ManagementError::Database)?;
        return Ok(ApplyResult {replayed:true,items});
    }
    let before=binding.confirmation.native_frame.as_ref().ok_or(ManagementError::Confirmation)?;
    fence::lock(&tx,before,config,binding).await?;
    let checked=records::revalidate_confirmed_in_transaction(&tx,config,auth,binding).await?;
    if checked.len()!=binding.confirmation.bindings.len() || checked.iter().any(|item|!matches!(item.outcome,RevalidationOutcome::Checked(_))) {
        return Err(ManagementError::Confirmation);
    }
    let mut items=Vec::with_capacity(checked.len());
    for entry in &binding.confirmation.bindings {
        items.push(executor::execute(&tx,config,auth,binding,entry,before).await?);
    }
    // Check DB-clock deadline at the publication boundary too. Business time
    // passing during an allocation does not silently extend confirmed authority.
    binding.check(&tx,auth).await?;
    status::fence_business_expiry(&tx,&items).await?;
    for item in &items {
        records::write_committed_action_in_transaction(&tx,auth,binding,item,before).await?;
    }
    for item in &mut items {
        let state=status::current_state(&tx,item).await?;
        records::update_committed_state_in_transaction(&tx,auth,binding,item,state).await?;
        item.state=state;
    }
    native::capture(&tx).await?;
    binding.check(&tx,auth).await?;
    status::fence_business_expiry(&tx,&items).await?;
    match tx.commit().await {
        Ok(()) => Ok(ApplyResult {replayed:false,items}),
        Err(_) => match replay(db,auth,binding).await? {
            Some(items)=>Ok(ApplyResult {replayed:true,items}),
            None=>Err(ManagementError::Database),
        },
    }
}

/// No failure commits a business prefix. The typed stopped report refers to the
/// rolled-back entire batch, not a successful action record or a new grant.
pub struct ApplyFailure {pub cause:ManagementError,pub items:Vec<StoppedItem>}
pub struct StoppedItem {pub item_id:String,pub state:records::ActionState}
pub async fn apply_detailed(db:&DatabaseConnection,config:&ManagementConfig,auth:&records::LocalAuthorization,binding:&records::ConfirmedBinding)->std::result::Result<ApplyResult,ApplyFailure> {
    apply(db,config,auth,binding).await.map_err(|cause|ApplyFailure {cause,items:binding.request.items.iter().map(|item|StoppedItem {item_id:item.item_id.clone(),state:records::ActionState::Stopped}).collect()})
}

async fn replay<C:ConnectionTrait>(db:&C,auth:&records::LocalAuthorization,binding:&records::ConfirmedBinding)->Result<Option<Vec<records::CommittedAction>>> {
    auth.check(db).await?;
    if binding.request.items.len()!=binding.confirmation.bindings.len() {return Err(ManagementError::Confirmation);}
    let mut items=Vec::new();
    for entry in &binding.confirmation.bindings {
        if let Some(receipt)=records::lookup_committed(db,auth,binding,&entry.request.item_id).await? {items.push(receipt);}
    }
    if items.is_empty() {return Ok(None);}
    // Formal apply never commits a prefix. A mixed foundation/manual record set
    // is not authority to finish an expired or drifting remainder.
    if items.len()!=binding.request.items.len() {return Err(ManagementError::Confirmation);}
    Ok(Some(items))
}

pub(super) fn database(error:crate::error::AppError)->ManagementError {
    match error {crate::error::AppError::InvalidPinningRequest(_)=>ManagementError::Changed,_=>ManagementError::Database}
}
