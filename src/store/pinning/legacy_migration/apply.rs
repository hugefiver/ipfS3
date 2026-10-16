//! One transaction owns mutations, post-image fence and committed receipt.
use super::{
    Confirmation, Declaration, Receipt,
    fence::{drained, lock_before_routes, lock_from_routes, maintenance, validate_plan},
    inventory::validate,
    mutation::write_changes,
    projection, receipt, refused, snapshot,
};
use crate::error::AppResult;
use sea_orm::{ConnectionTrait, TransactionTrait};

pub async fn apply<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    declaration: &Declaration,
    plan: &Confirmation,
) -> AppResult<Receipt> {
    validate(declaration)?;
    if declaration != &plan.declaration
        || plan.version != 1
        || plan.database_scope != declaration.database_scope
        || plan.rollback_of.is_some()
        || declaration.rollback_receipt.is_some()
    {
        return Err(refused(
            "confirmation declaration differs or is rollback-only",
        ));
    }
    maintenance(declaration)?;
    // Fresh default READ COMMITTED transaction, never a dry-run RR snapshot.
    let txn = db.begin().await?;
    let schema = snapshot::schema(&txn, true).await?;
    lock_before_routes(&txn, &schema).await?;
    // A trusted committed receipt is replay, not another allocation. Re-read it
    // below under the complete receipt fence; do not inspect mutable records or
    // apply a new open-domain admission to its original before image.
    let admission_routes = if super::management_closure::enabled(&schema)
        && receipt::lookup(&txn, &plan.operation_id).await?.is_none()
    {
        validate_plan(&txn, plan, &schema).await?;
        drained(&plan.before)?;
        let current = snapshot::read(&txn, &schema).await?;
        let management =
            super::management_closure::collect(&txn, &current, declaration, &schema).await?;
        let projected = projection::project(&current, declaration, &management)?;
        if current != plan.before
            || projected != plan.after
            || snapshot::frontier(&txn, &schema).await? != plan.receipt_frontier
        {
            return Err(refused("complete before/after frontier changed"));
        }
        Some(management.routes().to_vec())
    } else {
        None
    };
    lock_from_routes(&txn, &schema, admission_routes.as_deref()).await?;
    if snapshot::schema(&txn, true).await? != schema {
        return Err(refused("receipt schema changed before maintenance locks"));
    }
    if let Some(existing) = receipt::lookup(&txn, &plan.operation_id).await? {
        receipt::matches(&existing, plan)?;
        if receipt::reverse(&txn, &plan.operation_id).await?.is_some() {
            return Err(refused(
                "apply was rolled back; new declaration and dry-run required",
            ));
        }
        txn.commit().await?;
        return Ok(receipt::public(&existing));
    }
    if super::management_closure::enabled(&schema) && admission_routes.is_none() {
        return Err(refused(
            "committed receipt changed before the complete fence",
        ));
    }
    validate_plan(&txn, plan, &schema).await?;
    for namespace in &declaration.namespaces {
        if receipt::source_has_receipt(&txn, &namespace.source).await? {
            return Err(refused(
                "source already has an active receipt; replay original apply or explicitly rollback",
            ));
        }
    }
    drained(&plan.before)?;
    let current = snapshot::read(&txn, &schema).await?;
    let management =
        super::management_closure::collect(&txn, &current, declaration, &schema).await?;
    let projected = projection::project(&current, declaration, &management)?;
    if current != plan.before
        || projected != plan.after
        || snapshot::frontier(&txn, &schema).await? != plan.receipt_frontier
    {
        return Err(refused("complete before/after frontier changed"));
    }
    write_changes(&txn, &plan.before, &plan.after, declaration, false).await?;
    if snapshot::read(&txn, &schema).await? != plan.after {
        return Err(refused("final native post-image fence failed"));
    }
    let result = receipt::append(&txn, plan, None).await?;
    txn.commit().await?;
    Ok(result)
}

pub async fn rollback<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    original_id: &str,
    confirmation: &Confirmation,
) -> AppResult<Receipt> {
    validate(&confirmation.declaration)?;
    maintenance(&confirmation.declaration)?;
    let txn = db.begin().await?;
    let schema = snapshot::schema(&txn, true).await?;
    lock_before_routes(&txn, &schema).await?;
    lock_from_routes(&txn, &schema, None).await?;
    if snapshot::schema(&txn, true).await? != schema {
        return Err(refused("receipt schema changed before maintenance locks"));
    }
    let original = receipt::lookup(&txn, original_id)
        .await?
        .filter(|r| r.direction == "apply")
        .ok_or_else(|| refused("committed apply receipt not found"))?;
    receipt::rollback_matches(&original, confirmation)?;
    if let Some(reverse) = receipt::reverse(&txn, original_id).await? {
        txn.commit().await?;
        return Ok(receipt::public(&reverse));
    }
    if confirmation.schema != schema
        || snapshot::identity(&txn).await? != confirmation.database_identity
    {
        return Err(refused("rollback database/schema differs"));
    }
    validate_plan(&txn, confirmation, &schema).await?;
    let mut frontier = confirmation.receipt_frontier.clone();
    frontier.push(original_id.into());
    frontier.sort();
    let current = snapshot::read(&txn, &schema).await?;
    if current != confirmation.after || snapshot::frontier(&txn, &schema).await? != frontier {
        return Err(refused("post-state has new work; rollback is unsafe"));
    }
    drained(&current)?;
    write_changes(
        &txn,
        &confirmation.after,
        &confirmation.before,
        &confirmation.declaration,
        true,
    )
    .await?;
    if snapshot::read(&txn, &schema).await? != confirmation.before {
        return Err(refused("final native rollback fence failed"));
    }
    let result = receipt::append(&txn, confirmation, Some(original_id)).await?;
    txn.commit().await?;
    Ok(result)
}
