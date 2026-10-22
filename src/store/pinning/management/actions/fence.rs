use super::*;
use sea_orm::{DatabaseTransaction, Statement};

pub(super) async fn write_intent(tx: &DatabaseTransaction) -> Result<()> {
    crate::store::pinning::management::admission::lock_sqlite_writer_in_caller_transaction(tx)
        .await
        .map_err(database)
}

#[cfg(test)]
#[path = "fence_tests.rs"]
mod tests;

pub(super) async fn lock(
    tx: &DatabaseTransaction,
    before: records::FrozenRows<'_>,
    _config: &ManagementConfig,
    binding: &records::ConfirmedBinding,
    scope: &crate::pinning::management::db_snapshot::PlanningScope,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    before.validate_current()?;
    if tx.get_database_backend() != DatabaseBackend::Postgres {
        crate::store::pinning::management::admission::lock_open_with_work(
            tx,
            &allocation_routes(binding, work)?,
            work,
        )
        .await
        .map_err(database)?;
        revalidate(tx, scope, work).await?;
        return Ok(());
    }
    // The request-derived current closure fixes the row lock set before any
    // lock; record revalidation still proves the original reviewed authority.
    for name in [
        "buckets",
        "objects",
        "object_versions",
        "import_destinations",
        "import_prefix_claims",
        "standard_mutation_leases",
        "pin_leases",
        "pin_lease_targets",
        "remote_pins",
        "remote_pin_ledger",
        "pin_provider_usage",
    ] {
        lock_rows(tx, scope, name, work).await?;
    }
    crate::store::pinning::management::admission::lock_open_with_work(
        tx,
        &allocation_routes(binding, work)?,
        work,
    )
    .await
    .map_err(database)?;
    for name in ["pin_management_retirements", "pin_provider_routes"] {
        lock_rows(tx, scope, name, work).await?;
    }
    // Tail NOWAIT barriers cover the complete selected memberships, including
    // empty sets. No unrelated registry rows are enumerated or locked.
    records::locator::fence_authority(tx).await?;
    revalidate(tx, scope, work).await?;
    Ok(())
}

async fn revalidate(
    tx: &DatabaseTransaction,
    scope: &crate::pinning::management::db_snapshot::PlanningScope,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let mut budget = native::meter::Budget::default();
    let result = scope.revalidate(tx, &mut budget).await;
    super::super::admission::authority::charge(&budget, work)?;
    result.map_err(local_scope::snapshot_error)
}

fn allocation_routes(
    binding: &records::ConfirmedBinding,
    work: records::DecodeWork<'_>,
) -> Result<Vec<crate::pinning::identity::ProviderRouteSnapshot>> {
    use crate::pinning::management::{Action, Capture, PlannedStep, RemoteDirective};
    let mut selected = Vec::new();
    work.with(|work| work.nodes(1))?;
    let mut push = |original: &crate::pinning::identity::ProviderRouteSnapshot| -> Result<()> {
        if !selected.contains(original) {
            work.copy(original)?;
            work.with(|work| work.nodes(1))?;
            selected.push(original.clone());
        }
        Ok(())
    };
    for entry in &binding.confirmation.bindings {
        let Some(Capture::Object(capture)) = &entry.capture else {
            continue;
        };
        match &entry.request.action {
            Action::Create(_) | Action::Backfill(_) | Action::Reapply(_) | Action::Renew { .. } => {
                for route in &capture.requirement_routes {
                    if let Some(original) = &route.original {
                        push(original)?;
                    }
                }
            }
            Action::Retry => {
                if matches!(&entry.step,Some(PlannedStep::Object(step)) if step.source==RemoteDirective::SubmitWithFence)
                    && let Some(original) = &capture.route.original
                {
                    push(original)?;
                }
            }
            Action::Migrate { destination } => {
                if matches!(&entry.step,Some(PlannedStep::Object(step)) if step.destination!=Some(RemoteDirective::NoWrite))
                    && let Some(original) = &destination.original
                {
                    push(original)?;
                }
            }
            _ => {}
        }
    }
    // Current config equality is revalidated before business writes. Only exact
    // reviewed allocation/renewal routes enter this native admission fence.
    Ok(selected)
}

async fn lock_rows(
    tx: &DatabaseTransaction,
    scope: &crate::pinning::management::db_snapshot::PlanningScope,
    name: &str,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let table = native::current_catalog()
        .iter()
        .find(|table| table.table == name)
        .ok_or(ManagementError::Schema)?;
    let mut rows = scope
        .originals()
        .iter()
        .filter(|(key, _)| key.table == name)
        .map(|(_, row)| row)
        .collect::<Vec<_>>();
    work.with(|work| work.nodes(1 + rows.len()))?;
    if name == "pin_lease_targets" {
        rows.sort_by_key(|row| (native::value(*row, "created_at"), native::value(*row, "id")));
    }
    for row in rows {
        work.with(|work| work.nodes(2 + table.primary_key.len() * 2))?;
        let mut key = Vec::with_capacity(table.primary_key.len());
        for column in &table.primary_key {
            let value = native::value(row, column).ok_or(ManagementError::InvalidEvidence)?;
            work.copy(value)?;
            key.push(value);
        }
        let layout = native::current_catalog()
            .iter()
            .find(|layout| layout.table == name)
            .ok_or(ManagementError::Schema)?;
        let (predicate, values) = sql::exact_key(layout, &key)?;
        let sql =
            format!("SELECT 1 AS present FROM \"{name}\" WHERE {predicate} FOR UPDATE NOWAIT");
        if tx
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                sql,
                values,
            ))
            .await
            .map_err(super::super::nowait_error)?
            .is_none()
        {
            return Err(ManagementError::Changed);
        }
    }
    Ok(())
}
