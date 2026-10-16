use super::*;
use sea_orm::{DatabaseTransaction, Statement};

pub(super) async fn write_intent(tx: &DatabaseTransaction) -> Result<()> {
    if tx.get_database_backend() == DatabaseBackend::Sqlite {
        tx.execute_unprepared("UPDATE pin_management_records SET database_scope=database_scope WHERE kind='database_scope'").await.map_err(|_|ManagementError::Database)?;
    }
    Ok(())
}

pub(super) async fn lock(
    tx: &DatabaseTransaction,
    before: records::FrozenRows<'_>,
    _config: &ManagementConfig,
    binding: &records::ConfirmedBinding,
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
        return Ok(());
    }
    // Full frozen membership, not a growing CID-local lock set. This preserves
    // the global native fence while refusing scope expansion after any lock.
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
        lock_rows(tx, before, name, work).await?;
    }
    crate::store::pinning::management::admission::lock_open_with_work(
        tx,
        &allocation_routes(binding, work)?,
        work,
    )
    .await
    .map_err(database)?;
    for name in ["pin_management_retirements", "pin_provider_routes"] {
        lock_rows(tx, before, name, work).await?;
    }
    // Tail NOWAIT barriers include empty tables and insert phantoms. Callback
    // job→remote locks cannot make us wait while holding lifecycle rows.
    for table in native::registry() {
        tx.execute_unprepared(&format!(
            "LOCK TABLE \"{}\" IN SHARE ROW EXCLUSIVE MODE NOWAIT",
            table.table
        ))
        .await
        .map_err(|_| ManagementError::Changed)?;
    }
    // The final record-layer revalidator removes only validated exact self keys
    // and rereads every row/private tuple/frontier after these barriers.
    Ok(())
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
    frame: records::FrozenRows<'_>,
    name: &str,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let table = frame.table(name).ok_or(ManagementError::Schema)?;
    work.with(|work| work.nodes(1 + table.rows.iter().count()))?;
    let mut rows = table.rows.iter().collect::<Vec<_>>();
    if name == "pin_lease_targets" {
        rows.sort_by_key(|row| (native::value(*row, "created_at"), native::value(*row, "id")));
    }
    for row in rows {
        work.with(|work| work.nodes(2 + table.primary_key.len() * 2))?;
        let mut clauses = Vec::new();
        let mut values = Vec::<sea_orm::Value>::new();
        for (index, column) in table.primary_key.iter().enumerate() {
            let value = native::value(row, column).ok_or(ManagementError::InvalidEvidence)?;
            work.copy(value)?;
            clauses.push(format!("CAST(\"{column}\" AS TEXT)=${}", index + 1));
            values.push(value.to_owned().into());
        }
        let sql = format!(
            "SELECT 1 AS present FROM \"{name}\" WHERE {} FOR UPDATE NOWAIT",
            clauses.join(" AND ")
        );
        if tx
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                sql,
                values,
            ))
            .await
            .map_err(|_| ManagementError::Changed)?
            .is_none()
        {
            return Err(ManagementError::Changed);
        }
    }
    Ok(())
}
