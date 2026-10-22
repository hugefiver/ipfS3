use super::*;
use crate::store::{
    initialization::history,
    pinning::management::{native, records::RecordedPlan},
};

/// Canonical frozen historical data, never a current execution binding.
pub async fn recorded<W: Write>(
    db: &sea_orm::DatabaseConnection,
    writer: &mut Writer<W>,
    plan: &RecordedPlan,
    projection: Projection,
) -> Result<()> {
    let index = &plan.index;
    writer.frame(&Frame::Header {
        schema_version: SCHEMA_VERSION,
        projection,
        applied_trust: AppliedTrust::UntrustedClaimsOnly,
        database_scope: index.database_scope.clone(),
        database_identity: index.database_identity.clone(),
        principal: index.principal.clone(),
        catalog: index.catalog.clone(),
        created_at: index.staged_at.to_rfc3339(),
        expires_at: Some(index.expires_at.to_rfc3339()),
    })?;
    writer.frame(&Frame::Request {
        request: Some(plan.request.clone()),
    })?;
    writer.frame(&Frame::Configuration {
        configuration: plan.snapshot.configuration.clone(),
    })?;
    writer.frame(&Frame::Acknowledgements {
        item_ids: index.acknowledgements.clone(),
    })?;
    for entry in &plan.entries {
        writer.frame(&Frame::Item {
            entry: Box::new(ItemFact {
                id: entry.id.clone(),
                request: entry.request.clone(),
                status: entry.status,
                step: entry.step.clone(),
            }),
        })?;
    }
    writer.frame(&Frame::Summary {
        summary: plan.summary,
    })?;
    if index.rows.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err(SafeError::UnknownEvidence);
    }
    for reference in &index.rows {
        let row: native::NativeRow =
            history::read_part(db, &reference.original, writer.budget_mut())
                .await
                .map_err(error)?;
        writer.frame(&Frame::Row {
            section: Section::Native,
            key: reference.key.clone(),
            cells: row.len() as u32,
        })?;
        for (column, value) in &row {
            cell(writer, column, value.as_deref())?;
        }
        writer.frame(&Frame::RowEnd)?;
    }
    let catalog = native::archived_catalog(&index.catalog).map_err(error)?;
    local_memberships(writer, catalog, None)?;
    for (i, applied) in plan.snapshot.applied.iter().enumerate() {
        fact(
            writer,
            Section::Inventory,
            "management_applied_frozen",
            i,
            applied,
        )?;
    }
    let view = &plan.snapshot;
    semantic(
        writer,
        SnapshotView {
            work: None,
            observed_at: view.observed_at,
            database_now: view.database_now,
            configuration: &view.configuration,
            native_frame: None,
            rows: None,
            objects: &view.objects,
            routes: &view.routes,
            targets: &view.targets,
            remotes: &view.remotes,
            historical_remotes: &view.historical_remotes,
            unknown_resources: &view.unknown_resources,
            ledger_domains: &view.ledger_domains,
            inventories: &view.inventories,
            root_references: &view.root_references,
            applied: &[],
        },
    )?;
    fact(
        writer,
        Section::Inventory,
        "management_z_confirmation",
        0,
        &(index.confirmed_at, index.expires_at),
    )?;
    for (i, point) in plan.migration_points.iter().enumerate() {
        fact(
            writer,
            Section::Inventory,
            "management_z_migration_point",
            i,
            point,
        )?;
    }
    for (i, (key, reference)) in plan.originals.iter().enumerate() {
        let original: native::NativeRow = history::read_part(db, reference, writer.budget_mut())
            .await
            .map_err(error)?;
        fact(
            writer,
            Section::Result,
            "historical_original",
            i,
            &(key, original),
        )?;
    }
    for (i, action) in plan.actions().iter().enumerate() {
        fact(writer, Section::Result, "historical_recorded", i, action)?;
    }
    for (i, entry) in plan.entries.iter().enumerate() {
        fact(
            writer,
            Section::Result,
            "management_capture",
            i,
            &entry.capture,
        )?;
    }
    Ok(())
}

fn error(error: crate::store::pinning::management::ManagementError) -> SafeError {
    if error == crate::store::pinning::management::ManagementError::Budget {
        SafeError::LimitExceeded
    } else {
        SafeError::UnknownEvidence
    }
}
