//! Local captures are bounded; global callers stream original rows separately.
use super::*;
use crate::{
    pinning::management::{DryRunPlan, SafeError, Snapshot, SnapshotView},
    store::pinning::management::records::{ConfirmedBinding, ReviewHandle},
};
use std::io::Write;
type Result<T> = std::result::Result<T, SafeError>;
mod global;
pub use global::confirmed;
pub(crate) use global::confirmed_with_limits;
mod recorded;
pub use recorded::recorded;

pub struct Header<'a> {
    pub projection: Projection,
    pub database_scope: &'a str,
    pub database_identity: &'a str,
    pub principal: &'a str,
    pub catalog: &'a [String],
    pub created_at: String,
    pub expires_at: Option<String>,
}
pub fn prefix<W: Write>(
    writer: &mut Writer<W>,
    header: Header<'_>,
    request: &PlanRequest,
    configuration: &Option<ConfigurationBinding>,
    items: &[PlanEntry],
    summary: Summary,
    acks: Option<&[String]>,
) -> Result<()> {
    writer.frame(&Frame::Header {
        schema_version: SCHEMA_VERSION,
        projection: header.projection,
        applied_trust: AppliedTrust::UntrustedClaimsOnly,
        database_scope: header.database_scope.into(),
        database_identity: header.database_identity.into(),
        principal: header.principal.into(),
        catalog: header.catalog.to_vec(),
        created_at: header.created_at,
        expires_at: header.expires_at,
    })?;
    writer.frame(&Frame::Request {
        request: Some(request.clone()),
    })?;
    writer.frame(&Frame::Configuration {
        configuration: configuration.clone(),
    })?;
    writer.frame(&Frame::Acknowledgements {
        item_ids: acks.map(<[String]>::to_vec),
    })?;
    for entry in items {
        writer.frame(&Frame::Item {
            entry: entry.into(),
        })?;
    }
    writer.frame(&Frame::Summary { summary })
}
/// Each semantic fact is individually streamed, never an aggregate JSON archive.
pub fn fact<W: Write>(
    writer: &mut Writer<W>,
    section: Section,
    kind: &str,
    ordinal: usize,
    value: &impl Serialize,
) -> Result<()> {
    writer.frame(&Frame::Row {
        section,
        key: NativeKey {
            table: kind.into(),
            values: vec![format!("{ordinal:020}")],
        },
        cells: 1,
    })?;
    writer.frame(&Frame::Cell {
        column: "payload".into(),
    })?;
    let _resident = writer
        .budget_mut()
        .reserve(CELL_BYTES)
        .map_err(|_| SafeError::LimitExceeded)?;
    let mut payload = std::io::BufWriter::with_capacity(CELL_BYTES, writer.cell_output());
    serde_json::to_writer(&mut payload, value).map_err(|_| SafeError::LimitExceeded)?;
    payload.flush().map_err(|_| SafeError::LimitExceeded)?;
    drop(payload);
    writer.frame(&Frame::CellEnd {
        value: CellValue::Text,
    })?;
    writer.frame(&Frame::RowEnd)
}
pub fn local<W: Write>(
    writer: &mut Writer<W>,
    header: Header<'_>,
    request: &PlanRequest,
    snapshot: &Snapshot,
    plan: &DryRunPlan,
    acks: Option<&[String]>,
    scope: Option<&crate::pinning::management::db_snapshot::PlanningScope>,
) -> Result<()> {
    if request.items.iter().any(|item| {
        matches!(
            item.subject,
            crate::pinning::management::Subject::Provider { .. }
        )
    }) {
        return Err(SafeError::IncompleteInventory);
    }
    prefix(
        writer,
        header,
        request,
        &snapshot.configuration,
        plan.items(),
        *plan.summary(),
        acks,
    )?;
    let frame = snapshot
        .native_frame
        .as_ref()
        .ok_or(SafeError::UnknownEvidence)?;
    if frame.frontier.len() > crate::store::pinning::management::native::MAX_ROWS {
        return Err(SafeError::LimitExceeded);
    }
    let _resident = writer
        .budget_mut()
        .reserve(frame.frontier.len() * std::mem::size_of::<&NativeKey>())
        .map_err(|_| SafeError::LimitExceeded)?;
    let mut keys = frame.frontier.iter().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        let table = frame.table(&key.table).ok_or(SafeError::UnknownEvidence)?;
        writer.frame(&Frame::Row {
            section: Section::Native,
            key: key.clone(),
            cells: table.columns.len() as u32,
        })?;
        let values = key.values.iter().map(String::as_str).collect::<Vec<_>>();
        for column in &table.columns {
            cell(
                writer,
                column,
                frame.original_value(&key.table, &values, column),
            )?;
        }
        writer.frame(&Frame::RowEnd)?;
    }
    local_memberships(writer, &frame.tables, scope)?;
    semantic(writer, snapshot.into())?;
    for (i, entry) in plan.items().iter().enumerate() {
        fact(
            writer,
            Section::Result,
            "management_capture",
            i,
            &entry.capture,
        )?;
    }
    for (i, entry) in snapshot.applied.iter().enumerate() {
        fact(writer, Section::AppliedClaim, "management_claim", i, entry)?;
    }
    Ok(())
}
fn cell<W: Write>(writer: &mut Writer<W>, column: &str, value: Option<&str>) -> Result<()> {
    writer.frame(&Frame::Cell {
        column: column.into(),
    })?;
    if let Some(value) = value {
        writer
            .cell_output()
            .write_all(value.as_bytes())
            .map_err(|_| SafeError::InvalidInput)?;
    }
    writer.frame(&Frame::CellEnd {
        value: if value.is_some() {
            CellValue::Text
        } else {
            CellValue::Null
        },
    })
}
/// The complete confirmed projection is derived only from the immutable review.
pub fn local_confirmed<W: Write>(
    writer: &mut Writer<W>,
    binding: &ConfirmedBinding,
    projection: Projection,
) -> Result<()> {
    let review = binding.review();
    let view = review.planning_view();
    if review.entries().iter().any(|entry| {
        matches!(
            entry.request.subject,
            crate::pinning::management::Subject::Provider { .. }
        )
    }) {
        return Err(SafeError::IncompleteInventory);
    }
    let rows = review.archive();
    prefix(
        writer,
        Header {
            projection,
            database_scope: rows.scope(),
            database_identity: rows.identity(),
            principal: &binding.principal,
            catalog: rows.schema_versions(),
            created_at: review.staged_at().to_rfc3339(),
            expires_at: Some(binding.expires_at.to_rfc3339()),
        },
        review.request(),
        view.configuration,
        review.entries(),
        *review.summary(),
        review.candidate_item_ids(),
    )?;
    native_review(writer, &review)?;
    local_memberships(writer, rows.catalog(), None)?;
    semantic(writer, view)?;
    fact(
        writer,
        Section::Inventory,
        "management_z_confirmation",
        0,
        &(binding.confirmed_at, binding.expires_at),
    )?;
    for (i, point) in review.migration_points().iter().enumerate() {
        fact(
            writer,
            Section::Inventory,
            "management_z_migration_point",
            i,
            point,
        )?;
    }
    for (i, entry) in review.entries().iter().enumerate() {
        fact(
            writer,
            Section::Result,
            "management_capture",
            i,
            &entry.capture,
        )?;
    }
    for (i, entry) in view.applied.iter().enumerate() {
        fact(writer, Section::AppliedClaim, "management_claim", i, entry)?;
    }
    Ok(())
}
fn native_review<W: Write>(writer: &mut Writer<W>, review: &ReviewHandle) -> Result<()> {
    let count = review
        .archive()
        .rows()
        .take(crate::store::pinning::management::native::MAX_ROWS + 1)
        .count();
    if count > crate::store::pinning::management::native::MAX_ROWS {
        return Err(SafeError::LimitExceeded);
    }
    let _resident = writer
        .budget_mut()
        .reserve(count * 128)
        .map_err(|_| SafeError::LimitExceeded)?;
    let mut rows = review.archive().rows().collect::<Vec<_>>();
    rows.sort_by(|a, b| (&a.0.table, a.1).cmp(&(&b.0.table, b.1)));
    for (table, key, row) in rows {
        writer.frame(&Frame::Row {
            section: Section::Native,
            key: NativeKey {
                table: table.table.clone(),
                values: key.to_vec(),
            },
            cells: table.columns.len() as u32,
        })?;
        for (column, value) in row.iter() {
            cell(writer, column, value.as_deref())?;
        }
        writer.frame(&Frame::RowEnd)?;
    }
    Ok(())
}
fn local_memberships<W: Write>(
    writer: &mut Writer<W>,
    tables: &[crate::store::pinning::management::native::NativeTable],
    scope: Option<&crate::pinning::management::db_snapshot::PlanningScope>,
) -> Result<()> {
    for (i, table) in tables.iter().enumerate() {
        fact(
            writer,
            Section::Membership,
            "management_catalog",
            i,
            &(
                &table.table,
                &table.columns,
                &table.primary_key,
                &table.definitions,
            ),
        )?;
    }
    let Some(scope) = scope else { return Ok(()) };
    let mut ordinal = 0;
    for (proof, view) in scope.authority_reads().iter().enumerate() {
        for (membership, m) in view.proof.memberships.iter().enumerate() {
            fact(
                writer,
                Section::Membership,
                "scope_authority_membership",
                ordinal,
                &(proof, membership, &m.selector, m.sources.len()),
            )?;
            ordinal += 1;
        }
    }
    ordinal = 0;
    for (proof, view) in scope.authority_reads().iter().enumerate() {
        for (membership, m) in view.proof.memberships.iter().enumerate() {
            for key in &m.sources {
                fact(
                    writer,
                    Section::Membership,
                    "scope_authority_source",
                    ordinal,
                    &(proof, membership, key),
                )?;
                ordinal += 1;
            }
        }
    }
    ordinal = 0;
    for (proof, view) in scope.authority_reads().iter().enumerate() {
        for seal in &view.proof.seals {
            fact(
                writer,
                Section::Membership,
                "scope_authority_version",
                ordinal,
                &(proof, seal),
            )?;
            ordinal += 1;
        }
    }
    ordinal = 0;
    for (membership, (selector, keys)) in scope.memberships.iter().enumerate() {
        fact(
            writer,
            Section::Membership,
            "scope_business_membership",
            membership,
            &(selector, keys.len()),
        )?;
    }
    for (membership, keys) in scope.memberships.values().enumerate() {
        for key in keys {
            fact(
                writer,
                Section::Membership,
                "scope_business_source",
                ordinal,
                &(membership, key),
            )?;
            ordinal += 1;
        }
    }
    for (i, read) in scope.business_reads().iter().enumerate() {
        fact(
            writer,
            Section::Membership,
            "scope_specification",
            i,
            read.spec(),
        )?;
    }
    Ok(())
}
pub(crate) fn snapshot_facts<W: Write>(writer: &mut Writer<W>, snapshot: &Snapshot) -> Result<()> {
    semantic(writer, snapshot.into())
}

pub(crate) fn verify_output_header<R: std::io::Read>(
    input: R,
    projection: Projection,
    header: &crate::store::pinning::management::records::segmented::ArchiveHeader,
    budget: crate::store::pinning::management::native::meter::Budget,
) -> (
    Result<()>,
    crate::store::pinning::management::native::meter::Budget,
) {
    match Reader::new_metered(input, projection, budget) {
        Err((error, budget)) => (Err(error), *budget),
        Ok(mut reader) => {
            let result = match reader.next_frame() {
                Ok(Some(Event::Frame(Frame::Header {
                    projection: actual,
                    database_scope,
                    database_identity,
                    principal,
                    catalog,
                    created_at,
                    expires_at,
                    applied_trust,
                    ..
                }))) if actual == projection
                    && database_scope == header.database_scope
                    && database_identity == header.database_identity
                    && principal == header.principal
                    && catalog == header.catalog
                    && created_at == header.created_at
                    && expires_at.is_none()
                    && header.expires_at.is_none()
                    && applied_trust == AppliedTrust::UntrustedClaimsOnly =>
                {
                    Ok(())
                }
                Err(error) => Err(error),
                _ => Err(SafeError::UnknownEvidence),
            };
            (result, reader.take_budget())
        }
    }
}

pub(crate) fn verify_output<R: std::io::Read>(
    file: R,
    projection: Projection,
    budget: crate::store::pinning::management::native::meter::Budget,
    limits: &crate::pinning::management::retirement::RunLimits,
) -> (
    Result<()>,
    crate::store::pinning::management::native::meter::Budget,
) {
    match Reader::new_metered(file, projection, budget) {
        Err((error, budget)) => (Err(error), *budget),
        Ok(mut reader) => {
            let checked = (|| {
                reader.enable_buffering()?;
                loop {
                    if limits.cancellation.is_cancelled() {
                        return Err(SafeError::LimitExceeded);
                    }
                    if reader.next_frame()?.is_none() {
                        break;
                    }
                }
                if !reader.verified() {
                    return Err(SafeError::UnknownEvidence);
                }
                reader
                    .budget()
                    .check()
                    .map_err(|_| SafeError::LimitExceeded)
            })();
            (checked, reader.take_budget())
        }
    }
}

fn semantic<W: Write>(writer: &mut Writer<W>, view: SnapshotView<'_>) -> Result<()> {
    fact(
        writer,
        Section::Inventory,
        "management_clock",
        0,
        &(view.observed_at, view.database_now),
    )?;
    macro_rules! facts {
        ($kind:literal,$values:expr) => {
            for (i, value) in $values.iter().enumerate() {
                fact(writer, Section::Inventory, $kind, i, value)?;
            }
        };
    }
    facts!("management_history", view.historical_remotes);
    facts!("management_inventory", view.inventories);
    facts!("management_ledger", view.ledger_domains);
    facts!("management_objects", view.objects);
    facts!("management_remotes", view.remotes);
    facts!("management_roots", view.root_references);
    facts!("management_routes", view.routes);
    facts!("management_targets", view.targets);
    facts!("management_unknown", view.unknown_resources);
    Ok(())
}
