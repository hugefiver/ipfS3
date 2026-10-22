use super::super::codec::Record;
use super::*;
use crate::pinning::management::retirement::PageMeter;
use crate::store::pinning::management::{ManagementError, records};
use sea_orm::DatabaseConnection;
use std::io::{Seek, SeekFrom};
use std::sync::Arc;

fn storage(error: ManagementError) -> SafeError {
    if error == ManagementError::Budget {
        SafeError::LimitExceeded
    } else {
        SafeError::UnknownEvidence
    }
}

/// The canonical export uses only the frozen local review and its sealed full
/// inventory archives. Current configuration, clocks and registry are irrelevant.
pub async fn confirmed<W: Write>(
    db: &DatabaseConnection,
    writer: &mut Writer<W>,
    binding: &ConfirmedBinding,
    projection: Projection,
) -> Result<()> {
    confirmed_inner(db, writer, binding, projection, None).await
}

pub(crate) async fn confirmed_with_limits<W: Write>(
    db: &DatabaseConnection,
    writer: &mut Writer<W>,
    binding: &ConfirmedBinding,
    projection: Projection,
    limits: &crate::pinning::management::retirement::RunLimits,
    pages: Arc<PageMeter>,
) -> Result<()> {
    confirmed_inner(db, writer, binding, projection, Some((limits, pages))).await
}

async fn confirmed_inner<W: Write>(
    db: &DatabaseConnection,
    writer: &mut Writer<W>,
    binding: &ConfirmedBinding,
    projection: Projection,
    limits: Option<(
        &crate::pinning::management::retirement::RunLimits,
        Arc<PageMeter>,
    )>,
) -> Result<()> {
    let review = binding.review();
    let provider = review.entries().iter().find(|entry| {
        matches!(
            entry.capture,
            Some(crate::pinning::management::Capture::Provider(_))
        )
    });
    let Some(provider) = provider else {
        return local_confirmed(writer, binding, projection);
    };
    let rows = review.archive();
    let view = review.planning_view();
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
    copy_sections(db, writer, binding, provider, projection, limits).await?;
    for (i, entry) in review
        .entries()
        .iter()
        .enumerate()
        .filter(|_| projection == Projection::PrivateRecovery)
    {
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

fn confirmed_facts<W: Write>(writer: &mut Writer<W>, binding: &ConfirmedBinding) -> Result<()> {
    let review = binding.review();
    let view = review.planning_view();
    semantic(writer, view)?;
    confirmation_facts(writer, binding)
}

fn confirmation_facts<W: Write>(writer: &mut Writer<W>, binding: &ConfirmedBinding) -> Result<()> {
    let review = binding.review();
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
    Ok(())
}

async fn copy_sections<W: Write>(
    db: &DatabaseConnection,
    writer: &mut Writer<W>,
    binding: &ConfirmedBinding,
    entry: &PlanEntry,
    projection: Projection,
    limits: Option<(
        &crate::pinning::management::retirement::RunLimits,
        Arc<PageMeter>,
    )>,
) -> Result<()> {
    let mut file = files::stream::temporary().map_err(|_| SafeError::UnknownEvidence)?;
    let prepared = projection == Projection::Review;
    let mut output: Box<dyn Write + '_> = match limits {
        Some((limits, pages)) => Box::new(crate::pinning::management::retirement::PageOutput::new(
            &mut file, limits, pages,
        )),
        None => Box::new(&mut file),
    };
    let document = if prepared {
        records::copy_prepared_output_document(db, binding, entry, &mut output, writer.budget_mut())
            .await
    } else {
        records::copy_original_inventory_document(
            db,
            binding,
            entry,
            &mut output,
            writer.budget_mut(),
        )
        .await
    }
    .map_err(storage)?;
    drop(output);
    file.seek(SeekFrom::Start(0))
        .map_err(|_| SafeError::UnknownEvidence)?;
    let mut reader = match Reader::new_metered(file, projection, writer.take_budget()) {
        Ok(reader) => reader,
        Err((error, budget)) => {
            writer.restore_budget(*budget);
            return Err(error);
        }
    };
    let enabled = reader.enable_buffering();
    writer.restore_budget(reader.take_budget());
    enabled?;
    let mut include = false;
    let mut facts_written = false;
    loop {
        reader.restore_budget(writer.take_budget());
        let event = reader.next_record();
        writer.restore_budget(reader.take_budget());
        let Some(event) = event? else { break };
        let _event_resident = match &event {
            Record::Event(_) => Some(
                writer
                    .budget_mut()
                    .reserve(reader.last_frame_resident())
                    .map_err(storage)?,
            ),
            Record::ShortCell(_) => None,
        };
        if let Record::Event(Event::Frame(frame)) = &event {
            document
                .check_frame(frame, writer.budget_mut())
                .map_err(storage)?;
        }
        if let Record::Event(Event::Frame(Frame::Row { section, key, .. })) = &event {
            if *section == Section::Inventory
                && !facts_written
                && (!prepared || key.table.starts_with("retirement_"))
            {
                if prepared {
                    confirmation_facts(writer, binding)?;
                } else {
                    confirmed_facts(writer, binding)?;
                }
                facts_written = true;
            }
            include = (prepared && *section == Section::Result)
                || matches!(
                    section,
                    Section::Native | Section::Membership | Section::Inventory
                );
        }
        if include {
            match event {
                Record::Event(Event::Frame(frame)) => {
                    writer.frame(&frame)?;
                    if matches!(frame, Frame::RowEnd) {
                        include = false;
                    }
                }
                Record::Event(Event::Chunk(bytes)) => writer
                    .cell_output()
                    .write_all(&bytes)
                    .map_err(|_| SafeError::UnknownEvidence)?,
                Record::ShortCell(held) => {
                    writer.complete_cell(&held.cell.column, &held.cell.bytes, held.cell.value)?
                }
            }
        }
    }
    if !reader.verified() || !facts_written {
        return Err(SafeError::UnknownEvidence);
    }
    Ok(())
}
