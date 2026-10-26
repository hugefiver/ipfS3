use super::super::codec::physical::{CheckedRecord, Decoder, Sink, Value};
use super::*;
use crate::pinning::management::retirement::PageMeter;
use crate::store::pinning::management::{ManagementError, records};
use sea_orm::DatabaseConnection;
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
    if let Err(error) = copy_sections(db, writer, binding, provider, projection, limits).await {
        writer.invalidate();
        return Err(error);
    }
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
    let prepared = projection == Projection::Review;
    let (document, mut pages) = records::open_original_inventory_document(
        db,
        binding,
        entry,
        writer.budget_mut(),
        prepared,
    )
    .await
    .map_err(storage)?;
    let mut decoder = Decoder::new(projection, writer.budget_mut())?;
    let mut input_pages = limits.map(|(limits, pages)| {
        crate::pinning::management::retirement::PageOutput::new(std::io::sink(), limits, pages)
    });
    let mut sink = Sections {
        writer,
        binding,
        document,
        prepared,
        include: false,
        facts_written: false,
    };
    while let Some(page) = pages
        .next(db, sink.writer.budget_mut())
        .await
        .map_err(storage)?
    {
        for chunk in &page.chunks {
            if let Some(pages) = &mut input_pages {
                pages
                    .write_all(chunk)
                    .map_err(|_| SafeError::LimitExceeded)?;
            }
            decoder.feed(chunk, &mut sink)?;
        }
    }
    decoder.finish(sink.writer.budget())?;
    if !sink.facts_written {
        return Err(SafeError::UnknownEvidence);
    }
    Ok(())
}

struct Sections<'a, W> {
    writer: &'a mut Writer<W>,
    binding: &'a ConfirmedBinding,
    document: records::OriginalInventoryDocument,
    prepared: bool,
    include: bool,
    facts_written: bool,
}
impl<W: Write> Sink for Sections<'_, W> {
    fn budget(&mut self) -> &mut crate::store::pinning::management::native::meter::Budget {
        self.writer.budget_mut()
    }
    fn record(&mut self, record: CheckedRecord<'_>) -> Result<()> {
        let Self {
            writer,
            binding,
            document,
            prepared,
            include,
            facts_written,
        } = self;
        if let Value::Frame(frame) = &record.value {
            document
                .check_frame(frame, writer.budget_mut())
                .map_err(storage)?;
        }
        if let Value::Frame(Frame::Row { section, key, .. }) = &record.value {
            if *section == Section::Inventory
                && !*facts_written
                && (!*prepared || key.table.starts_with("retirement_"))
            {
                if *prepared {
                    confirmation_facts(writer, binding)?;
                } else {
                    confirmed_facts(writer, binding)?;
                }
                *facts_written = true;
            }
            *include = (*prepared && *section == Section::Result)
                || matches!(
                    section,
                    Section::Native | Section::Membership | Section::Inventory
                );
        }
        if *include && !record.terminal() {
            writer.checked_record(&record)?;
            if matches!(record.value, Value::Frame(Frame::RowEnd)) {
                *include = false;
            }
        }
        Ok(())
    }
}
