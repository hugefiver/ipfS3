use super::*;
use crate::pinning::management::{
    Snapshot,
    retirement::{InventorySpool, RunLimits, SourceCheckpoint, SourceTransition},
    wire::v3::{self, AppliedTrust, Frame, Projection, Writer},
};
use native::meter::{Budget, CELL_SLICE};
use sea_orm::DatabaseConnection;
use sha2::digest::common::hazmat::{SerializableState, SerializedState};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom};

fn wire_error(error: crate::pinning::management::SafeError) -> ManagementError {
    if error == crate::pinning::management::SafeError::LimitExceeded {
        ManagementError::Budget
    } else {
        ManagementError::InvalidEvidence
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store::pinning::management::records) struct Publication {
    pub bound: BoundInventory,
    pub checkpoint: SourceCheckpoint,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommonPublication {
    request: PlanRequest,
    items: Vec<Publication>,
    output: Option<OutputPlan>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputPlan {
    items: Vec<PlanEntry>,
    summary: crate::pinning::management::Summary,
}

impl CommonPublication {
    fn check(&self, header: &segmented::ArchiveHeader) -> Result<()> {
        crate::pinning::management::validate_request(&self.request)
            .map_err(|_| ManagementError::Confirmation)?;
        if self.items.is_empty() || self.items.len() > crate::pinning::management::MAX_ITEMS {
            return Err(ManagementError::InvalidEvidence);
        }
        let primary = &self.items[0];
        if let Some(output) = &self.output
            && (output.items.len() != self.request.items.len()
                || output.items.iter().enumerate().any(|(index, entry)| {
                    !self.request.items.contains(&entry.request)
                        || output.items[..index]
                            .iter()
                            .any(|prior| prior.request.item_id == entry.request.item_id)
                })
                || self
                    .items
                    .iter()
                    .any(|item| !output.items.contains(&item.bound.entry)))
        {
            return Err(ManagementError::Confirmation);
        }
        for (index, item) in self.items.iter().enumerate() {
            let Some(Capture::Provider(capture)) = &item.bound.entry.capture else {
                return Err(ManagementError::InvalidEvidence);
            };
            let mut expected = primary.checkpoint.clone();
            expected.route = item.checkpoint.route.clone();
            if item.bound.scope != header.database_scope
                || item.bound.identity != header.database_identity
                || item.bound.principal != header.principal
                || item.bound.plan != primary.bound.plan
                || item.bound.plan != self.request.plan_id
                || !self.request.items.contains(&item.bound.entry.request)
                || item.checkpoint != expected
                || !item.checkpoint.complete()
                || item.checkpoint.database_scope != header.database_scope
                || item.checkpoint.database_identity != header.database_identity
                || item.checkpoint.catalog != header.catalog
                || item.bound.entry.request.subject != (crate::pinning::management::Subject::Provider { route: item.checkpoint.route.clone() })
                || self.items[..index].iter().any(|prior| {
                    prior.bound.entry.request.item_id == item.bound.entry.request.item_id
                        || matches!(&prior.bound.entry.capture, Some(Capture::Provider(prior)) if prior.inventory == capture.inventory)
                })
            {
                return Err(ManagementError::Confirmation);
            }
        }
        if !header.acknowledgements.is_empty()
            || header.expires_at.is_some()
            || header.created_at != primary.checkpoint.observed_at.to_rfc3339()
        {
            return Err(ManagementError::Confirmation);
        }
        Ok(())
    }

    fn selected(&self, entry: &PlanEntry) -> Result<&Publication> {
        self.items
            .iter()
            .find(|item| item.bound.entry == *entry)
            .ok_or(ManagementError::Confirmation)
    }
}

fn archive_id(scope: &str, plan: &str, entries: &[PlanEntry]) -> Result<String> {
    let source = entries
        .iter()
        .find_map(|entry| match &entry.capture {
            Some(Capture::Provider(capture)) => Some(&capture.inventory.archive_id),
            _ => None,
        })
        .ok_or(ManagementError::Confirmation)?;
    Ok(format!(
        "inventory-plan:{}",
        serde_json::to_string(&(scope, plan, source)).expect("string tuple")
    ))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveProgress {
    writer: Option<v3::WriterContinuation>,
    export: crate::pinning::management::retirement::ArchiveExport,
    hash: String,
    metered_before: crate::pinning::management::retirement::TaskWork,
    metered_after: crate::pinning::management::retirement::TaskWork,
}

struct ArchivePage<'a> {
    header: &'a segmented::ArchiveHeader,
    file: std::fs::File,
    previous: Option<segmented::progress::Position>,
    progress: ArchiveProgress,
    complete: bool,
    checkpoint: &'a SourceCheckpoint,
}

/// Full originals are staged in bounded immutable chunks before the source
/// writer boundary. Only a confirmed record can publish their authority.
pub(crate) async fn publish(
    db: &DatabaseConnection,
    auth: &LocalAuthorization,
    request: &PlanRequest,
    entries: &[PlanEntry],
    (spools, limits): (&[InventorySpool], &RunLimits),
    work: &SharedDecodeBudget,
    effects: &mut crate::pinning::management::retirement::ConfirmationEffects,
) -> Result<bool> {
    publish_document(
        db,
        auth,
        request,
        entries,
        (spools, limits, work, effects),
        None,
    )
    .await
}

pub(crate) async fn publish_output(
    db: &DatabaseConnection,
    auth: &LocalAuthorization,
    request: &PlanRequest,
    entries: &[PlanEntry],
    (spools, limits, work, effects): (
        &[InventorySpool],
        &RunLimits,
        &SharedDecodeBudget,
        &mut crate::pinning::management::retirement::ConfirmationEffects,
    ),
    snapshot: &Snapshot,
    summary: crate::pinning::management::Summary,
) -> Result<bool> {
    publish_document(
        db,
        auth,
        request,
        entries,
        (spools, limits, work, effects),
        Some((snapshot, summary)),
    )
    .await
}

async fn publish_document(
    db: &DatabaseConnection,
    auth: &LocalAuthorization,
    request: &PlanRequest,
    entries: &[PlanEntry],
    (spools, limits, work, effects): (
        &[InventorySpool],
        &RunLimits,
        &SharedDecodeBudget,
        &mut crate::pinning::management::retirement::ConfirmationEffects,
    ),
    output_plan: Option<(&Snapshot, crate::pinning::management::Summary)>,
) -> Result<bool> {
    let spool = spools.first().ok_or(ManagementError::InvalidEvidence)?;
    let task = spool
        .task
        .as_ref()
        .ok_or(ManagementError::InvalidEvidence)?;
    let metered_before = task.total().await?;
    let review = spool.frozen_review().await?;
    if entries.len() > crate::pinning::management::MAX_ITEMS {
        return Err(ManagementError::Budget);
    }
    let mut publication = CommonPublication {
        request: request.clone(),
        items: Vec::new(),
        output: output_plan.map(|(_, summary)| OutputPlan {
            items: entries.to_vec(),
            summary,
        }),
    };
    for entry in entries {
        let Some(Capture::Provider(capture)) = &entry.capture else {
            continue;
        };
        let source = spools
            .iter()
            .find(|source| source.reference() == &capture.inventory)
            .ok_or(ManagementError::Confirmation)?;
        publication.items.push(Publication {
            bound: BoundInventory {
                scope: auth.database_scope.clone(),
                identity: auth.database_identity.clone(),
                principal: auth.principal.clone(),
                plan: request.plan_id.clone(),
                entry: entry.clone(),
            },
            checkpoint: source.checkpoint().await?,
        });
    }
    let configuration = review.configuration().await?;
    let header = segmented::ArchiveHeader {
        database_scope: auth.database_scope.clone(),
        database_identity: auth.database_identity.clone(),
        principal: auth.principal.clone(),
        catalog: review.checkpoint().catalog.clone(),
        request: serde_json::to_string(&publication)
            .map_err(|_| ManagementError::InvalidEvidence)?,
        configuration: serde_json::to_string(&configuration)
            .map_err(|_| ManagementError::InvalidEvidence)?,
        acknowledgements: Vec::new(),
        created_at: review.checkpoint().observed_at.to_rfc3339(),
        expires_at: None,
    };
    publication.check(&header)?;
    let mut id = archive_id(&auth.database_scope, &request.plan_id, entries)?;
    if output_plan.is_some() {
        id.push_str(":review");
    }
    let raw = serde_json::to_string(&header).map_err(|_| ManagementError::InvalidEvidence)?;
    let previous = segmented::progress::load(db, &id, &raw, false).await?;
    if previous.as_ref().is_some_and(|position| position.sealed) {
        return Ok(true);
    }
    let mut progress = if let Some(previous) = &previous {
        serde_json::from_str::<ArchiveProgress>(&previous.cursor)
            .map_err(|_| ManagementError::InvalidEvidence)?
    } else {
        for item in &publication.items {
            let Some(Capture::Provider(capture)) = &item.bound.entry.capture else {
                unreachable!()
            };
            let source = spools
                .iter()
                .find(|source| source.reference() == &capture.inventory)
                .ok_or(ManagementError::Confirmation)?;
            let review = source.frozen_review().await?;
            review
                .stage_evidence_into(spool, &request.plan_id, &item.bound.entry)
                .await?;
            prepare_retirement_review(auth, &review, request, &item.bound.entry, work).await?;
        }
        let mut hash = Sha256::new_with_prefix(b"S75A1\0");
        hash.update((raw.len() as u64).to_be_bytes());
        hash.update(raw.as_bytes());
        ArchiveProgress {
            writer: None,
            export: spool.publication_export(&request.plan_id, entries).await?,
            hash: hex::encode(SerializableState::serialize(&hash)),
            metered_before,
            metered_after: metered_before,
        }
    };
    let first = progress.writer.is_none();
    progress.metered_before = metered_before;
    let (mut accounting, budget) = match &spool.task {
        Some(task) => {
            let (accounting, budget) = task.begin_output(limits).await?;
            (Some(accounting), budget)
        }
        None => (None, v3::files::stream::budget()),
    };
    let output = v3::files::stream::temporary().map_err(|_| ManagementError::Database)?;
    let initialized = match progress.writer.take() {
        Some(continuation) => Writer::continue_metered(output, continuation, budget),
        None => Writer::new_metered(
            output,
            if output_plan.is_some() {
                Projection::Review
            } else {
                Projection::PrivateRecovery
            },
            budget,
        ),
    };
    let mut writer = match initialized {
        Ok(writer) => writer,
        Err((error, budget)) => {
            if let Some(accounting) = accounting {
                accounting.finish(budget.work).await?;
            }
            return Err(wire_error(error));
        }
    };
    let written = async {
        if first && let Some((snapshot, summary)) = output_plan {
            v3::planning::prefix(
                &mut writer,
                v3::planning::Header {
                    projection: Projection::Review,
                    database_scope: &header.database_scope,
                    database_identity: &header.database_identity,
                    principal: &header.principal,
                    catalog: &header.catalog,
                    created_at: header.created_at.clone(),
                    expires_at: None,
                },
                request,
                &snapshot.configuration,
                entries,
                summary,
                None,
            )
            .map_err(wire_error)?;
        } else if first {
            writer
                .frame(&Frame::Header {
                    schema_version: v3::SCHEMA_VERSION,
                    projection: Projection::PrivateRecovery,
                    database_scope: auth.database_scope.clone(),
                    database_identity: auth.database_identity.clone(),
                    principal: auth.principal.clone(),
                    applied_trust: AppliedTrust::UntrustedClaimsOnly,
                    catalog: header.catalog.clone(),
                    created_at: header.created_at.clone(),
                    expires_at: None,
                })
                .map_err(wire_error)?;
            writer
                .frame(&Frame::Request { request: None })
                .map_err(wire_error)?;
            writer
                .frame(&Frame::Configuration {
                    configuration: Some(configuration),
                })
                .map_err(wire_error)?;
            writer
                .frame(&Frame::Acknowledgements { item_ids: None })
                .map_err(wire_error)?;
            writer
                .frame(&Frame::Summary {
                    summary: Default::default(),
                })
                .map_err(wire_error)?;
        }
        let capped;
        let source_limits = if let Some(task) = &spool.task {
            capped = task.caps(spool, limits).await?;
            &capped
        } else {
            limits
        };
        let advanced = match output_plan {
            Some((snapshot, _)) => {
                progress
                    .export
                    .advance_planning_page(spool, &mut writer, snapshot, source_limits)
                    .await?
            }
            None => {
                progress
                    .export
                    .advance_page(spool, &mut writer, source_limits)
                    .await?
            }
        };
        if advanced == crate::pinning::management::retirement::Progress::Archived
            && output_plan.is_some()
        {
            for (index, entry) in entries.iter().enumerate() {
                v3::planning::fact(
                    &mut writer,
                    v3::Section::Result,
                    "management_capture",
                    index,
                    &entry.capture,
                )
                .map_err(wire_error)?;
            }
        }
        Ok(advanced)
    }
    .await;
    let complete = matches!(
        written,
        Ok(crate::pinning::management::retirement::Progress::Archived)
    );
    let (output, mut budget) = match written {
        Ok(_) if complete => {
            let (output, budget) = writer.finalize();
            (output.map_err(wire_error), budget)
        }
        Ok(_) => {
            let (paused, budget) = writer.pause();
            let output = paused
                .map(|(output, continuation)| {
                    progress.writer = Some(continuation);
                    output
                })
                .map_err(wire_error);
            (output, budget)
        }
        Err(error) => (Err(error), writer.abort()),
    };
    spool.forward_confirmation_work().await?;
    let result = async {
        let file = output?;
        let bytes = file
            .metadata()
            .map_err(|_| ManagementError::Database)?
            .len();
        let pages = bytes.div_ceil(CELL_SLICE as u64);
        budget.charge(
            pages + 8,
            pages + 8,
            pages,
            bytes
                .checked_mul(2)
                .and_then(|n| n.checked_add(raw.len() as u64))
                .ok_or(ManagementError::Budget)?,
        )?;
        if let Some(accounting) = &mut accounting {
            accounting.persist(budget.work).await?;
        }
        progress.metered_after = task.total().await?;
        persist_archive(
            db,
            auth,
            &id,
            ArchivePage {
                header: &header,
                file,
                previous,
                progress,
                complete,
                checkpoint: review.checkpoint(),
            },
            &mut budget,
            effects,
            limits,
        )
        .await
    }
    .await;
    let accounted = match accounting {
        Some(accounting) => accounting.finish(budget.work).await,
        None => Ok(()),
    };
    match (result, accounted) {
        (Err(error), _) => return Err(error),
        (_, Err(error)) => return Err(error),
        _ => {}
    }
    Ok(complete)
}

async fn persist_archive(
    db: &DatabaseConnection,
    auth: &LocalAuthorization,
    id: &str,
    page: ArchivePage<'_>,
    budget: &mut Budget,
    effects: &mut crate::pinning::management::retirement::ConfirmationEffects,
    limits: &RunLimits,
) -> Result<()> {
    let ArchivePage {
        header,
        mut file,
        previous,
        mut progress,
        complete,
        checkpoint,
    } = page;
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ManagementError::Database)?;
    let id = id.to_owned();
    let raw = serde_json::to_string(header).map_err(|_| ManagementError::InvalidEvidence)?;
    if raw.len() > native::meter::PAGE_BYTES {
        return Err(ManagementError::Budget);
    }
    budget.check()?;
    let hash = hex::decode(&progress.hash).map_err(|_| ManagementError::InvalidEvidence)?;
    let hash = SerializedState::<Sha256>::try_from(hash.as_slice())
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let mut hash = <Sha256 as SerializableState>::deserialize(&hash)
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let mut buffer = vec![0; CELL_SLICE];
    let mut chunks = previous.as_ref().map_or(0, |p| p.chunks);
    let mut payload = Vec::new();
    let _resident = budget.reserve(8 * native::meter::PAGE_BYTES)?;
    loop {
        budget.check()?;
        let n = file
            .read(&mut buffer)
            .map_err(|_| ManagementError::Database)?;
        if n == 0 {
            break;
        }
        budget.check()?;
        if n > 8 * native::meter::PAGE_BYTES - payload.len() {
            return Err(ManagementError::Budget);
        }
        payload.extend_from_slice(&buffer[..n]);
    }
    for chunk in payload.chunks(CELL_SLICE) {
        hash.update((chunks as u64).to_be_bytes());
        hash.update((chunk.len() as u64).to_be_bytes());
        hash.update(chunk);
        chunks += 1;
    }
    budget.check()?;
    progress.hash = hex::encode(SerializableState::serialize(&hash));
    let cursor = if complete {
        hex::encode(hash.finalize())
    } else {
        serde_json::to_string(&progress).map_err(|_| ManagementError::InvalidEvidence)?
    };
    use crate::store::pinning::management::transaction::{
        ManagementTransaction, TransactionFailure,
    };
    let mut tx = ManagementTransaction::begin(db, limits.remaining(), limits.cancellation.clone())
        .await
        .map_err(TransactionFailure::management_error)?;
    let (before, after) = tx
        .run_with(
            (
                id.clone(),
                raw.clone(),
                previous,
                cursor,
                checkpoint.clone(),
                &payload,
                auth,
            ),
            move |tx, _, (id, raw, previous, cursor, checkpoint, payload, auth)| {
                Box::pin(async move {
                    native::scope::writer_boundary(tx).await?;
                    checkpoint.check(tx).await?;
                    let stored = segmented::progress::load(tx, id, raw, true).await?;
                    if &stored != previous {
                        return Err(ManagementError::Changed);
                    }
                    let before = match stored {
                        Some(before) => before,
                        None => {
                            segmented::progress::create(
                                tx,
                                id,
                                raw,
                                &auth.database_scope,
                                &auth.database_identity,
                                "initial",
                            )
                            .await?
                        }
                    };
                    let after = segmented::progress::append(
                        tx, id, raw, &before, cursor, payload, complete,
                    )
                    .await?;
                    checkpoint.check(tx).await?;
                    Ok((before, after))
                })
            },
        )
        .await
        .map_err(|failure| {
            effects.commit_unknown |= !failure.rollback_confirmed;
            failure.management_error()
        })?;
    match tx.commit().await {
        Ok(()) => effects.database_commits += 1,
        Err(failure) => {
            match segmented::progress::committed(db, &id, &raw, &before, &after, &payload).await {
                Ok(true) => effects.database_commits += 1,
                Ok(false) => {
                    effects.commit_unknown |= !failure.rollback_confirmed;
                    return Err(failure.management_error());
                }
                Err(_) => {
                    effects.commit_unknown = true;
                    return Err(ManagementError::Database);
                }
            }
        }
    }
    Ok(())
}

async fn reference<C: ConnectionTrait>(db: &C, id: String) -> Result<segmented::ArchiveRef> {
    segmented::reference(db, &id)
        .await?
        .ok_or(ManagementError::Confirmation)
}

pub(crate) async fn prepared_output_reference<C: ConnectionTrait>(
    db: &C,
    scope: &str,
    plan: &str,
    entries: &[PlanEntry],
    budget: &mut Budget,
) -> Result<segmented::ArchiveRef> {
    budget.charge(1, 1, 3, 0)?;
    let mut id = archive_id(scope, plan, entries)?;
    id.push_str(":review");
    reference(db, id).await
}

pub async fn copy_original_inventory<C: ConnectionTrait, W: std::io::Write>(
    db: &C,
    binding: &ConfirmedBinding,
    entry: &PlanEntry,
    output: &mut W,
    budget: &mut Budget,
) -> Result<()> {
    copy_original_inventory_document(db, binding, entry, output, budget).await?;
    Ok(())
}

pub(crate) struct OriginalInventoryDocument {
    header: segmented::ArchiveHeader,
    reference: segmented::ArchiveRef,
    _resident: native::meter::ResidentGuard,
    _header_resident: native::meter::ResidentGuard,
    request: PlanRequest,
    output: Option<OutputPlan>,
}

impl OriginalInventoryDocument {
    pub(crate) fn check_frame(&self, frame: &Frame, budget: &mut Budget) -> Result<()> {
        budget.check()?;
        let header = &self.header;
        let valid = match frame {
            Frame::Header {
                schema_version,
                projection,
                database_scope,
                database_identity,
                principal,
                applied_trust,
                catalog,
                created_at,
                expires_at,
            } => {
                *schema_version == v3::SCHEMA_VERSION
                    && *projection
                        == if self.output.is_some() {
                            Projection::Review
                        } else {
                            Projection::PrivateRecovery
                        }
                    && *applied_trust == AppliedTrust::UntrustedClaimsOnly
                    && database_scope == &header.database_scope
                    && database_identity == &header.database_identity
                    && principal == &header.principal
                    && catalog == &header.catalog
                    && created_at == &header.created_at
                    && expires_at == &header.expires_at
            }
            Frame::Request { request } => {
                if self.output.is_some() {
                    request.as_ref() == Some(&self.request)
                } else {
                    request.is_none()
                }
            }
            Frame::Configuration { configuration } => {
                budget.charge(0, 0, 0, header.configuration.len() as u64)?;
                let expected = serde_json::from_str::<
                    Option<crate::pinning::management::ConfigurationBinding>,
                >(&header.configuration)
                .map_err(|_| ManagementError::InvalidEvidence)?;
                configuration == &expected
            }
            Frame::Acknowledgements { item_ids } => item_ids.is_none(),
            Frame::Summary { summary } => {
                *summary
                    == self
                        .output
                        .as_ref()
                        .map_or_else(Default::default, |output| output.summary)
            }
            Frame::Item { entry } => self.output.as_ref().is_some_and(|output| {
                output
                    .items
                    .iter()
                    .any(|item| *entry == Box::<v3::ItemFact>::from(item))
            }),
            _ => true,
        };
        if valid {
            Ok(())
        } else {
            Err(ManagementError::Confirmation)
        }
    }
}

pub(crate) async fn copy_original_inventory_document<C: ConnectionTrait, W: std::io::Write>(
    db: &C,
    binding: &ConfirmedBinding,
    entry: &PlanEntry,
    output: &mut W,
    budget: &mut Budget,
) -> Result<OriginalInventoryDocument> {
    copy_document(db, binding, entry, output, budget, false).await
}

async fn copy_document<C: ConnectionTrait, W: std::io::Write>(
    db: &C,
    binding: &ConfirmedBinding,
    entry: &PlanEntry,
    output: &mut W,
    budget: &mut Budget,
    prepared: bool,
) -> Result<OriginalInventoryDocument> {
    let reference = document_reference(db, binding, entry, budget, prepared).await?;
    let _raw_memory = budget.reserve(native::meter::PAGE_BYTES)?;
    let raw = segmented::read_archive_raw(db, &reference, output, budget).await?;
    let header = segmented::header::DecodedHeader::decode(&raw, budget)?;
    check_document(binding, entry, budget, prepared, reference, header)
}

pub(crate) async fn open_original_inventory_document<C: ConnectionTrait>(
    db: &C,
    binding: &ConfirmedBinding,
    entry: &PlanEntry,
    budget: &mut Budget,
    prepared: bool,
) -> Result<(OriginalInventoryDocument, segmented::pages::Pages)> {
    let reference = document_reference(db, binding, entry, budget, prepared).await?;
    let (header, pages) = segmented::pages::Pages::open(db, &reference, budget).await?;
    let document = check_document(binding, entry, budget, prepared, reference, header)?;
    Ok((document, pages))
}

async fn document_reference<C: ConnectionTrait>(
    db: &C,
    binding: &ConfirmedBinding,
    entry: &PlanEntry,
    budget: &mut Budget,
    prepared: bool,
) -> Result<segmented::ArchiveRef> {
    if !binding.confirmation.bindings.contains(entry) {
        return Err(ManagementError::Confirmation);
    }
    budget.charge(1, 1, 4, 0)?;
    let mut id = archive_id(
        &binding.database_scope,
        &binding.request.plan_id,
        &binding.confirmation.bindings,
    )?;
    if prepared {
        id.push_str(":review");
    }
    reference(db, id).await
}

fn check_document(
    binding: &ConfirmedBinding,
    entry: &PlanEntry,
    budget: &mut Budget,
    prepared: bool,
    reference: segmented::ArchiveRef,
    header: segmented::header::DecodedHeader,
) -> Result<OriginalInventoryDocument> {
    let segmented::header::DecodedHeader {
        value: header,
        memory: header_resident,
    } = header;
    budget.charge(
        0,
        0,
        0,
        (header.request.len() + header.configuration.len()) as u64,
    )?;
    let resident = budget.reserve(
        header
            .request
            .len()
            .checked_add(header.configuration.len())
            .and_then(|bytes| bytes.checked_mul(8))
            .ok_or(ManagementError::Budget)?,
    )?;
    let common: CommonPublication =
        serde_json::from_str(&header.request).map_err(|_| ManagementError::InvalidEvidence)?;
    let configuration = serde_json::from_str::<
        Option<crate::pinning::management::ConfigurationBinding>,
    >(&header.configuration)
    .map_err(|_| ManagementError::InvalidEvidence)?;
    common.check(&header)?;
    if common.output.is_some() != prepared
        || common.output.as_ref().is_some_and(|output| {
            output.items != *binding.confirmation.bindings
                || output.summary != *binding.review().summary()
        })
    {
        return Err(ManagementError::Confirmation);
    }
    let publication = common.selected(entry)?;
    if &configuration != binding.review().planning_view().configuration
        || common.request != *binding.request
        || common.items.len()
            != binding
                .confirmation
                .bindings
                .iter()
                .filter(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
                .count()
        || common
            .items
            .iter()
            .any(|item| !binding.confirmation.bindings.contains(&item.bound.entry))
    {
        return Err(ManagementError::Confirmation);
    }
    if publication.bound.scope != binding.database_scope
        || publication.bound.identity != binding.database_identity
        || publication.bound.principal != binding.principal
        || publication.bound.plan != binding.request.plan_id
        || publication.bound.entry != *entry
    {
        return Err(ManagementError::Unauthorized);
    }
    Ok(OriginalInventoryDocument {
        header,
        reference,
        _resident: resident,
        _header_resident: header_resident,
        request: common.request,
        output: common.output,
    })
}

/// The publication envelope is not an ordinary v3 Request. Only this loader
/// accepts that envelope, with the selected item bound to the local original.
pub async fn load_original_inventory<C: ConnectionTrait, S: Read + std::io::Write + Seek>(
    db: &C,
    binding: &ConfirmedBinding,
    entry: &PlanEntry,
    mut file: S,
    mut budget: Budget,
) -> Result<v3::archive_cursor::ReviewCursor<S>> {
    if file
        .seek(SeekFrom::End(0))
        .map_err(|_| ManagementError::Database)?
        != 0
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let document =
        copy_original_inventory_document(db, binding, entry, &mut file, &mut budget).await?;
    let reference = document.reference.clone();
    budget.work.copied_bytes = budget
        .work
        .copied_bytes
        .checked_add(u64::try_from(reference.bytes).map_err(|_| ManagementError::InvalidEvidence)?)
        .ok_or(ManagementError::Budget)?;
    let staged = v3::archive_cursor::stage_checked(
        file,
        Projection::PrivateRecovery,
        budget,
        Some(reference),
        |frame, budget| {
            document.check_frame(frame, budget).map_err(|error| {
                if error == ManagementError::Budget {
                    crate::pinning::management::SafeError::LimitExceeded
                } else {
                    crate::pinning::management::SafeError::UnknownEvidence
                }
            })
        },
    )
    .map_err(wire_error)?;
    Ok(staged.into_cursor())
}

pub(crate) async fn prepare_apply(
    db: &DatabaseConnection,
    binding: &ConfirmedBinding,
) -> Result<()> {
    let mut budget = v3::files::stream::budget();
    if let Some(entry) = binding
        .confirmation
        .bindings
        .iter()
        .find(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
    {
        copy_original_inventory(db, binding, entry, &mut std::io::sink(), &mut budget).await?;
    }
    Ok(())
}

async fn load_publication<C: ConnectionTrait>(
    db: &C,
    scope: &str,
    plan: &str,
    entries: &[PlanEntry],
    work: &SharedDecodeBudget,
) -> Result<CommonPublication> {
    let id = archive_id(scope, plan, entries)?;
    let (rows, _) = bounded_projection(
        db,
        "FROM (SELECT archive_id,header FROM pin_management_archive WHERE state='sealed' AND format='S75A1' UNION ALL SELECT archive_id,header FROM pin_management_inline_archive WHERE format='S75I2') AS archives WHERE archive_id=$1",
        &[("header", "header")],
        "archive_id",
        vec![id.into()],
        (1, native::meter::PAGE_BYTES),
        DecodeWork::shared(work),
    )
    .await?;
    let row = rows.first().ok_or(ManagementError::Confirmation)?;
    let raw: String = row
        .try_get("", "header")
        .map_err(|_| ManagementError::Budget)?;
    let header: segmented::ArchiveHeader =
        DecodeWork::shared(work).with(|work| work.typed(&raw))?;
    let common: CommonPublication =
        DecodeWork::shared(work).with(|work| work.typed(&header.request))?;
    common.check(&header)?;
    if common.output.is_some() {
        return Err(ManagementError::Confirmation);
    }
    if header.database_scope != scope || common.request.plan_id != plan {
        return Err(ManagementError::Confirmation);
    }
    Ok(common)
}

pub(in crate::store::pinning::management::records) async fn hydrate<C: ConnectionTrait>(
    db: &C,
    review: &ReviewHandle,
    work: &SharedDecodeBudget,
) -> Result<()> {
    let providers = review
        .entries()
        .iter()
        .filter(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
        .collect::<Vec<_>>();
    if providers.is_empty() {
        return Ok(());
    }
    let common = load_publication(
        db,
        review.scope(),
        &review.request().plan_id,
        review.entries(),
        work,
    )
    .await?;
    if common.request != *review.request() || common.items.len() != providers.len() {
        return Err(ManagementError::Confirmation);
    }
    for entry in providers {
        let publication = common.selected(entry)?;
        if publication.bound.scope != review.scope()
            || publication.bound.identity != review.identity()
            || publication.bound.principal != review.principal()
            || publication.bound.plan != review.request().plan_id
        {
            return Err(ManagementError::Unauthorized);
        }
        let mut work = work.lock().map_err(|_| ManagementError::InvalidEvidence)?;
        work.copy(&publication.bound)?;
        if !work.retirement_reviews.contains(&publication.bound) {
            work.retirement_reviews.push(publication.bound.clone())
        }
    }
    Ok(())
}

pub(in crate::store::pinning::management::records) async fn bind_canonical_archive(
    tx: &DatabaseTransaction,
    auth: &LocalAuthorization,
    request: &PlanRequest,
    entries: &[PlanEntry],
    origin: &SourceCheckpoint,
    work: &SharedDecodeBudget,
) -> Result<()> {
    let common =
        load_publication(tx, &auth.database_scope, &request.plan_id, entries, work).await?;
    let providers = entries
        .iter()
        .filter(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
        .collect::<Vec<_>>();
    if common.request != *request || common.items.len() != providers.len() {
        return Err(ManagementError::Confirmation);
    }
    for entry in providers {
        let publication = common.selected(entry)?;
        let mut expected = origin.clone();
        expected.route = publication.checkpoint.route.clone();
        if publication.bound.scope != auth.database_scope
            || publication.bound.identity != auth.database_identity
            || publication.bound.principal != auth.principal
            || publication.bound.plan != request.plan_id
            || publication.checkpoint != expected
        {
            return Err(ManagementError::Changed);
        }
        let mut work = work.lock().map_err(|_| ManagementError::InvalidEvidence)?;
        work.copy(&publication.bound)?;
        if !work.retirement_reviews.contains(&publication.bound) {
            work.retirement_reviews.push(publication.bound.clone());
        }
    }
    Ok(())
}

fn transition_id(scope: &str, plan: &str) -> String {
    format!(
        "inventory-confirmed:{}",
        serde_json::to_string(&(scope, plan)).expect("string tuple")
    )
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmedTransition {
    original: SourceCheckpoint,
    confirmed: SourceCheckpoint,
    changes: Vec<SourceTransition>,
}

pub(in crate::store::pinning::management::records) async fn publish_transition(
    tx: &DatabaseTransaction,
    auth: &LocalAuthorization,
    request: &PlanRequest,
    original: &SourceCheckpoint,
    changes: Vec<SourceTransition>,
) -> Result<()> {
    let mut budget = Budget::default();
    let confirmed = original
        .verify_transition_after(tx, &changes, &mut budget)
        .await?;
    let body = serde_json::to_vec(&ConfirmedTransition {
        original: original.clone(),
        confirmed,
        changes,
    })
    .map_err(|_| ManagementError::InvalidEvidence)?;
    let header = segmented::ArchiveHeader {
        database_scope: auth.database_scope.clone(),
        database_identity: auth.database_identity.clone(),
        principal: auth.principal.clone(),
        catalog: original.catalog.clone(),
        request: serde_json::to_string(request).map_err(|_| ManagementError::InvalidEvidence)?,
        configuration: String::new(),
        acknowledgements: Vec::new(),
        created_at: original.observed_at.to_rfc3339(),
        expires_at: None,
    };
    segmented::write_archive(
        tx,
        &transition_id(&auth.database_scope, &request.plan_id),
        &header,
        &mut body.as_slice(),
        &mut budget,
    )
    .await?;
    Ok(())
}

pub(in crate::store::pinning::management::records) async fn seal_confirmed(
    tx: &DatabaseTransaction,
    binding: &ConfirmedBinding,
) -> Result<()> {
    if !binding
        .confirmation
        .bindings
        .iter()
        .any(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
    {
        return Ok(());
    }
    let transition = read_confirmed_transition(tx, binding).await?;
    transition.confirmed.fence(tx).await?;
    check_confirmed_transition(tx, binding, &transition, true).await
}

async fn read_confirmed_transition(
    tx: &DatabaseTransaction,
    binding: &ConfirmedBinding,
) -> Result<ConfirmedTransition> {
    let work = BindingView::from(binding).decode_work();
    let mut budget = work.with(|work| work.storage_budget())?;
    let result: Result<_> = async {
        budget.charge(1, 1, 4, 0)?;
        let reference = reference(
            tx,
            transition_id(&binding.database_scope, &binding.request.plan_id),
        )
        .await?;
        if reference.bytes > native::meter::PAGE_BYTES as i64 {
            return Err(ManagementError::Budget);
        }
        let resident = budget.reserve(
            usize::try_from(reference.bytes).map_err(|_| ManagementError::InvalidEvidence)?,
        )?;
        let mut body = Vec::new();
        let raw = segmented::read_archive_raw(tx, &reference, &mut body, &mut budget).await?;
        Ok((raw, body, resident))
    }
    .await;
    charge_storage_work(&budget, work)?;
    let (raw, body, _resident) = result?;
    let header: segmented::ArchiveHeader = work.with(|work| work.typed(&raw))?;
    if header.database_scope != binding.database_scope
        || header.database_identity != binding.database_identity
        || header.principal != binding.principal
        || work.with(|work| work.typed::<PlanRequest>(&header.request))? != *binding.request
    {
        return Err(ManagementError::Unauthorized);
    }
    work.with(|work| {
        work.typed(std::str::from_utf8(&body).map_err(|_| ManagementError::InvalidEvidence)?)
    })
}

async fn check_confirmed_transition(
    tx: &DatabaseTransaction,
    binding: &ConfirmedBinding,
    transition: &ConfirmedTransition,
    current_fence: bool,
) -> Result<()> {
    let work = BindingView::from(binding).decode_work();
    let mut budget = work.with(|work| work.storage_budget())?;
    if transition.changes.len() != binding.request.items.len()
        || transition.changes.iter().any(|change| {
            change.before.is_some()
                || change.after.is_none()
                || change.writes != 1
                || change.key.table != crate::store::pinning::management::schema::RECORDS
                || !binding.request.items.iter().any(|item| {
                    change.key.values
                        == [
                            binding.database_scope.clone(),
                            binding.request.plan_id.clone(),
                            item.item_id.clone(),
                            "action".into(),
                        ]
                })
        })
    {
        return Err(ManagementError::Confirmation);
    }
    let checked = if current_fence {
        transition
            .original
            .verify_transition_after(tx, &transition.changes, &mut budget)
            .await
    } else {
        transition
            .original
            .verify_recorded_transition(tx, &transition.changes, &mut budget)
            .await
    };
    charge_storage_work(&budget, work)?;
    if checked? != transition.confirmed {
        return Err(ManagementError::Changed);
    }
    let common = load_publication(
        tx,
        &binding.database_scope,
        &binding.request.plan_id,
        &binding.confirmation.bindings,
        work.budget(),
    )
    .await?;
    let providers = binding
        .confirmation
        .bindings
        .iter()
        .filter(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
        .collect::<Vec<_>>();
    if common.request != *binding.request || common.items.len() != providers.len() {
        return Err(ManagementError::Confirmation);
    }
    for entry in providers {
        let publication = common.selected(entry)?;
        let mut expected = transition.original.rewind();
        expected.route = publication.checkpoint.route.clone();
        if publication.checkpoint.rewind() != expected {
            return Err(ManagementError::Changed);
        }
    }
    Ok(())
}

pub(in crate::store::pinning::management::records) async fn check_checkpoint_confirmation(
    tx: &DatabaseTransaction,
    binding: &ConfirmedBinding,
    origin: &SourceCheckpoint,
    inventories: &[crate::pinning::management::retirement::RetirementInventoryRef],
) -> Result<()> {
    let transition = read_confirmed_transition(tx, binding).await?;
    let mut expected = origin.rewind();
    expected.route = transition.original.route.clone();
    if expected != transition.original.rewind() {
        return Err(ManagementError::Changed);
    }
    let captures = binding
        .confirmation
        .bindings
        .iter()
        .filter_map(|entry| {
            if let Some(Capture::Provider(capture)) = &entry.capture {
                Some(&capture.inventory)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if captures.is_empty()
        || captures.len() != inventories.len()
        || !captures.iter().all(|capture| inventories.contains(capture))
    {
        return Err(ManagementError::Confirmation);
    }
    check_confirmed_transition(tx, binding, &transition, false).await
}

pub(crate) async fn finish_apply(
    tx: &DatabaseTransaction,
    binding: &ConfirmedBinding,
    changes: Vec<SourceTransition>,
) -> Result<()> {
    if !binding
        .confirmation
        .bindings
        .iter()
        .any(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
    {
        return Ok(());
    }
    let reference = reference(
        tx,
        transition_id(&binding.database_scope, &binding.request.plan_id),
    )
    .await?;
    if reference.bytes > native::MAX_BYTES as i64 {
        return Err(ManagementError::Budget);
    }
    let mut body = Vec::new();
    let mut budget = Budget::default();
    let header = segmented::read_archive(tx, &reference, &mut body, &mut budget).await?;
    let previous: ConfirmedTransition =
        serde_json::from_slice(&body).map_err(|_| ManagementError::InvalidEvidence)?;
    let confirmed = previous
        .confirmed
        .verify_transition_after(tx, &changes, &mut budget)
        .await?;
    let body = serde_json::to_vec(&ConfirmedTransition {
        original: previous.confirmed,
        confirmed,
        changes,
    })
    .map_err(|_| ManagementError::InvalidEvidence)?;
    let id = format!(
        "inventory-applied:{}",
        serde_json::to_string(&(&binding.database_scope, &binding.request.plan_id))
            .map_err(|_| ManagementError::InvalidEvidence)?
    );
    segmented::write_archive(tx, &id, &header, &mut body.as_slice(), &mut budget).await?;
    Ok(())
}
