use super::*;
use crate::store::pinning::management::records::segmented::{self, progress};
use sea_orm::TransactionTrait;
use sha2::digest::common::hazmat::{SerializableState, SerializedState};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom};

const OUTPUT: &str = "planning-review-output";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedOutputBinding {
    projection: Projection,
    archive_header: segmented::ArchiveHeader,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    writer: Option<v3::WriterContinuation>,
    export: ArchiveExport,
    hash: String,
}

fn snapshot_digest(snapshot: &Snapshot, budget: &mut Budget) -> Result<String> {
    struct Hash<'a>(Sha256, &'a mut Budget);
    impl Write for Hash<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.1
                .charge(0, 0, 0, bytes.len() as u64)
                .map_err(std::io::Error::other)?;
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut hash = Hash(Sha256::new_with_prefix(b"S75-output-binding-1\0"), budget);
    let result = serde_json::to_writer(&mut hash, snapshot);
    hash.1.check()?;
    result.map_err(|_| ManagementError::InvalidEvidence)?;
    Ok(hex::encode(hash.0.finalize()))
}

impl PreparedPlanning {
    /// Prepare a complete untrusted output in the existing owner-private task
    /// spool. No target database connection or confirmation authority is used.
    pub async fn prepare_output(
        &self,
        mut budget: Budget,
        header: v3::planning::Header<'_>,
        request: &PlanRequest,
        snapshot: &Snapshot,
        limits: &RunLimits,
    ) -> (Result<Progress>, Budget) {
        let production = limits.for_production();
        let limits = &production;
        let Some(primary) = self.spools.first() else {
            return (Ok(Progress::Archived), budget);
        };
        let Some(task) = primary.task.as_deref() else {
            return (Err(ManagementError::InvalidEvidence), budget);
        };
        let mut accounting = match task.adopt_output(limits, &mut budget, true).await {
            Ok(accounting) => accounting,
            Err(error) => return (Err(error), budget),
        };
        let initialized = async {
            if header.expires_at.is_some() {
                return Err(ManagementError::Confirmation);
            }
            // These are spool-local instances of the archive protocol, never
            // management tables in the target database.
            budget.charge(2, 0, 0, 0)?;
            for sql in [
                "CREATE TABLE IF NOT EXISTS pin_management_archive(archive_id TEXT PRIMARY KEY,format TEXT NOT NULL,database_scope TEXT NOT NULL,native_identity TEXT NOT NULL,header TEXT NOT NULL,state TEXT NOT NULL,chunks BIGINT NOT NULL DEFAULT 0,bytes BIGINT NOT NULL DEFAULT 0,digest TEXT NOT NULL DEFAULT '')",
                "CREATE TABLE IF NOT EXISTS pin_management_archive_chunks(archive_id TEXT NOT NULL,ordinal BIGINT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(archive_id,ordinal)) WITHOUT ROWID",
            ] { spool::exec(&primary.db, sql, vec![]).await?; }
            let bound = segmented::ArchiveHeader {
                database_scope: header.database_scope.into(), database_identity: header.database_identity.into(),
                principal: header.principal.into(), catalog: header.catalog.to_vec(),
                request: encode(&(request, self.plan.items(), self.plan.summary(), snapshot_digest(snapshot, &mut budget)?))?,
                configuration: encode(&snapshot.configuration)?, acknowledgements: Vec::new(),
                created_at: header.created_at.clone(), expires_at: None,
            };
            let raw = encode(&PreparedOutputBinding { projection: header.projection, archive_header: bound })?;
            budget.charge(1, 1, 1, raw.len() as u64)?;
            let previous = progress::load(&primary.db, OUTPUT, &raw, false).await?;
            if previous.as_ref().is_some_and(|position| position.sealed) {
                return Ok((raw, previous, None));
            }
            let cursor = if let Some(previous) = &previous {
                budget.charge(0, 0, 0, previous.cursor.len() as u64)?;
                let cursor = decode::<Cursor>(&previous.cursor)?;
                if cursor
                    .writer
                    .as_ref()
                    .is_none_or(|writer| writer.projection() != header.projection)
                {
                    return Err(ManagementError::Confirmation);
                }
                cursor
            } else {
                for entry in self.plan.items() {
                    let Some(Capture::Provider(capture)) = &entry.capture else { continue };
                    let source = self.spools.iter().find(|source| source.reference() == &capture.inventory)
                        .ok_or(ManagementError::Confirmation)?;
                    source.frozen_review().await?.stage_evidence_into(primary, &request.plan_id, entry).await?;
                }
                let mut hash = Sha256::new_with_prefix(b"S75A1\0");
                hash.update((raw.len() as u64).to_be_bytes());
                hash.update(raw.as_bytes());
                Cursor { writer: None, export: primary.publication_export(&request.plan_id, self.plan.items()).await?,
                    hash: hex::encode(SerializableState::serialize(&hash)) }
            };
            Ok((raw, previous, Some(cursor)))
        }.await;
        let (raw, previous, cursor) = match initialized {
            Ok(value) => value,
            Err(error) => {
                let accounted = accounting.finish_metered(&mut budget).await;
                return (Err(accounted.err().unwrap_or(error)), budget);
            }
        };
        let Some(mut cursor) = cursor else {
            let result = accounting
                .finish_metered(&mut budget)
                .await
                .and_then(|()| limits.check())
                .map(|()| Progress::Archived);
            return (result, budget);
        };
        let file = match v3::files::stream::temporary() {
            Ok(file) => file,
            Err(_) => {
                let accounted = accounting.finish_metered(&mut budget).await;
                return (
                    Err(accounted.err().unwrap_or(ManagementError::Database)),
                    budget,
                );
            }
        };
        let first = cursor.writer.is_none();
        let initialized = match cursor.writer.take() {
            Some(continuation) => Writer::continue_metered(file, continuation, budget),
            None => Writer::new_metered(file, header.projection, budget),
        };
        let mut writer = match initialized {
            Ok(writer) => writer,
            Err((error, mut budget)) => {
                let accounted = accounting.finish_metered(&mut budget).await;
                return (
                    Err(accounted.err().unwrap_or_else(|| planning::safe(error))),
                    *budget,
                );
            }
        };
        let output_scope = header.database_scope;
        let output_identity = header.database_identity;
        let written = async {
            if first {
                v3::planning::prefix(
                    &mut writer,
                    header,
                    request,
                    &snapshot.configuration,
                    self.plan.items(),
                    *self.plan.summary(),
                    None,
                )
                .map_err(planning::safe)?;
            }
            let caps = task.caps(primary, limits).await?;
            let progress = cursor
                .export
                .advance_planning_page(primary, &mut writer, snapshot, &caps)
                .await?;
            if progress == Progress::Archived {
                for (index, entry) in self.plan.items().iter().enumerate() {
                    v3::planning::fact(
                        &mut writer,
                        v3::Section::Result,
                        "management_capture",
                        index,
                        &entry.capture,
                    )
                    .map_err(planning::safe)?;
                }
            }
            Ok(progress)
        }
        .await;
        let complete = matches!(written, Ok(Progress::Archived));
        let (file, mut budget) = match written {
            Ok(_) if complete => {
                let (file, budget) = writer.finalize();
                (file.map_err(planning::safe), budget)
            }
            Ok(_) => {
                let (paused, budget) = writer.pause();
                (
                    paused
                        .map(|(file, continuation)| {
                            cursor.writer = Some(continuation);
                            file
                        })
                        .map_err(planning::safe),
                    budget,
                )
            }
            Err(error) => (Err(error), writer.abort()),
        };
        let result = async {
            let mut file = file?;
            let length = usize::try_from(
                file.metadata()
                    .map_err(|_| ManagementError::Database)?
                    .len(),
            )
            .map_err(|_| ManagementError::Budget)?;
            if length > 8 * crate::store::pinning::management::native::meter::PAGE_BYTES {
                return Err(ManagementError::Budget);
            }
            let _resident = budget.reserve(length)?;
            budget.charge(
                length.div_ceil(v3::CELL_BYTES) as u64 + 4,
                0,
                0,
                length as u64 * 2,
            )?;
            let mut bytes = Vec::with_capacity(length);
            file.seek(SeekFrom::Start(0))
                .map_err(|_| ManagementError::Database)?;
            file.read_to_end(&mut bytes)
                .map_err(|_| ManagementError::Database)?;
            let serialized =
                hex::decode(&cursor.hash).map_err(|_| ManagementError::InvalidEvidence)?;
            let serialized = SerializedState::<Sha256>::try_from(serialized.as_slice())
                .map_err(|_| ManagementError::InvalidEvidence)?;
            let mut hash = <Sha256 as SerializableState>::deserialize(&serialized)
                .map_err(|_| ManagementError::InvalidEvidence)?;
            let mut ordinal = previous.as_ref().map_or(0, |position| position.chunks);
            for chunk in bytes.chunks(v3::CELL_BYTES) {
                hash.update((ordinal as u64).to_be_bytes());
                hash.update((chunk.len() as u64).to_be_bytes());
                hash.update(chunk);
                ordinal += 1;
            }
            cursor.hash = hex::encode(SerializableState::serialize(&hash));
            let next = if complete {
                hex::encode(hash.finalize())
            } else {
                encode(&cursor)?
            };
            budget.charge(0, 0, 0, next.len() as u64 * 2)?;
            accounting.persist(budget.work).await?;
            let tx = primary
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            let stored = progress::load(&tx, OUTPUT, &raw, true).await?;
            if stored != previous {
                return Err(ManagementError::Changed);
            }
            let before = match stored {
                Some(position) => position,
                None => {
                    progress::create(&tx, OUTPUT, &raw, output_scope, output_identity, "initial")
                        .await?
                }
            };
            progress::append(&tx, OUTPUT, &raw, &before, &next, &bytes, complete).await?;
            budget.check()?;
            limits.check()?;
            tx.commit().await.map_err(|_| ManagementError::Database)?;
            limits.check()?;
            Ok(if complete {
                Progress::Archived
            } else {
                Progress::Pending
            })
        }
        .await;
        let accounted = accounting.finish_metered(&mut budget).await;
        (
            result.and_then(|value| accounted.and_then(|()| limits.check().map(|()| value))),
            budget,
        )
    }

    /// Copy the explicitly selected output to a newly reserved handle and verify its EOF.
    /// Failure preserves the immutable spool and leaves the partial untouched.
    pub async fn export_prepared_output(
        &self,
        (mut file, mut budget): (File, Budget),
        projection: Projection,
        limits: &RunLimits,
    ) -> (Result<()>, Budget) {
        let Some(primary) = self.spools.first() else {
            return (Err(ManagementError::InvalidEvidence), budget);
        };
        let Some(task) = primary.task.as_deref() else {
            return (Err(ManagementError::InvalidEvidence), budget);
        };
        let accounting = match task.adopt_output(limits, &mut budget, false).await {
            Ok(accounting) => accounting,
            Err(error) => return (Err(error), budget),
        };
        let initial = budget.work;
        let accounting = match accounting.prepay_transfer(&mut budget, limits).await {
            Ok(accounting) => accounting,
            Err(error) => return (Err(error), budget),
        };
        let pages = Arc::new(PageMeter::default());
        let copied = async {
            budget.charge(1, 1, 1, 0)?;
            let row = spool::one(&primary.db, "SELECT length(CAST(header AS BLOB)) AS header_bytes FROM pin_management_archive WHERE archive_id=$1 AND state='sealed'", vec![OUTPUT.into()]).await?.ok_or(ManagementError::InvalidEvidence)?;
            let header_bytes = usize::try_from(spool::get::<i64>(&row, "header_bytes")?).map_err(|_| ManagementError::InvalidEvidence)?;
            if header_bytes > crate::store::pinning::management::native::meter::PAGE_BYTES { return Err(ManagementError::Budget); }
            let _binding_memory = budget.reserve(header_bytes * 3)?;
            budget.charge(1, 1, 4, header_bytes as u64)?;
            let row = spool::one(&primary.db, "SELECT header,chunks,bytes,CASE WHEN length(digest)=64 THEN digest END AS digest FROM pin_management_archive WHERE archive_id=$1 AND state='sealed' AND length(CAST(header AS BLOB))=$2", vec![OUTPUT.into(), (header_bytes as i64).into()]).await?.ok_or(ManagementError::Changed)?;
            let raw: String = spool::get(&row, "header")?;
            let binding = decode::<PreparedOutputBinding>(&raw)?;
            if binding.projection != projection || binding.archive_header.expires_at.is_some() { return Err(ManagementError::Confirmation); }
            let reference = segmented::ArchiveRef { archive_id: OUTPUT.into(), chunks: spool::get(&row, "chunks")?, bytes: spool::get(&row, "bytes")?, digest: spool::get(&row, "digest")? };
            // Only the bounded wire header is read before any target bytes.
            let _prefix_memory = budget.reserve(v3::FRAME_BYTES + 2 * v3::CELL_BYTES)?;
            let mut prefix = Vec::with_capacity(v3::FRAME_BYTES + v3::CELL_BYTES);
            let mut needed = v3::MAGIC.len() + 4;
            let mut ordinal = 0i64;
            while prefix.len() < needed {
                limits.check()?;
                if ordinal >= reference.chunks { return Err(ManagementError::InvalidEvidence); }
                budget.charge(1, 1, 1, 0)?;
                let row = spool::one(&primary.db, "SELECT CASE WHEN length(payload)<=65536 THEN payload END AS payload FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal=$2", vec![OUTPUT.into(), ordinal.into()]).await?.ok_or(ManagementError::InvalidEvidence)?;
                let chunk: Vec<u8> = spool::get(&row, "payload")?;
                if chunk.is_empty() { return Err(ManagementError::InvalidEvidence); }
                budget.charge(0, 0, 0, chunk.len() as u64)?;
                prefix.extend_from_slice(&chunk);
                if prefix.len() >= v3::MAGIC.len() + 4 {
                    let length = u32::from_be_bytes(prefix[v3::MAGIC.len()..v3::MAGIC.len()+4].try_into().map_err(|_| ManagementError::InvalidEvidence)?) as usize;
                    if length == 0 || length > v3::FRAME_BYTES + 1 { return Err(ManagementError::InvalidEvidence); }
                    needed = v3::MAGIC.len() + 4 + length;
                }
                ordinal += 1;
            }
            Ok((reference, raw, binding, prefix, _binding_memory, _prefix_memory))
        }.await;
        let copied = match copied {
            Err(error) => Err(error),
            Ok((reference, raw, binding, prefix, _binding_memory, _prefix_memory)) => {
                let (checked, returned) = v3::planning::verify_output_header(
                    prefix.as_slice(),
                    projection,
                    &binding.archive_header,
                    budget,
                );
                budget = returned;
                async {
                    checked.map_err(planning::safe)?;
                    let returned = if projection == Projection::Review {
                        let mut output = PageOutput::new(&mut file, limits, pages.clone());
                        tokio::select! {
                            biased;
                            _ = limits.cancellation.cancelled() => return Err(ManagementError::Budget),
                            result = segmented::read_archive_raw(&primary.db, &reference, &mut output, &mut budget) => result?,
                        }
                    } else {
                        let mut output = CheckedOutput {
                            io: &mut file,
                            limits,
                        };
                        tokio::select! {
                            biased;
                            _ = limits.cancellation.cancelled() => return Err(ManagementError::Budget),
                            result = segmented::read_archive_raw(&primary.db, &reference, &mut output, &mut budget) => result?,
                        }
                    };
                    if returned != raw {
                        return Err(ManagementError::Changed);
                    }
                    limits.check()?;
                    file.sync_all().map_err(|_| ManagementError::Database)?;
                    file.seek(SeekFrom::Start(0))
                        .map_err(|_| ManagementError::Database)?;
                    u64::try_from(reference.bytes).map_err(|_| ManagementError::InvalidEvidence)
                }
                .await
            }
        };
        let (result, mut budget) = match copied {
            Err(error) => (Err(error), budget),
            Ok(bytes) => {
                let (result, budget) = if projection == Projection::Review {
                    let input = PageOutput::reader(file, limits, pages.clone(), bytes);
                    v3::planning::verify_output(input, projection, budget, limits)
                } else {
                    v3::planning::verify_output(
                        CheckedOutput { io: file, limits },
                        projection,
                        budget,
                        limits,
                    )
                };
                (result.map_err(planning::safe), budget)
            }
        };
        budget.work.pages += pages.used.load(Ordering::Relaxed);
        let result = pages.result(result);
        let accounted = accounting.finish_output(initial, &mut budget).await;
        (result.and(accounted).and_then(|()| limits.check()), budget)
    }
}

struct CheckedOutput<'a, T> {
    io: T,
    limits: &'a RunLimits,
}
impl<T: Write> Write for CheckedOutput<'_, T> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.limits.check().map_err(std::io::Error::other)?;
        self.io.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.limits.check().map_err(std::io::Error::other)?;
        self.io.flush()
    }
}
impl<T: Read> Read for CheckedOutput<'_, T> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.limits.check().map_err(std::io::Error::other)?;
        self.io.read(bytes)
    }
}
