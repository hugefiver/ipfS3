//! Canonical v3 evidence producer. File integrity never supplies authority.
use super::{
    spool::{exec, get, one, wire_key},
    *,
};
use crate::{
    pinning::management::wire::v3::{CellValue, Event, Frame, Section, Writer},
    store::pinning::management::native::{
        self,
        meter::{Budget, CELL_SLICE},
    },
};
use sea_orm::{ConnectionTrait, TransactionTrait};
use std::io::Write;
mod page;
mod short_cell;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementMutationRef {
    pub archive_id: String,
    pub plan_id: String,
    pub item_id: String,
    pub reducer_version: u32,
}
/// Both are coordinates into this file's Inventory section. Importers resolve
/// the complete original rows; neither coordinate nor Reader::verified grants
/// authority, supplies a source fence, or replaces local binding validation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementEvidenceRefs {
    pub inventory: RetirementInventoryRef,
    pub mutation: RetirementMutationRef,
}

impl RetirementEvidenceRefs {
    pub fn for_entry(plan_id: &str, entry: &PlanEntry) -> Result<Option<Self>> {
        let Some(Capture::Provider(capture)) = &entry.capture else {
            return Ok(None);
        };
        let refs = Self {
            inventory: capture.inventory.clone(),
            mutation: RetirementMutationRef {
                archive_id: capture.inventory.archive_id.clone(),
                plan_id: plan_id.into(),
                item_id: entry.request.item_id.clone(),
                reducer_version: capture.inventory.reducer_version,
            },
        };
        refs.validate_coordinates(plan_id, &entry.request.item_id)?;
        if entry.id.as_str() != format!("management-v1:{plan_id}:{}", entry.request.item_id) {
            return Err(ManagementError::Confirmation);
        }
        Ok(Some(refs))
    }
    /// Structural validation only; the complete referred evidence still needs
    /// local original comparison and the executor's source/identity fences.
    pub fn validate_coordinates(&self, plan_id: &str, item_id: &str) -> Result<()> {
        super::super::validate::label(plan_id).map_err(|_| ManagementError::InvalidEvidence)?;
        super::super::validate::label(item_id).map_err(|_| ManagementError::InvalidEvidence)?;
        for value in [&self.inventory.archive_id, &self.inventory.inventory_id] {
            if value.is_empty()
                || value.len() > 256
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
            {
                return Err(ManagementError::InvalidEvidence);
            }
        }
        if self.inventory.reducer_version != REDUCER_VERSION
            || self.mutation.reducer_version != REDUCER_VERSION
            || self.inventory.archive_id != self.mutation.archive_id
            || self.mutation.plan_id != plan_id
            || self.mutation.item_id != item_id
        {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(())
    }
}

impl RetirementReview<'_> {
    pub async fn stage_evidence(
        &self,
        plan_id: &str,
        entry: &PlanEntry,
    ) -> Result<RetirementEvidenceRefs> {
        self.stage_evidence_into(self.spool, plan_id, entry).await
    }

    /// Multiple provider roots share one file only after complete original
    /// equality. Native/membership sections are emitted once by `primary`.
    pub async fn stage_evidence_into(
        &self,
        primary: &InventorySpool,
        plan_id: &str,
        entry: &PlanEntry,
    ) -> Result<RetirementEvidenceRefs> {
        self.stage_evidence_at(primary, primary.reference(), plan_id, entry)
            .await
    }

    async fn stage_evidence_at(
        &self,
        primary: &InventorySpool,
        membership: &RetirementInventoryRef,
        plan_id: &str,
        entry: &PlanEntry,
    ) -> Result<RetirementEvidenceRefs> {
        if membership != self.reference() && self.spool.shared_source.as_ref() != Some(membership) {
            return Err(ManagementError::InvalidEvidence);
        }
        if !std::ptr::eq(self.spool, primary)
            && self.spool.shared_source.as_ref() != Some(primary.reference())
        {
            return Err(ManagementError::InvalidEvidence);
        }
        if !primary
            .reduced_here
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ManagementError::InvalidEvidence);
        }
        super::super::validate::label(plan_id).map_err(|_| ManagementError::InvalidEvidence)?;
        if entry.id.as_str() != format!("management-v1:{plan_id}:{}", entry.request.item_id) {
            return Err(ManagementError::Confirmation);
        }
        let Some(Capture::Provider(capture)) = &entry.capture else {
            return Err(ManagementError::InvalidEvidence);
        };
        if capture.inventory != *self.reference() || capture.totals != *self.totals() {
            return Err(ManagementError::Confirmation);
        }
        super::super::validate::Budget::default()
            .entry(entry)
            .map_err(|_| ManagementError::Budget)?;
        let expected = self
            .capture(&entry.request, capture.mutation_scope.clone())
            .await?;
        if expected != **capture {
            return Err(ManagementError::Confirmation);
        }
        let refs = RetirementEvidenceRefs::for_entry(plan_id, entry)?
            .ok_or(ManagementError::InvalidEvidence)?;
        let mutation = &refs.mutation;
        let tx = primary
            .db
            .begin()
            .await
            .map_err(|_| ManagementError::Database)?;
        let inventory_key = NativeKey {
            table: "retirement_inventory".into(),
            values: vec![self.reference().inventory_id.clone()],
        };
        store_cell(&tx, &inventory_key, "membership_reference", membership).await?;
        store_cell(&tx, &inventory_key, "reference", self.reference()).await?;
        // These column names deliberately use the existing v3 private-cell
        // classification, so review projection never exposes original routes.
        store_cell(&tx, &inventory_key, "source_descriptor", self.checkpoint()).await?;
        store_cell(&tx, &inventory_key, "totals", self.totals()).await?;
        let mutation_key = NativeKey {
            table: "retirement_mutation".into(),
            values: vec![plan_id.into(), entry.request.item_id.clone()],
        };
        store_cell(&tx, &mutation_key, "inventory_reference", self.reference()).await?;
        store_cell(
            &tx,
            &mutation_key,
            "input_identity",
            &capture.mutation_scope,
        )
        .await?;
        store_cell(&tx, &mutation_key, "reference", &mutation).await?;
        tx.commit().await.map_err(|_| ManagementError::Database)?;
        Ok(refs)
    }
}
async fn store_cell(
    tx: &impl ConnectionTrait,
    key: &NativeKey,
    column: &str,
    value: &impl Serialize,
) -> Result<()> {
    let payload = encode(value)?.into_bytes();
    if payload.len() > super::super::MAX_TEXT_BYTES {
        return Err(ManagementError::Budget);
    }
    let order = wire_key(key);
    if let Some(row) = one(
        tx,
        "SELECT payload FROM review_cells WHERE table_name=$1 AND sort_key=$2 AND column_name=$3",
        vec![
            key.table.clone().into(),
            order.clone().into(),
            column.into(),
        ],
    )
    .await?
    {
        if get::<Vec<u8>>(&row, "payload")? != payload {
            return Err(ManagementError::Changed);
        }
        return Ok(());
    }
    let mut checksums = String::new();
    for (index, bytes) in payload.chunks(CELL_SLICE).enumerate() {
        checksums.push_str(
            &ArchivePart {
                table: key.table.clone(),
                key: key.clone(),
                column: column.into(),
                offset: (index * CELL_SLICE) as u64,
                length: Some(payload.len() as u64),
                bytes: bytes.to_vec(),
            }
            .checksum()?,
        );
    }
    exec(
        tx,
        "INSERT INTO review_cells VALUES($1,$2,$3,$4,$5,$6)",
        vec![
            key.table.clone().into(),
            order.into(),
            encode(key)?.into(),
            column.into(),
            payload.into(),
            checksums.into(),
        ],
    )
    .await
}

#[derive(Serialize, Deserialize)]
enum Source {
    Native(NativeKey),
    Review(NativeKey),
    Inline(String),
}
#[derive(Serialize, Deserialize)]
struct Row {
    columns: Vec<String>,
    source: Source,
    column: usize,
    offset: u64,
    opened: bool,
    length: Option<Option<u64>>,
    #[serde(skip)]
    initial: Option<page::Cells>,
    #[serde(skip)]
    key_memory: Option<std::sync::Arc<native::meter::ResidentGuard>>,
    #[serde(skip)]
    part_memory: Option<std::sync::Arc<page::Memory>>,
}
/// A cursor emits Native, Membership, then Inventory, exactly in v3 key order.
/// Source-native integer order is retained separately in membership positions.
#[derive(Default, Serialize, Deserialize)]
struct RetirementArchiveCursor {
    section: u8,
    after_table: String,
    after: Vec<u8>,
    row: Option<Row>,
    membership_table: usize,
    eof_emitted: bool,
    #[serde(skip)]
    keys: page::Keys,
    publication: Option<Vec<(NativeKey, NativeKey)>>,
    membership: Option<RetirementInventoryRef>,
}

/// Ordinary exports own a fresh writer. Confirmation persists this cursor with
/// its writer continuation in the same archive-page transaction as the bytes.
#[derive(Serialize, Deserialize)]
pub struct ArchiveExport {
    reference: RetirementInventoryRef,
    cursor: RetirementArchiveCursor,
    complete: bool,
    planning_facts_written: bool,
}

impl ArchiveExport {
    pub async fn advance<W: Write>(
        &mut self,
        spool: &InventorySpool,
        writer: &mut Writer<W>,
        limits: &RunLimits,
    ) -> Result<Progress> {
        self.advance_inner(spool, writer, limits, false, None).await
    }
    pub(crate) async fn advance_page<W: Write>(
        &mut self,
        spool: &InventorySpool,
        writer: &mut Writer<W>,
        limits: &RunLimits,
    ) -> Result<Progress> {
        self.advance_inner(spool, writer, limits, true, None).await
    }
    pub(crate) async fn advance_planning_page<W: Write>(
        &mut self,
        spool: &InventorySpool,
        writer: &mut Writer<W>,
        snapshot: &Snapshot,
        limits: &RunLimits,
    ) -> Result<Progress> {
        self.advance_inner(spool, writer, limits, true, Some(snapshot))
            .await
    }
    async fn advance_inner<W: Write>(
        &mut self,
        spool: &InventorySpool,
        writer: &mut Writer<W>,
        limits: &RunLimits,
        one_page: bool,
        snapshot: Option<&Snapshot>,
    ) -> Result<Progress> {
        if self.reference != *spool.reference()
            || !spool
                .reduced_here
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut run = super::spool::task_run(&spool.db, limits).await?;
        if snapshot.is_some() {
            run.check()?;
            writer.budget().check()?;
        }
        if self.complete {
            return Ok(Progress::Archived);
        }
        loop {
            if !run.page(0, 0)? {
                if snapshot.is_some() {
                    run.check()?;
                    writer.budget().check()?;
                }
                return Ok(Progress::Pending);
            }
            let page_started = std::time::Instant::now();
            let initial = writer.budget().work.output_bytes;
            let initial_work = writer.budget().work.raw_bytes;
            let prior_work = spool.task_work().await?;
            let mut rows = 0u64;
            let mut events = 0usize;
            let outcome = async {
                while rows < 256
                    && writer.budget().work.output_bytes - initial < 1024 * 1024
                    && events < 4096
                {
                    run.check()?;
                    writer.budget().check()?;
                    if snapshot.is_some() && !limits.production_available() {
                        break;
                    }
                    let remaining = (1024 * 1024) - (writer.budget().work.output_bytes - initial);
                    if let Some(logical) =
                        self.cursor
                            .complete_initial(writer, remaining, 4096 - events)?
                    {
                        events += logical;
                        continue;
                    }
                    let resident = writer.budget_mut().reserve(CELL_SLICE * 3)?;
                    let Some(event) = self
                        .cursor
                        .next(spool, writer.budget_mut(), (256 - rows) as usize)
                        .await?
                    else {
                        self.complete = true;
                        break;
                    };
                    match event {
                        Event::Frame(frame) => {
                            if matches!(
                                frame,
                                Frame::Row {
                                    section: Section::Inventory,
                                    ..
                                }
                            ) && !self.planning_facts_written
                                && let Some(snapshot) = snapshot
                            {
                                super::super::wire::v3::planning::snapshot_facts(writer, snapshot)
                                    .map_err(map_wire)?;
                                self.planning_facts_written = true;
                            }
                            if matches!(frame, Frame::Row { .. }) {
                                run.task(prior_work.rows + rows + 1, prior_work.bytes)?;
                            }
                            rows += u64::from(matches!(frame, Frame::Row { .. }));
                            writer.frame(&frame).map_err(map_wire)?;
                        }
                        Event::Chunk(bytes) => writer
                            .cell_output()
                            .write_all(&bytes)
                            .map_err(|_| ManagementError::Database)?,
                    }
                    drop(resident);
                    events += 1;
                }
                Ok(())
            }
            .await;
            super::spool::budget::charge(
                &spool.db,
                &run,
                rows,
                writer.budget().work.raw_bytes - initial_work,
            )
            .await?;
            super::spool::budget::timed(&spool.db, "export", page_started.elapsed()).await?;
            if let Err(error) = outcome {
                if snapshot.is_none()
                    && error == ManagementError::Budget
                    && run.remaining().is_zero()
                    && !limits.cancellation.is_cancelled()
                    && writer.budget().check().is_ok()
                {
                    return Ok(Progress::Pending);
                }
                return Err(error);
            }
            run.check()?;
            writer.budget().check()?;
            if self.complete {
                return Ok(Progress::Archived);
            }
            if one_page {
                return Ok(Progress::Pending);
            }
        }
    }
}

impl InventorySpool {
    /// Header/request/configuration/ACK/items/summary are supplied by the owner
    /// of the complete plan. This method writes only its evidence sections; the
    /// caller emits the trailer after all required result sections succeed.
    pub async fn write_archive_sections<W: Write>(
        &self,
        writer: &mut Writer<W>,
        limits: &RunLimits,
    ) -> Result<()> {
        let mut export = self.archive_export().await?;
        match export.advance(self, writer, limits).await? {
            Progress::Archived => Ok(()),
            _ => Err(ManagementError::Budget),
        }
    }

    pub async fn archive_export(&self) -> Result<ArchiveExport> {
        self.archive_export_with_membership(self.shared_source.as_ref().unwrap_or(self.reference()))
            .await
    }

    pub(crate) async fn publication_export(
        &self,
        plan_id: &str,
        entries: &[PlanEntry],
    ) -> Result<ArchiveExport> {
        if entries.len() > super::super::MAX_ITEMS {
            return Err(ManagementError::Budget);
        }
        let mut export = self.archive_export().await?;
        export.cursor.publication = Some(
            entries
                .iter()
                .filter_map(|entry| {
                    let Some(Capture::Provider(capture)) = &entry.capture else {
                        return None;
                    };
                    Some((
                        NativeKey {
                            table: "retirement_inventory".into(),
                            values: vec![capture.inventory.inventory_id.clone()],
                        },
                        NativeKey {
                            table: "retirement_mutation".into(),
                            values: vec![plan_id.into(), entry.request.item_id.clone()],
                        },
                    ))
                })
                .collect(),
        );
        Ok(export)
    }

    async fn archive_export_with_membership(
        &self,
        membership: &RetirementInventoryRef,
    ) -> Result<ArchiveExport> {
        if !self.reduced_here.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ManagementError::InvalidEvidence);
        }
        let review = self.frozen_review().await?;
        let inventory_key = NativeKey {
            table: "retirement_inventory".into(),
            values: vec![self.reference().inventory_id.clone()],
        };
        let tx = self
            .db
            .begin()
            .await
            .map_err(|_| ManagementError::Database)?;
        store_cell(&tx, &inventory_key, "membership_reference", membership).await?;
        store_cell(&tx, &inventory_key, "reference", self.reference()).await?;
        store_cell(
            &tx,
            &inventory_key,
            "source_descriptor",
            review.checkpoint(),
        )
        .await?;
        store_cell(&tx, &inventory_key, "totals", review.totals()).await?;
        tx.commit().await.map_err(|_| ManagementError::Database)?;
        Ok(ArchiveExport {
            reference: self.reference.clone(),
            cursor: RetirementArchiveCursor {
                membership: Some(membership.clone()),
                ..Default::default()
            },
            complete: false,
            planning_facts_written: false,
        })
    }
}
fn map_wire(error: SafeError) -> ManagementError {
    match error {
        SafeError::LimitExceeded => ManagementError::Budget,
        _ => ManagementError::InvalidEvidence,
    }
}

impl RetirementArchiveCursor {
    async fn next(
        &mut self,
        spool: &InventorySpool,
        budget: &mut Budget,
        row_cap: usize,
    ) -> Result<Option<Event>> {
        loop {
            budget.check()?;
            if let Some(row) = &mut self.row {
                if row.column == row.columns.len() {
                    self.row = None;
                    return Ok(Some(Event::Frame(Frame::RowEnd)));
                }
                let column = &row.columns[row.column];
                if !row.opened {
                    row.opened = true;
                    return Ok(Some(Event::Frame(Frame::Cell {
                        column: column.clone(),
                    })));
                }
                if row
                    .length
                    .is_some_and(|length| length.is_none_or(|n| row.offset == n))
                {
                    let value = if row.length == Some(None) {
                        CellValue::Null
                    } else {
                        CellValue::Text
                    };
                    row.column += 1;
                    row.offset = 0;
                    row.opened = false;
                    row.length = None;
                    return Ok(Some(Event::Frame(Frame::CellEnd { value })));
                }
                let (length, bytes, charged) = match &row.source {
                    Source::Native(key) => {
                        let first = if row.offset == 0 {
                            match row.initial.as_mut() {
                                Some(initial) => initial.take(column)?,
                                None => None,
                            }
                        } else {
                            None
                        };
                        if let Some(part) = first {
                            row.part_memory = Some(part.memory);
                            (part.length, part.bytes, true)
                        } else {
                            let part =
                                page::part(spool, key, column, row.offset, row.length, budget)
                                    .await?;
                            row.part_memory = Some(part.memory);
                            (part.length, part.bytes, true)
                        }
                    }
                    Source::Review(key) => {
                        budget.charge(1, 0, 1, 0)?;
                        let part=one(&spool.db,"SELECT length(payload) AS length,substr(payload,$4,$5) AS bytes,substr(checksums,$6,64) AS checksum FROM review_cells WHERE table_name=$1 AND sort_key=$2 AND column_name=$3",vec![key.table.clone().into(),wire_key(key).into(),column.clone().into(),((row.offset+1) as i64).into(),(CELL_SLICE as i64).into(),((row.offset/CELL_SLICE as u64*64+1) as i64).into()]).await?.ok_or(ManagementError::InvalidEvidence)?;
                        let length = Some(
                            u64::try_from(get::<i64>(&part, "length")?)
                                .map_err(|_| ManagementError::InvalidEvidence)?,
                        );
                        let bytes = get::<Vec<u8>>(&part, "bytes")?;
                        ArchivePart {
                            table: key.table.clone(),
                            key: key.clone(),
                            column: column.clone(),
                            offset: row.offset,
                            length,
                            bytes: bytes.clone(),
                        }
                        .check_checksum(&get::<String>(&part, "checksum")?)?;
                        (length, bytes, false)
                    }
                    Source::Inline(text) => {
                        (Some(text.len() as u64), text.as_bytes().to_vec(), false)
                    }
                };
                if bytes.len() > CELL_SLICE
                    || length.is_none_or(|n| {
                        row.offset > n
                            || bytes.len() as u64 != (n - row.offset).min(CELL_SLICE as u64)
                    }) && !(length.is_none() && bytes.is_empty() && row.offset == 0)
                    || row.length.is_some_and(|old| old != length)
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                row.length = Some(length);
                row.offset += bytes.len() as u64;
                if !charged {
                    budget.charge(0, 0, 0, bytes.len() as u64)?;
                }
                if !bytes.is_empty() {
                    return Ok(Some(Event::Chunk(bytes)));
                }
                continue;
            }
            match self.section {
                0 => {
                    let row = self
                        .keys
                        .native(spool, budget, &self.after_table, &self.after, row_cap)
                        .await?;
                    if let Some(row) = row {
                        let key = row.native;
                        self.after_table = key.table.clone();
                        self.after = row.order;
                        let columns = native::current_catalog()
                            .iter()
                            .find(|t| t.table == key.table)
                            .ok_or(ManagementError::InvalidEvidence)?
                            .columns
                            .to_vec();
                        let event =
                            self.begin(Section::Native, key.clone(), columns, Source::Native(key));
                        let current = self.row.as_mut().ok_or(ManagementError::InvalidEvidence)?;
                        current.initial = row.initial;
                        current.key_memory = Some(row.memory);
                        return Ok(Some(event));
                    }
                    self.section = 1;
                    self.after.clear();
                    self.after_table.clear();
                }
                1 => {
                    let Some(table) = native::current_catalog().get(self.membership_table) else {
                        self.section = 2;
                        self.after.clear();
                        continue;
                    };
                    if !self.eof_emitted {
                        budget.charge(1, 0, 0, 0)?;
                        if one(
                            &spool.db,
                            "SELECT table_name FROM eof WHERE table_name=$1",
                            vec![table.table.clone().into()],
                        )
                        .await?
                        .is_none()
                        {
                            return Err(ManagementError::InvalidEvidence);
                        }
                        self.eof_emitted = true;
                        let key = NativeKey {
                            table: "retirement_membership".into(),
                            values: vec![
                                self.membership.as_ref().map_or_else(
                                    || spool.reference().inventory_id.clone(),
                                    |membership| membership.inventory_id.clone(),
                                ),
                                table.table.clone(),
                                "eof".into(),
                            ],
                        };
                        return Ok(Some(self.begin(
                            Section::Membership,
                            key,
                            vec!["complete".into()],
                            Source::Inline("true".into()),
                        )));
                    }
                    let row = self
                        .keys
                        .next(spool, budget, Some(&table.table), "", &self.after)
                        .await?;
                    if let Some(row) = row {
                        let original = row.native;
                        self.after = row.order;
                        let mut values = vec![
                            self.membership.as_ref().map_or_else(
                                || spool.reference().inventory_id.clone(),
                                |membership| membership.inventory_id.clone(),
                            ),
                            table.table.clone(),
                            "member".into(),
                        ];
                        values.extend(original.values);
                        let event = self.begin(
                            Section::Membership,
                            NativeKey {
                                table: "retirement_membership".into(),
                                values,
                            },
                            vec!["position".into()],
                            Source::Inline(
                                row.position
                                    .ok_or(ManagementError::InvalidEvidence)?
                                    .to_string(),
                            ),
                        );
                        self.row
                            .as_mut()
                            .ok_or(ManagementError::InvalidEvidence)?
                            .key_memory = Some(row.memory);
                        return Ok(Some(event));
                    }
                    self.membership_table += 1;
                    self.eof_emitted = false;
                    self.after.clear();
                }
                2 => {
                    budget.charge(1, 0, 0, 0)?;
                    let row=one(&spool.db,"SELECT table_name,sort_key,row_key FROM review_cells WHERE (table_name,sort_key)>($1,$2) ORDER BY table_name,sort_key,column_name LIMIT 1",vec![self.after_table.clone().into(),self.after.clone().into()]).await?;
                    let Some(row) = row else {
                        self.section = 3;
                        continue;
                    };
                    let key: NativeKey = decode(&get::<String>(&row, "row_key")?)?;
                    self.after_table = key.table.clone();
                    self.after = get(&row, "sort_key")?;
                    if self.publication.as_ref().is_some_and(|items| {
                        !items
                            .iter()
                            .any(|(inventory, mutation)| &key == inventory || &key == mutation)
                    }) {
                        continue;
                    }
                    let columns = if key.table == "retirement_inventory" {
                        vec![
                            "membership_reference".into(),
                            "reference".into(),
                            "source_descriptor".into(),
                            "totals".into(),
                        ]
                    } else if key.table == "retirement_mutation" {
                        vec![
                            "input_identity".into(),
                            "inventory_reference".into(),
                            "reference".into(),
                        ]
                    } else {
                        return Err(ManagementError::InvalidEvidence);
                    };
                    return Ok(Some(self.begin(
                        Section::Inventory,
                        key.clone(),
                        columns,
                        Source::Review(key),
                    )));
                }
                _ => return Ok(None),
            }
        }
    }
    fn begin(
        &mut self,
        section: Section,
        key: NativeKey,
        columns: Vec<String>,
        source: Source,
    ) -> Event {
        let cells = columns.len() as u32;
        self.row = Some(Row {
            columns,
            source,
            column: 0,
            offset: 0,
            opened: false,
            length: None,
            initial: None,
            key_memory: None,
            part_memory: None,
        });
        Event::Frame(Frame::Row {
            section,
            key,
            cells,
        })
    }
}

#[cfg(test)]
#[path = "export/tests.rs"]
mod pause_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disk_order_is_native_key_lexical_not_json_or_numeric_order() {
        let mut keys = ["10", "2", "a", "a\0", "a\"", "a\\", "", "é"]
            .into_iter()
            .map(|value| NativeKey {
                table: "rows".into(),
                values: vec![value.into(), "suffix".into()],
            })
            .collect::<Vec<_>>();
        let mut disk = keys.clone();
        keys.sort();
        disk.sort_by_key(wire_key);
        assert_eq!(disk, keys);
    }
}
