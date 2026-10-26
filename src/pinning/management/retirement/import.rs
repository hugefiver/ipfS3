//! Private v3 restoration uses the same wire cursor as normal archive review.
//! Imported originals never acquire a live-source seal.
use super::{
    spool::{Run, exec, get, one, put_part},
    *,
};
use crate::{
    pinning::management::wire::v3::{
        self, CellValue, Event, Frame, Section, archive_cursor::ReviewCursor,
    },
    store::pinning::management::native::{
        self,
        meter::{Budget, CELL_SLICE, ResidentGuard},
    },
};
use sea_orm::{ConnectionTrait, DatabaseTransaction, TransactionTrait};
use std::{
    io::{Read, Seek, Write},
    path::Path,
};
mod finish;

pub struct FrozenRetirementImport<S> {
    cursor: ReviewCursor<S>,
    spool: InventorySpool,
    expected: RetirementInventoryRef,
    header: Option<(String, String, Vec<String>)>,
    configuration: Option<ConfigurationBinding>,
    row: Option<(Section, NativeKey)>,
    column: String,
    buffer: Vec<u8>,
    guards: Vec<ResidentGuard>,
    _resident: ResidentGuard,
    offset: u64,
    position: u64,
    part_column: usize,
    part_offset: u64,
    eof_table: usize,
    rows: u64,
    bytes: u64,
    wire_bytes: u64,
    exhausted: bool,
    archived: bool,
    reduced: bool,
    failed: bool,
}
impl<S: Read + Write + Seek> FrozenRetirementImport<S> {
    /// Resolve the common publication through its typed local binding. Generic
    /// archive loading below retains the ordinary Request envelope contract.
    pub async fn load_original<C: ConnectionTrait>(
        db: &C,
        binding: &crate::store::pinning::management::records::ConfirmedBinding,
        entry: &PlanEntry,
        file: S,
        path: &Path,
        reservation: std::fs::File,
        budget: Budget,
    ) -> Result<Self> {
        let Some(Capture::Provider(capture)) = &entry.capture else {
            return Err(ManagementError::Confirmation);
        };
        let cursor = crate::store::pinning::management::records::load_original_inventory(
            db, binding, entry, file, budget,
        )
        .await?;
        Self::create(cursor, path, reservation, capture.inventory.clone()).await
    }
}
impl<S: Read + Seek> FrozenRetirementImport<S> {
    /// Normal stored-archive entry point. Both file reservations are supplied by
    /// the protected local artifact adapter, not by coordinates inside the file.
    pub async fn load<C: ConnectionTrait>(
        db: &C,
        reference: &crate::store::pinning::management::records::segmented::ArchiveRef,
        canonical: S,
        path: &Path,
        reservation: std::fs::File,
        expected: RetirementInventoryRef,
        budget: Budget,
    ) -> Result<Self>
    where
        S: Write,
    {
        let cursor = v3::archive_cursor::load(
            db,
            reference,
            canonical,
            v3::Projection::PrivateRecovery,
            budget,
        )
        .await
        .map_err(wire_error)?;
        Self::create(cursor, path, reservation, expected).await
    }
    /// `expected` is a coordinate claim. The complete selected inventory,
    /// original membership and recomputed totals must agree before it is usable.
    pub async fn create(
        mut cursor: ReviewCursor<S>,
        path: &Path,
        reservation: std::fs::File,
        expected: RetirementInventoryRef,
    ) -> Result<Self> {
        if expected.reducer_version != REDUCER_VERSION {
            return Err(ManagementError::InvalidEvidence);
        }
        let resident = cursor.budget_mut().reserve(v3::FRAME_BYTES * 4)?;
        let spool = InventorySpool::allocate(path, reservation, None).await?;
        for sql in [
            "CREATE TABLE import_chunks(row_key TEXT,column_name TEXT,offset INTEGER,bytes BLOB NOT NULL,PRIMARY KEY(row_key,column_name,offset)) WITHOUT ROWID",
            "CREATE TABLE import_rows(row_key TEXT PRIMARY KEY,table_name TEXT NOT NULL) WITHOUT ROWID",
            "CREATE TABLE import_columns(row_key TEXT,column_name TEXT,length INTEGER,PRIMARY KEY(row_key,column_name)) WITHOUT ROWID",
            "CREATE TABLE import_members(inventory TEXT,table_name TEXT,row_key TEXT,position INTEGER NOT NULL,PRIMARY KEY(inventory,table_name,row_key),UNIQUE(inventory,position)) WITHOUT ROWID",
            "CREATE TABLE import_eof(inventory TEXT,table_name TEXT,PRIMARY KEY(inventory,table_name)) WITHOUT ROWID",
            "CREATE TABLE import_meta(row_key TEXT,column_name TEXT,payload BLOB NOT NULL,PRIMARY KEY(row_key,column_name)) WITHOUT ROWID",
        ] {
            exec(&spool.db, sql, vec![]).await?;
        }
        Ok(Self {
            cursor,
            spool,
            expected,
            header: None,
            configuration: None,
            row: None,
            column: String::new(),
            buffer: vec![],
            guards: vec![],
            _resident: resident,
            offset: 0,
            position: 0,
            part_column: 0,
            part_offset: 0,
            eof_table: 0,
            rows: 0,
            bytes: 0,
            wire_bytes: 0,
            exhausted: false,
            archived: false,
            reduced: false,
            failed: false,
        })
    }
    /// Keep this bounded cursor between calls. A cancelled/invalid cursor is
    /// terminal; restart from the protected original file with a fresh importer.
    pub async fn advance(&mut self, limits: &RunLimits) -> Result<Progress> {
        if self.failed {
            return Err(ManagementError::InvalidEvidence);
        }
        let result = self.advance_inner(limits).await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    async fn advance_inner(&mut self, limits: &RunLimits) -> Result<Progress> {
        let mut run = super::spool::task_run(&self.spool.db, limits).await?;
        run.check()?;
        if self.reduced {
            return Ok(Progress::Reduced);
        }
        while !self.exhausted {
            let tx = self
                .spool
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            let mut pending = false;
            let result = async {
                for _ in 0..16 {
                    if !run.page(self.rows, self.wire_bytes)? {
                        pending = true;
                        break;
                    }
                    self.cursor.budget_mut().charge(4, 0, 0, 0)?;
                    match self.cursor.next_event().map_err(wire_error)? {
                        Some(event) => self.event(&tx, event, &run).await?,
                        None => {
                            if !self.cursor.verified() || self.row.is_some() {
                                return Err(ManagementError::InvalidEvidence);
                            }
                            self.exhausted = true;
                            break;
                        }
                    }
                }
                run.check()
            }
            .await;
            if let Err(error) = result {
                tx.rollback().await.map_err(|_| ManagementError::Database)?;
                return Err(error);
            }
            tx.commit().await.map_err(|_| ManagementError::Database)?;
            if pending {
                return Ok(Progress::Pending);
            }
        }
        if !self.archived {
            if !self.finish_originals(&mut run).await? {
                return Ok(Progress::Pending);
            }
            self.archived = true;
        }
        let config = self
            .configuration
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?;
        if self.spool.reduce(config, limits).await? == Progress::Pending {
            return Ok(Progress::Pending);
        }
        let expected: InventoryTotals = self.metadata("totals").await?;
        if self.spool.frozen_review().await?.totals() != &expected {
            return Err(ManagementError::Changed);
        }
        run.check()?;
        self.reduced = true;
        Ok(Progress::Reduced)
    }
    pub fn into_parts(self) -> Result<(InventorySpool, S, Budget)> {
        if !self.reduced || self.failed {
            return Err(ManagementError::InvalidEvidence);
        }
        let (original, budget) = self.cursor.into_verified().map_err(wire_error)?;
        Ok((self.spool, original, budget))
    }
    async fn event(&mut self, tx: &DatabaseTransaction, event: Event, run: &Run<'_>) -> Result<()> {
        match event {
            Event::Frame(Frame::Header {
                projection,
                database_scope,
                database_identity,
                catalog,
                ..
            }) => {
                if projection != v3::Projection::PrivateRecovery {
                    return Err(ManagementError::InvalidEvidence);
                }
                self.header = Some((database_scope, database_identity, catalog));
            }
            Event::Frame(Frame::Configuration { configuration }) => {
                self.configuration = configuration
            }
            Event::Frame(Frame::Row {
                section,
                key,
                cells,
            }) => {
                if section == Section::Native {
                    let table = native::current_catalog()
                        .iter()
                        .find(|t| t.table == key.table)
                        .ok_or(ManagementError::InvalidEvidence)?;
                    if cells as usize != table.columns.len()
                        || key.values.len() != table.primary_key.len()
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                }
                self.row = Some((section, key));
            }
            Event::Frame(Frame::Cell { column }) => {
                if self
                    .row
                    .as_ref()
                    .is_some_and(|(section, _)| *section == Section::Native)
                {
                    let key = &self.row.as_ref().unwrap().1;
                    if !native::current_catalog()
                        .iter()
                        .find(|t| t.table == key.table)
                        .unwrap()
                        .columns
                        .contains(&column)
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                }
                self.column = column;
                self.offset = 0;
                self.buffer.clear();
                self.guards.clear();
            }
            Event::Chunk(bytes) => {
                self.wire_bytes = self
                    .wire_bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or(ManagementError::Budget)?;
                if self.wire_bytes > run.total_bytes {
                    return Err(ManagementError::Budget);
                }
                let (section, key) = self.row.as_ref().ok_or(ManagementError::InvalidEvidence)?;
                if *section == Section::Native {
                    self.bytes = self
                        .bytes
                        .checked_add(bytes.len() as u64)
                        .ok_or(ManagementError::Budget)?;
                    self.buffer.extend_from_slice(&bytes);
                    if self.buffer.len() >= CELL_SLICE {
                        let tail = self.buffer.split_off(CELL_SLICE);
                        store_chunk(tx, &encode(key)?, &self.column, self.offset, &self.buffer)
                            .await?;
                        self.offset += CELL_SLICE as u64;
                        self.buffer = tail;
                    }
                } else if matches!(section, Section::Membership | Section::Inventory) {
                    if self.buffer.len().saturating_add(bytes.len()) > super::super::MAX_TEXT_BYTES
                    {
                        return Err(ManagementError::Budget);
                    }
                    self.guards
                        .push(self.cursor.budget_mut().reserve(bytes.len() * 2)?);
                    self.buffer.extend_from_slice(&bytes);
                }
            }
            Event::Frame(Frame::CellEnd { value }) => {
                if value == CellValue::Redacted {
                    return Err(ManagementError::InvalidEvidence);
                }
                let (section, key) = self.row.as_ref().ok_or(ManagementError::InvalidEvidence)?;
                if *section == Section::Native {
                    let length = (value == CellValue::Text)
                        .then_some(self.offset + self.buffer.len() as u64);
                    let table = native::current_catalog()
                        .iter()
                        .find(|t| t.table == key.table)
                        .unwrap();
                    if let Some(index) = table
                        .primary_key
                        .iter()
                        .position(|name| name == &self.column)
                        && (value != CellValue::Text
                            || self.offset != 0
                            || self.buffer != key.values[index].as_bytes())
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    if !self.buffer.is_empty() || self.offset == 0 {
                        store_chunk(tx, &encode(key)?, &self.column, self.offset, &self.buffer)
                            .await?;
                    }
                    exec(
                        tx,
                        "INSERT INTO import_columns VALUES($1,$2,$3)",
                        vec![
                            encode(key)?.into(),
                            self.column.clone().into(),
                            length
                                .map(|n| i64::try_from(n).map_err(|_| ManagementError::Budget))
                                .transpose()?
                                .into(),
                        ],
                    )
                    .await?;
                } else if matches!(section, Section::Membership | Section::Inventory) {
                    if value != CellValue::Text {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    self.metadata_cell(tx, *section, key).await?;
                }
                self.buffer.clear();
                self.guards.clear();
            }
            Event::Frame(Frame::RowEnd) => {
                let (section, key) = self.row.take().ok_or(ManagementError::InvalidEvidence)?;
                if section == Section::Native {
                    self.rows = self.rows.checked_add(1).ok_or(ManagementError::Budget)?;
                    if self.rows > run.total_rows {
                        return Err(ManagementError::Budget);
                    }
                    exec(
                        tx,
                        "INSERT INTO import_rows VALUES($1,$2)",
                        vec![encode(&key)?.into(), key.table.into()],
                    )
                    .await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn metadata_cell(
        &self,
        tx: &DatabaseTransaction,
        section: Section,
        key: &NativeKey,
    ) -> Result<()> {
        if section == Section::Membership && key.table == "retirement_membership" {
            let [inventory, table, kind, rest @ ..] = key.values.as_slice() else {
                return Err(ManagementError::InvalidEvidence);
            };
            let definition = native::current_catalog()
                .iter()
                .find(|known| &known.table == table)
                .ok_or(ManagementError::InvalidEvidence)?;
            match (kind.as_str(), self.column.as_str()) {
                ("eof", "complete")
                    if rest.is_empty()
                        && serde_json::from_slice::<bool>(&self.buffer).ok() == Some(true) =>
                {
                    exec(
                        tx,
                        "INSERT INTO import_eof VALUES($1,$2)",
                        vec![inventory.clone().into(), table.clone().into()],
                    )
                    .await?
                }
                ("member", "position") => {
                    if rest.len() != definition.primary_key.len() {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    let position: i64 = serde_json::from_slice(&self.buffer)
                        .map_err(|_| ManagementError::InvalidEvidence)?;
                    if position < 1 {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    let native = NativeKey {
                        table: table.clone(),
                        values: rest.to_vec(),
                    };
                    exec(
                        tx,
                        "INSERT INTO import_members VALUES($1,$2,$3,$4)",
                        vec![
                            inventory.clone().into(),
                            table.clone().into(),
                            encode(&native)?.into(),
                            position.into(),
                        ],
                    )
                    .await?;
                }
                _ => return Err(ManagementError::InvalidEvidence),
            }
        } else if section == Section::Inventory {
            exec(
                tx,
                "INSERT INTO import_meta VALUES($1,$2,$3)",
                vec![
                    encode(key)?.into(),
                    self.column.clone().into(),
                    self.buffer.clone().into(),
                ],
            )
            .await?;
        }
        Ok(())
    }
    async fn metadata<T: serde::de::DeserializeOwned>(&self, column: &str) -> Result<T> {
        let key = NativeKey {
            table: "retirement_inventory".into(),
            values: vec![self.expected.inventory_id.clone()],
        };
        let row = one(
            &self.spool.db,
            "SELECT payload FROM import_meta WHERE row_key=$1 AND column_name=$2",
            vec![encode(&key)?.into(), column.into()],
        )
        .await?
        .ok_or(ManagementError::InvalidEvidence)?;
        serde_json::from_slice(&get::<Vec<u8>>(&row, "payload")?)
            .map_err(|_| ManagementError::InvalidEvidence)
    }
}
async fn store_chunk(
    tx: &impl ConnectionTrait,
    key: &str,
    column: &str,
    offset: u64,
    bytes: &[u8],
) -> Result<()> {
    let offset = i64::try_from(offset).map_err(|_| ManagementError::Budget)?;
    if let Some(old) = one(
        tx,
        "SELECT bytes FROM import_chunks WHERE row_key=$1 AND column_name=$2 AND offset=$3",
        vec![key.into(), column.into(), offset.into()],
    )
    .await?
    {
        if get::<Vec<u8>>(&old, "bytes")? != bytes {
            return Err(ManagementError::Changed);
        }
    } else {
        exec(
            tx,
            "INSERT INTO import_chunks VALUES($1,$2,$3,$4)",
            vec![
                key.into(),
                column.into(),
                offset.into(),
                bytes.to_vec().into(),
            ],
        )
        .await?;
    }
    Ok(())
}
fn wire_error(error: SafeError) -> ManagementError {
    match error {
        SafeError::LimitExceeded => ManagementError::Budget,
        SafeError::SnapshotChanged => ManagementError::Changed,
        _ => ManagementError::InvalidEvidence,
    }
}

impl InventorySpool {
    /// Reconstruct the bounded capture referred to by normal v3 item processing.
    /// The result is frozen review evidence, never execution authority.
    pub async fn restore_capture(
        &self,
        plan_id: &str,
        request: &ItemRequest,
        refs: &RetirementEvidenceRefs,
    ) -> Result<RetirementCapture> {
        refs.validate_coordinates(plan_id, &request.item_id)?;
        if &refs.inventory != self.reference() {
            return Err(ManagementError::Changed);
        }
        let key = NativeKey {
            table: "retirement_mutation".into(),
            values: vec![plan_id.into(), request.item_id.clone()],
        };
        let row=one(&self.db,"SELECT payload FROM import_meta WHERE row_key=$1 AND column_name='inventory_reference'",vec![encode(&key)?.into()]).await?.ok_or(ManagementError::InvalidEvidence)?;
        let inventory: RetirementInventoryRef =
            serde_json::from_slice(&get::<Vec<u8>>(&row, "payload")?)
                .map_err(|_| ManagementError::InvalidEvidence)?;
        if inventory != refs.inventory {
            return Err(ManagementError::Changed);
        }
        let row = one(
            &self.db,
            "SELECT payload FROM import_meta WHERE row_key=$1 AND column_name='reference'",
            vec![encode(&key)?.into()],
        )
        .await?
        .ok_or(ManagementError::InvalidEvidence)?;
        let reference: RetirementMutationRef =
            serde_json::from_slice(&get::<Vec<u8>>(&row, "payload")?)
                .map_err(|_| ManagementError::InvalidEvidence)?;
        if reference != refs.mutation {
            return Err(ManagementError::Changed);
        }
        let row = one(
            &self.db,
            "SELECT payload FROM import_meta WHERE row_key=$1 AND column_name='input_identity'",
            vec![encode(&key)?.into()],
        )
        .await?
        .ok_or(ManagementError::InvalidEvidence)?;
        let mutation: RetirementMutationScope =
            serde_json::from_slice(&get::<Vec<u8>>(&row, "payload")?)
                .map_err(|_| ManagementError::InvalidEvidence)?;
        let capture = self
            .frozen_review()
            .await?
            .capture(request, mutation.clone())
            .await?;
        if capture.mutation_scope != mutation {
            return Err(ManagementError::Changed);
        }
        Ok(capture)
    }
}
