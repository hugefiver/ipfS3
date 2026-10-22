use super::*;
use crate::store::pinning::management::native::meter::CELL_SLICE;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value};
use std::{
    path::Path,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
pub(super) mod budget;
mod keys;
#[cfg(test)]
#[path = "spool/run_tests.rs"]
mod production_tests;
mod projection;
mod rows;
pub(super) use budget::task_run;
pub(super) use keys::page_keys;

/// Per invocation limits do not reset the cumulative task counters.
pub struct RunLimits {
    pub pages: u64,
    pub total_bytes: u64,
    pub total_rows: u64,
    pub elapsed: Duration,
    pub cancellation: crate::store::pinning::management::transaction::CancellationToken,
    started: std::sync::OnceLock<Instant>,
    consumed_pages: std::sync::Arc<std::sync::atomic::AtomicU64>,
    production_deadline: Option<Instant>,
    #[cfg(test)]
    pub(super) production_clock: Option<std::sync::Arc<std::sync::Mutex<Instant>>>,
}
impl Default for RunLimits {
    fn default() -> Self {
        Self {
            pages: 64,
            total_bytes: 16 * 1024 * 1024 * 1024,
            total_rows: 10_000_000,
            elapsed: Duration::from_secs(30),
            cancellation: Default::default(),
            started: Default::default(),
            consumed_pages: Default::default(),
            production_deadline: None,
            #[cfg(test)]
            production_clock: None,
        }
    }
}
impl RunLimits {
    /// Fix the invocation start before deriving limits and only tighten the
    /// existing hard budgets. Capture and output retain their charged work.
    pub fn constrain_deadline(
        &mut self,
        deadline: Instant,
        budget: &mut crate::store::pinning::management::native::meter::Budget,
    ) -> Result<()> {
        let _ = self.pages_remaining();
        let start = *self.started.get().expect("invocation started");
        self.elapsed = self.elapsed.min(deadline.saturating_duration_since(start));
        let (rows, bytes) = budget.remaining_io_allowance()?;
        budget.restrict_remaining(rows, bytes, self.remaining())
    }
    /// Production stops early enough to settle accepted work under the unchanged
    /// hard deadline. Short invocations retain five sixths for useful work.
    pub fn production_available(&self) -> bool {
        #[cfg(test)]
        if let Some(clock) = &self.production_clock {
            return self.production_available_at(*clock.lock().unwrap());
        }
        self.production_available_at(Instant::now())
    }
    fn production_available_at(&self, now: Instant) -> bool {
        now < self.production_end()
    }
    fn production_end(&self) -> Instant {
        let start = *self.started.get_or_init(Instant::now);
        let end = start + self.elapsed - Duration::from_secs(5).min(self.elapsed / 6);
        self.production_deadline.map_or(end, |prior| prior.min(end))
    }
    pub(super) fn for_production(&self) -> Self {
        let mut limits = self.with_task_caps(self.total_rows, self.total_bytes);
        limits.production_deadline = Some(self.production_end());
        limits
    }
    pub fn check(&self) -> Result<()> {
        Run::new(self).check()
    }
    pub fn pages_remaining(&self) -> u64 {
        if self.remaining().is_zero() {
            return 0;
        }
        self.pages
            .saturating_sub(self.consumed_pages.load(Ordering::Relaxed))
    }
    pub(super) fn reserve_capture(
        &self,
        envelope: (u64, u64),
        budget: &crate::store::pinning::management::native::meter::Budget,
    ) -> Result<Self> {
        let remaining = budget.remaining()?;
        let capture = super::task_budget::capture_work(budget)?;
        let mut limits = self.with_task_caps(
            envelope
                .0
                .min(self.total_rows)
                .checked_sub(capture.rows)
                .ok_or(ManagementError::Budget)?,
            envelope
                .1
                .min(self.total_bytes)
                .checked_sub(capture.bytes)
                .ok_or(ManagementError::Budget)?,
        );
        let start = *limits.started.get_or_init(Instant::now);
        limits.elapsed = limits
            .elapsed
            .min(start.elapsed().saturating_add(remaining));
        Ok(limits)
    }
    pub(crate) fn remaining(&self) -> Duration {
        Run::new(self).remaining()
    }
    pub(super) fn with_task_caps(&self, rows: u64, bytes: u64) -> Self {
        self.started.get_or_init(Instant::now);
        Self {
            pages: self.pages,
            total_rows: rows,
            total_bytes: bytes,
            elapsed: self.elapsed,
            cancellation: self.cancellation.clone(),
            started: self.started.clone(),
            consumed_pages: self.consumed_pages.clone(),
            production_deadline: self.production_deadline,
            #[cfg(test)]
            production_clock: self.production_clock.clone(),
        }
    }
}
pub(super) struct Run<'a> {
    limits: &'a RunLimits,
    deadline: Instant,
    pub(super) total_rows: u64,
    pub(super) total_bytes: u64,
}

impl<'a> Run<'a> {
    pub fn new(limits: &'a RunLimits) -> Self {
        Self {
            limits,
            deadline: *limits.started.get_or_init(Instant::now) + limits.elapsed,
            total_rows: limits.total_rows,
            total_bytes: limits.total_bytes,
        }
    }
    pub fn check(&self) -> Result<()> {
        if self.limits.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            return Err(ManagementError::Budget);
        }
        Ok(())
    }
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
    pub fn task(&self, rows: u64, bytes: u64) -> Result<()> {
        if self.limits.cancellation.is_cancelled()
            || rows > self.total_rows
            || bytes > self.total_bytes
        {
            return Err(ManagementError::Budget);
        }
        Ok(())
    }
    pub fn page(&mut self, rows: u64, bytes: u64) -> Result<bool> {
        self.task(rows, bytes)?;
        if self.remaining().is_zero() {
            return Ok(false);
        }
        if self.limits.production_deadline.is_some() && !self.limits.production_available() {
            return Ok(false);
        }
        if self
            .limits
            .consumed_pages
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pages| {
                if pages < self.limits.pages {
                    Some(pages + 1)
                } else {
                    None
                }
            })
            .is_err()
        {
            return Ok(false);
        }
        Ok(true)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress {
    Pending,
    Archived,
    Reduced,
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveCursor {
    pub ordinal: i64,
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipCursor {
    table: usize,
    position: i64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum InventoryMembership {
    Member(NativeKey),
    End { table: String },
}

/// Original UTF-8 byte slices; SQL NULL and an empty string remain distinct.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivePart {
    pub table: String,
    pub key: NativeKey,
    pub column: String,
    pub offset: u64,
    pub length: Option<u64>,
    pub bytes: Vec<u8>,
}
impl ArchivePart {
    pub(super) fn checksum(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"retirement-original-cell-1\0");
        let key = encode(&self.key)?;
        for value in [self.table.as_str(), key.as_str(), self.column.as_str()] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        hash.update(self.offset.to_be_bytes());
        hash.update([u8::from(self.length.is_some())]);
        hash.update(self.length.unwrap_or(0).to_be_bytes());
        hash.update((self.bytes.len() as u64).to_be_bytes());
        hash.update(&self.bytes);
        Ok(hex::encode(hash.finalize()))
    }
    pub(super) fn check_checksum(&self, expected: &str) -> Result<()> {
        if self.checksum()? != expected {
            return Err(ManagementError::Changed);
        }
        Ok(())
    }
}

pub struct InventorySpool {
    pub(crate) task: Option<std::sync::Arc<super::task_budget::TaskBudget>>,
    pub(super) path: std::path::PathBuf,
    pub(super) shared_route: Option<RouteIdentity>,
    pub(super) db: DatabaseConnection,
    pub(super) reference: RetirementInventoryRef,
    pub(super) source_scanned: bool,
    pub(super) reduced_here: std::sync::atomic::AtomicBool,
    pub(super) verification: super::verify::VerificationCursor,
    pub(super) shared_source: Option<RetirementInventoryRef>,
    pub(super) shared_verification: super::verify::VerificationCursor,
}

pub(super) async fn exec(db: &impl ConnectionTrait, sql: &str, values: Vec<Value>) -> Result<()> {
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        sql,
        values,
    ))
    .await
    .map_err(|_| ManagementError::Database)?;
    Ok(())
}
pub(super) async fn one(
    db: &impl ConnectionTrait,
    sql: &str,
    values: Vec<Value>,
) -> Result<Option<sea_orm::QueryResult>> {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        sql,
        values,
    ))
    .await
    .map_err(|_| ManagementError::Database)
}
pub(super) fn get<T: sea_orm::TryGetable>(row: &sea_orm::QueryResult, name: &str) -> Result<T> {
    row.try_get("", name)
        .map_err(|_| ManagementError::InvalidEvidence)
}

pub(super) async fn connect_file(path: &Path) -> Result<DatabaseConnection> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(false),
        )
        .await
        .map_err(|_| ManagementError::Database)?;
    Ok(sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool))
}

pub(super) async fn close_failed_open<T>(db: &DatabaseConnection, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            db.clone()
                .close()
                .await
                .map_err(|_| ManagementError::Database)?;
            Err(error)
        }
    }
}

impl InventorySpool {
    pub(crate) async fn forward_confirmation_work(&self) -> Result<()> {
        super::task_budget::TaskBudget::forward_cache(&self.db, false).await
    }
    pub async fn close(&self) -> Result<()> {
        self.db
            .clone()
            .close()
            .await
            .map_err(|_| ManagementError::Database)
    }
    /// The local artifact adapter supplies its exclusive, owner-protected
    /// reservation and a protected parent (SQLite journals share that parent).
    /// Neither path nor handle may come from an imported plan.
    pub async fn create(
        path: &Path,
        reservation: std::fs::File,
        checkpoint: &SourceCheckpoint,
    ) -> Result<Self> {
        if !checkpoint.is_start() {
            return Err(ManagementError::InvalidEvidence);
        }
        Self::allocate(path, reservation, Some(checkpoint)).await
    }
    pub(super) async fn allocate(
        path: &Path,
        reservation: std::fs::File,
        checkpoint: Option<&SourceCheckpoint>,
    ) -> Result<Self> {
        if reservation
            .metadata()
            .map_err(|_| ManagementError::Database)?
            .len()
            != 0
        {
            return Err(ManagementError::InvalidEvidence);
        }
        drop(reservation);
        let db = connect_file(path).await?;
        let initialized = async {
        db.execute_unprepared("PRAGMA journal_mode=DELETE; PRAGMA cache_size=-2048; PRAGMA temp_store=FILE; PRAGMA busy_timeout=1000;").await.map_err(|_|ManagementError::Database)?;
        for sql in [
            budget::SCHEMA,
            budget::QUERY_SCHEMA,
            "CREATE TABLE shared_progress(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL)",
            "CREATE TABLE state (id INTEGER PRIMARY KEY CHECK(id=1), reference TEXT NOT NULL, checkpoint TEXT NOT NULL, phase INTEGER NOT NULL, rows BIGINT NOT NULL, bytes BIGINT NOT NULL, reducer_table INTEGER NOT NULL, reducer_after TEXT NOT NULL)",
            "CREATE TABLE parts (ordinal INTEGER PRIMARY KEY, table_name TEXT NOT NULL, row_key TEXT NOT NULL, column_name TEXT NOT NULL, offset BIGINT NOT NULL, length BIGINT, bytes BLOB NOT NULL, checksum TEXT NOT NULL, UNIQUE(table_name,row_key,column_name,offset))",
            "CREATE INDEX parts_first_cells ON parts(table_name,row_key,column_name) WHERE offset=0",
            "CREATE TABLE members (table_name TEXT NOT NULL,row_key TEXT NOT NULL,position BIGINT NOT NULL UNIQUE, PRIMARY KEY(table_name,row_key)) WITHOUT ROWID",
            "CREATE INDEX members_order ON members(table_name,position)",
            "CREATE TABLE wire_keys(table_name TEXT NOT NULL,sort_key BLOB NOT NULL,row_key TEXT NOT NULL,PRIMARY KEY(table_name,sort_key),UNIQUE(table_name,row_key)) WITHOUT ROWID",
            "CREATE TABLE review_cells(table_name TEXT NOT NULL,sort_key BLOB NOT NULL,row_key TEXT NOT NULL,column_name TEXT NOT NULL,payload BLOB NOT NULL,checksums TEXT NOT NULL,PRIMARY KEY(table_name,sort_key,column_name)) WITHOUT ROWID",
            "CREATE TABLE eof (table_name TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID",
            "CREATE TABLE facts (kind TEXT NOT NULL, identity TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(kind,identity)) WITHOUT ROWID",
            "CREATE TABLE quotes (identity TEXT PRIMARY KEY NOT NULL, remote_key TEXT NOT NULL, cid TEXT NOT NULL, bytes TEXT NOT NULL, unknown INTEGER NOT NULL, inflight INTEGER NOT NULL, cleanup INTEGER NOT NULL, retained INTEGER NOT NULL) WITHOUT ROWID",
            "CREATE INDEX quotes_remote ON quotes(remote_key)",
            "CREATE TABLE cids (cid TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID",
            "CREATE TABLE target_edges (target_id TEXT PRIMARY KEY NOT NULL,lease_id TEXT NOT NULL,cid TEXT NOT NULL,allocation TEXT NOT NULL,logical_bytes TEXT NOT NULL,active INTEGER NOT NULL,referenced INTEGER NOT NULL) WITHOUT ROWID",
            "CREATE INDEX targets_resource ON target_edges(allocation,cid,active)",
            "CREATE INDEX targets_lease ON target_edges(lease_id)",
            "CREATE TABLE jobs (identity TEXT PRIMARY KEY NOT NULL,allocation TEXT NOT NULL,cid TEXT NOT NULL,inflight INTEGER NOT NULL,cleanup INTEGER NOT NULL) WITHOUT ROWID",
            "CREATE INDEX jobs_resource ON jobs(allocation,cid,inflight)",
            "CREATE TABLE resource_counts(resource_key TEXT PRIMARY KEY,refs INTEGER NOT NULL,inflight INTEGER NOT NULL,cleanup INTEGER NOT NULL) WITHOUT ROWID",
            "CREATE TABLE allocation_attribution(allocation TEXT PRIMARY KEY,domain TEXT) WITHOUT ROWID",
            "CREATE TABLE totals_progress(id INTEGER PRIMARY KEY CHECK(id=1),stage INTEGER NOT NULL,after_key TEXT NOT NULL,payload TEXT NOT NULL)",
            "CREATE TABLE restore_progress(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL)",
        ] {
            db.execute_unprepared(sql)
                .await
                .map_err(|_| ManagementError::Database)?;
        }
        let reference = RetirementInventoryRef {
            archive_id: uuid::Uuid::new_v4().to_string(),
            inventory_id: uuid::Uuid::new_v4().to_string(),
            reducer_version: REDUCER_VERSION,
        };
        exec(
            &db,
            "INSERT INTO state VALUES (1,$1,$2,0,0,0,0,'')",
            vec![encode(&reference)?.into(), encode(&checkpoint)?.into()],
        )
        .await?;
        exec(
            &db,
            "INSERT INTO totals_progress VALUES(1,0,'',$1)",
            vec![
                encode(&InventoryTotals {
                    attribution_complete: true,
                    routes_available: true,
                    ..Default::default()
                })?
                .into(),
            ],
        )
        .await?;
        Ok(reference)
        }.await;
        let reference = close_failed_open(&db, initialized).await?;
        Ok(Self {
            task: None,
            path: path.to_owned(),
            shared_route: None,
            db,
            reference,
            source_scanned: checkpoint.is_some(),
            reduced_here: std::sync::atomic::AtomicBool::new(false),
            verification: Default::default(),
            shared_source: None,
            shared_verification: Default::default(),
        })
    }

    pub async fn resume(path: &Path) -> Result<Self> {
        if !path.is_file() {
            return Err(ManagementError::InvalidEvidence);
        }
        let db = connect_file(path).await?;
        let initialized = async {
            db.execute_unprepared(
                "PRAGMA cache_size=-2048; PRAGMA temp_store=FILE; PRAGMA busy_timeout=1000;",
            )
            .await
            .map_err(|_| ManagementError::Database)?;
            let row = one(&db, "SELECT reference FROM state WHERE id=1", vec![])
                .await?
                .ok_or(ManagementError::InvalidEvidence)?;
            let reference: RetirementInventoryRef = decode(&get::<String>(&row, "reference")?)?;
            if reference.reducer_version != REDUCER_VERSION {
                return Err(ManagementError::InvalidEvidence);
            }
            Ok(reference)
        }
        .await;
        let reference = close_failed_open(&db, initialized).await?;
        Ok(Self {
            task: None,
            path: path.to_owned(),
            shared_route: None,
            db,
            reference,
            source_scanned: false,
            reduced_here: std::sync::atomic::AtomicBool::new(false),
            verification: Default::default(),
            shared_source: None,
            shared_verification: Default::default(),
        })
    }
    /// Only PlanningCheckpoint may adopt operator-selected, owner-protected
    /// local task progress. Imported archives continue through untrusted resume.
    pub(super) async fn resume_task(path: &Path) -> Result<Self> {
        let spool = Self::resume(path).await?;
        let state = one(
            &spool.db,
            "SELECT phase,checkpoint FROM state WHERE id=1",
            vec![],
        )
        .await?
        .ok_or(ManagementError::InvalidEvidence)?;
        let phase = get::<i64>(&state, "phase")?;
        let mut spool = spool;
        if !(0..=2).contains(&phase)
            || phase > 0
                && get::<String>(&state, "checkpoint")? != "null"
                && !spool.checkpoint().await?.complete()
        {
            spool.close().await?;
            return Err(ManagementError::InvalidEvidence);
        }
        spool.source_scanned = false;
        spool.shared_verification = super::verify::restore_shared(&spool.db).await?;
        spool
            .reduced_here
            .store(phase == 2, std::sync::atomic::Ordering::Release);
        Ok(spool)
    }
    pub fn reference(&self) -> &RetirementInventoryRef {
        &self.reference
    }
    pub async fn checkpoint(&self) -> Result<SourceCheckpoint> {
        let sql = if self.shared_route.is_some() {
            "SELECT checkpoint FROM original.state WHERE id=1"
        } else {
            "SELECT checkpoint FROM state WHERE id=1"
        };
        let row = one(&self.db, sql, vec![])
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
        let mut checkpoint: SourceCheckpoint = decode(&get::<String>(&row, "checkpoint")?)?;
        if let Some(route) = &self.shared_route {
            checkpoint.route = route.clone();
        }
        Ok(checkpoint)
    }
    pub async fn read_part(&self, cursor: &mut ArchiveCursor) -> Result<Option<ArchivePart>> {
        let Some(row) = one(
            &self.db,
            "SELECT * FROM parts WHERE ordinal>$1 ORDER BY ordinal LIMIT 1",
            vec![cursor.ordinal.into()],
        )
        .await?
        else {
            return Ok(None);
        };
        let bytes: Vec<u8> = get(&row, "bytes")?;
        if bytes.len() > CELL_SLICE {
            return Err(ManagementError::InvalidEvidence);
        }
        let next: i64 = get(&row, "ordinal")?;
        if next != cursor.ordinal + 1 {
            return Err(ManagementError::InvalidEvidence);
        }
        cursor.ordinal = next;
        let part = ArchivePart {
            table: get(&row, "table_name")?,
            key: decode(&get::<String>(&row, "row_key")?)?,
            column: get(&row, "column_name")?,
            offset: get::<i64>(&row, "offset")?
                .try_into()
                .map_err(|_| ManagementError::InvalidEvidence)?,
            length: get::<Option<i64>>(&row, "length")?
                .map(u64::try_from)
                .transpose()
                .map_err(|_| ManagementError::InvalidEvidence)?,
            bytes,
        };
        part.check_checksum(&get::<String>(&row, "checksum")?)?;
        Ok(Some(part))
    }
    /// Includes an explicit end marker for every source table, including empty
    /// memberships. Member order is the source's native composite-key order.
    pub async fn read_membership(
        &self,
        cursor: &mut MembershipCursor,
    ) -> Result<Option<InventoryMembership>> {
        let Some(table) =
            crate::store::pinning::management::native::current_catalog().get(cursor.table)
        else {
            return Ok(None);
        };
        if let Some(row)=one(&self.db,"SELECT row_key,position FROM members WHERE table_name=$1 AND position>$2 ORDER BY position LIMIT 1",vec![table.table.clone().into(),cursor.position.into()]).await? {
            cursor.position=get(&row,"position")?;
            return Ok(Some(InventoryMembership::Member(decode(&get::<String>(&row,"row_key")?)?)))
        }
        if one(
            &self.db,
            "SELECT table_name FROM eof WHERE table_name=$1",
            vec![table.table.clone().into()],
        )
        .await?
        .is_none()
        {
            return Err(ManagementError::InvalidEvidence);
        }
        cursor.table += 1;
        cursor.position = 0;
        Ok(Some(InventoryMembership::End {
            table: table.table.clone(),
        }))
    }
    pub(super) async fn row(&self, key: &NativeKey) -> Result<Option<NativeRow>> {
        self.read_row(&self.db, key, false).await
    }
    pub(super) async fn contains_in(
        &self,
        db: &impl ConnectionTrait,
        key: &NativeKey,
    ) -> Result<bool> {
        Ok(one(
            db,
            "SELECT position FROM members WHERE table_name=$1 AND row_key=$2",
            vec![key.table.clone().into(), encode(key)?.into()],
        )
        .await?
        .is_some())
    }
    /// Error text and opaque root verification bytes remain in the original
    /// archive. Responsibility classification does not parse or copy them.
    pub(super) async fn projection_row(&self, key: &NativeKey) -> Result<Option<NativeRow>> {
        self.read_row(&self.db, key, true).await
    }
    pub(super) async fn projection_row_in(
        &self,
        db: &impl ConnectionTrait,
        key: &NativeKey,
    ) -> Result<Option<NativeRow>> {
        self.read_row(db, key, true).await
    }
}

pub(super) async fn put_part(tx: &impl ConnectionTrait, part: &ArchivePart) -> Result<()> {
    if part.bytes.len() > CELL_SLICE
        || part
            .length
            .is_none_or(|n| part.offset + part.bytes.len() as u64 > n)
            && !(part.length.is_none() && part.offset == 0 && part.bytes.is_empty())
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let key = encode(&part.key)?;
    let args = vec![
        part.table.clone().into(),
        key.clone().into(),
        part.column.clone().into(),
        (part.offset as i64).into(),
    ];
    if let Some(row)=one(tx,"SELECT length,bytes FROM parts WHERE table_name=$1 AND row_key=$2 AND column_name=$3 AND offset=$4",args).await? {
        if get::<Option<i64>>(&row,"length")?!=part.length.map(|n|n as i64)||get::<Vec<u8>>(&row,"bytes")?!=part.bytes {return Err(ManagementError::Changed)}
        return Ok(())
    }
    exec(tx,"INSERT INTO parts(table_name,row_key,column_name,offset,length,bytes,checksum) VALUES($1,$2,$3,$4,$5,$6,$7)",vec![part.table.clone().into(),key.into(),part.column.clone().into(),(part.offset as i64).into(),part.length.map(|n|n as i64).into(),part.bytes.clone().into(),part.checksum()?.into()]).await
}

pub(super) async fn fact(
    tx: &impl ConnectionTrait,
    kind: &str,
    identity: &str,
    value: &impl Serialize,
) -> Result<()> {
    let payload = encode(value)?;
    let inserted = tx
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO facts VALUES($1,$2,$3) ON CONFLICT(kind,identity) DO NOTHING",
            vec![kind.into(), identity.into(), payload.clone().into()],
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    if inserted.rows_affected() == 1 {
        return Ok(());
    }
    if let Some(row) = one(
        tx,
        "SELECT payload FROM facts WHERE kind=$1 AND identity=$2",
        vec![kind.into(), identity.into()],
    )
    .await?
    {
        if get::<String>(&row, "payload")? != payload {
            return Err(ManagementError::Changed);
        }
        return Ok(());
    }
    Err(ManagementError::Changed)
}

/// Preserves NativeKey component order, including NUL and prefix strings.
/// JSON serialization is not a lexical key encoding.
pub(super) fn wire_key(key: &NativeKey) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in &key.values {
        for byte in value.as_bytes() {
            bytes.extend_from_slice(&[1, *byte]);
        }
        bytes.push(0);
    }
    bytes
}
pub(super) async fn index_wire_key(tx: &impl ConnectionTrait, key: &NativeKey) -> Result<()> {
    exec(
        tx,
        "INSERT INTO wire_keys VALUES($1,$2,$3)",
        vec![
            key.table.clone().into(),
            wire_key(key).into(),
            encode(key)?.into(),
        ],
    )
    .await
}

#[cfg(test)]
mod run_tests {
    use super::*;

    #[test]
    fn elapsed_slice_yields_but_cancellation_and_task_exhaustion_fail() {
        let limits = RunLimits {
            elapsed: Duration::ZERO,
            ..Default::default()
        };
        assert_eq!(Run::new(&limits).page(0, 0), Ok(false));
        limits.cancellation.cancel();
        assert_eq!(Run::new(&limits).page(0, 0), Err(ManagementError::Budget));
        let limits = RunLimits {
            total_rows: 1,
            total_bytes: 1,
            ..Default::default()
        };
        assert_eq!(Run::new(&limits).page(2, 0), Err(ManagementError::Budget));
        assert_eq!(Run::new(&limits).page(0, 2), Err(ManagementError::Budget));
    }
}
