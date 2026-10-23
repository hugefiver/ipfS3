use super::{
    spool::{Run, exec, get, one},
    *,
};
use crate::store::pinning::management::native::{
    self, NativeTable,
    meter::{Budget, CELL_SLICE},
};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction, Statement,
    TransactionTrait, Value,
};
use std::collections::BTreeMap;
pub(super) mod attest;
mod batch;
mod cells;
mod check_read;
mod fence;
mod page;
mod part;
pub(super) use part::Part;
mod stage;
mod verify;

#[derive(Default)]
pub(super) struct ReadWork {
    keyset: QueryWork,
    cells: QueryWork,
    returned_bytes: u64,
    copied_bytes: u64,
}

impl ReadWork {
    // SQL values use their returned UTF-8/BLOB length; nullable BIGINT metadata
    // reserves an eight-byte slot. Explicit original-projection copies and
    // temporary key JSON are separate from reads and from archive serialization.
    fn returned(&mut self, bytes: u64) -> Result<()> {
        self.returned_bytes = self
            .returned_bytes
            .checked_add(bytes)
            .ok_or(ManagementError::Budget)?;
        Ok(())
    }
    fn copied(&mut self, bytes: u64) -> Result<()> {
        self.copied_bytes = self
            .copied_bytes
            .checked_add(bytes)
            .ok_or(ManagementError::Budget)?;
        Ok(())
    }
    fn total_bytes(&self) -> Result<u64> {
        self.returned_bytes
            .checked_add(self.copied_bytes)
            .ok_or(ManagementError::Budget)
    }
}

fn key_owned_bytes(key: &NativeKey) -> u64 {
    (key.table.len() + key.values.iter().map(String::len).sum::<usize>()) as u64
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCheckpoint {
    pub database_scope: String,
    pub database_identity: String,
    pub catalog_revision: String,
    pub catalog: Vec<String>,
    pub revisions: BTreeMap<String, i64>,
    pub route: RouteIdentity,
    pub observed_at: chrono::DateTime<chrono::Utc>,
    status_scan: bool,
    table: usize,
    after: Option<NativeKey>,
    row: Option<NativeKey>,
    column: usize,
    offset: u64,
}
impl SourceCheckpoint {
    pub async fn start(tx: &DatabaseTransaction, route: RouteIdentity) -> Result<Self> {
        let catalog = crate::store::pinning::management::schema::validate(tx).await?;
        crate::store::pinning::management::catalog::validate(tx, native::current_catalog()).await?;
        let (database_scope, database_identity) = native::scope::binding(tx).await?;
        let clock = if tx.get_database_backend() == DatabaseBackend::Sqlite {
            "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now') AS now"
        } else {
            "SELECT CAST(clock_timestamp() AS TEXT) AS now"
        };
        let now = tx
            .query_one(Statement::from_string(tx.get_database_backend(), clock))
            .await
            .map_err(|_| ManagementError::Database)?
            .ok_or(ManagementError::Database)?;
        let observed_at = native::parse_time(&get::<String>(&now, "now")?)?;
        Ok(Self {
            database_scope,
            database_identity,
            catalog_revision: crate::store::pinning::management::schema::VERSION.into(),
            catalog,
            revisions: vector(tx).await?,
            route,
            observed_at,
            status_scan: false,
            table: 0,
            after: None,
            row: None,
            column: 0,
            offset: 0,
        })
    }
    pub async fn start_status(tx: &DatabaseTransaction, route: RouteIdentity) -> Result<Self> {
        let mut checkpoint = Self::start(tx, route).await?;
        checkpoint.status_scan = true;
        Ok(checkpoint)
    }
    pub(super) fn is_status(&self) -> bool {
        self.status_scan
    }
    pub(super) fn is_start(&self) -> bool {
        self.table == 0
            && self.row.is_none()
            && self.after.is_none()
            && self.column == 0
            && self.offset == 0
    }
    /// Rewinds coordinates for rebuilding a frozen archive in a new task spool;
    /// the original revision vector, identity and clock remain unchanged.
    pub fn rewind(&self) -> Self {
        let mut checkpoint = self.clone();
        checkpoint.table = 0;
        checkpoint.after = None;
        checkpoint.row = None;
        checkpoint.column = 0;
        checkpoint.offset = 0;
        checkpoint
    }
    pub async fn check(&self, tx: &DatabaseTransaction) -> Result<()> {
        self.check_counted(tx).await.map(|_| ())
    }
    pub(super) async fn check_counted(&self, tx: &DatabaseTransaction) -> Result<u64> {
        let catalog = native::current_catalog();
        if self.table > catalog.len()
            || self.row.is_none() && (self.column != 0 || self.offset != 0)
            || self.offset > i64::MAX as u64
        {
            return Err(ManagementError::InvalidEvidence);
        }
        for key in self.row.iter().chain(self.after.iter()) {
            let table = catalog
                .get(self.table)
                .ok_or(ManagementError::InvalidEvidence)?;
            if key.table != table.table
                || key.values.len() != table.primary_key.len()
                || key.values.iter().any(|value| value.len() > 1024)
                || self.row.is_some() && self.column >= table.columns.len()
            {
                return Err(ManagementError::InvalidEvidence);
            }
        }
        if self.catalog_revision != crate::store::pinning::management::schema::VERSION {
            return Err(ManagementError::Changed);
        }
        let read = check_read::CheckRead::new(tx);
        let binding = native::scope::binding(&read).await?;
        if binding.0 != self.database_scope
            || binding.1 != self.database_identity
            || vector(&read).await? != self.revisions
        {
            return Err(ManagementError::Changed);
        }
        Ok(read.bytes())
    }
    pub fn complete(&self) -> bool {
        self.table == native::current_catalog().len() && self.row.is_none()
    }
    /// Called only after the executor's ordered row locks. Holds a phantom fence
    /// while the caller publishes its exact marker/receipt transaction.
    pub async fn fence(&self, tx: &DatabaseTransaction) -> Result<()> {
        native::scope::writer_boundary(tx).await?;
        if tx.get_database_backend() == DatabaseBackend::Postgres {
            fence::lock_sources(tx).await?;
        }
        if crate::store::pinning::management::schema::validate(tx).await? != self.catalog {
            return Err(ManagementError::Changed);
        }
        crate::store::pinning::management::catalog::validate(tx, native::current_catalog()).await?;
        self.check(tx).await
    }
}
async fn vector(tx: &impl ConnectionTrait) -> Result<BTreeMap<String, i64>> {
    let catalog = native::current_catalog();
    let parameters = (1..=catalog.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(",");
    let rows = tx.query_all(Statement::from_sql_and_values(
        tx.get_database_backend(),
        format!("SELECT source_table,revision FROM pin_management_revision WHERE source_table IN ({parameters}) ORDER BY source_table"),
        catalog.iter().map(|table| Value::from(table.table.clone())),
    )).await.map_err(|_| ManagementError::Database)?;
    if rows.len() != catalog.len() {
        return Err(ManagementError::Schema);
    }
    let mut result = BTreeMap::new();
    for row in rows {
        if result
            .insert(get(&row, "source_table")?, get(&row, "revision")?)
            .is_some()
        {
            return Err(ManagementError::Schema);
        }
    }
    Ok(result)
}
fn bound(t: &NativeTable, column: &str, value: &str) -> Result<Value> {
    match t
        .definitions
        .iter()
        .find(|c| c.name == column)
        .ok_or(ManagementError::Schema)?
        .postgres_type
        .as_str()
    {
        "bigint" => Ok(value
            .parse::<i64>()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .into()),
        "integer" => Ok(value
            .parse::<i32>()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .into()),
        "boolean" => Ok(match value {
            "1" | "true" => true,
            "0" | "false" => false,
            _ => return Err(ManagementError::InvalidEvidence),
        }
        .into()),
        "text" => Ok(value.into()),
        _ => Err(ManagementError::Schema),
    }
}
fn octets(db: DatabaseBackend, column: &str) -> String {
    if db == DatabaseBackend::Sqlite {
        format!("CAST(\"{column}\" AS BLOB)")
    } else {
        format!("convert_to(CAST(\"{column}\" AS TEXT),'UTF8')")
    }
}
async fn next_key(
    tx: &DatabaseTransaction,
    table: &NativeTable,
    after: Option<&NativeKey>,
    work: &mut ReadWork,
) -> Result<Option<NativeKey>> {
    let mut values = Vec::new();
    let order = table
        .primary_key
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let predicate = if let Some(after) = after {
        if after.table != table.table || after.values.len() != table.primary_key.len() {
            return Err(ManagementError::InvalidEvidence);
        }
        for (column, value) in table.primary_key.iter().zip(&after.values) {
            values.push(bound(table, column, value)?)
        }
        format!(
            "WHERE ({order}) > ({})",
            (1..=values.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    } else {
        String::new()
    };
    let projection = table
        .primary_key
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "CASE WHEN length({})<=1024 THEN CAST(\"{c}\" AS TEXT) END AS k{i}",
                octets(tx.get_database_backend(), c)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let started = std::time::Instant::now();
    let row = tx
        .query_one(Statement::from_sql_and_values(
            tx.get_database_backend(),
            format!(
                "SELECT {projection} FROM \"{}\" {predicate} ORDER BY {order} LIMIT 1",
                table.table
            ),
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    work.keyset.observe(
        u64::from(row.is_some()),
        u64::from(row.is_some()) * table.primary_key.len() as u64,
        0,
        started.elapsed(),
    );
    let Some(row) = row else {
        return Ok(None);
    };
    let key = NativeKey {
        table: table.table.clone(),
        values: (0..table.primary_key.len())
            .map(|i| get::<Option<String>>(&row, &format!("k{i}"))?.ok_or(ManagementError::Budget))
            .collect::<Result<_>>()?,
    };
    let returned = key.values.iter().map(|v| v.len() as u64).sum::<u64>();
    work.keyset.bytes += returned;
    work.returned(returned)?;
    work.copied(table.table.len() as u64)?;
    Ok(Some(key))
}
impl InventorySpool {
    /// No source transaction survives a chunk/page. Each accepted chunk and its
    /// exact resumable cursor commit atomically in the task-owned spool.
    pub async fn scan(&self, source: &DatabaseConnection, limits: &RunLimits) -> Result<Progress> {
        if self.shared_route.is_some() {
            return self.adopt_shared_scan().await;
        }
        let mut run = super::spool::task_run(&self.db, limits).await?;
        let mut staged_page = false;
        loop {
            if limits.cancellation.is_cancelled() {
                return Err(ManagementError::Budget);
            }
            let state = one(
                &self.db,
                "SELECT checkpoint,phase,rows,bytes FROM state WHERE id=1",
                vec![],
            )
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
            let original_checkpoint: String = get(&state, "checkpoint")?;
            let checkpoint: SourceCheckpoint = decode(&original_checkpoint)?;
            if get::<i64>(&state, "phase")? > 0 {
                return Ok(Progress::Archived);
            }
            let rows = get::<i64>(&state, "rows")? as u64;
            let bytes = get::<i64>(&state, "bytes")? as u64;
            if !run.page(rows, bytes)? {
                return Ok(Progress::Pending);
            }
            let read_started = std::time::Instant::now();
            let mut tx =
                match crate::store::pinning::management::transaction::ManagementTransaction::begin(
                    source,
                    run.remaining(),
                    limits.cancellation.clone(),
                )
                .await
                {
                    Ok(tx) => tx,
                    Err(error) => {
                        return page::interrupted(
                            error,
                            staged_page && !limits.cancellation.is_cancelled(),
                        );
                    }
                };
            let control = page::Control {
                cancel: limits.cancellation.clone(),
                total_rows: run.total_rows,
                total_bytes: run.total_bytes,
                ordered_cells: false,
            };
            let page = match tx
                .run(move |tx, _| Box::pin(page::read(tx, checkpoint, rows, bytes, control)))
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    return page::interrupted(
                        error,
                        staged_page && !limits.cancellation.is_cancelled(),
                    );
                }
            };
            if let Err(error) = tx.rollback().await {
                return page::interrupted(
                    error,
                    staged_page && !limits.cancellation.is_cancelled(),
                );
            }
            let read_elapsed = read_started.elapsed();
            super::spool::budget::charge(
                &self.db,
                &run,
                page.rows - rows,
                page.work.total_bytes()?,
            )
            .await?;
            super::spool::budget::timed(&self.db, "source_read", read_elapsed).await?;
            let stage_started = std::time::Instant::now();
            match self.stage_page(&original_checkpoint, page, &run).await {
                Ok(complete) => {
                    super::spool::budget::timed(&self.db, "spool_stage", stage_started.elapsed())
                        .await?;
                    if complete {
                        return Ok(Progress::Archived);
                    }
                    staged_page = true;
                }
                Err(ManagementError::Budget)
                    if staged_page
                        && run.remaining().is_zero()
                        && !limits.cancellation.is_cancelled() =>
                {
                    return Ok(Progress::Pending);
                }
                Err(error) => return Err(error),
            }
        }
    }
}

/// Exact executor-owned source writes. Revision deltas count every row write,
/// including same-value updates, so an extra same-plan row cannot be ignored.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceTransition {
    pub key: NativeKey,
    pub before: Option<NativeRow>,
    pub after: Option<NativeRow>,
    pub writes: u64,
}
impl SourceCheckpoint {
    pub async fn verify_transition_before(
        &self,
        tx: &DatabaseTransaction,
        changes: &[SourceTransition],
        budget: &mut Budget,
    ) -> Result<()> {
        validate_changes(changes)?;
        self.check(tx).await?;
        for change in changes {
            verify_row(tx, &change.key, change.before.as_ref(), budget).await?;
        }
        Ok(())
    }
    pub async fn verify_transition_after(
        &self,
        tx: &DatabaseTransaction,
        changes: &[SourceTransition],
        budget: &mut Budget,
    ) -> Result<Self> {
        let next = self.verify_recorded_transition(tx, changes, budget).await?;
        if vector(tx).await? != next.revisions {
            return Err(ManagementError::Changed);
        }
        Ok(next)
    }
    /// Verify the exact recorded writes without requiring unrelated live tables
    /// to retain their historical revision vector. This grants no apply fence.
    pub(crate) async fn verify_recorded_transition(
        &self,
        tx: &DatabaseTransaction,
        changes: &[SourceTransition],
        budget: &mut Budget,
    ) -> Result<Self> {
        validate_changes(changes)?;
        let mut expected = self.revisions.clone();
        for change in changes {
            let value = expected
                .get_mut(&change.key.table)
                .ok_or(ManagementError::InvalidEvidence)?;
            *value = value
                .checked_add(
                    change
                        .writes
                        .try_into()
                        .map_err(|_| ManagementError::Budget)?,
                )
                .ok_or(ManagementError::Budget)?;
            verify_row(tx, &change.key, change.after.as_ref(), budget).await?;
        }
        let mut next = self.clone();
        next.revisions = expected;
        Ok(next)
    }
}
fn validate_changes(changes: &[SourceTransition]) -> Result<()> {
    if changes.len() > super::super::MAX_ITEMS * 4 {
        return Err(ManagementError::Budget);
    }
    let mut keys = std::collections::BTreeSet::new();
    if changes
        .iter()
        .any(|change| change.writes == 0 || !keys.insert(&change.key))
    {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}
async fn verify_row(
    tx: &DatabaseTransaction,
    key: &NativeKey,
    expected: Option<&NativeRow>,
    budget: &mut Budget,
) -> Result<()> {
    use native::scope::{ScopeSpec, Selector, collect_scope};
    let rows = collect_scope(
        tx,
        &ScopeSpec::new(vec![Selector::Exact(key.clone())])?,
        budget,
    )
    .await?;
    if rows.rows().get(key) != expected {
        return Err(ManagementError::Changed);
    }
    Ok(())
}
