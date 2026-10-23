use super::*;
use crate::pinning::management::retirement::Workspace;
use crate::store::pinning::management::native::meter::ResidentGuard;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait, Value};
use std::path::PathBuf;
mod cache;
mod checkpoint;

const SCHEMA: &[&str] = &[
    "CREATE TABLE rows(table_name TEXT NOT NULL,sort_key BLOB NOT NULL,row_key TEXT NOT NULL,archive_id TEXT,ordinal BIGINT,PRIMARY KEY(table_name,sort_key)) WITHOUT ROWID",
    "CREATE INDEX chunks ON rows(table_name,archive_id,ordinal)",
    "CREATE TABLE cells(table_name TEXT NOT NULL,sort_key BLOB NOT NULL,column_name TEXT NOT NULL,length BIGINT,PRIMARY KEY(table_name,sort_key,column_name)) WITHOUT ROWID",
    "CREATE TABLE parts(table_name TEXT NOT NULL,sort_key BLOB NOT NULL,column_name TEXT NOT NULL,offset BIGINT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(table_name,sort_key,column_name,offset)) WITHOUT ROWID",
    "CREATE TABLE checkpoint(id INTEGER PRIMARY KEY CHECK(id=1),source TEXT NOT NULL,budget TEXT NOT NULL,sealed INTEGER NOT NULL,principal TEXT NOT NULL,created_at TEXT NOT NULL)",
    "CREATE TABLE sealed_wire(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL)",
];

pub(super) struct Spool {
    pub db: DatabaseConnection,
    workspace: Option<Workspace>,
    path: PathBuf,
    _guard: Option<files::stream::ReopenedTask>,
    _memory: ResidentGuard,
}
pub(super) fn sort_key(key: &NativeKey) -> Vec<u8> {
    let mut result = Vec::new();
    for value in &key.values {
        for byte in value.bytes() {
            result.push(byte);
            if byte == 0 {
                result.push(255)
            }
        }
        result.extend_from_slice(&[0, 0]);
    }
    result
}
pub(super) async fn exec(
    db: &impl ConnectionTrait,
    sql: &str,
    values: Vec<Value>,
    budget: &mut Budget,
) -> Result<()> {
    budget.charge(1, 0, 0, 0).map_err(storage)?;
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        sql,
        values,
    ))
    .await
    .map_err(invalid)?;
    Ok(())
}
pub(super) async fn one(
    db: &impl ConnectionTrait,
    sql: &str,
    values: Vec<Value>,
    budget: &mut Budget,
) -> Result<Option<sea_orm::QueryResult>> {
    budget.charge(1, 0, 0, 0).map_err(storage)?;
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        sql,
        values,
    ))
    .await
    .map_err(invalid)
}
pub(super) fn get<T: sea_orm::TryGetable>(row: &sea_orm::QueryResult, column: &str) -> Result<T> {
    row.try_get("", column).map_err(invalid)
}
impl Spool {
    pub async fn create(budget: &mut Budget) -> Result<Self> {
        let memory = budget.reserve(3 * 1024 * 1024).map_err(storage)?;
        let mut workspace = Workspace::create().map_err(storage)?;
        let (path, file) = workspace.reserve().await.map_err(storage)?;
        drop(file);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(false),
            )
            .await
            .map_err(invalid)?;
        let db = sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
        db.execute_unprepared("PRAGMA journal_mode=DELETE; PRAGMA cache_size=-2048; PRAGMA temp_store=FILE; PRAGMA busy_timeout=1000;").await.map_err(invalid)?;
        for sql in SCHEMA {
            db.execute_unprepared(sql).await.map_err(invalid)?;
        }
        Ok(Self {
            db,
            workspace: Some(workspace),
            path,
            _guard: None,
            _memory: memory,
        })
    }
    pub async fn close(self) -> Result<()> {
        self.db.close().await.map_err(invalid)?;
        if let Some(workspace) = self.workspace {
            workspace.cleanup().map_err(storage)?;
        }
        Ok(())
    }
    async fn save(
        db: &impl ConnectionTrait,
        checkpoint: &source::Checkpoint,
        budget: &mut Budget,
        sealed: bool,
        principal: &str,
        created_at: &str,
    ) -> Result<()> {
        budget.charge(1, 0, 0, 0).map_err(storage)?;
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO checkpoint VALUES(1,$1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET source=excluded.source,budget=excluded.budget,sealed=excluded.sealed",vec![serde_json::to_string(checkpoint).map_err(invalid)?.into(),serde_json::to_string(&budget.checkpoint().map_err(storage)?).map_err(invalid)?.into(),sealed.into(),principal.into(),created_at.into()])).await.map_err(invalid)?;
        Ok(())
    }
    pub async fn checkpoint(
        &self,
        checkpoint: &source::Checkpoint,
        budget: &mut Budget,
        sealed: bool,
        principal: &str,
        created_at: &str,
    ) -> Result<()> {
        Self::save(&self.db, checkpoint, budget, sealed, principal, created_at).await
    }
    pub async fn stage(
        &self,
        parts: &[source::Part],
        checkpoint: &source::Checkpoint,
        budget: &mut Budget,
        principal: &str,
        created_at: &str,
    ) -> Result<()> {
        let tx = self.db.begin().await.map_err(invalid)?;
        let result = async {
            for part in parts {
                self.part(&tx, part, budget).await?;
            }
            Self::save(&tx, checkpoint, budget, false, principal, created_at).await?;
            Ok(())
        }
        .await;
        if let Err(e) = result {
            tx.rollback().await.map_err(invalid)?;
            return Err(e);
        }
        tx.commit().await.map_err(invalid)
    }
    pub async fn part(
        &self,
        db: &impl ConnectionTrait,
        part: &source::Part,
        budget: &mut Budget,
    ) -> Result<()> {
        let key = sort_key(&part.key);
        let chunk = part.key.table == "pin_management_archive_chunks";
        let ordinal = if chunk {
            let text = part.key.values.get(1).ok_or(SafeError::InvalidInput)?;
            let ordinal = text.parse::<i64>().map_err(invalid)?;
            if ordinal < 0 || ordinal.to_string() != *text {
                return Err(SafeError::InvalidInput);
            }
            Some(ordinal)
        } else {
            None
        };
        let parent = if chunk
            || matches!(
                part.key.table.as_str(),
                "pin_management_archive" | "pin_management_inline_archive"
            ) {
            part.key.values.first().cloned()
        } else {
            None
        };
        exec(
            db,
            "INSERT INTO rows VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
            vec![
                part.key.table.clone().into(),
                key.clone().into(),
                serde_json::to_string(&part.key).map_err(invalid)?.into(),
                parent.into(),
                ordinal.into(),
            ],
            budget,
        )
        .await?;
        if part.offset == 0 {
            exec(
                db,
                "INSERT INTO cells VALUES($1,$2,$3,$4)",
                vec![
                    part.key.table.clone().into(),
                    key.clone().into(),
                    part.column.clone().into(),
                    part.length
                        .map(i64::try_from)
                        .transpose()
                        .map_err(invalid)?
                        .into(),
                ],
                budget,
            )
            .await?;
        }
        if !part.bytes.is_empty() {
            budget
                .charge(0, 0, 0, part.bytes.len() as u64)
                .map_err(storage)?;
            exec(
                db,
                "INSERT INTO parts VALUES($1,$2,$3,$4,$5)",
                vec![
                    part.key.table.clone().into(),
                    key.into(),
                    part.column.clone().into(),
                    i64::try_from(part.offset).map_err(invalid)?.into(),
                    part.bytes.clone().into(),
                ],
                budget,
            )
            .await?;
            budget.work.copied_bytes = budget
                .work
                .copied_bytes
                .checked_add(part.bytes.len() as u64)
                .ok_or(SafeError::LimitExceeded)?;
        }
        Ok(())
    }
    pub async fn next_key(
        &self,
        table: &str,
        after: Option<&NativeKey>,
        budget: &mut Budget,
    ) -> Result<Option<NativeKey>> {
        let mut values = vec![table.into()];
        let filter = if let Some(after) = after {
            values.push(sort_key(after).into());
            " AND sort_key>$2"
        } else {
            ""
        };
        one(
            &self.db,
            &format!(
                "SELECT row_key FROM rows WHERE table_name=$1{filter} ORDER BY sort_key LIMIT 1"
            ),
            values,
            budget,
        )
        .await?
        .map(|r| serde_json::from_str(&get::<String>(&r, "row_key")?).map_err(invalid))
        .transpose()
    }
    pub async fn cell<W: Write>(
        &self,
        key: &NativeKey,
        column: &str,
        output: &mut W,
        budget: &mut Budget,
    ) -> Result<Option<u64>> {
        let _resident = budget
            .reserve(2 * super::super::CELL_BYTES)
            .map_err(storage)?;
        let sorted = sort_key(key);
        let row = one(
            &self.db,
            "SELECT length FROM cells WHERE table_name=$1 AND sort_key=$2 AND column_name=$3",
            vec![
                key.table.clone().into(),
                sorted.clone().into(),
                column.into(),
            ],
            budget,
        )
        .await?
        .ok_or(SafeError::InvalidInput)?;
        let length: Option<i64> = get(&row, "length")?;
        let Some(length) = length else {
            return Ok(None);
        };
        let length = u64::try_from(length).map_err(invalid)?;
        let mut offset = 0u64;
        while offset < length {
            let row=one(&self.db,"SELECT payload FROM parts WHERE table_name=$1 AND sort_key=$2 AND column_name=$3 AND offset=$4",vec![key.table.clone().into(),sorted.clone().into(),column.into(),i64::try_from(offset).map_err(invalid)?.into()],budget).await?.ok_or(SafeError::InvalidInput)?;
            let bytes: Vec<u8> = get(&row, "payload")?;
            if bytes.is_empty()
                || bytes.len() > super::super::CELL_BYTES
                || bytes.len() as u64 > length - offset
            {
                return Err(SafeError::InvalidInput);
            }
            budget
                .charge(0, 0, 0, bytes.len() as u64)
                .map_err(storage)?;
            output.write_all(&bytes).map_err(invalid)?;
            offset += bytes.len() as u64;
        }
        Ok(Some(length))
    }
    pub async fn write_rows<W: Write>(&self, writer: &mut Writer<W>) -> Result<()> {
        let _resident = writer
            .budget_mut()
            .reserve(3 * super::super::CELL_BYTES + 16384)
            .map_err(storage)?;
        for table in source::tables() {
            let mut after = None;
            while let Some(key) = self
                .next_key(&table.name, after.as_ref(), writer.budget_mut())
                .await?
            {
                writer.frame(&Frame::Row {
                    section: Section::Native,
                    key: key.clone(),
                    cells: table.columns.len() as u32,
                })?;
                for column in &table.columns {
                    writer.frame(&Frame::Cell {
                        column: column.clone(),
                    })?;
                    let sorted = sort_key(&key);
                    let row=one(&self.db,"SELECT length FROM cells WHERE table_name=$1 AND sort_key=$2 AND column_name=$3",vec![key.table.clone().into(),sorted.clone().into(),column.clone().into()],writer.budget_mut()).await?.ok_or(SafeError::InvalidInput)?;
                    let length: Option<i64> = get(&row, "length")?;
                    let mut offset = 0i64;
                    while length.is_some_and(|length| offset < length) {
                        let row=one(&self.db,"SELECT payload FROM parts WHERE table_name=$1 AND sort_key=$2 AND column_name=$3 AND offset=$4",vec![key.table.clone().into(),sorted.clone().into(),column.clone().into(),offset.into()],writer.budget_mut()).await?.ok_or(SafeError::InvalidInput)?;
                        let bytes: Vec<u8> = get(&row, "payload")?;
                        if bytes.is_empty()
                            || bytes.len() > super::super::CELL_BYTES
                            || Some(offset + bytes.len() as i64) > length
                        {
                            return Err(SafeError::InvalidInput);
                        }
                        writer
                            .budget_mut()
                            .charge(0, 0, 0, bytes.len() as u64)
                            .map_err(storage)?;
                        writer.cell_output().write_all(&bytes).map_err(invalid)?;
                        offset += bytes.len() as i64;
                    }
                    writer.frame(&Frame::CellEnd {
                        value: if length.is_some() {
                            CellValue::Text
                        } else {
                            CellValue::Null
                        },
                    })?;
                }
                writer.frame(&Frame::RowEnd)?;
                after = Some(key);
            }
        }
        Ok(())
    }
}
