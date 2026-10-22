//! Full management history only. Archive storage is not an authority catalog.
use crate::store::pinning::management::native::{
    self, NativeKey, NativeTable,
    meter::{Budget, CELL_SLICE, PAGE_BYTES, ResidentGuard},
};
use crate::store::pinning::management::{ManagementError, Result, schema};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement, Value};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::LazyLock};

#[derive(Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
    pub keys: Vec<String>,
    native: Option<NativeTable>,
}
pub fn tables() -> &'static [Table] {
    static TABLES: LazyLock<Vec<Table>> = LazyLock::new(|| {
        let mut tables = native::registry()
            .into_iter()
            .filter(|t| schema::storage::SOURCE_TABLES.contains(&t.table.as_str()))
            .map(|t| Table {
                name: t.table.clone(),
                columns: t.columns.to_vec(),
                keys: t.primary_key.to_vec(),
                native: Some(t),
            })
            .collect::<Vec<_>>();
        for (name, columns, keys) in [
            (
                "pin_management_archive",
                "archive_id,bytes,chunks,database_scope,digest,format,header,native_identity,state",
                "archive_id",
            ),
            (
                "pin_management_archive_chunks",
                "archive_id,ordinal,payload",
                "archive_id,ordinal",
            ),
        ] {
            tables.push(Table {
                name: name.into(),
                columns: columns.split(',').map(str::to_owned).collect(),
                keys: keys.split(',').map(str::to_owned).collect(),
                native: None,
            });
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        tables
    });
    &TABLES
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub database_scope: String,
    pub database_identity: String,
    pub catalog: Vec<String>,
    revisions: BTreeMap<String, i64>,
    table: usize,
    after: Option<NativeKey>,
    row: Option<NativeKey>,
    column: usize,
    offset: u64,
}
impl Checkpoint {
    pub fn rewind(&self) -> Self {
        let mut checkpoint = self.clone();
        checkpoint.table = 0;
        checkpoint.after = None;
        checkpoint.row = None;
        checkpoint.column = 0;
        checkpoint.offset = 0;
        checkpoint
    }
    pub fn complete(&self) -> bool {
        self.table == tables().len() && self.row.is_none()
    }
    pub async fn check(&self, tx: &DatabaseTransaction, budget: &mut Budget) -> Result<()> {
        if self.table > tables().len()
            || self.column != 0 && self.row.is_none()
            || self.offset != 0 && self.row.is_none()
        {
            return Err(ManagementError::InvalidEvidence);
        }
        budget.charge(1, 0, 0, 0)?;
        if native::scope::binding(tx).await?
            != (self.database_scope.clone(), self.database_identity.clone())
            || vector(tx, budget).await? != self.revisions
        {
            return Err(ManagementError::Changed);
        }
        Ok(())
    }
}
async fn vector(tx: &DatabaseTransaction, budget: &mut Budget) -> Result<BTreeMap<String, i64>> {
    budget.charge(1, 0, 0, 0)?;
    let parameters = (1..=tables().len())
        .map(|n| format!("${n}"))
        .collect::<Vec<_>>()
        .join(",");
    let rows=tx.query_all(Statement::from_sql_and_values(tx.get_database_backend(),format!("SELECT source_table,revision FROM pin_management_revision WHERE source_table IN ({parameters}) ORDER BY source_table"),tables().iter().map(|t|Value::from(t.name.clone())))).await.map_err(|_|ManagementError::Database)?;
    if rows.len() != tables().len() {
        return Err(ManagementError::Schema);
    }
    rows.into_iter()
        .map(|r| {
            let revision: i64 = r
                .try_get("", "revision")
                .map_err(|_| ManagementError::Schema)?;
            if revision < 0 {
                return Err(ManagementError::Schema);
            }
            Ok((
                r.try_get("", "source_table")
                    .map_err(|_| ManagementError::Schema)?,
                revision,
            ))
        })
        .collect()
}
pub async fn start(tx: &DatabaseTransaction, budget: &mut Budget) -> Result<Checkpoint> {
    budget.charge(1, 0, 0, 0)?;
    let catalog = schema::validate(tx).await?;
    crate::store::pinning::management::catalog::validate(tx, native::current_catalog()).await?;
    let (database_scope, database_identity) = native::scope::binding(tx).await?;
    Ok(Checkpoint {
        database_scope,
        database_identity,
        catalog,
        revisions: vector(tx, budget).await?,
        table: 0,
        after: None,
        row: None,
        column: 0,
        offset: 0,
    })
}

/// Payload is original UTF-8, except archive_chunks.payload, whose binary bytes
/// use lowercase hex. Offsets/lengths describe that lossless wire representation.
pub struct Part {
    pub key: NativeKey,
    pub column: String,
    pub offset: u64,
    pub length: Option<u64>,
    pub bytes: Vec<u8>,
}
pub struct Page {
    pub checkpoint: Checkpoint,
    pub parts: Vec<Part>,
    _resident: ResidentGuard,
}

fn bound(table: &Table, column: &str, value: &str) -> Result<Value> {
    if let Some(t) = &table.native {
        return native::scope::bound_value(t, column, value);
    }
    if column == "ordinal" {
        Ok(value
            .parse::<i64>()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .into())
    } else {
        Ok(value.into())
    }
}
fn predicate(table: &Table, key: &NativeKey) -> Result<(String, Vec<Value>)> {
    if key.table != table.name || key.values.len() != table.keys.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok((
        table
            .keys
            .iter()
            .enumerate()
            .map(|(i, k)| format!("\"{k}\"=${}", i + 1))
            .collect::<Vec<_>>()
            .join(" AND "),
        table
            .keys
            .iter()
            .zip(&key.values)
            .map(|(k, v)| bound(table, k, v))
            .collect::<Result<_>>()?,
    ))
}
async fn next_key(
    tx: &DatabaseTransaction,
    table: &Table,
    after: Option<&NativeKey>,
    budget: &mut Budget,
) -> Result<Option<NativeKey>> {
    let mut values = Vec::new();
    if let Some(key) = after {
        values = predicate(table, key)?.1;
    }
    let keys = table
        .keys
        .iter()
        .map(|k| format!("\"{k}\""))
        .collect::<Vec<_>>()
        .join(",");
    let filter = if values.is_empty() {
        String::new()
    } else {
        format!(
            "WHERE ({keys})>({})",
            (1..=values.len())
                .map(|n| format!("${n}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let fields = table
        .keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let octets = native::scope::octets(tx.get_database_backend(), k);
            format!("CASE WHEN length({octets})<=1024 THEN CAST(\"{k}\" AS TEXT) END AS k{i}")
        })
        .collect::<Vec<_>>()
        .join(",");
    budget.charge(1, 0, 0, 0)?;
    let row = tx
        .query_one(Statement::from_sql_and_values(
            tx.get_database_backend(),
            format!(
                "SELECT {fields} FROM \"{}\" {filter} ORDER BY {keys} LIMIT 1",
                table.name
            ),
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    row.map(|row| {
        Ok(NativeKey {
            table: table.name.clone(),
            values: (0..table.keys.len())
                .map(|i| {
                    row.try_get::<Option<String>>("", &format!("k{i}"))
                        .map_err(|_| ManagementError::InvalidEvidence)?
                        .ok_or(ManagementError::Budget)
                })
                .collect::<Result<_>>()?,
        })
    })
    .transpose()
}

pub async fn page(
    tx: &DatabaseTransaction,
    checkpoint: &Checkpoint,
    budget: &mut Budget,
) -> Result<Page> {
    checkpoint.check(tx, budget).await?;
    let resident = budget.reserve(4 * PAGE_BYTES)?;
    let mut cp = checkpoint.clone();
    let mut parts = Vec::new();
    let mut bytes = 0;
    let mut rows = 0;
    while cp.table < tables().len() && parts.len() < 256 && bytes < PAGE_BYTES && rows < 256 {
        let table = &tables()[cp.table];
        if cp.row.is_none() {
            cp.row = next_key(tx, table, cp.after.as_ref(), budget).await?;
            if cp.row.is_none() {
                cp.table += 1;
                cp.after = None;
                continue;
            }
            budget.charge(0, 1, 0, 0)?;
            rows += 1;
        }
        let key = cp.row.as_ref().ok_or(ManagementError::InvalidEvidence)?;
        let column = table
            .columns
            .get(cp.column)
            .ok_or(ManagementError::InvalidEvidence)?;
        let binary = table.name == "pin_management_archive_chunks" && column == "payload";
        let factor = if binary { 2 } else { 1 };
        let capacity = ((PAGE_BYTES - bytes) / factor).min(CELL_SLICE / factor);
        if capacity == 0 {
            break;
        }
        let (predicate, values) = predicate(table, key)?;
        let octets = if binary {
            "\"payload\"".to_owned()
        } else {
            native::scope::octets(tx.get_database_backend(), column)
        };
        let slice = if tx.get_database_backend() == DatabaseBackend::Sqlite {
            format!("substr({octets},{},{capacity})", cp.offset + 1)
        } else {
            format!("substring({octets} FROM {} FOR {capacity})", cp.offset + 1)
        };
        budget.charge(1, 0, 1, 0)?;
        let row=tx.query_one(Statement::from_sql_and_values(tx.get_database_backend(),format!("SELECT CAST(length({octets}) AS BIGINT) AS size,{slice} AS payload FROM \"{}\" WHERE {predicate}",table.name),values)).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Changed)?;
        let length: Option<i64> = row
            .try_get("", "size")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        let length = length
            .map(|n| u64::try_from(n).map_err(|_| ManagementError::InvalidEvidence))
            .transpose()?;
        let payload: Option<Vec<u8>> = row
            .try_get("", "payload")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        let payload = payload.unwrap_or_default();
        if length.is_some_and(|n| {
            cp.offset > n || payload.len() as u64 != (n - cp.offset).min(capacity as u64)
        }) || length.is_none() && !payload.is_empty()
        {
            return Err(ManagementError::Changed);
        }
        budget.charge(0, 0, 0, payload.len() as u64)?;
        let consumed = payload.len() as u64;
        let payload = if binary {
            hex::encode(payload).into_bytes()
        } else {
            payload
        };
        bytes += payload.len();
        parts.push(Part {
            key: key.clone(),
            column: column.clone(),
            offset: cp
                .offset
                .checked_mul(factor as u64)
                .ok_or(ManagementError::Budget)?,
            length: length
                .map(|n| n.checked_mul(factor as u64).ok_or(ManagementError::Budget))
                .transpose()?,
            bytes: payload,
        });
        cp.offset += consumed;
        if length.is_none_or(|n| cp.offset == n) {
            cp.offset = 0;
            cp.column += 1;
            if cp.column == table.columns.len() {
                cp.column = 0;
                cp.after = cp.row.take();
            }
        }
    }
    checkpoint.check(tx, budget).await?;
    budget.work.pages = budget
        .work
        .pages
        .checked_add(1)
        .ok_or(ManagementError::Budget)?;
    Ok(Page {
        checkpoint: cp,
        parts,
        _resident: resident,
    })
}

/// Short phantom fence, after every source byte has already reached task storage.
pub async fn seal(
    tx: &DatabaseTransaction,
    checkpoint: &Checkpoint,
    budget: &mut Budget,
) -> Result<()> {
    if !checkpoint.complete() {
        return Err(ManagementError::InvalidEvidence);
    }
    native::scope::writer_boundary(tx).await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        budget.charge(1, 0, 0, 0)?;
        tx.execute_unprepared("LOCK TABLE seaql_migrations IN SHARE MODE NOWAIT")
            .await
            .map_err(|_| ManagementError::Changed)?;
        for table in tables() {
            budget.charge(1, 0, 0, 0)?;
            tx.execute_unprepared(&format!(
                "LOCK TABLE \"{}\" IN SHARE MODE NOWAIT",
                table.name
            ))
            .await
            .map_err(|_| ManagementError::Changed)?;
        }
    }
    if schema::validate(tx).await? != checkpoint.catalog {
        return Err(ManagementError::Changed);
    }
    checkpoint.check(tx, budget).await
}
