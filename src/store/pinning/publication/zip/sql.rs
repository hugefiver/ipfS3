//! Parameter- and byte-bounded statements for the ZIP final write set.
use crate::{
    error::{AppError, AppResult},
    store::pinning::management::{
        admission::work_error,
        native::meter::{ResidentGuard, WorkMeter},
        records::DecodeWork,
    },
};
use sea_orm::{
    ConnectionTrait, EntityTrait, FromQueryResult, Iden, Iterable, ModelTrait, PrimaryKeyToColumn,
    Statement, Value,
};

pub(super) const PARAMS: usize = 900;

pub(super) async fn lock<C: ConnectionTrait>(
    db: &C,
    table: &str,
    predicate: &str,
    values: Vec<Value>,
    order: &str,
    work: DecodeWork<'_>,
    maintenance: Option<&mut super::PublicationLease<'_>>,
) -> AppResult<()> {
    if db.get_database_backend() != sea_orm::DatabaseBackend::Postgres {
        return Ok(());
    }
    if let Some(lease) = maintenance {
        lease.checkpoint(db).await?;
    }
    charge(
        work,
        WorkMeter {
            sql: 1,
            ..Default::default()
        },
    )?;
    db.query_all(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!("SELECT 1 FROM \"{table}\" WHERE {predicate} ORDER BY {order} FOR UPDATE NOWAIT"),
        values,
    ))
    .await
    .map_err(|error| {
        if error.to_string().contains("could not obtain lock") {
            AppError::Database("stale ZIP frontier compare-and-set".into())
        } else {
            error.into()
        }
    })?;
    Ok(())
}

pub(super) async fn delete<E: EntityTrait + Default, C: ConnectionTrait>(
    db: &C,
    rows: &[E::Model],
    work: DecodeWork<'_>,
    mut maintenance: Option<&mut super::PublicationLease<'_>>,
) -> AppResult<()> {
    let columns = E::Column::iter().collect::<Vec<_>>();
    let equal = if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };
    for chunk in rows.chunks(PARAMS / columns.len()) {
        if let Some(lease) = maintenance.as_deref_mut() {
            lease.checkpoint(db).await?;
        }
        let mut values = Vec::new();
        let mut terms = Vec::new();
        for row in chunk {
            let mut terms_row = Vec::new();
            for column in &columns {
                values.push(row.get(*column));
                terms_row.push(format!(
                    "\"{}\" {equal} ${}",
                    Iden::to_string(column),
                    values.len()
                ));
            }
            terms.push(format!("({})", terms_row.join(" AND ")));
        }
        charge(
            work,
            WorkMeter {
                sql: 1,
                output_bytes: values.iter().map(bytes).sum::<usize>() as u64,
                ..Default::default()
            },
        )?;
        let result = db
            .execute(Statement::from_sql_and_values(
                db.get_database_backend(),
                format!(
                    "DELETE FROM \"{}\" WHERE {}",
                    E::default().table_name(),
                    terms.join(" OR ")
                ),
                values,
            ))
            .await?;
        if result.rows_affected() != chunk.len() as u64 {
            return Err(super::invalid());
        }
    }
    Ok(())
}
const ENCODED_BYTES: usize = 1024 * 1024;

fn bytes(value: &Value) -> usize {
    match value {
        Value::String(Some(s)) => s.len(),
        Value::Bytes(Some(b)) => b.len(),
        Value::Json(Some(j)) => j.to_string().len(),
        _ => 64,
    }
}

pub(super) fn retain(work: DecodeWork<'_>, size: usize) -> AppResult<ResidentGuard> {
    work.with(|session| {
        let mut budget = session.storage_budget()?;
        let retained = budget.reserve(size);
        let finished = session.finish_storage(&budget);
        match retained {
            Ok(guard) => {
                finished?;
                Ok(guard)
            }
            Err(error) => {
                let _ = finished;
                Err(error)
            }
        }
    })
    .map_err(work_error)
}

pub(super) fn retain_copy(work: DecodeWork<'_>, size: usize) -> AppResult<ResidentGuard> {
    charge(
        work,
        WorkMeter {
            copied_bytes: size as u64,
            ..Default::default()
        },
    )?;
    retain(work, size)
}

pub(super) fn charge(work: DecodeWork<'_>, used: WorkMeter) -> AppResult<()> {
    work.with(|session| session.charge_storage(used))
        .map_err(work_error)
}

pub(super) async fn read<E: EntityTrait + Default, C: ConnectionTrait>(
    db: &C,
    predicate: &str,
    values: Vec<Value>,
    order: &str,
    work: DecodeWork<'_>,
    resident: &mut Vec<ResidentGuard>,
    mut maintenance: Option<&mut super::PublicationLease<'_>>,
) -> AppResult<Vec<E::Model>> {
    let table = E::default().table_name().to_owned();
    let columns = E::Column::iter()
        .map(|c| Iden::to_string(&c))
        .collect::<Vec<_>>();
    let terms = columns
        .iter()
        .map(|c| {
            if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
                format!("LENGTH(CAST(COALESCE(CAST(\"{c}\" AS TEXT),'') AS BLOB))")
            } else {
                format!("OCTET_LENGTH(COALESCE(CAST(\"{c}\" AS TEXT),''))::bigint")
            }
        })
        .collect::<Vec<_>>()
        .join("+");
    if let Some(lease) = maintenance.as_deref_mut() {
        lease.checkpoint(db).await?;
    }
    charge(
        work,
        WorkMeter {
            sql: 1,
            ..Default::default()
        },
    )?;
    let measured = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        format!("SELECT COUNT(*) AS n,CAST(COALESCE(SUM({terms}),0) AS BIGINT) AS bytes FROM \"{table}\" WHERE {predicate}"), values.clone())).await?.ok_or_else(super::invalid)?;
    let count = usize::try_from(measured.try_get::<i64>("", "n")?)
        .map_err(|_| AppError::ManagementBudget)?;
    let size = usize::try_from(measured.try_get::<i64>("", "bytes")?)
        .map_err(|_| AppError::ManagementBudget)?;
    let cells = count
        .checked_mul(columns.len())
        .ok_or(AppError::ManagementBudget)?;
    resident.push(retain(
        work,
        size.checked_add(cells.checked_mul(64).ok_or(AppError::ManagementBudget)?)
            .ok_or(AppError::ManagementBudget)?,
    )?);
    if let Some(lease) = maintenance {
        lease.checkpoint(db).await?;
    }
    charge(
        work,
        WorkMeter {
            sql: 1,
            rows: count as u64,
            cells: cells as u64,
            raw_bytes: size as u64,
            ..Default::default()
        },
    )?;
    work.index_cells(cells, size).map_err(work_error)?;
    let rows = db
        .query_all(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                "SELECT * FROM \"{table}\" WHERE {predicate} ORDER BY {order} LIMIT {}",
                count.saturating_add(1)
            ),
            values,
        ))
        .await?;
    if rows.len() != count {
        return Err(super::invalid());
    }
    rows.iter()
        .map(|row| E::Model::from_query_result(row, "").map_err(AppError::from))
        .collect()
}

pub(super) fn slots(count: usize, offset: usize) -> String {
    (1..=count)
        .map(|i| format!("${}", i + offset))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) async fn insert<E: EntityTrait + Default, C: ConnectionTrait>(
    db: &C,
    rows: &[E::Model],
    work: DecodeWork<'_>,
    mut maintenance: Option<&mut super::PublicationLease<'_>>,
) -> AppResult<()> {
    let columns = E::Column::iter().collect::<Vec<_>>();
    let names = columns
        .iter()
        .map(|c| format!("\"{}\"", Iden::to_string(c)))
        .collect::<Vec<_>>()
        .join(",");
    let prefix = format!(
        "INSERT INTO \"{}\" ({names}) VALUES ",
        E::default().table_name()
    );
    let mut start = 0;
    while start < rows.len() {
        if let Some(lease) = maintenance.as_deref_mut() {
            lease.checkpoint(db).await?;
        }
        let mut values = Vec::new();
        let mut tuples = Vec::new();
        let mut size = prefix.len();
        for row in &rows[start..] {
            let cells = columns.iter().map(|c| row.get(*c)).collect::<Vec<_>>();
            let added = cells.iter().map(bytes).sum::<usize>() + columns.len() * 8;
            if added + prefix.len() > ENCODED_BYTES {
                return Err(AppError::ManagementBudget);
            }
            if values.len() + columns.len() > PARAMS || size + added > ENCODED_BYTES {
                break;
            }
            tuples.push(format!("({})", slots(cells.len(), values.len())));
            values.extend(cells);
            size += added;
        }
        let _resident = retain(work, size)?;
        charge(
            work,
            WorkMeter {
                sql: 1,
                output_bytes: size as u64,
                ..Default::default()
            },
        )?;
        let result = db
            .execute(Statement::from_sql_and_values(
                db.get_database_backend(),
                format!("{prefix}{}", tuples.join(",")),
                values,
            ))
            .await?;
        if result.rows_affected() != tuples.len() as u64 {
            return Err(super::invalid());
        }
        start += tuples.len();
    }
    Ok(())
}

pub(super) async fn update<E: EntityTrait + Default, C: ConnectionTrait>(
    db: &C,
    rows: &[(E::Model, E::Model)],
    work: DecodeWork<'_>,
    mut maintenance: Option<&mut super::PublicationLease<'_>>,
) -> AppResult<()> {
    let table = E::default().table_name().to_owned();
    let columns = E::Column::iter().collect::<Vec<_>>();
    let keys = E::PrimaryKey::iter()
        .map(|k| Iden::to_string(&k.into_column()))
        .collect::<Vec<_>>();
    let names = columns.iter().map(Iden::to_string).collect::<Vec<_>>();
    let equal = if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };
    let matches = names
        .iter()
        .map(|n| format!("\"{table}\".\"{n}\" {equal} c.\"old_{n}\""))
        .collect::<Vec<_>>()
        .join(" AND ");
    let identity = keys
        .iter()
        .map(|n| format!("\"{table}\".\"{n}\" = c.\"old_{n}\""))
        .collect::<Vec<_>>()
        .join(" AND ");
    let header = names
        .iter()
        .map(|n| format!("\"old_{n}\""))
        .chain(names.iter().map(|n| format!("\"new_{n}\"")))
        .collect::<Vec<_>>()
        .join(",");
    let set = names
        .iter()
        .filter(|n| !keys.contains(n))
        .map(|n| format!("\"{n}\"=(SELECT c.\"new_{n}\" FROM changes c WHERE {identity})"))
        .collect::<Vec<_>>()
        .join(",");
    let mut start = 0;
    while start < rows.len() {
        if let Some(lease) = maintenance.as_deref_mut() {
            lease.checkpoint(db).await?;
        }
        let mut values = Vec::new();
        let mut tuples = Vec::new();
        let mut size = header.len() + set.len() + matches.len() + 128;
        for (old, new) in &rows[start..] {
            let cells = columns
                .iter()
                .map(|c| old.get(*c))
                .chain(columns.iter().map(|c| new.get(*c)))
                .collect::<Vec<_>>();
            let added = cells.iter().map(bytes).sum::<usize>() + cells.len() * 8;
            if added + header.len() + set.len() + matches.len() + 128 > ENCODED_BYTES {
                return Err(AppError::ManagementBudget);
            }
            if values.len() + cells.len() > PARAMS || size + added > ENCODED_BYTES {
                break;
            }
            tuples.push(format!("({})", slots(cells.len(), values.len())));
            values.extend(cells);
            size += added;
        }
        let _resident = retain(work, size)?;
        charge(
            work,
            WorkMeter {
                sql: 1,
                output_bytes: size as u64,
                ..Default::default()
            },
        )?;
        let result = db.execute(Statement::from_sql_and_values(db.get_database_backend(),
            format!("WITH changes({header}) AS (VALUES {}) UPDATE \"{table}\" SET {set} WHERE EXISTS (SELECT 1 FROM changes c WHERE {matches})", tuples.join(",")), values)).await?;
        if result.rows_affected() != tuples.len() as u64 {
            return Err(super::invalid());
        }
        start += tuples.len();
    }
    Ok(())
}
