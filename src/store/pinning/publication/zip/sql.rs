//! Parameter-batched statements for the ZIP final write set.
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
pub(super) fn retain(work: DecodeWork<'_>, _size: usize) -> AppResult<ResidentGuard> {
    work.with(|session| {
        let mut budget = session.storage_budget()?;
        let retained = budget.reserve(0);
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

pub(super) fn retain_copy(work: DecodeWork<'_>, _size: usize) -> AppResult<ResidentGuard> {
    retain(work, 0)
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
    _resident: &mut Vec<ResidentGuard>,
    maintenance: Option<&mut super::PublicationLease<'_>>,
) -> AppResult<Vec<E::Model>> {
    let table = E::default().table_name().to_owned();
    let columns = E::Column::iter().count();
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
    let rows = db
        .query_all(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!("SELECT * FROM \"{table}\" WHERE {predicate} ORDER BY {order}"),
            values,
        ))
        .await?;
    let cells = rows
        .len()
        .checked_mul(columns)
        .ok_or(AppError::ManagementBudget)?;
    charge(
        work,
        WorkMeter {
            rows: rows.len() as u64,
            cells: cells as u64,
            ..Default::default()
        },
    )?;
    work.index_cells(cells, 0).map_err(work_error)?;
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
        for row in &rows[start..] {
            if values.len() + columns.len() > PARAMS {
                break;
            }
            let cells = columns.iter().map(|c| row.get(*c)).collect::<Vec<_>>();
            tuples.push(format!("({})", slots(cells.len(), values.len())));
            values.extend(cells);
        }
        charge(
            work,
            WorkMeter {
                sql: 1,
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
        for (old, new) in &rows[start..] {
            let cells = columns
                .iter()
                .map(|c| old.get(*c))
                .chain(columns.iter().map(|c| new.get(*c)))
                .collect::<Vec<_>>();
            if values.len() + cells.len() > PARAMS {
                break;
            }
            tuples.push(format!("({})", slots(cells.len(), values.len())));
            values.extend(cells);
        }
        charge(
            work,
            WorkMeter {
                sql: 1,
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
