//! Raw bounded classification input. No SQL JSON operator sees unclassified text.
use crate::store::pinning::management::{ManagementError, Result, native, records::DecodeWork};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

pub(super) async fn rows<C: ConnectionTrait>(
    db: &C,
    work: DecodeWork<'_>,
) -> Result<Vec<(native::NativeRow, native::NativeRow)>> {
    let registry = native::current_catalog();
    let retirement = registry
        .iter()
        .find(|table| table.table == "pin_management_retirements")
        .ok_or(ManagementError::Schema)?;
    let action = registry
        .iter()
        .find(|table| table.table == "pin_management_records")
        .ok_or(ManagementError::Schema)?;
    work.with(|work| work.nodes(1 + retirement.columns.len() + action.columns.len()))?;
    let columns = retirement
        .columns
        .iter()
        .map(|column| ("r", column))
        .chain(action.columns.iter().map(|column| ("a", column)))
        .collect::<Vec<_>>();
    let backend = db.get_database_backend();
    let join = "FROM pin_management_retirements r LEFT JOIN pin_management_records a ON a.database_scope=r.database_scope AND a.plan_id=r.plan_id AND a.item_id=r.item_id AND a.kind='action'";
    let terms = columns
        .iter()
        .map(|(alias, column)| {
            if backend == DatabaseBackend::Sqlite {
                format!("LENGTH(CAST(COALESCE(CAST({alias}.\"{column}\" AS TEXT),'') AS BLOB))")
            } else {
                format!("OCTET_LENGTH(COALESCE(CAST({alias}.\"{column}\" AS TEXT),''))::bigint")
            }
        })
        .collect::<Vec<_>>()
        .join("+");
    let sum = if backend == DatabaseBackend::Postgres {
        format!("CAST(COALESCE(SUM({terms}),0) AS BIGINT)")
    } else {
        format!("COALESCE(SUM({terms}),0)")
    };
    let measured = db
        .query_one(Statement::from_string(
            backend,
            format!("SELECT COUNT(*) AS rows,{sum} AS bytes {join}"),
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Database)?;
    let count = usize::try_from(
        measured
            .try_get::<i64>("", "rows")
            .map_err(|_| ManagementError::InvalidEvidence)?,
    )
    .map_err(|_| ManagementError::Budget)?;
    let bytes = usize::try_from(
        measured
            .try_get::<i64>("", "bytes")
            .map_err(|_| ManagementError::InvalidEvidence)?,
    )
    .map_err(|_| ManagementError::Budget)?;
    if count > native::MAX_ROWS / 2 || bytes > native::MAX_BYTES {
        return Err(ManagementError::Budget);
    }
    for _ in 0..2 {
        work.with(|work| work.nodes(1 + count * columns.len()))?;
        work.with(|work| work.bytes(bytes))?;
    }
    for _ in 0..count {
        work.copy(&retirement.columns)?;
        work.copy(&action.columns)?;
    }
    let fields = columns
        .iter()
        .map(|(alias, column)| {
            format!("CAST({alias}.\"{column}\" AS TEXT) AS \"{alias}_{column}\"")
        })
        .collect::<Vec<_>>()
        .join(",");
    let rows=db.query_all(Statement::from_string(backend,format!("SELECT {fields} {join} WHERE (SELECT {sum} {join})<={bytes} AND (SELECT COUNT(*) {join})<={count} ORDER BY r.allocation_domain LIMIT {}",native::MAX_ROWS+1))).await.map_err(|_|ManagementError::Database)?;
    if rows.len() != count {
        return Err(ManagementError::Changed);
    }
    let mut actual = 0usize;
    rows.into_iter()
        .map(|row| {
            let mut r = native::NativeRow::new();
            let mut a = native::NativeRow::new();
            for (alias, column) in &columns {
                let value = row
                    .try_get::<Option<String>>("", &format!("{alias}_{column}"))
                    .map_err(|_| ManagementError::InvalidEvidence)?;
                actual = actual
                    .checked_add(value.as_ref().map_or(0, String::len))
                    .ok_or(ManagementError::Budget)?;
                if actual > bytes {
                    return Err(ManagementError::Changed);
                }
                if *alias == "r" {
                    r.insert((*column).clone(), value);
                } else {
                    a.insert((*column).clone(), value);
                }
            }
            Ok((r, a))
        })
        .collect()
}
