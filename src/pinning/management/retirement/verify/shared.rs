//! Exact bounded raw/member comparison and private local progress coordinates.
use super::*;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};

pub(super) async fn parts(db: &DatabaseConnection, after: i64) -> Result<Vec<(i64, ArchivePart)>> {
    let rows=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT * FROM (SELECT *,SUM(payload_bytes) OVER (ORDER BY ordinal ROWS UNBOUNDED PRECEDING) AS page_bytes FROM (SELECT ordinal,CASE WHEN length(table_name)<=256 THEN table_name END AS table_name,CASE WHEN length(CAST(row_key AS BLOB))<=65536 THEN row_key END AS row_key,CASE WHEN length(column_name)<=256 THEN column_name END AS column_name,offset,length,CASE WHEN length(bytes)<=65536 THEN bytes END AS bytes,length(bytes)+length(CAST(row_key AS BLOB))+length(table_name)+length(column_name) AS payload_bytes FROM parts WHERE ordinal>$1 ORDER BY ordinal LIMIT 256)) WHERE page_bytes<=1048576 ORDER BY ordinal",vec![after.into()]
    )).await.map_err(|_|ManagementError::Database)?;
    if rows.is_empty()
        && one(
            db,
            "SELECT ordinal FROM parts WHERE ordinal>$1 ORDER BY ordinal LIMIT 1",
            vec![after.into()],
        )
        .await?
        .is_some()
    {
        return Err(ManagementError::Budget);
    }
    rows.into_iter()
        .map(|row| {
            let table = get::<String>(&row, "table_name")?;
            let key: NativeKey = decode(&get::<String>(&row, "row_key")?)?;
            if table != key.table {
                return Err(ManagementError::InvalidEvidence);
            }
            let length = get::<Option<i64>>(&row, "length")?
                .map(u64::try_from)
                .transpose()
                .map_err(|_| ManagementError::InvalidEvidence)?;
            Ok((
                get(&row, "ordinal")?,
                ArchivePart {
                    table,
                    key,
                    column: get(&row, "column_name")?,
                    offset: u64::try_from(get::<i64>(&row, "offset")?)
                        .map_err(|_| ManagementError::InvalidEvidence)?,
                    length,
                    bytes: get(&row, "bytes")?,
                },
            ))
        })
        .collect()
}
pub(super) async fn members(db: &DatabaseConnection, after: i64) -> Result<Vec<(i64, NativeKey)>> {
    let rows=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT * FROM (SELECT *,SUM(payload_bytes) OVER (ORDER BY position ROWS UNBOUNDED PRECEDING) AS page_bytes FROM (SELECT position,CASE WHEN length(table_name)<=256 THEN table_name END AS table_name,CASE WHEN length(CAST(row_key AS BLOB))<=65536 THEN row_key END AS row_key,length(CAST(row_key AS BLOB))+length(table_name) AS payload_bytes FROM members WHERE position>$1 ORDER BY position LIMIT 256)) WHERE page_bytes<=1048576 ORDER BY position",vec![after.into()]
    )).await.map_err(|_|ManagementError::Database)?;
    if rows.is_empty()
        && one(
            db,
            "SELECT position FROM members WHERE position>$1 ORDER BY position LIMIT 1",
            vec![after.into()],
        )
        .await?
        .is_some()
    {
        return Err(ManagementError::Budget);
    }
    rows.into_iter()
        .map(|row| {
            let table = get::<String>(&row, "table_name")?;
            let key: NativeKey = decode(&get::<String>(&row, "row_key")?)?;
            if key.table != table {
                return Err(ManagementError::InvalidEvidence);
            }
            Ok((get(&row, "position")?, key))
        })
        .collect()
}
pub(super) async fn eof(db: &DatabaseConnection) -> Result<()> {
    let mut expected = crate::store::pinning::management::native::current_catalog()
        .iter()
        .map(|table| table.table.clone())
        .collect::<Vec<_>>();
    expected.sort();
    let rows=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"SELECT CASE WHEN length(table_name)<=256 THEN table_name END AS table_name FROM eof ORDER BY table_name LIMIT $1",vec![((expected.len()+1) as i64).into()])).await.map_err(|_|ManagementError::Database)?;
    let actual = rows
        .into_iter()
        .map(|row| get::<String>(&row, "table_name"))
        .collect::<Result<Vec<_>>>()?;
    if actual != expected {
        return Err(ManagementError::Changed);
    }
    Ok(())
}
pub(super) async fn save(db: &impl ConnectionTrait, cursor: &VerificationCursor) -> Result<()> {
    super::super::spool::exec(db,"INSERT INTO shared_progress VALUES(1,$1) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",vec![encode(cursor)?.into()]).await
}
pub(in crate::pinning::management::retirement) async fn restore(
    db: &DatabaseConnection,
) -> Result<VerificationCursor> {
    let row=one(db,"SELECT CASE WHEN length(CAST(payload AS BLOB))<=65536 THEN payload END AS payload FROM shared_progress WHERE id=1",vec![]).await?;
    row.map(|row| decode(&get::<String>(&row, "payload")?))
        .unwrap_or_else(|| Ok(VerificationCursor::default()))
}
