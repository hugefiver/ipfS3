//! Bounded, compare-and-swap progress inside the existing immutable archive.
//! A staging cursor is not a confirmed management record.
use super::*;
#[cfg(test)]
mod tests;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Position {
    pub cursor: String,
    pub chunks: i64,
    pub bytes: i64,
    pub sealed: bool,
}

pub(crate) async fn load<C: ConnectionTrait>(
    db: &C,
    id: &str,
    header: &str,
    lock: bool,
) -> Result<Option<Position>> {
    if id.is_empty() || id.len() > 1024 || header.len() > PAGE_BYTES {
        return Err(ManagementError::Budget);
    }
    let suffix = if lock && db.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        " FOR UPDATE NOWAIT"
    } else {
        ""
    };
    let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        format!("SELECT format,CASE WHEN length(header)<=1048576 THEN header END AS header,state,chunks,bytes,CASE WHEN length(digest)<=1048576 THEN digest END AS digest FROM pin_management_archive WHERE archive_id=$1{suffix}"),
        [id.into()])).await.map_err(|_| ManagementError::Database)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row
        .try_get::<String>("", "format")
        .map_err(|_| ManagementError::InvalidEvidence)?
        != "S75A1"
        || row
            .try_get::<String>("", "header")
            .map_err(|_| ManagementError::InvalidEvidence)?
            != header
    {
        return Err(ManagementError::Changed);
    }
    let position = Position {
        cursor: row
            .try_get("", "digest")
            .map_err(|_| ManagementError::InvalidEvidence)?,
        chunks: row
            .try_get("", "chunks")
            .map_err(|_| ManagementError::InvalidEvidence)?,
        bytes: row
            .try_get("", "bytes")
            .map_err(|_| ManagementError::InvalidEvidence)?,
        sealed: row
            .try_get::<String>("", "state")
            .map_err(|_| ManagementError::InvalidEvidence)?
            == "sealed",
    };
    if position.chunks < 0 || position.bytes < 0 {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(Some(position))
}

pub(crate) async fn create(
    tx: &DatabaseTransaction,
    id: &str,
    header: &str,
    scope: &str,
    identity: &str,
    cursor: &str,
) -> Result<Position> {
    if header.len() > PAGE_BYTES || cursor.len() > PAGE_BYTES {
        return Err(ManagementError::Budget);
    }
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "INSERT INTO pin_management_archive(archive_id,format,database_scope,native_identity,header,state,digest) VALUES($1,'S75A1',$2,$3,$4,'staging',$5)",
        [id.into(),scope.into(),identity.into(),header.into(),cursor.into()]))
        .await.map_err(|_| ManagementError::Database)?;
    Ok(Position {
        cursor: cursor.into(),
        chunks: 0,
        bytes: 0,
        sealed: false,
    })
}

/// The complete byte payload and before/after coordinates are one transaction.
/// This function never changes a fixed header and never rewrites a chunk.
pub(crate) async fn append(
    tx: &DatabaseTransaction,
    id: &str,
    header: &str,
    expected: &Position,
    cursor: &str,
    payload: &[u8],
    sealed: bool,
) -> Result<Position> {
    if expected.sealed || cursor.len() > PAGE_BYTES || payload.len() > PAGE_BYTES * 8 {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut next = expected.clone();
    for chunk in payload.chunks(CELL_SLICE) {
        tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
            "INSERT INTO pin_management_archive_chunks(archive_id,ordinal,payload) VALUES($1,$2,$3)",
            [id.into(),next.chunks.into(),chunk.to_vec().into()]))
            .await.map_err(|_| ManagementError::Database)?;
        next.chunks = next.chunks.checked_add(1).ok_or(ManagementError::Budget)?;
        next.bytes = next
            .bytes
            .checked_add(chunk.len() as i64)
            .ok_or(ManagementError::Budget)?;
    }
    next.cursor = cursor.into();
    next.sealed = sealed;
    let changed = tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "UPDATE pin_management_archive SET chunks=$1,bytes=$2,digest=$3,state=$4 WHERE archive_id=$5 AND header=$6 AND state='staging' AND chunks=$7 AND bytes=$8 AND digest=$9",
        [next.chunks.into(),next.bytes.into(),cursor.into(),if sealed {"sealed"} else {"staging"}.into(),id.into(),header.into(),expected.chunks.into(),expected.bytes.into(),expected.cursor.clone().into()]))
        .await.map_err(|_| ManagementError::Database)?;
    if changed.rows_affected() != 1 {
        return Err(ManagementError::Changed);
    }
    Ok(next)
}

/// Resolve an ambiguous commit using its full immutable page, not just a flag.
pub(crate) async fn committed<C: ConnectionTrait>(
    db: &C,
    id: &str,
    header: &str,
    before: &Position,
    after: &Position,
    payload: &[u8],
) -> Result<bool> {
    if before
        .chunks
        .checked_add(payload.len().div_ceil(CELL_SLICE) as i64)
        != Some(after.chunks)
        || before.bytes.checked_add(payload.len() as i64) != Some(after.bytes)
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let Some(current) = load(db, id, header, false).await? else {
        return Ok(false);
    };
    if &current == before {
        return Ok(false);
    }
    if &current != after {
        return Err(ManagementError::Changed);
    }
    for (offset, expected) in payload.chunks(CELL_SLICE).enumerate() {
        let ordinal = before
            .chunks
            .checked_add(offset as i64)
            .ok_or(ManagementError::Budget)?;
        let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
            "SELECT payload FROM pin_management_archive_chunks WHERE archive_id=$1 AND ordinal=$2",
            [id.into(),ordinal.into()])).await.map_err(|_|ManagementError::Database)?
            .ok_or(ManagementError::InvalidEvidence)?;
        if row
            .try_get::<Vec<u8>>("", "payload")
            .map_err(|_| ManagementError::InvalidEvidence)?
            != expected
        {
            return Err(ManagementError::InvalidEvidence);
        }
    }
    Ok(true)
}
