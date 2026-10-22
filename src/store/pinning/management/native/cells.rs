//! Large opaque cells are streamed as native UTF-8 bytes, never parsed as authority.
use super::{
    ManagementError, NativeKey, Result,
    meter::{Budget, CELL_SLICE},
    scope,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement};
use std::io::{self, Write};

/// Count each completed physical write, including prefixes of failed cells.
pub(crate) fn write_counted(
    output: &mut impl Write,
    budget: &mut Budget,
    mut bytes: &[u8],
) -> Result<()> {
    while !bytes.is_empty() {
        match output.write(bytes) {
            Ok(0) => return Err(ManagementError::Database),
            Ok(n) => {
                budget.work.output_bytes = budget
                    .work
                    .output_bytes
                    .checked_add(n as u64)
                    .ok_or(ManagementError::Budget)?;
                bytes = &bytes[n..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(ManagementError::Database),
        }
    }
    Ok(())
}

/// `None` is SQL NULL; `Some(0)` is an empty non-NULL cell. The caller owns
/// transaction consistency and must not publish the sink until this returns.
pub async fn copy_cell<W: Write>(
    tx: &DatabaseTransaction,
    key: &NativeKey,
    column: &str,
    output: &mut W,
    budget: &mut Budget,
) -> Result<Option<u64>> {
    copy_cell_with(tx, key, column, budget, |chunk, budget| {
        write_counted(output, budget, chunk)
    })
    .await
}

/// The source prepays SQL and raw bytes; the emitter owns physical output and
/// any additional framing admission on the same operation budget.
pub(crate) async fn copy_cell_with(
    tx: &DatabaseTransaction,
    key: &NativeKey,
    column: &str,
    budget: &mut Budget,
    mut emit: impl FnMut(&[u8], &mut Budget) -> Result<()>,
) -> Result<Option<u64>> {
    let query = scope::queries(&scope::Selector::Exact(key.clone()))?.remove(0);
    if !query.table.columns.iter().any(|c| c == column) {
        return Err(ManagementError::InvalidEvidence);
    }
    let backend = tx.get_database_backend();
    let octets = scope::octets(backend, column);
    let _resident = budget.reserve(CELL_SLICE * 2)?;
    budget.charge(1, 1, 1, 0)?;
    let row = tx
        .query_one(Statement::from_sql_and_values(
            backend,
            format!(
                "SELECT CAST(length({octets}) AS BIGINT) AS size FROM \"{}\" WHERE {}",
                key.table, query.predicate
            ),
            query.values.clone(),
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Changed)?;
    let length: Option<i64> = row
        .try_get("", "size")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let Some(length) = length else {
        return Ok(None);
    };
    let length = u64::try_from(length).map_err(|_| ManagementError::Budget)?;
    let mut offset = 0u64;
    while offset < length {
        let n = (length - offset).min(CELL_SLICE as u64);
        budget.charge(1, 0, 0, n)?;
        let slice = if backend == DatabaseBackend::Sqlite {
            format!("substr({octets},{},{n})", offset + 1)
        } else {
            format!("substring({octets} FROM {} FOR {n})", offset + 1)
        };
        let row = tx
            .query_one(Statement::from_sql_and_values(
                backend,
                format!(
                    "SELECT {slice} AS chunk FROM \"{}\" WHERE {}",
                    key.table, query.predicate
                ),
                query.values.clone(),
            ))
            .await
            .map_err(|_| ManagementError::Database)?
            .ok_or(ManagementError::Changed)?;
        let chunk: Vec<u8> = row
            .try_get("", "chunk")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if chunk.len() as u64 != n {
            return Err(ManagementError::Changed);
        }
        emit(&chunk, budget)?;
        offset += n;
    }
    budget.check()?;
    Ok(Some(length))
}
