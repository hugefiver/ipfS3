//! Length-first reads of a bounded group of original cells from one exact row.
use super::*;

struct Slice {
    column: usize,
    offset: u64,
    length: Option<u64>,
    bytes: usize,
}

pub(super) async fn read(
    tx: &DatabaseTransaction,
    table: &'static NativeTable,
    key: &NativeKey,
    position: (usize, u64),
    max_parts: usize,
    max_bytes: u64,
    work: &mut ReadWork,
) -> Result<Vec<Part>> {
    let (first_column, first_offset) = position;
    let mut values = Vec::new();
    let predicate = table
        .primary_key
        .iter()
        .zip(&key.values)
        .enumerate()
        .map(|(i, (column, value))| {
            values.push(bound(table, column, value)?);
            Ok(format!("\"{column}\"=${}", i + 1))
        })
        .collect::<Result<Vec<_>>>()?
        .join(" AND ");
    let columns = first_column..table.columns.len().min(first_column + max_parts);
    let metadata = columns
        .clone()
        .map(|i| {
            format!(
                "CAST(length({}) AS BIGINT) AS n{i}",
                octets(tx.get_database_backend(), &table.columns[i])
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let started = std::time::Instant::now();
    let row = tx
        .query_one(Statement::from_sql_and_values(
            tx.get_database_backend(),
            format!(
                "SELECT {metadata} FROM \"{}\" WHERE {predicate}",
                table.table
            ),
            values.clone(),
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Changed)?;
    work.cells.observe(
        1,
        (table.columns.len().min(first_column + max_parts) - first_column) as u64,
        0,
        started.elapsed(),
    );
    work.returned((table.columns.len().min(first_column + max_parts) - first_column) as u64 * 8)?;
    let mut remaining = max_bytes;
    let mut slices = Vec::new();
    for column in columns {
        let length = get::<Option<i64>>(&row, &format!("n{column}"))?
            .map(u64::try_from)
            .transpose()
            .map_err(|_| ManagementError::InvalidEvidence)?;
        let offset = if column == first_column {
            first_offset
        } else {
            0
        };
        let unread = length
            .unwrap_or(0)
            .checked_sub(offset)
            .ok_or(ManagementError::Changed)?;
        let bytes = unread.min(CELL_SLICE as u64) as usize;
        if bytes as u64 > remaining {
            break;
        }
        slices.push(Slice {
            column,
            offset,
            length,
            bytes,
        });
        remaining -= bytes as u64;
        if (bytes as u64) < unread {
            break;
        }
    }
    drop(row);
    if slices.is_empty() {
        return Ok(Vec::new());
    }
    let projection = slices
        .iter()
        .enumerate()
        .map(|(i, slice)| {
            let raw = octets(tx.get_database_backend(), &table.columns[slice.column]);
            let start = slice.offset + 1;
            let bytes = slice.bytes;
            if tx.get_database_backend() == DatabaseBackend::Sqlite {
                format!("substr({raw},{start},{bytes}) AS c{i}")
            } else {
                format!("substring({raw} FROM {start} FOR {bytes}) AS c{i}")
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let started = std::time::Instant::now();
    let row = tx
        .query_one(Statement::from_sql_and_values(
            tx.get_database_backend(),
            format!(
                "SELECT {projection} FROM \"{}\" WHERE {predicate}",
                table.table
            ),
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Changed)?;
    work.cells
        .observe(1, slices.len() as u64, 0, started.elapsed());
    work.copied(key_owned_bytes(key))?;
    let shared_key = std::sync::Arc::new(key.clone());
    let mut parts = Vec::with_capacity(slices.len());
    for (i, slice) in slices.into_iter().enumerate() {
        let bytes = get::<Option<Vec<u8>>>(&row, &format!("c{i}"))?;
        let returned = bytes.as_ref().map_or(0, Vec::len) as u64;
        work.cells.bytes += returned;
        work.returned(returned)?;
        // SQLite may return NULL for a zero-length BLOB slice; the separate
        // original length preserves SQL NULL versus an empty original value.
        if bytes.as_ref().map_or(0, Vec::len) != slice.bytes {
            return Err(ManagementError::Changed);
        }
        parts.push(Part {
            table: table.table.as_str(),
            key: shared_key.clone(),
            column: table.columns[slice.column].as_str(),
            offset: slice.offset,
            length: slice.length,
            bytes: bytes.unwrap_or_default(),
        });
    }
    Ok(parts)
}
