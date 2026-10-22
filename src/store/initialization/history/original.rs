//! Length-first, byte-sliced SQL originals for the isolated legacy decoder.
use super::*;
use native::meter::{CELL_SLICE, ResidentGuard};
use sea_orm::DatabaseBackend;

pub(super) async fn read<C: ConnectionTrait>(
    db: &C,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<(NativeRow, ResidentGuard)> {
    let table = native::current_catalog()
        .iter()
        .find(|t| t.table == key.table)
        .ok_or(ManagementError::Schema)?;
    if table.primary_key.len() != key.values.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    let values = table
        .primary_key
        .iter()
        .zip(&key.values)
        .map(|(c, v)| native::scope::bound_value(table, c, v))
        .collect::<Result<Vec<_>>>()?;
    let predicate = table
        .primary_key
        .iter()
        .enumerate()
        .map(|(i, c)| format!("\"{c}\"=${}", i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let octets = |column: &str| {
        if db.get_database_backend() == DatabaseBackend::Sqlite {
            format!("CAST(\"{column}\" AS BLOB)")
        } else {
            format!("convert_to(CAST(\"{column}\" AS TEXT),'UTF8')")
        }
    };
    let projection = table
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("CAST(length({}) AS BIGINT) AS n{i}", octets(c)))
        .collect::<Vec<_>>()
        .join(",");
    budget.charge(1, 1, table.columns.len() as u64, 0)?;
    let sizes = db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                "SELECT {projection} FROM \"{}\" WHERE {predicate}",
                table.table
            ),
            values.clone(),
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Changed)?;
    let lengths = (0..table.columns.len())
        .map(|i| {
            sizes
                .try_get::<Option<i64>>("", &format!("n{i}"))
                .map_err(|_| ManagementError::InvalidEvidence)
        })
        .collect::<Result<Vec<_>>>()?;
    let bytes = lengths.iter().try_fold(0usize, |sum, n| {
        sum.checked_add(
            usize::try_from(n.unwrap_or(0)).map_err(|_| ManagementError::InvalidEvidence)?,
        )
        .ok_or(ManagementError::Budget)
    })?;
    let resident = budget.reserve(
        bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(CELL_SLICE * 2 + table.columns.len() * 128))
            .ok_or(ManagementError::Budget)?,
    )?;
    let mut result = NativeRow::new();
    let mut small = Vec::new();
    let mut small_bytes = 0usize;
    for (index, length) in lengths.iter().enumerate() {
        if let Some(length) = length {
            let length = *length as usize;
            if length > 0 && length <= CELL_SLICE - small_bytes {
                small.push(index);
                small_bytes += length;
            }
        }
    }
    let mut small_values = std::collections::BTreeMap::new();
    if !small.is_empty() {
        budget.charge(1, 0, 0, small_bytes as u64)?;
        let projection = small
            .iter()
            .map(|index| {
                let value = octets(&table.columns[*index]);
                let length = lengths[*index].unwrap_or(0);
                let slice = if db.get_database_backend() == DatabaseBackend::Sqlite {
                    format!("substr({value},1,{length})")
                } else {
                    format!("substring({value} FROM 1 FOR {length})")
                };
                format!("{slice} AS v{index}")
            })
            .collect::<Vec<_>>()
            .join(",");
        let row = db
            .query_one(Statement::from_sql_and_values(
                db.get_database_backend(),
                format!(
                    "SELECT {projection} FROM \"{}\" WHERE {predicate}",
                    table.table
                ),
                values.clone(),
            ))
            .await
            .map_err(|_| ManagementError::Database)?
            .ok_or(ManagementError::Changed)?;
        for index in small {
            let value: Vec<u8> = row
                .try_get("", &format!("v{index}"))
                .map_err(|_| ManagementError::Changed)?;
            if Some(value.len() as i64) != lengths[index] {
                return Err(ManagementError::Changed);
            }
            small_values.insert(
                index,
                String::from_utf8(value).map_err(|_| ManagementError::InvalidEvidence)?,
            );
        }
    }
    for (index, (column, length)) in table.columns.iter().zip(lengths).enumerate() {
        if let Some(value) = small_values.remove(&index) {
            result.insert(column.clone(), Some(value));
            continue;
        }
        let value = if let Some(length) = length {
            let mut bytes = Vec::with_capacity(length as usize);
            while bytes.len() < length as usize {
                let count = CELL_SLICE.min(length as usize - bytes.len());
                budget.charge(1, 0, 0, count as u64)?;
                let slice = if db.get_database_backend() == DatabaseBackend::Sqlite {
                    format!("substr({},{},{count})", octets(column), bytes.len() + 1)
                } else {
                    format!(
                        "substring({} FROM {} FOR {count})",
                        octets(column),
                        bytes.len() + 1
                    )
                };
                let row = db
                    .query_one(Statement::from_sql_and_values(
                        db.get_database_backend(),
                        format!(
                            "SELECT {slice} AS chunk FROM \"{}\" WHERE {predicate}",
                            table.table
                        ),
                        values.clone(),
                    ))
                    .await
                    .map_err(|_| ManagementError::Database)?
                    .ok_or(ManagementError::Changed)?;
                let chunk: Vec<u8> = row
                    .try_get("", "chunk")
                    .map_err(|_| ManagementError::InvalidEvidence)?;
                if chunk.len() != count {
                    return Err(ManagementError::Changed);
                }
                bytes.extend_from_slice(&chunk);
            }
            Some(String::from_utf8(bytes).map_err(|_| ManagementError::InvalidEvidence)?)
        } else {
            None
        };
        result.insert(column.clone(), value);
    }
    Ok((result, resident))
}
