//! Length-first native-keyset pages for rows whose cells fit one slice.
use super::*;

pub(super) async fn read(
    tx: &DatabaseTransaction,
    table: &NativeTable,
    after: Option<&NativeKey>,
    part_cap: Option<usize>,
    work: &mut ReadWork,
) -> Result<Vec<(NativeKey, Vec<ArchivePart>)>> {
    let metadata_rows =
        256.min(1024 * 1024 / (table.primary_key.len() * 1024 + table.columns.len() * 8));
    let metadata_rows = part_cap.map_or(metadata_rows, |cap| {
        metadata_rows.min(cap / table.columns.len())
    });
    if metadata_rows == 0 {
        return Ok(Vec::new());
    }
    let mut values = Vec::new();
    let order = table
        .primary_key
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let predicate = if let Some(key) = after {
        for (column, value) in table.primary_key.iter().zip(&key.values) {
            values.push(bound(table, column, value)?);
        }
        format!(
            "WHERE ({order}) > ({})",
            (1..=values.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    } else {
        String::new()
    };
    let keys = table
        .primary_key
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "CASE WHEN length({})<=1024 THEN CAST(\"{c}\" AS TEXT) END AS k{i}",
                octets(tx.get_database_backend(), c)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let lengths = table
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "CAST(length({}) AS BIGINT) AS n{i}",
                octets(tx.get_database_backend(), c)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let started = std::time::Instant::now();
    let metadata = tx
        .query_all(Statement::from_sql_and_values(
            tx.get_database_backend(),
            format!(
                "SELECT {keys},{lengths} FROM \"{}\" {predicate} ORDER BY {order} LIMIT {metadata_rows}",
                table.table
            ),
            values.clone(),
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    work.keyset.observe(
        metadata.len() as u64,
        (metadata.len() * (table.primary_key.len() + table.columns.len())) as u64,
        0,
        started.elapsed(),
    );
    let mut selected = Vec::new();
    let mut bytes = 0u64;
    let mut resident = 0usize;
    let mut accepting = true;
    for row in metadata {
        let key = NativeKey {
            table: table.table.clone(),
            values: (0..table.primary_key.len())
                .map(|i| {
                    get::<Option<String>>(&row, &format!("k{i}"))?.ok_or(ManagementError::Budget)
                })
                .collect::<Result<_>>()?,
        };
        let key_bytes = key.values.iter().map(|v| v.len() as u64).sum::<u64>();
        work.keyset.bytes += key_bytes;
        if !accepting {
            continue;
        }
        let sizes = (0..table.columns.len())
            .map(|i| {
                get::<Option<i64>>(&row, &format!("n{i}"))?
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| ManagementError::InvalidEvidence)
            })
            .collect::<Result<Vec<_>>>()?;
        let size: u64 = sizes.iter().flatten().sum();
        let footprint = encode(&key)?
            .len()
            .checked_add(256)
            .and_then(|n| n.checked_mul(table.columns.len()))
            .and_then(|n| n.checked_add(size as usize * 2))
            .ok_or(ManagementError::Budget)?;
        if sizes.iter().flatten().any(|n| *n > CELL_SLICE as u64)
            || size + key_bytes > 1024 * 1024 - bytes
            || footprint > 4 * 1024 * 1024 - resident
        {
            accepting = false;
            continue;
        }
        bytes += size + key_bytes;
        resident += footprint;
        selected.push((key, sizes));
    }
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let projection = table
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let raw = octets(tx.get_database_backend(), c);
            if tx.get_database_backend() == DatabaseBackend::Sqlite {
                format!("substr({raw},1,{CELL_SLICE}) AS c{i}")
            } else {
                format!("substring({raw} FROM 1 FOR {CELL_SLICE}) AS c{i}")
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let started = std::time::Instant::now();
    let rows = tx
        .query_all(Statement::from_sql_and_values(
            tx.get_database_backend(),
            format!(
                "SELECT {keys},{projection} FROM \"{}\" {predicate} ORDER BY {order} LIMIT {}",
                table.table,
                selected.len()
            ),
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    work.cells.observe(
        rows.len() as u64,
        (rows.len() * (table.primary_key.len() + table.columns.len())) as u64,
        bytes,
        started.elapsed(),
    );
    if rows.len() != selected.len() {
        return Err(ManagementError::Changed);
    }
    rows.into_iter()
        .zip(selected)
        .map(|(row, (key, sizes))| {
            for (i, expected) in key.values.iter().enumerate() {
                if get::<String>(&row, &format!("k{i}"))? != *expected {
                    return Err(ManagementError::Changed);
                }
            }
            let parts = sizes
                .into_iter()
                .enumerate()
                .map(|(i, length)| {
                    let bytes = get::<Option<Vec<u8>>>(&row, &format!("c{i}"))?.unwrap_or_default();
                    if bytes.len() as u64 != length.unwrap_or(0) {
                        return Err(ManagementError::Changed);
                    }
                    Ok(ArchivePart {
                        table: table.table.clone(),
                        key: key.clone(),
                        column: table.columns[i].clone(),
                        offset: 0,
                        length,
                        bytes,
                    })
                })
                .collect::<Result<_>>()?;
            Ok((key, parts))
        })
        .collect()
}
