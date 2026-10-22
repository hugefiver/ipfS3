//! Reducer reconstruction batches share a one-MiB payload envelope.
use super::*;
use std::collections::BTreeMap;

pub(super) fn ignored(table: &str, column: &str) -> bool {
    matches!(
        column,
        "last_error" | "last_error_text" | "first_error" | "safe_error" | "last_error_redacted"
    ) || table == "zip_root_references" && column == "verification_receipt"
}

impl InventorySpool {
    pub(in crate::pinning::management::retirement) async fn projection_page(
        &self,
        db: &impl ConnectionTrait,
        keys: &[String],
    ) -> Result<Vec<(NativeKey, Result<NativeRow>)>> {
        let Some(first) = keys.first() else {
            return Ok(Vec::new());
        };
        let key: NativeKey = decode(first)?;
        let table = crate::store::pinning::management::native::current_catalog()
            .iter()
            .find(|t| t.table == key.table)
            .ok_or(ManagementError::InvalidEvidence)?;
        let mut parameters = vec![Value::from(key.table.clone())];
        parameters.extend(keys.iter().cloned().map(Value::from));
        let slots = (2..=parameters.len())
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(",");
        let metadata = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            format!("SELECT row_key,column_name,length FROM parts WHERE table_name=$1 AND row_key IN ({slots}) AND offset=0 ORDER BY row_key,column_name LIMIT {}",keys.len()*table.columns.len()+1),parameters
        )).await.map_err(|_|ManagementError::Database)?;
        let mut lengths: BTreeMap<String, Vec<(String, Option<usize>)>> = BTreeMap::new();
        for row in metadata {
            let column: String = get(&row, "column_name")?;
            if !table.columns.contains(&column) {
                return Err(ManagementError::InvalidEvidence);
            }
            let length = get::<Option<i64>>(&row, "length")?
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ManagementError::InvalidEvidence)?;
            lengths
                .entry(get(&row, "row_key")?)
                .or_default()
                .push((column, length));
        }
        let mut chosen = Vec::new();
        let mut total = 0usize;
        let mut parts = 0usize;
        for identity in keys {
            let columns = lengths
                .get(identity)
                .ok_or(ManagementError::InvalidEvidence)?;
            if columns.len() != table.columns.len() {
                return Err(ManagementError::InvalidEvidence);
            }
            let size = columns
                .iter()
                .filter(|(c, _)| !ignored(&table.table, c))
                .try_fold(0usize, |n, (_, length)| {
                    n.checked_add(length.unwrap_or(0))
                        .ok_or(ManagementError::Budget)
                })?;
            if size > 1024 * 1024 {
                if chosen.is_empty() {
                    return Ok(vec![(decode(identity)?, Err(ManagementError::Budget))]);
                }
                break;
            }
            if size > 1024 * 1024 - total {
                break;
            }
            total += size;
            parts += columns
                .iter()
                .filter(|(c, _)| !ignored(&table.table, c))
                .map(|(_, length)| length.unwrap_or(0).div_ceil(CELL_SLICE).max(1))
                .sum::<usize>();
            chosen.push(identity.clone());
        }
        let mut parameters = vec![Value::from(key.table.clone())];
        parameters.extend(chosen.iter().cloned().map(Value::from));
        let slots = (2..=parameters.len())
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(",");
        let ignored_columns = table
            .columns
            .iter()
            .filter(|c| ignored(&table.table, c))
            .map(|c| format!("'{c}'"))
            .collect::<Vec<_>>();
        let predicate = if ignored_columns.is_empty() {
            String::new()
        } else {
            format!(" AND column_name NOT IN ({})", ignored_columns.join(","))
        };
        let rows = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            format!("SELECT row_key,column_name,offset,length,checksum,CASE WHEN stored_bytes<={CELL_SLICE} AND page_bytes<=1048576 THEN bytes END AS bytes FROM (SELECT *,SUM(stored_bytes) OVER (ORDER BY row_key,column_name,offset ROWS UNBOUNDED PRECEDING) AS page_bytes FROM (SELECT row_key,column_name,offset,length,checksum,length(bytes) AS stored_bytes,CASE WHEN length(bytes)<={CELL_SLICE} THEN bytes END AS bytes FROM parts WHERE table_name=$1 AND row_key IN ({slots}){predicate} ORDER BY row_key,column_name,offset LIMIT {})) ORDER BY row_key,column_name,offset",parts+1),parameters
        )).await.map_err(|_|ManagementError::Database)?;
        if rows.len() != parts {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut cells: BTreeMap<(String, String), Vec<u8>> = BTreeMap::new();
        for row in rows {
            let identity: String = get(&row, "row_key")?;
            let column: String = get(&row, "column_name")?;
            let length = lengths
                .get(&identity)
                .and_then(|cols| cols.iter().find(|(c, _)| c == &column))
                .ok_or(ManagementError::InvalidEvidence)?
                .1
                .unwrap_or(0);
            let cell = cells.entry((identity.clone(), column.clone())).or_default();
            let offset = usize::try_from(get::<i64>(&row, "offset")?)
                .map_err(|_| ManagementError::InvalidEvidence)?;
            let bytes: Vec<u8> = get(&row, "bytes")?;
            ArchivePart {
                table: table.table.clone(),
                key: decode(&identity)?,
                column,
                offset: offset as u64,
                length: get::<Option<i64>>(&row, "length")?
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| ManagementError::InvalidEvidence)?,
                bytes: bytes.clone(),
            }
            .check_checksum(&get::<String>(&row, "checksum")?)?;
            if offset != cell.len()
                || offset > length
                || bytes.len() != (length - offset).min(CELL_SLICE)
            {
                return Err(ManagementError::InvalidEvidence);
            }
            cell.extend(bytes);
        }
        chosen
            .into_iter()
            .map(|identity| {
                let mut row = NativeRow::new();
                for (column, length) in lengths
                    .remove(&identity)
                    .ok_or(ManagementError::InvalidEvidence)?
                {
                    let value = if ignored(&table.table, &column) {
                        None
                    } else {
                        let bytes = cells
                            .remove(&(identity.clone(), column.clone()))
                            .ok_or(ManagementError::InvalidEvidence)?;
                        if bytes.len() != length.unwrap_or(0) {
                            return Err(ManagementError::InvalidEvidence);
                        }
                        length
                            .map(|_| {
                                String::from_utf8(bytes)
                                    .map_err(|_| ManagementError::InvalidEvidence)
                            })
                            .transpose()?
                    };
                    row.insert(column, value);
                }
                Ok((decode(&identity)?, Ok(row)))
            })
            .collect()
    }
}
