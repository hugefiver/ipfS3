//! Indexed, length-bounded reconstruction of one original or semantic row.
use super::*;
use std::collections::BTreeMap;

struct Cell {
    length: usize,
    bytes: Vec<u8>,
    parts: usize,
}

impl InventorySpool {
    pub(super) async fn read_row(
        &self,
        db: &impl ConnectionTrait,
        key: &NativeKey,
        projection: bool,
    ) -> Result<Option<NativeRow>> {
        let identity = encode(key)?;
        if one(
            db,
            "SELECT 1 AS present FROM members WHERE table_name=$1 AND row_key=$2",
            vec![key.table.clone().into(), identity.clone().into()],
        )
        .await?
        .is_none()
        {
            return Ok(None);
        }
        let table = crate::store::pinning::management::native::current_catalog()
            .iter()
            .find(|table| table.table == key.table)
            .ok_or(ManagementError::InvalidEvidence)?;
        let metadata = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            format!("SELECT CASE WHEN length(column_name)<=256 THEN column_name END AS column_name,length FROM parts WHERE table_name=$1 AND row_key=$2 AND offset=0 ORDER BY column_name LIMIT {}", table.columns.len() + 1),
            [key.table.clone().into(), identity.clone().into()],
        )).await.map_err(|_| ManagementError::Database)?;
        if metadata.len() != table.columns.len() {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut result = NativeRow::new();
        let mut cells = BTreeMap::new();
        let mut total = 0usize;
        let mut parts = 0usize;
        for row in metadata {
            let column: String = get(&row, "column_name")?;
            if !table.columns.contains(&column) {
                return Err(ManagementError::InvalidEvidence);
            }
            let ignored = projection
                && (matches!(
                    column.as_str(),
                    "last_error"
                        | "last_error_text"
                        | "first_error"
                        | "safe_error"
                        | "last_error_redacted"
                ) || key.table == "zip_root_references" && column == "verification_receipt");
            let length: Option<i64> = get(&row, "length")?;
            if ignored || length.is_none() {
                result.insert(column, None);
                continue;
            }
            let length = usize::try_from(length.ok_or(ManagementError::InvalidEvidence)?)
                .map_err(|_| ManagementError::InvalidEvidence)?;
            total = total.checked_add(length).ok_or(ManagementError::Budget)?;
            if total > 1024 * 1024 {
                return Err(ManagementError::Budget);
            }
            parts += length.div_ceil(CELL_SLICE).max(1);
            cells.insert(
                column,
                Cell {
                    length,
                    bytes: Vec::with_capacity(length),
                    parts: 0,
                },
            );
        }
        if !cells.is_empty() {
            let mut parameters = vec![Value::from(key.table.clone()), identity.into()];
            parameters.extend(cells.keys().cloned().map(Value::from));
            let columns = (3..=parameters.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(",");
            let rows = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                format!("SELECT column_name,offset,checksum,CASE WHEN length(bytes)<={CELL_SLICE} THEN bytes END AS bytes FROM parts WHERE table_name=$1 AND row_key=$2 AND column_name IN ({columns}) ORDER BY column_name,offset LIMIT {}", parts + 1), parameters,
            )).await.map_err(|_| ManagementError::Database)?;
            if rows.len() != parts {
                return Err(ManagementError::InvalidEvidence);
            }
            for row in rows {
                let column: String = get(&row, "column_name")?;
                let cell = cells
                    .get_mut(&column)
                    .ok_or(ManagementError::InvalidEvidence)?;
                let offset = usize::try_from(get::<i64>(&row, "offset")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
                let bytes: Vec<u8> = get(&row, "bytes")?;
                ArchivePart {
                    table: key.table.clone(),
                    key: key.clone(),
                    column: column.clone(),
                    offset: offset as u64,
                    length: Some(cell.length as u64),
                    bytes: bytes.clone(),
                }
                .check_checksum(&get::<String>(&row, "checksum")?)?;
                if offset != cell.bytes.len()
                    || bytes.len() != (cell.length - offset).min(CELL_SLICE)
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                cell.parts += 1;
                cell.bytes.extend(bytes);
            }
        }
        for (column, cell) in cells {
            if cell.bytes.len() != cell.length
                || cell.parts != cell.length.div_ceil(CELL_SLICE).max(1)
            {
                return Err(ManagementError::InvalidEvidence);
            }
            result.insert(
                column,
                Some(String::from_utf8(cell.bytes).map_err(|_| ManagementError::InvalidEvidence)?),
            );
        }
        Ok(Some(result))
    }
}
