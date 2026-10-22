use super::*;
use sea_orm::{DatabaseConnection, QueryResult};

pub(super) struct Candidate<'a> {
    pub key: &'a NativeKey,
    pub columns: &'a [String],
}

pub(crate) struct Memory {
    _metadata: Arc<ResidentGuard>,
    _payload: ResidentGuard,
}

#[derive(PartialEq, Eq)]
struct Meta {
    candidate: usize,
    column: String,
    offset: u64,
    length: Option<u64>,
    n: usize,
}

fn slice_length(length: Option<u64>, offset: u64) -> Result<usize> {
    if !offset.is_multiple_of(CELL_SLICE as u64)
        || (offset > 0 && length.is_none_or(|n| offset >= n))
    {
        return Err(ManagementError::InvalidEvidence);
    }
    match length {
        Some(length) if offset <= length && offset <= i64::MAX as u64 => {
            Ok((length - offset).min(CELL_SLICE as u64) as usize)
        }
        None if offset == 0 => Ok(0),
        _ => Err(ManagementError::InvalidEvidence),
    }
}

// parts is not STRICT. Only bounded bytes and controlled integer projections
// may leave SQLite, including when a numeric-affinity column contains a blob.
fn projection(column_limit: usize) -> String {
    format!(
        "substr(CAST(p.column_name AS BLOB),1,{column_limit}) AS column_name,\
         length(CAST(p.column_name AS BLOB)) AS column_bytes,\
         typeof(p.column_name)='text' AS column_type,\
         CASE typeof(p.length) WHEN 'null' THEN 0 WHEN 'integer' THEN CASE WHEN p.length>=0 THEN 1 ELSE -1 END ELSE -1 END AS length_type,\
         CASE WHEN typeof(p.length)='integer' AND p.length>=0 THEN p.length ELSE 0 END AS length,\
         typeof(p.offset)='integer' AS offset_type,\
         CASE WHEN typeof(p.offset)='integer' AND p.offset>=0 THEN p.offset ELSE -1 END AS offset,\
         typeof(p.bytes)='blob' AS bytes_type,\
         length(CAST(p.bytes AS BLOB)) AS stored_bytes,\
         typeof(p.checksum)='text' AS checksum_type,\
         length(CAST(p.checksum AS BLOB)) AS checksum_bytes"
    )
}

fn metadata(row: &QueryResult, candidate: usize, column: &str, offset: u64) -> Result<Meta> {
    let length = match get::<i64>(row, "length_type")? {
        0 if get::<i64>(row, "length")? == 0 => None,
        1 => Some(
            u64::try_from(get::<i64>(row, "length")?)
                .map_err(|_| ManagementError::InvalidEvidence)?,
        ),
        _ => return Err(ManagementError::InvalidEvidence),
    };
    let n = slice_length(length, offset)?;
    if get::<i64>(row, "candidate")? != candidate as i64
        || get::<i64>(row, "column_type")? != 1
        || get::<Vec<u8>>(row, "column_name")? != column.as_bytes()
        || get::<i64>(row, "column_bytes")? != column.len() as i64
        || get::<i64>(row, "offset_type")? != 1
        || u64::try_from(get::<i64>(row, "offset")?).ok() != Some(offset)
        || get::<i64>(row, "bytes_type")? != 1
        || get::<i64>(row, "stored_bytes")? != n as i64
        || get::<i64>(row, "checksum_type")? != 1
        || get::<i64>(row, "checksum_bytes")? != 64
    {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(Meta {
        candidate,
        column: column.into(),
        offset,
        length,
        n,
    })
}

fn reserve_metadata(
    candidates: &[Candidate<'_>],
    budget: &mut Budget,
) -> Result<Arc<ResidentGuard>> {
    let mut bytes = 0;
    for candidate in candidates {
        let key_bytes =
            candidate.key.table.len() + candidate.key.values.iter().map(String::len).sum::<usize>();
        bytes += 4096 + key_bytes * 16;
        for column in candidate.columns {
            bytes += 1024 + column.len() * 8;
        }
    }
    Ok(Arc::new(budget.reserve(bytes)?))
}

async fn read_metadata(
    tx: &impl ConnectionTrait,
    candidates: &[Candidate<'_>],
    offset: u64,
    budget: &mut Budget,
) -> Result<Vec<Meta>> {
    let count = candidates
        .iter()
        .map(|c| c.columns.len() + 1)
        .sum::<usize>();
    budget.charge(1, 0, count as u64, 0)?;
    let mut queries = Vec::new();
    let mut values = Vec::new();
    for (ordinal, candidate) in candidates.iter().enumerate() {
        let limit = candidate
            .columns
            .iter()
            .map(String::len)
            .max()
            .ok_or(ManagementError::Schema)?
            + 1;
        queries.push(format!(
            "SELECT * FROM (SELECT {ordinal} AS candidate,{} FROM parts p WHERE p.table_name=? AND p.row_key=? AND p.offset=? ORDER BY p.column_name LIMIT {})",
            projection(limit), candidate.columns.len() + 1,
        ));
        values.extend([
            Value::from(candidate.key.table.clone()),
            encode(candidate.key)?.into(),
            (offset as i64).into(),
        ]);
    }
    let rows = tx
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!(
                "{} ORDER BY candidate,column_name",
                queries.join(" UNION ALL ")
            ),
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    if rows.len() != count - candidates.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut rows = rows.iter();
    let mut result = Vec::new();
    for (ordinal, candidate) in candidates.iter().enumerate() {
        for column in candidate.columns {
            result.push(metadata(
                rows.next().ok_or(ManagementError::InvalidEvidence)?,
                ordinal,
                column,
                offset,
            )?);
        }
    }
    Ok(result)
}

async fn payload(
    tx: &impl ConnectionTrait,
    candidates: &[Candidate<'_>],
    selected: &[Meta],
    metadata_memory: Arc<ResidentGuard>,
    budget: &mut Budget,
) -> Result<(Vec<Part>, Arc<Memory>)> {
    let bytes = selected.iter().map(|cell| cell.n).sum::<usize>();
    // SQLx retains its result buffers while get() decodes owned bytes. The
    // checksum owns that decoded buffer, rather than cloning another payload.
    budget.charge(1, 0, selected.len() as u64, bytes as u64)?;
    let memory = Arc::new(Memory {
        _metadata: metadata_memory,
        _payload: budget.reserve(bytes * 2 + CELL_SLICE + selected.len() * 1024)?,
    });
    let mut bindings = Vec::new();
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for (ordinal, candidate) in candidates.iter().enumerate() {
        keys.push(format!("({ordinal},?,?)"));
        values.extend([
            Value::from(candidate.key.table.clone()),
            encode(candidate.key)?.into(),
        ]);
    }
    for (ordinal, cell) in selected.iter().enumerate() {
        bindings.push(format!("({ordinal},{},?,?,?)", cell.candidate));
        values.extend([
            Value::from(cell.column.clone()),
            (cell.offset as i64).into(),
            (cell.n as i64).into(),
        ]);
    }
    let column_limit = selected
        .iter()
        .map(|cell| cell.column.len())
        .max()
        .ok_or(ManagementError::InvalidEvidence)?
        + 1;
    let rows = tx.query_all(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        format!(
            "WITH candidates(candidate,table_name,row_key) AS (VALUES {}),\
             selected(ordinal,candidate,column_name,offset,n) AS (VALUES {}) \
             SELECT s.ordinal,s.candidate,{},COALESCE(substr(CAST(p.bytes AS BLOB),1,s.n),X'') AS bytes,\
             substr(CAST(p.checksum AS BLOB),1,64) AS checksum FROM selected s \
             JOIN candidates c ON c.candidate=s.candidate JOIN parts p \
             ON p.table_name=c.table_name AND p.row_key=c.row_key AND p.column_name=s.column_name AND p.offset=s.offset ORDER BY s.ordinal",
            keys.join(","), bindings.join(","), projection(column_limit),
        ), values,
    )).await.map_err(|_| ManagementError::Database)?;
    if rows.len() != selected.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut parts = Vec::with_capacity(rows.len());
    for (ordinal, (row, expected)) in rows.iter().zip(selected).enumerate() {
        let actual = metadata(row, expected.candidate, &expected.column, expected.offset)?;
        let bytes = get::<Vec<u8>>(row, "bytes")?;
        let checksum = get::<Vec<u8>>(row, "checksum")?;
        if get::<i64>(row, "ordinal")? != ordinal as i64
            || actual != *expected
            || bytes.len() != expected.n
            || checksum.len() != 64
        {
            return Err(ManagementError::InvalidEvidence);
        }
        let key = candidates[expected.candidate].key;
        let part = ArchivePart {
            table: key.table.clone(),
            key: key.clone(),
            column: expected.column.clone(),
            offset: expected.offset,
            length: expected.length,
            bytes,
        };
        part.check_checksum(
            std::str::from_utf8(&checksum).map_err(|_| ManagementError::InvalidEvidence)?,
        )?;
        parts.push(Part {
            length: part.length,
            bytes: part.bytes,
            memory: memory.clone(),
        });
    }
    Ok((parts, memory))
}

pub(super) async fn read(
    db: &DatabaseConnection,
    candidates: &[Candidate<'_>],
    budget: &mut Budget,
) -> Result<Vec<Cells>> {
    let metadata_memory = reserve_metadata(candidates, budget)?;
    let tx = db.begin().await.map_err(|_| ManagementError::Database)?;
    let metadata = read_metadata(&tx, candidates, 0, budget).await?;
    let cap = budget.remaining_io_allowance()?.1.min(PAGE_BYTES as u64) as usize;
    let mut bytes = 0;
    let count = metadata
        .iter()
        .take_while(|cell| {
            if cell.n > cap - bytes {
                return false;
            }
            bytes += cell.n;
            true
        })
        .count();
    if count == 0 {
        return Err(ManagementError::Budget);
    }
    let selected = &metadata[..count];
    let (parts, memory) = payload(&tx, candidates, selected, metadata_memory, budget).await?;
    tx.commit().await.map_err(|_| ManagementError::Database)?;
    let last = selected
        .last()
        .ok_or(ManagementError::InvalidEvidence)?
        .candidate;
    let mut cells = candidates[..=last]
        .iter()
        .map(|candidate| Cells {
            first: candidate
                .columns
                .iter()
                .map(|column| (column.clone(), None))
                .collect(),
            _memory: memory.clone(),
        })
        .collect::<Vec<_>>();
    for (cell, part) in selected.iter().zip(parts) {
        *cells[cell.candidate]
            .first
            .get_mut(&cell.column)
            .ok_or(ManagementError::InvalidEvidence)? = Some(part);
    }
    Ok(cells)
}

pub(super) async fn part(
    db: &DatabaseConnection,
    key: &NativeKey,
    column: &str,
    offset: u64,
    length: Option<Option<u64>>,
    budget: &mut Budget,
) -> Result<Part> {
    let columns = vec![column.to_owned()];
    let candidates = [Candidate {
        key,
        columns: &columns,
    }];
    let metadata_memory = reserve_metadata(&candidates, budget)?;
    let tx = db.begin().await.map_err(|_| ManagementError::Database)?;
    let metadata = if let Some(length) = length {
        vec![Meta {
            candidate: 0,
            column: column.into(),
            offset,
            length,
            n: slice_length(length, offset)?,
        }]
    } else {
        // Fallback addresses one column, not every other column at this offset.
        read_one_metadata(&tx, key, column, offset, budget).await?
    };
    let (mut parts, _) = payload(&tx, &candidates, &metadata, metadata_memory, budget).await?;
    tx.commit().await.map_err(|_| ManagementError::Database)?;
    parts.pop().ok_or(ManagementError::InvalidEvidence)
}

async fn read_one_metadata(
    tx: &impl ConnectionTrait,
    key: &NativeKey,
    column: &str,
    offset: u64,
    budget: &mut Budget,
) -> Result<Vec<Meta>> {
    if offset > i64::MAX as u64 {
        return Err(ManagementError::InvalidEvidence);
    }
    budget.charge(1, 0, 1, 0)?;
    let row = one(tx, &format!(
        "SELECT 0 AS candidate,{} FROM parts p WHERE p.table_name=? AND p.row_key=? AND p.column_name=? AND p.offset=?",
        projection(column.len() + 1),
    ), vec![key.table.clone().into(), encode(key)?.into(), column.into(), (offset as i64).into()])
        .await?.ok_or(ManagementError::InvalidEvidence)?;
    Ok(vec![metadata(&row, 0, column, offset)?])
}
