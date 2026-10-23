//! Bounded page staging with exact duplicate-byte verification before checkpoint CAS.
use super::*;

pub(super) async fn parts(
    tx: &DatabaseTransaction,
    parts: &[Part],
    run: &Run<'_>,
    work: &mut QueryWork,
) -> Result<()> {
    let mut identities = std::collections::BTreeMap::new();
    for part in parts {
        if let std::collections::btree_map::Entry::Vacant(entry) = identities.entry(&part.key) {
            let encoded = encode(part.key.as_ref())?;
            work.bytes += encoded.len() as u64;
            entry.insert(encoded);
        }
    }
    let mut after = 0;
    while after < parts.len() {
        run.check()?;
        let mut keys = Vec::new();
        let mut payload = 0usize;
        for part in parts[after..].iter().take(256) {
            let key = identities
                .get(&part.key)
                .ok_or(ManagementError::InvalidEvidence)?;
            let size = part.table.len() + key.len() + part.column.len() + part.bytes.len();
            if size > 1024 * 1024 - payload {
                break;
            }
            payload += size;
            keys.push(key);
        }
        if keys.is_empty() {
            return Err(ManagementError::Budget);
        }
        let batch = &parts[after..after + keys.len()];
        after += batch.len();
        let mut values = Vec::new();
        let mut tuples = Vec::new();
        for (part, key) in batch.iter().zip(&keys) {
            let first = values.len() + 1;
            tuples.push(format!(
                "({})",
                (first..first + 7)
                    .map(|i| format!("${i}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            values.extend([
                Value::from(part.table),
                key.as_str().into(),
                part.column.into(),
                (part.offset as i64).into(),
                part.length.map(|length| length as i64).into(),
                part.bytes.clone().into(),
                part.checksum_with_key(key).into(),
            ]);
        }
        let inserted = insert(tx,&format!("INSERT INTO parts(table_name,row_key,column_name,offset,length,bytes,checksum) VALUES {} ON CONFLICT(table_name,row_key,column_name,offset) DO NOTHING", tuples.join(",")), values,work).await?;
        if inserted == batch.len() as u64 {
            continue;
        }
        run.check()?;
        let mut values = Vec::new();
        let mut predicates = Vec::new();
        for (part, key) in batch.iter().zip(&keys) {
            let first = values.len() + 1;
            predicates.push(format!(
                "(table_name=${first} AND row_key=${} AND column_name=${} AND offset=${})",
                first + 1,
                first + 2,
                first + 3
            ));
            values.extend([
                Value::from(part.table),
                key.as_str().into(),
                part.column.into(),
                (part.offset as i64).into(),
            ]);
        }
        let started = std::time::Instant::now();
        let rows = tx.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            format!("SELECT table_name,row_key,column_name,offset,length,CASE WHEN length(bytes)<={CELL_SLICE} THEN bytes END AS bytes FROM parts WHERE {}", predicates.join(" OR ")),
            values,
        )).await.map_err(|_| ManagementError::Database)?;
        work.observe(
            rows.len() as u64,
            rows.len() as u64 * 6,
            0,
            started.elapsed(),
        );
        if rows.len() != batch.len() {
            return Err(ManagementError::Changed);
        }
        for row in rows {
            let table: String = get(&row, "table_name")?;
            let key: String = get(&row, "row_key")?;
            let column: String = get(&row, "column_name")?;
            let bytes: Vec<u8> = get(&row, "bytes")?;
            work.bytes += (table.len() + key.len() + column.len() + bytes.len() + 2 * 8) as u64;
            let offset: i64 = get(&row, "offset")?;
            let (part, _) = batch
                .iter()
                .zip(&keys)
                .find(|(part, original_key)| {
                    part.table == table
                        && original_key.as_str() == key
                        && part.column == column
                        && part.offset as i64 == offset
                })
                .ok_or(ManagementError::Changed)?;
            if get::<Option<i64>>(&row, "length")? != part.length.map(|length| length as i64)
                || bytes != part.bytes
            {
                return Err(ManagementError::Changed);
            }
        }
        run.check()?;
    }
    Ok(())
}

pub(super) async fn members(
    tx: &DatabaseTransaction,
    members: &[(NativeKey, u64)],
    run: &Run<'_>,
    work: &mut QueryWork,
) -> Result<()> {
    let mut values = Vec::new();
    let mut wire = Vec::new();
    let (mut row_bytes, mut wire_bytes) = (0usize, 0usize);
    for (key, position) in members {
        run.check()?;
        let identity = encode(key)?;
        let order = super::super::spool::wire_key(key);
        let row_size = key.table.len() + identity.len();
        let wire_size = row_size + order.len();
        if !values.is_empty()
            && (values.len() == 128 * 3
                || row_size > 1024 * 1024 - row_bytes
                || wire_size > 1024 * 1024 - wire_bytes)
        {
            member_batch(
                tx,
                std::mem::take(&mut values),
                std::mem::take(&mut wire),
                work,
            )
            .await?;
            row_bytes = 0;
            wire_bytes = 0;
        }
        values.extend([
            Value::from(key.table.clone()),
            identity.clone().into(),
            i64::try_from(*position)
                .map_err(|_| ManagementError::Budget)?
                .into(),
        ]);
        wire.extend([
            Value::from(key.table.clone()),
            order.into(),
            identity.into(),
        ]);
        row_bytes += row_size;
        wire_bytes += wire_size;
    }
    if !values.is_empty() {
        member_batch(tx, values, wire, work).await?;
    }
    Ok(())
}

async fn member_batch(
    tx: &DatabaseTransaction,
    values: Vec<Value>,
    wire: Vec<Value>,
    work: &mut QueryWork,
) -> Result<()> {
    let tuples = (0..values.len() / 3)
        .map(|row| format!("(${},${},${})", row * 3 + 1, row * 3 + 2, row * 3 + 3))
        .collect::<Vec<_>>()
        .join(",");
    insert(
        tx,
        &format!("INSERT INTO members VALUES {tuples}"),
        values,
        work,
    )
    .await?;
    insert(
        tx,
        &format!("INSERT INTO wire_keys VALUES {tuples}"),
        wire,
        work,
    )
    .await?;
    Ok(())
}

async fn insert(
    tx: &DatabaseTransaction,
    sql: &str,
    values: Vec<Value>,
    work: &mut QueryWork,
) -> Result<u64> {
    let bytes = values
        .iter()
        .map(|value| match value {
            Value::String(Some(text)) => text.len() as u64,
            Value::Bytes(Some(bytes)) => bytes.len() as u64,
            Value::BigInt(_) => 8,
            _ => 0,
        })
        .sum();
    let cells = values.len() as u64;
    let started = std::time::Instant::now();
    let result = tx
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            sql,
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    work.observe(result.rows_affected(), cells, bytes, started.elapsed());
    Ok(result.rows_affected())
}
