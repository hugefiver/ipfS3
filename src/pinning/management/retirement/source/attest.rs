//! Exact source comparison. The caller's archive CAS owns these coordinates.
use super::*;
mod proof;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::pinning::management::retirement) struct Cursor {
    pub source: SourceCheckpoint,
    pub ordinal: i64,
    pub partial: u64,
    pub position: i64,
    pub eof: usize,
}
impl Cursor {
    pub(in crate::pinning::management::retirement) fn new(source: SourceCheckpoint) -> Self {
        Self {
            source: source.rewind(),
            ordinal: 0,
            partial: 0,
            position: 0,
            eof: 0,
        }
    }
}

pub(in crate::pinning::management::retirement) struct LocalPage {
    parts: Vec<ArchivePart>,
    members: Vec<NativeKey>,
    eof: Vec<String>,
    pub bytes: u64,
}

fn member_limit(parts: &[ArchivePart]) -> Result<usize> {
    let catalog = native::current_catalog();
    let mut completed = 0usize;
    for part in parts {
        let table = catalog
            .binary_search_by(|table| table.table.cmp(&part.table))
            .map(|index| &catalog[index])
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if table.columns.last() == Some(&part.column)
            && part.offset.checked_add(part.bytes.len() as u64) == Some(part.length.unwrap_or(0))
        {
            completed += 1;
        }
    }
    // Keep the contiguous member prefix, including one unseen tail sentinel.
    // A partial row contributes only when its final column is fully present.
    Ok((completed + 1).min(257))
}

/// Bounded local IO finishes before the source writer boundary is acquired.
pub(in crate::pinning::management::retirement) async fn local(
    spool: &InventorySpool,
    cursor: &Cursor,
    run: &Run<'_>,
) -> Result<LocalPage> {
    super::super::spool::budget::charge(&spool.db, run, 0, 0).await?;
    let part_cap = if cursor.partial == 0 { 256i64 } else { 512 };
    let rows = spool.db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT ordinal,CASE WHEN length(table_name)<=256 THEN table_name END AS table_name,CASE WHEN length(CAST(row_key AS BLOB))<=65536 THEN row_key END AS row_key,CASE WHEN length(column_name)<=256 THEN column_name END AS column_name,offset,length,CASE WHEN length(checksum)=64 THEN checksum END AS checksum,CASE WHEN length(bytes)<=65536 THEN bytes END AS bytes,page_bytes FROM (SELECT *,SUM(length(bytes)*4+length(CAST(row_key AS BLOB))+length(table_name)+length(column_name)+256) OVER(ORDER BY ordinal ROWS UNBOUNDED PRECEDING) AS page_bytes FROM (SELECT * FROM parts WHERE ordinal>$1 ORDER BY ordinal LIMIT $2)) WHERE page_bytes<=8388608 ORDER BY ordinal",
        [cursor.ordinal.into(), part_cap.into()])).await.map_err(|_|ManagementError::Database)?;
    let measured = rows
        .last()
        .map(|row| get::<i64>(row, "page_bytes"))
        .transpose()?
        .unwrap_or(0);
    super::super::spool::budget::charge(
        &spool.db,
        run,
        rows.len() as u64,
        u64::try_from(measured).map_err(|_| ManagementError::InvalidEvidence)?,
    )
    .await?;
    let mut parts = Vec::<ArchivePart>::new();
    let mut part_bytes = 0u64;
    let mut checksum_key = String::new();
    let mut checksum_key_bytes = 0u64;
    for (offset, row) in rows.into_iter().enumerate() {
        if get::<i64>(&row, "ordinal")? != cursor.ordinal + offset as i64 + 1 {
            return Err(ManagementError::Changed);
        }
        let raw_key = get::<String>(&row, "row_key")?;
        let checksum = get::<String>(&row, "checksum")?;
        let part = ArchivePart {
            table: get(&row, "table_name")?,
            key: decode(&raw_key)?,
            column: get(&row, "column_name")?,
            offset: u64::try_from(get::<i64>(&row, "offset")?)
                .map_err(|_| ManagementError::InvalidEvidence)?,
            length: get::<Option<i64>>(&row, "length")?
                .map(u64::try_from)
                .transpose()
                .map_err(|_| ManagementError::InvalidEvidence)?,
            bytes: get(&row, "bytes")?,
        };
        if parts.last().is_none_or(|previous| previous.key != part.key) {
            checksum_key = encode(&part.key)?;
            checksum_key_bytes = checksum_key_bytes
                .checked_add(checksum_key.len() as u64)
                .ok_or(ManagementError::Budget)?;
        }
        if part.checksum_with_key(&checksum_key) != checksum {
            return Err(ManagementError::Changed);
        }
        // The four returned integer coordinates occupy at most eight bytes
        // each. The 8 MiB query footprint remains a resident/prepayment bound,
        // not a claim that its padding was read or serialized.
        part_bytes = part_bytes
            .checked_add(
                (part.table.len()
                    + raw_key.len()
                    + part.column.len()
                    + checksum.len()
                    + part.bytes.len()
                    + 4 * 8) as u64,
            )
            .ok_or(ManagementError::Budget)?;
        parts.push(part);
    }
    let member_limit = member_limit(&parts)?;
    let rows = spool.db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT m.table_name,CASE WHEN length(m.row_key)<=65536 THEN m.row_key END AS row_key,m.position,k.sort_key FROM members m LEFT JOIN wire_keys k ON k.table_name=m.table_name AND k.row_key=m.row_key WHERE position>$1 ORDER BY position LIMIT $2",
        [cursor.position.into(), (member_limit as i64).into()])).await.map_err(|_|ManagementError::Database)?;
    let mut members = Vec::new();
    let mut member_bytes = 0u64;
    for (offset, row) in rows.into_iter().enumerate() {
        let raw_key = get::<String>(&row, "row_key")?;
        let table = get::<String>(&row, "table_name")?;
        let order = get::<Vec<u8>>(&row, "sort_key")?;
        let key: NativeKey = decode(&raw_key)?;
        if get::<i64>(&row, "position")? != cursor.position + offset as i64 + 1
            || table != key.table
            || order != super::super::spool::wire_key(&key)
        {
            return Err(ManagementError::Changed);
        }
        // Include both returned fields and the decoded membership projection.
        member_bytes = member_bytes
            .checked_add((2 * (table.len() + order.len() + raw_key.len()) + 8) as u64)
            .ok_or(ManagementError::Budget)?;
        members.push(key);
    }
    let rows = spool
        .db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!(
                "SELECT table_name FROM eof ORDER BY table_name LIMIT {}",
                native::current_catalog().len() + 1
            ),
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    let mut eof = rows
        .iter()
        .map(|row| get::<String>(row, "table_name"))
        .collect::<Result<Vec<_>>>()?;
    let eof_bytes = eof.iter().try_fold(0usize, |bytes, table| {
        bytes
            .checked_add(table.len())
            .ok_or(ManagementError::Budget)
    })?;
    super::super::spool::budget::copy_work(&spool.db, run, eof_bytes).await?;
    let mut expected = native::current_catalog()
        .iter()
        .map(|t| t.table.clone())
        .collect::<Vec<_>>();
    expected.sort();
    if eof != expected {
        return Err(ManagementError::Changed);
    }
    eof.clear();
    eof.extend(native::current_catalog().iter().map(|t| t.table.clone()));
    if cursor.source.complete()
        && (!parts.is_empty()
            || !members.is_empty()
            || cursor.partial != 0
            || cursor.eof != eof.len()
            || one(
                &spool.db,
                "SELECT ordinal FROM parts WHERE ordinal<1 LIMIT 1",
                vec![],
            )
            .await?
            .is_some()
            || one(
                &spool.db,
                "SELECT position FROM members WHERE position<1 LIMIT 1",
                vec![],
            )
            .await?
            .is_some())
    {
        return Err(ManagementError::Changed);
    }
    super::super::spool::budget::copy_work(
        &spool.db,
        run,
        usize::try_from(
            member_bytes
                .checked_add(checksum_key_bytes)
                .ok_or(ManagementError::Budget)?,
        )
        .map_err(|_| ManagementError::Budget)?,
    )
    .await?;
    super::super::spool::budget::settle_local_read(
        spool,
        parts.len() as u64,
        u64::try_from(measured).map_err(|_| ManagementError::InvalidEvidence)?,
        TaskWork {
            rows: parts.len() as u64,
            bytes: part_bytes,
            pages: 0,
        },
    )
    .await?;
    Ok(LocalPage {
        parts,
        members,
        eof,
        bytes: u64::try_from(measured).map_err(|_| ManagementError::InvalidEvidence)?,
    })
}

#[derive(Serialize)]
pub(in crate::pinning::management::retirement) struct AcceptedPage {
    format: &'static str,
    pub before: Cursor,
    pub after: Cursor,
    #[serde(serialize_with = "proof::serialize_parts")]
    pub parts: Vec<Part>,
    pub members: Vec<NativeKey>,
    pub eof: Vec<String>,
    #[serde(skip)]
    pub read_bytes: u64,
}

pub(in crate::pinning::management::retirement) async fn wire(
    spool: &InventorySpool,
    after: Option<&(String, Vec<u8>)>,
) -> Result<(Option<(String, Vec<u8>)>, i64, Vec<u8>, bool, TaskWork)> {
    let (predicate, values) = match after {
        Some((table, key)) => (
            "WHERE (k.table_name,k.sort_key)>($1,$2)",
            vec![Value::from(table.clone()), key.clone().into()],
        ),
        None => ("", vec![]),
    };
    let rows=spool.db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        format!("SELECT table_name,sort_key,row_key,member FROM (SELECT *,SUM(length(table_name)+length(sort_key)+length(CAST(row_key AS BLOB))) OVER(ORDER BY table_name,sort_key ROWS UNBOUNDED PRECEDING) AS page_bytes FROM (SELECT k.table_name,k.sort_key,k.row_key,m.row_key AS member FROM wire_keys k LEFT JOIN members m ON m.table_name=k.table_name AND m.row_key=k.row_key {predicate} ORDER BY k.table_name,k.sort_key LIMIT 256)) WHERE page_bytes<=1048576 ORDER BY table_name,sort_key"),values
    )).await.map_err(|_|ManagementError::Database)?;
    let mut next = after.cloned();
    let mut payload = Vec::new();
    let mut keys = Vec::new();
    let mut bytes = 0u64;
    for row in &rows {
        let raw = get::<String>(row, "row_key")?;
        let key: NativeKey = decode(&raw)?;
        let table = get::<String>(row, "table_name")?;
        let order = get::<Vec<u8>>(row, "sort_key")?;
        if key.table != table
            || get::<Option<String>>(row, "member")?.as_ref() != Some(&raw)
            || super::super::spool::wire_key(&key) != order
        {
            return Err(ManagementError::Changed);
        }
        // The joined membership copy and decoded key fit a second projection.
        bytes = bytes
            .checked_add(2 * (table.len() + order.len() + raw.len()) as u64)
            .ok_or(ManagementError::Budget)?;
        next = Some((table, order));
        keys.push(key);
    }
    serde_json::to_writer(&mut payload, &keys).map_err(|_| ManagementError::InvalidEvidence)?;
    let work = TaskWork {
        rows: 2 * rows.len() as u64,
        bytes,
        pages: 0,
    };
    Ok((next, rows.len() as i64, payload, rows.is_empty(), work))
}

pub(in crate::pinning::management::retirement) async fn compare(
    tx: &DatabaseTransaction,
    before: Cursor,
    local: LocalPage,
    cancel: crate::store::pinning::management::transaction::CancellationToken,
) -> Result<AcceptedPage> {
    let read = page::read(
        tx,
        before.source.clone(),
        0,
        0,
        page::Control {
            cancel,
            total_rows: 256,
            total_bytes: 1024 * 1024,
            ordered_cells: true,
        },
    )
    .await?;
    let mut after = before.clone();
    let mut index = 0usize;
    for live in &read.parts {
        let mut consumed = 0usize;
        loop {
            let stored = local.parts.get(index).ok_or(ManagementError::Changed)?;
            let offset =
                usize::try_from(after.partial).map_err(|_| ManagementError::InvalidEvidence)?;
            if stored.table != live.table
                || stored.key != *live.key
                || stored.column != live.column
                || stored.length != live.length
                || stored.offset.checked_add(after.partial) != Some(live.offset + consumed as u64)
                || offset > stored.bytes.len()
            {
                return Err(ManagementError::Changed);
            }
            let size = (stored.bytes.len() - offset).min(live.bytes.len() - consumed);
            if stored.bytes[offset..offset + size] != live.bytes[consumed..consumed + size] {
                return Err(ManagementError::Changed);
            }
            consumed += size;
            after.partial += size as u64;
            if after.partial == stored.bytes.len() as u64 {
                after.ordinal += 1;
                after.partial = 0;
                index += 1;
            } else if size == 0 {
                return Err(ManagementError::Changed);
            }
            if consumed == live.bytes.len() {
                break;
            }
        }
    }
    let members = read
        .members
        .into_iter()
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    if local.members.len() < members.len() || local.members[..members.len()] != members {
        return Err(ManagementError::Changed);
    }
    after.position += members.len() as i64;
    for table in &read.eof {
        if local.eof.get(after.eof) != Some(table) {
            return Err(ManagementError::Changed);
        }
        after.eof += 1;
    }
    after.source = read.checkpoint;
    if after.source.complete()
        && (index != local.parts.len()
            || after.partial != 0
            || members.len() != local.members.len())
    {
        return Err(ManagementError::Changed);
    }
    Ok(AcceptedPage {
        format: "S75-source-rows-1",
        before,
        after,
        parts: read.parts,
        members,
        eof: read.eof,
        read_bytes: read.work.total_bytes()?,
    })
}

#[cfg(test)]
mod accounting_tests;
#[cfg(test)]
mod prefetch_tests;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::pinning::management::actions::tests::fixture;

    #[tokio::test]
    async fn standard_parts_attest_across_pages_with_bounded_aligned_prefetch() {
        let fixture = fixture("source-attestation-prefetch").await;
        fixture.db.execute_unprepared("WITH RECURSIVE epochs(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM epochs WHERE n<300) INSERT INTO pin_resource_history(provider,cid,epoch,ledger) SELECT 'source','cid',n,'{\"private\":\"秘密🔒\"}' FROM epochs").await.unwrap();
        let tx = fixture.db.begin().await.unwrap();
        let checkpoint = SourceCheckpoint::start(
            &tx,
            RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot()),
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let spool = InventorySpool::create(&path, file, &checkpoint)
            .await
            .unwrap();
        while spool
            .scan(&fixture.db, &RunLimits::default())
            .await
            .unwrap()
            == Progress::Pending
        {}
        let limits = RunLimits::default();
        let run = super::super::super::spool::task_run(&spool.db, &limits)
            .await
            .unwrap();
        let mut cursor = Cursor::new(checkpoint);
        let first = local(&spool, &cursor, &run).await.unwrap();
        assert_eq!(first.parts.len(), 256);
        assert!(first.members.len() < 257);
        let mut partial = cursor.clone();
        partial.ordinal = first
            .parts
            .iter()
            .position(|part| part.bytes.len() > 1)
            .unwrap() as i64;
        partial.partial = 1;
        assert_eq!(
            local(&spool, &partial, &run).await.unwrap().parts.len(),
            512
        );
        let mut members = Vec::new();
        let mut checked_prefetch = false;
        while !cursor.source.complete() {
            let stored = local(&spool, &cursor, &run).await.unwrap();
            if !checked_prefetch
                && stored
                    .parts
                    .first()
                    .is_some_and(|part| part.table == "pin_resource_history")
            {
                let rows = spool
                    .db
                    .query_all(Statement::from_sql_and_values(
                        DatabaseBackend::Sqlite,
                        "SELECT row_key FROM members WHERE position>$1 ORDER BY position LIMIT 257",
                        [cursor.position.into()],
                    ))
                    .await
                    .unwrap();
                let all_members = rows
                    .iter()
                    .map(|row| {
                        decode::<NativeKey>(&get::<String>(row, "row_key").unwrap()).unwrap()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(all_members.len(), 257);
                assert!(stored.members.len() < all_members.len());
                assert!(&all_members[..stored.members.len()] == stored.members.as_slice());
                let fetched_members = stored.members.len();
                let saved_bytes = all_members[stored.members.len()..]
                    .iter()
                    .map(|key| encode(key).unwrap().len() as u64)
                    .sum::<u64>();
                assert!(saved_bytes > 0);
                let original = LocalPage {
                    parts: stored.parts.clone(),
                    members: all_members,
                    eof: stored.eof.clone(),
                    bytes: stored.bytes + saved_bytes,
                };
                let tx = fixture.db.begin().await.unwrap();
                let baseline = compare(&tx, cursor.clone(), original, Default::default())
                    .await
                    .unwrap();
                tx.rollback().await.unwrap();
                let tx = fixture.db.begin().await.unwrap();
                let bounded = compare(&tx, cursor.clone(), stored, Default::default())
                    .await
                    .unwrap();
                tx.rollback().await.unwrap();
                assert!(baseline.before == bounded.before && baseline.after == bounded.after);
                proof::assert_round_trip(&bounded);
                assert!(
                    baseline.parts == bounded.parts
                        && baseline.members == bounded.members
                        && baseline.eof == bounded.eof
                );
                eprintln!(
                    "source member prefetch accepted={} fetched={fetched_members} saved_serialized_key_bytes={saved_bytes}",
                    bounded.members.len()
                );
                members.extend(
                    bounded
                        .members
                        .into_iter()
                        .filter(|key| key.table == "pin_resource_history"),
                );
                cursor = bounded.after;
                checked_prefetch = true;
                continue;
            }
            let tx = fixture.db.begin().await.unwrap();
            let accepted = compare(&tx, cursor, stored, Default::default())
                .await
                .unwrap();
            tx.rollback().await.unwrap();
            proof::assert_round_trip(&accepted);
            assert_eq!(accepted.after.partial, 0);
            members.extend(
                accepted
                    .members
                    .into_iter()
                    .filter(|key| key.table == "pin_resource_history"),
            );
            cursor = accepted.after;
        }
        assert_eq!(members.len(), 300);
        assert!(checked_prefetch);
        assert!(
            members
                .iter()
                .enumerate()
                .all(|(index, key)| key.values[2] == (index + 1).to_string())
        );
        let tail = local(&spool, &cursor, &run).await.unwrap();
        assert!(tail.parts.is_empty());
        assert!(tail.members.is_empty());
        spool.close().await.unwrap();
        workspace.cleanup().unwrap();
    }
}
