use super::*;
use crate::store::pinning::management::native::meter::{PAGE_BYTES, ResidentGuard};
use sea_orm::{DatabaseBackend, Statement, Value};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
mod cells;
pub(super) use cells::Memory;

pub(super) struct Key {
    pub native: NativeKey,
    pub order: Vec<u8>,
    pub position: Option<i64>,
    pub initial: Option<Cells>,
    pub memory: Arc<ResidentGuard>,
}

#[derive(Default)]
pub(super) struct Keys {
    rows: VecDeque<Key>,
}
impl Keys {
    pub async fn next(
        &mut self,
        spool: &InventorySpool,
        budget: &mut Budget,
        membership: Option<&str>,
        after_table: &str,
        after: &[u8],
    ) -> Result<Option<Key>> {
        self.load(spool, budget, membership, after_table, after, 64)
            .await?;
        Ok(self.rows.pop_front())
    }
    pub async fn native(
        &mut self,
        spool: &InventorySpool,
        budget: &mut Budget,
        after_table: &str,
        after: &[u8],
        row_cap: usize,
    ) -> Result<Option<Key>> {
        let cap = row_cap
            .min(64)
            .min(budget.remaining_io_allowance()?.0 as usize);
        if cap == 0 {
            return Err(ManagementError::Budget);
        }
        self.load(spool, budget, None, after_table, after, cap)
            .await?;
        if self.rows.front().is_some_and(|key| key.initial.is_none()) {
            let catalog = native::current_catalog();
            let candidates = self
                .rows
                .iter()
                .take(cap)
                .map(|key| {
                    let columns = &catalog
                        .iter()
                        .find(|layout| layout.table == key.native.table)
                        .ok_or(ManagementError::InvalidEvidence)?
                        .columns;
                    Ok(cells::Candidate {
                        key: &key.native,
                        columns,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let initial = cells::read(&spool.db, &candidates, budget).await?;
            for (key, cells) in self.rows.iter_mut().zip(initial) {
                key.initial = Some(cells);
            }
        }
        Ok(self.rows.pop_front())
    }
    async fn load(
        &mut self,
        spool: &InventorySpool,
        budget: &mut Budget,
        membership: Option<&str>,
        after_table: &str,
        after: &[u8],
        cap: usize,
    ) -> Result<()> {
        if !self.rows.is_empty() {
            return Ok(());
        }
        let (source, order, values, position) = if let Some(table) = membership {
            (
                "FROM wire_keys k JOIN members m ON m.table_name=k.table_name AND m.row_key=k.row_key WHERE k.table_name=$1 AND k.sort_key>$2",
                "k.sort_key",
                vec![table.into(), after.to_vec().into()],
                "m.position",
            )
        } else {
            (
                "FROM wire_keys k WHERE (k.table_name,k.sort_key)>($1,$2)",
                "k.table_name,k.sort_key",
                vec![after_table.into(), after.to_vec().into()],
                "NULL",
            )
        };
        budget.charge(2, 0, 0, 0)?;
        let _sizes_memory = budget.reserve(cap * 256)?;
        let tx = spool
            .db
            .begin()
            .await
            .map_err(|_| ManagementError::Database)?;
        let sizes = tx.query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!("SELECT length(CAST(k.row_key AS BLOB))+length(k.sort_key)+length(k.table_name) AS bytes {source} ORDER BY {order} LIMIT {cap}"),
            values.clone(),
        )).await.map_err(|_| ManagementError::Database)?;
        let mut bytes = 0usize;
        let mut count = 0;
        for row in &sizes {
            let size = usize::try_from(get::<i64>(row, "bytes")?)
                .map_err(|_| ManagementError::InvalidEvidence)?;
            if size > PAGE_BYTES - bytes {
                break;
            }
            bytes += size;
            count += 1;
        }
        if count == 0 {
            budget.charge(1, 0, 0, 0)?;
            tx.rollback().await.map_err(|_| ManagementError::Database)?;
            return if sizes.is_empty() {
                Ok(())
            } else {
                Err(ManagementError::Budget)
            };
        }
        // Raw SQL values, decoded keys and malformed bounded JSON all remain
        // covered until the last key from this page has finished its row.
        let memory = Arc::new(budget.reserve(bytes * 16 + count * 256)?);
        budget.charge(2, 0, count as u64 * 4, bytes as u64)?;
        let rows = tx.query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!("SELECT k.table_name,k.sort_key,k.row_key,{position} AS position {source} ORDER BY {order} LIMIT {count}"),
            values,
        )).await.map_err(|_| ManagementError::Database)?;
        if rows.len() != count {
            return Err(ManagementError::Changed);
        }
        tx.commit().await.map_err(|_| ManagementError::Database)?;
        let mut copied = 0usize;
        for row in rows {
            let raw: String = get(&row, "row_key")?;
            let order: Vec<u8> = get(&row, "sort_key")?;
            let table: String = get(&row, "table_name")?;
            copied = copied
                .checked_add(raw.len() + order.len() + table.len())
                .ok_or(ManagementError::Budget)?;
            if copied > bytes {
                return Err(ManagementError::Changed);
            }
            let native: NativeKey = decode(&raw)?;
            let layout = native::current_catalog()
                .iter()
                .find(|layout| layout.table == native.table)
                .ok_or(ManagementError::InvalidEvidence)?;
            if native.table != table
                || native.values.len() != layout.primary_key.len()
                || wire_key(&native) != order
            {
                return Err(ManagementError::InvalidEvidence);
            }
            self.rows.push_back(Key {
                native,
                order,
                position: get(&row, "position")?,
                initial: None,
                memory: memory.clone(),
            });
        }
        Ok(())
    }
}

pub(super) struct Part {
    pub length: Option<u64>,
    pub bytes: Vec<u8>,
    pub memory: Arc<cells::Memory>,
}
pub(super) async fn part(
    spool: &InventorySpool,
    key: &NativeKey,
    column: &str,
    offset: u64,
    length: Option<Option<u64>>,
    budget: &mut Budget,
) -> Result<Part> {
    cells::part(&spool.db, key, column, offset, length, budget).await
}
pub(super) struct Cells {
    first: BTreeMap<String, Option<Part>>,
    _memory: Arc<cells::Memory>,
}
impl Cells {
    #[cfg(test)]
    pub async fn read(
        spool: &InventorySpool,
        key: &NativeKey,
        columns: &[String],
        budget: &mut Budget,
    ) -> Result<Self> {
        cells::read(&spool.db, &[cells::Candidate { key, columns }], budget)
            .await?
            .pop()
            .ok_or(ManagementError::InvalidEvidence)
    }
    pub fn take(&mut self, column: &str) -> Result<Option<Part>> {
        self.first
            .remove(column)
            .ok_or(ManagementError::InvalidEvidence)
    }
    pub fn peek(&self, column: &str) -> Result<Option<&Part>> {
        self.first
            .get(column)
            .map(Option::as_ref)
            .ok_or(ManagementError::InvalidEvidence)
    }
}

#[cfg(test)]
mod native_tests;

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) async fn insert(
        spool: &InventorySpool,
        key: &NativeKey,
        column: &str,
        value: Option<&[u8]>,
    ) {
        let bytes = value.unwrap_or_default().to_vec();
        let part = ArchivePart {
            table: key.table.clone(),
            key: key.clone(),
            column: column.into(),
            offset: 0,
            length: value.map(|value| value.len() as u64),
            bytes,
        };
        let checksum = part.checksum().unwrap();
        exec(&spool.db,
            "INSERT INTO parts(table_name,row_key,column_name,offset,length,bytes,checksum) VALUES($1,$2,$3,0,$4,$5,$6)",
            vec![key.table.clone().into(), encode(key).unwrap().into(), column.into(),
                part.length.map(|n| n as i64).into(), part.bytes.into(), checksum.into()],
        ).await.unwrap();
    }

    #[tokio::test]
    async fn original_cell_prefetch_batches_keys_and_preserves_null_empty_unicode() {
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
        let columns = vec!["empty".into(), "null".into(), "text".into()];
        let keys = (0..65)
            .map(|n| NativeKey {
                table: "originals".into(),
                values: vec![format!("{n:03}")],
            })
            .collect::<Vec<_>>();
        for key in &keys {
            insert(&spool, key, "empty", Some(b"")).await;
            insert(&spool, key, "null", None).await;
            insert(
                &spool,
                key,
                "text",
                Some(format!("原文-é-{}", key.values[0]).as_bytes()),
            )
            .await;
        }
        let mut budget = Budget::default();
        let mut raw = 0;
        for (batch, chunk) in keys.chunks(64).enumerate() {
            let candidates = chunk
                .iter()
                .map(|key| cells::Candidate {
                    key,
                    columns: &columns,
                })
                .collect::<Vec<_>>();
            let initial = cells::read(&spool.db, &candidates, &mut budget)
                .await
                .unwrap();
            assert_eq!(initial.len(), chunk.len());
            assert_eq!(budget.work.sql, (batch as u64 + 1) * 2);
            for (key, mut cells) in chunk.iter().zip(initial) {
                let empty = cells.take("empty").unwrap().unwrap();
                assert_eq!(empty.length, Some(0));
                assert!(empty.bytes.is_empty());
                let null = cells.take("null").unwrap().unwrap();
                assert_eq!(null.length, None);
                assert!(null.bytes.is_empty());
                let text = cells.take("text").unwrap().unwrap();
                let expected = format!("原文-é-{}", key.values[0]);
                assert_eq!(text.bytes, expected.as_bytes());
                raw += expected.len() as u64;
            }
        }
        assert_eq!(budget.work.raw_bytes, raw);
        assert_eq!(budget.work.rows, 0);
        exec(&spool.db, "UPDATE parts SET checksum=(SELECT checksum FROM parts e WHERE e.table_name=parts.table_name AND e.row_key=parts.row_key AND e.column_name='empty') WHERE column_name='null'", vec![]).await.unwrap();
        let mut budget = Budget::default();
        assert!(matches!(
            part(&spool, &keys[0], "null", 0, None, &mut budget).await,
            Err(ManagementError::Changed)
        ));
        assert_eq!(budget.work.raw_bytes, 0);
        assert_eq!(budget.work.sql, 2);
        spool.close().await.unwrap();
        workspace.cleanup().unwrap();
    }

    #[tokio::test]
    async fn original_cell_prefetch_caps_the_whole_batch_not_each_key() {
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
        let columns = vec!["value".into()];
        let keys = (0..17)
            .map(|n| NativeKey {
                table: "originals".into(),
                values: vec![n.to_string()],
            })
            .collect::<Vec<_>>();
        for (n, key) in keys.iter().enumerate() {
            insert(
                &spool,
                key,
                "value",
                Some(&vec![b'a' + n as u8; CELL_SLICE]),
            )
            .await;
        }
        let candidates = keys
            .iter()
            .map(|key| cells::Candidate {
                key,
                columns: &columns,
            })
            .collect::<Vec<_>>();
        let mut budget = Budget::default();
        let initial = cells::read(&spool.db, &candidates, &mut budget)
            .await
            .unwrap();
        assert_eq!(budget.work.raw_bytes, PAGE_BYTES as u64);
        assert_eq!(initial.len(), PAGE_BYTES / CELL_SLICE);
        for (n, mut cells) in initial.into_iter().enumerate() {
            assert_eq!(
                cells.take("value").unwrap().unwrap().bytes,
                vec![b'a' + n as u8; CELL_SLICE]
            );
        }
        let mut last = cells::read(&spool.db, &candidates[16..], &mut budget)
            .await
            .unwrap();
        assert_eq!(last.len(), 1);
        assert_eq!(
            last[0].take("value").unwrap().unwrap().bytes,
            vec![b'q'; CELL_SLICE]
        );
        assert_eq!(budget.work.raw_bytes, (PAGE_BYTES + CELL_SLICE) as u64);
        assert_eq!(budget.work.sql, 4);
        drop(last);
        let mut capped = Budget::default();
        capped
            .restrict_remaining(256, (CELL_SLICE - 1) as u64, capped.remaining().unwrap())
            .unwrap();
        assert!(matches!(
            cells::read(&spool.db, &candidates, &mut capped).await,
            Err(ManagementError::Budget)
        ));
        assert_eq!(capped.work.sql, 1);
        assert_eq!(capped.work.raw_bytes, 0);
        spool.close().await.unwrap();
        workspace.cleanup().unwrap();
    }

    #[tokio::test]
    async fn malformed_metadata_is_bounded_and_checksum_failure_keeps_batch_debt() {
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
        let key = NativeKey {
            table: "originals".into(),
            values: vec!["key".into()],
        };
        let columns = vec!["a".into(), "b".into()];
        insert(&spool, &key, "a", Some(b"first")).await;
        insert(&spool, &key, "b", Some(b"second")).await;
        for (field, bad) in [
            ("checksum", Value::from("x".repeat(PAGE_BYTES * 2))),
            ("length", Value::from(vec![b'x'; PAGE_BYTES * 2])),
            ("length", Value::from("invalid".repeat(PAGE_BYTES / 4))),
        ] {
            exec(
                &spool.db,
                &format!("UPDATE parts SET {field}=$1 WHERE column_name='a'"),
                vec![bad],
            )
            .await
            .unwrap();
            let mut budget = Budget::default();
            assert!(matches!(
                Cells::read(&spool, &key, &columns, &mut budget).await,
                Err(ManagementError::InvalidEvidence)
            ));
            assert_eq!(budget.work.raw_bytes, 0);
            assert_eq!(budget.work.sql, 1);
            assert!(budget.peak_resident() < PAGE_BYTES);
            exec(&spool.db, "DELETE FROM parts WHERE column_name='a'", vec![])
                .await
                .unwrap();
            insert(&spool, &key, "a", Some(b"first")).await;
        }
        exec(
            &spool.db,
            "UPDATE parts SET checksum=$1 WHERE column_name='a'",
            vec!["0".repeat(64).into()],
        )
        .await
        .unwrap();
        let mut budget = Budget::default();
        assert!(matches!(
            Cells::read(&spool, &key, &columns, &mut budget).await,
            Err(ManagementError::Changed)
        ));
        assert_eq!(budget.work.raw_bytes, 11);
        assert_eq!(budget.work.sql, 2);
        // The same bounded metadata validation is used after skipped cache reload.
        exec(
            &spool.db,
            "UPDATE parts SET length=$1 WHERE column_name='a'",
            vec![vec![0u8; PAGE_BYTES * 2].into()],
        )
        .await
        .unwrap();
        let mut fallback = Budget::default();
        assert!(matches!(
            part(&spool, &key, "a", 0, None, &mut fallback).await,
            Err(ManagementError::InvalidEvidence)
        ));
        assert_eq!(fallback.work.raw_bytes, 0);
        assert_eq!(fallback.work.sql, 1);
        spool.close().await.unwrap();
        workspace.cleanup().unwrap();
    }

    #[tokio::test]
    async fn fallback_prepays_known_and_unknown_length_before_payload() {
        use crate::store::pinning::management::native::meter::Limits;
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
        let key = NativeKey {
            table: "originals".into(),
            values: vec!["key".into()],
        };
        insert(&spool, &key, "value", Some(b"payload")).await;
        exec(
            &spool.db,
            "UPDATE parts SET checksum=$1",
            vec!["0".repeat(64).into()],
        )
        .await
        .unwrap();
        for length in [Some(Some(7)), None] {
            let mut budget = Budget::new(Limits {
                bytes: 6,
                ..Limits::default()
            });
            assert!(matches!(
                part(&spool, &key, "value", 0, length, &mut budget).await,
                Err(ManagementError::Budget)
            ));
            assert_eq!(budget.work.raw_bytes, 7);
            assert_eq!(budget.work.sql, if length.is_some() { 1 } else { 2 });
        }
        let mut budget = Budget::default();
        assert!(matches!(
            part(&spool, &key, "value", 0, Some(Some(7)), &mut budget).await,
            Err(ManagementError::Changed)
        ));
        assert_eq!(budget.work.raw_bytes, 7);
        spool.close().await.unwrap();
        workspace.cleanup().unwrap();
    }

    #[tokio::test]
    async fn original_cell_prefetch_never_exceeds_one_payload_page() {
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
        let key = NativeKey {
            table: "source_cells".into(),
            values: vec!["original".into()],
        };
        let columns = (0..=PAGE_BYTES / CELL_SLICE)
            .map(|n| format!("cell_{n:03}"))
            .collect::<Vec<_>>();
        for (n, column) in columns.iter().enumerate() {
            exec(
                &spool.db,
                "INSERT INTO parts VALUES($1,$2,$3,$4,0,$5,$6,$7)",
                vec![
                    (n as i64).into(),
                    key.table.clone().into(),
                    encode(&key).unwrap().into(),
                    column.clone().into(),
                    (CELL_SLICE as i64).into(),
                    vec![b'x'; CELL_SLICE].into(),
                    ArchivePart {
                        table: key.table.clone(),
                        key: key.clone(),
                        column: column.clone(),
                        offset: 0,
                        length: Some(CELL_SLICE as u64),
                        bytes: vec![b'x'; CELL_SLICE],
                    }
                    .checksum()
                    .unwrap()
                    .into(),
                ],
            )
            .await
            .unwrap();
        }
        let mut budget = Budget::default();
        let read = async {
            let initial = Cells::read(&spool, &key, &columns, &mut budget).await?;
            let prefetched = budget.work.raw_bytes;
            let mut cursor = RetirementArchiveCursor {
                section: 3,
                row: Some(Row {
                    columns: columns.clone(),
                    source: Source::Native(key),
                    column: 0,
                    offset: 0,
                    opened: false,
                    length: None,
                    initial: Some(initial),
                    key_memory: None,
                    part_memory: None,
                }),
                ..Default::default()
            };
            let mut streamed = 0usize;
            loop {
                match cursor.next(&spool, &mut budget, 256).await? {
                    Some(Event::Chunk(bytes)) => {
                        if bytes.iter().any(|byte| *byte != b'x') {
                            return Err(ManagementError::InvalidEvidence);
                        }
                        streamed += bytes.len();
                    }
                    Some(Event::Frame(Frame::RowEnd)) => break,
                    Some(Event::Frame(_)) => {}
                    None => return Err(ManagementError::InvalidEvidence),
                }
            }
            Ok((prefetched, streamed, budget.work.raw_bytes))
        }
        .await;
        spool.close().await.unwrap();
        workspace.cleanup().unwrap();
        let (prefetched, streamed, charged) = read.unwrap();
        assert!(
            prefetched <= PAGE_BYTES as u64,
            "prefetch returned {prefetched} raw bytes"
        );
        assert_eq!(prefetched, PAGE_BYTES as u64);
        assert_eq!(streamed, columns.len() * CELL_SLICE);
        assert_eq!(charged, streamed as u64);
    }
}
