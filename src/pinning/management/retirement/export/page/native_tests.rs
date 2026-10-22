use super::*;
use crate::pinning::management::wire::v3::{self, Projection};
use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

struct StopAtNativeChunk {
    bytes: Vec<u8>,
    clock: Arc<Mutex<Instant>>,
}
impl Write for StopAtNativeChunk {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        if bytes.len() == CELL_SLICE {
            *self.clock.lock().unwrap() = Instant::now() + Duration::from_secs(30);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn prefix<W: Write>(writer: &mut Writer<W>, projection: Projection) {
    writer
        .frame(&Frame::Header {
            schema_version: v3::SCHEMA_VERSION,
            projection,
            applied_trust: v3::AppliedTrust::UntrustedClaimsOnly,
            database_scope: "scope".into(),
            database_identity: "identity".into(),
            principal: "operator".into(),
            catalog: vec!["fixture".into()],
            created_at: "2026-01-01T00:00:00Z".into(),
            expires_at: None,
        })
        .unwrap();
    writer.frame(&Frame::Request { request: None }).unwrap();
    writer
        .frame(&Frame::Configuration {
            configuration: None,
        })
        .unwrap();
    writer
        .frame(&Frame::Acknowledgements { item_ids: None })
        .unwrap();
    writer
        .frame(&Frame::Summary {
            summary: Default::default(),
        })
        .unwrap();
}

#[tokio::test]
async fn complete_initial_requires_page_room_and_never_compacts_review_private_cells() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    for (index, (projection, column, value, eligible)) in [
        (
            Projection::PrivateRecovery,
            "cid",
            Some(b"short".as_slice()),
            true,
        ),
        (
            Projection::PrivateRecovery,
            "cid",
            Some(b"".as_slice()),
            true,
        ),
        (Projection::PrivateRecovery, "cid", None, true),
        (Projection::Review, "cid", Some(b"short".as_slice()), true),
        (
            Projection::Review,
            "metadata",
            Some(b"short".as_slice()),
            false,
        ),
        (Projection::Review, "metadata", Some(b"".as_slice()), false),
        (Projection::Review, "metadata", None, false),
    ]
    .into_iter()
    .enumerate()
    {
        let key = NativeKey {
            table: "objects".into(),
            values: vec![index.to_string()],
        };
        super::tests::insert(&spool, &key, column, value).await;
        let columns = vec![column.to_owned()];
        let mut budget = Budget::default();
        let initial = Cells::read(&spool, &key, &columns, &mut budget)
            .await
            .unwrap();
        let mut writer = Writer::new(Vec::new(), projection, budget).unwrap();
        prefix(&mut writer, projection);
        let mut cursor = RetirementArchiveCursor {
            section: 3,
            ..Default::default()
        };
        let row = cursor.begin(Section::Native, key.clone(), columns, Source::Native(key));
        let Event::Frame(row) = row else {
            panic!("row")
        };
        writer.frame(&row).unwrap();
        cursor.row.as_mut().unwrap().initial = Some(initial);
        let before = writer.budget().work;
        let bytes = value.unwrap_or_default();
        let physical = (8 + column.len() + bytes.len()) as u64;
        let events = if bytes.is_empty() { 2 } else { 3 };
        assert_eq!(
            cursor
                .complete_initial(&mut writer, physical - 1, events)
                .unwrap(),
            None
        );
        assert_eq!(
            cursor
                .complete_initial(&mut writer, physical, events - 1)
                .unwrap(),
            None
        );
        assert!(
            cursor
                .row
                .as_ref()
                .unwrap()
                .initial
                .as_ref()
                .unwrap()
                .peek(column)
                .unwrap()
                .is_some()
        );
        assert_eq!(cursor.row.as_ref().unwrap().column, 0);
        let result = cursor
            .complete_initial(&mut writer, physical, events)
            .unwrap();
        assert_eq!(result, eligible.then_some(events));
        assert_eq!(writer.budget().work.sql, before.sql);
        assert_eq!(
            writer.budget().work.raw_bytes - before.raw_bytes,
            if eligible { physical } else { 0 }
        );
        if eligible {
            assert_eq!(cursor.row.as_ref().unwrap().column, 1);
            assert_eq!(writer.budget().work.cells - before.cells, 1);
            assert_eq!(
                writer.budget().work.output_bytes - before.output_bytes,
                physical
            );
        } else {
            assert_eq!(cursor.row.as_ref().unwrap().column, 0);
        }
    }
    spool.close().await.unwrap();
    workspace.cleanup().unwrap();
}

async fn native_roundtrip(projection: Projection, reopen: bool) -> (Vec<u8>, u64, u64, u64) {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let mut spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    spool.reference.archive_id = "archive".into();
    spool.reference.inventory_id = "inventory".into();
    exec(
        &spool.db,
        "UPDATE state SET reference=$1 WHERE id=1",
        vec![encode(spool.reference()).unwrap().into()],
    )
    .await
    .unwrap();
    let layout = native::current_catalog()
        .iter()
        .find(|layout| layout.table == "objects")
        .unwrap();
    let long_column = layout
        .columns
        .iter()
        .find(|column| !native::confidential(&layout.table, column))
        .unwrap();
    let short_column = layout
        .columns
        .iter()
        .find(|column| *column > long_column && !native::confidential(&layout.table, column))
        .unwrap();
    let short_payload = "短文-é\0尾".as_bytes();
    // The first SQL slice ends inside a UTF-8 scalar; continuation must retain
    // the writer's UTF-8 state as well as the exact source offset.
    let payload = format!("{}é尾", "x".repeat(CELL_SLICE - 1)).into_bytes();
    let mut second_key_bytes = 0;
    for index in 0..2 {
        let key = NativeKey {
            table: layout.table.clone(),
            values: vec![index.to_string(); layout.primary_key.len()],
        };
        let raw = encode(&key).unwrap();
        let order = wire_key(&key);
        if index == 1 {
            second_key_bytes = raw.len() + order.len() + key.table.len();
        }
        exec(
            &spool.db,
            "INSERT INTO wire_keys VALUES($1,$2,$3)",
            vec![key.table.clone().into(), order.into(), raw.clone().into()],
        )
        .await
        .unwrap();
        for (column_index, column) in layout.columns.iter().enumerate() {
            let value = if column == long_column {
                Some(payload.as_slice())
            } else if column == short_column {
                Some(short_payload)
            } else if column == "metadata" || column_index % 2 == 0 {
                Some(b"".as_slice())
            } else {
                None
            };
            let bytes = value.unwrap_or_default();
            let length = value.map(|value| value.len() as u64);
            let chunks = bytes.chunks(CELL_SLICE).collect::<Vec<_>>();
            let chunks = if chunks.is_empty() {
                vec![b"".as_slice()]
            } else {
                chunks
            };
            for (index, bytes) in chunks.into_iter().enumerate() {
                let part = ArchivePart {
                    table: key.table.clone(),
                    key: key.clone(),
                    column: column.clone(),
                    offset: (index * CELL_SLICE) as u64,
                    length,
                    bytes: bytes.to_vec(),
                };
                let checksum = part.checksum().unwrap();
                exec(&spool.db,
                    "INSERT INTO parts(table_name,row_key,column_name,offset,length,bytes,checksum) VALUES($1,$2,$3,$4,$5,$6,$7)",
                    vec![key.table.clone().into(), raw.clone().into(), column.clone().into(),
                        (part.offset as i64).into(), length.map(|n| n as i64).into(),
                        part.bytes.into(), checksum.into()],
                ).await.unwrap();
            }
        }
    }
    for table in native::current_catalog() {
        exec(
            &spool.db,
            "INSERT INTO eof(table_name) VALUES($1)",
            vec![table.table.clone().into()],
        )
        .await
        .unwrap();
    }
    spool
        .reduced_here
        .store(true, std::sync::atomic::Ordering::Release);
    let clock = Arc::new(Mutex::new(Instant::now()));
    let mut limits = RunLimits::default().for_production();
    limits.production_clock = Some(clock.clone());
    let mut writer = Writer::new(
        StopAtNativeChunk {
            bytes: Vec::new(),
            clock,
        },
        projection,
        Budget::default(),
    )
    .unwrap();
    prefix(&mut writer, projection);
    let initial_raw = writer.budget().work.raw_bytes;
    let initial_output = writer.budget().work.output_bytes;
    let mut export = ArchiveExport {
        reference: spool.reference().clone(),
        cursor: RetirementArchiveCursor::default(),
        complete: false,
        planning_facts_written: true,
    };
    assert_eq!(
        export
            .advance_planning_page(&spool, &mut writer, &Snapshot::default(), &limits)
            .await
            .unwrap(),
        Progress::Pending
    );
    assert!(!limits.production_available());
    assert_eq!(
        export.cursor.row.as_ref().unwrap().offset,
        CELL_SLICE as u64
    );
    assert_eq!(
        export.cursor.after,
        wire_key(&NativeKey {
            table: layout.table.clone(),
            values: vec!["0".into(); layout.primary_key.len()],
        })
    );
    assert_eq!(export.cursor.keys.rows.len(), 1);
    assert!(export.cursor.keys.rows.front().unwrap().initial.is_some());
    let before = spool.task_work().await.unwrap();
    assert_eq!(before.bytes, writer.budget().work.raw_bytes - initial_raw);
    assert!(before.bytes >= (3 * CELL_SLICE) as u64);
    assert_eq!(before.rows, 1);
    let (paused, budget) = writer.pause();
    let (sink, mut continuation) = paused.unwrap();
    if reopen {
        let encoded = encode(&export).unwrap();
        continuation = decode(&encode(&continuation).unwrap()).unwrap();
        drop(export);
        spool.close().await.unwrap();
        spool = InventorySpool::resume(&path).await.unwrap();
        spool
            .reduced_here
            .store(true, std::sync::atomic::Ordering::Release);
        export = decode(&encoded).unwrap();
        assert!(export.cursor.keys.rows.is_empty());
        assert!(export.cursor.row.as_ref().unwrap().initial.is_none());
    }
    // The same Budget is returned by pause: cache guards keep its resident Arc
    // live in the in-memory case, without charging source IO again.
    let mut writer = Writer::continue_metered(sink.bytes, continuation, budget)
        .map_err(|(error, _)| error)
        .unwrap();
    while export
        .advance_planning_page(
            &spool,
            &mut writer,
            &Snapshot::default(),
            &RunLimits::default(),
        )
        .await
        .unwrap()
        != Progress::Archived
    {}
    let after = spool.task_work().await.unwrap();
    assert_eq!(after.bytes, writer.budget().work.raw_bytes - initial_raw);
    assert!(after.pages > before.pages);
    let physical_output = writer.budget().work.output_bytes - initial_output;
    let bytes = writer.finish().unwrap();
    let mut reader = v3::Reader::new(bytes.as_slice(), projection, Budget::default()).unwrap();
    let mut cell_payload = Vec::new();
    let mut cell_column = String::new();
    let mut native_cells = 0;
    let mut nulls = 0;
    let mut empties = 0;
    let mut redacted = 0;
    let mut in_native = false;
    let mut native_rows = 0;
    let mut eof_rows = 0;
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            Event::Frame(Frame::Row { section, key, .. }) => {
                in_native = section == Section::Native;
                if in_native {
                    assert_eq!(key.table, layout.table);
                    assert_eq!(
                        key.values,
                        vec![native_rows.to_string(); layout.primary_key.len()]
                    );
                } else if section == Section::Membership {
                    assert_eq!(key.values.last().unwrap(), "eof");
                }
                native_rows += usize::from(in_native);
                eof_rows += usize::from(section == Section::Membership);
            }
            Event::Frame(Frame::Cell { column }) if in_native => {
                assert!(cell_payload.is_empty());
                assert_eq!(column, layout.columns[native_cells % layout.columns.len()]);
                cell_column = column;
            }
            Event::Chunk(bytes) if in_native => cell_payload.extend(bytes),
            Event::Frame(Frame::CellEnd { value }) if in_native => {
                let index = native_cells % layout.columns.len();
                let expected = if &cell_column == long_column {
                    Some(payload.as_slice())
                } else if &cell_column == short_column {
                    Some(short_payload)
                } else if cell_column == "metadata" || index % 2 == 0 {
                    Some(b"".as_slice())
                } else {
                    None
                };
                if projection == Projection::Review
                    && native::confidential(&layout.table, &cell_column)
                    && expected.is_some()
                {
                    assert!(value == CellValue::Redacted);
                    assert!(cell_payload.is_empty());
                    redacted += 1;
                } else {
                    assert!(
                        value
                            == if expected.is_some() {
                                CellValue::Text
                            } else {
                                CellValue::Null
                            }
                    );
                    assert_eq!(cell_payload, expected.unwrap_or_default());
                    nulls += usize::from(expected.is_none());
                    empties += usize::from(expected == Some(b"".as_slice()));
                }
                cell_payload.clear();
                native_cells += 1;
            }
            _ => {}
        }
    }
    assert!(reader.verified());
    assert_eq!(native_rows, 2);
    assert_eq!(eof_rows, native::current_catalog().len());
    assert_eq!(native_cells, layout.columns.len() * 2);
    assert!(nulls > 0 && empties > 0);
    if projection == Projection::Review {
        assert!(redacted > 0);
    }
    spool.close().await.unwrap();
    workspace.cleanup().unwrap();
    (
        bytes,
        after.bytes,
        (CELL_SLICE + second_key_bytes + 2 * short_payload.len()) as u64,
        physical_output,
    )
}

#[tokio::test]
async fn native_batch_soft_stop_reopen_preserves_logical_originals_and_read_debt() {
    for projection in [Projection::PrivateRecovery, Projection::Review] {
        let (_, retained_debt, _, retained_output) = native_roundtrip(projection, false).await;
        let (_, reopened_debt, reread, reopened_output) = native_roundtrip(projection, true).await;
        assert_eq!(
            reopened_debt as i128 - retained_debt as i128,
            reread as i128 + reopened_output as i128 - retained_output as i128,
        );
    }
}

#[tokio::test]
async fn native_batch_checksum_error_settles_prepaid_debt_before_return() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    let layout = &native::current_catalog()[0];
    let mut key_bytes = 0;
    for index in 0..2 {
        let key = NativeKey {
            table: layout.table.clone(),
            values: vec![index.to_string(); layout.primary_key.len()],
        };
        let raw = encode(&key).unwrap();
        let order = wire_key(&key);
        key_bytes += raw.len() + order.len() + key.table.len();
        exec(
            &spool.db,
            "INSERT INTO wire_keys VALUES($1,$2,$3)",
            vec![key.table.clone().into(), order.into(), raw.into()],
        )
        .await
        .unwrap();
        for column in &layout.columns {
            super::tests::insert(&spool, &key, column, Some(b"paid")).await;
        }
    }
    // A one-row remainder must not load or prefetch the following key. Moving
    // the last queued key into Row keeps both ownership guards until RowEnd.
    let mut cursor = RetirementArchiveCursor::default();
    let mut budget = Budget::default();
    assert!(matches!(
        cursor.next(&spool, &mut budget, 1).await.unwrap(),
        Some(Event::Frame(Frame::Row { .. }))
    ));
    assert!(cursor.keys.rows.is_empty());
    assert_eq!(
        budget.work.raw_bytes,
        (key_bytes / 2 + layout.columns.len() * 4) as u64
    );
    let row = cursor.row.as_ref().unwrap();
    let key_memory = Arc::downgrade(row.key_memory.as_ref().unwrap());
    let cell_memory = Arc::downgrade(&row.initial.as_ref().unwrap()._memory);
    loop {
        assert!(key_memory.upgrade().is_some());
        assert!(cell_memory.upgrade().is_some());
        if matches!(
            cursor.next(&spool, &mut budget, 1).await.unwrap(),
            Some(Event::Frame(Frame::RowEnd))
        ) {
            break;
        }
    }
    assert!(key_memory.upgrade().is_none());
    assert!(cell_memory.upgrade().is_none());
    drop(cursor);
    exec(
        &spool.db,
        "UPDATE parts SET checksum=$1 WHERE column_name=$2",
        vec!["0".repeat(64).into(), layout.columns[0].clone().into()],
    )
    .await
    .unwrap();
    spool
        .reduced_here
        .store(true, std::sync::atomic::Ordering::Release);
    let mut writer = Writer::new(
        StopAtNativeChunk {
            bytes: Vec::new(),
            clock: Arc::new(Mutex::new(Instant::now())),
        },
        Projection::PrivateRecovery,
        Budget::default(),
    )
    .unwrap();
    prefix(&mut writer, Projection::PrivateRecovery);
    let initial_raw = writer.budget().work.raw_bytes;
    let initial_output = writer.budget().work.output_bytes;
    let mut export = ArchiveExport {
        reference: spool.reference().clone(),
        cursor: RetirementArchiveCursor::default(),
        complete: false,
        planning_facts_written: true,
    };
    assert!(matches!(
        export
            .advance_page(&spool, &mut writer, &RunLimits::default())
            .await,
        Err(ManagementError::Changed)
    ));
    let paid = (key_bytes + layout.columns.len() * 4 * 2) as u64;
    assert_eq!(writer.budget().work.raw_bytes - initial_raw, paid);
    assert_eq!(writer.budget().work.output_bytes, initial_output);
    let work = spool.task_work().await.unwrap();
    assert_eq!(work.bytes, paid);
    assert_eq!(work.rows, 0);
    assert_eq!(export.cursor.after, Vec::<u8>::new());
    spool.close().await.unwrap();
    workspace.cleanup().unwrap();
}

struct FailOnShortCell {
    failed: bool,
}
impl Write for FailOnShortCell {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.failed {
            return Err(std::io::Error::other("output unavailable"));
        }
        if bytes.len() == 5 && bytes[4] == 7 {
            self.failed = true;
            return Ok(3);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn native_short_write_failure_retains_cursor_and_settles_page_debt() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    let layout = &native::current_catalog()[0];
    let key = NativeKey {
        table: layout.table.clone(),
        values: vec!["key".into(); layout.primary_key.len()],
    };
    let raw = encode(&key).unwrap();
    let order = wire_key(&key);
    let source_bytes = raw.len() + order.len() + key.table.len() + 4 * layout.columns.len();
    exec(
        &spool.db,
        "INSERT INTO wire_keys VALUES($1,$2,$3)",
        vec![key.table.clone().into(), order.into(), raw.into()],
    )
    .await
    .unwrap();
    for column in &layout.columns {
        super::tests::insert(&spool, &key, column, Some(b"paid")).await;
    }
    spool
        .reduced_here
        .store(true, std::sync::atomic::Ordering::Release);
    let mut writer = Writer::new(
        FailOnShortCell { failed: false },
        Projection::PrivateRecovery,
        Budget::default(),
    )
    .unwrap();
    prefix(&mut writer, Projection::PrivateRecovery);
    let before = writer.budget().work;
    let row_bytes = 5 + serde_json::to_vec(&Frame::Row {
        section: Section::Native,
        key,
        cells: layout.columns.len() as u32,
    })
    .unwrap()
    .len();
    let physical = 8 + layout.columns[0].len() + 4;
    let mut export = ArchiveExport {
        reference: spool.reference().clone(),
        cursor: RetirementArchiveCursor::default(),
        complete: false,
        planning_facts_written: true,
    };
    assert!(
        export
            .advance_page(&spool, &mut writer, &RunLimits::default())
            .await
            .is_err()
    );
    let row = export.cursor.row.as_ref().unwrap();
    assert_eq!(row.column, 0);
    assert_eq!(row.offset, 0);
    assert!(!row.opened);
    assert_eq!(
        writer.budget().work.raw_bytes - before.raw_bytes,
        (source_bytes + row_bytes + physical) as u64
    );
    assert_eq!(
        writer.budget().work.output_bytes - before.output_bytes,
        row_bytes as u64 + 3
    );
    let paid = spool.task_work().await.unwrap();
    assert_eq!(
        paid.bytes,
        writer.budget().work.raw_bytes - before.raw_bytes
    );
    assert_eq!(paid.rows, 1);
    assert!(writer.finish().is_err());
    drop(export);
    spool.close().await.unwrap();
    workspace.cleanup().unwrap();
}
