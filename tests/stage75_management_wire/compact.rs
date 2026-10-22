use super::*;
use std::{cell::Cell, rc::Rc};

#[test]
fn compact_records_have_canonical_tags_and_roundtrip_events() {
    let bytes = archive(b"hello");
    let mut offset = MAGIC.len();
    let mut tags = Vec::new();
    while offset < bytes.len() {
        let length = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let kind = bytes[offset + 4];
        let payload = &bytes[offset + 5..offset + 4 + length];
        if kind == 2 {
            assert!(payload == b"metadata" || payload == b"size");
        }
        if (3..=6).contains(&kind) {
            assert!(payload.is_empty());
        }
        tags.push(kind);
        offset += length + 4;
    }
    assert_eq!(tags, [0, 0, 0, 0, 0, 0, 2, 5, 2, 1, 4, 6, 0]);
    assert!(read_all(&bytes, Projection::Review).is_ok());
}

struct ShortInput {
    input: Cursor<Vec<u8>>,
    read: Rc<Cell<usize>>,
    error: bool,
    cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}
impl Read for ShortInput {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.input.position() as usize == self.input.get_ref().len() && self.error {
            return Err(io::Error::other("read failed"));
        }
        let count = out.len().min(2);
        let n = self.input.read(&mut out[..count])?;
        self.read.set(self.read.get() + n);
        if let Some(cancel) = &self.cancel
            && self.read.get() > MAGIC.len()
        {
            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(n)
    }
}

#[test]
fn failed_headers_retain_prepaid_work_without_reading_payload() {
    let mut bad_length = Vec::from(0u32.to_be_bytes());
    let mut unknown = Vec::from(50u32.to_be_bytes());
    unknown.push(255);
    let mut bad_cell = Vec::from(258u32.to_be_bytes());
    bad_cell.push(2);
    let mut cases = vec![
        (vec![0, 0], 4),
        (vec![0, 0, 0, 1], 5),
        (std::mem::take(&mut bad_length), 4),
        (Vec::from(((FRAME_BYTES + 2) as u32).to_be_bytes()), 4),
        (unknown, 5),
        (bad_cell, 5),
    ];
    for (kind, length) in [
        (1, 1),
        (1, CELL_BYTES + 2),
        (2, 1),
        (3, 2),
        (4, 2),
        (5, 2),
        (6, 2),
    ] {
        let mut tail = Vec::from((length as u32).to_be_bytes());
        tail.push(kind);
        cases.push((tail, 5));
    }
    for (tail, prepaid) in cases {
        for error in [false, true] {
            let read = Rc::new(Cell::new(0));
            let mut bytes = MAGIC.to_vec();
            bytes.extend(&tail);
            let mut reader = Reader::new(
                ShortInput {
                    input: Cursor::new(bytes),
                    read: read.clone(),
                    error,
                    cancel: None,
                },
                Projection::Review,
                budget(),
            )
            .unwrap();
            assert!(reader.next_frame().is_err());
            assert_eq!(read.get(), MAGIC.len() + tail.len());
            assert_eq!(reader.budget().work.raw_bytes, MAGIC.len() as u64 + prepaid);
            assert_eq!(reader.budget().work.cells, 1);
            assert!(!reader.verified());
        }
    }
}

#[test]
fn cancellation_during_header_read_does_not_erase_read_debt() {
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&10u32.to_be_bytes());
    let work = budget();
    let read = Rc::new(Cell::new(0));
    let mut reader = Reader::new(
        ShortInput {
            input: Cursor::new(bytes),
            read: read.clone(),
            error: true,
            cancel: Some(work.cancellation()),
        },
        Projection::Review,
        work,
    )
    .unwrap();
    assert!(reader.next_frame().is_err());
    assert_eq!(reader.budget().work.raw_bytes, MAGIC.len() as u64 + 4);
    assert_eq!(read.get(), MAGIC.len() + 2);
    assert!(reader.budget().check().is_err());
}

struct ShortOutput {
    remaining: Rc<Cell<usize>>,
    written: Rc<Cell<u64>>,
}
impl Write for ShortOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = bytes.len().min(self.remaining.get()).min(17);
        if n == 0 {
            return Err(io::Error::other("write failed"));
        }
        self.remaining.set(self.remaining.get() - n);
        self.written.set(self.written.get() + n as u64);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn native_and_plain_cell_failures_count_actual_partial_output_and_framing() {
    use ipfs_s3_gateway::store::pinning::management::native::cells;
    use sea_orm::{ConnectionTrait, Database, DbBackend, Statement, TransactionTrait};
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("CREATE TABLE objects(id TEXT PRIMARY KEY, cid TEXT, metadata TEXT)")
        .await
        .unwrap();
    let text = "x".repeat(CELL_BYTES * 2);
    db.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO objects VALUES ('a',$1,$1)",
        [text.into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    for capacity in [100, CELL_BYTES + 100, 1, 10] {
        let remaining = Rc::new(Cell::new(usize::MAX));
        let written = Rc::new(Cell::new(0));
        let mut writer = Writer::new(
            ShortOutput {
                remaining: remaining.clone(),
                written: written.clone(),
            },
            Projection::Review,
            budget(),
        )
        .unwrap();
        prefix(&mut writer);
        writer
            .frame(&Frame::Row {
                section: Section::Native,
                key: key("a"),
                cells: 1,
            })
            .unwrap();
        let before = writer.budget().work;
        remaining.set(capacity);
        assert!(writer.native_cell(&tx, &key("a"), "cid").await.is_err());
        let work = writer.abort().work;
        assert_eq!(work.output_bytes, written.get(), "capacity {capacity}");
        if capacity >= 100 {
            let chunks = if capacity > CELL_BYTES { 2 } else { 1 };
            assert_eq!(work.sql - before.sql, 1 + chunks);
            assert_eq!(
                work.raw_bytes - before.raw_bytes,
                8 + chunks * (CELL_BYTES as u64 + 5)
            );
            assert_eq!(work.cells - before.cells, 2 + chunks);
        }
        let mut work = budget();
        let written = Rc::new(Cell::new(0));
        let mut output = ShortOutput {
            remaining: Rc::new(Cell::new(capacity)),
            written: written.clone(),
        };
        assert!(
            cells::copy_cell(&tx, &key("a"), "cid", &mut output, &mut work)
                .await
                .is_err()
        );
        assert_eq!(work.work.output_bytes, written.get());
    }
    // A redacted source can fail on its second slice without recording the
    // suppressed first slice as physical output or charging wire chunk framing.
    let mut sizing = Writer::new(Vec::new(), Projection::Review, budget()).unwrap();
    prefix(&mut sizing);
    sizing
        .frame(&Frame::Row {
            section: Section::Native,
            key: key("a"),
            cells: 1,
        })
        .unwrap();
    let prefix_bytes = sizing.budget().work.raw_bytes;
    let mut writer = Writer::new(
        Vec::new(),
        Projection::Review,
        Budget::new(Limits {
            bytes: prefix_bytes + 5 + "metadata".len() as u64 + CELL_BYTES as u64,
            ..Limits::default()
        }),
    )
    .unwrap();
    prefix(&mut writer);
    writer
        .frame(&Frame::Row {
            section: Section::Native,
            key: key("a"),
            cells: 1,
        })
        .unwrap();
    assert!(
        writer
            .native_cell(&tx, &key("a"), "metadata")
            .await
            .is_err()
    );
    let work = writer.abort();
    assert!(work.check().is_err());
    assert_eq!(
        work.work.output_bytes,
        prefix_bytes + 5 + "metadata".len() as u64
    );
    assert_eq!(
        work.work.raw_bytes,
        prefix_bytes + 5 + "metadata".len() as u64 + 2 * CELL_BYTES as u64
    );
    assert_eq!(work.work.sql, 3);
    let mut writer = Writer::new(
        Vec::new(),
        Projection::Review,
        Budget::new(Limits {
            bytes: prefix_bytes + 8 + CELL_BYTES as u64,
            ..Limits::default()
        }),
    )
    .unwrap();
    prefix(&mut writer);
    writer
        .frame(&Frame::Row {
            section: Section::Native,
            key: key("a"),
            cells: 1,
        })
        .unwrap();
    assert!(writer.native_cell(&tx, &key("a"), "cid").await.is_err());
    let work = writer.abort();
    assert!(work.check().is_err());
    assert_eq!(work.work.output_bytes, prefix_bytes + 8);
    assert_eq!(
        work.work.raw_bytes,
        prefix_bytes + 8 + CELL_BYTES as u64 + 5
    );
    assert_eq!(work.work.sql, 2);
    tx.rollback().await.unwrap();
}

fn replace_record(bytes: &[u8], index: usize, kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut at = MAGIC.len();
    for _ in 0..index {
        at += 4 + u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    }
    let end = at + 4 + u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    let mut changed = bytes[..at].to_vec();
    changed.extend_from_slice(&((payload.len() + 1) as u32).to_be_bytes());
    changed.push(kind);
    changed.extend_from_slice(payload);
    changed.extend_from_slice(&bytes[end..]);
    changed
}

#[test]
fn compact_decoder_rejects_noncanonical_json_invalid_columns_and_order_at_record() {
    let bytes = archive(b"hello");
    for (index, kind, payload) in [
        (6, 0, br#"{"type":"cell","column":"metadata"}"#.as_slice()),
        (7, 0, br#"{"type":"cell_end","value":"redacted"}"#),
        (11, 0, br#"{"type":"row_end"}"#),
        (6, 2, b"\xff"),
        (6, 2, "列".as_bytes()),
        (6, 2, b"not-an-identifier"),
        (8, 2, b"metadata"),
        (7, 6, b""),
        (9, 1, b"\xff"),
    ] {
        let changed = replace_record(&bytes, index, kind, payload);
        let mut reader = Reader::new(changed.as_slice(), Projection::Review, budget()).unwrap();
        for _ in 0..index {
            assert!(reader.next_frame().unwrap().is_some());
        }
        assert!(reader.next_frame().is_err(), "index {index}, kind {kind}");
        assert!(!reader.verified());
    }
}

#[test]
fn payload_failure_keeps_full_prepayment_and_column_copies_are_not_resident_charges() {
    let bytes = archive(b"hello");
    // The first control payload is admitted in full even if its source fails.
    let length =
        u32::from_be_bytes(bytes[MAGIC.len()..MAGIC.len() + 4].try_into().unwrap()) as usize;
    for error in [true, false] {
        let short = bytes[..MAGIC.len() + 8].to_vec();
        let mut reader = Reader::new(
            ShortInput {
                input: Cursor::new(short),
                read: Rc::new(Cell::new(0)),
                error,
                cancel: None,
            },
            Projection::Review,
            budget(),
        )
        .unwrap();
        assert!(reader.next_frame().is_err());
        assert_eq!(
            reader.budget().work.raw_bytes,
            (MAGIC.len() + 4 + length) as u64
        );
    }
    let mut reader = Reader::new(bytes.as_slice(), Projection::Review, budget()).unwrap();
    while reader.next_frame().unwrap().is_some() {}
    assert_eq!(
        reader.budget().work.copied_bytes,
        ("metadata".len() + "size".len()) as u64
    );
    let mut writer = Writer::new(Vec::new(), Projection::Review, budget()).unwrap();
    prefix(&mut writer);
    writer
        .frame(&Frame::Row {
            section: Section::Native,
            key: key("a"),
            cells: 1,
        })
        .unwrap();
    writer
        .frame(&Frame::Cell {
            column: "size".into(),
        })
        .unwrap();
    assert_eq!(writer.budget().work.copied_bytes, 4);
}

#[test]
fn short_io_preserves_complete_digest_and_control_depth_is_bounded() {
    let remaining = Rc::new(Cell::new(usize::MAX));
    let written = Rc::new(Cell::new(0));
    let mut writer = Writer::new(
        ShortOutput {
            remaining,
            written: written.clone(),
        },
        Projection::Review,
        budget(),
    )
    .unwrap();
    prefix(&mut writer);
    let before = writer.budget().work.output_bytes;
    let (_, work) = writer.finish_with_budget().unwrap();
    assert!(work.work.output_bytes > before);
    assert_eq!(work.work.output_bytes, written.get());

    let bytes = archive(b"hello");
    let mut reader = Reader::new(
        ShortInput {
            input: Cursor::new(bytes),
            read: Rc::new(Cell::new(0)),
            error: false,
            cancel: None,
        },
        Projection::Review,
        budget(),
    )
    .unwrap();
    while reader.next_frame().unwrap().is_some() {}
    assert!(reader.verified());

    let deep = format!(
        r#"{{"type":"request","request":{}null{}}}"#,
        "[".repeat(65),
        "]".repeat(65)
    );
    let changed = replace_record(&archive(b"hello"), 1, 0, deep.as_bytes());
    let mut reader = Reader::new(changed.as_slice(), Projection::Review, budget()).unwrap();
    reader.next_frame().unwrap();
    assert!(reader.next_frame().is_err());
}
