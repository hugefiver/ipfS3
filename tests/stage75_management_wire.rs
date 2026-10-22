use ipfs_s3_gateway::{
    pinning::management::{NativeKey, wire::v3::*},
    store::pinning::management::native::meter::{Budget, Limits},
};
use std::io::{self, Cursor, Read, Write};

#[path = "stage75_management_wire/compact.rs"]
mod compact;

fn budget() -> Budget {
    Budget::new(Limits {
        bytes: 128 * 1024 * 1024,
        rows: 100_000,
        ..Limits::default()
    })
}
fn header(projection: Projection) -> Frame {
    Frame::Header {
        schema_version: 3,
        projection,
        database_scope: "scope".into(),
        database_identity: "physical".into(),
        principal: "original principal".into(),
        applied_trust: AppliedTrust::UntrustedClaimsOnly,
        catalog: vec!["v1".into()],
        created_at: "original time text".into(),
        expires_at: None,
    }
}
fn key(id: &str) -> NativeKey {
    NativeKey {
        table: "objects".into(),
        values: vec![id.into()],
    }
}
fn prefix<W: Write>(writer: &mut Writer<W>) {
    writer.frame(&header(Projection::Review)).unwrap();
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
fn archive(text: &[u8]) -> Vec<u8> {
    let mut writer = Writer::new(Vec::new(), Projection::Review, budget()).unwrap();
    prefix(&mut writer);
    writer
        .frame(&Frame::Row {
            section: Section::Native,
            key: key("a"),
            cells: 2,
        })
        .unwrap();
    writer
        .frame(&Frame::Cell {
            column: "metadata".into(),
        })
        .unwrap();
    writer.cell_output().write_all(b"secret").unwrap();
    writer
        .frame(&Frame::CellEnd {
            value: CellValue::Text,
        })
        .unwrap();
    writer
        .frame(&Frame::Cell {
            column: "size".into(),
        })
        .unwrap();
    writer.cell_output().write_all(text).unwrap();
    writer
        .frame(&Frame::CellEnd {
            value: CellValue::Text,
        })
        .unwrap();
    writer.frame(&Frame::RowEnd).unwrap();
    writer.finish().unwrap()
}
fn read_all(
    bytes: &[u8],
    mode: Projection,
) -> Result<Vec<Event>, ipfs_s3_gateway::pinning::management::SafeError> {
    let mut reader = Reader::new(Cursor::new(bytes), mode, budget())?;
    let mut events = Vec::new();
    while let Some(event) = reader.next_frame()? {
        events.push(event);
    }
    assert!(reader.verified());
    Ok(events)
}

#[test]
fn streaming_roundtrip_preserves_utf8_bytes_and_redacts_private_cells() {
    // A multibyte codepoint straddles the 64 KiB binary frame boundary.
    let text = format!(
        "{}界\u{0} original 2020-01-01T00:00:00.000000Z",
        "a".repeat(CELL_BYTES - 1)
    );
    let bytes = archive(text.as_bytes());
    assert!(!bytes.windows(6).any(|part| part == b"secret"));
    let events = read_all(&bytes, Projection::Review).unwrap();
    let raw: Vec<u8> = events
        .iter()
        .filter_map(|event| match event {
            Event::Chunk(bytes) => Some(bytes.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    assert_eq!(raw, text.as_bytes());
    assert!(events.iter().any(|event| matches!(
        event,
        Event::Frame(Frame::CellEnd {
            value: CellValue::Redacted
        })
    )));
}

#[test]
fn ordinary_review_withholds_unknown_native_and_opaque_segment_payloads() {
    let mut writer = Writer::new(Vec::new(), Projection::Review, budget()).unwrap();
    prefix(&mut writer);
    for section in [
        Section::Native,
        Section::Membership,
        Section::Inventory,
        Section::Result,
        Section::AppliedClaim,
    ] {
        writer
            .frame(&Frame::Row {
                section,
                key: key("a"),
                cells: 1,
            })
            .unwrap();
        writer
            .frame(&Frame::Cell {
                column: "unclassified_payload".into(),
            })
            .unwrap();
        writer
            .cell_output()
            .write_all(b"PRIVATE_UNCLASSIFIED_ORIGINAL")
            .unwrap();
        writer
            .frame(&Frame::CellEnd {
                value: CellValue::Text,
            })
            .unwrap();
        writer.frame(&Frame::RowEnd).unwrap();
    }
    let bytes = writer.finish().unwrap();
    assert!(
        !bytes
            .windows(b"PRIVATE_UNCLASSIFIED_ORIGINAL".len())
            .any(|v| v == b"PRIVATE_UNCLASSIFIED_ORIGINAL")
    );
    let events = read_all(&bytes, Projection::Review).unwrap();
    assert!(!events.iter().any(|event| matches!(event, Event::Chunk(_))));
}

#[test]
fn unclassified_native_primary_keys_cannot_be_disclosed_in_an_ordinary_review() {
    let mut writer = Writer::new(Vec::new(), Projection::Review, budget()).unwrap();
    prefix(&mut writer);
    assert!(
        writer
            .frame(&Frame::Row {
                section: Section::Native,
                key: NativeKey {
                    table: "unclassified_historical_credentials".into(),
                    values: vec!["PRIVATE_PRIMARY_KEY".into()]
                },
                cells: 0
            })
            .is_err()
    );
    assert!(writer.finish().is_err());
}

#[test]
fn document_inspection_keeps_only_bounded_claims_and_rewinds_verified_originals() {
    let mut writer = Writer::new(Vec::new(), Projection::Review, budget()).unwrap();
    prefix(&mut writer);
    writer
        .frame(&Frame::Row {
            section: Section::AppliedClaim,
            key: key("claimed"),
            cells: 0,
        })
        .unwrap();
    writer.frame(&Frame::RowEnd).unwrap();
    let bytes = writer.finish().unwrap();
    let (head, source, work) =
        document::inspect(Cursor::new(bytes.clone()), Projection::Review, budget()).unwrap();
    assert!(head.applied_claims);
    assert!(head.request.is_none());
    assert!(head.acknowledgements.is_none());
    assert_eq!(head.created_at, "original time text");
    assert_eq!(source.position(), 0);
    assert_eq!(source.into_inner(), bytes);
    assert!(work.work.raw_bytes >= bytes.len() as u64);
}

#[test]
fn null_empty_and_private_recovery_remain_distinct() {
    let mut writer = Writer::new(Vec::new(), Projection::PrivateRecovery, budget()).unwrap();
    writer.frame(&header(Projection::PrivateRecovery)).unwrap();
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
    writer
        .frame(&Frame::Row {
            section: Section::Native,
            key: key("a"),
            cells: 2,
        })
        .unwrap();
    for (column, value) in [("metadata", CellValue::Text), ("size", CellValue::Null)] {
        writer
            .frame(&Frame::Cell {
                column: column.into(),
            })
            .unwrap();
        writer.frame(&Frame::CellEnd { value }).unwrap();
    }
    writer.frame(&Frame::RowEnd).unwrap();
    let bytes = writer.finish().unwrap();
    assert!(read_all(&bytes, Projection::Review).is_err());
    let events = read_all(&bytes, Projection::PrivateRecovery).unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::Frame(Frame::CellEnd {
            value: CellValue::Null
        })
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::Frame(Frame::CellEnd {
            value: CellValue::Text
        })
    )));
}

#[test]
fn rejects_truncation_replacement_extra_data_and_v2() {
    let bytes = archive(b"hello");
    for cut in [0, MAGIC.len(), bytes.len() - 1, bytes.len() / 2] {
        assert!(read_all(&bytes[..cut], Projection::Review).is_err());
    }
    let mut changed = bytes.clone();
    let at = changed.windows(5).position(|p| p == b"hello").unwrap();
    changed[at] = b'j';
    assert!(read_all(&changed, Projection::Review).is_err());
    let mut extra = bytes;
    extra.push(0);
    assert!(read_all(&extra, Projection::Review).is_err());
    assert!(read_all(br#"{"schema_version":2}"#, Projection::Review).is_err());
}

#[test]
fn rejects_unknown_duplicate_fields_invalid_utf8_and_length_before_payload_read() {
    fn raw(payload: &[u8]) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        let header = serde_json::to_vec(&header(Projection::Review)).unwrap();
        bytes.extend_from_slice(&((header.len() + 1) as u32).to_be_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&((payload.len() + 1) as u32).to_be_bytes());
        bytes.push(0);
        bytes.extend_from_slice(payload);
        bytes
    }
    for payload in [
        br#"{"type":"request","request":null,"request":null}"#.as_slice(),
        br#"{"type":"request","request":null,"other":0}"#,
        br#"{"type":"request","type":"request","request":null}"#,
        br#"{"type":"request"}"#,
        b"\xff",
    ] {
        let mut reader =
            Reader::new(Cursor::new(raw(payload)), Projection::Review, budget()).unwrap();
        assert!(reader.next_frame().unwrap().is_some());
        assert!(reader.next_frame().is_err());
        assert!(!reader.verified());
    }
    struct LengthOnly(Cursor<Vec<u8>>);
    impl Read for LengthOnly {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            assert!(!self.0.get_ref().is_empty());
            let n = self.0.read(out)?;
            assert_ne!(n, 0, "oversized payload must not be requested");
            Ok(n)
        }
    }
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&((FRAME_BYTES + 2) as u32).to_be_bytes());
    let mut reader =
        Reader::new(LengthOnly(Cursor::new(bytes)), Projection::Review, budget()).unwrap();
    assert!(reader.next_frame().is_err());
}

#[test]
fn rejects_duplicate_nested_map_fields_without_overwriting_them() {
    // Obtain the valid prefix without finishing an incomplete archive.
    let mut prefix = MAGIC.to_vec();
    for frame in [header(Projection::Review), Frame::Request { request: None }] {
        let payload = serde_json::to_vec(&frame).unwrap();
        prefix.extend_from_slice(&((payload.len() + 1) as u32).to_be_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&payload);
    }
    let value = r#"{"priority":0,"max_bytes":100,"max_pins":10,"enabled":true}"#;
    let payload = format!(
        r#"{{"type":"configuration","configuration":{{"routes":[],"policies":[],"limits":{{"p":{value},"p":{value}}},"zip_rules":null,"source_node":null}}}}"#
    );
    prefix.extend_from_slice(&((payload.len() + 1) as u32).to_be_bytes());
    prefix.push(0);
    prefix.extend_from_slice(payload.as_bytes());
    let mut reader = Reader::new(Cursor::new(prefix), Projection::Review, budget()).unwrap();
    reader.next_frame().unwrap();
    reader.next_frame().unwrap();
    assert!(reader.next_frame().is_err());
}

#[test]
fn segmented_membership_inventory_results_and_claims_are_not_authority() {
    let mut writer = Writer::new(Vec::new(), Projection::PrivateRecovery, budget()).unwrap();
    writer.frame(&header(Projection::PrivateRecovery)).unwrap();
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
    for section in [
        Section::Membership,
        Section::Inventory,
        Section::Result,
        Section::AppliedClaim,
    ] {
        for i in 0..4200 {
            writer
                .frame(&Frame::Row {
                    section,
                    key: key(&format!("{i:05}")),
                    cells: 0,
                })
                .unwrap();
            writer.frame(&Frame::RowEnd).unwrap();
        }
    }
    let bytes = writer.finish().unwrap();
    let mut reader =
        Reader::new(Cursor::new(bytes), Projection::PrivateRecovery, budget()).unwrap();
    let mut claims = 0;
    while let Some(event) = reader.next_frame().unwrap() {
        if matches!(
            event,
            Event::Frame(Frame::Row {
                section: Section::AppliedClaim,
                ..
            })
        ) {
            claims += 1;
        }
    }
    assert_eq!(claims, 4200);
    // The only decoded representation is an explicitly named claim section, not
    // Snapshot.applied or a local receipt-bearing/authorization-bearing handle.
    assert!(reader.verified());
}

#[test]
fn work_deadline_cancellation_and_resident_limits_are_fail_closed() {
    let budget = budget();
    budget
        .cancellation()
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(Writer::new(Vec::new(), Projection::Review, budget).is_err());
    assert!(
        Writer::new(
            Vec::new(),
            Projection::Review,
            Budget::new(Limits {
                elapsed: std::time::Duration::ZERO,
                ..Limits::default()
            })
        )
        .is_err()
    );
    assert!(
        Writer::new(
            Vec::new(),
            Projection::Review,
            Budget::new(Limits {
                resident: 1024,
                ..Limits::default()
            }),
        )
        .is_err()
    );
    let bytes = archive(b"text");
    let budget = Budget::new(Limits {
        bytes: MAGIC.len() as u64,
        ..Limits::default()
    });
    let mut reader = Reader::new(Cursor::new(bytes), Projection::Review, budget).unwrap();
    assert!(reader.next_frame().is_err());
    assert!(!reader.verified());
}

#[tokio::test]
async fn native_sql_cells_stream_with_one_budget_preserving_original_text_and_null() {
    use sea_orm::{ConnectionTrait, Database, DbBackend, Statement, TransactionTrait};
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared(
        "CREATE TABLE objects(id TEXT PRIMARY KEY, cid TEXT, content_type TEXT, metadata TEXT)",
    )
    .await
    .unwrap();
    let text = format!("{}界\u{0} original", "a".repeat(CELL_BYTES - 1));
    let secret = format!("  {{ \"opaque\" : \"{}\" }}\n", "private".repeat(30_000));
    db.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO objects VALUES ('a',$1,NULL,$2)",
        [text.clone().into(), secret.clone().into()],
    ))
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    for projection in [Projection::Review, Projection::PrivateRecovery] {
        let mut writer = Writer::new(Vec::new(), projection, budget()).unwrap();
        writer.frame(&header(projection)).unwrap();
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
        writer
            .frame(&Frame::Row {
                section: Section::Native,
                key: key("a"),
                cells: 3,
            })
            .unwrap();
        for column in ["cid", "content_type", "metadata"] {
            writer.native_cell(&tx, &key("a"), column).await.unwrap();
        }
        writer.frame(&Frame::RowEnd).unwrap();
        let (bytes, work) = writer.finish_with_budget().unwrap();
        assert!(work.work.sql > 3);
        assert!(work.work.raw_bytes >= (text.len() + secret.len()) as u64);
        assert_eq!(work.work.output_bytes, bytes.len() as u64);
        let events = read_all(&bytes, projection).unwrap();
        let raw: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                Event::Chunk(bytes) => Some(bytes.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect();
        let expected = if projection == Projection::PrivateRecovery {
            format!("{text}{secret}")
        } else {
            text.clone()
        };
        assert_eq!(raw, expected.as_bytes());
        assert!(events.iter().any(|e| matches!(
            e,
            Event::Frame(Frame::CellEnd {
                value: CellValue::Null
            })
        )));
    }
    tx.rollback().await.unwrap();
}

#[test]
fn rejects_order_duplicate_rows_incomplete_cells_and_invalid_utf8() {
    let mut writer = Writer::new(Vec::new(), Projection::Review, budget()).unwrap();
    prefix(&mut writer);
    assert!(
        writer
            .frame(&Frame::Configuration {
                configuration: None
            })
            .is_err()
    );
    let mut writer = Writer::new(Vec::new(), Projection::Review, budget()).unwrap();
    prefix(&mut writer);
    writer
        .frame(&Frame::Row {
            section: Section::Native,
            key: key("a"),
            cells: 0,
        })
        .unwrap();
    writer.frame(&Frame::RowEnd).unwrap();
    assert!(
        writer
            .frame(&Frame::Row {
                section: Section::Native,
                key: key("a"),
                cells: 0
            })
            .is_err()
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
            column: "value".into(),
        })
        .unwrap();
    assert!(writer.cell_output().write_all(b"\xff").is_err());
    assert!(writer.finish().is_err());
}

#[test]
fn streams_beyond_old_document_limit_with_fixed_size_chunks() {
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
            column: "content_type".into(),
        })
        .unwrap();
    for _ in 0..270 {
        writer.cell_output().write_all(&[b'x'; CELL_BYTES]).unwrap();
    }
    writer
        .frame(&Frame::CellEnd {
            value: CellValue::Text,
        })
        .unwrap();
    writer.frame(&Frame::RowEnd).unwrap();
    let bytes = writer.finish().unwrap();
    assert!(bytes.len() > 16 * 1024 * 1024);
    let mut reader = Reader::new(Cursor::new(bytes), Projection::Review, budget()).unwrap();
    let mut count = 0;
    while let Some(event) = reader.next_frame().unwrap() {
        if let Event::Chunk(bytes) = event {
            assert!(bytes.len() <= CELL_BYTES);
            count += bytes.len();
        }
    }
    assert_eq!(count, 270 * CELL_BYTES);
}
