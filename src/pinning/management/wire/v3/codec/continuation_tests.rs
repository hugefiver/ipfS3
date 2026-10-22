use super::*;

#[test]
fn persisted_continuation_keeps_private_utf8_null_and_complete_wire_digest() {
    let mut writer =
        Writer::new(Vec::new(), Projection::PrivateRecovery, Budget::default()).unwrap();
    for frame in [
        Frame::Header {
            schema_version: SCHEMA_VERSION,
            projection: Projection::PrivateRecovery,
            database_scope: "scope".into(),
            database_identity: "physical".into(),
            principal: "owner".into(),
            applied_trust: AppliedTrust::UntrustedClaimsOnly,
            catalog: vec!["catalog".into()],
            created_at: "2026-01-01T00:00:00Z".into(),
            expires_at: None,
        },
        Frame::Request { request: None },
        Frame::Configuration {
            configuration: None,
        },
        Frame::Acknowledgements { item_ids: None },
        Frame::Summary {
            summary: Default::default(),
        },
        Frame::Row {
            section: Section::Native,
            key: NativeKey {
                table: "private_rows".into(),
                values: vec!["original".into()],
            },
            cells: 2,
        },
        Frame::Cell {
            column: "private_text".into(),
        },
    ] {
        writer.frame(&frame).unwrap();
    }
    writer.cell_output().write_all(&[0xe4]).unwrap();
    let (paused, budget) = writer.pause();
    let (mut prefix, continuation) = paused.unwrap();
    let prepaid = budget.work;
    let persisted = serde_json::to_vec(&continuation).unwrap();
    let restored = serde_json::from_slice(&persisted).unwrap();
    let mut writer = Writer::continue_metered(Vec::new(), restored, budget)
        .unwrap_or_else(|_| panic!("valid continuation"));
    assert_eq!(writer.budget().work.raw_bytes, prepaid.raw_bytes);
    assert_eq!(writer.budget().work.copied_bytes, prepaid.copied_bytes);
    assert_eq!(writer.budget().work.output_bytes, prepaid.output_bytes);
    assert_eq!(writer.budget().work.cells, prepaid.cells);
    writer.cell_output().write_all(&[0xb8, 0xad]).unwrap();
    writer
        .frame(&Frame::CellEnd {
            value: CellValue::Text,
        })
        .unwrap();
    writer
        .frame(&Frame::Cell {
            column: "sql_null".into(),
        })
        .unwrap();
    writer
        .frame(&Frame::CellEnd {
            value: CellValue::Null,
        })
        .unwrap();
    writer.frame(&Frame::RowEnd).unwrap();
    prefix.extend(writer.finish().unwrap());
    let mut reader = Reader::new(
        prefix.as_slice(),
        Projection::PrivateRecovery,
        Budget::default(),
    )
    .unwrap();
    let mut text = Vec::new();
    let mut nulls = 0;
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            Event::Chunk(bytes) => {
                assert_eq!(reader.last_frame_resident(), bytes.len() + 4);
                text.extend(bytes);
            }
            Event::Frame(Frame::Cell { column }) => {
                assert_eq!(
                    reader.last_frame_resident(),
                    2 * column.len() + std::mem::size_of::<Frame>()
                );
            }
            Event::Frame(Frame::CellEnd {
                value: CellValue::Null,
            }) => {
                assert_eq!(reader.last_frame_resident(), std::mem::size_of::<Frame>());
                nulls += 1;
            }
            _ => {}
        }
    }
    assert!(reader.verified());
    assert_eq!(text, "中".as_bytes());
    assert_eq!(nulls, 1);
}

#[test]
fn continuation_retains_cancelled_budget_without_emitting_a_prefix() {
    let writer = Writer::new(Vec::new(), Projection::Review, Budget::default()).unwrap();
    let (paused, budget) = writer.pause();
    let (_, continuation) = paused.unwrap();
    let prepaid = budget.work;
    budget
        .cancellation()
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let (error, returned) = match Writer::continue_metered(Vec::new(), continuation, budget) {
        Ok(_) => panic!("cancelled continuation accepted"),
        Err(value) => value,
    };
    assert!(matches!(error, SafeError::LimitExceeded));
    assert_eq!(returned.work.raw_bytes, prepaid.raw_bytes);
    assert_eq!(returned.work.output_bytes, prepaid.output_bytes);
    assert!(returned.check().is_err());
}
