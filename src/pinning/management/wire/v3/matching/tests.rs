use crate::pinning::management::{
    NativeKey,
    wire::v3::{self, archive_cursor, *},
};
use std::io::{Cursor, Write};

fn document(payload: &[u8], split: usize, change: bool) -> Vec<u8> {
    let mut writer =
        Writer::new(Vec::new(), Projection::Review, v3::files::stream::budget()).unwrap();
    writer
        .frame(&Frame::Header {
            schema_version: SCHEMA_VERSION,
            projection: Projection::Review,
            database_scope: "scope".into(),
            database_identity: "physical".into(),
            principal: "local_configuration".into(),
            applied_trust: AppliedTrust::UntrustedClaimsOnly,
            catalog: vec!["catalog".into()],
            created_at: "original time".into(),
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
    writer
        .frame(&Frame::Row {
            section: Section::Native,
            key: NativeKey {
                table: "objects".into(),
                values: vec!["0".into()],
            },
            cells: 1,
        })
        .unwrap();
    writer
        .frame(&Frame::Cell {
            column: "size".into(),
        })
        .unwrap();
    for chunk in payload.chunks(split) {
        writer.cell_output().write_all(chunk).unwrap();
    }
    writer
        .frame(&Frame::CellEnd {
            value: if change {
                CellValue::Null
            } else {
                CellValue::Text
            },
        })
        .unwrap();
    writer.frame(&Frame::RowEnd).unwrap();
    writer.finish().unwrap()
}

#[test]
fn complete_stream_match_ignores_only_transport_chunk_boundaries() {
    let bytes = "original 时钟 and bytes".repeat(100);
    let a = document(bytes.as_bytes(), CELL_BYTES, false);
    let b = document(bytes.as_bytes(), 7, false);
    let left = archive_cursor::stage_import(
        Cursor::new(a),
        Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap()
    .into_cursor();
    let right = Cursor::new(b);
    v3::matching::public_stream(left, right).unwrap();
}

#[test]
fn self_consistent_changed_cell_is_not_the_original_stream() {
    let a = document(b"original", 7, false);
    let b = document(b"tampered", 3, false);
    let left = archive_cursor::stage_import(
        Cursor::new(a),
        Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap()
    .into_cursor();
    let right = Cursor::new(b);
    assert!(v3::matching::public_stream(left, right).is_err());
}

#[test]
fn null_and_empty_text_are_distinct_original_cells() {
    let a = document(b"", 1, false);
    let b = document(b"", 1, true);
    let left = archive_cursor::stage_import(
        Cursor::new(a),
        Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap()
    .into_cursor();
    let right = Cursor::new(b);
    assert!(v3::matching::public_stream(left, right).is_err());
}

#[test]
fn matching_payload_without_original_terminal_marker_is_not_complete() {
    let original = document(b"original", CELL_BYTES, false);
    let mut truncated = original.clone();
    truncated.pop();
    let supplied = archive_cursor::stage_import(
        Cursor::new(original),
        Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap()
    .into_cursor();
    assert!(v3::matching::public_stream(supplied, Cursor::new(truncated)).is_err());
}

#[test]
fn cancellation_after_staging_remains_effective_during_comparison() {
    let original = document(b"original", CELL_BYTES, false);
    let budget = v3::files::stream::budget();
    let cancellation = budget.cancellation();
    let supplied =
        archive_cursor::stage_import(Cursor::new(original.clone()), Projection::Review, budget)
            .unwrap()
            .into_cursor();
    cancellation.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(v3::matching::public_stream(supplied, Cursor::new(original)).is_err());
}
