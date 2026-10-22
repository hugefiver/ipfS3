use super::*;
use crate::store::pinning::management::native::meter::Limits;
use std::{cell::Cell, io::Cursor, rc::Rc, sync::atomic::Ordering};

fn prefix(projection: Projection) -> Vec<Frame> {
    vec![
        Frame::Header {
            schema_version: SCHEMA_VERSION,
            projection,
            database_scope: "scope".into(),
            database_identity: "identity".into(),
            principal: "principal".into(),
            applied_trust: AppliedTrust::UntrustedClaimsOnly,
            catalog: vec!["catalog".into()],
            created_at: "time".into(),
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
                table: "objects".into(),
                values: vec!["a".into()],
            },
            cells: 3,
        },
    ]
}

fn ready<W: Write>(output: W, projection: Projection) -> Writer<W> {
    let mut writer = Writer::new(output, projection, Budget::default()).unwrap();
    for frame in prefix(projection) {
        writer.frame(&frame).unwrap();
    }
    writer
}

fn record(bytes: &mut Vec<u8>, kind: u8, payload: &[u8]) {
    bytes.extend_from_slice(&(payload.len() as u32 + 1).to_be_bytes());
    bytes.push(kind);
    bytes.extend_from_slice(payload);
}

fn short(column: &[u8], tag: u8, text: &[u8]) -> Vec<u8> {
    let mut bytes = (column.len() as u16).to_be_bytes().to_vec();
    bytes.push(tag);
    bytes.extend_from_slice(column);
    bytes.extend_from_slice(text);
    bytes
}

// Independent producer, including the physical footer: no Writer or Integrity.
fn independent(projection: Projection, cells: &[(u8, Vec<u8>)]) -> Vec<u8> {
    let mut bytes = MAGIC.to_vec();
    let mut controls = prefix(projection);
    if let Frame::Row { cells: count, .. } = controls.last_mut().unwrap() {
        *count = cells.len() as u32;
    }
    for frame in &controls {
        record(&mut bytes, 0, &serde_json::to_vec(frame).unwrap());
    }
    for (kind, payload) in cells {
        record(&mut bytes, *kind, payload);
    }
    record(&mut bytes, 6, b"");
    let trailer = Frame::Trailer {
        frames: (controls.len() + cells.len() + 1) as u64,
        bytes: bytes.len() as u64,
        sha256: hex::encode(Sha256::digest(&bytes)),
    };
    record(&mut bytes, 0, &serde_json::to_vec(&trailer).unwrap());
    bytes
}

fn logical(bytes: &[u8], projection: Projection) -> Result<Vec<Vec<u8>>> {
    let mut reader = Reader::new(bytes, projection, Budget::default())?;
    let mut events = Vec::new();
    while let Some(event) = reader.next_frame()? {
        events.push(match event {
            Event::Frame(frame) => serde_json::to_vec(&frame).unwrap(),
            Event::Chunk(mut bytes) => {
                bytes.insert(0, 0xff);
                bytes
            }
        });
    }
    assert!(reader.verified());
    Ok(events)
}

fn archive(compact: bool) -> Vec<u8> {
    let mut writer = ready(Vec::new(), Projection::PrivateRecovery);
    for (column, text, value) in [
        ("cid", "原文-é\0尾".as_bytes(), CellValue::Text),
        ("metadata", b"".as_slice(), CellValue::Null),
        ("size", b"".as_slice(), CellValue::Text),
    ] {
        if compact {
            writer.complete_cell(column, text, value).unwrap();
        } else {
            writer
                .frame(&Frame::Cell {
                    column: column.into(),
                })
                .unwrap();
            writer.cell_output().write_all(text).unwrap();
            writer.frame(&Frame::CellEnd { value }).unwrap();
        }
    }
    writer.frame(&Frame::RowEnd).unwrap();
    writer.finish().unwrap()
}

#[test]
fn producer_and_independent_short_records_preserve_typed_events_and_physical_digest() {
    let compact = archive(true);
    assert_eq!(
        logical(&compact, Projection::PrivateRecovery).unwrap(),
        logical(&archive(false), Projection::PrivateRecovery).unwrap()
    );
    let independent = independent(
        Projection::PrivateRecovery,
        &[
            (7, short(b"cid", 1, "原文-é\0尾".as_bytes())),
            (7, short(b"metadata", 0, b"")),
            (7, short(b"size", 1, b"")),
        ],
    );
    assert_eq!(compact, independent);
    let mut at = MAGIC.len();
    let mut frames = 0;
    let mut short_count = 0;
    loop {
        let length = u32::from_be_bytes(compact[at..at + 4].try_into().unwrap()) as usize;
        let kind = compact[at + 4];
        let payload = &compact[at + 5..at + 4 + length];
        if kind == 0
            && let Frame::Trailer {
                frames: count,
                bytes,
                sha256,
            } = serde_json::from_slice::<Frame>(payload).unwrap()
        {
            assert_eq!(count, frames);
            assert_eq!(bytes, at as u64);
            assert_eq!(sha256, hex::encode(Sha256::digest(&compact[..at])));
            assert_eq!(at + 4 + length, compact.len());
            break;
        }
        short_count += usize::from(kind == 7);
        frames += 1;
        at += 4 + length;
    }
    assert_eq!(short_count, 3);
}

#[test]
fn independent_short_cells_validate_shape_utf8_order_projection_and_row_before_first_event() {
    for payload in [
        short(b"", 1, b"x"),
        short(&vec![b'a'; 257], 1, b""),
        short(b"cid", 3, b""),
        short(b"cid", 0, b"x"),
        short(b"cid", 2, b"x"),
        short(b"cid", 1, &vec![b'x'; CELL_BYTES + 1]),
        short(b"cid", 1, b"\xff"),
        short(b"cid", 1, b"\xe4"),
        short(b"bad-column", 1, b""),
        short(b"\xff", 1, b""),
        short(b"cid", 2, b""),
    ] {
        let bytes = independent(Projection::PrivateRecovery, &[(7, payload)]);
        let mut reader = Reader::new(
            bytes.as_slice(),
            Projection::PrivateRecovery,
            Budget::default(),
        )
        .unwrap();
        for _ in 0..6 {
            reader.next_frame().unwrap().unwrap();
        }
        assert!(reader.next_frame().is_err());
        assert!(!reader.verified());
    }
    let bytes = independent(
        Projection::PrivateRecovery,
        &[(7, short(b"size", 1, b"1")), (7, short(b"cid", 1, b"2"))],
    );
    assert!(logical(&bytes, Projection::PrivateRecovery).is_err());
    for tag in [0, 1, 2] {
        let bytes = independent(Projection::Review, &[(7, short(b"metadata", tag, b""))]);
        assert!(logical(&bytes, Projection::Review).is_err());
    }
    // A redacted value is valid only in Review, in a non-private column.
    let bytes = independent(Projection::Review, &[(7, short(b"size", 2, b""))]);
    assert!(logical(&bytes, Projection::Review).is_ok());
    let bytes = independent(
        Projection::PrivateRecovery,
        &[(7, short(b"cid", 1, &vec![b'x'; CELL_BYTES]))],
    );
    assert!(logical(&bytes, Projection::PrivateRecovery).is_ok());
}

fn at_cell(bytes: Vec<u8>) -> Reader<Cursor<Vec<u8>>> {
    let mut reader = Reader::new(
        Cursor::new(bytes),
        Projection::PrivateRecovery,
        Budget::default(),
    )
    .unwrap();
    for _ in 0..6 {
        reader.next_frame().unwrap().unwrap();
    }
    reader
}

#[test]
fn short_record_expansion_prepays_once_keeps_resident_costs_and_never_reads_ahead() {
    let mut reader = at_cell(archive(true));
    let before = reader.budget().work;
    let Some(Event::Frame(Frame::Cell { column })) = reader.next_frame().unwrap() else {
        panic!("column")
    };
    assert_eq!(column, "cid");
    assert_eq!(
        reader.last_frame_resident(),
        2 * column.len() + std::mem::size_of::<Frame>()
    );
    let position = reader.input.position();
    let prepaid = reader.budget().work;
    assert_eq!(prepaid.cells, before.cells + 1);
    assert_eq!(
        prepaid.raw_bytes - before.raw_bytes,
        (8 + column.len() + "原文-é\0尾".len()) as u64
    );
    let Some(Event::Chunk(bytes)) = reader.next_frame().unwrap() else {
        panic!("text")
    };
    assert_eq!(reader.last_frame_resident(), bytes.len() + 4);
    assert!(matches!(
        reader.next_frame().unwrap(),
        Some(Event::Frame(Frame::CellEnd {
            value: CellValue::Text
        }))
    ));
    assert_eq!(reader.last_frame_resident(), std::mem::size_of::<Frame>());
    assert_eq!(reader.input.position(), position);
    assert_eq!(reader.budget().work.raw_bytes, prepaid.raw_bytes);
    assert_eq!(reader.budget().work.copied_bytes, prepaid.copied_bytes);
    assert_eq!(reader.budget().work.cells, prepaid.cells);
    while reader.next_frame().unwrap().is_some() {}
    assert!(reader.verified());
}

#[test]
fn pending_short_cell_checks_cancellation_on_every_public_step_and_rejects_record_mixing() {
    for delivered in 0..3 {
        let mut reader = at_cell(archive(true));
        for _ in 0..delivered {
            reader.next_frame().unwrap().unwrap();
        }
        let before = reader.input.position();
        let work = reader.budget().work;
        reader
            .budget()
            .cancellation()
            .store(true, Ordering::Relaxed);
        assert!(reader.next_frame().is_err());
        assert_eq!(reader.input.position(), before);
        assert_eq!(reader.budget().work.raw_bytes, work.raw_bytes);
    }
    let mut reader = at_cell(archive(true));
    reader.next_frame().unwrap().unwrap();
    let position = reader.input.position();
    assert!(reader.next_record().is_err());
    assert_eq!(reader.input.position(), position);
}

#[test]
fn malformed_truncated_short_record_and_trailer_suffix_cannot_be_verified() {
    let bytes = archive(true);
    let start = at_cell(bytes.clone()).input.position() as usize;
    let end = start + 4 + u32::from_be_bytes(bytes[start..start + 4].try_into().unwrap()) as usize;
    for cut in start..end {
        let mut reader = at_cell(bytes[..cut].to_vec());
        let before = reader.budget().work.raw_bytes;
        assert!(reader.next_frame().is_err());
        if cut >= start + 8 {
            assert_eq!(
                reader.budget().work.raw_bytes - before,
                (end - start) as u64
            );
        }
        assert!(!reader.verified());
    }
    let mut damaged = bytes.clone();
    damaged[start + 11] ^= 1;
    assert!(logical(&damaged, Projection::PrivateRecovery).is_err());
    let mut suffix = bytes.clone();
    suffix.push(0);
    assert!(logical(&suffix, Projection::PrivateRecovery).is_err());
    assert!(logical(&bytes[..bytes.len() - 1], Projection::PrivateRecovery).is_err());
}

#[test]
fn short_header_rejects_kind_length_and_value_before_variable_payload_io() {
    let bytes = archive(true);
    let start = at_cell(bytes.clone()).input.position() as usize;
    for (field, replacement, consumed) in [(4, 255, 5), (5, 1, 8), (6, 0, 8), (7, 255, 8)] {
        let mut malformed = bytes.clone();
        malformed[start + field] = replacement;
        let mut reader = at_cell(malformed);
        let before = reader.budget().work;
        assert!(reader.next_frame().is_err());
        assert_eq!(reader.input.position(), (start + consumed) as u64);
        assert_eq!(
            reader.budget().work.raw_bytes - before.raw_bytes,
            consumed as u64
        );
    }
    for length in [4, 4 + 256 + CELL_BYTES + 1] {
        let mut malformed = bytes.clone();
        malformed[start..start + 4].copy_from_slice(&(length as u32).to_be_bytes());
        let mut reader = at_cell(malformed);
        assert!(reader.next_frame().is_err());
        assert_eq!(reader.input.position(), (start + 5) as u64);
    }
}

#[test]
fn record_handoff_preserves_kind7_and_counts_source_and_output_independently() {
    let bytes = archive(true);
    let mut reader = Reader::new(
        bytes.as_slice(),
        Projection::PrivateRecovery,
        Budget::default(),
    )
    .unwrap();
    let mut writer = Writer::new(
        Vec::new(),
        Projection::PrivateRecovery,
        reader.take_budget(),
    )
    .unwrap();
    loop {
        reader.restore_budget(writer.take_budget());
        let record = reader.next_record();
        writer.restore_budget(reader.take_budget());
        match record.unwrap() {
            Some(Record::Event(Event::Frame(frame))) => writer.frame(&frame).unwrap(),
            Some(Record::Event(Event::Chunk(bytes))) => {
                writer.cell_output().write_all(&bytes).unwrap()
            }
            Some(Record::ShortCell(held)) => writer
                .complete_cell(&held.cell.column, &held.cell.bytes, held.cell.value)
                .unwrap(),
            None => break,
        }
    }
    assert!(reader.verified());
    let (output, budget) = writer.finish_with_budget().unwrap();
    assert_eq!(output, bytes);
    assert_eq!(budget.work.output_bytes, output.len() as u64);
    assert_eq!(budget.work.raw_bytes, 2 * output.len() as u64 + 1);
    assert_eq!(
        budget.work.copied_bytes,
        2 * ("cid".len() + "metadata".len() + "size".len()) as u64
    );
}

#[test]
fn short_payload_prepayment_failure_retains_full_debt_before_payload_io() {
    let bytes = archive(true);
    let mut reader = at_cell(bytes.clone());
    let start = reader.input.position();
    let physical = 8 + 3 + "原文-é\0尾".len() as u64;
    reader
        .budget_mut()
        .restrict_remaining(100, physical - 1, std::time::Duration::from_secs(30))
        .unwrap();
    let before = reader.budget().work;
    assert!(reader.next_frame().is_err());
    assert_eq!(reader.input.position(), start + 8);
    assert_eq!(reader.budget().work.raw_bytes - before.raw_bytes, physical);
    assert!(reader.budget().check().is_err());
}

struct PartialOutput {
    allowance: Rc<Cell<usize>>,
}
impl Write for PartialOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = bytes.len().min(self.allowance.get());
        self.allowance.set(self.allowance.get() - n);
        if n == 0 {
            return Err(io::Error::other("output unavailable"));
        }
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn complete_cell_partial_write_keeps_prepaid_physical_debt_and_poisons_writer() {
    for n in [0, 1, 5, 9] {
        let allowance = Rc::new(Cell::new(usize::MAX));
        let mut writer = ready(
            PartialOutput {
                allowance: allowance.clone(),
            },
            Projection::PrivateRecovery,
        );
        let before = writer.budget().work;
        allowance.set(n);
        assert!(
            writer
                .complete_cell("cid", b"original", CellValue::Text)
                .is_err()
        );
        assert_eq!(
            writer.budget().work.output_bytes - before.output_bytes,
            n as u64
        );
        assert_eq!(writer.budget().work.raw_bytes - before.raw_bytes, 8 + 3 + 8);
        assert_eq!(writer.budget().work.cells - before.cells, 1);
        assert!(writer.frame(&Frame::RowEnd).is_err());
        assert!(writer.finish().is_err());
    }
}

#[test]
fn short_record_handoff_holds_input_memory_while_output_has_independent_reservation() {
    let mut reader = at_cell(archive(true));
    let held = match reader.next_record().unwrap().unwrap() {
        Record::ShortCell(held) => held,
        _ => panic!("short record"),
    };
    let mut writer = Writer::new(
        Vec::new(),
        Projection::PrivateRecovery,
        reader.take_budget(),
    )
    .unwrap();
    for frame in prefix(Projection::PrivateRecovery) {
        writer.frame(&frame).unwrap();
    }
    let input_cost = 2 * (3 + "原文-é\0尾".len()) + 3 + std::mem::size_of::<Frame>();
    let output_cost = 3 + 3 + "原文-é\0尾".len() + 6 + std::mem::size_of::<Frame>();
    // Both reader and writer state guards, plus the held input, remain live.
    let free = Limits::default().resident - 2 * FRAME_BYTES - input_cost;
    let filler = writer.budget_mut().reserve(free - output_cost + 1).unwrap();
    assert!(
        writer
            .complete_cell(&held.cell.column, &held.cell.bytes, held.cell.value)
            .is_err()
    );
    assert!(writer.budget().check().is_err());
    drop(filler);
    drop(held);
    drop(reader);
}

#[test]
fn review_private_complete_and_streaming_cells_have_identical_representation_for_all_source_shapes()
{
    fn encode(text: &[u8], value: CellValue, compact: bool) -> (Vec<u8>, u64) {
        let mut writer = ready(Vec::new(), Projection::Review);
        let before = writer.budget().work.raw_bytes;
        if compact {
            writer.complete_cell("metadata", text, value).unwrap();
        } else {
            writer
                .frame(&Frame::Cell {
                    column: "metadata".into(),
                })
                .unwrap();
            writer.cell_output().write_all(text).unwrap();
            writer.frame(&Frame::CellEnd { value }).unwrap();
        }
        let raw = writer.budget().work.raw_bytes - before;
        let (paused, _) = writer.pause();
        (paused.unwrap().0, raw)
    }
    for (text, value) in [
        (b"".as_slice(), CellValue::Null),
        (b"".as_slice(), CellValue::Text),
        (b"private".as_slice(), CellValue::Text),
    ] {
        let a = encode(text, value, true);
        let b = encode(text, value, false);
        assert_eq!(a, b);
        assert_eq!(a.1, 5 + 8 + 5 + text.len() as u64);
    }
    let short = encode(b"private", CellValue::Text, true);
    let long = encode(&vec![b'x'; CELL_BYTES + 1], CellValue::Text, false);
    assert_eq!(short.0, long.0);
    assert_eq!(long.1 - short.1, (CELL_BYTES + 1 - 7) as u64);
}
