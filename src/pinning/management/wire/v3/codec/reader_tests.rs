use super::*;
use crate::store::pinning::management::native::meter::Limits;
use read_buffer::READ_BUFFER_BYTES;
use std::{
    io::{Cursor, Seek, SeekFrom},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

#[path = "reader_tests/failures.rs"]
mod failures;

struct Input {
    source: Cursor<Vec<u8>>,
    requests: Vec<usize>,
    short: usize,
    interrupt: bool,
    interrupt_next: bool,
    cancel: Option<Arc<std::sync::atomic::AtomicBool>>,
    boundary: Option<usize>,
    eof_error: bool,
}
impl Input {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            source: Cursor::new(bytes),
            requests: Vec::new(),
            short: usize::MAX,
            interrupt: false,
            interrupt_next: false,
            cancel: None,
            boundary: None,
            eof_error: false,
        }
    }
}
impl Read for Input {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.requests.push(output.len());
        if self.interrupt_next {
            self.interrupt_next = false;
            if let Some(cancel) = &self.cancel {
                cancel.store(true, Ordering::Relaxed);
            }
            return Err(io::ErrorKind::Interrupted.into());
        }
        self.interrupt_next = self.interrupt;
        let position = self.source.position() as usize;
        if self.eof_error && position == self.source.get_ref().len() {
            return Err(io::Error::other("source unavailable"));
        }
        let mut length = output.len().min(self.short);
        if let Some(boundary) = self.boundary
            && position < boundary
        {
            length = length.min(boundary - position);
        }
        let n = self.source.read(&mut output[..length])?;
        if let Some(cancel) = &self.cancel {
            cancel.store(true, Ordering::Relaxed);
        }
        Ok(n)
    }
}

impl Seek for Input {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.source.seek(position)
    }
}

fn archive(projection: Projection, text: &[u8]) -> Vec<u8> {
    let mut writer = Writer::new(Vec::new(), projection, Budget::default()).unwrap();
    for frame in [
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
            cells: 2,
        },
        Frame::Cell {
            column: "metadata".into(),
        },
    ] {
        writer.frame(&frame).unwrap();
    }
    writer.cell_output().write_all(b"private original").unwrap();
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

fn reader(bytes: Vec<u8>) -> Reader<Input> {
    Reader::new(Input::new(bytes), Projection::Review, Budget::default()).unwrap()
}

fn reader_with_cached_prefix(frame: &[u8], cached: usize) -> Reader<Input> {
    let mut bytes = MAGIC.to_vec();
    bytes.push(0);
    bytes.extend_from_slice(frame);
    let mut reader = reader(bytes);
    reader.enable_buffering().unwrap();
    reader.input.boundary = Some(MAGIC.len() + 1 + cached);
    reader.prepay_read(1).unwrap();
    reader.read_prepaid_exact(&mut [0]).unwrap();
    assert_eq!(reader.buffer.as_ref().unwrap().available(), cached);
    reader.input.boundary = None;
    reader
}

fn drain(reader: &mut Reader<Input>) -> Result<()> {
    while reader.next_frame()?.is_some() {}
    assert!(reader.verified());
    Ok(())
}

#[test]
fn opt_in_is_zero_io_idempotent_and_releases_memory_with_staging_parts() {
    let resident = FRAME_BYTES + READ_BUFFER_BYTES;
    let budget = Budget::new(Limits {
        resident,
        ..Limits::default()
    });
    let mut reader = Reader::new(Input::new(MAGIC.to_vec()), Projection::Review, budget).unwrap();
    assert_eq!(reader.input.requests, [MAGIC.len()]);
    assert!(reader.buffer.is_none());
    reader.enable_buffering().unwrap();
    reader.enable_buffering().unwrap();
    assert_eq!(reader.input.requests, [MAGIC.len()]);
    assert_eq!(reader.budget().work.raw_bytes, MAGIC.len() as u64);
    assert_eq!(reader.budget().work.copied_bytes, 0);
    assert_eq!(reader.budget().peak_resident(), resident);
    let (input, mut budget) = reader.into_staging_parts();
    assert_eq!(input.source.position(), MAGIC.len() as u64);
    let _all_memory = budget.reserve(resident).unwrap();
}

#[test]
fn enabling_failure_retains_and_poisons_the_original_budget() {
    let budget = Budget::new(Limits {
        resident: FRAME_BYTES + READ_BUFFER_BYTES - 1,
        ..Limits::default()
    });
    let cancellation = budget.cancellation();
    let mut reader = Reader::new(Input::new(MAGIC.to_vec()), Projection::Review, budget).unwrap();
    assert!(matches!(
        reader.enable_buffering(),
        Err(SafeError::LimitExceeded)
    ));
    assert_eq!(reader.input.requests, [MAGIC.len()]);
    assert!(reader.next_frame().is_err());
    let budget = reader.take_budget();
    assert_eq!(budget.work.raw_bytes, MAGIC.len() as u64);
    assert!(Arc::ptr_eq(&cancellation, &budget.cancellation()));
    assert!(budget.check().is_err());
}

#[test]
fn started_or_failed_readers_cannot_bypass_opt_in_checks_with_an_existing_cache() {
    let bytes = archive(Projection::Review, b"payload");
    for buffered in [false, true] {
        let mut reader = reader(bytes.clone());
        if buffered {
            reader.enable_buffering().unwrap();
        }
        assert!(reader.next_frame().unwrap().is_some());
        let calls = reader.input.requests.len();
        assert!(matches!(
            reader.enable_buffering(),
            Err(SafeError::InvalidInput)
        ));
        assert_eq!(reader.input.requests.len(), calls);
        assert!(reader.budget().check().is_err());
    }
    let mut reader = reader(bytes);
    reader.enable_buffering().unwrap();
    reader
        .budget()
        .cancellation()
        .store(true, Ordering::Relaxed);
    assert!(reader.enable_buffering().is_err());
    assert!(reader.next_frame().is_err());
    assert_eq!(reader.input.requests, [MAGIC.len()]);
}

#[test]
fn read_ahead_and_cached_bytes_keep_one_budget_across_writer_handoffs() {
    let bytes = archive(Projection::Review, &vec![b'x'; 2 * READ_BUFFER_BYTES + 1]);
    let mut direct = reader(bytes.clone());
    drain(&mut direct).unwrap();
    let direct_work = direct.budget().work;
    assert_eq!(direct_work.raw_bytes, bytes.len() as u64 + 1);

    let mut reader = reader(bytes.clone());
    reader.enable_buffering().unwrap();
    let first = reader.next_frame().unwrap().unwrap();
    assert_eq!(
        reader.input.source.position() as usize,
        MAGIC.len() + READ_BUFFER_BYTES
    );
    assert_eq!(
        reader.budget().work.raw_bytes,
        reader.input.source.position()
    );
    let cached = reader.buffer.as_ref().unwrap().available();
    let mut writer = Writer::new(Vec::new(), Projection::Review, reader.take_budget()).unwrap();
    let Event::Frame(first) = first else {
        panic!("header required")
    };
    writer.frame(&first).unwrap();
    assert_eq!(reader.buffer.as_ref().unwrap().available(), cached);
    let mut first_cached = true;
    loop {
        reader.restore_budget(writer.take_budget());
        let before = reader.budget().work;
        let calls = reader.input.requests.len();
        let event = reader.next_frame().unwrap();
        assert_eq!(reader.budget().work.cells, before.cells + 1);
        if first_cached {
            assert!(cached >= 5);
            assert_eq!(reader.input.requests.len(), calls);
            assert_eq!(reader.budget().work.raw_bytes, before.raw_bytes);
            assert!(reader.budget().work.copied_bytes > before.copied_bytes);
            first_cached = false;
        }
        writer.restore_budget(reader.take_budget());
        match event {
            Some(Event::Frame(frame)) => writer.frame(&frame).unwrap(),
            Some(Event::Chunk(bytes)) => writer.cell_output().write_all(&bytes).unwrap(),
            None => break,
        }
    }
    assert!(reader.verified());
    let (output, mut budget) = writer.finish_with_budget().unwrap();
    assert_eq!(output, bytes);
    assert_eq!(
        budget.work.raw_bytes,
        direct_work.raw_bytes + bytes.len() as u64
    );
    assert_eq!(
        budget.work.copied_bytes,
        2 * direct_work.copied_bytes + (bytes.len() - MAGIC.len() + 1) as u64
    );
    assert_eq!(budget.work.output_bytes, bytes.len() as u64);
    // The reader still owns its cache guard while the writer owns the budget.
    let resident = Limits::default().resident;
    let free = resident - FRAME_BYTES - READ_BUFFER_BYTES;
    let memory = budget.reserve(free).unwrap();
    drop(memory);
    drop(reader);
    let _all_memory = budget.reserve(resident).unwrap();
}

#[test]
fn zero_payload_frames_keep_resident_accounting_state_and_integrity() {
    let bytes = archive(Projection::Review, b"payload");
    for buffered in [false, true] {
        let mut reader = reader(bytes.clone());
        if buffered {
            reader.enable_buffering().unwrap();
        }
        while let Some(event) = reader.next_frame().unwrap() {
            if matches!(
                event,
                Event::Frame(Frame::Cell { column }) if column == "metadata"
            ) {
                break;
            }
        }
        let frame_resident = std::mem::size_of::<Frame>();
        let cache_resident = if buffered { READ_BUFFER_BYTES } else { 0 };
        let held = reader
            .budget_mut()
            .reserve(Limits::default().resident - FRAME_BYTES - cache_resident - frame_resident)
            .unwrap();
        let before = reader.budget().work;
        let calls = reader.input.requests.len();
        assert!(matches!(
            reader.next_frame().unwrap(),
            Some(Event::Frame(Frame::CellEnd {
                value: CellValue::Redacted
            }))
        ));
        assert_eq!(reader.last_frame_resident(), frame_resident);
        assert_eq!(reader.budget().work.cells, before.cells + 1);
        assert_eq!(reader.budget().peak_resident(), Limits::default().resident);
        assert_eq!(
            reader.budget().work.copied_bytes - before.copied_bytes,
            if buffered { 5 } else { 0 }
        );
        if buffered {
            assert_eq!(reader.input.requests.len(), calls);
            assert_eq!(reader.budget().work.raw_bytes, before.raw_bytes);
        }
        drop(held);
        while let Some(event) = reader.next_frame().unwrap() {
            if matches!(event, Event::Frame(Frame::CellEnd { .. } | Frame::RowEnd)) {
                assert_eq!(reader.last_frame_resident(), frame_resident);
            }
        }
        assert!(reader.verified());
    }
}

#[test]
fn read_ahead_uses_the_minimum_ordinary_transfer_and_task_allowance() {
    // Each ceiling is independently the binding one; output participates only
    // in the task ceiling, and copied bytes participate in both shared ceilings.
    for (ordinary, transfer, task, expected) in [(3, 7, 9, 7), (8, 5, 9, 9), (8, 7, 6, 10)] {
        let magic = MAGIC.len() as u64;
        let budget = Budget::new(Limits {
            bytes: magic + 4 + ordinary,
            ..Limits::default()
        });
        let mut bytes = MAGIC.to_vec();
        bytes.extend(vec![b'x'; READ_BUFFER_BYTES]);
        let mut reader = Reader::new(Input::new(bytes), Projection::Review, budget).unwrap();
        reader.enable_buffering().unwrap();
        reader.budget_mut().work.output_bytes = 11;
        reader
            .budget_mut()
            .restrict_transfer_bytes(8 + transfer)
            .unwrap();
        reader
            .budget_mut()
            .restrict_work_bytes(magic + 11 + 8 + task)
            .unwrap();
        reader.prepay_read(4).unwrap();
        let mut out = [0; 4];
        reader.read_prepaid_exact(&mut out).unwrap();
        assert_eq!(out, [b'x'; 4]);
        assert_eq!(reader.input.requests, [MAGIC.len(), expected]);
        assert_eq!(reader.budget().work.raw_bytes, magic + expected as u64);
        assert_eq!(reader.budget().work.copied_bytes, 4);
    }
}

#[test]
fn verified_exit_releases_cache_before_reusing_the_same_budget() {
    let bytes = archive(Projection::Review, b"payload");
    let mut reader = reader(bytes);
    reader.enable_buffering().unwrap();
    drain(&mut reader).unwrap();
    let (_, mut budget) = reader.into_verified().unwrap();
    let _all_memory = budget.reserve(Limits::default().resident).unwrap();
}

#[test]
fn review_verification_returns_the_budget_when_buffer_reservation_fails() {
    let budget = Budget::new(Limits {
        resident: FRAME_BYTES,
        ..Limits::default()
    });
    let cancellation = budget.cancellation();
    let (result, budget) = super::super::planning::verify_output(
        Input::new(MAGIC.to_vec()),
        Projection::Review,
        budget,
        &crate::pinning::management::retirement::RunLimits::default(),
    );
    assert!(matches!(result, Err(SafeError::LimitExceeded)));
    assert_eq!(budget.work.raw_bytes, MAGIC.len() as u64);
    assert!(budget.check().is_err());
    assert!(Arc::ptr_eq(&cancellation, &budget.cancellation()));
}

#[test]
fn staging_verifies_eof_then_returns_an_unconsumed_second_pass_on_the_same_budget() {
    let bytes = archive(Projection::Review, b"payload");
    let budget = Budget::new(Limits {
        resident: 6 * FRAME_BYTES + READ_BUFFER_BYTES,
        ..Limits::default()
    });
    let mut headers = 0;
    let staged = super::super::archive_cursor::stage_checked(
        Input::new(bytes.clone()),
        Projection::Review,
        budget,
        None,
        |frame, budget| {
            if matches!(frame, Frame::Header { .. }) {
                headers += 1;
            }
            limit(budget.check())
        },
    )
    .unwrap();
    assert_eq!(headers, 1);
    let mut cursor = staged.into_cursor();
    assert!(!cursor.verified());
    assert_eq!(
        cursor.budget().work.raw_bytes,
        (bytes.len() + 1 + MAGIC.len()) as u64
    );
    let first_pass_copied = cursor.budget().work.copied_bytes;
    while cursor.next_event().unwrap().is_some() {}
    let (input, mut budget) = cursor.into_verified().unwrap();
    assert_eq!(
        input.requests,
        [
            MAGIC.len(),
            READ_BUFFER_BYTES,
            1,
            MAGIC.len(),
            READ_BUFFER_BYTES,
            1
        ]
    );
    assert_eq!(budget.work.raw_bytes, 2 * (bytes.len() + 1) as u64);
    assert_eq!(budget.work.copied_bytes, 2 * first_pass_copied);
    assert_eq!(budget.work.cells, 26);
    let _all_memory = budget.reserve(6 * FRAME_BYTES + READ_BUFFER_BYTES).unwrap();
}
