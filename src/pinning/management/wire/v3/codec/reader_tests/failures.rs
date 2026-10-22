use super::*;

#[test]
fn successful_read_ahead_is_recorded_even_when_the_read_cancels_the_budget() {
    let mut bytes = MAGIC.to_vec();
    bytes.extend(vec![b'x'; READ_BUFFER_BYTES]);
    let mut reader = reader(bytes);
    reader.enable_buffering().unwrap();
    reader.input.cancel = Some(reader.budget().cancellation());
    assert!(matches!(reader.next_frame(), Err(SafeError::LimitExceeded)));
    assert_eq!(
        reader.budget().work.raw_bytes,
        (MAGIC.len() + READ_BUFFER_BYTES) as u64
    );
    assert_eq!(reader.budget().work.copied_bytes, 4);
    assert!(reader.budget().check().is_err());
    let calls = reader.input.requests.len();
    assert!(reader.next_frame().is_err());
    assert_eq!(reader.input.requests.len(), calls);
    let budget = reader.take_budget();
    assert_eq!(budget.work.cells, 1);
    assert_eq!(
        budget.work.raw_bytes,
        (MAGIC.len() + READ_BUFFER_BYTES) as u64
    );
    assert_eq!(budget.work.copied_bytes, 4);
    assert!(budget.check().is_err());
}

#[test]
fn short_and_interrupted_reads_finish_without_double_billing_cached_raw_bytes() {
    let bytes = archive(Projection::Review, b"original payload");
    let mut direct = reader(bytes.clone());
    drain(&mut direct).unwrap();
    let mut reader = reader(bytes.clone());
    reader.enable_buffering().unwrap();
    reader.input.short = 17;
    reader.input.interrupt = true;
    reader.input.interrupt_next = true;
    drain(&mut reader).unwrap();
    assert_eq!(reader.budget().work.raw_bytes, bytes.len() as u64 + 1);
    assert_eq!(
        reader.budget().work.copied_bytes,
        direct.budget().work.copied_bytes + (bytes.len() - MAGIC.len() + 1) as u64
    );
    assert_eq!(reader.input.source.position() as usize, bytes.len());
    assert!(
        reader
            .input
            .requests
            .iter()
            .all(|&length| length <= READ_BUFFER_BYTES)
    );
}

#[test]
fn interrupted_read_checks_cancellation_before_retrying_and_keeps_prepayment() {
    let mut reader = reader(archive(Projection::Review, b"payload"));
    reader.enable_buffering().unwrap();
    reader.input.cancel = Some(reader.budget().cancellation());
    reader.input.interrupt_next = true;
    assert!(matches!(reader.next_frame(), Err(SafeError::LimitExceeded)));
    assert_eq!(reader.input.requests, [MAGIC.len(), READ_BUFFER_BYTES]);
    assert_eq!(reader.budget().work.raw_bytes, MAGIC.len() as u64 + 4);
    assert_eq!(reader.budget().work.copied_bytes, 4);
}

#[test]
fn cached_reads_still_obey_copied_byte_caps_and_the_original_deadline() {
    let bytes = archive(Projection::Review, b"payload");
    for transfer in [true, false] {
        let mut reader = reader(bytes.clone());
        reader.enable_buffering().unwrap();
        reader.next_frame().unwrap().unwrap();
        assert!(reader.buffer.as_ref().unwrap().available() >= 5);
        let before = reader.budget().work;
        if transfer {
            reader.budget_mut().restrict_transfer_bytes(0).unwrap();
        } else {
            reader
                .budget_mut()
                .restrict_work_bytes(before.raw_bytes + before.copied_bytes)
                .unwrap();
        }
        let calls = reader.input.requests.len();
        assert!(matches!(reader.next_frame(), Err(SafeError::LimitExceeded)));
        assert_eq!(reader.input.requests.len(), calls);
        assert_eq!(reader.budget().work.raw_bytes, before.raw_bytes);
        assert_eq!(reader.budget().work.copied_bytes, before.copied_bytes + 5);
        assert!(reader.budget().check().is_err());
        let budget = reader.take_budget();
        assert_eq!(budget.work.cells, before.cells + 1);
        assert_eq!(budget.work.raw_bytes, before.raw_bytes);
        assert_eq!(budget.work.copied_bytes, before.copied_bytes + 5);
        assert!(budget.check().is_err());
    }
    let mut reader = reader(bytes);
    reader.enable_buffering().unwrap();
    reader.next_frame().unwrap().unwrap();
    let calls = reader.input.requests.len();
    assert!(
        reader
            .budget_mut()
            .restrict_remaining(0, 0, Duration::ZERO)
            .is_err()
    );
    assert!(matches!(reader.next_frame(), Err(SafeError::LimitExceeded)));
    assert_eq!(reader.input.requests.len(), calls);
}

#[test]
fn malformed_lengths_and_kinds_do_not_allocate_declared_payloads() {
    for (length, kind, copied) in [(u32::MAX, 0, 4), (FRAME_BYTES as u32 + 1, 255, 5)] {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(length.to_be_bytes());
        bytes.push(kind);
        bytes.resize(MAGIC.len() + READ_BUFFER_BYTES, b'x');
        let mut reader = reader(bytes);
        reader.enable_buffering().unwrap();
        assert!(reader.next_frame().is_err());
        assert_eq!(reader.input.requests, [MAGIC.len(), READ_BUFFER_BYTES]);
        assert_eq!(
            reader.budget().work.raw_bytes,
            (MAGIC.len() + READ_BUFFER_BYTES) as u64
        );
        assert_eq!(reader.budget().work.copied_bytes, copied);
        assert_eq!(
            reader.budget().peak_resident(),
            FRAME_BYTES + READ_BUFFER_BYTES
        );
        assert!(!reader.verified());
    }
}

#[test]
fn incomplete_cached_headers_validate_length_before_reading_kind() {
    let mut frame = u32::MAX.to_be_bytes().to_vec();
    frame.extend([255; 16]);
    for cached in 0..=4 {
        let mut reader = reader_with_cached_prefix(&frame, cached);
        let before = reader.budget().work;
        let calls = reader.input.requests.len();
        assert!(matches!(reader.next_frame(), Err(SafeError::LimitExceeded)));
        assert_eq!(reader.budget().work.copied_bytes, before.copied_bytes + 4);
        assert_eq!(
            reader.buffer.as_ref().unwrap().available(),
            if cached == 4 { 0 } else { frame.len() - 4 }
        );
        assert_eq!(reader.input.requests.len(), calls + usize::from(cached < 4));
        if cached == 4 {
            assert_eq!(reader.budget().work.raw_bytes, before.raw_bytes);
        }
        assert_eq!(
            reader.budget().peak_resident(),
            FRAME_BYTES + READ_BUFFER_BYTES
        );
    }
}

#[test]
fn direct_and_cached_headers_reject_malformed_lengths_kinds_and_empty_json() {
    for (length, kind, global_limit) in [(u32::MAX, 255, true), (2, 3, false), (1, 0, false)] {
        let mut frame = length.to_be_bytes().to_vec();
        frame.push(kind);
        for buffered in [false, true] {
            let mut reader = if buffered {
                reader_with_cached_prefix(&frame, 5)
            } else {
                let mut bytes = MAGIC.to_vec();
                bytes.extend_from_slice(&frame);
                reader(bytes)
            };
            let before = reader.budget().work;
            let calls = reader.input.requests.len();
            let result = reader.next_frame();
            if global_limit {
                assert!(matches!(result, Err(SafeError::LimitExceeded)));
            } else {
                assert!(matches!(result, Err(SafeError::InvalidInput)));
            }
            if buffered {
                assert_eq!(reader.input.requests.len(), calls);
                assert_eq!(reader.budget().work.raw_bytes, before.raw_bytes);
                assert_eq!(reader.budget().work.copied_bytes, before.copied_bytes + 5);
            } else {
                assert_eq!(
                    &reader.input.requests[calls..],
                    if global_limit { &[4][..] } else { &[4, 1][..] }
                );
                assert_eq!(reader.budget().work.copied_bytes, before.copied_bytes);
            }
            assert!(!reader.verified());
        }
    }
}

#[test]
fn truncated_payload_keeps_all_prepaid_debt() {
    let mut bytes = MAGIC.to_vec();
    bytes.extend(100u32.to_be_bytes());
    bytes.extend([0, b'{']);
    let mut reader = reader(bytes);
    reader.enable_buffering().unwrap();
    assert!(reader.next_frame().is_err());
    assert_eq!(reader.budget().work.raw_bytes, MAGIC.len() as u64 + 104);
    assert_eq!(reader.budget().work.copied_bytes, 104);
    assert!(!reader.verified());
    let budget = reader.take_budget();
    assert_eq!(budget.work.cells, 1);
    assert_eq!(budget.work.raw_bytes, MAGIC.len() as u64 + 104);
    assert_eq!(budget.work.copied_bytes, 104);
}

#[test]
fn trailer_rejects_cached_and_uncached_suffixes_and_requires_successful_real_eof() {
    let bytes = archive(Projection::Review, b"payload");
    let mut valid = reader(bytes.clone());
    valid.enable_buffering().unwrap();
    drain(&mut valid).unwrap();
    assert_eq!(valid.input.requests, [MAGIC.len(), READ_BUFFER_BYTES, 1]);
    assert_eq!(valid.budget().work.raw_bytes, bytes.len() as u64 + 1);
    assert_eq!(
        valid.budget().work.copied_bytes,
        (bytes.len() - MAGIC.len() + 1 + 12) as u64
    );

    let mut suffixed = bytes.clone();
    suffixed.push(b'x');
    let mut cached = reader(suffixed.clone());
    cached.enable_buffering().unwrap();
    assert!(drain(&mut cached).is_err());
    assert_eq!(cached.input.requests, [MAGIC.len(), READ_BUFFER_BYTES]);
    assert_eq!(cached.budget().work.raw_bytes, suffixed.len() as u64);
    assert_eq!(
        cached.budget().work.copied_bytes,
        valid.budget().work.copied_bytes - 1
    );
    assert!(!cached.verified());

    let mut uncached = reader(suffixed);
    uncached.input.boundary = Some(bytes.len());
    uncached.enable_buffering().unwrap();
    assert!(drain(&mut uncached).is_err());
    assert_eq!(uncached.input.requests, [MAGIC.len(), READ_BUFFER_BYTES, 1]);
    assert_eq!(uncached.budget().work.raw_bytes, bytes.len() as u64 + 1);
    assert!(!uncached.verified());

    let mut failing = reader(bytes);
    failing.enable_buffering().unwrap();
    failing.input.eof_error = true;
    assert!(drain(&mut failing).is_err());
    assert_eq!(
        failing.budget().work.raw_bytes,
        valid.budget().work.raw_bytes
    );
    assert!(!failing.verified());
}

#[test]
fn buffered_decoding_keeps_private_opt_in_redaction_strict_json_and_hash_checks() {
    let text = format!("{}界\0original", "x".repeat(CELL_BYTES - 1));
    for projection in [Projection::Review, Projection::PrivateRecovery] {
        let bytes = archive(projection, text.as_bytes());
        let mut reader = Reader::new(Input::new(bytes), projection, Budget::default()).unwrap();
        reader.enable_buffering().unwrap();
        let mut chunks = Vec::new();
        let mut redacted = 0;
        while let Some(event) = reader.next_frame().unwrap() {
            match event {
                Event::Chunk(bytes) => chunks.extend(bytes),
                Event::Frame(Frame::CellEnd {
                    value: CellValue::Redacted,
                }) => redacted += 1,
                _ => {}
            }
        }
        assert!(reader.verified());
        match projection {
            Projection::Review => {
                assert_eq!(chunks, text.as_bytes());
                assert_eq!(redacted, 1);
            }
            Projection::PrivateRecovery => {
                assert_eq!(&chunks[..16], b"private original");
                assert_eq!(&chunks[16..], text.as_bytes());
                assert_eq!(redacted, 0);
            }
        }
    }
    let mut wrong_mode = reader(archive(Projection::PrivateRecovery, b"payload"));
    wrong_mode.enable_buffering().unwrap();
    assert!(wrong_mode.next_frame().is_err());

    let mut tampered = archive(Projection::Review, b"payload");
    let offset = tampered
        .windows(7)
        .position(|bytes| bytes == b"payload")
        .unwrap();
    tampered[offset] = b'P';
    let mut tampered = reader(tampered);
    tampered.enable_buffering().unwrap();
    assert!(drain(&mut tampered).is_err());
    assert!(!tampered.verified());

    let header = Frame::Header {
        schema_version: SCHEMA_VERSION,
        projection: Projection::Review,
        database_scope: "scope".into(),
        database_identity: "identity".into(),
        principal: "principal".into(),
        applied_trust: AppliedTrust::UntrustedClaimsOnly,
        catalog: vec!["catalog".into()],
        created_at: "time".into(),
        expires_at: None,
    };
    let mut duplicate = serde_json::to_vec(&header).unwrap();
    duplicate.pop();
    duplicate.extend_from_slice(br#", "schema_version":3}"#);
    let mut bytes = MAGIC.to_vec();
    bytes.extend(record_header(0, duplicate.len()));
    bytes.extend(duplicate);
    let mut duplicate = reader(bytes);
    duplicate.enable_buffering().unwrap();
    assert!(duplicate.next_frame().is_err());
}
