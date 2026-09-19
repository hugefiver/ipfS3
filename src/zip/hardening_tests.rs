mod hardening {
    use super::super::{MetadataLimits, extract_observed_with_metadata_limits};
    use super::*;

    async fn run(
        bytes: Vec<u8>,
        prefix: &str,
        limits: MetadataLimits,
        fail: bool,
    ) -> (
        Result<super::super::ExtractOutcome, ObservedExtractionError<io::Error>>,
        RecordingObserver,
        MockServer,
    ) {
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(if fail {
                ResponseTemplate::new(500)
            } else {
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n")
            })
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&kubo)
            .await;
        let state = test_state(kubo.uri()).await;
        let mut observer = RecordingObserver::default();
        // Fragment headers, data and descriptors across transport chunks.
        let chunks: Vec<_> = bytes
            .chunks(3)
            .map(|b| Ok::<_, io::Error>(Bytes::copy_from_slice(b)))
            .collect();
        let result = extract_observed_with_metadata_limits(
            &state,
            prefix,
            stream::iter(chunks),
            1024,
            &mut observer,
            limits,
        )
        .await;
        (result, observer, kubo)
    }

    async fn entry_budget(name: &[u8], fail: bool) {
        let entry = ZipEntryFixture {
            name,
            data: b"",
            method: 0,
            descriptor: false,
        };
        let limits = MetadataLimits {
            entries: 2,
            bytes: 64 * 1024 * 1024,
        };
        let (exact, _, _) = run(zip(&[entry; 2]), "p/", limits, fail).await;
        assert!(exact.is_ok(), "exact entry limit: {exact:?}");
        let (over, observer, kubo) = run(zip(&[entry; 3]), "p/", limits, fail).await;
        assert!(
            matches!(over, Err(ObservedExtractionError::Limit(_))),
            "entry budget ignored: {over:?}"
        );
        assert_eq!(
            observer
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.starts_with("start:"))
                .count(),
            if name.ends_with(b"/") { 0 } else { 2 }
        );
        assert_kubo_call_counts(
            &kubo,
            if name.ends_with(b"/") { 0 } else { 2 },
            if fail || name.ends_with(b"/") { 0 } else { 2 },
        )
        .await;
    }

    #[tokio::test]
    async fn empty_files_consume_entry_budget() {
        entry_budget(b"empty", false).await;
    }
    #[tokio::test]
    async fn directories_consume_entry_budget() {
        entry_budget(b"dir/", false).await;
    }
    #[tokio::test]
    async fn upload_failures_consume_entry_budget() {
        entry_budget(b"failed", true).await;
    }

    #[tokio::test]
    async fn default_budget_bounds_large_empty_directory_archives() {
        let entry = ZipEntryFixture {
            name: b"d/",
            data: b"",
            method: 0,
            descriptor: false,
        };
        let limits = MetadataLimits::default();
        let archive = zip(&vec![entry; limits.entries as usize]);
        let (exact, _, kubo) = run(archive, "", limits, false).await;
        assert!(exact.unwrap().failures.is_empty());
        assert_kubo_call_counts(&kubo, 0, 0).await;
        let archive = zip(&vec![entry; limits.entries as usize + 1]);
        let (over, _, kubo) = run(archive, "", limits, false).await;
        assert!(matches!(over, Err(ObservedExtractionError::Limit(_))));
        assert_kubo_call_counts(&kubo, 0, 0).await;
    }

    #[tokio::test]
    async fn rejected_entry_is_charged_before_name_validation() {
        let (result, observer, kubo) = run(
            single_entry_zip_named(b"../escape"),
            "",
            MetadataLimits {
                entries: 0,
                bytes: u64::MAX,
            },
            false,
        )
        .await;
        assert!(matches!(result, Err(ObservedExtractionError::Limit(_))));
        assert!(observer.events.lock().unwrap().is_empty());
        assert_kubo_call_counts(&kubo, 0, 0).await;
    }

    async fn metadata_budget(directory: bool, fail: bool) {
        let name = format!("{}{}", "a".repeat(900), if directory { "/" } else { "" });
        let entry = ZipEntryFixture {
            name: name.as_bytes(),
            data: b"",
            method: 0,
            descriptor: false,
        };
        let charge = 4096 + 8 * (name.len() as u64 + 2);
        let (exact, _, _) = run(
            zip(&[entry; 2]),
            "p/",
            MetadataLimits {
                entries: 10,
                bytes: charge * 2,
            },
            fail,
        )
        .await;
        assert!(exact.is_ok(), "{exact:?}");
        let (over, _, kubo) = run(
            zip(&[entry; 2]),
            "p/",
            MetadataLimits {
                entries: 10,
                bytes: charge * 2 - 1,
            },
            fail,
        )
        .await;
        assert!(
            matches!(over, Err(ObservedExtractionError::Limit(_))),
            "metadata budget ignored: {over:?}"
        );
        assert_kubo_call_counts(
            &kubo,
            usize::from(!directory),
            usize::from(!directory && !fail),
        )
        .await;
    }

    #[tokio::test]
    async fn directory_long_name_metadata_budget() {
        metadata_budget(true, false).await;
    }
    #[tokio::test]
    async fn successful_long_name_metadata_budget() {
        metadata_budget(false, false).await;
    }
    #[tokio::test]
    async fn failed_long_name_metadata_budget() {
        metadata_budget(false, true).await;
    }

    #[tokio::test]
    async fn extra_metadata_is_reserved_before_variable_fields_are_read() {
        let mut archive = single_entry_zip_named(b"file");
        archive[28..30].copy_from_slice(&8_u16.to_le_bytes());
        archive.splice(34..34, [0xff, 0xff, 4, 0, 1, 2, 3, 4]);
        let charge = 4096 + 8 * (4 + 8 + 2);
        let (exact, _, _) = run(
            archive.clone(),
            "p/",
            MetadataLimits {
                entries: 1,
                bytes: charge,
            },
            false,
        )
        .await;
        assert_eq!(exact.unwrap().entries.len(), 1);
        // No name or extra bytes are even present: rejection must happen before
        // the parser allocates/reads either field, not at EOF afterwards.
        archive.truncate(30);
        let (over, observer, kubo) = run(
            archive,
            "p/",
            MetadataLimits {
                entries: 1,
                bytes: charge - 1,
            },
            false,
        )
        .await;
        assert!(
            matches!(over, Err(ObservedExtractionError::Limit(_))),
            "{over:?}"
        );
        assert!(observer.events.lock().unwrap().is_empty());
        assert_kubo_call_counts(&kubo, 0, 0).await;
    }

    #[tokio::test]
    async fn signed_and_unsigned_deflate_descriptors_remain_compatible() {
        for signed in [true, false] {
            let mut archive = single_entry_zip(8, true);
            if !signed {
                archive.drain(45..49);
            }
            let (outcome, observer, kubo) =
                run(archive, "p/", MetadataLimits::default(), false).await;
            assert_eq!(outcome.unwrap().entries.len(), 1);
            assert_eq!(observer.bytes, 5);
            assert!(
                observer
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| event == "finish:p/file.txt")
            );
            assert_kubo_call_counts(&kubo, 1, 1).await;
        }
    }

    #[tokio::test]
    async fn final_unicode_key_uses_1024_byte_limit_before_observer_or_upload() {
        let prefix = format!("{}/", "界".repeat(339)); // 1018 UTF-8 bytes
        for (name, valid) in [("界界", true), ("界界x", false)] {
            let mut archive = single_entry_zip_named(name.as_bytes());
            archive[7] |= 8; // general-purpose bit 11: UTF-8 name
            let (result, observer, kubo) =
                run(archive, &prefix, MetadataLimits::default(), false).await;
            if valid {
                assert_eq!(result.unwrap().entries[0].key.len(), 1024);
                assert_kubo_call_counts(&kubo, 1, 1).await;
            } else {
                assert!(
                    result.is_err(),
                    "inaccessible 1025-byte key accepted: {result:?}"
                );
                assert!(observer.events.lock().unwrap().is_empty());
                assert_kubo_call_counts(&kubo, 0, 0).await;
            }
        }
    }

    async fn rejects_corruption(method: u16, descriptor: bool, field: usize) {
        let entries = [
            ZipEntryFixture {
                name: b"bad",
                data: HELLO,
                method,
                descriptor,
            },
            ZipEntryFixture {
                name: b"good",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
        ];
        let mut archive = zip(&entries);
        let offset = if descriptor {
            30 + 3 + HELLO_DEFLATED.len() + 4 + field * 4
        } else {
            14 + field * 4
        };
        archive[offset] ^= 1;
        let (result, observer, _) = run(archive, "p/", MetadataLimits::default(), false).await;
        let result = result.unwrap();
        assert!(
            !result.entries.iter().any(|e| e.key == "p/bad"),
            "corrupt entry published: {result:?}"
        );
        assert!(
            !observer
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e == "finish:p/bad")
        );
        assert_eq!(result.failures[0].code, "EntryReadFailed");
        assert!(
            result.entries.iter().any(|e| e.key == "p/good"),
            "independent next entry lost: {result:?}"
        );
    }

    #[tokio::test]
    async fn stored_crc_checked_before_publication() {
        rejects_corruption(0, false, 0).await;
    }
    #[tokio::test]
    async fn stored_size_checked_before_publication() {
        rejects_corruption(0, false, 2).await;
    }
    #[tokio::test]
    async fn descriptor_crc_checked_before_publication() {
        rejects_corruption(8, true, 0).await;
    }
    #[tokio::test]
    async fn descriptor_compressed_size_checked_before_publication() {
        rejects_corruption(8, true, 1).await;
    }
    #[tokio::test]
    async fn descriptor_uncompressed_size_checked_before_publication() {
        rejects_corruption(8, true, 2).await;
    }
}
