use super::*;

#[tokio::test]
async fn extraction_sampling_keeps_latest_counters_without_command_backpressure() {
    let (extraction, mut sampled) = watch::channel(ExtractionProgress::default());
    let (commands, mut command_rx) = mpsc::channel(1);
    let (kubo, _kubo_rx) = mpsc::channel(1);
    let (download, _download_rx) = watch::channel(0);
    let reporter = ProgressReporter {
        commands,
        kubo,
        download,
        extraction,
        task: AbortOnDropTask::new(tokio::spawn(std::future::pending())),
    };
    let cancellation = JobCancellation {
        shutdown: tokio_util::sync::CancellationToken::new(),
        ownership_lost: tokio_util::sync::CancellationToken::new(),
    };
    // A non-progress command waiting for the consumer must not block byte accounting.
    assert!(
        reporter
            .commands
            .send(ProgressCommand::LogicalSize(9))
            .await
            .is_ok()
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        for count in 1..=100 {
            reporter
                .extraction(
                    ExtractionProgress {
                        entries_processed: 1,
                        decompressed_bytes: count * 64 * 1024,
                        ..ExtractionProgress::default()
                    },
                    &cancellation,
                )
                .await
                .unwrap();
        }
    })
    .await
    .expect("byte accounting must not wait for the command consumer");
    assert_eq!(
        sampled.borrow_and_update().decompressed_bytes,
        100 * 64 * 1024
    );
    assert!(matches!(
        command_rx.try_recv(),
        Ok(ProgressCommand::LogicalSize(9))
    ));
    assert!(matches!(
        command_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    cancellation.ownership_lost.cancel();
    assert!(matches!(
        reporter
            .extraction(ExtractionProgress::default(), &cancellation)
            .await,
        Err(ImportExecutionError::Superseded)
    ));
}

#[tokio::test]
async fn phase_and_finish_flush_latest_extraction_before_the_periodic_tick() {
    use crate::{
        crypto::key::MasterKey,
        import::ImportSource,
        kubo::KuboClient,
        store::{
            Store,
            import::{jobs::NewImportJob, ownership},
        },
    };
    use sea_orm::Database;

    let db = Database::connect("sqlite::memory:").await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let now = Utc::now();
    ownership::submit(
        &db,
        NewImportJob {
            id: "sampled-progress".into(),
            bucket: "bucket".into(),
            key: "archive.zip".into(),
            source: ImportSource::Cid(
                "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".into(),
            ),
            request_fingerprint: "progress".into(),
            client_token: None,
            object_content_type: None,
            metadata: Default::default(),
            tags: Vec::new(),
            decompress_prefix: None,
        },
        now,
    )
    .await
    .unwrap();
    let claimed = jobs::claim_due(&db, "worker", now, now + chrono::Duration::seconds(60), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let state = Arc::new(AppState {
        kubo: KuboClient::new("http://127.0.0.1:1".into()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: Default::default(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let cancellation = JobCancellation {
        shutdown: tokio_util::sync::CancellationToken::new(),
        ownership_lost: tokio_util::sync::CancellationToken::new(),
    };
    let reporter = ProgressReporter::start(
        state.clone(),
        &claimed.job,
        claimed.claim,
        cancellation.clone(),
        Duration::from_secs(3600),
    );
    reporter
        .extraction(
            ExtractionProgress {
                entries_processed: 2,
                entries_succeeded: 1,
                entries_failed: 1,
                decompressed_bytes: 123,
            },
            &cancellation,
        )
        .await
        .unwrap();
    reporter
        .phase(ImportPhase::Publishing, &cancellation)
        .await
        .unwrap();
    let phased = import_job::Entity::find_by_id("sampled-progress")
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(phased.phase, "publishing");
    assert_eq!(
        (
            phased.entries_processed,
            phased.entries_succeeded,
            phased.entries_failed,
            phased.decompressed_bytes
        ),
        (2, 1, 1, 123)
    );
    // Directory payloads can update bytes without an entry-finished event.
    reporter
        .extraction(
            ExtractionProgress {
                decompressed_bytes: 456,
                ..ExtractionProgress::default()
            },
            &cancellation,
        )
        .await
        .unwrap();
    reporter.finish(&cancellation).await.unwrap();
    let finished = import_job::Entity::find_by_id("sampled-progress")
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            finished.entries_processed,
            finished.entries_succeeded,
            finished.entries_failed,
            finished.decompressed_bytes
        ),
        (2, 1, 1, 456)
    );
}
