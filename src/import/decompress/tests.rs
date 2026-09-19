use std::{collections::HashMap, io, sync::Arc, time::Duration};

use bytes::Bytes;
use chrono::Utc;
use futures_util::{Stream, stream};
use sea_orm::{ColumnTrait, Database, EntityTrait, PaginatorTrait, QueryFilter, Set};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use super::*;
use crate::{
    crypto::key::MasterKey,
    import::{
        ImportClaim, ImportExecutionError, ImportFailure, ImportFailureCode, ImportSource,
        SupersedeReason,
        pipeline::{ImportArtifact, JobCancellation},
    },
    kubo::KuboClient,
    state::AppState,
    store::{
        Store,
        entities::{
            import_destination, import_job, import_job_result, import_job_target, object, pin_lease,
        },
        import::{
            jobs::{self, NewImportJob},
            ownership,
        },
    },
    zip::{extract::ExtractionObserver, response::ExtractedEntry},
};

const ARCHIVE_CID: &str = "QmArchive";
const ENTRY_CID: &str = "QmEntry";
const HELLO: &[u8] = b"hello";

fn push_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut output = Vec::new();
    let mut offsets = Vec::new();
    for (name, data) in entries {
        offsets.push(output.len() as u32);
        push_u32(&mut output, 0x0403_4b50);
        push_u16(&mut output, 20);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u32(&mut output, crc32(data));
        push_u32(&mut output, data.len() as u32);
        push_u32(&mut output, data.len() as u32);
        push_u16(&mut output, name.len() as u16);
        push_u16(&mut output, 0);
        output.extend_from_slice(name.as_bytes());
        output.extend_from_slice(data);
    }
    let central_offset = output.len() as u32;
    for (((name, data), offset), index) in entries.iter().zip(offsets).zip(0_u16..) {
        let _ = index;
        push_u32(&mut output, 0x0201_4b50);
        push_u16(&mut output, 20);
        push_u16(&mut output, 20);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u32(&mut output, crc32(data));
        push_u32(&mut output, data.len() as u32);
        push_u32(&mut output, data.len() as u32);
        push_u16(&mut output, name.len() as u16);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u32(&mut output, 0);
        push_u32(&mut output, offset);
        output.extend_from_slice(name.as_bytes());
    }
    let central_size = output.len() as u32 - central_offset;
    push_u32(&mut output, 0x0605_4b50);
    push_u16(&mut output, 0);
    push_u16(&mut output, 0);
    push_u16(&mut output, entries.len() as u16);
    push_u16(&mut output, entries.len() as u16);
    push_u32(&mut output, central_size);
    push_u32(&mut output, central_offset);
    push_u16(&mut output, 0);
    output
}

async fn test_state_with_kubo(kubo: KuboClient) -> Arc<AppState> {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    Arc::new(AppState {
        kubo,
        cold_kubo: None,
        store: Store::new(db),
        credentials: HashMap::new(),
        master_key: MasterKey::from_hex(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap(),
        pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    })
}

async fn test_state(server: &MockServer) -> Arc<AppState> {
    test_state_with_kubo(KuboClient::new(server.uri())).await
}

async fn claimed_job(
    state: &AppState,
    id: &str,
    archive_key: &str,
    prefix: &str,
) -> (import_job::Model, ImportClaim) {
    let now = Utc::now();
    ownership::submit(
        state.store.db(),
        NewImportJob {
            id: id.to_owned(),
            bucket: "bucket".to_owned(),
            key: archive_key.to_owned(),
            source: ImportSource::Cid(
                "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_owned(),
            ),
            request_fingerprint: format!("fingerprint-{id}"),
            client_token: None,
            object_content_type: Some("application/zip".to_owned()),
            metadata: HashMap::from([("source".to_owned(), "test".to_owned())]),
            tags: Vec::new(),
            decompress_prefix: Some(prefix.to_owned()),
        },
        now,
    )
    .await
    .unwrap();
    let claimed = jobs::claim_due(
        state.store.db(),
        "worker",
        now,
        now + chrono::Duration::seconds(60),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    (claimed.job, claimed.claim)
}

fn artifact(size: usize) -> ImportArtifact {
    ImportArtifact {
        cid: ARCHIVE_CID.to_owned(),
        logical_size: size as u64,
        object_content_type: Some("application/zip".to_owned()),
    }
}

async fn mount_archive(server: &MockServer, bytes: Vec<u8>) {
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .expect(1)
        .mount(server)
        .await;
}

async fn mount_entry_success(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{ENTRY_CID}\",\"Size\":\"5\"}}\n")),
        )
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn successful_import_publishes_archive_entry_results_and_progress_atomically() {
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    mount_entry_success(&server).await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_job(&state, "success", "archive.zip", "out/").await;

    let published = decompress_import(
        &state,
        &job,
        artifact(archive.len()),
        &claim,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert!(!published.object_id.is_empty());
    let objects = object::Entity::find()
        .filter(object::Column::Bucket.eq("bucket"))
        .all(state.store.db())
        .await
        .unwrap();
    assert_eq!(objects.len(), 2);
    assert!(objects.iter().any(|row| row.key == "archive.zip"));
    assert!(objects.iter().any(|row| row.key == "out/file.txt"));
    let rows = import_job_result::Entity::find()
        .filter(import_job_result::Column::JobId.eq("success"))
        .all(state.store.db())
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].key, "archive.zip");
    assert_eq!(rows[1].key, "out/file.txt");
    let completed = import_job::Entity::find_by_id("success")
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, "completed");
    assert_eq!(completed.entries_processed, 1);
    assert_eq!(completed.entries_succeeded, 1);
    assert_eq!(completed.entries_failed, 0);
    assert_eq!(completed.decompressed_bytes, HELLO.len() as i64);
}

#[tokio::test]
async fn observer_wait_is_shutdown_cancel_safe_and_publishes_nothing() {
    struct BlockingObserver {
        arrived: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl crate::import::pipeline::ImportExecutionObserver for BlockingObserver {
        async fn before_publication(&self, job_id: &str) {
            assert_eq!(job_id, "observer-cancel");
            self.arrived.notify_one();
            std::future::pending::<()>().await;
        }
    }

    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    mount_entry_success(&server).await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_job(&state, "observer-cancel", "archive.zip", "out/").await;
    let cancellation = JobCancellation {
        shutdown: CancellationToken::new(),
        ownership_lost: CancellationToken::new(),
    };
    let observer = Arc::new(BlockingObserver {
        arrived: tokio::sync::Notify::new(),
    });
    let execution_state = state.clone();
    let execution_cancellation = cancellation.clone();
    let execution_observer = observer.clone();
    let mut execution = tokio::spawn(async move {
        let reporter = crate::import::progress::ProgressReporter::start(
            execution_state.clone(),
            &job,
            claim.clone(),
            execution_cancellation.clone(),
            Duration::from_millis(5),
        );
        let result = decompress_import_with_context(
            &execution_state,
            &job,
            artifact(archive.len()),
            &claim,
            &execution_cancellation,
            &reporter,
            MAX_DECOMPRESSED_ARCHIVE_BYTES,
            execution_observer.as_ref(),
        )
        .await;
        let _ = reporter.finish(&execution_cancellation).await;
        result
    });

    if tokio::time::timeout(Duration::from_secs(2), observer.arrived.notified())
        .await
        .is_err()
    {
        cancellation.shutdown.cancel();
        execution.abort();
        let _ = execution.await;
        panic!("combined import did not reach the observer boundary within two seconds");
    }
    cancellation.shutdown.cancel();
    let execution_result = match tokio::time::timeout(Duration::from_secs(2), &mut execution).await
    {
        Ok(join_result) => join_result.expect("observer cancellation task joins"),
        Err(_) => {
            cancellation.shutdown.cancel();
            execution.abort();
            let _ = execution.await;
            panic!("shutdown did not cancel the observer wait within two seconds");
        }
    };
    let error = execution_result.expect_err("shutdown interrupts the import");
    assert!(matches!(error, ImportExecutionError::Interrupted));
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        import_job_result::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn archive_key_collision_is_terminal_before_entry_add_or_publication() {
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_job(&state, "collision", "out/file.txt", "out/").await;

    let error = decompress_import(
        &state,
        &job,
        artifact(archive.len()),
        &claim,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ImportExecutionError::Terminal(ImportFailure {
            code: ImportFailureCode::InvalidArchive,
            ..
        })
    ));
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        import_job_result::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/api/v0/add")
    );
}

#[tokio::test]
async fn entry_upload_failure_publishes_archive_and_failure_row_without_output() {
    let server = MockServer::start().await;
    let archive = zip(&[("failed.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(500).set_body_string("failed"))
        .expect(1)
        .mount(&server)
        .await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_job(&state, "entry-failure", "archive.zip", "out/").await;

    decompress_import(
        &state,
        &job,
        artifact(archive.len()),
        &claim,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    let objects = object::Entity::find().all(state.store.db()).await.unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key, "archive.zip");
    let rows = import_job_result::Entity::find()
        .filter(import_job_result::Column::JobId.eq("entry-failure"))
        .all(state.store.db())
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].key, "out/failed.txt");
    assert_eq!(rows[1].error_code.as_deref(), Some("EntryUploadFailed"));
}

#[tokio::test]
async fn parser_and_limit_failures_are_terminal_and_publish_nothing() {
    for (id, body, limit, expected_code) in [
        (
            "parser",
            b"not a zip".to_vec(),
            100,
            ImportFailureCode::InvalidArchive,
        ),
        (
            "limit-text-in-invalid-name",
            zip(&[("../decompression limit.txt", HELLO)]),
            100,
            ImportFailureCode::InvalidArchive,
        ),
        (
            "limit",
            zip(&[("file.txt", HELLO)]),
            2,
            ImportFailureCode::DecompressionLimitExceeded,
        ),
    ] {
        let server = MockServer::start().await;
        mount_archive(&server, body.clone()).await;
        let state = test_state(&server).await;
        let (job, claim) = claimed_job(&state, id, "archive.zip", "out/").await;
        let cancellation = JobCancellation {
            shutdown: CancellationToken::new(),
            ownership_lost: CancellationToken::new(),
        };
        let reporter = crate::import::progress::ProgressReporter::start(
            state.clone(),
            &job,
            claim.clone(),
            cancellation.clone(),
            Duration::from_millis(5),
        );

        let error = decompress_import_with_context(
            &state,
            &job,
            artifact(body.len()),
            &claim,
            &cancellation,
            &reporter,
            limit,
            &crate::import::pipeline::NoopImportExecutionObserver,
        )
        .await
        .unwrap_err();
        let _ = reporter.finish(&cancellation).await;

        match error {
            ImportExecutionError::Terminal(failure) => {
                assert_eq!(failure.code, expected_code, "case {id}")
            }
            other => panic!("case {id} returned non-terminal error: {other:?}"),
        }
        assert_eq!(
            object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            import_job_result::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn cancellation_and_lost_prefix_claim_preserve_typed_errors() {
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    let state = test_state(&server).await;
    let (job, claim) = claimed_job(&state, "canceled", "archive.zip", "out/").await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = decompress_import(&state, &job, artifact(archive.len()), &claim, cancel)
        .await
        .unwrap_err();
    assert!(matches!(error, ImportExecutionError::Interrupted));

    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_job(&state, "prefix-lost", "archive.zip", "out/").await;
    crate::store::entities::import_prefix_claim::Entity::delete_many()
        .filter(crate::store::entities::import_prefix_claim::Column::JobId.eq("prefix-lost"))
        .exec(state.store.db())
        .await
        .unwrap();
    let error = decompress_import(
        &state,
        &job,
        artifact(archive.len()),
        &claim,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ImportExecutionError::Superseded));
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn observer_captures_the_exact_claimed_generation() {
    let server = MockServer::start().await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_job(&state, "generation", "archive.zip", "out/").await;
    import_destination::Entity::insert(import_destination::ActiveModel {
        bucket: Set("bucket".to_owned()),
        key: Set("out/file.txt".to_owned()),
        generation: Set(41),
        owner_job_id: Set(None),
        mutation_id: Set(None),
        mutation_prefix: Set(None),
        updated_at: Set(Utc::now()),
    })
    .exec(state.store.db())
    .await
    .unwrap();
    let cancellation = JobCancellation {
        shutdown: CancellationToken::new(),
        ownership_lost: CancellationToken::new(),
    };
    let reporter = crate::import::progress::ProgressReporter::start(
        state.clone(),
        &job,
        claim.clone(),
        cancellation.clone(),
        Duration::from_millis(5),
    );
    let mut observer = super::observer::ImportExtractionObserver::new(
        &state,
        &job.bucket,
        &job.key,
        &claim,
        &cancellation,
        &reporter,
    );

    observer.entry_started("out/file.txt").await.unwrap();
    observer
        .entry_finished(&ExtractedEntry {
            key: "out/file.txt".to_owned(),
            cid: ENTRY_CID.to_owned(),
            size: HELLO.len() as i64,
        })
        .await
        .unwrap();

    assert_eq!(observer.successful[0].target.generation, 42);
    let durable = import_job_target::Entity::find_by_id((
        "generation".to_owned(),
        "bucket".to_owned(),
        "out/file.txt".to_owned(),
    ))
    .one(state.store.db())
    .await
    .unwrap()
    .unwrap();
    assert_eq!(durable.expected_generation, 42);
    drop(observer);
    let mut retry_observer = super::observer::ImportExtractionObserver::new(
        &state,
        &job.bucket,
        &job.key,
        &claim,
        &cancellation,
        &reporter,
    );
    retry_observer.entry_started("out/file.txt").await.unwrap();
    retry_observer
        .entry_finished(&ExtractedEntry {
            key: "out/file.txt".to_owned(),
            cid: "QmRetriedEntry".to_owned(),
            size: HELLO.len() as i64,
        })
        .await
        .unwrap();
    assert_eq!(retry_observer.successful[0].target.generation, 42);
    assert_eq!(
        import_job_target::Entity::find()
            .filter(import_job_target::Column::JobId.eq("generation"))
            .filter(import_job_target::Column::Key.eq("out/file.txt"))
            .count(state.store.db())
            .await
            .unwrap(),
        1,
        "retry observation must reuse the durable target without bumping generation"
    );
    reporter.finish(&cancellation).await.unwrap();
}

#[tokio::test]
async fn superseded_output_prevents_archive_entries_and_result_rows_from_publishing() {
    let server = MockServer::start().await;
    let archive = zip(&[("one.txt", HELLO), ("two.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{ENTRY_CID}\",\"Size\":\"5\"}}\n")),
        )
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_job(&state, "superseded", "archive.zip", "out/").await;
    struct PublicationGate {
        arrived: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }

    impl PublicationGate {
        fn release(&self) {
            self.resume.notify_one();
        }
    }

    struct PublicationGateRelease(Arc<PublicationGate>);

    impl Drop for PublicationGateRelease {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    #[async_trait::async_trait]
    impl crate::import::pipeline::ImportExecutionObserver for PublicationGate {
        async fn before_publication(&self, job_id: &str) {
            assert_eq!(job_id, "superseded");
            self.arrived.notify_one();
            self.resume.notified().await;
        }
    }

    let gate = Arc::new(PublicationGate {
        arrived: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let execution_state = state.clone();
    let execution_job = job.clone();
    let execution_claim = claim.clone();
    let archive_size = archive.len();
    let execution_gate = gate.clone();
    let _release_guard = PublicationGateRelease(gate.clone());
    let mut execution = tokio::spawn(async move {
        let cancellation = JobCancellation {
            shutdown: CancellationToken::new(),
            ownership_lost: CancellationToken::new(),
        };
        let reporter = crate::import::progress::ProgressReporter::start(
            execution_state.clone(),
            &execution_job,
            execution_claim.clone(),
            cancellation.clone(),
            Duration::from_millis(5),
        );
        let result = decompress_import_with_context(
            &execution_state,
            &execution_job,
            artifact(archive_size),
            &execution_claim,
            &cancellation,
            &reporter,
            MAX_DECOMPRESSED_ARCHIVE_BYTES,
            execution_gate.as_ref(),
        )
        .await;
        let reporter_result = reporter.finish(&cancellation).await;
        match (result, reporter_result) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    });
    if tokio::time::timeout(Duration::from_secs(2), gate.arrived.notified())
        .await
        .is_err()
    {
        gate.release();
        execution.abort();
        let _ = execution.await;
        panic!("combined import did not reach the publication gate within two seconds");
    }
    ownership::admit_content_mutation(
        state.store.db(),
        "bucket",
        "out/one.txt",
        None,
        SupersedeReason::PutObject,
        Utc::now(),
    )
    .await
    .unwrap();
    gate.release();
    let execution_result = match tokio::time::timeout(Duration::from_secs(2), &mut execution).await
    {
        Ok(join_result) => join_result.expect("superseded publication task joins"),
        Err(_) => {
            gate.release();
            execution.abort();
            let _ = execution.await;
            panic!("superseded publication task did not stop within two seconds");
        }
    };
    let error = execution_result.unwrap_err();

    assert!(matches!(error, ImportExecutionError::Superseded));
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        import_job_result::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    let superseded = import_job::Entity::find_by_id("superseded")
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(superseded.state, "superseded");
}

#[test]
fn archive_stream_fixture_is_streamable_without_whole_body_collection() {
    let bytes = zip(&[("file.txt", HELLO)]);
    let chunks = bytes
        .chunks(7)
        .map(|chunk| Ok::<_, io::Error>(Bytes::copy_from_slice(chunk)))
        .collect::<Vec<_>>();
    let stream = stream::iter(chunks);
    assert_eq!(stream.size_hint().0, bytes.len().div_ceil(7));
}

#[tokio::test]
async fn midstream_kubo_cat_failure_is_retryable_without_publication() {
    let archive = zip(&[("file.txt", HELLO)]);
    let entry_data_offset = 30 + "file.txt".len();
    let first_archive_chunk = archive[..entry_data_offset + 1].to_vec();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        socket
            .write_all(format!("{:X}\r\n", first_archive_chunk.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(&first_archive_chunk).await.unwrap();
        socket.write_all(b"\r\n").await.unwrap();
        socket.flush().await.unwrap();
        let (mut add_socket, _) = listener.accept().await.unwrap();
        let mut add_request = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            add_socket.read_exact(&mut byte).await.unwrap();
            add_request.push(byte[0]);
            if add_request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        assert!(add_request.starts_with(b"POST /api/v0/add"));
        add_socket
            .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        add_socket.flush().await.unwrap();
        std::future::pending::<()>().await;
    });

    let state = test_state_with_kubo(KuboClient::new_with_timeouts(
        endpoint,
        Duration::from_secs(300),
        Duration::from_millis(50),
    ))
    .await;
    let (job, claim) = claimed_job(&state, "cat-stream-failure", "archive.zip", "out/").await;
    let cancellation = JobCancellation {
        shutdown: CancellationToken::new(),
        ownership_lost: CancellationToken::new(),
    };
    let reporter = crate::import::progress::ProgressReporter::start(
        state.clone(),
        &job,
        claim.clone(),
        cancellation.clone(),
        Duration::from_millis(5),
    );

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        decompress_import_with_context(
            &state,
            &job,
            artifact(archive.len()),
            &claim,
            &cancellation,
            &reporter,
            crate::zip::extract::MAX_DECOMPRESSED_ARCHIVE_BYTES,
            &crate::import::pipeline::NoopImportExecutionObserver,
        ),
    )
    .await
    .expect("a stalled Kubo body must not hang combined import")
    .expect_err("the mid-stream Kubo failure must abort combined import");
    reporter.finish(&cancellation).await.unwrap();

    assert!(matches!(
        error,
        ImportExecutionError::Retryable(ImportFailure {
            code: ImportFailureCode::CidNotFile,
            retryable: true,
            ..
        })
    ));
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        import_job_result::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        import_job_target::Entity::find()
            .filter(import_job_target::Column::JobId.eq("cat-stream-failure"))
            .filter(import_job_target::Column::Key.eq("out/file.txt"))
            .count(state.store.db())
            .await
            .unwrap(),
        0,
        "the failed staged output must release its extracted target"
    );
    let running = import_job::Entity::find_by_id("cat-stream-failure")
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.state, "running");
    assert_eq!(running.entries_processed, 1);
    assert_eq!(running.decompressed_bytes, 1);
    server.abort();
    let _ = server.await;
}
