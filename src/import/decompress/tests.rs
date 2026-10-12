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

const ARCHIVE_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
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

pub(crate) fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
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
    archive: &[u8],
) -> (import_job::Model, ImportClaim) {
    let now = Utc::now();
    let request = NewImportJob {
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
    };
    let capture = crate::import::test_support::capture_legacy(state, &request, false);
    ownership::submit_captured_zip(state.store.db(), request, capture, now)
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
    crate::import::test_support::prove_legacy_input(state, &claimed.job, &claimed.claim, archive)
        .await;
    (claimed.job, claimed.claim)
}

async fn claimed_stage4_job(
    state: &AppState,
    id: &str,
    enabled: bool,
    archive: &[u8],
) -> (import_job::Model, ImportClaim) {
    use crate::store::import::jobs::NewImportJob;
    let now = Utc::now();
    let request = NewImportJob {
        id: id.into(),
        bucket: "bucket".into(),
        key: "archive.zip".into(),
        source: ImportSource::Cid(
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".into(),
        ),
        request_fingerprint: format!("fingerprint-{id}"),
        client_token: None,
        object_content_type: Some("application/zip".into()),
        metadata: HashMap::new(),
        tags: Vec::new(),
        decompress_prefix: Some("out/".into()),
    };
    let capture = crate::import::test_support::capture_legacy(state, &request, enabled);
    ownership::submit_captured_zip(state.store.db(), request, capture, now)
        .await
        .unwrap();
    let claimed = jobs::claim_due(
        state.store.db(),
        "stage4-worker",
        now,
        now + chrono::Duration::seconds(60),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    crate::import::test_support::prove_legacy_input(state, &claimed.job, &claimed.claim, archive)
        .await;
    (claimed.job, claimed.claim)
}

async fn claimed_historical_job(
    state: &AppState,
    id: &str,
    archive: &[u8],
) -> (import_job::Model, ImportClaim) {
    use crate::pinning::{
        config::ValidatedPinningConfig,
        decision::DecisionOrigin,
        policy::{PinPolicyEvaluator, PublicationContext},
    };
    // Known original empty configuration, not the worker's current settings.
    let original =
        ValidatedPinningConfig::from_raw(&crate::config::PinningConfig::default(), |_| None)
            .unwrap();
    let request = NewImportJob {
        id: id.into(),
        bucket: "bucket".into(),
        key: "archive.zip".into(),
        source: ImportSource::Cid(ARCHIVE_CID.into()),
        request_fingerprint: format!("fingerprint-{id}"),
        client_token: None,
        object_content_type: Some("application/zip".into()),
        metadata: HashMap::from([("source".into(), "test".into())]),
        tags: vec![],
        decompress_prefix: Some("out/".into()),
    };
    let (_, mut decision) = PinPolicyEvaluator::new(&original)
        .evaluate_publication_decision(
            PublicationContext {
                bucket: &request.bucket,
                key: &request.key,
                tags: &request.tags,
                is_decompress_zip: true,
            },
            DecisionOrigin::new("test", id),
        )
        .unwrap();
    decision
        .capture_durable_revision(&original, crate::config::OptionalPinControlMode::default())
        .unwrap();
    let now = Utc::now();
    ownership::submit_decided(state.store.db(), request, decision, now)
        .await
        .unwrap();
    let claimed = jobs::claim_due(
        state.store.db(),
        "historical-worker",
        now,
        now + chrono::Duration::seconds(60),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert!(claimed.job.zip_intent_json.is_none());
    assert!(claimed.job.root_capture_json.is_none());
    crate::import::test_support::prove_legacy_input(state, &claimed.job, &claimed.claim, archive)
        .await;
    (claimed.job, claimed.claim)
}

#[tokio::test]
async fn historical_zip_missing_or_unknown_capture_never_inherits_current_authority() {
    let server = MockServer::start().await;
    let state = test_state(&server).await;
    let archive = zip(&[("file.txt", HELLO)]);
    let (job, claim) = claimed_historical_job(&state, "historical-boundary", &archive).await;
    assert!(state.pinning.zip_root_default());
    let original = crate::import::zip_capture::legacy_capture(&job).unwrap();
    assert!(!original.controls().root.enabled());
    assert!(original.source_decision().effective_intents.is_empty());

    let config = crate::import::ImportConfig::default().validate().unwrap();
    let coordinator = crate::import::pipeline::ImportCoordinator::new(
        config.clone(),
        crate::import::downloader::SourceDownloader::production(Arc::new(config)),
    );
    let mut missing = job.clone();
    missing.pin_decision_json = None;
    let mut unknown = job;
    unknown.zip_intent_json = Some("{\"schema_version\":0}".into());
    for invalid in [missing, unknown] {
        let error = crate::import::pipeline::execute_job(
            coordinator.clone(),
            state.clone(),
            invalid,
            claim.clone(),
            JobCancellation {
                shutdown: CancellationToken::new(),
                ownership_lost: CancellationToken::new(),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ImportExecutionError::Terminal(ImportFailure {
                code: ImportFailureCode::PublicationFailed,
                ..
            })
        ));
    }
    assert!(server.received_requests().await.unwrap().is_empty());
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
async fn stage4_off_and_zero_files_publish_archive_with_truthful_root_status() {
    for (id, enabled, entries, expected) in [
        ("stage4-off", false, vec![("file.txt", HELLO)], "disabled"),
        ("stage4-zero", true, vec![("failed.txt", HELLO)], "empty"),
    ] {
        let server = MockServer::start().await;
        let archive = zip(&entries);
        mount_archive(&server, archive.clone()).await;
        if id == "stage4-zero" {
            Mock::given(method("POST"))
                .and(path("/api/v0/add"))
                .respond_with(ResponseTemplate::new(500))
                .expect(1)
                .mount(&server)
                .await;
        } else {
            mount_entry_success(&server).await;
        }
        let state = test_state(&server).await;
        let (job, claim) = claimed_stage4_job(&state, id, enabled, &archive).await;
        decompress_import(
            &state,
            &job,
            artifact(archive.len()),
            &claim,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let snapshot = crate::store::zip::snapshot(state.store.db(), id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.batch.state, "published");
        assert_eq!(snapshot.batch.root_status, expected);
        assert_eq!(snapshot.batch.root_cid, None);
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| { request.url.path() != "/api/v0/dag/put" })
        );
        assert_eq!(
            import_job::Entity::find_by_id(id)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "completed"
        );
    }
}

#[tokio::test]
async fn stage4_root_failure_preserves_archive_entries_and_safe_failure_code() {
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    mount_entry_success(&server).await; // legacy fixture CID is intentionally invalid for a directory.
    let state = test_state(&server).await;
    let (job, claim) = claimed_stage4_job(&state, "stage4-failed", true, &archive).await;
    decompress_import(
        &state,
        &job,
        artifact(archive.len()),
        &claim,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let snapshot = crate::store::zip::snapshot(state.store.db(), "stage4-failed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.root_status, "failed");
    assert_eq!(
        snapshot.batch.root_error_code.as_deref(),
        Some("invalid_manifest")
    );
    assert_eq!(snapshot.entries.len(), 1);
    assert!(snapshot.entries[0].version_row_id.is_some());
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn stage4_root_post_put_failures_retain_candidate_and_complete_import() {
    use wiremock::matchers::query_param;

    const LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let root =
        cid::Cid::new_v1(0x70, LEAF.parse::<cid::Cid>().unwrap().hash().to_owned()).to_string();
    for failed_rpc in ["pin/add", "resolve", "pin/ls"] {
        let server = MockServer::start().await;
        let archive = zip(&[("file.txt", HELLO)]);
        mount_archive(&server, archive.clone()).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Hash\":\"{LEAF}\",\"Size\":\"5\"}}\n")),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .and(query_param("arg", LEAF))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .and(query_param("arg", format!("/ipfs/{LEAF}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Hash\":\"{LEAF}\",\"CumulativeSize\":5}}")),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"ID\":\"localnode\"}"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/put"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Cid\":{{\"/\":\"{root}\"}}}}")),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .and(query_param("arg", root.clone()))
            .respond_with(if failed_rpc == "pin/add" {
                ResponseTemplate::new(500).set_body_string("private pin failure")
            } else {
                ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{root}\"]}}"))
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/resolve"))
            .respond_with(if failed_rpc == "resolve" {
                ResponseTemplate::new(500).set_body_string("private resolve failure")
            } else {
                ResponseTemplate::new(200).set_body_string(format!("{{\"Path\":\"/ipfs/{LEAF}\"}}"))
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .respond_with(if failed_rpc == "pin/ls" {
                ResponseTemplate::new(500).set_body_string("private verification failure")
            } else {
                ResponseTemplate::new(200).set_body_string(format!(
                    "{{\"Keys\":{{\"{root}\":{{\"Type\":\"recursive\"}}}}}}"
                ))
            })
            .mount(&server)
            .await;

        let state = test_state(&server).await;
        let id = format!("stage4-post-put-{}", failed_rpc.replace('/', "-"));
        let (job, claim) = claimed_stage4_job(&state, &id, true, &archive).await;
        decompress_import(
            &state,
            &job,
            artifact(archive.len()),
            &claim,
            CancellationToken::new(),
        )
        .await
        .expect("root failure must not fail the ordinary import");
        let snapshot = crate::store::zip::snapshot(state.store.db(), &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.batch.state, "published", "{failed_rpc}");
        assert_eq!(snapshot.batch.root_status, "failed", "{failed_rpc}");
        assert_eq!(
            snapshot.batch.root_error_code.as_deref(),
            Some("directory_build_failed")
        );
        assert!(snapshot.batch.root_cid.is_none());
        assert_eq!(snapshot.references.len(), 1, "{failed_rpc}");
        assert_eq!(snapshot.references[0].cid, root);
        assert_eq!(snapshot.references[0].node_identity, "localnode");
        assert_eq!(snapshot.references[0].state, "retained");
        assert!(snapshot.references[0].verification_receipt.is_none());
        assert_eq!(snapshot.entries.len(), 1);
        assert!(snapshot.entries[0].version_row_id.is_some());
        assert_eq!(
            object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            import_job_result::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            import_job::Entity::find_by_id(&id)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "completed"
        );
    }
}

#[tokio::test]
async fn stage4_shutdown_during_root_pin_retains_candidate_without_publication() {
    use wiremock::matchers::query_param;

    const LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let root =
        cid::Cid::new_v1(0x70, LEAF.parse::<cid::Cid>().unwrap().hash().to_owned()).to_string();
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{LEAF}\",\"Size\":\"5\"}}\n")),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", LEAF))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{LEAF}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{LEAF}\",\"CumulativeSize\":5}}")),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"ID\":\"localnode\"}"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Cid\":{{\"/\":\"{root}\"}}}}")),
        )
        .mount(&server)
        .await;
    let reached_pin = Arc::new(tokio::sync::Notify::new());
    let notify = reached_pin.clone();
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", root.clone()))
        .respond_with(move |_: &wiremock::Request| {
            notify.notify_one();
            ResponseTemplate::new(200)
                .set_body_string("{\"Pins\":[]}")
                .set_delay(Duration::from_secs(2))
        })
        .mount(&server)
        .await;

    let state = test_state(&server).await;
    let (job, claim) = claimed_stage4_job(&state, "stage4-shutdown-root", true, &archive).await;
    let shutdown = CancellationToken::new();
    let execution_state = state.clone();
    let execution_shutdown = shutdown.clone();
    let execution = tokio::spawn(async move {
        decompress_import(
            &execution_state,
            &job,
            artifact(archive.len()),
            &claim,
            execution_shutdown,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), reached_pin.notified())
        .await
        .expect("root pin started after root CID was emitted");
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(3), execution)
        .await
        .expect("shutdown waits for root build ownership")
        .expect("import task joins");
    assert!(matches!(result, Err(ImportExecutionError::Interrupted)));
    let snapshot = crate::store::zip::snapshot(state.store.db(), "stage4-shutdown-root")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "open");
    assert!(snapshot.batch.root_cid.is_none());
    assert_eq!(snapshot.builds.len(), 1);
    assert_eq!(snapshot.builds[0].status, "unknown");
    assert_eq!(snapshot.references.len(), 1);
    assert_eq!(snapshot.references[0].cid, root);
    assert_eq!(snapshot.references[0].state, "retained");
    assert!(snapshot.references[0].verification_receipt.is_none());
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
async fn stage4_reclaimed_worker_reuses_exact_prepared_manifest() {
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    mount_entry_success(&server).await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_stage4_job(&state, "stage4-resume", true, &archive).await;
    let db = state.store.db();
    struct PreparedBoundary(tokio::sync::Notify);
    #[async_trait::async_trait]
    impl crate::import::pipeline::ImportExecutionObserver for PreparedBoundary {
        async fn before_publication(&self, _: &str) {
            self.0.notify_one();
            std::future::pending::<()>().await;
        }
    }
    let boundary = Arc::new(PreparedBoundary(tokio::sync::Notify::new()));
    let cancellation = JobCancellation {
        shutdown: CancellationToken::new(),
        ownership_lost: CancellationToken::new(),
    };
    let first_state = state.clone();
    let first_job = job.clone();
    let first_claim = claim.clone();
    let first_cancel = cancellation.clone();
    let first_boundary = boundary.clone();
    let mut first = tokio::spawn(async move {
        let reporter = ProgressReporter::start(
            first_state.clone(),
            &first_job,
            first_claim.clone(),
            first_cancel.clone(),
            Duration::from_millis(5),
        );
        let result = decompress_import_with_context(
            &first_state,
            &first_job,
            artifact(archive.len()),
            &first_claim,
            &first_cancel,
            &reporter,
            MAX_DECOMPRESSED_ARCHIVE_BYTES,
            first_boundary.as_ref(),
        )
        .await;
        let _ = reporter.finish(&first_cancel).await;
        result
    });
    tokio::select! {
        result = &mut first => panic!("valid first attempt did not freeze: {result:?}"),
        _ = boundary.0.notified() => {}
    }
    assert!(
        crate::store::zip::read(db, &job.id)
            .await
            .unwrap()
            .unwrap()
            .manifest_prepared
    );
    cancellation.shutdown.cancel();
    assert!(matches!(
        first.await.unwrap(),
        Err(ImportExecutionError::Interrupted)
    ));
    let now = Utc::now();
    jobs::retry(
        db,
        &job.id,
        &claim.worker_id,
        claim.claim_epoch,
        claim.attempt,
        now,
        &ImportFailure {
            code: ImportFailureCode::SourceUnreachable,
            message: "prior worker stopped".into(),
            retryable: true,
        },
        now,
    )
    .await
    .unwrap();
    let reclaimed = jobs::claim_due(
        db,
        "reclaimed-worker",
        now,
        now + chrono::Duration::seconds(60),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert!(reclaimed.claim.claim_epoch > claim.claim_epoch);
    let config = crate::import::ImportConfig::default().validate().unwrap();
    let coordinator = crate::import::pipeline::ImportCoordinator::new(
        config.clone(),
        crate::import::downloader::SourceDownloader::production(Arc::new(config)),
    );
    crate::import::pipeline::execute_job(
        coordinator,
        state.clone(),
        reclaimed.job,
        reclaimed.claim,
        JobCancellation {
            shutdown: CancellationToken::new(),
            ownership_lost: CancellationToken::new(),
        },
    )
    .await
    .unwrap();
    let snapshot = crate::store::zip::snapshot(db, &job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(snapshot.batch.root_status, "failed");
    assert!(snapshot.entries[0].version_row_id.is_some());
    assert_eq!(object::Entity::find().count(db).await.unwrap(), 2);
}

#[tokio::test]
async fn stage4_stale_guard_blocks_root_side_effect_and_all_publication() {
    struct SupersedingObserver<'a> {
        state: Arc<AppState>,
        server: &'a MockServer,
        baseline: tokio::sync::Mutex<Option<(UnpublishedAdmission, usize)>>,
    }
    #[async_trait::async_trait]
    impl crate::import::pipeline::ImportExecutionObserver for SupersedingObserver<'_> {
        async fn before_publication(&self, id: &str) {
            let baseline = UnpublishedAdmission::capture(&self.state, id).await;
            let calls = self.server.received_requests().await.unwrap().len();
            *self.baseline.lock().await = Some((baseline, calls));
            ownership::admit_content_mutation(
                self.state.store.db(),
                "bucket",
                "out/file.txt",
                None,
                SupersedeReason::PutObject,
                Utc::now(),
            )
            .await
            .unwrap();
        }
    }
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    mount_entry_success(&server).await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_stage4_job(&state, "stage4-stale", true, &archive).await;
    let intake = UnpublishedAdmission::capture(&state, &job.id).await;
    assert!(!intake.snapshot.batch.manifest_prepared);
    let observer = SupersedingObserver {
        state: state.clone(),
        server: &server,
        baseline: tokio::sync::Mutex::new(None),
    };
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
    let result = decompress_import_with_context(
        &state,
        &job,
        artifact(archive.len()),
        &claim,
        &cancellation,
        &reporter,
        MAX_DECOMPRESSED_ARCHIVE_BYTES,
        &observer,
    )
    .await;
    let _ = reporter.finish(&cancellation).await;
    assert!(matches!(result, Err(ImportExecutionError::Superseded)));
    let (baseline, calls) = observer
        .baseline
        .lock()
        .await
        .take()
        .expect("stale guard was triggered");
    assert_eq!(baseline.proof, intake.proof);
    let mut prepared_options: serde_json::Value =
        serde_json::from_str(&baseline.snapshot.batch.captured_options).unwrap();
    assert!(
        prepared_options
            .as_object_mut()
            .unwrap()
            .remove("legacy_prepared_import")
            .is_some()
    );
    assert_eq!(
        prepared_options,
        serde_json::from_str::<serde_json::Value>(&intake.snapshot.batch.captured_options).unwrap()
    );
    assert!(baseline.snapshot.batch.manifest_prepared);
    assert_eq!(baseline.snapshot.entries.len(), 1);
    baseline.assert_unchanged(&state, &job.id).await;
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        calls,
        "stale guard permits no subsequent local RPC"
    );
    server.verify().await;
    assert_eq!(
        object::Entity::find()
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
            .all(|request| request.url.path() != "/api/v0/dag/put")
    );
}

#[tokio::test]
async fn stage4_duplicate_destination_retains_only_intake_proof_without_publication_or_root() {
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO), ("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    mount_entry_success(&server).await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_stage4_job(&state, "stage4-duplicate", true, &archive).await;
    let baseline = UnpublishedAdmission::capture(&state, &job.id).await;
    assert!(!baseline.snapshot.batch.manifest_prepared);
    assert!(baseline.snapshot.entries.is_empty());
    let failure = decompress_import(
        &state,
        &job,
        artifact(archive.len()),
        &claim,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        failure,
        ImportExecutionError::Terminal(ImportFailure {
            code: ImportFailureCode::InvalidArchive,
            ..
        })
    ));
    baseline.assert_unchanged(&state, &job.id).await;
    server.verify().await;
    assert_eq!(
        object::Entity::find()
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
            .all(|request| request.url.path() != "/api/v0/dag/put")
    );
}

struct UnpublishedAdmission {
    snapshot: crate::store::zip::BatchSnapshot,
    proof: crate::store::pinning::publication::LegacyZipInputProof,
}

impl UnpublishedAdmission {
    async fn capture(state: &AppState, id: &str) -> Self {
        let snapshot = crate::store::zip::snapshot(state.store.db(), id)
            .await
            .unwrap()
            .unwrap();
        let proof =
            crate::store::pinning::publication::read_legacy_import_input(state.store.db(), id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(snapshot.batch.state, "open");
        assert!(!snapshot.batch.source_published);
        assert!(snapshot.batch.terminal_result.is_none());
        assert_eq!(snapshot.batch.root_status, "pending");
        assert!(snapshot.batch.root_cid.is_none());
        assert!(snapshot.batch.root_error_code.is_none());
        assert_eq!(snapshot.batch.root_epoch, 0);
        assert_eq!(snapshot.batch.root_revision, 0);
        assert!(snapshot.builds.is_empty());
        assert!(snapshot.references.is_empty());
        assert!(
            snapshot
                .entries
                .iter()
                .all(|entry| entry.version_row_id.is_none())
        );
        use crate::store::entities::{
            object_version, pin_extension_decision, pin_job, pin_lease_target, pin_provider_usage,
            remote_pin, remote_pin_ledger,
        };
        let counts = [
            object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            object_version::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            import_job_result::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            pin_extension_decision::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            pin_lease::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            pin_lease_target::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            pin_job::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            remote_pin::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            remote_pin_ledger::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            pin_provider_usage::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
        ];
        assert_eq!(
            counts, [0; 10],
            "no published objects/versions/results/decisions, leases/targets/jobs, remote or quota rows"
        );
        Self { snapshot, proof }
    }

    async fn assert_unchanged(&self, state: &AppState, id: &str) {
        let after = Self::capture(state, id).await;
        assert_eq!(
            after.proof, self.proof,
            "native full input proof must survive rejection unchanged"
        );
        assert_eq!(
            after.snapshot.batch, self.snapshot.batch,
            "reject must not rewrite captured admission or terminal/root state"
        );
        assert_eq!(
            after.snapshot.entries, self.snapshot.entries,
            "reject must not publish manifest bindings"
        );
        assert_eq!(after.snapshot.builds, self.snapshot.builds);
        assert_eq!(after.snapshot.references, self.snapshot.references);
    }
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

pub(crate) async fn mount_verified_directory(server: &MockServer, leaf: &str, root: &str) {
    use wiremock::matchers::query_param;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{leaf}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{leaf}\",\"CumulativeSize\":5}}")),
        )
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Cid\":{{\"/\":\"{root}\"}}}}")),
        )
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", root))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{root}\"]}}")),
        )
        .with_priority(1)
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/resolve"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Path\":\"/ipfs/{leaf}\"}}")),
        )
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"ID\":\"localnode\"}"))
        .expect(3)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Keys\":{{\"{root}\":{{\"Type\":\"recursive\"}}}}}}"
        )))
        .expect(2)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{root}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Hash\":\"{root}\",\"WithLocality\":true,\"Local\":true}}"
        )))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn stage4_verified_root_is_bound_to_published_version_and_partial_failure_is_retained() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO), ("failed.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    const LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let root =
        cid::Cid::new_v1(0x70, LEAF.parse::<cid::Cid>().unwrap().hash().to_owned()).to_string();
    let adds = Arc::new(AtomicUsize::new(0));
    let add_calls = adds.clone();
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(move |_: &wiremock::Request| {
            if add_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Hash\":\"{LEAF}\",\"Size\":\"5\"}}\n"))
            } else {
                ResponseTemplate::new(500)
            }
        })
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    mount_verified_directory(&server, LEAF, &root).await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_stage4_job(&state, "stage4-partial", true, &archive).await;
    decompress_import(
        &state,
        &job,
        artifact(archive.len()),
        &claim,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let snapshot = crate::store::zip::snapshot(state.store.db(), "stage4-partial")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot.batch.root_status,
        "partial",
        "root error {:?}; requests {:?}",
        snapshot.batch.root_error_code,
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|request| request.url.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(snapshot.batch.root_cid.as_deref(), Some(root.as_str()));
    assert_eq!(snapshot.entries.len(), 2);
    assert_eq!(snapshot.references.len(), 1);
    assert_eq!(snapshot.references[0].state, "adopted");
    assert!(snapshot.references[0].verification_receipt.is_some());
    assert!(
        snapshot
            .entries
            .iter()
            .any(|entry| entry.cid.is_some() && entry.version_row_id.is_some())
    );
    assert!(
        snapshot
            .entries
            .iter()
            .any(|entry| entry.error_code.is_some() && entry.version_row_id.is_none())
    );
}

#[tokio::test]
async fn successful_import_publishes_archive_entry_results_and_progress_atomically() {
    let server = MockServer::start().await;
    let archive = zip(&[("file.txt", HELLO)]);
    mount_archive(&server, archive.clone()).await;
    mount_entry_success(&server).await;
    let state = test_state(&server).await;
    let (job, claim) = claimed_historical_job(&state, "success", &archive).await;

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
    assert!(completed.root_capture_json.is_none());
    assert!(completed.zip_intent_json.is_none());
    assert_eq!(completed.source_type, "cid");
    assert_eq!(completed.source_value, ARCHIVE_CID);
    let original: crate::pinning::decision::ExtensionDecision =
        serde_json::from_str(completed.pin_decision_json.as_deref().unwrap()).unwrap();
    assert!(original.effective_intents.is_empty());
    assert_eq!(original.origin.principal_id, "test");
    assert_eq!(original.origin.request_id, "success");
    let snapshot = crate::store::zip::snapshot(state.store.db(), "success")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(
        snapshot.batch.root_status, "disabled",
        "a pre-Stage4 job cannot inherit the new default root"
    );
    assert!(snapshot.batch.source_published);
    assert!(snapshot.batch.manifest_prepared);
    assert!(snapshot.batch.root_cid.is_none());
    assert_eq!(snapshot.batch.root_epoch, 0);
    assert_eq!(snapshot.batch.root_revision, 0);
    assert!(snapshot.builds.is_empty());
    assert!(snapshot.references.is_empty());
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(snapshot.entries[0].cid.as_deref(), Some(ENTRY_CID));
    assert!(snapshot.entries[0].version_row_id.is_some());
    assert_eq!(
        objects
            .iter()
            .find(|row| row.key == "archive.zip")
            .unwrap()
            .cid,
        ARCHIVE_CID
    );
    assert_eq!(
        objects
            .iter()
            .find(|row| row.key == "out/file.txt")
            .unwrap()
            .cid,
        ENTRY_CID
    );
    assert_eq!(rows[0].cid.as_deref(), Some(ARCHIVE_CID));
    assert_eq!(rows[1].cid.as_deref(), Some(ENTRY_CID));
    use crate::store::entities::{object_version, pin_job, pin_lease_target, pin_provider_usage};
    let versions = object_version::Entity::find()
        .all(state.store.db())
        .await
        .unwrap();
    assert_eq!(versions.len(), 2);
    for object in &objects {
        let version = versions
            .iter()
            .find(|version| version.key == object.key)
            .unwrap();
        assert_eq!(version.bucket, object.bucket);
        assert_eq!(version.object_id.as_deref(), Some(object.id.as_str()));
        assert!(version.is_latest);
        if object.key == "archive.zip" {
            assert_eq!(published.object_id, object.id);
        } else {
            assert_eq!(
                snapshot.entries[0].version_row_id.as_deref(),
                Some(version.id.as_str())
            );
            assert_eq!(
                snapshot.entries[0].object_key.as_deref(),
                Some(object.key.as_str())
            );
        }
    }
    assert_eq!(
        pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    let requests = server.received_requests().await.unwrap();
    assert!(requests.iter().all(|request| matches!(
        request.url.path(),
        "/api/v0/cat" | "/api/v0/add" | "/api/v0/pin/add"
    )));
    assert!(
        requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/pin/add")
            .all(|request| request
                .url
                .query_pairs()
                .any(|(name, value)| name == "arg" && value == ENTRY_CID))
    );
    server.verify().await;
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
    let (job, claim) =
        claimed_job(&state, "observer-cancel", "archive.zip", "out/", &archive).await;
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
    let (job, claim) = claimed_job(&state, "collision", "out/file.txt", "out/", &archive).await;

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
    let (job, claim) = claimed_job(&state, "entry-failure", "archive.zip", "out/", &archive).await;

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
        let (job, claim) = claimed_job(&state, id, "archive.zip", "out/", &body).await;
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
    let (job, claim) = claimed_job(&state, "canceled", "archive.zip", "out/", &archive).await;
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
    let (job, claim) = claimed_job(&state, "prefix-lost", "archive.zip", "out/", &archive).await;
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
    let archive = zip(&[("file.txt", HELLO)]);
    let (job, claim) = claimed_job(&state, "generation", "archive.zip", "out/", &archive).await;
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
    let (job, claim) = claimed_job(&state, "superseded", "archive.zip", "out/", &archive).await;
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
    let (job, claim) = claimed_job(
        &state,
        "cat-stream-failure",
        "archive.zip",
        "out/",
        &archive,
    )
    .await;
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
