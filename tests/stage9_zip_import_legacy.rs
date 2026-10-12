//! Real signed legacy HTTP intake plus native proof/prepared recovery.
#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
    pub mod stage9_zip_replay;
    pub mod stage9_zip_replay_requests;
    pub mod stage9_zip_replay_source;
}
use chrono::Utc;
use futures_util::FutureExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    import::{
        ImportConfig,
        downloader::{
            AuthorizedSource, DownloadError, DownloadLimits, DownloadStream, ImportHttpTransport,
            SourceDownloader, StrictPublicAddressPolicy,
        },
        pipeline::{ImportCoordinator, ImportExecutionObserver, JobCancellation, execute_job},
        v2_source::fetch_v2_zip_source,
    },
    pinning::zip_policy::CapturedZipIntent,
    store::{
        entities::{import_job, object, object_version, pin_lease},
        import::jobs,
        pinning::publication::{bind_legacy_import_input, read_legacy_import_input},
        zip::{self, BatchAdmission},
    },
    zip::extract::ZipExtractionLimits,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, Statement};
use sha2::{Digest, Sha256};
use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::{
    decompress::S3TestEndpoint,
    sigv4::send_sigv4,
    stage9_zip_replay::{ARCHIVE, BUCKET, Entry, Fixture, KEY, OUTPUT, Outcome, run},
    stage9_zip_replay_requests::{self as request, Ledger},
    stage9_zip_replay_source::{ORIGIN, URL},
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path, query_param},
};

async fn submit(h: &Fixture, object_type: Option<&'static str>) -> String {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    if let Some(object_type) = object_type {
        headers.insert(
            "x-ipfs3-object-content-type",
            HeaderValue::from_static(object_type),
        );
    }
    let response = send_sigv4(
        reqwest::Method::POST,
        h.endpoint(),
        BUCKET,
        KEY,
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        request::url_xml(URL).into_bytes(),
        headers,
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    response.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned()
}

async fn claim(h: &Fixture) -> ipfs_s3_gateway::store::import::jobs::ClaimedImportJob {
    let now = Utc::now();
    jobs::claim_due(
        h.state.store.db(),
        "before-crash",
        now,
        now + chrono::TimeDelta::seconds(60),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
}

async fn expire(h: &Fixture, id: &str) {
    h.state
        .store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE import_jobs SET locked_until='2000-01-01 00:00:00' WHERE id=?",
            [id.into()],
        ))
        .await
        .unwrap();
}

async fn terminal(h: &Fixture, id: &str) -> import_job::Model {
    let outcome = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let job = import_job::Entity::find_by_id(id)
                .one(h.state.store.db())
                .await
                .unwrap()
                .unwrap();
            if matches!(job.state.as_str(), "completed" | "failed" | "superseded") {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    match outcome {
        Ok(job) => job,
        Err(_) => {
            let job = import_job::Entity::find_by_id(id)
                .one(h.state.store.db())
                .await
                .unwrap()
                .unwrap();
            panic!(
                "legacy terminal observation: state={} phase={} attempts={} code={:?} message={:?}",
                job.state, job.phase, job.attempts, job.failure_code, job.failure_message
            );
        }
    }
}

struct GoneSource(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl ImportHttpTransport for GoneSource {
    async fn open(
        &self,
        _: AuthorizedSource,
        _: DownloadLimits,
        _: tokio::sync::watch::Sender<u64>,
        _: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(DownloadError::HttpStatus(404))
    }
}
fn no_source(h: &mut Fixture, calls: Arc<AtomicUsize>) {
    let config = ImportConfig {
        allowed_https_origins: vec![ORIGIN.into()],
        ..Default::default()
    }
    .validate()
    .unwrap();
    h.imports = ImportCoordinator::new(
        config.clone(),
        SourceDownloader::with_components(
            Arc::new(config),
            h.source.clone(),
            Arc::new(StrictPublicAddressPolicy),
            Arc::new(GoneSource(calls)),
        ),
    );
}

#[tokio::test]
async fn legacy_bound_restart_source_404_publishes_original_input_without_get() {
    run(Entry::Import, Outcome::Complete, |mut h| async move {
        let result = AssertUnwindSafe(async {
            let id = submit(&h, Some("application/zip")).await;
            let claimed = claim(&h).await;
            let capture = CapturedZipIntent::decode_json(claimed.job.zip_intent_json.as_deref().unwrap()).unwrap();
            zip::admit(h.state.store.db(), &BatchAdmission { id: id.clone(), owner: capture.origin().principal_id.clone(), source: "import".into(),
                token: id.clone(), fingerprint: claimed.job.request_fingerprint.clone(), bucket: BUCKET.into(), archive_key: KEY.into(), input_identity: id.clone(),
                captured_options: serde_json::json!({"zip_intent":capture,"target_prefix":"out/"}).to_string() }).await.unwrap();
            let artifact = fetch_v2_zip_source(&h.imports, &h.state, &serde_json::to_string(&("url",URL)).unwrap(), None, CancellationToken::new(), &ZipExtractionLimits::default()).await.unwrap();
            let proof = bind_legacy_import_input(h.state.store.db(), &claimed.claim, &artifact.sha256, &artifact.cid, artifact.size as i64).await.unwrap();
            assert_eq!(proof.sha256(), hex::encode(Sha256::digest(Outcome::Complete.archive())));
            assert_eq!(proof.size(), Outcome::Complete.archive().len() as i64);
            assert!(bind_legacy_import_input(h.state.store.db(), &claimed.claim, &"f".repeat(64), &artifact.cid, artifact.size as i64).await.is_err());
            let mut late = claimed.claim.clone(); late.claim_epoch += 1;
            assert!(bind_legacy_import_input(h.state.store.db(), &late, &artifact.sha256, &artifact.cid, artifact.size as i64).await.is_err());
            Ledger::read(&h).await.assert_unpublished();
            expire(&h, &id).await;
            h.restart(false).await;
            let calls = Arc::new(AtomicUsize::new(0)); no_source(&mut h, calls.clone()); h.start_worker();
            let job = terminal(&h, &id).await;
            assert_eq!(job.state, "completed", "{job:?}");
            assert_eq!(request::get(&h,"out/file.txt").await, (StatusCode::OK,b"single entry bytes".to_vec()));
            assert_eq!(read_legacy_import_input(h.state.store.db(), &id).await.unwrap().unwrap(), proof);
            assert_eq!(h.source.calls(), 1); assert_eq!(calls.load(Ordering::SeqCst), 0);
            let objects = object::Entity::find().all(h.state.store.db()).await.unwrap();
            let versions = object_version::Entity::find().all(h.state.store.db()).await.unwrap();
            let leases = pin_lease::Entity::find().all(h.state.store.db()).await.unwrap();
            assert_eq!(versions.len(), 2); assert_eq!(leases.len(), 1);
            assert!(leases.iter().all(|l| objects.iter().any(|o| o.id == l.owner_object_id && o.key == "out/file.txt")));
            assert_eq!(h.adds().await, 2);
            println!("R25_LEGACY_BOUND size={} sha256={} source_gets={} resumed_gets={} artifact_and_entry_adds={}", proof.size(),proof.sha256(),h.source.calls(),calls.load(Ordering::SeqCst),h.adds().await);
        }).catch_unwind().await;
        (h,result)
    }).await;
}

struct PreparedGate(tokio::sync::Notify);
#[async_trait::async_trait]
impl ImportExecutionObserver for PreparedGate {
    async fn before_publication(&self, _: &str) {
        self.0.notify_one();
        std::future::pending::<()>().await;
    }
}

#[tokio::test]
async fn legacy_prepared_restart_publishes_frozen_manifest_without_cat_or_extract() {
    run(Entry::Import, Outcome::Partial, |mut h| async move {
        let result = AssertUnwindSafe(async {
            let size = Outcome::Partial.archive().len() as i64;
            // This fixture reports real RPC progress; do not infer counters from
            // the artifact's final size when Kubo provides no progress records.
            h.kubo.reset().await;
            Mock::given(method("POST"))
                .and(path("/api/v0/cat"))
                .and(query_param("arg", ARCHIVE))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(Outcome::Partial.archive()))
                .mount(&h.kubo).await;
            let add_calls = Arc::new(AtomicUsize::new(0));
            Mock::given(method("POST"))
                .and(path("/api/v0/add"))
                .respond_with(move |_: &wiremock::Request| {
                    match add_calls.fetch_add(1, Ordering::SeqCst) {
                        0 => ResponseTemplate::new(200).set_body_string(format!(
                            "{{\"Bytes\":{size}}}\n{{\"Hash\":\"{ARCHIVE}\",\"Size\":\"{size}\"}}\n"
                        )),
                        1 => ResponseTemplate::new(200).set_body_string(format!(
                            "{{\"Hash\":\"{OUTPUT}\",\"Size\":\"17\"}}\n"
                        )),
                        _ => ResponseTemplate::new(500).set_body_string("entry refused"),
                    }
                })
                .mount(&h.kubo).await;
            Mock::given(method("POST"))
                .and(path("/api/v0/pin/add"))
                .and(query_param("arg", ARCHIVE))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    "{{\"Progress\":7,\"Bytes\":{size}}}\n{{\"Pins\":[\"{ARCHIVE}\"]}}\n"
                )))
                .with_priority(1)
                .mount(&h.kubo)
                .await;
            let id = submit(&h, Some("application/zip")).await;
            let claimed = claim(&h).await;
            let gate = Arc::new(PreparedGate(tokio::sync::Notify::new()));
            let config = ImportConfig {
                allowed_https_origins: vec![ORIGIN.into()],
                ..Default::default()
            }
            .validate()
            .unwrap();
            let imports = ImportCoordinator::new_with_observer(
                config.clone(),
                SourceDownloader::with_components(
                    Arc::new(config),
                    h.source.clone(),
                    Arc::new(StrictPublicAddressPolicy),
                    h.source.clone(),
                ),
                gate.clone(),
            );
            let cancellation = JobCancellation {
                shutdown: CancellationToken::new(),
                ownership_lost: CancellationToken::new(),
            };
            let mut running = tokio::spawn(execute_job(
                imports,
                h.state.clone(),
                claimed.job,
                claimed.claim,
                cancellation.clone(),
            ));
            tokio::select! {
                result = &mut running => panic!("worker returned before prepared boundary: {result:?}"),
                result = tokio::time::timeout(Duration::from_secs(10), gate.0.notified()) => result.expect("prepared boundary"),
            }
            let batch = zip::read(h.state.store.db(), &id).await.unwrap().unwrap();
            assert!(batch.manifest_prepared);
            assert!(batch.terminal_result.is_none());
            assert!(serde_json::from_str::<serde_json::Value>(&batch.captured_options).unwrap().get("legacy_prepared_import").is_some());
            let proof = read_legacy_import_input(h.state.store.db(), &id)
                .await
                .unwrap()
                .unwrap();
            let progress = import_job::Entity::find_by_id(&id)
                .one(h.state.store.db()).await.unwrap().unwrap();
            assert_eq!(progress.downloaded_bytes, size);
            assert_eq!(progress.download_total, Some(size));
            assert_eq!(progress.ipfs_add_bytes, size);
            assert_eq!(progress.pin_nodes_processed, 7);
            assert_eq!(progress.pin_bytes_processed, size);
            cancellation.ownership_lost.cancel();
            assert!(matches!(
                running.await.unwrap(),
                Err(ipfs_s3_gateway::import::ImportExecutionError::Superseded)
            ));
            Ledger::read(&h).await.assert_unpublished();
            let adds = h.adds().await;
            let cats = h
                .kubo
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.url.path() == "/api/v0/cat")
                .count();
            expire(&h, &id).await;
            h.restart(false).await;
            Mock::given(method("POST"))
                .and(path("/api/v0/cat"))
                .respond_with(ResponseTemplate::new(404))
                .with_priority(1)
                .mount(&h.kubo)
                .await;
            let calls = Arc::new(AtomicUsize::new(0));
            no_source(&mut h, calls.clone());
            h.start_worker();
            let job = terminal(&h, &id).await;
            assert_eq!(job.state, "completed", "{job:?}");
            assert_eq!(h.adds().await, adds);
            assert_eq!(
                h.kubo
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .filter(|r| r.url.path() == "/api/v0/cat")
                    .count(),
                cats
            );
            assert_eq!(h.source.calls(), 1);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let rows =
                ipfs_s3_gateway::store::import::results::page(h.state.store.db(), &id, None, 10)
                    .await
                    .unwrap()
                    .rows;
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].key, KEY);
            assert_eq!(rows[1].key, "out/first.txt");
            assert_eq!(rows[2].key, "out/second.txt");
            assert!(rows[2].error_message.is_some());
            let receipt = request::status(&h, &id, true).await;
            let ledger = Ledger::read(&h).await;
            h.restart(true).await;
            assert_eq!(request::status(&h, &id, true).await, receipt);
            assert_eq!(Ledger::read(&h).await, ledger);
            assert_eq!(
                read_legacy_import_input(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .unwrap(),
                proof
            );
        })
        .catch_unwind()
        .await;
        (h, result)
    })
    .await;
}

#[tokio::test]
async fn legacy_live_bind_restart_preserves_source_headers_without_get() {
    run(Entry::Import, Outcome::Complete, |mut h| async move {
        let result = AssertUnwindSafe(async {
            let blocked = Mock::given(method("POST"))
                .and(path("/api/v0/cat"))
                .and(query_param("arg", ARCHIVE))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_bytes(Outcome::Complete.archive())
                        .set_delay(Duration::from_secs(30)),
                )
                .with_priority(2)
                .mount_as_scoped(&h.kubo)
                .await;
            let id = submit(&h, None).await;
            h.start_worker();
            let proof = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(proof) = read_legacy_import_input(h.state.store.db(), &id)
                        .await
                        .unwrap()
                    {
                        break proof;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("live server proof bind");
            let batch = zip::read(h.state.store.db(), &id).await.unwrap().unwrap();
            assert!(!batch.manifest_prepared);
            assert!(batch.terminal_result.is_none());
            let capture: serde_json::Value = serde_json::from_str(&batch.captured_options).unwrap();
            assert_eq!(
                capture["legacy_input_metadata"]["content_type"],
                "application/zip"
            );
            assert_eq!(capture["legacy_input_metadata"]["sha256"], proof.sha256());
            Ledger::read(&h).await.assert_unpublished();
            expire(&h, &id).await;
            drop(blocked);
            h.restart(false).await;
            Mock::given(method("POST"))
                .and(path("/api/v0/cat"))
                .and(query_param("arg", ARCHIVE))
                .respond_with(
                    ResponseTemplate::new(200).set_body_bytes(Outcome::Complete.archive()),
                )
                .with_priority(1)
                .mount(&h.kubo)
                .await;
            let calls = Arc::new(AtomicUsize::new(0));
            no_source(&mut h, calls.clone());
            h.start_worker();
            let job = terminal(&h, &id).await;
            assert_eq!(job.state, "completed", "{:?}", job.failure_message);
            assert_eq!(h.source.calls(), 1);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                read_legacy_import_input(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .unwrap(),
                proof
            );
            let head = send_sigv4(
                reqwest::Method::HEAD,
                h.endpoint(),
                BUCKET,
                KEY,
                &[],
                Vec::new(),
                HeaderMap::new(),
                "test",
            )
            .await;
            assert_eq!(head.status(), StatusCode::OK);
            assert_eq!(
                head.headers()[http::header::CONTENT_TYPE],
                "application/zip"
            );
            assert_eq!(
                head.headers()[http::header::CONTENT_LENGTH]
                    .to_str()
                    .unwrap()
                    .parse::<i64>()
                    .unwrap(),
                proof.size()
            );
            assert_eq!(h.adds().await, 2);
        })
        .catch_unwind()
        .await;
        (h, result)
    })
    .await;
}
