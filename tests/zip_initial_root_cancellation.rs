//! Actual initial ZIP v2 builders canceled while the root recursive pin is inflight.
#[allow(dead_code)]
mod support;

use std::{collections::HashMap, sync::Arc, time::Duration};

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    import::{ImportConfig, downloader::SourceDownloader, pipeline::ImportCoordinator},
    state::AppState,
    store::{self, zip},
};
use sea_orm::{ConnectionTrait, Statement};
use support::decompress::{
    AddReply, KuboScript, S3ServerHandle, legal_single_entry_zip, start_kubo_harness,
    start_s3_server_with_imports,
};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const ROOT: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";
const BUCKET: &str = "test-bkt";

// These long root-pin gates share the directory builder's four process-wide
// slots. Admit fixtures before starting any job/lease/deadline, so the five-
// second root gate measures this build rather than another test's slot queue.
static ROOT_FIXTURE_SLOTS: Semaphore = Semaphore::const_new(4);

struct Rig {
    state: Arc<AppState>,
    imports: Arc<ImportCoordinator>,
    kubo: MockServer,
    server: S3ServerHandle,
    _root_slot: SemaphorePermit<'static>,
}

async fn rig(job_timeout_secs: u64) -> Rig {
    let root_slot = ROOT_FIXTURE_SLOTS.acquire().await.unwrap();
    let archive = legal_single_entry_zip();
    let kubo = start_kubo_harness(KuboScript {
        add_replies: vec![AddReply::Ok(LEAF), AddReply::Ok(LEAF)],
        cat_bodies: HashMap::from([(LEAF.into(), archive)]),
    })
    .await;
    let cfg: Config = toml::from_str(&format!(
        r#"
        [kubo]
        rpc_url = {:?}
        [storage]
        database_url = "sqlite::memory:"
        [decompress_zip]
        unixfs_directory_root = true
        [[pinning.providers]]
        name = "alpha"
        kind = "noop"
        priority = 1
        max_bytes = 1000000
        max_pins = 100
        [[pinning.policies]]
        bucket = "test-bkt"
        prefix = "out/"
        trigger = "always"
        provider_mode = "one"
        providers = ["alpha"]
        default_duration = "1h"
        max_duration = "2h"
        allow_decompressed = true
        "#,
        kubo.server.uri()
    ))
    .unwrap();
    let state = AppState::new(&cfg).await.unwrap();
    store::bucket::create(state.store.db(), BUCKET, None)
        .await
        .unwrap();
    let validated = ImportConfig {
        worker_concurrency: 1,
        poll_interval_ms: 10,
        lease_duration_secs: 2,
        job_timeout_secs,
        max_attempts: 1,
        ..ImportConfig::default()
    }
    .validate()
    .unwrap();
    let imports = ImportCoordinator::new(
        validated.clone(),
        SourceDownloader::production(Arc::new(validated)),
    );
    let server = start_s3_server_with_imports(
        state.clone(),
        Arc::new(tokio::sync::Mutex::new(Vec::new())),
        imports.clone(),
    )
    .await;
    for (endpoint, body) in [
        ("id", "{\"ID\":\"initialRootNode\"}".to_owned()),
        (
            "files/stat",
            format!("{{\"Hash\":\"{LEAF}\",\"CumulativeSize\":18}}"),
        ),
        ("dag/put", format!("{{\"Cid\":{{\"/\":\"{ROOT}\"}}}}")),
    ] {
        Mock::given(method("POST"))
            .and(path(format!("/api/v0/{endpoint}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&kubo.server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", LEAF))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{LEAF}\"]}}")),
        )
        .with_priority(1)
        .mount(&kubo.server)
        .await;
    // An asynchronous gate, not a blocking Respond closure: the request log,
    // heartbeat and successor DB actor remain live while pin/add cannot finish.
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", ROOT))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Pins\":[\"{ROOT}\"]}}"))
                .set_delay(Duration::from_secs(60)),
        )
        .with_priority(1)
        .mount(&kubo.server)
        .await;
    Rig {
        state,
        imports,
        kubo: kubo.server,
        server,
        _root_slot: root_slot,
    }
}

fn controls(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "extracted"),
        ("x-ipfs3-zip-token", token),
        ("x-amz-tagging", "ipfs-s3%3Azip-root=true"),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    headers
}

async fn wait_for_root_pin(rig: &Rig) -> zip::BatchSnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let requests = rig.kubo.received_requests().await.unwrap();
            if requests.iter().any(|request| {
                request.url.path() == "/api/v0/pin/add"
                    && request
                        .url
                        .query_pairs()
                        .any(|(key, value)| key == "arg" && value == ROOT)
            }) {
                let row = rig
                    .state
                    .store
                    .db()
                    .query_one(Statement::from_string(
                        rig.state.store.db().get_database_backend(),
                        "SELECT id FROM zip_batches".to_owned(),
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                let id: String = row.try_get("", "id").unwrap();
                return zip::snapshot(rig.state.store.db(), &id)
                    .await
                    .unwrap()
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real builder reached root pin after clean dag/put EOF")
}

async fn take_over_root(rig: &Rig, old: &zip::BatchSnapshot) -> zip::RootClaim {
    let db = rig.state.store.db();
    db.execute_unprepared("UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z'")
        .await
        .unwrap();
    zip::claim_root(db, &old.batch.id, "new-root-owner", 60)
        .await
        .unwrap()
}

async fn assert_retained_only(
    rig: &Rig,
    old: &zip::BatchSnapshot,
    takeover: Option<&zip::RootClaim>,
) {
    let snapshot = zip::snapshot(rig.state.store.db(), &old.batch.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot.references.len(),
        1,
        "canceled initial root lost its known candidate"
    );
    let retained = &snapshot.references[0];
    assert_eq!(retained.revision, old.batch.root_revision);
    assert_eq!(retained.epoch, old.batch.root_epoch);
    assert_eq!(retained.cid, ROOT);
    assert_eq!(retained.node_identity, "initialRootNode");
    assert_eq!(retained.tier, "hot");
    assert_eq!(retained.state, "retained");
    assert!(retained.verification_receipt.is_none());
    assert_eq!(snapshot.batch.state, "open");
    assert!(snapshot.batch.root_cid.is_none());
    assert!(!snapshot.batch.source_published);
    assert_eq!(snapshot.entries, old.entries);
    assert_eq!(snapshot.builds[0].status, "invoked");
    if let Some(new) = takeover {
        assert_eq!(snapshot.batch.root_epoch, new.epoch);
        assert_eq!(snapshot.builds.len(), 2);
        assert_eq!(snapshot.builds[1].worker, new.worker);
        assert_eq!(snapshot.builds[1].status, "intent");
        let original = zip::RootClaim {
            batch_id: old.batch.id.clone(),
            revision: old.batch.root_revision,
            epoch: old.batch.root_epoch,
            worker: old.builds[0].worker.clone(),
        };
        assert!(
            zip::verify_root(
                rig.state.store.db(),
                &original,
                "initialRootNode",
                "hot",
                ROOT,
                "{}"
            )
            .await
            .is_err(),
            "superseded root actor must not verify/adopt into the new epoch"
        );
    } else {
        assert_eq!(snapshot.builds.len(), 1);
        assert_eq!(snapshot.builds[0].worker, old.builds[0].worker);
        assert_eq!(snapshot.builds[0].error_code, old.builds[0].error_code);
        assert!(
            snapshot.builds[0].lease_until >= old.builds[0].lease_until,
            "only the pre-cancellation heartbeat may extend the lease; cancellation must not release it"
        );
    }
    for table in [
        "objects",
        "object_versions",
        "pin_jobs",
        "remote_pins",
        "pin_leases",
    ] {
        let row = rig
            .state
            .store
            .db()
            .query_one(Statement::from_string(
                rig.state.store.db().get_database_backend(),
                format!("SELECT COUNT(*) AS total FROM {table}"),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.try_get::<i64>("", "total").unwrap(),
            0,
            "canceled actor must not publish {table}"
        );
    }
    let requests = rig.kubo.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.url.path() == "/api/v0/dag/put")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.url.path() == "/api/v0/pin/add"
                && r.url.query_pairs().any(|(k, v)| k == "arg" && v == ROOT))
            .count(),
        1
    );
    assert!(
        !requests.iter().any(
            |r| ["/api/v0/resolve", "/api/v0/pin/ls", "/api/v0/pin/rm"].contains(&r.url.path())
        )
    );
}

#[derive(Clone, Copy)]
enum ImportStop {
    Shutdown,
    LostExecution,
    LostRoot,
    Timeout,
}

async fn import_cancellation(stop: ImportStop, takeover: bool) {
    let rig = rig(if matches!(stop, ImportStop::Timeout) {
        2
    } else {
        30
    })
    .await;
    let mut headers = controls("initial-import-root");
    headers.insert("content-type", HeaderValue::from_static("application/xml"));
    headers.insert(
        "x-ipfs3-client-token",
        HeaderValue::from_static("initial-import-root"),
    );
    let accepted = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &rig.server.endpoint,
        BUCKET,
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        format!("<IPFS3ImportRequest><CID>{LEAF}</CID></IPFS3ImportRequest>").into_bytes(),
        headers,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let shutdown = CancellationToken::new();
    let worker = rig.imports.start(rig.state.clone(), shutdown.clone());
    let mut old = wait_for_root_pin(&rig).await;
    assert_eq!(old.builds[0].status, "invoked");
    let new = if takeover {
        Some(take_over_root(&rig, &old).await)
    } else {
        None
    };
    match stop {
        ImportStop::Shutdown => shutdown.cancel(),
        ImportStop::LostExecution => {
            rig.state
                .store
                .db()
                .execute_unprepared("UPDATE zip_v2_executions SET worker='successor',epoch=epoch+1")
                .await
                .unwrap();
        }
        ImportStop::LostRoot => {
            rig.state
                .store
                .db()
                .execute_unprepared("UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z'")
                .await
                .unwrap();
            old = zip::snapshot(rig.state.store.db(), &old.batch.id)
                .await
                .unwrap()
                .unwrap();
        }
        ImportStop::Timeout => {}
    }
    if !matches!(stop, ImportStop::Shutdown) {
        // Max-attempt timeout is fenced; renewal loss cancels without publication.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = zip::snapshot(rig.state.store.db(), &old.batch.id)
                    .await
                    .unwrap()
                    .unwrap();
                let execution = zip::execution::read(rig.state.store.db(), &old.batch.id)
                    .await
                    .unwrap()
                    .unwrap();
                if !snapshot.references.is_empty() || execution.state == "fenced" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("canceled/expired import attempt stopped within its bound");
    }
    worker.shutdown(Duration::from_secs(2)).await;
    assert_retained_only(&rig, &old, new.as_ref()).await;
    rig.server.shutdown().await;
}

#[tokio::test]
async fn import_shutdown_retains_inflight_initial_root_without_publication() {
    import_cancellation(ImportStop::Shutdown, false).await;
}

#[tokio::test]
async fn import_lost_execution_retains_only_original_root_claim() {
    import_cancellation(ImportStop::LostExecution, false).await;
}

#[tokio::test]
async fn import_lost_root_renewal_retains_only_original_candidate_without_publication() {
    import_cancellation(ImportStop::LostRoot, false).await;
}

#[tokio::test]
async fn import_job_timeout_retains_inflight_candidate_without_publication() {
    import_cancellation(ImportStop::Timeout, false).await;
}

#[tokio::test]
async fn import_shutdown_after_root_takeover_cannot_adopt_or_register_for_successor() {
    import_cancellation(ImportStop::Shutdown, true).await;
}

async fn handler_loss(mpu: bool, takeover: bool) {
    let rig = rig(30).await;
    let query;
    let body;
    let headers;
    let upload;
    if mpu {
        let create = support::sigv4::send_sigv4(
            reqwest::Method::POST,
            &rig.server.endpoint,
            BUCKET,
            "archive.zip",
            &[("uploads", ""), ("decompress-zip", "out/")],
            vec![],
            controls("initial-mpu-root"),
            "test",
        )
        .await;
        assert_eq!(create.status(), StatusCode::OK);
        let xml = create.text().await.unwrap();
        upload = xml
            .split("<UploadId>")
            .nth(1)
            .unwrap()
            .split("</UploadId>")
            .next()
            .unwrap()
            .to_owned();
        store::multipart::upsert_part(
            rig.state.store.db(),
            &upload,
            1,
            LEAF,
            legal_single_entry_zip().len() as i64,
            LEAF,
        )
        .await
        .unwrap();
        query = vec![("uploadId", upload.as_str())];
        body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{LEAF}\"</ETag></Part></CompleteMultipartUpload>").into_bytes();
        headers = HeaderMap::new();
    } else {
        upload = String::new();
        query = vec![("decompress-zip", "out/")];
        body = legal_single_entry_zip();
        headers = controls("initial-direct-root");
    }
    let endpoint = rig.server.endpoint.clone();
    let request = support::sigv4::send_sigv4(
        if mpu {
            reqwest::Method::POST
        } else {
            reqwest::Method::PUT
        },
        &endpoint,
        BUCKET,
        "archive.zip",
        &query,
        body,
        headers,
        "test",
    );
    tokio::pin!(request);
    let old = tokio::select! {
        snapshot = wait_for_root_pin(&rig) => snapshot,
        response = &mut request => panic!("handler returned before root gate: {}", response.status()),
    };
    let new = if takeover {
        Some(take_over_root(&rig, &old).await)
    } else {
        None
    };
    rig.state
        .store
        .db()
        .execute_unprepared("UPDATE zip_v2_executions SET worker='successor',epoch=epoch+1")
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(12), &mut request)
        .await
        .expect("lease loss stops handler without waiting for gated root pin");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_retained_only(&rig, &old, new.as_ref()).await;
    if mpu {
        assert!(
            store::multipart::get_upload(rig.state.store.db(), &upload)
                .await
                .is_ok(),
            "a canceled complete must not consume its MPU upload"
        );
    }
    rig.server.shutdown().await;
}

#[tokio::test]
async fn direct_lost_execution_retains_inflight_initial_root_without_publication() {
    handler_loss(false, false).await;
}

#[tokio::test]
async fn mpu_lost_execution_retains_inflight_initial_root_without_publication() {
    handler_loss(true, false).await;
}

#[tokio::test]
async fn direct_root_takeover_rejects_old_actor_and_preserves_original_candidate() {
    handler_loss(false, true).await;
}

#[tokio::test]
async fn mpu_root_takeover_rejects_old_actor_and_preserves_original_candidate() {
    handler_loss(true, true).await;
}
