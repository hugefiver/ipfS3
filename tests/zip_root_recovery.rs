use ipfs_s3_gateway::{
    kubo::KuboClient,
    store::{
        self,
        zip::{self, BatchAdmission, ManifestItem, RootOutcome, VersionBinding},
    },
    zip::recovery::{run_page, start_worker},
};
use sea_orm::{ConnectionTrait, TransactionTrait};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const ROOT: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";

async fn setup() -> (
    tempfile::TempDir,
    sea_orm::DatabaseConnection,
    sea_orm::DatabaseConnection,
) {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("retry.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let a = store::connect_database(&url).await.unwrap();
    store::run_migrations(&a).await.unwrap();
    let b = store::connect_database(&url).await.unwrap();
    store::bucket::create(&a, "bucket", None).await.unwrap();
    zip::admit(
        &a,
        &BatchAdmission {
            id: "batch".into(),
            owner: "owner".into(),
            source: "direct".into(),
            token: "token".into(),
            fingerprint: "fingerprint".into(),
            bucket: "bucket".into(),
            archive_key: "archive.zip".into(),
            input_identity: "input".into(),
            captured_options: "{\"root_enabled\":true,\"target_prefix\":\"out/\"}".into(),
        },
    )
    .await
    .unwrap();
    zip::prepare_manifest(
        &a,
        "batch",
        &[ManifestItem::Success {
            path: "child".into(),
            object_key: "out/child".into(),
            cid: LEAF.into(),
            size: 0,
        }],
    )
    .await
    .unwrap();
    a.execute_unprepared(&format!("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('old','bucket','out/child','{LEAF}',0,'{LEAF}')")).await.unwrap();
    a.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('old-version','bucket','out/child','object','old',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let tx = a.begin().await.unwrap();
    zip::publish(&tx, "batch", &[VersionBinding { path: "child".into(), version_row_id: "old-version".into() }], false,
        "{\"archive_cid\":\"original\",\"root_status\":\"failed\",\"root_warning\":\"directory_build_failed\",\"root_cid\":null}",
        RootOutcome::Failed { code: "directory_build_failed" }).await.unwrap();
    tx.commit().await.unwrap();
    a.execute_unprepared(
        "UPDATE zip_batches SET updated_at='2000-01-01T00:00:00Z' WHERE id='batch'",
    )
    .await
    .unwrap();
    (directory, a, b)
}

async fn mock_directory(server: &MockServer) {
    for (endpoint, body) in [
        ("/api/v0/id", "{\"ID\":\"localNode\"}".to_string()),
        (
            "/api/v0/dag/put",
            format!("{{\"Cid\":{{\"/\":\"{ROOT}\"}}}}"),
        ),
        ("/api/v0/resolve", format!("{{\"Path\":\"/ipfs/{LEAF}\"}}")),
        (
            "/api/v0/pin/ls",
            format!("{{\"Keys\":{{\"{ROOT}\":{{\"Type\":\"recursive\"}}}}}}"),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("with-local", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Hash\":\"{ROOT}\",\"WithLocality\":true,\"Local\":true}}"
        )))
        .with_priority(2)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{LEAF}\",\"CumulativeSize\":0}}")),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{ROOT}\"]}}")),
        )
        .mount(server)
        .await;
}

async fn wait_for_request(server: &MockServer, endpoint: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| request.url.path() == endpoint)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovery must reach the RPC after the complete root dag/put response");
}

async fn assert_inflight_cancellation_retains_known_root(endpoint: &str) {
    let (_dir, a, b) = setup().await;
    let kubo = MockServer::start().await;
    mock_directory(&kubo).await;
    Mock::given(method("POST"))
        .and(path(endpoint))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(5)))
        .with_priority(1)
        .mount(&kubo)
        .await;
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let client = KuboClient::new(kubo.uri());
    let recovery = tokio::spawn(async move { run_page(&b, &client, &worker_cancel).await });

    // Reaching pin/add or resolve proves the builder consumed the root
    // dag/put response through clean EOF and recorded its candidate.
    wait_for_request(&kubo, endpoint).await;
    let inflight = zip::snapshot(&a, "batch").await.unwrap().unwrap();
    assert_eq!(inflight.builds.last().unwrap().status, "invoked");
    assert!(inflight.references.is_empty());
    assert!(!recovery.is_finished(), "{endpoint} must still be inflight");
    cancel.cancel();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(3), recovery)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        1
    );

    let canceled = zip::snapshot(&a, "batch").await.unwrap().unwrap();
    assert_eq!(
        canceled.references.len(),
        1,
        "canceling inflight {endpoint} must retain the known root candidate"
    );
    let reference = &canceled.references[0];
    assert_eq!(reference.batch_id, "batch");
    assert_eq!(reference.revision, inflight.batch.root_revision);
    assert_eq!(reference.epoch, inflight.batch.root_epoch);
    assert_eq!(reference.node_identity, "localNode");
    assert_eq!(reference.tier, "hot");
    assert_eq!(reference.cid, ROOT);
    assert_eq!(reference.state, "retained");
    assert!(reference.verification_receipt.is_none());
    assert_eq!(canceled.batch.root_status, "failed");
    assert!(canceled.batch.root_cid.is_none());
    assert_eq!(canceled.batch, inflight.batch);
    assert_eq!(
        canceled.builds, inflight.builds,
        "cancellation must keep the lease"
    );
    assert_eq!(canceled.entries, inflight.entries);
    assert!(zip::claim_root(&a, "batch", "early", 60).await.is_err());
    let requests = kubo.received_requests().await.unwrap();
    assert_eq!(
        run_page(&a, &KuboClient::new(kubo.uri()), &CancellationToken::new())
            .await
            .unwrap(),
        0,
        "the canceled claim still blocks retry until lease expiry"
    );
    assert_eq!(
        kubo.received_requests().await.unwrap().len(),
        requests.len()
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/dag/put")
            .count(),
        1
    );
    assert!(!requests.iter().any(|request| {
        [
            "/api/v0/add",
            "/api/v0/cat",
            "/api/v0/pin/rm",
            "/api/v0/pin/ls",
        ]
        .contains(&request.url.path())
    }));
}

#[tokio::test]
async fn inflight_cancellation_during_pin_retains_known_root_without_adoption() {
    assert_inflight_cancellation_retains_known_root("/api/v0/pin/add").await;
}

#[tokio::test]
async fn inflight_cancellation_during_resolve_retains_known_root_without_adoption() {
    assert_inflight_cancellation_retains_known_root("/api/v0/resolve").await;
}

#[tokio::test]
async fn inflight_cancellation_after_takeover_retains_only_the_old_epoch_candidate() {
    let (_dir, a, b) = setup().await;
    let kubo = MockServer::start().await;
    mock_directory(&kubo).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(5)))
        .with_priority(1)
        .mount(&kubo)
        .await;
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let client = KuboClient::new(kubo.uri());
    let recovery = tokio::spawn(async move { run_page(&b, &client, &worker_cancel).await });
    wait_for_request(&kubo, "/api/v0/pin/add").await;
    let old = zip::snapshot(&a, "batch").await.unwrap().unwrap();
    assert_eq!(old.builds.last().unwrap().status, "invoked");
    a.execute_unprepared(
        "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id='batch'",
    )
    .await
    .unwrap();
    let successor = zip::claim_root(&a, "batch", "successor", 60).await.unwrap();
    assert!(successor.epoch > old.batch.root_epoch);
    let taken_over = zip::snapshot(&a, "batch").await.unwrap().unwrap();
    assert!(!recovery.is_finished(), "pin/add must still be inflight");
    cancel.cancel();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(3), recovery)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        1
    );
    let canceled = zip::snapshot(&a, "batch").await.unwrap().unwrap();
    assert_eq!(
        canceled.references.len(),
        1,
        "a canceled old epoch must not lose its already emitted root"
    );
    let reference = &canceled.references[0];
    assert_eq!(reference.revision, old.batch.root_revision);
    assert_eq!(reference.epoch, old.batch.root_epoch);
    assert_eq!(reference.node_identity, "localNode");
    assert_eq!(reference.tier, "hot");
    assert_eq!(reference.cid, ROOT);
    assert_eq!(reference.state, "retained");
    assert!(reference.verification_receipt.is_none());
    assert_eq!(canceled.batch, taken_over.batch);
    assert_eq!(canceled.builds, taken_over.builds);
    assert_eq!(canceled.entries, taken_over.entries);
    assert!(
        canceled
            .references
            .iter()
            .all(|reference| reference.epoch != successor.epoch)
    );
    assert!(zip::claim_root(&a, "batch", "early", 60).await.is_err());
    assert_eq!(
        run_page(&a, &KuboClient::new(kubo.uri()), &CancellationToken::new())
            .await
            .unwrap(),
        0
    );
    let requests = kubo.received_requests().await.unwrap();
    assert!(!requests.iter().any(|request| {
        [
            "/api/v0/add",
            "/api/v0/cat",
            "/api/v0/pin/rm",
            "/api/v0/pin/ls",
        ]
        .contains(&request.url.path())
    }));
}

#[tokio::test]
async fn restart_rebuilds_original_snapshot_without_republishing_or_readding_files() {
    let (dir, a, b) = setup().await;
    // A later overwrite must not replace the bound manifest version or CID.
    a.execute_unprepared("UPDATE object_versions SET is_latest=FALSE WHERE id='old-version'")
        .await
        .unwrap();
    a.execute_unprepared("DELETE FROM objects WHERE id='old'")
        .await
        .unwrap();
    a.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('new','bucket','out/child','new-cid',99,'new-cid')").await.unwrap();
    a.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('new-version','bucket','out/child','object','new',2,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    drop(b); // The process which committed the failure no longer exists.
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path()
            .join("retry.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let b = store::connect_database(&url).await.unwrap();
    let kubo = MockServer::start().await;
    mock_directory(&kubo).await;
    let client = KuboClient::new(kubo.uri());
    assert_eq!(
        run_page(&b, &client, &CancellationToken::new())
            .await
            .unwrap(),
        1
    );
    let snapshot = zip::snapshot(&a, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.root_status, "complete");
    assert_eq!(snapshot.batch.root_cid.as_deref(), Some(ROOT));
    assert_eq!(
        snapshot.entries[0].version_row_id.as_deref(),
        Some("old-version")
    );
    assert_eq!(snapshot.entries[0].cid.as_deref(), Some(LEAF));
    assert!(
        snapshot
            .references
            .iter()
            .any(|reference| reference.state == "adopted"
                && reference.verification_receipt.is_some())
    );
    assert_eq!(
        run_page(&a, &client, &CancellationToken::new())
            .await
            .unwrap(),
        0
    );
    let requests = kubo.received_requests().await.unwrap();
    assert!(!requests.iter().any(|request| {
        ["/api/v0/add", "/api/v0/cat", "/api/v0/pin/rm"].contains(&request.url.path())
    }));
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/dag/put")
            .count(),
        1
    );
}

#[tokio::test]
async fn live_claim_and_nonretryable_failure_never_start_root_io() {
    let (_dir, a, b) = setup().await;
    let kubo = MockServer::start().await;
    let client = KuboClient::new(kubo.uri());
    let old = zip::claim_root(&a, "batch", "old", 60).await.unwrap();
    assert_eq!(
        run_page(&b, &client, &CancellationToken::new())
            .await
            .unwrap(),
        0
    );
    let tx = b.begin().await.unwrap();
    assert!(
        old.settle_failed_retry(&tx, "{}", "root_build_failed")
            .await
            .is_ok()
    );
    tx.commit().await.unwrap();
    a.execute_unprepared("UPDATE zip_batches SET root_error_code='path_conflict' WHERE id='batch'")
        .await
        .unwrap();
    a.execute_unprepared(
        "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id='batch'",
    )
    .await
    .unwrap();
    assert_eq!(
        run_page(&b, &client, &CancellationToken::new())
            .await
            .unwrap(),
        0
    );
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_retry_retains_root_and_backoff_fences_old_epoch() {
    let (_dir, a, b) = setup().await;
    let kubo = MockServer::start().await;
    mock_directory(&kubo).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .mount(&kubo)
        .await;
    let client = KuboClient::new(kubo.uri());
    assert_eq!(
        run_page(&a, &client, &CancellationToken::new())
            .await
            .unwrap(),
        1
    );
    let failed = zip::snapshot(&b, "batch").await.unwrap().unwrap();
    assert_eq!(failed.batch.root_status, "failed");
    assert_eq!(
        failed.batch.root_error_code.as_deref(),
        Some("directory_build_failed")
    );
    assert!(
        failed
            .references
            .iter()
            .any(|reference| reference.cid == ROOT && reference.state == "retained")
    );
    assert_eq!(failed.builds.last().unwrap().status, "failed");
    let old = zip::RootClaim {
        batch_id: "batch".into(),
        revision: failed.batch.root_revision,
        epoch: failed.batch.root_epoch,
        worker: failed.builds.last().unwrap().worker.clone(),
    };
    assert_eq!(
        run_page(&b, &client, &CancellationToken::new())
            .await
            .unwrap(),
        0,
        "backoff persists across connections"
    );
    assert!(
        zip::claim_root(&b, "batch", "another", 60).await.is_err(),
        "backoff is enforced by the DB lease"
    );
    a.execute_unprepared(
        "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id='batch'",
    )
    .await
    .unwrap();
    let next = zip::claim_root(&b, "batch", "successor", 60).await.unwrap();
    assert!(next.epoch > old.epoch);
    let tx = a.begin().await.unwrap();
    assert!(
        old.settle_failed_retry(&tx, "{}", "directory_build_failed")
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        zip::read(&a, "batch")
            .await
            .unwrap()
            .unwrap()
            .root_error_code
            .as_deref(),
        Some("directory_build_failed")
    );
    let requests = kubo.received_requests().await.unwrap();
    assert!(!requests.iter().any(|request| {
        ["/api/v0/add", "/api/v0/cat", "/api/v0/pin/rm"].contains(&request.url.path())
    }));
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/dag/put")
            .count(),
        1
    );
}

#[tokio::test]
async fn captured_disabled_root_does_not_retry_even_if_legacy_warning_is_retryable() {
    let (_dir, a, b) = setup().await;
    a.execute_unprepared(
        "UPDATE zip_batches SET captured_options='{\"root_enabled\":false,\"target_prefix\":\"out/\"}' WHERE id='batch'",
    )
    .await
    .unwrap();
    let kubo = MockServer::start().await;
    assert_eq!(
        run_page(&b, &KuboClient::new(kubo.uri()), &CancellationToken::new())
            .await
            .unwrap(),
        0
    );
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn import_capture_is_restored_without_consulting_current_configuration() {
    let (_dir, a, b) = setup().await;
    a.execute_unprepared("UPDATE zip_batches SET captured_options='{\"root_capture\":{\"tagged\":true},\"target_prefix\":\"out/\"}' WHERE id='batch'")
        .await.unwrap();
    let kubo = MockServer::start().await;
    mock_directory(&kubo).await;
    assert_eq!(
        run_page(&b, &KuboClient::new(kubo.uri()), &CancellationToken::new())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        zip::read(&a, "batch")
            .await
            .unwrap()
            .unwrap()
            .root_cid
            .as_deref(),
        Some(ROOT)
    );
}

#[tokio::test]
async fn v2_capture_recovers_bound_manifest_without_reconstructing_prefix() {
    let (_dir, a, b) = setup().await;
    a.execute_unprepared("UPDATE zip_batches SET captured_options='{\"options\":{\"root_enabled\":true,\"publish_extracted\":true,\"publish_source\":false}}' WHERE id='batch'")
        .await.unwrap();
    let kubo = MockServer::start().await;
    mock_directory(&kubo).await;
    assert_eq!(
        run_page(&b, &KuboClient::new(kubo.uri()), &CancellationToken::new())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        zip::read(&a, "batch")
            .await
            .unwrap()
            .unwrap()
            .root_cid
            .as_deref(),
        Some(ROOT)
    );
}

#[tokio::test]
async fn synthetic_legacy_path_cannot_become_a_published_root_on_retry() {
    let (_dir, a, b) = setup().await;
    a.execute_unprepared("UPDATE zip_manifest_entries SET path='invalid/0' WHERE batch_id='batch'")
        .await
        .unwrap();
    let kubo = MockServer::start().await;
    assert_eq!(
        run_page(&b, &KuboClient::new(kubo.uri()), &CancellationToken::new())
            .await
            .unwrap(),
        1
    );
    let snapshot = zip::snapshot(&a, "batch").await.unwrap().unwrap();
    assert_eq!(
        snapshot.batch.root_error_code.as_deref(),
        Some("invalid_manifest")
    );
    assert!(snapshot.batch.root_cid.is_none());
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn bounded_failed_retries_end_in_needs_attention_without_touching_entry_errors() {
    let (_dir, a, b) = setup().await;
    for revision in 1..=5 {
        let claim = zip::claim_root(&a, "batch", &format!("worker-{revision}"), 60)
            .await
            .unwrap();
        let snapshot = zip::snapshot(&b, "batch").await.unwrap().unwrap();
        zip::recovery::settle_failed(&a, &snapshot, &claim, "directory_build_failed")
            .await
            .unwrap();
        if revision < 5 {
            a.execute_unprepared("UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id='batch'").await.unwrap();
        }
    }
    let snapshot = zip::snapshot(&b, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.root_status, "failed");
    assert_eq!(
        snapshot.batch.root_error_code.as_deref(),
        Some("needs_attention")
    );
    assert!(snapshot.batch.root_cid.is_none());
    assert_eq!(snapshot.entries.len(), 1);
    assert!(snapshot.entries[0].error_code.is_none());
    let terminal: serde_json::Value =
        serde_json::from_str(snapshot.batch.terminal_result.as_deref().unwrap()).unwrap();
    assert_eq!(terminal["archive_cid"], "original");
    assert_eq!(terminal["root_warning"], "needs_attention");
    assert!(zip::recovery::due_page(&a).await.unwrap().is_empty());
}

#[tokio::test]
async fn crashed_last_budgeted_attempt_is_quarantined_without_extra_root_io() {
    let (_dir, a, b) = setup().await;
    for revision in 1..=5 {
        let claim = zip::claim_root(&a, "batch", &format!("worker-{revision}"), 60)
            .await
            .unwrap();
        if revision < 5 {
            let snapshot = zip::snapshot(&a, "batch").await.unwrap().unwrap();
            zip::recovery::settle_failed(&a, &snapshot, &claim, "directory_build_failed")
                .await
                .unwrap();
        } else {
            zip::mark_invoked(&a, &claim).await.unwrap();
        }
        a.execute_unprepared(
            "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id='batch'",
        )
        .await
        .unwrap();
    }
    let kubo = MockServer::start().await;
    assert_eq!(
        run_page(&b, &KuboClient::new(kubo.uri()), &CancellationToken::new())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        zip::read(&a, "batch")
            .await
            .unwrap()
            .unwrap()
            .root_error_code
            .as_deref(),
        Some("needs_attention")
    );
    assert_eq!(
        run_page(&b, &KuboClient::new(kubo.uri()), &CancellationToken::new())
            .await
            .unwrap(),
        0
    );
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn killed_worker_restarts_from_durable_intent_after_lease_expiry() {
    let (dir, a, b) = setup().await;
    let kubo = MockServer::start().await;
    mock_directory(&kubo).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(5)))
        .with_priority(1)
        .mount(&kubo)
        .await;
    let first = start_worker(
        store::Store::new(b.clone()),
        KuboClient::new(kubo.uri()),
        CancellationToken::new(),
    );
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if kubo
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| request.url.path() == "/api/v0/pin/add")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let join = first.abort_for_test();
    assert!(join.await.unwrap_err().is_cancelled());
    assert_eq!(
        zip::read(&a, "batch").await.unwrap().unwrap().root_status,
        "failed"
    );
    assert!(zip::claim_root(&a, "batch", "early", 60).await.is_err());
    a.execute_unprepared(
        "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id='batch'",
    )
    .await
    .unwrap();
    kubo.reset().await;
    mock_directory(&kubo).await;
    drop(b);
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path()
            .join("retry.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let restarted_db = store::connect_database(&url).await.unwrap();
    let restarted = start_worker(
        store::Store::new(restarted_db),
        KuboClient::new(kubo.uri()),
        CancellationToken::new(),
    );
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if zip::read(&a, "batch").await.unwrap().unwrap().root_status == "complete" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    restarted.shutdown(std::time::Duration::from_secs(1)).await;
    let snapshot = zip::snapshot(&a, "batch").await.unwrap().unwrap();
    assert_eq!(
        snapshot.entries[0].version_row_id.as_deref(),
        Some("old-version")
    );
    assert_eq!(snapshot.batch.root_cid.as_deref(), Some(ROOT));
    assert!(snapshot.builds.len() >= 2);
    let requests = kubo.received_requests().await.unwrap();
    assert!(!requests.iter().any(|request| {
        ["/api/v0/add", "/api/v0/cat", "/api/v0/pin/rm"].contains(&request.url.path())
    }));
}

#[tokio::test]
async fn segmented_kubo_responses_must_finish_before_the_retry_publishes_receipt() {
    use axum::{
        Router,
        body::Body,
        http::{Response, Uri},
    };
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    let (_dir, a, b) = setup().await;
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = calls.clone();
    let router = Router::new().fallback(move |uri: Uri| {
        let recorded = recorded.clone();
        async move {
            recorded.lock().unwrap().push(uri.path().to_owned());
            let body = match uri.path() {
                "/api/v0/files/stat" if uri.query().unwrap_or("").contains("with-local=true") => {
                    format!("{{\"Hash\":\"{ROOT}\",\"WithLocality\":true,\"Local\":true}}")
                }
                "/api/v0/files/stat" => format!("{{\"Hash\":\"{LEAF}\",\"CumulativeSize\":0}}"),
                "/api/v0/id" => "{\"ID\":\"localNode\"}".into(),
                "/api/v0/dag/put" => format!("{{\"Cid\":{{\"/\":\"{ROOT}\"}}}}"),
                "/api/v0/pin/add" => format!("{{\"Pins\":[\"{ROOT}\"]}}"),
                "/api/v0/pin/ls" => {
                    format!("{{\"Keys\":{{\"{ROOT}\":{{\"Type\":\"recursive\"}}}}}}")
                }
                "/api/v0/resolve" => format!("{{\"Path\":\"/ipfs/{LEAF}\"}}"),
                _ => return Response::builder().status(404).body(Body::empty()).unwrap(),
            };
            let middle = body.len() / 2;
            let chunks = [
                bytes::Bytes::copy_from_slice(&body.as_bytes()[..middle]),
                bytes::Bytes::copy_from_slice(&body.as_bytes()[middle..]),
            ];
            let stream = futures_util::stream::iter(chunks.into_iter().map(Ok::<_, Infallible>));
            Response::builder().body(Body::from_stream(stream)).unwrap()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let result = run_page(
        &b,
        &KuboClient::new(format!("http://{address}")),
        &CancellationToken::new(),
    )
    .await;
    server.abort();
    assert_eq!(result.unwrap(), 1);
    let snapshot = zip::snapshot(&a, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.root_cid.as_deref(), Some(ROOT));
    assert!(
        snapshot
            .references
            .iter()
            .any(|reference| reference.state == "adopted"
                && reference.verification_receipt.is_some())
    );
    let paths = calls.lock().unwrap();
    assert!(paths.iter().any(|path| path == "/api/v0/pin/ls"));
    assert!(
        !paths
            .iter()
            .any(|path| ["/api/v0/add", "/api/v0/cat", "/api/v0/pin/rm"].contains(&path.as_str()))
    );
}
