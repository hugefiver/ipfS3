//! ZIP v2 import is accepted at SigV4 intake and finished by the shared worker.
#[allow(dead_code)]
mod support;

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store::{
    self,
    entities::{object, object_version, pin_job, pin_lease},
    import::ownership,
    zip::{self, execution, import_intake},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, PaginatorTrait, QueryFilter,
    Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::Duration;
use support::{
    decompress::{
        AddReply, KuboScript, duplicate_entry_zip, legal_single_entry_zip, legal_two_entry_zip,
    },
    import::{
        ImportHarness, ImportHarnessConfig, TestHttpsReply, get_import_status, start_import_harness,
    },
    sigv4::send_sigv4,
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

fn headers(token: &str) -> HeaderMap {
    let mut result = HeaderMap::new();
    for (name, value) in [
        ("content-type", "application/xml"),
        ("x-ipfs3-client-token", token),
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", token),
        ("x-amz-tagging", "ipfs-s3%3Azip-root=false"),
    ] {
        result.insert(name, HeaderValue::from_str(value).unwrap());
    }
    result
}

async fn submit(
    harness: &ImportHarness,
    token: &str,
    source: &str,
    expected: Option<&str>,
) -> String {
    let mut controls = headers(token);
    if let Some(digest) = expected {
        controls.insert(
            "x-ipfs3-zip-expected-sha256",
            HeaderValue::from_str(digest).unwrap(),
        );
    }
    let accepted = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        source.as_bytes().to_vec(),
        controls,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned()
}

async fn wait_for_state(harness: &ImportHarness, id: &str) -> &'static str {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = import_intake::read_for_path(
                harness.state.store.db(),
                id,
                "test",
                &harness.bucket,
                "archive.zip",
            )
            .await
            .unwrap()
            .unwrap();
            if status.state != "pending" {
                return status.state;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("v2 import reached a durable terminal state")
}

fn cid_xml() -> String {
    format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>")
}

async fn expire_claim(harness: &ImportHarness, id: &str) {
    harness
        .state
        .store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_v2_executions SET lease_until='2000-01-01 00:00:00' WHERE id=?",
            [id.into()],
        ))
        .await
        .unwrap();
}

async fn admit_one_output(harness: &ImportHarness, id: &str, archive: &[u8]) -> execution::Claim {
    let db = harness.state.store.db();
    let claim = execution::claim(db, id, "crashed-worker", 30)
        .await
        .unwrap()
        .unwrap();
    let sha = hex::encode(Sha256::digest(archive));
    import_intake::bind_verified_input(db, &claim, &sha, CID, archive.len() as i64)
        .await
        .unwrap();
    let admitted = execution::read(db, id).await.unwrap().unwrap();
    zip::admit(
        db,
        &zip::BatchAdmission {
            id: id.into(),
            owner: admitted.owner.clone(),
            source: "import".into(),
            token: admitted.token.clone(),
            fingerprint: "pending".into(),
            bucket: harness.bucket.clone(),
            archive_key: "archive.zip".into(),
            input_identity: "pending".into(),
            captured_options: admitted.captured_options,
        },
    )
    .await
    .unwrap();
    let item = zip::ManifestItem::Success {
        path: "file.txt".into(),
        object_key: "out/file.txt".into(),
        cid: CID.into(),
        size: 18,
    };
    let tx = db.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, &harness.bucket)
        .await
        .unwrap();
    execution::admit_manifest_in_transaction(
        &tx,
        &claim,
        &[execution::ManifestItem::Success {
            path: "file.txt".into(),
            object_key: "out/file.txt".into(),
            cid: CID.into(),
            size: 18,
        }],
        &BTreeMap::from([("out/file.txt".into(), "old-exact-guard".into())]),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    zip::prepare_manifest(db, id, &[item]).await.unwrap();
    claim
}

#[tokio::test]
async fn claimed_cid_import_finishes_without_publishing_a_source_object() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    let archive = legal_single_entry_zip();
    harness.set_cat_body(CID, archive);
    let id = submit(&harness, "worker-cid", &cid_xml(), None).await;
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    let response =
        get_import_status(&harness, &harness.bucket, "archive.zip", &id, None, None).await;
    let text = String::from_utf8(response.into_body()).unwrap();
    assert!(text.contains("<State>ready</State>"));
    assert_eq!(
        object::Entity::find()
            .filter(object::Column::Key.eq("archive.zip"))
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        object::Entity::find()
            .filter(object::Column::Key.eq("out/file.txt"))
            .count(harness.state.store.db())
            .await
            .unwrap(),
        1
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn url_first_verified_read_is_replayed_without_a_second_get_after_source_changes() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    let archive = legal_single_entry_zip();
    let expected = hex::encode(Sha256::digest(&archive));
    harness.set_cat_body(CID, archive.clone());
    harness.source.set_reply(
        "/archive.zip?secret=private",
        TestHttpsReply::chunked(archive),
    );
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        harness.source.url("/archive.zip?secret=private")
    );
    let id = submit(&harness, "verified-url", &xml, Some(&expected)).await;
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    assert_eq!(harness.source.requests().len(), 1);
    harness.source.set_reply(
        "/archive.zip?secret=private",
        TestHttpsReply::chunked(b"changed".to_vec()),
    );
    assert_eq!(
        submit(&harness, "verified-url", &xml, Some(&expected)).await,
        id
    );
    let response =
        get_import_status(&harness, &harness.bucket, "archive.zip", &id, None, None).await;
    let status = String::from_utf8(response.into_body()).unwrap();
    assert!(status.contains("<State>ready</State>"));
    assert!(!status.contains("private"));
    assert_eq!(
        harness.source.requests().len(),
        1,
        "replay/status must never GET the URL again"
    );
    assert_eq!(harness.kubo_call_count("/api/v0/add").await, 2);
    assert_eq!(
        object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pin_job::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn changed_first_url_read_is_terminally_failed_with_redacted_measured_mismatch() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    let archive = legal_single_entry_zip();
    let expected = hex::encode(Sha256::digest(b"original promised bytes"));
    harness.source.set_reply(
        "/mismatch.zip?secret=private",
        TestHttpsReply::chunked(archive),
    );
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        harness.source.url("/mismatch.zip?secret=private")
    );
    let id = submit(&harness, "mismatch-url", &xml, Some(&expected)).await;
    assert_eq!(wait_for_state(&harness, &id).await, "failed");
    let response =
        get_import_status(&harness, &harness.bucket, "archive.zip", &id, None, None).await;
    let text = String::from_utf8(response.into_body()).unwrap();
    assert!(text.contains("<State>failed</State>"));
    assert!(!text.contains("private"));
    assert_eq!(harness.source.requests().len(), 1);
    let persisted = ipfs_s3_gateway::store::zip::execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.terminal_result.as_deref(),
        Some("expected_sha256_mismatch")
    );
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn cid_expected_digest_is_measured_from_the_complete_cat_body() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    harness.set_cat_body(CID, legal_single_entry_zip());
    let wrong = hex::encode(Sha256::digest(CID.as_bytes()));
    let id = submit(&harness, "cid-measured", &cid_xml(), Some(&wrong)).await;
    assert_eq!(wait_for_state(&harness, &id).await, "failed");
    assert_eq!(
        harness.kubo_call_count("/api/v0/cat").await,
        2,
        "CID verification measures the source but never enters ZIP extraction"
    );
    let stored = execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.terminal_result.as_deref(),
        Some("expected_sha256_mismatch")
    );
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn duplicate_output_keys_reject_the_entire_import_without_a_last_wins_version() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    harness.set_cat_body(CID, duplicate_entry_zip());
    let id = submit(&harness, "duplicate-output", &cid_xml(), None).await;
    assert_eq!(wait_for_state(&harness, &id).await, "failed");
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    let persisted = ipfs_s3_gateway::store::zip::execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.terminal_result.as_deref(), Some("invalid_zip"));
    harness.shutdown().await;
}

#[tokio::test]
async fn invalid_zip_is_terminal_with_a_safe_code_and_no_published_objects() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    harness.set_cat_body(CID, b"not a ZIP file".to_vec());
    let id = submit(&harness, "invalid-zip", &cid_xml(), None).await;
    assert_eq!(wait_for_state(&harness, &id).await, "failed");
    let stored = execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.terminal_result.as_deref(), Some("invalid_zip"));
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn one_failed_entry_does_not_publish_it_or_discard_the_successful_entry() {
    let harness = start_import_harness(ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: vec![
                AddReply::Ok(CID),
                AddReply::Error(StatusCode::SERVICE_UNAVAILABLE, "test add failure"),
            ],
            cat_bodies: Default::default(),
        },
        ..ImportHarnessConfig::default()
    })
    .await;
    harness.set_cat_body(CID, legal_two_entry_zip());
    let id = submit(&harness, "partial-worker", &cid_xml(), None).await;
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    let execution = ipfs_s3_gateway::store::zip::execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let terminal: serde_json::Value =
        serde_json::from_str(execution.terminal_result.as_deref().unwrap()).unwrap();
    assert_eq!(terminal["published_count"], 1);
    assert_eq!(terminal["failed_count"], 1);
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        1
    );
    assert!(
        ipfs_s3_gateway::store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "out/second.txt"
        )
        .await
        .is_err()
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn root_build_failure_is_observable_but_successful_outputs_remain_published() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    harness.set_cat_body(CID, legal_single_entry_zip());
    let mut controls = headers("root-failed-worker");
    controls.remove("x-amz-tagging");
    let accepted = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        controls,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    let status = import_intake::read_for_path(
        harness.state.store.db(),
        &id,
        "test",
        &harness.bucket,
        "archive.zip",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(status.root_status.as_deref(), Some("failed"));
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(harness.kubo_call_count("/api/v0/cat").await, 3);
    harness.shutdown().await;
}

#[tokio::test]
async fn verified_local_unixfs_root_is_published_only_with_its_output_and_receipt() {
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path, query_param},
    };
    const ROOT: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    harness.set_cat_body(CID, legal_single_entry_zip());
    for (endpoint, body) in [
        ("/api/v0/id", "{\"ID\":\"localNode\"}".to_owned()),
        (
            "/api/v0/dag/put",
            format!("{{\"Cid\":{{\"/\":\"{ROOT}\"}}}}"),
        ),
        ("/api/v0/resolve", format!("{{\"Path\":\"/ipfs/{CID}\"}}")),
        (
            "/api/v0/pin/ls",
            format!("{{\"Keys\":{{\"{ROOT}\":{{\"Type\":\"recursive\"}}}}}}"),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&harness.kubo)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("with-local", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Hash\":\"{ROOT}\",\"WithLocality\":true,\"Local\":true}}"
        )))
        .with_priority(2)
        .mount(&harness.kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{CID}\",\"CumulativeSize\":0}}")),
        )
        .mount(&harness.kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", ROOT))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{ROOT}\"]}}")),
        )
        .with_priority(1)
        .mount(&harness.kubo)
        .await;
    let mut controls = headers("verified-worker-root");
    controls.remove("x-amz-tagging");
    let accepted = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        controls,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    let status = import_intake::read_for_path(
        harness.state.store.db(),
        &id,
        "test",
        &harness.bucket,
        "archive.zip",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(status.root_status.as_deref(), Some("complete"));
    let root = zip::snapshot(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        root.references
            .iter()
            .any(|reference| reference.cid == ROOT && reference.verification_receipt.is_some())
    );
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        1
    );
    let response =
        get_import_status(&harness, &harness.bucket, "archive.zip", &id, None, None).await;
    assert!(
        String::from_utf8(response.into_body())
            .unwrap()
            .contains("<State>ready</State>")
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn zero_successful_files_produces_no_source_output_or_root_cid() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    let mut archive = legal_single_entry_zip();
    archive[14] ^= 1; // corrupt the local-header CRC: this entry cannot publish.
    harness.set_cat_body(CID, archive);
    let mut controls = headers("zero-worker");
    controls.remove("x-amz-tagging");
    let accepted = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        controls,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    let status = import_intake::read_for_path(
        harness.state.store.db(),
        &id,
        "test",
        &harness.bucket,
        "archive.zip",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(status.root_status.as_deref(), Some("empty"));
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn bound_url_artifact_survives_a_crash_without_repeating_the_get() {
    let harness = start_import_harness(ImportHarnessConfig {
        poll_interval_ms: 1_000,
        ..ImportHarnessConfig::default()
    })
    .await;
    let archive = legal_single_entry_zip();
    harness.set_cat_body(CID, archive.clone());
    let sha = hex::encode(Sha256::digest(&archive));
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        harness.source.url("/crash.zip?secret=do-not-render")
    );
    let id = submit(&harness, "bound-recovery", &xml, Some(&sha)).await;
    let old = execution::claim(harness.state.store.db(), &id, "crashed-worker", 30)
        .await
        .unwrap()
        .unwrap();
    import_intake::bind_verified_input(
        harness.state.store.db(),
        &old,
        &sha,
        CID,
        archive.len() as i64,
    )
    .await
    .unwrap();
    expire_claim(&harness, &id).await;
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    assert_eq!(
        harness.source.requests().len(),
        0,
        "durably bound URL input is never re-fetched"
    );
    assert_eq!(harness.kubo_call_count("/api/v0/cat").await, 1);
    assert_eq!(
        execution::read(harness.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap()
            .epoch,
        old.epoch + 1
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn admitted_recovery_rotates_exact_guards_without_cat_reextract_or_re_admission() {
    let harness = start_import_harness(ImportHarnessConfig {
        poll_interval_ms: 1_000,
        ..ImportHarnessConfig::default()
    })
    .await;
    let archive = legal_single_entry_zip();
    let id = submit(&harness, "admitted-recovery", &cid_xml(), None).await;
    let db = harness.state.store.db();
    let old = admit_one_output(&harness, &id, &archive).await;
    expire_claim(&harness, &id).await;
    let successor = execution::claim(db, &id, "replacement-worker", 30)
        .await
        .unwrap()
        .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        execution::verify_in_transaction(&tx, &old).await.is_err(),
        "stolen epoch cannot verify guards even before successor publication"
    );
    tx.rollback().await.unwrap();
    expire_claim(&harness, &id).await;
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    assert!(
        execution::read_targets(db, &old).await.is_err(),
        "old epoch cannot use rotated guards"
    );
    assert!(
        execution::complete(db, &old, "{}").await.is_err(),
        "old epoch cannot complete successor publication"
    );
    assert_eq!(
        execution::read(db, &id).await.unwrap().unwrap().epoch,
        successor.epoch + 1
    );
    assert_eq!(object::Entity::find().count(db).await.unwrap(), 1);
    assert_eq!(harness.kubo_call_count("/api/v0/cat").await, 0);
    assert_eq!(harness.kubo_call_count("/api/v0/add").await, 0);
    harness.shutdown().await;
}

#[tokio::test]
async fn admitted_root_recovery_waits_for_old_intent_without_burning_retry_epochs() {
    let harness = start_import_harness(ImportHarnessConfig {
        poll_interval_ms: 1_000,
        max_attempts: 3,
        ..ImportHarnessConfig::default()
    })
    .await;
    let mut controls = headers("old-root-intent");
    controls.remove("x-amz-tagging");
    let accepted = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        controls,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let old = admit_one_output(&harness, &id, &legal_single_entry_zip()).await;
    let old_root = zip::claim_root(harness.state.store.db(), &id, "crashed-root", 2)
        .await
        .unwrap();
    zip::mark_invoked(harness.state.store.db(), &old_root)
        .await
        .unwrap();
    expire_claim(&harness, &id).await;
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    let after = execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.epoch,
        old.epoch + 1,
        "root intent wait must not consume claim retries"
    );
    let status = import_intake::read_for_path(
        harness.state.store.db(),
        &id,
        "test",
        &harness.bucket,
        "archive.zip",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(status.root_status.as_deref(), Some("failed"));
    assert_eq!(harness.kubo_call_count("/api/v0/cat").await, 0);
    harness.shutdown().await;
}

#[tokio::test]
async fn source_key_existing_object_remains_the_same_version_after_outputs_only_import() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    harness.set_cat_body(CID, legal_single_entry_zip());
    let put = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[],
        b"previous source object".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let before = object::Entity::find()
        .filter(object::Column::Key.eq("archive.zip"))
        .one(harness.state.store.db())
        .await
        .unwrap()
        .unwrap();
    let id = submit(&harness, "existing-source", &cid_xml(), None).await;
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    let after = object::Entity::find()
        .filter(object::Column::Key.eq("archive.zip"))
        .one(harness.state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.id, after.id);
    assert_eq!(before.cid, after.cid);
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        2
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn lost_admitted_guard_fences_recovery_instead_of_readmitting_an_output() {
    let harness = start_import_harness(ImportHarnessConfig {
        poll_interval_ms: 1_000,
        ..ImportHarnessConfig::default()
    })
    .await;
    let id = submit(&harness, "lost-output-guard", &cid_xml(), None).await;
    let old = admit_one_output(&harness, &id, &legal_single_entry_zip()).await;
    let db = harness.state.store.db();
    // Model an independent successor that won the exact destination. The
    // earlier epoch may not recreate the guard after takeover.
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE import_destinations SET mutation_id='successor' WHERE bucket=? AND key=?",
        [harness.bucket.clone().into(), "out/file.txt".into()],
    ))
    .await
    .unwrap();
    expire_claim(&harness, &id).await;
    assert_eq!(wait_for_state(&harness, &id).await, "failed");
    let final_row = execution::read(db, &id).await.unwrap().unwrap();
    assert_eq!(final_row.state, "fenced");
    assert_eq!(final_row.epoch, old.epoch);
    assert_eq!(object::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(harness.kubo_total_call_count().await, 0);
    harness.shutdown().await;
}

#[tokio::test]
async fn transient_source_failure_uses_a_bounded_database_clock_retry_budget() {
    let harness = start_import_harness(ImportHarnessConfig {
        max_attempts: 2,
        lease_duration_secs: 2,
        ..ImportHarnessConfig::default()
    })
    .await;
    let mut unavailable = TestHttpsReply::chunked(Vec::new());
    unavailable.status = 503;
    harness.source.set_reply("/unavailable.zip", unavailable);
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        harness.source.url("/unavailable.zip")
    );
    let expected = "a".repeat(64);
    let id = submit(&harness, "retry-budget", &xml, Some(&expected)).await;
    assert_eq!(wait_for_state(&harness, &id).await, "failed");
    assert_eq!(harness.source.requests().len(), 2);
    assert_eq!(
        execution::read(harness.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap()
            .epoch,
        2
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        harness.source.requests().len(),
        2,
        "terminal request must not spin or refetch"
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn local_only_targets_ignore_unrelated_output_rule_revision_changes() {
    let harness = start_import_harness(ImportHarnessConfig {
        poll_interval_ms: 1_000,
        ..ImportHarnessConfig::default()
    })
    .await;
    harness.set_cat_body(CID, legal_single_entry_zip());
    let id = submit(&harness, "local-only-rules", &cid_xml(), None).await;
    let before = execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let mut captured: serde_json::Value = serde_json::from_str(&before.captured_options).unwrap();
    captured["rule_revision"] = serde_json::json!("unrelated-revision");
    harness
        .state
        .store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_v2_executions SET captured_options=? WHERE id=?",
            [captured.to_string().into(), id.clone().into()],
        ))
        .await
        .unwrap();
    assert_eq!(wait_for_state(&harness, &id).await, "ready");
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        1
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn shutdown_cancels_blocked_url_io_and_joins_without_publishing() {
    let mut harness = start_import_harness(ImportHarnessConfig::default()).await;
    let source = harness
        .source
        .set_blocked_chunked_reply("/shutdown.zip", legal_single_entry_zip());
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        harness.source.url("/shutdown.zip")
    );
    let digest = hex::encode(Sha256::digest(legal_single_entry_zip()));
    let id = submit(&harness, "shutdown-v2", &xml, Some(&digest)).await;
    source.wait_until_blocked().await;
    let replacement = harness.start_additional_worker();
    let old = std::mem::replace(&mut harness.worker, replacement);
    tokio::time::timeout(Duration::from_secs(2), old.shutdown(Duration::from_secs(1)))
        .await
        .expect("worker shutdown must interrupt a blocked URL request");
    source.release();
    let snapshot = execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state, "pending");
    assert_eq!(snapshot.input_sha256, None);
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn blocked_v2_url_allows_legacy_progress_with_two_shared_slots() {
    use ipfs_s3_gateway::store::entities::import_job;
    let harness = start_import_harness(ImportHarnessConfig {
        worker_concurrency: 2,
        ..ImportHarnessConfig::default()
    })
    .await;
    let blocker = harness
        .source
        .set_blocked_chunked_reply("/slow-v2.zip", legal_single_entry_zip());
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        harness.source.url("/slow-v2.zip")
    );
    let digest = hex::encode(Sha256::digest(legal_single_entry_zip()));
    let v2_id = submit(&harness, "v2-blocks-one-slot", &xml, Some(&digest)).await;
    blocker.wait_until_blocked().await;
    harness.set_cat_body(CID, b"legacy unzipped content".to_vec());
    let mut legacy_headers = HeaderMap::new();
    legacy_headers.insert("content-type", HeaderValue::from_static("application/xml"));
    let accepted = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "legacy-key",
        &[("ipfs3-import", "")],
        cid_xml().into_bytes(),
        legacy_headers,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let legacy_id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let job = import_job::Entity::find_by_id(&legacy_id)
                .one(harness.state.store.db())
                .await
                .unwrap()
                .unwrap();
            if job.state != "queued" && job.state != "running" {
                break job.state;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("remaining shared slot finishes legacy while v2 source remains blocked");
    assert_eq!(result, "completed");
    let blocked = execution::read(harness.state.store.db(), &v2_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(blocked.state, "pending");
    assert!(blocked.input_art_cid.is_none());
    assert!(blocked.input_sha256.is_none());
    harness.set_cat_body(CID, legal_single_entry_zip());
    blocker.release();
    assert_eq!(wait_for_state(&harness, &v2_id).await, "ready");
    harness.shutdown().await;
}

#[tokio::test]
async fn extracted_target_policy_is_evaluated_per_output_without_source_manual_inheritance() {
    use ipfs_s3_gateway::{
        config::Config,
        import::{ImportConfig, downloader::SourceDownloader, pipeline::ImportCoordinator},
        pinning::{
            config::ValidatedPinningConfig,
            coordinator::normalize_validated_config,
            zip_policy::{ZipOutputRuleConfig, ZipRuleEffect},
        },
    };
    use std::sync::Arc;
    use support::decompress::{start_kubo_harness, start_s3_server_with_imports};
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };

    let kubo = start_kubo_harness(KuboScript {
        add_replies: vec![AddReply::Ok(CID), AddReply::Ok(CID)],
        cat_bodies: std::collections::HashMap::from([(CID.into(), legal_two_entry_zip())]),
    })
    .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Progress\":1,\"Bytes\":15}}\n{{\"Pins\":[\"{CID}\"]}}\n"
        )))
        .with_priority(1)
        .mount(&kubo.server)
        .await;
    let raw = format!(
        r#"
        [kubo]
        rpc_url = {:?}
        [storage]
        database_url = "sqlite::memory:"
        [decompress_zip]
        unixfs_directory_root = false
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
    );
    let mut cfg: Config = toml::from_str(&raw).unwrap();
    let normalized =
        normalize_validated_config(ValidatedPinningConfig::from_config(&cfg, |_| None).unwrap())
            .unwrap();
    cfg.decompress_zip.pin_output_rules = vec![
        ZipOutputRuleConfig {
            name: "only-first".into(),
            priority: 10,
            bucket: "test-bkt".into(),
            prefix: "out/first.txt".into(),
            effect: ZipRuleEffect::Allow,
            policy_id: Some(normalized.policies[0].identity.clone()),
        },
        ZipOutputRuleConfig {
            name: "deny-second".into(),
            priority: 20,
            bucket: "test-bkt".into(),
            prefix: "out/second.txt".into(),
            effect: ZipRuleEffect::Deny,
            policy_id: None,
        },
    ];
    let state = ipfs_s3_gateway::state::AppState::new(&cfg).await.unwrap();
    store::bucket::create(state.store.db(), "test-bkt", None)
        .await
        .unwrap();
    let validated = ImportConfig {
        worker_concurrency: 2,
        poll_interval_ms: 10,
        lease_duration_secs: 2,
        ..ImportConfig::default()
    }
    .validate()
    .unwrap();
    let imports = ImportCoordinator::new(
        validated.clone(),
        SourceDownloader::production(Arc::new(validated)),
    );
    let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let server = start_s3_server_with_imports(state.clone(), observed, imports.clone()).await;
    let worker = imports.start(state.clone(), tokio_util::sync::CancellationToken::new());
    let mut controls = headers("rule-per-output");
    controls.insert("x-ipfs3-zip-targets", HeaderValue::from_static("extracted"));
    controls.insert(
        "x-amz-tagging",
        HeaderValue::from_static("ipfs-s3%3Azip-root=false&ipfs-s3%3Apin=true"),
    );
    let accepted = send_sigv4(
        reqwest::Method::POST,
        &server.endpoint,
        "test-bkt",
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        cid_xml().into_bytes(),
        controls,
        "test",
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = import_intake::read_for_path(
                state.store.db(),
                &id,
                "test",
                "test-bkt",
                "archive.zip",
            )
            .await
            .unwrap()
            .unwrap();
            if status.state != "pending" {
                break status.state;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    let status = match status {
        Ok(status) => status,
        Err(_) => {
            let execution = execution::read(state.store.db(), &id)
                .await
                .unwrap()
                .unwrap();
            panic!(
                "v2 policy import timed out: state={} epoch={} input_bound={}",
                execution.state,
                execution.epoch,
                execution.input_sha256.is_some()
            );
        }
    };
    assert_eq!(status, "ready");
    let leases = pin_lease::Entity::find()
        .all(state.store.db())
        .await
        .unwrap();
    let first = object::Entity::find()
        .filter(object::Column::Key.eq("out/first.txt"))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].owner_object_id, first.id);
    assert_eq!(leases[0].policy_id, normalized.policies[0].identity);
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        pin_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
    worker.shutdown(Duration::from_secs(2)).await;
    server.shutdown().await;
}
