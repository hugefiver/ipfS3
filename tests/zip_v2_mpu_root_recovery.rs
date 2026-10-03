//! Real SigV4 MPU publication followed by the production root-only recovery page.
#[allow(dead_code)]
mod support;

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    store::{
        self,
        entities::{object, object_version},
        zip,
    },
    zip::recovery::run_page,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, QueryOrder, Statement};
use serde_json::{Value, json};
use support::decompress::{
    AddReply, KuboScript, TestHarness, legal_single_entry_zip, start_harness_with_root_default,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path, query_param},
};

const LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const ROOT: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";

async fn directory_rpc(harness: &TestHarness, fail_first: bool) {
    for (endpoint, body) in [
        ("/api/v0/id", json!({"ID":"localNode"})),
        ("/api/v0/dag/put", json!({"Cid":{"/":ROOT}})),
        ("/api/v0/resolve", json!({"Path":format!("/ipfs/{LEAF}")})),
        ("/api/v0/pin/add", json!({"Pins":[ROOT]})),
        (
            "/api/v0/pin/ls",
            json!({"Keys":{ROOT:{"Type":"recursive"}}}),
        ),
        (
            "/api/v0/files/stat",
            json!({"Hash":LEAF,"CumulativeSize":0}),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .with_priority(3)
            .mount(&harness.kubo)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("with-local", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Hash":ROOT,"WithLocality":true,"Local":true
        })))
        .with_priority(2)
        .mount(&harness.kubo)
        .await;
    if fail_first {
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/put"))
            .respond_with(ResponseTemplate::new(503).set_body_string("temporary root outage"))
            .with_priority(1)
            .up_to_n_times(1)
            .expect(1)
            .mount(&harness.kubo)
            .await;
    }
}

async fn create_and_upload(source: bool, extracted: bool) -> (TestHarness, String) {
    let archive = legal_single_entry_zip();
    let mut add_replies = vec![AddReply::Ok("QmPart"), AddReply::Ok("QmArchive")];
    if extracted {
        add_replies.push(AddReply::Ok(LEAF));
    }
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies,
            cat_bodies: [
                ("QmPart".to_owned(), archive.clone()),
                ("QmArchive".to_owned(), archive.clone()),
            ]
            .into(),
        },
        true,
    )
    .await;
    store::bucket::set_versioning_state(
        harness.state.store.db(),
        &harness.bucket,
        store::object_version::BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    directory_rpc(&harness, extracted).await;
    let mut controls = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        (
            "x-ipfs3-zip-publish-source",
            if source { "true" } else { "false" },
        ),
        (
            "x-ipfs3-zip-publish-extracted",
            if extracted { "true" } else { "false" },
        ),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", "mpu-root-recovery"),
    ] {
        controls.insert(name, HeaderValue::from_static(value));
    }
    let create = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        controls,
        "test",
    )
    .await;
    let status = create.status();
    let xml = create.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    let id = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_owned();
    let part = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", &id), ("partNumber", "1")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    assert_eq!(part.headers()[http::header::ETAG], "\"QmPart\"");
    (harness, id)
}

async fn complete(harness: &TestHarness, id: &str) -> reqwest::Response {
    support::sigv4::send_sigv4(
        reqwest::Method::POST, &harness.endpoint, &harness.bucket, "archive.zip",
        &[("uploadId", id)],
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>".to_vec(),
        HeaderMap::new(), "test",
    ).await
}

async fn expire_root_lease(harness: &TestHarness, id: &str) {
    harness
        .state
        .store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id=?",
            vec![id.into()],
        ))
        .await
        .unwrap();
}

async fn rows(harness: &TestHarness) -> (Vec<object::Model>, Vec<object_version::Model>) {
    let db = harness.state.store.db();
    (
        object::Entity::find()
            .order_by_asc(object::Column::Id)
            .all(db)
            .await
            .unwrap(),
        object_version::Entity::find()
            .order_by_asc(object_version::Column::Id)
            .all(db)
            .await
            .unwrap(),
    )
}

async fn remote_counts(harness: &TestHarness) -> Vec<i64> {
    let mut counts = Vec::new();
    for table in [
        "pin_jobs",
        "pin_leases",
        "pin_lease_targets",
        "remote_pins",
        "remote_pin_ledger",
    ] {
        counts.push(
            harness
                .state
                .store
                .db()
                .query_one(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    format!("SELECT COUNT(*) AS n FROM {table}"),
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "n")
                .unwrap(),
        );
    }
    counts
}

async fn assert_root_recovered(source: bool) {
    let (harness, id) = create_and_upload(source, true).await;
    let first = complete(&harness, &id).await;
    let status = first.status();
    let headers = first.headers().clone();
    let xml = first.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    assert_eq!(headers["x-ipfs-s3-zip-root-status"], "failed");
    assert!(
        xml.contains(&format!("<SourcePublished>{source}</SourcePublished>")),
        "{xml}"
    );
    let db = harness.state.store.db();
    let before = zip::snapshot(db, &id).await.unwrap().unwrap();
    assert_eq!(before.batch.source, "mpu");
    assert_eq!(before.batch.state, "published");
    assert_eq!(before.batch.source_published, source);
    assert_eq!(
        before.batch.root_error_code.as_deref(),
        Some("directory_build_failed")
    );
    assert_eq!(before.entries.len(), 1);
    assert!(before.entries[0].version_row_id.is_some());
    let capture: Value = serde_json::from_str(&before.batch.captured_options).unwrap();
    assert!(capture.get("options").is_none());
    assert!(capture.get("target_prefix").is_none());
    assert_eq!(capture["result_version"], 2);
    let published = rows(&harness).await;
    assert_eq!(published.0.len(), 1 + usize::from(source));
    assert_eq!(published.1.len(), 1 + usize::from(source));
    let remotes = remote_counts(&harness).await;
    assert_eq!(remotes, vec![0; 5]);
    let execution = zip::execution::read(db, &id).await.unwrap().unwrap();
    assert!(
        zip::recovery::due_page(db).await.unwrap().is_empty(),
        "live lease fences recovery"
    );
    expire_root_lease(&harness, &id).await;
    assert_eq!(zip::recovery::due_page(db).await.unwrap(), vec![id.clone()]);
    let calls = harness.kubo.received_requests().await.unwrap().len();
    let additions = harness.captured_add_file_bytes();
    assert_eq!(
        additions.len(),
        3,
        "part, assembled archive, extracted file"
    );
    assert_eq!(
        run_page(db, &harness.state.kubo, &CancellationToken::new())
            .await
            .unwrap(),
        1
    );
    let after = zip::snapshot(db, &id).await.unwrap().unwrap();
    assert_eq!(
        after.batch.root_status, "complete",
        "worker must recover the flat MPU capture, not quarantine it: {:?}",
        after.batch.root_error_code
    );
    assert_eq!(after.batch.root_cid.as_deref(), Some(ROOT));
    assert_eq!(after.batch.root_error_code, None);
    assert_eq!(after.batch.root_revision, before.batch.root_revision + 1);
    assert!(
        after
            .references
            .iter()
            .any(|reference| reference.cid == ROOT
                && reference.state == "adopted"
                && reference.verification_receipt.is_some())
    );
    assert_eq!(after.entries, before.entries);
    assert_eq!(after.batch.captured_options, before.batch.captured_options);
    assert_eq!(after.batch.source_published, source);
    assert_eq!(
        rows(&harness).await,
        published,
        "root recovery cannot publish a new object/version"
    );
    assert_eq!(remote_counts(&harness).await, remotes);
    assert_eq!(
        zip::execution::read(db, &id).await.unwrap().unwrap(),
        execution
    );
    let mut original: Value =
        serde_json::from_str(before.batch.terminal_result.as_deref().unwrap()).unwrap();
    let mut recovered: Value =
        serde_json::from_str(after.batch.terminal_result.as_deref().unwrap()).unwrap();
    for name in ["root_status", "root_warning", "root_cid"] {
        original.as_object_mut().unwrap().remove(name);
        recovered.as_object_mut().unwrap().remove(name);
    }
    assert_eq!(
        recovered, original,
        "source/MPU result identity remains frozen"
    );
    let replay = complete(&harness, &id).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers(), &headers);
    assert_eq!(replay.text().await.unwrap(), xml);
    let batch_status = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("ipfs3-zip-batch", &id)],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(batch_status.status(), StatusCode::OK);
    let batch_xml = batch_status.text().await.unwrap();
    assert!(
        batch_xml.contains("<RootStatus>complete</RootStatus>"),
        "{batch_xml}"
    );
    assert!(
        batch_xml.contains(&format!("<RootCID>{ROOT}</RootCID>")),
        "{batch_xml}"
    );
    assert_eq!(
        run_page(db, &harness.state.kubo, &CancellationToken::new())
            .await
            .unwrap(),
        0
    );
    let requests = harness.kubo.received_requests().await.unwrap();
    let root_requests = &requests[calls..];
    assert_eq!(
        root_requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/dag/put")
            .count(),
        1
    );
    assert!(
        root_requests.iter().all(|request| [
            "/api/v0/id",
            "/api/v0/files/stat",
            "/api/v0/dag/put",
            "/api/v0/pin/add",
            "/api/v0/resolve",
            "/api/v0/pin/ls",
        ]
        .contains(&request.url.path())),
        "no cat/add/re-extraction/export/import/remote RPC"
    );
    assert_eq!(harness.captured_add_file_bytes(), additions);
    assert_eq!(rows(&harness).await, published);
}

#[tokio::test]
async fn outputs_only_mpu_temporary_root_failure_recovers_through_actual_worker() {
    assert_root_recovered(false).await;
}

#[tokio::test]
async fn source_and_outputs_mpu_temporary_root_failure_recovers_through_actual_worker() {
    assert_root_recovered(true).await;
}

#[tokio::test]
async fn flat_mpu_capture_requires_exact_protocol_flags_token_and_root_identity() {
    let (harness, id) = create_and_upload(false, true).await;
    assert_eq!(complete(&harness, &id).await.status(), StatusCode::OK);
    let db = harness.state.store.db();
    let snapshot = zip::snapshot(db, &id).await.unwrap().unwrap();
    let claim = zip::RootClaim {
        batch_id: id.clone(),
        revision: snapshot.batch.root_revision,
        epoch: snapshot.batch.root_epoch,
        worker: snapshot.builds.last().unwrap().worker.clone(),
    };
    assert!(zip::recovery::files(&snapshot, &claim).is_ok());
    let captured: Value = serde_json::from_str(&snapshot.batch.captured_options).unwrap();
    for (field, value) in [
        ("result_version", json!(1)),
        ("result_version", json!("2")),
        ("publish_source", json!(true)),
        ("publish_source", json!("false")),
        ("publish_extracted", json!(false)),
        ("publish_extracted", json!(1)),
        ("targets", json!("source")),
        ("targets", json!("both")),
        ("targets", json!("all")),
        ("token", json!("other")),
        ("token", json!("")),
        ("token", json!("bad/token")),
        ("root_enabled", json!(false)),
        ("root_override", json!(false)),
        ("root_override", json!("true")),
        ("target_prefix", json!("out/")),
        (
            "options",
            json!({"root_enabled":true,"publish_extracted":true}),
        ),
        ("root_capture", json!({"configured":true})),
    ] {
        let mut forged = zip::snapshot(db, &id).await.unwrap().unwrap();
        let mut options = captured.clone();
        options[field] = value;
        forged.batch.captured_options = options.to_string();
        assert!(
            zip::recovery::files(&forged, &claim).is_err(),
            "forged capture {options}"
        );
    }
    for field in captured.as_object().unwrap().keys() {
        let mut forged = zip::snapshot(db, &id).await.unwrap().unwrap();
        let mut options = captured.clone();
        options.as_object_mut().unwrap().remove(field);
        forged.batch.captured_options = options.to_string();
        assert!(
            zip::recovery::files(&forged, &claim).is_err(),
            "missing capture field {field}"
        );
    }
    for source in ["direct", "import"] {
        let mut forged = zip::snapshot(db, &id).await.unwrap().unwrap();
        forged.batch.source = source.into();
        assert!(
            zip::recovery::files(&forged, &claim).is_err(),
            "wrong flat capture source {source}"
        );
    }
    let mut forged = zip::snapshot(db, &id).await.unwrap().unwrap();
    forged.batch.source_published = true;
    assert!(
        zip::recovery::files(&forged, &claim).is_err(),
        "publication flags must match capture"
    );
    for token in ["bad/token".to_owned(), "x".repeat(129)] {
        let mut forged = zip::snapshot(db, &id).await.unwrap().unwrap();
        let mut options = captured.clone();
        options["token"] = token.clone().into();
        forged.batch.token = token;
        forged.batch.captured_options = options.to_string();
        assert!(
            zip::recovery::files(&forged, &claim).is_err(),
            "token syntax must be valid even when identities agree"
        );
    }
    for (source, targets) in [(false, "extracted"), (true, "source"), (true, "both")] {
        let mut valid = zip::snapshot(db, &id).await.unwrap().unwrap();
        let mut options = captured.clone();
        options["root_override"] = true.into();
        options["publish_source"] = source.into();
        options["targets"] = targets.into();
        valid.batch.source_published = source;
        valid.batch.captured_options = options.to_string();
        assert!(
            zip::recovery::files(&valid, &claim).is_ok(),
            "valid root override/target capture {options}"
        );
    }
    let mut legacy = zip::snapshot(db, &id).await.unwrap().unwrap();
    legacy.batch.captured_options = json!({"root_enabled":true,"target_prefix":"out/"}).to_string();
    assert!(zip::recovery::files(&legacy, &claim).is_ok());
    legacy.entries[0].path = "invalid/0".into();
    assert!(
        zip::recovery::files(&legacy, &claim).is_err(),
        "legacy prefix remains mandatory"
    );
}

#[tokio::test]
async fn source_only_mpu_does_not_build_or_schedule_a_root() {
    let (harness, id) = create_and_upload(true, false).await;
    let response = complete(&harness, &id).await;
    let status = response.status();
    let xml = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    let db = harness.state.store.db();
    let snapshot = zip::snapshot(db, &id).await.unwrap().unwrap();
    assert_eq!(snapshot.batch.root_status, "disabled");
    assert!(snapshot.batch.source_published);
    assert!(snapshot.entries.is_empty());
    let capture: Value = serde_json::from_str(&snapshot.batch.captured_options).unwrap();
    assert_eq!(
        capture["root_enabled"], true,
        "captured config alone is not root permission"
    );
    assert_eq!(capture["publish_extracted"], false);
    let calls = harness.kubo.received_requests().await.unwrap();
    assert!(
        !calls
            .iter()
            .any(|request| request.url.path() == "/api/v0/dag/put")
    );
    assert_eq!(
        run_page(db, &harness.state.kubo, &CancellationToken::new())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        calls.len()
    );

    // Even a stale retryable marker and a stray bound manifest row must not
    // convert this captured source-only contract into root authorization.
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE zip_batches SET root_status='failed',root_error_code='directory_build_failed',updated_at='2000-01-01T00:00:00Z' WHERE id=?", vec![id.clone().into()],
    )).await.unwrap();
    let version = rows(&harness).await.1.pop().unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO zip_manifest_entries (batch_id,path,object_key,cid,size,version_row_id,created_at) VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP)",
        vec![id.clone().into(), "file.txt".into(), "out/file.txt".into(), LEAF.into(), 0_i64.into(), version.id.into()],
    )).await.unwrap();
    assert!(
        zip::recovery::due_page(db).await.unwrap().is_empty(),
        "source-only excluded before claim"
    );
    assert_eq!(
        run_page(db, &harness.state.kubo, &CancellationToken::new())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        calls.len()
    );
}
