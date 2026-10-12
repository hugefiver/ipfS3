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
    create_and_upload_with_controls(source, extracted, "none", None, extracted).await
}

async fn create_and_upload_with_controls(
    source: bool,
    extracted: bool,
    targets: &'static str,
    root_tag: Option<bool>,
    fail_first_root: bool,
) -> (TestHarness, String) {
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
    directory_rpc(&harness, fail_first_root).await;
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
        ("x-ipfs3-zip-targets", targets),
        ("x-ipfs3-zip-token", "mpu-root-recovery"),
    ] {
        controls.insert(name, HeaderValue::from_static(value));
    }
    if let Some(enabled) = root_tag {
        controls.insert(
            "x-amz-tagging",
            HeaderValue::from_static(if enabled {
                "ipfs-s3%3Azip-root=true"
            } else {
                "ipfs-s3%3Azip-root=false"
            }),
        );
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
    let record = store::multipart::v2_zip::read_by_upload(db, &id)
        .await
        .unwrap()
        .unwrap();
    let options = record.options().unwrap();
    let intent = record.zip_intent().unwrap().unwrap();
    assert_eq!(capture["options"], serde_json::to_value(&options).unwrap());
    assert_eq!(intent.v2_options(), Some(options.clone()));
    assert_eq!(capture["schema_version"], 1);
    assert_eq!(capture["target_prefix"], "out/");
    assert_eq!(options.result_version, 2);
    assert_eq!(options.publish_source, source);
    assert!(options.publish_extracted && options.build_root());
    assert!(intent.controls().root.enabled());
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
        "worker must recover the original MPU envelope, not quarantine it: {:?}",
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
async fn persisted_mpu_capture_requires_exact_protocol_flags_token_and_root_identity() {
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
        ("/options/result_version", json!(1)),
        ("/options/result_version", json!("2")),
        ("/options/publish_source", json!(true)),
        ("/options/publish_source", json!("false")),
        ("/options/publish_extracted", json!(false)),
        ("/options/publish_extracted", json!(1)),
        ("/options/targets", json!("source")),
        ("/options/targets", json!("both")),
        ("/options/targets", json!("all")),
        ("/options/token", json!("other")),
        ("/options/token", json!("")),
        ("/options/token", json!("bad/token")),
        ("/options/root_enabled", json!(false)),
        ("/options/root_override", json!(false)),
        ("/options/root_override", json!("true")),
        ("/target_prefix", json!("other/")),
        ("/schema_version", json!(2)),
        ("/rule_revision", json!("different")),
        ("/zip_intent/bucket", json!("another")),
        ("/zip_intent/source_key", json!("another.zip")),
        ("/zip_intent/target_prefix", json!("other/")),
        (
            "/zip_intent/source_decision/origin/principal_id",
            json!("another"),
        ),
        (
            "/zip_intent/source_decision/origin/request_id",
            json!("another"),
        ),
        ("/zip_intent/controls/admission_token", json!("other")),
        ("/zip_intent/controls/root", json!({"configured":false})),
        ("/zip_intent/schema_version", json!(2)),
        (
            "/options",
            json!({"root_enabled":true,"publish_extracted":true}),
        ),
        ("/zip_intent", json!({"root_capture":{"configured":true}})),
    ] {
        let mut forged = zip::snapshot(db, &id).await.unwrap().unwrap();
        let mut options = captured.clone();
        *options.pointer_mut(field).unwrap() = value;
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
            "wrong MPU capture source {source}"
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
        options["options"]["token"] = token.clone().into();
        options["zip_intent"]["controls"]["admission_token"] = token.clone().into();
        forged.batch.token = token;
        forged.batch.captured_options = options.to_string();
        assert!(
            zip::recovery::files(&forged, &claim).is_err(),
            "token syntax must be valid even when identities agree"
        );
    }
    let mut stale = zip::snapshot(db, &id).await.unwrap().unwrap();
    stale.batch.root_revision += 1;
    assert!(zip::recovery::files(&stale, &claim).is_err());
    let mut stale = zip::snapshot(db, &id).await.unwrap().unwrap();
    stale.batch.root_epoch += 1;
    assert!(zip::recovery::files(&stale, &claim).is_err());
    let mut invalid_path = zip::snapshot(db, &id).await.unwrap().unwrap();
    invalid_path.entries[0].path = "invalid/0".into();
    assert!(
        zip::recovery::files(&invalid_path, &claim).is_err(),
        "original prefix remains mandatory"
    );
    let mut legacy = zip::snapshot(db, &id).await.unwrap().unwrap();
    legacy.batch.captured_options = captured["options"].to_string();
    assert!(
        zip::recovery::files(&legacy, &claim).is_ok(),
        "complete bare options are shape-valid, not original-capture authorization"
    );
    let published = rows(&harness).await;
    let remotes = remote_counts(&harness).await;
    let execution = zip::execution::read(db, &id).await.unwrap().unwrap();
    let intake = store::multipart::v2_zip::read_by_upload(db, &id)
        .await
        .unwrap()
        .unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE zip_batches SET captured_options=? WHERE id=?",
        vec![
            legacy.batch.captured_options.clone().into(),
            id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    expire_root_lease(&harness, &id).await;
    let calls = harness.kubo.received_requests().await.unwrap().len();
    let additions = harness.captured_add_file_bytes();
    assert_eq!(
        run_page(db, &harness.state.kubo, &CancellationToken::new())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        calls,
        "stripping a modern envelope must fail provenance before any Kubo request"
    );
    assert_eq!(harness.captured_add_file_bytes(), additions);
    assert_eq!(rows(&harness).await, published);
    assert_eq!(remote_counts(&harness).await, remotes);
    let after = zip::snapshot(db, &id).await.unwrap().unwrap();
    assert_eq!(after.batch.captured_options, legacy.batch.captured_options);
    assert_eq!(after.batch.terminal_result, snapshot.batch.terminal_result);
    assert_eq!(after.entries, snapshot.entries);
    assert!(!after.batch.source_published);
    assert_eq!(after.batch.root_status, "failed");
    assert_eq!(after.batch.root_cid, None);
    assert_eq!(after.builds.last().unwrap().status, "intent");
    assert_eq!(
        zip::execution::read(db, &id).await.unwrap().unwrap(),
        execution
    );
    assert_eq!(
        store::multipart::v2_zip::read_by_upload(db, &id)
            .await
            .unwrap()
            .unwrap()
            .captured_options,
        intake.captured_options
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
        capture["options"]["root_enabled"], true,
        "captured config alone is not root permission"
    );
    assert_eq!(capture["options"]["publish_extracted"], false);
    assert_eq!(
        capture["zip_intent"]["controls"]["root"]["configured"],
        true
    );
    assert_eq!(
        capture["zip_intent"]["controls"]["publish_extracted"],
        false
    );
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

#[tokio::test]
async fn root_override_and_targets_are_validated_from_real_original_mpu_admissions() {
    for (source, targets) in [(false, "extracted"), (true, "source"), (true, "both")] {
        let (harness, id) =
            create_and_upload_with_controls(source, true, targets, Some(true), true).await;
        let response = complete(&harness, &id).await;
        let status = response.status();
        let xml = response.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{xml}");
        let record = store::multipart::v2_zip::read_by_upload(harness.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap();
        let options = record.options().unwrap();
        let intent = record.zip_intent().unwrap().unwrap();
        assert_eq!(options.root_override, Some(true));
        assert_eq!(options.publish_source, source);
        assert_eq!(intent.v2_options(), Some(options));
        let snapshot = zip::snapshot(harness.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap();
        let claim = zip::RootClaim {
            batch_id: id,
            revision: snapshot.batch.root_revision,
            epoch: snapshot.batch.root_epoch,
            worker: snapshot.builds.last().unwrap().worker.clone(),
        };
        assert_eq!(zip::recovery::files(&snapshot, &claim).unwrap().len(), 1);
    }
}

#[tokio::test]
async fn paired_original_mpu_envelope_tampering_is_409_before_rpc_or_publication() {
    let (harness, id) = create_and_upload_with_controls(false, true, "none", None, false).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .expect(0)
        .mount(&harness.kubo)
        .await;
    let db = harness.state.store.db();
    let record = store::multipart::v2_zip::read_by_upload(db, &id)
        .await
        .unwrap()
        .unwrap();
    let original: Value = serde_json::from_str(&record.captured_options).unwrap();
    let before = harness.kubo.received_requests().await.unwrap().len();
    for (pointer, value) in [
        ("/schema_version", json!(2)),
        ("/options/token", json!("other")),
        ("/options/publish_source", json!(true)),
        ("/options/publish_extracted", json!(false)),
        ("/options/root_enabled", json!(false)),
        ("/options/root_override", json!(false)),
        ("/zip_intent/schema_version", json!(2)),
        (
            "/zip_intent/source_decision/origin/request_id",
            json!("other"),
        ),
        (
            "/zip_intent/source_decision/origin/principal_id",
            json!("other"),
        ),
        ("/zip_intent/controls/root", json!({"configured":false})),
        ("/target_prefix", json!("other/")),
        ("/rule_revision", json!("other")),
    ] {
        let mut tampered = original.clone();
        *tampered.pointer_mut(pointer).unwrap() = value;
        let bytes = tampered.to_string();
        for (table, identity) in [
            ("zip_v2_mpu_intakes", "original_upload_id"),
            ("zip_v2_executions", "id"),
        ] {
            db.execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                format!("UPDATE {table} SET captured_options=? WHERE {identity}=?"),
                vec![bytes.clone().into(), id.clone().into()],
            ))
            .await
            .unwrap();
        }
        let response = complete(&harness, &id).await;
        let status = response.status();
        let xml = response.text().await.unwrap();
        assert_eq!(
            harness.kubo.received_requests().await.unwrap().len(),
            before
        );
        assert_eq!(rows(&harness).await, (vec![], vec![]));
        assert_eq!(remote_counts(&harness).await, vec![0; 5]);
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "tamper {pointer}: {xml}; zero extra RPC/publication/remote rows verified"
        );
    }
}
