//! Signed HTTP regression tests for the legacy ZIP root opt-in/out and batch receipt.
#[allow(dead_code)]
mod support;

use std::collections::HashMap;
use std::sync::Arc;

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store::{object, zip};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, EntityTrait, PaginatorTrait, Statement, TransactionTrait,
};
use support::decompress::{
    AddReply, KuboScript, complete_multipart, create_multipart_with_headers, duplicate_entry_zip,
    legal_single_entry_zip, legal_two_entry_zip, start_harness, start_harness_with_root_default,
    start_harness_with_root_default_and_database, start_s3_server, upload_part,
};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path, query_param},
};

fn root_tag(enabled: bool) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(&format!("ipfs-s3%3Azip-root={enabled}")).unwrap(),
    );
    headers
}

const FILE_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const ROOT_CID: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";

async fn assert_no_root_rpc(kubo: &wiremock::MockServer) {
    let requests = kubo.received_requests().await.unwrap();
    assert!(
        !requests.iter().any(|request| matches!(
            request.url.path(),
            "/api/v0/block/stat"
                | "/api/v0/dag/stat"
                | "/api/v0/dag/put"
                | "/api/v0/files/stat"
                | "/api/v0/resolve"
        )),
        "root RPC observed while ZIP root is disabled: {requests:?}"
    );
}

#[tokio::test]
async fn signed_put_config_false_without_tag_skips_directory_rpc() {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmFile")],
            cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
        },
        false,
    )
    .await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "disabled");
    assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    assert_no_root_rpc(&harness.kubo).await;
}

#[tokio::test]
async fn signed_put_tag_true_overrides_config_false() {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok(FILE_CID)],
            cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
        },
        false,
    )
    .await;
    mount_verified_directory(&harness.kubo).await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        archive,
        root_tag(true),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "complete");
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap();
    let before = harness.kubo.received_requests().await.unwrap().len();
    let status = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("ipfs3-zip-batch", batch_id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(status.status(), StatusCode::OK);
    assert!(
        status
            .text()
            .await
            .unwrap()
            .contains(&format!("<RootCID>{ROOT_CID}</RootCID>"))
    );
    let wrong_key = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "unrelated.zip",
        &[("ipfs3-zip-batch", batch_id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(wrong_key.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        before
    );
}

#[tokio::test]
async fn signed_multipart_config_false_without_tag_stays_disabled_through_complete() {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![
                AddReply::Ok("QmPart"),
                AddReply::Ok("QmArchive"),
                AddReply::Ok("QmFile"),
            ],
            cat_bodies: HashMap::from([
                ("QmPart".to_owned(), archive.clone()),
                ("QmArchive".to_owned(), archive.clone()),
            ]),
        },
        false,
    )
    .await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        HeaderMap::new(),
    )
    .await;
    let before = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&before.batch.captured_options).unwrap()["root_enabled"],
        false
    );
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "disabled");
    assert_no_root_rpc(&harness.kubo).await;
}

#[tokio::test]
async fn signed_multipart_tag_true_overrides_config_false_at_initiation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![
                AddReply::Ok("QmPart"),
                AddReply::Ok("QmArchive"),
                AddReply::Ok(FILE_CID),
            ],
            cat_bodies: HashMap::from([
                ("QmPart".to_owned(), archive.clone()),
                ("QmArchive".to_owned(), archive.clone()),
            ]),
        },
        false,
    )
    .await;
    mount_verified_directory(&harness.kubo).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        root_tag(true),
    )
    .await;
    let before = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&before.batch.captured_options).unwrap()["root_enabled"],
        true
    );
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "complete");
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
}

struct RestartedEndpoint<'a> {
    endpoint: &'a str,
    bucket: &'a str,
}

impl support::decompress::S3TestEndpoint for RestartedEndpoint<'_> {
    fn endpoint(&self) -> &str {
        self.endpoint
    }

    fn bucket(&self) -> &str {
        self.bucket
    }
}

#[tokio::test]
async fn signed_multipart_untagged_root_capture_survives_config_change_and_restart() {
    for (original_default, restarted_default) in [(false, true), (true, false)] {
        let directory = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("gateway.sqlite")
                .display()
                .to_string()
                .replace('\\', "/")
        );
        let archive = legal_single_entry_zip();
        let harness = start_harness_with_root_default_and_database(
            KuboScript {
                add_replies: vec![
                    AddReply::Ok("QmPart"),
                    AddReply::Ok("QmArchive"),
                    AddReply::Ok(FILE_CID),
                ],
                cat_bodies: HashMap::from([
                    ("QmPart".to_owned(), archive.clone()),
                    ("QmArchive".to_owned(), archive.clone()),
                ]),
            },
            original_default,
            &database_url,
        )
        .await;
        if original_default {
            mount_verified_directory(&harness.kubo).await;
        }
        let upload_id = create_multipart_with_headers(
            &harness,
            "archive.zip",
            &[("decompress-zip", "expanded/")],
            HeaderMap::new(),
        )
        .await;
        let before = zip::snapshot(harness.state.store.db(), &upload_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&before.batch.captured_options).unwrap()["root_enabled"],
            original_default
        );
        let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;

        // Re-open the file-backed DB using a new configured application state.
        let config: ipfs_s3_gateway::config::Config = toml::from_str(&format!(
            "[kubo]\nrpc_url = {:?}\n[storage]\ndatabase_url = {:?}\n[decompress_zip]\nunixfs_directory_root = {restarted_default}\n",
            harness.kubo.uri(),
            database_url
        ))
        .unwrap();
        let restarted = ipfs_s3_gateway::state::AppState::new(&config)
            .await
            .unwrap();
        assert_eq!(restarted.pinning.zip_root_default(), restarted_default);
        assert!(
            zip::snapshot(restarted.store.db(), &upload_id)
                .await
                .unwrap()
                .is_some()
        );
        let server =
            start_s3_server(restarted, Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;
        let response = complete_multipart(
            &RestartedEndpoint {
                endpoint: &server.endpoint,
                bucket: &harness.bucket,
            },
            "archive.zip",
            &upload_id,
            &[(1, etag)],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["x-ipfs-s3-zip-root-status"],
            if original_default {
                "complete"
            } else {
                "disabled"
            }
        );
        if original_default {
            let requests = harness.kubo.received_requests().await.unwrap();
            assert!(
                requests
                    .iter()
                    .any(|request| request.url.path() == "/api/v0/dag/put")
            );
        } else {
            assert_no_root_rpc(&harness.kubo).await;
        }
    }
}

fn archive_with_one_byte_files(names: &[&str]) -> Vec<u8> {
    let mut zip = Vec::new();
    let mut offsets = Vec::new();
    for name in names {
        offsets.push(zip.len() as u32);
        zip.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        zip.extend_from_slice(&20u16.to_le_bytes());
        zip.extend_from_slice(&[0; 8]);
        zip.extend_from_slice(&0xd202_ef8du32.to_le_bytes());
        zip.extend_from_slice(&1u32.to_le_bytes());
        zip.extend_from_slice(&1u32.to_le_bytes());
        zip.extend_from_slice(&(name.len() as u16).to_le_bytes());
        zip.extend_from_slice(&0u16.to_le_bytes());
        zip.extend_from_slice(name.as_bytes());
        zip.push(0);
    }
    let central = zip.len() as u32;
    for (name, offset) in names.iter().zip(offsets) {
        zip.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        zip.extend_from_slice(&20u16.to_le_bytes());
        zip.extend_from_slice(&20u16.to_le_bytes());
        zip.extend_from_slice(&[0; 8]);
        zip.extend_from_slice(&0xd202_ef8du32.to_le_bytes());
        zip.extend_from_slice(&1u32.to_le_bytes());
        zip.extend_from_slice(&1u32.to_le_bytes());
        zip.extend_from_slice(&(name.len() as u16).to_le_bytes());
        zip.extend_from_slice(&0u16.to_le_bytes());
        zip.extend_from_slice(&[0; 12]);
        zip.extend_from_slice(&offset.to_le_bytes());
        zip.extend_from_slice(name.as_bytes());
    }
    let central_size = zip.len() as u32 - central;
    zip.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    zip.extend_from_slice(&[0; 4]);
    zip.extend_from_slice(&(names.len() as u16).to_le_bytes());
    zip.extend_from_slice(&(names.len() as u16).to_le_bytes());
    zip.extend_from_slice(&central_size.to_le_bytes());
    zip.extend_from_slice(&central.to_le_bytes());
    zip.extend_from_slice(&0u16.to_le_bytes());
    zip
}

#[tokio::test]
async fn signed_put_path_conflict_keeps_both_objects_and_fails_only_directory() {
    let archive = archive_with_one_byte_files(&["a", "a/b"]);
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmA"),
            AddReply::Ok("QmB"),
        ],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
    })
    .await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "failed");
    assert_eq!(
        response.headers()["x-ipfs-s3-zip-root-warning"],
        "path_conflict"
    );
    assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    assert_eq!(
        object::get_latest(harness.state.store.db(), &harness.bucket, "expanded/a")
            .await
            .unwrap()
            .cid,
        "QmA"
    );
    assert_eq!(
        object::get_latest(harness.state.store.db(), &harness.bucket, "expanded/a/b")
            .await
            .unwrap()
            .cid,
        "QmB"
    );
    let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap();
    let snapshot = zip::snapshot(harness.state.store.db(), batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.entries.len(), 2);
    let status = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("ipfs3-zip-batch", batch_id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(status.status(), StatusCode::OK);
    let status_xml = status.text().await.unwrap();
    assert!(status_xml.contains("<RootStatus>failed</RootStatus>"));
    assert!(status_xml.contains("path_conflict"));
    assert!(!status_xml.contains("<RootCID>"));
    assert!(
        snapshot
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_some())
    );
    assert!(
        !harness
            .kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path() == "/api/v0/pin/rm")
    );
}

#[tokio::test]
async fn signed_put_all_entry_uploads_failed_returns_empty_root_status_without_root_rpc() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Error(StatusCode::SERVICE_UNAVAILABLE, "private"),
        ],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
    })
    .await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    let headers = response.headers();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(headers["x-ipfs-s3-zip-root-status"], "empty");
    assert!(headers.get("x-ipfs-s3-zip-root-cid").is_none());
    let batch_id = headers["x-ipfs-s3-zip-batch-id"].to_str().unwrap();
    let snapshot = zip::snapshot(harness.state.store.db(), batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.entries.len(), 1);
    assert!(snapshot.entries[0].cid.is_none());
    assert!(snapshot.entries[0].error_code.is_some());
    assert!(
        !harness
            .kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path() == "/api/v0/dag/put")
    );
}

async fn mount_verified_directory(kubo: &wiremock::MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("with-local", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Hash\":\"{ROOT_CID}\",\"WithLocality\":true,\"Local\":true}}"
        )))
        .with_priority(2)
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{FILE_CID}\",\"CumulativeSize\":18}}")),
        )
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Cid\":{{\"/\":\"{ROOT_CID}\"}}}}")),
        )
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("recursive", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{ROOT_CID}\"]}}")),
        )
        .with_priority(2)
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/resolve"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Path\":\"/ipfs/{FILE_CID}\"}}")),
        )
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"ID\":\"localNode\"}"))
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Keys\":{{\"{ROOT_CID}\":{{\"Type\":\"recursive\"}}}}}}"
        )))
        .mount(kubo)
        .await;
}

#[tokio::test]
async fn signed_put_known_root_failures_retain_candidate_without_exposing_root() {
    for failure in ["pin", "resolve", "local"] {
        let archive = legal_single_entry_zip();
        let harness = start_harness(KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok(FILE_CID)],
            cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
        })
        .await;
        mount_verified_directory(&harness.kubo).await;
        let mock = match failure {
            "pin" => Mock::given(method("POST"))
                .and(path("/api/v0/pin/add"))
                .and(query_param("arg", ROOT_CID))
                .respond_with(ResponseTemplate::new(503).set_body_string("private pin error")),
            "resolve" => Mock::given(method("POST"))
                .and(path("/api/v0/resolve"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_string(format!("{{\"Path\":\"/ipfs/{ROOT_CID}\"}}")),
                ),
            _ => Mock::given(method("POST"))
                .and(path("/api/v0/files/stat"))
                .and(query_param("with-local", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    "{{\"Hash\":\"{ROOT_CID}\",\"WithLocality\":true,\"Local\":false}}"
                ))),
        };
        mock.with_priority(1).mount(&harness.kubo).await;
        let response = support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            &harness.bucket,
            "archive.zip",
            &[("decompress-zip", "expanded/")],
            archive,
            HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "{failure}");
        assert_eq!(
            response.headers()["x-ipfs-s3-zip-root-status"],
            "failed",
            "{failure}"
        );
        assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
        assert_eq!(response.headers()[http::header::ETAG], "\"QmArchive\"");
        let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
            .to_str()
            .unwrap();
        let snapshot = zip::snapshot(harness.state.store.db(), batch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.batch.root_status, "failed");
        assert_eq!(snapshot.builds.len(), 1);
        assert_eq!(snapshot.builds[0].status, "invoked");
        assert_eq!(snapshot.references.len(), 1);
        assert_eq!(snapshot.references[0].state, "retained");
        assert_eq!(snapshot.references[0].cid, ROOT_CID);
        assert_eq!(snapshot.references[0].node_identity, "localNode");
        assert!(snapshot.references[0].verification_receipt.is_none());
        assert_eq!(snapshot.entries[0].cid.as_deref(), Some(FILE_CID));
        assert!(snapshot.entries[0].version_row_id.is_some());
        assert_eq!(
            object::get_latest(
                harness.state.store.db(),
                &harness.bucket,
                "expanded/file.txt"
            )
            .await
            .unwrap()
            .cid,
            FILE_CID
        );
        assert!(
            !harness
                .kubo
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| request.url.path() == "/api/v0/pin/rm")
        );
    }
}

#[tokio::test]
async fn signed_put_candidate_retention_failure_keeps_objects_and_recoverable_intent() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok(FILE_CID)],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
    })
    .await;
    mount_verified_directory(&harness.kubo).await;
    harness.state.store.db().execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "CREATE TRIGGER reject_candidate BEFORE INSERT ON zip_root_references BEGIN SELECT RAISE(FAIL, 'private candidate error'); END;",
    )).await.unwrap();
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "failed");
    assert_eq!(
        response.headers()["x-ipfs-s3-zip-root-warning"],
        "root_receipt_failed"
    );
    assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap();
    let snapshot = zip::snapshot(harness.state.store.db(), batch_id)
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.references.is_empty());
    assert_eq!(snapshot.builds[0].status, "invoked");
    assert_eq!(
        zip::recovery(harness.state.store.db(), batch_id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(snapshot.entries[0].version_row_id.is_some());
    assert_eq!(
        object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "expanded/file.txt"
        )
        .await
        .unwrap()
        .cid,
        FILE_CID
    );
}

#[tokio::test]
async fn signed_put_verified_root_is_exposed_only_after_atomic_binding() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok(FILE_CID)],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
    })
    .await;
    let kubo = &harness.kubo;
    mount_verified_directory(kubo).await;

    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "")],
        archive,
        root_tag(true),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "complete");
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    assert_eq!(response.headers()[http::header::ETAG], "\"QmArchive\"");
    let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap();
    let snapshot = zip::snapshot(harness.state.store.db(), batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.root_cid.as_deref(), Some(ROOT_CID));
    assert!(
        snapshot
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_some())
    );
    assert_eq!(
        snapshot
            .references
            .iter()
            .filter(|reference| reference.state == "adopted")
            .count(),
        1
    );
    assert!(
        !kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path() == "/api/v0/pin/rm")
    );
}

#[tokio::test]
async fn signed_put_partial_root_excludes_failed_entry_and_binds_winner() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Error(StatusCode::SERVICE_UNAVAILABLE, "private-kubo-message"),
            AddReply::Ok(FILE_CID),
        ],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
    })
    .await;
    mount_verified_directory(&harness.kubo).await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "partial");
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap();
    let snapshot = zip::snapshot(harness.state.store.db(), batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot
            .entries
            .iter()
            .filter(|entry| entry.cid.is_some())
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .entries
            .iter()
            .filter(|entry| entry.error_code.is_some())
            .count(),
        1
    );
    assert_eq!(
        object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "expanded/second.txt"
        )
        .await
        .unwrap()
        .cid,
        FILE_CID
    );
    assert!(
        !response
            .text()
            .await
            .unwrap()
            .contains("private-kubo-message")
    );
}

#[tokio::test]
async fn signed_put_root_publication_transaction_failure_rolls_back_objects_and_receipt() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok(FILE_CID)],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
    })
    .await;
    mount_verified_directory(&harness.kubo).await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER reject_zip_batch_publish BEFORE UPDATE ON zip_batches \
         WHEN NEW.state = 'published' BEGIN SELECT RAISE(FAIL, 'private root DB error'); END;",
        ))
        .await
        .unwrap();

    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    assert!(
        !response
            .text()
            .await
            .unwrap()
            .contains("private root DB error")
    );
    assert!(
        object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
            .await
            .is_err()
    );
    assert!(
        object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "expanded/file.txt"
        )
        .await
        .is_err()
    );
    let batch = ipfs_s3_gateway::store::entities::zip_batch::Entity::find()
        .one(harness.state.store.db())
        .await
        .unwrap()
        .unwrap();
    let snapshot = zip::snapshot(harness.state.store.db(), &batch.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "open");
    assert_eq!(snapshot.batch.root_status, "pending");
    assert!(snapshot.entries[0].version_row_id.is_none());
    assert_eq!(snapshot.references.len(), 1);
    assert_eq!(snapshot.references[0].state, "retained");
    let requests = harness.kubo.received_requests().await.unwrap();
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/api/v0/pin/rm")
    );
}

#[tokio::test]
async fn signed_put_default_on_fails_root_safely_but_keeps_published_objects() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmFile")],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
    })
    .await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "failed");
    assert_eq!(
        response.headers()["x-ipfs-s3-zip-root-warning"],
        "invalid_manifest"
    );
    assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    assert_eq!(response.headers()[http::header::ETAG], "\"QmArchive\"");
    let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap();
    let snapshot = zip::snapshot(harness.state.store.db(), batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.root_status, "failed");
    assert_eq!(snapshot.entries.len(), 1);
    assert!(snapshot.entries[0].version_row_id.is_some());
    assert_eq!(
        object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "expanded/file.txt"
        )
        .await
        .unwrap()
        .cid,
        "QmFile"
    );
    let requests = harness.kubo.received_requests().await.unwrap();
    assert!(
        !requests
            .iter()
            .any(|req| req.url.path() == "/api/v0/pin/rm")
    );
}

#[tokio::test]
async fn signed_put_tag_false_publishes_only_last_successful_duplicate() {
    let archive = duplicate_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmFirst"),
            AddReply::Ok("QmLast"),
        ],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
    })
    .await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        archive,
        root_tag(false),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "disabled");
    let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap();
    let snapshot = zip::snapshot(harness.state.store.db(), batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(snapshot.entries[0].cid.as_deref(), Some("QmLast"));
    assert!(snapshot.entries[0].version_row_id.is_some());
    assert_eq!(
        object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "expanded/duplicate.txt"
        )
        .await
        .unwrap()
        .cid,
        "QmLast"
    );
    assert!(!response.text().await.unwrap().contains("QmFirst"));
}

#[tokio::test]
async fn signed_multipart_tag_false_is_captured_at_initiation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmFile"),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive.clone()),
            ("QmArchive".to_owned(), archive.clone()),
        ]),
    })
    .await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        root_tag(false),
    )
    .await;
    let before = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.batch.state, "open");
    assert!(
        before
            .batch
            .captured_options
            .contains("\"root_enabled\":false")
    );
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "disabled");
    assert_eq!(response.headers()["x-ipfs-s3-zip-batch-id"], upload_id);
    let after = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.batch.state, "published");
    assert_eq!(after.batch.root_status, "disabled");
    assert_eq!(after.entries.len(), 1);
    assert!(after.entries[0].version_row_id.is_some());
}

#[tokio::test]
async fn signed_multipart_default_on_returns_verified_directory_without_changing_archive_etag() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok(FILE_CID),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive.clone()),
            ("QmArchive".to_owned(), archive.clone()),
        ]),
    })
    .await;
    mount_verified_directory(&harness.kubo).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        HeaderMap::new(),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "complete");
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    assert_eq!(response.headers()[http::header::ETAG], "\"QmArchive\"");
    let snapshot = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.root_cid.as_deref(), Some(ROOT_CID));
    assert_eq!(snapshot.entries.len(), 1);
    assert!(snapshot.entries[0].version_row_id.is_some());
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("<ArchiveETag>QmArchive</ArchiveETag>")
    );
}

#[tokio::test]
async fn signed_multipart_replays_original_ack_after_archive_overwrite_and_delete() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok(FILE_CID),
            AddReply::Ok("QmOverwrite"),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive.clone()),
            ("QmArchive".to_owned(), archive.clone()),
        ]),
    })
    .await;
    mount_verified_directory(&harness.kubo).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        HeaderMap::new(),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let first = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_eq!(first.status(), StatusCode::OK);
    let headers = first.headers().clone();
    assert_eq!(headers["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    let body = first.text().await.unwrap();
    let overwritten = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[],
        b"replacement".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(overwritten.status(), StatusCode::OK);
    assert_eq!(
        object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
            .await
            .unwrap()
            .cid,
        "QmOverwrite"
    );
    let object_count = ipfs_s3_gateway::store::entities::object_version::Entity::find()
        .count(harness.state.store.db())
        .await
        .unwrap();
    let kubo_count = harness.kubo.received_requests().await.unwrap().len();
    let after_overwrite =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_eq!(
        ipfs_s3_gateway::store::entities::object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        object_count
    );
    let deleted = support::sigv4::send_sigv4(
        reqwest::Method::DELETE,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let count_after_delete = ipfs_s3_gateway::store::entities::object_version::Entity::find()
        .count(harness.state.store.db())
        .await
        .unwrap();
    let after_delete =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_eq!(
        ipfs_s3_gateway::store::entities::object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        count_after_delete
    );
    for replay in [after_overwrite, after_delete] {
        assert_eq!(replay.status(), StatusCode::OK);
        for name in [
            "etag",
            "content-type",
            "x-ipfs-s3-zip-batch-id",
            "x-ipfs-s3-zip-root-status",
            "x-ipfs-s3-zip-root-cid",
        ] {
            assert_eq!(replay.headers()[name], headers[name]);
        }
        assert_eq!(replay.text().await.unwrap(), body);
        assert_eq!(
            harness.kubo.received_requests().await.unwrap().len(),
            kubo_count
        );
    }
    let changed = complete_multipart(
        &harness,
        "archive.zip",
        &upload_id,
        &[(1, "different".into())],
    )
    .await;
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        kubo_count
    );
}

#[tokio::test]
async fn signed_multipart_replay_rejects_changed_checksum_and_authenticated_owner() {
    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("gateway.sqlite")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default_and_database(
        KuboScript {
            add_replies: vec![
                AddReply::Ok("QmPart"),
                AddReply::Ok("QmArchive"),
                AddReply::Ok("QmFile"),
            ],
            cat_bodies: HashMap::from([
                ("QmPart".to_owned(), archive.clone()),
                ("QmArchive".to_owned(), archive.clone()),
            ]),
        },
        false,
        &database_url,
    )
    .await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        HeaderMap::new(),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let first = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_eq!(first.status(), StatusCode::OK);
    let before_requests = harness.kubo.received_requests().await.unwrap().len();
    let before_versions = ipfs_s3_gateway::store::entities::object_version::Entity::find()
        .count(harness.state.store.db())
        .await
        .unwrap();
    let changed_xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{etag}\"</ETag><ChecksumCRC32>changed</ChecksumCRC32></Part></CompleteMultipartUpload>"
    );
    let changed = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", &upload_id)],
        changed_xml.into_bytes(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(changed.status(), StatusCode::CONFLICT);

    let other_config: ipfs_s3_gateway::config::Config = toml::from_str(&format!(
        "[kubo]\nrpc_url = {:?}\n[storage]\ndatabase_url = {:?}\n[auth]\n[[auth.credentials]]\naccess_key = 'other'\nsecret_key = 'other'\n",
        harness.kubo.uri(), database_url,
    )).unwrap();
    let other_state = ipfs_s3_gateway::state::AppState::new(&other_config)
        .await
        .unwrap();
    let other_server =
        start_s3_server(other_state, Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;
    let other_xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{etag}\"</ETag></Part></CompleteMultipartUpload>"
    );
    let signed_url = support::sigv4::presign_sigv4_query(
        &reqwest::Method::POST,
        &other_server.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", &upload_id)],
        "other",
        "other",
        None,
        60,
        chrono::Utc::now(),
    );
    let other = reqwest::Client::new()
        .post(signed_url)
        .body(other_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(other.status(), StatusCode::CONFLICT);
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        before_requests
    );
    assert_eq!(
        ipfs_s3_gateway::store::entities::object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        before_versions
    );
}

#[tokio::test]
async fn signed_multipart_failed_known_root_replay_never_exposes_candidate_cid() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok(FILE_CID),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive.clone()),
            ("QmArchive".to_owned(), archive.clone()),
        ]),
    })
    .await;
    mount_verified_directory(&harness.kubo).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/resolve"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Path\":\"/ipfs/{ROOT_CID}\"}}")),
        )
        .with_priority(1)
        .mount(&harness.kubo)
        .await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        HeaderMap::new(),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let first = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["x-ipfs-s3-zip-root-status"], "failed");
    assert!(first.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    let body = first.text().await.unwrap();
    let snapshot = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.references.len(), 1);
    assert_eq!(snapshot.references[0].state, "retained");
    assert_eq!(snapshot.references[0].cid, ROOT_CID);
    assert!(snapshot.references[0].verification_receipt.is_none());
    let kubo_count = harness.kubo.received_requests().await.unwrap().len();
    let replay = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["x-ipfs-s3-zip-root-status"], "failed");
    assert!(replay.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    assert_eq!(replay.text().await.unwrap(), body);
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        kubo_count
    );
}

#[tokio::test]
async fn signed_multipart_batch_transaction_rollback_reuses_prepared_manifest_on_retry() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmFile"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmFile"),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive.clone()),
            ("QmArchive".to_owned(), archive.clone()),
        ]),
    })
    .await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        root_tag(false),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER reject_mpu_batch_publish BEFORE UPDATE ON zip_batches \
         WHEN NEW.state = 'published' BEGIN SELECT RAISE(FAIL, 'private MPU root DB error'); END;",
        ))
        .await
        .unwrap();

    let response =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    assert!(
        !response
            .text()
            .await
            .unwrap()
            .contains("private MPU root DB error")
    );
    assert!(
        ipfs_s3_gateway::store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_ok()
    );
    assert!(
        object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
            .await
            .is_err()
    );
    assert!(
        object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "expanded/file.txt"
        )
        .await
        .is_err()
    );
    let snapshot = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "open");
    assert!(snapshot.batch.manifest_prepared);
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(snapshot.entries[0].cid.as_deref(), Some("QmFile"));
    assert!(snapshot.entries[0].version_row_id.is_none());
    assert!(
        !harness
            .kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path() == "/api/v0/pin/rm")
    );

    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DROP TRIGGER reject_mpu_batch_publish",
        ))
        .await
        .unwrap();
    let retry = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(
        retry.status(),
        StatusCode::OK,
        "{}",
        retry.text().await.unwrap()
    );
    let after = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.batch.state, "published");
    assert_eq!(after.entries.len(), 1);
    assert_eq!(after.entries[0].created_at, snapshot.entries[0].created_at);
    assert!(after.entries[0].version_row_id.is_some());
    assert_eq!(
        ipfs_s3_gateway::store::entities::object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        ipfs_s3_gateway::store::entities::zip_batch::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        ipfs_s3_gateway::store::entities::pin_lease::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn signed_multipart_replay_receipt_sql_failure_rolls_back_object_batch_and_receipt() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmFile"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmFile"),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive.clone()),
            ("QmArchive".to_owned(), archive.clone()),
        ]),
    })
    .await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        root_tag(false),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let db = harness.state.store.db();
    db.execute(Statement::from_string(DatabaseBackend::Sqlite,
        "CREATE TRIGGER reject_zip_replay BEFORE UPDATE ON zip_mpu_replays WHEN NEW.archive_cid IS NOT NULL BEGIN SELECT RAISE(FAIL, 'private replay SQL error'); END;",
    )).await.unwrap();
    let failed =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        !failed
            .text()
            .await
            .unwrap()
            .contains("private replay SQL error")
    );
    let pending = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
    assert_eq!(pending.batch.state, "open");
    assert!(pending.batch.manifest_prepared);
    assert!(pending.batch.terminal_result.is_none());
    assert!(pending.entries[0].version_row_id.is_none());
    let receipt = db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT prepared_archive_cid,archive_cid,response_xml FROM zip_mpu_replays WHERE batch_id=?",
        [upload_id.clone().into()],
    )).await.unwrap().unwrap();
    assert_eq!(
        receipt
            .try_get::<String>("", "prepared_archive_cid")
            .unwrap(),
        "QmArchive"
    );
    assert!(
        receipt
            .try_get::<Option<String>>("", "archive_cid")
            .unwrap()
            .is_none()
    );
    assert!(
        receipt
            .try_get::<Option<String>>("", "response_xml")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        ipfs_s3_gateway::store::entities::object_version::Entity::find()
            .count(db)
            .await
            .unwrap(),
        0
    );
    assert!(
        ipfs_s3_gateway::store::multipart::get_upload(db, &upload_id)
            .await
            .is_ok()
    );
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "DROP TRIGGER reject_zip_replay",
    ))
    .await
    .unwrap();
    let retry = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_eq!(retry.status(), StatusCode::OK);
    let xml = retry.text().await.unwrap();
    let published = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
    assert_eq!(published.batch.state, "published");
    assert_eq!(
        published.entries[0].created_at,
        pending.entries[0].created_at
    );
    let replay = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.text().await.unwrap(), xml);
}

#[tokio::test]
async fn signed_multipart_prepared_manifest_rejects_changed_cid_or_failed_extraction() {
    for changed in [
        AddReply::Ok("QmChanged"),
        AddReply::Error(StatusCode::SERVICE_UNAVAILABLE, "private"),
    ] {
        let archive = legal_single_entry_zip();
        let harness = start_harness(KuboScript {
            add_replies: vec![
                AddReply::Ok("QmPart"),
                AddReply::Ok("QmArchive"),
                AddReply::Ok("QmFile"),
                AddReply::Ok("QmArchive"),
                changed,
            ],
            cat_bodies: HashMap::from([
                ("QmPart".to_owned(), archive.clone()),
                ("QmArchive".to_owned(), archive.clone()),
            ]),
        })
        .await;
        let upload_id = create_multipart_with_headers(
            &harness,
            "archive.zip",
            &[("decompress-zip", "expanded/")],
            root_tag(false),
        )
        .await;
        let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
        let db = harness.state.store.db();
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER reject_mpu_batch_publish BEFORE UPDATE ON zip_batches \
             WHEN NEW.state = 'published' BEGIN SELECT RAISE(FAIL, 'rollback'); END;",
        ))
        .await
        .unwrap();
        assert_eq!(
            complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())])
                .await
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DROP TRIGGER reject_mpu_batch_publish",
        ))
        .await
        .unwrap();
        let before = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
        let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let after = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
        assert_eq!(after.batch.state, "open");
        assert_eq!(after.entries, before.entries);
        assert!(
            ipfs_s3_gateway::store::multipart::get_upload(db, &upload_id)
                .await
                .is_ok()
        );
        assert_eq!(
            ipfs_s3_gateway::store::entities::object_version::Entity::find()
                .count(db)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            ipfs_s3_gateway::store::entities::pin_lease::Entity::find()
                .count(db)
                .await
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn signed_multipart_prepared_manifest_rejects_changed_source_archive_cid() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmFile"),
            AddReply::Ok("QmChangedArchive"),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive.clone()),
            ("QmArchive".to_owned(), archive.clone()),
            ("QmChangedArchive".to_owned(), archive.clone()),
        ]),
    })
    .await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        root_tag(false),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let db = harness.state.store.db();
    db.execute(Statement::from_string(DatabaseBackend::Sqlite,
        "CREATE TRIGGER reject_mpu_batch_publish BEFORE UPDATE ON zip_batches WHEN NEW.state = 'published' BEGIN SELECT RAISE(FAIL, 'rollback'); END;",
    )).await.unwrap();
    assert_eq!(
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())])
            .await
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "DROP TRIGGER reject_mpu_batch_publish",
    ))
    .await
    .unwrap();
    let before = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
    assert!(before.batch.manifest_prepared);
    assert!(before.batch.terminal_result.is_none());
    let prepared = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT prepared_archive_cid,archive_cid FROM zip_mpu_replays WHERE batch_id=?",
            [upload_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        prepared
            .try_get::<String>("", "prepared_archive_cid")
            .unwrap(),
        "QmArchive"
    );
    assert_eq!(
        prepared
            .try_get::<Option<String>>("", "archive_cid")
            .unwrap(),
        None
    );
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let after = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
    assert_eq!(after.batch.terminal_result, before.batch.terminal_result);
    assert_eq!(after.entries, before.entries);
    assert_eq!(after.batch.state, "open");
    let bound = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT prepared_archive_cid,archive_cid FROM zip_mpu_replays WHERE batch_id=?",
            [upload_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        bound.try_get::<String>("", "prepared_archive_cid").unwrap(),
        "QmArchive"
    );
    assert_eq!(
        bound.try_get::<Option<String>>("", "archive_cid").unwrap(),
        None
    );
    assert!(
        ipfs_s3_gateway::store::multipart::get_upload(db, &upload_id)
            .await
            .is_ok()
    );
    assert_eq!(
        ipfs_s3_gateway::store::entities::object_version::Entity::find()
            .count(db)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn signed_multipart_prepared_manifest_survives_restart_after_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("gateway.sqlite")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default_and_database(
        KuboScript {
            add_replies: vec![
                AddReply::Ok("QmPart"),
                AddReply::Ok("QmArchive"),
                AddReply::Ok("QmFile"),
                AddReply::Ok("QmArchive"),
                AddReply::Ok("QmFile"),
            ],
            cat_bodies: HashMap::from([
                ("QmPart".to_owned(), archive.clone()),
                ("QmArchive".to_owned(), archive.clone()),
            ]),
        },
        false,
        &database_url,
    )
    .await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        HeaderMap::new(),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER reject_mpu_batch_publish BEFORE UPDATE ON zip_batches \
         WHEN NEW.state = 'published' BEGIN SELECT RAISE(FAIL, 'rollback'); END;",
        ))
        .await
        .unwrap();
    assert_eq!(
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())])
            .await
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let prepared = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert!(prepared.batch.manifest_prepared);
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DROP TRIGGER reject_mpu_batch_publish",
        ))
        .await
        .unwrap();

    let config: ipfs_s3_gateway::config::Config = toml::from_str(&format!(
        "[kubo]\nrpc_url = {:?}\n[storage]\ndatabase_url = {:?}\n[decompress_zip]\nunixfs_directory_root = true\n",
        harness.kubo.uri(), database_url,
    )).unwrap();
    let restarted = ipfs_s3_gateway::state::AppState::new(&config)
        .await
        .unwrap();
    let server = start_s3_server(restarted, Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;
    let response = complete_multipart(
        &RestartedEndpoint {
            endpoint: &server.endpoint,
            bucket: &harness.bucket,
        },
        "archive.zip",
        &upload_id,
        &[(1, etag)],
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let after = zip::snapshot(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.batch.root_status, "disabled");
    assert_eq!(after.batch.state, "published");
    assert_eq!(after.entries[0].created_at, prepared.entries[0].created_at);
}

#[tokio::test]
async fn signed_multipart_expired_root_claim_retries_prepared_manifest() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok(FILE_CID),
            AddReply::Ok("QmArchive"),
            AddReply::Ok(FILE_CID),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive.clone()),
            ("QmArchive".to_owned(), archive.clone()),
        ]),
    })
    .await;
    mount_verified_directory(&harness.kubo).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        HeaderMap::new(),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let db = harness.state.store.db();
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "CREATE TRIGGER reject_mpu_batch_publish BEFORE UPDATE ON zip_batches \
         WHEN NEW.state = 'published' BEGIN SELECT RAISE(FAIL, 'rollback'); END;",
    ))
    .await
    .unwrap();
    assert_eq!(
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())])
            .await
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "DROP TRIGGER reject_mpu_batch_publish",
    ))
    .await
    .unwrap();
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "UPDATE zip_root_builds SET lease_until = '2000-01-01 00:00:00'",
    ))
    .await
    .unwrap();

    let retry = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(
        retry.status(),
        StatusCode::OK,
        "{}",
        retry.text().await.unwrap()
    );
    let after = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
    assert_eq!(after.batch.state, "published");
    assert_eq!(after.batch.root_status, "complete");
    assert_eq!(after.batch.root_cid.as_deref(), Some(ROOT_CID));
    assert_eq!(after.entries.len(), 1);
    assert_eq!(after.builds.len(), 2);
    let old_build = &after.builds[0];
    let old_claim = zip::RootClaim {
        batch_id: upload_id.clone(),
        revision: old_build.revision,
        epoch: old_build.epoch,
        worker: old_build.worker.clone(),
    };
    let tx = db.begin().await.unwrap();
    assert!(
        old_claim
            .settle_failed_retry(&tx, "{\"archive_cid\":\"QmArchive\"}", "root_build_failed")
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    let still_current = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
    assert_eq!(still_current.batch.root_cid.as_deref(), Some(ROOT_CID));
    assert_eq!(still_current.batch.root_status, "complete");
    assert_eq!(
        still_current
            .references
            .iter()
            .filter(|reference| reference.state == "adopted")
            .count(),
        1
    );
    assert!(
        still_current
            .references
            .iter()
            .any(|reference| reference.epoch == old_claim.epoch && reference.state == "retained")
    );
    assert_eq!(
        after
            .references
            .iter()
            .filter(|reference| reference.state == "adopted")
            .count(),
        1
    );
}

#[tokio::test]
async fn signed_multipart_unexpired_root_claim_fails_root_only_on_retry() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok(FILE_CID),
            AddReply::Ok("QmArchive"),
            AddReply::Ok(FILE_CID),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive.clone()),
            ("QmArchive".to_owned(), archive.clone()),
        ]),
    })
    .await;
    mount_verified_directory(&harness.kubo).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "archive.zip",
        &[("decompress-zip", "expanded/")],
        HeaderMap::new(),
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let db = harness.state.store.db();
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "CREATE TRIGGER reject_mpu_batch_publish BEFORE UPDATE ON zip_batches \
         WHEN NEW.state = 'published' BEGIN SELECT RAISE(FAIL, 'rollback'); END;",
    ))
    .await
    .unwrap();
    assert_eq!(
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())])
            .await
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "DROP TRIGGER reject_mpu_batch_publish",
    ))
    .await
    .unwrap();

    let retry = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(
        retry.status(),
        StatusCode::OK,
        "{}",
        retry.text().await.unwrap()
    );
    let after = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
    assert_eq!(after.batch.state, "published");
    assert_eq!(after.batch.root_status, "failed");
    assert_eq!(
        after.batch.root_error_code.as_deref(),
        Some("root_intent_failed")
    );
    assert!(after.entries[0].version_row_id.is_some());
    assert_eq!(
        ipfs_s3_gateway::store::entities::object_version::Entity::find()
            .count(db)
            .await
            .unwrap(),
        2
    );
}
