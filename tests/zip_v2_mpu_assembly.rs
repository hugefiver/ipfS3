//! Stage 4 building block: authenticated DTO assembly is separate from HTTP Complete routing.
#[allow(dead_code)]
mod support;

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{s3::ops::multipart::assemble_zip_v2_archive, store};
use s3s::{S3Request, dto::*};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sha2::{Digest, Sha256};
use support::decompress::{KuboScript, TestHarness, start_harness};

async fn limited_server(
    harness: &TestHarness,
    max: usize,
) -> (
    std::sync::Arc<ipfs_s3_gateway::state::AppState>,
    support::decompress::S3ServerHandle,
) {
    use std::sync::Arc;
    let cfg = toml::from_str::<ipfs_s3_gateway::config::Config>(&format!(
        "[kubo]\nrpc_url = {:?}\n[decompress_zip]\nunixfs_directory_root = false\nmax_archive_bytes = {max}\n",
        harness.kubo.uri(),
    )).unwrap();
    let configured = ipfs_s3_gateway::state::AppState::new(&cfg).await.unwrap();
    let state = Arc::new(ipfs_s3_gateway::state::AppState {
        kubo: harness.state.kubo.clone(),
        cold_kubo: None,
        store: harness.state.store.clone(),
        credentials: harness.state.credentials.clone(),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: configured.pinning.clone(),
    });
    let server = support::decompress::start_s3_server(
        state.clone(),
        Arc::new(tokio::sync::Mutex::new(Vec::new())),
    )
    .await;
    (state, server)
}

async fn create_at(harness: &TestHarness, endpoint: &str, mode: &str) -> String {
    let mut headers = HeaderMap::new();
    if mode == "v2" {
        for (name, value) in [
            ("x-ipfs3-zip-contract", "v2"),
            ("x-ipfs3-zip-publish-source", "true"),
            ("x-ipfs3-zip-publish-extracted", "false"),
            ("x-ipfs3-zip-targets", "none"),
            ("x-ipfs3-zip-token", "raw-budget"),
        ] {
            headers.insert(name, HeaderValue::from_str(value).unwrap());
        }
    }
    let query = if mode == "ordinary" {
        vec![("uploads", "")]
    } else {
        vec![("uploads", ""), ("decompress-zip", "out/")]
    };
    let response = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        endpoint,
        &harness.bucket,
        "archive.zip",
        &query,
        vec![],
        headers,
        "test",
    )
    .await;
    let status = response.status();
    let xml = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{xml}");
    xml.split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .into()
}

async fn complete_at(
    harness: &TestHarness,
    endpoint: &str,
    id: &str,
    parts: &str,
) -> reqwest::Response {
    support::sigv4::send_sigv4(
        reqwest::Method::POST,
        endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", id)],
        format!("<CompleteMultipartUpload>{parts}</CompleteMultipartUpload>").into_bytes(),
        HeaderMap::new(),
        "test",
    )
    .await
}

#[tokio::test]
async fn signed_zip_upload_part_limits_actual_chunked_bytes_but_not_ordinary_mpu() {
    for mode in ["v2", "legacy", "ordinary"] {
        let harness =
            start_harness(KuboScript::repeated_add("QmPart", 2, Default::default())).await;
        let (_, server) = limited_server(&harness, 8).await;
        let id = create_at(&harness, &server.endpoint, mode).await;
        let url = support::sigv4::presign_sigv4_query(
            &reqwest::Method::PUT,
            &server.endpoint,
            &harness.bucket,
            "archive.zip",
            &[("uploadId", &id), ("partNumber", "1")],
            "test",
            "test",
            None,
            60,
            chrono::Utc::now(),
        );
        let body = reqwest::Body::wrap_stream(futures_util::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"12345678")),
            Ok(bytes::Bytes::from_static(b"9")),
        ]));
        let response = reqwest::Client::new()
            .put(url)
            .body(body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let xml = response.text().await.unwrap();
        if mode == "ordinary" {
            assert_eq!(status, StatusCode::OK, "{xml}");
            assert_eq!(count(&harness, "multipart_parts").await, 1);
            assert_eq!(
                harness.captured_add_file_bytes(),
                vec![b"123456789".to_vec()]
            );
            harness.set_cat_body("QmPart", b"123456789".to_vec());
            let response = complete_at(
                &harness,
                &server.endpoint,
                &id,
                "<Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part>",
            )
            .await;
            let status = response.status();
            let xml = response.text().await.unwrap();
            assert_eq!(status, StatusCode::OK, "{xml}");
            assert_eq!(
                harness.captured_add_file_bytes(),
                vec![b"123456789".to_vec(); 2]
            );
            assert_eq!(count(&harness, "objects").await, 1);
            assert_eq!(count(&harness, "multipart_uploads").await, 0);
        } else {
            assert_eq!(status, StatusCode::BAD_REQUEST, "{mode}: {xml}");
            assert!(xml.contains("<Code>InvalidRequest</Code>"), "{xml}");
            assert_eq!(count(&harness, "multipart_parts").await, 0);
            assert!(
                harness
                    .captured_add_file_bytes()
                    .iter()
                    .all(|bytes| bytes.len() <= 8)
            );
            // Exactly the bound is allowed, including a subsequent part after
            // an oversized attempt; budgets are per stream, not global quota.
            let response = support::sigv4::send_sigv4(
                reqwest::Method::PUT,
                &server.endpoint,
                &harness.bucket,
                "archive.zip",
                &[("uploadId", &id), ("partNumber", "1")],
                b"12345678".to_vec(),
                HeaderMap::new(),
                "test",
            )
            .await;
            let status = response.status();
            let xml = response.text().await.unwrap();
            assert_eq!(status, StatusCode::OK, "{mode}: {xml}");
            assert_eq!(count(&harness, "multipart_parts").await, 1);
            assert_eq!(count(&harness, "objects").await, 0);
            assert_eq!(count(&harness, "multipart_uploads").await, 1);
        }
    }
}

#[tokio::test]
async fn signed_v2_complete_rejects_oversize_archive_before_binding_or_publication() {
    let harness = start_harness(KuboScript::repeated_add(
        "QmArchive",
        1,
        [("QmPartOne".into(), EMPTY_ZIP.to_vec())].into(),
    ))
    .await;
    let (state, server) = limited_server(&harness, EMPTY_ZIP.len() - 1).await;
    let id = create_at(&harness, &server.endpoint, "v2").await;
    store::multipart::upsert_part(
        state.store.db(),
        &id,
        1,
        "QmPartOne",
        EMPTY_ZIP.len() as i64,
        "QmPartOne",
    )
    .await
    .unwrap();
    let response = complete_at(
        &harness,
        &server.endpoint,
        &id,
        "<Part><PartNumber>1</PartNumber><ETag>\"QmPartOne\"</ETag></Part>",
    )
    .await;
    let status = response.status();
    let xml = response.text().await.unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{xml}");
    assert!(xml.contains("<Code>InvalidRequest</Code>"), "{xml}");
    assert!(harness.captured_add_file_bytes().is_empty());
    let execution = store::zip::execution::read(state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.input_sha256, None);
    unchanged(&harness).await;
}

#[tokio::test]
async fn signed_v2_complete_applies_raw_limit_to_full_concat_not_each_part() {
    let first_size = 5 * 1024 * 1024;
    let harness = start_harness(KuboScript::repeated_add(
        "QmArchive",
        1,
        [
            ("QmPartOne".into(), vec![b'P'; first_size]),
            ("QmPartTwo".into(), EMPTY_ZIP.to_vec()),
        ]
        .into(),
    ))
    .await;
    let (state, server) = limited_server(&harness, first_size + EMPTY_ZIP.len() - 1).await;
    let id = create_at(&harness, &server.endpoint, "v2").await;
    for (pn, cid, size) in [
        (1, "QmPartOne", first_size),
        (2, "QmPartTwo", EMPTY_ZIP.len()),
    ] {
        store::multipart::upsert_part(state.store.db(), &id, pn, cid, size as i64, cid)
            .await
            .unwrap();
    }
    let result = complete_at(&harness, &server.endpoint, &id,
        "<Part><PartNumber>1</PartNumber><ETag>\"QmPartOne\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"QmPartTwo\"</ETag></Part>").await;
    let status = result.status();
    let xml = result.text().await.unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{xml}");
    assert!(harness.captured_add_file_bytes().is_empty());
    assert_eq!(count(&harness, "objects").await, 0);
    assert_eq!(count(&harness, "multipart_parts").await, 2);
    assert_eq!(
        store::zip::execution::read(state.store.db(), &id)
            .await
            .unwrap()
            .unwrap()
            .input_sha256,
        None
    );
}

#[tokio::test]
async fn signed_legacy_concat_checks_actual_bytes_even_when_recorded_size_is_below_limit() {
    let harness = start_harness(KuboScript::repeated_add(
        "QmArchive",
        1,
        [
            ("QmPartOne".into(), EMPTY_ZIP.to_vec()),
            ("QmArchive".into(), EMPTY_ZIP.to_vec()),
        ]
        .into(),
    ))
    .await;
    let (state, server) = limited_server(&harness, EMPTY_ZIP.len() - 1).await;
    let id = create_at(&harness, &server.endpoint, "legacy").await;
    store::multipart::upsert_part(state.store.db(), &id, 1, "QmPartOne", 1, "QmPartOne")
        .await
        .unwrap();
    let result = complete_at(
        &harness,
        &server.endpoint,
        &id,
        "<Part><PartNumber>1</PartNumber><ETag>\"QmPartOne\"</ETag></Part>",
    )
    .await;
    let status = result.status();
    let xml = result.text().await.unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{xml}");
    assert!(xml.contains("<Code>InvalidRequest</Code>"), "{xml}");
    assert!(
        harness
            .captured_add_file_bytes()
            .iter()
            .all(|bytes| bytes.len() < EMPTY_ZIP.len())
    );
    unchanged(&harness).await;
}

const EMPTY_ZIP: &[u8] = b"PK\x05\x06\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

async fn setup(bytes: &[u8], recorded_size: i64) -> (TestHarness, String) {
    let harness = start_harness(KuboScript::repeated_add(
        "QmArchiveRoot",
        1,
        [("QmPartOne".to_owned(), bytes.to_vec())].into(),
    ))
    .await;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "extracted"),
        ("x-ipfs3-zip-token", "assembly-token"),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    let response = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        headers,
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let xml = response.text().await.unwrap();
    let upload_id = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_owned();
    store::multipart::upsert_part(
        harness.state.store.db(),
        &upload_id,
        1,
        "QmPartOne",
        recorded_size,
        "QmPartOne",
    )
    .await
    .unwrap();
    (harness, upload_id)
}

fn request(harness: &TestHarness, upload_id: &str) -> S3Request<CompleteMultipartUploadInput> {
    request_for(&harness.bucket, upload_id)
}

fn request_for(bucket: &str, upload_id: &str) -> S3Request<CompleteMultipartUploadInput> {
    S3Request {
        input: CompleteMultipartUploadInput {
            bucket: bucket.into(),
            key: "archive.zip".into(),
            upload_id: upload_id.into(),
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(ETag::Strong("QmPartOne".into())),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        },
        method: http::Method::POST,
        uri: format!("/{bucket}/archive.zip?uploadId={upload_id}")
            .parse()
            .unwrap(),
        headers: HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: Some(s3s::auth::Credentials {
            access_key: "test".into(),
            secret_key: s3s::auth::SecretKey::from("test"),
        }),
        region: Some("us-east-1".parse().unwrap()),
        service: Some("s3".into()),
        trailing_headers: None,
    }
}

async fn count(harness: &TestHarness, table: &str) -> i64 {
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
        .unwrap()
}

async fn unchanged(harness: &TestHarness) {
    for table in [
        "objects",
        "object_versions",
        "object_tags",
        "standard_mutation_leases",
        "zip_v2_targets",
    ] {
        assert_eq!(count(harness, table).await, 0, "{table}");
    }
    assert_eq!(count(harness, "multipart_uploads").await, 1);
    assert_eq!(count(harness, "multipart_parts").await, 1);
}

#[tokio::test]
async fn archive_assembly_hashes_actual_cat_bytes_and_does_not_publish_source() {
    let (harness, id) = setup(EMPTY_ZIP, EMPTY_ZIP.len() as i64).await;
    let intake = store::multipart::v2_zip::read_by_upload(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let mut req = request(&harness, &id);
    use base64::Engine as _;
    req.input
        .multipart_upload
        .as_mut()
        .unwrap()
        .parts
        .as_mut()
        .unwrap()[0]
        .checksum_sha256 =
        Some(base64::engine::general_purpose::STANDARD.encode(Sha256::digest(EMPTY_ZIP)));
    let archive = assemble_zip_v2_archive(&harness.state, &req, &intake)
        .await
        .unwrap();
    assert_eq!(archive.archive_cid, "QmArchiveRoot");
    assert_eq!(archive.archive_size, EMPTY_ZIP.len() as i64);
    assert_eq!(archive.input_sha256, hex::encode(Sha256::digest(EMPTY_ZIP)));
    assert_eq!(harness.captured_add_file_bytes(), vec![EMPTY_ZIP.to_vec()]);
    unchanged(&harness).await;
}

#[tokio::test]
async fn multiple_parts_use_client_order_and_exact_uploaded_bytes() {
    let first = vec![b'P'; 5 * 1024 * 1024];
    let (harness, id) = setup(&first, first.len() as i64).await;
    harness.set_cat_body("QmPartTwo", EMPTY_ZIP.to_vec());
    store::multipart::upsert_part(
        harness.state.store.db(),
        &id,
        2,
        "QmPartTwo",
        EMPTY_ZIP.len() as i64,
        "QmPartTwo",
    )
    .await
    .unwrap();
    let intake = store::multipart::v2_zip::read_by_upload(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let mut req = request(&harness, &id);
    req.input
        .multipart_upload
        .as_mut()
        .unwrap()
        .parts
        .as_mut()
        .unwrap()
        .push(CompletedPart {
            part_number: Some(2),
            e_tag: Some(ETag::Strong("QmPartTwo".into())),
            ..Default::default()
        });
    let archive = assemble_zip_v2_archive(&harness.state, &req, &intake)
        .await
        .unwrap();
    let mut combined = first;
    combined.extend_from_slice(EMPTY_ZIP);
    assert_eq!(archive.archive_size, combined.len() as i64);
    assert_eq!(archive.input_sha256, hex::encode(Sha256::digest(&combined)));
    assert_eq!(harness.captured_add_file_bytes(), vec![combined]);
    for table in [
        "objects",
        "object_versions",
        "object_tags",
        "standard_mutation_leases",
        "zip_v2_targets",
    ] {
        assert_eq!(count(&harness, table).await, 0, "{table}");
    }
    assert_eq!(count(&harness, "multipart_parts").await, 2);
}

#[tokio::test]
async fn assembly_never_overwrites_or_supersedes_an_existing_source() {
    let (harness, id) = setup(EMPTY_ZIP, EMPTY_ZIP.len() as i64).await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES (?,?,?,?,?,?)",
            vec![
                "existing-source".into(),
                harness.bucket.clone().into(),
                "archive.zip".into(),
                "QmExisting".into(),
                42_i64.into(),
                "QmExisting".into(),
            ],
        ))
        .await
        .unwrap();
    let intake = store::multipart::v2_zip::read_by_upload(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assemble_zip_v2_archive(&harness.state, &request(&harness, &id), &intake)
        .await
        .unwrap();
    let row = harness
        .state
        .store
        .db()
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT cid,size,is_latest FROM objects WHERE id='existing-source'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<String>("", "cid").unwrap(), "QmExisting");
    assert_eq!(row.try_get::<i64>("", "size").unwrap(), 42);
    assert!(row.try_get::<bool>("", "is_latest").unwrap());
    assert_eq!(count(&harness, "objects").await, 1);
    assert_eq!(count(&harness, "object_tags").await, 0);
    assert_eq!(count(&harness, "standard_mutation_leases").await, 0);
    assert_eq!(count(&harness, "zip_v2_targets").await, 0);
}

#[tokio::test]
async fn incorrect_part_contract_and_short_cat_fail_without_source_side_effects() {
    let (harness, id) = setup(EMPTY_ZIP, EMPTY_ZIP.len() as i64).await;
    let intake = store::multipart::v2_zip::read_by_upload(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let mut invalid = request(&harness, &id);
    invalid
        .input
        .multipart_upload
        .as_mut()
        .unwrap()
        .parts
        .as_mut()
        .unwrap()[0]
        .e_tag = Some(ETag::Strong("wrong".into()));
    assert_eq!(
        assemble_zip_v2_archive(&harness.state, &invalid, &intake)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "InvalidPart"
    );
    assert!(harness.captured_add_file_bytes().is_empty());
    harness.set_cat_body("QmPartOne", EMPTY_ZIP[..10].to_vec());
    assert!(
        assemble_zip_v2_archive(&harness.state, &request(&harness, &id), &intake)
            .await
            .is_err()
    );
    unchanged(&harness).await;
}

#[tokio::test]
async fn identity_order_checksums_and_sse_fail_closed() {
    let (harness, id) = setup(EMPTY_ZIP, EMPTY_ZIP.len() as i64).await;
    let intake = store::multipart::v2_zip::read_by_upload(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let mut wrong_owner = request(&harness, &id);
    wrong_owner.credentials.as_mut().unwrap().access_key = "stranger".into();
    assert_eq!(
        assemble_zip_v2_archive(&harness.state, &wrong_owner, &intake)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "AccessDenied"
    );
    let mut wrong_bucket = request(&harness, &id);
    wrong_bucket.input.bucket = "other".into();
    assert!(
        assemble_zip_v2_archive(&harness.state, &wrong_bucket, &intake)
            .await
            .is_err()
    );
    let mut wrong_order = request(&harness, &id);
    let duplicate = wrong_order
        .input
        .multipart_upload
        .as_ref()
        .unwrap()
        .parts
        .as_ref()
        .unwrap()[0]
        .clone();
    wrong_order
        .input
        .multipart_upload
        .as_mut()
        .unwrap()
        .parts
        .as_mut()
        .unwrap()
        .push(duplicate);
    assert_eq!(
        assemble_zip_v2_archive(&harness.state, &wrong_order, &intake)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "InvalidPartOrder"
    );
    let mut unsupported = request(&harness, &id);
    unsupported
        .input
        .multipart_upload
        .as_mut()
        .unwrap()
        .parts
        .as_mut()
        .unwrap()[0]
        .checksum_crc32 = Some("AAAAAA==".into());
    assert!(
        assemble_zip_v2_archive(&harness.state, &unsupported, &intake)
            .await
            .is_err()
    );
    let mut sse = request(&harness, &id);
    sse.headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    assert!(
        assemble_zip_v2_archive(&harness.state, &sse, &intake)
            .await
            .is_err()
    );
    assert!(harness.captured_add_file_bytes().is_empty());
    unchanged(&harness).await;
}

#[tokio::test]
async fn changed_intake_or_persisted_encryption_is_not_assembled() {
    let (harness, id) = setup(EMPTY_ZIP, EMPTY_ZIP.len() as i64).await;
    let intake = store::multipart::v2_zip::read_by_upload(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let mut altered = intake.clone();
    altered.captured_config = r#"{"root_default":false}"#.into();
    assert_eq!(
        assemble_zip_v2_archive(&harness.state, &request(&harness, &id), &altered)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "OperationAborted"
    );
    harness
        .state
        .store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE multipart_uploads SET encryption_mode='sse_s3' WHERE upload_id=?",
            vec![id.clone().into()],
        ))
        .await
        .unwrap();
    assert_eq!(
        assemble_zip_v2_archive(&harness.state, &request(&harness, &id), &intake)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "InvalidRequest"
    );
    assert!(harness.captured_add_file_bytes().is_empty());
    unchanged(&harness).await;
}

#[tokio::test]
async fn sha256_checksum_is_verified_against_cat_and_zip_trailer_is_required() {
    use base64::Engine as _;
    let (harness, id) = setup(EMPTY_ZIP, EMPTY_ZIP.len() as i64).await;
    let intake = store::multipart::v2_zip::read_by_upload(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let mut req = request(&harness, &id);
    req.input
        .multipart_upload
        .as_mut()
        .unwrap()
        .parts
        .as_mut()
        .unwrap()[0]
        .checksum_sha256 = Some(base64::engine::general_purpose::STANDARD.encode([1_u8; 32]));
    assert_eq!(
        assemble_zip_v2_archive(&harness.state, &req, &intake)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "InvalidPart"
    );
    let mut bytes = EMPTY_ZIP.to_vec();
    bytes[0] = b'X';
    bytes[2] = b'X';
    harness.set_cat_body("QmPartOne", bytes);
    req.input
        .multipart_upload
        .as_mut()
        .unwrap()
        .parts
        .as_mut()
        .unwrap()[0]
        .checksum_sha256 = None;
    assert_eq!(
        assemble_zip_v2_archive(&harness.state, &req, &intake)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "IncompleteBody"
    );
    unchanged(&harness).await;
}

#[tokio::test]
async fn late_kubo_cat_error_trailer_never_becomes_a_verified_archive() {
    use std::{collections::HashMap, sync::Arc};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let kubo_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    let mut byte = [0_u8; 1];
                    if socket.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    headers.push(byte[0]);
                }
                if headers.starts_with(b"POST /api/v0/cat?") {
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n{:x}\r\n",
                        EMPTY_ZIP.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                    socket.write_all(EMPTY_ZIP).await.unwrap();
                    socket
                        .write_all(b"\r\n0\r\nX-Stream-Error: late failure\r\n\r\n")
                        .await
                        .unwrap();
                } else if headers.starts_with(b"POST /api/v0/add?") {
                    // An early add response must not conceal a later CAT trailer.
                    let body = b"{\"Hash\":\"QmEarly\",\"Size\":\"0\"}\n";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    if socket.write_all(response.as_bytes()).await.is_ok() {
                        let _ = socket.write_all(body).await;
                    }
                }
            });
        }
    });
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let state = Arc::new(ipfs_s3_gateway::state::AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo_url),
        cold_kubo: None,
        store: store::Store::new(db),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let bucket = "test-bkt";
    store::bucket::create(state.store.db(), bucket, None)
        .await
        .unwrap();
    let s3 = support::decompress::start_s3_server(
        state.clone(),
        Arc::new(tokio::sync::Mutex::new(Vec::new())),
    )
    .await;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "extracted"),
        ("x-ipfs3-zip-token", "late-cat-token"),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    let created = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &s3.endpoint,
        bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        headers,
        "test",
    )
    .await;
    assert_eq!(created.status(), StatusCode::OK);
    let xml = created.text().await.unwrap();
    let id = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap();
    store::multipart::upsert_part(
        state.store.db(),
        id,
        1,
        "QmPartOne",
        EMPTY_ZIP.len() as i64,
        "QmPartOne",
    )
    .await
    .unwrap();
    let intake = store::multipart::v2_zip::read_by_upload(state.store.db(), id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        assemble_zip_v2_archive(&state, &request_for(bucket, id), &intake)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "IncompleteBody"
    );
    for table in [
        "objects",
        "object_versions",
        "object_tags",
        "standard_mutation_leases",
        "zip_v2_targets",
    ] {
        let result = state
            .store
            .db()
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("SELECT COUNT(*) AS n FROM {table}"),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.try_get::<i64>("", "n").unwrap(), 0, "{table}");
    }
    server.abort();
}
