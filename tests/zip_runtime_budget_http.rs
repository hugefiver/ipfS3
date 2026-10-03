//! Configured ZIP budgets at the authenticated product entrypoints.
#[allow(dead_code)]
mod support;

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    import::{downloader::SourceDownloader, pipeline::ImportCoordinator},
    state::AppState,
    store::{
        self,
        entities::{
            import_job, multipart_upload, object, object_version, pin_lease,
            standard_mutation_lease, zip_batch, zip_manifest_entry,
        },
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, PaginatorTrait, Statement};
use support::decompress::{
    AddReply, KuboScript, complete_multipart, legal_single_entry_zip, legal_two_entry_zip,
    start_kubo_harness, start_s3_server, start_s3_server_with_imports,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

const IMPORT_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

// Eleven Deflate bytes expand a directory payload to 1024 bytes; directory
// entries must consume the same extraction budget even without a Kubo add.
fn compressed_directory() -> Vec<u8> {
    let name = b"bomb/";
    let compressed = [0x63, 0x60, 0x18, 0x05, 0xa3, 0x60, 0x14, 0x8c, 0x54, 0, 0];
    let mut crc = !0_u32;
    for _ in 0..1024 {
        crc ^= 0;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0_u32.wrapping_sub(crc & 1)));
        }
    }
    let crc = !crc;
    let mut archive = Vec::new();
    archive.extend_from_slice(&0x0403_4b50_u32.to_le_bytes());
    archive.extend_from_slice(&20_u16.to_le_bytes());
    archive.extend_from_slice(&0_u16.to_le_bytes());
    archive.extend_from_slice(&8_u16.to_le_bytes());
    archive.extend_from_slice(&[0; 4]);
    archive.extend_from_slice(&crc.to_le_bytes());
    archive.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    archive.extend_from_slice(&1024_u32.to_le_bytes());
    archive.extend_from_slice(&(name.len() as u16).to_le_bytes());
    archive.extend_from_slice(&0_u16.to_le_bytes());
    archive.extend_from_slice(name);
    archive.extend_from_slice(&compressed);
    let central_offset = archive.len() as u32;
    archive.extend_from_slice(&0x0201_4b50_u32.to_le_bytes());
    archive.extend_from_slice(&20_u16.to_le_bytes());
    archive.extend_from_slice(&20_u16.to_le_bytes());
    archive.extend_from_slice(&0_u16.to_le_bytes());
    archive.extend_from_slice(&8_u16.to_le_bytes());
    archive.extend_from_slice(&[0; 4]);
    archive.extend_from_slice(&crc.to_le_bytes());
    archive.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    archive.extend_from_slice(&1024_u32.to_le_bytes());
    archive.extend_from_slice(&(name.len() as u16).to_le_bytes());
    archive.extend_from_slice(&[0; 12]);
    archive.extend_from_slice(&0_u32.to_le_bytes());
    archive.extend_from_slice(name);
    let central_size = archive.len() as u32 - central_offset;
    archive.extend_from_slice(&0x0605_4b50_u32.to_le_bytes());
    archive.extend_from_slice(&[0; 4]);
    archive.extend_from_slice(&1_u16.to_le_bytes());
    archive.extend_from_slice(&1_u16.to_le_bytes());
    archive.extend_from_slice(&central_size.to_le_bytes());
    archive.extend_from_slice(&central_offset.to_le_bytes());
    archive.extend_from_slice(&0_u16.to_le_bytes());
    archive
}

async fn configured_gateway(
    archive: Vec<u8>,
    budget: &str,
) -> (
    Arc<AppState>,
    String,
    support::decompress::S3ServerHandle,
    support::decompress::KuboHarness,
) {
    let kubo = start_kubo_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmFile1"),
            AddReply::Ok("QmFile2"),
        ],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive)]),
    })
    .await;
    let cfg: Config = toml::from_str(&format!(
        "[kubo]\nrpc_url = {:?}\n[decompress_zip]\nunixfs_directory_root = false\n{budget}\n",
        kubo.server.uri(),
    ))
    .unwrap();
    let state = AppState::new(&cfg).await.unwrap();
    store::bucket::create(state.store.db(), "test-bkt", None)
        .await
        .unwrap();
    let server =
        start_s3_server(state.clone(), Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;
    (state, server.endpoint.clone(), server, kubo)
}

async fn assert_nothing_published(state: &AppState) {
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        object_version::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
}

async fn assert_direct_zip_not_published(state: &AppState) {
    assert_nothing_published(state).await;
    assert_eq!(
        pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        zip_manifest_entry::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    for batch in zip_batch::Entity::find()
        .all(state.store.db())
        .await
        .unwrap()
    {
        assert_eq!(batch.source, "direct");
        assert!(!batch.manifest_prepared);
        assert!(!batch.source_published);
        assert_ne!(batch.state, "published");
    }
}

async fn assert_no_oversize_kubo_add(kubo: &support::decompress::KuboHarness, max: usize) {
    let requests = kubo.server.received_requests().await.unwrap();
    for request in requests {
        if request.url.path() != "/api/v0/add" {
            continue;
        }
        let boundary = request.headers[http::header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .split("boundary=")
            .nth(1)
            .unwrap()
            .trim_matches('"');
        let start = request
            .body
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap()
            + 4;
        let end_marker = format!("\r\n--{boundary}");
        let end = request.body[start..]
            .windows(end_marker.len())
            .position(|bytes| bytes == end_marker.as_bytes())
            .unwrap();
        assert!(
            end <= max,
            "Kubo received {end} source bytes with limit {max}"
        );
    }
}

#[tokio::test]
async fn signed_legacy_direct_raw_input_limit_rejects_split_zip_and_retry_without_publication() {
    let archive = legal_single_entry_zip();
    let max = archive.len() - 1;
    let (state, endpoint, server, kubo) =
        configured_gateway(archive.clone(), &format!("max_archive_bytes = {max}")).await;
    for _ in 0..2 {
        let response = support::sigv4::send_sigv4_chunked_http1(
            reqwest::Method::PUT,
            &endpoint,
            "test-bkt",
            "archive.zip",
            &[("decompress-zip", "out/")],
            vec![
                Bytes::copy_from_slice(&archive[..max]),
                Bytes::copy_from_slice(&archive[max..]),
            ],
            HeaderMap::new(),
            "test",
        )
        .await;
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("InvalidRequest"), "{body}");
        assert_direct_zip_not_published(&state).await;
        assert_no_oversize_kubo_add(&kubo, max).await;
    }
    let requests = kubo.server.received_requests().await.unwrap();
    assert!(!requests.iter().any(|r| r.url.path() == "/api/v0/cat"));
    assert!(!requests.iter().any(|r| r.url.path() == "/api/v0/pin/add"));
    server.shutdown().await;
}

#[tokio::test]
async fn signed_zip_v2_direct_raw_input_limit_rejects_before_publication() {
    let archive = legal_single_entry_zip();
    let max = archive.len() - 1;
    let (state, endpoint, server, kubo) =
        configured_gateway(archive.clone(), &format!("max_archive_bytes = {max}")).await;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", "raw-limit-v2"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "test-bkt",
        "archive.zip",
        &[("decompress-zip", "out/")],
        archive,
        headers,
        "test",
    )
    .await;
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_nothing_published(&state).await;
    assert_no_oversize_kubo_add(&kubo, max).await;
    server.shutdown().await;
}

fn zip_v2_headers(token: &str, source: bool) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        (
            "x-ipfs3-zip-publish-source",
            if source { "true" } else { "false" },
        ),
        (
            "x-ipfs3-zip-publish-extracted",
            if source { "false" } else { "true" },
        ),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", token),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    headers
}

#[tokio::test]
async fn signed_zip_v2_split_raw_input_limit_rejects_source_only_and_retry_without_binding() {
    let archive = legal_single_entry_zip();
    let max = archive.len() - 1;
    let (state, endpoint, server, kubo) =
        configured_gateway(archive.clone(), &format!("max_archive_bytes = {max}")).await;
    for _ in 0..2 {
        let response = support::sigv4::send_sigv4_chunked_http1(
            reqwest::Method::PUT,
            &endpoint,
            "test-bkt",
            "archive.zip",
            &[("decompress-zip", "out/")],
            vec![
                Bytes::copy_from_slice(&archive[..max]),
                Bytes::copy_from_slice(&archive[max..]),
            ],
            zip_v2_headers("split-raw-limit-v2", true),
            "test",
        )
        .await;
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body.contains("InvalidRequest") && body.contains("raw input"),
            "{body}"
        );
        assert_direct_zip_not_published(&state).await;
        assert_no_oversize_kubo_add(&kubo, max).await;
        let row = state.store.db().query_one(Statement::from_string(
            state.store.db().get_database_backend(),
            "SELECT input_sha256,input_art_cid,input_art_size FROM zip_v2_executions WHERE token='split-raw-limit-v2'",
        )).await.unwrap().unwrap();
        assert!(
            row.try_get::<Option<String>>("", "input_sha256")
                .unwrap()
                .is_none()
        );
        assert!(
            row.try_get::<Option<String>>("", "input_art_cid")
                .unwrap()
                .is_none()
        );
        assert!(
            row.try_get::<Option<i64>>("", "input_art_size")
                .unwrap()
                .is_none()
        );
    }
    let requests = kubo.server.received_requests().await.unwrap();
    assert!(!requests.iter().any(|r| r.url.path() == "/api/v0/cat"));
    assert!(!requests.iter().any(|r| r.url.path() == "/api/v0/pin/add"));
    server.shutdown().await;
}

#[tokio::test]
async fn signed_zip_v2_exact_raw_boundary_replays_but_oversize_replay_is_invalid_request() {
    let archive = legal_single_entry_zip();
    let max = archive.len();
    let (state, endpoint, server, kubo) =
        configured_gateway(archive.clone(), &format!("max_archive_bytes = {max}")).await;
    let headers = zip_v2_headers("exact-raw-limit-v2", true);
    let mut first_xml = None;
    for _ in 0..2 {
        let response = support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &endpoint,
            "test-bkt",
            "archive.zip",
            &[("decompress-zip", "out/")],
            archive.clone(),
            headers.clone(),
            "test",
        )
        .await;
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        if let Some(first) = &first_xml {
            assert_eq!(&body, first);
        } else {
            first_xml = Some(body);
        }
    }
    let before = state.store.db().query_one(Statement::from_string(
        state.store.db().get_database_backend(),
        "SELECT input_sha256,input_art_cid,input_art_size FROM zip_v2_executions WHERE token='exact-raw-limit-v2'",
    )).await.unwrap().unwrap();
    let identity = (
        before.try_get::<String>("", "input_sha256").unwrap(),
        before.try_get::<String>("", "input_art_cid").unwrap(),
        before.try_get::<i64>("", "input_art_size").unwrap(),
    );
    assert_eq!(identity.2, max as i64);
    // A valid, independently signed ZIP with the same file and one extra EOCD
    // comment byte exceeds the raw limit before replay can compare its digest.
    let mut oversize = archive;
    oversize[max - 2] = 1;
    oversize.push(b'x');
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "test-bkt",
        "archive.zip",
        &[("decompress-zip", "out/")],
        oversize,
        headers,
        "test",
    )
    .await;
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.contains("InvalidRequest") && body.contains("raw input"),
        "{body}"
    );
    assert_eq!(
        object_version::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
    let after = state.store.db().query_one(Statement::from_string(
        state.store.db().get_database_backend(),
        "SELECT input_sha256,input_art_cid,input_art_size FROM zip_v2_executions WHERE token='exact-raw-limit-v2'",
    )).await.unwrap().unwrap();
    assert_eq!(
        identity,
        (
            after.try_get::<String>("", "input_sha256").unwrap(),
            after.try_get::<String>("", "input_art_cid").unwrap(),
            after.try_get::<i64>("", "input_art_size").unwrap(),
        )
    );
    let requests = kubo.server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.url.path() == "/api/v0/add")
            .count(),
        1
    );
    assert_no_oversize_kubo_add(&kubo, max).await;
    server.shutdown().await;
}

#[tokio::test]
async fn signed_legacy_direct_raw_input_exact_boundary_still_publishes_source_and_output() {
    let archive = legal_single_entry_zip();
    let max = archive.len();
    let (state, endpoint, server, kubo) =
        configured_gateway(archive.clone(), &format!("max_archive_bytes = {max}")).await;
    let response = support::sigv4::send_sigv4_chunked_http1(
        reqwest::Method::PUT,
        &endpoint,
        "test-bkt",
        "archive.zip",
        &[("decompress-zip", "out/")],
        vec![
            Bytes::copy_from_slice(&archive[..max - 1]),
            Bytes::copy_from_slice(&archive[max - 1..]),
        ],
        HeaderMap::new(),
        "test",
    )
    .await;
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        zip_manifest_entry::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        zip_batch::Entity::find()
            .all(state.store.db())
            .await
            .unwrap()[0]
            .state,
        "published"
    );
    assert_no_oversize_kubo_add(&kubo, max).await;
    server.shutdown().await;
}

#[tokio::test]
async fn signed_plain_put_does_not_inherit_zip_raw_input_budget() {
    let archive = legal_single_entry_zip();
    let max = archive.len() - 1;
    let (state, endpoint, server, kubo) =
        configured_gateway(archive.clone(), &format!("max_archive_bytes = {max}")).await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "test-bkt",
        "plain.bin",
        &[],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        zip_batch::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert!(
        kubo.server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.url.path() == "/api/v0/add")
    );
    server.shutdown().await;
}

#[tokio::test]
async fn signed_legacy_direct_kubo_failure_is_not_misclassified_as_input_limit_or_exposed() {
    let archive = legal_single_entry_zip();
    let kubo = start_kubo_harness(KuboScript {
        add_replies: vec![AddReply::Error(
            StatusCode::BAD_GATEWAY,
            "private-kubo-error-marker",
        )],
        cat_bodies: HashMap::new(),
    })
    .await;
    let cfg: Config = toml::from_str(&format!(
        "[kubo]\nrpc_url = {:?}\n[decompress_zip]\nunixfs_directory_root = false\nmax_archive_bytes = {}\n",
        kubo.server.uri(), archive.len() + 1,
    )).unwrap();
    let state = AppState::new(&cfg).await.unwrap();
    store::bucket::create(state.store.db(), "test-bkt", None)
        .await
        .unwrap();
    let server =
        start_s3_server(state.clone(), Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &server.endpoint,
        "test-bkt",
        "archive.zip",
        &[("decompress-zip", "out/")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_server_error(), "{body}");
    assert!(!body.contains("private-kubo-error-marker"), "{body}");
    assert_direct_zip_not_published(&state).await;
    server.shutdown().await;
}

#[tokio::test]
async fn signed_direct_put_rejects_configured_expansion_entries_and_metadata_before_publication() {
    for (archive, budget) in [
        (legal_single_entry_zip(), "max_decompressed_bytes = 1"),
        (compressed_directory(), "max_decompressed_bytes = 1023"),
        (legal_single_entry_zip(), "max_decompressed_bytes = 0"),
        (legal_single_entry_zip(), "max_single_entry_bytes = 1"),
        (legal_two_entry_zip(), "max_entries = 1"),
        (legal_single_entry_zip(), "max_metadata_bytes = 0"),
        (legal_two_entry_zip(), "max_staged_adds = 1"),
    ] {
        let (state, endpoint, server, _kubo) = configured_gateway(archive.clone(), budget).await;
        let response = support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &endpoint,
            "test-bkt",
            "archive.zip",
            &[("decompress-zip", "out/")],
            archive,
            HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{budget}: {:?}",
            response.text().await
        );
        assert_nothing_published(&state).await;
        server.shutdown().await;
    }
}

struct Endpoint<'a>(&'a str);
impl support::decompress::S3TestEndpoint for Endpoint<'_> {
    fn endpoint(&self) -> &str {
        self.0
    }
    fn bucket(&self) -> &str {
        "test-bkt"
    }
}

#[tokio::test]
async fn signed_mpu_complete_rejects_configured_budget_and_keeps_upload_for_retry() {
    let archive = legal_single_entry_zip();
    let (state, endpoint, server, _kubo) =
        configured_gateway(archive.clone(), "max_decompressed_bytes = 1").await;
    let upload_id = {
        // Shared SigV4 helpers take a TestHarness; use the same wire-level request here.
        let response = support::sigv4::send_sigv4(
            reqwest::Method::POST,
            &endpoint,
            "test-bkt",
            "archive.zip",
            &[("uploads", ""), ("decompress-zip", "out/")],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let xml = response.text().await.unwrap();
        xml.split("<UploadId>")
            .nth(1)
            .unwrap()
            .split("</UploadId>")
            .next()
            .unwrap()
            .to_owned()
    };
    let part_number = "1";
    let part = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "test-bkt",
        "archive.zip",
        &[("partNumber", part_number), ("uploadId", &upload_id)],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part.headers()[http::header::ETAG]
        .to_str()
        .unwrap()
        .trim_matches('"')
        .to_owned();
    let response = complete_multipart(
        &Endpoint(&endpoint),
        "archive.zip",
        &upload_id,
        &[(1, etag)],
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "{:?}",
        response.text().await
    );
    assert_nothing_published(&state).await;
    assert!(
        multipart_upload::Entity::find_by_id(&upload_id)
            .one(state.store.db())
            .await
            .unwrap()
            .is_some()
    );
    let parts = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &endpoint,
        "test-bkt",
        "archive.zip",
        &[("uploadId", &upload_id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(parts.status(), StatusCode::OK);
    assert!(
        parts
            .text()
            .await
            .unwrap()
            .contains("<PartNumber>1</PartNumber>")
    );
    server.shutdown().await;
}

#[tokio::test]
async fn signed_v2_outputs_only_uses_configured_budget_without_publishing_source() {
    let archive = legal_single_entry_zip();
    let (state, endpoint, server, _kubo) =
        configured_gateway(archive.clone(), "max_decompressed_bytes = 1").await;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", "budget-v2"),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &endpoint,
        "test-bkt",
        "source.zip",
        &[("decompress-zip", "out/")],
        archive,
        headers,
        "test",
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "{:?}",
        response.text().await
    );
    assert_nothing_published(&state).await;
    server.shutdown().await;
}

#[tokio::test]
async fn signed_import_worker_rejects_configured_budget_with_root_on_or_off() {
    for root_enabled in [false, true] {
        let archive = legal_single_entry_zip();
        let kubo = start_kubo_harness(KuboScript {
            add_replies: vec![AddReply::Ok("QmFile")],
            cat_bodies: HashMap::from([(IMPORT_CID.to_owned(), archive.clone())]),
        })
        .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Type\":0,\"Responses\":null}\n{\"Type\":4,\"Responses\":[{\"ID\":\"provider\"}]}\n",
            ))
            .mount(&kubo.server).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Pins\":[\"{IMPORT_CID}\"]}}")),
            )
            .with_priority(1)
            .mount(&kubo.server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Hash\":\"{IMPORT_CID}\",\"CumulativeSize\":{}}}",
                archive.len(),
            )))
            .mount(&kubo.server)
            .await;

        let cfg: Config = toml::from_str(&format!(
            "[kubo]\nrpc_url = {:?}\n[decompress_zip]\nunixfs_directory_root = {root_enabled}\nmax_decompressed_bytes = 1\n[imports]\npoll_interval_ms = 10\nmax_attempts = 1\n",
            kubo.server.uri(),
        )).unwrap();
        let state = AppState::new(&cfg).await.unwrap();
        store::bucket::create(state.store.db(), "test-bkt", None)
            .await
            .unwrap();
        let validated = cfg.imports.validate().unwrap();
        let coordinator = ImportCoordinator::new(
            validated.clone(),
            SourceDownloader::production(Arc::new(validated)),
        );
        let server = start_s3_server_with_imports(
            state.clone(),
            Arc::new(tokio::sync::Mutex::new(Vec::new())),
            coordinator.clone(),
        )
        .await;
        let shutdown = CancellationToken::new();
        let worker = coordinator.start(state.clone(), shutdown.clone());
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        );
        let response = support::sigv4::send_sigv4(
            reqwest::Method::POST,
            &server.endpoint,
            "test-bkt",
            "archive.zip",
            &[("ipfs3-import", ""), ("decompress-zip", "out/")],
            format!("<IPFS3ImportRequest><CID>{IMPORT_CID}</CID></IPFS3ImportRequest>")
                .into_bytes(),
            headers,
            "test",
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::ACCEPTED,
            "{:?}",
            response.status()
        );
        let job_id = response.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let job = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let job = import_job::Entity::find_by_id(&job_id)
                    .one(state.store.db())
                    .await
                    .unwrap()
                    .unwrap();
                if matches!(job.state.as_str(), "failed" | "completed") {
                    break job;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("import worker reached a terminal state");
        assert_eq!(job.state, "failed", "root={root_enabled}: {job:?}");
        assert_eq!(
            job.failure_code.as_deref(),
            Some("decompression_limit_exceeded")
        );
        assert_nothing_published(&state).await;
        shutdown.cancel();
        worker.shutdown(Duration::from_secs(2)).await;
        server.shutdown().await;
    }
}

#[tokio::test]
async fn signed_put_deadline_is_an_overall_processing_budget_not_a_kubo_idle_timeout() {
    let archive = legal_single_entry_zip();
    let (state, endpoint, server, kubo) =
        configured_gateway(archive.clone(), "processing_deadline_secs = 1").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Arc::new(Mutex::new(Some(release_rx)));
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with({
            let calls = calls.clone();
            move |_: &wiremock::Request| {
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    reached_tx.send(()).unwrap();
                    release
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmArchive\",\"Size\":\"0\"}\n")
            }
        })
        .with_priority(1)
        .up_to_n_times(2)
        .mount(&kubo.server)
        .await;
    let request = tokio::spawn(async move {
        support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &endpoint,
            "test-bkt",
            "archive.zip",
            &[("decompress-zip", "out/")],
            archive,
            HeaderMap::new(),
            "test",
        )
        .await
    });
    tokio::task::spawn_blocking(move || reached_rx.recv_timeout(Duration::from_secs(10)).unwrap())
        .await
        .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    release_tx.send(()).unwrap();
    let response = request.await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("processing deadline"), "{body}");
    tokio::time::resume();
    assert_nothing_published(&state).await;
    server.shutdown().await;
}
