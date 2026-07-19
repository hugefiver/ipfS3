mod support;

use base64::Engine as _;
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, HeaderValue, StatusCode};
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, Statement};
use std::collections::HashMap;

use ipfs_s3_gateway::store;
use support::decompress::{
    AddReply, KuboScript, TestHarness, abort_multipart, archive_key_collision_zip,
    assert_no_kubo_calls, assert_pin_calls, complete_multipart, complete_multipart_xml,
    create_multipart, duplicate_entry_zip, latest_observed_request, legal_single_entry_zip,
    legal_two_entry_zip, start_harness, traversal_zip, upload_part,
};
use support::sigv4::{presign_sigv4_query, send_sigv4, send_sigv4_chunked_http1};

const SINGLE_ENTRY_BYTES: &[u8] = b"single entry bytes";
const FIRST_ENTRY_BYTES: &[u8] = b"first entry bytes";
const SECOND_ENTRY_BYTES: &[u8] = b"second entry bytes";
const FIRST_DUPLICATE_BYTES: &[u8] = b"first duplicate bytes";
const SECOND_DUPLICATE_BYTES: &[u8] = b"second duplicate bytes";

fn scripted(cids: &[&'static str], cat_bodies: Vec<(&str, Vec<u8>)>) -> KuboScript {
    KuboScript {
        add_replies: cids.iter().map(|cid| AddReply::Ok(cid)).collect(),
        cat_bodies: cat_bodies
            .into_iter()
            .map(|(cid, body)| (cid.to_owned(), body))
            .collect(),
    }
}

fn standard_script(calls: usize) -> KuboScript {
    KuboScript::repeated_add(
        "QmTestCid",
        calls,
        HashMap::from([("QmTestCid".to_owned(), b"hello world".to_vec())]),
    )
}

/// Convenience: build a path-style rust-s3 client for the real test endpoint.
fn test_bucket(harness: &TestHarness) -> Box<Bucket> {
    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: harness.endpoint.clone(),
    };
    let credentials =
        Credentials::new(Some("test"), Some("test"), None, None, None).expect("credentials");
    Bucket::new(&harness.bucket, region, credentials)
        .expect("bucket")
        .with_path_style()
}

fn bad_bucket(harness: &TestHarness) -> Box<Bucket> {
    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: harness.endpoint.clone(),
    };
    let credentials =
        Credentials::new(Some("wrong"), Some("wrong"), None, None, None).expect("credentials");
    Bucket::new(&harness.bucket, region, credentials)
        .expect("bucket")
        .with_path_style()
}

async fn signed_get(harness: &TestHarness, key: &str) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_put(
    harness: &TestHarness,
    key: &str,
    query: &[(&str, &str)],
    body: Vec<u8>,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        key,
        query,
        body,
        headers,
        "test",
    )
    .await
}

async fn assert_signed_body(harness: &TestHarness, key: &str, expected: &[u8]) {
    let response = signed_get(harness, key).await;
    assert_eq!(response.status(), StatusCode::OK, "signed GET {key}");
    assert_eq!(response.bytes().await.expect("GET body").as_ref(), expected);
}

async fn assert_s3_error(
    response: reqwest::Response,
    status: StatusCode,
    code: &str,
    message: &str,
) {
    assert_eq!(response.status(), status);
    let body = response.text().await.expect("S3 error body");
    assert!(body.contains(code), "missing error code {code}: {body}");
    assert!(
        body.contains(message),
        "missing error message {message}: {body}"
    );
}

async fn assert_latest_absent(harness: &TestHarness, key: &str) {
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .is_err(),
        "{key} must not have a latest object row"
    );
}

async fn listed_db_keys(harness: &TestHarness) -> Vec<String> {
    store::object::list(harness.state.store.db(), &harness.bucket, None, None, 1000)
        .await
        .expect("list latest DB objects")
        .into_iter()
        .map(|object| object.key)
        .collect()
}

async fn kubo_log(harness: &TestHarness) -> Vec<String> {
    harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .iter()
        .map(|request| format!("{request:?}"))
        .collect()
}

async fn kubo_query_args(harness: &TestHarness, path: &str) -> Vec<String> {
    harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .iter()
        .filter(|request| request.url.path() == path)
        .filter_map(|request| {
            request
                .url
                .query_pairs()
                .find(|(name, _)| name == "arg")
                .map(|(_, value)| value.into_owned())
        })
        .collect()
}

fn sse_c_headers() -> HeaderMap {
    let key = [7_u8; 32];
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES256"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key",
        HeaderValue::from_str(&key_b64).expect("base64 customer key header"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&md5_b64).expect("base64 customer key MD5 header"),
    );
    headers
}

// ---------------------------------------------------------------------------
// Retained standard behaviour regressions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_create_and_put_and_get_plain_object() {
    let harness = start_harness(standard_script(1)).await;
    let bucket = test_bucket(&harness);

    let put = bucket
        .put_object("hello.txt", b"hello world")
        .await
        .expect("put object");
    assert_eq!(put.status_code(), 200);
    assert!(
        put.headers()
            .get("etag")
            .expect("etag header")
            .contains("QmTestCid")
    );

    let get = bucket.get_object("hello.txt").await.expect("get object");
    assert_eq!(get.status_code(), 200);
    assert_eq!(get.as_slice(), b"hello world");
}

#[tokio::test]
async fn test_head_object_signed_nested_key_succeeds() {
    let harness = start_harness(standard_script(1)).await;
    let bucket = test_bucket(&harness);
    bucket
        .put_object("nested/path/file.txt", b"hello world")
        .await
        .expect("put nested object");

    let (head, status) = bucket
        .head_object("nested/path/file.txt")
        .await
        .expect("head nested object");
    assert_eq!(status, 200);
    assert!(head.e_tag.expect("etag header").contains("QmTestCid"));
}

#[tokio::test]
async fn test_list_objects() {
    let harness = start_harness(standard_script(2)).await;
    let bucket = test_bucket(&harness);
    bucket
        .put_object("obj1.txt", b"hello world")
        .await
        .expect("put obj1");
    bucket
        .put_object("obj2.txt", b"hello world")
        .await
        .expect("put obj2");

    let pages = bucket
        .list(String::new(), None)
        .await
        .expect("list objects");
    assert_eq!(
        pages.iter().map(|page| page.contents.len()).sum::<usize>(),
        2
    );
}

#[tokio::test]
async fn test_list_objects_with_delimiter_returns_common_prefixes() {
    let harness = start_harness(standard_script(4)).await;
    let bucket = test_bucket(&harness);
    for key in [
        "a.txt",
        "photos/cat.jpg",
        "photos/dog.jpg",
        "videos/clip.mp4",
    ] {
        bucket
            .put_object(key, b"hello world")
            .await
            .unwrap_or_else(|error| panic!("put {key}: {error}"));
    }

    let pages = bucket
        .list(String::new(), Some("/".to_string()))
        .await
        .expect("list with delimiter");
    let mut keys: Vec<_> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|object| object.key.clone()))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["a.txt"]);
    let mut prefixes: Vec<_> = pages
        .iter()
        .flat_map(|page| {
            page.common_prefixes
                .iter()
                .flat_map(|prefixes| prefixes.iter().map(|prefix| prefix.prefix.clone()))
        })
        .collect();
    prefixes.sort();
    assert_eq!(prefixes, vec!["photos/", "videos/"]);
}

#[tokio::test]
async fn test_list_objects_with_prefix_and_delimiter_returns_one_level() {
    let harness = start_harness(standard_script(3)).await;
    let bucket = test_bucket(&harness);
    for key in ["photos/cat.jpg", "photos/dog.jpg", "photos/2024/jan.jpg"] {
        bucket
            .put_object(key, b"hello world")
            .await
            .unwrap_or_else(|error| panic!("put {key}: {error}"));
    }

    let pages = bucket
        .list("photos/".to_string(), Some("/".to_string()))
        .await
        .expect("list with prefix and delimiter");
    let mut keys: Vec<_> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|object| object.key.clone()))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["photos/cat.jpg", "photos/dog.jpg"]);
    let mut prefixes: Vec<_> = pages
        .iter()
        .flat_map(|page| {
            page.common_prefixes
                .iter()
                .flat_map(|prefixes| prefixes.iter().map(|prefix| prefix.prefix.clone()))
        })
        .collect();
    prefixes.sort();
    assert_eq!(prefixes, vec!["photos/2024/"]);
}

#[tokio::test]
async fn test_wrong_credentials_rejected() {
    let harness = start_harness(standard_script(0)).await;
    let result = bad_bucket(&harness)
        .put_object("hello.txt", b"hello world")
        .await;
    assert!(result.is_err(), "wrong credentials must be rejected");
}

// ---------------------------------------------------------------------------
// Task 8: PutObject, authentication, and failure acceptance coverage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sigv4_valid_request_reaches_decompress_route() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;

    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("decompress response body");
    assert!(body.contains("<DecompressZipResult>"));
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
        .await
        .expect("archive DB row");
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "file.txt")
        .await
        .expect("entry DB row");
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "file.txt", SINGLE_ENTRY_BYTES).await;
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmArchive", "QmEntry"], &[]).await;
}

#[tokio::test]
async fn test_sigv4_wrong_signature_is_rejected_before_kubo() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&[], vec![])).await;
    let response = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "")],
        archive,
        HeaderMap::new(),
        "wrong",
    )
    .await;
    assert_s3_error(response, StatusCode::FORBIDDEN, "SignatureDoesNotMatch", "").await;
    assert_latest_absent(&harness, "archive.zip").await;
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_sigv4_query_tuple_decodes_prefix_once() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let query = [("decompress-zip", "prefix/nested/")];
    let response = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &query,
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .url()
            .as_str()
            .contains("decompress-zip=prefix%2Fnested%2F")
    );
    store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "prefix/nested/file.txt",
    )
    .await
    .expect("decoded target key");
    assert_latest_absent(&harness, "prefix%2Fnested%2Ffile.txt").await;
}

#[tokio::test]
async fn test_presigned_put_signs_custom_query_and_lists_gets_objects() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[
            ("decompress-zip", "prefix/"),
            ("decompress-zip-result", "true"),
        ],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    let response = reqwest::Client::new()
        .put(url)
        .body(archive.clone())
        .send()
        .await
        .expect("presigned PUT");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("decompress result body");
    assert!(body.contains("<DecompressZipResult>"));
    let observed = latest_observed_request(&harness).await;
    assert!(
        observed
            .uri
            .query()
            .unwrap_or_default()
            .contains("X-Amz-Algorithm")
    );
    assert!(!observed.headers.contains_key(http::header::AUTHORIZATION));

    let pages = test_bucket(&harness)
        .list(String::new(), None)
        .await
        .expect("ListObjectsV2");
    let listed: Vec<_> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|object| object.key.as_str()))
        .collect();
    assert!(listed.contains(&"archive.zip"));
    assert!(listed.contains(&"prefix/file.txt"));
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "prefix/file.txt", SINGLE_ENTRY_BYTES).await;
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmArchive", "QmEntry"], &[]).await;
    let log = kubo_log(&harness).await.join("\n");
    assert!(log.contains("/api/v0/add"));
    assert!(log.contains("/api/v0/pin/add"));
    assert!(log.contains("/api/v0/cat"));
    assert!(log.contains("QmArchive"));
    assert!(log.contains("QmEntry"));
}

#[tokio::test]
async fn test_presigned_space_target_raw_plus_rewrite_is_semantically_stable() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let signed_url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "reports Q3/")],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    assert!(signed_url.contains("decompress-zip=reports%20Q3%2F"));
    let rewritten_url = signed_url.replacen(
        "decompress-zip=reports%20Q3%2F",
        "decompress-zip=reports+Q3%2F",
        1,
    );
    assert_ne!(rewritten_url, signed_url);

    let response = reqwest::Client::new()
        .put(rewritten_url)
        .body(archive)
        .send()
        .await
        .expect("rewritten presigned PUT");

    assert_eq!(response.status(), StatusCode::OK);
    let observed = latest_observed_request(&harness).await;
    assert!(
        observed
            .uri
            .query()
            .unwrap_or_default()
            .contains("decompress-zip=reports+Q3%2F")
    );
    store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "reports Q3/file.txt",
    )
    .await
    .expect("space-decoded entry DB row");
    assert_latest_absent(&harness, "reports+Q3/file.txt").await;
    assert!(
        !listed_db_keys(&harness)
            .await
            .iter()
            .any(|key| key == "reports+Q3/file.txt")
    );
}

#[tokio::test]
async fn test_presigned_put_tampered_decompress_query_is_rejected_without_mutation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&[], vec![])).await;
    let url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[
            ("decompress-zip", "prefix/"),
            ("decompress-zip-result", "true"),
        ],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    let response = reqwest::Client::new()
        .put(format!("{url}&decompress-zip=other%2F"))
        .body(archive)
        .send()
        .await
        .expect("tampered presigned PUT");
    assert_s3_error(response, StatusCode::FORBIDDEN, "SignatureDoesNotMatch", "").await;
    for key in ["archive.zip", "prefix/file.txt", "other/file.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    assert!(
        ipfs_s3_gateway::store::entities::multipart_upload::Entity::find()
            .all(harness.state.store.db())
            .await
            .expect("multipart upload rows")
            .is_empty()
    );
    assert!(
        ipfs_s3_gateway::store::entities::multipart_part::Entity::find()
            .all(harness.state.store.db())
            .await
            .expect("multipart part rows")
            .is_empty()
    );
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_put_decompress_zip_signed_default_result() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry1", "QmEntry2"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry1", FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["etag"], "\"QmArchive\"");
    let body = response.text().await.expect("decompress result body");
    assert!(body.contains("<DecompressZipResult>"));
    assert!(body.contains("<ExtractedCount>2</ExtractedCount>"));
    for key in ["archive.zip", "first.txt", "second.txt"] {
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .unwrap_or_else(|error| panic!("latest {key}: {error}"));
    }
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "second.txt", SECOND_ENTRY_BYTES).await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmArchive", "QmEntry1", "QmEntry2"],
        &[],
    )
    .await;
}

#[tokio::test]
async fn test_put_duplicate_entry_key_last_wins() {
    let archive = duplicate_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmFirstDuplicate", "QmSecondDuplicate"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmFirstDuplicate", FIRST_DUPLICATE_BYTES.to_vec()),
            ("QmSecondDuplicate", SECOND_DUPLICATE_BYTES.to_vec()),
        ],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "prefix/")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "prefix/duplicate.txt",
        )
        .await
        .expect("latest duplicate entry")
        .cid,
        "QmSecondDuplicate"
    );
    assert_signed_body(&harness, "prefix/duplicate.txt", SECOND_DUPLICATE_BYTES).await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmArchive", "QmFirstDuplicate", "QmSecondDuplicate"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmArchive", "QmFirstDuplicate", "QmSecondDuplicate"],
    )
    .await;
}

#[tokio::test]
async fn test_put_decompress_zip_signed_result_false() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry1", "QmEntry2"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", ""), ("decompress-zip-result", "false")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["etag"], "\"QmArchive\"");
    assert!(
        response
            .bytes()
            .await
            .expect("empty response body")
            .is_empty()
    );
    assert_eq!(
        listed_db_keys(&harness).await,
        vec!["archive.zip", "first.txt", "second.txt"]
    );
}

#[tokio::test]
async fn test_put_decompress_zip_traversal_hides_db_and_keeps_archive_pin() {
    let archive = traversal_zip();
    let harness = start_harness(scripted(
        &["QmArchive"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "prefix/")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "",
    )
    .await;
    for key in ["archive.zip", "escape.txt", "prefix/escape.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    assert!(listed_db_keys(&harness).await.is_empty());
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmArchive"], &[]).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmArchive"]).await;
}

#[tokio::test]
async fn test_put_decompress_zip_archive_key_collision_is_global_reject() {
    let archive = archive_key_collision_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmCollisionEntry"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "zip entry collides with archive key: archive.zip",
    )
    .await;
    assert_latest_absent(&harness, "archive.zip").await;
    assert!(listed_db_keys(&harness).await.is_empty());
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmArchive", "QmCollisionEntry"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmArchive", "QmCollisionEntry"],
    )
    .await;
}

#[tokio::test]
async fn test_put_decompress_zip_one_entry_kubo_failure_is_partial() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "entry add failed"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive.clone()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    })
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("partial response body");
    assert!(body.contains("<FailedCount>1</FailedCount>"));
    assert!(body.contains("EntryUploadFailed"));
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
        .await
        .expect("archive latest row");
    assert_latest_absent(&harness, "first.txt").await;
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "second.txt")
        .await
        .expect("second entry latest row");
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "second.txt", SECOND_ENTRY_BYTES).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmArchive", "QmEntry2"]).await;
}

#[tokio::test]
async fn test_put_decompress_zip_one_entry_publish_failure_keeps_pins() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry1", "QmEntry2"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_first_entry BEFORE INSERT ON objects \
             WHEN NEW.key = 'first.txt' \
             BEGIN SELECT RAISE(FAIL, 'forced entry publish failure'); END;",
        ))
        .await
        .expect("install entry publish failure trigger");
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("partial response body");
    assert!(body.contains("EntryPublishFailed"));
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
        .await
        .expect("archive latest row");
    assert_latest_absent(&harness, "first.txt").await;
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "second.txt")
        .await
        .expect("second entry latest row");
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "second.txt", SECOND_ENTRY_BYTES).await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmArchive", "QmEntry1", "QmEntry2"],
    )
    .await;
}

#[tokio::test]
async fn test_put_decompress_zip_rejects_sse_s3() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        legal_single_entry_zip(),
        headers,
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_put_decompress_zip_rejects_sse_c() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        legal_single_entry_zip(),
        sse_c_headers(),
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
    assert_no_kubo_calls(&harness).await;
}

// ---------------------------------------------------------------------------
// Task 8: Multipart acceptance coverage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multipart_decompress_signed_default_result() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        vec![
            ("QmPart", archive.clone()),
            ("QmRoot", archive.clone()),
            ("QmEntry1", FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive.clone()).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("multipart decompress body");
    assert!(body.contains("<DecompressZipResult>"));
    for key in ["archive.zip", "prefix/first.txt", "prefix/second.txt"] {
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .unwrap_or_else(|error| panic!("latest {key}: {error}"));
    }
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after complete")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart", "QmRoot"]
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "prefix/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "prefix/second.txt", SECOND_ENTRY_BYTES).await;
}

#[tokio::test]
async fn test_multipart_duplicate_entry_key_last_wins() {
    let archive = duplicate_entry_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmFirstDuplicate", "QmSecondDuplicate"],
        vec![
            ("QmPart", archive.clone()),
            ("QmRoot", archive.clone()),
            ("QmFirstDuplicate", FIRST_DUPLICATE_BYTES.to_vec()),
            ("QmSecondDuplicate", SECOND_DUPLICATE_BYTES.to_vec()),
        ],
    ))
    .await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "prefix/duplicate.txt",
        )
        .await
        .expect("latest duplicate entry")
        .cid,
        "QmSecondDuplicate"
    );
    assert_signed_body(&harness, "prefix/duplicate.txt", SECOND_DUPLICATE_BYTES).await;
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after complete")
            .is_empty()
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmFirstDuplicate", "QmSecondDuplicate"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmFirstDuplicate", "QmSecondDuplicate"],
    )
    .await;
}

#[tokio::test]
async fn test_multipart_decompress_signed_result_false() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        vec![
            ("QmPart", archive.clone()),
            ("QmRoot", archive.clone()),
            ("QmEntry1", FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let upload_id = create_multipart(
        &harness,
        "archive.zip",
        &[
            ("decompress-zip", "prefix/"),
            ("decompress-zip-result", "false"),
        ],
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive.clone()).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["etag"], "\"QmRoot\"");
    let body = response.text().await.expect("multipart response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    assert!(!body.contains("<DecompressZipResult>"));
    assert_eq!(
        listed_db_keys(&harness).await,
        vec!["archive.zip", "prefix/first.txt", "prefix/second.txt"]
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after complete")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart", "QmRoot"]
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "prefix/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "prefix/second.txt", SECOND_ENTRY_BYTES).await;
}

#[tokio::test]
async fn test_complete_xml_content_length_over_limit_rejected_without_complete_mutation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&["QmPart"], vec![("QmPart", archive.clone())])).await;
    let upload_id = create_multipart(&harness, "archive.zip", &[]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload snapshot");
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("part snapshot");
    let kubo_before = kubo_log(&harness).await;
    let too_large_xml = vec![b'x'; 4 * 1024 * 1024 + 1];
    let response = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", upload_id.as_str())],
        too_large_xml,
        HeaderMap::new(),
        "test",
    )
    .await;
    let observed = latest_observed_request(&harness).await;
    let declared_length = observed
        .headers
        .get(http::header::CONTENT_LENGTH)
        .expect("declared Content-Length")
        .to_str()
        .expect("numeric Content-Length")
        .parse::<usize>()
        .expect("Content-Length number");
    assert!(declared_length > 4 * 1024 * 1024);
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "CompleteMultipartUpload XML exceeds 4 MiB",
    )
    .await;
    assert_eq!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged upload"),
        upload_before
    );
    assert_eq!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged parts"),
        parts_before
    );
    assert_latest_absent(&harness, "archive.zip").await;
    assert_eq!(
        kubo_log(&harness).await,
        kubo_before,
        "no Complete Kubo calls"
    );
    assert_eq!(etag, "QmPart", "setup part ETag is retained");
}

#[tokio::test]
async fn test_complete_xml_chunked_over_limit_rejected_without_complete_mutation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&["QmPart"], vec![("QmPart", archive.clone())])).await;
    let upload_id = create_multipart(&harness, "archive.zip", &[]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload snapshot");
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("part snapshot");
    let kubo_before = kubo_log(&harness).await;
    let too_large_xml = vec![b'x'; 4 * 1024 * 1024 + 1];
    let chunk_size = too_large_xml.len().div_ceil(3);
    let chunks = too_large_xml
        .chunks(chunk_size)
        .map(Bytes::copy_from_slice)
        .collect();
    let response = send_sigv4_chunked_http1(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", upload_id.as_str())],
        chunks,
        HeaderMap::new(),
        "test",
    )
    .await;
    let observed = latest_observed_request(&harness).await;
    assert_eq!(observed.method, http::Method::POST);
    assert!(
        observed
            .headers
            .get(http::header::TRANSFER_ENCODING)
            .expect("Transfer-Encoding")
            .to_str()
            .expect("Transfer-Encoding value")
            .contains("chunked")
    );
    assert!(!observed.headers.contains_key(http::header::CONTENT_LENGTH));
    assert_eq!(
        observed.headers["x-amz-decoded-content-length"],
        (4 * 1024 * 1024 + 1).to_string()
    );
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "CompleteMultipartUpload XML exceeds 4 MiB",
    )
    .await;
    assert_eq!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged upload"),
        upload_before
    );
    assert_eq!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged parts"),
        parts_before
    );
    assert_latest_absent(&harness, "archive.zip").await;
    assert_eq!(
        kubo_log(&harness).await,
        kubo_before,
        "no Complete Kubo calls"
    );
    assert_eq!(etag, "QmPart", "setup part ETag is retained");
}

#[tokio::test]
async fn test_multipart_traversal_keeps_root_pin_and_retry_state() {
    let archive = traversal_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot"],
        vec![("QmPart", archive.clone()), ("QmRoot", archive.clone())],
    ))
    .await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "",
    )
    .await;
    for key in ["archive.zip", "prefix/escape.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("retry part")
            .etag,
        etag
    );
    store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("retry upload");
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmRoot"], &[]).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart", "QmRoot"]).await;
}

#[tokio::test]
async fn test_multipart_archive_key_collision_is_global_reject_and_retryable() {
    let archive = archive_key_collision_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmCollisionEntry"],
        vec![("QmPart", archive.clone()), ("QmRoot", archive.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "archive.zip", &[("decompress-zip", "")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "zip entry collides with archive key: archive.zip",
    )
    .await;
    assert_latest_absent(&harness, "archive.zip").await;
    assert!(listed_db_keys(&harness).await.is_empty());
    store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("retryable upload row");
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("retryable part row")
            .etag,
        etag
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmCollisionEntry"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmCollisionEntry"],
    )
    .await;
}

#[tokio::test]
async fn test_multipart_abort_signed_removes_rows_and_keeps_part_pin() {
    let harness = start_harness(scripted(&["QmPart"], vec![])).await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    upload_part(
        &harness,
        "archive.zip",
        &upload_id,
        1,
        legal_single_entry_zip(),
    )
    .await;
    let response = abort_multipart(&harness, "archive.zip", &upload_id).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed parts")
            .is_empty()
    );
    assert_latest_absent(&harness, "archive.zip").await;
    assert_latest_absent(&harness, "prefix/file.txt").await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart"]).await;
    assert!(
        !kubo_log(&harness)
            .await
            .iter()
            .any(|entry| entry.contains("/api/v0/cat")),
        "abort must not cat content"
    );
}

#[tokio::test]
async fn test_multipart_single_part_equal_root_remains_readable() {
    let content = b"standard multipart bytes".to_vec();
    let harness = start_harness(scripted(
        &["QmPart", "QmPart"],
        vec![("QmPart", content.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "archive.bin", &[]).await;
    let etag = upload_part(&harness, "archive.bin", &upload_id, 1, content.clone()).await;
    let response = complete_multipart(&harness, "archive.bin", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("complete response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.bin")
            .await
            .expect("completed latest row")
            .cid,
        "QmPart"
    );
    assert_signed_body(&harness, "archive.bin", &content).await;
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed parts")
            .is_empty()
    );
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart"]).await;
}

#[tokio::test]
async fn test_multipart_shared_part_cid_survives_replace_abort_and_complete() {
    let shared = b"shared bytes".to_vec();
    let harness = start_harness(scripted(
        &[
            "QmSharedPart",
            "QmSharedPart",
            "QmReplacement",
            "QmSharedPart",
            "QmSharedPart",
            "QmRoot",
        ],
        vec![
            ("QmSharedPart", shared.clone()),
            ("QmReplacement", b"replacement bytes".to_vec()),
            ("QmRoot", shared.clone()),
        ],
    ))
    .await;
    let put = signed_put(
        &harness,
        "shared.bin",
        &[],
        shared.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);

    let replace_upload = create_multipart(&harness, "replace.bin", &[]).await;
    upload_part(&harness, "replace.bin", &replace_upload, 1, shared.clone()).await;
    upload_part(
        &harness,
        "replace.bin",
        &replace_upload,
        1,
        b"replacement bytes".to_vec(),
    )
    .await;
    assert_signed_body(&harness, "shared.bin", &shared).await;

    let abort_upload = create_multipart(&harness, "abort.bin", &[]).await;
    upload_part(&harness, "abort.bin", &abort_upload, 1, shared.clone()).await;
    let abort = abort_multipart(&harness, "abort.bin", &abort_upload).await;
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);
    assert_signed_body(&harness, "shared.bin", &shared).await;

    let complete_upload = create_multipart(&harness, "complete.bin", &[]).await;
    let etag = upload_part(
        &harness,
        "complete.bin",
        &complete_upload,
        1,
        shared.clone(),
    )
    .await;
    let complete =
        complete_multipart(&harness, "complete.bin", &complete_upload, &[(1, etag)]).await;
    assert_eq!(complete.status(), StatusCode::OK);
    assert_signed_body(&harness, "shared.bin", &shared).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmSharedPart"]).await;
}

#[tokio::test]
async fn test_upload_part_db_failure_keeps_new_pin_and_old_record() {
    let harness = start_harness(scripted(&["QmOldPart", "QmNewPart"], vec![])).await;
    let upload_id = create_multipart(&harness, "archive.zip", &[]).await;
    upload_part(
        &harness,
        "archive.zip",
        &upload_id,
        1,
        b"old part bytes".to_vec(),
    )
    .await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_part_update BEFORE UPDATE ON multipart_parts \
             BEGIN SELECT RAISE(FAIL, 'forced part update failure'); END;",
        ))
        .await
        .expect("install part update failure trigger");
    let response = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"new part bytes".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.text().await.expect("failed upload-part body");
    assert!(body.contains("InternalError"));
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("original part row")
            .cid,
        "QmOldPart"
    );
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmNewPart"], &[]).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmOldPart", "QmNewPart"]).await;
}

// ---------------------------------------------------------------------------
// Task 8: standard operation compatibility regressions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_standard_put_sse_s3_still_succeeds() {
    let harness = start_harness(standard_script(1)).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let response = signed_put(
        &harness,
        "encrypted.bin",
        &[],
        b"encrypted bytes".to_vec(),
        headers,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-amz-server-side-encryption"], "AES256");
    let latest =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "encrypted.bin")
            .await
            .expect("encrypted latest row");
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_some());
}

#[tokio::test]
async fn test_standard_put_sse_c_still_succeeds() {
    let harness = start_harness(standard_script(1)).await;
    let response = signed_put(
        &harness,
        "customer-encrypted.bin",
        &[],
        b"customer encrypted bytes".to_vec(),
        sse_c_headers(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let latest = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "customer-encrypted.bin",
    )
    .await
    .expect("customer encrypted latest row");
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_none());
}

#[tokio::test]
async fn test_standard_multipart_signed_still_succeeds() {
    let completed = b"standard multipart bytes".to_vec();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot"],
        vec![("QmPart", completed.clone()), ("QmRoot", completed.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "multipart.bin", &[]).await;
    let etag = upload_part(&harness, "multipart.bin", &upload_id, 1, completed.clone()).await;
    let response = complete_multipart(&harness, "multipart.bin", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("complete response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    let latest =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "multipart.bin")
            .await
            .expect("completed multipart latest row");
    assert_eq!(latest.cid, "QmRoot");
    assert!(latest.multipart);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed multipart parts")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart"]
    );
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmPart", "QmRoot"], &[]).await;
    assert_signed_body(&harness, "multipart.bin", &completed).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart", "QmRoot"]).await;
}

#[tokio::test]
async fn test_standard_multipart_complete_accepts_weak_part_etag_and_checksums() {
    let completed = b"standard multipart bytes".to_vec();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot"],
        vec![("QmPart", completed.clone()), ("QmRoot", completed.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "multipart-checksums.bin", &[]).await;
    let etag = upload_part(
        &harness,
        "multipart-checksums.bin",
        &upload_id,
        1,
        completed.clone(),
    )
    .await;
    let weak_etag = format!("W/\"{etag}\"");
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag><ChecksumCRC32>crc32-value</ChecksumCRC32><ChecksumCRC32C>crc32c-value</ChecksumCRC32C><ChecksumCRC64NVME>crc64nvme-value</ChecksumCRC64NVME><ChecksumSHA1>sha1-value</ChecksumSHA1><ChecksumSHA256>sha256-value</ChecksumSHA256></Part></CompleteMultipartUpload>",
        quick_xml::escape::escape(&weak_etag),
    );
    assert!(xml.contains(&format!("W/&quot;{etag}&quot;")));
    let response =
        complete_multipart_xml(&harness, "multipart-checksums.bin", &upload_id, xml).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("complete response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    let latest = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "multipart-checksums.bin",
    )
    .await
    .expect("completed multipart latest row");
    assert_eq!(latest.cid, "QmRoot");
    assert!(latest.multipart);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed multipart parts")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart"]
    );
    assert_signed_body(&harness, "multipart-checksums.bin", &completed).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart", "QmRoot"]).await;
}
