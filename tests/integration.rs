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
    assert_no_kubo_calls, assert_pin_calls, complete_multipart, complete_multipart_with_headers,
    complete_multipart_xml, create_multipart, create_multipart_with_headers, duplicate_entry_zip,
    latest_observed_request, legal_single_entry_zip, legal_two_entry_zip, start_harness,
    traversal_zip, upload_part, upload_part_with_headers,
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
    signed_get_with_headers(harness, key, HeaderMap::new()).await
}

async fn signed_get_with_headers(
    harness: &TestHarness,
    key: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_head(harness: &TestHarness, key: &str, range: Option<&str>) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if let Some(range) = range {
        headers.insert(
            http::header::RANGE,
            HeaderValue::from_str(range).expect("valid Range header"),
        );
    }
    signed_head_with_headers(harness, key, headers).await
}

async fn signed_head_with_headers(
    harness: &TestHarness,
    key: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::HEAD,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_copy(
    harness: &TestHarness,
    source_key: &str,
    destination_key: &str,
    mut headers: HeaderMap,
) -> reqwest::Response {
    headers.insert(
        "x-amz-copy-source",
        HeaderValue::from_str(&format!("/{}/{source_key}", harness.bucket))
            .expect("copy source header"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        destination_key,
        &[],
        Vec::new(),
        headers,
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

async fn seed_latest(harness: &TestHarness, key: &str, cid: &str, size: i64) {
    store::object::upsert(
        harness.state.store.db(),
        &format!("id-{}", key.replace('/', "-")),
        &harness.bucket,
        key,
        cid,
        size,
        Some("text/plain"),
        cid,
        None,
        false,
        None,
        None,
        false,
    )
    .await
    .expect("seed latest object");
}

fn xml_sections(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut rest = xml;
    let mut values = Vec::new();
    while let Some(start) = rest.find(&open) {
        let content = &rest[start + open.len()..];
        let Some(end) = content.find(&close) else {
            break;
        };
        values.push(content[..end].to_owned());
        rest = &content[end + close.len()..];
    }
    values
}

fn xml_text(xml: &str, tag: &str) -> Option<String> {
    xml_sections(xml, tag).into_iter().next()
}

fn delete_xml(keys: &[&str], quiet: bool) -> Vec<u8> {
    let objects = keys
        .iter()
        .map(|key| format!("<Object><Key>{key}</Key></Object>"))
        .collect::<String>();
    format!(
        "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{objects}<Quiet>{quiet}</Quiet></Delete>"
    )
    .into_bytes()
}

fn delete_headers(body: &[u8]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let digest = base64::engine::general_purpose::STANDARD.encode(md5::compute(body).0);
    headers.insert(
        "content-md5",
        HeaderValue::from_str(&digest).expect("base64 MD5 header"),
    );
    headers
}

async fn signed_delete_objects(
    harness: &TestHarness,
    keys: &[&str],
    quiet: bool,
) -> reqwest::Response {
    let body = delete_xml(keys, quiet);
    send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("delete", "")],
        body.clone(),
        delete_headers(&body),
        "test",
    )
    .await
}

fn sse_c_headers_for(key: [u8; 32]) -> HeaderMap {
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

fn sse_c_headers() -> HeaderMap {
    sse_c_headers_for([7; 32])
}

fn copy_source_sse_c_headers_for(key: [u8; 32]) -> HeaderMap {
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES256"),
    );
    headers.insert(
        "x-amz-copy-source-server-side-encryption-customer-key",
        HeaderValue::from_str(&key_b64).expect("base64 copy-source key header"),
    );
    headers.insert(
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&md5_b64).expect("base64 copy-source key MD5 header"),
    );
    headers
}

fn fixed_sse_c_ciphertext(key: [u8; 32], nonce: [u8; 12], plaintext: &[u8]) -> Vec<u8> {
    ipfs_s3_gateway::crypto::aes_gcm::encrypt_chunk(
        &ipfs_s3_gateway::crypto::ObjectKey { bytes: key },
        &nonce,
        plaintext,
    )
    .expect("fixed SSE-C ciphertext")
    .to_vec()
}

async fn seed_sse_c_object(
    harness: &TestHarness,
    key: &str,
    cid: &str,
    plaintext: &[u8],
    fingerprinted: bool,
    recorded_size: i64,
) {
    harness.set_cat_body(cid, fixed_sse_c_ciphertext([7; 32], [0x5a; 12], plaintext));
    let object_key = ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] };
    let fingerprint =
        fingerprinted.then(|| harness.state.master_key.sse_c_key_fingerprint(&object_key));
    store::object::upsert(
        harness.state.store.db(),
        &format!("id-{}", key.replace('/', "-")),
        &harness.bucket,
        key,
        cid,
        recorded_size,
        Some("application/octet-stream"),
        cid,
        None,
        true,
        None,
        fingerprint.as_deref(),
        false,
    )
    .await
    .expect("seed SSE-C object");
}

fn inner_complete_request(
    harness: &TestHarness,
    key: &str,
    upload_id: &str,
    etag: &str,
    headers: HeaderMap,
) -> s3s::S3Request<s3s::dto::CompleteMultipartUploadInput> {
    s3s::S3Request {
        input: s3s::dto::CompleteMultipartUploadInput {
            bucket: harness.bucket.clone(),
            key: key.to_owned(),
            upload_id: upload_id.to_owned(),
            multipart_upload: Some(s3s::dto::CompletedMultipartUpload {
                parts: Some(vec![s3s::dto::CompletedPart {
                    e_tag: Some(s3s::dto::ETag::Strong(etag.to_owned())),
                    part_number: Some(1),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        },
        method: http::Method::POST,
        uri: format!("/{}/{key}?uploadId={upload_id}", harness.bucket)
            .parse()
            .unwrap(),
        headers,
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

async fn kubo_call_counts(harness: &TestHarness) -> (usize, usize, usize, usize) {
    let requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log");
    let count = |path| {
        requests
            .iter()
            .filter(|request| request.url.path() == path)
            .count()
    };
    (
        count("/api/v0/add"),
        count("/api/v0/cat"),
        count("/api/v0/pin/add"),
        count("/api/v0/pin/rm"),
    )
}

fn assert_put_cid_headers(response: &reqwest::Response, cid: &str) {
    assert_eq!(
        response.headers()[http::header::ETAG],
        HeaderValue::from_str(&format!("\"{cid}\"")).expect("CID ETag header")
    );
    assert_eq!(response.headers()["x-amz-meta-ipfs-cid"], cid);
    assert_eq!(
        response.headers()["x-amz-meta-ipfs-url"],
        HeaderValue::from_str(&format!("ipfs://{cid}")).expect("IPFS URL header")
    );
}

// ---------------------------------------------------------------------------
// Retained standard behaviour regressions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_aws_bucket_name_validation_rejects_before_store_or_kubo() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let invalid_buckets = [
        "ab".to_owned(),
        "UPPERCASE".to_owned(),
        "under_score".to_owned(),
        ".leading-dot".to_owned(),
        "trailing-hyphen-".to_owned(),
        "adjacent..dot".to_owned(),
        "192.168.0.1".to_owned(),
        "a".repeat(64),
    ];

    for bucket in invalid_buckets {
        let response = send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            &bucket,
            "",
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await;

        assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidBucketName", "").await;
        assert!(
            !store::bucket::exists(harness.state.store.db(), &bucket)
                .await
                .expect("check bucket row"),
            "invalid bucket {bucket} must not have a store row"
        );
    }

    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_aws_bucket_name_validation_accepts_lowercase_dot_and_hyphen() {
    let harness = start_harness(scripted(&[], vec![])).await;

    for bucket in ["abc", "valid-bucket", "valid.bucket"] {
        let response = send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            bucket,
            "",
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK, "create bucket {bucket}");
        assert!(
            store::bucket::exists(harness.state.store.db(), bucket)
                .await
                .expect("check bucket row"),
            "valid bucket {bucket} must have a store row"
        );
    }

    assert_no_kubo_calls(&harness).await;
}

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
async fn test_harness_captures_kubo_file_bytes_and_updates_cat_body() {
    let payload = b"captured exact bytes".to_vec();
    let harness = start_harness(scripted(&["QmCaptured"], vec![])).await;

    let put = signed_put(
        &harness,
        "captured.bin",
        &[],
        payload.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(harness.captured_add_file_bytes(), vec![payload.clone()]);

    harness.set_cat_body("QmCaptured", payload.clone());
    let get = signed_get(&harness, "captured.bin").await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.bytes().await.expect("GET body").as_ref(), &payload);
}

#[tokio::test]
async fn test_client_compat_head_nested_key_signed_on_localhost() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "nested/path/file.txt", "QmNestedCid", 11).await;
    let response = send_sigv4(
        reqwest::Method::HEAD,
        &harness.endpoint,
        &harness.bucket,
        "nested/path/file.txt",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .expect("Content-Length"),
        "11"
    );
    assert!(
        response
            .headers()
            .get(http::header::ETAG)
            .expect("ETag")
            .to_str()
            .expect("ETag is text")
            .contains("QmNestedCid")
    );
}

#[tokio::test]
async fn test_head_range_changes_only_content_length_and_never_calls_kubo() {
    let harness = start_harness(standard_script(0)).await;
    store::object::upsert(
        harness.state.store.db(),
        "head-range-id",
        &harness.bucket,
        "range.bin",
        "QmRange",
        11,
        Some("text/plain"),
        "QmRange",
        Some(serde_json::json!({"color": "blue"})),
        true,
        Some("wrapped-fixture"),
        None,
        false,
    )
    .await
    .expect("seed ranged HEAD object");

    let full = signed_head(&harness, "range.bin", None).await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(full.headers()[http::header::CONTENT_LENGTH], "11");
    assert_eq!(full.headers()[http::header::ETAG], "\"QmRange\"");
    assert_eq!(full.headers()[http::header::CONTENT_TYPE], "text/plain");
    assert_eq!(full.headers()["x-amz-server-side-encryption"], "AES256");
    assert_eq!(full.headers()["x-amz-meta-color"], "blue");
    assert!(full.headers().get(http::header::LAST_MODIFIED).is_some());

    let ranged = signed_head(&harness, "range.bin", Some("bytes=2-5")).await;
    assert_eq!(ranged.status(), StatusCode::OK);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "4");
    assert!(ranged.headers().get(http::header::CONTENT_RANGE).is_none());
    for name in [
        http::header::ETAG.as_str(),
        http::header::CONTENT_TYPE.as_str(),
        http::header::LAST_MODIFIED.as_str(),
        "x-amz-server-side-encryption",
        "x-amz-meta-color",
    ] {
        assert_eq!(
            ranged.headers().get(name),
            full.headers().get(name),
            "{name}"
        );
    }
    assert!(full.bytes().await.expect("full HEAD body").is_empty());
    assert!(ranged.bytes().await.expect("ranged HEAD body").is_empty());

    let unsatisfied = signed_head(&harness, "range.bin", Some("bytes=20-30")).await;
    assert_eq!(unsatisfied.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert!(
        unsatisfied
            .bytes()
            .await
            .expect("unsatisfied HEAD body")
            .is_empty()
    );
    assert_no_kubo_calls(&harness).await;
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
async fn test_client_compat_get_bucket_location_is_standard_us_east_1() {
    let harness = start_harness(standard_script(0)).await;
    let raw = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("location", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(raw.status(), StatusCode::OK);
    let body = raw.bytes().await.expect("GetBucketLocation body");
    let mut deserializer = s3s::xml::Deserializer::new(body.as_ref());
    let decoded = <s3s::dto::GetBucketLocationOutput as s3s::xml::Deserialize>::deserialize(
        &mut deserializer,
    )
    .expect("decode GetBucketLocationOutput with s3s 0.14 restXml");
    deserializer
        .expect_eof()
        .expect("GetBucketLocation XML EOF");
    assert_eq!(decoded.location_constraint, None);

    let body_text = std::str::from_utf8(body.as_ref()).expect("GetBucketLocation UTF-8 XML");
    assert!(body_text.contains("<LocationConstraint"), "{body_text}");
    assert!(!body_text.contains("us-east-1"), "{body_text}");

    let missing = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        "missing-bkt",
        "",
        &[("location", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_s3_error(
        missing,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "bucket not found: missing-bkt",
    )
    .await;
}

#[tokio::test]
async fn test_client_compat_list_v1_delimiter_marker_pages_without_replay() {
    let harness = start_harness(standard_script(0)).await;
    for key in ["a", "photos/1", "photos/2", "videos/1"] {
        seed_latest(&harness, key, &format!("Qm-{}", key.replace('/', "-")), 1).await;
    }

    let first = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("delimiter", "/"), ("max-keys", "2")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = first.text().await.expect("first ListObjects body");
    assert_eq!(xml_sections(&first_body, "Key"), vec!["a"]);
    assert_eq!(
        xml_sections(&first_body, "Prefix")
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>(),
        vec!["photos/"]
    );
    assert_eq!(
        xml_text(&first_body, "IsTruncated").as_deref(),
        Some("true")
    );
    let marker = xml_text(&first_body, "NextMarker").expect("NextMarker");
    assert_eq!(marker, "photos/2");

    let second = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("delimiter", "/"),
            ("marker", marker.as_str()),
            ("max-keys", "2"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = second.text().await.expect("second ListObjects body");
    assert!(xml_sections(&second_body, "Key").is_empty());
    assert_eq!(
        xml_sections(&second_body, "Prefix")
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>(),
        vec!["videos/"]
    );
    assert_eq!(
        xml_text(&second_body, "IsTruncated").as_deref(),
        Some("false")
    );
    assert!(xml_text(&second_body, "NextMarker").is_none());
}

#[tokio::test]
async fn test_client_compat_list_url_encoding_projects_wire_fields_and_preserves_raw_pagination() {
    let harness = start_harness(standard_script(0)).await;
    let raw_prefix = "prefix/";
    let raw_object = "prefix/a%2F(é)";
    let raw_common_key = "prefix/dir%2F(é)/one";
    for key in [raw_object, raw_common_key, "prefix/z"] {
        seed_latest(&harness, key, &format!("Qm-{}", key.replace('/', "-")), 1).await;
    }

    let first_v1 = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("prefix", raw_prefix),
            ("delimiter", "/"),
            ("marker", raw_prefix),
            ("max-keys", "2"),
            ("encoding-type", "url"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(first_v1.status(), StatusCode::OK);
    let first_v1_body = first_v1.text().await.expect("v1 URL-encoded body");
    assert_eq!(
        xml_text(&first_v1_body, "Name").as_deref(),
        Some("test-bkt")
    );
    assert_eq!(
        xml_text(&first_v1_body, "Prefix").as_deref(),
        Some("prefix%2F")
    );
    assert_eq!(
        xml_text(&first_v1_body, "Delimiter").as_deref(),
        Some("%2F")
    );
    assert_eq!(
        xml_text(&first_v1_body, "Marker").as_deref(),
        Some("prefix%2F")
    );
    assert_eq!(
        xml_sections(&first_v1_body, "Key"),
        vec!["prefix%2Fa%252F%28%C3%A9%29"]
    );
    assert!(
        xml_sections(&first_v1_body, "Prefix")
            .contains(&"prefix%2Fdir%252F%28%C3%A9%29%2F".to_owned())
    );
    assert_eq!(
        xml_text(&first_v1_body, "NextMarker").as_deref(),
        Some("prefix%2Fdir%252F%28%C3%A9%29%2Fone")
    );

    let second_v1 = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("prefix", raw_prefix),
            ("delimiter", "/"),
            ("marker", raw_common_key),
            ("max-keys", "2"),
            ("encoding-type", "url"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(second_v1.status(), StatusCode::OK);
    let second_v1_body = second_v1.text().await.expect("second v1 URL-encoded body");
    assert_eq!(xml_sections(&second_v1_body, "Key"), vec!["prefix%2Fz"]);
    assert!(xml_sections(&second_v1_body, "CommonPrefixes").is_empty());
    assert!(xml_text(&second_v1_body, "NextMarker").is_none());

    let v2 = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("list-type", "2"),
            ("prefix", raw_prefix),
            ("delimiter", "/"),
            ("continuation-token", raw_prefix),
            ("start-after", "ignored/%2F(é)"),
            ("max-keys", "2"),
            ("encoding-type", "url"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(v2.status(), StatusCode::OK);
    let v2_body = v2.text().await.expect("v2 URL-encoded body");
    assert_eq!(xml_text(&v2_body, "Name").as_deref(), Some("test-bkt"));
    assert_eq!(xml_text(&v2_body, "Prefix").as_deref(), Some("prefix%2F"));
    assert_eq!(xml_text(&v2_body, "Delimiter").as_deref(), Some("%2F"));
    assert_eq!(
        xml_text(&v2_body, "StartAfter").as_deref(),
        Some("ignored%2F%252F%28%C3%A9%29")
    );
    assert_eq!(
        xml_text(&v2_body, "ContinuationToken").as_deref(),
        Some(raw_prefix),
        "continuation tokens remain opaque"
    );
    assert_eq!(
        xml_text(&v2_body, "NextContinuationToken").as_deref(),
        Some(raw_common_key),
        "the raw cursor identity is not URL-projected"
    );
    assert_eq!(
        xml_sections(&v2_body, "Key"),
        vec!["prefix%2Fa%252F%28%C3%A9%29"]
    );
    assert!(
        xml_sections(&v2_body, "Prefix").contains(&"prefix%2Fdir%252F%28%C3%A9%29%2F".to_owned())
    );
}

#[tokio::test]
async fn test_client_compat_delete_objects_is_retry_safe_and_ordered() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "a", "QmA", 1).await;
    seed_latest(&harness, "b", "QmB", 1).await;

    let response = signed_delete_objects(&harness, &["a", "missing", "a", "b"], false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("DeleteObjects body");
    let deleted = xml_sections(&body, "Deleted")
        .into_iter()
        .map(|section| xml_text(&section, "Key").expect("Deleted key"))
        .collect::<Vec<_>>();
    assert_eq!(deleted, vec!["a", "missing", "a", "b"]);
    assert!(xml_sections(&body, "Error").is_empty());
    assert_latest_absent(&harness, "a").await;
    assert_latest_absent(&harness, "b").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_quiet_hides_successes() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "quiet", "QmQuiet", 5).await;

    let response = signed_delete_objects(&harness, &["quiet", "missing"], true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("quiet DeleteObjects body");
    assert!(xml_sections(&body, "Deleted").is_empty());
    assert!(xml_sections(&body, "Error").is_empty());
    assert_latest_absent(&harness, "quiet").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_continues_after_store_error() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "before", "QmBefore", 6).await;
    seed_latest(&harness, "fail", "QmFail", 4).await;
    seed_latest(&harness, "after", "QmAfter", 5).await;
    harness
        .state
        .store
        .db()
        .execute_unprepared(
            "CREATE TRIGGER fail_one_batch_delete BEFORE UPDATE OF is_latest ON objects \
             WHEN OLD.bucket = 'test-bkt' AND OLD.key = 'fail' AND NEW.is_latest = FALSE \
             BEGIN SELECT RAISE(FAIL, 'injected delete failure'); END",
        )
        .await
        .expect("install delete failure trigger");

    let response = signed_delete_objects(&harness, &["before", "fail", "after"], false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("partial DeleteObjects body");
    let deleted = xml_sections(&body, "Deleted")
        .into_iter()
        .map(|section| xml_text(&section, "Key").expect("Deleted key"))
        .collect::<Vec<_>>();
    assert_eq!(deleted, vec!["before", "after"]);
    let errors = xml_sections(&body, "Error");
    assert_eq!(errors.len(), 1);
    assert_eq!(xml_text(&errors[0], "Key").as_deref(), Some("fail"));
    assert_eq!(
        xml_text(&errors[0], "Code").as_deref(),
        Some("InternalError")
    );
    assert_eq!(
        xml_text(&errors[0], "Message").as_deref(),
        Some("failed to delete object")
    );
    assert_latest_absent(&harness, "before").await;
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "fail")
        .await
        .expect("failed item remains latest");
    assert_latest_absent(&harness, "after").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_missing_bucket_is_request_error() {
    let harness = start_harness(standard_script(0)).await;
    let body = delete_xml(&["a"], false);
    let response = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        "missing-bkt",
        "",
        &[("delete", "")],
        body.clone(),
        delete_headers(&body),
        "test",
    )
    .await;

    assert_s3_error(
        response,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "bucket not found: missing-bkt",
    )
    .await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

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
// Task 6: SSE-C UploadPart fingerprint validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mismatched_sse_c_upload_part_is_rejected_before_kubo_and_preserves_part() {
    let harness = start_harness(scripted(&["QmOriginalPart"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let initial = signed_put(
        &harness,
        "customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"original encrypted part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(initial.status(), StatusCode::OK);
    let calls_before = kubo_call_counts(&harness).await;
    let part_before = store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
        .await
        .expect("original part row");

    let mut missing_algorithm = sse_c_headers_for([7; 32]);
    missing_algorithm.remove("x-amz-server-side-encryption-customer-algorithm");
    let mut mixed_sse = sse_c_headers_for([7; 32]);
    mixed_sse.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );

    for headers in [sse_c_headers_for([8; 32]), missing_algorithm, mixed_sse] {
        let response = signed_put(
            &harness,
            "customer-encrypted.bin",
            &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
            b"rejected replacement".to_vec(),
            headers,
        )
        .await;
        assert_s3_error(
            response,
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "SSE-C",
        )
        .await;
        assert_eq!(
            kubo_call_counts(&harness).await,
            calls_before,
            "rejected UploadPart must not call Kubo"
        );
        assert_eq!(
            store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
                .await
                .expect("unchanged part row"),
            part_before,
            "rejected UploadPart must retain the original part row"
        );
    }
}

#[tokio::test]
async fn legacy_sse_c_upload_part_claims_fingerprint_before_add() {
    let harness = start_harness(scripted(&["QmLegacyPart"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "legacy-customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!(
                "UPDATE multipart_uploads SET sse_c_key_fingerprint = NULL WHERE upload_id = '{upload_id}'"
            ),
        ))
        .await
        .expect("clear legacy fingerprint");
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("legacy upload")
            .sse_c_key_fingerprint
            .is_none()
    );

    let response = signed_put(
        &harness,
        "legacy-customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"legacy encrypted part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let upload = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("claimed upload");
    let expected = harness
        .state
        .master_key
        .sse_c_key_fingerprint(&ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] });
    assert_eq!(
        upload.sse_c_key_fingerprint.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(kubo_call_counts(&harness).await, (1, 0, 1, 0));
}

// ---------------------------------------------------------------------------
// Task 7: SSE-C Complete fingerprint validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mismatched_sse_c_complete_is_pre_kubo_and_upload_remains_retryable() {
    let harness = start_harness(scripted(&["QmEncryptedPart", "QmRoot"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part = signed_put(
        &harness,
        "customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"encrypted multipart part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    let ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured encrypted part");
    harness.set_cat_body("QmEncryptedPart", ciphertext);

    let calls_before = kubo_call_counts(&harness).await;
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload before rejected completes");
    let part_before = store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
        .await
        .expect("part before rejected completes");

    let mut missing_algorithm = sse_c_headers_for([7; 32]);
    missing_algorithm.remove("x-amz-server-side-encryption-customer-algorithm");
    let mut mixed_sse = sse_c_headers_for([7; 32]);
    mixed_sse.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );

    for headers in [sse_c_headers_for([8; 32]), missing_algorithm, mixed_sse] {
        let response = complete_multipart_with_headers(
            &harness,
            "customer-encrypted.bin",
            &upload_id,
            &[(1, etag.clone())],
            headers,
        )
        .await;
        assert_s3_error(
            response,
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "SSE-C",
        )
        .await;
        assert_eq!(
            kubo_call_counts(&harness).await,
            calls_before,
            "rejected CompleteMultipartUpload must not call Kubo"
        );
        assert_eq!(
            store::multipart::get_upload(harness.state.store.db(), &upload_id)
                .await
                .expect("unchanged upload row"),
            upload_before,
            "rejected CompleteMultipartUpload must retain the upload row"
        );
        assert_eq!(
            store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
                .await
                .expect("unchanged part row"),
            part_before,
            "rejected CompleteMultipartUpload must retain the part row"
        );
    }

    let response = complete_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &upload_id,
        &[(1, etag)],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn legacy_sse_c_complete_claims_before_corrupt_ciphertext_error() {
    let harness = start_harness(scripted(&["QmEncryptedPart", "QmRoot"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "legacy-customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part = signed_put(
        &harness,
        "legacy-customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"legacy encrypted multipart part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!(
                "UPDATE multipart_uploads SET sse_c_key_fingerprint = NULL WHERE upload_id = '{upload_id}'"
            ),
        ))
        .await
        .expect("clear legacy fingerprint");
    harness.set_cat_body("QmEncryptedPart", b"corrupt ciphertext".to_vec());
    let calls_before = kubo_call_counts(&harness).await;

    let response = complete_multipart_with_headers(
        &harness,
        "legacy-customer-encrypted.bin",
        &upload_id,
        &[(1, etag.clone())],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidPart", "decrypt").await;
    assert_eq!(
        kubo_call_counts(&harness).await,
        (
            calls_before.0,
            calls_before.1 + 1,
            calls_before.2,
            calls_before.3
        ),
        "corrupt ciphertext must cat once without root add, pin, or unpin"
    );

    let upload = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("retryable legacy upload");
    let expected = harness
        .state
        .master_key
        .sse_c_key_fingerprint(&ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] });
    assert_eq!(
        upload.sse_c_key_fingerprint.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("retryable legacy part")
            .etag,
        etag
    );
}

#[tokio::test]
async fn sse_c_abort_requires_no_customer_key() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;

    let response = abort_multipart(&harness, "customer-encrypted.bin", &upload_id).await;
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
    assert_eq!(kubo_call_counts(&harness).await, (0, 0, 0, 0));
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

// ---------------------------------------------------------------------------
// Task 9: standard PutObject CID response headers and presigned TCP contract
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_standard_presigned_put_get_and_tamper_contract() {
    let cid = "QmStandardPresignedCid";
    let key = "standard-presigned.bin";
    let tampered_key = "tampered-presigned.bin";
    let payload = b"standard presigned exact payload".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;
    let client = reqwest::Client::new();
    let put_url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );

    let put = client
        .put(&put_url)
        .body(payload.clone())
        .send()
        .await
        .expect("presigned standard PUT");
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(
        put.headers()[http::header::ETAG],
        "\"QmStandardPresignedCid\""
    );
    assert_eq!(put.headers()["x-amz-meta-ipfs-cid"], cid);
    assert_eq!(
        put.headers()["x-amz-meta-ipfs-url"],
        "ipfs://QmStandardPresignedCid"
    );
    assert_eq!(harness.captured_add_file_bytes(), vec![payload.clone()]);

    harness.set_cat_body(cid, payload.clone());
    let get_url = presign_sigv4_query(
        &reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    let get = client
        .get(get_url)
        .send()
        .await
        .expect("presigned standard GET");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.bytes().await.expect("presigned GET body").as_ref(),
        payload
    );

    let calls_before_tampering = kubo_call_counts(&harness).await;
    let tampered_path_url =
        put_url.replacen("/standard-presigned.bin?", "/tampered-presigned.bin?", 1);
    let tampered_path = client
        .put(tampered_path_url)
        .body(b"tampered path payload".to_vec())
        .send()
        .await
        .expect("tampered presigned path PUT");
    assert_s3_error(
        tampered_path,
        StatusCode::FORBIDDEN,
        "SignatureDoesNotMatch",
        "",
    )
    .await;
    assert_eq!(kubo_call_counts(&harness).await, calls_before_tampering);
    assert_latest_absent(&harness, tampered_key).await;

    let (signed_url_prefix, signature) = put_url
        .rsplit_once("X-Amz-Signature=")
        .expect("presigned URL includes signature");
    let replacement = if signature.starts_with('0') { '1' } else { '0' };
    let tampered_signature_url = format!(
        "{signed_url_prefix}X-Amz-Signature={replacement}{}",
        &signature[1..]
    );
    let tampered_signature = client
        .put(tampered_signature_url)
        .body(b"tampered signature payload".to_vec())
        .send()
        .await
        .expect("tampered presigned signature PUT");
    assert_s3_error(
        tampered_signature,
        StatusCode::FORBIDDEN,
        "SignatureDoesNotMatch",
        "",
    )
    .await;
    assert_eq!(kubo_call_counts(&harness).await, calls_before_tampering);
    assert_latest_absent(&harness, tampered_key).await;
}

#[tokio::test]
async fn put_object_cid_headers_absent_on_failure() {
    let harness = start_harness(KuboScript {
        add_replies: vec![AddReply::Error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "forced add failure",
        )],
        cat_bodies: HashMap::new(),
    })
    .await;

    let response = signed_put(
        &harness,
        "failed-cid-header.bin",
        &[],
        b"failed put payload".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().get("x-amz-meta-ipfs-cid").is_none());
    assert!(response.headers().get("x-amz-meta-ipfs-url").is_none());
    assert_latest_absent(&harness, "failed-cid-header.bin").await;
    assert_eq!(kubo_call_counts(&harness).await, (1, 0, 0, 0));
}

// ---------------------------------------------------------------------------
// Task 10: authoritative real-TCP encryption, multipart, and range matrix
// ---------------------------------------------------------------------------

#[tokio::test]
async fn v03_plaintext_get_and_head_range_matrix() {
    let cid = "QmV03PlaintextRange";
    let plaintext = b"0123456789".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;

    let put = signed_put(
        &harness,
        "plaintext-range.bin",
        &[],
        plaintext.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.set_cat_body(cid, plaintext.clone());

    let mut range_headers = HeaderMap::new();
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=2-5"));
    let ranged = signed_get_with_headers(&harness, "plaintext-range.bin", range_headers).await;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "4");
    assert_eq!(
        ranged.headers()[http::header::CONTENT_RANGE],
        "bytes 2-5/10"
    );
    assert_eq!(
        ranged.bytes().await.expect("ranged GET body").as_ref(),
        b"2345"
    );

    let cat_requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .into_iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .collect::<Vec<_>>();
    assert_eq!(cat_requests.len(), 1, "ranged plaintext GET cats once");
    assert_eq!(
        cat_requests[0]
            .url
            .query_pairs()
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect::<Vec<_>>(),
        vec![
            ("arg".to_owned(), cid.to_owned()),
            ("bytes".to_owned(), "2-5".to_owned()),
        ]
    );

    let calls_before_unsatisfiable = kubo_call_counts(&harness).await;
    let mut unsatisfiable_headers = HeaderMap::new();
    unsatisfiable_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=10-12"));
    let unsatisfiable =
        signed_get_with_headers(&harness, "plaintext-range.bin", unsatisfiable_headers).await;
    assert_s3_error(
        unsatisfiable,
        StatusCode::RANGE_NOT_SATISFIABLE,
        "InvalidRange",
        "",
    )
    .await;
    assert_eq!(
        kubo_call_counts(&harness).await,
        calls_before_unsatisfiable,
        "unsatisfiable range must not call Kubo"
    );

    let head = signed_head(&harness, "plaintext-range.bin", Some("bytes=2-5")).await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers()[http::header::CONTENT_LENGTH], "4");
    assert!(head.headers().get(http::header::CONTENT_RANGE).is_none());
    assert!(head.bytes().await.expect("ranged HEAD body").is_empty());
}

#[tokio::test]
async fn v03_sse_s3_put_get_and_range_matrix() {
    let cid = "QmV03SseS3";
    let plaintext = b"0123456789abcdef".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;
    let mut encryption_headers = HeaderMap::new();
    encryption_headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );

    let put = signed_put(
        &harness,
        "sse-s3-range.bin",
        &[],
        plaintext.clone(),
        encryption_headers,
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, cid);
    assert_eq!(put.headers()["x-amz-server-side-encryption"], "AES256");
    let ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured SSE-S3 ciphertext");
    assert_ne!(
        ciphertext, plaintext,
        "SSE-S3 Kubo add must receive ciphertext"
    );
    harness.set_cat_body(cid, ciphertext);

    let full = signed_get(&harness, "sse-s3-range.bin").await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(
        full.bytes().await.expect("full SSE-S3 GET body").as_ref(),
        plaintext
    );

    let mut range_headers = HeaderMap::new();
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=4-9"));
    let ranged = signed_get_with_headers(&harness, "sse-s3-range.bin", range_headers).await;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "6");
    assert_eq!(
        ranged.headers()[http::header::CONTENT_RANGE],
        "bytes 4-9/16"
    );
    assert_eq!(
        ranged
            .bytes()
            .await
            .expect("ranged SSE-S3 GET body")
            .as_ref(),
        b"456789"
    );

    let cat_requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .into_iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .collect::<Vec<_>>();
    assert_eq!(
        cat_requests.len(),
        2,
        "full and ranged SSE-S3 GET cat once each"
    );
    for request in cat_requests {
        assert_eq!(
            request
                .url
                .query_pairs()
                .find(|(name, _)| name == "arg")
                .map(|(_, value)| value.into_owned())
                .as_deref(),
            Some(cid)
        );
        assert!(
            request.url.query_pairs().all(|(name, _)| name != "bytes"),
            "encrypted GET must fully cat, decrypt, then slice"
        );
    }
}

#[tokio::test]
async fn v03_sse_c_put_get_and_wrong_key_range_matrix() {
    let cid = "QmV03SseC";
    let plaintext = b"0123456789abcdef".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;

    let put = signed_put(
        &harness,
        "sse-c-range.bin",
        &[],
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, cid);
    let ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured SSE-C ciphertext");
    assert_ne!(
        ciphertext, plaintext,
        "SSE-C Kubo add must receive ciphertext"
    );
    harness.set_cat_body(cid, ciphertext);

    let latest =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "sse-c-range.bin")
            .await
            .expect("SSE-C latest row");
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_none());

    let full =
        signed_get_with_headers(&harness, "sse-c-range.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(
        full.bytes().await.expect("full SSE-C GET body").as_ref(),
        plaintext
    );

    let mut correct_range_headers = sse_c_headers_for([7; 32]);
    correct_range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=3-8"));
    let ranged = signed_get_with_headers(&harness, "sse-c-range.bin", correct_range_headers).await;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "6");
    assert_eq!(
        ranged.headers()[http::header::CONTENT_RANGE],
        "bytes 3-8/16"
    );
    assert_eq!(
        ranged
            .bytes()
            .await
            .expect("ranged SSE-C GET body")
            .as_ref(),
        b"345678"
    );

    let mut wrong_range_headers = sse_c_headers_for([8; 32]);
    wrong_range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=3-8"));
    let wrong_key = signed_get_with_headers(&harness, "sse-c-range.bin", wrong_range_headers).await;
    assert_eq!(wrong_key.status(), StatusCode::FORBIDDEN);
    let wrong_body = wrong_key.text().await.expect("wrong-key SSE-C error body");
    assert!(
        wrong_body.contains("AccessDenied"),
        "wrong-key SSE-C response: {wrong_body}"
    );
    assert!(
        !wrong_body.contains(std::str::from_utf8(&plaintext).expect("plaintext is UTF-8")),
        "wrong-key SSE-C error must not leak plaintext: {wrong_body}"
    );
}

#[tokio::test]
async fn v03_sse_c_multipart_round_trip_matrix() {
    let part_cid = "QmV03SseCPart";
    let root_cid = "QmV03SseCRoot";
    let plaintext = b"SSE-C multipart plaintext".to_vec();
    let harness = start_harness(scripted(&[part_cid, root_cid], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "sse-c-multipart.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;

    let upload = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("SSE-C multipart upload row");
    let fingerprint = upload
        .sse_c_key_fingerprint
        .as_deref()
        .expect("versioned SSE-C key fingerprint");
    assert!(fingerprint.starts_with("v1:hmac-sha256:"));
    assert_eq!(fingerprint.len(), "v1:hmac-sha256:".len() + 64);
    assert!(
        !fingerprint.contains(&base64::engine::general_purpose::STANDARD.encode([7; 32])),
        "SSE-C fingerprint must not persist the raw customer key"
    );
    assert!(
        !fingerprint
            .contains(&base64::engine::general_purpose::STANDARD.encode(md5::compute([7; 32]).0)),
        "SSE-C fingerprint must not persist the customer-key MD5"
    );

    let part_etag = upload_part_with_headers(
        &harness,
        "sse-c-multipart.bin",
        &upload_id,
        1,
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part_ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured encrypted multipart part");
    assert_ne!(
        part_ciphertext, plaintext,
        "SSE-C multipart part Kubo add must receive ciphertext"
    );
    harness.set_cat_body(part_cid, part_ciphertext);

    let completed = complete_multipart_with_headers(
        &harness,
        "sse-c-multipart.bin",
        &upload_id,
        &[(1, part_etag)],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(completed.status(), StatusCode::OK);
    assert!(
        completed
            .text()
            .await
            .expect("SSE-C complete body")
            .contains("<CompleteMultipartUploadResult>")
    );
    let root_ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .nth(1)
        .expect("captured encrypted multipart root");
    assert_ne!(
        root_ciphertext, plaintext,
        "SSE-C multipart root Kubo add must receive ciphertext"
    );
    harness.set_cat_body(root_cid, root_ciphertext);

    let latest = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "sse-c-multipart.bin",
    )
    .await
    .expect("SSE-C completed multipart object");
    assert_eq!(latest.cid, root_cid);
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_none());

    let get =
        signed_get_with_headers(&harness, "sse-c-multipart.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.bytes()
            .await
            .expect("SSE-C multipart GET body")
            .as_ref(),
        plaintext
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err(),
        "completed multipart upload row must be removed"
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("completed multipart parts")
            .is_empty(),
        "completed multipart part rows must be removed"
    );
    assert_eq!(
        kubo_call_counts(&harness).await,
        (2, 3, 2, 0),
        "SSE-C Complete pre-authenticates its part, cats it again to build the root, then GET cats the root"
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec![
            part_cid.to_owned(),
            part_cid.to_owned(),
            root_cid.to_owned(),
        ],
        "SSE-C multipart must cat its part twice and its root once"
    );
    assert_pin_calls(&harness, "/api/v0/pin/add", &[part_cid, root_cid], &[]).await;
    assert!(
        kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty(),
        "SSE-C multipart success must not unpin"
    );
}

#[tokio::test]
async fn v03_put_object_cid_header_matrix() {
    let plaintext_harness = start_harness(scripted(&["QmV03PlainPut"], vec![])).await;
    let plaintext_put = signed_put(
        &plaintext_harness,
        "plain-cid.bin",
        &[],
        b"plain CID header payload".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(plaintext_put.status(), StatusCode::OK);
    assert_put_cid_headers(&plaintext_put, "QmV03PlainPut");

    let sse_s3_harness = start_harness(scripted(&["QmV03SseS3Put"], vec![])).await;
    let mut sse_s3_headers = HeaderMap::new();
    sse_s3_headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let sse_s3_put = signed_put(
        &sse_s3_harness,
        "sse-s3-cid.bin",
        &[],
        b"SSE-S3 CID header payload".to_vec(),
        sse_s3_headers,
    )
    .await;
    assert_eq!(sse_s3_put.status(), StatusCode::OK);
    assert_put_cid_headers(&sse_s3_put, "QmV03SseS3Put");

    let sse_c_harness = start_harness(scripted(&["QmV03SseCPut"], vec![])).await;
    let sse_c_put = signed_put(
        &sse_c_harness,
        "sse-c-cid.bin",
        &[],
        b"SSE-C CID header payload".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(sse_c_put.status(), StatusCode::OK);
    assert_put_cid_headers(&sse_c_put, "QmV03SseCPut");
}

#[tokio::test]
async fn v03_random_nonce_retries_and_part_replacement_never_reuse_nonce() {
    let plaintext = b"identical encrypted multipart payload".to_vec();
    let harness = start_harness(scripted(
        &[
            "QmNoncePart1",
            "QmNoncePart2",
            "QmNonceRoot1",
            "QmNonceRoot2",
        ],
        vec![],
    ))
    .await;
    let upload_id =
        create_multipart_with_headers(&harness, "nonce.bin", &[], sse_c_headers_for([7; 32])).await;

    upload_part_with_headers(
        &harness,
        "nonce.bin",
        &upload_id,
        1,
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let replacement_etag = upload_part_with_headers(
        &harness,
        "nonce.bin",
        &upload_id,
        1,
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part_ciphertext = harness
        .captured_add_file_bytes()
        .get(1)
        .cloned()
        .expect("replacement ciphertext");
    harness.set_cat_body("QmNoncePart2", part_ciphertext);

    for _ in 0..2 {
        ipfs_s3_gateway::s3::ops::multipart::complete_multipart_upload_inner(
            &harness.state,
            inner_complete_request(
                &harness,
                "nonce.bin",
                &upload_id,
                &replacement_etag,
                sse_c_headers_for([7; 32]),
            ),
        )
        .await
        .expect("retryable CompleteMultipartUpload inner result");
    }

    let captured = harness.captured_add_file_bytes();
    assert_eq!(captured.len(), 4);
    let key = ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] };
    let mut nonces = std::collections::HashSet::new();
    for ciphertext in &captured {
        assert_eq!(
            ipfs_s3_gateway::crypto::aes_gcm::decrypt_chunk(&key, ciphertext)
                .expect("captured frame decrypts")
                .as_ref(),
            plaintext
        );
        nonces.insert(<[u8; 12]>::try_from(&ciphertext[..12]).unwrap());
    }
    assert_eq!(nonces.len(), captured.len());
}

#[tokio::test]
async fn fingerprinted_sse_c_get_head_wrong_key_is_zero_kubo_access_denied() {
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "fingerprinted.bin",
        "QmFingerprinted",
        b"fingerprinted body",
        true,
        18,
    )
    .await;

    let get =
        signed_get_with_headers(&harness, "fingerprinted.bin", sse_c_headers_for([8; 32])).await;
    assert_s3_error(get, StatusCode::FORBIDDEN, "AccessDenied", "").await;
    let head =
        signed_head_with_headers(&harness, "fingerprinted.bin", sse_c_headers_for([8; 32])).await;
    assert_eq!(head.status(), StatusCode::FORBIDDEN);
    assert_eq!(kubo_call_counts(&harness).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_get_claims_after_exact_authentication_and_streams_second_cat() {
    let plaintext = b"legacy get body";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "legacy-get.bin",
        "QmLegacyGet",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;

    let response =
        signed_get_with_headers(&harness, "legacy-get.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), plaintext);

    let object =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "legacy-get.bin")
            .await
            .unwrap();
    assert!(object.sse_c_key_fingerprint.is_some());
    assert_eq!(kubo_call_counts(&harness).await, (0, 2, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_head_and_head_range_authenticate_once_then_zero_kubo() {
    let plaintext = b"legacy head body";
    let harness = start_harness(scripted(&[], vec![])).await;
    for (key, cid) in [
        ("legacy-head.bin", "QmLegacyHead"),
        ("legacy-head-range.bin", "QmLegacyHeadRange"),
    ] {
        seed_sse_c_object(
            &harness,
            key,
            cid,
            plaintext,
            false,
            i64::try_from(plaintext.len()).unwrap(),
        )
        .await;
    }

    let first =
        signed_head_with_headers(&harness, "legacy-head.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(first.status(), StatusCode::OK);
    let mut range_headers = sse_c_headers_for([7; 32]);
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=1-4"));
    let first_range =
        signed_head_with_headers(&harness, "legacy-head-range.bin", range_headers.clone()).await;
    assert_eq!(first_range.status(), StatusCode::OK);
    assert_eq!(first_range.headers()[http::header::CONTENT_LENGTH], "4");
    assert!(first_range.bytes().await.unwrap().is_empty());
    assert_eq!(kubo_call_counts(&harness).await, (0, 2, 0, 0));

    let repeated =
        signed_head_with_headers(&harness, "legacy-head.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(repeated.status(), StatusCode::OK);
    let repeated_range =
        signed_head_with_headers(&harness, "legacy-head-range.bin", range_headers).await;
    assert_eq!(repeated_range.status(), StatusCode::OK);
    assert_eq!(kubo_call_counts(&harness).await, (0, 2, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_wrong_key_or_size_mismatch_never_claims() {
    let plaintext = b"legacy authentication";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "legacy-wrong-key.bin",
        "QmLegacyWrongKey",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;
    seed_sse_c_object(
        &harness,
        "legacy-wrong-size.bin",
        "QmLegacyWrongSize",
        plaintext,
        false,
        i64::try_from(plaintext.len() + 1).unwrap(),
    )
    .await;
    seed_sse_c_object(&harness, "legacy-empty.bin", "QmLegacyEmpty", b"", false, 0).await;

    let wrong_key =
        signed_get_with_headers(&harness, "legacy-wrong-key.bin", sse_c_headers_for([8; 32])).await;
    assert_s3_error(wrong_key, StatusCode::FORBIDDEN, "AccessDenied", "").await;
    let wrong_size = signed_head_with_headers(
        &harness,
        "legacy-wrong-size.bin",
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(wrong_size.status(), StatusCode::FORBIDDEN);
    let empty =
        signed_head_with_headers(&harness, "legacy-empty.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(empty.status(), StatusCode::FORBIDDEN);

    for key in [
        "legacy-wrong-key.bin",
        "legacy-wrong-size.bin",
        "legacy-empty.bin",
    ] {
        assert!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .unwrap()
                .sse_c_key_fingerprint
                .is_none(),
            "{key} must remain unclaimed"
        );
    }
}

#[tokio::test]
async fn sse_c_get_and_head_return_customer_response_fields() {
    let plaintext = b"response fields";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "response-fields.bin",
        "QmResponseFields",
        plaintext,
        true,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;
    let expected_md5 = base64::engine::general_purpose::STANDARD.encode(md5::compute([7; 32]).0);

    let get =
        signed_get_with_headers(&harness, "response-fields.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.headers()["x-amz-server-side-encryption-customer-algorithm"],
        "AES256"
    );
    assert_eq!(
        get.headers()["x-amz-server-side-encryption-customer-key-md5"],
        expected_md5
    );
    assert_eq!(get.bytes().await.unwrap().as_ref(), plaintext);

    let head =
        signed_head_with_headers(&harness, "response-fields.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()["x-amz-server-side-encryption-customer-algorithm"],
        "AES256"
    );
    assert_eq!(
        head.headers()["x-amz-server-side-encryption-customer-key-md5"],
        expected_md5
    );
}

#[tokio::test]
async fn copy_sse_c_source_headers_are_required_and_wrong_key_never_publishes() {
    let plaintext = b"copy source";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "copy-source.bin",
        "QmCopySource",
        plaintext,
        true,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;

    let valid = copy_source_sse_c_headers_for([7; 32]);
    let mut malformed = Vec::new();
    for missing in [
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        "x-amz-copy-source-server-side-encryption-customer-key",
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
    ] {
        let mut headers = valid.clone();
        headers.remove(missing);
        malformed.push(headers);
    }
    let mut wrong_algorithm = valid.clone();
    wrong_algorithm.insert(
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES128"),
    );
    malformed.push(wrong_algorithm);
    let mut invalid_base64 = valid.clone();
    invalid_base64.insert(
        "x-amz-copy-source-server-side-encryption-customer-key",
        HeaderValue::from_static("not-base64"),
    );
    malformed.push(invalid_base64);
    let mut short_key = valid.clone();
    short_key.insert(
        "x-amz-copy-source-server-side-encryption-customer-key",
        HeaderValue::from_str(&base64::engine::general_purpose::STANDARD.encode([7; 31])).unwrap(),
    );
    malformed.push(short_key);
    let mut wrong_md5 = valid.clone();
    wrong_md5.insert(
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&base64::engine::general_purpose::STANDARD.encode([0; 16])).unwrap(),
    );
    malformed.push(wrong_md5);
    malformed.push(sse_c_headers_for([7; 32]));

    for (index, headers) in malformed.into_iter().enumerate() {
        let destination = format!("invalid-copy-{index}.bin");
        let response = signed_copy(&harness, "copy-source.bin", &destination, headers).await;
        assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
        assert_latest_absent(&harness, &destination).await;
    }

    let response = signed_copy(
        &harness,
        "copy-source.bin",
        "wrong-key-copy.bin",
        copy_source_sse_c_headers_for([8; 32]),
    )
    .await;
    assert_s3_error(response, StatusCode::FORBIDDEN, "AccessDenied", "").await;
    assert_latest_absent(&harness, "wrong-key-copy.bin").await;
    assert_eq!(kubo_call_counts(&harness).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_copy_claims_then_copies_fingerprint() {
    let plaintext = b"legacy copy source";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "legacy-copy-source.bin",
        "QmLegacyCopySource",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;

    let response = signed_copy(
        &harness,
        "legacy-copy-source.bin",
        "legacy-copy-destination.bin",
        copy_source_sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let source = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "legacy-copy-source.bin",
    )
    .await
    .unwrap();
    let destination = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "legacy-copy-destination.bin",
    )
    .await
    .unwrap();
    assert!(source.sse_c_key_fingerprint.is_some());
    assert_eq!(
        destination.sse_c_key_fingerprint,
        source.sse_c_key_fingerprint
    );
    assert_eq!(kubo_call_counts(&harness).await, (0, 1, 1, 0));
}

#[tokio::test]
async fn all_object_publication_paths_and_completion_reconciliation_keep_fingerprint() {
    let harness = start_harness(scripted(
        &[
            "QmPlainPublication",
            "QmSseS3Publication",
            "QmSseCPublication",
            "QmPartPublication",
            "QmRootPublication",
        ],
        vec![],
    ))
    .await;
    assert_eq!(
        signed_put(
            &harness,
            "plain-publication.bin",
            &[],
            b"plain".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let mut sse_s3 = HeaderMap::new();
    sse_s3.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    assert_eq!(
        signed_put(
            &harness,
            "sse-s3-publication.bin",
            &[],
            b"sse-s3".to_vec(),
            sse_s3,
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put(
            &harness,
            "sse-c-publication.bin",
            &[],
            b"sse-c".to_vec(),
            sse_c_headers_for([7; 32]),
        )
        .await
        .status(),
        StatusCode::OK
    );
    for (key, expected) in [
        ("plain-publication.bin", false),
        ("sse-s3-publication.bin", false),
        ("sse-c-publication.bin", true),
    ] {
        assert_eq!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .unwrap()
                .sse_c_key_fingerprint
                .is_some(),
            expected,
            "publication path {key}"
        );
    }

    let upload_id = create_multipart_with_headers(
        &harness,
        "complete-publication.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let upload_fingerprint = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .sse_c_key_fingerprint
        .unwrap();
    let part_etag = upload_part_with_headers(
        &harness,
        "complete-publication.bin",
        &upload_id,
        1,
        b"multipart publication".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part_ciphertext = harness
        .captured_add_file_bytes()
        .get(3)
        .cloned()
        .expect("multipart publication part ciphertext");
    harness.set_cat_body("QmPartPublication", part_ciphertext);
    let complete = complete_multipart_with_headers(
        &harness,
        "complete-publication.bin",
        &upload_id,
        &[(1, part_etag)],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(complete.status(), StatusCode::OK);
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "complete-publication.bin",
        )
        .await
        .unwrap()
        .sse_c_key_fingerprint
        .as_deref(),
        Some(upload_fingerprint.as_str())
    );
}
