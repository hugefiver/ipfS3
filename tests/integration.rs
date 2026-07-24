mod support;

use base64::Engine as _;
use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use http::{HeaderMap, HeaderValue, StatusCode};
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, QueryFilter, QueryOrder, Statement,
};
use std::collections::{BTreeSet, HashMap};

use ipfs_s3_gateway::config::PolicyConfig;
use ipfs_s3_gateway::store;
use support::decompress::{
    AddReply, KuboScript, S3TestEndpoint, TestHarness, abort_multipart, archive_key_collision_zip,
    assert_no_kubo_calls, assert_pin_calls, complete_multipart, complete_multipart_with_headers,
    complete_multipart_xml, create_multipart, create_multipart_with_headers, duplicate_entry_zip,
    latest_observed_request, legal_single_entry_zip, legal_two_entry_zip, start_harness,
    traversal_zip, upload_part, upload_part_with_headers,
};
use support::pinning::{
    PinningHarness, PinningHarnessConfig, PsaReply, TestProviderConfig, start_pinning_harness,
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

async fn signed_get(harness: &impl S3TestEndpoint, key: &str) -> reqwest::Response {
    signed_get_with_headers(harness, key, HeaderMap::new()).await
}

async fn signed_get_with_headers(
    harness: &impl S3TestEndpoint,
    key: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_head(
    harness: &impl S3TestEndpoint,
    key: &str,
    range: Option<&str>,
) -> reqwest::Response {
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
    harness: &impl S3TestEndpoint,
    key: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::HEAD,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_copy(
    harness: &impl S3TestEndpoint,
    source_key: &str,
    destination_key: &str,
    mut headers: HeaderMap,
) -> reqwest::Response {
    headers.insert(
        "x-amz-copy-source",
        HeaderValue::from_str(&format!("/{}/{source_key}", harness.bucket()))
            .expect("copy source header"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        destination_key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_put(
    harness: &impl S3TestEndpoint,
    key: &str,
    query: &[(&str, &str)],
    body: Vec<u8>,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        key,
        query,
        body,
        headers,
        "test",
    )
    .await
}

async fn signed_put_with_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    body: Vec<u8>,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if !tagging.is_empty() {
        headers.insert(
            "x-amz-tagging",
            HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
        );
    }
    signed_put(harness, key, &[], body, headers).await
}

#[allow(dead_code)]
async fn signed_get_object_tagging(harness: &impl S3TestEndpoint, key: &str) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("tagging", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

#[allow(dead_code)]
async fn signed_put_object_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    tags: &[(&str, &str)],
) -> reqwest::Response {
    let mut xml =
        String::from("<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>");
    for (tag_key, value) in tags {
        xml.push_str("<Tag><Key>");
        xml.push_str(&quick_xml::escape::escape(*tag_key));
        xml.push_str("</Key><Value>");
        xml.push_str(&quick_xml::escape::escape(*value));
        xml.push_str("</Value></Tag>");
    }
    xml.push_str("</TagSet></Tagging>");
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("tagging", "")],
        xml.into_bytes(),
        headers,
        "test",
    )
    .await
}

#[allow(dead_code)]
async fn signed_delete_object_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("tagging", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

#[allow(dead_code)]
async fn signed_copy_with_tagging(
    harness: &impl S3TestEndpoint,
    source_key: &str,
    destination_key: &str,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging-directive",
        HeaderValue::from_static("REPLACE"),
    );
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
    );
    signed_copy(harness, source_key, destination_key, headers).await
}

#[allow(dead_code)]
async fn signed_create_multipart_upload_with_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if !tagging.is_empty() {
        headers.insert(
            "x-amz-tagging",
            HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
        );
    }
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploads", "")],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_create_multipart_zip_upload_with_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    target_prefix: &str,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
    );
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploads", ""), ("decompress-zip", target_prefix)],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_upload_part(
    harness: &impl S3TestEndpoint,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> reqwest::Response {
    let part_number = part_number.to_string();
    signed_put(
        harness,
        key,
        &[("partNumber", &part_number), ("uploadId", upload_id)],
        body,
        HeaderMap::new(),
    )
    .await
}

async fn signed_complete_multipart(
    harness: &impl S3TestEndpoint,
    key: &str,
    upload_id: &str,
    part_number: i32,
    etag: &str,
) -> reqwest::Response {
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>{part_number}</PartNumber><ETag>\"{}\"</ETag></Part></CompleteMultipartUpload>",
        quick_xml::escape::escape(etag)
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploadId", upload_id)],
        xml.into_bytes(),
        headers,
        "test",
    )
    .await
}

async fn signed_decompress_zip_put(
    harness: &impl S3TestEndpoint,
    key: &str,
    target_prefix: &str,
    body: Vec<u8>,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
    );
    signed_put(
        harness,
        key,
        &[("decompress-zip", target_prefix)],
        body,
        headers,
    )
    .await
}

async fn assert_signed_body(harness: &impl S3TestEndpoint, key: &str, expected: &[u8]) {
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

fn tagging_pairs(xml: &str) -> Vec<(String, String)> {
    xml_sections(xml, "Tag")
        .into_iter()
        .map(|tag| {
            (
                xml_text(&tag, "Key").expect("Tag Key"),
                xml_text(&tag, "Value").expect("Tag Value"),
            )
        })
        .collect()
}

async fn assert_tagging(harness: &impl S3TestEndpoint, key: &str, expected: &[(&str, &str)]) {
    let response = signed_get_object_tagging(harness, key).await;
    assert_eq!(response.status(), StatusCode::OK, "GetObjectTagging {key}");
    let body = response.text().await.expect("GetObjectTagging XML");
    let expected = expected
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(
        tagging_pairs(&body),
        expected,
        "GetObjectTagging XML: {body}"
    );
}

async fn latest_pinning_object(
    harness: &PinningHarness,
    key: &str,
) -> store::entities::object::Model {
    store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
        .await
        .expect("latest pinning object")
}

async fn owner_leases(
    harness: &PinningHarness,
    owner_object_id: &str,
) -> Vec<store::entities::pin_lease::Model> {
    store::entities::pin_lease::Entity::find()
        .filter(store::entities::pin_lease::Column::OwnerObjectId.eq(owner_object_id))
        .order_by_asc(store::entities::pin_lease::Column::Source)
        .all(harness.state.store.db())
        .await
        .expect("owner pinning leases")
}

async fn lease_targets(
    harness: &PinningHarness,
    lease_id: &str,
) -> Vec<store::entities::pin_lease_target::Model> {
    store::entities::pin_lease_target::Entity::find()
        .filter(store::entities::pin_lease_target::Column::LeaseId.eq(lease_id))
        .order_by_asc(store::entities::pin_lease_target::Column::Provider)
        .order_by_asc(store::entities::pin_lease_target::Column::Id)
        .all(harness.state.store.db())
        .await
        .expect("lease targets")
}

async fn remote_pin(
    harness: &PinningHarness,
    provider: &str,
    cid: &str,
) -> store::entities::remote_pin::Model {
    store::entities::remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
        .one(harness.state.store.db())
        .await
        .expect("load remote pin")
        .expect("remote pin exists")
}

async fn assert_no_kubo_pin_removes(harness: &PinningHarness) {
    let requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log");
    assert!(
        requests
            .iter()
            .all(|request| request.url.path() != "/api/v0/pin/rm"),
        "remote lifecycle must not remove Kubo pins: {requests:?}"
    );
}

fn assert_submit_request(
    request: &support::pinning::ObservedPsaRequest,
    expected_path: &str,
    expected_cid: &str,
) {
    assert_eq!(request.method, http::Method::POST);
    assert_eq!(request.path, expected_path);
    assert!(request.has_valid_authorization(), "PSA authorization");
    let body: serde_json::Value = serde_json::from_slice(&request.body).expect("PSA submit JSON");
    assert_eq!(
        body.get("cid").and_then(serde_json::Value::as_str),
        Some(expected_cid)
    );
}

fn assert_submit_request_for_job(
    request: &support::pinning::ObservedPsaRequest,
    expected_path: &str,
    expected_cid: &str,
    job: &store::entities::pin_job::Model,
) {
    assert_submit_request(request, expected_path, expected_cid);
    let body: serde_json::Value = serde_json::from_slice(&request.body).expect("PSA submit JSON");
    assert_eq!(
        body.pointer("/meta/gateway_job_id")
            .and_then(serde_json::Value::as_str),
        Some(job.id.as_str())
    );
    assert_eq!(
        body.pointer("/meta/gateway_lease_id")
            .and_then(serde_json::Value::as_str),
        job.lease_id.as_deref()
    );
    assert_eq!(
        body.pointer("/meta/gateway_target_id")
            .and_then(serde_json::Value::as_str),
        job.target_id.as_deref()
    );
}

fn assert_find_request_for_job(
    request: &support::pinning::ObservedPsaRequest,
    expected_path: &str,
    expected_cid: &str,
    job_id: &str,
) {
    assert_eq!(request.method, http::Method::GET);
    assert_eq!(request.path, expected_path);
    assert!(request.has_valid_authorization(), "PSA Find authorization");
    let query = request.query.as_deref().expect("PSA Find query");
    let url = reqwest::Url::parse(&format!("http://localhost{expected_path}?{query}"))
        .expect("PSA Find URL");
    assert_eq!(
        url.query_pairs().into_owned().collect::<Vec<_>>(),
        vec![
            ("cid".to_owned(), expected_cid.to_owned()),
            (
                "meta".to_owned(),
                serde_json::json!({ "gateway_job_id": job_id }).to_string(),
            ),
        ]
    );
}

fn assert_delete_request(request: &support::pinning::ObservedPsaRequest, expected_path: &str) {
    assert_eq!(request.method, http::Method::DELETE);
    assert_eq!(request.path, expected_path);
    assert!(
        request.has_valid_authorization(),
        "PSA DELETE authorization"
    );
}

async fn only_submit_job(harness: &PinningHarness) -> store::entities::pin_job::Model {
    let jobs = harness.pin_jobs().await;
    assert_eq!(jobs.len(), 1, "one Submit must be published");
    assert_eq!(jobs[0].operation, "submit");
    jobs.into_iter().next().expect("published Submit")
}

fn pinning_policy(prefix: &str, provider_mode: &str, providers: &[&str]) -> PolicyConfig {
    PolicyConfig {
        bucket: "test-bkt".to_owned(),
        prefix: prefix.to_owned(),
        trigger: "request".to_owned(),
        provider_mode: provider_mode.to_owned(),
        providers: providers
            .iter()
            .map(|provider| (*provider).to_owned())
            .collect(),
        default_duration: "1h".to_owned(),
        max_duration: "30d".to_owned(),
        allow_decompressed: true,
    }
}

fn repeated_shared_kubo(add_calls: usize) -> KuboScript {
    KuboScript::repeated_add(
        "QmShared",
        add_calls,
        HashMap::from([("QmShared".to_owned(), b"shared".to_vec())]),
    )
}

fn two_provider_request_config(
    policies: Vec<PolicyConfig>,
    add_calls: usize,
) -> PinningHarnessConfig {
    let mut config = PinningHarnessConfig::request_one();
    config.providers = vec![
        TestProviderConfig::pinata("pinata-primary", 10),
        TestProviderConfig::filebase("filebase-primary", 20),
    ];
    config.policies = policies;
    config.kubo_script = repeated_shared_kubo(add_calls);
    config.pinata_script.clear();
    config.filebase_script.clear();
    config
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
    harness: &impl S3TestEndpoint,
    keys: &[&str],
    quiet: bool,
) -> reqwest::Response {
    let body = delete_xml(keys, quiet);
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
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

#[tokio::test]
async fn pinning_harness_runs_signed_s3_and_async_psa_over_real_tcp() {
    let mut harness = start_pinning_harness(PinningHarnessConfig::request_one()).await;
    let put =
        signed_put_with_tagging(&harness, "key", b"body".to_vec(), "ipfs-s3%3Apin=true").await;
    assert_eq!(put.status(), StatusCode::OK);
    assert!(
        harness.pinata_requests().await.is_empty(),
        "provider is not on the S3 response path"
    );

    harness.run_worker_until_idle().await;
    assert_eq!(
        harness
            .pinata_requests()
            .await
            .iter()
            .map(|request| request.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/psa/pins"]
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_automatic_put_is_async_and_eventually_pinned() {
    let mut harness = start_pinning_harness(PinningHarnessConfig::automatic_all()).await;

    let response = signed_put_with_tagging(&harness, "happy.txt", b"happy".to_vec(), "").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_put_cid_headers(&response, "QmTestCid");
    assert!(
        harness.provider_requests().await.is_empty(),
        "provider traffic must not be on the signed S3 response path"
    );

    let object = latest_pinning_object(&harness, "happy.txt").await;
    let leases = owner_leases(&harness, &object.id).await;
    assert_eq!(leases.len(), 1);
    let automatic = &leases[0];
    assert_eq!(automatic.owner_object_id, object.id);
    assert_eq!(automatic.source, "automatic");
    assert_eq!(automatic.state, "active");
    assert_eq!(automatic.generation, 1);
    let targets = lease_targets(&harness, &automatic.id).await;
    assert_eq!(
        targets
            .iter()
            .map(|target| {
                (
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            ("filebase-primary", "QmTestCid", "waiting"),
            ("pinata-primary", "QmTestCid", "waiting"),
        ]
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| (job.operation.as_str(), job.state.as_str()))
            .collect::<Vec<_>>(),
        vec![("submit", "pending"), ("submit", "pending")]
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| {
                (
                    usage.provider.as_str(),
                    usage.reserved_bytes,
                    usage.reserved_pins,
                )
            })
            .collect::<Vec<_>>(),
        vec![("filebase-primary", 5, 1), ("pinata-primary", 5, 1)]
    );

    harness.run_worker_until_idle().await;
    assert_eq!(
        harness.target_states("happy.txt").await,
        vec![
            ("filebase-primary".to_owned(), "pinned".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
        ]
    );
    for (provider, request_id, path) in [
        ("filebase-primary", "filebase-request-1", "/v1/ipfs/pins"),
        ("pinata-primary", "pinata-request-1", "/psa/pins"),
    ] {
        let remote = remote_pin(&harness, provider, "QmTestCid").await;
        assert_eq!(remote.status, "pinned");
        assert_eq!(remote.request_id.as_deref(), Some(request_id));
        assert_eq!(remote.epoch, 1);
        let request = harness
            .provider_requests()
            .await
            .into_iter()
            .find(|request| request.path == path)
            .expect("expected PSA submit request");
        assert_submit_request(&request, path, "QmTestCid");
    }
    assert_signed_body(&harness, "happy.txt", b"happy").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_manual_header_put_and_standard_tag_round_trip() {
    let mut harness = start_pinning_harness(PinningHarnessConfig::request_one()).await;

    let response = signed_put_with_tagging(
        &harness,
        "manual.txt",
        b"body".to_vec(),
        "team=storage&ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1h",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_put_cid_headers(&response, "QmTestCid");
    assert_tagging(
        &harness,
        "manual.txt",
        &[
            ("ipfs-s3:duration", "1h"),
            ("ipfs-s3:pin", "true"),
            ("team", "storage"),
        ],
    )
    .await;

    let object = latest_pinning_object(&harness, "manual.txt").await;
    let leases = owner_leases(&harness, &object.id).await;
    assert_eq!(leases.len(), 1);
    let manual = &leases[0];
    assert_eq!(manual.owner_object_id, object.id);
    assert_eq!(manual.source, "manual");
    assert_eq!(manual.state, "active");
    assert_eq!(manual.generation, 1);
    let targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(targets.len(), 1);
    assert_eq!(
        (
            targets[0].provider.as_str(),
            targets[0].cid.as_str(),
            targets[0].state.as_str(),
        ),
        ("pinata-primary", "QmTestCid", "waiting")
    );
    let remote_before = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (
            remote_before.status.as_str(),
            remote_before.request_id.as_deref(),
            remote_before.epoch,
        ),
        ("reserved", None, 1)
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| {
                (
                    job.operation.as_str(),
                    job.state.as_str(),
                    job.lease_id.as_deref(),
                    job.target_id.as_deref(),
                    job.expected_generation,
                )
            })
            .collect::<Vec<_>>(),
        vec![(
            "submit",
            "pending",
            Some(manual.id.as_str()),
            Some(targets[0].id.as_str()),
            Some(1),
        )]
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| (
                usage.provider.as_str(),
                usage.reserved_bytes,
                usage.reserved_pins
            ))
            .collect::<Vec<_>>(),
        vec![("pinata-primary", 4, 1)]
    );
    assert!(harness.provider_requests().await.is_empty());

    harness.run_worker_until_idle().await;
    let remote_after = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (
            remote_after.status.as_str(),
            remote_after.request_id.as_deref(),
            remote_after.epoch,
        ),
        ("pinned", Some("pinata-request-1"), 1)
    );
    assert_eq!(lease_targets(&harness, &manual.id).await[0].state, "pinned");
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1);
    assert_submit_request(&requests[0], "/psa/pins", "QmTestCid");
    assert_signed_body(&harness, "manual.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_copy_copies_or_replaces_tags_and_reuses_cid_usage() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok("QmShared")],
        cat_bodies: HashMap::from([("QmShared".to_owned(), b"shared".to_vec())]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-shared-request",
        "QmShared",
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "source.txt",
        b"shared".to_vec(),
        "team=source&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, "QmShared");
    harness.run_worker_until_idle().await;

    let source = latest_pinning_object(&harness, "source.txt").await;
    let source_lease = owner_leases(&harness, &source.id).await.remove(0);
    assert_eq!(
        (
            source_lease.owner_object_id.as_str(),
            source_lease.source.as_str(),
            source_lease.state.as_str(),
            source_lease.generation,
        ),
        (source.id.as_str(), "manual", "active", 1)
    );
    assert_eq!(
        lease_targets(&harness, &source_lease.id).await[0].state,
        "pinned"
    );
    let jobs_before_copy = harness
        .pin_jobs()
        .await
        .into_iter()
        .map(|job| (job.id, job.operation, job.provider, job.cid, job.state))
        .collect::<Vec<_>>();

    let copied = signed_copy(&harness, "source.txt", "copied.txt", HeaderMap::new()).await;
    assert_eq!(copied.status(), StatusCode::OK);
    let copied_xml = copied.text().await.expect("CopyObject XML");
    assert_eq!(
        xml_text(&copied_xml, "ETag").as_deref(),
        Some("\"QmShared\"")
    );
    assert!(
        xml_text(&copied_xml, "LastModified").is_some(),
        "CopyObject XML: {copied_xml}"
    );
    assert_tagging(
        &harness,
        "copied.txt",
        &[("ipfs-s3:pin", "true"), ("team", "source")],
    )
    .await;
    assert_signed_body(&harness, "copied.txt", b"shared").await;

    let replaced = signed_copy_with_tagging(
        &harness,
        "source.txt",
        "replaced.txt",
        "team=replaced&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(replaced.status(), StatusCode::OK);
    let replaced_xml = replaced.text().await.expect("CopyObject replacement XML");
    assert_eq!(
        xml_text(&replaced_xml, "ETag").as_deref(),
        Some("\"QmShared\"")
    );
    assert_tagging(
        &harness,
        "replaced.txt",
        &[("ipfs-s3:pin", "true"), ("team", "replaced")],
    )
    .await;
    assert_signed_body(&harness, "replaced.txt", b"shared").await;

    for key in ["copied.txt", "replaced.txt"] {
        let object = latest_pinning_object(&harness, key).await;
        assert_ne!(object.id, source.id, "copy must have a new owner object");
        assert_eq!(object.cid, "QmShared");
        let lease = owner_leases(&harness, &object.id).await.remove(0);
        assert_eq!(
            (
                lease.owner_object_id.as_str(),
                lease.source.as_str(),
                lease.state.as_str(),
                lease.generation,
            ),
            (object.id.as_str(), "manual", "active", 1)
        );
        assert_eq!(
            lease_targets(&harness, &lease.id)
                .await
                .iter()
                .map(|target| {
                    (
                        target.provider.as_str(),
                        target.cid.as_str(),
                        target.state.as_str(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![("pinata-primary", "QmShared", "pinned")]
        );
    }
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| (
                usage.provider.as_str(),
                usage.reserved_bytes,
                usage.reserved_pins
            ))
            .collect::<Vec<_>>(),
        vec![("pinata-primary", 6, 1)],
        "one CID must consume one provider reservation"
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .into_iter()
            .map(|job| (job.id, job.operation, job.provider, job.cid, job.state))
            .collect::<Vec<_>>(),
        jobs_before_copy,
        "pinned CID reuse must not enqueue additional work"
    );
    let remote = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch
        ),
        ("pinned", Some("pinata-shared-request"), 3)
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1, "copies must not submit another PSA pin");
    assert_submit_request(&requests[0], "/psa/pins", "QmShared");
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_multipart_create_tags_apply_only_to_completed_root() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok("QmPart"), AddReply::Ok("QmRoot")],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), b"part-data".to_vec()),
            ("QmRoot".to_owned(), b"part-data".to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-root-request",
        "QmRoot",
    )];
    let mut harness = start_pinning_harness(config).await;

    let create = signed_create_multipart_upload_with_tagging(
        &harness,
        "root.bin",
        "team=multipart&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let create_xml = create.text().await.expect("CreateMultipartUpload XML");
    let upload_id = xml_text(&create_xml, "UploadId").expect("CreateMultipartUpload UploadId");
    assert!(
        xml_text(&create_xml, "Bucket").is_some(),
        "CreateMultipartUpload XML: {create_xml}"
    );
    assert!(harness.pin_leases().await.is_empty());
    assert!(harness.pin_targets().await.is_empty());
    assert!(harness.remote_pins().await.is_empty());
    assert!(harness.provider_usages().await.is_empty());
    assert!(harness.pin_jobs().await.is_empty());

    let part = signed_upload_part(&harness, "root.bin", &upload_id, 1, b"part-data".to_vec()).await;
    assert_eq!(part.status(), StatusCode::OK);
    let part_etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    assert_eq!(part_etag, "QmPart");
    assert!(
        harness.pin_leases().await.is_empty(),
        "parts have no remote leases"
    );
    assert!(
        harness.pin_targets().await.is_empty(),
        "parts have no remote targets"
    );
    assert!(
        harness.remote_pins().await.is_empty(),
        "parts reserve no PSA CID"
    );
    assert!(
        harness.pin_jobs().await.is_empty(),
        "parts enqueue no remote work"
    );

    let complete = signed_complete_multipart(&harness, "root.bin", &upload_id, 1, &part_etag).await;
    assert_eq!(complete.status(), StatusCode::OK);
    let complete_xml = complete.text().await.expect("CompleteMultipartUpload XML");
    assert_eq!(
        xml_text(&complete_xml, "ETag").as_deref(),
        Some("\"QmRoot\"")
    );
    assert_eq!(xml_text(&complete_xml, "Key").as_deref(), Some("root.bin"));
    assert_tagging(
        &harness,
        "root.bin",
        &[("ipfs-s3:pin", "true"), ("team", "multipart")],
    )
    .await;
    let root = latest_pinning_object(&harness, "root.bin").await;
    assert_eq!(root.cid, "QmRoot");
    assert!(root.multipart);
    let leases = owner_leases(&harness, &root.id).await;
    assert_eq!(leases.len(), 1);
    let manual = &leases[0];
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (root.id.as_str(), "manual", "active", 1)
    );
    let targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(
        targets
            .iter()
            .map(|target| {
                (
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![("pinata-primary", "QmRoot", "waiting")]
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| (job.operation.as_str(), job.state.as_str(), job.cid.as_str()))
            .collect::<Vec<_>>(),
        vec![("submit", "pending", "QmRoot")]
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| (
                usage.provider.as_str(),
                usage.reserved_bytes,
                usage.reserved_pins
            ))
            .collect::<Vec<_>>(),
        vec![("pinata-primary", 9, 1)]
    );

    harness.run_worker_until_idle().await;
    let remote = remote_pin(&harness, "pinata-primary", "QmRoot").await;
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch
        ),
        ("pinned", Some("pinata-root-request"), 1)
    );
    assert_eq!(lease_targets(&harness, &manual.id).await[0].state, "pinned");
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1);
    assert_submit_request(&requests[0], "/psa/pins", "QmRoot");
    assert_signed_body(&harness, "root.bin", b"part-data").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_put_tagging_renews_idempotently_and_delete_tagging_cancels_manual_only() {
    let mut config = PinningHarnessConfig::automatic_all();
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-renew-request",
        "QmTestCid",
    )];
    config.filebase_script = vec![PsaReply::pinned_submit(
        "/v1/ipfs/pins",
        "filebase-renew-request",
        "QmTestCid",
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "renew.txt",
        b"happy".to_vec(),
        "team=initial&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, "QmTestCid");
    harness.run_worker_until_idle().await;

    let object = latest_pinning_object(&harness, "renew.txt").await;
    let leases = owner_leases(&harness, &object.id).await;
    assert_eq!(leases.len(), 2);
    let automatic = leases
        .iter()
        .find(|lease| lease.source == "automatic")
        .expect("automatic lease")
        .clone();
    let manual_before = leases
        .iter()
        .find(|lease| lease.source == "manual")
        .expect("manual lease")
        .clone();
    assert_eq!(automatic.owner_object_id, object.id);
    assert_eq!(manual_before.owner_object_id, object.id);
    assert_eq!(
        (
            manual_before.state.as_str(),
            manual_before.generation,
            automatic.state.as_str(),
            automatic.generation,
        ),
        ("active", 1, "active", 1)
    );
    let manual_target_ids = lease_targets(&harness, &manual_before.id)
        .await
        .into_iter()
        .map(|target| target.id)
        .collect::<Vec<_>>();
    let mut remote_epochs_before_renewal = std::collections::BTreeMap::new();
    for provider in ["pinata-primary", "filebase-primary"] {
        remote_epochs_before_renewal.insert(
            provider,
            remote_pin(&harness, provider, "QmTestCid").await.epoch,
        );
    }
    assert_eq!(
        harness.target_states("renew.txt").await,
        vec![
            ("filebase-primary".to_owned(), "pinned".to_owned()),
            ("filebase-primary".to_owned(), "pinned".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
        ]
    );

    let retain_until = (manual_before.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let renewal_tags = [
        ("team", "renewed"),
        ("ipfs-s3:pin", "true"),
        ("ipfs-s3:retain-until", retain_until.as_str()),
    ];
    let renewal = signed_put_object_tagging(&harness, "renew.txt", &renewal_tags).await;
    assert_eq!(renewal.status(), StatusCode::OK);
    assert_tagging(
        &harness,
        "renew.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
            ("team", "renewed"),
        ],
    )
    .await;
    let leases_after_renewal = owner_leases(&harness, &object.id).await;
    let manual_after_renewal = leases_after_renewal
        .iter()
        .find(|lease| lease.id == manual_before.id)
        .expect("renewed manual lease");
    let automatic_after_renewal = leases_after_renewal
        .iter()
        .find(|lease| lease.id == automatic.id)
        .expect("preserved automatic lease");
    assert_eq!(
        (
            manual_after_renewal.owner_object_id.as_str(),
            manual_after_renewal.source.as_str(),
            manual_after_renewal.state.as_str(),
            manual_after_renewal.generation,
            manual_after_renewal.expires_at,
        ),
        (
            object.id.as_str(),
            "manual",
            "active",
            2,
            chrono::DateTime::parse_from_rfc3339(&retain_until)
                .expect("retain-until RFC3339")
                .with_timezone(&Utc),
        )
    );
    assert_eq!(
        (
            automatic_after_renewal.state.as_str(),
            automatic_after_renewal.generation,
        ),
        ("active", 1)
    );
    assert_eq!(
        lease_targets(&harness, &manual_before.id)
            .await
            .into_iter()
            .map(|target| target.id)
            .collect::<Vec<_>>(),
        manual_target_ids,
        "renewal retains original targets"
    );
    let renewal_jobs = harness.pin_jobs().await;
    assert!(renewal_jobs.iter().any(|job| {
        job.operation == "reconcile"
            && job.state == "pending"
            && job.provider == "pinata-primary"
            && job.cid == "QmTestCid"
    }));
    assert!(renewal_jobs.iter().any(|job| {
        job.operation == "reconcile"
            && job.state == "pending"
            && job.provider == "filebase-primary"
            && job.cid == "QmTestCid"
    }));
    for (provider, request_id) in [
        ("pinata-primary", "pinata-renew-request"),
        ("filebase-primary", "filebase-renew-request"),
    ] {
        let remote = remote_pin(&harness, provider, "QmTestCid").await;
        assert_eq!(
            (
                remote.status.as_str(),
                remote.request_id.as_deref(),
                remote.epoch
            ),
            (
                "pinned",
                Some(request_id),
                remote_epochs_before_renewal[provider] + 1,
            )
        );
    }

    let jobs_before_idempotent = renewal_jobs
        .iter()
        .map(|job| job.id.clone())
        .collect::<Vec<_>>();
    let idempotent = signed_put_object_tagging(&harness, "renew.txt", &renewal_tags).await;
    assert_eq!(idempotent.status(), StatusCode::OK);
    let manual_after_idempotent = owner_leases(&harness, &object.id)
        .await
        .into_iter()
        .find(|lease| lease.id == manual_before.id)
        .expect("idempotently retained manual lease");
    assert_eq!(
        (
            manual_after_idempotent.state.as_str(),
            manual_after_idempotent.generation,
            manual_after_idempotent.expires_at,
        ),
        (
            "active",
            2,
            chrono::DateTime::parse_from_rfc3339(&retain_until)
                .expect("retain-until RFC3339")
                .with_timezone(&Utc),
        )
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| job.id.clone())
            .collect::<Vec<_>>(),
        jobs_before_idempotent,
        "equal retain-until must not enqueue more work"
    );

    let delete = signed_delete_object_tagging(&harness, "renew.txt").await;
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    assert_tagging(&harness, "renew.txt", &[]).await;
    let cancelled_manual = owner_leases(&harness, &object.id)
        .await
        .into_iter()
        .find(|lease| lease.id == manual_before.id)
        .expect("cancelled manual lease");
    let retained_automatic = owner_leases(&harness, &object.id)
        .await
        .into_iter()
        .find(|lease| lease.id == automatic.id)
        .expect("retained automatic lease");
    assert_eq!(
        (
            cancelled_manual.owner_object_id.as_str(),
            cancelled_manual.source.as_str(),
            cancelled_manual.state.as_str(),
            cancelled_manual.generation,
        ),
        (object.id.as_str(), "manual", "cancelled", 3)
    );
    assert_eq!(
        (
            retained_automatic.owner_object_id.as_str(),
            retained_automatic.source.as_str(),
            retained_automatic.state.as_str(),
            retained_automatic.generation,
        ),
        (object.id.as_str(), "automatic", "active", 1)
    );
    assert!(
        lease_targets(&harness, &cancelled_manual.id)
            .await
            .iter()
            .all(|target| target.state == "released")
    );
    assert!(
        lease_targets(&harness, &retained_automatic.id)
            .await
            .iter()
            .all(|target| target.state == "pinned")
    );
    assert!(
        harness
            .provider_requests()
            .await
            .iter()
            .all(|request| request.method != http::Method::DELETE),
        "the automatic lease keeps both remote pins desired"
    );
    assert_signed_body(&harness, "renew.txt", b"happy").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_put_tagging_rejects_expired_lease_after_confirmed_release() {
    let archive_bytes = legal_single_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmEntry")],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive_bytes.clone()),
            ("QmEntry".to_owned(), SINGLE_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-request", "QmEntry"),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/pinata-entry-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "entries/",
        archive_bytes,
        "team=archive&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let put_xml = put.text().await.expect("decompress ZIP result XML");
    assert_eq!(
        xml_text(&put_xml, "ArchiveKey").as_deref(),
        Some("archive.zip")
    );
    assert_eq!(
        xml_text(&put_xml, "ArchiveETag").as_deref(),
        Some("QmArchive")
    );

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    let leases = owner_leases(&harness, &archive.id).await;
    assert_eq!(leases.len(), 1);
    let manual = &leases[0];
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.content_mode.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (archive.id.as_str(), "manual", "decompressed", "active", 1)
    );
    let original_targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(
        original_targets
            .iter()
            .map(|target| {
                (
                    target.id.as_str(),
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(
            original_targets[0].id.as_str(),
            "pinata-primary",
            "QmEntry",
            "waiting",
        )]
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| (job.operation.as_str(), job.state.as_str(), job.cid.as_str()))
            .collect::<Vec<_>>(),
        vec![("submit", "pending", "QmEntry")]
    );

    harness.run_worker_until_idle().await;
    let submitted_remote = remote_pin(&harness, "pinata-primary", "QmEntry").await;
    assert_eq!(
        (
            submitted_remote.status.as_str(),
            submitted_remote.request_id.as_deref(),
            submitted_remote.epoch,
        ),
        ("pinned", Some("pinata-entry-request"), 1)
    );
    assert_eq!(lease_targets(&harness, &manual.id).await[0].state, "pinned");
    let submit = harness.provider_requests().await;
    assert_eq!(submit.len(), 1);
    assert_submit_request(&submit[0], "/psa/pins", "QmEntry");

    harness.advance_past_lease_expiry("archive.zip").await;
    let delete_block = harness.block_next_delete("pinata-primary").await;
    harness.restart_worker();
    delete_block.wait_until_blocked().await;
    delete_block.release();
    harness.wait_for_worker_idle().await;
    harness.stop_worker_without_unlocking().await;
    let expired = owner_leases(&harness, &archive.id).await.remove(0);
    assert_eq!(
        (
            expired.owner_object_id.as_str(),
            expired.source.as_str(),
            expired.content_mode.as_str(),
            expired.state.as_str(),
            expired.generation,
        ),
        (archive.id.as_str(), "manual", "decompressed", "expired", 2)
    );
    let released_targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(
        released_targets
            .iter()
            .map(|target| {
                (
                    target.id.as_str(),
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(
            original_targets[0].id.as_str(),
            "pinata-primary",
            "QmEntry",
            "released",
        )]
    );
    let released_remote = remote_pin(&harness, "pinata-primary", "QmEntry").await;
    assert_eq!(
        (
            released_remote.status.as_str(),
            released_remote.request_id.as_deref(),
            released_remote.epoch,
        ),
        ("absent", None, 2)
    );
    let requests_after_release = harness.provider_requests().await;
    assert_eq!(requests_after_release.len(), 2);
    assert_eq!(
        (
            requests_after_release[1].method.clone(),
            requests_after_release[1].path.as_str(),
            requests_after_release[1].has_valid_authorization(),
        ),
        (http::Method::DELETE, "/psa/pins/pinata-entry-request", true,)
    );
    let target_count_before_rejected_renewal = harness.pin_targets().await.len();
    let kubo_requests_before_rejected_renewal = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .len();

    let retain_until = (Utc::now() + ChronoDuration::hours(1)).to_rfc3339();
    let renewal = signed_put_object_tagging(
        &harness,
        "archive.zip",
        &[
            ("team", "renewed"),
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
        ],
    )
    .await;
    assert_s3_error(renewal, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
    let still_expired = owner_leases(&harness, &archive.id).await.remove(0);
    assert_eq!(
        (
            still_expired.id.as_str(),
            still_expired.owner_object_id.as_str(),
            still_expired.state.as_str(),
            still_expired.generation,
        ),
        (
            manual.id.as_str(),
            archive.id.as_str(),
            "expired",
            expired.generation,
        )
    );
    assert_eq!(
        lease_targets(&harness, &manual.id)
            .await
            .into_iter()
            .map(|target| (target.id, target.cid, target.provider, target.state))
            .collect::<Vec<_>>(),
        released_targets
            .into_iter()
            .map(|target| (target.id, target.cid, target.provider, target.state))
            .collect::<Vec<_>>(),
        "confirmed release must not reconstruct extracted targets"
    );
    assert_eq!(
        harness.pin_targets().await.len(),
        target_count_before_rejected_renewal
    );
    assert_eq!(
        harness
            .kubo
            .received_requests()
            .await
            .expect("Kubo request log")
            .len(),
        kubo_requests_before_rejected_renewal,
        "rejected renewal must not reopen or re-extract the archive"
    );
    assert_tagging(
        &harness,
        "archive.zip",
        &[
            ("ipfs-s3:content", "decompressed"),
            ("ipfs-s3:pin", "true"),
            ("team", "archive"),
        ],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &legal_single_entry_zip()).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_overwrite_delete_and_delete_objects_end_only_owned_remote_leases() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmOld"),
            AddReply::Ok("QmNew"),
            AddReply::Ok("QmDelete"),
            AddReply::Ok("QmBatchA"),
            AddReply::Ok("QmBatchB"),
        ],
        cat_bodies: HashMap::from([
            ("QmOld".to_owned(), b"old".to_vec()),
            ("QmNew".to_owned(), b"new".to_vec()),
            ("QmDelete".to_owned(), b"delete".to_vec()),
            ("QmBatchA".to_owned(), b"batch-a".to_vec()),
            ("QmBatchB".to_owned(), b"batch-b".to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-old-request", "QmOld"),
        PsaReply::pinned_submit("/psa/pins", "pinata-new-request", "QmNew"),
        PsaReply::pinned_submit("/psa/pins", "pinata-delete-request", "QmDelete"),
        PsaReply::pinned_submit("/psa/pins", "pinata-batch-a-request", "QmBatchA"),
        PsaReply::pinned_submit("/psa/pins", "pinata-batch-b-request", "QmBatchB"),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/pinata-old-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/pinata-delete-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/pinata-batch-a-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/pinata-batch-b-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
    ];
    let mut harness = start_pinning_harness(config).await;

    let old_put = signed_put_with_tagging(
        &harness,
        "overwrite.txt",
        b"old".to_vec(),
        "team=old&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(old_put.status(), StatusCode::OK);
    assert_put_cid_headers(&old_put, "QmOld");
    harness.run_worker_until_idle().await;
    let old = latest_pinning_object(&harness, "overwrite.txt").await;
    let old_lease = owner_leases(&harness, &old.id).await.remove(0);
    assert_eq!(
        lease_targets(&harness, &old_lease.id).await[0].state,
        "pinned"
    );
    assert_eq!(
        remote_pin(&harness, "pinata-primary", "QmOld")
            .await
            .request_id
            .as_deref(),
        Some("pinata-old-request")
    );

    let overwrite = signed_put_with_tagging(
        &harness,
        "overwrite.txt",
        b"new".to_vec(),
        "team=new&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(overwrite.status(), StatusCode::OK);
    assert_put_cid_headers(&overwrite, "QmNew");
    assert_tagging(
        &harness,
        "overwrite.txt",
        &[("ipfs-s3:pin", "true"), ("team", "new")],
    )
    .await;
    let replacement = latest_pinning_object(&harness, "overwrite.txt").await;
    assert_ne!(replacement.id, old.id);
    assert_eq!(replacement.cid, "QmNew");
    let cancelled_old = owner_leases(&harness, &old.id).await.remove(0);
    assert_eq!(
        (
            cancelled_old.owner_object_id.as_str(),
            cancelled_old.source.as_str(),
            cancelled_old.state.as_str(),
            cancelled_old.generation,
        ),
        (old.id.as_str(), "manual", "cancelled", 2)
    );
    assert!(
        lease_targets(&harness, &old_lease.id)
            .await
            .iter()
            .all(|target| target.state == "released")
    );
    assert!(
        harness.pin_jobs().await.iter().any(|job| {
            job.operation == "unpin"
                && job.provider == "pinata-primary"
                && job.cid == "QmOld"
                && job.expected_remote_epoch == Some(2)
        }),
        "overwrite must enqueue remote-scoped unpin work"
    );
    harness.run_worker_until_idle().await;
    let replacement_lease = owner_leases(&harness, &replacement.id).await.remove(0);
    assert_eq!(
        (
            replacement_lease.owner_object_id.as_str(),
            replacement_lease.source.as_str(),
            replacement_lease.state.as_str(),
            replacement_lease.generation,
        ),
        (replacement.id.as_str(), "manual", "active", 1)
    );
    assert_eq!(
        lease_targets(&harness, &replacement_lease.id)
            .await
            .iter()
            .map(|target| {
                (
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![("pinata-primary", "QmNew", "pinned")]
    );
    assert_eq!(
        (
            remote_pin(&harness, "pinata-primary", "QmOld")
                .await
                .status
                .as_str(),
            remote_pin(&harness, "pinata-primary", "QmNew")
                .await
                .request_id
                .as_deref(),
        ),
        ("absent", Some("pinata-new-request"))
    );
    assert_signed_body(&harness, "overwrite.txt", b"new").await;

    for (key, body, cid) in [
        ("delete.txt", b"delete".as_slice(), "QmDelete"),
        ("batch-a.txt", b"batch-a".as_slice(), "QmBatchA"),
        ("batch-b.txt", b"batch-b".as_slice(), "QmBatchB"),
    ] {
        let response = signed_put_with_tagging(
            &harness,
            key,
            body.to_vec(),
            "team=remove&ipfs-s3%3Apin=true",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "PUT {key}");
        assert_put_cid_headers(&response, cid);
        harness.run_worker_until_idle().await;
        let object = latest_pinning_object(&harness, key).await;
        let lease = owner_leases(&harness, &object.id).await.remove(0);
        let target = lease_targets(&harness, &lease.id).await.remove(0);
        assert_eq!(
            (
                lease.owner_object_id.as_str(),
                lease.source.as_str(),
                lease.state.as_str(),
                lease.generation,
                target.provider.as_str(),
                target.cid.as_str(),
                target.state.as_str(),
            ),
            (
                object.id.as_str(),
                "manual",
                "active",
                1,
                "pinata-primary",
                cid,
                "pinned"
            )
        );
    }

    let delete_object_response = send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        "delete.txt",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(delete_object_response.status(), StatusCode::NO_CONTENT);
    let batch_delete =
        signed_delete_objects(&harness, &["batch-a.txt", "batch-b.txt"], false).await;
    assert_eq!(batch_delete.status(), StatusCode::OK);
    let batch_delete_xml = batch_delete.text().await.expect("DeleteObjects XML");
    assert_eq!(
        xml_sections(&batch_delete_xml, "Deleted")
            .into_iter()
            .map(|deleted| xml_text(&deleted, "Key").expect("Deleted Key"))
            .collect::<Vec<_>>(),
        vec!["batch-a.txt".to_owned(), "batch-b.txt".to_owned()],
        "DeleteObjects XML: {batch_delete_xml}"
    );
    for (key, cid) in [
        ("delete.txt", "QmDelete"),
        ("batch-a.txt", "QmBatchA"),
        ("batch-b.txt", "QmBatchB"),
    ] {
        assert!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .is_err(),
            "deleted key {key} must not remain latest"
        );
        assert!(
            harness.pin_jobs().await.iter().any(|job| {
                job.operation == "unpin"
                    && job.provider == "pinata-primary"
                    && job.cid == cid
                    && job.expected_remote_epoch == Some(2)
            }),
            "{key} must only end its own remote lease"
        );
    }
    harness.run_worker_until_idle().await;
    for cid in ["QmDelete", "QmBatchA", "QmBatchB"] {
        let remote = remote_pin(&harness, "pinata-primary", cid).await;
        assert_eq!(
            (
                remote.status.as_str(),
                remote.request_id.as_deref(),
                remote.epoch
            ),
            ("absent", None, 2),
            "remote lifecycle for {cid}"
        );
    }
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| (
                usage.provider.as_str(),
                usage.reserved_bytes,
                usage.reserved_pins
            ))
            .collect::<Vec<_>>(),
        vec![("pinata-primary", 3, 1)],
        "only the replacement CID remains reserved"
    );
    let requests = harness.provider_requests().await;
    let delete_paths = requests
        .iter()
        .filter(|request| request.method == http::Method::DELETE)
        .map(|request| {
            assert!(
                request.has_valid_authorization(),
                "PSA DELETE authorization"
            );
            request.path.clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        delete_paths,
        vec![
            "/psa/pins/pinata-old-request".to_owned(),
            "/psa/pins/pinata-delete-request".to_owned(),
            "/psa/pins/pinata-batch-a-request".to_owned(),
            "/psa/pins/pinata-batch-b-request".to_owned(),
        ]
    );
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_worker_recovers_expired_claim_and_adopts_ambiguous_submit() {
    let mut config = PinningHarnessConfig::request_one();
    config.policies[0].provider_mode = "all".to_owned();
    let mut failed_find =
        PsaReply::find_for_job("/psa/pins", "accepted-request", "QmTestCid", "pinned");
    failed_find.status = StatusCode::INTERNAL_SERVER_ERROR.as_u16();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "accepted-request", "QmTestCid"),
        failed_find,
        PsaReply::find_for_job("/psa/pins", "accepted-request", "QmTestCid", "pinned"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "recover.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    let submit_block = harness.block_next_submit("pinata-primary").await;
    harness.restart_worker();
    submit_block.wait_until_blocked().await;
    let post = harness
        .wait_for_provider_request("pinata-primary", http::Method::POST, "/psa/pins", 1)
        .await;
    assert_submit_request_for_job(&post, "/psa/pins", "QmTestCid", &submit);

    harness.stop_worker_without_unlocking().await;
    submit_block.release();
    let interrupted = harness.pin_job(&submit.id).await;
    assert_eq!(
        (
            interrupted.state.as_str(),
            interrupted.submit_phase.as_deref(),
            interrupted.attempts,
        ),
        ("running", Some("calling"), 0),
        "accepted response must remain unpersisted at the crash point"
    );
    assert!(
        interrupted.locked_until.is_some(),
        "calling Submit holds a lock"
    );

    harness.advance_past_job_lock().await;
    harness.restart_worker();
    let failed_find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 1)
        .await;
    assert_find_request_for_job(&failed_find, "/psa/pins", "QmTestCid", &submit.id);
    assert!(
        post.sequence < failed_find.sequence,
        "recovery Find follows the accepted POST"
    );
    let retrying = harness.wait_for_job_state(&submit.id, "pending").await;
    harness.stop_worker_without_unlocking().await;
    assert_eq!(retrying.submit_phase.as_deref(), Some("recovering"));
    assert_eq!(
        harness.target_states("recover.txt").await,
        vec![("pinata-primary".to_owned(), "degraded".to_owned())],
        "a reclaimed all-mode Submit must surface a failed recovery Find before retry"
    );

    harness.advance_job_due(&submit.id).await;
    harness.restart_worker();
    let adopted_find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 2)
        .await;
    assert_find_request_for_job(&adopted_find, "/psa/pins", "QmTestCid", &submit.id);
    assert!(failed_find.sequence < adopted_find.sequence);
    let recovered = harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;

    assert_eq!(recovered.submit_phase.as_deref(), Some("recovering"));
    let remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch,
        ),
        ("pinned", Some("accepted-request"), 1)
    );
    assert_eq!(
        harness.target_states("recover.txt").await,
        vec![("pinata-primary".to_owned(), "pinned".to_owned()),]
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 3, "recovery must retry Find, not re-submit");
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
        ]
    );
    assert_signed_body(&harness, "recover.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_reclaimed_submit_with_cancelled_target_adopts_then_unpins() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "cancelled-request", "QmTestCid"),
        PsaReply::find_for_job("/psa/pins", "cancelled-request", "QmTestCid", "pinned"),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/cancelled-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "cancelled-recover.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    let submit_block = harness.block_next_submit("pinata-primary").await;
    harness.restart_worker();
    submit_block.wait_until_blocked().await;
    let post = harness
        .wait_for_provider_request("pinata-primary", http::Method::POST, "/psa/pins", 1)
        .await;
    assert_submit_request_for_job(&post, "/psa/pins", "QmTestCid", &submit);
    harness.stop_worker_without_unlocking().await;
    submit_block.release();

    let cancel = signed_delete_object_tagging(&harness, "cancelled-recover.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        harness.target_states("cancelled-recover.txt").await,
        vec![("pinata-primary".to_owned(), "released".to_owned()),]
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 4, 1)],
        "cancelling an ambiguous Submit retains the reservation"
    );

    harness.advance_past_job_lock().await;
    harness.restart_worker();
    let find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 1)
        .await;
    assert_find_request_for_job(&find, "/psa/pins", "QmTestCid", &submit.id);
    assert!(
        post.sequence < find.sequence,
        "Find adopts the accepted POST"
    );
    harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;
    let adopted = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (adopted.status.as_str(), adopted.request_id.as_deref()),
        ("pinned", Some("cancelled-request"))
    );

    harness
        .run_current_reconcile("pinata-primary", "QmTestCid")
        .await;
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("current Unpin after adoption");
    let delete_block = harness.block_next_delete("pinata-primary").await;
    harness.restart_worker();
    delete_block.wait_until_blocked().await;
    let delete = harness
        .wait_for_provider_request(
            "pinata-primary",
            http::Method::DELETE,
            "/psa/pins/cancelled-request",
            1,
        )
        .await;
    assert_delete_request(&delete, "/psa/pins/cancelled-request");
    assert_eq!(
        remote_pin(&harness, "pinata-primary", "QmTestCid")
            .await
            .request_id
            .as_deref(),
        Some("cancelled-request"),
        "release waits for the confirmed provider DELETE"
    );
    assert_eq!(
        harness.provider_usages().await[0].reserved_pins,
        1,
        "release waits for the confirmed provider DELETE"
    );
    delete_block.release();
    harness.wait_for_job_state(&unpin.id, "done").await;
    harness.stop_worker_without_unlocking().await;

    let released = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (released.status.as_str(), released.request_id.as_deref()),
        ("absent", None)
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 0, 0)]
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 3, "cancelled recovery must not re-submit");
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
            (http::Method::DELETE, "/psa/pins/cancelled-request"),
        ]
    );
    assert_signed_body(&harness, "cancelled-recover.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_recovery_find_none_with_no_desired_target_never_posts() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "lost-request", "QmTestCid"),
        PsaReply::find_none_for_job("/psa/pins", "QmTestCid"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "no-match.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    let submit_block = harness.block_next_submit("pinata-primary").await;
    harness.restart_worker();
    submit_block.wait_until_blocked().await;
    let post = harness
        .wait_for_provider_request("pinata-primary", http::Method::POST, "/psa/pins", 1)
        .await;
    assert_submit_request_for_job(&post, "/psa/pins", "QmTestCid", &submit);
    harness.stop_worker_without_unlocking().await;
    submit_block.release();

    let cancel = signed_delete_object_tagging(&harness, "no-match.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        harness.target_states("no-match.txt").await,
        vec![("pinata-primary".to_owned(), "released".to_owned()),]
    );

    harness.advance_past_job_lock().await;
    harness.restart_worker();
    let find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 1)
        .await;
    assert_find_request_for_job(&find, "/psa/pins", "QmTestCid", &submit.id);
    assert!(
        post.sequence < find.sequence,
        "reclaimed recovery must Find before it can exit for a stale generation"
    );
    let completed_submit = harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;
    assert_eq!(
        completed_submit.submit_phase.as_deref(),
        Some("ready"),
        "a zero-result recovery with no target is safe to complete"
    );

    harness
        .run_current_reconcile("pinata-primary", "QmTestCid")
        .await;
    let remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (remote.status.as_str(), remote.request_id.as_deref()),
        ("absent", None)
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 0, 0)],
        "the no-request reservation releases exactly once"
    );
    let reconciles = harness
        .pin_jobs()
        .await
        .into_iter()
        .filter(|job| job.operation == "reconcile")
        .collect::<Vec<_>>();
    assert_eq!(reconciles.len(), 1, "one stable Reconcile owns the release");
    assert_eq!(reconciles[0].state, "done");
    let requests = harness.provider_requests().await;
    assert_eq!(
        requests.len(),
        2,
        "zero-result recovery must never POST again"
    );
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
        ]
    );
    assert_signed_body(&harness, "no-match.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_running_submit_blocks_no_request_quota_release() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "live-lock-request", "QmTestCid"),
        PsaReply::find_for_job("/psa/pins", "live-lock-request", "QmTestCid", "pinned"),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/live-lock-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "live-lock.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    let submit_block = harness.block_next_submit("pinata-primary").await;
    harness.restart_worker();
    submit_block.wait_until_blocked().await;
    let post = harness
        .wait_for_provider_request("pinata-primary", http::Method::POST, "/psa/pins", 1)
        .await;
    assert_submit_request_for_job(&post, "/psa/pins", "QmTestCid", &submit);

    let cancel = signed_delete_object_tagging(&harness, "live-lock.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    harness.stop_worker_without_unlocking().await;
    submit_block.release();
    let live_submit = harness.pin_job(&submit.id).await;
    assert_eq!(
        (
            live_submit.state.as_str(),
            live_submit.submit_phase.as_deref()
        ),
        ("running", Some("calling"))
    );
    let live_lock = live_submit.locked_until.expect("live calling lock");

    let no_request_unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("cancellation schedules current no-request cleanup");
    harness.restart_worker();
    harness
        .wait_for_job_state(&no_request_unpin.id, "done")
        .await;
    harness.stop_worker_without_unlocking().await;

    harness
        .run_current_reconcile("pinata-primary", "QmTestCid")
        .await;
    let retained = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (retained.status.as_str(), retained.request_id.as_deref()),
        ("reserved", None),
        "Reconcile cannot release a live ambiguous Submit"
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 4, 1)]
    );
    let waiting_reconcile = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "reconcile")
        .expect("Reconcile remains durable while the Submit is live");
    assert_eq!(waiting_reconcile.state, "pending");
    assert!(
        waiting_reconcile.next_attempt_at >= live_lock,
        "Reconcile reschedules at the live Submit lock boundary"
    );

    harness.advance_past_job_lock().await;
    harness.restart_worker();
    let find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 1)
        .await;
    assert_find_request_for_job(&find, "/psa/pins", "QmTestCid", &submit.id);
    assert!(post.sequence < find.sequence);
    harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;

    harness
        .run_current_reconcile("pinata-primary", "QmTestCid")
        .await;
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("adopted cancelled remote gets an Unpin");
    harness.restart_worker();
    let delete = harness
        .wait_for_provider_request(
            "pinata-primary",
            http::Method::DELETE,
            "/psa/pins/live-lock-request",
            1,
        )
        .await;
    assert_delete_request(&delete, "/psa/pins/live-lock-request");
    harness.wait_for_job_state(&unpin.id, "done").await;
    harness.stop_worker_without_unlocking().await;

    let released = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (released.status.as_str(), released.request_id.as_deref()),
        ("absent", None)
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 0, 0)]
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 3, "live-lock recovery must not re-submit");
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
            (http::Method::DELETE, "/psa/pins/live-lock-request"),
        ]
    );
    assert_signed_body(&harness, "live-lock.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_poll_reschedules_one_stable_job_until_pinned() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::submit_status("/psa/pins", "poll-request", "QmTestCid", "queued"),
        PsaReply::pin_status(
            "/psa/pins/poll-request",
            "poll-request",
            "QmTestCid",
            "pinning",
        ),
        PsaReply::pin_status(
            "/psa/pins/poll-request",
            "poll-request",
            "QmTestCid",
            "pinned",
        ),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put =
        signed_put_with_tagging(&harness, "poll.txt", b"body".to_vec(), "ipfs-s3%3Apin=true").await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    harness.run_worker_until_idle().await;
    let poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll")
        .expect("queued Submit creates one Poll");
    assert_eq!(
        (poll.state.as_str(), poll.attempts),
        ("pending", 0),
        "the first Poll waits for its durable interval"
    );
    assert_submit_request_for_job(
        &harness.provider_requests().await[0],
        "/psa/pins",
        "QmTestCid",
        &submit,
    );

    harness.advance_job_due(&poll.id).await;
    harness.run_worker_until_idle().await;
    let after_first_get = harness.pin_job(&poll.id).await;
    assert_eq!(
        (after_first_get.state.as_str(), after_first_get.attempts),
        ("pending", 0),
        "pinning status reuses the same Poll without a retry"
    );
    assert_eq!(after_first_get.id, poll.id);
    assert_eq!(
        remote_pin(&harness, "pinata-primary", "QmTestCid")
            .await
            .status,
        "pinning"
    );

    harness.advance_job_due(&poll.id).await;
    harness.run_worker_until_idle().await;
    let finished_poll = harness.pin_job(&poll.id).await;
    assert_eq!(
        (finished_poll.state.as_str(), finished_poll.attempts),
        ("done", 0)
    );
    assert_eq!(finished_poll.id, poll.id);
    assert_eq!(
        remote_pin(&harness, "pinata-primary", "QmTestCid")
            .await
            .status,
        "pinned"
    );
    assert_eq!(
        harness.target_states("poll.txt").await,
        vec![("pinata-primary".to_owned(), "pinned".to_owned()),]
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins/poll-request"),
            (http::Method::GET, "/psa/pins/poll-request"),
        ]
    );
    for request in &requests {
        assert!(
            request.has_valid_authorization(),
            "PSA request authorization"
        );
    }
    assert_signed_body(&harness, "poll.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_shared_remote_projects_automatic_manual_and_copy_targets() {
    let mut config = PinningHarnessConfig::automatic_all();
    config.providers.truncate(1);
    config.policies[0].providers = vec!["pinata-primary".to_owned()];
    config.kubo_script = repeated_shared_kubo(1);
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "shared-request",
        "QmShared",
    )];
    config.filebase_script.clear();
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "shared.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true&team=shared",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, "QmShared");
    let copied = signed_copy(&harness, "shared.txt", "copy.txt", HeaderMap::new()).await;
    assert_eq!(copied.status(), StatusCode::OK);
    assert_eq!(
        xml_text(&copied.text().await.expect("CopyObject XML"), "ETag").as_deref(),
        Some("\"QmShared\"")
    );
    assert!(harness.provider_requests().await.is_empty());

    for key in ["shared.txt", "copy.txt"] {
        let object = latest_pinning_object(&harness, key).await;
        let leases = owner_leases(&harness, &object.id).await;
        assert_eq!(leases.len(), 2, "automatic and manual leases for {key}");
        assert!(leases.iter().all(|lease| lease.state == "active"));
        assert_eq!(
            leases
                .iter()
                .map(|lease| lease.source.as_str())
                .collect::<Vec<_>>(),
            vec!["automatic", "manual"]
        );
        for lease in &leases {
            assert!(
                lease_targets(&harness, &lease.id)
                    .await
                    .iter()
                    .all(|target| target.state == "waiting"),
                "all targets start before the shared Submit"
            );
        }
    }
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 6, 1)],
        "one shared CID reserves one provider slot"
    );

    harness.run_worker_until_idle().await;
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1, "all shared targets use one PSA Submit");
    assert_submit_request(&requests[0], "/psa/pins", "QmShared");
    for key in ["shared.txt", "copy.txt"] {
        assert_eq!(
            harness.target_states(key).await,
            vec![
                ("pinata-primary".to_owned(), "pinned".to_owned()),
                ("pinata-primary".to_owned(), "pinned".to_owned()),
            ],
            "the shared remote projects pinned to every target for {key}"
        );
        assert_signed_body(&harness, key, b"shared").await;
    }

    let jobs_before_later_copy = harness.pin_jobs().await;
    let later = signed_copy(&harness, "shared.txt", "later-copy.txt", HeaderMap::new()).await;
    assert_eq!(later.status(), StatusCode::OK);
    assert_eq!(
        xml_text(&later.text().await.expect("later CopyObject XML"), "ETag").as_deref(),
        Some("\"QmShared\"")
    );
    assert_eq!(
        harness.target_states("later-copy.txt").await,
        vec![
            ("pinata-primary".to_owned(), "pinned".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
        ],
        "a later copied target projects directly from the pinned remote"
    );
    assert_eq!(
        harness.pin_jobs().await,
        jobs_before_later_copy,
        "later target creates no job"
    );
    assert_eq!(
        harness.provider_requests().await.len(),
        1,
        "later target does not POST"
    );
    assert_signed_body(&harness, "later-copy.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_shared_terminal_failure_coordinates_each_lease() {
    let mut config = two_provider_request_config(
        vec![
            pinning_policy("one-a/", "one", &["pinata-primary", "filebase-primary"]),
            pinning_policy("one-b/", "one", &["pinata-primary", "filebase-primary"]),
            pinning_policy("all/", "all", &["pinata-primary"]),
        ],
        3,
    );
    config.pinata_script = vec![
        PsaReply::submit_status("/psa/pins", "shared-failed", "QmShared", "queued"),
        PsaReply::pin_status(
            "/psa/pins/shared-failed",
            "shared-failed",
            "QmShared",
            "failed",
        ),
    ];
    config.filebase_script = vec![PsaReply::submit_status(
        "/v1/ipfs/pins",
        "fallback-queued",
        "QmShared",
        "queued",
    )];
    let mut harness = start_pinning_harness(config).await;

    for key in ["one-a/key.txt", "one-b/key.txt", "all/key.txt"] {
        let response =
            signed_put_with_tagging(&harness, key, b"shared".to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(response.status(), StatusCode::OK, "signed PutObject {key}");
    }
    harness.run_worker_until_idle().await;
    let poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("one canonical pending Poll for the shared request");
    harness.advance_job_due(&poll.id).await;
    let fallback_block = harness.block_next_submit("filebase-primary").await;
    harness.restart_worker();
    fallback_block.wait_until_blocked().await;
    harness.stop_worker_without_unlocking().await;
    fallback_block.release();

    let failed = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        (
            failed.status.as_str(),
            failed.request_id.as_deref(),
            failed.failure_attempts
        ),
        ("failed", Some("shared-failed"), 1)
    );
    assert!(
        failed.next_retry_at.is_some(),
        "the all-mode lease owns one bounded retry"
    );
    let primary_targets = harness
        .pin_targets()
        .await
        .into_iter()
        .filter(|target| target.provider == "pinata-primary")
        .collect::<Vec<_>>();
    assert_eq!(primary_targets.len(), 3);
    assert!(
        primary_targets
            .iter()
            .all(|target| target.state == "degraded")
    );

    for key in ["one-a/key.txt", "one-b/key.txt"] {
        let object = latest_pinning_object(&harness, key).await;
        let lease = owner_leases(&harness, &object.id).await.remove(0);
        assert_eq!(
            lease.generation, 2,
            "terminal shared failure fails over {key} exactly once"
        );
        assert_eq!(
            lease_targets(&harness, &lease.id)
                .await
                .into_iter()
                .filter(|target| target.provider == "filebase-primary")
                .map(|target| target.state)
                .collect::<Vec<_>>(),
            vec!["waiting".to_owned()]
        );
        assert_signed_body(&harness, key, b"shared").await;
    }
    let all_object = latest_pinning_object(&harness, "all/key.txt").await;
    let all_lease = owner_leases(&harness, &all_object.id).await.remove(0);
    assert_eq!(all_lease.generation, 1, "all-mode must not fail over");
    assert_eq!(
        lease_targets(&harness, &all_lease.id).await[0].state,
        "degraded"
    );
    let retries = harness
        .pin_jobs()
        .await
        .into_iter()
        .filter(|job| job.operation == "reconcile" && job.provider == "pinata-primary")
        .collect::<Vec<_>>();
    assert_eq!(retries.len(), 1, "one all-mode retry is remote scoped");
    assert!(retries[0].lease_id.is_none() && retries[0].target_id.is_none());
    let requests = harness.provider_requests().await;
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins/shared-failed"),
            (http::Method::POST, "/v1/ipfs/pins"),
        ]
    );
    for request in &requests {
        assert!(request.has_valid_authorization());
    }
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_failed_all_remote_forgets_and_resubmits_once() {
    let mut config = two_provider_request_config(
        vec![
            pinning_policy("one/", "one", &["pinata-primary", "filebase-primary"]),
            pinning_policy("all-a/", "all", &["pinata-primary"]),
            pinning_policy("all-b/", "all", &["pinata-primary"]),
        ],
        3,
    );
    config.pinata_script = vec![
        PsaReply::submit_status("/psa/pins", "failed-all", "QmShared", "queued"),
        PsaReply::pin_status("/psa/pins/failed-all", "failed-all", "QmShared", "failed"),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/failed-all",
            StatusCode::NO_CONTENT.as_u16(),
        ),
        PsaReply::pinned_submit("/psa/pins", "replacement-all", "QmShared"),
    ];
    config.filebase_script = vec![PsaReply::pinned_submit(
        "/v1/ipfs/pins",
        "one-fallback",
        "QmShared",
    )];
    let mut harness = start_pinning_harness(config).await;

    for key in ["one/key.txt", "all-a/key.txt", "all-b/key.txt"] {
        let response =
            signed_put_with_tagging(&harness, key, b"shared".to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    harness.run_worker_until_idle().await;
    let poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("shared failed request Poll");
    harness.advance_job_due(&poll.id).await;
    harness.restart_worker();
    let fallback_post = harness
        .wait_for_provider_request("filebase-primary", http::Method::POST, "/v1/ipfs/pins", 1)
        .await;
    let fallback_job_id = serde_json::from_slice::<serde_json::Value>(&fallback_post.body)
        .expect("fallback Submit body")
        .pointer("/meta/gateway_job_id")
        .and_then(serde_json::Value::as_str)
        .expect("fallback job id")
        .to_owned();
    harness.wait_for_job_state(&fallback_job_id, "done").await;
    harness.stop_worker_without_unlocking().await;

    let failed = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        (failed.status.as_str(), failed.failure_attempts),
        ("failed", 1)
    );
    let failed_epoch = failed.epoch;
    assert!(failed.next_retry_at.is_some());
    assert_eq!(
        harness.provider_usages().await[0].reserved_pins,
        1,
        "failed retry retains primary capacity"
    );

    harness
        .advance_remote_retry_due("pinata-primary", "QmShared")
        .await;
    harness
        .run_current_reconcile("pinata-primary", "QmShared")
        .await;
    harness.restart_worker();
    let replacement_post = harness
        .wait_for_provider_request("pinata-primary", http::Method::POST, "/psa/pins", 2)
        .await;
    let replacement_job_id = serde_json::from_slice::<serde_json::Value>(&replacement_post.body)
        .expect("replacement body")
        .pointer("/meta/gateway_job_id")
        .and_then(serde_json::Value::as_str)
        .expect("replacement job id")
        .to_owned();
    harness
        .wait_for_job_state(&replacement_job_id, "done")
        .await;
    harness.stop_worker_without_unlocking().await;

    let replacement = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        (
            replacement.status.as_str(),
            replacement.request_id.as_deref(),
            replacement.epoch
        ),
        ("pinned", Some("replacement-all"), failed_epoch + 1),
        "one durable DELETE/forget advances one epoch before the canonical replacement Submit"
    );
    assert_eq!(replacement.failure_attempts, 0);
    assert_eq!(
        harness.provider_usages().await[0].reserved_pins,
        1,
        "replacement retains the primary reservation"
    );
    for key in ["all-a/key.txt", "all-b/key.txt"] {
        assert_eq!(
            harness.target_states(key).await,
            vec![("pinata-primary".to_owned(), "pinned".to_owned())]
        );
        assert_signed_body(&harness, key, b"shared").await;
    }
    let one_object = latest_pinning_object(&harness, "one/key.txt").await;
    let one_lease = owner_leases(&harness, &one_object.id).await.remove(0);
    assert_eq!(
        one_lease.generation, 3,
        "one terminal failover and its pinned-secondary convergence are the only generation advances"
    );
    assert_eq!(
        lease_targets(&harness, &one_lease.id)
            .await
            .into_iter()
            .filter(|target| target.provider == "filebase-primary")
            .map(|target| target.state)
            .collect::<Vec<_>>(),
        vec!["pinned".to_owned()]
    );
    let pinata_requests = harness.pinata_requests().await;
    assert_eq!(
        pinata_requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins/failed-all"),
            (http::Method::DELETE, "/psa/pins/failed-all"),
            (http::Method::POST, "/psa/pins"),
        ]
    );
    let replacement_job = harness.pin_job(&replacement_job_id).await;
    assert_eq!(replacement_job.operation, "submit");
    assert!(replacement_job.lease_id.is_some() && replacement_job.target_id.is_some());
    assert_eq!(
        harness.filebase_requests().await.len(),
        1,
        "one fallback Submit was selected for the one lease"
    );
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_failed_remote_eight_cycles_stop_without_spin() {
    let mut config = PinningHarnessConfig::request_one();
    config.policies[0].provider_mode = "all".to_owned();
    config.kubo_script = repeated_shared_kubo(1);
    config.pinata_script.clear();
    for cycle in 1..=8 {
        let request_id = format!("failed-cycle-{cycle}");
        config.pinata_script.push(PsaReply::submit_status(
            "/psa/pins",
            &request_id,
            "QmShared",
            "queued",
        ));
        config.pinata_script.push(PsaReply::pin_status(
            format!("/psa/pins/{request_id}"),
            &request_id,
            "QmShared",
            "failed",
        ));
        if cycle < 8 {
            config.pinata_script.push(PsaReply::empty(
                http::Method::DELETE,
                format!("/psa/pins/{request_id}"),
                StatusCode::NO_CONTENT.as_u16(),
            ));
        }
    }
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "eight.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;

    for cycle in 1..=8 {
        let poll = harness
            .pin_jobs()
            .await
            .into_iter()
            .find(|job| job.operation == "poll" && job.state == "pending")
            .expect("each replacement has exactly one pending Poll");
        harness.advance_job_due(&poll.id).await;
        harness.run_worker_until_idle().await;

        let failed = remote_pin(&harness, "pinata-primary", "QmShared").await;
        assert_eq!(failed.status, "failed");
        assert_eq!(
            failed.failure_attempts, cycle,
            "one count per failed request cycle"
        );
        assert_eq!(
            failed.last_failed_request_id.as_deref(),
            Some(format!("failed-cycle-{cycle}").as_str())
        );
        let requests = harness.pinata_requests().await;
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == http::Method::POST)
                .count(),
            cycle as usize,
            "one initial/replacement Submit per cycle"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == http::Method::GET)
                .count(),
            cycle as usize,
            "each distinct request is observed once by its Poll"
        );

        if cycle < 8 {
            assert!(failed.next_retry_at.is_some());
            // Reconcile re-observes the same failure durably before its backoff; the request ID
            // must not consume a second failure-budget slot.
            harness
                .run_current_reconcile("pinata-primary", "QmShared")
                .await;
            assert_eq!(
                remote_pin(&harness, "pinata-primary", "QmShared")
                    .await
                    .failure_attempts,
                cycle,
                "duplicate observation of one request is idempotent"
            );
            harness
                .advance_remote_retry_due("pinata-primary", "QmShared")
                .await;
            harness
                .run_current_reconcile("pinata-primary", "QmShared")
                .await;
            harness.run_worker_until_idle().await;
            let requests = harness.pinata_requests().await;
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.method == http::Method::DELETE)
                    .count(),
                cycle as usize,
                "each non-exhausted cycle forgets exactly one remote request"
            );
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.method == http::Method::POST)
                    .count(),
                cycle as usize + 1,
                "the DELETE creates exactly one canonical replacement Submit"
            );
        } else {
            assert_eq!(
                failed.next_retry_at, None,
                "the eighth request exhausts the retry budget"
            );
            let requests_before_duplicate = harness.pinata_requests().await;
            let request_counts_before_duplicate = (
                requests_before_duplicate.len(),
                requests_before_duplicate
                    .iter()
                    .filter(|request| request.method == http::Method::POST)
                    .count(),
                requests_before_duplicate
                    .iter()
                    .filter(|request| request.method == http::Method::GET)
                    .count(),
                requests_before_duplicate
                    .iter()
                    .filter(|request| request.method == http::Method::DELETE)
                    .count(),
            );
            harness
                .enqueue_current_reconcile("pinata-primary", "QmShared")
                .await;
            harness
                .run_current_reconcile("pinata-primary", "QmShared")
                .await;
            let duplicate = remote_pin(&harness, "pinata-primary", "QmShared").await;
            assert_eq!(
                (
                    duplicate.failure_attempts,
                    duplicate.last_failed_request_id.as_deref(),
                    duplicate.next_retry_at,
                ),
                (8, Some("failed-cycle-8"), None),
                "the eighth request's duplicate observation is idempotent after exhaustion"
            );
            assert_eq!(
                harness.target_states("eight.txt").await,
                vec![("pinata-primary".to_owned(), "degraded".to_owned())]
            );
            let requests_after_duplicate = harness.pinata_requests().await;
            assert_eq!(
                (
                    requests_after_duplicate.len(),
                    requests_after_duplicate
                        .iter()
                        .filter(|request| request.method == http::Method::POST)
                        .count(),
                    requests_after_duplicate
                        .iter()
                        .filter(|request| request.method == http::Method::GET)
                        .count(),
                    requests_after_duplicate
                        .iter()
                        .filter(|request| request.method == http::Method::DELETE)
                        .count(),
                ),
                request_counts_before_duplicate,
                "the exhausted duplicate must not DELETE, POST, or trigger a ninth request"
            );
            assert!(
                harness
                    .pin_jobs()
                    .await
                    .iter()
                    .all(|job| { job.state != "pending" || job.next_attempt_at > Utc::now() }),
                "exhaustion leaves no due retry job to spin"
            );
        }
    }

    let object = latest_pinning_object(&harness, "eight.txt").await;
    let manual = owner_leases(&harness, &object.id).await.remove(0);
    let equal_retain_until = manual.expires_at.to_rfc3339();
    let equal = signed_put_object_tagging(
        &harness,
        "eight.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", equal_retain_until.as_str()),
        ],
    )
    .await;
    assert_eq!(equal.status(), StatusCode::OK);
    let equal_remote = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        (equal_remote.failure_attempts, equal_remote.next_retry_at),
        (8, None),
        "equal renewal cannot restart exhaustion"
    );

    let extended_retain_until = (manual.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let extended = signed_put_object_tagging(
        &harness,
        "eight.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", extended_retain_until.as_str()),
        ],
    )
    .await;
    assert_eq!(extended.status(), StatusCode::OK);
    let reset_by_extension = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        reset_by_extension.failure_attempts, 0,
        "generation-advancing extension resets failed retry state"
    );
    assert!(reset_by_extension.next_retry_at.is_some());

    let copied = signed_copy_with_tagging(
        &harness,
        "eight.txt",
        "eight-new-target.txt",
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(copied.status(), StatusCode::OK);
    assert_eq!(
        xml_text(&copied.text().await.expect("CopyObject XML"), "ETag").as_deref(),
        Some("\"QmShared\"")
    );
    let reset_by_target = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        reset_by_target.failure_attempts, 0,
        "a new shared target also keeps the retry reset"
    );
    assert!(reset_by_target.next_retry_at.is_some());
    assert_signed_body(&harness, "eight.txt", b"shared").await;
    assert_signed_body(&harness, "eight-new-target.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_stale_poll_owner_hands_off_without_duplicate_post() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = repeated_shared_kubo(2);
    config.pinata_script = vec![PsaReply::submit_status(
        "/psa/pins",
        "handoff-request",
        "QmShared",
        "queued",
    )];
    let mut harness = start_pinning_harness(config).await;

    for key in ["first.txt", "second.txt"] {
        let response =
            signed_put_with_tagging(&harness, key, b"shared".to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let first = latest_pinning_object(&harness, "first.txt").await;
    let first_lease = owner_leases(&harness, &first.id).await.remove(0);
    let first_target = lease_targets(&harness, &first_lease.id).await.remove(0);
    let second = latest_pinning_object(&harness, "second.txt").await;
    let second_lease = owner_leases(&harness, &second.id).await.remove(0);
    let second_target = lease_targets(&harness, &second_lease.id).await.remove(0);

    harness.run_worker_until_idle().await;
    let stale_poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("the queued shared remote has one Poll");
    assert_eq!(
        stale_poll.target_id.as_deref(),
        Some(first_target.id.as_str())
    );
    assert_eq!(stale_poll.expected_generation, Some(first_lease.generation));

    let cancelled = signed_delete_object_tagging(&harness, "first.txt").await;
    assert_eq!(cancelled.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        lease_targets(&harness, &first_lease.id).await[0].state,
        "released"
    );
    let remote_after_cancel = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        remote_after_cancel.epoch, 3,
        "cancelling the old Poll owner advances its remote epoch"
    );

    harness
        .run_current_reconcile("pinata-primary", "QmShared")
        .await;
    let handoff_poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| {
            job.operation == "poll"
                && job.state == "pending"
                && job.target_id.as_deref() == Some(second_target.id.as_str())
        })
        .expect("current Reconcile hands queued polling to the next target");
    assert_eq!(
        handoff_poll.expected_generation,
        Some(second_lease.generation)
    );
    assert_ne!(handoff_poll.id, stale_poll.id);
    assert_eq!(
        harness.provider_requests().await.len(),
        1,
        "Poll handoff never re-Submits"
    );
    assert_submit_request(
        &harness.provider_requests().await[0],
        "/psa/pins",
        "QmShared",
    );
    assert_signed_body(&harness, "first.txt", b"shared").await;
    assert_signed_body(&harness, "second.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_stale_remote_epoch_skips_delete() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = repeated_shared_kubo(1);
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "epoch-pinned",
        "QmShared",
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "epoch.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;
    let object = latest_pinning_object(&harness, "epoch.txt").await;
    let manual = owner_leases(&harness, &object.id).await.remove(0);

    let first_extension = (manual.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let first_renewal = signed_put_object_tagging(
        &harness,
        "epoch.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", first_extension.as_str()),
        ],
    )
    .await;
    assert_eq!(first_renewal.status(), StatusCode::OK);
    let second_extension = (manual.expires_at + ChronoDuration::hours(2)).to_rfc3339();
    let second_renewal = signed_put_object_tagging(
        &harness,
        "epoch.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", second_extension.as_str()),
        ],
    )
    .await;
    assert_eq!(second_renewal.status(), StatusCode::OK);
    assert_eq!(
        remote_pin(&harness, "pinata-primary", "QmShared")
            .await
            .epoch,
        3
    );

    let cancelled = signed_delete_object_tagging(&harness, "epoch.txt").await;
    assert_eq!(cancelled.status(), StatusCode::NO_CONTENT);
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin" && job.expected_remote_epoch == Some(4))
        .expect("cancellation publishes the epoch-four Unpin");
    assert_eq!(
        remote_pin(&harness, "pinata-primary", "QmShared")
            .await
            .epoch,
        4
    );

    let copied = signed_copy_with_tagging(
        &harness,
        "epoch.txt",
        "epoch-shared.txt",
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(copied.status(), StatusCode::OK);
    assert_eq!(
        xml_text(&copied.text().await.expect("CopyObject XML"), "ETag").as_deref(),
        Some("\"QmShared\"")
    );
    let current = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!((current.epoch, current.status.as_str()), (5, "pinned"));

    harness.run_worker_until_idle().await;
    assert_eq!(
        harness.pin_job(&unpin.id).await.state,
        "done",
        "stale Unpin is fenced before provider I/O"
    );
    assert!(
        harness
            .pin_jobs()
            .await
            .iter()
            .any(|job| job.operation == "reconcile" && job.expected_remote_epoch == Some(5)),
        "the new desired set owns a current-epoch Reconcile"
    );
    assert!(
        harness
            .provider_requests()
            .await
            .iter()
            .all(|request| request.method != http::Method::DELETE),
        "epoch-four Unpin must not DELETE after epoch five becomes desired"
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(6, 1)],
        "the stale DELETE path retains the unique reservation"
    );
    assert_signed_body(&harness, "epoch.txt", b"shared").await;
    assert_signed_body(&harness, "epoch-shared.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_one_mode_sticky_priority_failover_converges_duplicate() {
    let mut config = two_provider_request_config(
        vec![pinning_policy(
            "",
            "one",
            &["pinata-primary", "filebase-primary"],
        )],
        1,
    );
    config.pinata_script = vec![
        PsaReply::submit_status("/psa/pins", "sticky-primary", "QmShared", "queued"),
        PsaReply::empty(
            http::Method::GET,
            "/psa/pins/sticky-primary",
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        ),
        PsaReply::pin_status(
            "/psa/pins/sticky-primary",
            "sticky-primary",
            "QmShared",
            "failed",
        ),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/sticky-primary",
            StatusCode::NO_CONTENT.as_u16(),
        ),
    ];
    config.filebase_script = vec![PsaReply::pinned_submit(
        "/v1/ipfs/pins",
        "sticky-secondary",
        "QmShared",
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "sticky.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;
    let initial_poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("queued primary Poll");
    harness.advance_job_due(&initial_poll.id).await;
    harness.run_worker_until_idle().await;

    let object = latest_pinning_object(&harness, "sticky.txt").await;
    let lease_after_transient = owner_leases(&harness, &object.id).await.remove(0);
    assert_eq!(
        lease_after_transient.generation, 1,
        "transient primary failure stays sticky"
    );
    assert!(
        lease_targets(&harness, &lease_after_transient.id)
            .await
            .iter()
            .all(|target| target.provider == "pinata-primary"),
        "transient failure must not create a fallback target"
    );
    assert!(harness.filebase_requests().await.is_empty());
    let retry_poll = harness.pin_job(&initial_poll.id).await;
    assert_eq!(
        (retry_poll.state.as_str(), retry_poll.attempts),
        ("pending", 1)
    );

    harness.advance_job_due(&retry_poll.id).await;
    harness.restart_worker();
    let fallback_post = harness
        .wait_for_provider_request("filebase-primary", http::Method::POST, "/v1/ipfs/pins", 1)
        .await;
    let fallback_job_id = serde_json::from_slice::<serde_json::Value>(&fallback_post.body)
        .expect("fallback Submit body")
        .pointer("/meta/gateway_job_id")
        .and_then(serde_json::Value::as_str)
        .expect("fallback job id")
        .to_owned();
    harness.wait_for_job_state(&fallback_job_id, "done").await;
    let delete = harness
        .wait_for_provider_request(
            "pinata-primary",
            http::Method::DELETE,
            "/psa/pins/sticky-primary",
            1,
        )
        .await;
    assert_delete_request(&delete, "/psa/pins/sticky-primary");
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin" && job.provider == "pinata-primary")
        .expect("pinned fallback schedules old-primary Unpin");
    harness.wait_for_job_state(&unpin.id, "done").await;
    harness.stop_worker_without_unlocking().await;
    let converged_lease = owner_leases(&harness, &object.id).await.remove(0);
    assert_eq!(
        converged_lease.generation, 3,
        "terminal failover then pinned convergence advance the generation once each"
    );
    let targets = lease_targets(&harness, &converged_lease.id).await;
    assert_eq!(
        targets
            .iter()
            .map(|target| (target.provider.as_str(), target.state.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("filebase-primary", "pinned"),
            ("pinata-primary", "released"),
        ]
    );
    let required = targets
        .iter()
        .filter(|target| {
            matches!(
                target.state.as_str(),
                "waiting" | "submitted" | "pinned" | "degraded"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        required
            .iter()
            .map(|target| target.provider.as_str())
            .collect::<Vec<_>>(),
        vec!["filebase-primary"],
        "one-mode converges to exactly one required provider without failback"
    );
    assert_eq!(
        (
            remote_pin(&harness, "filebase-primary", "QmShared")
                .await
                .status
                .as_str(),
            remote_pin(&harness, "pinata-primary", "QmShared")
                .await
                .status
                .as_str()
        ),
        ("pinned", "absent")
    );
    assert_eq!(
        harness
            .pinata_requests()
            .await
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins/sticky-primary"),
            (http::Method::GET, "/psa/pins/sticky-primary"),
            (http::Method::DELETE, "/psa/pins/sticky-primary"),
        ]
    );
    assert_eq!(
        harness
            .filebase_requests()
            .await
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![(http::Method::POST, "/v1/ipfs/pins")]
    );
    assert_signed_body(&harness, "sticky.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_all_mode_partial_success_keeps_retrying_degraded_provider() {
    let mut config = PinningHarnessConfig::automatic_all();
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "partial-pinata",
        "QmTestCid",
    )];
    config.filebase_script = vec![
        PsaReply::empty(
            http::Method::POST,
            "/v1/ipfs/pins",
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        ),
        PsaReply::find_none_for_job("/v1/ipfs/pins", "QmTestCid"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(&harness, "partial.txt", b"happy".to_vec(), "").await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;

    assert_eq!(
        harness.target_states("partial.txt").await,
        vec![
            ("filebase-primary".to_owned(), "degraded".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
        ],
        "all-mode remains available through Pinata while Filebase is visibly degraded"
    );
    let pinata = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    let filebase = remote_pin(&harness, "filebase-primary", "QmTestCid").await;
    assert_eq!(
        (pinata.status.as_str(), filebase.status.as_str()),
        ("pinned", "reserved")
    );
    let retry = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "submit" && job.provider == "filebase-primary")
        .expect("degraded all-mode provider retains a durable Submit recovery");
    assert_eq!(retry.state, "pending");
    assert_eq!(retry.submit_phase.as_deref(), Some("recovery_backoff"));
    assert!(retry.next_attempt_at > Utc::now());
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 3);
    assert_submit_request(&requests[0], "/psa/pins", "QmTestCid");
    assert_submit_request(&requests[1], "/v1/ipfs/pins", "QmTestCid");
    assert_find_request_for_job(&requests[2], "/v1/ipfs/pins", "QmTestCid", &retry.id);
    assert_signed_body(&harness, "partial.txt", b"happy").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_quota_evicts_oldest_unique_cid_after_confirmed_unpin() {
    let mut config = PinningHarnessConfig::request_one();
    config.providers[0].max_bytes = 6;
    config.providers[0].max_pins = 2;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmOld"),
            AddReply::Ok("QmNewer"),
            AddReply::Ok("QmIncoming"),
        ],
        cat_bodies: HashMap::from([
            ("QmOld".to_owned(), b"old".to_vec()),
            ("QmNewer".to_owned(), b"new".to_vec()),
            ("QmIncoming".to_owned(), b"in!".to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "old-request", "QmOld"),
        PsaReply::pinned_submit("/psa/pins", "newer-request", "QmNewer"),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/old-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
        PsaReply::pinned_submit("/psa/pins", "incoming-request", "QmIncoming"),
    ];
    let mut harness = start_pinning_harness(config).await;

    for (key, body, cid) in [
        ("old.txt", b"old".as_slice(), "QmOld"),
        ("newer.txt", b"new".as_slice(), "QmNewer"),
    ] {
        let put = signed_put_with_tagging(&harness, key, body.to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(put.status(), StatusCode::OK, "PUT {key}");
        assert_put_cid_headers(&put, cid);
        harness.run_worker_until_idle().await;
    }

    let newer = latest_pinning_object(&harness, "newer.txt").await;
    let newer_lease = owner_leases(&harness, &newer.id).await.remove(0);
    let renewed_until = (newer_lease.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let renew = signed_put_object_tagging(
        &harness,
        "newer.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", renewed_until.as_str()),
        ],
    )
    .await;
    assert_eq!(renew.status(), StatusCode::OK);
    assert_eq!(
        owner_leases(&harness, &newer.id).await[0].generation,
        newer_lease.generation + 1,
        "renewing the newer CID makes the older CID the eviction candidate"
    );

    let incoming = signed_put_with_tagging(
        &harness,
        "incoming.txt",
        b"in!".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(incoming.status(), StatusCode::OK);
    assert_put_cid_headers(&incoming, "QmIncoming");
    assert_eq!(
        harness.target_states("incoming.txt").await,
        vec![("pinata-primary".to_owned(), "quota_waiting".to_owned())]
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 2),
        "the incoming CID cannot reserve until a remote DELETE is confirmed"
    );

    let delete_block = harness.block_next_delete("pinata-primary").await;
    harness.restart_worker();
    delete_block.wait_until_blocked().await;
    let delete = harness
        .wait_for_provider_request(
            "pinata-primary",
            http::Method::DELETE,
            "/psa/pins/old-request",
            1,
        )
        .await;
    assert_delete_request(&delete, "/psa/pins/old-request");
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 2),
        "the eviction reservation remains until the provider confirms DELETE"
    );
    assert_eq!(
        harness.target_states("incoming.txt").await,
        vec![("pinata-primary".to_owned(), "quota_waiting".to_owned())]
    );

    delete_block.release();
    harness.wait_for_worker_idle().await;
    harness.stop_worker_without_unlocking().await;

    assert_eq!(
        (
            remote_pin(&harness, "pinata-primary", "QmOld")
                .await
                .status
                .as_str(),
            remote_pin(&harness, "pinata-primary", "QmIncoming")
                .await
                .request_id
                .as_deref(),
        ),
        ("absent", Some("incoming-request"))
    );
    assert_eq!(
        harness.target_states("incoming.txt").await,
        vec![("pinata-primary".to_owned(), "pinned".to_owned())]
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 2),
        "the awakened incoming CID replaces exactly one released unique CID"
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 4);
    assert_submit_request(&requests[0], "/psa/pins", "QmOld");
    assert_submit_request(&requests[1], "/psa/pins", "QmNewer");
    assert_delete_request(&requests[2], "/psa/pins/old-request");
    assert_submit_request(&requests[3], "/psa/pins", "QmIncoming");
    assert_signed_body(&harness, "incoming.txt", b"in!").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_shared_cid_counts_once_and_blocks_unsafe_unpin() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = repeated_shared_kubo(2);
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "shared-request",
        "QmShared",
    )];
    let mut harness = start_pinning_harness(config).await;

    for key in ["first.txt", "second.txt"] {
        let put =
            signed_put_with_tagging(&harness, key, b"shared".to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(put.status(), StatusCode::OK, "PUT {key}");
        assert_put_cid_headers(&put, "QmShared");
    }
    harness.run_worker_until_idle().await;
    let first = latest_pinning_object(&harness, "first.txt").await;
    let second = latest_pinning_object(&harness, "second.txt").await;
    let first_lease = owner_leases(&harness, &first.id).await.remove(0);
    let second_lease = owner_leases(&harness, &second.id).await.remove(0);
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 1),
        "two S3 keys sharing one CID reserve the provider once"
    );

    let cancel = signed_delete_object_tagging(&harness, "first.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    assert_tagging(&harness, "first.txt", &[]).await;
    harness.run_worker_until_idle().await;

    assert_eq!(
        (
            owner_leases(&harness, &first.id).await[0].id.as_str(),
            owner_leases(&harness, &first.id).await[0].state.as_str(),
            lease_targets(&harness, &first_lease.id).await[0]
                .state
                .as_str(),
        ),
        (first_lease.id.as_str(), "cancelled", "released")
    );
    assert_eq!(
        (
            owner_leases(&harness, &second.id).await[0].id.as_str(),
            owner_leases(&harness, &second.id).await[0].state.as_str(),
            lease_targets(&harness, &second_lease.id).await[0]
                .state
                .as_str(),
        ),
        (second_lease.id.as_str(), "active", "pinned")
    );
    let remote = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        (remote.status.as_str(), remote.request_id.as_deref()),
        ("pinned", Some("shared-request"))
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 1)
    );
    let requests = harness.provider_requests().await;
    assert_eq!(
        requests.len(),
        1,
        "the remaining desired target blocks Unpin"
    );
    assert_submit_request(&requests[0], "/psa/pins", "QmShared");
    assert_signed_body(&harness, "first.txt", b"shared").await;
    assert_signed_body(&harness, "second.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_renewal_generation_wins_against_inflight_expiry_unpin() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "expiry-request", "QmTestCid"),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/expiry-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
        PsaReply::submit_status("/psa/pins", "renewed-request", "QmTestCid", "queued"),
        PsaReply::pin_status(
            "/psa/pins/renewed-request",
            "renewed-request",
            "QmTestCid",
            "pinned",
        ),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "expiry-renew.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let initial_submit = only_submit_job(&harness).await;
    harness.run_worker_until_idle().await;
    let object = latest_pinning_object(&harness, "expiry-renew.txt").await;
    let initial_lease = owner_leases(&harness, &object.id).await.remove(0);
    let initial_target = lease_targets(&harness, &initial_lease.id).await.remove(0);

    harness.advance_past_lease_expiry("expiry-renew.txt").await;
    let delete_block = harness.block_next_delete("pinata-primary").await;
    harness.restart_worker();
    delete_block.wait_until_blocked().await;
    let delete = harness
        .wait_for_provider_request(
            "pinata-primary",
            http::Method::DELETE,
            "/psa/pins/expiry-request",
            1,
        )
        .await;
    assert_delete_request(&delete, "/psa/pins/expiry-request");
    let expired_lease = owner_leases(&harness, &object.id).await.remove(0);
    let expired_remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (
            expired_lease.id.as_str(),
            expired_lease.state.as_str(),
            lease_targets(&harness, &initial_lease.id).await[0]
                .state
                .as_str(),
        ),
        (initial_lease.id.as_str(), "expired", "released")
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (4, 1),
        "the claimed DELETE cannot release before its provider response"
    );

    let retain_until = (Utc::now() + ChronoDuration::hours(1)).to_rfc3339();
    let renew = signed_put_object_tagging(
        &harness,
        "expiry-renew.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
        ],
    )
    .await;
    assert_eq!(renew.status(), StatusCode::OK);
    let renewed_lease = owner_leases(&harness, &object.id).await.remove(0);
    let renewed_target = lease_targets(&harness, &renewed_lease.id).await.remove(0);
    let renewed_remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (
            renewed_lease.id.as_str(),
            renewed_lease.state.as_str(),
            renewed_target.id.as_str(),
        ),
        (
            initial_lease.id.as_str(),
            "active",
            initial_target.id.as_str()
        )
    );
    assert!(renewed_lease.generation > expired_lease.generation);
    assert!(renewed_remote.epoch > expired_remote.epoch);
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (4, 1),
        "reactivation retains the original unique reservation"
    );

    delete_block.release();
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("expiry publishes one Unpin");
    harness.wait_for_job_state(&unpin.id, "done").await;
    harness.wait_for_worker_idle().await;
    harness.stop_worker_without_unlocking().await;

    let compensation = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "submit" && job.id != initial_submit.id)
        .expect("DELETE compensation publishes a replacement Submit");
    assert_eq!(compensation.state, "done");
    assert_eq!(
        compensation.target_id.as_deref(),
        Some(initial_target.id.as_str())
    );
    let poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("queued compensation Submit publishes a Poll");
    harness.advance_job_due(&poll.id).await;
    harness.run_worker_until_idle().await;

    let pinned = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (pinned.status.as_str(), pinned.request_id.as_deref()),
        ("pinned", Some("renewed-request"))
    );
    assert_eq!(
        lease_targets(&harness, &initial_lease.id).await[0].state,
        "pinned"
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 4);
    assert_submit_request_for_job(&requests[0], "/psa/pins", "QmTestCid", &initial_submit);
    assert_delete_request(&requests[1], "/psa/pins/expiry-request");
    assert_submit_request_for_job(&requests[2], "/psa/pins", "QmTestCid", &compensation);
    assert_eq!(
        (
            requests[3].method.clone(),
            requests[3].path.as_str(),
            requests[3].has_valid_authorization(),
        ),
        (http::Method::GET, "/psa/pins/renewed-request", true)
    );
    assert_signed_body(&harness, "expiry-renew.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_new_shared_target_during_delete_compensates_without_release() {
    let mut config = PinningHarnessConfig::request_one();
    config.providers[0].max_bytes = 6;
    config.providers[0].max_pins = 1;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmShared"),
            AddReply::Ok("QmWait"),
            AddReply::Ok("QmShared"),
        ],
        cat_bodies: HashMap::from([
            ("QmShared".to_owned(), b"shared".to_vec()),
            ("QmWait".to_owned(), b"wait".to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "shared-before-delete", "QmShared"),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/shared-before-delete",
            StatusCode::NOT_FOUND.as_u16(),
        ),
    ];
    let mut harness = start_pinning_harness(config).await;

    let first = signed_put_with_tagging(
        &harness,
        "delete-race-first.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;
    let initial_remote = remote_pin(&harness, "pinata-primary", "QmShared").await;

    let wait = signed_put_with_tagging(
        &harness,
        "quota-waiter.txt",
        b"wait".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(wait.status(), StatusCode::OK);
    assert_eq!(
        harness.target_states("quota-waiter.txt").await,
        vec![("pinata-primary".to_owned(), "quota_waiting".to_owned())]
    );

    let cancel = signed_delete_object_tagging(&harness, "delete-race-first.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    let delete_block = harness.block_next_delete("pinata-primary").await;
    harness.restart_worker();
    delete_block.wait_until_blocked().await;
    let delete = harness
        .wait_for_provider_request(
            "pinata-primary",
            http::Method::DELETE,
            "/psa/pins/shared-before-delete",
            1,
        )
        .await;
    assert_delete_request(&delete, "/psa/pins/shared-before-delete");

    let shared_new = signed_put_with_tagging(
        &harness,
        "delete-race-new.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(shared_new.status(), StatusCode::OK);
    assert_put_cid_headers(&shared_new, "QmShared");
    let before_delete_completion = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(before_delete_completion.epoch, initial_remote.epoch + 2);

    delete_block.release();
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("cancelled original target publishes one Unpin");
    harness.wait_for_job_state(&unpin.id, "done").await;
    harness.stop_worker_without_unlocking().await;

    let compensated = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        (
            compensated.status.as_str(),
            compensated.request_id.as_deref(),
            compensated.epoch,
        ),
        ("reserved", None, initial_remote.epoch + 2)
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 1),
        "NotFound DELETE compensates the shared reservation rather than releasing it"
    );
    assert_eq!(
        harness.target_states("quota-waiter.txt").await,
        vec![("pinata-primary".to_owned(), "quota_waiting".to_owned())],
        "the retained reservation must not wake an unrelated quota waiter"
    );
    let new_object = latest_pinning_object(&harness, "delete-race-new.txt").await;
    let new_lease = owner_leases(&harness, &new_object.id).await.remove(0);
    let new_target = lease_targets(&harness, &new_lease.id).await.remove(0);
    let replacement_submit = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| {
            job.operation == "submit" && job.target_id.as_deref() == Some(new_target.id.as_str())
        })
        .expect("current shared target receives a compensation Submit");
    assert_eq!(replacement_submit.state, "pending");
    assert_eq!(new_target.state, "waiting");
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 2, "the replacement Submit remains pending");
    assert_submit_request(&requests[0], "/psa/pins", "QmShared");
    assert_delete_request(&requests[1], "/psa/pins/shared-before-delete");
    assert_signed_body(&harness, "delete-race-new.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_expiry_removes_remote_pin_but_preserves_s3_and_kubo_pin() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "expiry-delete-request", "QmTestCid"),
        PsaReply::empty(
            http::Method::DELETE,
            "/psa/pins/expiry-delete-request",
            StatusCode::NO_CONTENT.as_u16(),
        ),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "expired.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, "QmTestCid");
    harness.run_worker_until_idle().await;
    let object = latest_pinning_object(&harness, "expired.txt").await;
    let manual = owner_leases(&harness, &object.id).await.remove(0);
    let target = lease_targets(&harness, &manual.id).await.remove(0);

    harness.advance_past_lease_expiry("expired.txt").await;
    let delete_block = harness.block_next_delete("pinata-primary").await;
    harness.restart_worker();
    delete_block.wait_until_blocked().await;
    delete_block.release();
    harness.wait_for_worker_idle().await;
    harness.stop_worker_without_unlocking().await;

    let expired = owner_leases(&harness, &object.id).await.remove(0);
    let released_target = lease_targets(&harness, &manual.id).await.remove(0);
    let remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (
            expired.id.as_str(),
            expired.state.as_str(),
            expired.generation,
            released_target.id.as_str(),
            released_target.state.as_str(),
        ),
        (
            manual.id.as_str(),
            "expired",
            manual.generation + 1,
            target.id.as_str(),
            "released",
        )
    );
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch
        ),
        ("absent", None, 2)
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (0, 0)
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 2);
    assert_submit_request(&requests[0], "/psa/pins", "QmTestCid");
    assert_delete_request(&requests[1], "/psa/pins/expiry-delete-request");
    assert_signed_body(&harness, "expired.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_decompressed_pins_entries_not_archive() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmEntry1"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive_bytes.clone()),
            ("QmEntry1".to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-1", "QmEntry1"),
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-2", "QmEntry2"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "entries/",
        archive_bytes.clone(),
        "team=zip&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let put_xml = put.text().await.expect("decompress ZIP result XML");
    assert_eq!(
        xml_text(&put_xml, "ArchiveKey").as_deref(),
        Some("archive.zip")
    );
    assert_eq!(
        xml_text(&put_xml, "ArchiveETag").as_deref(),
        Some("QmArchive")
    );
    assert_eq!(xml_text(&put_xml, "ExtractedCount").as_deref(), Some("2"));
    assert_tagging(
        &harness,
        "archive.zip",
        &[
            ("ipfs-s3:content", "decompressed"),
            ("ipfs-s3:pin", "true"),
            ("team", "zip"),
        ],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive_bytes).await;
    assert_signed_body(&harness, "entries/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "entries/second.txt", SECOND_ENTRY_BYTES).await;

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    let first = latest_pinning_object(&harness, "entries/first.txt").await;
    let second = latest_pinning_object(&harness, "entries/second.txt").await;
    assert!(owner_leases(&harness, &first.id).await.is_empty());
    assert!(owner_leases(&harness, &second.id).await.is_empty());

    let leases = owner_leases(&harness, &archive.id).await;
    assert_eq!(leases.len(), 1);
    let manual = leases.into_iter().next().expect("manual archive lease");
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.content_mode.as_str(),
            manual.provider_mode.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (
            archive.id.as_str(),
            "manual",
            "decompressed",
            "one",
            "active",
            1
        )
    );
    let targets = lease_targets(&harness, &manual.id).await;
    let target_cids = targets
        .iter()
        .map(|target| target.cid.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        target_cids,
        BTreeSet::from(["QmEntry1".to_owned(), "QmEntry2".to_owned()])
    );
    assert!(!target_cids.contains("QmArchive"));
    assert!(targets.iter().all(|target| {
        target.provider == "pinata-primary"
            && target.state == "waiting"
            && target.lease_id == manual.id
    }));
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(
            "pinata-primary".to_owned(),
            (FIRST_ENTRY_BYTES.len() + SECOND_ENTRY_BYTES.len()) as i64,
            2,
        )]
    );
    let jobs = harness.pin_jobs().await;
    assert_eq!(jobs.len(), 2);
    assert_eq!(
        jobs.iter()
            .map(|job| {
                (
                    job.operation.as_str(),
                    job.provider.as_str(),
                    job.cid.as_str(),
                    job.lease_id.as_deref(),
                    job.expected_generation,
                    job.expected_remote_epoch,
                    job.state.as_str(),
                )
            })
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (
                "submit",
                "pinata-primary",
                "QmEntry1",
                Some(manual.id.as_str()),
                Some(1),
                None,
                "pending",
            ),
            (
                "submit",
                "pinata-primary",
                "QmEntry2",
                Some(manual.id.as_str()),
                Some(1),
                None,
                "pending",
            ),
        ])
    );
    assert_eq!(
        jobs.iter()
            .map(|job| job.target_id.as_deref())
            .collect::<BTreeSet<_>>(),
        targets
            .iter()
            .map(|target| Some(target.id.as_str()))
            .collect::<BTreeSet<_>>()
    );

    harness.run_worker_until_idle().await;
    let requests = harness.pinata_requests().await;
    assert_eq!(requests.len(), 2);
    let submitted_cids = requests
        .iter()
        .map(|request| {
            serde_json::from_slice::<serde_json::Value>(&request.body)
                .expect("PSA submit JSON")
                .get("cid")
                .and_then(serde_json::Value::as_str)
                .expect("PSA submit CID")
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(submitted_cids, target_cids);
    for job in &jobs {
        let request = requests
            .iter()
            .find(|request| {
                request
                    .body
                    .windows(job.cid.len())
                    .any(|window| window == job.cid.as_bytes())
            })
            .expect("PSA request for submitted entry");
        assert_submit_request_for_job(request, "/psa/pins", &job.cid, job);
        assert_eq!(harness.pin_job(&job.id).await.state, "done");
    }
    for (cid, request_id) in [
        ("QmEntry1", "pinata-entry-1"),
        ("QmEntry2", "pinata-entry-2"),
    ] {
        let remote = remote_pin(&harness, "pinata-primary", cid).await;
        assert_eq!(
            (
                remote.status.as_str(),
                remote.request_id.as_deref(),
                remote.epoch
            ),
            ("pinned", Some(request_id), 1)
        );
    }
    assert!(
        lease_targets(&harness, &manual.id)
            .await
            .iter()
            .all(|target| target.state == "pinned")
    );
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_partial_success_targets_only_published_entries() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "entry add failed"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive_bytes.clone()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-entry-2",
        "QmEntry2",
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "entries/",
        archive_bytes.clone(),
        "team=partial&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let put_xml = put.text().await.expect("partial decompression XML");
    assert_eq!(xml_text(&put_xml, "ExtractedCount").as_deref(), Some("1"));
    assert_eq!(xml_text(&put_xml, "FailedCount").as_deref(), Some("1"));
    assert!(put_xml.contains("EntryUploadFailed"));
    assert_signed_body(&harness, "archive.zip", &archive_bytes).await;
    assert_s3_error(
        signed_get(&harness, "entries/first.txt").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    assert_signed_body(&harness, "entries/second.txt", SECOND_ENTRY_BYTES).await;

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    let published = latest_pinning_object(&harness, "entries/second.txt").await;
    assert!(owner_leases(&harness, &published.id).await.is_empty());
    let manual = owner_leases(&harness, &archive.id)
        .await
        .into_iter()
        .next()
        .expect("manual archive lease");
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.content_mode.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (archive.id.as_str(), "manual", "decompressed", "active", 1)
    );
    let targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(
        targets
            .iter()
            .map(|target| (
                target.provider.as_str(),
                target.cid.as_str(),
                target.state.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![("pinata-primary", "QmEntry2", "waiting")]
    );
    assert_ne!(targets[0].cid, archive.cid);
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(
            "pinata-primary".to_owned(),
            SECOND_ENTRY_BYTES.len() as i64,
            1
        )]
    );
    let jobs = harness.pin_jobs().await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        (
            jobs[0].operation.as_str(),
            jobs[0].provider.as_str(),
            jobs[0].cid.as_str(),
            jobs[0].lease_id.as_deref(),
            jobs[0].target_id.as_deref(),
            jobs[0].expected_generation,
            jobs[0].expected_remote_epoch,
            jobs[0].state.as_str(),
        ),
        (
            "submit",
            "pinata-primary",
            "QmEntry2",
            Some(manual.id.as_str()),
            Some(targets[0].id.as_str()),
            Some(1),
            None,
            "pending",
        )
    );

    harness.run_worker_until_idle().await;
    let requests = harness.pinata_requests().await;
    assert_eq!(requests.len(), 1);
    assert_submit_request_for_job(&requests[0], "/psa/pins", "QmEntry2", &jobs[0]);
    let remote = remote_pin(&harness, "pinata-primary", "QmEntry2").await;
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch
        ),
        ("pinned", Some("pinata-entry-2"), 1)
    );
    assert!(
        harness
            .remote_pins()
            .await
            .iter()
            .all(|remote| remote.cid != "QmArchive")
    );
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_archive_renewal_updates_all_entry_targets() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmEntry1"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive_bytes.clone()),
            ("QmEntry1".to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-1", "QmEntry1"),
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-2", "QmEntry2"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "entries/",
        archive_bytes.clone(),
        "team=initial&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_signed_body(&harness, "archive.zip", &archive_bytes).await;
    harness.run_worker_until_idle().await;

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    let manual_before = owner_leases(&harness, &archive.id)
        .await
        .into_iter()
        .next()
        .expect("manual archive lease");
    let targets_before = lease_targets(&harness, &manual_before.id).await;
    assert_eq!(targets_before.len(), 2);
    assert!(targets_before.iter().all(|target| target.state == "pinned"));
    let target_ids = targets_before
        .iter()
        .map(|target| target.id.clone())
        .collect::<BTreeSet<_>>();
    let target_cids = targets_before
        .iter()
        .map(|target| target.cid.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        target_cids,
        BTreeSet::from(["QmEntry1".to_owned(), "QmEntry2".to_owned()])
    );
    let retain_until = (manual_before.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let renewal = signed_put_object_tagging(
        &harness,
        "archive.zip",
        &[
            ("team", "renewed"),
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
        ],
    )
    .await;
    assert_eq!(renewal.status(), StatusCode::OK);
    assert_tagging(
        &harness,
        "archive.zip",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
            ("team", "renewed"),
        ],
    )
    .await;

    let manual_after = owner_leases(&harness, &archive.id)
        .await
        .into_iter()
        .next()
        .expect("renewed manual archive lease");
    let expected_expiry = chrono::DateTime::parse_from_rfc3339(&retain_until)
        .expect("retain-until RFC3339")
        .with_timezone(&Utc);
    assert_eq!(
        (
            manual_after.id.as_str(),
            manual_after.owner_object_id.as_str(),
            manual_after.source.as_str(),
            manual_after.content_mode.as_str(),
            manual_after.state.as_str(),
            manual_after.generation,
            manual_after.expires_at,
        ),
        (
            manual_before.id.as_str(),
            archive.id.as_str(),
            "manual",
            "decompressed",
            "active",
            2,
            expected_expiry,
        )
    );
    let targets_after = lease_targets(&harness, &manual_after.id).await;
    assert_eq!(
        targets_after
            .iter()
            .map(|target| target.id.clone())
            .collect::<BTreeSet<_>>(),
        target_ids
    );
    assert_eq!(
        targets_after
            .iter()
            .map(|target| target.cid.clone())
            .collect::<BTreeSet<_>>(),
        target_cids
    );
    assert!(targets_after.iter().all(|target| {
        target.lease_id == manual_after.id
            && target.state == "pinned"
            && manual_after.generation == 2
            && manual_after.expires_at == expected_expiry
    }));
    let renewal_jobs = harness
        .pin_jobs()
        .await
        .into_iter()
        .filter(|job| job.operation == "reconcile")
        .collect::<Vec<_>>();
    assert_eq!(renewal_jobs.len(), 2);
    assert_eq!(
        renewal_jobs
            .iter()
            .map(|job| {
                (
                    job.provider.as_str(),
                    job.cid.as_str(),
                    job.lease_id.as_deref(),
                    job.target_id.as_deref(),
                    job.expected_generation,
                    job.expected_remote_epoch,
                    job.state.as_str(),
                )
            })
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (
                "pinata-primary",
                "QmEntry1",
                None,
                None,
                None,
                Some(2),
                "pending",
            ),
            (
                "pinata-primary",
                "QmEntry2",
                None,
                None,
                None,
                Some(2),
                "pending",
            ),
        ])
    );
    let requests = harness.pinata_requests().await;
    assert_eq!(requests.len(), 2, "renewal must not re-submit entry CIDs");
    assert_eq!(
        requests
            .iter()
            .map(|request| {
                serde_json::from_slice::<serde_json::Value>(&request.body)
                    .expect("PSA submit JSON")
                    .get("cid")
                    .and_then(serde_json::Value::as_str)
                    .expect("PSA submit CID")
                    .to_owned()
            })
            .collect::<BTreeSet<_>>(),
        target_cids
    );
    assert_signed_body(&harness, "entries/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "entries/second.txt", SECOND_ENTRY_BYTES).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_global_reject_creates_no_manual_lease() {
    let archive_bytes = archive_key_collision_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmCollisionEntry")],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive_bytes.clone())]),
    };
    config.pinata_script.clear();
    let harness = start_pinning_harness(config).await;

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "",
        archive_bytes,
        "team=reject&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_s3_error(
        put,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "zip entry collides with archive key: archive.zip",
    )
    .await;
    assert_s3_error(
        signed_get(&harness, "archive.zip").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    let db = harness.state.store.db();
    assert!(
        store::entities::object::Entity::find()
            .all(db)
            .await
            .expect("object rows after global reject")
            .is_empty()
    );
    assert!(
        store::entities::object_tag::Entity::find()
            .all(db)
            .await
            .expect("tag rows after global reject")
            .is_empty()
    );
    assert!(harness.pin_leases().await.is_empty());
    assert!(harness.pin_targets().await.is_empty());
    assert!(harness.remote_pins().await.is_empty());
    assert!(harness.provider_usages().await.is_empty());
    assert!(harness.pin_jobs().await.is_empty());
    assert!(harness.provider_requests().await.is_empty());
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_multipart_zip_complete_commits_entries_and_upload_delete_atomically() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmRoot"),
            AddReply::Ok("QmEntry1"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive_bytes.clone()),
            ("QmRoot".to_owned(), archive_bytes.clone()),
            ("QmEntry1".to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-1", "QmEntry1"),
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-2", "QmEntry2"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let create = signed_create_multipart_zip_upload_with_tagging(
        &harness,
        "archive.zip",
        "entries/",
        "team=multipart&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let create_xml = create.text().await.expect("CreateMultipartUpload XML");
    assert_eq!(xml_text(&create_xml, "Bucket").as_deref(), Some("test-bkt"));
    assert_eq!(xml_text(&create_xml, "Key").as_deref(), Some("archive.zip"));
    let upload_id = xml_text(&create_xml, "UploadId").expect("CreateMultipartUpload UploadId");
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("multipart upload before complete");
    assert!(harness.pin_leases().await.is_empty());
    assert!(harness.pin_jobs().await.is_empty());

    let part = signed_upload_part(
        &harness,
        "archive.zip",
        &upload_id,
        1,
        archive_bytes.clone(),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let part_etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    assert_eq!(part_etag, "QmPart");
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("multipart part before complete");
    assert_eq!(parts_before.len(), 1);

    let complete =
        signed_complete_multipart(&harness, "archive.zip", &upload_id, 1, &part_etag).await;
    assert_eq!(complete.status(), StatusCode::OK);
    let complete_xml = complete.text().await.expect("CompleteMultipartUpload XML");
    assert_eq!(
        xml_text(&complete_xml, "ArchiveKey").as_deref(),
        Some("archive.zip")
    );
    assert_eq!(
        xml_text(&complete_xml, "ArchiveETag").as_deref(),
        Some("QmRoot")
    );
    assert_eq!(
        xml_text(&complete_xml, "ExtractedCount").as_deref(),
        Some("2")
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after completed ZIP publication")
            .is_empty()
    );

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    assert_eq!(archive.cid, "QmRoot");
    assert!(archive.multipart);
    assert_ne!(archive.id, upload_before.object_id);
    uuid::Uuid::parse_str(&archive.id).expect("completion_attempt_id is a UUID");
    assert_tagging(
        &harness,
        "archive.zip",
        &[
            ("ipfs-s3:content", "decompressed"),
            ("ipfs-s3:pin", "true"),
            ("team", "multipart"),
        ],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive_bytes).await;
    assert_signed_body(&harness, "entries/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "entries/second.txt", SECOND_ENTRY_BYTES).await;
    let first = latest_pinning_object(&harness, "entries/first.txt").await;
    let second = latest_pinning_object(&harness, "entries/second.txt").await;
    assert!(owner_leases(&harness, &first.id).await.is_empty());
    assert!(owner_leases(&harness, &second.id).await.is_empty());

    let manual = owner_leases(&harness, &archive.id)
        .await
        .into_iter()
        .next()
        .expect("manual decompressed archive lease");
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.content_mode.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (archive.id.as_str(), "manual", "decompressed", "active", 1)
    );
    let targets = lease_targets(&harness, &manual.id).await;
    let target_cids = targets
        .iter()
        .map(|target| target.cid.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        target_cids,
        BTreeSet::from(["QmEntry1".to_owned(), "QmEntry2".to_owned()])
    );
    assert!(!target_cids.contains("QmRoot"));
    assert!(targets.iter().all(|target| {
        target.provider == "pinata-primary"
            && target.state == "waiting"
            && target.lease_id == manual.id
    }));
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(
            "pinata-primary".to_owned(),
            (FIRST_ENTRY_BYTES.len() + SECOND_ENTRY_BYTES.len()) as i64,
            2,
        )]
    );
    let jobs = harness.pin_jobs().await;
    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| {
        job.operation == "submit"
            && job.provider == "pinata-primary"
            && job.lease_id.as_deref() == Some(manual.id.as_str())
            && job.expected_generation == Some(1)
            && job.expected_remote_epoch.is_none()
            && job.state == "pending"
            && job
                .target_id
                .as_ref()
                .is_some_and(|id| targets.iter().any(|target| &target.id == id))
    }));

    harness.run_worker_until_idle().await;
    let requests = harness.pinata_requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests
            .iter()
            .map(|request| {
                serde_json::from_slice::<serde_json::Value>(&request.body)
                    .expect("PSA submit JSON")
                    .get("cid")
                    .and_then(serde_json::Value::as_str)
                    .expect("PSA submit CID")
                    .to_owned()
            })
            .collect::<BTreeSet<_>>(),
        target_cids
    );
    for job in &jobs {
        let request = requests
            .iter()
            .find(|request| {
                request
                    .body
                    .windows(job.cid.len())
                    .any(|window| window == job.cid.as_bytes())
            })
            .expect("PSA request for completed ZIP entry");
        assert_submit_request_for_job(request, "/psa/pins", &job.cid, job);
        assert_eq!(harness.pin_job(&job.id).await.state, "done");
    }
    for (cid, request_id) in [
        ("QmEntry1", "pinata-entry-1"),
        ("QmEntry2", "pinata-entry-2"),
    ] {
        let remote = remote_pin(&harness, "pinata-primary", cid).await;
        assert_eq!(
            (
                remote.status.as_str(),
                remote.request_id.as_deref(),
                remote.epoch
            ),
            ("pinned", Some(request_id), 1)
        );
    }
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_multipart_zip_outbox_failure_preserves_upload_and_parts() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmRoot"),
            AddReply::Ok("QmEntry1"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive_bytes.clone()),
            ("QmRoot".to_owned(), archive_bytes.clone()),
            ("QmEntry1".to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script.clear();
    let harness = start_pinning_harness(config).await;

    let create = signed_create_multipart_zip_upload_with_tagging(
        &harness,
        "archive.zip",
        "entries/",
        "team=rollback&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let create_xml = create.text().await.expect("CreateMultipartUpload XML");
    let upload_id = xml_text(&create_xml, "UploadId").expect("CreateMultipartUpload UploadId");
    let part = signed_upload_part(&harness, "archive.zip", &upload_id, 1, archive_bytes).await;
    assert_eq!(part.status(), StatusCode::OK);
    let part_etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload before forced outbox failure");
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("parts before forced outbox failure");
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_zip_outbox BEFORE INSERT ON pin_jobs \
             BEGIN SELECT RAISE(FAIL, 'forced zip outbox failure'); END;",
        ))
        .await
        .expect("install ZIP outbox failure trigger");

    let complete =
        signed_complete_multipart(&harness, "archive.zip", &upload_id, 1, &part_etag).await;
    assert_eq!(complete.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let complete_xml = complete
        .text()
        .await
        .expect("failed CompleteMultipartUpload XML");
    assert!(complete_xml.contains("<Code>InternalError</Code>"));
    assert!(!complete_xml.contains("forced zip outbox failure"));
    assert_eq!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("upload preserved after rollback"),
        upload_before
    );
    assert_eq!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts preserved after rollback"),
        parts_before
    );
    assert_s3_error(
        signed_get(&harness, "archive.zip").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    for key in ["entries/first.txt", "entries/second.txt"] {
        assert!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .is_err(),
            "{key} must not be published after rollback"
        );
    }
    let db = harness.state.store.db();
    assert!(
        store::entities::object::Entity::find()
            .all(db)
            .await
            .expect("object rows after outbox rollback")
            .is_empty()
    );
    assert!(
        store::entities::object_tag::Entity::find()
            .all(db)
            .await
            .expect("tag rows after outbox rollback")
            .is_empty()
    );
    assert!(harness.pin_leases().await.is_empty());
    assert!(harness.pin_targets().await.is_empty());
    assert!(harness.remote_pins().await.is_empty());
    assert!(harness.provider_usages().await.is_empty());
    assert!(harness.pin_jobs().await.is_empty());
    assert!(harness.provider_requests().await.is_empty());
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
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
async fn test_put_decompress_zip_db_publish_failure_rolls_back_atomically_and_keeps_pins() {
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
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.text().await.expect("S3 error response body");
    assert!(
        body.contains("<Code>InternalError</Code>"),
        "expected an S3 InternalError response"
    );
    assert!(
        !body.contains("forced entry publish failure"),
        "S3 database errors must not expose internal database details"
    );

    for key in ["archive.zip", "first.txt", "second.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    let db = harness.state.store.db();
    assert!(
        store::entities::object::Entity::find()
            .all(db)
            .await
            .expect("load object rows after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no object rows"
    );
    assert!(
        store::entities::object_tag::Entity::find()
            .all(db)
            .await
            .expect("load object tags after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no object tags"
    );
    assert!(
        store::entities::pin_lease::Entity::find()
            .all(db)
            .await
            .expect("load pin leases after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no pin leases"
    );
    assert!(
        store::entities::pin_lease_target::Entity::find()
            .all(db)
            .await
            .expect("load pin targets after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no pin targets"
    );
    assert!(
        store::entities::remote_pin::Entity::find()
            .all(db)
            .await
            .expect("load remote pins after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no remote pins"
    );
    assert!(
        store::entities::pin_provider_usage::Entity::find()
            .all(db)
            .await
            .expect("load provider usage after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no provider usage"
    );
    assert!(
        store::entities::pin_job::Entity::find()
            .all(db)
            .await
            .expect("load pin jobs after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no pin jobs"
    );
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
