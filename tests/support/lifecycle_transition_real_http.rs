//! Signed HTTP and SDK primitives for the real lifecycle-transition suite.

use std::{future::Future, time::Duration};

use base64::Engine;
use http::{HeaderMap, HeaderValue, StatusCode, header};
use s3::{
    bucket::Bucket, bucket_ops::BucketConfiguration, creds::Credentials, error::S3Error,
    region::Region,
};

use crate::lifecycle_transition_real_sigv4::send_sigv4;

pub const HTTP_TIMEOUT: Duration = Duration::from_secs(20);
pub const S3_TIMEOUT: Duration = Duration::from_secs(30);

pub fn endpoint_from_env(name: &str) -> String {
    let endpoint = std::env::var(name)
        .unwrap_or_else(|_| panic!("NOT RUN: {name} is required for real transition tests"));
    let endpoint = endpoint.trim().trim_end_matches('/').to_owned();
    let parsed = url::Url::parse(&endpoint)
        .unwrap_or_else(|_| panic!("{name} must be an absolute HTTP(S) URL"));
    assert!(
        matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some(),
        "{name} must be an absolute HTTP(S) URL"
    );
    assert!(
        parsed.path().is_empty() || parsed.path() == "/",
        "{name} must not contain a path"
    );
    assert!(
        parsed.query().is_none() && parsed.fragment().is_none(),
        "{name} must not contain a query or fragment"
    );
    endpoint
}

pub async fn bounded_http<T, F>(label: &str, future: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(HTTP_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("HTTP operation timed out: {label}"))
}

pub async fn bounded_s3<T, F>(label: &str, future: F) -> Result<T, S3Error>
where
    F: Future<Output = Result<T, S3Error>>,
{
    tokio::time::timeout(S3_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("SDK operation timed out: {label}"))
}

fn credentials() -> Credentials {
    Credentials::new(Some("test"), Some("test"), None, None, None)
        .expect("construct transition test credentials")
}

fn region(endpoint: &str) -> Region {
    Region::Custom {
        region: "us-east-1".to_owned(),
        endpoint: endpoint.to_owned(),
    }
}

pub fn sdk_bucket(endpoint: &str, bucket: &str) -> Box<Bucket> {
    Bucket::new(bucket, region(endpoint), credentials())
        .expect("construct transition SDK bucket")
        .with_path_style()
}

pub async fn sdk_create_bucket(endpoint: &str, bucket: &str) -> Box<Bucket> {
    let response = bounded_s3(
        "create transition bucket",
        Bucket::create_with_path_style(
            bucket,
            region(endpoint),
            credentials(),
            BucketConfiguration::default(),
        ),
    )
    .await
    .expect("SDK CreateBucket against real gateway must succeed");
    assert_eq!(response.response_code, 200, "SDK CreateBucket status");
    sdk_bucket(endpoint, bucket)
}

pub async fn sdk_assert_get(endpoint: &str, bucket: &str, key: &str, expected: &[u8]) {
    let response = bounded_s3(
        "GET transitioned object through rust-s3 SDK",
        sdk_bucket(endpoint, bucket).get_object(key),
    )
    .await
    .expect("SDK GET of transitioned object must succeed");
    assert_eq!(response.status_code(), 200, "SDK GET status");
    assert_eq!(response.bytes().as_ref(), expected, "SDK GET bytes");
}

pub async fn sdk_delete_bucket(endpoint: &str, bucket: &str) {
    let status = bounded_s3(
        "delete transition bucket",
        sdk_bucket(endpoint, bucket).delete(),
    )
    .await
    .expect("SDK DeleteBucket against real gateway must succeed");
    assert_eq!(status, 204, "SDK DeleteBucket status");
}

pub async fn signed_request(
    method: reqwest::Method,
    endpoint: &str,
    bucket: &str,
    key: &str,
    query: &[(&str, &str)],
    body: Vec<u8>,
    headers: HeaderMap,
) -> reqwest::Response {
    bounded_http(
        "signed transition request",
        send_sigv4(method, endpoint, bucket, key, query, body, headers, "test"),
    )
    .await
}

pub async fn put_versioning(endpoint: &str, bucket: &str, status: &str) {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let response = signed_request(
        reqwest::Method::PUT,
        endpoint,
        bucket,
        "",
        &[("versioning", "")],
        format!(
            "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>{status}</Status></VersioningConfiguration>"
        )
        .into_bytes(),
        headers,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "PUT versioning {status}");
}

pub async fn put_lifecycle(endpoint: &str, bucket: &str, xml: String) {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let response = signed_request(
        reqwest::Method::PUT,
        endpoint,
        bucket,
        "",
        &[("lifecycle", "")],
        xml.into_bytes(),
        headers,
    )
    .await;
    let status = response.status();
    let body = response.text().await.expect("read lifecycle PUT response");
    assert_eq!(status, StatusCode::OK, "PUT lifecycle response: {body}");
}

pub async fn assert_lifecycle(endpoint: &str, bucket: &str, fragments: &[&str]) {
    let response = signed_request(
        reqwest::Method::GET,
        endpoint,
        bucket,
        "",
        &[("lifecycle", "")],
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "GET lifecycle status");
    let body = response.text().await.expect("read lifecycle XML");
    for fragment in fragments {
        assert!(
            body.contains(fragment),
            "lifecycle XML omitted {fragment}: {body}"
        );
    }
}

pub async fn delete_lifecycle(endpoint: &str, bucket: &str) {
    let response = signed_request(
        reqwest::Method::DELETE,
        endpoint,
        bucket,
        "",
        &[("lifecycle", "")],
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "DELETE lifecycle status"
    );
}

pub async fn put_object(
    endpoint: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
    headers: HeaderMap,
) -> reqwest::Response {
    signed_request(
        reqwest::Method::PUT,
        endpoint,
        bucket,
        key,
        &[],
        body.to_vec(),
        headers,
    )
    .await
}

pub async fn get_object(
    endpoint: &str,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    headers: HeaderMap,
) -> reqwest::Response {
    let query = version_id.map_or_else(Vec::new, |id| vec![("versionId", id)]);
    signed_request(
        reqwest::Method::GET,
        endpoint,
        bucket,
        key,
        &query,
        Vec::new(),
        headers,
    )
    .await
}

pub async fn head_object(
    endpoint: &str,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    headers: HeaderMap,
) -> reqwest::Response {
    let query = version_id.map_or_else(Vec::new, |id| vec![("versionId", id)]);
    signed_request(
        reqwest::Method::HEAD,
        endpoint,
        bucket,
        key,
        &query,
        Vec::new(),
        headers,
    )
    .await
}

pub async fn list_objects(endpoint: &str, bucket: &str, prefix: &str, v2: bool) -> String {
    let query = if v2 {
        vec![("list-type", "2"), ("prefix", prefix)]
    } else {
        vec![("prefix", prefix)]
    };
    let response = signed_request(
        reqwest::Method::GET,
        endpoint,
        bucket,
        "",
        &query,
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "ListObjects status");
    response.text().await.expect("read ListObjects XML")
}

pub async fn list_versions(endpoint: &str, bucket: &str, prefix: &str) -> String {
    let response = signed_request(
        reqwest::Method::GET,
        endpoint,
        bucket,
        "",
        &[("versions", ""), ("prefix", prefix)],
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "ListObjectVersions status"
    );
    response.text().await.expect("read ListObjectVersions XML")
}

pub async fn copy_object(
    endpoint: &str,
    bucket: &str,
    source_key: &str,
    destination_key: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-copy-source",
        HeaderValue::from_str(&format!("/{bucket}/{source_key}"))
            .expect("copy source is a valid header"),
    );
    put_object(endpoint, bucket, destination_key, &[], headers).await
}

pub async fn put_object_tagging(
    endpoint: &str,
    bucket: &str,
    key: &str,
    version_id: &str,
    tag_key: &str,
    tag_value: &str,
) {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let body = format!(
        "<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet><Tag><Key>{tag_key}</Key><Value>{tag_value}</Value></Tag></TagSet></Tagging>"
    );
    let response = signed_request(
        reqwest::Method::PUT,
        endpoint,
        bucket,
        key,
        &[("tagging", ""), ("versionId", version_id)],
        body.into_bytes(),
        headers,
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "PUT object tagging status"
    );
    assert_eq!(
        response.headers()["x-amz-version-id"],
        version_id,
        "PUT object tagging version"
    );
}

pub async fn assert_object_tagging(
    endpoint: &str,
    bucket: &str,
    key: &str,
    version_id: &str,
    tag_key: &str,
    tag_value: &str,
) {
    let response = signed_request(
        reqwest::Method::GET,
        endpoint,
        bucket,
        key,
        &[("tagging", ""), ("versionId", version_id)],
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "GET object tagging status"
    );
    assert_eq!(
        response.headers()["x-amz-version-id"],
        version_id,
        "GET object tagging version"
    );
    let body = response.text().await.expect("read object tagging XML");
    assert!(body.contains(&format!("<Key>{tag_key}</Key>")), "{body}");
    assert!(
        body.contains(&format!("<Value>{tag_value}</Value>")),
        "{body}"
    );
}

pub async fn submit_cid_import(endpoint: &str, bucket: &str, key: &str, cid: &str) -> String {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    headers.insert(
        "x-amz-meta-transition-suite",
        HeaderValue::from_static("nondefault-import"),
    );
    let body = format!(
        "<IPFS3ImportRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><CID>{cid}</CID></IPFS3ImportRequest>"
    );
    let response = signed_request(
        reqwest::Method::POST,
        endpoint,
        bucket,
        key,
        &[("ipfs3-import", "")],
        body.into_bytes(),
        headers,
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::ACCEPTED,
        "signed CID import route must be enabled and accept the request"
    );
    let job_id = response
        .headers()
        .get("x-ipfs3-import-job-id")
        .expect("accepted import omitted job ID")
        .to_str()
        .expect("import job ID is ASCII")
        .to_owned();
    let body = response.text().await.expect("read import accepted XML");
    assert!(body.contains("<IPFS3ImportAccepted>"));
    assert!(body.contains("<State>queued</State>"));
    assert!(body.contains("<Phase>queued</Phase>"));
    job_id
}

pub async fn wait_for_cid_import(
    endpoint: &str,
    bucket: &str,
    key: &str,
    job_id: &str,
    cid: &str,
    expected_size: usize,
) {
    let started = std::time::Instant::now();
    loop {
        let response = signed_request(
            reqwest::Method::GET,
            endpoint,
            bucket,
            key,
            &[("ipfs3-import", job_id)],
            Vec::new(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "signed import status GET"
        );
        let body = response.text().await.expect("read import status XML");
        assert!(body.contains("<IPFS3ImportStatus>"));
        assert!(
            !body.contains("<State>failed</State>"),
            "real CID import entered failed state"
        );
        if body.contains("<State>completed</State>") {
            assert!(body.contains("<Artifact>"));
            assert!(body.contains(&format!("<CID>{cid}</CID>")));
            assert!(body.contains(&format!("<Size>{expected_size}</Size>")));
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "real CID import did not complete within 120 seconds"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub async fn create_multipart_upload(endpoint: &str, bucket: &str, key: &str) -> String {
    let response = signed_request(
        reqwest::Method::POST,
        endpoint,
        bucket,
        key,
        &[("uploads", "")],
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "CreateMultipartUpload");
    let body = response
        .text()
        .await
        .expect("read CreateMultipartUpload XML");
    required_xml_element(&body, "UploadId")
}

pub async fn upload_part(
    endpoint: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: u32,
    body: Vec<u8>,
) -> String {
    let part_number = part_number.to_string();
    let response = signed_request(
        reqwest::Method::PUT,
        endpoint,
        bucket,
        key,
        &[("partNumber", &part_number), ("uploadId", upload_id)],
        body,
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "UploadPart");
    response_etag(&response, "UploadPart")
}

pub async fn complete_multipart_upload(
    endpoint: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[(u32, String)],
) -> String {
    let mut body = String::from("<CompleteMultipartUpload>");
    for (part_number, etag) in parts {
        body.push_str(&format!(
            "<Part><PartNumber>{part_number}</PartNumber><ETag>\"{}\"</ETag></Part>",
            quick_xml::escape::escape(etag)
        ));
    }
    body.push_str("</CompleteMultipartUpload>");
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let response = signed_request(
        reqwest::Method::POST,
        endpoint,
        bucket,
        key,
        &[("uploadId", upload_id)],
        body.into_bytes(),
        headers,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "CompleteMultipartUpload");
    let body = response
        .text()
        .await
        .expect("read CompleteMultipartUpload XML");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    required_xml_element(&body, "ETag")
        .trim_matches('"')
        .to_owned()
}

pub async fn delete_version(endpoint: &str, bucket: &str, key: &str, version_id: &str) {
    let response = signed_request(
        reqwest::Method::DELETE,
        endpoint,
        bucket,
        key,
        &[("versionId", version_id)],
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "DELETE exact version {key}?versionId={version_id}"
    );
}

pub fn sse_s3_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    headers
}

pub fn sse_c_headers(key: [u8; 32]) -> HeaderMap {
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES256"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key",
        HeaderValue::from_str(&key_b64).expect("SSE-C key header"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&md5_b64).expect("SSE-C key MD5 header"),
    );
    headers
}

pub fn with_metadata(mut headers: HeaderMap, value: &'static str) -> HeaderMap {
    headers.insert(
        "x-amz-meta-transition-suite",
        HeaderValue::from_static(value),
    );
    headers
}

pub fn response_etag(response: &reqwest::Response, operation: &str) -> String {
    response
        .headers()
        .get(header::ETAG)
        .unwrap_or_else(|| panic!("{operation} omitted ETag"))
        .to_str()
        .expect("ETag is ASCII")
        .trim_matches('"')
        .to_owned()
}

pub fn response_version(response: &reqwest::Response, operation: &str) -> String {
    response
        .headers()
        .get("x-amz-version-id")
        .unwrap_or_else(|| panic!("{operation} omitted x-amz-version-id"))
        .to_str()
        .expect("version ID is ASCII")
        .to_owned()
}

pub fn assert_s3_error(status: StatusCode, body: &str, code: &str) {
    assert!(
        status.is_client_error(),
        "expected {code}, got {status}: {body}"
    );
    assert!(
        body.contains(&format!("<Code>{code}</Code>")),
        "expected {code}, got {status}: {body}"
    );
}

fn required_xml_element(xml: &str, name: &str) -> String {
    let opening = format!("<{name}>");
    let closing = format!("</{name}>");
    let start = xml
        .find(&opening)
        .unwrap_or_else(|| panic!("response XML omitted {opening}"))
        + opening.len();
    let end = xml[start..]
        .find(&closing)
        .map(|offset| start + offset)
        .unwrap_or_else(|| panic!("response XML omitted {closing}"));
    quick_xml::escape::unescape(&xml[start..end])
        .expect("response XML escaping")
        .into_owned()
}
