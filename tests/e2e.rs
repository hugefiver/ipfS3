//! End-to-end tests against a real docker-compose stack.
//!
//! Prerequisite: the Compose `gateway` and `kubo` services are healthy.
//! Default endpoints are IPv4 loopback (`http://127.0.0.1:9000` and
//! `http://127.0.0.1:5001`) and can be overridden with
//! `IPFS_S3_E2E_ENDPOINT` and `IPFS_S3_E2E_KUBO_URL`.
//!
//! Run: cargo test --test e2e -- --nocapture --test-threads=1

use s3::bucket::Bucket;
use s3::bucket_ops::BucketConfiguration;
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::region::Region;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const S3_TIMEOUT: Duration = Duration::from_secs(30);
const KUBO_TIMEOUT: Duration = Duration::from_secs(15);
static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn endpoint_from_env(name: &str, default: &str) -> String {
    let endpoint = std::env::var(name).unwrap_or_else(|_| default.to_owned());
    let endpoint = endpoint.trim_end_matches('/').to_owned();
    assert!(
        endpoint.starts_with("http://") || endpoint.starts_with("https://"),
        "{name} must be an HTTP(S) URL, got {endpoint}"
    );
    assert!(
        !endpoint.to_ascii_lowercase().contains("localhost"),
        "{name} must use an IPv4 address or explicit hostname; localhost is forbidden"
    );
    endpoint
}

fn gateway_endpoint() -> String {
    endpoint_from_env("IPFS_S3_E2E_ENDPOINT", "http://127.0.0.1:9000")
}

fn kubo_endpoint() -> String {
    endpoint_from_env("IPFS_S3_E2E_KUBO_URL", "http://127.0.0.1:5001")
}

fn test_creds() -> Credentials {
    Credentials::new(Some("test"), Some("test"), None, None, None).unwrap()
}

fn test_region() -> Region {
    Region::Custom {
        region: "us-east-1".into(),
        endpoint: gateway_endpoint(),
    }
}

fn make_bucket(name: &str) -> Box<Bucket> {
    Bucket::new(name, test_region(), test_creds())
        .unwrap()
        .with_path_style()
}

fn make_bucket_with(name: &str, creds: Credentials) -> Box<Bucket> {
    Bucket::new(name, test_region(), creds)
        .unwrap()
        .with_path_style()
}

fn unique_bucket(scenario: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    let counter = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!(
        "e2e-{scenario}-{}-{nanos:x}-{counter:x}",
        std::process::id()
    );
    assert!(
        name.len() <= 63,
        "bucket name exceeds 63 characters: {name}"
    );
    name
}

async fn s3_call<T, F>(label: &str, future: F) -> Result<T, S3Error>
where
    F: Future<Output = Result<T, S3Error>>,
{
    tokio::time::timeout(S3_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("rust-s3 operation '{label}' timed out after 30 seconds"))
}

async fn create_bucket(scenario: &str) -> (String, Box<Bucket>) {
    let name = unique_bucket(scenario);
    let label = format!("create bucket {name}");
    let response = s3_call(
        &label,
        Bucket::create_with_path_style(
            &name,
            test_region(),
            test_creds(),
            BucketConfiguration::default(),
        ),
    )
    .await
    .unwrap_or_else(|error| panic!("{label} failed: {error}"));
    assert!(
        response.success(),
        "{label} returned status {}",
        response.response_code
    );
    let bucket = make_bucket(&name);
    (name, bucket)
}

async fn cleanup_success(bucket: &Bucket, keys: &[&str]) {
    for key in keys {
        let label = format!("cleanup delete object {}/{key}", bucket.name());
        let response = s3_call(&label, bucket.delete_object(key))
            .await
            .unwrap_or_else(|error| panic!("{label} failed: {error}"));
        assert_eq!(
            response.status_code(),
            204,
            "{label} returned an unexpected status"
        );
    }

    let label = format!("cleanup delete bucket {}", bucket.name());
    let status = s3_call(&label, bucket.delete())
        .await
        .unwrap_or_else(|error| panic!("{label} failed: {error}"));
    assert_eq!(status, 204, "{label} returned an unexpected status");
}

fn etag_from_headers(headers: &std::collections::HashMap<String, String>) -> String {
    headers
        .get("etag")
        .or_else(|| headers.get("ETag"))
        .or_else(|| headers.get("e-tag"))
        .cloned()
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

async fn kubo_cat(cid: &str) -> Vec<u8> {
    let client = reqwest::Client::builder()
        .timeout(KUBO_TIMEOUT)
        .build()
        .expect("build 15-second Kubo client");
    let url = format!("{}/api/v0/cat?arg={cid}", kubo_endpoint());
    let response = client
        .post(&url)
        .send()
        .await
        .unwrap_or_else(|error| panic!("kubo cat request failed for {cid}: {error}"));
    assert!(
        response.status().is_success(),
        "kubo cat failed for {cid}: {}",
        response.status()
    );
    response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("kubo cat body failed for {cid}: {error}"))
        .to_vec()
}

#[tokio::test]
async fn test_02_create_bucket() {
    let (_name, bucket) = create_bucket("create").await;
    cleanup_success(&bucket, &[]).await;
}

#[tokio::test]
async fn test_03_04_put_get_plain_object() {
    let (_name, bucket) = create_bucket("plain").await;
    let content = b"hello world from e2e";

    let put_response = s3_call(
        "plain put_object hello.txt",
        bucket.put_object("hello.txt", content),
    )
    .await
    .expect("plain put_object must succeed");
    assert_eq!(put_response.status_code(), 200, "put_object failed");

    let etag = etag_from_headers(&put_response.headers());
    assert!(!etag.is_empty(), "ETag should not be empty");
    println!("ETag (CID): {etag}");
    assert_eq!(
        kubo_cat(&etag).await.as_slice(),
        content,
        "kubo cat should return plaintext for a plain object"
    );

    let get_response = s3_call("plain get_object hello.txt", bucket.get_object("hello.txt"))
        .await
        .expect("plain get_object must succeed");
    assert_eq!(get_response.status_code(), 200, "get_object failed");
    assert_eq!(get_response.bytes().as_ref(), content, "content mismatch");

    cleanup_success(&bucket, &["hello.txt"]).await;
}

#[tokio::test]
async fn test_05_list_objects() {
    let (_name, bucket) = create_bucket("list").await;
    let objects: [(&str, &[u8]); 2] = [("file1.txt", b"data1"), ("file2.txt", b"data2")];
    for (key, content) in objects {
        let label = format!("list setup put_object {key}");
        let response = s3_call(&label, bucket.put_object(key, content))
            .await
            .unwrap_or_else(|error| panic!("{label} failed: {error}"));
        assert_eq!(
            response.status_code(),
            200,
            "{label} returned an unexpected status"
        );
    }

    let results = s3_call("list_objects root", bucket.list(String::new(), None))
        .await
        .expect("ListObjects must deserialize successfully");
    let keys = results
        .iter()
        .flat_map(|result| result.contents.iter())
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert!(
        keys.contains(&"file1.txt"),
        "ListObjects omitted file1.txt: {keys:?}"
    );
    assert!(
        keys.contains(&"file2.txt"),
        "ListObjects omitted file2.txt: {keys:?}"
    );

    cleanup_success(&bucket, &["file1.txt", "file2.txt"]).await;
}

#[tokio::test]
async fn test_06_delete_then_404() {
    let (_name, bucket) = create_bucket("delete").await;
    let put_response = s3_call(
        "delete setup put_object todelete.txt",
        bucket.put_object("todelete.txt", b"temp"),
    )
    .await
    .expect("delete setup put_object must succeed");
    assert_eq!(put_response.status_code(), 200);

    let delete_response = s3_call(
        "delete_object todelete.txt",
        bucket.delete_object("todelete.txt"),
    )
    .await
    .expect("delete_object must succeed");
    assert_eq!(delete_response.status_code(), 204, "delete_object failed");

    let error = s3_call(
        "get deleted object todelete.txt",
        bucket.get_object("todelete.txt"),
    )
    .await
    .expect_err("get after delete must fail");
    let message = error.to_string();
    assert!(
        message.contains("404") || message.contains("NoSuchKey"),
        "expected 404/NoSuchKey, got: {message}"
    );

    cleanup_success(&bucket, &[]).await;
}

#[tokio::test]
async fn test_07_copy_object_same_cid() {
    let (_name, bucket) = create_bucket("copy").await;
    let content = b"copy source data";

    let put_response = s3_call(
        "copy setup put_object src.txt",
        bucket.put_object("src.txt", content),
    )
    .await
    .expect("copy source upload must succeed");
    assert_eq!(put_response.status_code(), 200);
    let source_etag = etag_from_headers(&put_response.headers());
    assert!(
        !source_etag.is_empty(),
        "copy source ETag must not be empty"
    );

    let copy_status = s3_call(
        "signed copy_object_internal src.txt to dst.txt",
        bucket.copy_object_internal("src.txt", "dst.txt"),
    )
    .await
    .expect("signed CopyObject must succeed");
    assert_eq!(copy_status, 200, "CopyObject must return HTTP 200");

    let (head, head_status) = s3_call("head copied dst.txt", bucket.head_object("dst.txt"))
        .await
        .expect("HeadObject for copied destination must succeed");
    assert_eq!(head_status, 200);
    let destination_etag = head.e_tag.unwrap_or_default().trim_matches('"').to_owned();
    assert_eq!(
        source_etag, destination_etag,
        "CopyObject should preserve the source CID"
    );

    cleanup_success(&bucket, &["src.txt", "dst.txt"]).await;
}

#[tokio::test]
async fn test_08_range_request() {
    let (_name, bucket) = create_bucket("range").await;
    let content = (0..1024u32)
        .map(|index| (index % 256) as u8)
        .collect::<Vec<_>>();

    let put_response = s3_call(
        "range setup put_object large.bin",
        bucket.put_object("large.bin", &content),
    )
    .await
    .expect("range setup upload must succeed");
    assert_eq!(put_response.status_code(), 200);

    let range_response = s3_call(
        "signed get_object_range large.bin bytes 0-99",
        bucket.get_object_range("large.bin", 0, Some(99)),
    )
    .await
    .expect("signed ranged GET must succeed");
    assert_eq!(
        range_response.status_code(),
        206,
        "Range GET must return HTTP 206"
    );
    assert_eq!(
        range_response.bytes().len(),
        100,
        "range body should contain 100 bytes"
    );
    assert_eq!(
        range_response.bytes().as_ref(),
        &content[0..100],
        "range content mismatch"
    );

    cleanup_success(&bucket, &["large.bin"]).await;
}

#[tokio::test]
async fn test_09_wrong_credentials() {
    let (name, bucket) = create_bucket("auth").await;
    let bad_credentials = Credentials::new(Some("wrong"), Some("wrong"), None, None, None).unwrap();
    let bad_bucket = make_bucket_with(&name, bad_credentials);

    let error = s3_call(
        "wrong-credential put_object test.txt",
        bad_bucket.put_object("test.txt", b"data"),
    )
    .await
    .expect_err("wrong credentials must be rejected");
    let message = error.to_string();
    assert!(
        message.contains("403")
            || message.contains("SignatureDoesNotMatch")
            || message.contains("InvalidAccessKeyId"),
        "expected 403/SignatureDoesNotMatch/InvalidAccessKeyId, got: {message}"
    );

    cleanup_success(&bucket, &[]).await;
}

#[tokio::test]
async fn test_10_multipart_upload() {
    let (_name, bucket) = create_bucket("multipart").await;
    let content = (0..6_291_456u32)
        .map(|index| (index % 256) as u8)
        .collect::<Vec<_>>();

    let initiated = s3_call(
        "initiate multipart upload bigfile.bin",
        bucket.initiate_multipart_upload("bigfile.bin", "application/octet-stream"),
    )
    .await
    .expect("multipart initiation must succeed; NotImplemented is a failure");

    let part = s3_call(
        "upload 6 MiB multipart part 1",
        bucket.put_multipart_chunk(
            content.clone(),
            "bigfile.bin",
            1,
            &initiated.upload_id,
            "application/octet-stream",
        ),
    )
    .await
    .expect("6 MiB multipart part upload must succeed");

    let complete_response = s3_call(
        "complete multipart upload bigfile.bin",
        bucket.complete_multipart_upload("bigfile.bin", &initiated.upload_id, vec![part]),
    )
    .await
    .expect("multipart completion must succeed");
    assert_eq!(
        complete_response.status_code(),
        200,
        "multipart completion returned an unexpected status"
    );

    let get_response = s3_call(
        "multipart round-trip get_object bigfile.bin",
        bucket.get_object("bigfile.bin"),
    )
    .await
    .expect("6 MiB round-trip download must succeed");
    assert_eq!(get_response.status_code(), 200);
    assert_eq!(
        get_response.bytes().as_ref(),
        content.as_slice(),
        "6 MiB round-trip content mismatch"
    );

    cleanup_success(&bucket, &["bigfile.bin"]).await;
}

#[tokio::test]
async fn test_11_encrypted_object() {
    let (_name, bucket) = create_bucket("encrypted").await;
    let content = b"secret encrypted data";
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-amz-server-side-encryption", "AES256".parse().unwrap());

    let put_response = s3_call(
        "encrypted put_object_with_headers secret.txt",
        bucket.put_object_with_headers("secret.txt", content, Some(headers)),
    )
    .await
    .expect("encrypted put must succeed");
    assert_eq!(put_response.status_code(), 200, "encrypted put failed");

    let etag = etag_from_headers(&put_response.headers());
    assert!(!etag.is_empty(), "encrypted object ETag must not be empty");
    let kubo_content = kubo_cat(&etag).await;
    assert_ne!(
        kubo_content.as_slice(),
        content,
        "Kubo must return ciphertext for an encrypted object"
    );

    let get_response = s3_call(
        "encrypted get_object secret.txt",
        bucket.get_object("secret.txt"),
    )
    .await
    .expect("encrypted S3 GET must succeed");
    assert_eq!(get_response.status_code(), 200);
    assert_eq!(
        get_response.bytes().as_ref(),
        content,
        "S3 GET must return decrypted plaintext"
    );

    cleanup_success(&bucket, &["secret.txt"]).await;
}

#[tokio::test]
async fn test_12_plain_object_ipfs_cat() {
    let (_name, bucket) = create_bucket("plain-ipfs").await;
    let content = b"plain data for ipfs cat";

    let put_response = s3_call(
        "plain IPFS put_object plain.txt",
        bucket.put_object("plain.txt", content),
    )
    .await
    .expect("plain IPFS setup put must succeed");
    assert_eq!(put_response.status_code(), 200);
    let etag = etag_from_headers(&put_response.headers());
    assert_eq!(
        kubo_cat(&etag).await.as_slice(),
        content,
        "Kubo must return plaintext for a plain object"
    );

    cleanup_success(&bucket, &["plain.txt"]).await;
}

#[tokio::test]
async fn test_14_etag_is_cid() {
    let (_name, bucket) = create_bucket("etag").await;
    let put_response = s3_call(
        "ETag put_object etag-test.txt",
        bucket.put_object("etag-test.txt", b"etag test"),
    )
    .await
    .expect("ETag setup put must succeed");
    assert_eq!(put_response.status_code(), 200);
    let etag = etag_from_headers(&put_response.headers());
    assert!(
        etag.starts_with("Qm")
            || etag.starts_with("bafy")
            || etag.starts_with("bafk")
            || etag.starts_with("bafz"),
        "ETag '{etag}' does not look like a CID"
    );

    cleanup_success(&bucket, &["etag-test.txt"]).await;
}
