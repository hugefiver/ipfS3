use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use s3s::Body as S3Body;
use s3s::HttpError;
use s3s::auth::SecretKey;
use s3s::service::S3ServiceBuilder;

use ipfs_s3_gateway::auth::GatewayAuth;
use ipfs_s3_gateway::crypto::key::MasterKey;
use ipfs_s3_gateway::kubo::KuboClient;
use ipfs_s3_gateway::s3::handler::S3Impl;
use ipfs_s3_gateway::state::AppState;
use ipfs_s3_gateway::store::Store;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Mirrors `src/main.rs::handle_s3_error`.
async fn handle_s3_error(err: HttpError) -> Response<S3Body> {
    eprintln!("s3 service error: {err:?}");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(S3Body::from("Internal Server Error".to_string()))
        .unwrap()
}

/// Test harness: spins up axum + s3s + in-memory SQLite + wiremock Kubo.
///
/// Returns:
/// - `addr` — `"localhost:{port}"` for constructing the S3 endpoint URL.
/// - `bucket_name` — pre-created bucket name (created directly in the DB to
///   avoid rust-s3's virtual-hosted-style `Bucket::create`).
/// - `_kubo` — MockServer handle (must stay alive for the test duration).
async fn harness() -> (String, String, MockServer) {
    let kubo = MockServer::start().await;

    // -- wiremock Kubo stubs --

    // POST /api/v0/add → returns a single newline-delimited JSON line
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("{\"Hash\":\"QmTestCid\",\"Size\":\"11\"}\n"),
        )
        .mount(&kubo)
        .await;

    // POST /api/v0/pin/add → success
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmTestCid\"]}"))
        .mount(&kubo)
        .await;

    // POST /api/v0/cat → return the plaintext body
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello world".to_vec()))
        .mount(&kubo)
        .await;

    // -- application state --

    let kubo_client = KuboClient::new(kubo.uri());
    let db = sea_orm::Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite");
    ipfs_s3_gateway::store::run_migrations(&db)
        .await
        .expect("run migrations");

    // Create the test bucket directly in the DB so we bypass the
    // virtual-hosted-style-only `Bucket::create`.
    let bucket_name = "test-bkt";
    ipfs_s3_gateway::store::bucket::create(&db, bucket_name, None)
        .await
        .expect("create bucket in DB");

    let store = Store::new(db);

    let mut credentials = HashMap::new();
    credentials.insert("test".to_string(), SecretKey::from("test"));

    let master_key =
        MasterKey::from_hex("0000000000000000000000000000000000000000000000000000000000000000")
            .expect("master key from hex");

    let state = Arc::new(AppState {
        kubo: kubo_client,
        store,
        credentials,
        master_key,
    });

    // -- axum app --

    let s3_impl = S3Impl::new(state.clone());
    let gateway_auth = GatewayAuth::new(state);

    let s3_service = {
        let mut builder = S3ServiceBuilder::new(s3_impl);
        builder.set_auth(gateway_auth);
        builder.build()
    };

    let s3_service = HandleError::new(s3_service, handle_s3_error);
    let app = Router::new().fallback_service(s3_service);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("local addr").port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });

    // Let the server start listening.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (format!("localhost:{port}"), bucket_name.to_string(), kubo)
}

/// Convenience: build a path-style `Bucket` for the test endpoint.
fn test_bucket(addr: &str, bucket_name: &str) -> Box<Bucket> {
    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: format!("http://{addr}"),
    };
    let creds =
        Credentials::new(Some("test"), Some("test"), None, None, None).expect("credentials");
    Bucket::new(bucket_name, region, creds)
        .expect("bucket")
        .with_path_style()
}

/// Convenience: build a bucket with wrong credentials.
fn bad_bucket(addr: &str, bucket_name: &str) -> Box<Bucket> {
    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: format!("http://{addr}"),
    };
    let bad_creds =
        Credentials::new(Some("wrong"), Some("wrong"), None, None, None).expect("bad credentials");
    Bucket::new(bucket_name, region, bad_creds)
        .expect("bucket")
        .with_path_style()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full round-trip: put plain object → get it back.
/// Verifies the ETag equals the CID returned by the mocked Kubo add.
#[tokio::test]
async fn test_create_and_put_and_get_plain_object() {
    let (addr, bucket_name, _kubo) = harness().await;
    let bucket = test_bucket(&addr, &bucket_name);

    // Put object
    let resp = bucket
        .put_object("hello.txt", b"hello world")
        .await
        .expect("put object");
    assert_eq!(resp.status_code(), 200);

    // ETag must contain the CID returned by our mocked Kubo add.
    let headers = resp.headers();
    let etag = headers.get("etag").cloned().expect("etag header");
    assert!(
        etag.contains("QmTestCid"),
        "ETag should contain the CID, got: {etag}"
    );

    // Get object — content must match.
    let resp = bucket.get_object("hello.txt").await.expect("get object");
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.as_slice(), b"hello world");
}

/// HeadObject on a nested key returns 200 and the CID as ETag.
#[tokio::test]
async fn test_head_object_signed_nested_key_succeeds() {
    let (addr, bucket_name, _kubo) = harness().await;
    let bucket = test_bucket(&addr, &bucket_name);

    bucket
        .put_object("nested/path/file.txt", b"hello world")
        .await
        .expect("put nested object");

    let (head, status_code) = bucket
        .head_object("nested/path/file.txt")
        .await
        .expect("head nested path");
    assert_eq!(status_code, 200);

    let etag = head.e_tag.expect("etag header");
    assert!(
        etag.contains("QmTestCid"),
        "ETag should contain the CID, got: {etag}"
    );
}

/// Put two objects, then list them.
#[tokio::test]
async fn test_list_objects() {
    let (addr, bucket_name, _kubo) = harness().await;
    let bucket = test_bucket(&addr, &bucket_name);

    bucket
        .put_object("obj1.txt", b"hello world")
        .await
        .expect("put obj1");
    bucket
        .put_object("obj2.txt", b"hello world")
        .await
        .expect("put obj2");

    let result = bucket.list(String::new(), None).await;
    match result {
        Ok(pages) => {
            // `list` returns one page per continuation token; merge contents.
            let total: usize = pages.iter().map(|p| p.contents.len()).sum();
            assert_eq!(total, 2, "expected 2 objects, got {total}");
        }
        Err(e) => panic!("unexpected list error: {e}"),
    }
}

/// Directory-style delimiter at the root returns direct keys and common prefixes.
#[tokio::test]
async fn test_list_objects_with_delimiter_returns_common_prefixes() {
    let (addr, bucket_name, _kubo) = harness().await;
    let bucket = test_bucket(&addr, &bucket_name);

    bucket
        .put_object("a.txt", b"hello world")
        .await
        .expect("put a.txt");
    bucket
        .put_object("photos/cat.jpg", b"hello world")
        .await
        .expect("put photos/cat.jpg");
    bucket
        .put_object("photos/dog.jpg", b"hello world")
        .await
        .expect("put photos/dog.jpg");
    bucket
        .put_object("videos/clip.mp4", b"hello world")
        .await
        .expect("put videos/clip.mp4");

    let pages = bucket
        .list(String::new(), Some("/".to_string()))
        .await
        .expect("list with delimiter");

    let mut keys: Vec<String> = pages
        .iter()
        .flat_map(|p| p.contents.iter().map(|o| o.key.clone()))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["a.txt".to_string()]);

    let mut prefixes: Vec<String> = pages
        .iter()
        .flat_map(|p| {
            p.common_prefixes
                .iter()
                .flat_map(|v| v.iter().map(|cp| cp.prefix.clone()))
        })
        .collect();
    prefixes.sort();
    assert_eq!(prefixes, vec!["photos/".to_string(), "videos/".to_string()]);
}

/// Prefix + delimiter returns one level of keys and the next common prefix.
#[tokio::test]
async fn test_list_objects_with_prefix_and_delimiter_returns_one_level() {
    let (addr, bucket_name, _kubo) = harness().await;
    let bucket = test_bucket(&addr, &bucket_name);

    bucket
        .put_object("photos/cat.jpg", b"hello world")
        .await
        .expect("put photos/cat.jpg");
    bucket
        .put_object("photos/dog.jpg", b"hello world")
        .await
        .expect("put photos/dog.jpg");
    bucket
        .put_object("photos/2024/jan.jpg", b"hello world")
        .await
        .expect("put photos/2024/jan.jpg");

    let pages = bucket
        .list("photos/".to_string(), Some("/".to_string()))
        .await
        .expect("list with prefix and delimiter");

    let mut keys: Vec<String> = pages
        .iter()
        .flat_map(|p| p.contents.iter().map(|o| o.key.clone()))
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["photos/cat.jpg".to_string(), "photos/dog.jpg".to_string()]
    );

    let mut prefixes: Vec<String> = pages
        .iter()
        .flat_map(|p| {
            p.common_prefixes
                .iter()
                .flat_map(|v| v.iter().map(|cp| cp.prefix.clone()))
        })
        .collect();
    prefixes.sort();
    assert_eq!(prefixes, vec!["photos/2024/".to_string()]);
}

/// Wrong credentials must be rejected by the auth layer.
#[tokio::test]
async fn test_wrong_credentials_rejected() {
    let (addr, bucket_name, _kubo) = harness().await;
    let bucket = bad_bucket(&addr, &bucket_name);

    let result = bucket.put_object("hello.txt", b"hello world").await;
    assert!(
        result.is_err(),
        "expected error with wrong credentials, got success"
    );
}
