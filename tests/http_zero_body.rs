//! Zero-body POST compatibility through the production HTTP/S3 boundary.
#[allow(dead_code)]
#[path = "support/cors.rs"]
mod cors;
#[allow(dead_code)]
mod sigv4 {
    include!("support/sigv4.rs");

    pub fn empty_post_headers(endpoint: &str, bucket: &str, secret_key: &str) -> HeaderMap {
        let endpoint = normalize_endpoint(endpoint);
        signed_headers(
            &reqwest::Method::POST,
            &canonical_uri(bucket, "empty-post"),
            "uploads=",
            &endpoint.authority,
            &sha256_hex(b""),
            HeaderMap::new(),
            secret_key,
            Utc::now(),
        )
    }
}

use http::{Method, StatusCode};
use ipfs_s3_gateway::store::entities::multipart_upload;
use sea_orm::{EntityTrait, PaginatorTrait};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

async fn post_without_framing(url: &str, headers: http::HeaderMap) -> String {
    let url = url::Url::parse(url).unwrap();
    let port = url.port().unwrap();
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let query = url.query().map(|q| format!("?{q}")).unwrap_or_default();
    let mut request = format!(
        "POST {}{query} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n",
        url.path()
    );
    for (name, value) in &headers {
        if name != http::header::HOST {
            request.push_str(&format!("{name}: {}\r\n", value.to_str().unwrap()));
        }
    }
    request.push_str("\r\n");
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.read_to_end(&mut response),
    )
    .await
    .expect("empty POST must finish without waiting for a body")
    .unwrap();
    String::from_utf8(response).unwrap()
}

#[tokio::test]
async fn signed_create_multipart_without_content_length_creates_and_aborts() {
    let harness = cors::start_harness().await;
    let url = format!("{}/{}/empty-post?uploads", harness.endpoint, harness.bucket);
    let headers = sigv4::empty_post_headers(&harness.endpoint, &harness.bucket, "test");
    let response = post_without_framing(&url, headers).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let upload_id = response
        .split("<UploadId>")
        .nth(1)
        .expect("CreateMultipartUpload response has an upload id")
        .split("</UploadId>")
        .next()
        .unwrap();
    let upload = multipart_upload::Entity::find_by_id(upload_id.to_owned())
        .one(harness.state.store.db())
        .await
        .unwrap()
        .expect("successful create must persist the upload");
    assert_eq!(upload.bucket, harness.bucket);
    assert_eq!(upload.key, "empty-post");

    let aborted = sigv4::send_sigv4(
        Method::DELETE,
        &harness.endpoint,
        &harness.bucket,
        "empty-post",
        &[("uploadId", upload_id)],
        vec![],
        http::HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(aborted.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        multipart_upload::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(harness.kubo_request_count().await, 0);
}

#[tokio::test]
async fn empty_post_without_content_length_still_requires_a_valid_signature() {
    let harness = cors::start_harness().await;
    let url = format!("{}/{}/empty-post?uploads", harness.endpoint, harness.bucket);
    let headers = sigv4::empty_post_headers(&harness.endpoint, &harness.bucket, "wrong-secret");
    let response = post_without_framing(&url, headers).await;
    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    assert!(
        response.contains("<Code>SignatureDoesNotMatch</Code>"),
        "{response}"
    );
    let anonymous = post_without_framing(&url, http::HeaderMap::new()).await;
    assert!(anonymous.starts_with("HTTP/1.1 403"), "{anonymous}");
    assert_eq!(
        multipart_upload::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(harness.kubo_request_count().await, 0);
}
