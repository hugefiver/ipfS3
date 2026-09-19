//! Admission regressions through the production router, without including the
//! shared support test module (and thereby running its tests again).
#[allow(dead_code)]
#[path = "support/cors.rs"]
mod cors;
#[allow(dead_code)]
#[path = "support/sigv4.rs"]
mod sigv4;

use axum::body::Body;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use sea_orm::{EntityTrait, PaginatorTrait};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt as _;

#[tokio::test]
async fn anonymous_form_is_rejected_without_polling_pending_or_large_body() {
    let harness = cors::start_harness().await;
    for content_type in [
        "multipart/form-data; boundary=upload",
        "Multipart/Form-Data ; boundary=\"upload\"; charset=utf-8",
    ] {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let stream = futures_util::stream::poll_fn(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Pending::<Option<Result<Bytes, std::io::Error>>>
        });
        let app = cors::gateway_router(harness.state.clone(), harness.imports.clone());
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            app.oneshot(
                http::Request::builder()
                    .method(Method::POST)
                    .uri("/test-bkt")
                    .header("content-type", content_type)
                    .header("content-length", "5368709120")
                    .body(Body::from_stream(stream))
                    .unwrap(),
            ),
        )
        .await
        .expect("admission must not wait for an anonymous body")
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    assert_eq!(harness.kubo_request_count().await, 0);
}

#[tokio::test]
async fn signed_sse_form_cannot_publish_plaintext() {
    let harness = cors::start_harness().await;
    let response = reqwest::Client::new()
        .post(format!("{}/{}", harness.endpoint, harness.bucket))
        .header("content-type", "multipart/form-data; boundary=upload")
        .body(signed_browser_form(&harness.bucket))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("<Code>InvalidRequest</Code>")
    );
    assert_eq!(harness.kubo_request_count().await, 0);
    assert_eq!(
        ipfs_s3_gateway::store::entities::object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
}

fn signed_browser_form(bucket: &str) -> String {
    use base64::Engine as _;
    use hmac::{Hmac, KeyInit, Mac};
    fn hmac(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(key).unwrap();
        mac.update(value);
        mac.finalize().into_bytes().to_vec()
    }
    let now = chrono::Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let credential = format!("test/{date}/us-east-1/s3/aws4_request");
    let fields = [
        ("key", "form-object"),
        ("x-amz-server-side-encryption", "AES256"),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-credential", &credential),
        ("x-amz-date", &amz_date),
    ];
    let mut conditions = vec![serde_json::json!({"bucket": bucket})];
    conditions.extend(
        fields
            .iter()
            .map(|(name, value)| serde_json::json!(["eq", format!("${name}"), value])),
    );
    let policy = base64::engine::general_purpose::STANDARD.encode(
        serde_json::to_vec(&serde_json::json!({
            "expiration": (now + chrono::Duration::hours(1)).to_rfc3339(), "conditions": conditions,
        }))
        .unwrap(),
    );
    let key = hmac(b"AWS4test", date.as_bytes());
    let key = hmac(&key, b"us-east-1");
    let key = hmac(&key, b"s3");
    let key = hmac(&key, b"aws4_request");
    let signature = hex::encode(hmac(&key, policy.as_bytes()));
    let mut body = String::new();
    for (name, value) in fields.into_iter().chain([
        ("policy", policy.as_str()),
        ("x-amz-signature", signature.as_str()),
    ]) {
        body.push_str(&format!(
            "--upload\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
        ));
    }
    body.push_str("--upload\r\nContent-Disposition: form-data; name=\"file\"; filename=\"payload\"\r\n\r\nsecret\r\n--upload--\r\n");
    body
}

#[tokio::test]
async fn real_http_rejects_anonymous_form_before_any_body_arrives() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let harness = cors::start_harness().await;
    let endpoint = url::Url::parse(&harness.endpoint).unwrap();
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", endpoint.port().unwrap()))
        .await
        .unwrap();
    socket.write_all(format!("POST /{} HTTP/1.1\r\nHost: localhost\r\nContent-Type: multipart/form-data; boundary=upload\r\nContent-Length: 5368709120\r\nConnection: close\r\n\r\n", harness.bucket).as_bytes()).await.unwrap();
    let mut bytes = [0u8; 4096];
    let size = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(&bytes[..size])
            .unwrap()
            .starts_with("HTTP/1.1 400")
    );
    assert_eq!(harness.kubo_request_count().await, 0);
}

#[tokio::test]
async fn signed_conditions_rejected_before_guards_kubo_and_completion_xml() {
    let harness = cors::start_harness().await;
    // A real custom POST still routes normally. Unsupported conditional writes
    // must not supersede its live ownership claim even if they later fail.
    let imported = sigv4::send_sigv4(Method::POST, &harness.endpoint, &harness.bucket, "object", &[("ipfs3-import", "")],
        b"<IPFS3ImportRequest><CID>bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku</CID></IPFS3ImportRequest>".to_vec(),
        HeaderMap::from_iter([(http::header::CONTENT_TYPE, HeaderValue::from_static("application/xml"))]), "test").await;
    assert_eq!(imported.status(), StatusCode::ACCEPTED);
    let jobs = ipfs_s3_gateway::store::entities::import_job::Entity::find()
        .all(harness.state.store.db())
        .await
        .unwrap();
    for (method, query) in [
        (Method::PUT, vec![]),
        (Method::PUT, vec![("decompress-zip", "expanded/")]),
        (Method::POST, vec![("uploadId", "absent")]),
        (
            Method::POST,
            vec![("uploadId", "absent"), ("decompress-zip", "expanded/")],
        ),
    ] {
        for values in [
            vec![("if-match", "\"cid\"")],
            vec![("if-none-match", "*")],
            vec![("if-match", "")],
            vec![("if-none-match", "")],
            vec![("if-match", ""), ("if-none-match", "*")],
        ] {
            let mut headers = HeaderMap::new();
            for (name, value) in values {
                headers.insert(name, HeaderValue::from_str(value).unwrap());
            }
            let response = sigv4::send_sigv4(
                method.clone(),
                &harness.endpoint,
                &harness.bucket,
                "object",
                &query,
                b"not completion XML".to_vec(),
                headers,
                "test",
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(
                response
                    .text()
                    .await
                    .unwrap()
                    .contains("<Code>InvalidRequest</Code>")
            );
        }
    }
    assert_eq!(harness.kubo_request_count().await, 0);
    assert_eq!(
        ipfs_s3_gateway::store::entities::import_job::Entity::find()
            .all(harness.state.store.db())
            .await
            .unwrap(),
        jobs
    );
    assert_eq!(
        ipfs_s3_gateway::store::entities::standard_mutation_lease::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        ipfs_s3_gateway::store::entities::object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
}

fn dto_request<T>(input: T) -> s3s::S3Request<T> {
    s3s::S3Request {
        input,
        method: Method::PUT,
        uri: "/test-bkt/object".parse().unwrap(),
        headers: HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

#[tokio::test]
async fn typed_conditions_cannot_bypass_raw_header_gate() {
    use ipfs_s3_gateway::s3::ops::{multipart, object};
    let harness = cors::start_harness().await;
    for if_match in [true, false] {
        let put = s3s::dto::PutObjectInput {
            bucket: harness.bucket.clone(),
            key: "object".into(),
            if_match: if_match.then_some(s3s::dto::ETagCondition::Any),
            if_none_match: (!if_match).then_some(s3s::dto::ETagCondition::Any),
            ..Default::default()
        };
        let error = object::put_object(&harness.state, dto_request(put))
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidRequest");
        let complete = s3s::dto::CompleteMultipartUploadInput {
            bucket: harness.bucket.clone(),
            key: "object".into(),
            upload_id: "missing".into(),
            if_match: if_match.then_some(s3s::dto::ETagCondition::Any),
            if_none_match: (!if_match).then_some(s3s::dto::ETagCondition::Any),
            ..Default::default()
        };
        let error = multipart::complete_multipart_upload(&harness.state, dto_request(complete))
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidRequest");
    }
    assert_eq!(harness.kubo_request_count().await, 0);
}

#[tokio::test]
async fn rejected_complete_preserves_parts_and_normal_mpu_still_completes() {
    use ipfs_s3_gateway::store::entities::{multipart_part, multipart_upload};
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };
    let harness = cors::start_harness().await;
    for (endpoint, response) in [
        ("/api/v0/add", "{\"Hash\":\"QmTestCid\",\"Size\":\"3\"}\n"),
        ("/api/v0/pin/add", "{\"Pins\":[]}"),
        ("/api/v0/cat", "abc"),
    ] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_string(response))
            .mount(&harness.kubo)
            .await;
    }
    let create = sigv4::send_sigv4(
        Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "mpu",
        &[("uploads", "")],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let xml = create.text().await.unwrap();
    let id = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap();
    let part = sigv4::send_sigv4(
        Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "mpu",
        &[("uploadId", id), ("partNumber", "1")],
        b"abc".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part.headers()["etag"].to_str().unwrap();
    let complete = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>").into_bytes();
    let uploads = multipart_upload::Entity::find()
        .all(harness.state.store.db())
        .await
        .unwrap();
    let parts = multipart_part::Entity::find()
        .all(harness.state.store.db())
        .await
        .unwrap();
    let calls = harness.kubo_request_count().await;
    let rejected = sigv4::send_sigv4(
        Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "mpu",
        &[("uploadId", id)],
        complete.clone(),
        HeaderMap::from_iter([(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"))]),
        "test",
    )
    .await;
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    assert!(
        rejected
            .text()
            .await
            .unwrap()
            .contains("<Code>InvalidRequest</Code>")
    );
    assert_eq!(harness.kubo_request_count().await, calls);
    assert_eq!(
        multipart_upload::Entity::find()
            .all(harness.state.store.db())
            .await
            .unwrap(),
        uploads
    );
    assert_eq!(
        multipart_part::Entity::find()
            .all(harness.state.store.db())
            .await
            .unwrap(),
        parts
    );
    let success = sigv4::send_sigv4(
        Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "mpu",
        &[("uploadId", id)],
        complete,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(
        success.status(),
        StatusCode::OK,
        "{}",
        success.text().await.unwrap()
    );
    assert_eq!(
        multipart_part::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
}
