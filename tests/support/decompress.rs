//! Real-service test support contract for Task 8.
//!
//! ZIP fixtures are executable so test data is deterministic. The harness
//! starts the production S3 service against scripted Kubo RPC responses.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response as AxumResponse;
use axum::{Router, extract};
use s3s::service::S3ServiceBuilder;
use s3s::{Body as S3Body, HttpError};
use sea_orm::Database;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[allow(dead_code)]
pub enum AddReply {
    Ok(&'static str),
    Error(http::StatusCode, &'static str),
}

#[allow(dead_code)]
pub struct KuboScript {
    pub add_replies: Vec<AddReply>,
    pub cat_bodies: HashMap<String, Vec<u8>>,
}

impl KuboScript {
    pub fn repeated_add(
        cid: &'static str,
        calls: usize,
        cat_bodies: HashMap<String, Vec<u8>>,
    ) -> Self {
        Self {
            add_replies: (0..calls).map(|_| AddReply::Ok(cid)).collect(),
            cat_bodies,
        }
    }
}

#[allow(dead_code)]
pub struct TestHarness {
    pub endpoint: String,
    pub bucket: String,
    pub state: Arc<ipfs_s3_gateway::state::AppState>,
    pub kubo: wiremock::MockServer,
    pub observed_http: Arc<tokio::sync::Mutex<Vec<ObservedHttpRequest>>>,
}

#[derive(Clone)]
pub struct ObservedHttpRequest {
    pub method: http::Method,
    pub uri: http::Uri,
    pub headers: http::HeaderMap,
}

pub async fn start_harness(script: KuboScript) -> TestHarness {
    let kubo = MockServer::start().await;
    let KuboScript {
        add_replies,
        cat_bodies,
    } = script;

    if !add_replies.is_empty() {
        let reply_count = add_replies.len() as u64;
        let add_replies = Arc::new(std::sync::Mutex::new(VecDeque::from(add_replies)));
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with({
                let add_replies = add_replies.clone();
                move |_: &wiremock::Request| {
                    let reply = add_replies
                        .lock()
                        .expect("scripted add reply mutex")
                        .pop_front()
                        .expect("unexpected /api/v0/add call after scripted replies");
                    match reply {
                        AddReply::Ok(cid) => ResponseTemplate::new(200)
                            .set_body_string(format!("{{\"Hash\":\"{cid}\",\"Size\":\"0\"}}\n")),
                        AddReply::Error(status, body) => {
                            ResponseTemplate::new(status.as_u16()).set_body_string(body)
                        }
                    }
                }
            })
            .up_to_n_times(reply_count)
            .mount(&kubo)
            .await;
    }

    let cat_bodies = Arc::new(cat_bodies);
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(move |request: &wiremock::Request| {
            let arg = request
                .url
                .query_pairs()
                .find(|(name, _)| name == "arg")
                .map(|(_, value)| value.into_owned());
            match arg.and_then(|arg| cat_bodies.get(&arg).cloned()) {
                Some(body) => ResponseTemplate::new(200).set_body_bytes(body),
                None => ResponseTemplate::new(404).set_body_string("unknown scripted CID"),
            }
        })
        .mount(&kubo)
        .await;
    for pin_path in ["/api/v0/pin/add", "/api/v0/pin/rm"] {
        Mock::given(method("POST"))
            .and(path(pin_path))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"))
            .mount(&kubo)
            .await;
    }

    let db = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite database");
    ipfs_s3_gateway::store::run_migrations(&db)
        .await
        .expect("run test migrations");
    let bucket = "test-bkt".to_owned();
    ipfs_s3_gateway::store::bucket::create(&db, &bucket, None)
        .await
        .expect("create test bucket");
    let state = Arc::new(ipfs_s3_gateway::state::AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.uri()),
        store: ipfs_s3_gateway::store::Store::new(db),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64))
            .expect("zero test master key"),
    });

    let s3_impl = ipfs_s3_gateway::s3::handler::S3Impl::new(state.clone());
    let mut builder = S3ServiceBuilder::new(s3_impl);
    builder.set_auth(ipfs_s3_gateway::auth::GatewayAuth::new(state.clone()));
    builder.set_route(
        ipfs_s3_gateway::s3::route::decompress_zip::DecompressZipRoute::new(state.clone()),
    );
    let service = HandleError::new(builder.build(), handle_s3_error);
    let observed_http = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let app = Router::new()
        .fallback_service(service)
        .layer(middleware::from_fn(
            ipfs_s3_gateway::s3::http::bridge_chunked_content_length,
        ))
        .layer(middleware::from_fn_with_state(
            observed_http.clone(),
            observe_request,
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test S3 listener");
    let port = listener.local_addr().expect("test listener address").port();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test S3 server terminated unexpectedly");
    });

    TestHarness {
        endpoint: format!("http://127.0.0.1:{port}"),
        bucket,
        state,
        kubo,
        observed_http,
    }
}

pub async fn assert_pin_calls(
    harness: &TestHarness,
    path: &str,
    required: &[&str],
    forbidden: &[&str],
) {
    let requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log");
    let args: Vec<String> = requests
        .iter()
        .filter(|request| request.url.path() == path)
        .filter_map(|request| {
            request
                .url
                .query_pairs()
                .find(|(name, _)| name == "arg")
                .map(|(_, value)| value.into_owned())
        })
        .collect();

    for cid in required {
        let calls = args.iter().filter(|arg| arg == cid).count();
        assert_eq!(calls, 1, "expected one {path} call for {cid}; got {args:?}");
    }
    for cid in forbidden {
        let calls = args.iter().filter(|arg| arg == cid).count();
        assert_eq!(calls, 0, "unexpected {path} call for {cid}; got {args:?}");
    }
}

pub async fn assert_no_kubo_calls(harness: &TestHarness) {
    let requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log");
    assert!(requests.is_empty(), "unexpected Kubo calls: {requests:?}");
}

pub async fn latest_observed_request(harness: &TestHarness) -> ObservedHttpRequest {
    harness
        .observed_http
        .lock()
        .await
        .last()
        .cloned()
        .expect("an observed HTTP request")
}

pub async fn create_multipart(
    harness: &TestHarness,
    key: &str,
    options: &[(&str, &str)],
) -> String {
    let mut query = Vec::with_capacity(options.len() + 1);
    query.push(("uploads", ""));
    query.extend_from_slice(options);
    let response = crate::support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        key,
        &query,
        Vec::new(),
        http::HeaderMap::new(),
        "test",
    )
    .await;
    let status = response.status();
    let body = response.text().await.expect("CreateMultipartUpload body");
    assert_eq!(status, StatusCode::OK, "CreateMultipartUpload: {body}");
    upload_id_from_xml(&body)
}

pub async fn upload_part(
    harness: &TestHarness,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> String {
    let part_number = part_number.to_string();
    let response = crate::support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[("partNumber", &part_number), ("uploadId", upload_id)],
        body,
        http::HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "UploadPart");
    response
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag is text")
        .trim_matches('"')
        .to_owned()
}

pub async fn complete_multipart(
    harness: &TestHarness,
    key: &str,
    upload_id: &str,
    parts: &[(i32, String)],
) -> reqwest::Response {
    let mut parts = parts.to_vec();
    parts.sort_by_key(|(number, _)| *number);
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (number, etag) in parts {
        xml.push_str(&format!(
            "<Part><PartNumber>{number}</PartNumber><ETag>\"{}\"</ETag></Part>",
            quick_xml::escape::escape(&etag),
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    complete_multipart_xml(harness, key, upload_id, xml).await
}

pub async fn complete_multipart_xml(
    harness: &TestHarness,
    key: &str,
    upload_id: &str,
    xml: String,
) -> reqwest::Response {
    crate::support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[("uploadId", upload_id)],
        xml.into_bytes(),
        http::HeaderMap::new(),
        "test",
    )
    .await
}

pub async fn abort_multipart(
    harness: &TestHarness,
    key: &str,
    upload_id: &str,
) -> reqwest::Response {
    crate::support::sigv4::send_sigv4(
        reqwest::Method::DELETE,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[("uploadId", upload_id)],
        Vec::new(),
        http::HeaderMap::new(),
        "test",
    )
    .await
}

async fn handle_s3_error(err: HttpError) -> Response<S3Body> {
    tracing::error!(?err, "s3 service error");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(S3Body::from("Internal Server Error".to_string()))
        .expect("internal error response")
}

async fn observe_request(
    extract::State(observed): extract::State<Arc<tokio::sync::Mutex<Vec<ObservedHttpRequest>>>>,
    request: extract::Request,
    next: Next,
) -> AxumResponse {
    observed.lock().await.push(ObservedHttpRequest {
        method: request.method().clone(),
        uri: request.uri().clone(),
        headers: request.headers().clone(),
    });
    next.run(request).await
}

fn upload_id_from_xml(xml: &str) -> String {
    let mut reader = quick_xml::Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    loop {
        match reader.read_event() {
            Ok(quick_xml::events::Event::Start(event)) if event.name().as_ref() == b"UploadId" => {
                let text = reader.read_text(event.name()).expect("UploadId XML text");
                let text = std::str::from_utf8(text.as_ref()).expect("UploadId is UTF-8");
                return quick_xml::escape::unescape(text)
                    .expect("UploadId XML escaping")
                    .into_owned();
            }
            Ok(quick_xml::events::Event::Eof) => panic!("missing UploadId in response: {xml}"),
            Err(error) => panic!("invalid CreateMultipartUpload XML: {error}: {xml}"),
            _ => {}
        }
    }
}

pub fn legal_single_entry_zip() -> Vec<u8> {
    zip(&[ZipEntryFixture {
        name: b"file.txt",
        data: b"single entry bytes",
    }])
}

pub fn legal_two_entry_zip() -> Vec<u8> {
    zip(&[
        ZipEntryFixture {
            name: b"first.txt",
            data: b"first entry bytes",
        },
        ZipEntryFixture {
            name: b"second.txt",
            data: b"second entry bytes",
        },
    ])
}

pub fn duplicate_entry_zip() -> Vec<u8> {
    zip(&[
        ZipEntryFixture {
            name: b"duplicate.txt",
            data: b"first duplicate bytes",
        },
        ZipEntryFixture {
            name: b"duplicate.txt",
            data: b"second duplicate bytes",
        },
    ])
}

pub fn traversal_zip() -> Vec<u8> {
    zip(&[ZipEntryFixture {
        name: b"../escape.txt",
        data: b"escape bytes",
    }])
}

pub fn archive_key_collision_zip() -> Vec<u8> {
    zip(&[ZipEntryFixture {
        name: b"archive.zip",
        data: b"collision bytes",
    }])
}

#[derive(Clone, Copy)]
struct ZipEntryFixture<'a> {
    name: &'a [u8],
    data: &'a [u8],
}

fn push_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

fn zip(entries: &[ZipEntryFixture<'_>]) -> Vec<u8> {
    let mut output = Vec::new();
    let mut offsets = Vec::with_capacity(entries.len());

    for entry in entries {
        let crc = crc32(entry.data);
        offsets.push(output.len() as u32);
        push_u32(&mut output, 0x0403_4b50);
        push_u16(&mut output, 20);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u32(&mut output, crc);
        push_u32(&mut output, entry.data.len() as u32);
        push_u32(&mut output, entry.data.len() as u32);
        push_u16(&mut output, entry.name.len() as u16);
        push_u16(&mut output, 0);
        output.extend_from_slice(entry.name);
        output.extend_from_slice(entry.data);
    }

    let central_offset = output.len() as u32;
    for (entry, offset) in entries.iter().zip(offsets) {
        let crc = crc32(entry.data);
        push_u32(&mut output, 0x0201_4b50);
        push_u16(&mut output, 20);
        push_u16(&mut output, 20);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u32(&mut output, crc);
        push_u32(&mut output, entry.data.len() as u32);
        push_u32(&mut output, entry.data.len() as u32);
        push_u16(&mut output, entry.name.len() as u16);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u32(&mut output, 0);
        push_u32(&mut output, offset);
        output.extend_from_slice(entry.name);
    }

    let central_size = output.len() as u32 - central_offset;
    push_u32(&mut output, 0x0605_4b50);
    push_u16(&mut output, 0);
    push_u16(&mut output, 0);
    push_u16(&mut output, entries.len() as u16);
    push_u16(&mut output, entries.len() as u16);
    push_u32(&mut output, central_size);
    push_u32(&mut output, central_offset);
    push_u16(&mut output, 0);
    output
}
