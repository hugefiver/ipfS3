//! Real-service test support contract for Task 8.
//!
//! ZIP fixtures are executable so test data is deterministic. The harness
//! starts the production S3 service against scripted Kubo RPC responses.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, mpsc};

use axum::extract;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response as AxumResponse;
use sea_orm::Database;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub trait S3TestEndpoint {
    fn endpoint(&self) -> &str;
    fn bucket(&self) -> &str;
}

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
    add_file_bytes: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    cat_bodies: Arc<std::sync::RwLock<HashMap<String, Vec<u8>>>>,
    _server: S3ServerHandle,
}

impl TestHarness {
    pub fn captured_add_file_bytes(&self) -> Vec<Vec<u8>> {
        self.add_file_bytes
            .lock()
            .expect("add capture mutex")
            .clone()
    }

    pub fn set_cat_body(&self, cid: &str, body: Vec<u8>) {
        self.cat_bodies
            .write()
            .expect("cat body map")
            .insert(cid.to_owned(), body);
    }
}

impl S3TestEndpoint for TestHarness {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}

pub struct KuboHarness {
    pub server: MockServer,
    add_file_bytes: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    cat_bodies: Arc<std::sync::RwLock<HashMap<String, Vec<u8>>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KuboBlockTarget {
    Add,
    Cat,
    PinAdd,
}

pub struct KuboBlockControl {
    reached: Option<mpsc::Receiver<()>>,
    release: Option<mpsc::Sender<()>>,
}

impl KuboBlockControl {
    pub async fn wait_until_blocked(&mut self) {
        let reached = self.reached.take().expect("Kubo block is awaited once");
        tokio::task::spawn_blocking(move || {
            reached
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("Kubo request reached deterministic block")
        })
        .await
        .expect("join Kubo block waiter");
    }

    pub fn release(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for KuboBlockControl {
    fn drop(&mut self) {
        self.release();
    }
}

/// Arm a single request after setup (for example Complete's add, not a part's add).
pub async fn block_next_kubo_request(
    server: &MockServer,
    target: KuboBlockTarget,
    response: ResponseTemplate,
) -> KuboBlockControl {
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let blocker = KuboBlocker {
        target,
        reached: reached_tx,
        release: Arc::new(Mutex::new(Some(release_rx))),
    };
    let endpoint = match target {
        KuboBlockTarget::Add => "/api/v0/add",
        KuboBlockTarget::Cat => "/api/v0/cat",
        KuboBlockTarget::PinAdd => "/api/v0/pin/add",
    };
    Mock::given(method("POST"))
        .and(path(endpoint))
        .respond_with(move |_: &wiremock::Request| {
            blocker.block_once(target);
            response.clone()
        })
        .with_priority(1)
        .up_to_n_times(1)
        .expect(1)
        .mount(server)
        .await;
    KuboBlockControl {
        reached: Some(reached_rx),
        release: Some(release_tx),
    }
}

#[derive(Clone)]
struct KuboBlocker {
    target: KuboBlockTarget,
    reached: mpsc::Sender<()>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
}

impl KuboBlocker {
    fn block_once(&self, target: KuboBlockTarget) {
        if self.target != target {
            return;
        }
        let Some(release) = self.release.lock().expect("Kubo block mutex").take() else {
            return;
        };
        let _ = self.reached.send(());
        let _ = release.recv_timeout(std::time::Duration::from_secs(10));
    }
}

pub use crate::support::cors::S3ServerHandle;

#[derive(Clone)]
pub struct ObservedHttpRequest {
    pub method: http::Method,
    pub uri: http::Uri,
    pub headers: http::HeaderMap,
}

pub async fn start_kubo_harness(script: KuboScript) -> KuboHarness {
    start_kubo_harness_with_blocker(script, None).await
}

async fn start_kubo_harness_with_blocker(
    script: KuboScript,
    blocker: Option<KuboBlocker>,
) -> KuboHarness {
    let kubo = MockServer::start().await;
    let KuboScript {
        add_replies,
        cat_bodies,
    } = script;
    let add_file_bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let cat_bodies = Arc::new(std::sync::RwLock::new(cat_bodies));

    if !add_replies.is_empty() {
        let reply_count = add_replies.len() as u64;
        let add_replies = Arc::new(std::sync::Mutex::new(VecDeque::from(add_replies)));
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with({
                let add_replies = add_replies.clone();
                let add_file_bytes = add_file_bytes.clone();
                let blocker = blocker.clone();
                move |request: &wiremock::Request| {
                    if let Some(blocker) = &blocker {
                        blocker.block_once(KuboBlockTarget::Add);
                    }
                    add_file_bytes
                        .lock()
                        .expect("add capture mutex")
                        .push(kubo_add_file_bytes(request));
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

    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with({
            let cat_bodies = cat_bodies.clone();
            let blocker = blocker.clone();
            move |request: &wiremock::Request| {
                if let Some(blocker) = &blocker {
                    blocker.block_once(KuboBlockTarget::Cat);
                }
                let arg = request
                    .url
                    .query_pairs()
                    .find(|(name, _)| name == "arg")
                    .map(|(_, value)| value.into_owned());
                match arg
                    .and_then(|arg| cat_bodies.read().expect("cat body map").get(&arg).cloned())
                {
                    Some(body) => {
                        ResponseTemplate::new(200).set_body_bytes(kubo_cat_body(request, body))
                    }
                    None => ResponseTemplate::new(404).set_body_string("unknown scripted CID"),
                }
            }
        })
        .mount(&kubo)
        .await;
    for pin_path in ["/api/v0/pin/add", "/api/v0/pin/rm"] {
        let blocker = blocker.clone();
        let block_target = (pin_path == "/api/v0/pin/add").then_some(KuboBlockTarget::PinAdd);
        Mock::given(method("POST"))
            .and(path(pin_path))
            .respond_with(move |_: &wiremock::Request| {
                if let (Some(blocker), Some(target)) = (&blocker, block_target) {
                    blocker.block_once(target);
                }
                ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}")
            })
            .mount(&kubo)
            .await;
    }

    KuboHarness {
        server: kubo,
        add_file_bytes,
        cat_bodies,
    }
}

pub async fn start_harness(script: KuboScript) -> TestHarness {
    build_harness(start_kubo_harness(script).await).await
}

pub async fn start_blocking_harness(
    script: KuboScript,
    target: KuboBlockTarget,
) -> (TestHarness, KuboBlockControl) {
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let blocker = KuboBlocker {
        target,
        reached: reached_tx,
        release: Arc::new(Mutex::new(Some(release_rx))),
    };
    let kubo = start_kubo_harness_with_blocker(script, Some(blocker)).await;
    let harness = build_harness(kubo).await;
    (
        harness,
        KuboBlockControl {
            reached: Some(reached_rx),
            release: Some(release_tx),
        },
    )
}

async fn build_harness(kubo_harness: KuboHarness) -> TestHarness {
    let KuboHarness {
        server: kubo,
        add_file_bytes,
        cat_bodies,
    } = kubo_harness;

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
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });

    let observed_http = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let server = start_s3_server(state.clone(), observed_http.clone()).await;

    TestHarness {
        endpoint: server.endpoint.clone(),
        bucket,
        state,
        kubo,
        observed_http,
        add_file_bytes,
        cat_bodies,
        _server: server,
    }
}

pub async fn start_s3_server(
    state: Arc<ipfs_s3_gateway::state::AppState>,
    observed_http: Arc<tokio::sync::Mutex<Vec<ObservedHttpRequest>>>,
) -> S3ServerHandle {
    let imports = crate::support::cors::default_import_coordinator();
    start_s3_server_with_imports(state, observed_http, imports).await
}

pub async fn start_s3_server_with_imports(
    state: Arc<ipfs_s3_gateway::state::AppState>,
    observed_http: Arc<tokio::sync::Mutex<Vec<ObservedHttpRequest>>>,
    imports: Arc<ipfs_s3_gateway::import::pipeline::ImportCoordinator>,
) -> S3ServerHandle {
    let app = crate::support::cors::gateway_router(state, imports).layer(
        middleware::from_fn_with_state(observed_http.clone(), observe_request),
    );
    crate::support::cors::start_router(app).await
}

fn kubo_add_file_bytes(request: &wiremock::Request) -> Vec<u8> {
    let content_type = request
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .expect("Kubo add Content-Type");
    let boundary = content_type
        .split("boundary=")
        .nth(1)
        .map(|value| value.trim_matches('"'))
        .expect("multipart boundary");
    let header_end = request
        .body
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("multipart file headers")
        + 4;
    let terminator = format!("\r\n--{boundary}").into_bytes();
    let file_end = request.body[header_end..]
        .windows(terminator.len())
        .position(|window| window == terminator.as_slice())
        .expect("multipart file terminator");
    request.body[header_end..header_end + file_end].to_vec()
}

fn kubo_cat_body(request: &wiremock::Request, body: Vec<u8>) -> Vec<u8> {
    let range = request
        .url
        .query_pairs()
        .find(|(name, _)| name == "bytes")
        .map(|(_, value)| value.into_owned());
    let Some(range) = range else {
        return body;
    };
    let (start, end) = range.split_once('-').expect("Kubo bytes=start-end");
    let start: usize = start.parse().expect("Kubo byte start");
    let end: usize = end.parse().expect("Kubo byte end");
    assert!(start <= end, "Kubo byte range is ascending");
    assert!(end < body.len(), "S3 range is checked before Kubo");
    body[start..=end].to_vec()
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
    create_multipart_with_headers(harness, key, options, http::HeaderMap::new()).await
}

pub async fn create_multipart_with_headers(
    harness: &TestHarness,
    key: &str,
    options: &[(&str, &str)],
    headers: http::HeaderMap,
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
        headers,
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
    upload_part_with_headers(
        harness,
        key,
        upload_id,
        part_number,
        body,
        http::HeaderMap::new(),
    )
    .await
}

pub async fn send_upload_part_with_headers(
    harness: &TestHarness,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
    headers: http::HeaderMap,
) -> reqwest::Response {
    let part_number = part_number.to_string();
    crate::support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[("partNumber", &part_number), ("uploadId", upload_id)],
        body,
        headers,
        "test",
    )
    .await
}

pub async fn upload_part_with_headers(
    harness: &TestHarness,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
    headers: http::HeaderMap,
) -> String {
    let response =
        send_upload_part_with_headers(harness, key, upload_id, part_number, body, headers).await;
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
    harness: &(impl S3TestEndpoint + ?Sized),
    key: &str,
    upload_id: &str,
    parts: &[(i32, String)],
) -> reqwest::Response {
    complete_multipart_with_headers(harness, key, upload_id, parts, http::HeaderMap::new()).await
}

pub async fn complete_multipart_with_headers(
    harness: &(impl S3TestEndpoint + ?Sized),
    key: &str,
    upload_id: &str,
    parts: &[(i32, String)],
    headers: http::HeaderMap,
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
    crate::support::sigv4::send_sigv4(
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
