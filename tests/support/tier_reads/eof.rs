use std::{collections::VecDeque, sync::Arc, time::Duration};

use base64::Engine as _;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    crypto::{ObjectKey, chunker::CHUNK_SIZE},
    state::AppState,
    store::{
        self, Store,
        entities::{object, object_version, residency_reference, version_residency},
    },
};
use sea_orm::{ConnectionTrait, Database, EntityTrait, Set};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

use super::{BUCKET, seed_physical, signed_get, start_s3_server};

const PRIVATE_TRAILER: &str = "private-kubo-late-failure-do-not-leak";
const POLL_WINDOW: Duration = Duration::from_millis(250);
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

const CONTROL_CID: &str = "QmEofCleanControl";
const PLAIN_FULL_CID: &str = "QmEofPlainFull";
const PLAIN_RANGE_CID: &str = "QmEofPlainRange";
const SSE_S3_FULL_CID: &str = "QmEofSseS3Full";
const SSE_S3_RANGE_CID: &str = "QmEofSseS3Range";
const SSE_C_FULL_CID: &str = "QmEofSseCFingerprintFull";
const SSE_C_RANGE_CID: &str = "QmEofSseCFingerprintRange";
const SSE_C_EMPTY_CID: &str = "QmEofSseCFingerprintEmpty";
const SSE_C_LEGACY_CID: &str = "QmEofSseCLegacy";

#[derive(Clone, Copy)]
enum Terminal {
    CleanEof,
    ErrorTrailer,
}

struct CatReply {
    data: Vec<u8>,
    terminal: Terminal,
}

struct CatEvent {
    target: String,
}

struct RawKuboFixture {
    endpoint: String,
    events: mpsc::Receiver<CatEvent>,
    releases: mpsc::Sender<()>,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl RawKuboFixture {
    async fn next_event(&mut self) -> CatEvent {
        tokio::time::timeout(TEST_TIMEOUT, self.events.recv())
            .await
            .expect("raw Kubo must transmit the scripted data without hanging")
            .expect("raw Kubo event channel must remain open")
    }

    async fn release_terminal(&self) {
        self.releases
            .send(())
            .await
            .expect("release raw Kubo trailer/EOF")
    }

    async fn finish(&mut self) {
        let mut task = self.task.take().expect("raw Kubo task is joined once");
        match tokio::time::timeout(TEST_TIMEOUT, &mut task).await {
            Ok(result) => result
                .expect("join raw Kubo task")
                .expect("raw Kubo task completed its script"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("raw Kubo task did not finish its bounded script");
            }
        }
    }
}

impl Drop for RawKuboFixture {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug)]
struct GetOutcome {
    status: StatusCode,
    body: Result<Vec<u8>, String>,
}

struct ErrorCase<'a> {
    name: &'static str,
    key: &'static str,
    cid: &'static str,
    headers: HeaderMap,
    expected_status: StatusCode,
    expected_body: &'a [u8],
    expected_kubo_range: Option<(u64, u64)>,
}

pub(super) async fn assert_fixed_length_gets_wait_for_kubo_eof() {
    let plaintext = vec![0x5a; CHUNK_SIZE];
    let sse_s3_key = ObjectKey { bytes: [9; 32] };
    let sse_c_key = ObjectKey { bytes: [7; 32] };
    let sse_s3_ciphertext = fixed_ciphertext(&sse_s3_key, [0x31; 12], &plaintext);
    let sse_c_ciphertext = fixed_ciphertext(&sse_c_key, [0x42; 12], &plaintext);

    let mut raw = start_raw_kubo(vec![
        CatReply {
            data: b"clean-control".to_vec(),
            terminal: Terminal::CleanEof,
        },
        CatReply {
            data: b"plain-full".to_vec(),
            terminal: Terminal::ErrorTrailer,
        },
        CatReply {
            data: b"2345".to_vec(),
            terminal: Terminal::ErrorTrailer,
        },
        CatReply {
            data: sse_s3_ciphertext.clone(),
            terminal: Terminal::ErrorTrailer,
        },
        CatReply {
            data: sse_s3_ciphertext,
            terminal: Terminal::ErrorTrailer,
        },
        CatReply {
            data: sse_c_ciphertext.clone(),
            terminal: Terminal::ErrorTrailer,
        },
        CatReply {
            data: sse_c_ciphertext.clone(),
            terminal: Terminal::ErrorTrailer,
        },
        CatReply {
            // The current encrypted stream format emits no cipher chunk for an
            // empty plaintext object. The trailer is therefore the only frame.
            data: Vec::new(),
            terminal: Terminal::ErrorTrailer,
        },
        CatReply {
            data: sse_c_ciphertext,
            terminal: Terminal::ErrorTrailer,
        },
    ])
    .await;
    let (state, server) = start_eof_harness(&raw.endpoint).await;

    seed_object(
        &state,
        "eof/clean-control",
        CONTROL_CID,
        13,
        false,
        None,
        None,
    )
    .await;
    seed_object(
        &state,
        "eof/plain-full",
        PLAIN_FULL_CID,
        10,
        false,
        None,
        None,
    )
    .await;
    seed_object(
        &state,
        "eof/plain-range",
        PLAIN_RANGE_CID,
        10,
        false,
        None,
        None,
    )
    .await;

    let sse_s3_wrap = state
        .master_key
        .wrap(&sse_s3_key)
        .expect("wrap SSE-S3 EOF fixture key");
    for (key, cid) in [
        ("eof/sse-s3-full", SSE_S3_FULL_CID),
        ("eof/sse-s3-range", SSE_S3_RANGE_CID),
    ] {
        seed_object(
            &state,
            key,
            cid,
            plaintext.len(),
            true,
            Some(sse_s3_wrap.clone()),
            None,
        )
        .await;
    }

    let sse_c_fingerprint = state.master_key.sse_c_key_fingerprint(&sse_c_key);
    for (key, cid, size) in [
        ("eof/sse-c-full", SSE_C_FULL_CID, plaintext.len()),
        ("eof/sse-c-range", SSE_C_RANGE_CID, plaintext.len()),
        ("eof/sse-c-empty", SSE_C_EMPTY_CID, 0),
    ] {
        seed_object(
            &state,
            key,
            cid,
            size,
            true,
            None,
            Some(sse_c_fingerprint.clone()),
        )
        .await;
    }
    seed_object(
        &state,
        "eof/sse-c-legacy",
        SSE_C_LEGACY_CID,
        plaintext.len(),
        true,
        None,
        None,
    )
    .await;

    run_clean_control(&mut raw, &server.endpoint).await;

    let mut plain_range = HeaderMap::new();
    plain_range.insert(http::header::RANGE, HeaderValue::from_static("bytes=2-5"));
    let mut sse_s3_range = HeaderMap::new();
    sse_s3_range.insert(http::header::RANGE, HeaderValue::from_static("bytes=7-10"));
    let sse_c_headers = sse_c_headers([7; 32]);
    let mut sse_c_range = sse_c_headers.clone();
    sse_c_range.insert(http::header::RANGE, HeaderValue::from_static("bytes=7-10"));

    let cases = vec![
        ErrorCase {
            name: "plain full GET",
            key: "eof/plain-full",
            cid: PLAIN_FULL_CID,
            headers: HeaderMap::new(),
            expected_status: StatusCode::OK,
            expected_body: b"plain-full",
            expected_kubo_range: None,
        },
        ErrorCase {
            name: "plain Range GET",
            key: "eof/plain-range",
            cid: PLAIN_RANGE_CID,
            headers: plain_range,
            expected_status: StatusCode::PARTIAL_CONTENT,
            expected_body: b"2345",
            expected_kubo_range: Some((2, 4)),
        },
        ErrorCase {
            name: "SSE-S3 full GET",
            key: "eof/sse-s3-full",
            cid: SSE_S3_FULL_CID,
            headers: HeaderMap::new(),
            expected_status: StatusCode::OK,
            expected_body: &plaintext,
            expected_kubo_range: None,
        },
        ErrorCase {
            name: "SSE-S3 Range GET",
            key: "eof/sse-s3-range",
            cid: SSE_S3_RANGE_CID,
            headers: sse_s3_range,
            expected_status: StatusCode::PARTIAL_CONTENT,
            expected_body: &plaintext[7..11],
            expected_kubo_range: None,
        },
        ErrorCase {
            name: "fingerprinted SSE-C full GET",
            key: "eof/sse-c-full",
            cid: SSE_C_FULL_CID,
            headers: sse_c_headers.clone(),
            expected_status: StatusCode::OK,
            expected_body: &plaintext,
            expected_kubo_range: None,
        },
        ErrorCase {
            name: "fingerprinted SSE-C Range GET",
            key: "eof/sse-c-range",
            cid: SSE_C_RANGE_CID,
            headers: sse_c_range,
            expected_status: StatusCode::PARTIAL_CONTENT,
            expected_body: &plaintext[7..11],
            expected_kubo_range: None,
        },
        ErrorCase {
            name: "empty fingerprinted SSE-C full GET",
            key: "eof/sse-c-empty",
            cid: SSE_C_EMPTY_CID,
            headers: sse_c_headers.clone(),
            expected_status: StatusCode::OK,
            expected_body: b"",
            expected_kubo_range: None,
        },
        ErrorCase {
            name: "legacy SSE-C full authentication GET",
            key: "eof/sse-c-legacy",
            cid: SSE_C_LEGACY_CID,
            headers: sse_c_headers,
            expected_status: StatusCode::OK,
            expected_body: &plaintext,
            expected_kubo_range: None,
        },
    ];

    let mut failures = Vec::new();
    for case in cases {
        if let Err(failure) = run_error_case(&mut raw, &server.endpoint, case).await {
            failures.push(failure);
        }
    }

    raw.finish().await;
    server.shutdown().await;
    assert!(
        failures.is_empty(),
        "fixed Content-Length GETs accepted a late Kubo X-Stream-Error:\n{}",
        failures.join("\n")
    );
}

async fn run_clean_control(raw: &mut RawKuboFixture, endpoint: &str) {
    let request = spawn_get(endpoint, "eof/clean-control", HeaderMap::new());
    let event = raw.next_event().await;
    raw.release_terminal().await;
    let outcome = join_get(request).await;
    assert_cat_request(&event, CONTROL_CID, None);
    assert_eq!(outcome.status, StatusCode::OK);
    assert_eq!(
        outcome.body.expect("clean delayed EOF body"),
        b"clean-control"
    );
}

async fn run_error_case(
    raw: &mut RawKuboFixture,
    endpoint: &str,
    case: ErrorCase<'_>,
) -> Result<(), String> {
    let mut request = spawn_get(endpoint, case.key, case.headers);
    let event = raw.next_event().await;
    let premature = tokio::time::timeout(POLL_WINDOW, &mut request)
        .await
        .ok()
        .map(|result| result.expect("signed GET task must not panic"));
    raw.release_terminal().await;
    let completed_before_trailer = premature.is_some();
    let outcome = match premature {
        Some(outcome) => outcome,
        None => join_get(request).await,
    };
    assert_cat_request(&event, case.cid, case.expected_kubo_range);

    let clean_expected_body = outcome.status == case.expected_status
        && outcome
            .body
            .as_ref()
            .is_ok_and(|body| body.as_slice() == case.expected_body);
    if completed_before_trailer && clean_expected_body {
        return Err(format!(
            "{} completed successfully before the error trailer was released",
            case.name
        ));
    }

    let rendered = match &outcome.body {
        Ok(body) => String::from_utf8_lossy(body).into_owned(),
        Err(error) => error.clone(),
    };
    if rendered.contains(PRIVATE_TRAILER) {
        return Err(format!(
            "{} leaked the private Kubo trailer: {rendered}",
            case.name
        ));
    }
    if outcome.status.is_success() && outcome.body.is_ok() {
        return Err(format!(
            "{} returned a clean successful response after an error trailer: status={} body_len={}",
            case.name,
            outcome.status,
            outcome.body.as_ref().map_or(0, Vec::len)
        ));
    }
    if let Ok(body) = &outcome.body {
        let body = String::from_utf8_lossy(body);
        if outcome.status != StatusCode::INTERNAL_SERVER_ERROR
            || !body.contains("<Code>InternalError</Code>")
            || !body.contains("<Message>internal storage backend error</Message>")
        {
            return Err(format!(
                "{} did not return a redacted S3 error: status={} body={body}",
                case.name, outcome.status
            ));
        }
    }
    Ok(())
}

fn spawn_get(
    endpoint: &str,
    key: &'static str,
    headers: HeaderMap,
) -> tokio::task::JoinHandle<GetOutcome> {
    let endpoint = endpoint.to_owned();
    tokio::spawn(async move {
        let response = signed_get(&endpoint, key, None, headers).await;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| error.to_string());
        GetOutcome { status, body }
    })
}

async fn join_get(request: tokio::task::JoinHandle<GetOutcome>) -> GetOutcome {
    tokio::time::timeout(TEST_TIMEOUT, request)
        .await
        .expect("signed GET must not hang")
        .expect("signed GET task must not panic")
}

fn assert_cat_request(event: &CatEvent, cid: &str, expected_range: Option<(u64, u64)>) {
    let url = reqwest::Url::parse(&format!("http://raw-kubo.invalid{}", event.target))
        .expect("raw Kubo request target");
    assert_eq!(url.path(), "/api/v0/cat");
    assert_eq!(query_value(&url, "arg").as_deref(), Some(cid));
    match expected_range {
        Some((offset, length)) => {
            assert_eq!(query_value(&url, "offset"), Some(offset.to_string()));
            assert_eq!(query_value(&url, "length"), Some(length.to_string()));
        }
        None => {
            assert_eq!(query_value(&url, "offset"), None);
            assert_eq!(query_value(&url, "length"), None);
        }
    }
}

fn query_value(url: &reqwest::Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

async fn start_raw_kubo(replies: Vec<CatReply>) -> RawKuboFixture {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind raw Kubo listener");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("raw Kubo listener address")
    );
    let (event_tx, events) = mpsc::channel(replies.len());
    let (releases, mut release_rx) = mpsc::channel(replies.len());
    let mut replies = VecDeque::from(replies);
    let task = tokio::spawn(async move {
        while let Some(reply) = replies.pop_front() {
            let (mut socket, _) = tokio::time::timeout(TEST_TIMEOUT, listener.accept())
                .await
                .map_err(|_| "timed out accepting raw Kubo request".to_owned())?
                .map_err(|error| format!("accept raw Kubo request: {error}"))?;
            let request = read_request_headers(&mut socket).await?;
            let target = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .ok_or_else(|| "raw Kubo request line is malformed".to_owned())?
                .to_owned();
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n",
                )
                .await
                .map_err(|error| format!("write raw Kubo headers: {error}"))?;
            if !reply.data.is_empty() {
                socket
                    .write_all(format!("{:X}\r\n", reply.data.len()).as_bytes())
                    .await
                    .map_err(|error| format!("write raw Kubo chunk length: {error}"))?;
                socket
                    .write_all(&reply.data)
                    .await
                    .map_err(|error| format!("write raw Kubo data: {error}"))?;
                socket
                    .write_all(b"\r\n")
                    .await
                    .map_err(|error| format!("write raw Kubo chunk terminator: {error}"))?;
            }
            socket
                .flush()
                .await
                .map_err(|error| format!("flush raw Kubo data: {error}"))?;
            event_tx
                .send(CatEvent { target })
                .await
                .map_err(|_| "raw Kubo event receiver closed".to_owned())?;
            tokio::time::timeout(TEST_TIMEOUT, release_rx.recv())
                .await
                .map_err(|_| "timed out waiting to release raw Kubo terminal frame".to_owned())?
                .ok_or_else(|| "raw Kubo release channel closed".to_owned())?;
            let terminal = match reply.terminal {
                Terminal::CleanEof => b"0\r\n\r\n".to_vec(),
                Terminal::ErrorTrailer => {
                    format!("0\r\nX-Stream-Error: {PRIVATE_TRAILER}\r\n\r\n").into_bytes()
                }
            };
            socket
                .write_all(&terminal)
                .await
                .map_err(|error| format!("write raw Kubo terminal frame: {error}"))?;
            socket
                .shutdown()
                .await
                .map_err(|error| format!("shutdown raw Kubo socket: {error}"))?;
        }
        Ok(())
    });
    RawKuboFixture {
        endpoint,
        events,
        releases,
        task: Some(task),
    }
}

async fn read_request_headers(socket: &mut tokio::net::TcpStream) -> Result<String, String> {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        if request.len() >= 64 * 1024 {
            return Err("raw Kubo request headers exceeded fixture limit".to_owned());
        }
        let mut byte = [0_u8; 1];
        tokio::time::timeout(TEST_TIMEOUT, socket.read_exact(&mut byte))
            .await
            .map_err(|_| "timed out reading raw Kubo request headers".to_owned())?
            .map_err(|error| format!("read raw Kubo request headers: {error}"))?;
        request.push(byte[0]);
    }
    String::from_utf8(request).map_err(|error| format!("raw Kubo request is not UTF-8: {error}"))
}

async fn start_eof_harness(kubo_endpoint: &str) -> (Arc<AppState>, super::S3ServerHandle) {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("connect EOF SQLite database");
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .expect("enable EOF SQLite foreign keys");
    store::run_migrations(&db)
        .await
        .expect("run EOF migrations");
    store::bucket::create(&db, BUCKET, Some("tier-read-owner"))
        .await
        .expect("create EOF bucket");
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo_endpoint.to_owned()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: std::collections::HashMap::from([(
            "test".to_owned(),
            s3s::auth::SecretKey::from("test"),
        )]),
        master_key: ipfs_s3_gateway::crypto::MasterKey::from_hex(&"0".repeat(64))
            .expect("EOF master key"),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let server =
        start_s3_server(state.clone(), Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;
    (state, server)
}

async fn seed_object(
    state: &Arc<AppState>,
    key: &str,
    cid: &str,
    size: usize,
    encrypted: bool,
    key_wrap: Option<String>,
    sse_c_key_fingerprint: Option<String>,
) {
    let db = state.store.db();
    let now = chrono::Utc::now();
    seed_physical(db, "hot", cid, false, now).await;
    let object_id = uuid::Uuid::new_v4().to_string();
    let version_row_id = uuid::Uuid::new_v4().to_string();
    object::Entity::insert(object::ActiveModel {
        id: Set(object_id.clone()),
        bucket: Set(BUCKET.to_owned()),
        key: Set(key.to_owned()),
        cid: Set(cid.to_owned()),
        size: Set(i64::try_from(size).expect("EOF fixture size fits i64")),
        content_type: Set(Some("application/octet-stream".to_owned())),
        etag: Set(cid.to_owned()),
        metadata: Set(None),
        encrypted: Set(encrypted),
        key_wrap: Set(key_wrap),
        sse_c_key_fingerprint: Set(sse_c_key_fingerprint),
        multipart: Set(false),
        is_latest: Set(true),
        created_at: Set(now),
    })
    .exec(db)
    .await
    .expect("seed EOF object");
    object_version::Entity::insert(object_version::ActiveModel {
        id: Set(version_row_id.clone()),
        bucket: Set(BUCKET.to_owned()),
        key: Set(key.to_owned()),
        version_id: Set(None),
        kind: Set("object".to_owned()),
        object_id: Set(Some(object_id.clone())),
        sequence: Set(1),
        is_latest: Set(true),
        lifecycle_age_started_at: Set(now),
        became_noncurrent_at: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(db)
    .await
    .expect("seed EOF object version");
    version_residency::Entity::insert(version_residency::ActiveModel {
        version_row_id: Set(version_row_id.clone()),
        object_id: Set(object_id.clone()),
        primary_tier: Set("hot".to_owned()),
        storage_class: Set("STANDARD".to_owned()),
        cid: Set(cid.to_owned()),
        revision: Set(1),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(db)
    .await
    .expect("seed EOF version residency");
    residency_reference::Entity::insert(residency_reference::ActiveModel {
        owner_kind: Set("version".to_owned()),
        owner_id: Set(version_row_id.clone()),
        reason: Set("retained_version".to_owned()),
        version_row_id: Set(version_row_id),
        object_id: Set(object_id),
        tier: Set("hot".to_owned()),
        cid: Set(cid.to_owned()),
        created_at: Set(now),
    })
    .exec(db)
    .await
    .expect("seed EOF residency reference");
}

fn fixed_ciphertext(key: &ObjectKey, nonce: [u8; 12], plaintext: &[u8]) -> Vec<u8> {
    ipfs_s3_gateway::crypto::aes_gcm::encrypt_chunk(key, &nonce, plaintext)
        .expect("encrypt EOF fixture chunk")
        .to_vec()
}

fn sse_c_headers(key: [u8; 32]) -> HeaderMap {
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
