use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use ipfs_s3_gateway::{
    config::{Config, OptionalPinControlMode},
    crypto::key::MasterKey,
    import::{
        ImportConfig,
        downloader::{
            AddressPolicy, AuthorizedSource, DownloadError, DownloadLimits, DownloadStream,
            ImportHttpTransport, ImportResolver, SourceDownloader,
        },
        pipeline::ImportCoordinator,
        v2_source::{V2SourceError, fetch_v2_zip_source},
    },
    kubo::KuboClient,
    pinning::{config::ValidatedPinningConfig, coordinator::PinningCoordinator},
    state::AppState,
    store::Store,
    zip::extract::ZipExtractionLimits,
};
use sea_orm::Database;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const SIGNED: &str = "https://downloads.example.test/zip?signature=private-token";

struct PublicResolver;

#[async_trait::async_trait]
impl ImportResolver for PublicResolver {
    async fn resolve(&self, _: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
        Ok(vec![SocketAddr::from(([8, 8, 8, 8], port))])
    }
}

struct PublicOnly;

impl AddressPolicy for PublicOnly {
    fn validate(&self, addresses: &[SocketAddr]) -> Result<(), DownloadError> {
        if addresses.iter().any(|addr| addr.ip().is_loopback()) {
            return Err(DownloadError::NotAllowed);
        }
        Ok(())
    }
}

// Exercise the real SourceDownloader authorization path, with WireMock serving
// the source response behind a test-only HTTP transport (no external HTTPS).
struct MockSourceTransport {
    endpoint: String,
}

#[async_trait::async_trait]
impl ImportHttpTransport for MockSourceTransport {
    async fn open(
        &self,
        _: AuthorizedSource,
        _: DownloadLimits,
        _: tokio::sync::watch::Sender<u64>,
        _: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let response = tokio::time::timeout(
            Duration::from_millis(100),
            client.get(format!("{}/zip", self.endpoint)).send(),
        )
        .await
        .map_err(|_| DownloadError::Stalled)?
        .map_err(|_| DownloadError::Connect)?;
        if response.status().is_redirection() {
            return Err(DownloadError::Redirect);
        }
        let total = response.content_length();
        let body = response
            .bytes_stream()
            .map(|frame| frame.map_err(|_| DownloadError::InvalidResponse));
        Ok(DownloadStream {
            body: Box::pin(body),
            total,
            content_type: None,
        })
    }
}

use futures_util::{StreamExt, stream};

async fn setup(
    source: &MockServer,
    kubo: &MockServer,
    max_bytes: u64,
) -> (Arc<ImportCoordinator>, AppState) {
    let config = ImportConfig {
        allowed_https_origins: vec!["https://downloads.example.test".into()],
        max_download_bytes: max_bytes,
        job_timeout_secs: 5,
        ..ImportConfig::default()
    }
    .validate()
    .unwrap();
    let downloader = SourceDownloader::with_components(
        Arc::new(config.clone()),
        Arc::new(PublicResolver),
        Arc::new(PublicOnly),
        Arc::new(MockSourceTransport {
            endpoint: source.uri(),
        }),
    );
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let state = AppState {
        kubo: KuboClient::new_with_timeouts(
            kubo.uri(),
            Duration::from_secs(1),
            Duration::from_millis(200),
        ),
        cold_kubo: None,
        store: Store::new(db),
        credentials: HashMap::new(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: PinningCoordinator::disabled_for_test(),
    };
    (ImportCoordinator::new(config, downloader), state)
}

struct ScriptedTransport {
    frames: Vec<Result<Bytes, DownloadError>>,
    total: Option<u64>,
}

#[async_trait::async_trait]
impl ImportHttpTransport for ScriptedTransport {
    async fn open(
        &self,
        _: AuthorizedSource,
        _: DownloadLimits,
        _: tokio::sync::watch::Sender<u64>,
        _: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        Ok(DownloadStream {
            body: Box::pin(stream::iter(
                self.frames
                    .iter()
                    .map(|frame| match frame {
                        Ok(bytes) => Ok(bytes.clone()),
                        Err(DownloadError::Stalled) => Err(DownloadError::Stalled),
                        Err(_) => Err(DownloadError::InvalidResponse),
                    })
                    .collect::<Vec<_>>(),
            )),
            total: self.total,
            content_type: None,
        })
    }
}

fn descriptor(kind: &str, value: &str) -> String {
    serde_json::to_string(&(kind, value)).unwrap()
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

async fn add_and_pin(kubo: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{CID}\",\"Size\":\"7\"}}\n")),
        )
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
        )
        .mount(kubo)
        .await;
}

#[tokio::test]
async fn zip_archive_byte_budget_applies_before_larger_import_download_budget() {
    let source = MockServer::start().await;
    let kubo = MockServer::start().await;
    let (coordinator, mut state) = setup(&source, &kubo, 1024).await;
    state.pinning = PinningCoordinator::build_with_zip_limits(
        ValidatedPinningConfig::from_config(&toml::from_str::<Config>("").unwrap(), |_| None)
            .unwrap(),
        None,
        OptionalPinControlMode::Strict,
        true,
        &[],
        ZipExtractionLimits::default()
            .with_archive_bytes(3)
            .unwrap(),
    )
    .unwrap();
    add_and_pin(&kubo).await;
    Mock::given(method("GET"))
        .and(path("/zip"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ZIP-abc"))
        .mount(&source)
        .await;
    let result = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("url", SIGNED),
        Some(&digest(b"ZIP-abc")),
        CancellationToken::new(),
        &ZipExtractionLimits::default(),
    )
    .await;
    assert!(matches!(result, Err(V2SourceError::SourceTooLarge)));
    assert!(
        kubo.received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/api/v0/add")
    );
}

#[tokio::test]
async fn url_source_measures_all_bytes_and_rejects_changed_bytes_at_same_url() {
    let source = MockServer::start().await;
    let kubo = MockServer::start().await;
    let (coordinator, state) = setup(&source, &kubo, 1024).await;
    add_and_pin(&kubo).await;
    Mock::given(method("GET"))
        .and(path("/zip"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ZIP-abc"))
        .mount(&source)
        .await;
    let expected = digest(b"ZIP-abc");
    let first = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("url", SIGNED),
        Some(&expected),
        CancellationToken::new(),
        &ZipExtractionLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        (first.cid.as_str(), first.size, first.sha256.as_str()),
        (CID, 7, expected.as_str())
    );
    source.reset().await;
    Mock::given(method("GET"))
        .and(path("/zip"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ZIP-xyz"))
        .mount(&source)
        .await;
    let error = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("url", SIGNED),
        Some(&expected),
        CancellationToken::new(),
        &ZipExtractionLimits::default(),
    )
    .await
    .unwrap_err();
    let measured = digest(b"ZIP-xyz");
    assert_eq!(error.code(), "sha256_mismatch");
    assert_eq!(error.measured_sha256(), Some(measured.as_str()));
    assert!(!error.retryable());
    assert!(!format!("{error:?}").contains("private-token"));
    assert!(!format!("{error:?}").contains(&measured));
    assert_eq!(
        kubo.received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == "/api/v0/pin/add")
            .count(),
        1
    );
}

#[tokio::test]
async fn forbidden_url_and_redirect_cannot_produce_an_artifact() {
    let source = MockServer::start().await;
    let kubo = MockServer::start().await;
    let (coordinator, state) = setup(&source, &kubo, 1024).await;
    let expected = digest(b"zip");
    let denied = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("url", "https://127.0.0.1/internal?token=private-token"),
        Some(&expected),
        CancellationToken::new(),
        &ZipExtractionLimits::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(denied, V2SourceError::SourceDenied);
    Mock::given(method("GET"))
        .and(path("/zip"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("Location", "http://127.0.0.1/private"),
        )
        .mount(&source)
        .await;
    let redirected = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("url", SIGNED),
        Some(&expected),
        CancellationToken::new(),
        &ZipExtractionLimits::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(redirected, V2SourceError::SourceRedirected);
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn cid_pin_inspect_and_complete_cat_are_required_before_return() {
    let source = MockServer::start().await;
    let kubo = MockServer::start().await;
    let (coordinator, state) = setup(&source, &kubo, 1024).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"full zip"))
        .expect(2)
        .mount(&kubo)
        .await;
    let result = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("cid", CID),
        None,
        CancellationToken::new(),
        &ZipExtractionLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        (result.cid.as_str(), result.size, result.sha256.as_str()),
        (CID, 8, digest(b"full zip").as_str())
    );
    kubo.verify().await;
    let requests = kubo.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.url.path())
            .collect::<Vec<_>>(),
        ["/api/v0/cat", "/api/v0/cat", "/api/v0/pin/add"]
    );
    assert!(requests.iter().all(|request| {
        request
            .url
            .query_pairs()
            .any(|(name, value)| name == "arg" && value == CID)
    }));
}

#[tokio::test]
async fn source_short_body_and_late_stream_error_never_pin_or_complete() {
    let source = MockServer::start().await;
    let kubo = MockServer::start().await;
    let (_, state) = setup(&source, &kubo, 1024).await;
    add_and_pin(&kubo).await;
    let config = ImportConfig {
        allowed_https_origins: vec!["https://downloads.example.test".into()],
        ..ImportConfig::default()
    }
    .validate()
    .unwrap();
    for (frames, total, expected_failure) in [
        (
            vec![Ok(Bytes::from_static(b"ZIP"))],
            Some(9),
            V2SourceError::SourceIncomplete,
        ),
        (
            vec![
                Ok(Bytes::from_static(b"ZIP")),
                Err(DownloadError::InvalidResponse),
            ],
            None,
            V2SourceError::SourceIncomplete,
        ),
        (
            vec![Ok(Bytes::from_static(b"ZIP")), Err(DownloadError::Stalled)],
            None,
            V2SourceError::SourceStalled,
        ),
    ] {
        let coordinator = ImportCoordinator::new(
            config.clone(),
            SourceDownloader::with_components(
                Arc::new(config.clone()),
                Arc::new(PublicResolver),
                Arc::new(PublicOnly),
                Arc::new(ScriptedTransport { frames, total }),
            ),
        );
        let result = fetch_v2_zip_source(
            &coordinator,
            &state,
            &descriptor("url", SIGNED),
            Some(&digest(b"ZIP")),
            CancellationToken::new(),
            &ZipExtractionLimits::default(),
        )
        .await;
        assert_eq!(result.unwrap_err(), expected_failure);
        if matches!(
            expected_failure,
            V2SourceError::SourceStalled | V2SourceError::SourceIncomplete
        ) {
            assert!(expected_failure.retryable());
        }
    }
    assert_eq!(
        kubo.received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == "/api/v0/pin/add")
            .count(),
        0
    );
}

#[tokio::test]
async fn wiremock_slow_response_headers_and_cancellation_do_not_return_artifact() {
    let source = MockServer::start().await;
    let kubo = MockServer::start().await;
    let (coordinator, state) = setup(&source, &kubo, 1024).await;
    Mock::given(method("GET"))
        .and(path("/zip"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(300))
                .set_body_bytes(b"ZIP"),
        )
        .mount(&source)
        .await;
    let canceled = CancellationToken::new();
    let trigger = canceled.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        trigger.cancel();
    });
    let error = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("url", SIGNED),
        Some(&digest(b"ZIP")),
        canceled,
        &ZipExtractionLimits::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, V2SourceError::Canceled);
    assert!(kubo.received_requests().await.unwrap().is_empty());

    let stalled = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("url", SIGNED),
        Some(&digest(b"ZIP")),
        CancellationToken::new(),
        &ZipExtractionLimits::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(stalled, V2SourceError::SourceStalled);
    assert!(stalled.retryable());
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn cid_partial_second_cat_is_not_a_verified_source() {
    let source = MockServer::start().await;
    let kubo = MockServer::start().await;
    let (coordinator, state) = setup(&source, &kubo, 1024).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
        )
        .expect(0)
        .mount(&kubo)
        .await;
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(move |_: &wiremock::Request| {
            if recorded.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(200).set_body_bytes(b"full zip")
            } else {
                ResponseTemplate::new(200).set_body_bytes(b"part")
            }
        })
        .mount(&kubo)
        .await;
    let result = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("cid", CID),
        None,
        CancellationToken::new(),
        &ZipExtractionLimits::default(),
    )
    .await;
    assert_eq!(result.unwrap_err(), V2SourceError::SourceIncomplete);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        kubo.received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() == "/api/v0/cat")
    );
    kubo.verify().await;
}

#[tokio::test]
async fn cid_late_stream_error_trailer_is_not_a_verified_source() {
    let source = MockServer::start().await;
    let kubo = MockServer::start().await;
    let (coordinator, mut state) = setup(&source, &kubo, 1024).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    state.kubo = KuboClient::new_with_timeouts(
        format!("http://{}", listener.local_addr().unwrap()),
        Duration::from_secs(1),
        Duration::from_millis(200),
    );
    let finished = CancellationToken::new();
    let stop = finished.clone();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for index in 0..3 {
            let (mut socket, _) = tokio::select! {
                _ = stop.cancelled() => break,
                accepted = listener.accept() => accepted.unwrap(),
            };
            let mut headers = Vec::new();
            loop {
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                headers.push(byte[0]);
                if headers.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            requests.push(String::from_utf8(headers).unwrap());
            let response = match index {
                0 => "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nfull zip".to_owned(),
                1 => "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n8\r\nfull zip\r\n0\r\nX-Stream-Error: private backend failure\r\n\r\n".to_owned(),
                _ => {
                    let body = format!("{{\"Pins\":[\"{CID}\"]}}\n");
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
                }
            };
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        requests
    });
    let error = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor("cid", CID),
        None,
        CancellationToken::new(),
        &ZipExtractionLimits::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, V2SourceError::KuboTransport);
    assert!(!format!("{error:?}").contains("private backend failure"));
    finished.cancel();
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.starts_with("POST /api/v0/cat?"))
    );
}

#[tokio::test]
async fn cid_promise_budget_and_pre_pin_cancellation_fail_without_pin_or_verification() {
    for (name, max, inspected, full, cancel_full, expected, failure) in [
        (
            "promise",
            1024,
            &b"full zip"[..],
            &b"full zip"[..],
            false,
            Some(digest(b"promised archive")),
            V2SourceError::Sha256Mismatch {
                measured_sha256: digest(b"full zip"),
            },
        ),
        (
            "inspect budget",
            3,
            &b"full zip"[..],
            &b"full zip"[..],
            false,
            None,
            V2SourceError::SourceTooLarge,
        ),
        (
            "measured budget",
            4,
            &b"tiny"[..],
            &b"full zip"[..],
            false,
            None,
            V2SourceError::SourceTooLarge,
        ),
        (
            "cancel complete read",
            1024,
            &b"full zip"[..],
            &b"full zip"[..],
            true,
            None,
            V2SourceError::Canceled,
        ),
    ] {
        let source = MockServer::start().await;
        let kubo = MockServer::start().await;
        let (coordinator, state) = setup(&source, &kubo, max).await;
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(move |_: &wiremock::Request| {
                let first = observed.fetch_add(1, Ordering::SeqCst) == 0;
                if !first && cancel_full {
                    trigger.cancel();
                }
                ResponseTemplate::new(200).set_body_bytes(if first { inspected } else { full })
            })
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n")),
            )
            .expect(0)
            .mount(&kubo)
            .await;
        let error = fetch_v2_zip_source(
            &coordinator,
            &state,
            &descriptor("cid", CID),
            expected.as_deref(),
            cancel,
            &ZipExtractionLimits::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(error, failure, "{name}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if inspected.len() as u64 > max { 1 } else { 2 },
            "{name}"
        );
        assert!(
            kubo.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() == "/api/v0/cat"),
            "{name}"
        );
        kubo.verify().await;
    }
}

#[tokio::test]
async fn cid_verified_bytes_still_require_successful_uncanceled_pin_before_return() {
    for cancel_pin in [false, true] {
        let source = MockServer::start().await;
        let kubo = MockServer::start().await;
        let (coordinator, state) = setup(&source, &kubo, 1024).await;
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"full zip"))
            .expect(2)
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(move |_: &wiremock::Request| {
                if cancel_pin {
                    trigger.cancel();
                    ResponseTemplate::new(200)
                        .set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}\n"))
                } else {
                    ResponseTemplate::new(500)
                }
            })
            .expect(1)
            .mount(&kubo)
            .await;
        let error = fetch_v2_zip_source(
            &coordinator,
            &state,
            &descriptor("cid", CID),
            None,
            cancel,
            &ZipExtractionLimits::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            if cancel_pin {
                V2SourceError::Canceled
            } else {
                V2SourceError::KuboTransport
            }
        );
        let requests = kubo.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.url.path())
                .collect::<Vec<_>>(),
            ["/api/v0/cat", "/api/v0/cat", "/api/v0/pin/add"]
        );
        kubo.verify().await;
    }
}
