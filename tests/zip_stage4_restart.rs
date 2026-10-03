//! Real OS-process recovery of a signed source=false URL import.
//!
//! The child is this test executable, NOT CARGO_BIN_EXE_ipfs-s3-gateway:
//! production main intentionally cannot resolve/trust a loopback HTTPS mock.
//! With explicit caller approval, only the startup fixture injects the existing
//! downloader resolver/address-policy/CA seams. S3 routing, SQLite migrations,
//! SHA/artifact binding, leases, extraction and publication are production code.
//! No task abort is used as evidence of a gateway process crash.

#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
}

use std::{
    fs::File,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    error::AppError,
    import::{
        ImportConfig,
        downloader::{
            AddressPolicy, DownloadError, DownloadLimits, ImportResolver,
            ReqwestImportHttpTransport, SourceDownloader,
        },
        pipeline::ImportCoordinator,
    },
    state::AppState,
    store::{
        self,
        entities::{object, object_version, pin_job, pin_lease, pin_lease_target, remote_pin},
        zip::{self, execution, import_intake},
    },
};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, Statement,
};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path, query_param},
};

use support::{
    decompress::{KuboBlockTarget, block_next_kubo_request, legal_single_entry_zip},
    sigv4::send_sigv4,
};

const BUCKET: &str = "zip-stage4-restart";
const SOURCE_KEY: &str = "archive.zip";
const OUTPUT_KEY: &str = "out/file.txt";
const TOKEN: &str = "bound-snapshot-real-process";
const SOURCE_HOST: &str = "downloads.example.test";
const SOURCE_PATH: &str = "/archive.zip?secret=never-render";
const PAYLOAD: &[u8] = b"single entry bytes";
const OLD_SOURCE: &[u8] = b"existing source object stays unchanged";
const NEW_OUTPUT: &[u8] = b"a later independent output version";
const CHILD_TEST: &str = "process_gateway_fixture";

fn raw_cid(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap();
    cid::Cid::new_v1(0x55, hash).to_string()
}

// Small, local HTTPS source using the same CA/resolver pattern as support/import.
// Its gate holds the first GET before any source bytes are sent. The body can
// then be changed while the two gateway processes share the original artifact.
struct Source {
    address: SocketAddr,
    ca: Vec<u8>,
    body: watch::Sender<Vec<u8>>,
    requests: watch::Sender<Vec<String>>,
    release: watch::Sender<bool>,
    shutdown: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Source {
    async fn start(body: Vec<u8>) -> Self {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "IPFS S3 ZIP restart fixture CA");
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca_params, ca_key);
        let key = KeyPair::generate().unwrap();
        let mut server_params = CertificateParams::new(vec![SOURCE_HOST.into()]).unwrap();
        server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let certificate = server_params.signed_by(&key, &issuer).unwrap();
        let tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = watch::channel(body).0;
        let requests = watch::channel(Vec::new()).0;
        let release = watch::channel(false).0;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn({
            let body = body.clone();
            let requests = requests.clone();
            let release = release.clone();
            let shutdown = shutdown.clone();
            async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        joined = connections.join_next(), if !connections.is_empty() => {
                            joined.unwrap().expect("source mock connection");
                        }
                        accepted = listener.accept() => {
                            let (socket, _) = accepted.unwrap();
                            let acceptor = acceptor.clone();
                            let body = body.clone();
                            let requests = requests.clone();
                            let mut release = release.subscribe();
                            let shutdown = shutdown.clone();
                            connections.spawn(async move {
                                tokio::select! {
                                    _ = shutdown.cancelled() => {},
                                    _ = async {
                                        let mut socket = acceptor.accept(socket).await.unwrap();
                                        let mut request = Vec::new();
                                        while !request.ends_with(b"\r\n\r\n") {
                                            assert!(request.len() < 16 * 1024, "bounded mock headers");
                                            request.push(socket.read_u8().await.unwrap());
                                        }
                                        let text = String::from_utf8(request).unwrap();
                                        let target = text.lines().next().unwrap();
                                        assert_eq!(target, format!("GET {SOURCE_PATH} HTTP/1.1"));
                                        requests.send_modify(|rows| rows.push(target.into()));
                                        while !*release.borrow_and_update() {
                                            release.changed().await.unwrap();
                                        }
                                        let bytes = body.borrow().clone();
                                        socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
                                        // Split the full ZIP, including the central directory and
                                        // EOCD. Binding must attest all chunks, not only entry bytes.
                                        for chunk in bytes.chunks(17) {
                                            socket.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await.unwrap();
                                            socket.write_all(chunk).await.unwrap();
                                            socket.write_all(b"\r\n").await.unwrap();
                                        }
                                        socket.write_all(b"0\r\n\r\n").await.unwrap();
                                        socket.flush().await.unwrap();
                                    } => {},
                                }
                            });
                        }
                    }
                }
                while let Some(joined) = connections.join_next().await {
                    joined.expect("join source mock connection");
                }
            }
        });
        Self {
            address,
            ca: ca.der().to_vec(),
            body,
            requests,
            release,
            shutdown,
            task: Some(task),
        }
    }

    fn origin(&self) -> String {
        format!("https://{SOURCE_HOST}:{}", self.address.port())
    }

    async fn wait_for_get(&self) {
        let mut requests = self.requests.subscribe();
        tokio::time::timeout(Duration::from_secs(10), async {
            while requests.borrow_and_update().is_empty() {
                requests.changed().await.unwrap();
            }
        })
        .await
        .expect("gateway A reached the URL mock gate");
    }

    fn get_count(&self) -> usize {
        self.requests.borrow().len()
    }

    async fn stop(&mut self) {
        self.shutdown.cancel();
        self.task.take().unwrap().await.unwrap();
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

struct FixtureResolver(SocketAddr);

#[async_trait::async_trait]
impl ImportResolver for FixtureResolver {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
        if host == SOURCE_HOST && port == self.0.port() {
            Ok(vec![self.0])
        } else {
            Err(DownloadError::Dns)
        }
    }
}

impl AddressPolicy for FixtureResolver {
    fn validate(&self, addresses: &[SocketAddr]) -> Result<(), DownloadError> {
        if self.0.ip().is_loopback() && addresses == [self.0] {
            Ok(())
        } else {
            Err(DownloadError::NotAllowed)
        }
    }
}

// Only the parent invokes this ignored entrypoint with a test-owned config.
// It is a new OS process with its own runtime, DB pool and production worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "child fixture; invoked by the real restart regression with isolated config"]
async fn process_gateway_fixture() {
    let config_file = std::env::var("ZIP_STAGE4_CHILD_CONFIG").expect("parent fixture config");
    let cfg: Config = toml::from_str(&std::fs::read_to_string(config_file).unwrap()).unwrap();
    assert_eq!(
        cfg.imports.lease_duration_secs,
        ImportConfig::default().lease_duration_secs
    );
    let state = AppState::new(&cfg).await.unwrap();
    let config = cfg.imports.validate().unwrap();
    let limits = DownloadLimits {
        connect_timeout: Duration::from_secs(config.raw.connect_timeout_secs),
        idle_timeout: Duration::from_secs(config.raw.idle_timeout_secs),
        max_bytes: config.raw.max_download_bytes,
    };
    let address: SocketAddr = std::env::var("ZIP_STAGE4_SOURCE_ADDRESS")
        .unwrap()
        .parse()
        .unwrap();
    let ca = std::fs::read(std::env::var("ZIP_STAGE4_SOURCE_CA").unwrap()).unwrap();
    let downloader = SourceDownloader::with_components(
        Arc::new(config.clone()),
        Arc::new(FixtureResolver(address)),
        Arc::new(FixtureResolver(address)),
        Arc::new(ReqwestImportHttpTransport::new(
            limits,
            vec![reqwest::Certificate::from_der(&ca).unwrap()],
        )),
    );
    let imports = ImportCoordinator::new(config, downloader);
    let app = support::cors::gateway_router(state.clone(), imports.clone());
    let listener = tokio::net::TcpListener::bind(cfg.server.bind)
        .await
        .unwrap();
    let _worker = imports.start(state, CancellationToken::new());
    axum::serve(listener, app).await.unwrap();
    panic!("parent must kill this gateway process, not stop its task");
}

struct Gateway {
    child: Child,
    log: PathBuf,
    endpoint: String,
    reaped: bool,
}

impl Gateway {
    async fn start(
        directory: &Path,
        config: &Path,
        ca: &Path,
        address: SocketAddr,
        port: u16,
        label: &str,
    ) -> Self {
        let log = directory.join(format!("{label}.log"));
        let output = File::create(&log).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                CHILD_TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .current_dir(directory)
            .env("ZIP_STAGE4_CHILD_CONFIG", config)
            .env("ZIP_STAGE4_SOURCE_ADDRESS", address.to_string())
            .env("ZIP_STAGE4_SOURCE_CA", ca)
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .expect("spawn gateway test binary child");
        let mut gateway = Self {
            child,
            log,
            endpoint: format!("http://127.0.0.1:{port}"),
            reaped: false,
        };
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(status) = gateway.child.try_wait().unwrap() {
                    panic!(
                        "gateway {label} exited {status}: {}",
                        std::fs::read_to_string(&gateway.log).unwrap()
                    );
                }
                if let Ok(response) = client
                    .get(format!("{}/ready", gateway.endpoint))
                    .send()
                    .await
                    && response.status() == StatusCode::OK
                    && response.text().await.unwrap() == "READY"
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            ready.is_ok(),
            "gateway {label} readiness: {}",
            std::fs::read_to_string(&gateway.log).unwrap()
        );
        gateway
    }

    fn kill_and_reap(&mut self) -> ExitStatus {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "gateway exited before the OS kill"
        );
        self.child.kill().expect("OS kill of test-owned gateway");
        let status = self.child.wait().expect("reap killed gateway");
        self.reaped = true;
        assert!(
            !status.success(),
            "crash must not be a successful task shutdown"
        );
        status
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        if !self.reaped {
            if matches!(self.child.try_wait(), Ok(None)) {
                let _ = self.child.kill();
            }
            let _ = self.child.wait();
        }
    }
}

async fn signed(
    endpoint: &str,
    method: reqwest::Method,
    key: &str,
    query: &[(&str, &str)],
    body: Vec<u8>,
    headers: HeaderMap,
) -> reqwest::Response {
    tokio::time::timeout(
        Duration::from_secs(10),
        send_sigv4(method, endpoint, BUCKET, key, query, body, headers, "test"),
    )
    .await
    .expect("bounded signed request")
}

async fn require_ok(response: reqwest::Response) -> (HeaderMap, String) {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    (headers, body)
}

async fn submit(endpoint: &str, xml: &str, sha: &str) -> String {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("content-type", "application/xml"),
        ("x-ipfs3-client-token", TOKEN),
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", TOKEN),
        ("x-ipfs3-zip-expected-sha256", sha),
        ("x-amz-tagging", "ipfs-s3%3Azip-root=false"),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    let response = signed(
        endpoint,
        reqwest::Method::POST,
        SOURCE_KEY,
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        xml.as_bytes().to_vec(),
        headers,
    )
    .await;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert!(!headers.contains_key("etag"));
    assert!(!headers.contains_key("x-amz-version-id"));
    headers["x-ipfs3-import-job-id"].to_str().unwrap().into()
}

async fn status(endpoint: &str, id: &str) -> String {
    require_ok(
        signed(
            endpoint,
            reqwest::Method::GET,
            SOURCE_KEY,
            &[("ipfs3-import", id)],
            Vec::new(),
            HeaderMap::new(),
        )
        .await,
    )
    .await
    .1
}

async fn rows(db: &DatabaseConnection) -> [u64; 6] {
    [
        object::Entity::find().count(db).await.unwrap(),
        object_version::Entity::find().count(db).await.unwrap(),
        pin_job::Entity::find().count(db).await.unwrap(),
        pin_lease::Entity::find().count(db).await.unwrap(),
        pin_lease_target::Entity::find().count(db).await.unwrap(),
        remote_pin::Entity::find().count(db).await.unwrap(),
    ]
}

fn add_bytes(request: &Request) -> Vec<u8> {
    let content_type = request.headers[http::header::CONTENT_TYPE]
        .to_str()
        .unwrap();
    let boundary = content_type
        .split("boundary=")
        .nth(1)
        .unwrap()
        .trim_matches('"');
    let begin = request
        .body
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap()
        + 4;
    let end = format!("\r\n--{boundary}");
    let size = request.body[begin..]
        .windows(end.len())
        .position(|w| w == end.as_bytes())
        .unwrap();
    request.body[begin..begin + size].to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bound_url_snapshot_survives_real_process_kill_restart_and_history_replay() {
    let directory = tempfile::tempdir().unwrap();
    let archive = legal_single_entry_zip();
    let sha = hex::encode(Sha256::digest(&archive));
    let artifact_cid = raw_cid(&archive);
    let payload_cid = raw_cid(PAYLOAD);
    let mut source = Source::start(archive.clone()).await;
    let kubo = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(|request: &Request| {
            let bytes = add_bytes(request);
            ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"Hash": raw_cid(&bytes), "Size": bytes.len().to_string()}),
            )
        })
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(|request: &Request| {
            let cid = request
                .url
                .query_pairs()
                .find(|(k, _)| k == "arg")
                .unwrap()
                .1
                .into_owned();
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [cid]}))
        })
        .mount(&kubo)
        .await;
    for bytes in [&archive[..], PAYLOAD, OLD_SOURCE, NEW_OUTPUT] {
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", raw_cid(bytes)))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
            .mount(&kubo)
            .await;
    }
    let mut extraction_gate = block_next_kubo_request(
        &kubo,
        KuboBlockTarget::Cat,
        ResponseTemplate::new(200).set_body_bytes(archive.clone()),
    )
    .await;
    let db_url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("gateway.sqlite")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let config = directory.path().join("config.toml");
    let ca = directory.path().join("source-ca.der");
    std::fs::write(&ca, &source.ca).unwrap();
    std::fs::write(
        &config,
        format!(
            r#"
[server]
bind = "127.0.0.1:{port}"
[kubo]
rpc_url = "{}"
[storage]
database_url = "{db_url}"
[auth]
credentials = [{{ access_key = "test", secret_key = "test" }}]
[imports]
allowed_https_origins = ["{}"]
poll_interval_ms = 25
"#,
            kubo.uri(),
            source.origin()
        ),
    )
    .unwrap();
    let mut a = Gateway::start(
        directory.path(),
        &config,
        &ca,
        source.address,
        port,
        "gateway-a",
    )
    .await;
    let pid_a = a.child.id();
    require_ok(
        signed(
            &a.endpoint,
            reqwest::Method::PUT,
            "",
            &[],
            Vec::new(),
            HeaderMap::new(),
        )
        .await,
    )
    .await;
    require_ok(
        signed(
            &a.endpoint,
            reqwest::Method::PUT,
            "",
            &[("versioning", "")],
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
            HeaderMap::new(),
        )
        .await,
    )
    .await;
    let (source_headers, _) = require_ok(
        signed(
            &a.endpoint,
            reqwest::Method::PUT,
            SOURCE_KEY,
            &[],
            OLD_SOURCE.to_vec(),
            HeaderMap::new(),
        )
        .await,
    )
    .await;
    let source_version = source_headers["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let db_a = store::connect_database(&db_url).await.unwrap();
    let original_source = object::Entity::find()
        .filter(object::Column::Key.eq(SOURCE_KEY))
        .one(&db_a)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows(&db_a).await, [1, 1, 0, 0, 0, 0]);
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}{SOURCE_PATH}</URL></IPFS3ImportRequest>",
        source.origin()
    );
    let id = submit(&a.endpoint, &xml, &sha).await;
    source.wait_for_get().await;
    let pending = execution::read(&db_a, &id).await.unwrap().unwrap();
    assert_eq!(
        pending.input_sha256, None,
        "a blocked source has not reached complete EOF"
    );
    assert_eq!(pending.input_art_cid, None);
    source.release.send_replace(true);
    extraction_gate.wait_until_blocked().await;
    let bound = execution::read(&db_a, &id).await.unwrap().unwrap();
    assert_eq!(
        bound.state, "pending",
        "crash before extraction/admission/publication"
    );
    assert_eq!(bound.input_sha256.as_deref(), Some(sha.as_str()));
    assert_eq!(bound.input_art_cid.as_deref(), Some(artifact_cid.as_str()));
    assert_eq!(bound.input_art_size, Some(archive.len() as i64));
    assert_eq!(bound.epoch, 1);
    assert!(bound.lease_until.unwrap() > store::database_clock::database_now(&db_a).await.unwrap());
    assert_eq!(rows(&db_a).await, [1, 1, 0, 0, 0, 0]);
    let old_claim = execution::Claim {
        batch_id: id.clone(),
        epoch: bound.epoch,
        worker: bound.worker.clone().unwrap(),
    };
    let exit_a = a.kill_and_reap();
    db_a.close().await.unwrap();
    extraction_gate.release();
    source
        .body
        .send_replace(b"changed source must never be fetched after binding".to_vec());
    let db = store::connect_database(&db_url).await.unwrap();
    assert_eq!(
        execution::read(&db, &id).await.unwrap().unwrap(),
        bound,
        "the complete worker-attested snapshot survived OS termination and a new DB pool"
    );
    // A is dead and reaped. Expire ONLY its test-owned lease with the DB clock;
    // do not shorten the production lease or use a fabricated successor claim.
    let expired = db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE zip_v2_executions SET lease_until=strftime('%Y-%m-%d %H:%M:%f','now','-1 second') WHERE id=? AND epoch=? AND worker=? AND state='pending'",
        [id.clone().into(), bound.epoch.into(), old_claim.worker.clone().into()])).await.unwrap();
    assert_eq!(expired.rows_affected(), 1);
    assert!(
        execution::read(&db, &id)
            .await
            .unwrap()
            .unwrap()
            .lease_until
            .unwrap()
            < store::database_clock::database_now(&db).await.unwrap()
    );
    let mut b = Gateway::start(
        directory.path(),
        &config,
        &ca,
        source.address,
        port,
        "gateway-b",
    )
    .await;
    assert_ne!(
        pid_a,
        b.child.id(),
        "restart must use an independent OS process"
    );
    let ready_xml = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let xml = status(&b.endpoint, &id).await;
            assert!(!xml.contains("<State>failed</State>"), "{xml}");
            if xml.contains("<State>ready</State>") {
                break xml;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("gateway B finished the recovered import");
    assert!(ready_xml.contains(&format!("<ExpectedSHA256>{sha}</ExpectedSHA256>")));
    assert!(ready_xml.contains(&format!("<MeasuredSHA256>{sha}</MeasuredSHA256>")));
    assert!(ready_xml.contains("<Phase>source-free</Phase>"));
    assert!(!ready_xml.contains("never-render"));
    assert!(!ready_xml.contains("<ETag>"));
    assert!(!ready_xml.contains("<VersionId>"));
    let completed = execution::read(&db, &id).await.unwrap().unwrap();
    assert_eq!(completed.state, "completed");
    assert_eq!(completed.epoch, bound.epoch + 1);
    assert_ne!(completed.worker, bound.worker);
    assert_eq!(completed.input_sha256, bound.input_sha256);
    assert_eq!(completed.input_art_cid, bound.input_art_cid);
    assert_eq!(completed.input_art_size, bound.input_art_size);
    assert_eq!(completed.request_fingerprint, bound.request_fingerprint);
    assert_eq!(completed.captured_options, bound.captured_options);
    assert!(!import_intake::renew(&db, &old_claim, 60).await.unwrap());
    assert!(matches!(
        execution::complete(&db, &old_claim, "{}").await,
        Err(AppError::StaleContentMutation)
    ));
    assert_eq!(
        source.get_count(),
        1,
        "B must use the bound artifact, not GET the changed URL"
    );
    assert_eq!(rows(&db).await, [2, 2, 0, 0, 0, 0]);
    let historical = zip::snapshot(&db, &id).await.unwrap().unwrap();
    assert_eq!(historical.batch.state, "published");
    assert!(!historical.batch.source_published);
    assert_eq!(historical.batch.root_status, "disabled");
    assert_eq!(historical.batch.root_cid, None);
    assert_eq!(historical.batch.terminal_result, completed.terminal_result);
    assert!(historical.builds.is_empty());
    assert!(historical.references.is_empty());
    assert_eq!(historical.entries.len(), 1);
    let entry = &historical.entries[0];
    assert_eq!(entry.path, "file.txt");
    assert_eq!(entry.object_key.as_deref(), Some(OUTPUT_KEY));
    assert_eq!(entry.cid.as_deref(), Some(payload_cid.as_str()));
    assert_eq!(entry.size, Some(PAYLOAD.len() as i64));
    let version_row = object_version::Entity::find_by_id(entry.version_row_id.as_ref().unwrap())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(version_row.key, OUTPUT_KEY);
    assert_eq!(version_row.sequence, 1);
    assert!(version_row.is_latest);
    let output = object::Entity::find_by_id(version_row.object_id.as_ref().unwrap())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.cid, payload_cid);
    assert_eq!(output.size, PAYLOAD.len() as i64);
    let output_version = version_row.version_id.as_deref().unwrap();
    let response = signed(
        &b.endpoint,
        reqwest::Method::GET,
        OUTPUT_KEY,
        &[],
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-amz-version-id"], output_version);
    assert_eq!(response.bytes().await.unwrap().as_ref(), PAYLOAD);
    let unchanged_source = object::Entity::find_by_id(&original_source.id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged_source, original_source);
    let source_rows = object_version::Entity::find()
        .filter(object_version::Column::Key.eq(SOURCE_KEY))
        .all(&db)
        .await
        .unwrap();
    assert_eq!(source_rows.len(), 1);
    assert_eq!(
        source_rows[0].version_id.as_deref(),
        Some(source_version.as_str())
    );
    let response = signed(
        &b.endpoint,
        reqwest::Method::GET,
        SOURCE_KEY,
        &[],
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-amz-version-id"], source_version);
    assert_eq!(response.bytes().await.unwrap().as_ref(), OLD_SOURCE);
    let before_overwrite = kubo.received_requests().await.unwrap();
    let adds: Vec<_> = before_overwrite
        .iter()
        .filter(|r| r.url.path() == "/api/v0/add")
        .map(add_bytes)
        .collect();
    assert_eq!(
        adds,
        [OLD_SOURCE.to_vec(), archive.clone(), PAYLOAD.to_vec()],
        "source snapshot is added once; restart adds only the extracted output"
    );
    assert_eq!(
        before_overwrite
            .iter()
            .filter(|r| r.url.path() == "/api/v0/cat"
                && r.url
                    .query_pairs()
                    .any(|(k, v)| k == "arg" && v == artifact_cid))
            .count(),
        2,
        "A reached the extraction gate, B read the stored artifact"
    );
    assert!(
        !before_overwrite
            .iter()
            .any(|r| r.url.path().starts_with("/api/v0/dag/")
                || r.url.path().starts_with("/api/v0/files/")),
        "signed root=false must do zero root RPC"
    );
    // Change the current output independently. Historical SHA, root status and
    // manifest must still describe the exact version actually published by B.
    let (overwritten_headers, _) = require_ok(
        signed(
            &b.endpoint,
            reqwest::Method::PUT,
            OUTPUT_KEY,
            &[],
            NEW_OUTPUT.to_vec(),
            HeaderMap::new(),
        )
        .await,
    )
    .await;
    assert_ne!(overwritten_headers["x-amz-version-id"], output_version);
    let versions_before = object_version::Entity::find()
        .order_by_asc(object_version::Column::Id)
        .all(&db)
        .await
        .unwrap();
    assert_eq!(versions_before.len(), 3);
    let before_rows = rows(&db).await;
    assert_eq!(before_rows, [3, 3, 0, 0, 0, 0]);
    let before_kubo = kubo.received_requests().await.unwrap().len();
    assert_eq!(submit(&b.endpoint, &xml, &sha).await, id);
    assert_eq!(status(&b.endpoint, &id).await, ready_xml);
    tokio::time::sleep(Duration::from_millis(200)).await; // several worker polls
    assert_eq!(
        kubo.received_requests().await.unwrap().len(),
        before_kubo,
        "same token replay/status creates no URL GET, Kubo add or other Kubo work"
    );
    assert_eq!(source.get_count(), 1);
    assert_eq!(
        rows(&db).await,
        before_rows,
        "zero new versions or remote jobs/leases/targets/resources"
    );
    assert_eq!(
        object_version::Entity::find()
            .order_by_asc(object_version::Column::Id)
            .all(&db)
            .await
            .unwrap(),
        versions_before
    );
    assert_eq!(execution::read(&db, &id).await.unwrap().unwrap(), completed);
    let after_replay = zip::snapshot(&db, &id).await.unwrap().unwrap();
    assert_eq!(after_replay.batch, historical.batch);
    assert_eq!(after_replay.entries, historical.entries);
    assert_eq!(
        import_intake::read_for_path(&db, &id, "test", BUCKET, SOURCE_KEY)
            .await
            .unwrap()
            .unwrap()
            .root_status
            .as_deref(),
        Some("disabled")
    );
    let receipt: serde_json::Value =
        serde_json::from_str(completed.terminal_result.as_deref().unwrap()).unwrap();
    assert_eq!(receipt["input_sha256"], sha);
    assert_eq!(receipt["published_count"], 1);
    assert_eq!(receipt["failed_count"], 0);
    // This intentional content GET is separate from replay's zero-I/O check.
    let historical_bytes = signed(
        &b.endpoint,
        reqwest::Method::GET,
        OUTPUT_KEY,
        &[("versionId", output_version)],
        Vec::new(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(historical_bytes.status(), StatusCode::OK);
    assert_eq!(
        historical_bytes.headers()["x-amz-version-id"],
        output_version
    );
    assert_eq!(historical_bytes.bytes().await.unwrap().as_ref(), PAYLOAD);
    let after_get = kubo.received_requests().await.unwrap();
    assert_eq!(after_get.len(), before_kubo + 1);
    assert_eq!(after_get.last().unwrap().url.path(), "/api/v0/cat");
    eprintln!(
        "PROCESS_EVIDENCE child=test-executable pid_a={pid_a} kill_a={exit_a} pid_b={} epoch={}->{} url_gets=1 artifact_adds=1 import_outputs=1 versions_after_independent_overwrite=3 root=disabled replay_new_url_get=0 replay_new_kubo_add=0 replay_new_versions=0 replay_new_remote_jobs=0",
        b.child.id(),
        bound.epoch,
        completed.epoch
    );
    b.kill_and_reap();
    db.close().await.unwrap();
    source.stop().await;
    // Both processes are reaped and both DB pools closed before removing ONLY
    // this test's temporary directory (configs, test CA, logs and SQLite).
    drop(a);
    drop(b);
    drop(extraction_gate);
    drop(kubo);
    let directory_path = directory.path().to_owned();
    directory
        .close()
        .expect("remove test-owned temporary files");
    assert!(
        !directory_path.exists(),
        "no test-owned temporary directory remains"
    );
}
