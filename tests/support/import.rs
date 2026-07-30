use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::time::Duration;

use http::{HeaderMap, Response};
use http_body_util::BodyExt as _;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls::pki_types::PrivateKeyDer;
use sea_orm::{Database, EntityTrait};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::decompress::{
    AddReply, KuboScript, ObservedHttpRequest, S3ServerHandle, S3TestEndpoint,
    start_s3_server_with_imports,
};
use super::sigv4::send_sigv4;
use ipfs_s3_gateway::import::downloader::{
    AddressPolicy, AuthorizedSource, DownloadError, DownloadLimits, DownloadStream,
    ImportHttpTransport, ImportResolver, ReqwestImportHttpTransport, SourceDownloader,
    StrictPublicAddressPolicy,
};
use ipfs_s3_gateway::import::pipeline::{ImportCoordinator, ImportExecutionObserver};
use ipfs_s3_gateway::import::worker::ImportWorkerHandle;
use ipfs_s3_gateway::import::{ImportConfig, ValidatedImportConfig};
use ipfs_s3_gateway::state::AppState;
use ipfs_s3_gateway::store;
use ipfs_s3_gateway::store::entities::import_job;

#[derive(Debug)]
pub struct KuboFileIngress {
    first_file_bytes: tokio::sync::watch::Sender<bool>,
    bytes: Mutex<Vec<u8>>,
}

impl KuboFileIngress {
    fn new() -> Self {
        Self {
            first_file_bytes: tokio::sync::watch::channel(false).0,
            bytes: Mutex::new(Vec::new()),
        }
    }

    pub async fn wait_for_first_file_bytes(&self) -> Vec<u8> {
        let mut ready = self.first_file_bytes.subscribe();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !*ready.borrow_and_update() {
                ready
                    .changed()
                    .await
                    .expect("Kubo ingress readiness sender remains alive");
            }
        })
        .await
        .expect("Kubo received multipart file bytes before source EOF");
        let bytes = self.bytes.lock().expect("Kubo ingress bytes").clone();
        assert!(!bytes.is_empty(), "ready Kubo ingress contains file bytes");
        bytes
    }

    fn observe(&self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let mut first_file_bytes = self.bytes.lock().expect("Kubo ingress bytes");
        if !first_file_bytes.is_empty() {
            return;
        }
        first_file_bytes.extend_from_slice(bytes);
        drop(first_file_bytes);
        self.first_file_bytes.send_replace(true);
    }
}

const SOURCE_HOST: &str = "downloads.example.test";
const DEFAULT_ADD_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

pub struct ImportHarnessConfig {
    pub kubo_script: KuboScript,
    pub max_download_bytes: u64,
    pub worker_concurrency: usize,
    pub poll_interval_ms: u64,
    pub lease_duration_secs: u64,
    pub max_attempts: u32,
    pub streaming_kubo_add: bool,
    pub execution_observer: Option<Arc<dyn ImportExecutionObserver>>,
}

impl Default for ImportHarnessConfig {
    fn default() -> Self {
        Self {
            kubo_script: KuboScript {
                add_replies: (0..32).map(|_| AddReply::Ok(DEFAULT_ADD_CID)).collect(),
                cat_bodies: HashMap::new(),
            },
            max_download_bytes: 1024 * 1024,
            worker_concurrency: 4,
            poll_interval_ms: 10,
            lease_duration_secs: 2,
            max_attempts: 1,
            streaming_kubo_add: false,
            execution_observer: None,
        }
    }
}

#[derive(Debug)]
struct ImportPublicationGateState {
    arrived: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    entered: AtomicBool,
    released: AtomicBool,
    job_id: Mutex<Option<String>>,
}

#[derive(Clone, Debug)]
pub struct ImportPublicationBlockControl {
    state: Arc<ImportPublicationGateState>,
}

impl ImportPublicationBlockControl {
    pub fn new() -> Self {
        Self {
            state: Arc::new(ImportPublicationGateState {
                arrived: tokio::sync::Notify::new(),
                resume: tokio::sync::Notify::new(),
                entered: AtomicBool::new(false),
                released: AtomicBool::new(false),
                job_id: Mutex::new(None),
            }),
        }
    }

    pub async fn wait_until_blocked(&self, expected_job_id: &str) {
        if !self.state.entered.load(Ordering::SeqCst) {
            tokio::time::timeout(Duration::from_secs(10), self.state.arrived.notified())
                .await
                .expect("combined import reached the pre-publication observer");
        }
        assert_eq!(
            self.state
                .job_id
                .lock()
                .expect("publication gate job id")
                .as_deref(),
            Some(expected_job_id)
        );
    }

    pub fn release(&self) {
        self.state.released.store(true, Ordering::SeqCst);
        self.state.resume.notify_one();
    }
}

#[async_trait::async_trait]
impl ImportExecutionObserver for ImportPublicationBlockControl {
    async fn before_publication(&self, job_id: &str) {
        *self.state.job_id.lock().expect("publication gate job id") = Some(job_id.to_owned());
        self.state.entered.store(true, Ordering::SeqCst);
        self.state.arrived.notify_one();
        if !self.state.released.load(Ordering::SeqCst) {
            self.state.resume.notified().await;
        }
    }
}

impl Drop for ImportPublicationBlockControl {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone, Debug)]
pub struct TestHttpsReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub chunks: Vec<Vec<u8>>,
    pub content_length: bool,
    block: Option<Arc<HttpsBlockState>>,
    chunk_gate: Option<Arc<HttpsChunkGateState>>,
}

impl TestHttpsReply {
    pub fn chunked(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            chunks: vec![body.into()],
            content_length: false,
            block: None,
            chunk_gate: None,
        }
    }

    pub fn chunked_chunks(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            chunks,
            content_length: false,
            block: None,
            chunk_gate: None,
        }
    }

    pub fn redirect(location: &str) -> Self {
        Self {
            status: 302,
            headers: vec![("Location".to_owned(), location.to_owned())],
            chunks: Vec::new(),
            content_length: true,
            block: None,
            chunk_gate: None,
        }
    }
}

#[derive(Debug)]
struct HttpsBlockState {
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
    finished: tokio::sync::Notify,
    client_disconnected: tokio::sync::Notify,
    entered: AtomicBool,
}

#[derive(Clone)]
pub struct TestHttpsBlockControl {
    state: Arc<HttpsBlockState>,
}

impl TestHttpsBlockControl {
    pub async fn wait_until_blocked(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.state.reached.notified())
            .await
            .expect("HTTPS request reached deterministic block");
    }

    pub fn release(&self) {
        self.state.release.notify_one();
    }

    pub async fn wait_until_finished(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.state.finished.notified())
            .await
            .expect("blocked HTTPS response finished");
    }
}

impl Drop for TestHttpsBlockControl {
    fn drop(&mut self) {
        self.state.release.notify_one();
    }
}

#[derive(Debug)]
struct HttpsChunkGateState {
    first_chunk_sent: tokio::sync::Notify,
    release: tokio::sync::Notify,
    released: AtomicBool,
}

#[derive(Clone)]
pub struct TestHttpsChunkControl {
    state: Arc<HttpsChunkGateState>,
}

impl TestHttpsChunkControl {
    pub async fn wait_for_first_chunk(&self) {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.state.first_chunk_sent.notified(),
        )
        .await
        .expect("HTTPS source sent the first gated data chunk");
    }

    pub fn is_released(&self) -> bool {
        self.state.released.load(Ordering::SeqCst)
    }

    pub fn release(&self) {
        self.state.released.store(true, Ordering::SeqCst);
        self.state.release.notify_one();
    }
}

impl Drop for TestHttpsChunkControl {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct TestHttpsSource {
    address: SocketAddr,
    root: reqwest::Certificate,
    replies: Arc<RwLock<HashMap<String, TestHttpsReply>>>,
    requests: Arc<Mutex<Vec<String>>>,
    server_names: Arc<Mutex<Vec<String>>>,
    shutdown: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl TestHttpsSource {
    async fn start() -> Self {
        install_rustls_provider();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test HTTPS source");
        let address = listener.local_addr().expect("HTTPS source address");
        let (acceptor, root) = test_server_material(SOURCE_HOST);
        let replies = Arc::new(RwLock::new(HashMap::<String, TestHttpsReply>::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_names = Arc::new(Mutex::new(Vec::new()));
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task_replies = replies.clone();
        let task_requests = requests.clone();
        let task_server_names = server_names.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = task_shutdown.cancelled() => break,
                    joined = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(error)) = joined {
                            panic!("HTTPS source connection task failed: {error}");
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else {
                            break;
                        };
                        let acceptor = acceptor.clone();
                        let replies = task_replies.clone();
                        let requests = task_requests.clone();
                        let server_names = task_server_names.clone();
                        let connection_shutdown = task_shutdown.clone();
                        connections.spawn(async move {
                            serve_https_connection(
                                socket,
                                acceptor,
                                replies,
                                requests,
                                server_names,
                                connection_shutdown,
                            )
                            .await;
                        });
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            address,
            root,
            replies,
            requests,
            server_names,
            shutdown,
            task: Some(task),
        }
    }

    pub fn url(&self, path: &str) -> String {
        assert!(path.starts_with('/'), "HTTPS source path starts with /");
        format!("https://{SOURCE_HOST}:{}{path}", self.address.port())
    }

    pub fn origin(&self) -> String {
        format!("https://{SOURCE_HOST}:{}", self.address.port())
    }

    pub fn set_reply(&self, path: &str, reply: TestHttpsReply) {
        self.replies
            .write()
            .expect("HTTPS reply map")
            .insert(path.to_owned(), reply);
    }

    pub fn set_blocked_chunked_reply(
        &self,
        path: &str,
        body: impl Into<Vec<u8>>,
    ) -> TestHttpsBlockControl {
        let state = Arc::new(HttpsBlockState {
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            finished: tokio::sync::Notify::new(),
            client_disconnected: tokio::sync::Notify::new(),
            entered: AtomicBool::new(false),
        });
        let mut reply = TestHttpsReply::chunked(body);
        reply.block = Some(state.clone());
        self.set_reply(path, reply);
        TestHttpsBlockControl { state }
    }

    pub fn set_chunk_gated_reply(&self, path: &str, chunks: Vec<Vec<u8>>) -> TestHttpsChunkControl {
        assert!(
            chunks.len() >= 2,
            "chunk gate needs data before and after release"
        );
        let state = Arc::new(HttpsChunkGateState {
            first_chunk_sent: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            released: AtomicBool::new(false),
        });
        let mut reply = TestHttpsReply::chunked_chunks(chunks);
        reply.chunk_gate = Some(state.clone());
        self.set_reply(path, reply);
        TestHttpsChunkControl { state }
    }

    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("HTTPS request log").clone()
    }

    pub fn server_names(&self) -> Vec<String> {
        self.server_names.lock().expect("HTTPS SNI log").clone()
    }

    async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(2), &mut task).await {
                Ok(join_result) => join_result.expect("test HTTPS source task failed"),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
    }
}

impl Drop for TestHttpsSource {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn serve_https_connection(
    socket: tokio::net::TcpStream,
    acceptor: TlsAcceptor,
    replies: Arc<RwLock<HashMap<String, TestHttpsReply>>>,
    requests: Arc<Mutex<Vec<String>>>,
    server_names: Arc<Mutex<Vec<String>>>,
    shutdown: CancellationToken,
) {
    let tls = tokio::select! {
        _ = shutdown.cancelled() => return,
        tls = acceptor.accept(socket) => tls,
    };
    let Ok(mut tls) = tls else {
        return;
    };
    server_names.lock().expect("HTTPS SNI log").push(
        tls.get_ref()
            .1
            .server_name()
            .map(ToOwned::to_owned)
            .unwrap_or_default(),
    );
    let mut request = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        let read = tokio::select! {
            _ = shutdown.cancelled() => return,
            read = tls.read_exact(&mut byte) => read,
        };
        if read.is_err() {
            return;
        }
        request.push(byte[0]);
        if request.ends_with(b"\r\n\r\n") || request.len() > 64 * 1024 {
            break;
        }
    }
    let first_line = String::from_utf8_lossy(&request)
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned();
    let target = first_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_owned();
    requests
        .lock()
        .expect("HTTPS request log")
        .push(target.clone());
    let reply = replies
        .read()
        .expect("HTTPS reply map")
        .get(&target)
        .cloned()
        .unwrap_or(TestHttpsReply {
            status: 404,
            headers: Vec::new(),
            chunks: Vec::new(),
            content_length: true,
            block: None,
            chunk_gate: None,
        });
    let active_block = reply
        .block
        .clone()
        .filter(|block| !block.entered.swap(true, Ordering::SeqCst));
    if let Some(block) = active_block.as_ref() {
        block.reached.notify_one();
        let mut disconnect_probe = [0_u8; 1];
        tokio::select! {
            _ = shutdown.cancelled() => {
                block.finished.notify_one();
                return;
            },
            _ = block.release.notified() => {},
            _ = tls.read(&mut disconnect_probe) => {
                block.client_disconnected.notify_one();
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        block.finished.notify_one();
                        return;
                    },
                    _ = block.release.notified() => {}
                }
            }
        }
    }
    write_https_reply(&mut tls, reply).await;
    if let Some(block) = active_block.as_ref() {
        block.finished.notify_one();
    }
}

#[derive(Default)]
struct CountingTransport {
    calls: AtomicUsize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KuboCall {
    path: String,
    arg: Option<String>,
}

struct KuboPinBlocker {
    cid: String,
    reached: mpsc::Sender<()>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
    completed: mpsc::Sender<()>,
    connected: mpsc::Sender<()>,
}

pub struct KuboPinBlockControl {
    reached: Option<mpsc::Receiver<()>>,
    release: Option<mpsc::Sender<()>>,
    completed: Option<mpsc::Receiver<()>>,
    connected: mpsc::Receiver<()>,
}

impl KuboPinBlockControl {
    pub async fn wait_until_blocked(&mut self) {
        let reached = self.reached.take().expect("pin gate is awaited once");
        tokio::task::spawn_blocking(move || {
            reached
                .recv_timeout(Duration::from_secs(10))
                .expect("matching Kubo pin/add reached deterministic gate")
        })
        .await
        .expect("join Kubo pin gate waiter");
    }

    pub fn release(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }

    pub fn assert_not_disconnected(&self) {
        match self.connected.try_recv() {
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => {
                panic!("Kubo pin/add client disconnected before final Pins were released")
            }
            Ok(()) => unreachable!("the Kubo connection-liveness channel never sends values"),
        }
    }

    pub async fn wait_until_response_completed(&mut self) {
        let completed = self
            .completed
            .take()
            .expect("pin response completion is awaited once");
        tokio::task::spawn_blocking(move || {
            completed
                .recv_timeout(Duration::from_secs(10))
                .expect("Kubo pin/add responder completed after release")
        })
        .await
        .expect("join Kubo pin completion waiter");
    }
}

impl Drop for KuboPinBlockControl {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct StrictImportHarness {
    pub endpoint: String,
    pub bucket: String,
    pub state: Arc<AppState>,
    pub kubo: MockServer,
    transport: Arc<CountingTransport>,
    kubo_calls: Arc<Mutex<Vec<KuboCall>>>,
    server: S3ServerHandle,
}

impl S3TestEndpoint for StrictImportHarness {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}

impl StrictImportHarness {
    pub fn transport_calls(&self) -> usize {
        self.transport.calls.load(Ordering::SeqCst)
    }

    pub fn kubo_total_call_count(&self) -> usize {
        self.kubo_calls.lock().expect("Kubo call log").len()
    }

    pub async fn shutdown(self) {
        self.server.shutdown().await;
    }
}

#[async_trait::async_trait]
impl ImportHttpTransport for CountingTransport {
    async fn open(
        &self,
        _source: AuthorizedSource,
        _limits: DownloadLimits,
        _progress: tokio::sync::watch::Sender<u64>,
        _cancel: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(DownloadError::Connect)
    }
}

pub struct ImportHarness {
    pub endpoint: String,
    pub bucket: String,
    pub state: Arc<AppState>,
    pub coordinator: Arc<ImportCoordinator>,
    pub kubo: MockServer,
    pub source: TestHttpsSource,
    pub worker: TestImportWorkerGuard,
    cat_bodies: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    add_file_bytes: Arc<Mutex<Vec<Vec<u8>>>>,
    kubo_calls: Arc<Mutex<Vec<KuboCall>>>,
    pin_gate: Arc<Mutex<Option<KuboPinBlocker>>>,
    kubo_file_ingress: Option<Arc<KuboFileIngress>>,
    streaming_kubo: Option<StreamingKuboServer>,
    server: S3ServerHandle,
}

pub struct TestImportWorkerGuard {
    external_shutdown: CancellationToken,
    handle: Option<ImportWorkerHandle>,
}

impl TestImportWorkerGuard {
    fn start(coordinator: &Arc<ImportCoordinator>, state: Arc<AppState>) -> Self {
        let external_shutdown = CancellationToken::new();
        let handle = coordinator.start(state, external_shutdown.clone());
        Self {
            external_shutdown,
            handle: Some(handle),
        }
    }

    fn without_handle(external_shutdown: CancellationToken) -> Self {
        Self {
            external_shutdown,
            handle: None,
        }
    }

    pub async fn shutdown(mut self, grace: Duration) {
        self.external_shutdown.cancel();
        if let Some(handle) = self.handle.take() {
            handle.shutdown(grace).await;
        }
    }
}

impl Drop for TestImportWorkerGuard {
    fn drop(&mut self) {
        self.external_shutdown.cancel();
    }
}

#[test]
fn test_import_worker_guard_drop_cancels_external_shutdown() {
    let external_shutdown = CancellationToken::new();
    let guard = TestImportWorkerGuard::without_handle(external_shutdown.clone());
    assert!(!external_shutdown.is_cancelled());

    drop(guard);

    assert!(external_shutdown.is_cancelled());
}

impl S3TestEndpoint for ImportHarness {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}

impl ImportHarness {
    pub fn set_cat_body(&self, cid: &str, body: Vec<u8>) {
        self.cat_bodies
            .write()
            .expect("cat body map")
            .insert(cid.to_owned(), body);
    }

    pub fn captured_add_file_bytes(&self) -> Vec<Vec<u8>> {
        self.add_file_bytes
            .lock()
            .expect("add capture mutex")
            .clone()
    }

    pub async fn kubo_args(&self, request_path: &str) -> Vec<String> {
        self.kubo_calls
            .lock()
            .expect("Kubo call log")
            .iter()
            .filter(|call| call.path == request_path)
            .filter_map(|call| call.arg.clone())
            .collect()
    }

    pub async fn kubo_call_count(&self, request_path: &str) -> usize {
        self.kubo_calls
            .lock()
            .expect("Kubo call log")
            .iter()
            .filter(|call| call.path == request_path)
            .count()
    }

    pub async fn kubo_total_call_count(&self) -> usize {
        self.kubo_calls.lock().expect("Kubo call log").len()
    }

    pub fn block_pin_add_for(&self, cid: &str) -> KuboPinBlockControl {
        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let (connected_tx, connected_rx) = mpsc::channel();
        let mut gate = self.pin_gate.lock().expect("Kubo pin gate");
        assert!(gate.is_none(), "only one Kubo pin gate may be armed");
        *gate = Some(KuboPinBlocker {
            cid: cid.to_owned(),
            reached: reached_tx,
            release: Arc::new(Mutex::new(Some(release_rx))),
            completed: completed_tx,
            connected: connected_tx,
        });
        KuboPinBlockControl {
            reached: Some(reached_rx),
            release: Some(release_tx),
            completed: Some(completed_rx),
            connected: connected_rx,
        }
    }

    pub fn kubo_file_ingress_probe(&self) -> Arc<KuboFileIngress> {
        self.kubo_file_ingress
            .clone()
            .expect("streaming Kubo add fixture is enabled")
    }

    pub fn start_additional_worker(&self) -> TestImportWorkerGuard {
        TestImportWorkerGuard::start(&self.coordinator, self.state.clone())
    }

    pub async fn shutdown(self) {
        self.worker.shutdown(Duration::from_secs(2)).await;
        self.server.shutdown().await;
        self.source.shutdown().await;
        if let Some(streaming_kubo) = self.streaming_kubo {
            streaming_kubo.shutdown().await;
        }
    }
}

pub async fn start_import_harness(config: ImportHarnessConfig) -> ImportHarness {
    let source = TestHttpsSource::start().await;
    let kubo_harness =
        start_import_kubo_harness(config.kubo_script, config.streaming_kubo_add).await;
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite database");
    store::run_migrations(&db)
        .await
        .expect("run test migrations");
    let bucket = "test-bkt".to_owned();
    store::bucket::create(&db, &bucket, None)
        .await
        .expect("create test bucket");
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo_harness.endpoint.clone()),
        store: store::Store::new(db),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64))
            .expect("zero test master key"),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let raw_config = ImportConfig {
        allowed_https_origins: vec![source.origin()],
        worker_concurrency: config.worker_concurrency,
        poll_interval_ms: config.poll_interval_ms,
        lease_duration_secs: config.lease_duration_secs,
        progress_flush_interval_ms: 10,
        connect_timeout_secs: 2,
        idle_timeout_secs: 15,
        job_timeout_secs: 30,
        max_download_bytes: config.max_download_bytes,
        max_attempts: config.max_attempts,
        ..ImportConfig::default()
    };
    let validated = raw_config
        .validate()
        .expect("validate import test configuration");
    let limits = download_limits(&validated);
    let downloader = SourceDownloader::with_components(
        Arc::new(validated.clone()),
        Arc::new(FixedResolver(source.address)),
        Arc::new(PermitLoopbackForTest),
        Arc::new(ReqwestImportHttpTransport::new(
            limits,
            vec![source.root.clone()],
        )),
    );
    let coordinator = if let Some(observer) = config.execution_observer {
        ImportCoordinator::new_with_observer(validated, downloader, observer)
    } else {
        ImportCoordinator::new(validated, downloader)
    };
    let observed_http = Arc::new(tokio::sync::Mutex::new(Vec::<ObservedHttpRequest>::new()));
    let server =
        start_s3_server_with_imports(state.clone(), observed_http, coordinator.clone()).await;
    let worker = TestImportWorkerGuard::start(&coordinator, state.clone());

    ImportHarness {
        endpoint: server.endpoint.clone(),
        bucket,
        state,
        coordinator,
        kubo: kubo_harness.server,
        source,
        worker,
        cat_bodies: kubo_harness.cat_bodies,
        add_file_bytes: kubo_harness.add_file_bytes,
        kubo_calls: kubo_harness.calls,
        pin_gate: kubo_harness.pin_gate,
        kubo_file_ingress: kubo_harness.file_ingress,
        streaming_kubo: kubo_harness.streaming_server,
        server,
    }
}

pub async fn start_strict_import_harness() -> StrictImportHarness {
    let kubo_harness = start_import_kubo_harness(
        KuboScript {
            add_replies: Vec::new(),
            cat_bodies: HashMap::new(),
        },
        false,
    )
    .await;
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("strict harness SQLite database");
    store::run_migrations(&db)
        .await
        .expect("strict harness migrations");
    let bucket = "strict-bkt".to_owned();
    store::bucket::create(&db, &bucket, None)
        .await
        .expect("strict harness bucket");
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo_harness.endpoint.clone()),
        store: store::Store::new(db),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64))
            .expect("zero test master key"),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let validated = ImportConfig {
        allowed_https_origins: vec![format!("https://{SOURCE_HOST}")],
        ..ImportConfig::default()
    }
    .validate()
    .expect("strict import harness configuration");
    let transport = Arc::new(CountingTransport::default());
    let downloader = SourceDownloader::with_components(
        Arc::new(validated.clone()),
        Arc::new(FixedResolver(SocketAddr::from(([127, 0, 0, 1], 443)))),
        Arc::new(StrictPublicAddressPolicy),
        transport.clone(),
    );
    let coordinator = ImportCoordinator::new(validated, downloader);
    let observed_http = Arc::new(tokio::sync::Mutex::new(Vec::<ObservedHttpRequest>::new()));
    let server =
        start_s3_server_with_imports(state.clone(), observed_http, coordinator.clone()).await;
    StrictImportHarness {
        endpoint: server.endpoint.clone(),
        bucket,
        state,
        kubo: kubo_harness.server,
        transport,
        kubo_calls: kubo_harness.calls,
        server,
    }
}

pub async fn post_import<H: S3TestEndpoint>(
    harness: &H,
    bucket: &str,
    key: &str,
    query: &str,
    xml: &str,
    client_token: Option<&str>,
) -> Response<Vec<u8>> {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/xml"),
    );
    if let Some(token) = client_token {
        headers.insert(
            "x-ipfs3-client-token",
            http::HeaderValue::from_str(token).expect("valid import client token"),
        );
    }
    let query = query
        .split('&')
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .collect::<Vec<_>>();
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        send_sigv4(
            reqwest::Method::POST,
            harness.endpoint(),
            bucket,
            key,
            &query,
            xml.as_bytes().to_vec(),
            headers,
            "test",
        ),
    )
    .await
    .expect("signed import POST completed within timeout");
    buffered_response(response).await
}

pub async fn get_import_status(
    harness: &ImportHarness,
    bucket: &str,
    key: &str,
    job_id: &str,
    max_results: Option<u64>,
    continuation_token: Option<&str>,
) -> Response<Vec<u8>> {
    let max_results = max_results.map(|value| value.to_string());
    let mut query = vec![("ipfs3-import", job_id)];
    if let Some(max_results) = max_results.as_deref() {
        query.push(("max-results", max_results));
    }
    if let Some(token) = continuation_token {
        query.push(("continuation-token", token));
    }
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        send_sigv4(
            reqwest::Method::GET,
            &harness.endpoint,
            bucket,
            key,
            &query,
            Vec::new(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
    .expect("signed import status GET completed within timeout");
    buffered_response(response).await
}

pub async fn wait_for_import_state(
    harness: &ImportHarness,
    job_id: &str,
    expected: &[&str],
) -> import_job::Model {
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            let job = import_job::Entity::find_by_id(job_id)
                .one(harness.state.store.db())
                .await
                .expect("query import job");
            if let Some(job) = job
                && expected.contains(&job.state.as_str())
            {
                return job;
            }
        }
    })
    .await;
    match result {
        Ok(job) => job,
        Err(_) => {
            let current = import_job::Entity::find_by_id(job_id)
                .one(harness.state.store.db())
                .await
                .expect("query timed-out import job");
            panic!("timed out waiting for import {job_id} in {expected:?}; current={current:?}")
        }
    }
}

async fn buffered_response(response: reqwest::Response) -> Response<Vec<u8>> {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await.expect("read response body").to_vec();
    let mut result = Response::builder().status(status);
    *result.headers_mut().expect("response headers") = headers;
    result.body(body).expect("buffered HTTP response")
}

struct FixedResolver(SocketAddr);

#[async_trait::async_trait]
impl ImportResolver for FixedResolver {
    async fn resolve(&self, host: &str, _port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
        if host == SOURCE_HOST {
            Ok(vec![self.0])
        } else {
            Err(DownloadError::Dns)
        }
    }
}

struct PermitLoopbackForTest;

impl AddressPolicy for PermitLoopbackForTest {
    fn validate(&self, addresses: &[SocketAddr]) -> Result<(), DownloadError> {
        if addresses.len() == 1 && addresses[0].ip().is_loopback() {
            Ok(())
        } else {
            Err(DownloadError::NotAllowed)
        }
    }
}

fn download_limits(config: &ValidatedImportConfig) -> DownloadLimits {
    DownloadLimits {
        connect_timeout: Duration::from_secs(config.raw.connect_timeout_secs),
        idle_timeout: Duration::from_secs(config.raw.idle_timeout_secs),
        max_bytes: config.raw.max_download_bytes,
    }
}

struct ImportKuboHarness {
    server: MockServer,
    endpoint: String,
    add_file_bytes: Arc<Mutex<Vec<Vec<u8>>>>,
    cat_bodies: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    calls: Arc<Mutex<Vec<KuboCall>>>,
    pin_gate: Arc<Mutex<Option<KuboPinBlocker>>>,
    file_ingress: Option<Arc<KuboFileIngress>>,
    streaming_server: Option<StreamingKuboServer>,
}

async fn start_import_kubo_harness(script: KuboScript, streaming_add: bool) -> ImportKuboHarness {
    if streaming_add {
        return start_streaming_import_kubo_harness(script).await;
    }
    let server = MockServer::start().await;
    let KuboScript {
        add_replies,
        cat_bodies,
    } = script;
    let add_file_bytes = Arc::new(Mutex::new(Vec::new()));
    let cat_bodies = Arc::new(RwLock::new(cat_bodies));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let pin_gate = Arc::new(Mutex::new(None::<KuboPinBlocker>));
    let add_replies = Arc::new(Mutex::new(VecDeque::from(add_replies)));
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with({
            let add_replies = add_replies.clone();
            let add_file_bytes = add_file_bytes.clone();
            let calls = calls.clone();
            move |request: &wiremock::Request| {
                record_kubo_call(&calls, request.url.path(), query_arg(request));
                add_file_bytes
                    .lock()
                    .expect("add capture mutex")
                    .push(kubo_add_file_bytes(request));
                match add_replies
                    .lock()
                    .expect("scripted add reply mutex")
                    .pop_front()
                    .expect("unexpected /api/v0/add call")
                {
                    AddReply::Ok(cid) => ResponseTemplate::new(200)
                        .set_body_string(format!("{{\"Hash\":\"{cid}\",\"Size\":\"0\"}}\n")),
                    AddReply::Error(status, body) => {
                        ResponseTemplate::new(status.as_u16()).set_body_string(body)
                    }
                }
            }
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with({
            let cat_bodies = cat_bodies.clone();
            let calls = calls.clone();
            move |request: &wiremock::Request| {
                let arg = query_arg(request);
                record_kubo_call(&calls, request.url.path(), arg.clone());
                match arg
                    .and_then(|arg| cat_bodies.read().expect("cat body map").get(&arg).cloned())
                {
                    Some(body) => ResponseTemplate::new(200).set_body_bytes(body),
                    None => ResponseTemplate::new(404).set_body_string("unknown scripted CID"),
                }
            }
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/routing/findprovs"))
        .respond_with({
            let calls = calls.clone();
            move |request: &wiremock::Request| {
                record_kubo_call(&calls, request.url.path(), query_arg(request));
                ResponseTemplate::new(200).set_body_string(
                    "{\"Type\":0,\"Responses\":null}\n{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"},{\"ID\":\"provider-b\"}]}\n",
                )
            }
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with({
            let calls = calls.clone();
            let pin_gate = pin_gate.clone();
            move |request: &wiremock::Request| {
                let cid = query_arg(request).unwrap_or_default();
                record_kubo_call(&calls, request.url.path(), Some(cid.clone()));
                block_matching_pin_response(&pin_gate, &cid);
                ResponseTemplate::new(200).set_body_string(format!(
                    "{{\"Progress\":3,\"Bytes\":15}}\n{{\"Pins\":[\"{cid}\"]}}\n"
                ))
            }
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/rm"))
        .respond_with({
            let calls = calls.clone();
            move |request: &wiremock::Request| {
                record_kubo_call(&calls, request.url.path(), query_arg(request));
                ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}")
            }
        })
        .mount(&server)
        .await;

    let endpoint = server.uri();
    ImportKuboHarness {
        server,
        endpoint,
        add_file_bytes,
        cat_bodies,
        calls,
        pin_gate,
        file_ingress: None,
        streaming_server: None,
    }
}

fn record_kubo_call(calls: &Arc<Mutex<Vec<KuboCall>>>, path: &str, arg: Option<String>) {
    calls.lock().expect("Kubo call log").push(KuboCall {
        path: path.to_owned(),
        arg,
    });
}

fn block_matching_pin_response(pin_gate: &Arc<Mutex<Option<KuboPinBlocker>>>, requested_cid: &str) {
    let blocker = take_matching_pin_gate(pin_gate, requested_cid);
    let Some(blocker) = blocker else {
        return;
    };
    let KuboPinBlocker {
        reached,
        release,
        completed,
        ..
    } = blocker;
    let _ = reached.send(());
    let release = release
        .lock()
        .expect("Kubo pin gate release")
        .take()
        .expect("Kubo pin gate blocks once");
    tokio::task::block_in_place(|| {
        release
            .recv_timeout(Duration::from_secs(10))
            .expect("Kubo pin gate released within timeout")
    });
    let _ = completed.send(());
}

fn take_matching_pin_gate(
    pin_gate: &Arc<Mutex<Option<KuboPinBlocker>>>,
    requested_cid: &str,
) -> Option<KuboPinBlocker> {
    let mut slot = pin_gate.lock().expect("Kubo pin gate");
    if slot
        .as_ref()
        .is_some_and(|blocker| blocker.cid == requested_cid)
    {
        slot.take()
    } else {
        None
    }
}

struct StreamingKuboServer {
    shutdown: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl StreamingKuboServer {
    async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(2), &mut task).await {
                Ok(join_result) => join_result.expect("streaming Kubo server task failed"),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
    }
}

impl Drop for StreamingKuboServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct StreamingKuboState {
    add_replies: Arc<Mutex<VecDeque<AddReply>>>,
    add_file_bytes: Arc<Mutex<Vec<Vec<u8>>>>,
    cat_bodies: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    calls: Arc<Mutex<Vec<KuboCall>>>,
    pin_gate: Arc<Mutex<Option<KuboPinBlocker>>>,
    ingress: Arc<KuboFileIngress>,
}

async fn start_streaming_import_kubo_harness(script: KuboScript) -> ImportKuboHarness {
    let server = MockServer::start().await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind streaming Kubo fixture");
    let address = listener.local_addr().expect("streaming Kubo address");
    let add_file_bytes = Arc::new(Mutex::new(Vec::new()));
    let cat_bodies = Arc::new(RwLock::new(script.cat_bodies));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let pin_gate = Arc::new(Mutex::new(None));
    let ingress = Arc::new(KuboFileIngress::new());
    let state = StreamingKuboState {
        add_replies: Arc::new(Mutex::new(VecDeque::from(script.add_replies))),
        add_file_bytes: add_file_bytes.clone(),
        cat_bodies: cat_bodies.clone(),
        calls: calls.clone(),
        pin_gate: pin_gate.clone(),
        ingress: ingress.clone(),
    };
    let app = axum::Router::new()
        .fallback(streaming_kubo_handler)
        .with_state(state);
    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(task_shutdown.cancelled_owned())
            .await
            .expect("serve streaming Kubo fixture");
    });
    let endpoint = format!("http://{address}");
    ImportKuboHarness {
        server,
        endpoint,
        add_file_bytes,
        cat_bodies,
        calls,
        pin_gate,
        file_ingress: Some(ingress),
        streaming_server: Some(StreamingKuboServer {
            shutdown,
            task: Some(task),
        }),
    }
}

async fn streaming_kubo_handler(
    axum::extract::State(state): axum::extract::State<StreamingKuboState>,
    uri: http::Uri,
    headers: HeaderMap,
    mut body: axum::body::Body,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let request_path = uri.path().to_owned();
    let progress_pin = uri.query().is_some_and(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .any(|(name, value)| name == "progress" && value == "true")
    });
    let arg = uri.query().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(name, _)| name == "arg")
            .map(|(_, value)| value.into_owned())
    });
    record_kubo_call(&state.calls, &request_path, arg.clone());
    match request_path.as_str() {
        "/api/v0/add" => {
            let mut request_body = Vec::new();
            let mut file_start = None;
            while let Some(frame) = body.frame().await {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(_) => {
                        return (http::StatusCode::BAD_REQUEST, "request body failed")
                            .into_response();
                    }
                };
                let Ok(data) = frame.into_data() else {
                    continue;
                };
                request_body.extend_from_slice(&data);
                if file_start.is_none() {
                    file_start = request_body
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|offset| offset + 4);
                }
                if let Some(start) = file_start
                    && request_body.len() > start
                {
                    state.ingress.observe(&request_body[start..]);
                }
            }
            let file_bytes = kubo_add_file_bytes_raw(&headers, &request_body);
            state
                .add_file_bytes
                .lock()
                .expect("add capture mutex")
                .push(file_bytes);
            let reply = state
                .add_replies
                .lock()
                .expect("scripted add reply mutex")
                .pop_front()
                .expect("unexpected /api/v0/add call");
            match reply {
                AddReply::Ok(cid) => (
                    http::StatusCode::OK,
                    format!("{{\"Hash\":\"{cid}\",\"Size\":\"0\"}}\n"),
                )
                    .into_response(),
                AddReply::Error(status, message) => (status, message).into_response(),
            }
        }
        "/api/v0/cat" => match arg.and_then(|cid| {
            state
                .cat_bodies
                .read()
                .expect("cat body map")
                .get(&cid)
                .cloned()
        }) {
            Some(bytes) => (http::StatusCode::OK, bytes).into_response(),
            None => (http::StatusCode::NOT_FOUND, "unknown scripted CID").into_response(),
        },
        "/api/v0/routing/findprovs" => (
            http::StatusCode::OK,
            "{\"Type\":0,\"Responses\":null}\n{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"},{\"ID\":\"provider-b\"}]}\n",
        )
            .into_response(),
        "/api/v0/pin/add" => {
            let cid = arg.unwrap_or_default();
            if let Some(blocker) = take_matching_pin_gate(&state.pin_gate, &cid) {
                if progress_pin {
                    let stream = gated_pin_response_stream(cid, blocker);
                    http::Response::builder()
                        .status(http::StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, "application/x-ndjson")
                        .body(axum::body::Body::from_stream(stream))
                        .expect("gated pin response")
                } else {
                    release_pin_gate_before_headers(blocker).await;
                    (
                        http::StatusCode::OK,
                        format!("{{\"Pins\":[\"{cid}\"]}}\n"),
                    )
                        .into_response()
                }
            } else {
                (
                    http::StatusCode::OK,
                    format!(
                        "{{\"Progress\":3,\"Bytes\":15}}\n{{\"Pins\":[\"{cid}\"]}}\n"
                    ),
                )
                    .into_response()
            }
        }
        "/api/v0/pin/rm" => (http::StatusCode::OK, "{\"Pins\":[]}").into_response(),
        _ => (http::StatusCode::NOT_FOUND, "unknown Kubo endpoint").into_response(),
    }
}

async fn release_pin_gate_before_headers(blocker: KuboPinBlocker) {
    let KuboPinBlocker {
        reached,
        release,
        completed,
        connected: _connected,
        ..
    } = blocker;
    let _ = reached.send(());
    let release = release
        .lock()
        .expect("Kubo pin gate release")
        .take()
        .expect("Kubo pin gate blocks once");
    tokio::task::spawn_blocking(move || {
        release
            .recv_timeout(Duration::from_secs(10))
            .expect("Kubo pin gate released within timeout")
    })
    .await
    .expect("join Kubo pin release waiter");
    let _ = completed.send(());
}

fn gated_pin_response_stream(
    cid: String,
    blocker: KuboPinBlocker,
) -> impl futures_util::Stream<Item = Result<bytes::Bytes, std::convert::Infallible>> + Send {
    async_stream::stream! {
        let KuboPinBlocker {
            reached,
            release,
            completed,
            connected: _connected,
            ..
        } = blocker;
        yield Ok(bytes::Bytes::from_static(b"{\"Progress\":3,\"Bytes\":15}\n"));
        let _ = reached.send(());
        let release = release
            .lock()
            .expect("Kubo pin gate release")
            .take()
            .expect("Kubo pin gate blocks once");
        tokio::task::spawn_blocking(move || {
            release
                .recv_timeout(Duration::from_secs(10))
                .expect("Kubo pin gate released within timeout")
        })
        .await
        .expect("join Kubo pin release waiter");
        yield Ok(bytes::Bytes::from(format!("{{\"Pins\":[\"{cid}\"]}}\n")));
        let _ = completed.send(());
    }
}

fn kubo_add_file_bytes_raw(headers: &HeaderMap, body: &[u8]) -> Vec<u8> {
    let content_type = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .expect("Kubo add Content-Type");
    let boundary = content_type
        .split("boundary=")
        .nth(1)
        .map(|value| value.trim_matches('"'))
        .expect("multipart boundary");
    let header_end = body
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("multipart file headers")
        + 4;
    let terminator = format!("\r\n--{boundary}").into_bytes();
    let file_end = body[header_end..]
        .windows(terminator.len())
        .position(|window| window == terminator.as_slice())
        .expect("multipart file terminator");
    body[header_end..header_end + file_end].to_vec()
}

fn query_arg(request: &wiremock::Request) -> Option<String> {
    request
        .url
        .query_pairs()
        .find(|(name, _)| name == "arg")
        .map(|(_, value)| value.into_owned())
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

fn install_rustls_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn test_server_material(server_name: &str) -> (TlsAcceptor, reqwest::Certificate) {
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "IPFS S3 import integration test CA");
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().expect("generate test CA key");
    let ca_certificate = ca_params.self_signed(&ca_key).expect("self-sign test CA");
    let issuer = Issuer::new(ca_params, ca_key);

    let mut server_params =
        CertificateParams::new(vec![server_name.to_owned()]).expect("server certificate params");
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().expect("generate test server key");
    let server_certificate = server_params
        .signed_by(&server_key, &issuer)
        .expect("sign test server certificate");
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![server_certificate.der().clone()],
            PrivateKeyDer::Pkcs8(server_key.serialize_der().into()),
        )
        .expect("test TLS server config");
    let root =
        reqwest::Certificate::from_der(ca_certificate.der().as_ref()).expect("test CA for reqwest");
    (TlsAcceptor::from(Arc::new(server_config)), root)
}

async fn write_https_reply<S>(stream: &mut S, reply: TestHttpsReply)
where
    S: AsyncWriteExt + Unpin,
{
    let chunk_gate = reply.chunk_gate.clone();
    let reason = match reply.status {
        200 => "OK",
        302 => "Found",
        404 => "Not Found",
        _ => "Test Response",
    };
    let mut head = format!(
        "HTTP/1.1 {} {reason}\r\nConnection: close\r\n",
        reply.status
    );
    for (name, value) in &reply.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if reply.content_length {
        let length: usize = reply.chunks.iter().map(Vec::len).sum();
        head.push_str(&format!("Content-Length: {length}\r\n\r\n"));
        if stream.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        for chunk in reply.chunks {
            if stream.write_all(&chunk).await.is_err() {
                return;
            }
        }
    } else {
        head.push_str("Transfer-Encoding: chunked\r\n\r\n");
        if stream.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        for (index, chunk) in reply.chunks.into_iter().enumerate() {
            if stream
                .write_all(format!("{:X}\r\n", chunk.len()).as_bytes())
                .await
                .is_err()
                || stream.write_all(&chunk).await.is_err()
                || stream.write_all(b"\r\n").await.is_err()
            {
                return;
            }
            if index == 0
                && let Some(gate) = chunk_gate.as_ref()
            {
                if stream.flush().await.is_err() {
                    return;
                }
                gate.first_chunk_sent.notify_one();
                if tokio::time::timeout(Duration::from_secs(10), gate.release.notified())
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
        let _ = stream.write_all(b"0\r\n\r\n").await;
    }
    let _ = stream.flush().await;
}
