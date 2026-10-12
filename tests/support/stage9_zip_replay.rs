//! Owned, restartable real-TCP ZIP replay fixture. No provider worker runs.
use ipfs_s3_gateway::{
    config::Config,
    import::{
        downloader::{SourceDownloader, StrictPublicAddressPolicy},
        pipeline::ImportCoordinator,
        worker::ImportWorkerHandle,
    },
    state::AppState,
    store,
};
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use crate::support::{
    decompress::{
        AddReply, KuboScript, S3ServerHandle, S3TestEndpoint, start_kubo_harness,
        start_s3_server_with_imports,
    },
    stage9_zip_replay_source::{ORIGIN, Source},
};

pub const ARCHIVE: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
pub const OUTPUT: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyk4";
pub const BUCKET: &str = "test-bkt";
pub const KEY: &str = "archive.zip";

#[derive(Clone, Copy, Debug)]
pub enum Entry {
    Direct,
    Mpu,
    Import,
}

#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    Complete,
    Partial,
    Failed,
}

impl Outcome {
    pub fn archive(self) -> Vec<u8> {
        match self {
            Self::Partial => crate::support::decompress::legal_two_entry_zip(),
            _ => crate::support::decompress::legal_single_entry_zip(),
        }
    }

    pub fn published(self) -> usize {
        match self {
            Self::Complete | Self::Partial => 1,
            Self::Failed => 0,
        }
    }
}

pub struct Fixture {
    pub state: Arc<AppState>,
    pub kubo: MockServer,
    pub source: Arc<Source>,
    pub imports: Arc<ImportCoordinator>,
    config: Config,
    server: Option<S3ServerHandle>,
    worker: Option<ImportWorkerHandle>,
    cancel: CancellationToken,
    directory: Option<tempfile::TempDir>,
}

impl S3TestEndpoint for Fixture {
    fn endpoint(&self) -> &str {
        &self.server.as_ref().unwrap().endpoint
    }
    fn bucket(&self) -> &str {
        BUCKET
    }
}

impl Fixture {
    pub async fn new(entry: Entry, outcome: Outcome) -> Self {
        let archive = outcome.archive();
        let mut replies = vec![AddReply::Ok(ARCHIVE)];
        if matches!(entry, Entry::Mpu) {
            replies.push(AddReply::Ok(ARCHIVE));
        }
        match outcome {
            Outcome::Complete => replies.push(AddReply::Ok(OUTPUT)),
            Outcome::Partial => replies.extend([
                AddReply::Ok(OUTPUT),
                AddReply::Error(http::StatusCode::INTERNAL_SERVER_ERROR, "entry refused"),
            ]),
            Outcome::Failed => replies.push(AddReply::Error(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "entry refused",
            )),
        }
        let kubo = start_kubo_harness(KuboScript {
            add_replies: replies,
            cat_bodies: HashMap::from([
                (ARCHIVE.into(), archive.clone()),
                (
                    OUTPUT.into(),
                    if matches!(outcome, Outcome::Partial) {
                        b"first entry bytes".to_vec()
                    } else {
                        b"single entry bytes".to_vec()
                    },
                ),
            ]),
        })
        .await
        .server;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Pins\":[\"{ARCHIVE}\"]}}\n")),
            )
            .with_priority(1)
            .mount(&kubo)
            .await;
        let directory = tempfile::Builder::new()
            .prefix("stage9-zip-replay-")
            .tempdir()
            .unwrap();
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("gateway.sqlite")
                .display()
                .to_string()
                .replace('\\', "/")
        );
        let config: Config = toml::from_str(&format!(
            r#"
[kubo]
rpc_url = {kubo_url:?}
[storage]
database_url = {database_url:?}
[[auth.credentials]]
access_key = 'test'
secret_key = 'test'
[decompress_zip]
unixfs_directory_root = false
auto_pin_outputs = true
[imports]
allowed_https_origins = [{ORIGIN:?}]
[pinning_identity]
primary_storage_domain = 'stage9-replay-kubo'
[[pinning_identity.providers]]
config_name = 'local-noop'
provider_id = 'stage9-replay-noop'
display_name = 'Stage9 replay local Noop'
backend = 'noop'
scope = 'stage9-replay-scope'
storage_domain = 'stage9-replay-noop-domain'
credential_revision = 1
endpoint_revision = 1
api_profile = 'noop'
strategy = 'cid'
cleanup = 'retain'
[[pinning.providers]]
name = 'local-noop'
kind = 'noop'
priority = 1
max_bytes = 1000000
max_pins = 100
[[pinning.policies]]
bucket = 'test-bkt'
prefix = ''
trigger = 'always'
provider_mode = 'one'
providers = ['local-noop']
default_duration = '1h'
max_duration = '30d'
allow_decompressed = true
"#,
            kubo_url = kubo.uri()
        ))
        .unwrap();
        let state = AppState::new(&config).await.unwrap();
        let provider = &state.pinning.effective_config().providers[0];
        assert_eq!(
            state
                .pinning
                .provider(&provider.identity.allocation_key())
                .unwrap()
                .invocation_route(),
            ("noop", "cid")
        );
        store::bucket::create(state.store.db(), BUCKET, Some("test"))
            .await
            .unwrap();
        let source = Arc::new(Source::new(&archive));
        let imports = coordinator(&config, source.clone());
        let server =
            start_s3_server_with_imports(state.clone(), Arc::default(), imports.clone()).await;
        Self {
            state,
            kubo,
            source,
            imports,
            config,
            server: Some(server),
            worker: None,
            cancel: CancellationToken::new(),
            directory: Some(directory),
        }
    }

    pub fn start_worker(&mut self) {
        assert!(self.worker.is_none());
        self.worker = Some(self.imports.start(self.state.clone(), self.cancel.clone()));
    }

    async fn stop(&mut self) {
        self.cancel.cancel();
        if let Some(worker) = self.worker.take() {
            worker.shutdown(Duration::from_secs(5)).await;
        }
        if let Some(server) = self.server.take() {
            server.shutdown().await;
        }
        self.state.store.db().clone().close().await.unwrap();
    }

    pub async fn restart(&mut self, worker: bool) {
        self.stop().await;
        self.state = AppState::new(&self.config).await.unwrap();
        self.imports = coordinator(&self.config, self.source.clone());
        self.cancel = CancellationToken::new();
        self.server = Some(
            start_s3_server_with_imports(self.state.clone(), Arc::default(), self.imports.clone())
                .await,
        );
        if worker {
            self.start_worker();
        }
    }

    pub async fn shutdown(mut self) {
        self.stop().await;
        self.directory.take().unwrap().close().unwrap();
    }

    pub async fn adds(&self) -> usize {
        self.kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/api/v0/add")
            .count()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn coordinator(config: &Config, source: Arc<Source>) -> Arc<ImportCoordinator> {
    let validated = config.imports.validate().unwrap();
    let downloader = SourceDownloader::with_components(
        Arc::new(validated.clone()),
        source.clone(),
        Arc::new(StrictPublicAddressPolicy),
        source,
    );
    ImportCoordinator::new(validated, downloader)
}

pub async fn run<F, Fut>(entry: Entry, outcome: Outcome, check: F)
where
    F: FnOnce(Fixture) -> Fut,
    Fut: Future<Output = (Fixture, Result<(), Box<dyn std::any::Any + Send>>)>,
{
    let fixture = Fixture::new(entry, outcome).await;
    let (fixture, result) = check(fixture).await;
    fixture.shutdown().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
