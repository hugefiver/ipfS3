//! Real-TCP wire fixture with no remote pin policy and owned shutdown.
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

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
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

use crate::support::{
    decompress::{
        AddReply, KuboScript, S3ServerHandle, S3TestEndpoint, legal_single_entry_zip,
        legal_two_entry_zip, start_kubo_harness, start_s3_server_with_imports,
    },
    stage9_zip_replay_source::{ORIGIN, Source},
};

pub const ARCHIVE: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
pub const OUTPUT: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyk4";
pub const ROOT: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";
pub const BUCKET: &str = "test-bkt";
pub const KEY: &str = "archive.zip";

#[derive(Clone, Copy)]
pub enum Entry {
    Put,
    Mpu,
    Import,
}

#[derive(Clone, Copy)]
pub enum Outcome {
    Complete,
    Partial,
    Failed,
    RootWarning,
}

impl Outcome {
    pub fn archive(self) -> Vec<u8> {
        if matches!(self, Self::Partial) {
            legal_two_entry_zip()
        } else {
            legal_single_entry_zip()
        }
    }
}

pub struct Fixture {
    pub state: Arc<AppState>,
    pub kubo: MockServer,
    pub source: Arc<Source>,
    imports: Arc<ImportCoordinator>,
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
        // URL import's progress add has a dedicated archive response below.
        let mut replies = if matches!(entry, Entry::Import) {
            Vec::new()
        } else {
            vec![AddReply::Ok(ARCHIVE)]
        };
        if matches!(entry, Entry::Mpu) {
            replies.push(AddReply::Ok(ARCHIVE));
        }
        if matches!(outcome, Outcome::Failed) {
            replies.push(AddReply::Error(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "entry refused",
            ));
        } else {
            replies.push(AddReply::Ok(OUTPUT));
        }
        if matches!(outcome, Outcome::Partial) {
            replies.push(AddReply::Error(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "entry refused",
            ));
        }
        let output = if matches!(outcome, Outcome::Partial) {
            b"first entry bytes".as_slice()
        } else {
            b"single entry bytes".as_slice()
        };
        let kubo = start_kubo_harness(KuboScript {
            add_replies: replies,
            cat_bodies: HashMap::from([
                (ARCHIVE.into(), archive.clone()),
                (OUTPUT.into(), output.to_vec()),
            ]),
        })
        .await
        .server;
        mount_root(&kubo, archive.len(), output.len(), outcome).await;
        if matches!(entry, Entry::Import) {
            Mock::given(method("POST"))
                .and(path("/api/v0/add"))
                .and(query_param("progress", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    "{{\"Bytes\":{size}}}\n{{\"Hash\":\"{ARCHIVE}\",\"Size\":\"{size}\"}}\n",
                    size = archive.len()
                )))
                .with_priority(1)
                // Entry adds also use progress=true; only the first add is the
                // archive. Later adds consume the independent output script.
                .up_to_n_times(1)
                .mount(&kubo)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/v0/pin/add"))
                .and(query_param("arg", ARCHIVE))
                .and(query_param("progress", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    "{{\"Progress\":3,\"Bytes\":{size}}}\n{{\"Pins\":[\"{ARCHIVE}\"]}}\n",
                    size = archive.len()
                )))
                .with_priority(1)
                .mount(&kubo)
                .await;
        }
        let directory = tempfile::Builder::new()
            .prefix("stage9-zip-wire-")
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
unixfs_directory_root = true
auto_pin_outputs = false
[imports]
allowed_https_origins = [{ORIGIN:?}]
[pinning]
providers = []
policies = []
"#,
            kubo_url = kubo.uri()
        ))
        .unwrap();
        assert!(config.pinning.providers.is_empty() && config.pinning.policies.is_empty());
        let state = AppState::new(&config).await.unwrap();
        store::bucket::create(state.store.db(), BUCKET, Some("test"))
            .await
            .unwrap();
        let source = Arc::new(Source::new(&archive));
        let validated = config.imports.validate().unwrap();
        let downloader = SourceDownloader::with_components(
            Arc::new(validated.clone()),
            source.clone(),
            Arc::new(StrictPublicAddressPolicy),
            source.clone(),
        );
        let imports = ImportCoordinator::new(validated, downloader);
        let server =
            start_s3_server_with_imports(state.clone(), Arc::default(), imports.clone()).await;
        Self {
            state,
            kubo,
            source,
            imports,
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

    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(worker) = self.worker.take() {
            worker.shutdown(Duration::from_secs(5)).await;
        }
        self.server.take().unwrap().shutdown().await;
        self.state.store.db().clone().close().await.unwrap();
        self.directory.take().unwrap().close().unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn mount_root(kubo: &MockServer, archive_size: usize, output_size: usize, outcome: Outcome) {
    Mock::given(method("POST")).and(path("/api/v0/files/stat"))
        .respond_with(move |request: &wiremock::Request| {
            let arg = request.url.query_pairs().find(|(key, _)| key == "arg").unwrap().1.into_owned();
            let (cid, size) = if arg == format!("/ipfs/{ARCHIVE}") { (ARCHIVE, archive_size) }
                else if arg == format!("/ipfs/{ROOT}") { (ROOT, output_size) } else { (OUTPUT, output_size) };
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{cid}\",\"Size\":{size},\"CumulativeSize\":{size},\"Type\":\"file\",\"WithLocality\":true,\"Local\":true}}"))
        }).mount(kubo).await;
    let dag = if matches!(outcome, Outcome::RootWarning) {
        ResponseTemplate::new(500).set_body_string("DAG build refused")
    } else {
        ResponseTemplate::new(200).set_body_string(format!("{{\"Cid\":{{\"/\":\"{ROOT}\"}}}}"))
    };
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(dag)
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", ROOT))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{ROOT}\"]}}")),
        )
        .with_priority(1)
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/resolve"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Path\":\"/ipfs/{OUTPUT}\"}}")),
        )
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("{\"ID\":\"QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE\"}"),
        )
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .respond_with(|request: &wiremock::Request| {
            let arg = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "arg")
                .unwrap()
                .1
                .into_owned();
            ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Keys\":{{\"{arg}\":{{\"Type\":\"recursive\"}}}}}}"
            ))
        })
        .mount(kubo)
        .await;
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
