use std::{sync::Arc, time::Duration};

use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    state::AppState,
    store::{
        self,
        entities::{pin_job, pin_lease_target},
        pinning::ledger,
    },
};
use sea_orm::EntityTrait;
use tokio_util::sync::CancellationToken;

use crate::{
    node::HttpNode,
    support::{
        cors,
        decompress::{self, ObservedHttpRequest},
        sigv4,
    },
};

pub const BUCKET: &str = "stage75-pixivbot";
pub const KEY: &str = "archive.zip";
pub const PREFIX: &str = "pixiv/123/";
pub const TAGS: &str = "ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1d&ipfs-s3%3Acontent=decompressed";
pub const ENTRIES: [(&str, &[u8]); 2] = [
    ("first.txt", b"first entry bytes"),
    ("second.txt", b"second entry bytes"),
];

pub struct Harness {
    pub source: HttpNode,
    pub target: HttpNode,
    pub state: Arc<AppState>,
    pub gateway: cors::S3ServerHandle,
    pub observed: Arc<tokio::sync::Mutex<Vec<ObservedHttpRequest>>>,
    pub provider: String,
    pub history: Option<crate::history::History>,
    directory: Option<tempfile::TempDir>,
}

impl Harness {
    pub async fn start(root: bool, auto: bool) -> Self {
        Self::start_with_history(root, auto, false).await
    }

    pub async fn start_with_history(root: bool, auto: bool, history: bool) -> Self {
        let source = HttpNode::start(false).await;
        let target = HttpNode::start(true).await;
        // Synthetic test-only configuration; no files/environment credentials read.
        let mut config: Config = toml::from_str(&format!(
            r#"
            [kubo]
            rpc_url = '{}'
            [[auth.credentials]]
            access_key = 'test'
            secret_key = 'test'
            [decompress_zip]
            unixfs_directory_root = {root}
            auto_pin_outputs = {auto}
            [pinning]
            worker_interval = '1s'
            worker_concurrency = 1
            [[pinning.providers]]
            name = 'backup'
            kind = 'ipfs_rpc'
            api = 'rpc'
            strategy = 'upload'
            endpoint = '{}'
            priority = 1
            max_bytes = 8192
            max_pins = 8
            requests_per_second = 100
            [[pinning.policies]]
            bucket = '{BUCKET}'
            prefix = ''
            trigger = 'always'
            provider_mode = 'all'
            providers = ['backup']
            default_duration = '1h'
            max_duration = '30d'
            allow_decompressed = true
            [[pinning_rpc.providers]]
            config_name = 'backup'
            profile = 'kubo'
            auth = 'none'
            allow_private_network = true
            connect_timeout_seconds = 2
            control_timeout_seconds = 3
            idle_timeout_seconds = 3
            [pinning_identity]
            primary_storage_domain = 'kubo:stage75-primary'
            [[pinning_identity.providers]]
            config_name = 'backup'
            provider_id = 'stage75-pixivbot-backup'
            display_name = 'Loopback protocol mock'
            backend = 'kubo'
            scope = 'node:stage75-pixivbot'
            storage_domain = 'kubo:stage75-pixivbot'
            credential_revision = 1
            endpoint_revision = 1
            api_profile = 'kubo'
            strategy = 'upload'
            cleanup = 'retain'
        "#,
            source.server.uri(),
            target.server.uri()
        ))
        .unwrap();
        let directory = history.then(|| {
            tempfile::Builder::new()
                .prefix("stage75-pixivbot-")
                .tempdir()
                .unwrap()
        });
        if let Some(directory) = &directory {
            config.storage.database_url = format!(
                "sqlite://{}?mode=rwc",
                directory
                    .path()
                    .join("gateway.sqlite")
                    .to_string_lossy()
                    .replace('\\', "/")
            );
            crate::history::add_route(&mut config);
        }
        let state = AppState::new(&config)
            .await
            .expect("fresh isolated gateway initialization");
        let provider = state
            .pinning
            .effective_config()
            .providers
            .iter()
            .find(|provider| provider.identity.provider_id == "stage75-pixivbot-backup")
            .unwrap()
            .name
            .clone();
        assert_eq!(
            state
                .pinning
                .provider(&provider)
                .unwrap()
                .invocation_route(),
            ("kubo", "upload")
        );
        store::bucket::create(state.store.db(), BUCKET, Some("test"))
            .await
            .unwrap();
        let history = if history {
            Some(crate::history::History::prepare(&state, &config).await)
        } else {
            None
        };
        let observed: Arc<tokio::sync::Mutex<Vec<ObservedHttpRequest>>> = Arc::default();
        let gateway = decompress::start_s3_server_with_imports(
            state.clone(),
            observed.clone(),
            cors::default_import_coordinator(),
        )
        .await;
        let harness = Self {
            source,
            target,
            state,
            gateway,
            observed,
            provider,
            history,
            directory,
        };
        let response = harness
            .send(
                reqwest::Method::PUT,
                "",
                &[("versioning", "")],
                b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                    .to_vec(),
                HeaderMap::new(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        harness
    }

    pub async fn send(
        &self,
        method: reqwest::Method,
        key: &str,
        query: &[(&str, &str)],
        body: Vec<u8>,
        headers: HeaderMap,
    ) -> reqwest::Response {
        sigv4::send_sigv4(
            method,
            &self.gateway.endpoint,
            BUCKET,
            key,
            query,
            body,
            headers,
            "test",
        )
        .await
    }

    pub async fn shutdown(self) {
        self.gateway.shutdown().await;
        self.state.store.db().clone().close().await.unwrap();
        drop(self.state);
        if let Some(directory) = self.directory {
            directory
                .close()
                .expect("remove only owned Pixivbot test database");
        }
    }

    pub async fn run_worker(&self, target_count: usize) {
        let cancel = CancellationToken::new();
        let worker = self
            .state
            .pinning
            .start(self.state.store.clone(), cancel.clone());
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let db = self.state.store.db();
                let targets = pin_lease_target::Entity::find().all(db).await.unwrap();
                let jobs = pin_job::Entity::find().all(db).await.unwrap();
                let observations = ledger::submission::observations(db, &self.provider)
                    .await
                    .unwrap();
                if targets.len() == target_count
                    && targets.iter().all(|t| t.state == "pinned")
                    && !jobs.is_empty()
                    && jobs.iter().all(|j| j.state == "done")
                    && observations.len() == ENTRIES.len()
                    && observations.iter().all(|o| o.outcome == "matched")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if result.is_ok() {
            // Allow the restarted worker to perform an ordinary idle queue pass.
            tokio::time::sleep(Duration::from_millis(1100)).await;
        }
        cancel.cancel();
        worker.shutdown(Duration::from_secs(3)).await;
        if let Err(error) = result {
            let db = self.state.store.db();
            panic!(
                "registered worker incomplete: {error}; targets={:?}; jobs={:?}; observations={:?}; source_calls={:?}; target_calls={:?}",
                pin_lease_target::Entity::find().all(db).await.unwrap(),
                pin_job::Entity::find().all(db).await.unwrap(),
                ledger::submission::observations(db, &self.provider)
                    .await
                    .unwrap(),
                self.source.calls().await,
                self.target.calls().await,
            );
        }
    }
}
