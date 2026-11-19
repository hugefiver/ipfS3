use std::{sync::Arc, time::Duration};

use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::{config::Config, state::AppState, store};
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
}

impl Harness {
    pub async fn start(root: bool, auto: bool) -> Self {
        Self::start_with_capacity(root, auto, 8192).await
    }

    pub async fn start_for_client_multipart() -> Self {
        Self::start_with_capacity(false, false, 32 * 1024 * 1024).await
    }

    async fn start_with_capacity(root: bool, auto: bool, max_bytes: u64) -> Self {
        let source = HttpNode::start(false).await;
        let target = HttpNode::start(true).await;
        // Synthetic test-only configuration; no files/environment credentials read.
        let config: Config = toml::from_str(&format!(
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
            max_bytes = {max_bytes}
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
        store::bucket::create(state.store.db(), BUCKET, Some("test"))
            .await
            .unwrap();
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
                let resources = crate::pin_rows::resources(db).await;
                if resources.len() == ENTRIES.len()
                    && resources
                        .iter()
                        .all(|r| r.status == "accepted" && r.action.is_none() && r.handle.is_some())
                    && crate::pin_rows::count(db, "pin_refs").await == target_count as u64
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        cancel.cancel();
        worker.shutdown(Duration::from_secs(3)).await;
        if let Err(error) = result {
            panic!(
                "worker timeout: {error}; resources: {:?}; target: {:?}",
                crate::pin_rows::resources(self.state.store.db()).await,
                self.target.calls().await
            );
        }
    }
}
