//! Signed HTTP + file SQLite + actual AppState registration. RPC leaves are
//! protocol mocks, never real provider/DAG/PIN or stored-byte proof evidence.
#![allow(dead_code)]
use super::{
    cors, sigv4,
    stage7_retirement_barrier::{self as barrier, foundation},
};
use ipfs_s3_gateway::{
    config::Config, pinning::management::local_config::ManagementConfig, state::AppState,
};
use std::sync::Arc;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

pub struct Fixture {
    pub state: Arc<AppState>,
    pub raw: Config,
    pub config: ManagementConfig,
    pub source: MockServer,
    pub target: MockServer,
    pub server: cors::S3ServerHandle,
    directory: tempfile::TempDir,
}

impl Fixture {
    pub async fn new() -> Self {
        Self::with_trigger("request").await
    }

    pub async fn with_trigger(trigger: &str) -> Self {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Name\":\"object\",\"Hash\":\"{}\",\"Size\":\"10\"}}\n",
                foundation::CID
            )))
            .mount(&source)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"ID":foundation::NODE})),
            )
            .mount(&source)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
            .mount(&source)
            .await;
        let directory = tempfile::tempdir().unwrap();
        let auto_pin = trigger == "always";
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("retirement.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        );
        let text = format!(
            r#"
            [kubo]
            rpc_url='{}'
            [storage]
            database_url='{}'
            [crypto]
            master_key='{}'
            [decompress_zip]
            auto_pin_outputs={auto_pin}
            unixfs_directory_root=false
            [[pinning.providers]]
            name='fixture'
            kind='ipfs_rpc'
            api='rpc'
            strategy='cid'
            endpoint='{}'
            priority=1
            max_bytes=1000000
            max_pins=1000
            [[pinning.policies]]
            bucket='bucket'
            prefix=''
            trigger='{trigger}'
            provider_mode='all'
            providers=['fixture']
            default_duration='1h'
            max_duration='2h'
            allow_decompressed=true
            [[pinning_rpc.providers]]
            config_name='fixture'
            profile='kubo'
            auth='none'
            allow_private_network=true
            [pinning_identity]
            primary_storage_domain='hot'
            [[pinning_identity.providers]]
            config_name='fixture'
            provider_id='retirement-rpc'
            display_name='Retirement protocol mock'
            backend='kubo'
            scope='retirement-account'
            storage_domain='retirement-remote'
            credential_revision=1
            endpoint_revision=1
            api_profile='kubo'
            strategy='cid'
            cleanup='retain'
        "#,
            source.uri(),
            url,
            "17".repeat(32),
            target.uri()
        );
        let raw: Config = toml::from_str(&text).unwrap();
        let config = foundation::management_config(&raw);
        let state = AppState::new(&raw).await.unwrap();
        let server = cors::start_gateway(state.clone(), cors::default_import_coordinator()).await;
        let response = sigv4::send_sigv4(
            reqwest::Method::PUT,
            &server.endpoint,
            "bucket",
            "",
            &[],
            vec![],
            http::HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        Self {
            state,
            raw,
            config,
            source,
            target,
            server,
            directory,
        }
    }

    pub async fn request(
        &self,
        method: reqwest::Method,
        key: &str,
        query: &[(&str, &str)],
        body: Vec<u8>,
        headers: http::HeaderMap,
    ) -> reqwest::Response {
        sigv4::send_sigv4(
            method,
            &self.server.endpoint,
            "bucket",
            key,
            query,
            body,
            headers,
            "test",
        )
        .await
    }

    pub fn pin_headers() -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-amz-tagging",
            "ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1h".parse().unwrap(),
        );
        headers
    }

    pub async fn retire(&self) {
        barrier::retire(
            self.state.store.db(),
            &self.raw,
            &self.config,
            "http-retirement",
        )
        .await;
    }

    pub async fn assert_no_remote_mutation(&self) {
        let requests = self.target.received_requests().await.unwrap();
        assert!(!requests.iter().any(|request| request.method == "DELETE"
            || request.method == "POST"
                && matches!(
                    request.url.path(),
                    "/api/v0/add" | "/api/v0/pin/add" | "/api/v0/pin/rm"
                )));
    }
}

pub fn xml_value<'a>(xml: &'a str, tag: &str) -> &'a str {
    xml.split_once(&format!("<{tag}>"))
        .unwrap()
        .1
        .split_once(&format!("</{tag}>"))
        .unwrap()
        .0
}
