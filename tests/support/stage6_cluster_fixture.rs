//! Production registration + signed S3 + durable worker, with only HTTP leaves mocked.
use ipfs_s3_gateway::{config::Config, pinning::coordinator::PinningWorkerHandle, state::AppState};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[allow(dead_code)]
#[path = "cors.rs"]
mod cors;
#[allow(dead_code)]
#[path = "sigv4.rs"]
mod sigv4;
pub const CID: &str = "QmYwAPJzv5CZsnAzt8auVZRnGiRAz1F7TaqzM9zECsFcdX";

pub struct Fixture {
    pub state: Arc<AppState>,
    pub proxy: MockServer,
    pub rest: MockServer,
    worker: Option<PinningWorkerHandle>,
    server: cors::S3ServerHandle,
    _kubo: MockServer,
}

pub fn canonical() -> String {
    ipfs_s3_gateway::pinning::provider::canonical_resource_cid(CID).unwrap()
}
pub fn other_cid() -> String {
    let cid = cid::Cid::try_from(CID).unwrap();
    cid::Cid::new_v1(0x55, *cid.hash()).to_string()
}

impl Fixture {
    pub async fn new(protocol: &str, aliases: bool, database: &str) -> Self {
        Self::configured(protocol, aliases, database, |_| {}).await
    }

    pub async fn configured(
        protocol: &str,
        aliases: bool,
        database: &str,
        configure: impl FnOnce(&mut Config),
    ) -> Self {
        let kubo = MockServer::start().await;
        let proxy = MockServer::start().await;
        let rest = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                format!("{{\"Name\":\"file\",\"Hash\":\"{CID}\",\"Size\":\"4\"}}\n"),
                "application/json",
            ))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Pins":[CID]})))
            .mount(&kubo)
            .await;
        let providers = if aliases {
            vec!["cluster-a", "cluster-b"]
        } else {
            vec!["cluster-a"]
        };
        let mut text = format!(
            r#"
[kubo]
rpc_url = "{}"
[storage]
database_url = "{}"
[crypto]
master_key = "{}"
[decompress_zip]
auto_pin_outputs = false
[pinning]
worker_interval = "1s"
worker_concurrency = 1
[[pinning.policies]]
bucket = "stage6-bucket"
prefix = ""
trigger = "always"
provider_mode = "all"
providers = [{}]
default_duration = "1h"
max_duration = "1h"
allow_decompressed = false
[pinning_identity]
primary_storage_domain = "kubo-primary"
"#,
            kubo.uri(),
            database.replace('\\', "\\\\"),
            "0".repeat(64),
            providers
                .iter()
                .map(|name| format!("\"{name}\""))
                .collect::<Vec<_>>()
                .join(",")
        );
        for name in providers {
            text += &format!(
                r#"
[[pinning.providers]]
name = "{name}"
kind = "cluster"
strategy = "cid"
enabled = true
priority = 1
max_bytes = 1048576
max_pins = 10
requests_per_second = 100
[pinning.providers.cluster]
protocol = "{protocol}"
rest_endpoint = "{}"
{}
allow_private_network = true
[[pinning_identity.providers]]
config_name = "{name}"
provider_id = "stage6-{name}"
display_name = "Stage6 Cluster"
backend = "cluster"
scope = "pinset-a"
storage_domain = "cluster-independent"
credential_revision = 1
endpoint_revision = 1
api_profile = "cluster-{protocol}"
strategy = "cid"
"#,
                rest.uri(),
                if protocol == "proxy" {
                    format!("proxy_endpoint = \"{}\"", proxy.uri())
                } else {
                    String::new()
                }
            );
        }
        let mut config: Config = toml::from_str(&text).unwrap();
        configure(&mut config);
        let state = AppState::new(&config)
            .await
            .expect("actual registered AppState");
        let server = cors::start_gateway(state.clone(), cors::default_import_coordinator()).await;
        let response = sigv4::send_sigv4(
            reqwest::Method::PUT,
            &server.endpoint,
            "stage6-bucket",
            "",
            &[],
            vec![],
            http::HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "signed CreateBucket"
        );
        Self {
            state,
            proxy,
            rest,
            worker: None,
            server,
            _kubo: kubo,
        }
    }

    pub async fn put(&self, key: &str) {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-amz-tagging",
            http::HeaderValue::from_static("audit=cluster"),
        );
        let response = sigv4::send_sigv4(
            reqwest::Method::PUT,
            &self.server.endpoint,
            "stage6-bucket",
            key,
            &[],
            b"body".to_vec(),
            headers,
            "test",
        )
        .await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "signed PutObject: {}",
            response.text().await.unwrap()
        );
    }

    pub async fn s3(
        &self,
        method: reqwest::Method,
        key: &str,
        query: &[(&str, &str)],
        body: Vec<u8>,
    ) -> reqwest::Response {
        sigv4::send_sigv4(
            method,
            &self.server.endpoint,
            "stage6-bucket",
            key,
            query,
            body,
            http::HeaderMap::new(),
            "test",
        )
        .await
    }
    pub fn start(&mut self) {
        self.worker = Some(self.state.pinning.start(
            self.state.store.clone(),
            tokio_util::sync::CancellationToken::new(),
        ));
    }
    pub async fn stop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.shutdown(Duration::from_millis(500)).await;
        }
    }
}
