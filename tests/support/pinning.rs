//! Signed S3 against a real HTTP gateway, with Kubo and the remote PSA on loopback.
use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{
    decompress::{
        AddReply, KuboScript, S3ServerHandle, S3TestEndpoint, start_kubo_harness, start_s3_server,
    },
    pin_rows,
};
use ipfs_s3_gateway::{
    config::Config,
    pinning::{config::ValidatedPinningConfig, coordinator::PinningCoordinator},
    state::AppState,
    store::{self, Store},
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_partial_json, header, method, path},
};

pub const TEST_CID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
const HANDLE: &str = "request-1";
pub struct PinningHarnessConfig {
    pub kubo_script: KuboScript,
    pub pinata_script: Vec<Mock>,
    pub worker_interval: Duration,
    pub requests_per_second: u32,
}

impl PinningHarnessConfig {
    pub fn request_one() -> Self {
        let post = Mock::given(method("POST"))
            .and(path("/psa/pins"))
            .and(header("Authorization", "Bearer pinning-test-token"))
            .and(body_partial_json(serde_json::json!({"cid": TEST_CID})))
            .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
                "requestid": HANDLE, "status": "queued", "pin": {"cid": TEST_CID, "meta": {}}, "info": {}
            })))
            .expect(1);
        Self {
            kubo_script: KuboScript {
                add_replies: vec![AddReply::Ok(TEST_CID)],
                cat_bodies: HashMap::from([(TEST_CID.into(), b"body".to_vec())]),
            },
            pinata_script: vec![post],
            worker_interval: Duration::from_secs(1),
            requests_per_second: 1000,
        }
    }
}

pub struct PinningHarness {
    pub endpoint: String,
    pub bucket: String,
    pub state: Arc<AppState>,
    pub kubo: MockServer,
    pub pinata: MockServer,
    server: S3ServerHandle,
}

impl S3TestEndpoint for PinningHarness {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }
    fn bucket(&self) -> &str {
        &self.bucket
    }
}

pub async fn start_pinning_harness(config: PinningHarnessConfig) -> PinningHarness {
    start_pinning_harness_with_database(
        config,
        sea_orm::Database::connect("sqlite::memory:").await.unwrap(),
    )
    .await
}

pub async fn start_pinning_harness_with_database(
    config: PinningHarnessConfig,
    db: sea_orm::DatabaseConnection,
) -> PinningHarness {
    let pinata = MockServer::start().await;
    for mock in config.pinata_script {
        mock.mount(&pinata).await;
    }
    let config_text = format!(
        r#"
        [pinning]
        worker_interval = "{}s"
        [[pinning.providers]]
        name = "pinata-primary"
        kind = "pinata"
        endpoint = "{}/psa"
        token_env = "PSA_TEST_TOKEN"
        priority = 1
        max_bytes = 1048576
        max_pins = 100
        requests_per_second = {}
        [[pinning.policies]]
        bucket = "test-bkt"
        trigger = "request"
        provider_mode = "one"
        providers = ["pinata-primary"]
        default_duration = "1h"
        max_duration = "30d"
        allow_decompressed = true
        [pinning_identity]
        primary_storage_domain = "local"
        [[pinning_identity.providers]]
        config_name = "pinata-primary"
        provider_id = "test-remote"
        display_name = "Pinata"
        backend = "pinata"
        scope = "test-account"
        storage_domain = "remote"
        credential_revision = 1
        endpoint_revision = 1
        secret_ref = "env:PSA_TEST_TOKEN"
        api_profile = "pinata-psa"
        strategy = "cid"
    "#,
        config.worker_interval.as_secs(),
        pinata.uri(),
        config.requests_per_second,
    );
    let raw: Config = toml::from_str(&config_text).unwrap();
    let validated = ValidatedPinningConfig::from_config(&raw, |key| {
        (key == "PSA_TEST_TOKEN").then(|| "pinning-test-token".into())
    })
    .unwrap();
    let kubo = start_kubo_harness(config.kubo_script).await.server;
    store::run_migrations(&db).await.unwrap();
    let bucket = "test-bkt".to_owned();
    store::bucket::create(&db, &bucket, None).await.unwrap();
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: PinningCoordinator::build(validated).unwrap(),
    });
    let server = start_s3_server(state.clone(), Default::default()).await;
    PinningHarness {
        endpoint: server.endpoint.clone(),
        bucket,
        state,
        kubo,
        pinata,
        server,
    }
}

impl PinningHarness {
    pub async fn run_until_resource_status(&self, expected: &str) {
        let worker = self
            .state
            .pinning
            .start(self.state.store.clone(), CancellationToken::new());
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let rows = pin_rows::resources(self.state.store.db()).await;
                if matches!(rows.as_slice(), [resource] if resource.status == expected) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        worker.shutdown(Duration::from_secs(2)).await;
        result.expect("worker did not settle the requested resources");
    }
    pub async fn shutdown(self) {
        self.server.shutdown().await;
        self.pinata.verify().await;
        super::residency::assert_hot_standard_residency_invariant(self.state.store.db()).await;
    }
}
