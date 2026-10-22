//! Production registration + signed S3 + durable worker, with only HTTP leaves mocked.
use chrono::Utc;
use ipfs_s3_gateway::{
    config::Config,
    pinning::coordinator::{PinningCoordinator, PinningWorkerHandle},
    state::AppState,
    store::entities::{object, pin_job, pin_lease, pin_replica_observation},
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use wiremock::{
    Mock, MockServer, Request, Respond, ResponseTemplate,
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
    pub config: Config,
    pub proxy: MockServer,
    pub rest: MockServer,
    pub mode: Arc<AtomicU8>,
    pub write_mode: Arc<AtomicU8>,
    pub poll_delay_ms: Arc<AtomicU64>,
    pub poll_reads: Arc<AtomicU64>,
    pub replication_factor: Arc<AtomicI32>,
    pub endpoint: String,
    pub worker: Option<PinningWorkerHandle>,
    _server: cors::S3ServerHandle,
    _kubo: MockServer,
}

pub fn canonical() -> String {
    ipfs_s3_gateway::pinning::provider::canonical_resource_cid(CID).unwrap()
}
pub fn other_cid() -> String {
    let cid = cid::Cid::try_from(CID).unwrap();
    cid::Cid::new_v1(0x55, *cid.hash()).to_string()
}
fn peer(value: u8) -> String {
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &[value; 32]).unwrap();
    cid::multibase::encode(cid::multibase::Base::Base58Btc, hash.to_bytes())[1..].into()
}

#[derive(Clone)]
struct Replies {
    mode: Arc<AtomicU8>,
    write: Arc<AtomicU8>,
    poll_delay_ms: Arc<AtomicU64>,
    poll_reads: Arc<AtomicU64>,
    replication_factor: Arc<AtomicI32>,
}
impl Respond for Replies {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let mode = self.mode.load(Ordering::SeqCst);
        let path = request.url.path();
        if request.method == "POST" {
            if path == "/api/v0/id" {
                return ResponseTemplate::new(200).set_body_json(json!({"ID": "stage6-target"}));
            }
            if path == "/api/v0/files/stat" {
                return ResponseTemplate::new(200).set_body_json(json!({
                    "Hash": canonical(), "WithLocality": true, "Local": true
                }));
            }
            if path == "/api/v0/pin/ls" {
                return ResponseTemplate::new(200)
                    .set_body_json(json!({"Keys": {canonical(): {"Type": "recursive"}}}));
            }
            return match self.write.load(Ordering::SeqCst) {
                1 => ResponseTemplate::new(200).set_body_json(json!({"Pins":[CID,other_cid()]})),
                2 => ResponseTemplate::new(200).set_body_json(json!({"Pins":[]})),
                3 => ResponseTemplate::new(503),
                _ if path == "/api/v0/pin/add" => {
                    ResponseTemplate::new(200).set_body_json(json!({"Pins":[CID]}))
                }
                _ => ResponseTemplate::new(200).set_body_json(json!({"cid":canonical()})),
            };
        }
        if path == "/version" {
            return ResponseTemplate::new(200).set_body_json(json!({"version":"1.1.6"}));
        }
        if mode == 3 {
            return ResponseTemplate::new(503);
        }
        let cid = if mode == 4 { other_cid() } else { canonical() };
        let created = "2026-09-20T00:00:00Z";
        let a = peer(b'a');
        let b = peer(b'b');
        if path.starts_with("/allocations/") {
            let rf = self.replication_factor.load(Ordering::SeqCst);
            return ResponseTemplate::new(200).set_body_json(json!({"cid":cid,"allocations":[a,b],
                "timestamp":created,"replication_factor_min":rf,"replication_factor_max":rf}));
        }
        self.poll_reads.fetch_add(1, Ordering::SeqCst);
        let second = match mode {
            1 => "pinning",
            2 => "queued",
            5 => "unknown_future_status",
            _ => "pinned",
        };
        ResponseTemplate::new(200).set_delay(Duration::from_millis(self.poll_delay_ms.load(Ordering::SeqCst)))
            .set_body_json(json!({"cid":cid,"created":created,"allocations":[a,b],
            "peer_map":{a:{"status":if mode==2 {"queued"} else {"pinned"},"timestamp":created,"error":""},
                         b:{"status":second,"timestamp":created,"error":""}}}))
    }
}

impl Fixture {
    pub async fn postgres() -> Self {
        use sea_orm::{ConnectionTrait, DatabaseBackend};
        use sea_orm_migration::SchemaManager;
        let database = std::env::var("IPFS_S3_STAGE6_TEST_DATABASE_URL")
            .expect("explicit isolated PostgreSQL URL required");
        assert!(
            database.starts_with("postgres://") || database.starts_with("postgresql://"),
            "no SQLite fallback"
        );
        let preflight = ipfs_s3_gateway::store::connect_database(&database)
            .await
            .unwrap_or_else(|_| panic!("cannot connect to selected isolated PostgreSQL"));
        assert_eq!(preflight.get_database_backend(), DatabaseBackend::Postgres);
        if SchemaManager::new(&preflight)
            .has_table("pin_jobs")
            .await
            .unwrap()
        {
            assert!(
                pin_job::Entity::find()
                    .one(&preflight)
                    .await
                    .unwrap()
                    .is_none(),
                "requires fresh isolated Stage6 scope per exact test; preserve existing jobs and Unknown debt"
            );
        }
        preflight.close().await.unwrap();
        Self::new("rest", false, &database).await
    }

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
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Name\":\"file\",\"Hash\":\"{CID}\",\"Size\":\"4\"}}\n"
            )))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Pins":[CID]})))
            .mount(&kubo)
            .await;
        let mode = Arc::new(AtomicU8::new(0));
        let write_mode = Arc::new(AtomicU8::new(0));
        let poll_delay_ms = Arc::new(AtomicU64::new(0));
        let poll_reads = Arc::new(AtomicU64::new(0));
        let replication_factor = Arc::new(AtomicI32::new(2));
        let replies = Replies {
            mode: mode.clone(),
            write: write_mode.clone(),
            poll_delay_ms: poll_delay_ms.clone(),
            poll_reads: poll_reads.clone(),
            replication_factor: replication_factor.clone(),
        };
        Mock::given(method("GET"))
            .respond_with(replies.clone())
            .mount(&rest)
            .await;
        Mock::given(method("POST"))
            .respond_with(replies.clone())
            .mount(&rest)
            .await;
        Mock::given(method("POST"))
            .respond_with(replies)
            .mount(&proxy)
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
[pinning.providers.cluster]
protocol = "{protocol}"
rest_endpoint = "{}"
{}
min_replicas = 2
allow_private_network = true
[pinning.providers.cluster.verifier_limits]
max_age_secs = 2
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
cleanup = "retain"
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
        // Do not repair the registry here: AppState startup must capture the
        // complete configured route before the signed publication is accepted.
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
            config,
            proxy,
            rest,
            mode,
            write_mode,
            poll_delay_ms,
            poll_reads,
            replication_factor,
            endpoint: server.endpoint.clone(),
            worker: None,
            _server: server,
            _kubo: kubo,
        }
    }

    pub async fn put(&self, key: &str) -> String {
        self.put_with_headers(key, http::HeaderMap::new()).await
    }
    pub async fn put_with_headers(&self, key: &str, headers: http::HeaderMap) -> String {
        let response = sigv4::send_sigv4(
            reqwest::Method::PUT,
            &self.endpoint,
            "stage6-bucket",
            key,
            &[],
            b"body".to_vec(),
            headers,
            "test",
        )
        .await;
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(
            status,
            reqwest::StatusCode::OK,
            "registered signed PutObject: {body}"
        );
        let owner = object::Entity::find()
            .filter(object::Column::Bucket.eq("stage6-bucket"))
            .filter(object::Column::Key.eq(key))
            .one(self.state.store.db())
            .await
            .unwrap()
            .unwrap();
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(owner.id))
            .one(self.state.store.db())
            .await
            .unwrap()
            .unwrap()
            .id
    }
    pub async fn s3(
        &self,
        method: reqwest::Method,
        key: &str,
        query: &[(&str, &str)],
    ) -> reqwest::Response {
        sigv4::send_sigv4(
            method,
            &self.endpoint,
            "stage6-bucket",
            key,
            query,
            vec![],
            http::HeaderMap::new(),
            "test",
        )
        .await
    }
    pub async fn reconfigure(&mut self, change: impl FnOnce(&mut Config)) {
        self.stop().await;
        change(&mut self.config);
        let validated = ipfs_s3_gateway::pinning::config::ValidatedPinningConfig::from_config(
            &self.config,
            |_| None,
        )
        .unwrap();
        let coordinator =
            PinningCoordinator::build_with_kubo(validated, Some(self.state.kubo.clone())).unwrap();
        coordinator
            .register_identities(&self.state.store)
            .await
            .unwrap();
        self.state = Arc::new(AppState {
            kubo: self.state.kubo.clone(),
            cold_kubo: None,
            store: self.state.store.clone(),
            credentials: self.state.credentials.clone(),
            master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(
                &self.config.crypto.master_key,
            )
            .unwrap(),
            pinning: coordinator,
        });
        self._server =
            cors::start_gateway(self.state.clone(), cors::default_import_coordinator()).await;
        self.endpoint.clone_from(&self._server.endpoint);
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
    pub async fn due(&self) {
        use sea_orm::sea_query::Expr;
        pin_job::Entity::update_many()
            .col_expr(pin_job::Column::NextAttemptAt, Expr::value(Utc::now()))
            .filter(pin_job::Column::State.eq("pending"))
            .exec(self.state.store.db())
            .await
            .unwrap();
    }
    pub async fn wait_read(&self, after: u64) {
        tokio::time::timeout(Duration::from_secs(8), async {
            while self.poll_reads.load(Ordering::SeqCst) <= after {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual new global pins GET");
    }
    pub async fn wait(&self, outcome: &str, after: i64) -> pin_replica_observation::Model {
        self.wait_job(outcome, after, None).await
    }
    pub async fn wait_job(
        &self,
        outcome: &str,
        after: i64,
        job: Option<&str>,
    ) -> pin_replica_observation::Model {
        let result = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let mut query = pin_replica_observation::Entity::find()
                    .filter(pin_replica_observation::Column::Phase.eq("poll"))
                    .filter(pin_replica_observation::Column::Sequence.gt(after))
                    .filter(pin_replica_observation::Column::Outcome.eq(outcome))
                    .order_by_desc(pin_replica_observation::Column::Sequence);
                if let Some(job) = job {
                    query = query.filter(pin_replica_observation::Column::JobId.eq(job));
                }
                if let Some(row) = query.one(self.state.store.db()).await.unwrap() {
                    return row;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        match result {
            Ok(row) => row,
            Err(error) => panic!(
                "durable registered Cluster {outcome} after {after}: {error}; observations={:?}; jobs={:?}",
                pin_replica_observation::Entity::find()
                    .all(self.state.store.db())
                    .await
                    .unwrap(),
                pin_job::Entity::find()
                    .all(self.state.store.db())
                    .await
                    .unwrap()
            ),
        }
    }
}
