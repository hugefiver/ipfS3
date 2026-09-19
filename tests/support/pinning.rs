//! Real-TCP pinning test support.
//!
//! The harness keeps S3 and provider calls on their production HTTP surfaces.
//! Durable-state helpers are observation and clock controls only; they never
//! call an S3 or provider operation directly.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, header};
use axum::response::Response as AxumResponse;
use chrono::Utc;
use ipfs_s3_gateway::config::{PinningConfig, PolicyConfig, ProviderConfig};
use ipfs_s3_gateway::pinning::config::ValidatedPinningConfig;
use ipfs_s3_gateway::pinning::coordinator::{PinningCoordinator, PinningWorkerHandle};
use ipfs_s3_gateway::state::AppState;
use ipfs_s3_gateway::store::entities::{
    pin_job, pin_lease, pin_lease_target, pin_provider_usage, remote_pin,
};
use ipfs_s3_gateway::store::pinning::jobs;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, QueryOrder, Set};
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;
use wiremock::{Mock, MockServer, Request as WiremockRequest, Respond, ResponseTemplate};

use super::decompress::{
    AddReply, KuboHarness, KuboScript, ObservedHttpRequest, S3ServerHandle, S3TestEndpoint,
    start_kubo_harness, start_s3_server,
};
use super::residency::assert_hot_standard_residency_invariant;

const PINATA_TOKEN_ENV: &str = "TEST_PINATA_TOKEN";
const FILEBASE_TOKEN_ENV: &str = "TEST_FILEBASE_TOKEN";
const PINATA_TOKEN: &str = "pinata-test-token";
const FILEBASE_TOKEN: &str = "filebase-test-token";
const TEST_CID: &str = "QmTestCid";
const WORKER_WAIT: Duration = Duration::from_secs(10);
const WORKER_SHUTDOWN_GRACE: Duration = Duration::from_millis(250);

pub struct PinningHarness {
    pub endpoint: String,
    pub bucket: String,
    pub state: Arc<AppState>,
    pub kubo: MockServer,
    pub pinata: MockServer,
    pub filebase: MockServer,
    pub worker: Option<PinningWorkerHandle>,
    s3_server: Option<S3ServerHandle>,
    pinata_proxy: Option<ProviderProxy>,
    filebase_proxy: Option<ProviderProxy>,
    pinata_script: Arc<PsaScriptState>,
    filebase_script: Arc<PsaScriptState>,
    provider_kinds: HashMap<String, TestProviderKind>,
}

impl S3TestEndpoint for PinningHarness {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}

pub struct PinningHarnessConfig {
    pub providers: Vec<TestProviderConfig>,
    pub policies: Vec<PolicyConfig>,
    pub kubo_script: KuboScript,
    pub pinata_script: Vec<PsaReply>,
    pub filebase_script: Vec<PsaReply>,
}

impl PinningHarnessConfig {
    pub fn request_one() -> Self {
        Self {
            providers: vec![TestProviderConfig::pinata("pinata-primary", 10)],
            policies: vec![PolicyConfig {
                bucket: "test-bkt".to_owned(),
                prefix: String::new(),
                trigger: "request".to_owned(),
                provider_mode: "one".to_owned(),
                providers: vec!["pinata-primary".to_owned()],
                default_duration: "1h".to_owned(),
                max_duration: "30d".to_owned(),
                allow_decompressed: true,
            }],
            kubo_script: KuboScript {
                add_replies: vec![AddReply::Ok(TEST_CID)],
                cat_bodies: HashMap::from([(TEST_CID.to_owned(), b"body".to_vec())]),
            },
            pinata_script: vec![PsaReply::pinned_submit(
                "/psa/pins",
                "pinata-request-1",
                TEST_CID,
            )],
            filebase_script: Vec::new(),
        }
    }

    pub fn automatic_all() -> Self {
        Self {
            providers: vec![
                TestProviderConfig::pinata("pinata-primary", 10),
                TestProviderConfig::filebase("filebase-primary", 20),
            ],
            policies: vec![PolicyConfig {
                bucket: "test-bkt".to_owned(),
                prefix: String::new(),
                trigger: "always".to_owned(),
                provider_mode: "all".to_owned(),
                providers: vec!["pinata-primary".to_owned(), "filebase-primary".to_owned()],
                default_duration: "1h".to_owned(),
                max_duration: "30d".to_owned(),
                allow_decompressed: true,
            }],
            kubo_script: KuboScript {
                add_replies: vec![AddReply::Ok(TEST_CID)],
                cat_bodies: HashMap::from([(TEST_CID.to_owned(), b"happy".to_vec())]),
            },
            pinata_script: vec![PsaReply::pinned_submit(
                "/psa/pins",
                "pinata-request-1",
                TEST_CID,
            )],
            filebase_script: vec![PsaReply::pinned_submit(
                "/v1/ipfs/pins",
                "filebase-request-1",
                TEST_CID,
            )],
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TestProviderKind {
    Pinata,
    Filebase,
}

pub struct TestProviderConfig {
    pub name: String,
    pub kind: TestProviderKind,
    pub enabled: bool,
    pub priority: u32,
    pub max_bytes: u64,
    pub max_pins: u64,
    pub requests_per_second: Option<u32>,
}

impl TestProviderConfig {
    pub fn pinata(name: impl Into<String>, priority: u32) -> Self {
        Self::new(name, TestProviderKind::Pinata, priority)
    }

    pub fn filebase(name: impl Into<String>, priority: u32) -> Self {
        Self::new(name, TestProviderKind::Filebase, priority)
    }

    fn new(name: impl Into<String>, kind: TestProviderKind, priority: u32) -> Self {
        Self {
            name: name.into(),
            kind,
            enabled: true,
            priority,
            max_bytes: 1024 * 1024,
            max_pins: 100,
            requests_per_second: Some(1_000),
        }
    }

    fn into_provider_config(
        self,
        pinata_endpoint: &str,
        filebase_endpoint: &str,
    ) -> ProviderConfig {
        let (kind, token_env, endpoint) = match self.kind {
            TestProviderKind::Pinata => {
                ("pinata", PINATA_TOKEN_ENV, format!("{pinata_endpoint}/psa"))
            }
            TestProviderKind::Filebase => (
                "filebase",
                FILEBASE_TOKEN_ENV,
                format!("{filebase_endpoint}/v1/ipfs"),
            ),
        };
        ProviderConfig {
            name: self.name,
            kind: kind.to_owned(),
            token_env: Some(token_env.to_owned()),
            endpoint: Some(endpoint),
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled: self.enabled,
            priority: self.priority,
            max_bytes: self.max_bytes,
            max_pins: self.max_pins,
            requests_per_second: self.requests_per_second,
        }
    }
}

pub struct PsaReply {
    pub method: Method,
    pub path: String,
    pub query: Option<Vec<(String, String)>>,
    pub request_json_subset: Option<serde_json::Value>,
    pub status: u16,
    pub body: Vec<u8>,
    find_response: Option<PsaFindResponse>,
}

#[derive(Clone, PartialEq, Eq)]
enum PsaFindResponse {
    One {
        request_id: String,
        cid: String,
        status: String,
    },
    None {
        cid: String,
    },
}

impl PsaReply {
    pub fn json(
        method: Method,
        path: impl Into<String>,
        status: u16,
        body: serde_json::Value,
    ) -> Self {
        Self {
            method,
            path: path.into(),
            query: None,
            request_json_subset: None,
            status,
            body: serde_json::to_vec(&body).expect("serialize scripted PSA response"),
            find_response: None,
        }
    }

    pub fn empty(method: Method, path: impl Into<String>, status: u16) -> Self {
        Self {
            method,
            path: path.into(),
            query: None,
            request_json_subset: None,
            status,
            body: Vec::new(),
            find_response: None,
        }
    }

    pub fn pinned_submit(path: impl Into<String>, request_id: &str, cid: &str) -> Self {
        Self::submit_status(path, request_id, cid, "pinned")
    }

    pub fn submit_status(
        path: impl Into<String>,
        request_id: &str,
        cid: &str,
        status: &str,
    ) -> Self {
        Self::json(
            Method::POST,
            path,
            StatusCode::OK.as_u16(),
            serde_json::json!({
                "requestid": request_id,
                "status": status,
                "pin": { "cid": cid, "meta": {} },
                "info": {}
            }),
        )
        .with_json_subset(serde_json::json!({ "cid": cid }))
    }

    pub fn pin_status(path: impl Into<String>, request_id: &str, cid: &str, status: &str) -> Self {
        Self::json(
            Method::GET,
            path,
            StatusCode::OK.as_u16(),
            serde_json::json!({
                "requestid": request_id,
                "status": status,
                "pin": { "cid": cid, "meta": {} },
                "info": {}
            }),
        )
    }

    pub fn find_for_job(
        path: impl Into<String>,
        request_id: &str,
        cid: &str,
        status: &str,
    ) -> Self {
        Self {
            method: Method::GET,
            path: path.into(),
            query: None,
            request_json_subset: None,
            status: StatusCode::OK.as_u16(),
            body: Vec::new(),
            find_response: Some(PsaFindResponse::One {
                request_id: request_id.to_owned(),
                cid: cid.to_owned(),
                status: status.to_owned(),
            }),
        }
    }

    pub fn find_none_for_job(path: impl Into<String>, cid: &str) -> Self {
        Self {
            method: Method::GET,
            path: path.into(),
            query: None,
            request_json_subset: None,
            status: StatusCode::OK.as_u16(),
            body: Vec::new(),
            find_response: Some(PsaFindResponse::None {
                cid: cid.to_owned(),
            }),
        }
    }

    pub fn with_query(mut self, query: Vec<(String, String)>) -> Self {
        self.query = Some(query);
        self
    }

    pub fn with_json_subset(mut self, subset: serde_json::Value) -> Self {
        self.request_json_subset = Some(subset);
        self
    }

    fn has_same_matcher_signature(&self, other: &Self) -> bool {
        self.method == other.method
            && self.path == other.path
            && self.query.as_ref().map(|query| sorted_query(query.clone()))
                == other
                    .query
                    .as_ref()
                    .map(|query| sorted_query(query.clone()))
            && self.request_json_subset == other.request_json_subset
            && self.find_response == other.find_response
    }

    fn matches_find_request(&self, query: &[(String, String)]) -> bool {
        let Some(response) = &self.find_response else {
            return true;
        };
        let expected_cid = match response {
            PsaFindResponse::One { cid, .. } | PsaFindResponse::None { cid } => cid,
        };
        if query.len() != 2
            || query
                .first()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                != Some(("cid", expected_cid.as_str()))
        {
            return false;
        }
        let Some((key, metadata)) = query.get(1) else {
            return false;
        };
        if key != "meta" {
            return false;
        }
        let Ok(metadata) = serde_json::from_str::<BTreeMap<String, String>>(metadata) else {
            return false;
        };
        metadata.len() == 1
            && metadata
                .get("gateway_job_id")
                .is_some_and(|job_id| !job_id.is_empty())
    }

    fn response_body(&self, request: &WiremockRequest) -> Vec<u8> {
        let Some(response) = &self.find_response else {
            return self.body.clone();
        };
        let metadata = request
            .url
            .query_pairs()
            .find(|(key, _)| key == "meta")
            .and_then(|(_, value)| serde_json::from_str::<BTreeMap<String, String>>(&value).ok())
            .expect("matched dynamic PSA Find request has metadata");
        match response {
            PsaFindResponse::One {
                request_id,
                cid,
                status,
            } => serde_json::to_vec(&serde_json::json!({
                "count": 1,
                "results": [{
                    "requestid": request_id,
                    "status": status,
                    "pin": { "cid": cid, "meta": metadata },
                    "info": {}
                }]
            }))
            .expect("serialize dynamic PSA Find response"),
            PsaFindResponse::None { .. } => serde_json::to_vec(&serde_json::json!({
                "count": 0,
                "results": []
            }))
            .expect("serialize empty dynamic PSA Find response"),
        }
    }
}

#[derive(Clone)]
pub struct ObservedPsaRequest {
    pub sequence: u64,
    pub method: Method,
    pub path: String,
    pub query: Option<String>,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
    authorization_valid: bool,
}

impl ObservedPsaRequest {
    pub fn has_valid_authorization(&self) -> bool {
        self.authorization_valid
    }
}

pub struct ProviderRequestBlock {
    state: Arc<RequestBlockState>,
}

impl ProviderRequestBlock {
    pub async fn wait_until_blocked(&self) {
        tokio::time::timeout(WORKER_WAIT, self.state.wait_until_arrived())
            .await
            .expect("provider request did not reach its response block");
    }

    pub fn release(&self) {
        self.state.release();
    }
}

pub async fn start_pinning_harness(config: PinningHarnessConfig) -> PinningHarness {
    let pinata = MockServer::start().await;
    let filebase = MockServer::start().await;
    let pinata_script = Arc::new(PsaScriptState::new(config.pinata_script, PINATA_TOKEN));
    let filebase_script = Arc::new(PsaScriptState::new(config.filebase_script, FILEBASE_TOKEN));
    mount_psa_script(&pinata, pinata_script.clone()).await;
    mount_psa_script(&filebase, filebase_script.clone()).await;

    let sequence = Arc::new(AtomicU64::new(1));
    let pinata_proxy = ProviderProxy::start(pinata.uri(), PINATA_TOKEN, sequence.clone()).await;
    let filebase_proxy = ProviderProxy::start(filebase.uri(), FILEBASE_TOKEN, sequence).await;

    let mut provider_kinds = HashMap::new();
    let providers = config
        .providers
        .into_iter()
        .map(|provider| {
            provider_kinds.insert(provider.name.clone(), provider.kind);
            provider.into_provider_config(&pinata_proxy.endpoint, &filebase_proxy.endpoint)
        })
        .collect();
    let raw_pinning = PinningConfig {
        worker_interval: "5s".to_owned(),
        worker_concurrency: 4,
        providers,
        policies: config.policies,
    };
    let validated = ValidatedPinningConfig::from_raw(&raw_pinning, |name| match name {
        PINATA_TOKEN_ENV => Some(PINATA_TOKEN.to_owned()),
        FILEBASE_TOKEN_ENV => Some(FILEBASE_TOKEN.to_owned()),
        _ => None,
    })
    .expect("validate test pinning config");
    let pinning = PinningCoordinator::build(validated).expect("build test pinning coordinator");

    let KuboHarness { server: kubo, .. } = start_kubo_harness(config.kubo_script).await;
    let db = sea_orm::Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite database");
    ipfs_s3_gateway::store::run_migrations(&db)
        .await
        .expect("run test migrations");
    let bucket = "test-bkt".to_owned();
    ipfs_s3_gateway::store::bucket::create(&db, &bucket, None)
        .await
        .expect("create test bucket");
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: ipfs_s3_gateway::store::Store::new(db),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64))
            .expect("zero test master key"),
        pinning,
    });
    let observed_http = Arc::new(Mutex::new(Vec::<ObservedHttpRequest>::new()));
    let s3_server = start_s3_server(state.clone(), observed_http).await;

    PinningHarness {
        endpoint: s3_server.endpoint.clone(),
        bucket,
        state,
        kubo,
        pinata,
        filebase,
        worker: None,
        s3_server: Some(s3_server),
        pinata_proxy: Some(pinata_proxy),
        filebase_proxy: Some(filebase_proxy),
        pinata_script,
        filebase_script,
        provider_kinds,
    }
}

impl PinningHarness {
    pub async fn pinata_requests(&self) -> Vec<ObservedPsaRequest> {
        self.pinata_proxy().requests().await
    }

    pub async fn filebase_requests(&self) -> Vec<ObservedPsaRequest> {
        self.filebase_proxy().requests().await
    }

    pub async fn provider_requests(&self) -> Vec<ObservedPsaRequest> {
        let mut requests = self.pinata_requests().await;
        requests.extend(self.filebase_requests().await);
        requests.sort_by_key(|request| request.sequence);
        requests
    }

    pub async fn block_next_submit(&self, provider: &str) -> ProviderRequestBlock {
        self.proxy_for_provider(provider).block_next_submit().await
    }

    pub async fn block_next_delete(&self, provider: &str) -> ProviderRequestBlock {
        self.proxy_for_provider(provider).block_next_delete().await
    }

    pub async fn wait_for_provider_request(
        &self,
        provider: &str,
        method: Method,
        path: &str,
        occurrence: usize,
    ) -> ObservedPsaRequest {
        self.proxy_for_provider(provider)
            .wait_for_request(method, path, occurrence)
            .await
    }

    pub fn restart_worker(&mut self) {
        assert!(self.worker.is_none(), "pinning worker is already running");
        self.worker = Some(
            self.state
                .pinning
                .start(self.state.store.clone(), CancellationToken::new()),
        );
    }

    pub async fn stop_worker_without_unlocking(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.shutdown(WORKER_SHUTDOWN_GRACE).await;
        }
    }

    pub async fn run_worker_until_idle(&mut self) {
        self.restart_worker();
        self.wait_for_worker_idle().await;
        self.stop_worker_without_unlocking().await;
    }

    pub async fn wait_for_worker_idle(&self) {
        assert!(
            self.worker.is_some(),
            "start the worker before waiting for idle"
        );
        let wait_for_idle = async {
            loop {
                let jobs = self.pin_jobs().await;
                let now = Utc::now();
                let active = jobs.iter().any(|job| {
                    job.state == "running" || (job.state == "pending" && job.next_attempt_at <= now)
                });
                if !active {
                    tokio::task::yield_now().await;
                    let jobs = self.pin_jobs().await;
                    let now = Utc::now();
                    if !jobs.iter().any(|job| {
                        job.state == "running"
                            || (job.state == "pending" && job.next_attempt_at <= now)
                    }) {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(WORKER_WAIT, wait_for_idle)
            .await
            .expect("pinning worker did not become idle");
    }

    pub async fn advance_past_job_lock(&self) {
        let now = Utc::now() - chrono::Duration::seconds(1);
        let locked = pin_job::Entity::find()
            .filter(pin_job::Column::LockedUntil.is_not_null())
            .all(self.state.store.db())
            .await
            .expect("load locked pinning jobs");
        assert!(!locked.is_empty(), "no pinning job lock to advance past");
        for job in locked {
            let mut active: pin_job::ActiveModel = job.into();
            active.locked_until = Set(Some(now));
            active.next_attempt_at = Set(now);
            active
                .update(self.state.store.db())
                .await
                .expect("advance pinning job past its lock");
        }
    }

    pub async fn advance_job_due(&self, job_id: &str) {
        let job = self.pin_job(job_id).await;
        assert_eq!(job.state, "pending", "only pending work can be made due");
        let mut active: pin_job::ActiveModel = job.into();
        active.next_attempt_at = Set(Utc::now() - chrono::Duration::seconds(1));
        active
            .update(self.state.store.db())
            .await
            .expect("advance pinning job due time");
    }

    /// Deterministically makes the durable retry for a failed shared remote due.
    ///
    /// This is a clock control only: publication and provider lifecycle traffic still
    /// traverse the signed-S3 and PSA surfaces.
    pub async fn advance_remote_retry_due(&self, provider: &str, cid: &str) {
        let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(self.state.store.db())
            .await
            .expect("load failed remote retry")
            .expect("failed remote retry exists");
        assert_eq!(
            remote.status, "failed",
            "only failed remotes have a retry clock"
        );
        assert!(
            remote.next_retry_at.is_some(),
            "failed remote retry must not be exhausted"
        );
        let mut active: remote_pin::ActiveModel = remote.into();
        active.next_retry_at = Set(Some(Utc::now() - chrono::Duration::seconds(1)));
        active
            .update(self.state.store.db())
            .await
            .expect("advance failed remote retry due time");
    }

    pub async fn pin_job(&self, job_id: &str) -> pin_job::Model {
        pin_job::Entity::find_by_id(job_id.to_owned())
            .one(self.state.store.db())
            .await
            .expect("load pinning job")
            .expect("pinning job exists")
    }

    pub async fn wait_for_job_state(&self, job_id: &str, state: &str) -> pin_job::Model {
        let wait = async {
            loop {
                let job = self.pin_job(job_id).await;
                if job.state == state {
                    return job;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(WORKER_WAIT, wait)
            .await
            .expect("pinning job did not reach the expected state")
    }

    pub async fn advance_past_lease_expiry(&self, key: &str) {
        let object =
            ipfs_s3_gateway::store::object::get_latest(self.state.store.db(), &self.bucket, key)
                .await
                .expect("load latest object for expiry control");
        let leases = pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(object.id))
            .filter(pin_lease::Column::State.eq("active"))
            .all(self.state.store.db())
            .await
            .expect("load active leases for expiry control");
        assert!(!leases.is_empty(), "no active pinning lease to expire");
        let expired_at = Utc::now() - chrono::Duration::seconds(1);
        for lease in leases {
            let mut active: pin_lease::ActiveModel = lease.into();
            active.expires_at = Set(expired_at);
            active
                .update(self.state.store.db())
                .await
                .expect("advance pinning lease past expiry");
        }
    }

    pub async fn run_current_reconcile(&mut self, provider: &str, cid: &str) {
        assert!(
            self.worker.is_none(),
            "stop the worker before forcing Reconcile due"
        );
        let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(self.state.store.db())
            .await
            .expect("load current remote pin")
            .expect("current remote pin exists");
        let job = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("reconcile"))
            .filter(pin_job::Column::Provider.eq(provider))
            .filter(pin_job::Column::Cid.eq(cid))
            .filter(pin_job::Column::ExpectedRemoteEpoch.eq(remote.epoch))
            .one(self.state.store.db())
            .await
            .expect("load current Reconcile job")
            .expect("current Reconcile job exists");
        let job_id = job.id.clone();
        let mut active: pin_job::ActiveModel = job.into();
        active.next_attempt_at = Set(Utc::now());
        active
            .update(self.state.store.db())
            .await
            .expect("make current Reconcile due");
        self.restart_worker();
        let wait_for_reconcile = async {
            loop {
                let job = pin_job::Entity::find_by_id(job_id.clone())
                    .one(self.state.store.db())
                    .await
                    .expect("reload current Reconcile job")
                    .expect("current Reconcile job remains durable");
                let still_running = job.state == "running"
                    || (job.state == "pending" && job.next_attempt_at <= Utc::now());
                if !still_running {
                    break;
                }
                tokio::task::yield_now().await;
            }
        };
        if tokio::time::timeout(WORKER_WAIT, wait_for_reconcile)
            .await
            .is_err()
        {
            panic!(
                "current Reconcile job did not settle: {:?}",
                self.pin_job(&job_id).await
            );
        }
        self.stop_worker_without_unlocking().await;
    }

    pub async fn enqueue_current_reconcile(&self, provider: &str, cid: &str) {
        assert!(
            self.worker.is_none(),
            "stop the worker before adding deterministic Reconcile work"
        );
        let remote = remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(self.state.store.db())
            .await
            .expect("load current remote pin")
            .expect("current remote pin exists");
        jobs::enqueue_job(
            self.state.store.db(),
            jobs::reconcile_job(provider, cid, remote.epoch, Utc::now()),
        )
        .await
        .expect("enqueue deterministic current Reconcile");
    }

    pub async fn pin_jobs(&self) -> Vec<pin_job::Model> {
        pin_job::Entity::find()
            .order_by_asc(pin_job::Column::CreatedAt)
            .order_by_asc(pin_job::Column::Id)
            .all(self.state.store.db())
            .await
            .expect("load pinning jobs")
    }

    pub async fn pin_leases(&self) -> Vec<pin_lease::Model> {
        pin_lease::Entity::find()
            .order_by_asc(pin_lease::Column::CreatedAt)
            .order_by_asc(pin_lease::Column::Id)
            .all(self.state.store.db())
            .await
            .expect("load pinning leases")
    }

    pub async fn pin_targets(&self) -> Vec<pin_lease_target::Model> {
        pin_lease_target::Entity::find()
            .order_by_asc(pin_lease_target::Column::Provider)
            .order_by_asc(pin_lease_target::Column::Id)
            .all(self.state.store.db())
            .await
            .expect("load pinning targets")
    }

    pub async fn remote_pins(&self) -> Vec<remote_pin::Model> {
        remote_pin::Entity::find()
            .order_by_asc(remote_pin::Column::Provider)
            .order_by_asc(remote_pin::Column::Cid)
            .all(self.state.store.db())
            .await
            .expect("load remote pins")
    }

    pub async fn provider_usages(&self) -> Vec<pin_provider_usage::Model> {
        pin_provider_usage::Entity::find()
            .order_by_asc(pin_provider_usage::Column::Provider)
            .all(self.state.store.db())
            .await
            .expect("load pinning provider usage")
    }

    pub async fn provider_usage(&self, provider: &str) -> pin_provider_usage::Model {
        pin_provider_usage::Entity::find_by_id(provider.to_owned())
            .one(self.state.store.db())
            .await
            .expect("load pinning provider usage")
            .expect("pinning provider usage exists")
    }

    pub async fn active_lease_sources(&self, key: &str) -> Vec<String> {
        let object =
            ipfs_s3_gateway::store::object::get_latest(self.state.store.db(), &self.bucket, key)
                .await
                .expect("load latest object for lease assertion");
        let mut sources = pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(object.id))
            .filter(pin_lease::Column::State.eq("active"))
            .all(self.state.store.db())
            .await
            .expect("load active object leases")
            .into_iter()
            .map(|lease| lease.source)
            .collect::<Vec<_>>();
        sources.sort();
        sources
    }

    pub async fn target_states(&self, key: &str) -> Vec<(String, String)> {
        let object =
            ipfs_s3_gateway::store::object::get_latest(self.state.store.db(), &self.bucket, key)
                .await
                .expect("load latest object for target assertion");
        let lease_ids = pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(object.id))
            .all(self.state.store.db())
            .await
            .expect("load object leases")
            .into_iter()
            .map(|lease| lease.id)
            .collect::<Vec<_>>();
        let mut states = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.is_in(lease_ids))
            .all(self.state.store.db())
            .await
            .expect("load object lease targets")
            .into_iter()
            .map(|target| (target.provider, target.state))
            .collect::<Vec<_>>();
        states.sort();
        states
    }

    pub async fn shutdown(mut self) {
        self.stop_worker_without_unlocking().await;
        self.pinata_proxy().release_all_blocks().await;
        self.filebase_proxy().release_all_blocks().await;
        if let Some(server) = self.s3_server.take() {
            server.shutdown().await;
        }
        if let Some(proxy) = self.pinata_proxy.take() {
            proxy.shutdown().await;
        }
        if let Some(proxy) = self.filebase_proxy.take() {
            proxy.shutdown().await;
        }
        self.pinata_script.assert_clean("Pinata");
        self.filebase_script.assert_clean("Filebase");
        assert_hot_standard_residency_invariant(self.state.store.db()).await;
    }

    fn proxy_for_provider(&self, provider: &str) -> &ProviderProxy {
        match self.provider_kinds.get(provider) {
            Some(TestProviderKind::Pinata) => self.pinata_proxy(),
            Some(TestProviderKind::Filebase) => self.filebase_proxy(),
            None => panic!("unknown test pinning provider"),
        }
    }

    fn pinata_proxy(&self) -> &ProviderProxy {
        self.pinata_proxy.as_ref().expect("Pinata proxy is running")
    }

    fn filebase_proxy(&self) -> &ProviderProxy {
        self.filebase_proxy
            .as_ref()
            .expect("Filebase proxy is running")
    }
}

struct PsaScriptState {
    replies: StdMutex<VecDeque<PsaReply>>,
    unexpected: AtomicUsize,
    authorization_failures: AtomicUsize,
    expected_token: &'static str,
}

impl PsaScriptState {
    fn new(replies: Vec<PsaReply>, expected_token: &'static str) -> Self {
        Self {
            replies: StdMutex::new(VecDeque::from(replies)),
            unexpected: AtomicUsize::new(0),
            authorization_failures: AtomicUsize::new(0),
            expected_token,
        }
    }

    fn assert_clean(&self, provider: &str) {
        let remaining = self
            .replies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        assert_eq!(
            self.unexpected.load(Ordering::SeqCst),
            0,
            "{provider} received an unexpected scripted request"
        );
        assert_eq!(
            self.authorization_failures.load(Ordering::SeqCst),
            0,
            "{provider} received a request with invalid authorization"
        );
        assert_eq!(
            remaining, 0,
            "{provider} has unconsumed scripted PSA replies"
        );
    }
}

impl Respond for PsaScriptState {
    fn respond(&self, request: &WiremockRequest) -> ResponseTemplate {
        if !authorization_matches(&request.headers, self.expected_token) {
            self.authorization_failures.fetch_add(1, Ordering::SeqCst);
            return ResponseTemplate::new(StatusCode::UNAUTHORIZED.as_u16())
                .set_body_string("invalid test authorization");
        }
        let actual_query = sorted_query(
            request
                .url
                .query_pairs()
                .map(|(key, value)| (key.into_owned(), value.into_owned())),
        );
        let actual_json = serde_json::from_slice(&request.body).ok();
        let mut replies = self
            .replies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let matching_indices = replies
            .iter()
            .enumerate()
            .filter(|(_, reply)| {
                request.method == reply.method
                    && request.url.path() == reply.path
                    && reply
                        .query
                        .as_ref()
                        .is_none_or(|expected| sorted_query(expected.clone()) == actual_query)
                    && reply.request_json_subset.as_ref().is_none_or(|expected| {
                        actual_json
                            .as_ref()
                            .is_some_and(|actual| json_matches_subset(actual, expected))
                    })
                    && reply.matches_find_request(&actual_query)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let Some(&first_matching_index) = matching_indices.first() else {
            self.unexpected.fetch_add(1, Ordering::SeqCst);
            return ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR.as_u16())
                .set_body_string("unexpected scripted request");
        };
        let first_matching_reply = &replies[first_matching_index];
        if matching_indices
            .iter()
            .skip(1)
            .any(|&index| !first_matching_reply.has_same_matcher_signature(&replies[index]))
        {
            self.unexpected.fetch_add(1, Ordering::SeqCst);
            return ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR.as_u16())
                .set_body_string("ambiguous scripted request");
        }
        let reply = replies
            .remove(first_matching_index)
            .expect("matched scripted PSA reply must still be pending");
        ResponseTemplate::new(reply.status).set_body_bytes(reply.response_body(request))
    }
}

async fn mount_psa_script(server: &MockServer, script: Arc<PsaScriptState>) {
    Mock::given(wiremock::matchers::any())
        .respond_with(move |request: &WiremockRequest| script.respond(request))
        .mount(server)
        .await;
}

fn sorted_query<I>(query: I) -> Vec<(String, String)>
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut query = query.into_iter().collect::<Vec<_>>();
    query.sort();
    query
}

fn json_matches_subset(actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
    match (actual, expected) {
        (serde_json::Value::Object(actual), serde_json::Value::Object(expected)) => {
            expected.iter().all(|(key, value)| {
                actual
                    .get(key)
                    .is_some_and(|actual| json_matches_subset(actual, value))
            })
        }
        (serde_json::Value::Array(actual), serde_json::Value::Array(expected)) => {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected)
                    .all(|(actual, expected)| json_matches_subset(actual, expected))
        }
        _ => actual == expected,
    }
}

struct ProviderProxy {
    endpoint: String,
    state: Arc<ProviderProxyState>,
    cancellation: CancellationToken,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl ProviderProxy {
    async fn start(
        upstream: String,
        expected_token: &'static str,
        sequence: Arc<AtomicU64>,
    ) -> Self {
        let state = Arc::new(ProviderProxyState {
            upstream,
            expected_token,
            sequence,
            requests: Mutex::new(Vec::new()),
            request_observed: Notify::new(),
            next_submit_block: Mutex::new(None),
            next_delete_block: Mutex::new(None),
            all_blocks: Mutex::new(Vec::new()),
            client: reqwest::Client::new(),
        });
        let app = Router::new()
            .fallback(proxy_provider_request)
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind provider proxy listener");
        let port = listener
            .local_addr()
            .expect("provider proxy listener address")
            .port();
        let cancellation = CancellationToken::new();
        let server_cancellation = cancellation.clone();
        let join = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(server_cancellation.cancelled_owned())
                .await
                .expect("provider proxy terminated unexpectedly");
        });
        Self {
            endpoint: format!("http://127.0.0.1:{port}"),
            state,
            cancellation,
            join: Some(join),
        }
    }

    async fn requests(&self) -> Vec<ObservedPsaRequest> {
        self.state.requests.lock().await.clone()
    }

    async fn block_next_submit(&self) -> ProviderRequestBlock {
        self.state.arm_block(&self.state.next_submit_block).await
    }

    async fn block_next_delete(&self) -> ProviderRequestBlock {
        self.state.arm_block(&self.state.next_delete_block).await
    }

    async fn wait_for_request(
        &self,
        method: Method,
        path: &str,
        occurrence: usize,
    ) -> ObservedPsaRequest {
        assert!(occurrence > 0, "provider request occurrence is one-based");
        let wait = async {
            loop {
                let notified = self.state.request_observed.notified();
                if let Some(request) = self
                    .state
                    .requests
                    .lock()
                    .await
                    .iter()
                    .filter(|request| request.method == method && request.path == path)
                    .nth(occurrence - 1)
                    .cloned()
                {
                    return request;
                }
                notified.await;
            }
        };
        tokio::time::timeout(WORKER_WAIT, wait)
            .await
            .expect("expected provider request was not observed")
    }

    async fn release_all_blocks(&self) {
        self.state.release_all_blocks().await;
    }

    async fn shutdown(mut self) {
        self.release_all_blocks().await;
        self.cancellation.cancel();
        if let Some(mut join) = self.join.take() {
            match tokio::time::timeout(Duration::from_secs(2), &mut join).await {
                Ok(result) => result.expect("provider proxy task failed"),
                Err(_) => {
                    join.abort();
                    let _ = join.await;
                }
            }
        }
    }
}

impl Drop for ProviderProxy {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

struct ProviderProxyState {
    upstream: String,
    expected_token: &'static str,
    sequence: Arc<AtomicU64>,
    requests: Mutex<Vec<ObservedPsaRequest>>,
    request_observed: Notify,
    next_submit_block: Mutex<Option<Arc<RequestBlockState>>>,
    next_delete_block: Mutex<Option<Arc<RequestBlockState>>>,
    all_blocks: Mutex<Vec<Arc<RequestBlockState>>>,
    client: reqwest::Client,
}

impl ProviderProxyState {
    async fn arm_block(
        &self,
        slot: &Mutex<Option<Arc<RequestBlockState>>>,
    ) -> ProviderRequestBlock {
        let state = Arc::new(RequestBlockState::new());
        let mut armed = slot.lock().await;
        assert!(armed.is_none(), "a provider request block is already armed");
        *armed = Some(state.clone());
        self.all_blocks.lock().await.push(state.clone());
        ProviderRequestBlock { state }
    }

    async fn maybe_block(&self, method: &Method, path: &str) {
        let slot = if method == Method::POST && path.ends_with("/pins") {
            Some(&self.next_submit_block)
        } else if method == Method::DELETE {
            Some(&self.next_delete_block)
        } else {
            None
        };
        if let Some(slot) = slot
            && let Some(block) = slot.lock().await.take()
        {
            block.mark_arrived();
            block.wait_until_released().await;
        }
    }

    async fn release_all_blocks(&self) {
        for block in self.all_blocks.lock().await.iter() {
            block.release();
        }
    }
}

struct RequestBlockState {
    arrived: AtomicBool,
    released: AtomicBool,
    arrived_notify: Notify,
    released_notify: Notify,
}

impl RequestBlockState {
    fn new() -> Self {
        Self {
            arrived: AtomicBool::new(false),
            released: AtomicBool::new(false),
            arrived_notify: Notify::new(),
            released_notify: Notify::new(),
        }
    }

    fn mark_arrived(&self) {
        self.arrived.store(true, Ordering::SeqCst);
        self.arrived_notify.notify_waiters();
    }

    async fn wait_until_arrived(&self) {
        while !self.arrived.load(Ordering::SeqCst) {
            let notified = self.arrived_notify.notified();
            if self.arrived.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.released_notify.notify_waiters();
    }

    async fn wait_until_released(&self) {
        while !self.released.load(Ordering::SeqCst) {
            let notified = self.released_notify.notified();
            if self.released.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
    }
}

async fn proxy_provider_request(
    State(state): State<Arc<ProviderProxyState>>,
    request: Request<Body>,
) -> AxumResponse {
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, 1024 * 1024).await {
        Ok(body) => body,
        Err(_) => return safe_proxy_error(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let path = parts.uri.path().to_owned();
    let query = parts.uri.query().map(str::to_owned);
    let authorization_valid = authorization_matches(&parts.headers, state.expected_token);
    let mut captured_headers = parts.headers.clone();
    if captured_headers.contains_key(header::AUTHORIZATION) {
        captured_headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("[REDACTED]"),
        );
    }
    let sequence = state.sequence.fetch_add(1, Ordering::SeqCst);
    state.requests.lock().await.push(ObservedPsaRequest {
        sequence,
        method: parts.method.clone(),
        path: path.clone(),
        query,
        headers: captured_headers,
        body: body.to_vec(),
        authorization_valid,
    });
    state.request_observed.notify_waiters();

    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let mut upstream = state.client.request(
        parts.method.clone(),
        format!("{}{path_and_query}", state.upstream),
    );
    for (name, value) in &parts.headers {
        if !matches!(
            name,
            &header::HOST
                | &header::CONTENT_LENGTH
                | &header::TRANSFER_ENCODING
                | &header::CONNECTION
        ) {
            upstream = upstream.header(name, value);
        }
    }
    let response = match upstream.body(body).send().await {
        Ok(response) => response,
        Err(_) => return safe_proxy_error(StatusCode::BAD_GATEWAY),
    };
    let status = response.status();
    let response_headers = response.headers().clone();
    let response_body = match response.bytes().await {
        Ok(body) => body,
        Err(_) => return safe_proxy_error(StatusCode::BAD_GATEWAY),
    };

    state.maybe_block(&parts.method, &path).await;

    let mut output = Response::new(Body::from(response_body));
    *output.status_mut() = status;
    for (name, value) in &response_headers {
        if !matches!(name, &header::TRANSFER_ENCODING | &header::CONNECTION) {
            output.headers_mut().append(name, value.clone());
        }
    }
    output
}

fn safe_proxy_error(status: StatusCode) -> AxumResponse {
    let mut response = Response::new(Body::from("test provider proxy error"));
    *response.status_mut() = status;
    response
}

fn authorization_matches(headers: &HeaderMap, expected_token: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.len() == "Bearer ".len() + expected_token.len()
                && value.starts_with("Bearer ")
                && &value["Bearer ".len()..] == expected_token
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn submit(server: &MockServer, cid: &str) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("{}/psa/pins", server.uri()))
            .bearer_auth(PINATA_TOKEN)
            .json(&serde_json::json!({
                "cid": cid,
                "name": "test object",
                "origins": [],
                "meta": { "source": "test" }
            }))
            .send()
            .await
            .expect("send scripted PSA submit")
    }

    async fn get_pin(server: &MockServer) -> reqwest::Response {
        reqwest::Client::new()
            .get(format!("{}/psa/pins/request-1", server.uri()))
            .bearer_auth(PINATA_TOKEN)
            .send()
            .await
            .expect("send scripted PSA get")
    }

    fn get_pin_reply(status: &str) -> PsaReply {
        PsaReply::json(
            Method::GET,
            "/psa/pins/request-1",
            StatusCode::OK.as_u16(),
            serde_json::json!({
                "requestid": "request-1",
                "status": status,
                "pin": { "cid": "QmA", "meta": {} },
                "info": {}
            }),
        )
    }

    #[tokio::test]
    async fn psa_script_matches_reversed_submit_requests_by_cid() {
        let server = MockServer::start().await;
        let script = Arc::new(PsaScriptState::new(
            vec![
                PsaReply::pinned_submit("/psa/pins", "request-a", "QmA"),
                PsaReply::pinned_submit("/psa/pins", "request-b", "QmB"),
            ],
            PINATA_TOKEN,
        ));
        mount_psa_script(&server, script.clone()).await;

        let response_b = submit(&server, "QmB").await;
        assert_eq!(response_b.status(), StatusCode::OK);
        let response_b: serde_json::Value = response_b
            .json()
            .await
            .expect("decode scripted response for B");
        assert!(
            response_b.get("requestid").and_then(|value| value.as_str()) == Some("request-b")
                && response_b
                    .pointer("/pin/cid")
                    .and_then(|value| value.as_str())
                    == Some("QmB"),
            "CID B received the wrong scripted response"
        );

        let response_a = submit(&server, "QmA").await;
        assert_eq!(response_a.status(), StatusCode::OK);
        let response_a: serde_json::Value = response_a
            .json()
            .await
            .expect("decode scripted response for A");
        assert!(
            response_a.get("requestid").and_then(|value| value.as_str()) == Some("request-a")
                && response_a
                    .pointer("/pin/cid")
                    .and_then(|value| value.as_str())
                    == Some("QmA"),
            "CID A received the wrong scripted response"
        );
        script.assert_clean("test provider");
    }

    #[tokio::test]
    async fn psa_script_consumes_identical_get_signatures_in_script_order() {
        let server = MockServer::start().await;
        let script = Arc::new(PsaScriptState::new(
            vec![get_pin_reply("queued"), get_pin_reply("pinned")],
            PINATA_TOKEN,
        ));
        mount_psa_script(&server, script.clone()).await;

        let first = get_pin(&server).await;
        assert_eq!(first.status(), StatusCode::OK);
        let first: serde_json::Value = first.json().await.expect("decode first scripted PSA get");
        assert!(
            first.get("status").and_then(|value| value.as_str()) == Some("queued"),
            "first identical request did not receive the earliest scripted reply"
        );

        let second = get_pin(&server).await;
        assert_eq!(second.status(), StatusCode::OK);
        let second: serde_json::Value =
            second.json().await.expect("decode second scripted PSA get");
        assert!(
            second.get("status").and_then(|value| value.as_str()) == Some("pinned"),
            "second identical request did not receive the next scripted reply"
        );
        script.assert_clean("test provider");
    }

    #[tokio::test]
    async fn psa_script_does_not_consume_an_unmatched_reply() {
        let server = MockServer::start().await;
        let script = Arc::new(PsaScriptState::new(
            vec![PsaReply::pinned_submit("/psa/pins", "request-a", "QmA")],
            PINATA_TOKEN,
        ));
        mount_psa_script(&server, script.clone()).await;

        assert_eq!(
            submit(&server, "QmB").await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            script
                .replies
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1,
            "an unmatched request consumed a scripted reply"
        );
        assert_eq!(submit(&server, "QmA").await.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn psa_script_rejects_ambiguous_matches_without_consuming_them() {
        let server = MockServer::start().await;
        let script = Arc::new(PsaScriptState::new(
            vec![
                PsaReply::json(
                    Method::POST,
                    "/psa/pins",
                    StatusCode::OK.as_u16(),
                    serde_json::json!({
                        "requestid": "generic-request",
                        "status": "pinned",
                        "pin": { "cid": "QmA", "meta": {} },
                        "info": {}
                    }),
                ),
                PsaReply::pinned_submit("/psa/pins", "cid-request", "QmA"),
            ],
            PINATA_TOKEN,
        ));
        mount_psa_script(&server, script.clone()).await;

        assert_eq!(
            submit(&server, "QmA").await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            script
                .replies
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2,
            "an ambiguous request consumed a scripted reply"
        );
    }
}
