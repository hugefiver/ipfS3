//! ZIP v2 fanout through signed S3, the durable pin worker and real provider HTTP.
#[allow(dead_code)]
mod support;

use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use chrono::Utc;
use http::HeaderValue;
use ipfs_s3_gateway::{
    config::{Config, OptionalPinControlMode},
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::{PinningCoordinator, normalize_validated_config},
        identity::{CleanupMode, PinningIdentityConfig, ProviderIdentityConfig},
        zip_policy::{ZipOutputRuleConfig, ZipRuleEffect},
    },
    state::AppState,
    store::{
        self,
        entities::{pin_job, pin_lease_target, pin_provider_usage, remote_pin},
        pinning::jobs,
    },
};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, Set};
use serde_json::{Value, json};
use support::decompress::{AddReply, KuboScript, start_kubo_harness, start_s3_server};
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

const W: usize = 4;
const BLOCKED: &str = "QmBlocked";
const RECOVERED: &str = "QmA25";
const FAILED: &str = "QmB11";
const NORMAL: &str = "QmNormal";

#[derive(Clone, Debug)]
struct Call {
    provider: &'static str,
    method: &'static str,
    cid: String,
}

#[derive(Default)]
struct Traffic {
    events: Vec<Call>,
    current: usize,
    max_global: usize,
    a: usize,
    b: usize,
    max_a: usize,
    max_b: usize,
}

#[derive(Clone)]
struct ProviderState {
    name: &'static str,
    shared: Arc<Mutex<Traffic>>,
    changed: Arc<Notify>,
    first: Arc<AtomicBool>,
    release: Arc<Notify>,
    released: Arc<AtomicBool>,
}

struct ActiveCall(ProviderState);
impl Drop for ActiveCall {
    fn drop(&mut self) {
        let state = self.0.clone();
        tokio::spawn(async move {
            let mut traffic = state.shared.lock().await;
            traffic.current -= 1;
            if state.name == "a" {
                traffic.a -= 1;
            } else {
                traffic.b -= 1;
            }
            state.changed.notify_waiters();
        });
    }
}

impl ProviderState {
    async fn enter(&self, method: &'static str, cid: String) -> ActiveCall {
        let mut t = self.shared.lock().await;
        t.events.push(Call {
            provider: self.name,
            method,
            cid,
        });
        t.current += 1;
        t.max_global = t.max_global.max(t.current);
        if self.name == "a" {
            t.a += 1;
            t.max_a = t.max_a.max(t.a);
        } else {
            t.b += 1;
            t.max_b = t.max_b.max(t.b);
        }
        self.changed.notify_waiters();
        ActiveCall(self.clone())
    }
    async fn wait_first_post(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let notified = self.changed.notified();
                if self
                    .shared
                    .lock()
                    .await
                    .events
                    .iter()
                    .any(|e| e.provider == self.name && e.method == "POST")
                {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("first provider POST reached the barrier");
    }
    fn open(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

async fn submit(
    State(state): State<ProviderState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers.get("authorization").unwrap(), "Bearer test-token");
    let cid = body["cid"].as_str().expect("PSA CID").to_owned();
    let _active = state.enter("POST", cid.clone()).await;
    if !state.first.swap(true, Ordering::SeqCst) {
        while !state.released.load(Ordering::SeqCst) {
            state.release.notified().await;
        }
    }
    // Allows actual overlapping HTTP handlers to be measured after the explicit barrier.
    tokio::time::sleep(Duration::from_millis(25)).await;
    if cid == FAILED {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid CID"})),
        );
    }
    (
        StatusCode::OK,
        Json(
            json!({"requestid": format!("request-{cid}"), "status":"pinned", "pin":{"cid":cid,"meta":{}}, "info":{}}),
        ),
    )
}

async fn find(
    State(state): State<ProviderState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers.get("authorization").unwrap(), "Bearer test-token");
    let cid = query.get("cid").expect("find CID").clone();
    let metadata: Value =
        serde_json::from_str(query.get("meta").expect("find correlation")).unwrap();
    assert!(metadata["gateway_job_id"].is_string());
    let _active = state.enter("GET", cid.clone()).await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    if cid == RECOVERED {
        Json(
            json!({"count":1,"results":[{"requestid":format!("request-{cid}"),"status":"pinned","pin":{"cid":cid,"meta":metadata},"info":{}}]}),
        )
    } else {
        Json(json!({"count":0,"results":[]}))
    }
}

async fn provider(
    name: &'static str,
    shared: Arc<Mutex<Traffic>>,
    changed: Arc<Notify>,
) -> (ProviderState, tokio::task::JoinHandle<()>, String) {
    let state = ProviderState {
        name,
        shared,
        changed,
        first: Arc::new(AtomicBool::new(false)),
        release: Arc::new(Notify::new()),
        released: Arc::new(AtomicBool::new(false)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/v1/ipfs/pins", post(submit).get(find))
        .with_state(state.clone());
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (state, handle, endpoint)
}

fn u16_into(bytes: &mut Vec<u8>, n: usize) {
    bytes.extend_from_slice(&(n as u16).to_le_bytes());
}
fn u32_into(bytes: &mut Vec<u8>, n: usize) {
    bytes.extend_from_slice(&(n as u32).to_le_bytes());
}
fn crc(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
fn archive(names: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut central = Vec::new();
    for name in names {
        let data = if name == "b/blocked.txt" {
            vec![b'x'; 200]
        } else {
            b"data".to_vec()
        };
        let offset = out.len();
        let checksum = crc(&data);
        u32_into(&mut out, 0x04034b50);
        for n in [20, 0, 0, 0, 0] {
            u16_into(&mut out, n);
        }
        for n in [checksum as usize, data.len(), data.len()] {
            u32_into(&mut out, n);
        }
        u16_into(&mut out, name.len());
        u16_into(&mut out, 0);
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&data);
        u32_into(&mut central, 0x02014b50);
        for n in [20, 20, 0, 0, 0, 0] {
            u16_into(&mut central, n);
        }
        for n in [checksum as usize, data.len(), data.len()] {
            u32_into(&mut central, n);
        }
        for n in [name.len(), 0, 0, 0, 0] {
            u16_into(&mut central, n);
        }
        u32_into(&mut central, 0);
        u32_into(&mut central, offset);
        central.extend_from_slice(name.as_bytes());
    }
    let central_offset = out.len();
    let central_size = central.len();
    out.extend_from_slice(&central);
    u32_into(&mut out, 0x06054b50);
    for n in [0, 0, names.len(), names.len()] {
        u16_into(&mut out, n);
    }
    u32_into(&mut out, central_size);
    u32_into(&mut out, central_offset);
    u16_into(&mut out, 0);
    out
}

fn cid(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
fn zip_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (key, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "extracted"),
        ("x-ipfs3-zip-token", token),
        ("x-amz-tagging", "ipfs-s3%3Azip-root=false"),
    ] {
        headers.insert(key, HeaderValue::from_str(value).unwrap());
    }
    headers
}

#[tokio::test]
#[ignore = "run explicitly for the multi-provider ZIP fanout acceptance gate"]
async fn signed_zip_batches_share_quota_but_not_logical_leases_and_worker_remains_fair() {
    let shared = Arc::new(Mutex::new(Traffic::default()));
    let changed = Arc::new(Notify::new());
    let (a, a_handle, a_url) = provider("a", shared.clone(), changed.clone()).await;
    let (b, b_handle, b_url) = provider("b", shared.clone(), changed).await;

    let first = (0..33)
        .map(|i| format!("a/first-{i:02}.txt"))
        .chain((0..31).map(|i| format!("b/first-{i:02}.txt")))
        .chain(std::iter::once("b/blocked.txt".into()))
        .collect::<Vec<_>>();
    let second = (0..15)
        .map(|i| format!("a/second-{i:02}.txt"))
        .chain((0..15).map(|i| format!("b/second-{i:02}.txt")))
        .collect::<Vec<_>>();
    let first_zip = archive(&first);
    let second_zip = archive(&second);
    let mut replies = vec![AddReply::Ok("QmArchive1")];
    replies.extend((0..33).map(|i| AddReply::Ok(cid(format!("QmA{:02}", i % 26)))));
    replies.extend((0..31).map(|i| AddReply::Ok(cid(format!("QmB{:02}", i % 10)))));
    replies.push(AddReply::Ok(BLOCKED));
    replies.push(AddReply::Ok("QmArchive2"));
    replies.extend((0..15).map(|i| AddReply::Ok(cid(format!("QmA{:02}", (i + 14) % 28)))));
    replies.extend((0..15).map(|i| AddReply::Ok(cid(format!("QmB{:02}", i % 12)))));
    replies.push(AddReply::Ok(NORMAL));
    let kubo = start_kubo_harness(KuboScript {
        add_replies: replies,
        cat_bodies: HashMap::from([
            ("QmArchive1".into(), first_zip.clone()),
            ("QmArchive2".into(), second_zip.clone()),
        ]),
    })
    .await;
    let raw = format!(
        r#"
        [kubo]
        rpc_url = {:?}
        [storage]
        database_url = "sqlite::memory:"
        [pinning]
        worker_interval = "1s"
        worker_concurrency = 4
        [[pinning.providers]]
        name = "a"
        kind = "filebase"
        priority = 1
        endpoint = {:?}
        token_env = "ZIP_TEST_TOKEN"
        max_pins = 100
        max_bytes = 10000
        requests_per_second = 1000
        [[pinning.providers]]
        name = "b"
        kind = "filebase"
        priority = 2
        endpoint = {:?}
        token_env = "ZIP_TEST_TOKEN"
        max_pins = 100
        max_bytes = 120
        requests_per_second = 1000
        [[pinning.policies]]
        bucket = "test-bkt"
        prefix = "out/a/"
        trigger = "always"
        provider_mode = "one"
        providers = ["a"]
        default_duration = "1h"
        max_duration = "2h"
        allow_decompressed = true
        [[pinning.policies]]
        bucket = "test-bkt"
        prefix = "out/b/"
        trigger = "always"
        provider_mode = "one"
        providers = ["b"]
        default_duration = "1h"
        max_duration = "2h"
        allow_decompressed = true
        [[pinning.policies]]
        bucket = "test-bkt"
        prefix = "normal/"
        trigger = "always"
        provider_mode = "one"
        providers = ["b"]
        default_duration = "1h"
        max_duration = "2h"
    "#,
        kubo.server.uri(),
        format!("{a_url}/v1/ipfs"),
        format!("{b_url}/v1/ipfs")
    );
    let mut config: Config = toml::from_str(&raw).unwrap();
    config.pinning_identity = PinningIdentityConfig {
        primary_storage_domain: Some("test-local-kubo".into()),
        providers: ["a", "b"]
            .into_iter()
            .map(|name| ProviderIdentityConfig {
                config_name: name.into(),
                provider_id: format!("test-{name}"),
                display_name: name.into(),
                backend: "filebase".into(),
                scope: format!("test-{name}"),
                storage_domain: format!("test-{name}"),
                credential_revision: 1,
                endpoint_revision: 1,
                secret_ref: Some("env:ZIP_TEST_TOKEN".into()),
                api_profile: "filebase-psa".into(),
                strategy: "cid".into(),
                retired: false,
                cleanup: CleanupMode::Managed,
            })
            .collect(),
    };
    let validated = normalize_validated_config(
        ValidatedPinningConfig::from_config(&config, |name| {
            (name == "ZIP_TEST_TOKEN").then(|| "test-token".into())
        })
        .unwrap(),
    )
    .unwrap();
    let a_key = validated
        .providers
        .iter()
        .find(|p| p.identity.display_name == "a")
        .unwrap()
        .name
        .clone();
    let b_key = validated
        .providers
        .iter()
        .find(|p| p.identity.display_name == "b")
        .unwrap()
        .name
        .clone();
    config.decompress_zip.pin_output_rules = ["a", "b"]
        .into_iter()
        .enumerate()
        .map(|(i, name)| ZipOutputRuleConfig {
            name: format!("allow-{name}"),
            priority: 1,
            bucket: "test-bkt".into(),
            prefix: format!("out/{name}/"),
            effect: ZipRuleEffect::Allow,
            policy_id: Some(validated.policies[i].identity.clone()),
        })
        .collect();
    let pinning = PinningCoordinator::build_with_kubo_mode_and_zip_root_and_rules(
        validated,
        None,
        OptionalPinControlMode::Strict,
        true,
        &config.decompress_zip.pin_output_rules,
    )
    .unwrap();
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "test-bkt", None).await.unwrap();
    let store = store::Store::new(db);
    pinning.register_identities(&store).await.unwrap();
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.server.uri()),
        cold_kubo: None,
        store,
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning,
    });
    let server = start_s3_server(state.clone(), Arc::new(Mutex::new(Vec::new()))).await;

    for (source, token, bytes, count) in [
        ("first.zip", "first-load", first_zip, 65),
        ("second.zip", "second-load", second_zip, 30),
    ] {
        let response = support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &server.endpoint,
            "test-bkt",
            source,
            &[("decompress-zip", "out/")],
            bytes,
            zip_headers(token),
            "test",
        )
        .await;
        let status = response.status();
        let xml = response.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{xml}");
        assert!(
            xml.contains(&format!("<PublishedCount>{count}</PublishedCount>")),
            "{xml}"
        );
        assert!(xml.contains("<RootStatus>disabled</RootStatus>"), "{xml}");
    }
    let db = state.store.db();
    assert_eq!(
        pin_lease_target::Entity::find().count(db).await.unwrap(),
        95,
        "logical ZIP targets"
    );
    assert_eq!(
        remote_pin::Entity::find().count(db).await.unwrap(),
        40,
        "28 A + 12 B; quota-blocked CID has no remote allocation"
    );
    assert!(
        remote_pin::Entity::find_by_id((b_key.clone(), BLOCKED.to_owned()))
            .one(db)
            .await
            .unwrap()
            .is_none()
    );
    let blocked = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(&b_key))
        .filter(pin_lease_target::Column::Cid.eq(BLOCKED))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(blocked.state, "quota_blocked");
    let before = shared.lock().await;
    assert!(
        before.events.is_empty(),
        "publication does not perform remote HTTP"
    );
    drop(before);

    // A durable expired running Submit with an unknown outbound effect: the worker
    // must query the same historical PSA route, not repeat the uncertain POST.
    let job = pin_job::Entity::find()
        .filter(pin_job::Column::Cid.eq(RECOVERED))
        .filter(pin_job::Column::Operation.eq("submit"))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "pending");
    let mut crashed: pin_job::ActiveModel = job.clone().into();
    crashed.state = Set("running".into());
    crashed.locked_until = Set(Some(Utc::now() - chrono::Duration::seconds(1)));
    crashed.submit_phase = Set(Some("calling".into()));
    crashed.update(db).await.unwrap();
    jobs::history::Entity::insert(jobs::history::ActiveModel {
        job_id: Set(job.id.clone()),
        correlation: Set(None),
        api: Set("psa".into()),
        strategy: Set("cid".into()),
        effect: Set("unknown".into()),
        state: Set("active".into()),
        first_error: Set(None),
        last_error: Set(None),
        submit_calls: Set(1),
        recovery_queries: Set(0),
        started_at: Set(Utc::now()),
    })
    .exec(db)
    .await
    .unwrap();

    let worker = state
        .pinning
        .start(state.store.clone(), CancellationToken::new());
    a.wait_first_post().await;
    b.wait_first_post().await;
    let normal = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &server.endpoint,
        "test-bkt",
        "normal/job",
        &[],
        b"data".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(
        normal.status(),
        StatusCode::OK,
        "{}",
        normal.text().await.unwrap()
    );
    assert_eq!(
        pin_lease_target::Entity::find().count(db).await.unwrap(),
        96
    );
    a.open();
    b.open();

    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let jobs = pin_job::Entity::find().all(db).await.unwrap();
            let traffic = shared.lock().await;
            let normal_seen = traffic
                .events
                .iter()
                .any(|event| event.cid == NORMAL && event.method == "POST");
            let recovered = traffic
                .events
                .iter()
                .any(|event| event.cid == RECOVERED && event.method == "GET");
            let posts = traffic
                .events
                .iter()
                .filter(|event| event.method == "POST")
                .count();
            drop(traffic);
            if normal_seen
                && recovered
                && posts == 40
                && jobs.iter().all(|job| {
                    job.state == "done"
                        || (job.state == "pending" && job.next_attempt_at > Utc::now())
                        || (job.state == "running"
                            && job.locked_until.is_none()
                            && matches!(
                                (job.operation.as_str(), job.last_error.as_deref()),
                                ("submit", Some("provider rejected invalid input"))
                                    | ("reconcile", Some("submit requires operator attention"))
                                    | (
                                        "reconcile",
                                        Some("historical identity unavailable; needs_attention")
                                    )
                            ))
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ZIP fanout and ordinary/recovery work reach a safe settled or parked state");
    worker.shutdown(Duration::from_secs(2)).await;

    let targets = pin_lease_target::Entity::find().all(db).await.unwrap();
    assert_eq!(targets.len(), 96);
    assert_eq!(targets.iter().filter(|t| t.provider == a_key).count(), 48);
    assert_eq!(targets.iter().filter(|t| t.provider == b_key).count(), 48);
    assert_eq!(
        targets
            .iter()
            .filter(|t| t.state == "quota_blocked")
            .count(),
        1
    );
    let remotes = remote_pin::Entity::find().all(db).await.unwrap();
    let distinct = remotes
        .iter()
        .map(|r| (r.provider.clone(), r.cid.clone()))
        .collect::<BTreeSet<_>>();
    assert_eq!(distinct.len(), remotes.len(), "one DB row per provider/CID");
    assert_eq!(remotes.len(), 41, "28 A + 12 B + normal");
    let usage_a = pin_provider_usage::Entity::find_by_id(&a_key)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let usage_b = pin_provider_usage::Entity::find_by_id(&b_key)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(usage_a.reserved_pins, 28);
    assert_eq!(usage_b.reserved_pins, 13);

    let traffic = shared.lock().await;
    let posts = traffic
        .events
        .iter()
        .filter(|e| e.method == "POST")
        .collect::<Vec<_>>();
    assert_eq!(posts.len(), 40, "unique remotes minus adopted recovery");
    assert_eq!(
        posts.iter().filter(|e| e.cid == FAILED).count(),
        1,
        "terminal error has bounded attempts"
    );
    assert!(!posts.iter().any(|e| e.cid == BLOCKED || e.cid == RECOVERED));
    assert_eq!(
        traffic
            .events
            .iter()
            .filter(|e| e.method == "GET" && e.cid == RECOVERED)
            .count(),
        1
    );
    assert!(
        traffic.max_global <= W && traffic.max_a <= W && traffic.max_b <= W,
        "global/provider in-flight max: {}/{}/{}",
        traffic.max_global,
        traffic.max_a,
        traffic.max_b
    );
    assert!(
        traffic.max_global >= 2,
        "the barriers observed both providers in flight"
    );
    let normal_index = traffic
        .events
        .iter()
        .position(|e| e.cid == NORMAL && e.method == "POST")
        .unwrap();
    let final_zip_index = traffic
        .events
        .iter()
        .rposition(|e| e.method == "POST" && e.cid != NORMAL)
        .unwrap();
    assert!(
        normal_index < final_zip_index,
        "normal PUT received service before ZIP backlog drained"
    );
    let recovery_index = traffic
        .events
        .iter()
        .position(|e| e.cid == RECOVERED && e.method == "GET")
        .unwrap();
    assert!(
        recovery_index < final_zip_index,
        "reclaimed work received service during ZIP fanout"
    );
    drop(traffic);
    let history = jobs::submission_history(db, &job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((history.submit_calls, history.recovery_queries), (1, 1));
    server.shutdown().await;
    a_handle.abort();
    b_handle.abort();
}
