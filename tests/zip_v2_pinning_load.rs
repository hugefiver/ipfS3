//! ZIP v2 fanout through signed S3, the durable pin worker and real provider HTTP.
#[path = "support/pin_rows.rs"]
mod pin_rows;
#[allow(dead_code)]
mod support;

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, Method, StatusCode},
    routing::post,
};
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
    store::{self, entities::object_version},
};
use sea_orm::{EntityTrait, PaginatorTrait};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use support::decompress::{AddReply, KuboScript, start_kubo_harness, start_s3_server};
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;

const W: usize = 4;
const BLOCKED_PAYLOAD: &[u8] = &[b'x'; 200];
const FAILED_PAYLOAD: &[u8] = b"b-11";
const NORMAL_PAYLOAD: &[u8] = b"data";

#[derive(Default)]
struct Traffic {
    posts: Vec<(&'static str, String)>,
    unexpected: Vec<Method>,
    current: usize,
    max_global: usize,
}

#[derive(Clone)]
struct ProviderState {
    name: &'static str,
    shared: Arc<Mutex<Traffic>>,
    failed_cid: String,
    first_cid: watch::Sender<Option<String>>,
    release: Arc<Semaphore>,
}

struct ActiveCall(Arc<Mutex<Traffic>>);
impl Drop for ActiveCall {
    fn drop(&mut self) {
        self.0.lock().unwrap().current -= 1;
    }
}

impl ProviderState {
    async fn wait_first_post(&self) -> String {
        let mut first_cid = self.first_cid.subscribe();
        let reached =
            tokio::time::timeout(Duration::from_secs(10), first_cid.wait_for(Option::is_some))
                .await
                .expect("first provider POST reached the barrier")
                .unwrap();
        reached.clone().unwrap()
    }
}

async fn submit(
    State(state): State<ProviderState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers.get("authorization").unwrap(), "Bearer test-token");
    let cid = body["cid"].as_str().expect("PSA CID").to_owned();
    {
        let mut traffic = state.shared.lock().unwrap();
        traffic.posts.push((state.name, cid.clone()));
        traffic.current += 1;
        traffic.max_global = traffic.max_global.max(traffic.current);
    }
    let _active = ActiveCall(state.shared.clone());
    if state.first_cid.send_if_modified(|first| {
        if first.is_some() {
            return false;
        }
        *first = Some(cid.clone());
        true
    }) {
        state.release.acquire().await.unwrap().forget();
    }
    if cid == state.failed_cid {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid CID"})),
        );
    }
    (
        StatusCode::ACCEPTED,
        Json(
            json!({"requestid": format!("request-{cid}"), "status":"queued", "pin":{"cid":cid,"meta":{}}, "info":{}}),
        ),
    )
}

async fn unexpected(State(state): State<ProviderState>, method: Method) -> StatusCode {
    state.shared.lock().unwrap().unexpected.push(method);
    StatusCode::METHOD_NOT_ALLOWED
}

async fn provider(
    name: &'static str,
    shared: Arc<Mutex<Traffic>>,
) -> (ProviderState, tokio::task::JoinHandle<()>, String) {
    let state = ProviderState {
        name,
        shared,
        failed_cid: raw_cid(FAILED_PAYLOAD),
        first_cid: watch::channel(None).0,
        release: Arc::new(Semaphore::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/v1/ipfs/pins", post(submit).fallback(unexpected))
        .fallback(unexpected)
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
fn archive(entries: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut central = Vec::new();
    for (name, data) in entries {
        let offset = out.len();
        let checksum = crc(data);
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
        out.extend_from_slice(data);
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
    for n in [0, 0, entries.len(), entries.len()] {
        u16_into(&mut out, n);
    }
    u32_into(&mut out, central_size);
    u32_into(&mut out, central_offset);
    u16_into(&mut out, 0);
    out
}

fn raw_cid(bytes: &[u8]) -> String {
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &Sha256::digest(bytes)).unwrap();
    cid::Cid::new_v1(0x55, hash).to_string()
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
async fn signed_zip_batches_share_resources_and_provider_acks_leave_s3_unblocked() {
    let shared = Arc::new(Mutex::new(Traffic::default()));
    let (a, a_handle, a_url) = provider("a", shared.clone()).await;
    let (b, b_handle, b_url) = provider("b", shared.clone()).await;

    let first = (0..33)
        .map(|i| {
            (
                format!("a/first-{i:02}.txt"),
                format!("a-{:02}", i % 26).into_bytes(),
            )
        })
        .chain((0..31).map(|i| {
            (
                format!("b/first-{i:02}.txt"),
                format!("b-{:02}", i % 10).into_bytes(),
            )
        }))
        .chain(std::iter::once((
            "b/blocked.txt".into(),
            BLOCKED_PAYLOAD.to_vec(),
        )))
        .collect::<Vec<_>>();
    let second = (0..15)
        .map(|i| {
            (
                format!("a/second-{i:02}.txt"),
                format!("a-{:02}", (i + 14) % 28).into_bytes(),
            )
        })
        .chain((0..15).map(|i| {
            (
                format!("b/second-{i:02}.txt"),
                format!("b-{:02}", i % 12).into_bytes(),
            )
        }))
        .collect::<Vec<_>>();
    let first_zip = archive(&first);
    let second_zip = archive(&second);
    let mut replies = Vec::new();
    let mut cat_bodies = HashMap::new();
    for (zip, entries) in [(&first_zip, &first), (&second_zip, &second)] {
        let cid: &'static str = raw_cid(zip).leak();
        replies.push(AddReply::Ok(cid));
        cat_bodies.insert(cid.to_owned(), zip.clone());
        replies.extend(
            entries
                .iter()
                .map(|(_, data)| AddReply::Ok(raw_cid(data).leak())),
        );
    }
    replies.push(AddReply::Ok(raw_cid(NORMAL_PAYLOAD).leak()));
    let kubo = start_kubo_harness(KuboScript {
        add_replies: replies,
        cat_bodies,
    })
    .await;
    let raw = format!(
        r#"
        [kubo]
        rpc_url = {:?}
        [storage]
        database_url = "sqlite::memory:"
        [decompress_zip]
        auto_pin_outputs = true
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
        false,
        &config.decompress_zip.pin_output_rules,
    )
    .unwrap();
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "test-bkt", None).await.unwrap();
    let store = store::Store::new(db);
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.server.uri()),
        cold_kubo: None,
        store,
        credentials: HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning,
    });
    let server = start_s3_server(state.clone(), Arc::default()).await;

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
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 95);
    assert_eq!(pin_rows::count(db, "pin_refs").await, 95);
    let resources = pin_rows::resources(db).await;
    assert_eq!(resources.len(), 41, "28 A + 12 B + one oversized CID");
    assert!(resources.iter().all(|r| r.status == "waiting"
        && r.action.as_deref() == Some("submit")
        && r.handle.is_none()));
    {
        let traffic = shared.lock().unwrap();
        assert!(
            traffic.posts.is_empty() && traffic.unexpected.is_empty(),
            "publication does not perform remote HTTP"
        );
    }
    let blocked_cid = raw_cid(BLOCKED_PAYLOAD);
    let failed_cid = raw_cid(FAILED_PAYLOAD);
    let normal_cid = raw_cid(NORMAL_PAYLOAD);

    let worker = state
        .pinning
        .start(state.store.clone(), CancellationToken::new());
    let first_a = a.wait_first_post().await;
    b.wait_first_post().await;
    let normal = tokio::time::timeout(
        Duration::from_secs(10),
        support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &server.endpoint,
            "test-bkt",
            "normal/file",
            &[],
            NORMAL_PAYLOAD.to_vec(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
    .expect("S3 PUT must finish while both provider acknowledgments are blocked");
    assert_eq!(
        normal.status(),
        StatusCode::OK,
        "{}",
        normal.text().await.unwrap()
    );
    let ordinary = store::object::get_latest(db, "test-bkt", "normal/file")
        .await
        .unwrap();
    assert_eq!(ordinary.cid, normal_cid);
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 96);
    let refs = pin_rows::refs(db).await;
    assert_eq!(refs.len(), 96);
    assert!(
        refs.iter().any(|r| r.owner_object_id == ordinary.id
            && r.provider == b_key
            && r.cid == normal_cid)
    );
    b.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = pin_rows::resources(db).await;
            if rows
                .iter()
                .any(|r| r.cid == normal_cid && r.status == "accepted")
            {
                let stalled = rows.iter().find(|r| r.cid == first_a).unwrap();
                assert_eq!(stalled.status, "pending");
                assert!(stalled.handle.is_none());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("ordinary pin reached ACK on B while A still had a blocked ZIP pin");
    a.release.add_permits(1);

    let settled = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let rows = pin_rows::resources(db).await;
            if rows.iter().all(|r| {
                r.status
                    == if r.cid == blocked_cid {
                        "waiting"
                    } else if r.cid == failed_cid {
                        "failed"
                    } else {
                        "accepted"
                    }
            }) {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("ZIP fanout receives each ACK or terminal rejection except the oversized CID");
    worker.shutdown(Duration::from_secs(2)).await;

    assert_eq!(refs.iter().filter(|r| r.provider == a_key).count(), 48);
    assert_eq!(refs.iter().filter(|r| r.provider == b_key).count(), 48);
    assert_eq!(
        refs.iter()
            .map(|r| &r.owner_object_id)
            .collect::<BTreeSet<_>>()
            .len(),
        96
    );
    assert_eq!(settled.len(), 42, "28 A + 12 B + oversized + ordinary");
    for r in &settled {
        if r.cid == blocked_cid {
            assert_eq!(r.provider, b_key);
            assert_eq!(r.logical_size, BLOCKED_PAYLOAD.len() as i64);
            assert_eq!(r.action.as_deref(), Some("submit"));
            assert!(r.handle.is_none());
        } else {
            assert!(r.action.is_none());
            if r.cid == failed_cid {
                assert!(r.handle.is_none());
            } else {
                assert_eq!(r.handle, Some(format!("request-{}", r.cid)));
            }
        }
    }

    {
        let traffic = shared.lock().unwrap();
        assert!(
            traffic.unexpected.is_empty(),
            "ACKs need no verification HTTP: {:?}",
            traffic.unexpected
        );
        assert!(
            (2..=W).contains(&traffic.max_global),
            "in-flight POST peak: {}",
            traffic.max_global
        );
        let expected: BTreeSet<_> = settled
            .iter()
            .filter(|r| r.cid != blocked_cid)
            .map(|r| (if r.provider == a_key { "a" } else { "b" }, r.cid.as_str()))
            .collect();
        let posts: BTreeSet<_> = traffic
            .posts
            .iter()
            .map(|(provider, cid)| (*provider, cid.as_str()))
            .collect();
        assert_eq!(
            traffic.posts.len(),
            expected.len(),
            "one POST per eligible provider/CID, including the rejected CID"
        );
        assert_eq!(posts, expected);
    }
    server.shutdown().await;
    a_handle.abort();
    b_handle.abort();
}
