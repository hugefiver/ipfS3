use super::*;
use crate::pinning::ipfs_rpc::{IpfsRpcProvider, RpcProfile, RpcStrategy};
use crate::{
    config::PinningConfig,
    pinning::{
        config::{LeaseDuration, ProviderMode, ValidatedPinningConfig},
        policy::{LeaseIntent, LeaseSource},
        tags::ContentMode,
    },
    store::pinning::publication::PinTargetSpec,
};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement, TransactionTrait};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_partial_json, method, path},
};

async fn wait_status(store: &Store, cid: &str, expected: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = store
                .db()
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "SELECT status FROM pin_resources WHERE cid=?",
                    [cid.into()],
                ))
                .await
                .unwrap();
            if row
                .map(|row| row.try_get::<String>("", "status").unwrap())
                .as_deref()
                == Some(expected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn canonical_resource_uploads_with_original_cidv0_unixfs_profile() {
    use wiremock::matchers::query_param;
    const V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    let canonical = crate::pinning::provider::canonical_cid(V0).unwrap();
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Hash": V0, "Type": "file"})))
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"stored file"))
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("cid-version", "0"))
        .and(query_param("raw-leaves", "false"))
        .and(query_param("pin", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{V0}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let provider = IpfsRpcProvider::new(
        "rpc".into(),
        target.uri(),
        crate::kubo::KuboClient::new(source.uri()),
        RpcProfile::Kubo,
        RpcStrategy::Upload,
        None,
    )
    .unwrap();
    let store = Store::open(Database::connect("sqlite::memory:").await.unwrap())
        .await
        .unwrap();
    let limits = ProviderLimits {
        priority: 1,
        max_pins: 1,
        max_bytes: 100,
        enabled: true,
    };
    let tx = store.db().begin().await.unwrap();
    resources::attach(
        &tx,
        "file",
        &PinTargetSpec {
            cid: V0.into(),
            logical_size: 11,
        },
        &LeaseIntent {
            source: LeaseSource::Manual,
            provider_mode: ProviderMode::One,
            providers: vec!["rpc".into()],
            policy_id: "one".into(),
            content_mode: ContentMode::Object,
            duration: LeaseDuration::parse("1h").unwrap(),
        },
        &BTreeMap::from([("rpc".into(), limits.clone())]),
        Utc::now(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_provider(
        store.clone(),
        Arc::new(provider),
        limits,
        Arc::new(Semaphore::new(1)),
        Duration::from_secs(1),
        Duration::ZERO,
        watch::channel(()).1,
        stop.clone(),
    ));
    wait_status(&store, &canonical, "accepted").await;
    stop.cancel();
    worker.await.unwrap();
    let requests = target.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .body
            .windows(11)
            .any(|part| part == b"stored file")
    );
}

#[tokio::test]
async fn queued_ack_and_exact_delete_complete_while_another_post_is_stalled() {
    let server = MockServer::start().await;
    let raw: PinningConfig = toml::from_str(&format!(
        r#"
        worker_interval = "1s"
        worker_concurrency = 2
        [[providers]]
        name = "backup"
        kind = "filebase"
        endpoint = "{}"
        token_env = "BACKUP_TOKEN"
        requests_per_second = 100
        priority = 1
        max_bytes = 1000
        max_pins = 8
    "#,
        server.uri()
    ))
    .unwrap();
    let config = ValidatedPinningConfig::from_raw(&raw, |_| Some("test-token".into())).unwrap();
    let coordinator = PinningCoordinator::build(config).unwrap();
    let store = Store::open(Database::connect("sqlite::memory:").await.unwrap())
        .await
        .unwrap();
    let cid = |seed: u8| {
        cid::Cid::new_v1(
            0x55,
            cid::multihash::Multihash::wrap(0x12, &[seed; 32]).unwrap(),
        )
        .to_string()
    };
    let slow = cid(1);
    let good = cid(2);
    let unknown = cid(3);
    for (key, response) in [
        (
            &slow,
            ResponseTemplate::new(202).set_delay(Duration::from_secs(60)),
        ),
        (
            &good,
            ResponseTemplate::new(202).set_body_json(
                json!({"requestid":"request-42", "status":"queued", "pin":{"cid": good}}),
            ),
        ),
        (&unknown, ResponseTemplate::new(503)),
    ] {
        Mock::given(method("POST"))
            .and(path("/pins"))
            .and(body_partial_json(json!({"cid":key})))
            .respond_with(response)
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("DELETE"))
        .and(path("/pins/request-42"))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let intent = LeaseIntent {
        source: LeaseSource::Manual,
        provider_mode: ProviderMode::One,
        providers: vec!["backup".into()],
        policy_id: "policy".into(),
        content_mode: ContentMode::Object,
        duration: LeaseDuration::parse("1h").unwrap(),
    };
    let attach = async |key: &str| {
        let tx = store.db().begin().await.unwrap();
        resources::attach(
            &tx,
            key,
            &PinTargetSpec {
                cid: key.into(),
                logical_size: 1,
            },
            &intent,
            coordinator.provider_limits(),
            Utc::now(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    };
    attach(&slow).await;
    let worker = coordinator.start(store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), async {
        while server.received_requests().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    attach(&good).await;
    attach(&unknown).await;
    wait_status(&store, &good, "accepted").await;
    wait_status(&store, &unknown, "unknown").await;
    let tx = store.db().begin().await.unwrap();
    resources::release_owner(&tx, &good, Utc::now())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    wait_status(&store, &good, "cleaned").await;
    tokio::time::timeout(
        Duration::from_secs(2),
        worker.shutdown(Duration::from_millis(30)),
    )
    .await
    .unwrap();
    server.verify().await;
}
