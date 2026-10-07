//! Registered Cluster REST adapter -> durable worker authentication contract.
//! Only loopback HTTP is mocked; no scripted/replaced provider or PG fixture.
use chrono::Utc;
use ipfs_s3_gateway::{
    config::Config,
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::{PinningCoordinator, ProviderHealth},
        identity::Ownership,
        provider::{ObservedResource, ObservedResourceStatus, ProviderErrorClass},
    },
    s3::handler::S3Impl,
    state::AppState,
    store::{
        self,
        entities::{
            pin_job, pin_provider_usage, pin_replica_observation, pin_submit_observation,
            remote_pin, remote_pin_ledger,
        },
    },
};
use s3s::{
    S3, S3Request,
    dto::{PutObjectInput, StreamingBlob},
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, sea_query::Expr};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const ERROR_BODY: &str = "private-rest-auth-body-token-do-not-persist";
const PEER_A: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

fn config(kubo: &MockServer, rest: &MockServer) -> Config {
    toml::from_str(&format!(
        r#"
[kubo]
rpc_url = "{}"
[storage]
database_url = "sqlite::memory:"
[crypto]
master_key = "{}"
[pinning]
worker_interval = "1s"
worker_concurrency = 1
[[pinning.providers]]
name = "auth-cluster"
kind = "cluster"
strategy = "cid"
priority = 1
max_bytes = 1048576
max_pins = 10
requests_per_second = 100
[pinning.providers.cluster]
protocol = "rest"
rest_endpoint = "{}"
min_replicas = 2
allow_private_network = true
[pinning.providers.cluster.verifier_limits]
max_age_secs = 2
[[pinning.policies]]
bucket = "auth-bucket"
prefix = ""
trigger = "always"
provider_mode = "all"
providers = ["auth-cluster"]
default_duration = "1h"
max_duration = "1h"
[pinning_identity]
primary_storage_domain = "kubo-primary"
[[pinning_identity.providers]]
config_name = "auth-cluster"
provider_id = "stage6-auth-cluster"
display_name = "Auth scope"
backend = "cluster"
scope = "backup-pinset"
storage_domain = "cluster-independent"
credential_revision = 1
endpoint_revision = 1
api_profile = "cluster-rest"
strategy = "cid"
cleanup = "retain"
"#,
        kubo.uri(),
        "37".repeat(32),
        rest.uri()
    ))
    .unwrap()
}

fn read_reply(request: &Request) -> ResponseTemplate {
    let created = "2026-09-20T00:00:00Z";
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &[0x42; 32]).unwrap();
    let peer_b =
        cid::multibase::encode(cid::multibase::Base::Base58Btc, hash.to_bytes())[1..].to_owned();
    let body = match request.url.path() {
        "/version" => serde_json::json!({"version":"1.1.6"}),
        p if p.starts_with("/allocations/") => {
            serde_json::json!({"cid":CID,"allocations":[PEER_A,peer_b],"timestamp":created,"replication_factor_min":2,"replication_factor_max":2})
        }
        _ => {
            assert_eq!(request.url.path(), format!("/pins/{CID}"));
            assert_eq!(request.url.query(), Some("local=false"));
            serde_json::json!({"cid":CID,"allocations":[PEER_A,peer_b],"created":created,"peer_map":{
                PEER_A:{"status":"pinned","timestamp":created,"error":""},
                peer_b:{"status":"pinning","timestamp":created,"error":""}
            }})
        }
    };
    ResponseTemplate::new(200).set_body_json(body)
}

#[tokio::test]
async fn registered_cluster_401_at_each_rest_read_is_authentication_not_protocol_or_pinned() {
    for (read_index, denied) in [
        "/version".to_owned(),
        format!("/allocations/{CID}"),
        format!("/pins/{CID}"),
    ]
    .into_iter()
    .enumerate()
    {
        let kubo = MockServer::start().await;
        let rest = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(move |request: &Request| {
                if request.url.path() == denied {
                    ResponseTemplate::new(401).set_body_string(ERROR_BODY)
                } else {
                    read_reply(request)
                }
            })
            .mount(&rest)
            .await;
        let coordinator = PinningCoordinator::build(
            ValidatedPinningConfig::from_config(&config(&kubo, &rest), |_| None).unwrap(),
        )
        .unwrap();
        let provider = coordinator
            .provider(&coordinator.effective_config().providers[0].name)
            .unwrap();
        let observed = provider.get_observed(CID).await;
        let error = observed.result.unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Authentication, "{error:?}");
        assert!(!error.message.contains(ERROR_BODY) && !error.message.contains(&rest.uri()));
        assert_eq!(observed.resources.len(), 1);
        let resource = &observed.resources[0];
        assert_eq!(resource.cid, CID);
        assert_eq!(resource.status, ObservedResourceStatus::Reported);
        assert_eq!(resource.ownership, Ownership::Unknown);
        assert!(resource.replica_evidence.is_none());
        assert!(provider.unpin(CID).await.is_err());
        assert_eq!(
            rest.received_requests().await.unwrap().len(),
            read_index + 1
        );
        assert!(
            rest.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| r.method == "GET")
        );
    }
}

#[tokio::test]
async fn registered_cluster_401_poll_is_terminal_and_due_polls_keep_unknown_history_and_charge_without_http()
 {
    let kubo = MockServer::start().await;
    let rest = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            format!("{{\"Name\":\"file\",\"Hash\":\"{CID}\",\"Size\":\"4\"}}\n"),
            "application/json",
        ))
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[CID]})))
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/pins/{CID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"cid":CID})))
        .expect(1)
        .mount(&rest)
        .await;
    let reject = Arc::new(AtomicBool::new(false));
    Mock::given(method("GET"))
        .respond_with({
            let reject = reject.clone();
            move |request: &Request| {
                if reject.load(Ordering::SeqCst) {
                    ResponseTemplate::new(401).set_body_string(ERROR_BODY)
                } else {
                    read_reply(request)
                }
            }
        })
        .mount(&rest)
        .await;
    let state = AppState::new(&config(&kubo, &rest)).await.unwrap();
    store::bucket::create(state.store.db(), "auth-bucket", Some("test-owner"))
        .await
        .unwrap();
    // Use the production S3 publication handler to allocate the lease/job/charge.
    // This target does not claim SigV4 HTTP validation.
    S3Impl::new(state.clone())
        .put_object(S3Request {
            input: PutObjectInput {
                bucket: "auth-bucket".into(),
                key: "private-object".into(),
                body: Some(StreamingBlob::from(s3s::Body::from(
                    bytes::Bytes::from_static(b"body"),
                ))),
                ..Default::default()
            },
            method: http::Method::PUT,
            uri: "/auth-bucket/private-object".parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: Some(s3s::auth::Credentials {
                access_key: "test-owner".into(),
                secret_key: "test".into(),
            }),
            region: None,
            service: None,
            trailing_headers: None,
        })
        .await
        .unwrap();
    let provider = state.pinning.effective_config().providers[0].name.clone();
    let worker = state
        .pinning
        .start(state.store.clone(), CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if pin_submit_observation::Entity::find()
                .filter(pin_submit_observation::Column::Outcome.eq("accepted"))
                .one(state.store.db())
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("registered Cluster submit must be accepted but only 1/2 replicas");
    reject.store(true, Ordering::SeqCst);
    let first = wait_unknown_poll(&state, 0).await;
    worker.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        *state
            .pinning
            .provider_runtime(&provider)
            .unwrap()
            .health
            .read()
            .await,
        ProviderHealth::Terminal,
        "formal adapter 401 must reach worker health as Authentication"
    );
    let resources: Vec<ObservedResource> = serde_json::from_str(&first.resources).unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].ownership, Ownership::Unknown);
    assert_eq!(resources[0].status, ObservedResourceStatus::Reported);
    assert!(resources[0].replica_evidence.is_none());
    assert_eq!(first.outcome, "Unknown");
    assert!(
        first
            .safe_error
            .as_deref()
            .unwrap()
            .contains("Authentication")
    );
    assert!(!format!("{first:?}").contains(ERROR_BODY));
    let usage = pin_provider_usage::Entity::find_by_id(provider.clone())
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (4, 1));
    let debt = remote_pin_ledger::Entity::find_by_id((provider.clone(), CID.to_owned()))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(debt.ownership, "unknown");
    let remote = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(remote.status, "pinned");
    let http_count = rest.received_requests().await.unwrap().len();
    let kubo_count = kubo.received_requests().await.unwrap().len();
    let mut after = first.sequence;
    for _ in 0..2 {
        // An actual new durable Poll proves the worker had a scheduling chance;
        // setting attempts above the ordinary budget must not bypass Terminal.
        let changed = pin_job::Entity::update_many()
            .col_expr(pin_job::Column::NextAttemptAt, Expr::value(Utc::now()))
            .col_expr(
                pin_job::Column::Attempts,
                Expr::value(state.pinning.settings().max_attempts as i32 + 1),
            )
            .filter(pin_job::Column::Id.eq(&first.job_id))
            .filter(pin_job::Column::State.eq("pending"))
            .exec(state.store.db())
            .await
            .unwrap();
        assert_eq!(changed.rows_affected, 1);
        let worker = state
            .pinning
            .start(state.store.clone(), CancellationToken::new());
        let later = wait_unknown_poll(&state, after).await;
        worker.shutdown(Duration::from_secs(2)).await;
        assert!(later.sequence > after);
        after = later.sequence;
        assert_eq!(
            *state
                .pinning
                .provider_runtime(&provider)
                .unwrap()
                .health
                .read()
                .await,
            ProviderHealth::Terminal
        );
        assert_eq!(rest.received_requests().await.unwrap().len(), http_count);
        assert_eq!(kubo.received_requests().await.unwrap().len(), kubo_count);
        let retained_remote = remote_pin::Entity::find_by_id((provider.clone(), CID.to_owned()))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained_remote.epoch, remote.epoch);
        assert_ne!(retained_remote.status, "pinned");
        let current = pin_provider_usage::Entity::find_by_id(provider.clone())
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((current.reserved_bytes, current.reserved_pins), (4, 1));
        assert_eq!(
            remote_pin_ledger::Entity::find_by_id((provider.clone(), CID.to_owned()))
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .ownership,
            "unknown"
        );
        assert_eq!(
            pin_replica_observation::Entity::find_by_id(first.sequence)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap(),
            first
        );
    }
    assert!(
        rest.received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.method != "DELETE" && !r.url.path().contains("pin/rm"))
    );
}

#[tokio::test]
async fn non_authentication_unknown_is_not_promoted_to_authentication_absence_or_cleanup() {
    for status in [403, 404, 429, 503] {
        let kubo = MockServer::start().await;
        let rest = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/version"))
            .respond_with(ResponseTemplate::new(status).set_body_string(ERROR_BODY))
            .expect(1)
            .mount(&rest)
            .await;
        let coordinator = PinningCoordinator::build(
            ValidatedPinningConfig::from_config(&config(&kubo, &rest), |_| None).unwrap(),
        )
        .unwrap();
        let provider = coordinator
            .provider(&coordinator.effective_config().providers[0].name)
            .unwrap();
        let observation = provider.get_observed(CID).await;
        let error = observation.result.unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
        assert!(!error.message.contains(ERROR_BODY));
        assert_eq!(observation.resources[0].ownership, Ownership::Unknown);
        assert_eq!(
            observation.resources[0].status,
            ObservedResourceStatus::Reported
        );
        assert!(observation.resources[0].replica_evidence.is_none());
        assert!(provider.unpin(CID).await.is_err());
        assert_eq!(rest.received_requests().await.unwrap().len(), 1);
    }
}

async fn wait_unknown_poll(state: &AppState, after: i64) -> pin_replica_observation::Model {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(row) = pin_replica_observation::Entity::find()
                .filter(pin_replica_observation::Column::Phase.eq("poll"))
                .filter(pin_replica_observation::Column::Outcome.eq("Unknown"))
                .filter(pin_replica_observation::Column::Sequence.gt(after))
                .order_by_asc(pin_replica_observation::Column::Sequence)
                .one(state.store.db())
                .await
                .unwrap()
            {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("must record an actual new Unknown Cluster Poll")
}
