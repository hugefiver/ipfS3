use ipfs_s3_gateway::{
    config::Config,
    kubo::KuboClient,
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::{PinningCoordinator, build_cluster_provider, normalize_validated_config},
        identity::{ConfirmationRequirement, Ownership, ProviderRouteSnapshot, RemoteResourceType},
        provider::{ObservedResourceStatus, RemotePinStatus, SubmitEffect, SubmitPin},
    },
    state::AppState,
    store::{self, Store, entities::pin_provider_route},
};
use sea_orm::EntityTrait;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const OTHER: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";

fn sample(protocol: &str, strategy: &str) -> String {
    format!(
        r#"
        [kubo]
        rpc_url = "http://127.0.0.1:5001"
        [[pinning.providers]]
        name = "backup"
        kind = "cluster"
        strategy = "{strategy}"
        priority = 1
        max_bytes = 10000
        max_pins = 10
        [pinning.providers.cluster]
        protocol = "{protocol}"
        rest_endpoint = "http://127.0.0.1:9094"
        {}
        min_replicas = 2
        allow_private_network = true
        [pinning_identity]
        primary_storage_domain = "kubo:primary"
        [[pinning_identity.providers]]
        config_name = "backup"
        provider_id = "cluster-backup"
        display_name = "Independent cluster"
        backend = "cluster"
        scope = "backup-pinset"
        storage_domain = "cluster:backup"
        credential_revision = 1
        endpoint_revision = 1
        api_profile = "cluster-{protocol}"
        strategy = "{strategy}"
        "#,
        if protocol == "proxy" {
            "proxy_endpoint = 'http://127.0.0.1:9095'"
        } else {
            ""
        }
    )
}

fn validate(text: &str) -> anyhow::Result<ValidatedPinningConfig> {
    let config: Config = toml::from_str(text)?;
    ValidatedPinningConfig::from_config(&config, |_| None)
}

#[test]
fn legacy_provider_config_without_cluster_preserves_defaults_and_route() {
    let config: Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name = "old-noop"
        kind = "noop"
        priority = 1
        max_bytes = 10000
        max_pins = 10
        "#,
    )
    .unwrap();
    assert!(config.pinning.providers[0].cluster.is_none());
    assert!(config.pinning.providers[0].enabled);
    assert_eq!(config.pinning.worker_interval, "5s");
    assert_eq!(config.pinning.worker_concurrency, 4);
    let validated = ValidatedPinningConfig::from_config(&config, |_| None).unwrap();
    let provider = &validated.providers[0];
    assert!(provider.cluster.is_none());
    assert!(provider.route_snapshot().confirmation.is_none());
    assert_eq!(
        provider.route_snapshot(),
        provider.identity.route_snapshot()
    );
    provider
        .validate_route_snapshot(&provider.identity.route_snapshot())
        .unwrap();
}

#[test]
fn legacy_rpc_observation_json_remains_recursive_not_replica_confirmation() {
    let old_json = format!(
        r#"{{"resource_type":"rpc_pin","cid":"{CID}","request_id":"{CID}","status":"recursive_verified","ownership":"unknown"}}"#
    );
    let resource: ipfs_s3_gateway::pinning::provider::ObservedResource =
        serde_json::from_str(&old_json).unwrap();
    assert_eq!(resource.status, ObservedResourceStatus::RecursiveVerified);
    assert_eq!(resource.ownership, Ownership::Unknown);
    assert!(resource.replica_evidence.is_none());
    assert_eq!(serde_json::to_string(&resource).unwrap(), old_json);
}

#[test]
fn explicit_cluster_routes_validate_without_io() {
    for protocol in ["proxy", "rest"] {
        for strategy in ["cid", "upload", "car"] {
            let validated = validate(&sample(protocol, strategy)).unwrap();
            assert_eq!(
                validated.providers[0].identity.api_profile,
                format!("cluster-{protocol}")
            );
            assert!(validated.providers[0].rpc.is_none());
            normalize_validated_config(validated).unwrap();
        }
    }
}

#[test]
fn cluster_requires_rest_explicit_identity_and_independent_primary() {
    let text = sample("proxy", "cid");
    for bad in [
        text.replace("rest_endpoint = \"http://127.0.0.1:9094\"", ""),
        text.split("[pinning_identity]").next().unwrap().to_owned(),
        text.replace("http://127.0.0.1:9095", "http://127.0.0.1:5001/api/v0/"),
        text.replace("min_replicas = 2", "min_replicas = 0"),
        text.replace(
            "allow_private_network = true",
            "allow_private_network = false",
        ),
        text.replace("api_profile = \"cluster-proxy\"", "api_profile = \"kubo\""),
    ] {
        assert!(validate(&bad).is_err());
    }
}

#[test]
fn snapshots_freeze_confirmation_and_old_none_json_bytes_remain_unchanged() {
    let validated = validate(&sample("rest", "cid")).unwrap();
    let p = &validated.providers[0];
    assert_eq!(
        p.route_snapshot().confirmation,
        Some(ConfirmationRequirement::ClusterReplicas {
            min_replicas: 2,
            max_age_secs: 30,
        })
    );
    assert_eq!(
        p.route_snapshot().resource_type(),
        RemoteResourceType::ClusterPin
    );
    let json = serde_json::to_string(&p.route_snapshot()).unwrap();
    let decoded: ProviderRouteSnapshot = serde_json::from_str(&json).unwrap();
    p.validate_route_snapshot(&decoded).unwrap();
    let mut old = decoded.clone();
    old.confirmation = None;
    assert!(p.validate_route_snapshot(&old).is_err());
    old.confirmation = Some(ConfirmationRequirement::ClusterReplicas {
        min_replicas: 1,
        max_age_secs: 30,
    });
    assert!(p.validate_route_snapshot(&old).is_err());
    let old_json = r#"{"provider_id":"legacy:old","backend":"kubo","scope":"legacy:old","storage_domain":"legacy:old","credential_revision":1,"endpoint_revision":1,"secret_ref":null,"api_profile":"kubo","strategy":"cid","cleanup":"retain"}"#;
    let old: ProviderRouteSnapshot = serde_json::from_str(old_json).unwrap();
    assert!(old.confirmation.is_none());
    assert_eq!(serde_json::to_string(&old).unwrap(), old_json);
}

#[test]
fn source_is_main_kubo_only_and_factory_checks_actual_endpoint_without_io() {
    let text = sample("proxy", "upload");
    let validated = validate(&text).unwrap();
    let p = &validated.providers[0];
    assert_eq!(
        p.cluster
            .as_ref()
            .unwrap()
            .source
            .as_ref()
            .unwrap()
            .endpoint,
        "http://127.0.0.1:5001"
    );
    assert!(build_cluster_provider(p, None).is_err());
    assert!(
        build_cluster_provider(p, Some(KuboClient::new("http://127.0.0.1:5010".into()))).is_err()
    );
    assert!(
        build_cluster_provider(
            p,
            Some(KuboClient::new("http://127.0.0.1:5001/api/v0/".into()))
        )
        .is_ok()
    );
    assert!(
        PinningCoordinator::build_with_kubo(
            validated.clone(),
            Some(KuboClient::new("http://127.0.0.1:5010".into()))
        )
        .is_err()
    );
    assert!(
        PinningCoordinator::build_with_kubo(
            validated,
            Some(KuboClient::new("http://127.0.0.1:5001/api/v0/".into()))
        )
        .is_ok()
    );
    assert!(
        validate(&text.replace(
            "min_replicas = 2",
            "source = { endpoint = 'http://other.invalid', auth = 'none' }\nmin_replicas = 2"
        ))
        .is_err()
    );
    for option in [
        "max_age_secs = 0",
        "max_age_secs = 301",
        "total_timeout_seconds = 0",
        "max_body_bytes = 0",
    ] {
        let bad = text.replace(
            "min_replicas = 2",
            &format!("verifier_limits = {{ {option} }}\nmin_replicas = 2"),
        );
        assert!(validate(&bad).is_err(), "{option}");
    }
}

#[test]
fn same_pinset_aliases_share_one_allocation_and_reject_confirmation_or_domain_conflicts() {
    let base = sample("rest", "cid");
    let alias = base
        .replace("http://127.0.0.1:9094", "http://127.0.0.1:9194")
        .replace("[kubo]\n        rpc_url = \"http://127.0.0.1:5001\"", "")
        .replace(
            "[pinning_identity]\n        primary_storage_domain = \"kubo:primary\"",
            "",
        )
        .replace("\"backup\"", "\"alias\"")
        .replace(
            "provider_id = \"cluster-backup\"",
            "provider_id = \"cluster-alias\"",
        );
    let joined = format!("{base}\n{alias}");
    let normalized = normalize_validated_config(validate(&joined).unwrap()).unwrap();
    assert_eq!(normalized.provider_limits.len(), 1);
    assert_eq!(normalized.providers[0].name, normalized.providers[1].name);
    for conflict in [
        alias.replace("min_replicas = 2", "min_replicas = 1"),
        alias.replace(
            "storage_domain = \"cluster:backup\"",
            "storage_domain = \"cluster:other\"",
        ),
        alias.replace("endpoint_revision = 1", "endpoint_revision = 2"),
        alias.replace("strategy = \"cid\"", "strategy = \"car\""),
    ] {
        assert!(
            normalize_validated_config(validate(&format!("{base}\n{conflict}")).unwrap()).is_err()
        );
    }
}

#[test]
fn birth_route_rejects_protocol_strategy_credentials_endpoint_and_confirmation_drift() {
    let text = sample("rest", "cid");
    let p = validate(&text).unwrap();
    let birth = p.providers[0].route_snapshot();
    for drift in [
        text.replace(
            "protocol = \"rest\"",
            "protocol = \"proxy\"\nproxy_endpoint = 'http://127.0.0.1:9095'",
        )
        .replace("cluster-rest", "cluster-proxy"),
        text.replace("strategy = \"cid\"", "strategy = \"car\""),
        text.replace("credential_revision = 1", "credential_revision = 2"),
        text.replace("endpoint_revision = 1", "endpoint_revision = 2"),
        text.replace("min_replicas = 2", "min_replicas = 1"),
        text.replace(
            "min_replicas = 2",
            "min_replicas = 2\nverifier_limits = { max_age_secs = 10 }",
        ),
    ] {
        let current = validate(&drift).unwrap();
        assert!(
            current.providers[0]
                .validate_route_snapshot(&birth)
                .is_err()
        );
    }
}

#[tokio::test]
async fn formal_coordinator_registration_preserves_birth_contract_and_unknown_cannot_delete() {
    for protocol in ["proxy", "rest"] {
        let rest = MockServer::start().await;
        let proxy = MockServer::start().await;
        let text = sample(protocol, "cid")
            .replace("http://127.0.0.1:9094", &rest.uri())
            .replace("http://127.0.0.1:9095", &proxy.uri());
        let validated = validate(&text).unwrap();
        let birth = validated.providers[0].route_snapshot();
        let config: Config = toml::from_str(&text).unwrap();
        let state = AppState::new(&config).await.unwrap();
        let coordinator = &state.pinning;
        let effective = &coordinator.effective_config().providers[0];
        assert_eq!(effective.route_snapshot(), birth);
        let routes = pin_provider_route::Entity::find()
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].provider, effective.name);
        assert_eq!(
            serde_json::from_str::<ProviderRouteSnapshot>(&routes[0].snapshot).unwrap(),
            birth
        );
        assert_eq!(
            birth.confirmation,
            Some(ConfirmationRequirement::ClusterReplicas {
                min_replicas: 2,
                max_age_secs: 30,
            })
        );
        let adapter = coordinator.provider(&effective.name).unwrap();
        assert_eq!(
            adapter.invocation_route(),
            (
                if protocol == "proxy" {
                    "cluster-proxy"
                } else {
                    "cluster-rest"
                },
                "cid"
            )
        );
        assert!(rest.received_requests().await.unwrap().is_empty());
        assert!(proxy.received_requests().await.unwrap().is_empty());
        replicas(&rest, ["pinned", "pinned"], None).await;
        let observed = adapter
            .get_observed_historical(CID, adapter.invocation_route().0, "cid")
            .await;
        assert_eq!(observed.result.unwrap().status, RemotePinStatus::Pinned);
        assert_eq!(observed.resources[0].ownership, Ownership::Unknown);
        for (api, strategy) in [("kubo", "cid"), (adapter.invocation_route().0, "car")] {
            assert!(
                adapter
                    .get_observed_historical(CID, api, strategy)
                    .await
                    .result
                    .is_err()
            );
        }
        assert!(adapter.unpin(CID).await.is_err());
        assert!(
            adapter
                .unpin_historical(CID, adapter.invocation_route().0, "cid")
                .await
                .is_err()
        );
        assert_eq!(rest.received_requests().await.unwrap().len(), 4);
        assert!(proxy.received_requests().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn automatic_alias_registration_stores_only_the_canonical_invoked_route() {
    for protocol in ["proxy", "rest"] {
        let first_rest = MockServer::start().await;
        let first_proxy = MockServer::start().await;
        let alias_rest = MockServer::start().await;
        let alias_proxy = MockServer::start().await;
        let text = sample(protocol, "cid")
            .replace("http://127.0.0.1:9094", &first_rest.uri())
            .replace("http://127.0.0.1:9095", &first_proxy.uri());
        let mut config: Config = toml::from_str(&text).unwrap();
        let mut alias = config.pinning.providers[0].clone();
        alias.name = "alias".into();
        let options = alias.cluster.as_mut().unwrap();
        options.rest_endpoint = alias_rest.uri();
        if protocol == "proxy" {
            options.proxy_endpoint = Some(alias_proxy.uri());
        }
        config.pinning.providers.push(alias);
        let mut identity = config.pinning_identity.providers[0].clone();
        identity.config_name = "alias".into();
        identity.provider_id = "cluster-alias".into();
        identity.display_name = "Canonical alias".into();
        config.pinning_identity.providers.push(identity);

        // Raw order is deliberately opposite to canonical provider_id order.
        let state = AppState::new(&config).await.unwrap();
        let effective = state.pinning.effective_config();
        let selected = &effective.providers[0];
        let second = &effective.providers[1];
        assert_eq!(selected.identity.provider_id, "cluster-alias");
        assert_eq!(selected.name, second.name);
        assert_eq!(effective.provider_limits.len(), 1);
        let assert_registration = async || {
            let routes = pin_provider_route::Entity::find()
                .all(state.store.db())
                .await
                .unwrap();
            assert_eq!(routes.len(), 1, "one row per physical allocation");
            assert_eq!(routes[0].provider, selected.name);
            assert_eq!(routes[0].display_name, selected.identity.display_name);
            let stored: ProviderRouteSnapshot = serde_json::from_str(&routes[0].snapshot).unwrap();
            assert_eq!(stored, selected.route_snapshot());
            assert_ne!(stored, second.route_snapshot());
            assert!(stored.confirmation.is_some());
        };
        assert_registration().await;
        state
            .pinning
            .register_identities(&state.store)
            .await
            .unwrap();
        assert_registration().await;

        replicas(&alias_rest, ["pinned", "pinned"], None).await;
        let (target, route, body) = if protocol == "proxy" {
            (
                &alias_proxy,
                "/api/v0/pin/add".to_owned(),
                serde_json::json!({"Pins":[CID]}),
            )
        } else {
            (
                &alias_rest,
                format!("/pins/{CID}"),
                serde_json::json!({"cid":CID}),
            )
        };
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(target)
            .await;
        let submitted = state
            .pinning
            .provider(&selected.name)
            .unwrap()
            .submit_observed(request())
            .await;
        assert_eq!(submitted.result.unwrap().status, RemotePinStatus::Pinned);
        assert_eq!(
            submitted.resources[0].status,
            ObservedResourceStatus::ReplicasVerified
        );
        assert!(first_rest.received_requests().await.unwrap().is_empty());
        assert!(first_proxy.received_requests().await.unwrap().is_empty());
        let writes = target.received_requests().await.unwrap();
        let writes: Vec<_> = writes
            .iter()
            .filter(|request| request.method == "POST")
            .collect();
        assert_eq!(writes.len(), 1);
        assert!(
            !writes[0]
                .url
                .query()
                .unwrap_or_default()
                .contains("replication")
        );
        if protocol == "rest" {
            assert!(alias_proxy.received_requests().await.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn retired_and_disabled_cluster_routes_remain_readable_but_not_new_allocation_candidates() {
    for text in [
        sample("rest", "cid").replace("priority = 1", "enabled = false\npriority = 1"),
        sample("rest", "cid").replace(
            "credential_revision = 1",
            "retired = true\ncredential_revision = 1",
        ),
    ] {
        let config: Config = toml::from_str(&text).unwrap();
        let state = AppState::new(&config).await.unwrap();
        let coordinator = &state.pinning;
        let effective = &coordinator.effective_config().providers[0];
        assert!(!effective.limits.enabled);
        assert!(!coordinator.provider_limits()[&effective.name].enabled);
        assert!(coordinator.provider(&effective.name).is_some());
        assert!(effective.route_snapshot().confirmation.is_some());
        let routes = pin_provider_route::Entity::find()
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].retired, effective.identity.retired);
        assert_eq!(
            serde_json::from_str::<ProviderRouteSnapshot>(&routes[0].snapshot).unwrap(),
            effective.route_snapshot()
        );
    }
}

#[tokio::test]
async fn ordinary_rpc_and_psa_coordinator_registration_keeps_confirmation_none() {
    let config: Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name = "rpc"
        kind = "ipfs_rpc"
        endpoint = "http://127.0.0.1:5002"
        strategy = "cid"
        priority = 1
        max_bytes = 10000
        max_pins = 10
        [[pinning_rpc.providers]]
        config_name = "rpc"
        profile = "kubo"
        allow_private_network = true
        [[pinning.providers]]
        name = "psa"
        kind = "filebase"
        token_env = "TEST_PSA_TOKEN"
        priority = 1
        max_bytes = 10000
        max_pins = 10
        [pinning_identity]
        primary_storage_domain = "kubo:primary"
        [[pinning_identity.providers]]
        config_name = "rpc"
        provider_id = "rpc-backup"
        display_name = "RPC"
        backend = "kubo"
        scope = "rpc-pinset"
        storage_domain = "kubo:backup"
        credential_revision = 1
        endpoint_revision = 1
        api_profile = "kubo"
        strategy = "cid"
        [[pinning_identity.providers]]
        config_name = "psa"
        provider_id = "psa-backup"
        display_name = "PSA"
        backend = "filebase"
        scope = "psa-account"
        storage_domain = "filebase:backup"
        credential_revision = 1
        endpoint_revision = 1
        secret_ref = "env:TEST_PSA_TOKEN"
        api_profile = "filebase-psa"
        strategy = "cid"
        "#,
    )
    .unwrap();
    let validated = ValidatedPinningConfig::from_config(&config, |name| {
        (name == "TEST_PSA_TOKEN").then(|| "test-token".into())
    })
    .unwrap();
    let coordinator = PinningCoordinator::build(validated).unwrap();
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let store = Store::new(db);
    coordinator.register_identities(&store).await.unwrap();
    let routes = pin_provider_route::Entity::find()
        .all(store.db())
        .await
        .unwrap();
    assert_eq!(routes.len(), 2);
    for provider in &coordinator.effective_config().providers {
        let row = routes
            .iter()
            .find(|row| row.provider == provider.name)
            .unwrap();
        let stored: ProviderRouteSnapshot = serde_json::from_str(&row.snapshot).unwrap();
        assert_eq!(stored, provider.route_snapshot());
        assert!(stored.confirmation.is_none());
        assert_eq!(
            stored.resource_type(),
            if provider.rpc.is_some() {
                RemoteResourceType::RpcPin
            } else {
                RemoteResourceType::PsaRequest
            }
        );
        assert!(!row.snapshot.contains("confirmation"));
    }
}

#[test]
fn missing_independent_auth_cannot_reuse_another_route_credential() {
    let text = sample("proxy", "upload")
        .replace("min_replicas = 2", "proxy_auth = { auth = 'bearer', token_env = 'PROXY_TOKEN' }\nrest_auth = { auth = 'basic', username_env = 'REST_USER', token_env = 'REST_PASSWORD' }\nsource = { auth = 'bearer', token_env = 'SOURCE_TOKEN' }\nmin_replicas = 2")
        .replace("endpoint_revision = 1", "endpoint_revision = 1\nsecret_ref = 'env:PROXY_TOKEN'");
    let config: Config = toml::from_str(&text).unwrap();
    for missing in ["PROXY_TOKEN", "REST_USER", "REST_PASSWORD", "SOURCE_TOKEN"] {
        let result = ValidatedPinningConfig::from_config(&config, |name| {
            (name != missing).then(|| "present-value".into())
        });
        assert!(
            result.is_err(),
            "missing {missing} unexpectedly reused credentials"
        );
        assert!(!result.unwrap_err().to_string().contains("present-value"));
    }
    assert!(ValidatedPinningConfig::from_config(&config, |_| Some("present-value".into())).is_ok());
}

fn peer(byte: u8) -> String {
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &[byte; 32]).unwrap();
    cid::multibase::encode(cid::multibase::Base::Base58Btc, hash.to_bytes())[1..].into()
}

async fn replicas(rest: &MockServer, states: [&str; 2], auth: Option<&str>) {
    let created = (chrono::Utc::now() - chrono::Duration::seconds(60)).to_rfc3339();
    let timestamp = (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
    let a = peer(1);
    let b = peer(2);
    for (route, body) in [
        ("/version".into(), serde_json::json!({"version":"1.1.6"})),
        (
            format!("/allocations/{CID}"),
            serde_json::json!({"cid":CID,"allocations":[a,b],"timestamp":created,"replication_factor_min":2,"replication_factor_max":2}),
        ),
        (
            format!("/pins/{CID}"),
            serde_json::json!({"cid":CID,"allocations":[a,b],"created":created,
            "peer_map":{a:{"status":states[0],"timestamp":timestamp,"error":""}, b:{"status":states[1],"timestamp":timestamp,"error":""}}}),
        ),
    ] {
        let mut mock = Mock::given(method("GET")).and(path(route));
        if let Some(auth) = auth {
            mock = mock.and(header("authorization", auth));
        }
        mock.respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(rest)
            .await;
    }
}

fn request() -> SubmitPin {
    SubmitPin {
        cid: CID.into(),
        name: "do-not-export-object".into(),
        metadata: Default::default(),
    }
}

#[tokio::test]
async fn trait_submit_requires_fresh_n_of_n_and_read_only_get_can_downgrade() {
    for protocol in ["proxy", "rest"] {
        let proxy = MockServer::start().await;
        let rest = MockServer::start().await;
        let text = sample(protocol, "cid")
            .replace("http://127.0.0.1:9094", &rest.uri())
            .replace("http://127.0.0.1:9095", &proxy.uri());
        let p = validate(&text).unwrap();
        let adapter = build_cluster_provider(&p.providers[0], None).unwrap();
        let (target, route, body) = if protocol == "proxy" {
            (
                &proxy,
                "/api/v0/pin/add".to_owned(),
                serde_json::json!({"Pins":[CID]}),
            )
        } else {
            (
                &rest,
                format!("/pins/{CID}"),
                serde_json::json!({"cid":CID}),
            )
        };
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(target)
            .await;
        replicas(&rest, ["pinned", "pinning"], None).await;
        let submitted = adapter.submit_observed(request()).await;
        assert_eq!(submitted.effect, SubmitEffect::Observed);
        assert_eq!(submitted.result.unwrap().status, RemotePinStatus::Pinning);
        assert_eq!(
            submitted.resources[0].status,
            ObservedResourceStatus::PinAccepted
        );
        assert_eq!(
            submitted.resources[0]
                .replica_evidence
                .as_ref()
                .unwrap()
                .pinned_peers
                .len(),
            1
        );
        assert!(
            submitted
                .resources
                .iter()
                .all(|r| r.ownership == Ownership::Unknown)
        );
        // Subsequent reads only, with a new proof every time.
        rest.reset().await;
        replicas(&rest, ["pinned", "pinned"], None).await;
        let read = adapter.get_observed(CID).await;
        assert_eq!(read.result.unwrap().status, RemotePinStatus::Pinned);
        assert_eq!(
            read.resources[0].status,
            ObservedResourceStatus::ReplicasVerified
        );
        let json = serde_json::to_string(&read.resources[0]).unwrap();
        let historical: ipfs_s3_gateway::pinning::provider::ObservedResource =
            serde_json::from_str(&json).unwrap();
        assert_eq!(historical, read.resources[0]); // Historical facts, not ReplicaObservation.
        rest.reset().await;
        replicas(&rest, ["pinned", "remote"], None).await;
        let degraded = adapter.get_observed(CID).await;
        assert_eq!(degraded.result.unwrap().status, RemotePinStatus::Pinning);
        assert_eq!(
            degraded.resources[0].status,
            ObservedResourceStatus::PinAccepted
        );
        assert!(
            rest.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| r.method == "GET")
        );
        let incompatible = adapter.get_observed_historical(CID, "kubo", "cid").await;
        assert!(incompatible.result.is_err());
        assert!(incompatible.resources.is_empty());
        assert_eq!(rest.received_requests().await.unwrap().len(), 4);
    }
}

#[tokio::test]
async fn typed_adapter_keeps_unexpected_roots_and_unknown_write_effect() {
    for (code, expected_effect) in [(200, SubmitEffect::Unknown), (403, SubmitEffect::Unknown)] {
        let proxy = MockServer::start().await;
        let rest = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(
                ResponseTemplate::new(code).set_body_json(serde_json::json!({"Pins":[OTHER]})),
            )
            .expect(1)
            .mount(&proxy)
            .await;
        let p = validate(
            &sample("proxy", "cid")
                .replace("http://127.0.0.1:9094", &rest.uri())
                .replace("http://127.0.0.1:9095", &proxy.uri()),
        )
        .unwrap();
        let adapter = build_cluster_provider(&p.providers[0], None).unwrap();
        let submitted = adapter.submit_observed(request()).await;
        assert!(submitted.result.is_err());
        assert_eq!(submitted.effect, expected_effect);
        assert_eq!(submitted.resources.len(), 2);
        assert!(submitted.resources.iter().any(|r| r.cid == OTHER));
        assert!(
            submitted
                .resources
                .iter()
                .all(|r| r.ownership == Ownership::Unknown && r.replica_evidence.is_none())
        );
        assert!(rest.received_requests().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn proxy_rest_and_main_source_auth_and_actual_stored_bytes_are_isolated() {
    let source = MockServer::start().await;
    let proxy = MockServer::start().await;
    let rest = MockServer::start().await;
    let payload = b"\0actual SSE stored ciphertext\xff";
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(header("authorization", "Bearer source-secret"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Hash":CID,"Type":"raw"})),
        )
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(header("authorization", "Bearer source-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(header("authorization", "Bearer proxy-secret"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-chunked-output", "1")
                .set_body_raw(
                    format!(
                        "{{\"Name\":\"content\",\"Hash\":\"{CID}\",\"Bytes\":30,\"Size\":\"30\"}}\n"
                    ),
                    "application/json",
                ),
        )
        .mount(&proxy)
        .await;
    replicas(&rest, ["pinned", "pinned"], Some("Bearer rest-secret")).await;
    let text = sample("proxy", "upload").replace("http://127.0.0.1:5001", &source.uri())
        .replace("http://127.0.0.1:9094", &rest.uri()).replace("http://127.0.0.1:9095", &proxy.uri())
        .replace("min_replicas = 2", "proxy_auth = { auth = 'bearer', token_env = 'PROXY_TOKEN' }\nrest_auth = { auth = 'bearer', token_env = 'REST_TOKEN' }\nsource = { auth = 'bearer', token_env = 'SOURCE_TOKEN' }\nmin_replicas = 2")
        .replace("endpoint_revision = 1", "endpoint_revision = 1\nsecret_ref = 'env:PROXY_TOKEN'");
    let config: Config = toml::from_str(&text).unwrap();
    let p = ValidatedPinningConfig::from_config(&config, |name| match name {
        "PROXY_TOKEN" => Some("proxy-secret".into()),
        "REST_TOKEN" => Some("rest-secret".into()),
        "SOURCE_TOKEN" => Some("source-secret".into()),
        _ => None,
    })
    .unwrap();
    let debug = format!("{p:?}");
    for secret in ["proxy-secret", "rest-secret", "source-secret"] {
        assert!(!debug.contains(secret));
    }
    let adapter =
        build_cluster_provider(&p.providers[0], Some(KuboClient::new(source.uri()))).unwrap();
    let submitted = adapter.submit_observed(request()).await;
    assert_eq!(submitted.result.unwrap().status, RemotePinStatus::Pinned);
    assert_eq!(
        submitted.resources[0].status,
        ObservedResourceStatus::ReplicasVerified
    );
    let writes = proxy.received_requests().await.unwrap();
    assert_eq!(writes.len(), 1);
    assert!(
        writes[0]
            .body
            .windows(payload.len())
            .any(|window| window == payload)
    );
    assert!(!writes[0].url.query().unwrap().contains("replication"));
    assert_eq!(source.received_requests().await.unwrap().len(), 2);
    assert_eq!(rest.received_requests().await.unwrap().len(), 4);
}
