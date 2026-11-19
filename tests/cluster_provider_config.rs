use ipfs_s3_gateway::{
    config::Config,
    kubo::KuboClient,
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::{PinningCoordinator, build_cluster_provider, normalize_validated_config},
        provider::{RemotePinStatus, SubmitPin},
    },
    state::AppState,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

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
fn ordinary_provider_loads_without_cluster_settings() {
    let config: Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name = "ordinary-noop"
        kind = "noop"
        priority = 1
        max_bytes = 10000
        max_pins = 10
        "#,
    )
    .unwrap();
    assert_eq!(config.pinning.worker_interval, "5s");
    assert_eq!(config.pinning.worker_concurrency, 4);
    let validated = ValidatedPinningConfig::from_config(&config, |_| None).unwrap();
    let coordinator = PinningCoordinator::build(validated).unwrap();
    let provider = &coordinator.effective_config().providers[0];
    assert!(provider.cluster.is_none());
    assert!(coordinator.provider_limits()[&provider.name].enabled);
    assert_eq!(
        coordinator.provider(&provider.name).unwrap().name(),
        provider.name
    );
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
        format!("{text}\n[cold_kubo]\nrpc_url = 'http://127.0.0.1:9095/api/v0/'"),
        text.replace("protocol = \"proxy\"", "protocol = \"auto\""),
        text.replacen("strategy = \"cid\"", "strategy = \"auto\"", 1),
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
            "allow_private_network = true",
            "source = { endpoint = 'http://other.invalid', auth = 'none' }\nallow_private_network = true"
        ))
        .is_err()
    );
}

#[test]
fn same_pinset_aliases_share_one_allocation_and_reject_transport_or_domain_conflicts() {
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
        alias.replace(
            "allow_private_network = true",
            "allow_private_network = true\nstream_channels = false",
        ),
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

#[tokio::test]
async fn registered_alias_dispatches_only_to_the_canonical_write_route() {
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

        let state = AppState::new(&config).await.unwrap();
        let effective = state.pinning.effective_config();
        let selected = &effective.providers[0];
        assert_eq!(selected.identity.provider_id, "cluster-alias");
        assert_eq!(selected.name, effective.providers[1].name);
        assert_eq!(effective.provider_limits.len(), 1);
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
            .submit(request())
            .await
            .unwrap();
        assert_eq!(submitted.status, RemotePinStatus::Pinned);
        assert_eq!(submitted.request_id, CID);
        assert!(first_rest.received_requests().await.unwrap().is_empty());
        assert!(first_proxy.received_requests().await.unwrap().is_empty());
        let writes = target.received_requests().await.unwrap();

        assert_eq!(writes.len(), 1);
        assert!(
            !writes[0]
                .url
                .query()
                .unwrap_or_default()
                .contains("replication")
        );
        let unused = if protocol == "rest" {
            &alias_proxy
        } else {
            &alias_rest
        };
        assert!(unused.received_requests().await.unwrap().is_empty());
    }
}

#[test]
fn missing_independent_auth_cannot_reuse_another_route_credential() {
    let text = sample("proxy", "upload")
        .replace("allow_private_network = true", "proxy_auth = { auth = 'bearer', token_env = 'PROXY_TOKEN' }\nrest_auth = { auth = 'basic', username_env = 'REST_USER', token_env = 'REST_PASSWORD' }\nsource = { auth = 'bearer', token_env = 'SOURCE_TOKEN' }\nallow_private_network = true")
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

fn request() -> SubmitPin {
    SubmitPin {
        cid: CID.into(),
        name: "do-not-export-object".into(),
        metadata: Default::default(),
    }
}

#[tokio::test]
async fn configured_proxy_and_main_source_auth_are_isolated_during_upload() {
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
    let text = sample("proxy", "upload").replace("http://127.0.0.1:5001", &source.uri())
        .replace("http://127.0.0.1:9094", &rest.uri()).replace("http://127.0.0.1:9095", &proxy.uri())
        .replace("allow_private_network = true", "proxy_auth = { auth = 'bearer', token_env = 'PROXY_TOKEN' }\nrest_auth = { auth = 'bearer', token_env = 'REST_TOKEN' }\nsource = { auth = 'bearer', token_env = 'SOURCE_TOKEN' }\nallow_private_network = true")
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
    let submitted = adapter.submit(request()).await.unwrap();
    assert_eq!(submitted.status, RemotePinStatus::Pinned);
    assert_eq!(submitted.request_id, CID);
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
    assert!(rest.received_requests().await.unwrap().is_empty());
}
