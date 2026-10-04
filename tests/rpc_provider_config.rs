use ipfs_s3_gateway::{
    config::Config,
    kubo::KuboClient,
    pinning::{
        config::{ProviderKind, ValidatedPinningConfig},
        coordinator::{PinningCoordinator, normalize_validated_config},
        identity::{CleanupMode, RemoteResourceType},
        ipfs_rpc::{RpcAuth, RpcStrategy},
        provider::{ObservedResourceStatus, SubmitEffect, SubmitPin},
    },
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const OTHER: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";

/// Valid values for the later documentation wave. No real account is used.
fn sample(kind: &str, strategy: &str, auth: &str, endpoint: &str) -> String {
    let (profile, backend, api_profile, scope, domain) = if kind == "filebase" {
        (
            "filebase",
            "filebase",
            "filebase-rpc",
            "bucket:backup",
            "filebase:backup",
        )
    } else {
        ("kubo", "kubo", "kubo", "node:backup", "kubo:backup")
    };
    let credential = if auth == "none" {
        String::new()
    } else {
        "token_env = 'REMOTE_TOKEN'".into()
    };
    let secret_ref = if auth == "none" {
        String::new()
    } else {
        "secret_ref = 'env:REMOTE_TOKEN'".into()
    };
    let username = if auth == "basic" {
        "username_env = 'REMOTE_USERNAME'"
    } else {
        ""
    };
    format!(
        r#"
        [[pinning.providers]]
        name = "backup"
        kind = "{kind}"
        api = "rpc"
        strategy = "{strategy}"
        endpoint = "{endpoint}"
        {credential}
        priority = 1
        max_bytes = 1073741824
        max_pins = 100

        [[pinning_rpc.providers]]
        config_name = "backup"
        profile = "{profile}"
        auth = "{auth}"
        {username}
        allow_private_network = true

        [pinning_identity]
        primary_storage_domain = "kubo:primary"
        [[pinning_identity.providers]]
        config_name = "backup"
        provider_id = "remote-backup"
        display_name = "Independent backup"
        backend = "{backend}"
        scope = "{scope}"
        storage_domain = "{domain}"
        credential_revision = 1
        endpoint_revision = 1
        {secret_ref}
        api_profile = "{api_profile}"
        strategy = "{strategy}"
    "#
    )
}

fn validate(text: &str) -> anyhow::Result<ValidatedPinningConfig> {
    let config: Config = toml::from_str(text)?;
    ValidatedPinningConfig::from_config(&config, |name| match name {
        "REMOTE_TOKEN" => Some("mock-secret".into()),
        "REMOTE_USERNAME" => Some("mock-user".into()),
        _ => None,
    })
}

#[test]
fn independent_kubo_all_strategies_and_auth_profiles_build_without_network_io() {
    for strategy in ["cid", "upload", "car"] {
        for auth in ["none", "bearer", "basic"] {
            let validated =
                validate(&sample("ipfs_rpc", strategy, auth, "http://127.0.0.1:5002")).unwrap();
            let p = &validated.providers[0];
            assert_eq!(p.kind, ProviderKind::IpfsRpc);
            assert_eq!(p.identity.backend, "kubo");
            assert_eq!(p.identity.resource_type(), RemoteResourceType::RpcPin);
            assert_eq!(p.identity.cleanup, CleanupMode::Retain);
            assert!(!format!("{validated:?}").contains("mock-secret"));
            assert!(!format!("{validated:?}").contains("mock-user"));
            let key = p.identity.allocation_key();
            let coordinator = PinningCoordinator::build_with_kubo(
                validated,
                Some(KuboClient::new("http://127.0.0.1:5001".into())),
            )
            .unwrap();
            assert_eq!(
                coordinator.provider(&key).unwrap().invocation_route(),
                ("kubo", strategy)
            );
        }
    }
    let cid = validate(&sample("ipfs_rpc", "cid", "none", "http://127.0.0.1:5002")).unwrap();
    PinningCoordinator::build(cid).unwrap();
    for strategy in ["upload", "car"] {
        let validated = validate(&sample(
            "ipfs_rpc",
            strategy,
            "none",
            "http://127.0.0.1:5002",
        ))
        .unwrap();
        assert!(PinningCoordinator::build(validated).is_err());
    }
}

#[test]
fn old_filebase_omission_remains_psa_and_explicit_psa_cid_is_compatible() {
    for route in ["", "api = 'psa'\nstrategy = 'cid'"] {
        let text = format!(
            r#"
            [[pinning.providers]]
            name = 'old-filebase'
            kind = 'filebase'
            token_env = 'REMOTE_TOKEN'
            priority = 1
            max_bytes = 1000
            max_pins = 10
            {route}
        "#
        );
        let validated = validate(&text).unwrap();
        assert!(validated.providers[0].rpc.is_none());
        assert_eq!(validated.providers[0].identity.api_profile, "filebase-psa");
        let coordinator = PinningCoordinator::build(validated).unwrap();
        assert_eq!(
            coordinator
                .provider("old-filebase")
                .unwrap()
                .invocation_route(),
            ("psa", "cid")
        );
    }
}

#[test]
fn filebase_rpc_upload_builds_but_psa_upload_car_and_other_auth_are_rejected() {
    let text = sample("filebase", "upload", "bearer", "https://rpc.filebase.io");
    let validated = validate(&text).unwrap();
    let p = &validated.providers[0];
    assert_eq!(p.rpc.as_ref().unwrap().strategy, RpcStrategy::Upload);
    assert!(matches!(
        p.rpc.as_ref().unwrap().auth,
        Some(RpcAuth::Bearer(_))
    ));
    let key = p.identity.allocation_key();
    let coordinator = PinningCoordinator::build_with_kubo(
        validated,
        Some(KuboClient::new("http://127.0.0.1:5001".into())),
    )
    .unwrap();
    assert_eq!(
        coordinator.provider(&key).unwrap().invocation_route(),
        ("filebase-rpc", "upload")
    );
    for bad in [
        text.replace("api = \"rpc\"", "api = \"psa\""),
        sample("filebase", "car", "bearer", "https://rpc.filebase.io"),
        sample("filebase", "cid", "bearer", "https://rpc.filebase.io"),
        sample("filebase", "upload", "none", "https://rpc.filebase.io"),
        sample("filebase", "upload", "basic", "https://rpc.filebase.io"),
    ] {
        assert!(validate(&bad).is_err());
    }
}

#[test]
fn administrator_url_private_network_and_tls_capabilities_are_fail_closed() {
    let base = sample("ipfs_rpc", "cid", "none", "http://127.0.0.1:5002");
    for bad in [
        "http://user:sentinel@127.0.0.1:5002",
        "https://example.test/?password=sentinel",
        "https://example.test/#sentinel",
        "ftp://example.test",
    ] {
        let error = validate(&base.replace("http://127.0.0.1:5002", bad)).unwrap_err();
        assert!(!format!("{error:#}").contains("sentinel"));
    }
    for endpoint in [
        "http://127.0.0.1:5002",
        "https://10.0.0.1",
        "https://localhost",
        "https://[::1]",
        "https://[::ffff:127.0.0.1]",
        "https://100.64.0.1",
        "https://198.18.0.1",
    ] {
        let text = base.replace("http://127.0.0.1:5002", endpoint).replace(
            "allow_private_network = true",
            "allow_private_network = false",
        );
        assert!(validate(&text).is_err(), "{endpoint}");
    }
    for option in [
        "tls_insecure = true",
        "tls_ca_pem = 'sentinel'",
        "tls_client_cert_pem = 'sentinel'",
        "tls_client_key_pem = 'sentinel'",
    ] {
        let error = validate(&base.replace(
            "allow_private_network = true",
            &format!("allow_private_network = true\n{option}"),
        ))
        .unwrap_err();
        assert!(!format!("{error:#}").contains("sentinel"));
    }
    let public = base
        .replace("http://127.0.0.1:5002", "https://rpc.example.test/api/v0")
        .replace(
            "allow_private_network = true",
            "allow_private_network = false",
        );
    validate(&public).unwrap();
    let primary = validate(&base).unwrap();
    assert!(
        PinningCoordinator::build_with_kubo(
            primary,
            Some(KuboClient::new("http://127.0.0.1:5002/api/v0/".into()))
        )
        .is_err()
    );
}

#[test]
fn explicit_identity_profile_secret_routes_and_primary_domain_are_checked() {
    let base = sample("ipfs_rpc", "cid", "bearer", "http://127.0.0.1:5002");
    for bad in [
        base.replace("backend = \"kubo\"", "backend = \"filebase\""),
        base.replace("api_profile = \"kubo\"", "api_profile = \"filebase-rpc\""),
        base.replace("env:REMOTE_TOKEN", "env:OTHER_TOKEN"),
        base.replace(
            "storage_domain = \"kubo:backup\"",
            "storage_domain = \"kubo:primary\"",
        ),
        base.replace("profile = \"kubo\"", "profile = \"cluster\""),
        base.replace("strategy = \"cid\"", "strategy = \"magic\""),
    ] {
        assert!(validate(&bad).is_err());
    }
    let missing_identity = base.split("[pinning_identity]").next().unwrap();
    assert!(validate(missing_identity).is_err());
    let duplicate = format!(
        "{base}\n{}",
        base.split("[[pinning_identity.providers]]")
            .nth(1)
            .unwrap()
            .replace("config_name = \"backup\"", "config_name = \"missing\"")
    );
    assert!(validate(&duplicate).is_err());
}

#[test]
fn rpc_aliases_deduplicate_one_backend_scope_without_changing_object_cid() {
    let base = sample("ipfs_rpc", "cid", "none", "http://127.0.0.1:5002");
    let alias = base
        .replace("name = \"backup\"", "name = \"alias\"")
        .replace("config_name = \"backup\"", "config_name = \"alias\"")
        .replace(
            "provider_id = \"remote-backup\"",
            "provider_id = \"remote-alias\"",
        )
        .replace(
            "[pinning_identity]\n        primary_storage_domain = \"kubo:primary\"",
            "",
        );
    let validated =
        normalize_validated_config(validate(&format!("{base}\n{alias}")).unwrap()).unwrap();
    assert_eq!(validated.provider_limits.len(), 1);
    assert_eq!(validated.providers[0].name, validated.providers[1].name);
}

#[tokio::test]
async fn configured_filebase_typed_trait_keeps_mismatch_resources_and_source_auth_isolated() {
    let target = MockServer::start().await;
    let source = MockServer::start().await;
    for (command, response) in [
        (
            "files/stat",
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Hash": CID, "Type":"file"})),
        ),
        (
            "cat",
            ResponseTemplate::new(200).set_body_bytes(b"\0SSE stored ciphertext\xff"),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(format!("/api/v0/{command}")))
            .respond_with(response)
            .mount(&source)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(header("authorization", "Bearer mock-secret"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{OTHER}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let validated = validate(&sample("filebase", "upload", "bearer", &target.uri())).unwrap();
    let key = validated.providers[0].identity.allocation_key();
    let coordinator =
        PinningCoordinator::build_with_kubo(validated, Some(KuboClient::new(source.uri())))
            .unwrap();
    let observation = coordinator
        .provider(&key)
        .unwrap()
        .submit_observed(SubmitPin {
            cid: CID.into(),
            name: "not-exported".into(),
            metadata: Default::default(),
        })
        .await;
    assert!(observation.result.is_err());
    assert_eq!(observation.effect, SubmitEffect::Observed);
    assert_eq!(observation.resources.len(), 1);
    assert_eq!(observation.resources[0].cid, OTHER);
    assert_eq!(
        observation.resources[0].status,
        ObservedResourceStatus::PinAccepted
    );
    assert!(
        source
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| !r.headers.contains_key("authorization"))
    );
    assert!(
        target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path() == "/api/v0/add")
    );
}
