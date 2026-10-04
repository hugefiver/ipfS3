use std::{fs, io::ErrorKind, net::TcpListener, path::PathBuf, process::Command};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;

const TOKEN_ENV: &str = "RPC_DIAGNOSTIC_TOKEN_SENTINEL";
const TOKEN: &str = "rpc-diagnostic-secret-token-sentinel";
const USERNAME_ENV: &str = "RPC_DIAGNOSTIC_USERNAME_SENTINEL";
const USERNAME: &str = "rpc-diagnostic-secret-username-sentinel";

struct Fixture {
    directory: tempfile::TempDir,
    config: PathBuf,
    database_url: String,
    network: TcpListener,
}

impl Fixture {
    fn new(pinning: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let network = TcpListener::bind("127.0.0.1:0").unwrap();
        network.set_nonblocking(true).unwrap();
        let config = directory.path().join("gateway.toml");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("must-not-be-created.sqlite")
                .display()
                .to_string()
                .replace('\\', "/")
        );
        fs::write(
            &config,
            format!("[storage]\ndatabase_url = '{database_url}'\n{pinning}"),
        )
        .unwrap();
        Self {
            directory,
            config,
            database_url,
            network,
        }
    }

    fn run(&self, args: &[&str], credentials: &[(&str, &str)], success: bool) -> Value {
        // Catch accidental provider network access without creating a provider
        // client or answering any request. The child has no inherited proxy/env.
        let proxy = format!("http://{}", self.network.local_addr().unwrap());
        let output = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"))
            .args(args)
            .env_clear()
            .env("IPFS_S3_CONFIG", &self.config)
            .env("HTTP_PROXY", &proxy)
            .env("HTTPS_PROXY", &proxy)
            .envs(credentials.iter().copied())
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(output.status.success(), success, "{stdout}\n{stderr}");
        let combined = format!("{stdout}{stderr}");
        for forbidden in [
            TOKEN_ENV,
            TOKEN,
            USERNAME_ENV,
            USERNAME,
            "rpc-provider-sentinel",
            "rpc-endpoint-sentinel",
            "rpc-scope-sentinel",
            "env:",
            "https://",
            "http://",
            "sha256",
            "policy:0:",
            "rpc-bucket-sentinel",
            &self.database_url,
        ] {
            assert!(!combined.contains(forbidden), "leaked {forbidden}");
        }
        for (name, value) in credentials {
            assert!(!combined.contains(name), "leaked credential env name");
            assert!(!combined.contains(value), "leaked credential value");
        }
        assert!(
            !combined.contains(&STANDARD.encode(format!("{USERNAME}:{TOKEN}"))),
            "CLI emitted a Basic authorization value"
        );
        assert!(
            !combined
                .split(|character: char| !character.is_ascii_hexdigit())
                .any(|part| part.len() >= 64),
            "CLI emitted a digest or key"
        );
        match self.network.accept() {
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            _ => panic!("CLI attempted remote access"),
        }
        let files: Vec<_> = fs::read_dir(self.directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(files, vec![self.config.clone()], "CLI created local files");
        let report: Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(
            report["side_effects"],
            "no_database_connection_no_remote_probe_no_upload"
        );
        assert_eq!(report["pinning"]["database_status"], "not_observed");
        assert_eq!(
            report["pinning"]["remote_capabilities"],
            "unknown_not_probed"
        );
        assert_remote_unknown(&report);
        report
    }
}

fn assert_remote_unknown(value: &Value) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                match key.as_str() {
                    "remote_permission" => assert_eq!(value, "unknown"),
                    "remote_observation" => assert_eq!(value, "not_performed"),
                    _ => assert_remote_unknown(value),
                }
            }
        }
        Value::Array(values) => values.iter().for_each(assert_remote_unknown),
        _ => {}
    }
}

fn operation(provider: &Value, name: &str) -> Value {
    provider["operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|operation| operation["operation"] == name)
        .unwrap_or_else(|| panic!("missing {name} operation"))["configured_route"]
        .clone()
}

#[test]
fn legacy_filebase_psa_is_cid_only_not_an_rpc_upload_route() {
    let fixture = Fixture::new(&format!(
        r#"
        [pinning]
        [[pinning.providers]]
        name = "rpc-provider-sentinel"
        kind = "filebase"
        token_env = "{TOKEN_ENV}"
        endpoint = "https://rpc-endpoint-sentinel.invalid/psa"
        priority = 1
        max_bytes = 1000
        max_pins = 10
        "#,
    ));
    for args in [vec!["--pinning-doctor"], vec!["--config-explain"]] {
        let report = fixture.run(&args, &[(TOKEN_ENV, TOKEN)], true);
        let provider = &report["pinning"]["providers"][0];
        assert_eq!(provider["backend"], "filebase");
        assert_eq!(provider["profile"], "filebase-psa");
        assert_eq!(provider["strategy"], "cid");
        assert_eq!(provider["credential"], "present");
        assert_eq!(operation(provider, "submit_cid"), "supported");
        assert_eq!(operation(provider, "upload_bytes"), "denied");
        assert_eq!(operation(provider, "import_car"), "denied");
        assert_eq!(operation(provider, "query_remote"), "supported");
        assert_eq!(operation(provider, "query_recursive"), "denied");
    }
}

fn rpc_config(kind: &str, strategy: &str, auth: &str, aliases: usize) -> String {
    let (profile, backend, api_profile) = if kind == "filebase" {
        ("filebase", "filebase", "filebase-rpc")
    } else {
        ("kubo", "kubo", "kubo")
    };
    let token = if auth == "none" {
        String::new()
    } else {
        format!("token_env = '{TOKEN_ENV}'")
    };
    let username = if auth == "basic" {
        format!("username_env = '{USERNAME_ENV}'")
    } else {
        String::new()
    };
    let secret_ref = if auth == "none" {
        String::new()
    } else {
        format!("secret_ref = 'env:{TOKEN_ENV}'")
    };
    let mut config = String::from("[pinning]\n");
    for index in 0..aliases {
        config.push_str(&format!(
            r#"
            [[pinning.providers]]
            name = "rpc-provider-sentinel-{index}"
            kind = "{kind}"
            api = "rpc"
            strategy = "{strategy}"
            {token}
            endpoint = "https://rpc-endpoint-sentinel.invalid/api/v0"
            priority = 1
            enabled = true
            max_bytes = 1000
            max_pins = 10
            "#,
        ));
    }
    let providers: Vec<_> = (0..aliases)
        .map(|index| format!("rpc-provider-sentinel-{index}"))
        .collect();
    config.push_str(&format!(
        r#"
        [[pinning.policies]]
        bucket = "rpc-bucket-sentinel"
        prefix = "prefix/"
        trigger = "always"
        provider_mode = "all"
        providers = {}
        default_duration = "1d"
        max_duration = "2d"
        [pinning_rpc]
        "#,
        serde_json::to_string(&providers).unwrap(),
    ));
    for index in 0..aliases {
        config.push_str(&format!(
            r#"
            [[pinning_rpc.providers]]
            config_name = "rpc-provider-sentinel-{index}"
            profile = "{profile}"
            auth = "{auth}"
            {username}
            "#,
        ));
    }
    config.push_str("[pinning_identity]\nprimary_storage_domain = 'kubo:primary'\n");
    for index in 0..aliases {
        config.push_str(&format!(
            r#"
            [[pinning_identity.providers]]
            config_name = "rpc-provider-sentinel-{index}"
            provider_id = "rpc-provider-sentinel-id-{index}"
            display_name = "rpc-provider-sentinel-display-{index}"
            backend = "{backend}"
            scope = "account:rpc-scope-sentinel"
            storage_domain = "rpc:backup"
            credential_revision = 1
            endpoint_revision = 1
            {secret_ref}
            api_profile = "{api_profile}"
            strategy = "{strategy}"
            "#,
        ));
    }
    config
}

#[test]
fn filebase_rpc_upload_reports_its_actual_profile_and_denies_cid_and_car() {
    let fixture = Fixture::new(&rpc_config("filebase", "upload", "bearer", 1));
    for args in [vec!["--pinning-doctor"], vec!["--config-explain"]] {
        let report = fixture.run(&args, &[(TOKEN_ENV, TOKEN)], true);
        let provider = &report["pinning"]["providers"][0];
        assert_eq!(provider["backend"], "filebase");
        assert_eq!(provider["profile"], "filebase-rpc");
        assert_eq!(provider["strategy"], "upload");
        assert_eq!(provider["credential"], "present");
        assert_eq!(operation(provider, "submit_cid"), "denied");
        assert_eq!(operation(provider, "upload_bytes"), "supported");
        assert_eq!(operation(provider, "import_car"), "denied");
        assert_eq!(operation(provider, "query_remote"), "supported");
        assert_eq!(operation(provider, "query_recursive"), "supported");
        assert_eq!(operation(provider, "remove_remote"), "denied");
    }
}

#[test]
fn kubo_cid_upload_and_car_are_offline_with_anonymous_basic_and_bearer_auth() {
    for strategy in ["cid", "upload", "car"] {
        for auth in ["none", "basic", "bearer"] {
            let credentials = match auth {
                "basic" => vec![(TOKEN_ENV, TOKEN), (USERNAME_ENV, USERNAME)],
                "bearer" => vec![(TOKEN_ENV, TOKEN)],
                _ => Vec::new(),
            };
            let fixture = Fixture::new(&rpc_config("ipfs_rpc", strategy, auth, 1));
            for args in [vec!["--pinning-doctor"], vec!["--config-explain"]] {
                let report = fixture.run(&args, &credentials, true);
                let provider = &report["pinning"]["providers"][0];
                assert_eq!(provider["backend"], "kubo");
                assert_eq!(provider["profile"], "kubo");
                assert_eq!(provider["strategy"], strategy);
                assert_eq!(
                    provider["credential"],
                    if auth == "none" {
                        "not_required"
                    } else {
                        "present"
                    }
                );
                for (operation_name, selected) in [
                    ("submit_cid", "cid"),
                    ("upload_bytes", "upload"),
                    ("import_car", "car"),
                ] {
                    assert_eq!(
                        operation(provider, operation_name),
                        if strategy == selected {
                            "supported"
                        } else {
                            "denied"
                        }
                    );
                }
                assert_eq!(operation(provider, "query_recursive"), "supported");
                let variables = &report["provenance"]["variables"]["provider_tokens"][0];
                assert_eq!(
                    variables["token_env_reference"],
                    if auth == "none" {
                        "not_required"
                    } else {
                        "configured"
                    }
                );
                assert_eq!(
                    variables["token_env_value"],
                    if auth == "none" {
                        "not_required"
                    } else {
                        "present"
                    }
                );
                assert_eq!(
                    variables["username_env_value"],
                    if auth == "basic" {
                        "present"
                    } else {
                        "not_required"
                    }
                );
            }
        }
    }
}

#[test]
fn rpc_missing_credentials_fail_with_presence_only_evidence() {
    for (kind, auth, credentials, missing) in [
        ("filebase", "bearer", vec![], "token_env_value"),
        ("ipfs_rpc", "bearer", vec![], "token_env_value"),
        (
            "ipfs_rpc",
            "basic",
            vec![(TOKEN_ENV, TOKEN)],
            "username_env_value",
        ),
        (
            "ipfs_rpc",
            "basic",
            vec![(USERNAME_ENV, USERNAME)],
            "token_env_value",
        ),
    ] {
        let fixture = Fixture::new(&rpc_config(kind, "upload", auth, 1));
        let report = fixture.run(&["--config-explain"], &credentials, false);
        assert_eq!(report["pinning"]["validation"], "failed");
        assert!(report["pinning"]["providers"].is_null());
        let evidence = &report["provenance"]["variables"]["provider_tokens"][0];
        assert_eq!(evidence[missing], "missing");
        assert_eq!(evidence["token_env_reference"], "configured");
        if auth == "basic" {
            assert_eq!(evidence["username_env_reference"], "configured");
        }
    }
}

#[test]
fn unsupported_filebase_rpc_and_psa_modes_fail_closed() {
    for strategy in ["cid", "car"] {
        let fixture = Fixture::new(&rpc_config("filebase", strategy, "bearer", 1));
        let report = fixture.run(&["--pinning-doctor"], &[(TOKEN_ENV, TOKEN)], false);
        assert_eq!(report["pinning"]["validation"], "failed");
        assert!(report["pinning"]["providers"].is_null());
    }
    let fixture = Fixture::new(&format!(
        r#"
        [pinning]
        [[pinning.providers]]
        name = "rpc-provider-sentinel"
        kind = "filebase"
        api = "psa"
        strategy = "upload"
        token_env = "{TOKEN_ENV}"
        priority = 1
        max_bytes = 1000
        max_pins = 10
        "#,
    ));
    let report = fixture.run(&["--config-explain"], &[(TOKEN_ENV, TOKEN)], false);
    assert_eq!(report["pinning"]["validation"], "failed");
}

#[test]
fn rpc_aliases_share_the_normalized_policy_target_and_reject_conflicts() {
    let config = rpc_config("ipfs_rpc", "car", "basic", 2);
    let credentials = [(TOKEN_ENV, TOKEN), (USERNAME_ENV, USERNAME)];
    let args = [
        "--pinning-policy-explain",
        "rpc-bucket-sentinel",
        "prefix/a",
    ];
    let fixture = Fixture::new(&config);
    let report = fixture.run(&args, &credentials, true);
    assert_eq!(report["pinning"]["configured_providers"], 2);
    assert_eq!(report["pinning"]["providers"][0]["scope_ref"], 1);
    assert_eq!(report["pinning"]["providers"][1]["scope_ref"], 1);
    assert_eq!(
        report["policy_explain"]["provider_indices"],
        serde_json::json!([0])
    );
    assert_eq!(
        operation(&report["policy_explain"], "automatic_pin"),
        "supported"
    );
    for (original, replacement) in [
        ("max_pins = 10", "max_pins = 11"),
        ("endpoint_revision = 1", "endpoint_revision = 2"),
    ] {
        let mut conflict = config.clone();
        let index = conflict.rfind(original).unwrap();
        conflict.replace_range(index..index + original.len(), replacement);
        let fixture = Fixture::new(&conflict);
        for command in [
            &["--pinning-doctor"][..],
            &["--config-explain"][..],
            &args[..],
        ] {
            let report = fixture.run(command, &credentials, false);
            assert_eq!(report["pinning"]["validation"], "failed");
        }
    }
}
