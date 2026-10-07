use serde_json::Value;
use std::{fs, io::ErrorKind, net::TcpListener, process::Command};

const SECRETS: [(&str, &str); 3] = [
    ("CLUSTER_PROXY_SENTINEL", "proxy-secret-sentinel"),
    ("CLUSTER_REST_SENTINEL", "rest-secret-sentinel"),
    ("CLUSTER_SOURCE_SENTINEL", "source-secret-sentinel"),
];

fn config(endpoint: &str, database: &str, protocol: &str, strategy: &str, auth: bool) -> String {
    let proxy = if protocol == "proxy" {
        format!("proxy_endpoint = '{endpoint}/proxy'")
    } else {
        String::new()
    };
    let mut credentials = String::new();
    let mut secret_ref = String::new();
    if auth {
        credentials
            .push_str("rest_auth = { auth = 'bearer', token_env = 'CLUSTER_REST_SENTINEL' }\n");
        if protocol == "proxy" {
            credentials.push_str(
                "proxy_auth = { auth = 'bearer', token_env = 'CLUSTER_PROXY_SENTINEL' }\n",
            );
        }
        if strategy != "cid" {
            credentials
                .push_str("source = { auth = 'bearer', token_env = 'CLUSTER_SOURCE_SENTINEL' }\n");
        }
        secret_ref = format!(
            "secret_ref = 'env:{}'",
            if protocol == "proxy" {
                SECRETS[0].0
            } else {
                SECRETS[1].0
            }
        );
    }
    format!(
        r#"
        [storage]
        database_url = "{database}"
        [kubo]
        rpc_url = "{endpoint}/source"
        [[pinning.providers]]
        name = "cluster-name-sentinel"
        kind = "cluster"
        strategy = "{strategy}"
        priority = 1
        max_bytes = 10000
        max_pins = 10
        [pinning.providers.cluster]
        protocol = "{protocol}"
        rest_endpoint = "{endpoint}/rest"
        {proxy}
        {credentials}
        min_replicas = 2
        allow_private_network = true
        [pinning_identity]
        primary_storage_domain = "kubo:primary"
        [[pinning_identity.providers]]
        config_name = "cluster-name-sentinel"
        provider_id = "cluster-id-sentinel"
        display_name = "cluster-display-sentinel"
        backend = "cluster"
        scope = "cluster-scope-sentinel"
        storage_domain = "cluster:backup"
        credential_revision = 1
        endpoint_revision = 1
        {secret_ref}
        api_profile = "cluster-{protocol}"
        strategy = "{strategy}"
    "#
    )
}

fn operation(report: &Value, name: &str) -> Value {
    report["pinning"]["providers"][0]["operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["operation"] == name)
        .unwrap()["configured_route"]
        .clone()
}

fn run(protocol: &str, strategy: &str, auth: bool, missing: bool) {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let config_path = directory.path().join("cluster.toml");
    let database_path = directory.path().join("must-not-exist.sqlite");
    let database = format!(
        "sqlite://{}?mode=rwc",
        database_path.display().to_string().replace('\\', "/")
    );
    fs::write(
        &config_path,
        config(&endpoint, &database, protocol, strategy, auth),
    )
    .unwrap();
    for flag in ["--config-explain", "--pinning-doctor"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"));
        command
            .arg(flag)
            .env_clear()
            .env("IPFS_S3_CONFIG", &config_path)
            .env("HTTP_PROXY", &endpoint)
            .env("HTTPS_PROXY", &endpoint);
        if auth && !missing {
            command.envs(SECRETS);
        }
        let output = command.output().unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(output.status.success(), !missing, "{stdout}\n{stderr}");
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
        if missing {
            assert_eq!(report["pinning"]["validation"], "failed");
            assert!(report["pinning"]["providers"].is_null());
            assert_eq!(
                report["provenance"]["variables"]["provider_tokens"][0]["rest"]["token_env_value"],
                "missing"
            );
        } else {
            let provider = &report["pinning"]["providers"][0];
            assert_eq!(provider["profile"], format!("cluster-{protocol}"));
            assert_eq!(provider["strategy"], strategy);
            assert_eq!(provider["confirmation"]["min_replicas"], 2);
            assert_eq!(provider["confirmation"]["max_age_secs"], 30);
            assert_eq!(
                provider["confirmation"]["remote_observation"],
                "not_performed"
            );
            assert_eq!(
                provider["confirmation"]["account_replication"],
                "not_proven"
            );
            assert_eq!(
                provider["confirmation"]["failure_domain_independence"],
                "not_proven"
            );
            assert_eq!(operation(&report, "query_cluster_replicas"), "supported");
            assert_eq!(operation(&report, "query_recursive"), "denied");
            assert_eq!(operation(&report, "remove_remote"), "denied");
            for (name, route) in [
                ("submit_cid", "cid"),
                ("upload_bytes", "upload"),
                ("import_car", "car"),
            ] {
                assert_eq!(
                    operation(&report, name),
                    if route == strategy {
                        "supported"
                    } else {
                        "denied"
                    }
                );
            }
            for item in provider["operations"].as_array().unwrap() {
                assert_eq!(item["remote_permission"], "unknown");
                assert_eq!(item["remote_observation"], "not_performed");
            }
        }
        let combined = format!("{stdout}{stderr}");
        for sentinel in [
            "cluster-name-sentinel",
            "cluster-id-sentinel",
            "cluster-scope-sentinel",
            &endpoint,
            &database,
            "env:",
        ] {
            assert!(!combined.contains(sentinel), "leaked {sentinel}");
        }
        for (name, secret) in SECRETS {
            assert!(!combined.contains(name) && !combined.contains(secret));
        }
        assert!(!database_path.exists());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
        assert_eq!(listener.accept().unwrap_err().kind(), ErrorKind::WouldBlock);
    }
}

#[test]
fn cluster_cli_is_local_only_and_does_not_claim_account_replication() {
    for protocol in ["proxy", "rest"] {
        for strategy in ["cid", "upload", "car"] {
            for auth in [false, true] {
                run(protocol, strategy, auth, false);
            }
        }
    }
}

#[test]
fn missing_independent_credentials_report_presence_only_with_no_io() {
    for protocol in ["proxy", "rest"] {
        run(protocol, "upload", true, true);
    }
}
