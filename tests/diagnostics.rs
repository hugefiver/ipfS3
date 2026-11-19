use std::{fs, path::Path, process::Command};

fn run(config: &Path, token: Option<&str>) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"));
    command
        .arg("--config-explain")
        .env_clear()
        .env("IPFS_S3_CONFIG", config);
    if let Some(token) = token {
        command.env("DIAGNOSTIC_PIN_TOKEN", token);
    }
    command.output().unwrap()
}

#[test]
fn cli_reads_the_same_config_without_connecting_or_writing_and_redacts_values() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("gateway.toml");
    let database = temp.path().join("must-not-be-created.sqlite");
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database.display().to_string().replace('\\', "/")
    );
    let secret = "secret-credential-sentinel";
    let master_key = "a".repeat(64);
    let endpoint_query = "secret-query-sentinel";
    fs::write(
        &config,
        format!(
            r#"
        [storage]
        database_url = "{database_url}"
        [auth]
        [[auth.credentials]]
        access_key = "secret-access-sentinel"
        secret_key = "{secret}"
        [crypto]
        master_key = "{master_key}"
        [pinning]
        [[pinning.providers]]
        name = "remote"
        kind = "pinata"
        token_env = "DIAGNOSTIC_PIN_TOKEN"
        endpoint = "https://example.invalid/api?token={endpoint_query}"
        priority = 1
        max_bytes = 1000
        max_pins = 10
        [[pinning.policies]]
        bucket = "photos"
        prefix = "images/"
        trigger = "request"
        provider_mode = "one"
        providers = ["remote"]
        default_duration = "1d"
        max_duration = "2d"
        [pinning_identity]
        primary_storage_domain = "kubo:primary"
        [[pinning_identity.providers]]
        config_name = "remote"
        provider_id = "pinata-primary"
        display_name = "Pinata primary"
        backend = "pinata"
        scope = "account:secret-scope-sentinel"
        storage_domain = "pinata:primary"
        credential_revision = 2
        endpoint_revision = 3
        secret_ref = "env:DIAGNOSTIC_PIN_TOKEN"
        api_profile = "pinata-v3"
        strategy = "cid"
    "#,
        ),
    )
    .unwrap();

    let output = run(&config, Some("remote-token-sentinel"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    let combined = format!("{stdout}{stderr}");
    for forbidden in [
        secret,
        "secret-access-sentinel",
        "remote-token-sentinel",
        "DIAGNOSTIC_PIN_TOKEN",
        endpoint_query,
        "secret-scope-sentinel",
        "https://",
        &master_key,
        &database_url,
    ] {
        assert!(!combined.contains(forbidden), "leaked {forbidden}");
    }
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["command"], "config_explain");
    assert_eq!(
        report["provenance"]["evidence"],
        "independent_local_cli_load_not_running_gateway"
    );
    assert_eq!(report["provenance"]["config_file"], "present");
    assert_eq!(
        report["provenance"]["running_gateway_configuration"],
        "not_observed"
    );
    let pinning = &report["pinning"];
    assert_eq!(pinning["configured_policies"], 1);
    assert_eq!(pinning["configured_providers"], 1);
    assert_eq!(pinning["worker_completion"], "provider_ack");
    assert_eq!(pinning["physical_replicas"], "not_observed");
    let provider = &pinning["providers"][0];
    assert_eq!(provider["credential"], "present");
    assert_eq!(provider["scope_ref"], 1);
    assert_eq!(provider["credential_revision"], 2);
    assert_eq!(provider["endpoint_revision"], 3);
    assert_eq!(
        report["provenance"]["variables"]["provider_tokens"][0]["token_env_value"],
        "present"
    );
    assert!(!database.exists(), "diagnostic command created database");
}

fn alias_fixture(database_url: &str) -> String {
    format!(
        r#"
        [decompress_zip]
        auto_pin_outputs = false
        [storage]
        database_url = "{database_url}"
        [pinning]
        [[pinning.providers]]
        name = "alias-first"
        kind = "pinata"
        token_env = "DIAGNOSTIC_PIN_TOKEN"
        endpoint = "https://example.invalid/api?token=secret-query-sentinel"
        priority = 1
        max_bytes = 1000
        max_pins = 10
        [[pinning.providers]]
        name = "alias-second"
        kind = "pinata"
        token_env = "DIAGNOSTIC_PIN_TOKEN"
        endpoint = "https://example.invalid/api?token=secret-query-sentinel"
        priority = 1
        max_bytes = 1000
        max_pins = 10
        [[pinning.policies]]
        bucket = "photos"
        prefix = "images/"
        trigger = "always"
        provider_mode = "all"
        providers = ["alias-first", "alias-second"]
        default_duration = "1d"
        max_duration = "2d"
        [pinning_identity]
        primary_storage_domain = "kubo:primary"
        [[pinning_identity.providers]]
        config_name = "alias-first"
        provider_id = "pinata-first"
        display_name = "First"
        backend = "pinata"
        scope = "account:secret-scope-sentinel"
        storage_domain = "pinata:shared"
        credential_revision = 1
        endpoint_revision = 1
        secret_ref = "env:DIAGNOSTIC_PIN_TOKEN"
        api_profile = "pinata-v3"
        strategy = "cid"
        [[pinning_identity.providers]]
        config_name = "alias-second"
        provider_id = "pinata-second"
        display_name = "Second"
        backend = "pinata"
        scope = "account:secret-scope-sentinel"
        storage_domain = "pinata:shared"
        credential_revision = 1
        endpoint_revision = 1
        secret_ref = "env:DIAGNOSTIC_PIN_TOKEN"
        api_profile = "pinata-v3"
        strategy = "cid"
    "#
    )
}

#[test]
fn cli_aliases_report_one_scope_without_database_access() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("aliases.toml");
    let database = temp.path().join("must-not-be-created.sqlite");
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database.display().to_string().replace('\\', "/")
    );
    fs::write(&config, alias_fixture(&database_url)).unwrap();
    let output = run(&config, Some("secret-token-sentinel"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    let combined = format!("{stdout}{stderr}");
    for forbidden in [
        "secret-token-sentinel",
        "secret-query-sentinel",
        "secret-scope-sentinel",
        "account:",
        "domain:",
        "alias-first",
        "alias-second",
        &database_url,
    ] {
        assert!(!combined.contains(forbidden), "leaked {forbidden}");
    }
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["pinning"]["validation"], "passed");
    assert_eq!(
        report["provenance"]["gateway_startup_checks"],
        "pinning_config_only_no_full_startup"
    );
    assert_eq!(report["pinning"]["configured_providers"], 2);
    let providers = &report["pinning"]["providers"];
    assert_eq!(providers[0]["scope_ref"], 1);
    assert_eq!(providers[1]["scope_ref"], 1);
    assert!(!database.exists(), "diagnostic command created database");
}

#[test]
fn cli_rejects_alias_limits_and_revision_conflicts_without_leaking_or_writing() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("aliases.toml");
    let database = temp.path().join("must-not-be-created.sqlite");
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database.display().to_string().replace('\\', "/")
    );
    let fixture = alias_fixture(&database_url);
    for (original, conflict) in [
        ("max_pins = 10", "max_pins = 11"),
        ("endpoint_revision = 1", "endpoint_revision = 2"),
    ] {
        let last = fixture.rfind(original).unwrap();
        let mut invalid = fixture.clone();
        invalid.replace_range(last..last + original.len(), conflict);
        fs::write(&config, invalid).unwrap();
        let output = run(&config, Some("secret-token-sentinel"));
        assert!(!output.status.success(), "accepted conflicting alias");
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(report["pinning"]["validation"], "failed");
        let combined = format!("{stdout}{stderr}");
        for forbidden in [
            "secret-token-sentinel",
            "secret-query-sentinel",
            "secret-scope-sentinel",
            "alias-first",
            "alias-second",
            &database_url,
        ] {
            assert!(!combined.contains(forbidden), "leaked {forbidden}");
        }
        assert!(!database.exists(), "diagnostic command created database");
    }
}

#[test]
fn invalid_or_missing_credentials_never_print_free_form_config_errors() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("broken.toml");
    fs::write(
        &config,
        "[pinning]\nnot-a-valid-field = 'secret-error-sentinel'\nmalformed = [\n",
    )
    .unwrap();
    let output = run(&config, None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains("secret-error-sentinel"));
    assert!(!stderr.contains("broken.toml"));
    assert!(output.stdout.is_empty());

    fs::write(
        &config,
        r#"
        [pinning]
        [[pinning.providers]]
        name = "secret-provider-sentinel"
        kind = "pinata"
        token_env = "DIAGNOSTIC_PIN_TOKEN"
        priority = 1
        max_bytes = 1000
        max_pins = 10
    "#,
    )
    .unwrap();
    let output = run(&config, None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains("secret-provider-sentinel"));
    assert!(!stderr.contains("DIAGNOSTIC_PIN_TOKEN"));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["pinning"]["validation"], "failed");
    assert_eq!(
        report["provenance"]["variables"]["provider_tokens"][0]["token_env_value"],
        "missing"
    );
    assert!(!report.to_string().contains("secret-provider-sentinel"));
}

#[test]
fn default_config_reports_no_remote_evidence_or_database_side_effect() {
    let temp = tempfile::tempdir().unwrap();
    let missing_config = temp.path().join("missing.toml");
    let output = run(&missing_config, None);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["provenance"]["config_file"], "missing");
    assert_eq!(report["pinning"]["configured_providers"], 0);
    assert_eq!(
        report["pinning"]["remote_capabilities"],
        "unknown_not_probed"
    );
    assert_eq!(report["pinning"]["database_status"], "not_observed");
}
