use std::{fs, path::Path, process::Command};

use chrono::Utc;
use ipfs_s3_gateway::{
    pinning::{
        decision::{DecisionEffect, DecisionOrigin, ExtensionDecision},
        tags::PinControl,
    },
    store::{
        bucket,
        entities::{
            object_version, pin_extension_decision, pin_lease, pin_lease_target, remote_pin,
            remote_pin_ledger,
        },
        object,
        object_version::BucketVersioningState,
        run_migrations,
    },
};
use sea_orm::{Database, EntityTrait, Set};

fn run(config: &Path, args: &[&str], token: Option<&str>) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"));
    command.args(args).env_clear().env("IPFS_S3_CONFIG", config);
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
        master_key = "{}"
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
            "a".repeat(64)
        ),
    )
    .unwrap();

    for args in [
        vec!["--pinning-doctor"],
        vec!["--config-explain"],
        vec!["--pinning-policy-explain", "photos", "images/a.jpg"],
    ] {
        let output = run(&config, &args, Some("remote-token-sentinel"));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        for forbidden in [
            secret,
            "secret-access-sentinel",
            "remote-token-sentinel",
            endpoint_query,
            "secret-scope-sentinel",
            "https://",
            "policy:0:",
            &database_url,
        ] {
            assert!(
                !format!("{stdout}{stderr}").contains(forbidden),
                "leaked {forbidden}"
            );
        }
        let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(
            report["provenance"]["evidence"],
            "independent_local_cli_load_not_running_gateway"
        );
        assert_eq!(report["provenance"]["config_file"], "present");
        assert_eq!(
            report["provenance"]["running_gateway_configuration"],
            "not_observed"
        );
        assert_eq!(report["pinning"]["configured_providers"], 1);
        assert_eq!(report["pinning"]["providers"][0]["credential"], "present");
        assert_eq!(report["pinning"]["providers"][0]["scope_ref"], 1);
        assert_eq!(report["pinning"]["providers"][0]["credential_revision"], 2);
        assert_eq!(report["pinning"]["providers"][0]["endpoint_revision"], 3);
        assert_eq!(
            report["provenance"]["variables"]["provider_tokens"][0]["token_env_value"],
            "present"
        );
        assert_eq!(
            report["pinning"]["providers"][0]["operations"][0]["configured_route"],
            "supported"
        );
        assert_eq!(
            report["pinning"]["providers"][0]["operations"][0]["remote_permission"],
            "unknown"
        );
        assert!(!database.exists(), "diagnostic command created database");
    }
    let output = run(
        &config,
        &["--pinning-policy-explain", "photos", "images/a.jpg"],
        Some("token"),
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["policy_explain"]["policy_index"], 0);
    assert_eq!(
        report["policy_explain"]["operations"][0]["configured_route"],
        "denied"
    );
    assert_eq!(
        report["policy_explain"]["operations"][1]["configured_route"],
        "supported"
    );
    let no_match = run(
        &config,
        &["--pinning-policy-explain", "photos", "other/a.jpg"],
        Some("token"),
    );
    let report: serde_json::Value = serde_json::from_slice(&no_match.stdout).unwrap();
    assert_eq!(report["policy_explain"]["match"], "none");
    assert_eq!(
        report["policy_explain"]["operations"][1]["configured_route"],
        "denied"
    );
    assert!(!database.exists());
}

fn alias_fixture(database_url: &str) -> String {
    format!(
        r#"
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
fn cli_aliases_use_one_effective_policy_target_without_remote_or_database_access() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("aliases.toml");
    let database = temp.path().join("must-not-be-created.sqlite");
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database.display().to_string().replace('\\', "/")
    );
    fs::write(&config, alias_fixture(&database_url)).unwrap();
    for args in [
        vec!["--pinning-doctor"],
        vec!["--config-explain"],
        vec!["--pinning-policy-explain", "photos", "images/a.jpg"],
    ] {
        let output = run(&config, &args, Some("secret-token-sentinel"));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
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
            assert!(
                !format!("{stdout}{stderr}").contains(forbidden),
                "leaked {forbidden}"
            );
        }
        let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(report["pinning"]["validation"], "passed");
        assert_eq!(
            report["provenance"]["running_gateway_configuration"],
            "not_observed"
        );
        assert_eq!(
            report["provenance"]["gateway_startup_checks"],
            "pinning_config_only_no_full_startup"
        );
        assert_eq!(report["pinning"]["configured_providers"], 2);
        if args[0] == "--pinning-policy-explain" {
            assert_eq!(
                report["policy_explain"]["provider_indices"],
                serde_json::json!([0])
            );
            assert_eq!(
                report["policy_explain"]["operations"][0]["configured_route"],
                "supported"
            );
        }
        assert!(!database.exists(), "diagnostic command created database");
    }
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
        for args in [
            vec!["--pinning-doctor"],
            vec!["--config-explain"],
            vec!["--pinning-policy-explain", "photos", "images/a.jpg"],
        ] {
            let output = run(&config, &args, Some("secret-token-sentinel"));
            assert!(
                !output.status.success(),
                "{args:?} accepted conflicting alias"
            );
            let stdout = String::from_utf8(output.stdout).unwrap();
            let stderr = String::from_utf8(output.stderr).unwrap();
            let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
            assert_eq!(report["pinning"]["validation"], "failed");
            assert!(stderr.contains("pinning configuration validation failed"));
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
                assert!(
                    !format!("{stdout}{stderr}").contains(forbidden),
                    "leaked {forbidden}"
                );
            }
            assert!(!database.exists(), "diagnostic command created database");
        }
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
    let output = run(&config, &["--pinning-doctor"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("local configuration load failed"));
    assert!(!stderr.contains("secret-error-sentinel"));
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
    let output = run(&config, &["--config-explain"], None);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("pinning configuration validation failed"));
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
    let output = run(&missing_config, &["--pinning-doctor"], None);
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

#[tokio::test]
async fn status_pages_real_sqlite_snapshot_without_writing_or_leaking_raw_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("existing.sqlite");
    let db_url = format!(
        "sqlite://{}?mode=rwc",
        db_path.display().to_string().replace('\\', "/")
    );
    let config = temp.path().join("status.toml");
    fs::write(&config, format!("[storage]\ndatabase_url = '{db_url}'\n")).unwrap();
    let db = Database::connect(db_url).await.unwrap();
    run_migrations(&db).await.unwrap();
    bucket::create(&db, "secret-bucket-sentinel", None)
        .await
        .unwrap();
    bucket::set_versioning_state(
        &db,
        "secret-bucket-sentinel",
        BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    let key = "secret-key-sentinel";
    let cid = "secret-cid-sentinel";
    object::upsert(
        &db,
        "old-owner",
        "secret-bucket-sentinel",
        key,
        cid,
        3,
        None,
        cid,
        None,
        false,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    object::upsert(
        &db,
        "owner",
        "secret-bucket-sentinel",
        key,
        cid,
        3,
        None,
        cid,
        None,
        false,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    let now = Utc::now();
    let old_version = "11111111-1111-4111-8111-111111111111";
    let current_version = "22222222-2222-4222-8222-222222222222";
    for (row_id, version_id, latest) in [
        ("version-old", old_version, false),
        ("version-current", current_version, true),
    ] {
        object_version::Entity::insert(object_version::ActiveModel {
            id: Set(row_id.into()),
            bucket: Set("secret-bucket-sentinel".into()),
            key: Set(key.into()),
            version_id: Set(Some(version_id.into())),
            kind: Set("object".into()),
            object_id: Set(Some(if latest { "owner" } else { "old-owner" }.into())),
            sequence: Set(if latest { 2 } else { 1 }),
            is_latest: Set(latest),
            lifecycle_age_started_at: Set(now),
            became_noncurrent_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&db)
        .await
        .unwrap();
    }
    let decision = ExtensionDecision {
        effect: DecisionEffect::NoIntent,
        warning: None,
        legacy_unknown: false,
        origin: DecisionOrigin::new("principal", "request"),
        control_revision: "33333333-3333-4333-8333-333333333333".into(),
        config_revision: "a".repeat(64),
        control: PinControl::Absent,
        effective_intents: Vec::new(),
    };
    pin_extension_decision::Entity::insert(pin_extension_decision::ActiveModel {
        version_row_id: Set("version-current".into()),
        object_id: Set("owner".into()),
        control_revision: Set(decision.control_revision.clone()),
        config_revision: Set(decision.config_revision.clone()),
        effect: Set("no_intent".into()),
        snapshot: Set(serde_json::to_string(&decision).unwrap()),
    })
    .exec(&db)
    .await
    .unwrap();
    pin_lease::Entity::insert(pin_lease::ActiveModel {
        id: Set("lease-secret".into()),
        owner_object_id: Set("owner".into()),
        source: Set("manual".into()),
        policy_id: Set("secret-policy-sentinel".into()),
        provider_mode: Set("all".into()),
        content_mode: Set("object".into()),
        created_at: Set(now),
        last_touched_at: Set(now),
        expires_at: Set(now),
        generation: Set(1),
        state: Set("active".into()),
    })
    .exec(&db)
    .await
    .unwrap();
    pin_lease::Entity::insert(pin_lease::ActiveModel {
        id: Set("old-lease-secret".into()),
        owner_object_id: Set("old-owner".into()),
        source: Set("manual".into()),
        policy_id: Set("secret-policy-sentinel".into()),
        provider_mode: Set("all".into()),
        content_mode: Set("object".into()),
        created_at: Set(now),
        last_touched_at: Set(now),
        expires_at: Set(now),
        generation: Set(1),
        state: Set("expired".into()),
    })
    .exec(&db)
    .await
    .unwrap();
    for index in 0..53 {
        let provider = format!("secret-provider-{index}");
        let target_id = format!("target-secret-{index:02}");
        pin_lease_target::Entity::insert(pin_lease_target::ActiveModel {
            id: Set(target_id),
            lease_id: Set("lease-secret".into()),
            cid: Set(cid.into()),
            logical_size: Set(3),
            provider: Set(provider.clone()),
            state: Set("pinned".into()),
            created_at: Set(now),
            last_touched_at: Set(now),
        })
        .exec(&db)
        .await
        .unwrap();
        remote_pin::Entity::insert(remote_pin::ActiveModel {
            provider: Set(provider.clone()),
            cid: Set(cid.into()),
            request_id: Set(Some("secret-request-sentinel".into())),
            cid_size: Set(3),
            status: Set("pinned".into()),
            epoch: Set(1),
            failure_attempts: Set(0),
            next_retry_at: Set(None),
            last_failed_request_id: Set(None),
            last_touched_at: Set(now),
            last_error_class: Set(Some("https://secret-query-sentinel".into())),
            last_error_text: Set(Some("secret-provider-body-sentinel".into())),
        })
        .exec(&db)
        .await
        .unwrap();
        remote_pin_ledger::Entity::insert(remote_pin_ledger::ActiveModel {
            provider: Set(provider),
            cid: Set(cid.into()),
            route: Set(Some("secret-route-token-sentinel".into())),
            ownership: Set("application_created".into()),
            effect: Set("confirmed".into()),
            first_observed_at: Set(Some(now)),
            last_observed_at: Set(Some(now)),
            remote_pinned_at: Set(Some(now)),
            gateway_verified_at: Set(None),
            content_verified_at: Set(None),
            first_error: Set(Some("secret-first-error-sentinel".into())),
            last_error: Set(Some("secret-last-error-sentinel".into())),
        })
        .exec(&db)
        .await
        .unwrap();
    }
    db.close().await.unwrap();
    let before = fs::read(&db_path).unwrap();
    let page1 = run(
        &config,
        &["--pinning-status", "secret-bucket-sentinel", key],
        None,
    );
    assert!(
        page1.status.success(),
        "{}",
        String::from_utf8_lossy(&page1.stderr)
    );
    let page1_json: serde_json::Value = serde_json::from_slice(&page1.stdout).unwrap();
    assert_eq!(
        page1_json["status"]["entries"].as_array().unwrap().len(),
        50
    );
    assert_eq!(page1_json["status"]["next_cursor"], "50");
    assert_eq!(page1_json["status"]["decision"]["effect"], "no_intent");
    assert_eq!(
        page1_json["status"]["entries"][0]["local"]["target_state"],
        "pinned"
    );
    assert_eq!(
        page1_json["status"]["entries"][0]["remote"]["stored_status"],
        "pinned"
    );
    assert_eq!(
        page1_json["status"]["entries"][0]["remote"]["ledger_effect"],
        "confirmed"
    );
    assert_eq!(
        page1_json["status"]["entries"][0]["remote"]["first_error_category"],
        "other_redacted"
    );
    assert!(page1_json["status"]["entries"][0]["verification"]["content_verified_at"].is_null());
    let page2 = run(
        &config,
        &[
            "--pinning-status",
            "secret-bucket-sentinel",
            key,
            "--cursor",
            "50",
        ],
        None,
    );
    assert!(
        page2.status.success(),
        "{}",
        String::from_utf8_lossy(&page2.stderr)
    );
    let page2_json: serde_json::Value = serde_json::from_slice(&page2.stdout).unwrap();
    assert_eq!(page2_json["status"]["entries"].as_array().unwrap().len(), 3);
    assert!(page2_json["status"]["next_cursor"].is_null());
    assert_eq!(page2_json["status"]["entries"][0]["entry_index"], 50);
    let old = run(
        &config,
        &[
            "--pinning-status",
            "secret-bucket-sentinel",
            key,
            "--version-id",
            old_version,
        ],
        None,
    );
    assert!(
        old.status.success(),
        "{}",
        String::from_utf8_lossy(&old.stderr)
    );
    let old_json: serde_json::Value = serde_json::from_slice(&old.stdout).unwrap();
    assert_eq!(
        old_json["status"]["decision"]["state"],
        "unknown_legacy_absent"
    );
    assert_eq!(old_json["status"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(
        old_json["status"]["entries"][0]["lease"]["state"],
        "expired"
    );
    assert_eq!(
        old_json["status"]["entries"][0]["local"]["target_state"],
        "none"
    );
    for output in [&page1, &page2, &old] {
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for secret in [
            "secret-bucket-sentinel",
            key,
            cid,
            "secret-provider-",
            "target-secret-",
            "old-lease-secret",
            "secret-policy-sentinel",
            "secret-request-sentinel",
            "secret-query-sentinel",
            "secret-provider-body-sentinel",
            "secret-first-error-sentinel",
            "secret-last-error-sentinel",
            "secret-route-token-sentinel",
            &decision.config_revision,
        ] {
            assert!(!combined.contains(secret), "leaked {secret}");
        }
    }
    assert_eq!(
        fs::read(&db_path).unwrap(),
        before,
        "status modified SQLite file"
    );
}

#[test]
fn status_missing_database_and_bad_cursor_fail_closed_without_creating_files_or_echoing_input() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("missing.sqlite");
    let config = temp.path().join("status.toml");
    let url = format!(
        "sqlite://{}?mode=rwc",
        db.display().to_string().replace('\\', "/")
    );
    fs::write(&config, format!("[storage]\ndatabase_url = '{url}'\n")).unwrap();
    let output = run(
        &config,
        &["--pinning-status", "secret-bucket", "secret-key"],
        None,
    );
    assert!(!output.status.success());
    assert!(!db.exists());
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(&url));
    let output = run(
        &config,
        &[
            "--pinning-status",
            "secret-bucket",
            "secret-key",
            "--cursor",
            "secret-query",
        ],
        None,
    );
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret-query"));
    assert!(!db.exists());
}

#[tokio::test]
async fn status_does_not_migrate_an_existing_uninitialized_database() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("uninitialized.sqlite");
    let db_url = format!(
        "sqlite://{}?mode=rwc",
        db_path.display().to_string().replace('\\', "/")
    );
    let db = Database::connect(&db_url).await.unwrap();
    db.close().await.unwrap();
    let before = fs::read(&db_path).unwrap();
    let config = temp.path().join("status.toml");
    fs::write(&config, format!("[storage]\ndatabase_url = '{db_url}'\n")).unwrap();
    let output = run(&config, &["--pinning-status", "bucket", "key"], None);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains(&db_url));
    assert!(!stderr.contains("SELECT"));
    assert!(!stderr.contains("object_versions"));
    assert_eq!(fs::read(&db_path).unwrap(), before);
}
