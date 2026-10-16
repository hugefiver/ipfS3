#[path = "support/stage7_management_cli.rs"]
mod support;

#[path = "support/stage7_management_cli_fences.rs"]
mod fences;
#[path = "support/stage7_management_cli_roundtrip.rs"]
mod roundtrip;
#[path = "support/stage7_management_cli_scoped.rs"]
mod scoped;

#[tokio::test]
async fn argument_rejection_and_help_precede_config_database_and_network() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("secret-invalid-config.toml");
    std::fs::write(&config, "invalid SECRET_CONFIG_CONTENT_DO_NOT_PRINT").unwrap();
    for args in [
        vec!["--pinning-manage"],
        vec!["--pinning-manage", "request.json", "--unknown"],
        vec!["--pinning-manage", "request.json", "--run"],
        vec![
            "--pinning-manage",
            "request.json",
            "--source-node",
            "peer",
            "--source-node",
            "peer",
        ],
        vec![
            "--pinning-manage",
            "request.json",
            "--output",
            "a.json",
            "--output",
            "b.json",
        ],
        vec![
            "--pinning-manage",
            "request.json",
            "--confirm",
            "a.json",
            "--confirm",
            "b.json",
        ],
        vec!["--pinning-manage", "request.json", "--ack-item", "object"],
        vec![
            "--pinning-manage",
            "request.json",
            "--confirm",
            "out.json",
            "--output",
            "plan.json",
        ],
        vec![
            "--pinning-manage",
            "request.json",
            "--confirm",
            "out.json",
            "--ack-item",
            "x",
            "--ack-item",
            "x",
        ],
        vec![
            "--pinning-manage",
            "request.json",
            "--confirm",
            "out.json",
            "--ttl-seconds",
            "901",
        ],
        vec![
            "--pinning-manage",
            "--apply",
            "confirmed.json",
            "--status",
            "confirmed.json",
        ],
        vec![
            "--pinning-manage",
            "--apply",
            "confirmed.json",
            "--run",
            "--run",
        ],
        vec!["--pinning-manage", "--status", "confirmed.json", "--run"],
        vec![
            "--pinning-manage",
            "--prepare",
            "prepare.json",
            "--confirm",
            "out.json",
        ],
        vec!["--pinning-manage", "request.json", "--pinning-doctor"],
        vec!["--pinning-doctor", "--pinning-manage", "request.json"],
        vec!["--pinning-policy-explain", "--pinning-manage", "key"],
        vec![
            "--pinning-status",
            "bucket",
            "key",
            "--version-id",
            "--pinning-manage",
        ],
        vec!["--pinning-migrate-legacy", "--pinning-manage"],
        vec!["--ready-probe", "--pinning-manage", "request.json"],
        vec![
            "--pinning-migrate-legacy",
            "request.json",
            "--pinning-manage",
            "request.json",
        ],
        vec!["--pinning-manage", "--help", "extra"],
    ] {
        let result = support::output(&mut support::command(&config, &args)).await;
        assert!(!result.status.success());
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(!error.contains("local configuration load failed"));
        assert!(!error.contains("SECRET_CONFIG_CONTENT_DO_NOT_PRINT"));
        assert!(!error.contains(config.to_str().unwrap()));
    }
    let help = support::output(&mut support::command(
        &config,
        &["--pinning-manage", "--help"],
    ))
    .await;
    assert!(help.status.success());
    assert!(!help.stdout.is_empty());
    assert!(help.stderr.is_empty());
    assert!(!String::from_utf8_lossy(&help.stdout).contains("SECRET_CONFIG_CONTENT_DO_NOT_PRINT"));
}

#[tokio::test]
async fn help_and_errors_never_open_a_missing_database_or_contact_remote_services() {
    let fixture = support::Fixture::new(false).await;
    let missing = fixture.file("must-not-create-on-help-or-error.sqlite");
    let invalid = fixture.file("PRIVATE_INVALID_INPUT.json");
    std::fs::write(&invalid, b"PRIVATE_INVALID_JSON_CONTENT").unwrap();
    let before = fixture.frame().await;
    for (args, success) in [
        (vec!["--pinning-manage", "--help"], true),
        (
            vec![
                "--pinning-manage",
                "--apply",
                "PRIVATE_FILE.json",
                "--unknown",
            ],
            false,
        ),
        (vec!["--pinning-manage", invalid.to_str().unwrap()], false),
    ] {
        let mut command = fixture.command(&args);
        command.env(
            "IPFS_S3_DATABASE_URL",
            format!(
                "sqlite://{}?mode=rwc",
                missing.to_string_lossy().replace('\\', "/")
            ),
        );
        let result = support::output(&mut command).await;
        assert_eq!(result.status.success(), success);
        assert!(!missing.exists());
        assert!(fixture.frame().await == before);
        assert_eq!(fixture.remote_calls().await, 0);
        fixture.assert_redacted(&result);
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        for private in [
            "PRIVATE_INVALID_INPUT",
            "PRIVATE_INVALID_JSON_CONTENT",
            "PRIVATE_FILE",
            missing.to_str().unwrap(),
        ] {
            assert!(!text.contains(private));
        }
    }
}

#[tokio::test]
async fn invalid_bounded_json_is_rejected_before_config_loading() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("invalid.toml");
    std::fs::write(&config, "invalid SECRET_CONFIGURATION").unwrap();
    let input = directory.path().join("input.json");
    let oversized = vec![b' '; ipfs_s3_gateway::pinning::management::wire::MAX_WIRE_BYTES + 1];
    let deep = format!("{}0{}", "[".repeat(66), "]".repeat(66));
    for bytes in [
        b"{".to_vec(),
        oversized,
        deep.into_bytes(),
        br#"{"plan_id":"one","plan_id":"two","items":[]}"#.to_vec(),
        br#"{"plan_id":"one","items":[],"authority":true}"#.to_vec(),
    ] {
        std::fs::write(&input, bytes).unwrap();
        for mode in [None, Some("--apply"), Some("--status"), Some("--prepare")] {
            let mut args = vec!["--pinning-manage"];
            if let Some(mode) = mode {
                args.push(mode);
            }
            args.push(input.to_str().unwrap());
            let result = support::output(&mut support::command(&config, &args)).await;
            assert!(!result.status.success());
            let error = String::from_utf8_lossy(&result.stderr);
            assert!(error.contains("management input invalid"));
            assert!(!error.contains("SECRET_CONFIGURATION"));
            assert!(!error.contains("local configuration load failed"));
        }
    }
}

#[tokio::test]
async fn missing_database_is_not_created_even_when_configured_with_mode_rwc() {
    let fixture = support::Fixture::new(false).await;
    let request = fixture.request_file();
    let missing = fixture.file("must-not-create.sqlite");
    let before = fixture.frame().await;
    let mut command = fixture.command(&["--pinning-manage", request.to_str().unwrap()]);
    command.env(
        "IPFS_S3_DATABASE_URL",
        format!(
            "sqlite://{}?mode=rwc",
            missing.to_string_lossy().replace('\\', "/")
        ),
    );
    let result = support::output(&mut command).await;
    assert!(!result.status.success());
    assert!(!missing.exists());
    assert!(fixture.frame().await == before);
    assert_eq!(fixture.remote_calls().await, 0);
    fixture.assert_redacted(&result);
}

#[tokio::test]
async fn dry_run_and_public_export_leave_database_bytes_and_native_frame_unchanged() {
    let fixture = support::Fixture::new(false).await;
    assert!(!fixture.unrelated_queue().await.is_empty());
    let request = fixture.request_file();
    let original = fixture.frame().await;
    fixture.db.clone().close().await.unwrap();
    let before = std::fs::read(&fixture.database).unwrap();
    let default =
        support::output(&mut fixture.command(&["--pinning-manage", request.to_str().unwrap()]))
            .await;
    let default_report = support::report(&default);
    assert_eq!(default_report["mode"], "dry_run");
    assert_eq!(default_report["artifact_exported"], false);
    assert_eq!(default_report["database_writes"], false);
    assert_eq!(std::fs::read(&fixture.database).unwrap(), before);
    fixture.assert_redacted(&default);
    let artifact = fixture.file("public-plan.json");
    let result = support::output(&mut fixture.command(&[
        "--pinning-manage",
        request.to_str().unwrap(),
        "--output",
        artifact.to_str().unwrap(),
    ]))
    .await;
    let report = support::report(&result);
    assert_eq!(report["mode"], "dry_run");
    assert_eq!(report["database_writes"], false);
    assert_eq!(report["source_rpc"], "not_performed");
    assert_eq!(report["items"][0]["status"]["blocked"], "unreadable_object");
    assert_eq!(std::fs::read(&fixture.database).unwrap(), before);
    let mut options = sea_orm::ConnectOptions::new(format!(
        "sqlite://{}",
        fixture.database.to_string_lossy().replace('\\', "/")
    ));
    options.map_sqlx_sqlite_opts(|options| options.read_only(true).create_if_missing(false));
    let db = sea_orm::Database::connect(options).await.unwrap();
    assert!(
        ipfs_s3_gateway::store::pinning::management::native::capture(&db)
            .await
            .unwrap()
            == original
    );
    db.close().await.unwrap();
    assert_eq!(fixture.remote_calls().await, 0);
    fixture.assert_redacted(&result);
    let wire = std::fs::read(&artifact).unwrap();
    assert!(ipfs_s3_gateway::pinning::management::wire::decode_plan(&wire).is_ok());
    assert!(!String::from_utf8_lossy(&wire).contains(support::PRIVATE_METADATA));
    assert!(!String::from_utf8_lossy(&wire).contains(&fixture.server.uri()));
    assert!(serde_json::from_slice::<serde_json::Value>(&wire).unwrap()["confirmation"].is_null());

    let result = support::output(&mut fixture.command(&[
        "--pinning-manage",
        request.to_str().unwrap(),
        "--output",
        artifact.to_str().unwrap(),
    ]))
    .await;
    assert!(!result.status.success());
    assert_eq!(std::fs::read(&artifact).unwrap(), wire);
    assert_eq!(std::fs::read(&fixture.database).unwrap(), before);
    assert_eq!(fixture.remote_calls().await, 0);
    fixture.assert_redacted(&result);
}
