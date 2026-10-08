use std::{fs, io::ErrorKind, net::TcpListener, path::Path, process::Command};

use sea_orm::{ConnectionTrait, Database};
use serde_json::{Value, json};

#[path = "support/legacy_pinning_migration_cli.rs"]
mod fixture;

const SECRET: &str = "legacy-migration-secret-sentinel";

fn command(config: &Path, declaration: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"));
    command
        .arg("--pinning-migrate-legacy")
        .arg(declaration)
        .env_clear()
        .env("IPFS_S3_CONFIG", config);
    command
}

#[test]
fn malformed_declaration_is_rejected_without_creating_a_database_or_remote_io() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let database = directory.path().join("must-not-exist.sqlite");
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database.display().to_string().replace('\\', "/")
    );
    let config = directory.path().join("gateway.toml");
    fs::write(
        &config,
        format!(
            "[storage]\ndatabase_url = '{database_url}'\n[kubo]\nrpc_url = '{endpoint}'\n[auth]\n[[auth.credentials]]\naccess_key = 'test'\nsecret_key = '{SECRET}'\n"
        ),
    )
    .unwrap();
    let declaration = directory.path().join("declaration.json");
    fs::write(
        &declaration,
        format!("{{\"unexpected\":\"https://example.invalid/?token={SECRET}\""),
    )
    .unwrap();

    let output = command(&config, &declaration).output().unwrap();
    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        !stderr.contains("usage: ipfs-s3-gateway"),
        "the valid migration command must reach declaration validation: {stderr}"
    );
    for forbidden in [SECRET, database_url.as_str(), "https://example.invalid/"] {
        assert!(!stdout.contains(forbidden));
        assert!(!stderr.contains(forbidden));
    }
    assert!(!database.exists());
    assert_eq!(listener.accept().unwrap_err().kind(), ErrorKind::WouldBlock);
}

#[test]
fn missing_declaration_does_not_fall_back_to_gateway_startup() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("must-not-exist.sqlite");
    let config = directory.path().join("gateway.toml");
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        database.display().to_string().replace('\\', "/")
    );
    fs::write(
        &config,
        format!("[storage]\ndatabase_url = '{database_url}'\n"),
    )
    .unwrap();
    let output = command(&config, &directory.path().join("missing.json"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains("usage: ipfs-s3-gateway"),
        "the migration command must report a missing declaration, not be unknown"
    );
    assert!(!database.exists());
    assert!(
        fs::read_dir(directory.path())
            .unwrap()
            .all(|entry| { entry.unwrap().path() == config })
    );
}

#[tokio::test]
async fn dry_run_exports_untrusted_confirmation_without_writing_the_database_or_loading_providers()
{
    let fixture = fixture::Fixture::new().await;
    let before = fs::read(&fixture.database).unwrap();
    let output = fixture.migrate().output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["mode"], "dry_run");
    assert_eq!(report["evidence"], "untrusted_proposal_not_receipt");
    assert_eq!(report["confirmation_exported"], true);
    assert!(fixture.confirmation.exists());
    let proposal: Value =
        serde_json::from_slice(&fs::read(&fixture.confirmation).unwrap()).unwrap();
    assert!(proposal["before"].is_array());
    assert!(proposal["after"].is_array());
    assert_eq!(fs::read(&fixture.database).unwrap(), before);
    fixture.assert_no_remote_io();

    let repeat = fixture.migrate().output().unwrap();
    assert!(
        !repeat.status.success(),
        "must not overwrite a private evidence file"
    );
    assert_eq!(fs::read(&fixture.database).unwrap(), before);
    fixture.assert_no_remote_io();
}

fn successful_report(output: std::process::Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn apply_and_rollback_replay_use_durable_receipts_across_cli_processes() {
    let fixture = fixture::Fixture::new().await;
    successful_report(fixture.migrate().output().unwrap());
    let apply = || {
        fixture
            .migrate()
            .arg("--apply")
            .arg(&fixture.confirmation)
            .output()
            .unwrap()
    };
    let receipt = successful_report(apply());
    assert_eq!(receipt["direction"], "apply");
    assert_eq!(successful_report(apply()), receipt);
    fixture.assert_usage("legacy-cli", None).await;
    fixture.assert_usage(&fixture.target, Some((97, 3))).await;

    let forged = fixture.directory.path().join("forged-receipt.json");
    let mut forged_receipt = receipt.clone();
    forged_receipt["id"] = json!(uuid::Uuid::new_v4().to_string());
    fs::write(&forged, serde_json::to_vec(&forged_receipt).unwrap()).unwrap();
    let before_forgery = fs::read(&fixture.database).unwrap();
    assert!(
        !fixture
            .rollback(forged.to_str().unwrap())
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(fs::read(&fixture.database).unwrap(), before_forgery);

    let rollback = successful_report(
        fixture
            .rollback(receipt["id"].as_str().unwrap())
            .output()
            .unwrap(),
    );
    assert_eq!(rollback["direction"], "rollback");
    assert_eq!(rollback["reverses"], receipt["id"]);
    assert_eq!(
        successful_report(
            fixture
                .rollback(receipt["id"].as_str().unwrap())
                .output()
                .unwrap()
        ),
        rollback
    );
    assert!(
        !apply().status.success(),
        "an undone apply must not silently execute again"
    );
    fixture.assert_usage("legacy-cli", Some((97, 3))).await;
    fixture.assert_usage(&fixture.target, None).await;
    fixture.assert_no_remote_io();
}

#[tokio::test]
async fn missing_database_is_not_created_and_missing_receipt_schema_is_not_migrated() {
    let missing = fixture::Fixture::new().await;
    fs::rename(
        &missing.database,
        missing.directory.path().join("saved.sqlite"),
    )
    .unwrap();
    assert!(!missing.migrate().output().unwrap().status.success());
    assert!(!missing.database.exists());
    assert!(!missing.confirmation.exists());
    missing.assert_no_remote_io();

    let old_schema = fixture::Fixture::new().await;
    let url = format!(
        "sqlite://{}?mode=rw",
        old_schema.database.display().to_string().replace('\\', "/")
    );
    let db = Database::connect(url).await.unwrap();
    db.execute_unprepared("DROP TABLE pin_legacy_migration_items")
        .await
        .unwrap();
    db.close().await.unwrap();
    let before = fs::read(&old_schema.database).unwrap();
    assert!(!old_schema.migrate().output().unwrap().status.success());
    assert_eq!(fs::read(&old_schema.database).unwrap(), before);
    assert!(!old_schema.confirmation.exists());
    old_schema.assert_no_remote_io();
}

#[tokio::test]
async fn undrained_apply_and_oversized_input_are_refused_before_database_writes() {
    let fixture = fixture::Fixture::new().await;
    let before = fs::read(&fixture.database).unwrap();
    let mut declaration: Value =
        serde_json::from_slice(&fs::read(&fixture.declaration).unwrap()).unwrap();
    declaration["maintenance"]["all_workers_drained"] = json!(false);
    fs::write(
        &fixture.declaration,
        serde_json::to_vec(&declaration).unwrap(),
    )
    .unwrap();
    successful_report(fixture.migrate().output().unwrap());
    assert!(
        !fixture
            .migrate()
            .arg("--apply")
            .arg(&fixture.confirmation)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(fs::read(&fixture.database).unwrap(), before);
    let proposal = fs::read(&fixture.confirmation).unwrap();

    let oversized = vec![b' '; ipfs_s3_gateway::store::pinning::legacy_migration::MAX_BYTES + 1];
    fs::write(&fixture.declaration, oversized).unwrap();
    assert!(!fixture.migrate().output().unwrap().status.success());
    assert_eq!(fs::read(&fixture.database).unwrap(), before);
    assert_eq!(fs::read(&fixture.confirmation).unwrap(), proposal);
    fixture.assert_no_remote_io();
}

#[tokio::test]
async fn fresh_rollback_preview_is_read_only_and_uses_the_committed_receipt() {
    let fixture = fixture::Fixture::new().await;
    successful_report(fixture.migrate().output().unwrap());
    let receipt = successful_report(
        fixture
            .migrate()
            .arg("--apply")
            .arg(&fixture.confirmation)
            .output()
            .unwrap(),
    );
    let rollback_declaration = fixture.directory.path().join("rollback-declaration.json");
    let rollback_confirmation = fixture
        .directory
        .path()
        .join("private-rollback-confirmation.json");
    let mut declaration: Value =
        serde_json::from_slice(&fs::read(&fixture.declaration).unwrap()).unwrap();
    declaration["rollback_receipt"] = receipt["id"].clone();
    declaration["confirmation_file"] = json!(rollback_confirmation);
    fs::write(
        &rollback_declaration,
        serde_json::to_vec(&declaration).unwrap(),
    )
    .unwrap();
    let before = fs::read(&fixture.database).unwrap();
    successful_report(
        command(&fixture.config, &rollback_declaration)
            .output()
            .unwrap(),
    );
    assert_eq!(fs::read(&fixture.database).unwrap(), before);
    let rollback = successful_report(
        Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"))
            .arg("--rollback")
            .arg(receipt["id"].as_str().unwrap())
            .arg(&rollback_confirmation)
            .env_clear()
            .env("IPFS_S3_CONFIG", &fixture.config)
            .output()
            .unwrap(),
    );
    assert_eq!(rollback["reverses"], receipt["id"]);
    fixture.assert_usage("legacy-cli", Some((97, 3))).await;
    fixture.assert_usage(&fixture.target, None).await;
    fixture.assert_no_remote_io();
}
