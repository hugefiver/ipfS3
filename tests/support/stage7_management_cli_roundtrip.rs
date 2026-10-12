use super::support;
use ipfs_s3_gateway::store::{entities::{pin_job, pin_lease}, pinning::management::records};
use sea_orm::{ConnectionTrait, EntityTrait};
use serde_json::json;

#[path = "stage7_management_cli_directory.rs"]
mod directory;

#[tokio::test]
async fn explicit_offline_prepare_confirm_export_apply_restart_and_exact_replay_use_the_same_database() {
    let fixture = support::Fixture::new(false).await;
    let before_proof = fixture.frame().await;
    let prepared = fixture.prepare().await;
    assert_eq!(prepared["stored_bytes"], 10);
    assert_eq!(prepared["proof"], "file_clean_eof");
    assert!(fixture.frame().await != before_proof);
    let calls = fixture.remote_calls().await;
    let confirmed = fixture.file("confirmed.json");
    let confirmation = fixture.confirm(&confirmed, "300", false).await;
    assert_eq!(support::report(&confirmation)["mode"], "confirm");
    fixture.assert_redacted(&confirmation);
    let bytes = std::fs::read(&confirmed).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains(support::PRIVATE_METADATA));
    assert!(!String::from_utf8_lossy(&bytes).contains(&fixture.server.uri()));
    let imported = ipfs_s3_gateway::pinning::management::wire::decode_plan(&bytes).unwrap();
    assert!(imported.confirmation().is_some());
    assert!(!imported.snapshot_without_applied().native_frame.as_ref().unwrap().has_private_material());
    let result = fixture.import("--apply", &confirmed, false).await;
    let applied = support::report(&result);
    assert_eq!(applied["replayed"], false);
    assert_eq!(applied["run_requested"], false);
    assert_eq!(applied["items"][0]["receipt"], "verified_original_database_receipt");
    assert_eq!(pin_lease::Entity::find().all(&fixture.db).await.unwrap().len(), 1);
    assert!(!pin_job::Entity::find().all(&fixture.db).await.unwrap().is_empty());
    let after_apply = fixture.frame().await;
    // Each import starts the actual binary again, rather than reusing an in-process binding.
    let replay = fixture.import("--apply", &confirmed, false).await;
    assert_eq!(support::report(&replay)["replayed"], true);
    assert_eq!(support::report(&replay)["items"], applied["items"]);
    assert!(fixture.frame().await == after_apply);
    let status = fixture.import("--status", &confirmed, false).await;
    assert_eq!(support::report(&status)["items"][0]["state"], "queued");
    assert!(fixture.frame().await == after_apply);
    assert_eq!(fixture.remote_calls().await, calls);
    for result in [&result, &replay, &status] { fixture.assert_redacted(result); }
}

#[tokio::test]
async fn candidate_requires_exact_per_item_ack_and_artifact_never_overwrites_or_creates_parents() {
    let fixture = support::Fixture::new(true).await;
    fixture.prepare().await;
    let before = fixture.frame().await;
    let confirmed = fixture.file("candidate.json");
    let missing_ack = fixture.confirm(&confirmed, "300", false).await;
    assert!(!missing_ack.status.success());
    assert!(!confirmed.exists());
    assert!(fixture.frame().await == before);
    let absent_parent = fixture.file("absent-parent").join("confirmation.json");
    let result = fixture.confirm(&absent_parent, "300", true).await;
    assert!(!result.status.success());
    assert!(!absent_parent.parent().unwrap().exists());
    assert!(fixture.frame().await == before);
    std::fs::write(&confirmed, b"KEEP_EXISTING_FILE").unwrap();
    let result = fixture.confirm(&confirmed, "300", true).await;
    assert!(!result.status.success());
    assert_eq!(std::fs::read(&confirmed).unwrap(), b"KEEP_EXISTING_FILE");
    assert!(fixture.frame().await == before);
    let fresh = fixture.file("acknowledged.json");
    let result = fixture.confirm(&fresh, "300", true).await;
    assert_eq!(support::report(&result)["items"][0]["status"], "candidate");
    let imported = ipfs_s3_gateway::pinning::management::wire::decode_plan(&std::fs::read(&fresh).unwrap()).unwrap();
    assert_eq!(imported.confirmation().unwrap().candidate_item_ids, vec!["object"]);
    assert!(fixture.import("--apply", &fresh, false).await.status.success());
}

#[tokio::test]
async fn unapplied_expired_confirmation_rejects_business_but_committed_expired_receipts_remain_readable() {
    let unapplied = support::Fixture::new(false).await;
    unapplied.prepare().await;
    let file = unapplied.file("expires-without-apply.json");
    support::report(&unapplied.confirm(&file, "1", false).await);
    unapplied.wait_for_confirmation_expiry().await;
    let before = unapplied.frame().await;
    let result = unapplied.import("--apply", &file, false).await;
    assert!(!result.status.success());
    let stopped: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(stopped["state_evidence"], "typed_apply_failure");
    assert_eq!(stopped["business_commit"], "no_successful_prefix_reported");
    assert_eq!(stopped["items"][0]["item_id"], "object");
    assert_eq!(stopped["items"][0]["state"], "stopped");
    assert_eq!(stopped["run_started"], false);
    assert!(unapplied.frame().await == before);
    let status = unapplied.import("--status", &file, false).await;
    assert_eq!(support::report(&status)["items"][0]["state"], "not_applied");
    assert!(unapplied.frame().await == before);

    let applied = support::Fixture::new(false).await;
    applied.prepare().await;
    let file = applied.file("expires-after-apply.json");
    support::report(&applied.confirm(&file, "15", false).await);
    support::report(&applied.import("--apply", &file, false).await);
    applied.wait_for_confirmation_expiry().await;
    let before = applied.frame().await;
    let replay = applied.import("--apply", &file, false).await;
    assert_eq!(support::report(&replay)["replayed"], true);
    let status = applied.import("--status", &file, false).await;
    assert_eq!(support::report(&status)["items"][0]["receipt"], "verified_original_database_receipt");
    assert!(applied.frame().await == before);
}

#[tokio::test]
async fn exported_proposal_and_a_staged_only_database_record_are_not_confirmation_grants() {
    let fixture = support::Fixture::new(false).await;
    fixture.prepare().await;
    let request = fixture.request_file();
    let proposal = fixture.file("proposal.json");
    let result = support::output(&mut fixture.command(&["--pinning-manage", request.to_str().unwrap(),
        "--source-node", support::NODE, "--output", proposal.to_str().unwrap()])).await;
    support::report(&result);
    let mut document: serde_json::Value = serde_json::from_slice(&std::fs::read(&proposal).unwrap()).unwrap();
    document["confirmation"] = json!({
        "plan_id": fixture.request.plan_id, "bindings": document["plan"]["items"], "candidate_item_ids": [],
        "native_frame": document["snapshot"]["native_frame"], "configuration": document["snapshot"]["configuration"],
    });
    let forged = fixture.file("reviewed-but-not-confirmed.json");
    support::write_json(&forged, &document);
    let snapshot = ipfs_s3_gateway::pinning::management::db_snapshot::collect_with_config(&fixture.db, &fixture.config, &fixture.request)
        .await.unwrap().planning_snapshot().unwrap();
    let plan = ipfs_s3_gateway::pinning::management::dry_run(&fixture.request, &snapshot).unwrap();
    let raw: ipfs_s3_gateway::config::Config = toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    records::stage_confirmation(&fixture.db, &fixture.config, &auth, &fixture.request, &snapshot, &plan, chrono::Duration::minutes(5)).await.unwrap();
    let before = fixture.frame().await;
    for mode in ["--apply", "--status"] {
        let result = fixture.import(mode, &forged, false).await;
        assert!(!result.status.success());
        assert!(fixture.frame().await == before);
    }
}

#[tokio::test]
async fn directory_locality_without_a_complete_typed_export_cannot_attest_or_grant_work() {
    for export in directory::rejected_exports() {
        let mut fixture = support::Fixture::new(false).await;
        fixture.publish_directory(&export.cid).await;
        directory::mount_source(&fixture, &export).await;
        let prepare = directory::prepare_file(&fixture, export.payload_bytes);
        let before = fixture.frame().await;
        let result = support::output(&mut fixture.command(&["--pinning-manage", "--prepare", prepare.to_str().unwrap()])).await;
        assert_eq!(result.status.code(), Some(1), "{}", export.name);
        assert!(result.stdout.is_empty(), "{}", export.name);
        assert_eq!(String::from_utf8_lossy(&result.stderr).trim(), "Error: management native evidence invalid", "{}", export.name);
        assert!(fixture.frame().await == before, "failed directory proof changed trusted evidence, leases, jobs or quota: {}", export.name);
        let request = fixture.request_file();
        let result = support::output(&mut fixture.command(&["--pinning-manage", request.to_str().unwrap(), "--source-node", support::NODE])).await;
        assert_eq!(support::report(&result)["items"][0]["status"]["blocked"], "unreadable_object", "{}", export.name);
        assert!(fixture.frame().await == before, "{}", export.name);
        directory::assert_readonly_proof_requests(&fixture, 1, 1).await;
    }
}

#[tokio::test]
async fn explicit_preparation_enforces_real_source_identity_bytes_and_redacted_provider_errors() {
    use wiremock::{Mock, ResponseTemplate, matchers::{method, path}};
    let fixture = support::Fixture::new(false).await;
    let input = fixture.prepare_file(false);
    let original: serde_json::Value = serde_json::from_slice(&std::fs::read(&input).unwrap()).unwrap();
    let before = fixture.frame().await;
    for (field, value) in [("max_stored_bytes", json!(9)), ("expected_node", json!("different-local-peer"))] {
        let mut request = original.clone();
        request[field] = value;
        support::write_json(&input, &request);
        let result = support::output(&mut fixture.command(&["--pinning-manage", "--prepare", input.to_str().unwrap()])).await;
        assert!(!result.status.success());
        assert!(fixture.frame().await == before);
        fixture.assert_redacted(&result);
    }
    support::write_json(&input, &original);
    Mock::given(method("POST")).and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(500).set_body_string("ORIGINAL_PROVIDER_ERROR_SECRET_DO_NOT_PRINT"))
        .with_priority(1).mount(&fixture.server).await;
    let result = support::output(&mut fixture.command(&["--pinning-manage", "--prepare", input.to_str().unwrap()])).await;
    assert!(!result.status.success());
    assert!(fixture.frame().await == before);
    assert!(!String::from_utf8_lossy(&result.stderr).contains("ORIGINAL_PROVIDER_ERROR_SECRET_DO_NOT_PRINT"));
    fixture.assert_redacted(&result);
}
