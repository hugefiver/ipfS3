#[path = "support/stage7_management_cli.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::pinning::management::{
    Action, ItemRequest, PlanRequest, RetirementSelection, RouteIdentity, Subject, db_snapshot,
    local_config,
    retirement::{
        ConfirmationProgress, InventorySpool, PlanningCheckpoint, PlanningProgress, RunLimits,
        TaskWork,
    },
};
use ipfs_s3_gateway::store::pinning::management::records;
use sea_orm::{ConnectionTrait, Statement};
use std::{path::Path, process::Output};

async fn fixture() -> support::Fixture {
    let mut fixture = support::Fixture::new(false).await;
    fixture.request = PlanRequest {
        plan_id: "checkpoint-cli".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture.config.pinning.providers[0].route_snapshot(),
                ),
            },
            action: Action::RetireWith {
                selection: RetirementSelection::Retain,
            },
        }],
    };
    fixture
}

async fn run(
    fixture: &support::Fixture,
    option: &str,
    checkpoint: &Path,
    pages: &str,
    extra: &[&str],
) -> Output {
    let request = fixture.request_file();
    let mut args = vec![
        "--pinning-manage",
        request.to_str().unwrap(),
        "--source-node",
        support::NODE,
        option,
        checkpoint.to_str().unwrap(),
        "--scan-pages",
        pages,
    ];
    args.extend_from_slice(extra);
    support::output(&mut fixture.command(&args)).await
}

fn incomplete(output: &Output, checkpoint: &Path) {
    let report = support::report(output);
    assert_eq!(report["outcome"], "incomplete");
    assert_eq!(report["database_writes"], false);
    assert_eq!(report["artifact_exported"], false);
    assert_eq!(
        Path::new(report["checkpoint"].as_str().unwrap()),
        checkpoint.canonicalize().unwrap()
    );
    assert!(report.get("summary").is_none());
    assert!(report.get("items").is_none());
    assert!(report.get("interrupted").is_none());
    assert!(checkpoint.is_dir());
}

fn current_config(fixture: &support::Fixture) -> local_config::ManagementConfig {
    let raw = toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(support::NODE.into())
        .unwrap()
}

async fn source_work(checkpoint: &Path) -> TaskWork {
    let spool = InventorySpool::resume(&checkpoint.join("source-000.sqlite"))
        .await
        .unwrap();
    let work = spool.task_work().await.unwrap();
    spool.close().await.unwrap();
    work
}

#[tokio::test]
async fn checkpoint_exits_incomplete_then_resumes_into_complete_review_without_business_writes() {
    let fixture = fixture().await;
    let before = fixture.frame().await;
    let checkpoint = fixture.file("planning");
    let review = fixture.file("review.v3");
    let extra = ["--output", review.to_str().unwrap()];
    incomplete(
        &run(&fixture, "--checkpoint", &checkpoint, "1", &extra).await,
        &checkpoint,
    );
    assert!(!review.exists());
    assert!(fixture.frame().await == before);
    let frozen = PlanningCheckpoint::input(&checkpoint).await.unwrap();
    let occupied = fixture.file("operator-review.v3");
    std::fs::write(&occupied, b"operator output").unwrap();
    incomplete(
        &run(
            &fixture,
            "--resume",
            &checkpoint,
            "512",
            &["--output", occupied.to_str().unwrap()],
        )
        .await,
        &checkpoint,
    );
    assert_eq!(std::fs::read(&occupied).unwrap(), b"operator output");
    assert!(PlanningCheckpoint::input(&checkpoint).await.is_ok());
    assert!(fixture.frame().await == before);
    let capture =
        db_snapshot::collect_with_config(&fixture.db, &current_config(&fixture), &fixture.request)
            .await
            .unwrap();
    let capture_work = capture.work().unwrap();
    assert!(capture_work.rows > 0 && capture_work.raw_bytes > 0);
    let before_retry = source_work(&checkpoint).await;
    assert!(
        !run(
            &fixture,
            "--resume",
            &checkpoint,
            "512",
            &["--output", occupied.to_str().unwrap()]
        )
        .await
        .status
        .success()
    );
    let after_retry = source_work(&checkpoint).await;
    assert!(after_retry.rows - before_retry.rows >= capture_work.rows);
    assert!(after_retry.bytes - before_retry.bytes >= capture_work.raw_bytes);
    let completed = support::report(&run(&fixture, "--resume", &checkpoint, "512", &extra).await);
    assert_eq!(completed["mode"], "dry_run");
    assert_eq!(completed["outcome"], "complete");
    assert_eq!(completed["database_writes"], false);
    let document = support::document(&review);
    assert!(document.request.as_ref() == Some(&fixture.request));
    assert!(document.expires_at.is_none());
    assert!(
        chrono::DateTime::parse_from_rfc3339(&document.created_at).unwrap() <= frozen.observed_at
    );
    assert!(!checkpoint.exists());
    assert!(fixture.frame().await == before);
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn newly_sealed_review_waits_for_next_process_before_exclusive_delivery() {
    let fixture = fixture().await;
    let before = fixture.frame().await;
    let storage = confirmation_storage(&fixture).await;
    let checkpoint = fixture.file("sealed-review-checkpoint");
    let review = fixture.file("sealed-review.v3");
    let extra = ["--output", review.to_str().unwrap()];
    let prepared = run(&fixture, "--checkpoint", &checkpoint, "512", &extra).await;
    incomplete(&prepared, &checkpoint);
    assert_eq!(support::report(&prepared)["phase"], "output");
    assert!(!review.exists());
    assert!(fixture.frame().await == before);
    assert_eq!(confirmation_storage(&fixture).await, storage);
    let delivered = run(&fixture, "--resume", &checkpoint, "512", &extra).await;
    assert_eq!(support::report(&delivered)["outcome"], "complete");
    assert!(support::document(&review).request.as_ref() == Some(&fixture.request));
    assert!(!checkpoint.exists());
    assert!(fixture.frame().await == before);
    assert_eq!(confirmation_storage(&fixture).await, storage);
}

#[tokio::test]
async fn changed_source_rejects_resume_and_preserves_the_operator_checkpoint() {
    let fixture = fixture().await;
    let checkpoint = fixture.file("planning");
    incomplete(
        &run(&fixture, "--checkpoint", &checkpoint, "1", &[]).await,
        &checkpoint,
    );
    fixture.replace_object().await;
    let before = fixture.frame().await;
    let review = fixture.file("review.v3");
    let rejected = run(
        &fixture,
        "--resume",
        &checkpoint,
        "512",
        &["--output", review.to_str().unwrap()],
    )
    .await;
    assert!(!rejected.status.success());
    assert!(!review.exists());
    assert!(PlanningCheckpoint::input(&checkpoint).await.is_ok());
    assert!(fixture.frame().await == before);
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn review_output_preparation_resumes_across_cli_processes_without_target_writes() {
    let fixture = fixture().await;
    for index in 0..300 {
        let mut identity = fixture.config.pinning.providers[0].identity.clone();
        identity.provider_id = format!("foreign-output-{index}");
        identity.scope = format!("foreign-output-account-{index}");
        ipfs_s3_gateway::store::pinning::ledger::register_route(
            &fixture.db,
            &identity.allocation_key(),
            &identity,
        )
        .await
        .unwrap();
    }
    let before = fixture.frame().await;
    let storage_before = confirmation_storage(&fixture).await;
    let local = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &fixture.request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let raw = toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let checkpoint = fixture.file("output-pages");
    let mut task = PlanningCheckpoint::create(
        &checkpoint,
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &local,
        &RunLimits::default(),
    )
    .await
    .unwrap();
    loop {
        let mut limits = RunLimits::default();
        limits.pages = 512;
        if let PlanningProgress::Complete(prepared) =
            task.advance(&fixture.db, &limits).await.unwrap()
        {
            prepared.close().await.unwrap();
            break;
        }
    }
    let artifact = fixture.file("paged-review.v3");
    let extra = ["--output", artifact.to_str().unwrap()];
    let mut previous = source_work(&checkpoint).await;
    for _ in 0..2 {
        let output = run(&fixture, "--resume", &checkpoint, "1", &extra).await;
        incomplete(&output, &checkpoint);
        assert_eq!(support::report(&output)["phase"], "output");
        let accepted = source_work(&checkpoint).await;
        assert!(accepted.bytes > previous.bytes);
        previous = accepted;
        assert!(!artifact.exists());
        assert!(fixture.frame().await == before);
        assert_eq!(confirmation_storage(&fixture).await, storage_before);
    }
    let sealed = run(&fixture, "--resume", &checkpoint, "512", &extra).await;
    incomplete(&sealed, &checkpoint);
    assert_eq!(support::report(&sealed)["phase"], "output");
    assert!(!artifact.exists());
    let complete = support::report(&run(&fixture, "--resume", &checkpoint, "512", &extra).await);
    assert_eq!(complete["outcome"], "complete");
    assert_eq!(complete["database_writes"], false);
    let document = support::document(&artifact);
    assert!(document.request.as_ref() == Some(&fixture.request));
    assert!(document.expires_at.is_none());
    assert!(fixture.frame().await == before);
    assert_eq!(confirmation_storage(&fixture).await, storage_before);
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn resume_requires_current_principal_configuration_and_exact_request() {
    let mut fixture = fixture().await;
    let checkpoint = fixture.file("planning");
    incomplete(
        &run(&fixture, "--checkpoint", &checkpoint, "1", &[]).await,
        &checkpoint,
    );
    let before = fixture.frame().await;
    let original = std::fs::read_to_string(&fixture.config_path).unwrap();
    let raw = toml::from_str(&original).unwrap();
    let config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(support::NODE.into())
        .unwrap();
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &config, &fixture.request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let other_principal = fixture.file("other-principal");
    PlanningCheckpoint::create(
        &other_principal,
        &fixture.db,
        "different-admin",
        &fixture.request,
        &snapshot,
        &RunLimits::default(),
    )
    .await
    .unwrap()
    .close()
    .await
    .unwrap();
    let other_work = source_work(&other_principal).await;
    assert!(
        !run(&fixture, "--resume", &other_principal, "512", &[])
            .await
            .status
            .success()
    );
    assert_eq!(source_work(&other_principal).await, other_work);
    assert!(PlanningCheckpoint::input(&other_principal).await.is_ok());
    let bound_work = source_work(&checkpoint).await;
    let mut config: toml::Value = toml::from_str(&original).unwrap();
    config["pinning"]["providers"][0]["max_pins"] = 1001.into();
    std::fs::write(&fixture.config_path, toml::to_string(&config).unwrap()).unwrap();
    assert!(
        !run(&fixture, "--resume", &checkpoint, "512", &[])
            .await
            .status
            .success()
    );
    std::fs::write(&fixture.config_path, original).unwrap();
    fixture.request.plan_id = "different-request".into();
    assert!(
        !run(&fixture, "--resume", &checkpoint, "512", &[])
            .await
            .status
            .success()
    );
    assert!(PlanningCheckpoint::input(&checkpoint).await.is_ok());
    assert_eq!(source_work(&checkpoint).await, bound_work);
    assert!(fixture.frame().await == before);
}

#[tokio::test]
async fn checkpoint_paths_and_scan_options_fail_closed_without_creating_parents() {
    let fixture = fixture().await;
    let before = fixture.frame().await;
    let existing = fixture.file("existing");
    std::fs::create_dir(&existing).unwrap();
    let sentinel = existing.join("operator-data");
    std::fs::write(&sentinel, b"preserve").unwrap();
    assert!(
        !run(&fixture, "--checkpoint", &existing, "1", &[])
            .await
            .status
            .success()
    );
    assert!(
        !run(&fixture, "--resume", &existing, "1", &[])
            .await
            .status
            .success()
    );
    assert_eq!(std::fs::read(&sentinel).unwrap(), b"preserve");
    let missing = fixture.file("missing/planning");
    assert!(
        !run(&fixture, "--checkpoint", &missing, "1", &[])
            .await
            .status
            .success()
    );
    assert!(!missing.parent().unwrap().exists());
    let checkpoint = fixture.file("planning");
    for pages in ["0", "513", "-1"] {
        assert!(
            !run(&fixture, "--checkpoint", &checkpoint, pages, &[])
                .await
                .status
                .success()
        );
        assert!(!checkpoint.exists());
    }
    assert!(
        !run(
            &fixture,
            "--checkpoint",
            &checkpoint,
            "1",
            &["--resume", existing.to_str().unwrap()]
        )
        .await
        .status
        .success()
    );
    assert!(fixture.frame().await == before);
    let non_provider = support::Fixture::new(false).await;
    assert!(
        !run(
            &non_provider,
            "--checkpoint",
            &non_provider.file("planning"),
            "1",
            &[]
        )
        .await
        .status
        .success()
    );
    assert!(!non_provider.file("planning").exists());
}

#[tokio::test]
async fn confirmation_waits_for_completion_and_reserves_output_before_final_publication() {
    let fixture = fixture().await;
    let before = fixture.frame().await;
    let checkpoint = fixture.file("planning");
    let confirmed = fixture.file("confirmed.v3");
    let extra = [
        "--confirm",
        confirmed.to_str().unwrap(),
        "--ttl-seconds",
        "30",
    ];
    incomplete(
        &run(&fixture, "--checkpoint", &checkpoint, "1", &extra).await,
        &checkpoint,
    );
    assert!(!confirmed.exists());
    assert!(fixture.frame().await == before);
    std::fs::write(&confirmed, b"operator output").unwrap();
    assert!(
        !run(&fixture, "--resume", &checkpoint, "512", &extra)
            .await
            .status
            .success()
    );
    assert_eq!(std::fs::read(&confirmed).unwrap(), b"operator output");
    assert!(fixture.frame().await == before);
    let checkpoint = fixture.file("missing-output-plan");
    let missing = fixture.file("missing/confirmed.v3");
    assert!(
        !run(
            &fixture,
            "--checkpoint",
            &checkpoint,
            "512",
            &["--confirm", missing.to_str().unwrap()]
        )
        .await
        .status
        .success()
    );
    assert!(!missing.parent().unwrap().exists());
    assert!(fixture.frame().await == before);
    let checkpoint = fixture.file("ack-plan");
    let rejected = fixture.file("bad-ack.v3");
    assert!(
        !run(
            &fixture,
            "--checkpoint",
            &checkpoint,
            "512",
            &[
                "--confirm",
                rejected.to_str().unwrap(),
                "--ack-item",
                "provider"
            ]
        )
        .await
        .status
        .success()
    );
    assert!(!rejected.exists());
    assert!(fixture.frame().await == before);
    let checkpoint = fixture.file("final-plan");
    let confirmed = fixture.file("final.v3");
    let extra = [
        "--confirm",
        confirmed.to_str().unwrap(),
        "--ttl-seconds",
        "30",
    ];
    incomplete(
        &run(&fixture, "--checkpoint", &checkpoint, "1", &extra).await,
        &checkpoint,
    );
    let completed = support::report(&run(&fixture, "--resume", &checkpoint, "512", &extra).await);
    assert_eq!(completed["mode"], "confirm");
    assert_eq!(completed["database_writes"], true);
    let document = support::document(&confirmed);
    let created = chrono::DateTime::parse_from_rfc3339(&document.created_at).unwrap();
    let expires =
        chrono::DateTime::parse_from_rfc3339(document.expires_at.as_ref().unwrap()).unwrap();
    assert_eq!((expires - created).num_seconds(), 30);
    assert!(!checkpoint.exists());
    assert_eq!(
        support::report(&fixture.import("--status", &confirmed, false).await)["items"][0]["state"],
        "not_applied"
    );
    assert_eq!(fixture.remote_calls().await, 0);
}

async fn confirmation_storage(fixture: &support::Fixture) -> (i64, i64, i64) {
    let row = fixture.db.query_one(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "SELECT (SELECT COUNT(*) FROM pin_management_archive) AS archives, (SELECT COUNT(*) FROM pin_management_archive_chunks) AS chunks, (SELECT COUNT(*) FROM pin_management_records WHERE kind='action' AND plan_id=$1) AS confirmations",
        [fixture.request.plan_id.clone().into()],
    )).await.unwrap().unwrap();
    (
        row.try_get("", "archives").unwrap(),
        row.try_get("", "chunks").unwrap(),
        row.try_get("", "confirmations").unwrap(),
    )
}

#[tokio::test]
async fn confirmation_stages_pages_then_resumes_publication_after_output_failure() {
    let fixture = fixture().await;
    let checkpoint = fixture.file("staged-planning");
    let confirmed = fixture.file("staged-confirmed.v3");
    let extra = [
        "--confirm",
        confirmed.to_str().unwrap(),
        "--ttl-seconds",
        "30",
    ];
    let before = confirmation_storage(&fixture).await;
    incomplete(
        &run(&fixture, "--checkpoint", &checkpoint, "1", &extra).await,
        &checkpoint,
    );
    let mut staged = false;
    for _ in 0..128 {
        let report = support::report(&run(&fixture, "--resume", &checkpoint, "1", &extra).await);
        assert_eq!(report["outcome"], "incomplete");
        assert_eq!(report["effects"]["commit_unknown"], false);
        assert_eq!(report["confirmation_committed"], false);
        assert_eq!(report["artifact_exported"], false);
        assert!(report.get("summary").is_none());
        assert!(report.get("items").is_none());
        assert!(!confirmed.exists());
        let stored = confirmation_storage(&fixture).await;
        assert_eq!(stored.2, before.2);
        if stored.0 > before.0 && stored.1 > before.1 {
            assert_eq!(report["database_writes"], true);
            assert!(report["effects"]["database_commits"].as_u64().unwrap() > 0);
            assert_ne!(report["phase"], "planning");
            staged = true;
            break;
        }
    }
    assert!(
        staged,
        "paged confirmation must persist an archive and chunks before final publication"
    );
    assert!(PlanningCheckpoint::input(&checkpoint).await.is_ok());
    let occupied = fixture.file("staged-operator-output.v3");
    std::fs::write(&occupied, b"preserve operator output").unwrap();
    let rejected = run(
        &fixture,
        "--resume",
        &checkpoint,
        "512",
        &[
            "--confirm",
            occupied.to_str().unwrap(),
            "--ttl-seconds",
            "30",
        ],
    )
    .await;
    assert!(!rejected.status.success());
    let failure: serde_json::Value = serde_json::from_slice(&rejected.stdout).unwrap();
    assert_eq!(failure["database_writes"], true);
    assert!(failure["effects"]["database_commits"].as_u64().unwrap() > 0);
    assert_eq!(failure["effects"]["commit_unknown"], false);
    assert_eq!(failure["phase"], "ready");
    assert_eq!(failure["confirmation_committed"], false);
    assert_eq!(failure["artifact_exported"], false);
    assert_eq!(confirmation_storage(&fixture).await.2, before.2);
    assert_eq!(
        std::fs::read(&occupied).unwrap(),
        b"preserve operator output"
    );
    assert!(PlanningCheckpoint::input(&checkpoint).await.is_ok());
    let before_publication = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    let completed = support::report(&run(&fixture, "--resume", &checkpoint, "512", &extra).await);
    assert_eq!(completed["outcome"], "complete");
    assert_eq!(completed["phase"], "confirmed");
    assert_eq!(completed["database_writes"], true);
    assert!(completed["effects"]["database_commits"].as_u64().unwrap() > 0);
    assert_eq!(completed["effects"]["commit_unknown"], false);
    assert_eq!(completed["confirmation_committed"], true);
    assert_eq!(completed["artifact_exported"], true);
    assert_eq!(confirmation_storage(&fixture).await.2, before.2 + 1);
    let document = support::document(&confirmed);
    let created = chrono::DateTime::parse_from_rfc3339(&document.created_at).unwrap();
    let expires =
        chrono::DateTime::parse_from_rfc3339(document.expires_at.as_ref().unwrap()).unwrap();
    assert!(created >= before_publication);
    assert_eq!((expires - created).num_seconds(), 30);
    assert!(!checkpoint.exists());
    assert_eq!(fixture.remote_calls().await, 0);
}

async fn committed_fixture() -> (
    support::Fixture,
    std::path::PathBuf,
    std::path::PathBuf,
    Box<records::ConfirmedPlan>,
) {
    let fixture = fixture().await;
    let checkpoint = fixture.file("committed-planning");
    let occupied = fixture.file("committed-operator-output.v3");
    std::fs::write(&occupied, b"preserve original output").unwrap();
    let extra = [
        "--confirm",
        occupied.to_str().unwrap(),
        "--ttl-seconds",
        "30",
    ];
    let staged = run(&fixture, "--checkpoint", &checkpoint, "512", &extra).await;
    assert!(!staged.status.success());
    let report: serde_json::Value = serde_json::from_slice(&staged.stdout).unwrap();
    assert_eq!(report["phase"], "ready");
    assert_eq!(report["confirmation_committed"], false);

    let config = current_config(&fixture);
    let raw = toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &config, &fixture.request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let limits = RunLimits::default();
    let mut task = PlanningCheckpoint::resume(
        &checkpoint,
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &snapshot,
        &limits,
    )
    .await
    .unwrap();
    let frozen = task.snapshot().clone();
    let PlanningProgress::Complete(prepared) = task.advance(&fixture.db, &limits).await.unwrap()
    else {
        panic!("the staged task must retain its complete planning result");
    };
    let advanced = prepared
        .advance_confirmation(
            &fixture.db,
            &config,
            &auth,
            &fixture.request,
            &frozen,
            &limits,
        )
        .await;
    let ConfirmationProgress::Ready(ready) = advanced.progress.unwrap() else {
        panic!("the completed staging archive must retain its Ready boundary");
    };
    let published = records::confirm_ready(
        &fixture.db,
        &config,
        &auth,
        records::ReadyPublication {
            ready: &ready,
            acknowledgements: &[],
            ttl: chrono::Duration::seconds(30),
        },
        Default::default(),
        &limits,
    )
    .await;
    let ConfirmationProgress::Confirmed(binding) = published.progress.unwrap() else {
        panic!("the Ready confirmation must commit its original binding");
    };
    prepared.close().await.unwrap();
    (fixture, checkpoint, occupied, binding)
}

#[tokio::test]
async fn committed_recovery_accounts_preexport_rejections_and_completion_without_output() {
    let (fixture, checkpoint, occupied, binding) = committed_fixture().await;
    let config = current_config(&fixture);
    let extra = [
        "--confirm",
        occupied.to_str().unwrap(),
        "--ttl-seconds",
        "30",
    ];
    let before = fixture.frame().await;
    let capture = db_snapshot::collect_with_config(&fixture.db, &config, &fixture.request)
        .await
        .unwrap();
    let capture_work = capture.work().unwrap();
    assert!(capture_work.rows > 0 && capture_work.raw_bytes > 0);
    let ttl_mismatch = [
        "--confirm",
        occupied.to_str().unwrap(),
        "--ttl-seconds",
        "31",
    ];
    let ack_mismatch = [
        "--confirm",
        occupied.to_str().unwrap(),
        "--ttl-seconds",
        "30",
        "--ack-item",
        "provider",
    ];
    for args in [
        extra.as_slice(),
        ttl_mismatch.as_slice(),
        ack_mismatch.as_slice(),
    ] {
        let before_retry = source_work(&checkpoint).await;
        let rejected = run(&fixture, "--resume", &checkpoint, "512", args).await;
        assert!(!rejected.status.success());
        let report: serde_json::Value = serde_json::from_slice(&rejected.stdout).unwrap();
        assert_eq!(report["recovered_original_confirmation"], true);
        assert_eq!(report["confirmation_committed"], true);
        assert_eq!(report["database_writes"], false);
        assert_eq!(report["artifact_exported"], false);
        let after_retry = source_work(&checkpoint).await;
        assert!(after_retry.rows - before_retry.rows >= capture_work.rows);
        assert!(after_retry.bytes - before_retry.bytes >= capture_work.raw_bytes);
        assert_eq!(
            std::fs::read(&occupied).unwrap(),
            b"preserve original output"
        );
    }
    let before_no_output = source_work(&checkpoint).await;
    let report = support::report(&run(&fixture, "--resume", &checkpoint, "512", &[]).await);
    assert_eq!(report["outcome"], "complete");
    assert_eq!(report["recovered_original_confirmation"], true);
    assert_eq!(report["artifact_exported"], false);
    let after_no_output = source_work(&checkpoint).await;
    assert!(after_no_output.rows - before_no_output.rows >= capture_work.rows);
    assert!(after_no_output.bytes - before_no_output.bytes >= capture_work.raw_bytes);
    let exported = fixture.file("recovered-confirmed.v3");
    let report = support::report(
        &run(
            &fixture,
            "--resume",
            &checkpoint,
            "512",
            &[
                "--confirm",
                exported.to_str().unwrap(),
                "--ttl-seconds",
                "30",
            ],
        )
        .await,
    );
    assert_eq!(report["recovered_original_confirmation"], true);
    assert_eq!(report["database_writes"], false);
    let document = support::document(&exported);
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(&document.created_at).unwrap(),
        binding.confirmed_at
    );
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(document.expires_at.as_ref().unwrap()).unwrap(),
        binding.expires_at
    );
    assert!(fixture.frame().await == before);
    assert!(checkpoint.is_dir());
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn original_authority_rejection_charges_every_committed_retry() {
    let (fixture, checkpoint, occupied, _) = committed_fixture().await;
    let config = current_config(&fixture);
    let local = db_snapshot::collect_with_config(&fixture.db, &config, &fixture.request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let damaged = fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_management_records SET binding='!' || substr(binding,2) WHERE plan_id=$1 AND item_id=$2 AND kind='action'",
            vec![
                fixture.request.plan_id.clone().into(),
                fixture.request.items[0].item_id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(damaged.rows_affected(), 1);
    let original = fixture.frame().await;
    let raw = toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let baseline = source_work(&checkpoint).await;
    let mut no_capture = ipfs_s3_gateway::pinning::management::wire::v3::files::stream::budget();
    let rejected = PlanningCheckpoint::committed_task(
        &checkpoint,
        &fixture.db,
        &auth,
        &fixture.request,
        &local,
        &RunLimits::default(),
        &mut no_capture,
    )
    .await;
    assert!(rejected.is_err());
    let measured = source_work(&checkpoint).await;
    let authority_rows = measured.rows - baseline.rows;
    let authority_bytes = measured.bytes - baseline.bytes;
    assert!(authority_rows > 0 && authority_bytes > 0);
    let output = fixture.file("corrupt-original-rejected.v3");
    for _ in 0..2 {
        let before = source_work(&checkpoint).await;
        let rejected = run(
            &fixture,
            "--resume",
            &checkpoint,
            "512",
            &["--confirm", output.to_str().unwrap(), "--ttl-seconds", "30"],
        )
        .await;
        assert!(!rejected.status.success());
        let after = source_work(&checkpoint).await;
        assert_eq!(after.rows - before.rows, authority_rows);
        assert_eq!(after.bytes - before.bytes, authority_bytes);
        assert!(!output.exists());
        assert!(checkpoint.is_dir());
        assert_eq!(
            std::fs::read(&occupied).unwrap(),
            b"preserve original output"
        );
        assert!(fixture.frame().await == original);
    }
    assert_eq!(fixture.remote_calls().await, 0);
}
