#[path = "support/stage75_management_cli_scale.rs"]
mod support;

use ipfs_s3_gateway::{
    pinning::management::{
        Capture, PlanStatus, db_snapshot,
        retirement::{ConfirmationProgress, PlanningCheckpoint, PlanningProgress, RunLimits},
        wire::v3,
    },
    store::pinning::management::records,
};
use std::{collections::BTreeSet, time::Instant};

#[tokio::test]
async fn real_100k_two_provider_cli_output_and_original_confirmation_recovery() {
    let started = Instant::now();
    let fixture = support::fixture().await;
    eprintln!(
        "100k CLI task-owned fixture={}",
        fixture.directory.path().display()
    );
    support::seed_unrelated(&fixture.db).await;
    let keys = support::SourceKeys::capture(&fixture.db, &fixture.file("source-keys.jsonl")).await;
    let before = support::revisions(&fixture.db).await;
    assert!(
        support::confirmation_rows(&fixture.db, &fixture.request.plan_id)
            .await
            .is_empty()
    );
    let checkpoint = fixture.file("cli-output-checkpoint");
    let proposal = fixture.file("proposal.v3");

    // Reach output using real CLI processes. A second output invocation proves
    // continuation of output preparation, rather than just planning progress.
    let mut resume = false;
    let mut previous = None;
    let mut output_seen = false;
    let mut output_resumed = false;
    let mut previous_export_steps = None;
    let mut pages = "1";
    let output_started = Instant::now();
    loop {
        let output = support::run(&fixture, &checkpoint, resume, pages, &proposal, false).await;
        let report = support::cli::report(&output);
        if report["outcome"] == "complete" {
            assert!(
                output_resumed,
                "output must resume in another real CLI process"
            );
            assert_eq!(report["artifact_exported"], true);
            assert_eq!(report["database_writes"], false);
            assert!(report.get("interrupted").is_none());
            break;
        }
        let report = support::incomplete(&output, &checkpoint);
        assert!(!proposal.exists());
        let (work, export_steps) = support::task_work(&fixture, &checkpoint).await;
        if let Some(previous) = previous {
            support::advanced(previous, work);
        }
        previous = Some(work);
        if report["phase"] == "output" {
            if let Some(previous_export_steps) = previous_export_steps {
                assert!(
                    export_steps > previous_export_steps,
                    "a successful output continuation must advance its export phase, not just capture accounting"
                );
                output_resumed = true;
            }
            previous_export_steps = Some(export_steps);
            output_seen = true;
        }
        resume = true;
        pages = if output_seen && !output_resumed {
            "1"
        } else {
            "512"
        };
    }
    eprintln!(
        "complete CLI proposal wall={:?} last_retained_task_work={previous:?}",
        output_started.elapsed()
    );
    assert!(!checkpoint.exists());
    let proposal_claims = keys.verify(&proposal, &fixture);
    assert!(proposal_claims.expires_at.is_none());
    assert!(proposal_claims.acks.is_none());
    assert_eq!(support::revisions(&fixture.db).await, before);
    assert!(
        support::confirmation_rows(&fixture.db, &fixture.request.plan_id)
            .await
            .is_empty()
    );

    // The completed dry-run task has been cleaned up by the CLI. Confirmation
    // is a distinct task; its retained allowance is never reset on continuation.
    let checkpoint = fixture.file("cli-confirmation-checkpoint");
    let first = fixture.file("confirmed-original.v3");
    let second = fixture.file("confirmed-recovered-new-path.v3");
    support::incomplete(
        &support::run(&fixture, &checkpoint, false, "1", &first, true).await,
        &checkpoint,
    );
    assert!(!first.exists());
    // Commit at the public Ready boundary, then close without cleanup to model
    // a process exit after commit, before export.
    let raw = toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let local = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &fixture.request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let mut limits = RunLimits::default();
    limits.pages = 512;
    let mut task = PlanningCheckpoint::resume(
        &checkpoint,
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &local,
        &limits,
    )
    .await
    .unwrap();
    let frozen = task.snapshot().clone();
    let prepared = loop {
        let mut limits = RunLimits::default();
        limits.pages = 512;
        let previous = task.total_work().await.unwrap();
        match task.advance(&fixture.db, &limits).await.unwrap() {
            PlanningProgress::Complete(prepared) => break prepared,
            PlanningProgress::Incomplete => {
                let current = task.total_work().await.unwrap();
                support::advanced(previous, current);
                eprintln!("confirmation planning cumulative_task_work={current:?}");
            }
        }
    };
    let staging_started = Instant::now();
    let mut previous_total = prepared.task_work().await.unwrap();
    let ready = loop {
        let mut limits = RunLimits::default();
        limits.pages = 512;
        let advanced = prepared
            .advance_confirmation(
                &fixture.db,
                &fixture.config,
                &auth,
                &fixture.request,
                &frozen,
                &limits,
            )
            .await;
        assert!(!advanced.effects.commit_unknown);
        let current = prepared.task_work().await.unwrap();
        eprintln!(
            "confirmation staging phase={:?} wall={:?} cumulative_task_work={current:?}",
            advanced.phase,
            staging_started.elapsed()
        );
        match advanced.progress.unwrap() {
            ConfirmationProgress::Incomplete => support::advanced(previous_total, current),
            ConfirmationProgress::Ready(ready) => break ready,
            ConfirmationProgress::Confirmed(_) => {
                panic!("only explicit Ready publication may confirm")
            }
        }
        previous_total = current;
    };
    assert_eq!(ready.plan().items().len(), 2);
    assert!(
        ready
            .plan()
            .items()
            .iter()
            .all(|entry| entry.status == PlanStatus::Ready)
    );
    let acknowledgements: Vec<String> = Vec::new();
    let publication_started = Instant::now();
    let published = records::confirm_ready(
        &fixture.db,
        &fixture.config,
        &auth,
        records::ReadyPublication {
            ready: &ready,
            acknowledgements: &acknowledgements,
            ttl: chrono::Duration::seconds(30),
        },
        Default::default(),
        &RunLimits::default(),
    )
    .await;
    assert!(!published.effects.commit_unknown);
    assert_eq!(published.effects.database_commits, 1);
    let ConfirmationProgress::Confirmed(binding) = published.progress.unwrap() else {
        panic!("Ready publication must return its committed original confirmation");
    };
    assert_eq!(
        binding.confirmation.candidate_item_ids.as_slice(),
        acknowledgements.as_slice()
    );
    assert_eq!(
        (binding.expires_at - binding.confirmed_at).num_seconds(),
        30
    );
    eprintln!(
        "original publication wall={:?} cumulative_task_work={:?}",
        publication_started.elapsed(),
        prepared.task_work().await.unwrap()
    );
    drop(ready);
    prepared.close().await.unwrap();
    let committed_rows = support::confirmation_rows(&fixture.db, &fixture.request.plan_id).await;
    assert_eq!(committed_rows.len(), 2);
    let committed_revisions = support::revisions(&fixture.db).await;
    for (table, revision) in &before {
        // Confirmation publishes its record and original archive, not business state.
        if !matches!(
            table.as_str(),
            "pin_management_records" | "pin_management_archive" | "pin_management_archive_chunks"
        ) {
            assert_eq!(
                committed_revisions.get(table),
                Some(revision),
                "business source {table} changed"
            );
        }
    }

    // Both provider entries must resolve through the typed loader to the very
    // same complete original archive. Its contents stay file-backed.
    let mut original = None;
    let mut inventory_references = None;
    for entry in binding.confirmation.bindings.iter() {
        assert!(matches!(&entry.capture, Some(Capture::Provider(_))));
        let mut cursor = records::load_original_inventory(
            &fixture.db,
            &binding,
            entry,
            tempfile::tempfile_in(fixture.directory.path()).unwrap(),
            v3::files::stream::budget(),
        )
        .await
        .unwrap();
        let reference = cursor.archive_claim().unwrap().clone();
        if let Some(original) = &original {
            assert!(original == &reference);
        } else {
            original = Some(reference);
            inventory_references = Some(support::verify_inventory_references(
                &mut cursor,
                binding.confirmation.bindings.as_slice(),
            ));
        }
    }
    assert!(original.is_some());
    let inventory_references = inventory_references.unwrap();

    let mut previous = None;
    for output_path in [&first, &second] {
        assert!(!output_path.exists());
        let exported = support::run(&fixture, &checkpoint, true, "512", output_path, true).await;
        let report = support::cli::report(&exported);
        assert_eq!(report["outcome"], "complete");
        assert_eq!(report["recovered_original_confirmation"], true);
        assert_eq!(report["confirmation_committed"], true);
        assert_eq!(report["artifact_exported"], true);
        assert_eq!(report["database_writes"], false);
        assert_eq!(report["effects"]["database_commits"], 0);
        assert_eq!(report["effects"]["commit_unknown"], false);
        assert_eq!(report["source_rpc"], "not_performed");
        assert_eq!(report["provider_rpc"], "not_performed");
        assert!(report.get("interrupted").is_none());
        assert!(output_path.is_file());
        let claims = keys.verify(output_path, &fixture);
        assert_eq!(
            claims.inventory_ids,
            inventory_references
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            claims.membership_ids,
            inventory_references
                .values()
                .cloned()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(&claims.created_at).unwrap(),
            binding.confirmed_at
        );
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(claims.expires_at.as_ref().unwrap()).unwrap(),
            binding.expires_at
        );
        assert_eq!(claims.acks.as_ref(), Some(&acknowledgements));
        for (claim, entry) in claims
            .items
            .iter()
            .zip(binding.confirmation.bindings.iter())
        {
            assert!(claim.request == entry.request);
            assert!(
                claim.id == entry.id && claim.status == entry.status && claim.step == entry.step
            );
        }
        assert_eq!(
            support::confirmation_rows(&fixture.db, &fixture.request.plan_id).await,
            committed_rows
        );
        assert_eq!(support::revisions(&fixture.db).await, committed_revisions);

        // The read-only original loader retains the full task meter, including
        // the union; source-only telemetry above is not mislabelled as a total.
        let mut budget = v3::files::stream::budget();
        let retained = PlanningCheckpoint::committed_task(
            &checkpoint,
            &fixture.db,
            &auth,
            &fixture.request,
            &local,
            &RunLimits::default(),
            &mut budget,
        )
        .await
        .unwrap()
        .unwrap();
        let work = retained.task_work().await.unwrap();
        if let Some(previous) = previous {
            support::advanced(previous, work);
        }
        previous = Some(work);
        eprintln!(
            "recovered output={} cumulative_task_work={work:?} authority_read_work={:?} output_bytes={}",
            output_path.display(),
            budget.work,
            std::fs::metadata(output_path).unwrap().len()
        );
        retained.close().await.unwrap();
        assert!(checkpoint.is_dir());
    }
    support::same_file_bytes(&first, &second);
    assert_eq!(fixture.remote_calls().await, 0);
    assert_eq!(
        support::confirmation_rows(&fixture.db, &fixture.request.plan_id).await,
        committed_rows
    );
    assert_eq!(support::revisions(&fixture.db).await, committed_revisions);
    eprintln!(
        "real 100k two-provider CLI acceptance wall={:?} final_task_work={previous:?}",
        started.elapsed()
    );
    // Close DB handles before the standard fixture TempDir removes exactly its
    // owned source database, retained checkpoint, expectation file and artifacts.
    drop(binding);
    drop(task);
    fixture.db.clone().close().await.unwrap();
    fixture.directory.close().unwrap();
}
