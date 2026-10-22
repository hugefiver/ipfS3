use super::*;
use std::{future::Future, task::Poll};

fn header<'a>(auth: &'a records::LocalAuthorization, local: &'a Snapshot) -> Header<'a> {
    Header {
        projection: Projection::Review,
        database_scope: auth.database_scope(),
        database_identity: auth.database_identity(),
        principal: auth.principal(),
        catalog: &local.native_frame.as_ref().unwrap().schema_versions,
        created_at: local.database_now.unwrap().to_rfc3339(),
        expires_at: None,
    }
}

async fn planning_fixture(
    name: &str,
    foreign_routes: usize,
) -> (
    support::Fixture,
    records::LocalAuthorization,
    Snapshot,
    PreparedPlanning,
) {
    let (fixture, auth) = fixture(false).await;
    for index in 0..foreign_routes {
        let mut identity = fixture.config.pinning.providers[0].identity.clone();
        identity.provider_id = format!("output-boundary-{index}");
        identity.scope = format!("output-boundary-account-{index}");
        ledger::register_route(&fixture.db, &identity.allocation_key(), &identity)
            .await
            .unwrap();
    }
    let (local, prepared) = complete(&fixture, &auth, name, RunLimits::default().total_rows).await;
    (fixture, auth, local, prepared)
}

async fn prepare_output(
    fixture: &support::Fixture,
    auth: &records::LocalAuthorization,
    local: &Snapshot,
    prepared: &PreparedPlanning,
    projection: Projection,
) -> native::meter::Budget {
    let mut budget = v3::files::stream::budget();
    loop {
        let (result, returned) = prepared
            .prepare_output(
                budget,
                Header {
                    projection,
                    ..header(auth, local)
                },
                &fixture.request,
                local,
                &limits(RunLimits::default().total_rows),
            )
            .await;
        budget = returned;
        if result.unwrap() == Progress::Archived {
            return budget;
        }
    }
}

async fn reserved(path: &Path) -> std::fs::File {
    v3::files::reserve(path.to_str().unwrap())
        .await
        .unwrap()
        .into_std()
        .await
}

async fn output_position(db: &DatabaseConnection) -> (String, i64, i64, String, i64) {
    let row = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT state,chunks,bytes,digest,(SELECT count(*) FROM pin_management_archive_chunks) AS stored_chunks FROM pin_management_archive WHERE archive_id='planning-review-output'"))
        .await.unwrap().unwrap();
    (
        row.try_get("", "state").unwrap(),
        row.try_get("", "chunks").unwrap(),
        row.try_get("", "bytes").unwrap(),
        row.try_get("", "digest").unwrap(),
        row.try_get("", "stored_chunks").unwrap(),
    )
}

async fn reopen_output(
    fixture: &support::Fixture,
    auth: &records::LocalAuthorization,
    retained: &Path,
) -> (
    Snapshot,
    PreparedPlanning,
    native::meter::Budget,
    RunLimits,
    native::meter::ResidentGuard,
) {
    let (local, budget, resident) = capture_with_independent_copy(fixture).await;
    let run = limits(RunLimits::default().total_rows);
    let mut task = PlanningCheckpoint::resume(
        retained,
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &local,
        &run,
    )
    .await
    .unwrap();
    let local = task.snapshot().clone();
    let caps = task.reserve_capture(&budget, &run).unwrap();
    let PlanningProgress::Complete(prepared) = task.advance(&fixture.db, &caps).await.unwrap()
    else {
        panic!("small original task must reopen");
    };
    (local, *prepared, budget, run, resident)
}

#[tokio::test]
async fn prepared_projection_is_fixed_through_staging_and_sealed_resume() {
    for projection in [Projection::Review, Projection::PrivateRecovery] {
        let other = if projection == Projection::Review {
            Projection::PrivateRecovery
        } else {
            Projection::Review
        };
        let (fixture, auth, local, prepared) = planning_fixture("projection-resume", 0).await;
        let mut run = RunLimits::default();
        run.pages = 0;
        let (result, budget) = prepared
            .prepare_output(
                v3::files::stream::budget(),
                Header {
                    projection,
                    ..header(&auth, &local)
                },
                &fixture.request,
                &local,
                &run,
            )
            .await;
        assert_eq!(result.unwrap(), Progress::Pending);
        let spool = open_spool(
            &prepared
                .checkpoint_path()
                .unwrap()
                .join("source-000.sqlite"),
        )
        .await;
        let original = output_position(&spool).await;
        let (rejected, budget) = prepared
            .prepare_output(
                budget,
                Header {
                    projection: other,
                    ..header(&auth, &local)
                },
                &fixture.request,
                &local,
                &run,
            )
            .await;
        assert!(rejected.is_err());
        assert_eq!(output_position(&spool).await, original);
        let destination = fixture.file("staging-mismatch.v3");
        let (rejected, _) = prepared
            .export_prepared_output((reserved(&destination).await, budget), other, &run)
            .await;
        assert!(rejected.is_err());
        assert_eq!(std::fs::metadata(&destination).unwrap().len(), 0);
        spool.close().await.unwrap();
        let retained = prepared.checkpoint_path().unwrap().to_owned();
        prepared.close().await.unwrap();

        let (local, prepared, mut budget, run, _resident) =
            reopen_output(&fixture, &auth, &retained).await;
        loop {
            let (result, returned) = prepared
                .prepare_output(
                    budget,
                    Header {
                        projection,
                        ..header(&auth, &local)
                    },
                    &fixture.request,
                    &local,
                    &run,
                )
                .await;
            budget = returned;
            if result.unwrap() == Progress::Archived {
                break;
            }
        }
        let spool = open_spool(&retained.join("source-000.sqlite")).await;
        let sealed = output_position(&spool).await;
        assert_eq!(sealed.0, "sealed");
        let (rejected, budget) = prepared
            .prepare_output(
                budget,
                Header {
                    projection: other,
                    ..header(&auth, &local)
                },
                &fixture.request,
                &local,
                &run,
            )
            .await;
        assert!(rejected.is_err());
        assert_eq!(output_position(&spool).await, sealed);
        let destination = fixture.file("sealed-mismatch.v3");
        let (rejected, _) = prepared
            .export_prepared_output((reserved(&destination).await, budget), other, &run)
            .await;
        assert!(rejected.is_err());
        assert_eq!(std::fs::metadata(&destination).unwrap().len(), 0);
        let checkpoint: String = spool
            .query_one(Statement::from_string(
                spool.get_database_backend(),
                "SELECT checkpoint FROM state WHERE id=1",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "checkpoint")
            .unwrap();
        spool.close().await.unwrap();
        prepared.close().await.unwrap();
        drop(_resident);

        let (local, prepared, budget, run, _resident) =
            reopen_output(&fixture, &auth, &retained).await;
        let (ready, budget) = prepared
            .prepare_output(
                budget,
                Header {
                    projection,
                    ..header(&auth, &local)
                },
                &fixture.request,
                &local,
                &run,
            )
            .await;
        assert_eq!(ready.unwrap(), Progress::Archived);
        let destination = fixture.file("resumed-projection.v3");
        let (exported, _) = prepared
            .export_prepared_output((reserved(&destination).await, budget), projection, &run)
            .await;
        exported.unwrap();
        let mut reader = v3::Reader::new(
            std::fs::File::open(&destination).unwrap(),
            projection,
            v3::files::stream::budget(),
        )
        .unwrap();
        reader.enable_buffering().unwrap();
        let (mut inventory, mut descriptor) = (false, false);
        let mut raw = Vec::new();
        let mut end = None;
        while let Some(event) = reader.next_frame().unwrap() {
            match event {
                v3::Event::Frame(v3::Frame::Header {
                    expires_at,
                    applied_trust,
                    ..
                }) => {
                    assert!(expires_at.is_none());
                    assert!(applied_trust == v3::AppliedTrust::UntrustedClaimsOnly);
                }
                v3::Event::Frame(v3::Frame::Row { section, key, .. }) => {
                    inventory =
                        section == v3::Section::Inventory && key.table == "retirement_inventory"
                }
                v3::Event::Frame(v3::Frame::Cell { column }) => {
                    descriptor = inventory && column == "source_descriptor"
                }
                v3::Event::Chunk(bytes) if descriptor => raw.extend_from_slice(&bytes),
                v3::Event::Frame(v3::Frame::CellEnd { value }) if descriptor => end = Some(value),
                _ => {}
            }
        }
        assert!(reader.verified());
        if projection == Projection::Review {
            assert!(end == Some(v3::CellValue::Redacted));
            assert!(raw.is_empty());
        } else {
            assert!(end == Some(v3::CellValue::Text));
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&raw).unwrap(),
                serde_json::from_str::<serde_json::Value>(&checkpoint).unwrap()
            );
        }
        prepared.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn prepared_continuation_binding_and_wire_header_are_independently_checked() {
    let (fixture, auth, local, prepared) = planning_fixture("output-protocol-mismatch", 0).await;
    let mut run = RunLimits::default();
    run.pages = 0;
    let (ready, budget) = prepared
        .prepare_output(
            v3::files::stream::budget(),
            header(&auth, &local),
            &fixture.request,
            &local,
            &run,
        )
        .await;
    assert_eq!(ready.unwrap(), Progress::Pending);
    let retained = prepared.checkpoint_path().unwrap().to_owned();
    let spool = open_spool(&retained.join("source-000.sqlite")).await;
    let original = output_position(&spool).await;
    spool.execute_unprepared("UPDATE pin_management_archive SET digest=json_set(digest,'$.writer.state.projection','private_recovery') WHERE archive_id='planning-review-output'").await.unwrap();
    let damaged = output_position(&spool).await;
    let (rejected, _) = prepared
        .prepare_output(
            budget,
            header(&auth, &local),
            &fixture.request,
            &local,
            &run,
        )
        .await;
    assert!(rejected.is_err());
    assert_eq!(output_position(&spool).await, damaged);
    spool
        .execute(Statement::from_sql_and_values(
            spool.get_database_backend(),
            "UPDATE pin_management_archive SET digest=$1 WHERE archive_id='planning-review-output'",
            [original.3.into()],
        ))
        .await
        .unwrap();
    spool.close().await.unwrap();
    prepared.close().await.unwrap();

    let (local, prepared, mut budget, run, _resident) =
        reopen_output(&fixture, &auth, &retained).await;
    loop {
        let (ready, returned) = prepared
            .prepare_output(
                budget,
                header(&auth, &local),
                &fixture.request,
                &local,
                &run,
            )
            .await;
        budget = returned;
        if ready.unwrap() == Progress::Archived {
            break;
        }
    }
    let spool = open_spool(&retained.join("source-000.sqlite")).await;
    let raw: String = spool
        .query_one(Statement::from_string(
            spool.get_database_backend(),
            "SELECT header FROM pin_management_archive WHERE archive_id='planning-review-output'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "header")
        .unwrap();
    spool.execute_unprepared("UPDATE pin_management_archive SET header=json_set(header,'$.projection','private_recovery') WHERE archive_id='planning-review-output'").await.unwrap();
    let destination = fixture.file("binding-wire-mismatch.v3");
    let (rejected, _) = prepared
        .export_prepared_output(
            (reserved(&destination).await, budget),
            Projection::PrivateRecovery,
            &run,
        )
        .await;
    assert!(rejected.is_err());
    assert_eq!(std::fs::metadata(&destination).unwrap().len(), 0);
    spool
        .execute(Statement::from_sql_and_values(
            spool.get_database_backend(),
            "UPDATE pin_management_archive SET header=$1 WHERE archive_id='planning-review-output'",
            [raw.into()],
        ))
        .await
        .unwrap();
    spool.close().await.unwrap();
    prepared.close().await.unwrap();
    drop(_resident);
    let (local, prepared, budget, run, _resident) = reopen_output(&fixture, &auth, &retained).await;
    let (ready, budget) = prepared
        .prepare_output(
            budget,
            header(&auth, &local),
            &fixture.request,
            &local,
            &run,
        )
        .await;
    assert_eq!(ready.unwrap(), Progress::Archived);
    let spool = open_spool(&retained.join("source-000.sqlite")).await;
    let chunk: Vec<u8> = spool.query_one(Statement::from_string(spool.get_database_backend(), "SELECT payload FROM pin_management_archive_chunks WHERE archive_id='planning-review-output' AND ordinal=0")).await.unwrap().unwrap().try_get("", "payload").unwrap();
    let start = v3::MAGIC.len() + 5;
    let end = v3::MAGIC.len()
        + 4
        + u32::from_be_bytes(
            chunk[v3::MAGIC.len()..v3::MAGIC.len() + 4]
                .try_into()
                .unwrap(),
        ) as usize;
    let mut wire: serde_json::Value = serde_json::from_slice(&chunk[start..end]).unwrap();
    let length = wire["principal"].as_str().unwrap().len();
    wire["principal"] = "x".repeat(length).into();
    let changed = serde_json::to_vec(&wire).unwrap();
    assert_eq!(changed.len(), end - start);
    let mut chunk = chunk;
    chunk[start..end].copy_from_slice(&changed);
    spool.execute(Statement::from_sql_and_values(spool.get_database_backend(), "UPDATE pin_management_archive_chunks SET payload=$1 WHERE archive_id='planning-review-output' AND ordinal=0", [chunk.into()])).await.unwrap();
    let destination = fixture.file("wire-principal-mismatch.v3");
    let (rejected, _) = prepared
        .export_prepared_output(
            (reserved(&destination).await, budget),
            Projection::Review,
            &run,
        )
        .await;
    assert!(rejected.is_err());
    assert_eq!(std::fs::metadata(&destination).unwrap().len(), 0);
    spool.close().await.unwrap();
    prepared.cleanup().await.unwrap();
}

#[tokio::test]
async fn failed_output_page_keeps_persistent_cursor_and_pays_attempt_before_retry() {
    let (fixture, auth, local, prepared) = planning_fixture("uncommitted-output-page", 0).await;
    let mut no_pages = RunLimits::default();
    no_pages.pages = 0;
    let (first, budget) = prepared
        .prepare_output(
            v3::files::stream::budget(),
            header(&auth, &local),
            &fixture.request,
            &local,
            &no_pages,
        )
        .await;
    assert_eq!(first.unwrap(), Progress::Pending);
    let spool = open_spool(
        &prepared
            .checkpoint_path()
            .unwrap()
            .join("source-000.sqlite"),
    )
    .await;
    let initial = output_position(&spool).await;
    let before = prepared.task_work().await.unwrap();
    let initial_work = budget.work;
    // Fail after chunks have been inserted, at the atomic cursor update. The
    // transaction must roll back both bytes and position, but not their charge.
    spool.execute_unprepared("CREATE TRIGGER test_reject_output_cursor BEFORE UPDATE ON pin_management_archive BEGIN SELECT RAISE(FAIL,'output unavailable'); END").await.unwrap();
    let (failed, returned) = prepared
        .prepare_output(
            budget,
            header(&auth, &local),
            &fixture.request,
            &local,
            &RunLimits::default(),
        )
        .await;
    assert!(matches!(failed, Err(ManagementError::Database)));
    assert_eq!(output_position(&spool).await, initial);
    let after = prepared.task_work().await.unwrap();
    assert!(returned.work.output_bytes > initial_work.output_bytes);
    assert_eq!(
        after.bytes - before.bytes,
        returned.work.raw_bytes - initial_work.raw_bytes + returned.work.copied_bytes
            - initial_work.copied_bytes
    );
    assert!(after.bytes > before.bytes);
    spool
        .execute_unprepared("DROP TRIGGER test_reject_output_cursor")
        .await
        .unwrap();
    spool.close().await.unwrap();
    let retained = prepared.checkpoint_path().unwrap().to_owned();
    prepared.close().await.unwrap();
    let mut resumed = PlanningCheckpoint::resume(
        &retained,
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &local,
        &RunLimits::default(),
    )
    .await
    .unwrap();
    let prepared = match resumed
        .advance(&fixture.db, &limits(RunLimits::default().total_rows))
        .await
        .unwrap()
    {
        PlanningProgress::Complete(prepared) => prepared,
        PlanningProgress::Incomplete => panic!("small accepted plan must reopen"),
    };
    let budget = prepare_output(&fixture, &auth, &local, &prepared, Projection::Review).await;
    let output = fixture.file("retry-uncommitted-page.v3");
    let (result, _) = prepared
        .export_prepared_output(
            (reserved(&output).await, budget),
            Projection::Review,
            &limits(RunLimits::default().total_rows),
        )
        .await;
    result.unwrap();
    assert!(support::document(&output).request.as_ref() == Some(&fixture.request));
    prepared.cleanup().await.unwrap();
}

#[tokio::test]
async fn stopped_output_pages_do_not_commit_cursor_and_keep_attempt_charges() {
    use std::time::{Duration, Instant};
    enum Stop {
        Writer,
        Cancel,
        Deadline,
    }
    for stop in [Stop::Writer, Stop::Cancel, Stop::Deadline] {
        let (fixture, auth, local, prepared) = planning_fixture("stopped-output-page", 0).await;
        let mut no_pages = RunLimits::default();
        no_pages.pages = 0;
        let (first, mut budget) = prepared
            .prepare_output(
                v3::files::stream::budget(),
                header(&auth, &local),
                &fixture.request,
                &local,
                &no_pages,
            )
            .await;
        assert_eq!(first.unwrap(), Progress::Pending);
        let spool = open_spool(
            &prepared
                .checkpoint_path()
                .unwrap()
                .join("source-000.sqlite"),
        )
        .await;
        let initial = output_position(&spool).await;
        let before = prepared.task_work().await.unwrap();
        let initial_work = budget.work;
        let writer_cancel = budget.cancellation();
        let mut run = RunLimits::default();
        let deadline = Instant::now() + Duration::from_secs(1);
        run.constrain_deadline(deadline, &mut budget).unwrap();
        let (failed, returned) = {
            let operation = prepared.prepare_output(
                budget,
                header(&auth, &local),
                &fixture.request,
                &local,
                &run,
            );
            tokio::pin!(operation);
            std::future::poll_fn(|cx| match operation.as_mut().poll(cx) {
                Poll::Ready(_) => panic!("output page finished before the controlled stop"),
                Poll::Pending if run.pages_remaining() < run.pages => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            })
            .await;
            assert!(Instant::now() < deadline);
            match stop {
                Stop::Writer => writer_cancel.store(true, std::sync::atomic::Ordering::Relaxed),
                Stop::Cancel => run.cancellation.cancel(),
                Stop::Deadline => tokio::time::sleep_until(deadline.into()).await,
            }
            operation.await
        };
        assert!(matches!(failed, Err(ManagementError::Budget)));
        assert_eq!(output_position(&spool).await, initial);
        let after = prepared.task_work().await.unwrap();
        assert_eq!(
            after.bytes - before.bytes,
            returned.work.raw_bytes - initial_work.raw_bytes + returned.work.copied_bytes
                - initial_work.copied_bytes
        );
        assert!(after.bytes > before.bytes);
        spool.close().await.unwrap();
        prepared.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn rejected_output_accounting_remains_payable_once_after_spool_recovery() {
    let (fixture, auth, local, prepared) = planning_fixture("output-debt", 0).await;
    let (_, mut budget, _resident) = capture_with_independent_copy(&fixture).await;
    prepared
        .account_capture(&mut budget, &RunLimits::default())
        .await
        .unwrap();
    let prepaid = budget.work;
    let paid = prepared.task_work().await.unwrap();
    let target = fixture.frame().await;
    let storage = archive_storage(&fixture).await;
    let spool = open_spool(
        &prepared
            .checkpoint_path()
            .unwrap()
            .join("source-000.sqlite"),
    )
    .await;
    spool.execute_unprepared(
        "CREATE TRIGGER test_output_debt_rejection BEFORE UPDATE ON task_budget WHEN NEW.bytes_used>OLD.bytes_used BEGIN SELECT RAISE(FAIL,'output accounting unavailable'); END"
    ).await.unwrap();
    // Prefix and metadata work is real even when no producer page is requested.
    // The previously paid capture can pass the zero-delta accounting handoff.
    let mut run = RunLimits::default();
    run.pages = 0;
    let (failed, mut returned) = prepared
        .prepare_output(
            budget,
            header(&auth, &local),
            &fixture.request,
            &local,
            &run,
        )
        .await;
    let after_failure = prepared.task_work().await.unwrap();
    spool
        .execute_unprepared("DROP TRIGGER test_output_debt_rejection")
        .await
        .unwrap();
    spool.close().await.unwrap();
    let attempted = returned.work;
    let settled = prepared
        .account_capture(&mut returned, &RunLimits::default())
        .await;
    let after_settlement = prepared.task_work().await.unwrap();
    let repeated = prepared
        .account_capture(&mut returned, &RunLimits::default())
        .await;
    let after_repeat = prepared.task_work().await.unwrap();
    assert!(fixture.frame().await == target);
    assert_eq!(archive_storage(&fixture).await, storage);
    prepared.cleanup().await.unwrap();

    assert!(matches!(failed, Err(ManagementError::Database)));
    assert_eq!(after_failure, paid);
    assert!(attempted.raw_bytes > prepaid.raw_bytes);
    // A poisoned Budget may refuse reuse, but a successful DB handoff must
    // settle its real debt before advancing its paid watermark.
    assert!(settled.is_ok() || matches!(settled, Err(ManagementError::Budget)));
    assert!(repeated.is_ok() || matches!(repeated, Err(ManagementError::Budget)));
    assert_eq!(
        after_settlement.rows - paid.rows,
        attempted.rows - prepaid.rows
    );
    assert_eq!(
        after_settlement.bytes - paid.bytes,
        attempted.raw_bytes - prepaid.raw_bytes + attempted.copied_bytes - prepaid.copied_bytes
    );
    assert_eq!(after_repeat, after_settlement);
}

#[tokio::test]
async fn prepared_output_copy_stops_at_cancellation_and_keeps_paid_partial() {
    for projection in [Projection::Review, Projection::PrivateRecovery] {
        let (fixture, auth, local, prepared) = planning_fixture("cancelled-output-copy", 256).await;
        let budget = prepare_output(&fixture, &auth, &local, &prepared, projection).await;
        let initial = budget.work;
        let before = prepared.task_work().await.unwrap();
        let target = fixture.frame().await;
        let storage = archive_storage(&fixture).await;
        let spool = open_spool(
            &prepared
                .checkpoint_path()
                .unwrap()
                .join("source-000.sqlite"),
        )
        .await;
        let row = spool.query_one(Statement::from_string(spool.get_database_backend(),
        "SELECT bytes,chunks FROM pin_management_archive WHERE archive_id='planning-review-output' AND state='sealed'"))
        .await.unwrap().unwrap();
        let complete_bytes = row.try_get::<i64>("", "bytes").unwrap() as u64;
        assert!(row.try_get::<i64>("", "chunks").unwrap() > 2);
        spool.close().await.unwrap();
        let partial = fixture.file("cancelled-review.v3");
        let file = reserved(&partial).await;
        let probe = file.try_clone().unwrap();
        let run = limits(RunLimits::default().total_rows);
        let cancellation = run.cancellation.clone();
        let mut stopped_at = None;
        let (result, returned) = {
            let operation = prepared.export_prepared_output((file, budget), projection, &run);
            tokio::pin!(operation);
            std::future::poll_fn(|cx| {
                let polled = operation.as_mut().poll(cx);
                if matches!(polled, Poll::Pending) && stopped_at.is_none() {
                    let written = probe.metadata().unwrap().len();
                    if written > 0 && written < complete_bytes {
                        stopped_at = Some(written);
                        cancellation.cancel();
                    }
                }
                polled
            })
            .await
        };
        let written = probe.metadata().unwrap().len();
        drop(probe);
        let spent = prepared.task_work().await.unwrap();
        let preserved = std::fs::read(&partial).unwrap();
        let occupied = v3::files::reserve(partial.to_str().unwrap()).await;
        assert!(occupied.is_err());
        let retained = prepared.checkpoint_path().unwrap().to_owned();
        prepared.close().await.unwrap();
        let (local, prepared, budget, retry_run, _resident) =
            reopen_output(&fixture, &auth, &retained).await;
        let (ready, budget) = prepared
            .prepare_output(
                budget,
                Header {
                    projection,
                    ..header(&auth, &local)
                },
                &fixture.request,
                &local,
                &retry_run,
            )
            .await;
        assert_eq!(ready.unwrap(), Progress::Archived);
        let new_path = fixture.file("retry-cancelled-review.v3");
        let (retry, _) = prepared
            .export_prepared_output((reserved(&new_path).await, budget), projection, &retry_run)
            .await;
        let retry_document = if retry.is_ok() {
            Some(
                v3::document::inspect(
                    std::fs::File::open(&new_path).unwrap(),
                    projection,
                    support::wire_budget(),
                )
                .unwrap()
                .0,
            )
        } else {
            None
        };
        assert_eq!(std::fs::read(&partial).unwrap(), preserved);
        assert!(fixture.frame().await == target);
        assert_eq!(archive_storage(&fixture).await, storage);
        prepared.cleanup().await.unwrap();

        let stopped_at = stopped_at.expect("copy must expose an awaited chunk boundary before EOF");
        assert!(result.is_err());
        assert_eq!(
            written, stopped_at,
            "cancellation must stop subsequent archive chunks"
        );
        assert!(written < complete_bytes);
        assert_eq!(
            spent.bytes - before.bytes,
            returned.work.raw_bytes - initial.raw_bytes
        );
        assert!(spent.bytes > before.bytes);
        assert!(retry.is_ok());
        assert!(retry_document.unwrap().request.as_ref() == Some(&fixture.request));
    }
}

#[tokio::test]
async fn prepared_review_copy_and_verification_pages_are_durable() {
    let (fixture, auth, local, prepared) = planning_fixture("output-page-accounting", 0).await;
    let budget = prepare_output(&fixture, &auth, &local, &prepared, Projection::Review).await;
    let before = prepared.task_work().await.unwrap();
    let initial = budget.work;
    let output = fixture.file("successful-pages.v3");
    let run = limits(RunLimits::default().total_rows);
    let (result, returned) = prepared
        .export_prepared_output((reserved(&output).await, budget), Projection::Review, &run)
        .await;
    let after = prepared.task_work().await.unwrap();
    let consumed = run.pages - run.pages_remaining();
    let bytes = std::fs::metadata(&output).unwrap().len();
    let verified = if result.is_ok() {
        Some(support::document(&output))
    } else {
        None
    };
    prepared.cleanup().await.unwrap();
    result.unwrap();
    assert!(verified.unwrap().request.as_ref() == Some(&fixture.request));
    assert_eq!(consumed, 2 * bytes.div_ceil(1024 * 1024));
    assert_eq!(returned.work.pages - initial.pages, consumed);
    assert_eq!(after.pages - before.pages, consumed);
    assert_eq!(
        after.bytes - before.bytes,
        returned.work.raw_bytes - initial.raw_bytes
            + (returned.work.copied_bytes - initial.copied_bytes)
    );
    assert_eq!(returned.work.output_bytes - initial.output_bytes, bytes);
}

#[tokio::test]
async fn prepared_review_failed_verification_keeps_the_copy_page_charge() {
    let (fixture, auth, local, prepared) =
        planning_fixture("failed-output-page-accounting", 0).await;
    let budget = prepare_output(&fixture, &auth, &local, &prepared, Projection::Review).await;
    let before = prepared.task_work().await.unwrap();
    let initial = budget.work;
    let output = fixture.file("unverified-pages.v3");
    let mut run = RunLimits::default();
    run.pages = 1;
    let (result, returned) = prepared
        .export_prepared_output((reserved(&output).await, budget), Projection::Review, &run)
        .await;
    let after = prepared.task_work().await.unwrap();
    let consumed = run.pages - run.pages_remaining();
    let bytes = std::fs::metadata(&output).unwrap().len();
    let preserved = std::fs::read(&output).unwrap();
    assert!(v3::files::reserve(output.to_str().unwrap()).await.is_err());
    prepared.cleanup().await.unwrap();
    assert!(matches!(result, Err(ManagementError::Budget)), "{result:?}");
    assert!(bytes > 0 && bytes <= 1024 * 1024);
    assert_eq!(consumed, 1);
    assert_eq!(returned.work.pages - initial.pages, consumed);
    assert_eq!(after.pages - before.pages, consumed);
    assert_eq!(
        after.bytes - before.bytes,
        returned.work.raw_bytes - initial.raw_bytes
    );
    assert_eq!(std::fs::read(&output).unwrap(), preserved);
}

#[tokio::test]
async fn original_confirmation_page_output_charges_both_copy_and_final_file() {
    let (fixture, _, retained, _) = committed_without_output().await;
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let (local, mut budget, _resident) = capture_with_independent_copy(&fixture).await;
    let run = limits(RunLimits::default().total_rows);
    let task = PlanningCheckpoint::committed_task(
        &retained,
        &fixture.db,
        &auth,
        &fixture.request,
        &local,
        &run,
        &mut budget,
    )
    .await
    .unwrap()
    .unwrap();
    let before = task.task_work().await.unwrap();
    let initial = budget.work;
    let pages = run.pages_remaining();
    let output = fixture.file("confirmed-pages.v3");
    let (result, returned) = task
        .export_file(
            &fixture.db,
            (reserved(&output).await, budget),
            Projection::Review,
            &run,
        )
        .await;
    let consumed = pages - run.pages_remaining();
    let after = task.task_work().await.unwrap();
    let partial = fixture.file("confirmed-page-exhaustion.v3");
    let mut one_page = RunLimits::default();
    one_page.pages = 1;
    let (failed, failed_budget) = task
        .export_file(
            &fixture.db,
            (reserved(&partial).await, v3::files::stream::budget()),
            Projection::Review,
            &one_page,
        )
        .await;
    let failed_consumed = one_page.pages - one_page.pages_remaining();
    let after_failure = task.task_work().await.unwrap();
    let partial_bytes = std::fs::read(&partial).unwrap();
    assert!(v3::files::reserve(partial.to_str().unwrap()).await.is_err());
    task.close().await.unwrap();
    result.unwrap();
    let document = support::document(&output);
    assert!(document.request.as_ref() == Some(&fixture.request));
    assert!(consumed >= 2);
    assert_eq!(returned.work.pages - initial.pages, consumed);
    assert_eq!(after.pages - before.pages, consumed);
    assert_eq!(
        after.bytes - before.bytes,
        returned.work.raw_bytes - initial.raw_bytes
            + (returned.work.copied_bytes - initial.copied_bytes)
    );
    assert!(failed.is_err());
    assert!(!partial_bytes.is_empty());
    assert!(partial_bytes.len() < std::fs::metadata(&output).unwrap().len() as usize);
    assert_eq!(failed_consumed, 1);
    assert_eq!(failed_budget.work.pages, failed_consumed);
    assert_eq!(after_failure.pages - after.pages, failed_consumed);
    assert_eq!(std::fs::read(&partial).unwrap(), partial_bytes);
}
