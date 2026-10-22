use super::*;
use crate::store::pinning::management::native::meter::Limits;

enum CaptureStop {
    RunCancellation,
    BudgetCancellation,
    BudgetDeadline,
}

async fn capture_settlement_preserves_stop(stop: CaptureStop) {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let limits = RunLimits::default();
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = TaskBudget::new(&spools, &union, (100, 4096));
    let mut budget = Budget::default();
    budget.charge(1, 5, 6, 100).unwrap();
    budget.work.copied_bytes = 30;
    budget.work.pages = 2;
    match stop {
        CaptureStop::RunCancellation => limits.cancellation.cancel(),
        CaptureStop::BudgetCancellation => budget
            .cancellation()
            .store(true, std::sync::atomic::Ordering::Relaxed),
        CaptureStop::BudgetDeadline => {
            let mut expired = Budget::new(Limits {
                elapsed: std::time::Duration::ZERO,
                ..Default::default()
            });
            expired.work = budget.work;
            budget = expired;
        }
    }
    let cancel = budget.cancellation();
    let status = budget.check();
    task.account_capture(&limits, &mut budget).await.unwrap();
    let paid = task.total().await.unwrap();
    assert_eq!(
        paid,
        TaskWork {
            rows: 5,
            bytes: 130,
            pages: 2
        }
    );
    assert_eq!(
        capture_work(&budget).unwrap(),
        TaskWork {
            rows: 0,
            bytes: 0,
            pages: 0
        }
    );
    assert_eq!(budget.check(), status);
    assert!(std::sync::Arc::ptr_eq(&cancel, &budget.cancellation()));
    task.account_capture(&limits, &mut budget).await.unwrap();
    assert_eq!(task.total().await.unwrap(), paid);
    assert!(matches!(
        task.adopt_output(&limits, &mut budget, true).await,
        Err(ManagementError::Budget)
    ));
    assert_eq!(task.total().await.unwrap(), paid);
    spools[0].close().await.unwrap();
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn capture_settles_after_run_cancellation_without_admitting_output() {
    capture_settlement_preserves_stop(CaptureStop::RunCancellation).await;
}

#[tokio::test]
async fn capture_settles_after_budget_cancellation_without_admitting_output() {
    capture_settlement_preserves_stop(CaptureStop::BudgetCancellation).await;
}

#[tokio::test]
async fn capture_settles_after_budget_deadline_without_admitting_output() {
    capture_settlement_preserves_stop(CaptureStop::BudgetDeadline).await;
}

#[tokio::test]
async fn stopped_capture_settlement_rejects_cumulative_rows_or_bytes_exhaustion() {
    for envelope in [(4, 4096), (100, 120)] {
        let mut workspace = Workspace::create().unwrap();
        let (path, file) = workspace.reserve().await.unwrap();
        let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
        let (union_path, file) = workspace.reserve().await.unwrap();
        let union = PlanUnion::create(&union_path, file).await.unwrap();
        let limits = RunLimits::default();
        spool::task_run(&spools[0].db, &limits).await.unwrap();
        union.start_task(&limits).await.unwrap();
        let task = TaskBudget::new(&spools, &union, envelope);
        let mut budget = Budget::default();
        budget.charge(1, 5, 6, 100).unwrap();
        budget.work.copied_bytes = 30;
        limits.cancellation.cancel();
        assert_eq!(
            task.account_capture(&limits, &mut budget).await,
            Err(ManagementError::Budget)
        );
        let paid = task.total().await.unwrap();
        assert_eq!(
            (paid.rows, paid.bytes),
            (5.min(envelope.0), 130.min(envelope.1))
        );
        assert_eq!(
            task.account_capture(&limits, &mut budget).await,
            Err(ManagementError::Budget)
        );
        assert_eq!(task.total().await.unwrap(), paid);
        spools[0].close().await.unwrap();
        union.close().await.unwrap();
        workspace.cleanup().unwrap();
    }
}

#[tokio::test]
async fn transfer_independent_copy_cannot_exceed_remaining_task_bytes() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.total_rows = 100;
    limits.total_bytes = 48;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = TaskBudget::new(&spools, &union, (100, 48));
    let mut budget = Budget::default();
    let accounting = task.adopt_output(&limits, &mut budget, true).await.unwrap();
    let initial = budget.work;
    let accounting = accounting
        .prepay_transfer(&mut budget, &limits)
        .await
        .unwrap();
    let source = [b'x'; 16];
    budget.charge(0, 1, 1, source.len() as u64).unwrap();
    let resident = budget.reserve(source.len()).unwrap();
    budget.work.copied_bytes += source.len() as u64;
    budget.check().unwrap();
    let first_copy = source.to_vec();
    budget.charge(0, 1, 1, source.len() as u64).unwrap();
    budget.work.copied_bytes += source.len() as u64;
    let admitted = budget.check();
    let copied = admitted.as_ref().ok().map(|()| source.to_vec());
    let settled = accounting.finish_output(initial, &mut budget).await;
    let paid = task.total().await.unwrap();
    let debt = capture_work(&budget).unwrap();
    drop(resident);
    spools[0].close().await.unwrap();
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
    assert_eq!(first_copy, source);
    assert_eq!(admitted, Err(ManagementError::Budget));
    assert!(copied.is_none());
    assert_eq!(settled, Err(ManagementError::Budget));
    assert_eq!(paid.bytes, 48);
    assert_eq!(debt.bytes, 16);
}

#[tokio::test]
async fn transfer_settlement_covers_independent_copy_beyond_raw_prepayment() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.total_rows = 100;
    limits.total_bytes = 1024;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = TaskBudget::new(&spools, &union, (100, 1024));
    let mut budget = Budget::new(Limits {
        bytes: 48,
        ..Default::default()
    });
    let accounting = task.adopt_output(&limits, &mut budget, true).await.unwrap();
    let initial = budget.work;
    let accounting = accounting
        .prepay_transfer(&mut budget, &limits)
        .await
        .unwrap();
    let source = [b'x'; 32];
    budget.charge(0, 1, 1, source.len() as u64).unwrap();
    let resident = budget.reserve(source.len()).unwrap();
    let copied = source.to_vec();
    budget.work.copied_bytes += copied.len() as u64;
    accounting
        .finish_output(initial, &mut budget)
        .await
        .unwrap();
    let paid = task.total().await.unwrap();
    let debt = capture_work(&budget).unwrap();
    drop(resident);
    spools[0].close().await.unwrap();
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
    assert_eq!(paid.bytes, budget.work.raw_bytes + budget.work.copied_bytes);
    assert_eq!(debt.bytes, 0);
}

#[tokio::test]
async fn interrupted_transfer_prepayment_covers_admitted_read_and_copy_work() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.total_rows = 100;
    limits.total_bytes = 1024;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = TaskBudget::new(&spools, &union, (100, 1024));
    let mut budget = Budget::new(Limits {
        bytes: 48,
        ..Default::default()
    });
    let accounting = task.adopt_output(&limits, &mut budget, true).await.unwrap();
    let accounting = accounting
        .prepay_transfer(&mut budget, &limits)
        .await
        .unwrap();
    let source = [b'x'; 32];
    budget.charge(0, 1, 1, source.len() as u64).unwrap();
    let resident = budget.reserve(source.len()).unwrap();
    budget.work.copied_bytes += source.len() as u64;
    let copied = budget.check().ok().map(|()| source.to_vec());
    let admitted_bytes =
        budget.work.raw_bytes + copied.as_ref().map_or(0, |copy| copy.len() as u64);
    drop(accounting);
    let paid = task.total().await.unwrap();
    drop(resident);
    drop(task);
    spools[0].close().await.unwrap();
    union.close().await.unwrap();
    drop(spools);
    drop(union);
    let cleanup = workspace.cleanup();
    assert!(
        paid.bytes >= admitted_bytes,
        "paid={}, admitted={admitted_bytes}",
        paid.bytes
    );
    cleanup.unwrap();
}

#[tokio::test]
async fn output_partial_payment_survives_second_failure_without_double_charge() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.total_rows = 100;
    limits.total_bytes = 1024;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = TaskBudget::new(&spools, &union, (100, 1024));
    let mut budget = Budget::default();
    let mut accounting = task.adopt_output(&limits, &mut budget, true).await.unwrap();
    budget.charge(1, 1, 1, 32).unwrap();
    accounting.persist(budget.work).await.unwrap();
    spool::exec(&spools[0].db, "CREATE TRIGGER test_output_second_payment BEFORE UPDATE ON task_budget WHEN NEW.bytes_used>OLD.bytes_used BEGIN SELECT RAISE(ABORT,'test payment failure'); END", vec![]).await.unwrap();
    budget.charge(1, 1, 1, 32).unwrap();
    assert!(matches!(
        accounting.persist(budget.work).await,
        Err(ManagementError::Database)
    ));
    assert!(matches!(
        accounting.finish_metered(&mut budget).await,
        Err(ManagementError::Database)
    ));
    let partial = task.total().await.unwrap();
    let debt = capture_work(&budget).unwrap();
    spool::exec(
        &spools[0].db,
        "DROP TRIGGER test_output_second_payment",
        vec![],
    )
    .await
    .unwrap();
    task.adopt_output(&limits, &mut budget, true)
        .await
        .unwrap()
        .finish_metered(&mut budget)
        .await
        .unwrap();
    let paid = task.total().await.unwrap();
    task.adopt_output(&limits, &mut budget, true)
        .await
        .unwrap()
        .finish_metered(&mut budget)
        .await
        .unwrap();
    let repeated = task.total().await.unwrap();
    spools[0].close().await.unwrap();
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
    assert_eq!((partial.rows, partial.bytes), (1, 32));
    assert_eq!((debt.rows, debt.bytes), (1, 32));
    assert_eq!((paid.rows, paid.bytes), (2, 64));
    assert_eq!(repeated, paid);
}

#[tokio::test]
async fn failed_authority_decode_settles_actual_read_and_copy_work() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.total_rows = 100;
    limits.total_bytes = 4096;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = TaskBudget::new(&spools, &union, (100, 4096));
    let (accounting, mut session) = task.begin_read(&limits).await.unwrap();
    let mut read = session.storage_budget().unwrap();
    read.charge(1, 3, 4, 50).unwrap();
    session.charge_storage(read.work).unwrap();
    session.nodes(3).unwrap();
    session.bytes(60).unwrap();
    assert!(session.typed::<serde_json::Value>("not-json").is_err());
    let used = session.task_usage().unwrap();
    accounting.finish(used).await.unwrap();
    let total = task.total().await.unwrap();
    assert_eq!(total.rows, used.rows);
    assert_eq!(total.bytes, used.raw_bytes);
    assert!(total.rows >= 6 && total.bytes >= 110);
    spools[0].close().await.unwrap();
    let reopened = spool::connect_file(&path).await.unwrap();
    let row = one(
        &reopened,
        "SELECT rows_used,bytes_used FROM task_budget WHERE id=1",
        vec![],
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(get::<i64>(&row, "rows_used").unwrap() as u64, total.rows);
    assert_eq!(get::<i64>(&row, "bytes_used").unwrap() as u64, total.bytes);
    reopened.close().await.unwrap();
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn interrupted_authority_read_keeps_prepaid_allowance() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.total_rows = 8;
    limits.total_bytes = 1024;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = TaskBudget::new(&spools, &union, (8, 1024));
    let (accounting, session) = task.begin_read(&limits).await.unwrap();
    drop(session);
    drop(accounting);
    let total = task.total().await.unwrap();
    assert_eq!((total.rows, total.bytes), (8, 1024));
    assert!(matches!(
        task.begin_read(&RunLimits::default()).await,
        Err(ManagementError::Budget)
    ));
    spools[0].close().await.unwrap();
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn capture_handoff_charges_once_before_authority_and_preserves_the_budget() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.total_rows = 20;
    limits.total_bytes = 4096;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = TaskBudget::new(&spools, &union, (20, 4096));
    let mut budget = Budget::default();
    budget.charge(1, 5, 6, 100).unwrap();
    let cancel = budget.cancellation();
    let resident = budget.reserve(10).unwrap();
    let capture = budget.work;
    task.adopt_output(&limits, &mut budget, true)
        .await
        .unwrap()
        .finish(budget.work)
        .await
        .unwrap();
    assert_eq!(
        task.total().await.unwrap(),
        TaskWork {
            rows: 5,
            bytes: 100,
            pages: 0
        }
    );
    let (accounting, mut session) = task.begin_read(&limits).await.unwrap();
    session.nodes(2).unwrap();
    session.bytes(30).unwrap();
    accounting
        .finish(session.task_usage().unwrap())
        .await
        .unwrap();
    let total = task.total().await.unwrap();
    assert_eq!((total.rows, total.bytes), (7, 130));
    // Both None -> normal resume/preflight and Some -> recovery export construct
    // a task handle anew but keep the same operation Budget.
    let resumed = TaskBudget::new(&spools, &union, (20, 4096));
    let reserved = resumed.reserve_capture(&budget, &limits).unwrap();
    assert_eq!((reserved.total_rows, reserved.total_bytes), (20, 4096));
    resumed
        .adopt_output(&limits, &mut budget, true)
        .await
        .unwrap()
        .finish(budget.work)
        .await
        .unwrap();
    assert_eq!(resumed.total().await.unwrap(), total);
    assert_eq!(
        (budget.work.rows, budget.work.raw_bytes),
        (capture.rows, capture.raw_bytes)
    );
    assert!(std::sync::Arc::ptr_eq(&cancel, &budget.cancellation()));
    assert_eq!(budget.peak_resident(), 10);
    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(matches!(budget.check(), Err(ManagementError::Budget)));
    drop(resident);
    spools[0].close().await.unwrap();
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
}

#[tokio::test]
async fn capture_exhaustion_leaves_no_free_authority_allowance() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (union_path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&union_path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.total_rows = 5;
    limits.total_bytes = 100;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    let task = TaskBudget::new(&spools, &union, (5, 100));
    let mut budget = Budget::default();
    budget.charge(1, 5, 6, 100).unwrap();
    task.adopt_output(&limits, &mut budget, true)
        .await
        .unwrap()
        .finish(budget.work)
        .await
        .unwrap();
    assert!(matches!(
        task.begin_read(&RunLimits::default()).await,
        Err(ManagementError::Budget)
    ));
    assert_eq!(
        task.total().await.unwrap(),
        TaskWork {
            rows: 5,
            bytes: 100,
            pages: 0
        }
    );
    spools[0].close().await.unwrap();
    union.close().await.unwrap();
    workspace.cleanup().unwrap();
}
