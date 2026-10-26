use super::*;
use crate::store::pinning::management::native::meter::Budget;

#[test]
fn production_stop_leaves_hard_settlement_and_final_transfer_available() {
    let limits = RunLimits {
        elapsed: Duration::from_secs(30),
        ..Default::default()
    };
    let mut production = limits.for_production();
    let start = *limits.started.get().unwrap();
    assert_eq!(production.production_end(), start + Duration::from_secs(25));
    assert!(production.production_available_at(start + Duration::from_secs(24)));
    assert!(!production.production_available_at(start + Duration::from_secs(25)));
    production.production_deadline = Some(Instant::now());
    let mut run = Run::new(&production);
    assert!(!run.page(0, 0).unwrap());
    run.check().unwrap();
    assert!(run.remaining() > Duration::from_secs(29));
    assert!(!limits.cancellation.is_cancelled());
    assert!(Run::new(&limits).page(0, 0).unwrap());
    assert_eq!(production.pages_remaining(), limits.pages - 1);
}

#[test]
fn caps_and_capture_share_fixed_start_pages_and_never_extend_deadlines() {
    let mut limits = RunLimits::default();
    let mut budget = Budget::default();
    budget.charge(1, 2, 3, 4).unwrap();
    let work = budget.work;
    let resident = budget.reserve(8).unwrap();
    let cancel = budget.cancellation();
    let deadline = Instant::now() + Duration::from_secs(2);
    limits.constrain_deadline(deadline, &mut budget).unwrap();
    let production = limits.for_production();
    let start = *limits.started.get().unwrap();
    assert_eq!(start + limits.elapsed, deadline);
    assert!(production.production_end() > start);
    assert!(production.production_end() < deadline);
    assert!(Run::new(&production).page(0, 0).unwrap());
    let capture = production.reserve_capture((100, 1000), &budget).unwrap();
    let caps = capture.with_task_caps(80, 800).for_production();
    assert_eq!(caps.started.get(), limits.started.get());
    assert!(Run::new(&caps).deadline <= deadline);
    assert!(caps.production_end() <= production.production_end());
    assert_eq!(caps.pages_remaining(), limits.pages - 1);
    assert_eq!(
        (
            budget.work.sql,
            budget.work.rows,
            budget.work.cells,
            budget.work.raw_bytes
        ),
        (work.sql, work.rows, work.cells, work.raw_bytes)
    );
    cancel.store(true, Ordering::Relaxed);
    assert_eq!(budget.check(), Err(ManagementError::Budget));
    limits.cancellation.cancel();
    assert_eq!(caps.check(), Err(ManagementError::Budget));
    drop(resident);
}

#[tokio::test]
async fn higher_invocation_limits_cannot_refill_a_persisted_small_task() {
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared(budget::SCHEMA).await.unwrap();
    let original = RunLimits {
        total_rows: 8,
        total_bytes: 48,
        ..Default::default()
    };
    let run = task_run(&db, &original).await.unwrap();
    budget::charge(&db, &run, 2, 40).await.unwrap();
    let higher = RunLimits::default();
    let resumed = task_run(&db, &higher).await.unwrap();
    assert_eq!((resumed.total_rows, resumed.total_bytes), (8, 48));
    budget::charge(&db, &resumed, 1, 8).await.unwrap();
    assert_eq!(
        budget::charge(&db, &resumed, 0, 1).await,
        Err(ManagementError::Budget)
    );
    let another = RunLimits::default();
    let reopened = task_run(&db, &another).await.unwrap();
    assert_eq!(
        budget::charge(&db, &reopened, 0, 1).await,
        Err(ManagementError::Budget)
    );
    let row = one(
        &db,
        "SELECT rows_limit,bytes_limit,rows_used,bytes_used,pages_used FROM task_budget",
        vec![],
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(get::<i64>(&row, "rows_limit").unwrap(), 8);
    assert_eq!(get::<i64>(&row, "bytes_limit").unwrap(), 48);
    assert_eq!(get::<i64>(&row, "rows_used").unwrap(), 3);
    assert_eq!(get::<i64>(&row, "bytes_used").unwrap(), 48);
    assert_eq!(get::<i64>(&row, "pages_used").unwrap(), 2);
}
