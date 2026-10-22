use super::*;
use crate::store::pinning::management::native::meter::Budget;

#[test]
fn production_stop_leaves_hard_settlement_and_final_transfer_available() {
    let limits = RunLimits::default();
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
