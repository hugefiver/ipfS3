use super::*;

async fn stopped_capture(deadline_stop: bool) {
    use ipfs_s3_gateway::store::pinning::management::transaction::{CancellationToken, StopCause};
    use std::{
        future::Future,
        task::Poll,
        time::{Duration, Instant},
    };
    let (fixture, _) = fixture(false).await;
    let before = fixture.frame().await;
    let work = records::SharedDecodeBudget::default();
    let cancellation = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(1);
    let operation = db_snapshot::collect_with_control(
        &fixture.db,
        &fixture.config,
        &fixture.request,
        deadline,
        cancellation.clone(),
        work.clone(),
    );
    tokio::pin!(operation);
    // Stop at an actual awaited capture boundary, after native work began and
    // before the capture can be returned to planning. No paused Tokio clock.
    std::future::poll_fn(|cx| match operation.as_mut().poll(cx) {
        Poll::Ready(_) => panic!("capture finished before the controlled stop"),
        Poll::Pending if work.lock().unwrap().usage() != (0, 0) => Poll::Ready(()),
        Poll::Pending => Poll::Pending,
    })
    .await;
    assert!(Instant::now() < deadline);
    if deadline_stop {
        tokio::time::sleep_until(deadline.into()).await;
    } else {
        cancellation.cancel();
    }
    let failure = match operation.await {
        Err(db_snapshot::ControlledCaptureError::Stopped(failure)) => failure,
        _ => panic!("unfinished capture must not become a normal result"),
    };
    assert!(failure.rollback_confirmed);
    assert_eq!(
        failure.cause,
        if deadline_stop {
            StopCause::Deadline
        } else {
            StopCause::Cancelled
        }
    );
    assert!(fixture.frame().await == before);
    // A new transaction on the original pool remains usable after cleanup.
    db_snapshot::collect_with_config(&fixture.db, &fixture.config, &fixture.request)
        .await
        .unwrap();
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn unfinished_controlled_capture_acknowledges_cancelled_rollback() {
    stopped_capture(false).await;
}

#[tokio::test]
async fn unfinished_controlled_capture_acknowledges_deadline_rollback() {
    stopped_capture(true).await;
}

#[tokio::test]
async fn controlled_capture_preserves_snapshot_failure_and_closes_its_transaction() {
    let (fixture, _) = fixture(false).await;
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    let expected = db_snapshot::collect_with_config(&db, &fixture.config, &fixture.request)
        .await
        .err()
        .expect("uninitialized capture must fail");
    let failed = db_snapshot::collect_with_control(
        &db,
        &fixture.config,
        &fixture.request,
        std::time::Instant::now() + std::time::Duration::from_secs(1),
        Default::default(),
        Default::default(),
    )
    .await;
    assert!(
        matches!(failed, Err(db_snapshot::ControlledCaptureError::Snapshot(error)) if error == expected)
    );
    let tx = db.begin().await.unwrap();
    tx.rollback().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn expired_planning_slice_settles_capture_once_and_remains_resumable() {
    let (fixture, auth) = fixture(false).await;
    let (local, mut budget, _resident) = capture_with_independent_copy(&fixture).await;
    let mut first = RunLimits::default();
    first.pages = 1;
    let mut checkpoint = PlanningCheckpoint::create(
        &fixture.file("capture-finalization"),
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &local,
        &first,
    )
    .await
    .unwrap();
    let caps = checkpoint.reserve_capture(&budget, &first).unwrap();
    assert!(matches!(
        checkpoint.advance(&fixture.db, &caps).await.unwrap(),
        PlanningProgress::Incomplete
    ));
    let mut stopped = RunLimits::default();
    stopped.pages = 512;
    stopped.elapsed = std::time::Duration::ZERO;
    let caps = checkpoint.reserve_capture(&budget, &stopped).unwrap();
    let advanced = checkpoint.advance(&fixture.db, &caps).await;
    let before = checkpoint.total_work().await.unwrap();
    let captured = budget.work;
    assert!(before.rows + captured.rows < stopped.total_rows);
    assert!(before.bytes + captured.raw_bytes + captured.copied_bytes < stopped.total_bytes);
    budget.check().unwrap();
    let accounted = checkpoint.account_capture(&mut budget, &stopped).await;
    let paid = checkpoint.total_work().await.unwrap();
    let retained = checkpoint.close().await.unwrap();
    assert!(matches!(advanced.unwrap(), PlanningProgress::Incomplete));
    accounted.unwrap();
    budget.check().unwrap();
    assert_eq!(stopped.pages_remaining(), 0);
    assert_eq!(paid.rows - before.rows, captured.rows);
    assert_eq!(
        paid.bytes - before.bytes,
        captured.raw_bytes + captured.copied_bytes
    );
    assert_eq!(paid.pages - before.pages, captured.pages);

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
    let before_repeat = resumed.total_work().await.unwrap();
    resumed
        .account_capture(&mut budget, &stopped)
        .await
        .unwrap();
    assert_eq!(resumed.total_work().await.unwrap(), before_repeat);
    let mut next = RunLimits::default();
    next.pages = 1;
    assert!(matches!(
        resumed.advance(&fixture.db, &next).await.unwrap(),
        PlanningProgress::Incomplete
    ));
    assert!(resumed.total_work().await.unwrap().pages > before_repeat.pages);
    resumed.close().await.unwrap();
    assert_eq!(confirmations(&fixture).await, 0);
    assert_eq!(fixture.remote_calls().await, 0);
}
