use super::*;
use crate::store::pinning::management::{records, transaction};
use records::DecodeWork;

#[tokio::test]
async fn rebuilt_store_unpin_writer_rejects_invalid_structure() {
    for (change, repair) in [
        (
            "UPDATE pin_management_readiness SET state='backfill' WHERE id=1",
            "UPDATE pin_management_readiness SET state='ready' WHERE id=1",
        ),
        (
            "CREATE TABLE saved_versions AS SELECT * FROM seaql_migrations; DELETE FROM seaql_migrations WHERE version=(SELECT max(version) FROM seaql_migrations)",
            "INSERT OR IGNORE INTO seaql_migrations SELECT * FROM saved_versions; DROP TABLE saved_versions",
        ),
        (
            "CREATE TEMP TABLE PiN_PrOvIdEr_RoUtEs(invalid TEXT)",
            "DROP TABLE temp.PiN_PrOvIdEr_RoUtEs",
        ),
        (
            "CREATE TEMP VIEW ObJeCtS AS SELECT json_extract('invalid','$') AS invalid",
            "DROP VIEW temp.ObJeCtS",
        ),
    ] {
        let (fixture, claimed) =
            fixture::ordinary_with_database(fixture::single_connection().await).await;
        fixture::warm(&fixture.store).await;
        fixture.store.db().execute_unprepared(change).await.unwrap();
        let clone = Store::new(fixture.store.db().clone());
        let before = NativeEffects::read(&fixture.store, &claimed.model.id).await;
        assert!(
            prepare_unpin_writer(&clone, &claimed, 1).await.is_err(),
            "{change}"
        );
        fixture::assert_unchanged_and_released(&fixture, &claimed, &before).await;
        clone.db().execute_unprepared(repair).await.unwrap();
        assert!(!prepare_unpin_writer(&clone, &claimed, 1).await.unwrap());
    }
}

#[tokio::test]
async fn unpin_writer_conflict_and_failed_claim_fence_leave_no_effect() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("unpin.sqlite")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let db = sea_orm::Database::connect(&url).await.unwrap();
    let (fixture, claimed) = fixture::ordinary_with_database(db).await;
    fixture::warm(&fixture.store).await;
    let before = NativeEffects::read(&fixture.store, &claimed.model.id).await;
    let other = sea_orm::Database::connect(&url).await.unwrap();
    let writer = other.begin().await.unwrap();
    writer
        .execute_unprepared("UPDATE remote_pins SET epoch=epoch")
        .await
        .unwrap();
    assert!(
        prepare_unpin_writer(&fixture.store, &claimed, 1)
            .await
            .is_err()
    );
    writer.rollback().await.unwrap();
    fixture::assert_unchanged_and_released(&fixture, &claimed, &before).await;
    fixture
        .store
        .db()
        .execute_unprepared(
            "CREATE TRIGGER reject_claim_writer BEFORE UPDATE ON pin_jobs \
         BEGIN SELECT RAISE(ABORT,'claim writer unavailable'); END",
        )
        .await
        .unwrap();
    assert!(
        prepare_unpin_writer(&fixture.store, &claimed, 1)
            .await
            .is_err()
    );
    fixture
        .store
        .db()
        .execute_unprepared("DROP TRIGGER reject_claim_writer")
        .await
        .unwrap();
    fixture::assert_unchanged_and_released(&fixture, &claimed, &before).await;
    assert!(
        !prepare_unpin_writer(&fixture.store, &claimed, 1)
            .await
            .unwrap()
    );
    other.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_a_pending_unpin_checkout_releases_it_without_effect() {
    let (fixture, claimed) =
        fixture::ordinary_with_database(fixture::single_connection().await).await;
    fixture::warm(&fixture.store).await;
    let before = NativeEffects::read(&fixture.store, &claimed.model.id).await;
    let checkout = fixture.store.db().begin().await.unwrap();
    let cancel = transaction::CancellationToken::new();
    let work = Arc::new(std::sync::Mutex::new(records::ReadSession::for_task(
        crate::store::pinning::management::native::meter::Budget::default(),
        u64::MAX,
        u64::MAX,
        cancel.clone(),
    )));
    let mut pending = Box::pin(crate::pinning::worker::prepare_unpin_writer_with_work(
        &fixture.store,
        &claimed,
        1,
        work.clone(),
    ));
    assert!(futures_util::poll!(&mut pending).is_pending());
    assert!(work.lock().unwrap().storage_usage().sql > 0);
    cancel.cancel();
    checkout.rollback().await.unwrap();
    assert!(matches!(
        pending.await,
        Err(crate::error::AppError::ManagementBudget)
    ));
    fixture::assert_unchanged_and_released(&fixture, &claimed, &before).await;
    assert!(
        !prepare_unpin_writer(&fixture.store, &claimed, 1)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn sqlite_interrupt_after_epoch_fence_rolls_back_unpin_and_releases_transaction() {
    use std::sync::atomic::AtomicBool;
    let mut db = fixture::single_connection().await;
    let armed = Arc::new(AtomicBool::new(false));
    let interrupt = Arc::new(AtomicBool::new(false));
    let interrupted = Arc::new(AtomicBool::new(false));
    let handler = interrupt.clone();
    let observed = interrupted.clone();
    let mut connection = db.get_sqlite_connection_pool().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_progress_handler(1, move || {
            if handler.swap(false, Ordering::AcqRel) {
                observed.store(true, Ordering::Release);
                false
            } else {
                true
            }
        });
    drop(connection);
    let enabled = armed.clone();
    db.set_metric_callback(move |info| {
        if enabled.load(Ordering::Acquire)
            && info.statement.sql.starts_with("UPDATE \"remote_pins\"")
        {
            interrupt.store(true, Ordering::Release);
        }
    });
    let (fixture, claimed) = fixture::ordinary_with_database(db).await;
    fixture::warm(&fixture.store).await;
    let before = NativeEffects::read(&fixture.store, &claimed.model.id).await;
    armed.store(true, Ordering::Release);
    assert!(
        prepare_unpin_writer(&fixture.store, &claimed, 1)
            .await
            .is_err()
    );
    armed.store(false, Ordering::Release);
    assert!(interrupted.load(Ordering::Acquire));
    fixture::assert_unchanged_and_released(&fixture, &claimed, &before).await;
    let work = SharedDecodeBudget::default();
    let tx = fixture
        .store
        .begin_runtime(DecodeWork::shared(&work))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        !prepare_unpin_writer(&fixture.store, &claimed, 1)
            .await
            .unwrap()
    );
}
