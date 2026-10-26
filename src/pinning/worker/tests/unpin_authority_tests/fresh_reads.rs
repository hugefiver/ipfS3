use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy)]
enum FreshRead {
    AuthorityBinding,
    Ledger,
}

async fn interrupt_fresh_effect_read(target: FreshRead) {
    let mut db = fixture::single_connection().await;
    let armed = Arc::new(AtomicBool::new(false));
    let cleanup_read = Arc::new(AtomicBool::new(false));
    let fresh_binding = Arc::new(AtomicBool::new(false));
    let reached = Arc::new(AtomicBool::new(false));
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
    let completed_cleanup = cleanup_read.clone();
    let binding = fresh_binding.clone();
    let hit = reached.clone();
    db.set_metric_callback(move |info| {
        if !enabled.load(Ordering::Acquire) || info.failed {
            return;
        }
        let sql = &info.statement.sql;
        // Fault injection follows completed production reads, not query totals.
        if sql.starts_with("SELECT \"remote_pin_ledger\".") {
            completed_cleanup.store(true, Ordering::Release);
        }
        if completed_cleanup.load(Ordering::Acquire)
            && sql.starts_with("SELECT database_scope FROM pin_management_records")
        {
            binding.store(true, Ordering::Release);
            if matches!(target, FreshRead::AuthorityBinding) {
                hit.store(true, Ordering::Release);
                interrupt.store(true, Ordering::Release);
            }
        }
        if matches!(target, FreshRead::Ledger)
            && binding.load(Ordering::Acquire)
            && sql.starts_with("SELECT COUNT(*) AS count,")
            && sql.contains("FROM \"remote_pin_ledger\"")
        {
            hit.store(true, Ordering::Release);
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
    assert!(cleanup_read.load(Ordering::Acquire));
    assert!(fresh_binding.load(Ordering::Acquire));
    assert!(reached.load(Ordering::Acquire));
    assert!(interrupted.load(Ordering::Acquire));
    fixture::assert_unchanged_and_released(&fixture, &claimed, &before).await;
    assert!(
        !prepare_unpin_writer(&fixture.store, &claimed, 1)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn effect_publication_loads_fresh_authority_and_database_binding_after_cleanup() {
    interrupt_fresh_effect_read(FreshRead::AuthorityBinding).await;
}

#[tokio::test]
async fn effect_publication_reads_its_own_fresh_ledger_row_after_cleanup() {
    interrupt_fresh_effect_read(FreshRead::Ledger).await;
}
