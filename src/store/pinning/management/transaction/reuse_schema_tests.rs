use super::*;
use crate::store::pinning::management::native::scope::writer_boundary;

async fn warm(slot: &mut ConfirmationEngine<'_>) {
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    tx.run(|tx, _| Box::pin(async move { writer_boundary(tx).await }))
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn hot_writer_statement_reprepares_valid_ddl_and_rejects_missing_dependencies() {
    let (_directory, db) = fixture().await;
    db.execute_unprepared("CREATE TABLE pin_provider_routes(provider TEXT PRIMARY KEY)")
        .await
        .unwrap();
    let mut slot = ConfirmationEngine::new(&db);
    warm(&mut slot).await;
    db.execute_unprepared("ALTER TABLE pin_provider_routes ADD COLUMN fresh INTEGER")
        .await
        .unwrap();
    warm(&mut slot).await;
    assert!(slot.enabled);
    db.execute_unprepared("CREATE TABLE route_dependency(id TEXT); CREATE TRIGGER route_dependency_check BEFORE UPDATE ON pin_provider_routes BEGIN INSERT INTO route_dependency VALUES(NEW.provider); END")
        .await.unwrap();
    warm(&mut slot).await;
    db.execute_unprepared("DROP TABLE route_dependency")
        .await
        .unwrap();
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    let failure = tx
        .run(|tx, _| Box::pin(async move { writer_boundary(tx).await }))
        .await
        .unwrap_err();
    assert!(failure.rollback_confirmed);
    tx.rollback().await.unwrap();
    assert!(!slot.enabled);
}

#[tokio::test]
async fn hot_writer_statement_rejects_missing_route_table_and_provider_column() {
    let (_directory, db) = fixture().await;
    db.execute_unprepared("CREATE TABLE pin_provider_routes(provider TEXT PRIMARY KEY)")
        .await
        .unwrap();
    for ddl in [
        "ALTER TABLE pin_provider_routes RENAME COLUMN provider TO missing_provider",
        "DROP TABLE pin_provider_routes",
    ] {
        let mut slot = ConfirmationEngine::new(&db);
        warm(&mut slot).await;
        db.execute_unprepared(ddl).await.unwrap();
        let mut tx = slot
            .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
            .await
            .unwrap();
        let failure = tx
            .run(|tx, _| Box::pin(async move { writer_boundary(tx).await }))
            .await
            .unwrap_err();
        assert!(failure.rollback_confirmed);
        tx.rollback().await.unwrap();
        assert!(!slot.enabled);
        if ddl.starts_with("ALTER") {
            db.execute_unprepared(
                "ALTER TABLE pin_provider_routes RENAME COLUMN missing_provider TO provider",
            )
            .await
            .unwrap();
        }
    }
}

#[tokio::test]
async fn failed_ordered_barrier_cannot_qualify_normal_rollback_for_reuse() {
    let (_directory, db) = fixture().await;
    let mut slot = ConfirmationEngine::new(&db);
    let tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let interrupt_once = Arc::new(AtomicBool::new(false));
    let handler = interrupt_once.clone();
    let mut connection = slot
        .engine
        .as_ref()
        .unwrap()
        .db
        .get_sqlite_connection_pool()
        .acquire()
        .await
        .unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_progress_handler(1, move || !handler.swap(false, Ordering::AcqRel));
    drop(connection);
    let tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    // Only the ordered barrier is interrupted; rollback can still acknowledge.
    interrupt_once.store(true, Ordering::Release);
    tx.rollback().await.unwrap();
    assert!(!slot.enabled);
    assert!(slot.engine.is_none());
}
