use super::*;
use sea_orm::{Statement, sqlx::Connection};

async fn fixture() -> (tempfile::TempDir, DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("reuse.sqlite").display()
    );
    let db = crate::store::connect_database(&url).await.unwrap();
    db.execute_unprepared("CREATE TABLE reuse_marker(id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    (directory, db)
}

async fn insert(tx: &mut ManagementTransaction, id: i64) {
    tx.run(move |tx, _| {
        Box::pin(async move {
            tx.execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "INSERT INTO reuse_marker VALUES($1)",
                [id.into()],
            ))
            .await
            .map_err(|_| ManagementError::Database)?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn count(db: &DatabaseConnection) -> i64 {
    db.query_one(Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT count(*) AS n FROM reuse_marker",
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "n")
    .unwrap()
}

#[tokio::test]
async fn commits_and_normal_rollback_retain_connection_cache_with_fresh_controls() {
    let (_directory, db) = fixture().await;
    let mut slot = ConfirmationEngine::new(&db);
    let first_cancel = CancellationToken::new();
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, first_cancel.clone())
        .await
        .unwrap();
    let old = tx
        .run(|tx, deadline| {
            Box::pin(async move {
                tx.execute_unprepared("CREATE TEMP TABLE invocation_connection(id INTEGER)")
                    .await
                    .map_err(|_| ManagementError::Database)?;
                Ok(deadline)
            })
        })
        .await
        .unwrap();
    insert(&mut tx, 1).await;
    tx.commit().await.unwrap();
    assert!(slot.enabled);
    let pool = slot
        .engine
        .as_ref()
        .unwrap()
        .db
        .get_sqlite_connection_pool();
    assert!(pool.acquire().await.unwrap().cached_statements_size() > 0);
    first_cancel.cancel();
    old.shorten(Duration::ZERO);

    let second_cancel = CancellationToken::new();
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, second_cancel.clone())
        .await
        .unwrap();
    let second = tx.run(|tx, deadline| Box::pin(async move {
        tx.execute_unprepared("INSERT INTO invocation_connection VALUES(1)")
            .await.map_err(|_| ManagementError::Database)?;
        tx.query_one(Statement::from_string(DatabaseBackend::Sqlite,
            "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<5000) SELECT sum(x) FROM n"))
            .await.map_err(|_| ManagementError::Database)?;
        Ok(deadline)
    })).await.unwrap();
    insert(&mut tx, 2).await;
    tx.rollback().await.unwrap();
    assert!(slot.enabled);
    second_cancel.cancel();
    second.shorten(Duration::ZERO);
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    tx.run(|tx, _| {
        Box::pin(async move {
            let row = tx
                .query_one(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "SELECT count(*) AS n FROM invocation_connection",
                ))
                .await
                .map_err(|_| ManagementError::Database)?
                .unwrap();
            assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
            Ok(())
        })
    })
    .await
    .unwrap();
    insert(&mut tx, 3).await;
    tx.commit().await.unwrap();
    assert_eq!(count(&db).await, 2);
    assert!(slot.enabled);
}

async fn stopped_reused_writer(cancelled: bool, replace_connection: bool) {
    let (_directory, db) = fixture().await;
    let mut slot = ConfirmationEngine::new(&db);
    let tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    if replace_connection {
        slot.engine
            .as_ref()
            .unwrap()
            .db
            .get_sqlite_connection_pool()
            .acquire()
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
    }
    let cancel = CancellationToken::new();
    let mut tx = slot.begin(FINAL_WRITE_LIMIT, cancel.clone()).await.unwrap();
    insert(&mut tx, 1).await;
    let (started, ready) = tokio::sync::oneshot::channel();
    let work = tx.run(move |tx, deadline| Box::pin(async move {
        if !cancelled { deadline.shorten(Duration::from_millis(80)); }
        started.send(()).unwrap();
        tx.query_one(Statement::from_string(DatabaseBackend::Sqlite,
            "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n"))
            .await.map_err(|_| ManagementError::Database)?;
        Ok(())
    }));
    let trigger = async {
        ready.await.unwrap();
        if cancelled {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel.cancel();
        }
    };
    let (result, ()) = tokio::join!(tokio::time::timeout(Duration::from_secs(3), work), trigger);
    let failure = result.unwrap().unwrap_err();
    assert_eq!(
        failure.cause,
        if cancelled {
            StopCause::Cancelled
        } else {
            StopCause::Deadline
        }
    );
    assert!(failure.rollback_confirmed);
    tx.rollback().await.unwrap();
    assert!(!slot.enabled);
    assert!(slot.engine.is_none());
    tokio::time::timeout(
        Duration::from_secs(1),
        db.execute_unprepared("INSERT INTO reuse_marker VALUES(2)"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(count(&db).await, 1);
}

#[tokio::test]
async fn new_cancellation_interrupts_reused_writer() {
    stopped_reused_writer(true, false).await;
}

#[tokio::test]
async fn new_short_deadline_interrupts_reused_writer() {
    stopped_reused_writer(false, false).await;
}

#[tokio::test]
async fn replacement_physical_connection_inherits_current_deadline_slot() {
    stopped_reused_writer(false, true).await;
}

#[tokio::test]
async fn work_error_and_acknowledged_public_style_rollback_do_not_reenable_slot() {
    let (_directory, db) = fixture().await;
    let mut slot = ConfirmationEngine::new(&db);
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    insert(&mut tx, 1).await;
    let failure = tx
        .run::<(), _>(|_, _| Box::pin(async { Err(ManagementError::Changed) }))
        .await
        .unwrap_err();
    assert!(failure.rollback_confirmed);
    tx.rollback().await.unwrap();
    assert!(!slot.enabled);
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    insert(&mut tx, 2).await;
    tx.commit().await.unwrap();
    assert!(!slot.enabled);
    assert!(slot.engine.is_none());
    assert_eq!(count(&db).await, 1);
}

#[tokio::test]
async fn unknown_commit_with_successful_business_readback_never_recycles() {
    let (_directory, db) = fixture().await;
    let mut slot = ConfirmationEngine::new(&db);
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    insert(&mut tx, 1).await;
    tx.run(|tx, _| {
        Box::pin(async move {
            tx.execute_unprepared("COMMIT")
                .await
                .map_err(|_| ManagementError::Database)?;
            Ok(())
        })
    })
    .await
    .unwrap();
    assert!(!tx.commit().await.unwrap_err().rollback_confirmed);
    assert_eq!(count(&db).await, 1);
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    insert(&mut tx, 2).await;
    tx.commit().await.unwrap();
    assert!(!slot.enabled);
    assert!(slot.engine.is_none());
}

#[tokio::test]
async fn dropped_completion_cannot_recycle_even_after_background_commit_ack() {
    let (_directory, db) = fixture().await;
    let mut slot = ConfirmationEngine::new(&db);
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    insert(&mut tx, 1).await;
    let shared = tx.shared.clone();
    let engine = shared.engine.lock().await;
    {
        let completion = tx.commit();
        tokio::pin!(completion);
        tokio::select! {
            result = &mut completion => panic!("completion bypassed engine ownership barrier: {result:?}"),
            () = shared.done.cancelled() => {},
        }
        // Poll past done so the live caller waits at the ownership barrier.
        assert!(futures_util::poll!(completion.as_mut()).is_pending());
    }
    drop(engine);
    assert!(matches!(
        shared.finished().await,
        lifecycle::Outcome::Committed
    ));
    assert!(!slot.enabled);
    assert!(slot.engine.is_none());
    assert!(shared.engine.lock().await.is_some());
    assert_eq!(count(&db).await, 1);
}

#[tokio::test]
async fn cancellation_at_reclaim_barrier_discards_acknowledged_engine() {
    let (_directory, db) = fixture().await;
    let mut slot = ConfirmationEngine::new(&db);
    let cancel = CancellationToken::new();
    let tx = slot.begin(FINAL_WRITE_LIMIT, cancel.clone()).await.unwrap();
    let shared = tx.shared.clone();
    let engine = shared.engine.lock().await;
    {
        let completion = tx.commit();
        tokio::pin!(completion);
        tokio::select! {
            result = &mut completion => panic!("completion bypassed ownership barrier: {result:?}"),
            () = shared.done.cancelled() => {},
        }
        cancel.cancel();
        drop(engine);
        completion.await.unwrap();
    }
    assert!(!slot.enabled);
    assert!(slot.engine.is_none());
}

#[tokio::test]
async fn dropped_work_requires_ordered_cleanup_and_never_recycles() {
    let (_directory, db) = fixture().await;
    let mut slot = ConfirmationEngine::new(&db);
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    insert(&mut tx, 1).await;
    {
        let work = tx.run(|_, _| Box::pin(std::future::pending::<Result<()>>()));
        tokio::pin!(work);
        assert!(futures_util::poll!(work.as_mut()).is_pending());
    }
    tx.rollback().await.unwrap();
    assert!(!slot.enabled);
    tokio::time::timeout(
        Duration::from_secs(1),
        db.execute_unprepared("INSERT INTO reuse_marker VALUES(2)"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(count(&db).await, 1);
}

#[tokio::test]
async fn dropped_handle_keeps_cleanup_with_watchdog() {
    let (_directory, db) = fixture().await;
    let mut slot = ConfirmationEngine::new(&db);
    let mut tx = slot
        .begin(FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    insert(&mut tx, 1).await;
    let shared = tx.shared.clone();
    drop(tx);
    let failure = tokio::time::timeout(Duration::from_secs(3), shared.finished())
        .await
        .unwrap()
        .failure();
    assert!(failure.rollback_confirmed);
    assert!(!slot.enabled);
    assert!(slot.engine.is_none());
    assert!(shared.engine.lock().await.is_some());
    assert_eq!(count(&db).await, 0);
}

#[path = "reuse_schema_tests.rs"]
mod schema;
