use super::*;
use sea_orm::Statement;

async fn fixture() -> (tempfile::TempDir, DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("lifetime.sqlite").display()
    );
    let db = crate::store::connect_database(&url).await.unwrap();
    db.execute_unprepared("CREATE TABLE lifetime_marker(id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    (directory, db)
}

async fn no_business_writes(db: &DatabaseConnection) {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT count(*) AS n FROM lifetime_marker WHERE id IN (1,3)",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
}

async fn pending_business(cancelled: bool, shortened: bool) {
    let (_directory, db) = fixture().await;
    let cancel = CancellationToken::new();
    let mut tx = ManagementTransaction::begin(
        &db,
        if cancelled || shortened {
            Duration::from_secs(10)
        } else {
            Duration::from_millis(80)
        },
        cancel.clone(),
    )
    .await
    .unwrap();
    let (release, pending) = tokio::sync::oneshot::channel::<()>();
    let operation = tx.run(|tx, deadline| {
        Box::pin(async move {
            tx.execute_unprepared("INSERT INTO lifetime_marker VALUES(1)")
                .await
                .map_err(|_| ManagementError::Database)?;
            if shortened {
                deadline.shorten(Duration::from_millis(80));
            }
            let _ = pending.await;
            tx.execute_unprepared("INSERT INTO lifetime_marker VALUES(3)")
                .await
                .map_err(|_| ManagementError::Database)?;
            Ok(())
        })
    });
    let trigger = async {
        if cancelled {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel.cancel();
        }
    };
    let (result, ()) = tokio::join!(
        tokio::time::timeout(Duration::from_millis(700), operation),
        trigger
    );
    let result = match result {
        Ok(result) => result,
        Err(_) => {
            tx.rollback().await.unwrap();
            panic!(
                "deadline/cancel must finish cleanup even when business awaits a channel without running SQL"
            );
        }
    };
    let failure = result.unwrap_err();
    assert_eq!(
        failure.cause,
        if cancelled {
            StopCause::Cancelled
        } else {
            StopCause::Deadline
        }
    );
    assert!(failure.rollback_confirmed);
    assert!(
        release.send(()).is_err(),
        "expired business future cannot resume and publish late writes"
    );
    let late_commit = tx.commit().await.unwrap_err();
    assert!(late_commit.rollback_confirmed);
    tokio::time::timeout(
        Duration::from_millis(500),
        db.execute_unprepared("INSERT INTO lifetime_marker VALUES(2)"),
    )
    .await
    .expect("writer lock must already be released")
    .unwrap();
    no_business_writes(&db).await;
}

#[tokio::test]
async fn sqlite_deadline_stops_pending_business_and_releases_writer() {
    pending_business(false, false).await;
}

#[tokio::test]
async fn sqlite_cancel_stops_pending_business_and_releases_writer() {
    pending_business(true, false).await;
}

#[tokio::test]
async fn sqlite_shortened_deadline_wakes_business_and_watchdog() {
    pending_business(false, true).await;
}

async fn idle_transaction(cancelled: bool) {
    let (_directory, db) = fixture().await;
    let cancel = CancellationToken::new();
    let mut tx = ManagementTransaction::begin(
        &db,
        if cancelled {
            Duration::from_secs(10)
        } else {
            Duration::from_millis(80)
        },
        cancel.clone(),
    )
    .await
    .unwrap();
    tx.run(|tx, _| {
        Box::pin(async move {
            tx.execute_unprepared("INSERT INTO lifetime_marker VALUES(1)")
                .await
                .map_err(|_| ManagementError::Database)?;
            Ok(())
        })
    })
    .await
    .unwrap();
    let writer = db.execute_unprepared("INSERT INTO lifetime_marker VALUES(2)");
    tokio::pin!(writer);
    let wait = async {
        tokio::select! {
            result = &mut writer => Some(result),
            () = tokio::time::sleep(Duration::from_millis(500)) => None,
        }
    };
    let trigger = async {
        if cancelled {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel.cancel();
        }
    };
    let (result, ()) = tokio::join!(wait, trigger);
    match result {
        Some(result) => {
            result.unwrap();
        }
        None => {
            tx.rollback().await.unwrap();
            writer.await.unwrap();
            panic!("idle management transaction must roll back without another run/commit call");
        }
    }
    let late_commit = tx.commit().await.unwrap_err();
    assert_eq!(
        late_commit.cause,
        if cancelled {
            StopCause::Cancelled
        } else {
            StopCause::Deadline
        }
    );
    assert!(late_commit.rollback_confirmed);
    no_business_writes(&db).await;
}

#[tokio::test]
async fn sqlite_idle_transaction_expires_without_another_call() {
    idle_transaction(false).await;
}

#[tokio::test]
async fn sqlite_idle_transaction_cancels_without_another_call() {
    idle_transaction(true).await;
}

#[tokio::test]
async fn sqlite_unacknowledged_cleanup_never_claims_stopped() {
    let (_directory, db) = fixture().await;
    let mut tx =
        ManagementTransaction::begin(&db, Duration::from_millis(80), CancellationToken::new())
            .await
            .unwrap();
    tx.run(|tx, _| {
        Box::pin(async move {
            tx.execute_unprepared("INSERT INTO lifetime_marker VALUES(1)")
                .await
                .map_err(|_| ManagementError::Database)?;
            // End the engine transaction without acknowledging it through SQLx's
            // transaction manager, so its subsequent rollback cannot be confirmed.
            tx.execute_unprepared("ROLLBACK")
                .await
                .map_err(|_| ManagementError::Database)?;
            Ok(())
        })
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let failure = tx.commit().await.unwrap_err();
    assert_eq!(failure.cause, StopCause::Deadline);
    assert!(!failure.rollback_confirmed);
    db.execute_unprepared("INSERT INTO lifetime_marker VALUES(2)")
        .await
        .unwrap();
    no_business_writes(&db).await;
}

#[tokio::test]
async fn sqlite_abandoned_business_cannot_commit_its_unverified_prefix() {
    let (_directory, db) = fixture().await;
    let mut tx =
        ManagementTransaction::begin(&db, Duration::from_secs(10), CancellationToken::new())
            .await
            .unwrap();
    let (release, pending) = tokio::sync::oneshot::channel::<()>();
    let result = tokio::time::timeout(
        Duration::from_millis(50),
        tx.run(|tx, _| {
            Box::pin(async move {
                tx.execute_unprepared("INSERT INTO lifetime_marker VALUES(1)")
                    .await
                    .map_err(|_| ManagementError::Database)?;
                let _ = pending.await;
                Ok(())
            })
        }),
    )
    .await;
    assert!(result.is_err());
    assert!(release.send(()).is_err());
    let failure = tx.commit().await.unwrap_err();
    assert_eq!(failure.cause, StopCause::Cancelled);
    assert!(failure.rollback_confirmed);
    db.execute_unprepared("INSERT INTO lifetime_marker VALUES(2)")
        .await
        .unwrap();
    no_business_writes(&db).await;
}

#[tokio::test]
async fn sqlite_dropped_handle_releases_idle_writer_without_waiting_for_budget() {
    let (_directory, db) = fixture().await;
    let mut tx =
        ManagementTransaction::begin(&db, Duration::from_secs(10), CancellationToken::new())
            .await
            .unwrap();
    tx.run(|tx, _| {
        Box::pin(async move {
            tx.execute_unprepared("INSERT INTO lifetime_marker VALUES(1)")
                .await
                .map_err(|_| ManagementError::Database)?;
            Ok(())
        })
    })
    .await
    .unwrap();
    drop(tx);
    tokio::time::timeout(
        Duration::from_millis(500),
        db.execute_unprepared("INSERT INTO lifetime_marker VALUES(2)"),
    )
    .await
    .expect("watchdog owns cleanup after handle drop")
    .unwrap();
    no_business_writes(&db).await;
}
