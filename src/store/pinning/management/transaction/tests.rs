//! SQLite engine regressions for the Stage 7.5 transaction integration binary.
use ipfs_s3_gateway::store::{
    self,
    pinning::management::{
        ManagementError,
        transaction::{CancellationToken, ManagementTransaction, StopCause},
    },
};
use sea_orm::{ConnectionTrait, Statement};
use sea_orm::{DatabaseBackend, DatabaseConnection};
use std::time::{Duration, Instant};

const LONG_SQL: &str = "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) AS total FROM n";

async fn sqlite_file(db: &impl ConnectionTrait) -> Result<String, ManagementError> {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT file FROM pragma_database_list WHERE name='main'",
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Database)?;
    row.try_get("", "file")
        .map_err(|_| ManagementError::Database)
}

async fn fixture(memory: bool) -> (tempfile::TempDir, DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let url = if memory {
        "sqlite::memory:".into()
    } else {
        format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("transaction.db").display()
        )
    };
    let db = store::connect_database(&url).await.unwrap();
    db.execute_unprepared("CREATE TABLE marker(id INTEGER PRIMARY KEY); CREATE TABLE operation_scope(token BLOB NOT NULL); INSERT INTO operation_scope VALUES(randomblob(16))")
        .await
        .unwrap();
    (directory, db)
}

async fn assert_released(db: &DatabaseConnection) {
    tokio::time::timeout(
        Duration::from_secs(2),
        db.execute_unprepared("INSERT INTO marker VALUES(2)"),
    )
    .await
    .expect("writer lock must already be released")
    .unwrap();
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT count(*) AS n FROM marker WHERE id=1",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
}

#[tokio::test]
async fn sqlite_engine_deadline_rolls_back_writer_in_file_and_named_memory() {
    for memory in [false, true] {
        let (_directory, db) = fixture(memory).await;
        let identity = sqlite_file(&db).await.unwrap();
        let scope = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT hex(token) AS token FROM operation_scope",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "token")
            .unwrap();
        let mut tx =
            ManagementTransaction::begin(&db, Duration::from_millis(180), CancellationToken::new())
                .await
                .unwrap();
        let bounded = tx
            .run(|tx, _| {
                Box::pin(async move {
                    let identity = sqlite_file(tx).await?;
                    let row = tx
                        .query_one(Statement::from_string(
                            DatabaseBackend::Sqlite,
                            "SELECT hex(token) AS token FROM operation_scope",
                        ))
                        .await
                        .map_err(|_| ManagementError::Database)?
                        .ok_or(ManagementError::Database)?;
                    Ok((
                        identity,
                        row.try_get::<String>("", "token")
                            .map_err(|_| ManagementError::Database)?,
                    ))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            bounded,
            (identity, scope),
            "bounded pool must open the original file or exact named-memory database"
        );
        let started = Instant::now();
        let failure = tx
            .run(|tx, _| {
                Box::pin(async move {
                    tx.execute_unprepared("INSERT INTO marker VALUES(1)")
                        .await
                        .map_err(|_| ManagementError::Database)?;
                    tx.query_one(Statement::from_string(DatabaseBackend::Sqlite, LONG_SQL))
                        .await
                        .map_err(|_| ManagementError::Database)?;
                    Ok(())
                })
            })
            .await
            .unwrap_err();
        assert_eq!(failure.cause, StopCause::Deadline);
        assert!(failure.rollback_confirmed);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_released(&db).await;
    }
}

#[tokio::test]
async fn sqlite_external_cancel_interrupts_actual_sql_before_rollback() {
    for memory in [false, true] {
        let (_directory, db) = fixture(memory).await;
        let cancel = CancellationToken::new();
        let mut tx = ManagementTransaction::begin(&db, Duration::from_secs(10), cancel.clone())
            .await
            .unwrap();
        let (entered, running) = tokio::sync::oneshot::channel();
        let operation = tx.run(|tx, _| {
            Box::pin(async move {
                tx.execute_unprepared("INSERT INTO marker VALUES(1)")
                    .await
                    .map_err(|_| ManagementError::Database)?;
                entered.send(()).unwrap();
                tx.query_one(Statement::from_string(DatabaseBackend::Sqlite, LONG_SQL))
                    .await
                    .map_err(|_| ManagementError::Database)?;
                Ok(())
            })
        });
        let cancellation = async {
            running.await.unwrap();
            tokio::time::sleep(Duration::from_millis(80)).await;
            cancel.cancel();
        };
        let started = Instant::now();
        let (result, ()) = tokio::join!(operation, cancellation);
        let failure = result.unwrap_err();
        assert_eq!(failure.cause, StopCause::Cancelled);
        assert!(failure.rollback_confirmed);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "external cancellation must not wait for the ten-second budget"
        );
        assert_released(&db).await;
    }
}

#[tokio::test]
async fn sqlite_competing_writer_acquires_lock_after_cancelled_sql_is_rolled_back() {
    let (_directory, db) = fixture(false).await;
    let cancel = CancellationToken::new();
    let mut tx = ManagementTransaction::begin(&db, Duration::from_secs(10), cancel.clone())
        .await
        .unwrap();
    let (entered, running) = tokio::sync::oneshot::channel();
    let (competing, competition) = tokio::sync::oneshot::channel();
    let operation = tx.run(|tx, _| {
        Box::pin(async move {
            tx.execute_unprepared("INSERT INTO marker VALUES(1)")
                .await
                .map_err(|_| ManagementError::Database)?;
            entered.send(()).unwrap();
            tx.query_one(Statement::from_string(DatabaseBackend::Sqlite, LONG_SQL))
                .await
                .map_err(|_| ManagementError::Database)?;
            Ok(())
        })
    });
    let writer = async {
        running.await.unwrap();
        competing.send(()).unwrap();
        db.execute_unprepared("INSERT INTO marker VALUES(2)").await
    };
    let cancellation = async {
        competition.await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        cancel.cancel();
    };
    let started = Instant::now();
    let (result, written, ()) = tokio::join!(operation, writer, cancellation);
    let failure = result.unwrap_err();
    assert_eq!(failure.cause, StopCause::Cancelled);
    assert!(failure.rollback_confirmed);
    written.expect("a genuinely competing writer must resume after rollback");
    assert!(started.elapsed() < Duration::from_secs(2));
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT count(*) AS n FROM marker WHERE id=1",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
    db.execute_unprepared("INSERT INTO marker VALUES(3)")
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_lock_wait_is_finite_and_does_not_change_database_identity() {
    use sea_orm::TransactionTrait;
    let (_directory, db) = fixture(false).await;
    let owner = db.begin().await.unwrap();
    owner
        .execute_unprepared("INSERT INTO marker VALUES(1)")
        .await
        .unwrap();
    let mut tx =
        ManagementTransaction::begin(&db, Duration::from_millis(250), CancellationToken::new())
            .await
            .unwrap();
    let started = Instant::now();
    let failure = tx
        .run(|tx, _| {
            Box::pin(async move {
                tx.execute_unprepared("INSERT INTO marker VALUES(3)")
                    .await
                    .map_err(|_| ManagementError::Database)?;
                Ok(())
            })
        })
        .await
        .unwrap_err();
    assert!(failure.rollback_confirmed);
    assert!(started.elapsed() < Duration::from_secs(2));
    owner.rollback().await.unwrap();
    assert_released(&db).await;
}

#[tokio::test]
async fn sqlite_deadline_is_shared_and_can_only_shorten() {
    let (_directory, db) = fixture(false).await;
    let mut tx =
        ManagementTransaction::begin(&db, Duration::from_secs(10), CancellationToken::new())
            .await
            .unwrap();
    let insert = String::from("INSERT INTO marker VALUES(1)");
    tx.run_with(&insert, |tx, deadline, insert| {
        Box::pin(async move {
            tx.execute_unprepared(insert)
                .await
                .map_err(|_| ManagementError::Database)?;
            deadline.shorten(Duration::from_millis(90));
            deadline.shorten(Duration::from_secs(20));
            Ok(())
        })
    })
    .await
    .unwrap();
    let failure = tx
        .run(|tx, _| {
            Box::pin(async move {
                tx.query_one(Statement::from_string(DatabaseBackend::Sqlite, LONG_SQL))
                    .await
                    .map_err(|_| ManagementError::Database)?;
                Ok(())
            })
        })
        .await
        .unwrap_err();
    assert_eq!(failure.cause, StopCause::Deadline);
    assert!(failure.rollback_confirmed);
    assert_released(&db).await;
}

#[tokio::test]
async fn sqlite_cancellation_at_publication_boundary_prevents_commit() {
    let (_directory, db) = fixture(false).await;
    let cancel = CancellationToken::new();
    let mut tx = ManagementTransaction::begin(&db, Duration::from_secs(10), cancel.clone())
        .await
        .unwrap();
    tx.run(|tx, _| {
        Box::pin(async move {
            tx.execute_unprepared("INSERT INTO marker VALUES(1)")
                .await
                .map_err(|_| ManagementError::Database)?;
            Ok(())
        })
    })
    .await
    .unwrap();
    cancel.cancel();
    let failure = tx.commit().await.unwrap_err();
    assert_eq!(failure.cause, StopCause::Cancelled);
    assert!(failure.rollback_confirmed);
    assert_released(&db).await;
}
