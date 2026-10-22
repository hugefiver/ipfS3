use super::*;
use sea_orm::{ConnectOptions, Database, Statement};

#[tokio::test]
#[ignore = "requires a new parent-owned IPFS_S3_STAGE75_TRANSACTION_LIFETIME_DATABASE_URL schema"]
async fn postgres_idle_pending_and_uncertain_cleanup_hold_the_lifetime_boundary() {
    let url = std::env::var("IPFS_S3_STAGE75_TRANSACTION_LIFETIME_DATABASE_URL")
        .expect("parent supplies a new isolated schema");
    let mut options = ConnectOptions::new(url);
    options.max_connections(1);
    let db = Database::connect(options).await.unwrap();
    let row = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT current_schema() AS scope, to_regclass(current_schema() || '.stage75_lifetime_marker')::text AS existing"))
        .await.unwrap().unwrap();
    let scope = row.try_get::<String>("", "scope").unwrap();
    assert!(scope.starts_with("stage7_") || scope.starts_with("stage75_"));
    assert_ne!(
        scope, "stage7_transaction_75",
        "the occupied fixture is not authorized for this test"
    );
    assert!(
        row.try_get::<Option<String>>("", "existing")
            .unwrap()
            .is_none(),
        "require a fresh parent-owned fixture"
    );
    db.execute_unprepared(
        "CREATE TABLE stage75_lifetime_marker(id integer PRIMARY KEY,value integer NOT NULL)",
    )
    .await
    .unwrap();

    for (id, pending_business, cancelled) in [
        (1_i32, false, false),
        (2, false, true),
        (3, true, false),
        (4, true, true),
    ] {
        let cancel = CancellationToken::new();
        let mut tx = ManagementTransaction::begin(
            &db,
            if cancelled {
                Duration::from_secs(10)
            } else {
                Duration::from_millis(120)
            },
            cancel.clone(),
        )
        .await
        .unwrap();
        let identity = tx.run(move |tx, _| Box::pin(async move {
            tx.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
                "INSERT INTO stage75_lifetime_marker VALUES($1,10)", [id.into()]))
                .await.map_err(|_| ManagementError::Database)?;
            let row = tx.query_one(Statement::from_string(DatabaseBackend::Postgres,
                "SELECT pid, backend_start::text AS birth FROM pg_stat_activity WHERE pid=pg_backend_pid()"))
                .await.map_err(|_| ManagementError::Database)?.ok_or(ManagementError::Database)?;
            Ok((row.try_get::<i32>("", "pid").unwrap(), row.try_get::<String>("", "birth").unwrap()))
        })).await.unwrap();
        let birth = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT backend_start::text AS birth FROM pg_stat_activity WHERE pid=$1",
                [identity.0.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "birth")
            .unwrap();
        assert_eq!(birth, identity.1);
        let (release, pending) = tokio::sync::oneshot::channel::<()>();
        let operation = async {
            if pending_business {
                Some(
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        tx.run(|tx, _| {
                            Box::pin(async move {
                                let _ = pending.await;
                                tx.execute_unprepared(
                                    "INSERT INTO stage75_lifetime_marker VALUES(90,10)",
                                )
                                .await
                                .map_err(|_| ManagementError::Database)?;
                                Ok(())
                            })
                        }),
                    )
                    .await,
                )
            } else {
                None
            }
        };
        let writer = async {
            let writer = db.begin().await.unwrap();
            writer
                .execute_unprepared("SET LOCAL statement_timeout='2s'")
                .await
                .unwrap();
            let result = writer
                .execute(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "INSERT INTO stage75_lifetime_marker VALUES($1,20)",
                    [id.into()],
                ))
                .await;
            match result {
                Ok(_) => writer.commit().await.unwrap(),
                Err(error) => {
                    writer.rollback().await.unwrap();
                    panic!("watchdog did not release the actual key lock: {error:?}");
                }
            }
        };
        let trigger = async {
            if cancelled {
                tokio::time::sleep(Duration::from_millis(30)).await;
                cancel.cancel();
            }
        };
        let (result, (), ()) = tokio::join!(operation, writer, trigger);
        if let Some(result) = result {
            let failure = result
                .expect("pending business must stop within cleanup grace")
                .unwrap_err();
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
                "business cannot resume after its rollback acknowledgement"
            );
        }
        let failure = tx.commit().await.unwrap_err();
        assert_eq!(
            failure.cause,
            if cancelled {
                StopCause::Cancelled
            } else {
                StopCause::Deadline
            }
        );
        assert!(failure.rollback_confirmed);
        let row = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT value FROM stage75_lifetime_marker WHERE id=$1",
                [id.into()],
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<i32>("", "value").unwrap(), 20);
    }

    let mut tx = ManagementTransaction::begin(&db, FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    let failure = tx
        .run(|tx, _| {
            Box::pin(async move {
                tx.execute_unprepared("INSERT INTO stage75_lifetime_marker VALUES(99,10)")
                    .await
                    .map_err(|_| ManagementError::Database)?;
                // Only this test-owned transaction's own backend is terminated.
                tx.execute_unprepared("SELECT pg_terminate_backend(pg_backend_pid())")
                    .await
                    .map_err(|_| ManagementError::Database)?;
                Err::<(), _>(ManagementError::Database)
            })
        })
        .await
        .unwrap_err();
    assert!(
        !failure.rollback_confirmed,
        "disconnection is not a rollback acknowledgement"
    );
    assert!(!tx.commit().await.unwrap_err().rollback_confirmed);
    db.execute_unprepared("INSERT INTO stage75_lifetime_marker VALUES(99,20)")
        .await
        .unwrap();
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT count(*) AS n FROM stage75_lifetime_marker WHERE value=10",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
    db.close().await.unwrap();
}
